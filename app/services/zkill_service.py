"""
Void Market — zKillboard Fight Analysis Service

Pulls alliance losses from zKillboard API and:
- Groups kills by system + time window into "fights"
- Filters by minimum loss threshold (alliance-level ops only)
- Maps losses back to doctrine ships
- Tracks what's being burned through fastest
"""
import logging
from datetime import datetime, timezone, timedelta
from collections import defaultdict

import httpx
from sqlalchemy import select, delete, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.database import Fight, FightLoss, SdeType, DoctrineFit

logger = logging.getLogger("void_market.zkill")


class ZkillService:
    """Fetches and analyzes alliance killmail data from zKillboard."""

    def __init__(self):
        self._http: httpx.AsyncClient | None = None

    async def start(self):
        self._http = httpx.AsyncClient(
            timeout=30.0,
            headers={
                "User-Agent": f"VoidMarket/{settings.app_version} (EVE market tool)",
                "Accept-Encoding": "gzip",
            },
        )

    async def stop(self):
        if self._http:
            await self._http.aclose()

    async def fetch_recent_losses(
        self,
        db: AsyncSession,
        days: int = 7,
    ) -> dict:
        """
        Fetch recent alliance losses from zKillboard.

        zKill API: /api/losses/allianceID/{id}/pastSeconds/{seconds}/
        Returns killmail summaries. We then fetch full killmail from ESI.
        """
        alliance_id = settings.alliance_id
        if not alliance_id:
            return {"error": "No alliance_id configured"}

        seconds = days * 86400
        url = f"{settings.zkill_base_url}/losses/allianceID/{alliance_id}/pastSeconds/{seconds}/"

        logger.info(f"Fetching losses for alliance {alliance_id} (past {days} days)")

        try:
            resp = await self._http.get(url)
            resp.raise_for_status()
            zkill_data = resp.json()
        except Exception as e:
            logger.error(f"zKillboard fetch failed: {e}")
            return {"error": str(e)}

        if not isinstance(zkill_data, list):
            return {"error": "Unexpected zKillboard response format"}

        logger.info(f"Got {len(zkill_data)} loss entries from zKillboard")

        # For each zkill entry, fetch the full killmail from ESI
        losses = []
        for entry in zkill_data:
            killmail_id = entry.get("killmail_id")
            zkb = entry.get("zkb", {})
            killmail_hash = zkb.get("hash")

            if not killmail_id or not killmail_hash:
                continue

            try:
                # ESI killmail endpoint (public, no auth needed)
                km_resp = await self._http.get(
                    f"{settings.esi_base_url}/killmails/{killmail_id}/{killmail_hash}/",
                    params={"datasource": "tranquility"},
                )
                km_resp.raise_for_status()
                km = km_resp.json()

                victim = km.get("victim", {})
                losses.append({
                    "killmail_id": killmail_id,
                    "ship_type_id": victim.get("ship_type_id"),
                    "victim_id": victim.get("character_id"),
                    "killed_at": km.get("killmail_time"),
                    "solar_system_id": km.get("solar_system_id"),
                    "total_value": zkb.get("totalValue", 0),
                    "fit_items": [
                        {"type_id": i["item_type_id"], "quantity": i.get("quantity_dropped", 0) + i.get("quantity_destroyed", 0)}
                        for i in victim.get("items", [])
                    ],
                })

                # Be nice to ESI
                if len(losses) % 20 == 0:
                    import asyncio
                    await asyncio.sleep(0.5)

            except Exception as e:
                logger.warning(f"Failed to fetch killmail {killmail_id}: {e}")
                continue

        logger.info(f"Fetched {len(losses)} full killmails")

        # Group into fights
        fights = self._group_into_fights(losses)
        logger.info(f"Grouped into {len(fights)} potential fights")

        # Filter by minimum size
        major_fights = [
            f for f in fights
            if f["loss_count"] >= settings.min_fight_losses
        ]
        logger.info(f"{len(major_fights)} fights meet the {settings.min_fight_losses}-loss threshold")

        # Store fights
        await self._store_fights(db, major_fights)

        return {
            "total_losses": len(losses),
            "fights_detected": len(fights),
            "major_fights": len(major_fights),
        }

    def _group_into_fights(self, losses: list[dict]) -> list[dict]:
        """
        Group killmails into fights by system + time window.

        Logic: Sort by time, iterate. If a kill is in the same system
        and within FIGHT_WINDOW_MINUTES of the last kill in the current
        fight, it belongs to that fight. Otherwise, start a new fight.
        """
        if not losses:
            return []

        # Sort by time
        sorted_losses = sorted(
            losses,
            key=lambda x: x.get("killed_at", ""),
        )

        window = timedelta(minutes=settings.fight_window_minutes)
        fights = []
        current_fight = None

        for loss in sorted_losses:
            kill_time_str = loss.get("killed_at", "")
            if not kill_time_str:
                continue

            kill_time = datetime.fromisoformat(kill_time_str.replace("Z", "+00:00"))
            system_id = loss.get("solar_system_id")

            if (
                current_fight is None
                or system_id != current_fight["system_id"]
                or kill_time - current_fight["last_kill"] > window
            ):
                # Start new fight
                if current_fight:
                    fights.append(current_fight)

                current_fight = {
                    "system_id": system_id,
                    "started_at": kill_time,
                    "last_kill": kill_time,
                    "losses": [loss],
                    "loss_count": 1,
                }
            else:
                # Add to current fight
                current_fight["losses"].append(loss)
                current_fight["last_kill"] = kill_time
                current_fight["loss_count"] += 1

        if current_fight:
            fights.append(current_fight)

        return fights

    async def _store_fights(self, db: AsyncSession, fights: list[dict]):
        """Store fight data in the database."""
        for fight_data in fights:
            total_value = sum(l.get("total_value", 0) for l in fight_data["losses"])

            fight = Fight(
                system_id=fight_data["system_id"],
                system_name=str(fight_data["system_id"]),  # Will resolve later
                started_at=fight_data["started_at"],
                ended_at=fight_data["last_kill"],
                alliance_losses=fight_data["loss_count"],
                total_isk_lost=total_value,
            )
            db.add(fight)
            await db.flush()

            for loss in fight_data["losses"]:
                kill_time = datetime.fromisoformat(
                    loss["killed_at"].replace("Z", "+00:00")
                )
                db.add(FightLoss(
                    killmail_id=loss["killmail_id"],
                    fight_id=fight.id,
                    ship_type_id=loss["ship_type_id"],
                    victim_id=loss.get("victim_id"),
                    killed_at=kill_time,
                    total_value=loss.get("total_value", 0),
                    fit_items=loss.get("fit_items"),
                ))

        await db.commit()

    async def get_loss_summary(self, db: AsyncSession, days: int = 7) -> list[dict]:
        """
        Get a summary of what ships were lost in major fights.
        Returns items sorted by total losses, most lost first.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        result = await db.execute(
            select(
                FightLoss.ship_type_id,
                func.count().label("times_lost"),
                func.sum(FightLoss.total_value).label("total_value"),
            )
            .join(Fight, FightLoss.fight_id == Fight.id)
            .where(Fight.started_at >= cutoff)
            .group_by(FightLoss.ship_type_id)
            .order_by(func.count().desc())
        )

        summary = []
        for row in result.fetchall():
            ship = await db.get(SdeType, row[0])
            summary.append({
                "ship_type_id": row[0],
                "ship_name": ship.name if ship else f"Unknown ({row[0]})",
                "times_lost": row[1],
                "total_isk_lost": row[2] or 0,
            })

        return summary

    async def get_module_loss_summary(self, db: AsyncSession, days: int = 7) -> dict[int, int]:
        """
        Get aggregate module/item losses from fight killmails.
        Parses the fit_items JSON from each loss.

        Returns: {type_id: total_quantity_lost}
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        result = await db.execute(
            select(FightLoss.fit_items)
            .join(Fight, FightLoss.fight_id == Fight.id)
            .where(Fight.started_at >= cutoff)
            .where(FightLoss.fit_items.isnot(None))
        )

        item_totals: dict[int, int] = defaultdict(int)
        for row in result.fetchall():
            items = row[0]
            if isinstance(items, list):
                for item in items:
                    tid = item.get("type_id")
                    qty = item.get("quantity", 1)
                    if tid:
                        item_totals[tid] += qty

        return dict(item_totals)


# Singleton
zkill_service = ZkillService()
