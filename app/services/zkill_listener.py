"""
Void Market — zKillboard Real-Time Listener

Connects to zKillboard's RedisQ feed filtered by alliance_id.
Ingests every killmail in near real-time, stores in killmail.db,
and triggers fight detection + doctrine matching.

Cross-DB design:
  - Writes to: killmail.db (killmails, fights tables)
  - Reads from: void_market.db (doctrine_fits, doctrine_fit_items, sde_types, market_orders)
  - Cross-references happen at the application layer, never via SQL JOINs
"""
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from collections import defaultdict

import httpx
from sqlalchemy import select, func, update, delete, text, Integer

from app.config import settings
from app.models.killmail_models import Killmail, KillmailFight
from app.models.killmail_session import killmail_session, killmail_write_lock
from app.models.session import async_session as market_session

logger = logging.getLogger("void_market.zkill_listener")

R2Z2_BASE = "https://r2z2.zkillboard.com/ephemeral"

FIGHT_WINDOW_MINUTES = 30
MIN_FIGHT_LOSSES = 10

# Watched alliance IDs — kills where victim is in ANY of these are tracked as losses
# Defaults to just the configured alliance_id, expanded by fight settings
_watched_alliance_ids = set()

# Cache SDE lookups so we don't hit market DB on every killmail
_ship_name_cache = {}  # type_id -> name
_system_name_cache = {}  # system_id -> name
_system_to_region = {}  # system_id -> region_id (loaded from SDE)
_deployment_watched_regions = {}  # deployment_id -> set(region_ids) — refreshed periodically
_jita_price_cache = {}  # type_id -> sell_min
_jita_price_cache_time = None
_doctrine_fits_cache = None  # Loaded once, refreshed periodically
_doctrine_fits_cache_time = None


async def _load_region_cache():
    """Load system→region mapping from SDE for fast lookups."""
    global _system_to_region
    if _system_to_region:
        return
    try:
        from app.models.database import SdeSolarSystem
        async with market_session() as db:
            result = await db.execute(
                select(SdeSolarSystem.system_id, SdeSolarSystem.region_id)
            )
            _system_to_region = {r[0]: r[1] for r in result.fetchall()}
            logger.info(f"Region cache loaded: {len(_system_to_region)} systems")
    except Exception as e:
        logger.warning(f"Failed to load region cache: {e}")


async def _load_deployment_regions():
    """Load active deployment watched regions for region-based filtering."""
    global _deployment_watched_regions
    try:
        from app.models.killmail_models import Deployment
        async with killmail_session() as db:
            result = await db.execute(
                select(Deployment).where(Deployment.status == "active")
            )
            _deployment_watched_regions = {}
            for dep in result.scalars().all():
                if dep.watched_region_ids:
                    _deployment_watched_regions[dep.id] = set(dep.watched_region_ids)
            logger.info(f"Loaded {len(_deployment_watched_regions)} active deployments for region filtering")
    except Exception as e:
        logger.warning(f"Failed to load deployment regions: {e}")


def _is_in_watched_region(system_id: int) -> bool:
    """Check if a system is in any active deployment's watched regions."""
    if not _system_to_region or not _deployment_watched_regions:
        return False
    region_id = _system_to_region.get(system_id)
    if not region_id:
        return False
    for dep_regions in _deployment_watched_regions.values():
        if region_id in dep_regions:
            return True
    return False


def get_watched_alliance_ids() -> set:
    """Get the current set of watched alliance IDs."""
    global _watched_alliance_ids
    if not _watched_alliance_ids:
        _watched_alliance_ids = {settings.alliance_id} if settings.alliance_id else set()
    return _watched_alliance_ids


# Bulk import progress (module-level so API can read it)
_bulk_import_status = {"running": False, "total_days": 0, "completed_days": 0, "current_date": "", "results": []}


class ZkillListener:
    """Real-time killmail listener using zKillboard R2Z2 sequential feed."""

    def __init__(self):
        self._http: httpx.AsyncClient | None = None
        self._running = False
        self._task: asyncio.Task | None = None
        self._stats = {
            "total_ingested": 0,
            "fights_detected": 0,
            "doctrine_matches": 0,
            "last_killmail_at": None,
            "listener_status": "stopped",
            "errors": 0,
            "alliance_id": settings.alliance_id,
        }
        self._doctrine_match_pct = 0.60  # Updated by settings endpoint

    async def start(self):
        if not settings.alliance_id:
            logger.warning("No alliance_id configured — zKill listener disabled")
            self._stats["listener_status"] = "disabled (no alliance_id)"
            return

        # Load fight settings from DB (watched alliances, thresholds, etc.)
        await self._load_fight_settings()

        # Load region cache for region-based kill filtering
        await _load_region_cache()
        await _load_deployment_regions()

        self._http = httpx.AsyncClient(
            timeout=30.0,
            headers={
                "User-Agent": f"VoidMarket/{settings.app_version} (EVE market tool)",
                "Accept-Encoding": "gzip",
            },
            follow_redirects=True,
        )
        self._running = True
        self._task = asyncio.create_task(self._listen_loop())
        self._stats["listener_status"] = "running"
        logger.info(f"zKill listener started for alliance {settings.alliance_id}")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._http:
            await self._http.aclose()
        self._stats["listener_status"] = "stopped"
        logger.info("zKill listener stopped")

    def get_stats(self) -> dict:
        return dict(self._stats)

    async def _load_fight_settings(self):
        """Load fight settings from app_settings DB on startup."""
        global _watched_alliance_ids, MIN_FIGHT_LOSSES, FIGHT_WINDOW_MINUTES
        try:
            from app.models.database import AppSetting
            from app.models.session import async_session as market_session
            import json as _json
            async with market_session() as db:
                setting = await db.get(AppSetting, "fight_settings")
                if setting and setting.value:
                    data = _json.loads(setting.value)
                    MIN_FIGHT_LOSSES = data.get("min_fight_losses", 10)
                    FIGHT_WINDOW_MINUTES = data.get("fight_window_minutes", 30)
                    self._doctrine_match_pct = data.get("doctrine_match_pct", 60) / 100.0
                    alliances = data.get("watched_alliances", [])
                    if alliances:
                        _watched_alliance_ids = {a["id"] for a in alliances if a.get("id")}
                        logger.info(f"Loaded {len(_watched_alliance_ids)} watched alliances: {_watched_alliance_ids}")
                    else:
                        _watched_alliance_ids = {settings.alliance_id}
        except Exception as e:
            logger.warning(f"Could not load fight settings: {e}")
            _watched_alliance_ids = {settings.alliance_id}

    # ─── Main Listen Loop (R2Z2 Sequential Feed) ───────

    async def _listen_loop(self):
        """R2Z2 sequential killmail feed — near real-time kills."""
        await self._refresh_caches()

        # Get starting sequence
        sequence = await self._r2z2_get_sequence()
        if not sequence:
            logger.warning("R2Z2: failed to get starting sequence, retrying in 30s")
            self._stats["listener_status"] = "error (no sequence)"
            await asyncio.sleep(30)
            sequence = await self._r2z2_get_sequence()
            if not sequence:
                logger.error("R2Z2: still no sequence, kill feed disabled")
                self._stats["listener_status"] = "disabled (no sequence)"
                return

        logger.info(f"R2Z2: starting at sequence {sequence}")
        self._stats["listener_status"] = "running"
        consecutive_404s = 0

        while self._running:
            try:
                kill_data = await self._r2z2_get_kill(sequence)

                if kill_data is None:
                    # 404 — no more kills yet
                    consecutive_404s += 1
                    await asyncio.sleep(6)  # Mandatory 6s wait per R2Z2 docs

                    # Refresh caches during idle
                    if consecutive_404s % 100 == 0 and self._should_refresh_caches():
                        await self._refresh_caches()
                        await _load_deployment_regions()  # Refresh in case deployments changed
                    continue

                consecutive_404s = 0
                sequence += 1

                # Parse the R2Z2 kill data
                km_data = self._parse_r2z2_kill(kill_data)
                if not km_data:
                    await asyncio.sleep(0.1)
                    continue

                # Region-based filtering: store ALL kills in watched deployment regions
                system_id = km_data.get("solar_system_id")
                in_watched_region = _is_in_watched_region(system_id)
                is_coalition_loss = km_data["is_loss"]

                # Store if: in a watched region OR is a coalition loss anywhere
                if in_watched_region or is_coalition_loss:
                    async with killmail_write_lock:
                        async with killmail_session() as db:
                            stored = await self._store_killmail(db, km_data)
                            if stored:
                                self._stats["total_ingested"] += 1
                                self._stats["last_killmail_at"] = datetime.now(timezone.utc).isoformat()
                                await self._detect_fights(db)
                                await db.commit()

                await asyncio.sleep(0.1)  # 100ms between fetches per R2Z2 best practice

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._stats["errors"] += 1
                logger.error(f"R2Z2 loop error: {e}")
                self._stats["listener_status"] = f"error (retry in 10s)"
                await asyncio.sleep(10)
                self._stats["listener_status"] = "running"

    async def _r2z2_get_sequence(self) -> int | None:
        """Get the current starting sequence from R2Z2."""
        try:
            resp = await self._http.get(f"{R2Z2_BASE}/sequence.json", timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("sequence")
        except Exception as e:
            logger.warning(f"R2Z2 sequence fetch error: {e}")
        return None

    async def _r2z2_get_kill(self, sequence: int) -> dict | None:
        """Fetch a single killmail by sequence number. Returns None on 404."""
        try:
            resp = await self._http.get(f"{R2Z2_BASE}/{sequence}.json", timeout=10)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 404:
                return None
            if resp.status_code == 429:
                logger.warning("R2Z2: rate limited, backing off 30s")
                await asyncio.sleep(30)
                return None
        except Exception as e:
            logger.warning(f"R2Z2 fetch error for seq {sequence}: {e}")
        return None

    # ─── Kill Data Parsing (R2Z2 format) ──────────────

    def _parse_r2z2_kill(self, kill_data: dict) -> dict | None:
        """Parse an R2Z2 kill into our killmail format.
        R2Z2 nests ESI data in an 'esi' block."""
        try:
            killmail_id = kill_data.get("killmail_id")
            if not killmail_id:
                return None

            # R2Z2 nests the ESI killmail inside an "esi" block
            esi_data = kill_data.get("esi", {})
            zkb = kill_data.get("zkb", {})

            victim = esi_data.get("victim", {})
            ship_type_id = victim.get("ship_type_id")
            if not ship_type_id:
                return None

            kill_time = esi_data.get("killmail_time", "")
            if kill_time:
                killed_at = datetime.fromisoformat(kill_time.replace("Z", "+00:00"))
                # Normalize to naive UTC — SQLite doesn't store timezone info
                if killed_at.tzinfo:
                    killed_at = killed_at.replace(tzinfo=None)
            else:
                return None

            # Parse fitted items
            fit_items = []
            for item in victim.get("items", []):
                tid = item.get("item_type_id")
                if tid:
                    fit_items.append({
                        "type_id": tid,
                        "flag": item.get("flag", 0),
                        "qty_destroyed": item.get("quantity_destroyed", 0),
                        "qty_dropped": item.get("quantity_dropped", 0),
                    })

            # Is this our alliance's loss?
            victim_alliance = victim.get("alliance_id")
            is_loss = victim_alliance in get_watched_alliance_ids()

            # Parse attacker data
            attackers_raw = esi_data.get("attackers", [])
            attacker_data = [{
                "alliance_id": a.get("alliance_id"),
                "corp_id": a.get("corporation_id"),
                "character_id": a.get("character_id"),
                "ship_type_id": a.get("ship_type_id"),
                "weapon_type_id": a.get("weapon_type_id"),
                "damage_done": a.get("damage_done", 0),
                "final_blow": a.get("final_blow", False),
            } for a in attackers_raw]

            return {
                "killmail_id": killmail_id,
                "killmail_hash": zkb.get("hash"),
                "ship_type_id": ship_type_id,
                "victim_id": victim.get("character_id"),
                "victim_corp_id": victim.get("corporation_id"),
                "victim_alliance_id": victim_alliance,
                "solar_system_id": esi_data.get("solar_system_id"),
                "killed_at": killed_at,
                "total_value": zkb.get("totalValue", 0),
                "fit_items": fit_items,
                "attacker_data": attacker_data,
                "attacker_count": len(attackers_raw),
                "is_loss": is_loss,
            }
        except Exception as e:
            logger.warning(f"Failed to parse R2Z2 kill: {e}")
            return None

    # ─── Storage ──────────────────────────────────────────

    async def _store_killmail(self, db, km_data: dict) -> bool:
        """Store a killmail. Returns True if new, False if duplicate."""
        existing = await db.get(Killmail, km_data["killmail_id"])
        if existing:
            return False

        # Resolve names from cache
        ship_name = await self._get_ship_name(km_data["ship_type_id"])
        system_name = await self._get_system_name(km_data["solar_system_id"])

        # Resolve system → constellation → region (cached in SystemInfo table)
        sys_info = await self._resolve_system_region(db, km_data["solar_system_id"])
        constellation_id = sys_info.get("constellation_id") if sys_info else None
        region_id = sys_info.get("region_id") if sys_info else None
        region_name = sys_info.get("region_name") if sys_info else None

        # Doctrine matching
        doctrine_fit_id = None
        doctrine_fit_name = None
        doctrine_match_pct = None
        is_doctrine = False

        if km_data["is_loss"] and km_data["fit_items"]:
            match = self._match_doctrine_fit(km_data["ship_type_id"], km_data["fit_items"])
            if match:
                is_doctrine = True
                doctrine_fit_id = match["fit_id"]
                doctrine_fit_name = match["fit_name"]
                doctrine_match_pct = match["match_pct"]
                self._stats["doctrine_matches"] += 1

        # Deployment auto-tagging: match by active deployment's watched regions
        deployment_id = None
        deployment_fight_id = None
        if region_id:
            deployment_id = await self._match_deployment(db, region_id, km_data["killed_at"])
            if deployment_id:
                deployment_fight_id = await self._match_deployment_fight(
                    db, deployment_id, km_data["solar_system_id"], km_data["killed_at"]
                )

        # Filter out capsules and deployables
        is_capsule = ship_name and ("Capsule" in ship_name)
        is_deployable = ship_name and ship_name.startswith("Mobile ")

        # ISK value — R2Z2 and zKill backfill provide zkb.totalValue
        # EVE Ref imports have 0 — use "Fix ISK Values" button to backfill from zKill
        total_value = km_data.get("total_value", 0)

        km = Killmail(
            killmail_id=km_data["killmail_id"],
            killmail_hash=km_data.get("killmail_hash"),
            ship_type_id=km_data["ship_type_id"],
            ship_name=ship_name,
            victim_id=km_data.get("victim_id"),
            victim_corp_id=km_data.get("victim_corp_id"),
            victim_alliance_id=km_data.get("victim_alliance_id"),
            solar_system_id=km_data["solar_system_id"],
            solar_system_name=system_name,
            constellation_id=constellation_id,
            region_id=region_id,
            region_name=region_name,
            killed_at=km_data["killed_at"],
            total_value=total_value,
            fit_items=km_data["fit_items"],
            attacker_data=km_data.get("attacker_data"),
            attacker_count=km_data.get("attacker_count", 0),
            is_loss=km_data["is_loss"],
            is_doctrine_loss=is_doctrine,
            doctrine_fit_id=doctrine_fit_id,
            doctrine_fit_name=doctrine_fit_name,
            doctrine_match_pct=doctrine_match_pct,
            deployment_id=deployment_id,
            deployment_fight_id=deployment_fight_id,
        )
        db.add(km)
        await db.flush()

        if not is_capsule and not is_deployable:
            deploy_tag = f" [DEPLOY:{deployment_id}]" if deployment_id else ""
            fight_tag = f" [FIGHT:{deployment_fight_id}]" if deployment_fight_id else ""
            logger.info(f"Killmail {km_data['killmail_id']}: {ship_name} in {system_name}"
                        f" ({'LOSS' if km_data['is_loss'] else 'KILL'})"
                        f"{' [DOCTRINE: ' + doctrine_fit_name + ']' if is_doctrine else ''}"
                        f"{deploy_tag}{fight_tag}")

        return True

    # ─── Region & Deployment Resolution ──────────────────

    async def _resolve_system_region(self, db, system_id: int) -> dict | None:
        """Resolve system → constellation → region. Uses local SDE first, then ESI fallback."""
        if not system_id:
            return None
        from app.models.killmail_models import SystemInfo

        # Check cache first
        cached = await db.get(SystemInfo, system_id)
        if cached and cached.region_id:
            return {
                "system_name": cached.system_name,
                "constellation_id": cached.constellation_id,
                "constellation_name": cached.constellation_name,
                "region_id": cached.region_id,
                "region_name": cached.region_name,
            }

        # Try local SDE (market DB)
        try:
            from app.models.database import SdeSolarSystem, SdeConstellation, SdeRegion
            async with market_session() as mdb:
                sys_row = await mdb.get(SdeSolarSystem, system_id)
                if sys_row:
                    const_row = await mdb.get(SdeConstellation, sys_row.constellation_id)
                    reg_row = await mdb.get(SdeRegion, sys_row.region_id)
                    info = SystemInfo(
                        system_id=system_id,
                        system_name=sys_row.name,
                        constellation_id=sys_row.constellation_id,
                        constellation_name=const_row.name if const_row else None,
                        region_id=sys_row.region_id,
                        region_name=reg_row.name if reg_row else None,
                    )
                    db.add(info)
                    await db.flush()
                    return {
                        "system_name": sys_row.name,
                        "constellation_id": sys_row.constellation_id,
                        "constellation_name": const_row.name if const_row else None,
                        "region_id": sys_row.region_id,
                        "region_name": reg_row.name if reg_row else None,
                    }
        except Exception as e:
            logger.debug(f"SDE geo lookup failed for {system_id}: {e}")

        # Fall back to ESI chain: system → constellation → region
        try:
            sys_resp = await self._http.get(
                f"https://esi.evetech.net/latest/universe/systems/{system_id}/",
                params={"datasource": "tranquility"}, timeout=10
            )
            if sys_resp.status_code != 200:
                return None
            sys_data = sys_resp.json()
            constellation_id = sys_data.get("constellation_id")
            system_name = sys_data.get("name")

            constellation_name = None
            region_id = None
            region_name = None

            if constellation_id:
                con_resp = await self._http.get(
                    f"https://esi.evetech.net/latest/universe/constellations/{constellation_id}/",
                    params={"datasource": "tranquility"}, timeout=10
                )
                if con_resp.status_code == 200:
                    con_data = con_resp.json()
                    constellation_name = con_data.get("name")
                    region_id = con_data.get("region_id")

                    if region_id:
                        reg_resp = await self._http.get(
                            f"https://esi.evetech.net/latest/universe/regions/{region_id}/",
                            params={"datasource": "tranquility"}, timeout=10
                        )
                        if reg_resp.status_code == 200:
                            region_name = reg_resp.json().get("name")

            # Cache result
            info = SystemInfo(
                system_id=system_id,
                system_name=system_name,
                constellation_id=constellation_id,
                constellation_name=constellation_name,
                region_id=region_id,
                region_name=region_name,
            )
            db.add(info)
            await db.flush()

            return {
                "system_name": system_name,
                "constellation_id": constellation_id,
                "constellation_name": constellation_name,
                "region_id": region_id,
                "region_name": region_name,
            }
        except Exception as e:
            logger.warning(f"Failed to resolve system {system_id}: {e}")
            return None

    async def _match_deployment(self, db, region_id: int, killed_at: datetime) -> int | None:
        """Find an active deployment watching this region."""
        from app.models.killmail_models import Deployment

        # Strip timezone for Python comparisons (SQLite stores naive datetimes)
        killed_at_naive = killed_at.replace(tzinfo=None) if killed_at.tzinfo else killed_at

        result = await db.execute(select(Deployment))
        for dep in result.scalars().all():
            # Skip ended deployments where kill is after end date
            if dep.ended_at and dep.ended_at < killed_at_naive:
                continue
            # Check if region is watched
            watched = dep.watched_region_ids or []
            if region_id in watched:
                return dep.id
        return None

    async def _match_deployment_fight(self, db, deployment_id: int,
                                      system_id: int, killed_at: datetime) -> int | None:
        """Find a named fight in this deployment matching system + time window."""
        from app.models.killmail_models import DeploymentFight, DeploymentFightSystem

        result = await db.execute(
            select(DeploymentFightSystem, DeploymentFight)
            .join(DeploymentFight, DeploymentFight.id == DeploymentFightSystem.deployment_fight_id)
            .where(DeploymentFight.deployment_id == deployment_id)
            .where(DeploymentFightSystem.system_id == system_id)
            .where(DeploymentFightSystem.start_time <= killed_at)
            .where(DeploymentFightSystem.end_time >= killed_at)
        )
        row = result.first()
        if row:
            return row[0].deployment_fight_id
        return None

    # ─── Fight Detection ──────────────────────────────────

    async def _detect_fights(self, db):
        """
        Run fight detection on recent killmails.
        Groups losses in the same system within FIGHT_WINDOW_MINUTES into fights.
        """
        # Get unassigned killmails from the last 2 hours
        cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=2)
        result = await db.execute(
            select(Killmail)
            .where(Killmail.killed_at >= cutoff)
            .where(Killmail.fight_id.is_(None))
            .where(~Killmail.ship_name.like("Capsule%"))
            .where(~Killmail.ship_name.like("Mobile %"))
            .order_by(Killmail.killed_at.asc())
        )
        unassigned = result.scalars().all()

        if not unassigned:
            return

        window = timedelta(minutes=FIGHT_WINDOW_MINUTES)

        for km in unassigned:
            # Try to find an existing active fight in the same system
            fight = await db.execute(
                select(KillmailFight)
                .where(KillmailFight.system_id == km.solar_system_id)
                .where(KillmailFight.is_active == True)
                .where(KillmailFight.ended_at >= km.killed_at - window)
                .limit(1)
            )
            existing_fight = fight.scalar_one_or_none()

            if existing_fight:
                # Add to existing fight
                km.fight_id = existing_fight.id
                existing_fight.ended_at = max(existing_fight.ended_at, km.killed_at)
                existing_fight.alliance_losses += 1
                existing_fight.total_isk_lost += km.total_value or 0
                if km.is_doctrine_loss:
                    existing_fight.doctrine_losses += 1
            else:
                # Start new fight
                new_fight = KillmailFight(
                    system_id=km.solar_system_id,
                    system_name=km.solar_system_name or str(km.solar_system_id),
                    started_at=km.killed_at,
                    ended_at=km.killed_at,
                    alliance_losses=1,
                    total_isk_lost=km.total_value or 0,
                    is_active=True,
                    doctrine_losses=1 if km.is_doctrine_loss else 0,
                )
                db.add(new_fight)
                await db.flush()
                km.fight_id = new_fight.id

        # Mark fights as inactive if no kills for FIGHT_WINDOW_MINUTES
        stale_cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - window
        await db.execute(
            update(KillmailFight)
            .where(KillmailFight.is_active == True)
            .where(KillmailFight.ended_at < stale_cutoff)
            .values(is_active=False)
        )

        # Update top_ship_lost for each fight that changed
        fight_ids = set(km.fight_id for km in unassigned if km.fight_id)
        for fid in fight_ids:
            top = await db.execute(
                select(Killmail.ship_name, func.count().label("cnt"))
                .where(Killmail.fight_id == fid)
                .where(~Killmail.ship_name.like("Capsule%"))
                .where(~Killmail.ship_name.like("Mobile %"))
                .group_by(Killmail.ship_name)
                .order_by(func.count().desc())
                .limit(1)
            )
            row = top.fetchone()
            if row:
                await db.execute(
                    update(KillmailFight)
                    .where(KillmailFight.id == fid)
                    .values(top_ship_lost=f"{row[0]} ×{row[1]}")
                )

        await db.flush()

    # ─── Doctrine Matching ────────────────────────────────

    def _match_doctrine_fit(self, ship_type_id: int, fit_items: list) -> dict | None:
        """
        Compare a killmail's fit against stored doctrine fits.
        Returns best match if >= 60% of doctrine items are present.
        """
        if not _doctrine_fits_cache:
            return None

        # Get doctrine fits for this ship type
        fits_for_ship = _doctrine_fits_cache.get(ship_type_id)
        if not fits_for_ship:
            return None

        # Build set of type_ids from the killmail fit
        km_types = set()
        for item in fit_items:
            tid = item.get("type_id")
            if tid:
                km_types.add(tid)

        best_match = None
        best_pct = 0

        for fit in fits_for_ship:
            doctrine_types = fit["item_type_ids"]
            if not doctrine_types:
                continue

            # How many doctrine items are present in the killmail?
            matched = len(km_types & doctrine_types)
            total = len(doctrine_types)
            pct = (matched / total * 100) if total > 0 else 0

            if pct >= (self._doctrine_match_pct * 100) and pct > best_pct:
                best_pct = pct
                best_match = {
                    "fit_id": fit["fit_id"],
                    "fit_name": fit["fit_name"],
                    "match_pct": round(pct, 1),
                }

        return best_match

    # ─── Cache Management ─────────────────────────────────

    async def _refresh_caches(self):
        """Load doctrine fits and SDE data from market DB into memory."""
        global _doctrine_fits_cache, _doctrine_fits_cache_time

        try:
            async with market_session() as db:
                from app.models.database import DoctrineFit, DoctrineFitItem, SdeType

                # Load doctrine fits grouped by ship_type_id
                fits_result = await db.execute(
                    select(DoctrineFit)
                )
                fits = fits_result.scalars().all()

                cache = defaultdict(list)
                for fit in fits:
                    items_result = await db.execute(
                        select(DoctrineFitItem.type_id)
                        .where(DoctrineFitItem.fit_id == fit.id)
                    )
                    item_ids = set(r[0] for r in items_result.fetchall())

                    cache[fit.ship_type_id].append({
                        "fit_id": fit.id,
                        "fit_name": fit.name,
                        "item_type_ids": item_ids,
                    })

                _doctrine_fits_cache = dict(cache)
                _doctrine_fits_cache_time = datetime.now(timezone.utc)
                logger.info(f"Doctrine cache loaded: {len(fits)} fits for {len(cache)} ship types")

        except Exception as e:
            logger.error(f"Failed to refresh doctrine cache: {e}")

    def _should_refresh_caches(self) -> bool:
        if not _doctrine_fits_cache_time:
            return True
        return (datetime.now(timezone.utc) - _doctrine_fits_cache_time).total_seconds() > 3600

    async def _calc_isk_from_fit(self, ship_type_id: int, fit_items: list) -> float:
        """Calculate killmail ISK value from hull + fit items using Jita prices."""
        global _jita_price_cache, _jita_price_cache_time

        # Refresh price cache every 6 hours
        if not _jita_price_cache or not _jita_price_cache_time or \
           (datetime.now(timezone.utc) - _jita_price_cache_time).total_seconds() > 21600:
            try:
                async with market_session() as db:
                    result = await db.execute(text("SELECT type_id, sell_min FROM jita_prices WHERE sell_min > 0"))
                    _jita_price_cache = {r[0]: float(r[1]) for r in result.fetchall()}
                    _jita_price_cache_time = datetime.now(timezone.utc)
                    logger.info(f"Jita price cache loaded: {len(_jita_price_cache)} prices")
            except Exception as e:
                logger.warning(f"Failed to load Jita prices: {e}")
                return 0

        total = _jita_price_cache.get(ship_type_id, 0)
        for item in fit_items:
            tid = item.get("type_id")
            if not tid:
                continue
            qty = (item.get("qty_destroyed", 0) or 0) + (item.get("qty_dropped", 0) or 0)
            total += _jita_price_cache.get(tid, 0) * qty
        return total

    async def _get_ship_name(self, type_id: int) -> str | None:
        if type_id in _ship_name_cache:
            return _ship_name_cache[type_id]

        try:
            async with market_session() as db:
                from app.models.database import SdeType
                sde = await db.get(SdeType, type_id)
                name = sde.name if sde else None
                _ship_name_cache[type_id] = name
                return name
        except Exception:
            return None

    async def _get_system_name(self, system_id: int) -> str | None:
        if not system_id:
            return None
        if system_id in _system_name_cache:
            return _system_name_cache[system_id]

        # Try ESI (public, no auth needed)
        try:
            resp = await self._http.get(
                f"https://esi.evetech.net/latest/universe/systems/{system_id}/",
                params={"datasource": "tranquility"},
            )
            if resp.status_code == 200:
                name = resp.json().get("name")
                _system_name_cache[system_id] = name
                return name
        except Exception:
            pass

        _system_name_cache[system_id] = str(system_id)
        return str(system_id)


# ─── Cross-DB Query Helpers ──────────────────────────────
# These functions read from killmail.db and market.db separately,
# combining results at the application layer.

async def get_fight_timeline(days: int = 30, min_losses: int = MIN_FIGHT_LOSSES, deployment_id: int | None = None) -> list:
    """Get fight timeline from killmail DB. Optionally scoped to a deployment."""
    if deployment_id:
        # Use deployment start date instead of days cutoff
        async with killmail_session() as db:
            from app.models.killmail_models import Deployment
            dep = await db.get(Deployment, deployment_id)
            cutoff = dep.started_at if dep and dep.started_at else (datetime.now(timezone.utc) - timedelta(days=days))
    else:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    async with killmail_session() as db:
        query = select(KillmailFight).where(KillmailFight.started_at >= cutoff).where(KillmailFight.alliance_losses >= min_losses)

        # If deployment scoped, only show fights with killmails in this deployment
        if deployment_id:
            fight_ids_q = select(Killmail.fight_id).where(
                Killmail.deployment_id == deployment_id,
                Killmail.fight_id.isnot(None)
            ).distinct()
            query = query.where(KillmailFight.id.in_(fight_ids_q))

        result = await db.execute(query.order_by(KillmailFight.started_at.desc()))
        fights = result.scalars().all()

        timeline = []
        for f in fights:
            # Get ship breakdown for this fight
            ships = await db.execute(
                select(Killmail.ship_name, Killmail.ship_type_id,
                       func.count().label("cnt"),
                       func.sum(Killmail.total_value).label("isk"),
                       func.sum(func.cast(Killmail.is_doctrine_loss, Integer)).label("doctrine_cnt"))
                .where(Killmail.fight_id == f.id)
                .where(~Killmail.ship_name.like("Capsule%"))
                .where(~Killmail.ship_name.like("Mobile %"))
                .group_by(Killmail.ship_type_id, Killmail.ship_name)
                .order_by(func.count().desc())
            )
            ship_breakdown = [{
                "ship_name": r[0], "ship_type_id": r[1],
                "count": r[2], "isk": r[3] or 0,
                "doctrine_count": r[4] or 0,
            } for r in ships.fetchall()]

            duration_min = (f.ended_at - f.started_at).total_seconds() / 60

            timeline.append({
                "id": f.id,
                "system_name": f.system_name,
                "started_at": f.started_at.isoformat(),
                "ended_at": f.ended_at.isoformat(),
                "duration_min": round(duration_min, 0),
                "alliance_losses": f.alliance_losses,
                "doctrine_losses": f.doctrine_losses,
                "total_isk_lost": f.total_isk_lost,
                "is_active": f.is_active,
                "top_ship": f.top_ship_lost,
                "ships": ship_breakdown,
            })

        return timeline


async def get_doctrine_impact(days: int = 7, deployment_id: int | None = None) -> list:
    """
    Cross-DB: reads losses from killmail.db, reads doctrine fits + market stock from market.db.
    Returns per-doctrine-fit impact assessment. Optionally scoped to a deployment.
    """
    if deployment_id:
        async with killmail_session() as db:
            from app.models.killmail_models import Deployment
            dep = await db.get(Deployment, deployment_id)
            cutoff = dep.started_at if dep and dep.started_at else (datetime.now(timezone.utc) - timedelta(days=days))
    else:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    # Step 1: Get doctrine losses from killmail DB
    losses_by_fit = defaultdict(int)
    losses_by_ship = defaultdict(int)
    async with killmail_session() as db:
        # Build base filter
        base_filter = [Killmail.killed_at >= cutoff, Killmail.is_loss == True]
        if deployment_id:
            base_filter.append(Killmail.deployment_id == deployment_id)

        # Doctrine-matched losses
        result = await db.execute(
            select(Killmail.doctrine_fit_id, Killmail.ship_type_id, func.count())
            .where(*base_filter)
            .where(Killmail.is_doctrine_loss == True)
            .group_by(Killmail.doctrine_fit_id, Killmail.ship_type_id)
        )
        for fit_id, ship_tid, cnt in result.fetchall():
            losses_by_fit[fit_id] = cnt
            losses_by_ship[ship_tid] += cnt

        # All ship losses (not just doctrine)
        all_result = await db.execute(
            select(Killmail.ship_type_id, func.count())
            .where(*base_filter)
            .where(~Killmail.ship_name.like("Capsule%"))
            .where(~Killmail.ship_name.like("Mobile %"))
            .group_by(Killmail.ship_type_id)
        )
        all_losses_by_ship = {r[0]: r[1] for r in all_result.fetchall()}

    # Step 2: Get doctrine fits + market stock from market DB
    async with market_session() as db:
        from app.models.database import Doctrine, DoctrineFit, SdeType, MarketOrder

        doctrines_result = await db.execute(
            select(Doctrine)
        )
        doctrines = doctrines_result.scalars().all()

        impact = []
        for doctrine in doctrines:
            fits_result = await db.execute(
                select(DoctrineFit)
                .where(DoctrineFit.doctrine_id == doctrine.id)
            )
            fits = fits_result.scalars().all()

            doctrine_data = {
                "doctrine_id": doctrine.id,
                "doctrine_name": doctrine.name,
                "fits": [],
            }

            for fit in fits:
                sde = await db.get(SdeType, fit.ship_type_id)
                ship_name = sde.name if sde else f"Type {fit.ship_type_id}"

                # Market stock
                stock_result = await db.execute(
                    select(func.sum(MarketOrder.volume_remain))
                    .where(MarketOrder.type_id == fit.ship_type_id)
                    .where(MarketOrder.is_buy_order == False)
                    .where(MarketOrder.location_id == settings.keepstar_structure_id)
                )
                stock = stock_result.scalar() or 0

                doctrine_lost = losses_by_fit.get(fit.id, 0)
                all_lost = all_losses_by_ship.get(fit.ship_type_id, 0)

                short = max(0, fit.min_stock - stock)
                status = "ok" if stock >= fit.min_stock else ("critical" if stock < fit.min_stock * 0.5 else "low")

                doctrine_data["fits"].append({
                    "fit_id": fit.id,
                    "fit_name": fit.name,
                    "ship_type_id": fit.ship_type_id,
                    "ship_name": ship_name,
                    "role": fit.role,
                    "min_stock": fit.min_stock,
                    "market_stock": stock,
                    "doctrine_losses": doctrine_lost,
                    "total_losses": all_lost,
                    "short": short,
                    "status": status,
                })

            impact.append(doctrine_data)

        return impact


async def get_restock_list(days: int = 7, deployment_id: int | None = None) -> dict:
    """
    Cross-DB: reads losses from killmail.db, reads market stock + Jita prices from market.db.
    Returns hull + module restock shopping list. Optionally scoped to a deployment.
    """
    if deployment_id:
        async with killmail_session() as db:
            from app.models.killmail_models import Deployment
            dep = await db.get(Deployment, deployment_id)
            cutoff = dep.started_at if dep and dep.started_at else (datetime.now(timezone.utc) - timedelta(days=days))
    else:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    # Step 1: Aggregate module losses from killmail DB
    hull_losses = defaultdict(int)
    module_losses = defaultdict(int)

    async with killmail_session() as db:
        query = select(Killmail).where(
            Killmail.killed_at >= cutoff,
            Killmail.is_loss == True,
            Killmail.is_doctrine_loss == True,
        )
        if deployment_id:
            query = query.where(Killmail.deployment_id == deployment_id)
        result = await db.execute(query)
        for km in result.scalars().all():
            hull_losses[km.ship_type_id] += 1
            if km.fit_items:
                for item in km.fit_items:
                    tid = item.get("type_id")
                    qty = (item.get("qty_destroyed", 0) or 0) + (item.get("qty_dropped", 0) or 0)
                    if tid and qty > 0:
                        module_losses[tid] += qty

    if not hull_losses and not module_losses:
        return {"hulls": [], "modules": [], "total_cost": 0, "multibuy": ""}

    # Step 2: Cross-reference with market stock + Jita prices from market DB
    all_type_ids = set(hull_losses.keys()) | set(module_losses.keys())

    async with market_session() as db:
        from app.models.database import SdeType, MarketOrder, JitaPrice

        hulls = []
        modules = []
        total_cost = 0
        multibuy_lines = []

        for tid in all_type_ids:
            sde = await db.get(SdeType, tid)
            name = sde.name if sde else f"Type {tid}"

            # Market stock
            stock_r = await db.execute(
                select(func.sum(MarketOrder.volume_remain))
                .where(MarketOrder.type_id == tid)
                .where(MarketOrder.is_buy_order == False)
                .where(MarketOrder.location_id == settings.keepstar_structure_id)
            )
            stock = stock_r.scalar() or 0

            # Jita price
            jp = await db.get(JitaPrice, tid)
            jita_price = jp.sell_min if jp and jp.sell_min else 0

            lost = hull_losses.get(tid, 0) + module_losses.get(tid, 0)
            cost = jita_price * lost
            total_cost += cost

            entry = {
                "type_id": tid,
                "name": name,
                "lost": lost,
                "stock": stock,
                "jita_price": round(jita_price, 2),
                "total_cost": round(cost, 2),
                "needs_restock": stock < lost,
            }

            if tid in hull_losses:
                hulls.append(entry)
                if stock < hull_losses[tid]:
                    multibuy_lines.append(f"{name} {hull_losses[tid] - stock}")
            else:
                modules.append(entry)

        hulls.sort(key=lambda x: -x["lost"])
        modules.sort(key=lambda x: -x["total_cost"])

    return {
        "hulls": hulls,
        "modules": modules[:50],  # Top 50 modules by cost
        "total_cost": round(total_cost, 2),
        "multibuy": "\n".join(multibuy_lines),
    }


async def get_loss_trends(weeks: int = 8, deployment_id: int | None = None) -> list:
    """Weekly burn rates per ship type from killmail DB. Optionally scoped to a deployment."""
    if deployment_id:
        async with killmail_session() as db:
            from app.models.killmail_models import Deployment
            dep = await db.get(Deployment, deployment_id)
            cutoff = dep.started_at if dep and dep.started_at else (datetime.now(timezone.utc) - timedelta(weeks=weeks))
    else:
        cutoff = datetime.now(timezone.utc) - timedelta(weeks=weeks)

    deploy_filter = ""
    params = {"cutoff": cutoff}
    if deployment_id:
        deploy_filter = "AND deployment_id = :dep_id"
        params["dep_id"] = deployment_id

    async with killmail_session() as db:
        result = await db.execute(text(f"""
            SELECT ship_type_id, ship_name,
                   strftime('%Y-W%W', killed_at) as week,
                   COUNT(*) as cnt,
                   SUM(is_doctrine_loss) as doctrine_cnt
            FROM killmails
            WHERE killed_at >= :cutoff
              AND is_loss = 1
              AND ship_name NOT LIKE 'Capsule%'
              AND ship_name NOT LIKE 'Mobile %'
              {deploy_filter}
            GROUP BY ship_type_id, week
            ORDER BY ship_type_id, week
        """).bindparams(**params))
        rows = result.fetchall()

    # Aggregate into per-ship weekly averages
    ship_data = defaultdict(lambda: {"weeks": defaultdict(int), "doctrine_weeks": defaultdict(int)})
    all_weeks = set()
    for ship_tid, ship_name, week, cnt, doc_cnt in rows:
        ship_data[(ship_tid, ship_name)]["weeks"][week] = cnt
        ship_data[(ship_tid, ship_name)]["doctrine_weeks"][week] = doc_cnt or 0
        all_weeks.add(week)

    num_weeks = max(len(all_weeks), 1)
    trends = []
    for (ship_tid, ship_name), data in ship_data.items():
        total = sum(data["weeks"].values())
        doctrine_total = sum(data["doctrine_weeks"].values())
        avg_per_week = total / num_weeks

        trends.append({
            "ship_type_id": ship_tid,
            "ship_name": ship_name,
            "total_lost": total,
            "doctrine_lost": doctrine_total,
            "avg_per_week": round(avg_per_week, 1),
            "weeks": dict(data["weeks"]),
        })

    trends.sort(key=lambda x: -x["total_lost"])
    return trends[:30]


# ─── Comprehensive Burn Rate ─────────────────────────

async def get_burn_rate(deployment_id: int | None = None) -> dict:
    """
    Cross-DB: aggregate ALL items destroyed (ships, modules, ammo, drones, paste)
    from ALL losses in a deployment. Not just doctrine kills.
    Returns items grouped by category with stock levels and days-of-stock.
    """
    from app.models.session import async_session as market_session
    from collections import defaultdict

    # Step 1: Get deployment date range
    if deployment_id:
        async with killmail_session() as db:
            from app.models.killmail_models import Deployment
            dep = await db.get(Deployment, deployment_id)
            cutoff = dep.started_at if dep and dep.started_at else (datetime.now(timezone.utc) - timedelta(days=30))
    else:
        cutoff = datetime.now(timezone.utc) - timedelta(days=30)

    # Step 2: Aggregate all items from all losses
    hull_counts = defaultdict(int)      # type_id -> count
    item_counts = defaultdict(int)      # type_id -> total qty destroyed+dropped
    total_kills = 0
    deployment_days = max((datetime.now(timezone.utc).replace(tzinfo=None) - cutoff.replace(tzinfo=None) if cutoff.tzinfo else datetime.utcnow() - cutoff).days, 1)

    async with killmail_session() as db:
        query = (
            select(Killmail)
            .where(Killmail.is_loss == True)
            .where(Killmail.killed_at >= cutoff)
        )
        if deployment_id:
            query = query.where(Killmail.deployment_id == deployment_id)

        result = await db.execute(query)
        for km in result.scalars().all():
            total_kills += 1
            if km.ship_name and not km.ship_name.startswith("Capsule") and not km.ship_name.startswith("Mobile "):
                hull_counts[km.ship_type_id] += 1
            if km.fit_items:
                for item in km.fit_items:
                    tid = item.get("type_id")
                    qty = (item.get("qty_destroyed", 0) or 0) + (item.get("qty_dropped", 0) or 0)
                    if tid and qty > 0:
                        item_counts[tid] += qty

    # Merge hulls into items (hull qty = 1 per loss)
    all_type_ids = set(hull_counts.keys()) | set(item_counts.keys())
    merged = {}
    for tid in all_type_ids:
        merged[tid] = {
            "hull_losses": hull_counts.get(tid, 0),
            "item_losses": item_counts.get(tid, 0),
            "total_qty": hull_counts.get(tid, 0) + item_counts.get(tid, 0),
        }

    if not merged:
        return {"categories": [], "summary": {}, "deployment_days": deployment_days, "total_kills": total_kills}

    # Step 3: Cross-reference with market DB
    async with market_session() as db:
        from app.models.database import SdeType, SdeGroup, JitaPrice, MarketOrder

        # Get SDE info (name, group, category)
        type_ids_list = list(merged.keys())
        type_info = {}
        for batch_start in range(0, len(type_ids_list), 500):
            batch = type_ids_list[batch_start:batch_start + 500]
            r = await db.execute(
                select(SdeType.type_id, SdeType.name, SdeType.group_id, SdeType.category_id)
                .where(SdeType.type_id.in_(batch))
            )
            for row in r.fetchall():
                type_info[row[0]] = {"name": row[1], "group_id": row[2], "category_id": row[3]}

        # Get group names
        group_ids = list(set(v["group_id"] for v in type_info.values() if v.get("group_id")))
        group_names = {}
        if group_ids:
            for batch_start in range(0, len(group_ids), 500):
                batch = group_ids[batch_start:batch_start + 500]
                try:
                    r = await db.execute(
                        select(SdeGroup.group_id, SdeGroup.name).where(SdeGroup.group_id.in_(batch))
                    )
                    for row in r.fetchall():
                        group_names[row[0]] = row[1]
                except Exception:
                    pass

        # Get Jita prices
        jita_prices = {}
        for batch_start in range(0, len(type_ids_list), 500):
            batch = type_ids_list[batch_start:batch_start + 500]
            r = await db.execute(
                select(JitaPrice.type_id, JitaPrice.sell_min).where(JitaPrice.type_id.in_(batch))
            )
            for row in r.fetchall():
                jita_prices[row[0]] = float(row[1] or 0)

        # Get current Keepstar stock
        keepstar_id = settings.keepstar_structure_id
        stock_levels = {}
        if keepstar_id:
            for batch_start in range(0, len(type_ids_list), 500):
                batch = type_ids_list[batch_start:batch_start + 500]
                r = await db.execute(
                    select(MarketOrder.type_id, func.sum(MarketOrder.volume_remain))
                    .where(MarketOrder.type_id.in_(batch))
                    .where(MarketOrder.is_buy_order == False)
                    .where(MarketOrder.location_id == keepstar_id)
                    .group_by(MarketOrder.type_id)
                )
                for row in r.fetchall():
                    stock_levels[row[0]] = int(row[1] or 0)

        # Get staging Keepstar stock (if deployment has one configured)
        staging_stock_levels = {}
        staging_id = None
        staging_name = None
        if deployment_id:
            async with killmail_session() as kdb:
                from app.models.killmail_models import Deployment as Dep2
                dep2 = await kdb.get(Dep2, deployment_id)
                if dep2 and dep2.staging_structure_id:
                    staging_id = dep2.staging_structure_id
                    staging_name = dep2.staging_structure_name

            if staging_id:
                for batch_start in range(0, len(type_ids_list), 500):
                    batch = type_ids_list[batch_start:batch_start + 500]
                    r = await db.execute(
                        select(MarketOrder.type_id, func.sum(MarketOrder.volume_remain))
                        .where(MarketOrder.type_id.in_(batch))
                        .where(MarketOrder.is_buy_order == False)
                        .where(MarketOrder.location_id == staging_id)
                        .group_by(MarketOrder.type_id)
                    )
                    for row in r.fetchall():
                        staging_stock_levels[row[0]] = int(row[1] or 0)

    # Step 4: Build categorized output
    CATEGORY_NAMES = {6: "Ships", 7: "Modules", 8: "Charges & Ammo", 18: "Drones", 20: "Implants & Boosters", 32: "Subsystems"}
    categories = defaultdict(list)
    total_isk_burned = 0

    for tid, data in merged.items():
        info = type_info.get(tid, {})
        name = info.get("name", f"Type {tid}")
        cat_id = info.get("category_id", 0)
        group_id = info.get("group_id")
        cat_name = CATEGORY_NAMES.get(cat_id, "Other")
        group_name = group_names.get(group_id, "")
        jita = jita_prices.get(tid, 0)
        stock = stock_levels.get(tid, 0)
        staging_stock = staging_stock_levels.get(tid, 0)
        total_stock = stock + staging_stock
        total_qty = data["total_qty"]
        isk_burned = jita * total_qty
        total_isk_burned += isk_burned
        daily_rate = total_qty / deployment_days if deployment_days > 0 else 0
        days_of_stock = total_stock / daily_rate if daily_rate > 0 else 999

        categories[cat_name].append({
            "type_id": tid,
            "name": name,
            "group_name": group_name,
            "total_destroyed": total_qty,
            "hull_losses": data["hull_losses"],
            "daily_rate": round(daily_rate, 1),
            "jita_price": jita,
            "isk_burned": isk_burned,
            "stock": stock,
            "staging_stock": staging_stock,
            "total_stock": total_stock,
            "days_of_stock": round(days_of_stock, 1),
        })

    # Sort each category by ISK burned descending
    result_categories = []
    for cat_name in ["Ships", "Modules", "Charges & Ammo", "Drones", "Implants & Boosters", "Other"]:
        items = categories.get(cat_name, [])
        if not items:
            continue
        items.sort(key=lambda x: -x["isk_burned"])
        cat_isk = sum(i["isk_burned"] for i in items)
        result_categories.append({
            "category": cat_name,
            "total_isk": cat_isk,
            "item_count": len(items),
            "items": items[:50],  # Top 50 per category
        })

    return {
        "categories": result_categories,
        "summary": {
            "total_kills": total_kills,
            "total_isk_burned": total_isk_burned,
            "deployment_days": deployment_days,
            "avg_daily_isk": total_isk_burned / deployment_days if deployment_days > 0 else 0,
        },
        "staging": {
            "structure_id": staging_id,
            "structure_name": staging_name,
        } if staging_id else None,
        "deployment_days": deployment_days,
        "total_kills": total_kills,
    }


async def get_restock_alerts(deployment_id: int | None = None, hours: int = 24) -> list:
    """
    Feature 2: Post-Fight Restock Alerts.
    Look at recent fights, aggregate doctrine ship losses per fight,
    cross-reference with staging + home stock, suggest contract counts.
    """
    from app.models.session import async_session as market_session
    from app.models.killmail_models import Deployment, DeploymentFight, DeploymentFightSystem

    if not deployment_id:
        return []

    # Get deployment + staging info
    staging_id = None
    async with killmail_session() as db:
        dep = await db.get(Deployment, deployment_id)
        if not dep:
            return []
        staging_id = dep.staging_structure_id

        # Get recent fights (last N hours)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        fights_r = await db.execute(
            select(DeploymentFight)
            .where(DeploymentFight.deployment_id == deployment_id)
            .order_by(DeploymentFight.created_at.desc())
        )
        all_fights = fights_r.scalars().all()

    # For each fight, check if it has killmails in the time window
    alerts = []
    for fight in all_fights:
        async with killmail_session() as db:
            # Get fight time range from killmails
            time_r = await db.execute(
                select(func.min(Killmail.killed_at), func.max(Killmail.killed_at))
                .where(Killmail.deployment_fight_id == fight.id)
            )
            times = time_r.first()
            if not times or not times[0]:
                continue
            fight_end = times[1]

            # Only include fights that ended within the lookback window
            if fight_end.replace(tzinfo=timezone.utc) < cutoff:
                continue

            # Aggregate doctrine ship losses in this fight
            loss_r = await db.execute(
                select(Killmail.ship_type_id, Killmail.ship_name, func.count())
                .where(Killmail.deployment_fight_id == fight.id)
                .where(Killmail.is_loss == True)
                .where(func.coalesce(Killmail.ship_name, "").notlike("Capsule%"))
                .where(func.coalesce(Killmail.ship_name, "").notlike("Mobile %"))
                .group_by(Killmail.ship_type_id, Killmail.ship_name)
            )
            ship_losses = [{"type_id": r[0], "ship_name": r[1] or f"Type {r[0]}", "count": r[2]}
                           for r in loss_r.fetchall()]

        if not ship_losses:
            continue

        # Get stock at home + staging
        type_ids = [s["type_id"] for s in ship_losses]
        async with market_session() as mdb:
            from app.models.database import MarketOrder
            home_stock = {}
            staging_stock = {}
            for batch in [type_ids[i:i+500] for i in range(0, len(type_ids), 500)]:
                r = await mdb.execute(
                    select(MarketOrder.type_id, func.sum(MarketOrder.volume_remain))
                    .where(MarketOrder.type_id.in_(batch))
                    .where(MarketOrder.is_buy_order == False)
                    .where(MarketOrder.location_id == settings.keepstar_structure_id)
                    .group_by(MarketOrder.type_id)
                )
                for row in r.fetchall():
                    home_stock[row[0]] = int(row[1] or 0)

                if staging_id:
                    r2 = await mdb.execute(
                        select(MarketOrder.type_id, func.sum(MarketOrder.volume_remain))
                        .where(MarketOrder.type_id.in_(batch))
                        .where(MarketOrder.is_buy_order == False)
                        .where(MarketOrder.location_id == staging_id)
                        .group_by(MarketOrder.type_id)
                    )
                    for row in r2.fetchall():
                        staging_stock[row[0]] = int(row[1] or 0)

        # Build alert entries
        entries = []
        for s in sorted(ship_losses, key=lambda x: -x["count"]):
            tid = s["type_id"]
            h_stock = home_stock.get(tid, 0)
            s_stock = staging_stock.get(tid, 0)
            entries.append({
                "type_id": tid,
                "ship_name": s["ship_name"],
                "lost": s["count"],
                "home_stock": h_stock,
                "staging_stock": s_stock,
                "total_stock": h_stock + s_stock,
                "suggest_contracts": max(0, s["count"] - s_stock),
            })

        total_lost = sum(e["lost"] for e in entries)
        total_suggest = sum(e["suggest_contracts"] for e in entries)

        alerts.append({
            "fight_id": fight.id,
            "fight_name": fight.name,
            "fight_end": fight_end.isoformat() if fight_end else None,
            "hours_ago": round((datetime.now(timezone.utc) - fight_end.replace(tzinfo=timezone.utc)).total_seconds() / 3600, 1),
            "total_lost": total_lost,
            "total_suggest": total_suggest,
            "ships": entries[:20],
        })

    return alerts


async def get_import_opportunities(deployment_id: int | None = None) -> dict:
    """
    Feature 3: Module Burn → Import List.
    Aggregate destroyed modules, calculate margin (CJ sell - Jita - freight),
    filter to positive margin, sort by ISK opportunity.
    """
    from app.models.session import async_session as market_session

    # Get deployment date range
    if deployment_id:
        async with killmail_session() as db:
            from app.models.killmail_models import Deployment
            dep = await db.get(Deployment, deployment_id)
            cutoff = dep.started_at if dep and dep.started_at else (datetime.now(timezone.utc) - timedelta(days=30))
            staging_id = dep.staging_structure_id if dep else None
    else:
        cutoff = datetime.now(timezone.utc) - timedelta(days=30)
        staging_id = None

    # Aggregate all destroyed items (modules, ammo, drones — NOT hulls)
    item_counts = defaultdict(int)
    async with killmail_session() as db:
        query = (
            select(Killmail)
            .where(Killmail.is_loss == True)
            .where(Killmail.killed_at >= cutoff)
        )
        if deployment_id:
            query = query.where(Killmail.deployment_id == deployment_id)
        result = await db.execute(query)
        total_kills = 0
        for km in result.scalars().all():
            total_kills += 1
            if km.fit_items:
                for item in km.fit_items:
                    tid = item.get("type_id")
                    qty = (item.get("qty_destroyed", 0) or 0) + (item.get("qty_dropped", 0) or 0)
                    if tid and qty > 0:
                        item_counts[tid] += qty

    if not item_counts:
        return {"items": [], "total_kills": 0}

    deployment_days = max((datetime.now(timezone.utc).replace(tzinfo=None) - cutoff.replace(tzinfo=None) if cutoff.tzinfo else datetime.utcnow() - cutoff).days, 1)

    # Cross-reference with market data
    type_ids_list = list(item_counts.keys())
    async with market_session() as db:
        from app.models.database import SdeType, SdeGroup, JitaPrice, MarketOrder

        # Get SDE info
        type_info = {}
        for batch in [type_ids_list[i:i+500] for i in range(0, len(type_ids_list), 500)]:
            r = await db.execute(
                select(SdeType.type_id, SdeType.name, SdeType.group_id, SdeType.category_id,
                       SdeType.packaged_volume, SdeType.volume)
                .where(SdeType.type_id.in_(batch))
            )
            for row in r.fetchall():
                type_info[row[0]] = {"name": row[1], "group_id": row[2], "category_id": row[3],
                                     "volume": row[4] or row[5] or 0}

        # Get group names
        group_ids = list(set(v["group_id"] for v in type_info.values() if v.get("group_id")))
        group_names = {}
        if group_ids:
            for batch in [group_ids[i:i+500] for i in range(0, len(group_ids), 500)]:
                try:
                    r = await db.execute(select(SdeGroup.group_id, SdeGroup.name).where(SdeGroup.group_id.in_(batch)))
                    for row in r.fetchall():
                        group_names[row[0]] = row[1]
                except Exception:
                    pass

        # Jita prices
        jita_prices = {}
        for batch in [type_ids_list[i:i+500] for i in range(0, len(type_ids_list), 500)]:
            r = await db.execute(select(JitaPrice.type_id, JitaPrice.sell_min).where(JitaPrice.type_id.in_(batch)))
            for row in r.fetchall():
                jita_prices[row[0]] = float(row[1] or 0)

        # CJ sell prices (min sell)
        cj_prices = {}
        for batch in [type_ids_list[i:i+500] for i in range(0, len(type_ids_list), 500)]:
            r = await db.execute(
                select(MarketOrder.type_id, func.min(MarketOrder.price))
                .where(MarketOrder.type_id.in_(batch))
                .where(MarketOrder.is_buy_order == False)
                .where(MarketOrder.location_id == settings.keepstar_structure_id)
                .group_by(MarketOrder.type_id)
            )
            for row in r.fetchall():
                cj_prices[row[0]] = float(row[1] or 0)

        # CJ stock
        cj_stock = {}
        for batch in [type_ids_list[i:i+500] for i in range(0, len(type_ids_list), 500)]:
            r = await db.execute(
                select(MarketOrder.type_id, func.sum(MarketOrder.volume_remain))
                .where(MarketOrder.type_id.in_(batch))
                .where(MarketOrder.is_buy_order == False)
                .where(MarketOrder.location_id == settings.keepstar_structure_id)
                .group_by(MarketOrder.type_id)
            )
            for row in r.fetchall():
                cj_stock[row[0]] = int(row[1] or 0)

    # Build opportunities
    items = []
    for tid, total_qty in item_counts.items():
        info = type_info.get(tid, {})
        if not info:
            continue
        # Skip ships (category 6) — hulls don't have import margin
        if info.get("category_id") == 6:
            continue

        jita = jita_prices.get(tid, 0)
        if jita <= 0:
            continue
        cj_sell = cj_prices.get(tid, 0)
        vol_m3 = info.get("volume", 0)
        import_cost = jita + (vol_m3 * settings.freight_cost_per_m3)
        margin = cj_sell - import_cost if cj_sell > 0 else 0
        margin_pct = (margin / import_cost * 100) if import_cost > 0 else 0

        if margin <= 0:
            continue  # Only positive margin items

        weekly_qty = total_qty / deployment_days * 7
        weekly_isk = margin * weekly_qty
        stock = cj_stock.get(tid, 0)
        daily_rate = total_qty / deployment_days

        items.append({
            "type_id": tid,
            "name": info.get("name", f"Type {tid}"),
            "group_name": group_names.get(info.get("group_id"), ""),
            "total_destroyed": total_qty,
            "weekly_qty": round(weekly_qty, 1),
            "daily_rate": round(daily_rate, 1),
            "jita_price": round(jita, 2),
            "import_cost": round(import_cost, 2),
            "cj_sell": round(cj_sell, 2),
            "margin": round(margin, 2),
            "margin_pct": round(margin_pct, 1),
            "weekly_isk": round(weekly_isk, 2),
            "stock": stock,
            "days_of_stock": round(stock / daily_rate, 1) if daily_rate > 0 else 999,
        })

    items.sort(key=lambda x: -x["weekly_isk"])

    return {
        "items": items[:100],
        "total_kills": total_kills,
        "deployment_days": deployment_days,
        "total_weekly_isk": round(sum(i["weekly_isk"] for i in items), 2),
    }


async def get_stock_outs(deployment_id: int | None = None, hours: int = 168) -> list:
    """
    Feature 5: Stock-Out Detection.
    Find items where sell volume dropped to 0 in recent snapshots.
    Cross-references with fight times to identify fight-driven stock-outs.
    """
    from app.models.session import async_session as market_session
    from app.models.database import MarketSnapshot, MarketOrder, SdeType

    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    async with market_session() as db:
        # Find type_ids that had a snapshot with 0 sell volume recently
        # but had >0 sell volume in a prior snapshot
        stockout_r = await db.execute(
            select(
                MarketSnapshot.type_id,
                func.min(MarketSnapshot.timestamp).label("first_zero"),
            )
            .where(MarketSnapshot.timestamp >= cutoff)
            .where(MarketSnapshot.sell_volume == 0)
            .group_by(MarketSnapshot.type_id)
        )
        zero_events = stockout_r.fetchall()

        if not zero_events:
            return []

        results = []
        for row in zero_events:
            tid = row[0]
            first_zero = row[1]

            # Check if there was stock before this
            prior_r = await db.execute(
                select(MarketSnapshot.sell_volume, MarketSnapshot.timestamp)
                .where(MarketSnapshot.type_id == tid)
                .where(MarketSnapshot.timestamp < first_zero)
                .order_by(MarketSnapshot.timestamp.desc())
                .limit(1)
            )
            prior = prior_r.first()
            if not prior or (prior[0] or 0) == 0:
                continue  # Was already 0 before — not a new stock-out

            # Current stock
            cur_r = await db.execute(
                select(func.sum(MarketOrder.volume_remain))
                .where(MarketOrder.type_id == tid)
                .where(MarketOrder.is_buy_order == False)
                .where(MarketOrder.location_id == settings.keepstar_structure_id)
            )
            current_stock = cur_r.scalar() or 0

            # Check if restocked (latest snapshot has stock)
            latest_r = await db.execute(
                select(MarketSnapshot.sell_volume, MarketSnapshot.timestamp)
                .where(MarketSnapshot.type_id == tid)
                .order_by(MarketSnapshot.timestamp.desc())
                .limit(1)
            )
            latest = latest_r.first()
            restocked = latest and (latest[0] or 0) > 0

            # Time to restock (Feature 6: Competitor Restock Speed)
            restock_time_hours = None
            if restocked and first_zero:
                # Find first snapshot after zero that has stock again
                restock_r = await db.execute(
                    select(func.min(MarketSnapshot.timestamp))
                    .where(MarketSnapshot.type_id == tid)
                    .where(MarketSnapshot.timestamp > first_zero)
                    .where(MarketSnapshot.sell_volume > 0)
                )
                restock_time = restock_r.scalar()
                if restock_time:
                    delta = (restock_time - first_zero).total_seconds() / 3600
                    restock_time_hours = round(delta, 1)

            # Get item name
            sde = await db.get(SdeType, tid)
            name = sde.name if sde else f"Type {tid}"

            results.append({
                "type_id": tid,
                "name": name,
                "prior_stock": prior[0] if prior else 0,
                "went_zero_at": first_zero.isoformat() if first_zero else None,
                "current_stock": current_stock,
                "restocked": restocked,
                "restock_time_hours": restock_time_hours,
            })

        results.sort(key=lambda x: x["went_zero_at"] or "", reverse=True)
        return results[:50]


async def get_war_reserve(deployment_id: int | None = None) -> dict:
    """
    Feature 7: Predictive Stocking / War Reserve.
    Calculate minimum stock levels based on fight frequency × doctrine burn per fight.
    War reserve = 2× weekly burn. Show surplus/deficit per item.
    """
    from app.models.session import async_session as market_session
    from app.models.killmail_models import Deployment, DeploymentFight

    if not deployment_id:
        return {"items": [], "summary": {}}

    # Get deployment info
    async with killmail_session() as db:
        dep = await db.get(Deployment, deployment_id)
        if not dep:
            return {"items": [], "summary": {}}
        staging_id = dep.staging_structure_id
        cutoff = dep.started_at or (datetime.now(timezone.utc) - timedelta(days=30))

        # Count fights
        fights_r = await db.execute(
            select(func.count(DeploymentFight.id))
            .where(DeploymentFight.deployment_id == deployment_id)
        )
        fight_count = fights_r.scalar() or 0

    deployment_days = max((datetime.now(timezone.utc).replace(tzinfo=None) - cutoff.replace(tzinfo=None) if cutoff.tzinfo else datetime.utcnow() - cutoff).days, 1)
    fights_per_week = fight_count / deployment_days * 7 if deployment_days > 0 else 0

    # Get burn rate data (reuse existing function)
    burn = await get_burn_rate(deployment_id=deployment_id)

    # Build war reserve items from top burned items
    reserve_items = []
    for cat in burn.get("categories", []):
        for item in cat.get("items", []):
            weekly_burn = item["daily_rate"] * 7
            war_reserve = weekly_burn * 2  # 2 weeks buffer
            total_stock = item.get("total_stock", item["stock"])
            surplus = total_stock - war_reserve

            if war_reserve < 1:
                continue  # Skip negligible items

            status = "ok" if surplus >= 0 else ("critical" if surplus < -war_reserve * 0.5 else "low")

            reserve_items.append({
                "type_id": item["type_id"],
                "name": item["name"],
                "group_name": item.get("group_name", ""),
                "category": cat["category"],
                "daily_rate": item["daily_rate"],
                "weekly_burn": round(weekly_burn, 1),
                "war_reserve": round(war_reserve),
                "home_stock": item["stock"],
                "staging_stock": item.get("staging_stock", 0),
                "total_stock": total_stock,
                "surplus": round(surplus),
                "status": status,
                "jita_price": item.get("jita_price", 0),
                "deficit_isk": round(abs(surplus) * item.get("jita_price", 0)) if surplus < 0 else 0,
            })

    # Sort: critical first, then by deficit ISK
    reserve_items.sort(key=lambda x: (0 if x["status"] == "critical" else 1 if x["status"] == "low" else 2, -x["deficit_isk"]))

    critical = [i for i in reserve_items if i["status"] == "critical"]
    low = [i for i in reserve_items if i["status"] == "low"]
    total_deficit_isk = sum(i["deficit_isk"] for i in reserve_items)

    return {
        "items": reserve_items[:100],
        "summary": {
            "fight_count": fight_count,
            "fights_per_week": round(fights_per_week, 1),
            "deployment_days": deployment_days,
            "critical_count": len(critical),
            "low_count": len(low),
            "total_deficit_isk": total_deficit_isk,
        },
    }


async def get_fight_patterns(deployment_id: int | None = None) -> dict:
    """
    Feature 8: Granular Import Calendar.
    Analyze fight time patterns at day/hour granularity.
    Returns heatmap data + suggested import timing.
    """
    from app.models.killmail_models import Deployment, DeploymentFight

    if not deployment_id:
        return {"heatmap": [], "peak_windows": [], "suggestion": ""}

    async with killmail_session() as db:
        dep = await db.get(Deployment, deployment_id)
        if not dep:
            return {"heatmap": [], "peak_windows": [], "suggestion": ""}

        # Get fight start times from killmails (first kill per fight)
        fights_r = await db.execute(
            select(DeploymentFight.id)
            .where(DeploymentFight.deployment_id == deployment_id)
        )
        fight_ids = [r[0] for r in fights_r.fetchall()]

    if not fight_ids:
        return {"heatmap": [], "peak_windows": [], "suggestion": "No fights recorded yet."}

    # Get first kill time per fight
    fight_times = []
    async with killmail_session() as db:
        for fid in fight_ids:
            r = await db.execute(
                select(func.min(Killmail.killed_at))
                .where(Killmail.deployment_fight_id == fid)
            )
            t = r.scalar()
            if t:
                fight_times.append(t)

    if not fight_times:
        return {"heatmap": [], "peak_windows": [], "suggestion": "No fight killmails found."}

    # Build day-of-week × hour heatmap
    DAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    heatmap = [[0]*24 for _ in range(7)]
    for t in fight_times:
        dow = t.weekday()  # 0=Mon, 6=Sun
        hour = t.hour
        heatmap[dow][hour] += 1

    # Find peak windows (cells with most fights)
    cells = []
    for d in range(7):
        for h in range(24):
            if heatmap[d][h] > 0:
                cells.append({"day": d, "day_name": DAY_NAMES[d], "hour": h, "count": heatmap[d][h]})
    cells.sort(key=lambda x: -x["count"])

    # Find peak cluster: most common 4-hour window
    best_window = None
    best_count = 0
    for d in range(7):
        for start_h in range(24):
            window_count = sum(heatmap[d][(start_h + i) % 24] for i in range(4))
            if window_count > best_count:
                best_count = window_count
                best_window = {"day": d, "day_name": DAY_NAMES[d], "start_hour": start_h,
                               "end_hour": (start_h + 4) % 24, "count": window_count}

    # Generate suggestion
    suggestion = ""
    if best_window and best_count >= 2:
        import_day = (best_window["day"] - 1) % 7
        suggestion = (f"Fights cluster on {best_window['day_name']} "
                      f"{best_window['start_hour']:02d}:00–{best_window['end_hour']:02d}:00 UTC "
                      f"({best_count} fights). Import by {DAY_NAMES[import_day]} noon UTC.")
    elif fight_times:
        suggestion = "Not enough fights yet to identify a clear pattern."

    # Flatten heatmap for frontend
    heatmap_flat = []
    for d in range(7):
        for h in range(24):
            heatmap_flat.append({"day": d, "day_name": DAY_NAMES[d], "hour": h, "count": heatmap[d][h]})

    return {
        "heatmap": heatmap_flat,
        "peak_windows": cells[:10],
        "suggestion": suggestion,
        "total_fights": len(fight_times),
        "best_window": best_window,
    }


async def get_contract_timing(deployment_id: int | None = None) -> dict:
    """
    Feature 4: Contract Timing & Pricing.
    Correlate contract sell velocity after fights vs quiet periods.
    Detect premium pricing windows (4-hour post-fight reship).
    """
    from app.models.session import async_session as market_session
    from app.models.killmail_models import Deployment, DeploymentFight

    if not deployment_id:
        return {"post_fight": [], "baseline": {}, "summary": {}}

    # Step 1: Get fight end times
    fight_ends = []
    async with killmail_session() as db:
        dep = await db.get(Deployment, deployment_id)
        if not dep:
            return {"post_fight": [], "baseline": {}, "summary": {}}

        fights_r = await db.execute(
            select(DeploymentFight.id)
            .where(DeploymentFight.deployment_id == deployment_id)
        )
        fight_ids = [r[0] for r in fights_r.fetchall()]

        for fid in fight_ids:
            r = await db.execute(
                select(func.max(Killmail.killed_at))
                .where(Killmail.deployment_fight_id == fid)
            )
            end_time = r.scalar()
            if end_time:
                fight_ends.append(end_time)

    if not fight_ends:
        return {"post_fight": [], "baseline": {}, "summary": {"message": "No fights found"}}

    # Step 2: Get all completed contracts
    async with market_session() as db:
        from app.models.database import CharacterContract, CharacterContractItem, SdeType

        contracts_r = await db.execute(
            select(CharacterContract)
            .where(CharacterContract.status == "finished")
            .where(CharacterContract.date_completed.isnot(None))
            .where(CharacterContract.contract_type == "item_exchange")
        )
        contracts = contracts_r.scalars().all()

        if not contracts:
            return {"post_fight": [], "baseline": {}, "summary": {"message": "No completed contracts"}}

        # Step 3: Classify each contract as post-fight (within 4h) or baseline
        post_fight_contracts = []
        baseline_contracts = []
        POST_FIGHT_WINDOW_H = 4

        for contract in contracts:
            if not contract.date_completed:
                continue
            completed = contract.date_completed
            is_post_fight = False
            hours_after = None

            for fight_end in fight_ends:
                delta_h = (completed - fight_end).total_seconds() / 3600
                if 0 <= delta_h <= POST_FIGHT_WINDOW_H:
                    is_post_fight = True
                    hours_after = round(delta_h, 1)
                    break

            entry = {
                "contract_id": contract.contract_id,
                "title": contract.title,
                "price": contract.price,
                "date_completed": completed.isoformat() if completed else None,
                "date_issued": contract.date_issued.isoformat() if contract.date_issued else None,
            }

            # Time to sell
            if contract.date_issued and contract.date_completed:
                sell_time_h = (contract.date_completed - contract.date_issued).total_seconds() / 3600
                entry["sell_time_hours"] = round(sell_time_h, 1)
            else:
                entry["sell_time_hours"] = None

            if is_post_fight:
                entry["hours_after_fight"] = hours_after
                post_fight_contracts.append(entry)
            else:
                baseline_contracts.append(entry)

        # Step 4: Calculate averages
        pf_sell_times = [c["sell_time_hours"] for c in post_fight_contracts if c["sell_time_hours"] is not None]
        bl_sell_times = [c["sell_time_hours"] for c in baseline_contracts if c["sell_time_hours"] is not None]
        pf_prices = [c["price"] for c in post_fight_contracts if c["price"]]
        bl_prices = [c["price"] for c in baseline_contracts if c["price"]]

        avg_pf_sell_time = round(sum(pf_sell_times) / len(pf_sell_times), 1) if pf_sell_times else None
        avg_bl_sell_time = round(sum(bl_sell_times) / len(bl_sell_times), 1) if bl_sell_times else None
        avg_pf_price = round(sum(pf_prices) / len(pf_prices), 0) if pf_prices else None
        avg_bl_price = round(sum(bl_prices) / len(bl_prices), 0) if bl_prices else None

        demand_multiplier = None
        if avg_pf_sell_time and avg_bl_sell_time and avg_bl_sell_time > 0:
            demand_multiplier = round(avg_bl_sell_time / avg_pf_sell_time, 1)

        return {
            "post_fight": post_fight_contracts[:20],
            "baseline": {
                "count": len(baseline_contracts),
                "avg_sell_time_hours": avg_bl_sell_time,
                "avg_price": avg_bl_price,
            },
            "summary": {
                "total_fights": len(fight_ends),
                "post_fight_contracts": len(post_fight_contracts),
                "baseline_contracts": len(baseline_contracts),
                "avg_pf_sell_time": avg_pf_sell_time,
                "avg_bl_sell_time": avg_bl_sell_time,
                "avg_pf_price": avg_pf_price,
                "avg_bl_price": avg_bl_price,
                "demand_multiplier": demand_multiplier,
                "pricing_suggestion": f"Post-fight contracts sell {demand_multiplier}× faster. Consider +10-15% premium in the 4h window after fights." if demand_multiplier and demand_multiplier > 1.3 else "Not enough data to detect a clear premium window yet.",
            },
        }


async def get_deployment_stats(deployment_id: int) -> dict:
    """
    Generate alliance-shareable deployment statistics.
    Summary of total losses, ISK burned, top items, fight count, daily averages.
    """
    burn = await get_burn_rate(deployment_id=deployment_id)

    # Get fight summary
    from app.models.killmail_models import Deployment, DeploymentFight
    async with killmail_session() as db:
        dep = await db.get(Deployment, deployment_id)
        if not dep:
            return {"error": "Deployment not found"}

        fights_r = await db.execute(
            select(func.count(DeploymentFight.id)).where(DeploymentFight.deployment_id == deployment_id)
        )
        fight_count = fights_r.scalar() or 0

    # Build stats
    summary = burn.get("summary", {})
    categories = burn.get("categories", [])

    # Top losses by category
    top_ships = []
    top_modules = []
    top_ammo = []
    top_drones = []
    for cat in categories:
        items = cat.get("items", [])[:10]
        if cat["category"] == "Ships":
            top_ships = items
        elif cat["category"] == "Modules":
            top_modules = items
        elif cat["category"] == "Charges & Ammo":
            top_ammo = items
        elif cat["category"] == "Drones":
            top_drones = items

    return {
        "deployment": {
            "name": dep.name,
            "status": dep.status,
            "regions": dep.watched_region_names or [],
            "started_at": dep.started_at.isoformat() if dep.started_at else None,
            "days_active": summary.get("deployment_days", 0),
        },
        "overview": {
            "total_kills": summary.get("total_kills", 0),
            "total_isk_burned": summary.get("total_isk_burned", 0),
            "avg_daily_isk": summary.get("avg_daily_isk", 0),
            "fight_count": fight_count,
        },
        "top_ships": top_ships,
        "top_modules": top_modules,
        "top_ammo": top_ammo,
        "top_drones": top_drones,
        "categories": [{
            "category": c["category"],
            "total_isk": c["total_isk"],
            "item_count": c["item_count"],
        } for c in categories],
    }


# ─── Retroactive Fight Detection ─────────────────────

async def detect_fights_retroactive(deployment_id: int | None = None) -> dict:
    """
    Run fight detection on ALL deployment killmails.
    WIPES existing auto-detected fights first and re-detects with current settings.
    Creates DeploymentFight entries grouped by system + time gap.
    """
    from app.models.killmail_models import Deployment, DeploymentFight, DeploymentFightSystem

    stats = {"fights_created": 0, "killmails_assigned": 0, "old_fights_removed": 0}

    if not deployment_id:
        return {"error": "deployment_id required"}

    async with killmail_write_lock:
        async with killmail_session() as db:
            dep = await db.get(Deployment, deployment_id)
            if not dep:
                return {"error": "Deployment not found"}

            # Step 1: Delete only AUTO-DETECTED fights (preserve manually created ones)
            old_fights = await db.execute(
                select(DeploymentFight)
                .where(DeploymentFight.deployment_id == deployment_id)
                .where(DeploymentFight.auto_detected == True)
            )
            for old_fight in old_fights.scalars().all():
                # Clear killmail references
                await db.execute(
                    update(Killmail)
                    .where(Killmail.deployment_fight_id == old_fight.id)
                    .values(deployment_fight_id=None)
                )
                # Delete system rows
                await db.execute(
                    delete(DeploymentFightSystem)
                    .where(DeploymentFightSystem.deployment_fight_id == old_fight.id)
                )
                await db.delete(old_fight)
                stats["old_fights_removed"] += 1

            await db.flush()
            logger.info(f"Detect fights: cleared {stats['old_fights_removed']} old fights for deployment {deployment_id}")

            # Step 2: Get deployment killmails NOT in a manual fight
            # Get manual fight IDs to exclude their killmails
            manual_fights = await db.execute(
                select(DeploymentFight.id)
                .where(DeploymentFight.deployment_id == deployment_id)
                .where((DeploymentFight.auto_detected == False) | (DeploymentFight.auto_detected.is_(None)))
            )
            manual_fight_ids = {r[0] for r in manual_fights.fetchall()}

            query = (
                select(Killmail)
                .where(Killmail.deployment_id == deployment_id)
                .where(Killmail.ship_name.isnot(None))
                .where(~Killmail.ship_name.like("Capsule%"))
                .where(~Killmail.ship_name.like("Mobile %"))
            )
            # Exclude killmails already tagged to manual fights
            if manual_fight_ids:
                query = query.where(
                    (Killmail.deployment_fight_id.is_(None)) |
                    (~Killmail.deployment_fight_id.in_(manual_fight_ids))
                )
            query = query.order_by(Killmail.solar_system_id, Killmail.killed_at.asc())
            result = await db.execute(query)
            killmails = result.scalars().all()

            if not killmails:
                return stats

            logger.info(f"Retroactive fight detection: {len(killmails)} unassigned killmails in deployment {deployment_id}")

            # Group by system, cluster by time gap
            window = timedelta(minutes=FIGHT_WINDOW_MINUTES)
            clusters = []  # list of {"system_id", "system_name", "start", "end", "kills": [km...]}
            current = None

            for km in killmails:
                sys_id = km.solar_system_id
                if (current is None or
                    sys_id != current["system_id"] or
                    km.killed_at > current["end"] + window):
                    # Start new cluster
                    current = {
                        "system_id": sys_id,
                        "system_name": km.solar_system_name or str(sys_id),
                        "start": km.killed_at,
                        "end": km.killed_at,
                        "kills": [],
                    }
                    clusters.append(current)

                current["end"] = km.killed_at
                current["kills"].append(km)

            # Filter clusters: minimum total kills + coalition involvement required
            MONITOR_TYPE_ID = 45534
            watched = get_watched_alliance_ids()

            for cluster in clusters:
                if len(cluster["kills"]) < MIN_FIGHT_LOSSES:
                    continue

                # Coalition involvement check:
                # 1) Our FC (Monitor from watched alliance) in attacker data, OR
                # 2) 10+ coalition losses (organized fleet, FC may not be in Monitor)
                our_losses = sum(1 for km in cluster["kills"] if km.is_loss)
                has_our_fc = False
                for km in cluster["kills"]:
                    if not km.attacker_data:
                        continue
                    for a in km.attacker_data:
                        if (a.get("ship_type_id") == MONITOR_TYPE_ID and
                            a.get("alliance_id") in watched):
                            has_our_fc = True
                            break
                    if has_our_fc:
                        break

                if not has_our_fc and our_losses < 10:
                    continue

                # Create the named fight
                duration = cluster["end"] - cluster["start"]
                duration_str = f"{int(duration.total_seconds() // 3600)}h {int((duration.total_seconds() % 3600) // 60)}m"
                name = f"{cluster['system_name']} ({len(cluster['kills'])} kills, {duration_str})"

                fight = DeploymentFight(
                    deployment_id=deployment_id,
                    name=name,
                    auto_detected=True,
                )
                db.add(fight)
                await db.flush()

                # Add system+time window
                db.add(DeploymentFightSystem(
                    deployment_fight_id=fight.id,
                    system_id=cluster["system_id"],
                    system_name=cluster["system_name"],
                    start_time=cluster["start"],
                    end_time=cluster["end"],
                ))

                # Tag all killmails in this cluster
                for km in cluster["kills"]:
                    km.deployment_fight_id = fight.id
                    stats["killmails_assigned"] += 1

                stats["fights_created"] += 1

            await db.flush()

            # Step 3: FC-based fight expansion
            # For each fight, find Monitor pilots in attacker data,
            # then absorb untagged kills in other systems where those FCs appear
            MONITOR_TYPE_ID = 45534
            FC_BUFFER = timedelta(minutes=30)  # Look 30 min before/after fight window

            # Build list of created fights with their tagged killmail IDs
            created_fights = await db.execute(
                select(DeploymentFight)
                .where(DeploymentFight.deployment_id == deployment_id)
                .where(DeploymentFight.auto_detected == True)
            )
            fight_list = created_fights.scalars().all()

            # Get all untagged killmails (still available for expansion)
            untagged_result = await db.execute(
                select(Killmail)
                .where(Killmail.deployment_id == deployment_id)
                .where(Killmail.deployment_fight_id.is_(None))
            )
            untagged = untagged_result.scalars().all()

            if untagged and fight_list:
                stats["fc_expanded"] = 0

                for fight in fight_list:
                    # Get killmails tagged to this fight
                    fight_kms = await db.execute(
                        select(Killmail)
                        .where(Killmail.deployment_fight_id == fight.id)
                    )
                    fight_killmails = fight_kms.scalars().all()

                    # Find Monitor pilots in attacker data
                    fc_char_ids = set()
                    fight_start = None
                    fight_end = None
                    for km in fight_killmails:
                        if km.killed_at:
                            if fight_start is None or km.killed_at < fight_start:
                                fight_start = km.killed_at
                            if fight_end is None or km.killed_at > fight_end:
                                fight_end = km.killed_at
                        if not km.attacker_data:
                            continue
                        for a in km.attacker_data:
                            if a.get("ship_type_id") == MONITOR_TYPE_ID and a.get("character_id"):
                                fc_char_ids.add(a["character_id"])

                    if not fc_char_ids or not fight_start:
                        continue

                    logger.info(f"Fight '{fight.name}': found {len(fc_char_ids)} FC(s), scanning for expansion")

                    # Scan untagged kills for these FCs within the fight time window
                    expanded_systems = {}  # system_id -> {name, start, end}
                    absorbed = []
                    search_start = fight_start - FC_BUFFER
                    search_end = fight_end + FC_BUFFER

                    for km in untagged:
                        if not km.killed_at or km.killed_at < search_start or km.killed_at > search_end:
                            continue
                        if not km.attacker_data:
                            continue

                        # Check if any FC is on this kill
                        fc_present = False
                        for a in km.attacker_data:
                            if a.get("character_id") in fc_char_ids:
                                fc_present = True
                                break

                        if fc_present:
                            km.deployment_fight_id = fight.id
                            absorbed.append(km)

                            # Track new system
                            sid = km.solar_system_id
                            if sid not in expanded_systems:
                                expanded_systems[sid] = {
                                    "name": km.solar_system_name or str(sid),
                                    "start": km.killed_at,
                                    "end": km.killed_at,
                                }
                            else:
                                expanded_systems[sid]["start"] = min(expanded_systems[sid]["start"], km.killed_at)
                                expanded_systems[sid]["end"] = max(expanded_systems[sid]["end"], km.killed_at)

                    # Remove absorbed kills from untagged list
                    absorbed_ids = {km.killmail_id for km in absorbed}
                    untagged = [km for km in untagged if km.killmail_id not in absorbed_ids]

                    if absorbed:
                        # Add new system+time windows for expanded systems
                        # (only systems not already in the fight)
                        existing_sys = await db.execute(
                            select(DeploymentFightSystem.system_id)
                            .where(DeploymentFightSystem.deployment_fight_id == fight.id)
                        )
                        existing_sys_ids = {r[0] for r in existing_sys.fetchall()}

                        for sid, sdata in expanded_systems.items():
                            if sid not in existing_sys_ids:
                                db.add(DeploymentFightSystem(
                                    deployment_fight_id=fight.id,
                                    system_id=sid,
                                    system_name=sdata["name"],
                                    start_time=sdata["start"],
                                    end_time=sdata["end"],
                                ))

                        stats["fc_expanded"] += len(absorbed)
                        stats["killmails_assigned"] += len(absorbed)

                        # Update fight name with new totals
                        total_in_fight = await db.execute(
                            select(func.count(Killmail.killmail_id))
                            .where(Killmail.deployment_fight_id == fight.id)
                            .where(~Killmail.ship_name.like("Capsule%"))
                            .where(~Killmail.ship_name.like("Mobile %"))
                        )
                        new_count = total_in_fight.scalar() or 0

                        # Get primary system (most kills)
                        primary = await db.execute(
                            select(Killmail.solar_system_name, func.count().label("cnt"))
                            .where(Killmail.deployment_fight_id == fight.id)
                            .group_by(Killmail.solar_system_name)
                            .order_by(func.count().desc())
                            .limit(1)
                        )
                        primary_sys = primary.first()
                        primary_name = primary_sys[0] if primary_sys else fight.name.split(" (")[0]

                        # Recalculate duration
                        time_r = await db.execute(
                            select(func.min(Killmail.killed_at), func.max(Killmail.killed_at))
                            .where(Killmail.deployment_fight_id == fight.id)
                        )
                        times = time_r.first()
                        if times[0] and times[1]:
                            dur = (times[1] - times[0]).total_seconds()
                            dur_str = f"{int(dur // 3600)}h {int((dur % 3600) // 60)}m"
                        else:
                            dur_str = "0m"

                        all_systems = list(existing_sys_ids | set(expanded_systems.keys()))
                        sys_count = len(all_systems)
                        sys_suffix = f" +{sys_count - 1} systems" if sys_count > 1 else ""
                        fight.name = f"{primary_name}{sys_suffix} ({new_count} kills, {dur_str})"

                        logger.info(f"FC expansion: absorbed {len(absorbed)} kills into '{fight.name}' via {len(fc_char_ids)} FC(s)")

                await db.flush()

            await db.commit()

    logger.info(f"Retroactive fight detection complete: {stats}")
    return stats


# ─── zKill API Backfill ──────────────────────────────

async def backfill_from_zkill(start_time: datetime, end_time: datetime,
                              deployment_id: int | None = None,
                              region_ids: list[int] | None = None) -> dict:
    """
    Backfill historical alliance losses from zKillboard API.

    Uses two zKill API strategies (matching EIFT's proven approach):
    - Recent (≤7 days): /losses/allianceID/{id}/pastSeconds/{s}/page/{n}/
    - Historical: /losses/allianceID/{id}/year/{y}/month/{m}/page/{n}/
    Then filters client-side by exact date range and fetches full data from ESI.
    """
    import httpx
    from app.models.killmail_models import Deployment

    stats = {"fetched": 0, "stored": 0, "skipped": 0, "errors": 0}

    # Resolve region filter from deployment if given
    filter_regions = None
    if deployment_id:
        async with killmail_session() as db:
            dep = await db.get(Deployment, deployment_id)
            if dep:
                filter_regions = set(dep.watched_region_ids or [])
    elif region_ids:
        filter_regions = set(region_ids)

    watched = get_watched_alliance_ids()
    if not watched:
        return {"error": "No alliance_id configured"}
    primary_alliance = settings.alliance_id or next(iter(watched))

    # Normalize all datetimes to naive UTC for consistent comparisons
    if start_time.tzinfo:
        start_time = start_time.replace(tzinfo=None)
    if end_time.tzinfo:
        end_time = end_time.replace(tzinfo=None)

    headers = {
        "User-Agent": "VoidMarket/1.0 (EVE market tool)",
        "Accept-Encoding": "gzip",
    }

    # Build zKill URL paths to fetch
    # If within 7 days, use pastSeconds. Otherwise use year/month.
    now = datetime.utcnow()
    seconds_ago = int((now - start_time).total_seconds())

    url_paths = []
    if seconds_ago <= 604800:
        # Recent — single pastSeconds call
        url_paths.append(f"losses/allianceID/{primary_alliance}/pastSeconds/{seconds_ago}/")
    else:
        # Historical — build year/month combinations covering the range
        months_seen = set()
        dt = start_time
        while dt <= end_time:
            key = (dt.year, dt.month)
            if key not in months_seen:
                months_seen.add(key)
                url_paths.append(f"losses/allianceID/{primary_alliance}/year/{dt.year}/month/{dt.month}/")
            # Advance to next month
            if dt.month == 12:
                dt = dt.replace(year=dt.year + 1, month=1, day=1)
            else:
                dt = dt.replace(month=dt.month + 1, day=1)

    logger.info(f"Backfill: {len(url_paths)} URL paths for {start_time} to {end_time}")

    async with httpx.AsyncClient(timeout=30.0, headers=headers) as http:
        for url_path in url_paths:
            page = 1
            while page <= 50:  # Safety cap
                url = f"https://zkillboard.com/api/{url_path}page/{page}/"
                try:
                    logger.info(f"Backfill: fetching {url_path}page/{page}/")
                    resp = await http.get(url)
                    if resp.status_code != 200:
                        logger.warning(f"zKill API: status {resp.status_code} for {url}")
                        break
                    entries = resp.json()
                    if not isinstance(entries, list) or not entries:
                        break
                except Exception as e:
                    logger.error(f"zKill API error: {e}")
                    stats["errors"] += 1
                    break

                stats["fetched"] += len(entries)
                page_new = 0

                for entry in entries:
                    kill_id = entry.get("killmail_id")
                    zkb = entry.get("zkb", {})
                    kill_hash = zkb.get("hash")
                    if not kill_id or not kill_hash:
                        continue

                    # Check if already stored
                    async with killmail_session() as db:
                        existing = await db.get(Killmail, kill_id)
                        if existing:
                            stats["skipped"] += 1
                            continue

                    # Fetch full killmail from ESI (low priority — 1s delay)
                    await asyncio.sleep(1.0)
                    try:
                        esi_resp = await http.get(
                            f"https://esi.evetech.net/latest/killmails/{kill_id}/{kill_hash}/",
                            params={"datasource": "tranquility"},
                        )
                        if esi_resp.status_code != 200:
                            stats["errors"] += 1
                            continue
                        esi_data = esi_resp.json()
                    except Exception as e:
                        logger.warning(f"ESI fetch failed for {kill_id}: {e}")
                        stats["errors"] += 1
                        continue

                    # Parse killmail time and filter by date range
                    victim = esi_data.get("victim", {})
                    kill_time_str = esi_data.get("killmail_time", "")
                    try:
                        killed_at = datetime.fromisoformat(kill_time_str.replace("Z", "+00:00"))
                        if killed_at.tzinfo:
                            killed_at = killed_at.replace(tzinfo=None)
                    except Exception:
                        stats["errors"] += 1
                        continue

                    if killed_at < start_time or killed_at > end_time:
                        stats["skipped"] += 1
                        continue

                    # Build km_data for store pipeline
                    ship_type_id = victim.get("ship_type_id")
                    if not ship_type_id:
                        continue

                    victim_alliance = victim.get("alliance_id")
                    is_loss = victim_alliance in watched

                    fit_items = []
                    for item in victim.get("items", []):
                        tid = item.get("item_type_id")
                        if tid:
                            fit_items.append({
                                "type_id": tid,
                                "flag": item.get("flag", 0),
                                "qty_destroyed": item.get("quantity_destroyed", 0),
                                "qty_dropped": item.get("quantity_dropped", 0),
                            })

                    # Parse attacker data
                    attackers_raw = esi_data.get("attackers", [])
                    attacker_data = [{
                        "alliance_id": a.get("alliance_id"),
                        "corp_id": a.get("corporation_id"),
                        "character_id": a.get("character_id"),
                        "ship_type_id": a.get("ship_type_id"),
                        "weapon_type_id": a.get("weapon_type_id"),
                        "damage_done": a.get("damage_done", 0),
                        "final_blow": a.get("final_blow", False),
                    } for a in attackers_raw]

                    km_data = {
                        "killmail_id": kill_id,
                        "killmail_hash": kill_hash,
                        "ship_type_id": ship_type_id,
                        "victim_id": victim.get("character_id"),
                        "victim_corp_id": victim.get("corporation_id"),
                        "victim_alliance_id": victim_alliance,
                        "solar_system_id": esi_data.get("solar_system_id"),
                        "killed_at": killed_at,
                        "total_value": zkb.get("totalValue", 0),
                        "fit_items": fit_items,
                        "attacker_data": attacker_data,
                        "attacker_count": len(attackers_raw),
                        "is_loss": is_loss,
                    }

                    # Store through pipeline (region resolution + deployment tagging)
                    async with killmail_write_lock:
                        async with killmail_session() as db:
                            stored = await zkill_listener._store_killmail(db, km_data)
                            if stored:
                                stats["stored"] += 1
                                page_new += 1
                            await db.commit()

                logger.info(f"Backfill: page {page} done — {page_new} new, {len(entries)} on page")

                if len(entries) == 0:
                    break  # No more pages
                page += 1
                await asyncio.sleep(2)  # Be nice to zKill between pages

    logger.info(f"Backfill complete: {stats}")
    return stats


async def bulk_import_everef(date_str: str, deployment_id: int | None = None) -> dict:
    """
    Bulk import killmails from EVE Ref daily archives.
    Downloads a day's tar.bz2, extracts, filters for our alliance losses, stores.

    date_str: YYYY-MM-DD format
    """
    import httpx
    import tarfile
    import io
    import json as _json
    from app.models.killmail_models import Deployment

    stats = {"date": date_str, "downloaded": False, "total_in_archive": 0,
             "region_matches": 0, "stored": 0, "skipped": 0, "errors": 0}

    global _bulk_import_status
    _bulk_import_status["current_date"] = date_str

    watched = get_watched_alliance_ids()

    # Load region cache if needed
    await _load_region_cache()

    # Resolve watched regions from deployment
    watched_regions = set()
    if deployment_id:
        async with killmail_session() as db:
            dep = await db.get(Deployment, deployment_id)
            if dep:
                watched_regions = set(dep.watched_region_ids or [])
    else:
        # Use all active deployment regions
        await _load_deployment_regions()
        for regions in _deployment_watched_regions.values():
            watched_regions |= regions

    if not watched_regions:
        return {"error": "No watched regions — create a deployment first"}

    # URL pattern: https://data.everef.net/killmails/YYYY/killmails-YYYY-MM-DD.tar.bz2
    year = date_str[:4]
    url = f"https://data.everef.net/killmails/{year}/killmails-{date_str}.tar.bz2"

    headers = {"User-Agent": "VoidMarket/1.0 (EVE market tool)"}
    archive_data = None

    async with httpx.AsyncClient(timeout=120.0, headers=headers, follow_redirects=True) as http:
        try:
            logger.info(f"EVE Ref bulk: downloading {url}")
            resp = await http.get(url)
            if resp.status_code == 200:
                archive_data = resp.content
                stats["downloaded"] = True
                logger.info(f"EVE Ref bulk: downloaded {len(archive_data)} bytes")
            else:
                return {"error": f"HTTP {resp.status_code} for {url}", **stats}
        except Exception as e:
            return {"error": f"Download failed: {e}", **stats}

    if not archive_data:
        return {"error": f"Could not download killmail archive for {date_str}", **stats}

    # Extract and process
    try:
        tar = tarfile.open(fileobj=io.BytesIO(archive_data), mode="r:bz2")
    except Exception as e:
        return {"error": f"Failed to open archive: {e}", **stats}

    for member in tar.getmembers():
        if not member.isfile():
            continue
        stats["total_in_archive"] += 1

        # Progress logging every 5000 kills
        if stats["total_in_archive"] % 5000 == 0:
            logger.info(f"EVE Ref bulk {date_str}: {stats['total_in_archive']} processed, {stats['region_matches']} region matches, {stats['stored']} stored")
            _bulk_import_status["progress"] = f"{stats['total_in_archive']} processed, {stats['region_matches']} matches, {stats['stored']} stored"

        try:
            f = tar.extractfile(member)
            if not f:
                continue
            km_json = _json.loads(f.read())

            # Region-based filtering: keep ALL kills in watched regions
            system_id = km_json.get("solar_system_id")
            region_id = _system_to_region.get(system_id)
            if not region_id or region_id not in watched_regions:
                continue

            stats["region_matches"] += 1

            victim = km_json.get("victim", {})
            victim_alliance = victim.get("alliance_id")
            is_loss = victim_alliance in watched if watched else False

            killmail_id = km_json.get("killmail_id")
            if not killmail_id:
                continue

            # Check if already stored
            async with killmail_session() as db:
                existing = await db.get(Killmail, killmail_id)
                if existing:
                    stats["skipped"] += 1
                    continue

            # Parse into km_data format
            ship_type_id = victim.get("ship_type_id")
            if not ship_type_id:
                continue

            kill_time_str = km_json.get("killmail_time", "")
            try:
                killed_at = datetime.fromisoformat(kill_time_str.replace("Z", "+00:00"))
                if killed_at.tzinfo:
                    killed_at = killed_at.replace(tzinfo=None)
            except Exception:
                stats["errors"] += 1
                continue

            fit_items = []
            for item in victim.get("items", []):
                tid = item.get("item_type_id")
                if tid:
                    fit_items.append({
                        "type_id": tid,
                        "flag": item.get("flag", 0),
                        "qty_destroyed": item.get("quantity_destroyed", 0),
                        "qty_dropped": item.get("quantity_dropped", 0),
                    })

            # Parse attacker data
            attackers_raw = km_json.get("attackers", [])
            attacker_data = [{
                "alliance_id": a.get("alliance_id"),
                "corp_id": a.get("corporation_id"),
                "character_id": a.get("character_id"),
                "ship_type_id": a.get("ship_type_id"),
                "weapon_type_id": a.get("weapon_type_id"),
                "damage_done": a.get("damage_done", 0),
                "final_blow": a.get("final_blow", False),
            } for a in attackers_raw]

            km_data = {
                "killmail_id": killmail_id,
                "killmail_hash": km_json.get("killmail_hash", ""),
                "ship_type_id": ship_type_id,
                "victim_id": victim.get("character_id"),
                "victim_corp_id": victim.get("corporation_id"),
                "victim_alliance_id": victim_alliance,
                "solar_system_id": km_json.get("solar_system_id"),
                "killed_at": killed_at,
                "total_value": 0,  # Calculated from fit items in _store_killmail
                "fit_items": fit_items,
                "attacker_data": attacker_data,
                "attacker_count": len(attackers_raw),
                "is_loss": is_loss,
            }

            # Store through pipeline
            async with killmail_write_lock:
                async with killmail_session() as db:
                    stored = await zkill_listener._store_killmail(db, km_data)
                    if stored:
                        stats["stored"] += 1
                    await db.commit()

        except Exception as e:
            stats["errors"] += 1
            if stats["errors"] <= 3:
                logger.warning(f"EVE Ref bulk: error processing {member.name}: {e}")

    tar.close()
    logger.info(f"EVE Ref bulk complete for {date_str}: {stats}")
    _bulk_import_status["completed_days"] += 1
    _bulk_import_status["results"].append(stats)
    if _bulk_import_status["completed_days"] >= _bulk_import_status["total_days"]:
        _bulk_import_status["running"] = False
        _bulk_import_status["current_date"] = ""

    # Auto-backfill ISK values from zKill
    await backfill_isk_from_zkill()

    return stats


async def backfill_isk_from_zkill() -> dict:
    """
    Fetch ISK values from zKill API for all killmails with total_value=0.
    Groups by year/month, fetches alliance loss pages, updates matching killmail_ids.
    """
    import httpx
    from collections import defaultdict

    stats = {"updated": 0, "pages_fetched": 0, "missing": 0}

    # Step 1: Find all killmails with 0 ISK, group by year/month
    zero_value_ids = {}  # killmail_id → True
    months_needed = set()
    async with killmail_session() as db:
        result = await db.execute(
            select(Killmail.killmail_id, Killmail.killed_at)
            .where((Killmail.total_value == 0) | (Killmail.total_value.is_(None)))
        )
        for km_id, killed_at in result.fetchall():
            zero_value_ids[km_id] = True
            if killed_at:
                months_needed.add((killed_at.year, killed_at.month))

    if not zero_value_ids:
        logger.info("ISK backfill: no killmails with 0 value")
        return stats

    logger.info(f"ISK backfill: {len(zero_value_ids)} kills need ISK values across {len(months_needed)} months")

    # Get the actual alliance IDs from the kills that need fixing
    alliance_ids_needed = set()
    async with killmail_session() as db:
        result = await db.execute(
            select(Killmail.victim_alliance_id)
            .where((Killmail.total_value == 0) | (Killmail.total_value.is_(None)))
            .where(Killmail.victim_alliance_id.isnot(None))
            .distinct()
        )
        alliance_ids_needed = {r[0] for r in result.fetchall()}

    if not alliance_ids_needed:
        return {"error": "No alliance IDs found on kills"}

    logger.info(f"ISK backfill: querying {len(alliance_ids_needed)} alliances: {alliance_ids_needed}")

    headers = {
        "User-Agent": "VoidMarket/1.0 (EVE market tool)",
        "Accept-Encoding": "gzip",
    }

    # Step 2: For each alliance + month combo, fetch zKill pages and collect ISK values
    isk_values = {}  # killmail_id → totalValue
    async with httpx.AsyncClient(timeout=30.0, headers=headers) as http:
        for alliance_id in alliance_ids_needed:
            for year, month in sorted(months_needed):
                page = 1
                while True:
                    url = f"https://zkillboard.com/api/losses/allianceID/{alliance_id}/year/{year}/month/{month}/page/{page}/"
                    try:
                        resp = await http.get(url)
                        if resp.status_code == 429:
                            logger.warning("ISK backfill: rate limited, waiting 10s")
                            await asyncio.sleep(10)
                            continue
                        if resp.status_code != 200:
                            break
                        entries = resp.json()
                        if not isinstance(entries, list) or not entries:
                            break

                        for entry in entries:
                            km_id = entry.get("killmail_id")
                            zkb = entry.get("zkb", {})
                            total_value = zkb.get("totalValue", 0)
                            if km_id and km_id in zero_value_ids and total_value > 0:
                                isk_values[km_id] = total_value

                        stats["pages_fetched"] += 1
                        matched_this_page = sum(1 for e in entries if e.get("killmail_id") in zero_value_ids)
                        logger.info(f"ISK backfill: alliance {alliance_id} {year}/{month} page {page} — {len(entries)} entries, {matched_this_page} matched, {len(isk_values)} values total")

                        if len(entries) == 0:
                            break  # No more pages
                        page += 1
                        await asyncio.sleep(2)
                    except Exception as e:
                        logger.warning(f"ISK backfill: error {alliance_id} {year}/{month} p{page}: {e}")
                        break

                # Check if we've found all we need
                if len(isk_values) >= len(zero_value_ids):
                    break
            if len(isk_values) >= len(zero_value_ids):
                break

    if not isk_values:
        logger.info("ISK backfill: no values found on zKill")
        return stats

    # Step 3: Bulk update from zKill values
    async with killmail_write_lock:
        async with killmail_session() as db:
            for km_id, value in isk_values.items():
                await db.execute(
                    update(Killmail)
                    .where(Killmail.killmail_id == km_id)
                    .values(total_value=value)
                )
                stats["updated"] += 1
            await db.commit()

    stats["missing"] = len(zero_value_ids) - stats["updated"]
    if stats["missing"] > 0:
        logger.warning(f"ISK backfill: {stats['missing']} kills still have 0 ISK after month pagination — starting killID lookups")

        # Step 4: Individual killID lookups for remaining kills
        remaining_ids = []
        async with killmail_session() as db:
            result = await db.execute(
                select(Killmail.killmail_id)
                .where((Killmail.total_value == 0) | (Killmail.total_value.is_(None)))
            )
            remaining_ids = [r[0] for r in result.fetchall()]

        if remaining_ids:
            logger.info(f"ISK backfill: looking up {len(remaining_ids)} kills via killID endpoint")
            batch_values = {}
            for i, km_id in enumerate(remaining_ids):
                url = f"https://zkillboard.com/api/killID/{km_id}/"
                try:
                    async with httpx.AsyncClient(timeout=10.0, headers=headers) as http_single:
                        resp = await http_single.get(url)
                        if resp.status_code == 429:
                            logger.warning("ISK killID: rate limited, waiting 10s")
                            await asyncio.sleep(10)
                            continue
                        if resp.status_code == 200:
                            data = resp.json()
                            if isinstance(data, list) and data:
                                value = data[0].get("zkb", {}).get("totalValue", 0)
                                if value > 0:
                                    batch_values[km_id] = value
                except Exception as e:
                    if i < 3:
                        logger.warning(f"ISK killID: error for {km_id}: {e}")

                # Batch commit every 100
                if len(batch_values) >= 100:
                    async with killmail_write_lock:
                        async with killmail_session() as db:
                            for kid, val in batch_values.items():
                                await db.execute(
                                    update(Killmail).where(Killmail.killmail_id == kid).values(total_value=val)
                                )
                            await db.commit()
                    stats["updated"] += len(batch_values)
                    logger.info(f"ISK killID: {i+1}/{len(remaining_ids)} processed, {stats['updated']} total updated")
                    batch_values = {}

                await asyncio.sleep(1)  # 1 req/sec

            # Final batch
            if batch_values:
                async with killmail_write_lock:
                    async with killmail_session() as db:
                        for kid, val in batch_values.items():
                            await db.execute(
                                update(Killmail).where(Killmail.killmail_id == kid).values(total_value=val)
                            )
                        await db.commit()
                stats["updated"] += len(batch_values)

        stats["missing"] = len(zero_value_ids) - stats["updated"]

    logger.info(f"ISK backfill complete: {stats}")
    return stats


# Singleton
zkill_listener = ZkillListener()

