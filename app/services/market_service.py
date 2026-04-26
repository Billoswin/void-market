"""
Void Market — Market Data Service

Handles:
- Fetching market orders from the Keepstar (private structure)
- Fetching Jita sell/buy prices for comparison
- Taking market snapshots for velocity tracking
- Build cost estimation from SDE blueprints + mineral prices
"""
import logging
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, delete, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.database import (
    MarketOrder, MarketSnapshot, JitaPrice,
    SdeType, SdeBlueprint, SdeBlueprintMaterial,
)
from app.services.esi_client import esi_client

logger = logging.getLogger("void_market.market")

# Common mineral type IDs for build cost estimation
MINERAL_TYPE_IDS = {
    34: "Tritanium",
    35: "Pyerite",
    36: "Mexallon",
    37: "Isogen",
    38: "Nocxium",
    39: "Zydrine",
    40: "Megacyte",
    11399: "Morphite",
}


class MarketService:
    """Fetches and analyzes market data."""

    async def fetch_structure_orders(self, db: AsyncSession, structure_id: int = None) -> dict:
        """
        Fetch market orders from a structure.
        If structure_id is None, defaults to the primary Keepstar from settings.
        """
        sid = structure_id or settings.keepstar_structure_id
        if not sid:
            return {"error": "No keepstar_structure_id configured"}

        logger.info(f"Fetching market orders for structure {sid}")

        try:
            orders = await esi_client.get_paginated(
                f"/markets/structures/{sid}/",
                db,
                authenticated=True,
            )
        except Exception as e:
            logger.error(f"Failed to fetch structure orders for {sid}: {e}")
            return {"error": str(e), "structure_id": sid}

        # Clear old orders for this structure
        await db.execute(
            delete(MarketOrder).where(MarketOrder.location_id == sid)
        )

        now = datetime.now(timezone.utc)
        count = 0

        for order in orders:
            db.add(MarketOrder(
                order_id=order["order_id"],
                type_id=order["type_id"],
                is_buy_order=order.get("is_buy_order", False),
                price=order["price"],
                volume_remain=order["volume_remain"],
                volume_total=order["volume_total"],
                location_id=order["location_id"],
                issued=datetime.fromisoformat(order["issued"].replace("Z", "+00:00")),
                duration=order["duration"],
                last_seen=now,
            ))
            count += 1

            if count % 500 == 0:
                await db.flush()

        await db.commit()
        logger.info(f"Loaded {count} market orders from structure {sid}")
        return {"orders_loaded": count, "structure_id": sid}

    async def fetch_all_tracked_structure_orders(self, db: AsyncSession) -> dict:
        """
        Fetch market orders for the primary Keepstar AND all enabled tracked structures.
        Returns per-structure results.
        """
        from app.models.database import TrackedStructure

        results = {}
        # Primary Keepstar
        if settings.keepstar_structure_id:
            r = await self.fetch_structure_orders(db, settings.keepstar_structure_id)
            results[settings.keepstar_structure_id] = r

        # Tracked structures
        tracked_r = await db.execute(
            select(TrackedStructure).where(TrackedStructure.enabled == True)
        )
        for s in tracked_r.scalars().all():
            if s.structure_id == settings.keepstar_structure_id:
                continue  # Already fetched
            r = await self.fetch_structure_orders(db, s.structure_id)
            results[s.structure_id] = r

        return results

    async def fetch_jita_prices(self, db: AsyncSession, type_ids: list[int] | None = None) -> dict:
        """
        Fetch Jita prices for items.
        Uses the public regional market endpoint (The Forge / region 10000002).

        If type_ids is None, fetches prices for all items in our doctrine BOM.
        """
        if type_ids is None:
            # Get all type_ids from doctrine fits
            result = await db.execute(text(
                "SELECT DISTINCT type_id FROM doctrine_fit_items"
            ))
            type_ids = [row[0] for row in result.fetchall()]

        if not type_ids:
            return {"prices_updated": 0}

        logger.info(f"Fetching Jita prices for {len(type_ids)} items")

        # ESI regional orders endpoint — we need to query per type_id
        # For efficiency, batch into concurrent requests
        now = datetime.now(timezone.utc)
        updated = 0

        for type_id in type_ids:
            try:
                orders = await esi_client.get_paginated(
                    f"/markets/{settings.jita_region_id}/orders/",
                    db,
                    params={"type_id": type_id, "order_type": "all"},
                    authenticated=False,
                )

                # Filter to Jita station only
                jita_orders = [o for o in orders if o["location_id"] == settings.jita_station_id]

                sell_orders = [o for o in jita_orders if not o.get("is_buy_order", False)]
                buy_orders = [o for o in jita_orders if o.get("is_buy_order", False)]

                sell_min = min((o["price"] for o in sell_orders), default=None)
                sell_vol = sum(o["volume_remain"] for o in sell_orders)
                buy_max = max((o["price"] for o in buy_orders), default=None)
                buy_vol = sum(o["volume_remain"] for o in buy_orders)

                # Upsert
                existing = await db.get(JitaPrice, type_id)
                if existing:
                    existing.sell_min = sell_min
                    existing.sell_volume = sell_vol
                    existing.buy_max = buy_max
                    existing.buy_volume = buy_vol
                    existing.updated_at = now
                else:
                    db.add(JitaPrice(
                        type_id=type_id,
                        sell_min=sell_min,
                        sell_volume=sell_vol,
                        buy_max=buy_max,
                        buy_volume=buy_vol,
                        updated_at=now,
                    ))

                updated += 1

            except Exception as e:
                logger.warning(f"Failed to fetch Jita price for type {type_id}: {e}")
                continue

        await db.commit()
        logger.info(f"Updated {updated} Jita prices")
        return {"prices_updated": updated}

    async def take_snapshot(self, db: AsyncSession) -> dict:
        """
        Take a snapshot of current market state for velocity tracking.
        Aggregates current orders by type_id.
        """
        now = datetime.now(timezone.utc)
        structure_id = settings.keepstar_structure_id

        if not structure_id:
            return {"error": "No keepstar configured"}

        # Aggregate current orders
        result = await db.execute(
            select(
                MarketOrder.type_id,
                MarketOrder.is_buy_order,
                func.sum(MarketOrder.volume_remain).label("total_volume"),
                func.min(MarketOrder.price).label("min_price"),
                func.max(MarketOrder.price).label("max_price"),
                func.count().label("order_count"),
            )
            .where(MarketOrder.location_id == structure_id)
            .group_by(MarketOrder.type_id, MarketOrder.is_buy_order)
        )

        # Group by type_id
        type_data: dict[int, dict] = {}
        for row in result.fetchall():
            tid = row[0]
            is_buy = row[1]
            if tid not in type_data:
                type_data[tid] = {
                    "sell_volume": 0, "sell_min_price": None,
                    "buy_volume": 0, "buy_max_price": None,
                    "order_count": 0,
                }

            if is_buy:
                type_data[tid]["buy_volume"] = row[2] or 0
                type_data[tid]["buy_max_price"] = row[4]
            else:
                type_data[tid]["sell_volume"] = row[2] or 0
                type_data[tid]["sell_min_price"] = row[3]

            type_data[tid]["order_count"] += row[5] or 0

        # Insert snapshots
        count = 0
        for tid, data in type_data.items():
            db.add(MarketSnapshot(
                type_id=tid,
                timestamp=now,
                sell_volume=data["sell_volume"],
                sell_min_price=data["sell_min_price"],
                buy_volume=data["buy_volume"],
                buy_max_price=data["buy_max_price"],
                order_count=data["order_count"],
            ))
            count += 1

        await db.commit()
        logger.info(f"Market snapshot taken: {count} items")
        return {"snapshot_items": count, "timestamp": now.isoformat()}

    async def calculate_velocity(
        self,
        db: AsyncSession,
        type_id: int,
        days: int = 7,
    ) -> float:
        """
        Estimate daily sell velocity for an item based on snapshot history.

        We measure the decrease in sell volume between consecutive snapshots.
        When volume drops, items were sold. When volume jumps up, a restock happened.

        Returns estimated units sold per day.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        result = await db.execute(
            select(MarketSnapshot.timestamp, MarketSnapshot.sell_volume)
            .where(MarketSnapshot.type_id == type_id)
            .where(MarketSnapshot.timestamp >= cutoff)
            .order_by(MarketSnapshot.timestamp.asc())
        )
        snapshots = result.fetchall()

        if len(snapshots) < 2:
            return 0.0

        total_sold = 0
        for i in range(1, len(snapshots)):
            prev_vol = snapshots[i - 1][1] or 0
            curr_vol = snapshots[i][1] or 0
            delta = prev_vol - curr_vol
            if delta > 0:
                # Volume decreased = items sold
                total_sold += delta
            # If delta < 0, restock happened — we ignore this

        time_span = (snapshots[-1][0] - snapshots[0][0]).total_seconds()
        if time_span <= 0:
            return 0.0

        days_elapsed = time_span / 86400
        return total_sold / days_elapsed if days_elapsed > 0 else 0.0

    async def estimate_build_cost(self, db: AsyncSession, type_id: int) -> float | None:
        """
        Estimate T1 manufacturing cost for an item.
        Uses Jita mineral buy prices × blueprint material requirements.

        Returns None if no blueprint exists or we're missing mineral prices.
        """
        # Find blueprint for this product
        result = await db.execute(
            select(SdeBlueprint)
            .where(SdeBlueprint.product_type_id == type_id)
        )
        blueprint = result.scalar_one_or_none()

        if not blueprint:
            return None

        # Get materials
        result = await db.execute(
            select(SdeBlueprintMaterial)
            .where(SdeBlueprintMaterial.blueprint_type_id == blueprint.blueprint_type_id)
        )
        materials = result.scalars().all()

        if not materials:
            return None

        total_cost = 0.0
        for mat in materials:
            # Get Jita price for this material
            jita = await db.get(JitaPrice, mat.material_type_id)
            if jita and jita.sell_min:
                total_cost += jita.sell_min * mat.quantity
            else:
                # Can't price this material — return None
                return None

        # Divide by product quantity (some BPOs produce multiple units)
        if blueprint.product_quantity > 1:
            total_cost /= blueprint.product_quantity

        return total_cost

    async def get_market_summary(self, db: AsyncSession, type_id: int) -> dict:
        """Get a summary of market data for a specific item."""
        structure_id = settings.keepstar_structure_id

        # Local market data
        sell_result = await db.execute(
            select(
                func.sum(MarketOrder.volume_remain),
                func.min(MarketOrder.price),
            )
            .where(MarketOrder.type_id == type_id)
            .where(MarketOrder.location_id == structure_id)
            .where(MarketOrder.is_buy_order == False)
        )
        sell_row = sell_result.fetchone()

        buy_result = await db.execute(
            select(
                func.sum(MarketOrder.volume_remain),
                func.max(MarketOrder.price),
            )
            .where(MarketOrder.type_id == type_id)
            .where(MarketOrder.location_id == structure_id)
            .where(MarketOrder.is_buy_order == True)
        )
        buy_row = buy_result.fetchone()

        # Jita price
        jita = await db.get(JitaPrice, type_id)

        # Velocity
        velocity = await self.calculate_velocity(db, type_id)

        # Build cost
        build_cost = await self.estimate_build_cost(db, type_id)

        return {
            "type_id": type_id,
            "local_sell_volume": sell_row[0] or 0 if sell_row else 0,
            "local_sell_min": sell_row[1] if sell_row else None,
            "local_buy_volume": buy_row[0] or 0 if buy_row else 0,
            "local_buy_max": buy_row[1] if buy_row else None,
            "jita_sell_min": jita.sell_min if jita else None,
            "jita_buy_max": jita.buy_max if jita else None,
            "velocity_per_day": round(velocity, 2),
            "build_cost": round(build_cost, 2) if build_cost else None,
        }


# Singleton
market_service = MarketService()
