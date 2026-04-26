"""
Void Market — Goonmetrics API Integration

Pulls weekly_movement (weekly sales volume) for items on the C-J6MT Keepstar.
The Goonmetrics API is public, no auth required.
Returns XML with: weekly_movement, sell.min, sell.listed, buy.max, buy.listed

API: https://goonmetrics.apps.goonswarm.org/api/price_data/
     ?station_id=STRUCTURE_ID&type_id=TYPE1,TYPE2,...
     Up to 50 type_ids per request.
"""
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.database import GoonmetricsCache, MarketOrder
from app.config import settings

logger = logging.getLogger("void_market.goonmetrics")

GOONMETRICS_API_URL = "https://goonmetrics.apps.goonswarm.org/api/price_data/"
BATCH_SIZE = 50  # Max type_ids per API call


class GoonmetricsService:
    """Fetches weekly volume data from Goonmetrics' public API."""

    def __init__(self):
        self._http: httpx.AsyncClient | None = None

    async def start(self):
        self._http = httpx.AsyncClient(timeout=30.0)

    async def stop(self):
        if self._http:
            await self._http.aclose()

    async def sync_weekly_volumes(self, db: AsyncSession) -> dict:
        """
        Fetch weekly_movement for all items on the Keepstar.
        Batches requests in groups of 50 type_ids.
        """
        if not self._http:
            await self.start()

        if not settings.keepstar_structure_id:
            return {"error": "No Keepstar structure ID configured"}

        # Get all unique type_ids on the Keepstar
        from sqlalchemy import func
        result = await db.execute(
            select(MarketOrder.type_id)
            .where(MarketOrder.location_id == settings.keepstar_structure_id)
            .distinct()
        )
        type_ids = [r[0] for r in result.fetchall()]

        if not type_ids:
            return {"error": "No items on Keepstar"}

        logger.info(f"Fetching Goonmetrics data for {len(type_ids)} types in batches of {BATCH_SIZE}")

        total_fetched = 0
        total_errors = 0

        # Batch into groups of 50
        for i in range(0, len(type_ids), BATCH_SIZE):
            batch = type_ids[i:i + BATCH_SIZE]
            try:
                count = await self._fetch_batch(db, batch)
                total_fetched += count
            except Exception as e:
                logger.error(f"Goonmetrics batch {i//BATCH_SIZE + 1} failed: {e}")
                total_errors += 1

        await db.commit()
        logger.info(f"Goonmetrics sync complete: {total_fetched} types updated, {total_errors} batch errors")
        return {"updated": total_fetched, "errors": total_errors, "total_types": len(type_ids)}

    async def _fetch_batch(self, db: AsyncSession, type_ids: list[int]) -> int:
        """Fetch a batch of up to 50 type_ids from Goonmetrics."""
        type_id_str = ",".join(str(t) for t in type_ids)

        resp = await self._http.get(
            GOONMETRICS_API_URL,
            params={
                "station_id": settings.keepstar_structure_id,
                "type_id": type_id_str,
            },
        )

        if resp.status_code != 200:
            logger.warning(f"Goonmetrics returned {resp.status_code}")
            return 0

        # Parse XML response
        try:
            root = ET.fromstring(resp.text)
        except ET.ParseError as e:
            logger.error(f"Failed to parse Goonmetrics XML: {e}")
            return 0

        count = 0
        now = datetime.now(timezone.utc)

        for type_el in root.iter("type"):
            type_id = int(type_el.get("id", 0))
            if not type_id:
                continue

            weekly = 0
            sell_min = None
            sell_listed = 0
            buy_max = None
            buy_listed = 0

            all_el = type_el.find("all")
            if all_el is not None:
                wm = all_el.find("weekly_movement")
                if wm is not None and wm.text:
                    weekly = float(wm.text)

            buy_el = type_el.find("buy")
            if buy_el is not None:
                mx = buy_el.find("max")
                if mx is not None and mx.text:
                    buy_max = float(mx.text)
                ls = buy_el.find("listed")
                if ls is not None and ls.text:
                    buy_listed = int(float(ls.text))

            sell_el = type_el.find("sell")
            if sell_el is not None:
                mn = sell_el.find("min")
                if mn is not None and mn.text:
                    sell_min = float(mn.text)
                ls = sell_el.find("listed")
                if ls is not None and ls.text:
                    sell_listed = int(float(ls.text))

            # Upsert into cache
            existing = await db.get(GoonmetricsCache, type_id)
            if existing:
                existing.weekly_movement = weekly
                existing.sell_min = sell_min
                existing.sell_listed = sell_listed
                existing.buy_max = buy_max
                existing.buy_listed = buy_listed
                existing.updated_at = now
            else:
                db.add(GoonmetricsCache(
                    type_id=type_id,
                    weekly_movement=weekly,
                    sell_min=sell_min,
                    sell_listed=sell_listed,
                    buy_max=buy_max,
                    buy_listed=buy_listed,
                    updated_at=now,
                ))
            count += 1

        return count

    async def get_weekly_volume(self, db: AsyncSession, type_id: int) -> float:
        """Get cached weekly volume for a single item."""
        cached = await db.get(GoonmetricsCache, type_id)
        return cached.weekly_movement if cached else 0


goonmetrics_service = GoonmetricsService()
