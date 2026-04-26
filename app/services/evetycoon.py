"""
Void Market — EVE Tycoon API Client

Public API at https://evetycoon.com/api (no auth required).

Endpoints:
- /v1/market/stats/{regionId}/{typeId} — price/volume stats (5min cache)
- /v1/market/orders/{typeId} — full order book, filterable by locationId
- /v1/market/history/{regionId}/{typeId} — 30 days price history (daily cache)
"""
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx

logger = logging.getLogger("void_market.evetycoon")

BASE_URL = "https://evetycoon.com/api"

# Region IDs
THE_FORGE = 10000002  # Jita's region


class EveTycoonClient:
    """Client for EVE Tycoon's public market API."""

    def __init__(self):
        self._http: httpx.AsyncClient | None = None

    async def start(self):
        self._http = httpx.AsyncClient(
            timeout=15.0,
            headers={"User-Agent": "VoidMarket/0.1 (EVE market tool)"},
        )

    async def stop(self):
        if self._http:
            await self._http.aclose()

    async def get_stats(
        self,
        type_id: int,
        region_id: int = THE_FORGE,
        location_id: int | None = None,
    ) -> dict | None:
        """
        Get price/volume stats for an item in a region.

        Returns:
            {buyVolume, sellVolume, buyOrders, sellOrders,
             buyAvgFivePercent, sellAvgFivePercent, ...}
        """
        url = f"{BASE_URL}/v1/market/stats/{region_id}/{type_id}"
        params = {}
        if location_id:
            params["locationId"] = location_id

        try:
            resp = await self._http.get(url, params=params)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 404:
                return None
            else:
                logger.warning(f"EVE Tycoon stats {type_id}: HTTP {resp.status_code}")
                return None
        except Exception as e:
            logger.warning(f"EVE Tycoon stats {type_id} error: {e}")
            return None

    async def get_history(
        self,
        type_id: int,
        region_id: int = THE_FORGE,
    ) -> list[dict] | None:
        """
        Get 30 days of price history for an item in a region.

        Returns list of:
            {date, regionId, typeId, average, highest, lowest, orderCount, volume}
        """
        url = f"{BASE_URL}/v1/market/history/{region_id}/{type_id}"

        try:
            resp = await self._http.get(url)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 404:
                return None
            else:
                logger.warning(f"EVE Tycoon history {type_id}: HTTP {resp.status_code}")
                return None
        except Exception as e:
            logger.warning(f"EVE Tycoon history {type_id} error: {e}")
            return None

    async def get_weekly_volume(
        self,
        type_id: int,
        region_id: int = THE_FORGE,
    ) -> float:
        """Calculate average weekly volume from 30-day history."""
        history = await self.get_history(type_id, region_id)
        if not history:
            return 0.0

        # Sum last 7 days of volume
        total = sum(day.get("volume", 0) for day in history[-7:])
        return total

    async def get_jita_price(self, type_id: int) -> dict:
        """
        Get Jita buy/sell prices from EVE Tycoon stats.

        Returns:
            {sell_min: float, buy_max: float, sell_volume: int, buy_volume: int}
        """
        stats = await self.get_stats(type_id, region_id=THE_FORGE)
        if not stats:
            return {"sell_min": None, "buy_max": None, "sell_volume": 0, "buy_volume": 0}

        return {
            "sell_min": stats.get("sellAvgFivePercent"),
            "buy_max": stats.get("buyAvgFivePercent"),
            "sell_volume": stats.get("sellVolume", 0),
            "buy_volume": stats.get("buyVolume", 0),
        }

    async def get_jita_prices_batch(self, type_ids: list[int]) -> dict[int, dict]:
        """
        Get Jita prices for multiple items.
        EVE Tycoon doesn't support batch, so we query individually
        with a light throttle.
        """
        import asyncio
        results = {}
        for i, type_id in enumerate(type_ids):
            results[type_id] = await self.get_jita_price(type_id)
            if i % 10 == 9:
                await asyncio.sleep(0.5)  # Be nice
        return results


# Singleton
evetycoon = EveTycoonClient()
