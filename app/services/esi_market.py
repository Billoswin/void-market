"""
Void Market — ESI Market Data Fetcher

Pulls market data directly from CCP's ESI:
1. All orders in The Forge region → aggregate Jita prices
2. Price history per item → daily OHLCV data

Respects ESI rate limits:
- Checks X-Esi-Error-Limit-Remain header
- Pauses if error budget gets low
- Uses asyncio.Semaphore for concurrency control
"""
import logging
import asyncio
from datetime import datetime, timezone
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

import httpx

from app.config import settings
from app.models.database import JitaPrice, MarketHistory
from app.models.session import async_session

logger = logging.getLogger("void_market.esi_market")

ESI_BASE = "https://esi.evetech.net/latest"
FORGE_REGION = 10000002
JITA_STATION = 60003760  # Jita IV - Moon 4 - Caldari Navy Assembly Plant

# Progress reporting
_fetch_status = {
    "running": False,
    "task": "",
    "progress": "",
    "result": None,
    "error": None,
}


def get_fetch_status() -> dict:
    return dict(_fetch_status)


async def fetch_all_jita_prices() -> dict:
    """
    Pull ALL orders in The Forge from ESI, aggregate to Jita prices.
    Runs as background task — manages its own DB sessions.
    """
    if _fetch_status["running"]:
        return {"error": "Already running"}

    _fetch_status.update(running=True, task="jita_prices", progress="Starting...", result=None, error=None)

    try:
        result = await _do_fetch_jita()
        _fetch_status["result"] = result
        _fetch_status["progress"] = "Done"
        return result
    except Exception as e:
        logger.error(f"Jita fetch failed: {e}", exc_info=True)
        _fetch_status["error"] = str(e)
        _fetch_status["progress"] = f"Error: {e}"
        return {"error": str(e)}
    finally:
        _fetch_status["running"] = False


async def _do_fetch_jita() -> dict:
    """Pull all Forge orders, filter Jita, compute aggregates."""

    async with httpx.AsyncClient(timeout=30.0) as http:
        # Step 1: Get first page and total page count
        _fetch_status["progress"] = "Fetching page 1..."
        resp = await http.get(
            f"{ESI_BASE}/markets/{FORGE_REGION}/orders/",
            params={"datasource": "tranquility", "order_type": "all", "page": 1},
        )
        resp.raise_for_status()

        total_pages = int(resp.headers.get("X-Pages", 1))
        all_orders = resp.json()
        logger.info(f"Forge market: {total_pages} pages")
        _fetch_status["progress"] = f"Page 1/{total_pages}"

        # Step 2: Fetch remaining pages with concurrency
        sem = asyncio.Semaphore(15)  # Max concurrent requests
        errors = 0

        async def fetch_page(page: int):
            nonlocal errors
            async with sem:
                try:
                    r = await http.get(
                        f"{ESI_BASE}/markets/{FORGE_REGION}/orders/",
                        params={"datasource": "tranquility", "order_type": "all", "page": page},
                    )

                    # Check ESI error budget
                    error_remain = int(r.headers.get("X-Esi-Error-Limit-Remain", 100))
                    if error_remain < 20:
                        logger.warning(f"ESI error budget low: {error_remain} remaining, pausing...")
                        await asyncio.sleep(5)

                    if r.status_code == 200:
                        return r.json()
                    elif r.status_code == 420:
                        # Rate limited - back off
                        logger.warning("ESI rate limited, waiting 10s...")
                        await asyncio.sleep(10)
                        return []
                    else:
                        errors += 1
                        return []
                except Exception as e:
                    errors += 1
                    logger.warning(f"Page {page} error: {e}")
                    return []

        # Fetch in batches of 30 pages
        for batch_start in range(2, total_pages + 1, 30):
            batch_end = min(batch_start + 30, total_pages + 1)
            tasks = [fetch_page(p) for p in range(batch_start, batch_end)]
            results = await asyncio.gather(*tasks)
            for page_orders in results:
                all_orders.extend(page_orders)
            _fetch_status["progress"] = f"Pages {batch_end-1}/{total_pages} ({len(all_orders)} orders)"

            # Brief pause between batches
            await asyncio.sleep(0.5)

        logger.info(f"Fetched {len(all_orders)} total Forge orders ({errors} errors)")
        _fetch_status["progress"] = f"Processing {len(all_orders)} orders..."

        # Step 3: Filter to Jita station and aggregate
        jita_orders = [o for o in all_orders if o.get("location_id") == JITA_STATION]
        logger.info(f"Jita orders: {len(jita_orders)}")

        # Aggregate: min sell, max buy, volumes per type
        aggregates = {}  # type_id -> {sell_min, buy_max, sell_vol, buy_vol}
        for order in jita_orders:
            tid = order["type_id"]
            if tid not in aggregates:
                aggregates[tid] = {
                    "sell_min": None, "buy_max": None,
                    "sell_vol": 0, "buy_vol": 0,
                }
            agg = aggregates[tid]
            price = order["price"]
            vol = order.get("volume_remain", 0)

            if order["is_buy_order"]:
                agg["buy_vol"] += vol
                if agg["buy_max"] is None or price > agg["buy_max"]:
                    agg["buy_max"] = price
            else:
                agg["sell_vol"] += vol
                if agg["sell_min"] is None or price < agg["sell_min"]:
                    agg["sell_min"] = price

        logger.info(f"Aggregated {len(aggregates)} unique types at Jita")
        _fetch_status["progress"] = f"Writing {len(aggregates)} prices to cache..."

    # Step 4: Write to DB (no HTTP client open)
    now = datetime.now(timezone.utc)
    async with async_session() as db:
        for tid, agg in aggregates.items():
            existing = await db.get(JitaPrice, tid)
            if existing:
                existing.sell_min = agg["sell_min"]
                existing.buy_max = agg["buy_max"]
                existing.sell_volume = agg["sell_vol"]
                existing.buy_volume = agg["buy_vol"]
                existing.updated_at = now
            else:
                db.add(JitaPrice(
                    type_id=tid,
                    sell_min=agg["sell_min"],
                    buy_max=agg["buy_max"],
                    sell_volume=agg["sell_vol"],
                    buy_volume=agg["buy_vol"],
                    updated_at=now,
                ))
        await db.commit()

    logger.info(f"Cached {len(aggregates)} Jita prices")
    return {
        "total_forge_orders": len(all_orders),
        "jita_orders": len(jita_orders),
        "unique_types": len(aggregates),
        "pages_fetched": total_pages,
        "errors": errors,
    }


async def fetch_market_history(type_ids: list[int] = None) -> dict:
    """
    Pull price history from ESI for specified items (or all Keepstar items).
    Runs as background task — manages its own DB sessions.
    """
    if _fetch_status["running"]:
        return {"error": "Already running"}

    _fetch_status.update(running=True, task="history", progress="Starting...", result=None, error=None)

    try:
        # If no type_ids specified, get all from Keepstar
        if not type_ids:
            async with async_session() as db:
                from app.models.database import MarketOrder
                result = await db.execute(
                    select(MarketOrder.type_id)
                    .where(MarketOrder.location_id == settings.keepstar_structure_id)
                    .distinct()
                )
                type_ids = [row[0] for row in result.fetchall()]

        if not type_ids:
            _fetch_status.update(running=False, progress="No items")
            return {"error": "No items to fetch"}

        logger.info(f"Fetching history for {len(type_ids)} items")
        result = await _do_fetch_history(type_ids)
        _fetch_status["result"] = result
        _fetch_status["progress"] = "Done"
        return result
    except Exception as e:
        logger.error(f"History fetch failed: {e}", exc_info=True)
        _fetch_status["error"] = str(e)
        return {"error": str(e)}
    finally:
        _fetch_status["running"] = False


async def _do_fetch_history(type_ids: list[int]) -> dict:
    """Fetch ESI history for multiple items with rate limiting."""

    total = len(type_ids)
    sem = asyncio.Semaphore(8)  # Conservative concurrency
    all_history = {}  # type_id -> [rows]
    errors = 0
    done = 0

    async with httpx.AsyncClient(timeout=20.0) as http:

        async def fetch_one(tid: int):
            nonlocal done, errors
            async with sem:
                try:
                    r = await http.get(
                        f"{ESI_BASE}/markets/{FORGE_REGION}/history/",
                        params={"datasource": "tranquility", "type_id": tid},
                    )

                    # Check error budget
                    error_remain = int(r.headers.get("X-Esi-Error-Limit-Remain", 100))
                    if error_remain < 20:
                        await asyncio.sleep(5)

                    if r.status_code == 200:
                        all_history[tid] = r.json()
                    elif r.status_code == 420:
                        await asyncio.sleep(10)
                        errors += 1
                    else:
                        errors += 1
                except Exception:
                    errors += 1

                done += 1
                if done % 100 == 0:
                    _fetch_status["progress"] = f"History: {done}/{total} items"

        # Process in batches of 50
        for batch_start in range(0, total, 50):
            batch = type_ids[batch_start:batch_start + 50]
            await asyncio.gather(*[fetch_one(tid) for tid in batch])
            _fetch_status["progress"] = f"History: {min(batch_start + 50, total)}/{total} items"
            await asyncio.sleep(0.3)

    logger.info(f"Fetched history for {len(all_history)} items ({errors} errors)")
    _fetch_status["progress"] = f"Writing {len(all_history)} histories..."

    # Write to DB — MERGE, don't delete. This preserves accumulated history.
    async with async_session() as db:
        written = 0
        updated = 0
        for tid, rows in all_history.items():
            # Build a set of existing dates for this type so we can skip duplicates
            existing_result = await db.execute(
                select(MarketHistory.date)
                .where(MarketHistory.type_id == tid)
                .where(MarketHistory.region_id == FORGE_REGION)
            )
            existing_dates = {row[0] for row in existing_result.fetchall()}

            for row in rows:
                date_str = row.get("date", "")
                if not date_str:
                    continue

                if date_str in existing_dates:
                    # Update existing row (prices may have been revised)
                    await db.execute(
                        MarketHistory.__table__.update()
                        .where(MarketHistory.type_id == tid)
                        .where(MarketHistory.region_id == FORGE_REGION)
                        .where(MarketHistory.date == date_str)
                        .values(
                            average=row.get("average", 0),
                            highest=row.get("highest", 0),
                            lowest=row.get("lowest", 0),
                            volume=row.get("volume", 0),
                            order_count=row.get("order_count", 0),
                        )
                    )
                    updated += 1
                else:
                    db.add(MarketHistory(
                        type_id=tid,
                        region_id=FORGE_REGION,
                        date=date_str,
                        average=row.get("average", 0),
                        highest=row.get("highest", 0),
                        lowest=row.get("lowest", 0),
                        volume=row.get("volume", 0),
                        order_count=row.get("order_count", 0),
                    ))
                    written += 1

            # Flush periodically
            if (written + updated) % 3000 == 0:
                await db.flush()

        await db.commit()

    logger.info(f"History: {written} new rows, {updated} updated for {len(all_history)} items")
    return {
        "items_fetched": len(all_history),
        "new_rows": written,
        "updated_rows": updated,
        "errors": errors,
    }
