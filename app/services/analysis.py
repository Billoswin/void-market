"""
Void Market — Market Analysis Engine (v2)

Root cause fix: NEVER hold a DB session open while making external API calls.

Flow:
1. Read doctrine items from DB (fast, close session)
2. Fetch all EVE Tycoon data into memory (slow, no DB)
3. Read local market data from DB (fast, close session)
4. Compute analysis in memory
5. Write all results to DB in one batch (fast, close session)
"""
import logging
import asyncio
from datetime import datetime, timezone
from sqlalchemy import select, delete, func, case
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.database import (
    SdeType, MarketOrder, DoctrineFit, DoctrineFitItem,
    JitaPrice, ManualContract, AnalysisCache,
)
from app.services.evetycoon import evetycoon
from app.models.session import async_session

logger = logging.getLogger("void_market.analysis")

_analysis_lock = asyncio.Lock()


async def refresh_analysis(db: AsyncSession = None) -> dict:
    """Full refresh with proper session management."""
    if _analysis_lock.locked():
        return {"error": "Analysis already running"}
    async with _analysis_lock:
        return await _do_refresh()


async def _do_refresh() -> dict:
    # STEP 1: Read doctrine items from DB (fast)
    async with async_session() as db:
        result = await db.execute(select(DoctrineFitItem.type_id).distinct())
        type_ids = [row[0] for row in result.fetchall()]
        if not type_ids:
            return {"error": "No doctrine items found"}
        type_info = {}
        for tid in type_ids:
            item = await db.get(SdeType, tid)
            if item:
                type_info[tid] = {"name": item.name, "volume": item.volume}
    logger.info(f"Step 1: {len(type_ids)} doctrine items")
    _update_progress(f"Fetching prices for {len(type_ids)} items...")

    # STEP 2: Fetch EVE Tycoon data (slow, NO DB session open)
    jita_data = {}
    for i, type_id in enumerate(type_ids):
        try:
            jita = await evetycoon.get_jita_price(type_id)
            weekly_vol = await evetycoon.get_weekly_volume(type_id)
            jita_data[type_id] = {
                "sell_min": jita.get("sell_min"),
                "buy_max": jita.get("buy_max"),
                "sell_volume": jita.get("sell_volume", 0),
                "buy_volume": jita.get("buy_volume", 0),
                "weekly_volume": weekly_vol,
            }
        except Exception as e:
            logger.warning(f"EVE Tycoon {type_id}: {e}")
            jita_data[type_id] = {"sell_min": None, "buy_max": None, "sell_volume": 0, "buy_volume": 0, "weekly_volume": 0}
        if i % 5 == 4:
            await asyncio.sleep(0.3)
        if i % 20 == 0 and i > 0:
            logger.info(f"Step 2: {i}/{len(type_ids)} fetched")
            _update_progress(f"EVE Tycoon: {i}/{len(type_ids)} items...")
    logger.info(f"Step 2: {len(jita_data)} items from EVE Tycoon")

    # STEP 3: Read local market data (fast)
    async with async_session() as db:
        keepstar_stock = await _get_keepstar_stock(db)
        local_prices = await _get_local_prices(db)
        demand = await _get_doctrine_demand(db)
        contract_stock = await _get_contract_stock(db)
    logger.info("Step 3: Local data loaded")
    _update_progress("Computing analysis...")

    # STEP 4: Compute in memory
    now = datetime.now(timezone.utc)
    analysis_rows = []
    jita_rows = []

    for type_id in type_ids:
        info = type_info.get(type_id)
        if not info:
            continue
        jita = jita_data.get(type_id, {})
        jita_sell = jita.get("sell_min")
        weekly_vol = jita.get("weekly_volume", 0)
        import_cost = None
        if jita_sell and info["volume"]:
            import_cost = jita_sell + (info["volume"] * settings.freight_cost_per_m3)
        local_sell = local_prices.get(type_id)
        local_stock = keepstar_stock.get(type_id, 0)
        on_contracts = contract_stock.get(type_id, 0)
        doctrine_need = demand.get(type_id, 0)
        markup_pct = None
        if local_sell and import_cost and import_cost > 0:
            markup_pct = ((local_sell - import_cost) / import_cost) * 100
        target_price = import_cost * (1 + settings.default_markup_pct / 100) if import_cost else None
        weekly_profit = None
        if markup_pct is not None and import_cost and weekly_vol > 0:
            profit_per_unit = (local_sell or target_price or 0) - import_cost
            if profit_per_unit > 0:
                weekly_profit = profit_per_unit * min(weekly_vol * 0.01, doctrine_need or 10)
        stock_status = "ok"
        if local_stock == 0:
            stock_status = "out"
        elif doctrine_need > 0 and local_stock < doctrine_need * 0.5:
            stock_status = "low"

        analysis_rows.append(AnalysisCache(
            type_id=type_id, name=info["name"], volume_m3=info["volume"],
            doctrine_demand=doctrine_need, local_stock=local_stock,
            contract_stock=on_contracts,
            jita_sell=round(jita_sell, 2) if jita_sell else None,
            import_cost=round(import_cost, 2) if import_cost else None,
            local_sell=round(local_sell, 2) if local_sell else None,
            markup_pct=round(markup_pct, 1) if markup_pct is not None else None,
            target_price=round(target_price, 2) if target_price else None,
            weekly_volume=round(weekly_vol, 1) if weekly_vol else 0,
            weekly_profit=round(weekly_profit, 0) if weekly_profit else None,
            stock_status=stock_status, calculated_at=now,
        ))
        jita_rows.append({
            "type_id": type_id,
            "sell_min": jita.get("sell_min"), "buy_max": jita.get("buy_max"),
            "sell_volume": jita.get("sell_volume", 0), "buy_volume": jita.get("buy_volume", 0),
        })
    logger.info(f"Step 4: Computed {len(analysis_rows)} results")

    # STEP 5: Write to DB in one fast batch
    async with async_session() as db:
        for j in jita_rows:
            existing = await db.get(JitaPrice, j["type_id"])
            if existing:
                existing.sell_min = j["sell_min"]
                existing.buy_max = j["buy_max"]
                existing.sell_volume = j["sell_volume"]
                existing.buy_volume = j["buy_volume"]
                existing.updated_at = now
            else:
                db.add(JitaPrice(type_id=j["type_id"], sell_min=j["sell_min"],
                    buy_max=j["buy_max"], sell_volume=j["sell_volume"],
                    buy_volume=j["buy_volume"], updated_at=now))
        await db.execute(delete(AnalysisCache))
        db.add_all(analysis_rows)
        await db.commit()
    logger.info(f"Step 5: Cached {len(analysis_rows)} results")
    _update_progress("Done")
    return {"items_analyzed": len(analysis_rows), "jita_updated": len(jita_rows)}


async def load_cached_analysis(db: AsyncSession) -> list[dict]:
    result = await db.execute(
        select(AnalysisCache).order_by(
            case((AnalysisCache.stock_status == "out", 0),
                       (AnalysisCache.stock_status == "low", 1), else_=2),
            AnalysisCache.markup_pct.desc(),
        )
    )
    return [{
        "type_id": r.type_id, "name": r.name, "volume_m3": r.volume_m3,
        "doctrine_demand": r.doctrine_demand, "local_stock": r.local_stock,
        "contract_stock": r.contract_stock, "jita_sell": r.jita_sell,
        "import_cost": r.import_cost, "local_sell": r.local_sell,
        "markup_pct": r.markup_pct, "target_price": r.target_price,
        "weekly_volume": r.weekly_volume, "weekly_profit": r.weekly_profit,
        "stock_status": r.stock_status,
        "calculated_at": r.calculated_at.isoformat() if r.calculated_at else None,
    } for r in result.scalars().all()]


async def get_cache_age(db: AsyncSession) -> str | None:
    result = await db.execute(select(func.max(AnalysisCache.calculated_at)))
    ts = result.scalar()
    return ts.isoformat() if ts else None


async def _get_keepstar_stock(db):
    r = await db.execute(select(MarketOrder.type_id, func.sum(MarketOrder.volume_remain))
        .where(MarketOrder.location_id == settings.keepstar_structure_id)
        .where(MarketOrder.is_buy_order == False).group_by(MarketOrder.type_id))
    return {row[0]: row[1] for row in r.fetchall()}

async def _get_local_prices(db):
    r = await db.execute(select(MarketOrder.type_id, func.min(MarketOrder.price))
        .where(MarketOrder.location_id == settings.keepstar_structure_id)
        .where(MarketOrder.is_buy_order == False).group_by(MarketOrder.type_id))
    return {row[0]: row[1] for row in r.fetchall()}

async def _get_doctrine_demand(db):
    r = await db.execute(select(DoctrineFitItem.type_id, func.sum(DoctrineFitItem.quantity * DoctrineFit.min_stock))
        .join(DoctrineFit, DoctrineFitItem.fit_id == DoctrineFit.id).group_by(DoctrineFitItem.type_id))
    return {row[0]: row[1] for row in r.fetchall()}

async def _get_contract_stock(db):
    r = await db.execute(select(ManualContract.ship_type_id, func.sum(ManualContract.quantity))
        .group_by(ManualContract.ship_type_id))
    return {row[0]: row[1] for row in r.fetchall()}


# Progress reporting for background task
_progress_status = {"text": ""}

def _update_progress(text: str):
    _progress_status["text"] = text

def get_progress() -> str:
    return _progress_status["text"]
