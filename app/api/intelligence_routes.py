"""
Void Market — Intelligence & Industry Calculator & Doctrine & Fight API Routes (Complete)

Intelligence (QSNA-style with filters):
- /intelligence/trending — Weekly avg comparison with min ISK/orders filters
- /intelligence/underpriced — Current vs historical avg
- /intelligence/volume-anomaly — Recent vs historical volume
- /intelligence/items-to-seed — Jita volume gaps on Keepstar
- /intelligence/volume-leaders — With market cap %
- /intelligence/over-supply — With 10% price filter
- /intelligence/daily-profits — Daily profit chart data
- /intelligence/hourly-profits — Hourly profit chart data
- /intelligence/doctrine-costs/{fit_id} — Per-module cost comparison
- /intelligence/fight-restock — Restock shopping list from fight losses

Industry Calculator:
- /industry/calculator — Full build cost calculation
- /industry/blueprints/search — Blueprint search
"""
import logging
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.session import get_db
from app.services.intelligence import intelligence_service
from app.services.industry_calc import industry_calculator

logger = logging.getLogger("void_market.intel_routes")

intelligence_router = APIRouter(prefix="/intelligence", tags=["intelligence"])
industry_calc_router = APIRouter(prefix="/industry", tags=["industry"])


# ─── Intelligence Routes ───────────────────────────────────

@intelligence_router.get("/trending")
async def get_trending(
    market: str = Query("jita", regex="^(jita|local)$"),
    limit: int = Query(20, ge=5, le=100),
    min_isk: float = Query(0, ge=0),
    min_orders: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    return await intelligence_service.get_trending(db, market=market, limit=limit, min_isk=min_isk, min_orders=min_orders)


@intelligence_router.get("/underpriced")
async def get_underpriced(
    days: int = Query(30, ge=7, le=365),
    limit: int = Query(30, ge=5, le=100),
    min_isk: float = Query(0, ge=0),
    min_orders: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    return await intelligence_service.get_underpriced(db, days=days, limit=limit, min_isk=min_isk, min_orders=min_orders)


@intelligence_router.get("/volume-anomaly")
async def get_volume_anomaly(
    min_ratio: float = Query(2.0, ge=1.5, le=10.0),
    limit: int = Query(30, ge=5, le=100),
    min_isk: float = Query(0, ge=0),
    min_orders: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    return await intelligence_service.get_volume_anomaly(db, min_ratio=min_ratio, limit=limit, min_isk=min_isk, min_orders=min_orders)


@intelligence_router.get("/items-to-seed")
async def get_items_to_seed(
    limit: int = Query(50, ge=10, le=200),
    db: AsyncSession = Depends(get_db),
):
    return await intelligence_service.get_items_to_seed(db, limit=limit)


@intelligence_router.get("/volume-leaders")
async def get_volume_leaders(
    market: str = Query("jita", regex="^(jita|local)$"),
    limit: int = Query(30, ge=5, le=100),
    min_isk: float = Query(0, ge=0),
    min_orders: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    return await intelligence_service.get_volume_leaders(db, market=market, limit=limit, min_isk=min_isk, min_orders=min_orders)


@intelligence_router.get("/over-supply")
async def get_over_supply(
    market: str = Query("local", regex="^(jita|local)$"),
    limit: int = Query(30, ge=5, le=100),
    db: AsyncSession = Depends(get_db),
):
    return await intelligence_service.get_over_supply(db, market=market, limit=limit)


@intelligence_router.get("/daily-profits")
async def get_daily_profits(
    days: int = Query(30, ge=7, le=365),
    db: AsyncSession = Depends(get_db),
):
    return await intelligence_service.get_daily_profits(db, days=days)


@intelligence_router.get("/hourly-profits")
async def get_hourly_profits(
    days: int = Query(1, ge=1, le=7),
    db: AsyncSession = Depends(get_db),
):
    return await intelligence_service.get_hourly_profits(db, days=days)


@intelligence_router.get("/doctrine-costs/{fit_id}")
async def get_doctrine_costs(
    fit_id: int,
    db: AsyncSession = Depends(get_db),
):
    return await intelligence_service.get_doctrine_fit_costs(db, fit_id=fit_id)


@intelligence_router.get("/fight-restock")
async def get_fight_restock(
    days: int = Query(7, ge=1, le=30),
    db: AsyncSession = Depends(get_db),
):
    return await intelligence_service.get_restock_from_fights(db, days=days)


# ─── Industry Calculator Routes ────────────────────────────

@industry_calc_router.get("/blueprints/search")
async def search_blueprints(
    q: str = Query(..., min_length=2),
    limit: int = Query(20, ge=1, le=50),
    db: AsyncSession = Depends(get_db),
):
    return await industry_calculator.search_blueprints(db, query=q, limit=limit)


@industry_calc_router.get("/calculator")
async def calculate_blueprint(
    product_type_id: int = Query(None),
    blueprint_type_id: int = Query(None),
    runs: int = Query(1, ge=1, le=10000),
    me: int = Query(0, ge=0, le=10),
    te: int = Query(0, ge=0, le=20),
    facility: str = Query("station", regex="^(station|raitaru|azbel|sotiyo)$"),
    rig_bonus_pct: float = Query(0.0, ge=0, le=10),
    system_cost_index: float = Query(0.05, ge=0, le=1),
    facility_tax: float = Query(0.0, ge=0, le=50),
    price_source: str = Query("jita_sell", regex="^(jita_sell|jita_buy|local_sell)$"),
    sell_region: str = Query("jita", regex="^(jita|local)$"),
    db: AsyncSession = Depends(get_db),
):
    return await industry_calculator.calculate(
        db, blueprint_type_id=blueprint_type_id, product_type_id=product_type_id,
        runs=runs, me=me, te=te, facility=facility, rig_bonus_pct=rig_bonus_pct,
        system_cost_index=system_cost_index, facility_tax=facility_tax,
        price_source=price_source, sell_region=sell_region,
    )
