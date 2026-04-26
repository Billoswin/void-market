"""
Void Market — Data API Routes

Endpoints for market data, contracts, fights, and opportunity scoring.
"""
import logging
import asyncio
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks, Form, Request
from sqlalchemy import select, func, delete, Integer
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.session import get_db
from app.models.database import (
    SdeType, SdeMarketGroup, JitaPrice, MarketOrder, MarketHistory,
    ManualContract, Doctrine, DoctrineFit, DoctrineFitItem, AppSetting,
)
from app.config import settings
from app.services.market_service import market_service
from app.services.contract_service import contract_service
from app.services.zkill_service import zkill_service
from app.services.scoring_engine import scoring_engine
from app.services.analysis import refresh_analysis, load_cached_analysis, get_cache_age, get_progress
from app.services.esi_market import fetch_all_jita_prices, fetch_market_history, get_fetch_status

logger = logging.getLogger("void_market.data_api")


# ─── Market ─────────────────────────────────────────────────

market_router = APIRouter(prefix="/market", tags=["market"])


@market_router.post("/refresh")
async def refresh_market(db: AsyncSession = Depends(get_db)):
    """Fetch latest market orders from the Keepstar."""
    result = await market_service.fetch_structure_orders(db)
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


@market_router.post("/jita-prices")
async def refresh_jita_prices(db: AsyncSession = Depends(get_db)):
    """Fetch/update Jita prices for all doctrine items."""
    result = await market_service.fetch_jita_prices(db)
    return result


@market_router.post("/snapshot")
async def take_market_snapshot(db: AsyncSession = Depends(get_db)):
    """Take a market snapshot for velocity tracking."""
    result = await market_service.take_snapshot(db)
    return result


@market_router.get("/item/{type_id}")
async def get_item_market_data(type_id: int, db: AsyncSession = Depends(get_db)):
    """Get full market summary for a specific item."""
    return await market_service.get_market_summary(db, type_id)


# ─── Contracts ──────────────────────────────────────────────

contract_router = APIRouter(prefix="/contracts", tags=["contracts"])


@contract_router.post("/refresh")
async def refresh_contracts(db: AsyncSession = Depends(get_db)):
    """Fetch latest alliance contracts."""
    result = await contract_service.fetch_contracts(db)
    if "error" in result:
        raise HTTPException(400, result["error"])
    return result


@contract_router.get("/ships")
async def get_contract_ships(db: AsyncSession = Depends(get_db)):
    """Get ship counts available on alliance contracts."""
    counts = await contract_service.get_contract_ship_counts(db)
    return {"ships": counts}


@contract_router.get("/doctrine-match")
async def get_doctrine_contract_match(db: AsyncSession = Depends(get_db)):
    """Match alliance contracts against doctrine requirements."""
    return await contract_service.match_doctrine_contracts(db)


# ─── Fights ─────────────────────────────────────────────────

fight_router = APIRouter(prefix="/fights", tags=["fights"])


@fight_router.get("/timeline")
async def get_fight_timeline_endpoint(
    days: int = Query(default=30, ge=1, le=90),
    min_losses: int = Query(default=10, ge=1),
    deployment_id: int | None = Query(default=None),
):
    """Get fight timeline from killmail DB."""
    from app.services.zkill_listener import get_fight_timeline
    return await get_fight_timeline(days=days, min_losses=min_losses, deployment_id=deployment_id)


@fight_router.get("/doctrine-impact")
async def get_doctrine_impact_endpoint(
    days: int = Query(default=7, ge=1, le=90),
    deployment_id: int | None = Query(default=None),
):
    """Get doctrine impact assessment (cross-DB: killmails + market)."""
    from app.services.zkill_listener import get_doctrine_impact
    return await get_doctrine_impact(days=days, deployment_id=deployment_id)


@fight_router.get("/restock")
async def get_restock_list_endpoint(
    days: int = Query(default=7, ge=1, le=30),
    deployment_id: int | None = Query(default=None),
):
    """Get restock shopping list from doctrine losses (cross-DB)."""
    from app.services.zkill_listener import get_restock_list
    return await get_restock_list(days=days, deployment_id=deployment_id)


@fight_router.get("/loss-trends")
async def get_loss_trends_endpoint(
    weeks: int = Query(default=8, ge=1, le=52),
    deployment_id: int | None = Query(default=None),
):
    """Get weekly ship loss burn rates."""
    from app.services.zkill_listener import get_loss_trends
    return await get_loss_trends(weeks=weeks, deployment_id=deployment_id)


@fight_router.get("/burn-rate")
async def get_burn_rate_endpoint(
    deployment_id: int | None = Query(default=None),
):
    """Get comprehensive burn rate — all items destroyed (ships, modules, ammo, drones)."""
    from app.services.zkill_listener import get_burn_rate
    return await get_burn_rate(deployment_id=deployment_id)


@fight_router.get("/deployment-stats")
async def get_deployment_stats_endpoint(
    deployment_id: int = Query(...),
):
    """Get alliance-shareable deployment statistics."""
    from app.services.zkill_listener import get_deployment_stats
    return await get_deployment_stats(deployment_id=deployment_id)


@fight_router.get("/restock-alerts")
async def get_restock_alerts_endpoint(
    deployment_id: int | None = Query(default=None),
    hours: int = Query(default=24, ge=1, le=168),
):
    """Feature 2: Post-fight restock alerts — doctrine losses vs staging/home stock."""
    from app.services.zkill_listener import get_restock_alerts
    return await get_restock_alerts(deployment_id=deployment_id, hours=hours)


@fight_router.get("/import-opportunities")
async def get_import_opportunities_endpoint(
    deployment_id: int | None = Query(default=None),
):
    """Feature 3: Module burn → import list — positive-margin items sorted by ISK opportunity."""
    from app.services.zkill_listener import get_import_opportunities
    return await get_import_opportunities(deployment_id=deployment_id)


@fight_router.get("/stock-outs")
async def get_stock_outs_endpoint(
    deployment_id: int | None = Query(default=None),
    hours: int = Query(default=168, ge=1, le=720),
):
    """Feature 5+6: Stock-out detection + competitor restock speed."""
    from app.services.zkill_listener import get_stock_outs
    return await get_stock_outs(deployment_id=deployment_id, hours=hours)


@fight_router.get("/war-reserve")
async def get_war_reserve_endpoint(
    deployment_id: int | None = Query(default=None),
):
    """Feature 7: Predictive stocking / war reserve status."""
    from app.services.zkill_listener import get_war_reserve
    return await get_war_reserve(deployment_id=deployment_id)


@fight_router.get("/fight-patterns")
async def get_fight_patterns_endpoint(
    deployment_id: int | None = Query(default=None),
):
    """Feature 8: Granular import calendar — fight time patterns + import suggestions."""
    from app.services.zkill_listener import get_fight_patterns
    return await get_fight_patterns(deployment_id=deployment_id)


@fight_router.get("/contract-timing")
async def get_contract_timing_endpoint(
    deployment_id: int | None = Query(default=None),
):
    """Feature 4: Contract timing & pricing — sell velocity after fights vs baseline."""
    from app.services.zkill_listener import get_contract_timing
    return await get_contract_timing(deployment_id=deployment_id)


@market_router.get("/price-suggestion/{type_id}")
async def get_price_suggestion(
    type_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Suggest optimal sell price based on historical velocity at different price points."""
    from app.models.database import MarketOrder, JitaPrice, MarketSnapshot, SdeType
    from app.services.market_service import market_service

    # Get item info
    sde = await db.get(SdeType, type_id)
    name = sde.name if sde else f"Type {type_id}"

    # Current market state
    sell_orders = await db.execute(
        select(MarketOrder.price, MarketOrder.volume_remain)
        .where(MarketOrder.type_id == type_id)
        .where(MarketOrder.is_buy_order == False)
        .where(MarketOrder.location_id == settings.keepstar_structure_id)
        .order_by(MarketOrder.price.asc())
    )
    orders = sell_orders.fetchall()
    current_lowest = orders[0][0] if orders else None
    total_stock = sum(r[1] for r in orders) if orders else 0
    num_sellers = len(set(r[0] for r in orders))  # Approximate by unique prices

    # Jita price for cost basis
    jp = await db.get(JitaPrice, type_id)
    jita_sell = jp.sell_min if jp and jp.sell_min else 0
    freight_cost = (sde.volume or 0) * settings.freight_rate if sde else 0
    import_cost = jita_sell + freight_cost

    # Velocity (units/day)
    velocity = await market_service.calculate_velocity(db, type_id, days=14)

    # Days of stock
    days_of_stock = total_stock / velocity if velocity > 0 else 999

    # Analyze recent sales from snapshots to find price-velocity relationship
    cutoff = datetime.now(timezone.utc) - timedelta(days=14)
    snapshots = await db.execute(
        select(MarketSnapshot.timestamp, MarketSnapshot.sell_min, MarketSnapshot.sell_volume)
        .where(MarketSnapshot.type_id == type_id)
        .where(MarketSnapshot.timestamp >= cutoff)
        .order_by(MarketSnapshot.timestamp.asc())
    )
    snaps = snapshots.fetchall()

    # Track price at time of sales
    sale_prices = []
    for i in range(1, len(snaps)):
        prev_vol = snaps[i-1][2] or 0
        curr_vol = snaps[i][2] or 0
        if prev_vol > curr_vol:  # Volume decreased = sale happened
            sale_prices.append(snaps[i-1][1])  # Price at time of sale

    avg_sale_price = sum(sale_prices) / len(sale_prices) if sale_prices else current_lowest
    min_sale_price = min(sale_prices) if sale_prices else current_lowest
    max_sale_price = max(sale_prices) if sale_prices else current_lowest

    # Calculate margins
    margin_at_suggested = ((avg_sale_price - import_cost) / import_cost * 100) if import_cost > 0 and avg_sale_price else 0
    est_daily_profit = (avg_sale_price - import_cost) * velocity if velocity > 0 and avg_sale_price else 0

    return {
        "type_id": type_id,
        "name": name,
        "suggested_price": round(avg_sale_price, 2) if avg_sale_price else None,
        "current_lowest": current_lowest,
        "jita_price": jita_sell,
        "import_cost": round(import_cost, 2),
        "freight_cost": round(freight_cost, 2),
        "margin_pct": round(margin_at_suggested, 1),
        "velocity": round(velocity, 2),
        "days_of_stock": round(days_of_stock, 1),
        "total_stock": total_stock,
        "num_sellers": num_sellers,
        "recent_sales": len(sale_prices),
        "price_range": {
            "min": round(min_sale_price, 2) if min_sale_price else None,
            "avg": round(avg_sale_price, 2) if avg_sale_price else None,
            "max": round(max_sale_price, 2) if max_sale_price else None,
        },
        "est_daily_profit": round(est_daily_profit, 2),
    }


@fight_router.get("/listener-status")
async def get_listener_status():
    """Get zKill listener status and stats."""
    from app.services.zkill_listener import zkill_listener
    return zkill_listener.get_stats()


@fight_router.get("/diag/isk-debug")
async def isk_debug():
    """Debug ISK backfill — show sample killmail IDs and alliance breakdown."""
    from app.models.killmail_session import killmail_session
    from app.models.killmail_models import Killmail
    from sqlalchemy import select, func, distinct

    async with killmail_session() as db:
        # Count zero ISK kills
        total_zero = await db.execute(
            select(func.count(Killmail.killmail_id))
            .where(Killmail.is_loss == True)
            .where((Killmail.total_value == 0) | (Killmail.total_value.is_(None)))
        )
        zero_count = total_zero.scalar() or 0

        # Count kills with ISK
        total_has = await db.execute(
            select(func.count(Killmail.killmail_id))
            .where(Killmail.is_loss == True)
            .where(Killmail.total_value > 0)
        )
        has_count = total_has.scalar() or 0

        # Alliance breakdown for zero ISK
        alliance_breakdown = await db.execute(
            select(Killmail.victim_alliance_id, func.count(Killmail.killmail_id))
            .where(Killmail.is_loss == True)
            .where((Killmail.total_value == 0) | (Killmail.total_value.is_(None)))
            .group_by(Killmail.victim_alliance_id)
            .order_by(func.count(Killmail.killmail_id).desc())
        )
        alliances = [{"alliance_id": r[0], "count": r[1]} for r in alliance_breakdown.fetchall()]

        # Sample killmail IDs from zero ISK kills (GSF)
        sample = await db.execute(
            select(Killmail.killmail_id, Killmail.victim_alliance_id, Killmail.killed_at, Killmail.ship_name)
            .where(Killmail.is_loss == True)
            .where((Killmail.total_value == 0) | (Killmail.total_value.is_(None)))
            .where(Killmail.victim_alliance_id == 1354830081)
            .order_by(Killmail.killed_at.desc())
            .limit(10)
        )
        sample_ids = [{
            "killmail_id": r[0],
            "killmail_id_type": str(type(r[0])),
            "alliance_id": r[1],
            "killed_at": r[2].isoformat() if r[2] else None,
            "ship": r[3],
        } for r in sample.fetchall()]

        # Date range of zero ISK kills
        date_range = await db.execute(
            select(func.min(Killmail.killed_at), func.max(Killmail.killed_at))
            .where(Killmail.is_loss == True)
            .where((Killmail.total_value == 0) | (Killmail.total_value.is_(None)))
        )
        dr = date_range.first()

        return {
            "zero_isk_count": zero_count,
            "has_isk_count": has_count,
            "alliances_with_zero_isk": alliances,
            "sample_gsf_ids": sample_ids,
            "date_range": {
                "min": dr[0].isoformat() if dr[0] else None,
                "max": dr[1].isoformat() if dr[1] else None,
            },
        }


@fight_router.get("/settings")
async def get_fight_settings(db: AsyncSession = Depends(get_db)):
    """Get fight tracker settings."""
    import json
    defaults = {
        "min_fight_losses": 10,
        "fight_window_minutes": 30,
        "doctrine_match_pct": 60,
        "exclusion_patterns": "Capsule\nMobile ",
        "watched_alliances": [{"id": settings.alliance_id, "name": "Goonswarm Federation"}] if settings.alliance_id else [],
    }
    setting = await db.get(AppSetting, "fight_settings")
    if setting and setting.value:
        try:
            saved = json.loads(setting.value)
            defaults.update(saved)
        except Exception:
            pass
    return defaults


@fight_router.post("/settings")
async def save_fight_settings(
    db: AsyncSession = Depends(get_db),
    min_fight_losses: int = Form(default=10),
    fight_window_minutes: int = Form(default=30),
    doctrine_match_pct: int = Form(default=60),
    exclusion_patterns: str = Form(default="Capsule\nMobile "),
    watched_alliances: str = Form(default="[]"),
):
    """Save fight tracker settings and update listener."""
    import json
    data = {
        "min_fight_losses": min_fight_losses,
        "fight_window_minutes": fight_window_minutes,
        "doctrine_match_pct": doctrine_match_pct,
        "exclusion_patterns": exclusion_patterns,
        "watched_alliances": json.loads(watched_alliances),
    }
    setting = await db.get(AppSetting, "fight_settings")
    if setting:
        setting.value = json.dumps(data)
    else:
        db.add(AppSetting(key="fight_settings", value=json.dumps(data)))
    await db.commit()

    # Update listener runtime config
    from app.services.zkill_listener import zkill_listener, MIN_FIGHT_LOSSES, FIGHT_WINDOW_MINUTES
    import app.services.zkill_listener as zk_mod
    zk_mod.MIN_FIGHT_LOSSES = min_fight_losses
    zk_mod.FIGHT_WINDOW_MINUTES = fight_window_minutes
    zkill_listener._doctrine_match_pct = doctrine_match_pct / 100.0

    # Update watched alliance IDs
    alliances = json.loads(watched_alliances) if isinstance(watched_alliances, str) else watched_alliances
    zk_mod._watched_alliance_ids = {a["id"] for a in alliances if a.get("id")}

    return {"status": "saved", **data}


@fight_router.get("/ship-module-breakdown")
async def ship_module_breakdown(
    doctrine_id: int = Query(...),
    ship_type_id: int = Query(...),
    days: int = Query(default=7, ge=1, le=90),
    db: AsyncSession = Depends(get_db),
):
    """
    Return per-slot module breakdown for a doctrine ship.
    For each module in the doctrine fit: lost count (from killmails),
    market stock, needed (min_stock * qty), shortfall.
    """
    from app.models.database import DoctrineFit, DoctrineFitItem, SdeType, MarketOrder
    from app.models.killmail_session import killmail_session
    from app.models.killmail_models import Killmail
    from app.config import settings
    from datetime import datetime, timezone, timedelta
    import json as _json

    # Get the doctrine fit for this ship
    fit_result = await db.execute(
        select(DoctrineFit)
        .where(DoctrineFit.doctrine_id == doctrine_id)
        .where(DoctrineFit.ship_type_id == ship_type_id)
        .limit(1)
    )
    fit = fit_result.scalar_one_or_none()
    if not fit:
        return {"error": "Fit not found"}

    # Get all items for this fit
    items_result = await db.execute(
        select(DoctrineFitItem, SdeType.name, SdeType.group_id)
        .join(SdeType, SdeType.type_id == DoctrineFitItem.type_id)
        .where(DoctrineFitItem.fit_id == fit.id)
    )
    items = items_result.all()

    # Map slot_type strings to display categories
    def slot_label(slot_type: str | None) -> str:
        if not slot_type:
            return "Other"
        s = slot_type.lower()
        if s in ("high", "hi"): return "High"
        if s in ("mid", "med", "medium"): return "Medium"
        if s in ("low", "lo"): return "Low"
        if s == "rig": return "Rig"
        if s == "subsystem": return "Subsystem"
        if s in ("drone", "drones"): return "Drone"
        if s in ("cargo", "charge", "charges", "ammo"): return "Cargo"
        return "Other"

    # Min stock setting
    setting_r = await db.execute(select(AppSetting).where(AppSetting.key == "min_stock_default"))
    min_stock_row = setting_r.scalar_one_or_none()
    min_stock = int(min_stock_row.value) if min_stock_row and min_stock_row.value else 10

    # Get loss counts per item type from killmail DB (within days window)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    lost_counts = {}
    async with killmail_session() as kdb:
        km_result = await kdb.execute(
            select(Killmail.fit_items)
            .where(Killmail.is_loss == True)
            .where(Killmail.ship_type_id == ship_type_id)
            .where(Killmail.killed_at >= cutoff)
        )
        for row in km_result.all():
            fit_items = row[0] or []
            for mod in fit_items:
                tid = mod.get("type_id")
                qd = (mod.get("qty_destroyed", 0) or 0) + (mod.get("qty_dropped", 0) or 0)
                if tid:
                    lost_counts[tid] = lost_counts.get(tid, 0) + max(qd, 1)

    # Get market stock per type from Keepstar
    stock_counts = {}
    keepstar = settings.keepstar_structure_id
    if keepstar:
        item_tids = list({item[0].type_id for item in items})
        if item_tids:
            stock_r = await db.execute(
                select(MarketOrder.type_id, func.sum(MarketOrder.volume_remain))
                .where(MarketOrder.location_id == keepstar)
                .where(MarketOrder.is_buy_order == False)
                .where(MarketOrder.type_id.in_(item_tids))
                .group_by(MarketOrder.type_id)
            )
            for tid, qty in stock_r.all():
                stock_counts[tid] = int(qty or 0)

    # Group by slot
    slots = {}
    for fit_item, item_name, group_id in items:
        slot = slot_label(fit_item.slot_type)
        if slot not in slots:
            slots[slot] = []
        fit_qty = fit_item.quantity or 1
        need = min_stock * fit_qty
        lost = lost_counts.get(fit_item.type_id, 0)
        stock = stock_counts.get(fit_item.type_id, 0)
        short = max(0, need - stock)
        status = "ok" if stock >= need else ("critical" if stock < need * 0.5 else "low")
        slots[slot].append({
            "type_id": fit_item.type_id,
            "name": item_name,
            "fit_qty": fit_qty,
            "lost": lost,
            "stock": stock,
            "need": need,
            "short": short,
            "status": status,
        })

    # Order slots
    slot_order = ["High", "Medium", "Low", "Rig", "Subsystem", "Drone", "Cargo", "Other"]
    out_slots = []
    for s in slot_order:
        if s in slots:
            out_slots.append({"slot": s, "modules": sorted(slots[s], key=lambda m: -m["short"])})

    return {"fit_id": fit.id, "ship_type_id": ship_type_id, "slots": out_slots}


@fight_router.get("/losses")
async def get_loss_summary(
    days: int = Query(default=7, ge=1, le=30),
):
    """Get summary of ships lost (from killmail DB)."""
    from app.services.zkill_listener import get_loss_trends
    trends = await get_loss_trends(weeks=max(1, days // 7))
    # Convert to legacy format for any existing consumers
    return [{"ship_type_id": t["ship_type_id"], "ship_name": t["ship_name"],
             "times_lost": t["total_lost"], "total_isk_lost": 0} for t in trends]


# ─── Deployments ────────────────────────────────────

deployment_router = APIRouter(prefix="/deployments", tags=["deployments"])


@deployment_router.get("")
async def list_deployments():
    """List all deployments with aggregate stats."""
    from app.models.killmail_session import killmail_session
    from app.models.killmail_models import Deployment, Killmail
    from sqlalchemy import select, func

    async with killmail_session() as db:
        result = await db.execute(select(Deployment).order_by(Deployment.started_at.desc()))
        deployments = result.scalars().all()

        out = []
        for dep in deployments:
            # Aggregate stats
            stats_result = await db.execute(
                select(
                    func.count(Killmail.killmail_id).label("total_losses"),
                    func.coalesce(func.sum(Killmail.total_value), 0).label("total_isk"),
                    func.coalesce(func.sum(
                        func.cast(Killmail.is_doctrine_loss, Integer)
                    ), 0).label("doctrine_losses"),
                )
                .where(Killmail.deployment_id == dep.id)
                .where(Killmail.is_loss == True)
            )
            stats = stats_result.first()

            out.append({
                "id": dep.id,
                "name": dep.name,
                "status": dep.status,
                "watched_region_ids": dep.watched_region_ids or [],
                "watched_region_names": dep.watched_region_names or [],
                "staging_structure_id": dep.staging_structure_id,
                "staging_structure_name": dep.staging_structure_name,
                "started_at": dep.started_at.isoformat() if dep.started_at else None,
                "ended_at": dep.ended_at.isoformat() if dep.ended_at else None,
                "notes": dep.notes,
                "total_losses": stats.total_losses or 0,
                "total_isk_lost": float(stats.total_isk or 0),
                "doctrine_losses": stats.doctrine_losses or 0,
            })
        return out


@deployment_router.post("")
async def create_deployment(
    name: str = Form(...),
    watched_region_ids: str = Form(default="[]"),
    watched_region_names: str = Form(default="[]"),
    started_at: str | None = Form(default=None),
    notes: str = Form(default=""),
):
    """Create a new deployment."""
    import json
    from app.models.killmail_session import killmail_session, killmail_write_lock
    from app.models.killmail_models import Deployment

    region_ids = json.loads(watched_region_ids)
    region_names = json.loads(watched_region_names)

    start_dt = datetime.fromisoformat(started_at) if started_at else datetime.now(timezone.utc)
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)

    async with killmail_write_lock:
        async with killmail_session() as db:
            dep = Deployment(
                name=name,
                status="active",
                watched_region_ids=region_ids,
                watched_region_names=region_names,
                started_at=start_dt,
                notes=notes or None,
            )
            db.add(dep)
            await db.commit()
            await db.refresh(dep)
            return {"id": dep.id, "name": dep.name, "status": dep.status}


@deployment_router.post("/{deployment_id}/end")
async def end_deployment(deployment_id: int):
    """Mark a deployment as ended."""
    from app.models.killmail_session import killmail_session, killmail_write_lock
    from app.models.killmail_models import Deployment

    async with killmail_write_lock:
        async with killmail_session() as db:
            dep = await db.get(Deployment, deployment_id)
            if not dep:
                raise HTTPException(404, "Deployment not found")
            dep.status = "ended"
            dep.ended_at = datetime.now(timezone.utc)
            await db.commit()
            return {"id": dep.id, "status": "ended"}


@deployment_router.post("/{deployment_id}/staging")
async def set_deployment_staging(
    deployment_id: int,
    structure_id: int = Form(...),
    structure_name: str = Form(default=""),
):
    """Set or update the staging Keepstar for a deployment.
    Also auto-adds the structure to TrackedStructure for market order fetching."""
    from app.models.killmail_session import killmail_session, killmail_write_lock
    from app.models.killmail_models import Deployment

    async with killmail_write_lock:
        async with killmail_session() as db:
            dep = await db.get(Deployment, deployment_id)
            if not dep:
                raise HTTPException(404, "Deployment not found")
            dep.staging_structure_id = structure_id
            dep.staging_structure_name = structure_name or f"Staging {structure_id}"
            await db.commit()

    # Auto-add to TrackedStructure in market DB so orders get fetched on 5-min cycle
    from app.models.session import async_session as market_session, db_write_lock
    from app.models.database import TrackedStructure

    async with db_write_lock:
        async with market_session() as mdb:
            existing = await mdb.get(TrackedStructure, structure_id)
            if not existing:
                mdb.add(TrackedStructure(
                    structure_id=structure_id,
                    name=structure_name or f"Staging {structure_id}",
                    enabled=True,
                ))
                await mdb.commit()

    return {
        "id": dep.id,
        "staging_structure_id": structure_id,
        "staging_structure_name": dep.staging_structure_name,
    }


@deployment_router.delete("/{deployment_id}/staging")
async def clear_deployment_staging(deployment_id: int):
    """Clear the staging Keepstar from a deployment."""
    from app.models.killmail_session import killmail_session, killmail_write_lock
    from app.models.killmail_models import Deployment

    async with killmail_write_lock:
        async with killmail_session() as db:
            dep = await db.get(Deployment, deployment_id)
            if not dep:
                raise HTTPException(404, "Deployment not found")
            dep.staging_structure_id = None
            dep.staging_structure_name = None
            await db.commit()

    return {"id": dep.id, "staging_structure_id": None}


@deployment_router.post("/{deployment_id}/retag")
async def retag_deployment_killmails(deployment_id: int):
    """Re-scan all killmails and tag ones in watched regions to this deployment."""
    from app.models.killmail_session import killmail_session, killmail_write_lock
    from app.models.killmail_models import Deployment, Killmail, DeploymentFight, DeploymentFightSystem
    from sqlalchemy import update, or_

    async with killmail_write_lock:
        async with killmail_session() as db:
            dep = await db.get(Deployment, deployment_id)
            if not dep:
                raise HTTPException(404, "Deployment not found")

            watched = set(dep.watched_region_ids or [])
            if not watched:
                return {"tagged": 0, "fight_tagged": 0}

            # Tag all killmails in watched regions that aren't already tagged to another deployment
            result = await db.execute(
                update(Killmail)
                .where(Killmail.region_id.in_(watched))
                .where(or_(Killmail.deployment_id.is_(None), Killmail.deployment_id == deployment_id))
                .values(deployment_id=deployment_id)
            )
            tagged = result.rowcount or 0

            # Now match against named fights
            fight_tagged = 0
            fights_result = await db.execute(
                select(DeploymentFight).where(DeploymentFight.deployment_id == deployment_id)
            )
            for fight in fights_result.scalars().all():
                systems_result = await db.execute(
                    select(DeploymentFightSystem).where(DeploymentFightSystem.deployment_fight_id == fight.id)
                )
                for sys_row in systems_result.scalars().all():
                    r = await db.execute(
                        update(Killmail)
                        .where(Killmail.deployment_id == deployment_id)
                        .where(Killmail.solar_system_id == sys_row.system_id)
                        .where(Killmail.killed_at >= sys_row.start_time)
                        .where(Killmail.killed_at <= sys_row.end_time)
                        .values(deployment_fight_id=fight.id)
                    )
                    fight_tagged += r.rowcount or 0

            await db.commit()
            return {"tagged": tagged, "fight_tagged": fight_tagged}


@deployment_router.post("/{deployment_id}/detect-fights")
async def detect_deployment_fights(deployment_id: int):
    """Run retroactive fight detection on all unassigned killmails in this deployment."""
    from app.services.zkill_listener import detect_fights_retroactive
    result = await detect_fights_retroactive(deployment_id=deployment_id)
    return result


@deployment_router.get("/fights/{fight_id}/detail")
async def get_fight_detail(fight_id: int):
    """Get full fight detail: header, timeline, ship group breakdown, per-system stats."""
    from app.models.killmail_session import killmail_session
    from app.models.killmail_models import DeploymentFight, DeploymentFightSystem, Killmail
    from app.models.session import async_session as market_session
    from app.models.database import SdeType, SdeGroup
    from collections import defaultdict

    # 1. Load fight + systems from killmail DB
    async with killmail_session() as db:
        fight = await db.get(DeploymentFight, fight_id)
        if not fight:
            raise HTTPException(404, "Fight not found")

        sys_result = await db.execute(
            select(DeploymentFightSystem).where(DeploymentFightSystem.deployment_fight_id == fight_id)
        )
        systems = [{
            "system_name": s.system_name, "system_id": s.system_id,
            "start_time": s.start_time.isoformat() if s.start_time else None,
            "end_time": s.end_time.isoformat() if s.end_time else None,
        } for s in sys_result.scalars().all()]

        # 2. Load ALL killmails in this fight (both sides)
        km_result = await db.execute(
            select(Killmail)
            .where(Killmail.deployment_fight_id == fight_id)
            .order_by(Killmail.killed_at.asc())
        )
        all_killmails = km_result.scalars().all()

    # Split by side
    our_losses = [km for km in all_killmails if km.is_loss]
    enemy_losses = [km for km in all_killmails if not km.is_loss]

    if not all_killmails:
        return {
            "fight": {"id": fight.id, "name": fight.name},
            "systems": systems, "header": {}, "our_groups": [], "enemy_groups": [],
            "timeline": [], "system_breakdown": [], "enemy": {},
        }

    # 3. Get group names from market DB (cross-DB lookup)
    ship_type_ids = list(set(km.ship_type_id for km in all_killmails if km.ship_type_id))
    group_lookup = {}  # type_id → group_name
    async with market_session() as db:
        if ship_type_ids:
            result = await db.execute(
                select(SdeType.type_id, SdeType.group_id)
                .where(SdeType.type_id.in_(ship_type_ids))
            )
            type_to_group_id = {r[0]: r[1] for r in result.fetchall()}

            group_ids = list(set(v for v in type_to_group_id.values() if v))
            if group_ids:
                try:
                    gr = await db.execute(
                        select(SdeGroup.group_id, SdeGroup.name)
                        .where(SdeGroup.group_id.in_(group_ids))
                    )
                    gid_to_name = {r[0]: r[1] for r in gr.fetchall()}
                except Exception:
                    gid_to_name = {}

                for tid, gid in type_to_group_id.items():
                    group_lookup[tid] = gid_to_name.get(gid, "Unknown")

    # 4. Build header stats — both sides
    first_kill = all_killmails[0].killed_at
    last_kill = all_killmails[-1].killed_at
    duration_sec = (last_kill - first_kill).total_seconds()

    our_isk = sum(km.total_value or 0 for km in our_losses)
    enemy_isk = sum(km.total_value or 0 for km in enemy_losses)
    total_isk = our_isk + enemy_isk
    efficiency = round(enemy_isk / total_isk * 100, 1) if total_isk > 0 else 0

    # Filter capsules/deployables
    our_real = [km for km in our_losses if km.ship_name and not km.ship_name.startswith("Capsule") and not km.ship_name.startswith("Mobile ")]
    enemy_real = [km for km in enemy_losses if km.ship_name and not km.ship_name.startswith("Capsule") and not km.ship_name.startswith("Mobile ")]

    header = {
        "our_losses": len(our_real),
        "our_isk": our_isk,
        "enemy_losses": len(enemy_real),
        "enemy_isk": enemy_isk,
        "efficiency": efficiency,
        "total_kills": len(our_real) + len(enemy_real),
        "duration_seconds": duration_sec,
        "duration_str": f"{int(duration_sec // 3600)}h {int((duration_sec % 3600) // 60)}m",
        "first_kill": first_kill.isoformat(),
        "last_kill": last_kill.isoformat(),
    }

    # 5. Ship group breakdown — build for BOTH sides
    def _build_groups(killmails_list, total_isk_side):
        gdata = defaultdict(lambda: {"ships": defaultdict(lambda: {"count": 0, "isk": 0, "ship_type_id": 0, "doctrine": 0})})
        for km in killmails_list:
            if km.ship_name and km.ship_name.startswith("Capsule"):
                continue
            if km.ship_name and km.ship_name.startswith("Mobile "):
                continue
            gname = group_lookup.get(km.ship_type_id, "Unknown")
            ship = gdata[gname]["ships"][km.ship_name or "Unknown"]
            ship["count"] += 1
            ship["isk"] += km.total_value or 0
            ship["ship_type_id"] = km.ship_type_id
            if km.is_doctrine_loss:
                ship["doctrine"] += 1

        groups = []
        for gname, gd in gdata.items():
            ships_list = []
            gc = 0
            gi = 0
            for sn, sd in sorted(gd["ships"].items(), key=lambda x: -x[1]["isk"]):
                ships_list.append({
                    "ship_name": sn, "ship_type_id": sd["ship_type_id"],
                    "count": sd["count"], "isk": sd["isk"], "doctrine": sd["doctrine"],
                })
                gc += sd["count"]
                gi += sd["isk"]
            groups.append({
                "group_name": gname, "count": gc, "isk": gi,
                "pct": round(gi / total_isk_side * 100, 1) if total_isk_side > 0 else 0,
                "ships": ships_list,
            })
        groups.sort(key=lambda g: -g["isk"])
        return groups

    our_groups = _build_groups(our_losses, our_isk)
    enemy_groups = _build_groups(enemy_losses, enemy_isk)

    # 6. Timeline — dual cumulative ISK lines
    timeline = []
    cum_our = 0
    cum_enemy = 0
    for km in all_killmails:
        if km.ship_name and km.ship_name.startswith("Capsule"):
            continue
        if km.is_loss:
            cum_our += km.total_value or 0
        else:
            cum_enemy += km.total_value or 0
        timeline.append({
            "t": km.killed_at.isoformat(),
            "ts": int(km.killed_at.timestamp()),
            "our_isk": cum_our,
            "enemy_isk": cum_enemy,
            "ship_name": km.ship_name,
            "ship_type_id": km.ship_type_id,
            "value": km.total_value or 0,
            "is_loss": km.is_loss,
        })

    # 7. Per-system breakdown — both sides
    sys_stats = defaultdict(lambda: {"our_count": 0, "our_isk": 0, "enemy_count": 0, "enemy_isk": 0})
    for km in all_killmails:
        if km.ship_name and (km.ship_name.startswith("Capsule") or km.ship_name.startswith("Mobile ")):
            continue
        sname = km.solar_system_name or str(km.solar_system_id)
        if km.is_loss:
            sys_stats[sname]["our_count"] += 1
            sys_stats[sname]["our_isk"] += km.total_value or 0
        else:
            sys_stats[sname]["enemy_count"] += 1
            sys_stats[sname]["enemy_isk"] += km.total_value or 0
    system_breakdown = [{"system_name": k, **v} for k, v in
                        sorted(sys_stats.items(), key=lambda x: -(x[1]["our_isk"] + x[1]["enemy_isk"]))]

    # 8. FC Detection — Monitor pilots from BOTH sides' attacker data
    monitor_pilots = {}
    MONITOR_TYPE_ID = 45534
    for km in all_killmails:
        if not km.attacker_data:
            continue
        for a in km.attacker_data:
            ship_tid = a.get("ship_type_id")
            cid = a.get("character_id")
            if ship_tid == MONITOR_TYPE_ID and cid:
                side = "ours" if not km.is_loss else "enemy"  # If km is our loss, attackers are enemy
                if cid not in monitor_pilots:
                    monitor_pilots[cid] = {
                        "character_id": cid,
                        "alliance_id": a.get("alliance_id"),
                        "side": side,
                        "first_seen": km.killed_at.isoformat() if km.killed_at else None,
                        "last_seen": km.killed_at.isoformat() if km.killed_at else None,
                        "systems": set(),
                        "kill_count": 0,
                    }
                mp = monitor_pilots[cid]
                if km.killed_at:
                    mp["last_seen"] = km.killed_at.isoformat()
                if km.solar_system_name:
                    mp["systems"].add(km.solar_system_name)
                mp["kill_count"] += 1

    fc_list = sorted([
        {**mp, "systems": list(mp["systems"])}
        for mp in monitor_pilots.values()
    ], key=lambda x: -x["kill_count"])

    # Also detect our FCs who DIED in a Monitor
    for km in our_losses:
        if km.ship_type_id == MONITOR_TYPE_ID and km.victim_id:
            cid = km.victim_id
            if cid not in monitor_pilots:
                fc_list.append({
                    "character_id": cid,
                    "alliance_id": km.victim_alliance_id,
                    "side": "ours",
                    "first_seen": km.killed_at.isoformat() if km.killed_at else None,
                    "last_seen": km.killed_at.isoformat() if km.killed_at else None,
                    "systems": [km.solar_system_name] if km.solar_system_name else [],
                    "kill_count": 0,
                    "died": True,
                })

    # Resolve FC + alliance names via ESI
    if fc_list:
        import httpx
        resolve_ids = set()
        for fc in fc_list:
            if fc.get("character_id"):
                resolve_ids.add(fc["character_id"])
            if fc.get("alliance_id"):
                resolve_ids.add(fc["alliance_id"])
        if resolve_ids:
            try:
                async with httpx.AsyncClient(timeout=15.0) as http:
                    resp = await http.post(
                        "https://esi.evetech.net/latest/universe/names/",
                        params={"datasource": "tranquility"},
                        json=list(resolve_ids)[:1000],
                    )
                    if resp.status_code == 200:
                        name_map = {n["id"]: n["name"] for n in resp.json()}
                        for fc in fc_list:
                            if fc.get("character_id") and fc["character_id"] in name_map:
                                fc["character_name"] = name_map[fc["character_id"]]
                            if fc.get("alliance_id") and fc["alliance_id"] in name_map:
                                fc["alliance_name"] = name_map[fc["alliance_id"]]
            except Exception:
                pass

    return {
        "fight": {"id": fight.id, "name": fight.name},
        "systems": systems,
        "header": header,
        "our_groups": our_groups,
        "enemy_groups": enemy_groups,
        "timeline": timeline,
        "system_breakdown": system_breakdown,
        "fcs": fc_list,
    }


@deployment_router.delete("/{deployment_id}")
async def delete_deployment(deployment_id: int):
    """Delete a deployment and untag its killmails (they stay in DB)."""
    from app.models.killmail_session import killmail_session, killmail_write_lock
    from app.models.killmail_models import Deployment, DeploymentFight, Killmail
    from sqlalchemy import update, delete

    async with killmail_write_lock:
        async with killmail_session() as db:
            # Untag killmails
            await db.execute(
                update(Killmail)
                .where(Killmail.deployment_id == deployment_id)
                .values(deployment_id=None, deployment_fight_id=None)
            )
            # Delete fights
            fights_result = await db.execute(
                select(DeploymentFight).where(DeploymentFight.deployment_id == deployment_id)
            )
            for f in fights_result.scalars().all():
                await db.delete(f)
            # Delete deployment
            dep = await db.get(Deployment, deployment_id)
            if dep:
                await db.delete(dep)
            await db.commit()
            return {"deleted": deployment_id}


@deployment_router.get("/{deployment_id}/summary")
async def get_deployment_summary(deployment_id: int):
    """Aggregate stats: named fights + 'Other' bucket breakdown."""
    from app.models.killmail_session import killmail_session
    from app.models.killmail_models import Deployment, DeploymentFight, Killmail
    from sqlalchemy import select, func

    async with killmail_session() as db:
        dep = await db.get(Deployment, deployment_id)
        if not dep:
            raise HTTPException(404, "Deployment not found")

        # Get all fights
        fights_result = await db.execute(
            select(DeploymentFight)
            .where(DeploymentFight.deployment_id == deployment_id)
            .order_by(DeploymentFight.created_at.desc())
        )
        fights = []
        for f in fights_result.scalars().all():
            # Our losses
            loss_result = await db.execute(
                select(
                    func.count(Killmail.killmail_id).label("losses"),
                    func.coalesce(func.sum(Killmail.total_value), 0).label("isk"),
                )
                .where(Killmail.deployment_fight_id == f.id)
                .where(Killmail.is_loss == True)
            )
            ls = loss_result.first()
            # Enemy losses (our kills)
            kill_result = await db.execute(
                select(
                    func.count(Killmail.killmail_id).label("kills"),
                    func.coalesce(func.sum(Killmail.total_value), 0).label("isk"),
                )
                .where(Killmail.deployment_fight_id == f.id)
                .where(Killmail.is_loss == False)
            )
            ks = kill_result.first()
            total_kills = (ks.kills or 0)
            total_losses = (ls.losses or 0)
            our_isk = float(ls.isk or 0)
            enemy_isk = float(ks.isk or 0)
            efficiency = round(enemy_isk / (our_isk + enemy_isk) * 100, 1) if (our_isk + enemy_isk) > 0 else 0

            # Get actual fight time (first kill)
            time_result = await db.execute(
                select(func.min(Killmail.killed_at))
                .where(Killmail.deployment_fight_id == f.id)
            )
            started_at = time_result.scalar()

            fights.append({
                "id": f.id,
                "name": f.name,
                "notes": f.notes,
                "losses": total_losses,
                "kills": total_kills,
                "total_isk": our_isk,
                "isk_destroyed": enemy_isk,
                "efficiency": efficiency,
                "started_at": started_at.isoformat() if started_at else None,
                "created_at": f.created_at.isoformat() if f.created_at else None,
            })

        # Sort by most recent fight first
        fights.sort(key=lambda f: f["started_at"] or "", reverse=True)

        # "Other" bucket = deployment killmails without a fight
        other_result = await db.execute(
            select(
                func.count(Killmail.killmail_id).label("losses"),
                func.coalesce(func.sum(Killmail.total_value), 0).label("isk"),
            )
            .where(Killmail.deployment_id == deployment_id)
            .where(Killmail.deployment_fight_id.is_(None))
            .where(Killmail.is_loss == True)
        )
        other = other_result.first()

        # Top ships in "Other" (include NULL ship names; only filter explicit capsules/deployables)
        top_other_result = await db.execute(
            select(
                Killmail.ship_type_id,
                Killmail.ship_name,
                func.count(Killmail.killmail_id).label("cnt"),
            )
            .where(Killmail.deployment_id == deployment_id)
            .where(Killmail.deployment_fight_id.is_(None))
            .where(Killmail.is_loss == True)
            .where(func.coalesce(Killmail.ship_name, "").notlike("Capsule%"))
            .where(func.coalesce(Killmail.ship_name, "").notlike("Mobile %"))
            .group_by(Killmail.ship_type_id, Killmail.ship_name)
            .order_by(func.count(Killmail.killmail_id).desc())
            .limit(10)
        )
        top_other = [
            {"ship_type_id": r[0], "ship_name": r[1] or f"Type {r[0]}", "count": r[2]}
            for r in top_other_result.all()
        ]

        # System breakdown for "Other" — where are the untagged losses?
        other_systems_result = await db.execute(
            select(
                Killmail.solar_system_name,
                Killmail.solar_system_id,
                func.count(Killmail.killmail_id).label("cnt"),
                func.coalesce(func.sum(Killmail.total_value), 0).label("isk"),
            )
            .where(Killmail.deployment_id == deployment_id)
            .where(Killmail.deployment_fight_id.is_(None))
            .where(Killmail.is_loss == True)
            .where(func.coalesce(Killmail.ship_name, "").notlike("Capsule%"))
            .where(func.coalesce(Killmail.ship_name, "").notlike("Mobile %"))
            .group_by(Killmail.solar_system_id, Killmail.solar_system_name)
            .order_by(func.count(Killmail.killmail_id).desc())
            .limit(30)
        )
        other_systems = [
            {"system_name": r[0] or str(r[1]), "system_id": r[1], "count": r[2], "isk": float(r[3] or 0)}
            for r in other_systems_result.all()
        ]

        return {
            "deployment": {
                "id": dep.id,
                "name": dep.name,
                "status": dep.status,
                "watched_region_names": dep.watched_region_names or [],
                "staging_structure_id": dep.staging_structure_id,
                "staging_structure_name": dep.staging_structure_name,
                "started_at": dep.started_at.isoformat() if dep.started_at else None,
                "ended_at": dep.ended_at.isoformat() if dep.ended_at else None,
            },
            "fights": fights,
            "other": {
                "losses": other.losses or 0,
                "total_isk": float(other.isk or 0),
                "top_ships": top_other,
                "systems": other_systems,
            },
        }


@deployment_router.post("/{deployment_id}/fights")
async def create_deployment_fight(
    deployment_id: int,
    name: str = Form(...),
    systems: str = Form(...),
    notes: str = Form(default=""),
):
    """Create a named fight with system+time rows.
    systems JSON: [{"system_id": N, "system_name": "X", "start_time": "ISO", "end_time": "ISO"}]
    """
    import json
    from app.models.killmail_session import killmail_session, killmail_write_lock
    from app.models.killmail_models import (
        Deployment, DeploymentFight, DeploymentFightSystem, Killmail
    )
    from sqlalchemy import update, or_, and_

    system_rows = json.loads(systems)
    if not system_rows:
        raise HTTPException(400, "At least one system+time row required")

    async with killmail_write_lock:
        async with killmail_session() as db:
            dep = await db.get(Deployment, deployment_id)
            if not dep:
                raise HTTPException(404, "Deployment not found")

            fight = DeploymentFight(
                deployment_id=deployment_id,
                name=name,
                notes=notes or None,
            )
            db.add(fight)
            await db.flush()

            # Add system rows
            for row in system_rows:
                start_dt = datetime.fromisoformat(row["start_time"].replace("Z", "+00:00"))
                end_dt = datetime.fromisoformat(row["end_time"].replace("Z", "+00:00"))
                db.add(DeploymentFightSystem(
                    deployment_fight_id=fight.id,
                    system_id=row["system_id"],
                    system_name=row["system_name"],
                    start_time=start_dt,
                    end_time=end_dt,
                ))

            await db.flush()

            # Auto-extend deployment start date if fight goes further back
            for row in system_rows:
                fight_start = datetime.fromisoformat(row["start_time"].replace("Z", "+00:00"))
                if fight_start.tzinfo:
                    fight_start = fight_start.replace(tzinfo=None)
                if dep.started_at and fight_start < dep.started_at:
                    dep.started_at = fight_start

            # Retroactively tag existing killmails that match this fight
            tagged = 0
            for row in system_rows:
                start_dt = datetime.fromisoformat(row["start_time"].replace("Z", "+00:00"))
                end_dt = datetime.fromisoformat(row["end_time"].replace("Z", "+00:00"))
                result = await db.execute(
                    update(Killmail)
                    .where(Killmail.deployment_id == deployment_id)
                    .where(Killmail.solar_system_id == row["system_id"])
                    .where(Killmail.killed_at >= start_dt)
                    .where(Killmail.killed_at <= end_dt)
                    .values(deployment_fight_id=fight.id)
                )
                tagged += result.rowcount or 0

            await db.commit()
            return {"id": fight.id, "name": fight.name, "killmails_tagged": tagged}


@deployment_router.delete("/fights/{fight_id}")
async def delete_deployment_fight(fight_id: int):
    """Delete a fight and untag its killmails (they stay in 'Other')."""
    from app.models.killmail_session import killmail_session, killmail_write_lock
    from app.models.killmail_models import DeploymentFight, Killmail
    from sqlalchemy import update

    async with killmail_write_lock:
        async with killmail_session() as db:
            await db.execute(
                update(Killmail)
                .where(Killmail.deployment_fight_id == fight_id)
                .values(deployment_fight_id=None)
            )
            fight = await db.get(DeploymentFight, fight_id)
            if fight:
                await db.delete(fight)
            await db.commit()
            return {"deleted": fight_id}


@deployment_router.post("/fights/merge")
async def merge_fights(request: Request):
    """Merge multiple fights into one. Keeps the earliest fight, moves all killmails."""
    from app.models.killmail_session import killmail_session, killmail_write_lock
    from app.models.killmail_models import DeploymentFight, DeploymentFightSystem, Killmail
    from sqlalchemy import update, select, func, delete

    body = await request.json()
    fight_ids = body.get("fight_ids", [])
    custom_name = body.get("name")

    if len(fight_ids) < 2:
        raise HTTPException(400, "Need at least 2 fights to merge")

    async with killmail_write_lock:
        async with killmail_session() as db:
            # Load all fights
            result = await db.execute(
                select(DeploymentFight).where(DeploymentFight.id.in_(fight_ids))
            )
            fights = result.scalars().all()
            if len(fights) < 2:
                raise HTTPException(404, "Could not find all specified fights")

            # Find the earliest fight (by first kill timestamp)
            earliest_fight = None
            earliest_time = None
            for f in fights:
                t_result = await db.execute(
                    select(func.min(Killmail.killed_at))
                    .where(Killmail.deployment_fight_id == f.id)
                )
                t = t_result.scalar()
                if t and (earliest_time is None or t < earliest_time):
                    earliest_time = t
                    earliest_fight = f

            if not earliest_fight:
                earliest_fight = fights[0]

            keep_id = earliest_fight.id
            absorb_ids = [f.id for f in fights if f.id != keep_id]

            # Move all killmails from absorbed fights to the keeper
            killmails_moved = 0
            for fid in absorb_ids:
                r = await db.execute(
                    update(Killmail)
                    .where(Killmail.deployment_fight_id == fid)
                    .values(deployment_fight_id=keep_id)
                )
                killmails_moved += r.rowcount

            # Merge fight systems (copy systems from absorbed fights)
            for fid in absorb_ids:
                sys_result = await db.execute(
                    select(DeploymentFightSystem)
                    .where(DeploymentFightSystem.deployment_fight_id == fid)
                )
                for sys in sys_result.scalars().all():
                    # Check if system already exists in keeper
                    existing = await db.execute(
                        select(DeploymentFightSystem)
                        .where(DeploymentFightSystem.deployment_fight_id == keep_id)
                        .where(DeploymentFightSystem.system_id == sys.system_id)
                    )
                    if not existing.scalars().first():
                        db.add(DeploymentFightSystem(
                            deployment_fight_id=keep_id,
                            system_id=sys.system_id,
                            system_name=sys.system_name,
                            start_time=sys.start_time,
                            end_time=sys.end_time,
                        ))

            # Delete absorbed fights and their systems
            for fid in absorb_ids:
                await db.execute(
                    delete(DeploymentFightSystem)
                    .where(DeploymentFightSystem.deployment_fight_id == fid)
                )
                f = await db.get(DeploymentFight, fid)
                if f:
                    await db.delete(f)

            # Rename the kept fight
            if custom_name:
                earliest_fight.name = custom_name
            else:
                # Auto-generate name from merged data
                total = await db.execute(
                    select(func.count(Killmail.killmail_id))
                    .where(Killmail.deployment_fight_id == keep_id)
                )
                total_count = total.scalar() or 0
                # Get all unique system names
                sys_result = await db.execute(
                    select(DeploymentFightSystem.system_name)
                    .where(DeploymentFightSystem.deployment_fight_id == keep_id)
                )
                systems = [s[0] for s in sys_result.all()]
                primary = systems[0] if systems else "Unknown"
                sys_suffix = f" +{len(systems)-1} systems" if len(systems) > 1 else ""
                # Get time span
                time_r = await db.execute(
                    select(func.min(Killmail.killed_at), func.max(Killmail.killed_at))
                    .where(Killmail.deployment_fight_id == keep_id)
                )
                tr = time_r.first()
                if tr[0] and tr[1]:
                    dur = tr[1] - tr[0]
                    dur_str = f"{int(dur.total_seconds() // 3600)}h {int((dur.total_seconds() % 3600) // 60)}m"
                else:
                    dur_str = "?m"
                earliest_fight.name = f"{primary}{sys_suffix} ({total_count} kills, {dur_str})"

            await db.commit()

            return {
                "kept": keep_id,
                "merged": len(absorb_ids) + 1,
                "killmails_moved": killmails_moved,
                "name": earliest_fight.name,
            }


@deployment_router.post("/fights/from-br")
async def create_fight_from_br(request: Request):
    """Create a fight from a battle report — given systems and UTC time window, find matching killmails."""
    from app.models.killmail_session import killmail_session, killmail_write_lock
    from app.models.killmail_models import Deployment, DeploymentFight, DeploymentFightSystem, Killmail
    from sqlalchemy import select, func, update, and_

    body = await request.json()
    deployment_id = body.get("deployment_id")
    system_names = body.get("systems", [])
    start_time = body.get("start_time")
    end_time = body.get("end_time")
    custom_name = body.get("name")

    if not system_names or not start_time or not end_time:
        raise HTTPException(400, "systems, start_time, and end_time required")

    start_dt = datetime.fromisoformat(start_time)
    end_dt = datetime.fromisoformat(end_time)

    async with killmail_write_lock:
        async with killmail_session() as db:
            # Verify deployment exists
            dep = await db.get(Deployment, deployment_id)
            if not dep:
                raise HTTPException(404, "Deployment not found")

            # Look up system IDs from names in killmail DB
            # (solar_system_name is stored on killmails)
            sys_name_upper = [s.upper().strip() for s in system_names]

            # Find ALL matching killmails regardless of deployment tag
            result = await db.execute(
                select(Killmail)
                .where(
                    func.upper(Killmail.solar_system_name).in_(sys_name_upper),
                    Killmail.killed_at >= start_dt,
                    Killmail.killed_at <= end_dt,
                )
            )
            matching = result.scalars().all()

            if not matching:
                raise HTTPException(404, f"No killmails found in {', '.join(system_names)} between {start_time} and {end_time}")

            our_losses = sum(1 for km in matching if km.is_loss)
            enemy_kills = sum(1 for km in matching if not km.is_loss)
            total = len(matching)

            # Build fight name
            primary = system_names[0]
            sys_suffix = f" +{len(system_names)-1} systems" if len(system_names) > 1 else ""
            duration = end_dt - start_dt
            dur_str = f"{int(duration.total_seconds() // 3600)}h {int((duration.total_seconds() % 3600) // 60)}m"
            fight_name = custom_name or f"{primary}{sys_suffix} ({total} kills, {dur_str})"

            # Create fight
            fight = DeploymentFight(
                deployment_id=deployment_id,
                name=fight_name,
                auto_detected=False,
            )
            db.add(fight)
            await db.flush()

            # Add system entries
            # Group killmails by system to get per-system time ranges
            sys_data = {}
            for km in matching:
                sn = km.solar_system_name
                if sn not in sys_data:
                    sys_data[sn] = {"system_id": km.solar_system_id, "min": km.killed_at, "max": km.killed_at}
                else:
                    if km.killed_at < sys_data[sn]["min"]:
                        sys_data[sn]["min"] = km.killed_at
                    if km.killed_at > sys_data[sn]["max"]:
                        sys_data[sn]["max"] = km.killed_at

            for sn, sd in sys_data.items():
                db.add(DeploymentFightSystem(
                    deployment_fight_id=fight.id,
                    system_id=sd["system_id"] or 0,
                    system_name=sn,
                    start_time=sd["min"],
                    end_time=sd["max"],
                ))

            # Tag killmails to this deployment + fight
            km_ids = [km.killmail_id for km in matching]
            await db.execute(
                update(Killmail)
                .where(Killmail.killmail_id.in_(km_ids))
                .values(deployment_id=deployment_id, deployment_fight_id=fight.id)
            )

            await db.commit()

            return {
                "id": fight.id,
                "name": fight_name,
                "killmails_tagged": total,
                "our_losses": our_losses,
                "enemy_kills": enemy_kills,
            }


@deployment_router.post("/{deployment_id}/backfill")
async def backfill_deployment(
    deployment_id: int,
    background_tasks: BackgroundTasks,
    start_time: str = Form(...),
    end_time: str = Form(...),
):
    """Backfill killmails from zKill API for this deployment's time/region window."""
    from app.services.zkill_listener import backfill_from_zkill

    start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
    # Ensure timezone-aware (datetime-local inputs don't include timezone)
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=timezone.utc)

    async def _run():
        await backfill_from_zkill(start_dt, end_dt, deployment_id=deployment_id)

    background_tasks.add_task(_run)
    return {"status": "started", "deployment_id": deployment_id,
            "start": start_time, "end": end_time}


@deployment_router.post("/{deployment_id}/bulk-import")
async def bulk_import_deployment(
    deployment_id: int,
    background_tasks: BackgroundTasks,
    dates: str = Form(...),
):
    """Bulk import killmails from EVE Ref daily archives.
    dates: comma-separated YYYY-MM-DD dates, e.g. '2026-04-08,2026-04-09,2026-04-10'
    """
    from app.services.zkill_listener import bulk_import_everef, _bulk_import_status

    date_list = [d.strip() for d in dates.split(",") if d.strip()]
    if not date_list:
        raise HTTPException(400, "At least one date required")

    # Initialize progress
    _bulk_import_status.update({
        "running": True, "total_days": len(date_list),
        "completed_days": 0, "current_date": "", "results": []
    })

    async def _run():
        for date_str in date_list:
            await bulk_import_everef(date_str, deployment_id=deployment_id)

    background_tasks.add_task(_run)
    return {"status": "started", "deployment_id": deployment_id,
            "dates": date_list, "count": len(date_list)}


@deployment_router.get("/bulk-import/status")
async def bulk_import_status():
    """Get current bulk import progress."""
    from app.services.zkill_listener import _bulk_import_status
    return dict(_bulk_import_status)


@deployment_router.post("/recalculate-isk")
async def recalculate_isk(background_tasks: BackgroundTasks):
    """Backfill ISK values from zKill API for all killmails with 0 value."""
    from app.services.zkill_listener import backfill_isk_from_zkill

    async def _run():
        await backfill_isk_from_zkill()

    background_tasks.add_task(_run)
    return {"status": "started", "message": "ISK backfill running in background"}


@deployment_router.get("/esi/region-search")
async def search_regions(q: str = Query(..., min_length=2), db: AsyncSession = Depends(get_db)):
    """Autocomplete region names from local SDE."""
    from app.models.database import SdeRegion
    result = await db.execute(
        select(SdeRegion)
        .where(SdeRegion.name.ilike(f"%{q}%"))
        .order_by(SdeRegion.name)
        .limit(15)
    )
    return [{"id": r.region_id, "name": r.name} for r in result.scalars().all()]


@deployment_router.get("/esi/system-search")
async def search_systems(q: str = Query(..., min_length=2), db: AsyncSession = Depends(get_db)):
    """Autocomplete system names from local SDE."""
    from app.models.database import SdeSolarSystem
    result = await db.execute(
        select(SdeSolarSystem)
        .where(SdeSolarSystem.name.ilike(f"{q}%"))
        .order_by(SdeSolarSystem.name)
        .limit(15)
    )
    return [{"id": s.system_id, "name": s.name, "security": s.security_status}
            for s in result.scalars().all()]


@deployment_router.get("/esi/alliance-search")
async def search_alliances(q: str = Query(..., min_length=2)):
    """Search alliances via ESI universe/ids/."""
    import httpx
    try:
        async with httpx.AsyncClient(timeout=10.0) as http:
            resp = await http.post(
                "https://esi.evetech.net/latest/universe/ids/",
                params={"datasource": "tranquility"},
                json=[q],
            )
            if resp.status_code == 200:
                data = resp.json()
                alliances = data.get("alliances", [])
                return [{"id": a["id"], "name": a["name"]} for a in alliances]
    except Exception:
        pass
    return []


@deployment_router.post("/esi/names")
async def resolve_esi_names(ids: list[int]):
    """Batch resolve ESI IDs to names (characters, alliances, corporations, types)."""
    import httpx
    if not ids or len(ids) > 1000:
        return []
    try:
        async with httpx.AsyncClient(timeout=15.0) as http:
            resp = await http.post(
                "https://esi.evetech.net/latest/universe/names/",
                params={"datasource": "tranquility"},
                json=ids[:1000],
            )
            if resp.status_code == 200:
                return resp.json()  # [{id, name, category}, ...]
    except Exception:
        pass
    return []


# ─── Opportunities ──────────────────────────────────────────

opportunity_router = APIRouter(prefix="/opportunities", tags=["opportunities"])


@opportunity_router.post("/calculate")
async def calculate_opportunities(db: AsyncSession = Depends(get_db)):
    """Run the full opportunity scoring pipeline."""
    result = await scoring_engine.calculate_all(db)
    return result


@opportunity_router.get("")
async def get_opportunities(
    limit: int = Query(default=50, ge=1, le=200),
    min_score: float = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """Get ranked market opportunities."""
    return await scoring_engine.get_ranked_opportunities(db, limit=limit, min_score=min_score)


# ─── Full Refresh ───────────────────────────────────────────

refresh_router = APIRouter(prefix="/refresh", tags=["refresh"])


@refresh_router.post("/all")
async def refresh_all_data(db: AsyncSession = Depends(get_db)):
    """
    Run a full data refresh:
    1. Market orders
    2. Jita prices
    3. Market snapshot
    4. Contracts
    5. Fights (7 days)
    6. Opportunity scoring
    """
    results = {}

    logger.info("Starting full data refresh...")

    # Market
    results["market"] = await market_service.fetch_structure_orders(db)
    results["jita_prices"] = await market_service.fetch_jita_prices(db)
    results["snapshot"] = await market_service.take_snapshot(db)

    # Contracts
    results["contracts"] = await contract_service.fetch_contracts(db)

    # Fights
    results["fights"] = await zkill_service.fetch_recent_losses(db, days=7)

    # Score
    results["opportunities"] = await scoring_engine.calculate_all(db)

    logger.info("Full data refresh complete")
    return results


# ─── Manual Contracts ───────────────────────────────────────


manual_contract_router = APIRouter(prefix="/manual-contracts", tags=["manual-contracts"])


@manual_contract_router.get("")
async def list_manual_contracts(db: AsyncSession = Depends(get_db)):
    """List all manual contract entries."""
    result = await db.execute(
        select(ManualContract).order_by(ManualContract.added_at.desc())
    )
    contracts = result.scalars().all()

    output = []
    for c in contracts:
        ship = await db.get(SdeType, c.ship_type_id)
        doctrine = await db.get(Doctrine, c.doctrine_id) if c.doctrine_id else None
        output.append({
            "id": c.id,
            "doctrine_id": c.doctrine_id,
            "doctrine_name": doctrine.name if doctrine else None,
            "ship_type_id": c.ship_type_id,
            "ship_name": ship.name if ship else f"Type {c.ship_type_id}",
            "quantity": c.quantity,
            "price": c.price,
            "location": c.location,
            "notes": c.notes,
            "added_at": c.added_at.isoformat() if c.added_at else None,
        })
    return output


@manual_contract_router.post("")
async def add_manual_contract(
    ship_type_id: int = Form(...),
    quantity: int = Form(1),
    price: float = Form(0),
    doctrine_id: int = Form(None),
    location: str = Form(""),
    notes: str = Form(""),
    db: AsyncSession = Depends(get_db),
):
    """Add a manual contract entry."""
    contract = ManualContract(
        doctrine_id=doctrine_id if doctrine_id else None,
        ship_type_id=ship_type_id,
        quantity=quantity,
        price=price,
        location=location if location else None,
        notes=notes if notes else None,
    )
    db.add(contract)
    await db.commit()

    ship = await db.get(SdeType, ship_type_id)
    return {
        "id": contract.id,
        "ship_name": ship.name if ship else f"Type {ship_type_id}",
        "quantity": quantity,
    }


@manual_contract_router.delete("/{contract_id}")
async def delete_manual_contract(contract_id: int, db: AsyncSession = Depends(get_db)):
    """Delete a manual contract entry."""
    contract = await db.get(ManualContract, contract_id)
    if not contract:
        raise HTTPException(404, "Contract not found")
    await db.delete(contract)
    await db.commit()
    return {"deleted": True}


@manual_contract_router.post("/clear")
async def clear_manual_contracts(db: AsyncSession = Depends(get_db)):
    """Clear all manual contract entries (for re-entry)."""
    await db.execute(delete(ManualContract))
    await db.commit()
    return {"cleared": True}


@manual_contract_router.get("/summary")
async def manual_contract_summary(db: AsyncSession = Depends(get_db)):
    """Get ship counts from manual contracts for scoring."""
    from sqlalchemy import select, func, delete
    result = await db.execute(
        select(
            ManualContract.ship_type_id,
            func.sum(ManualContract.quantity).label("total"),
        )
        .group_by(ManualContract.ship_type_id)
    )
    return {row[0]: row[1] for row in result.fetchall()}


@manual_contract_router.get("/doctrine-ships")
async def get_doctrine_ships(db: AsyncSession = Depends(get_db)):
    """Get all ships across all doctrines for the dropdown."""
    result = await db.execute(
        select(DoctrineFit.ship_type_id, DoctrineFit.name, DoctrineFit.doctrine_id)
        .distinct(DoctrineFit.ship_type_id)
    )
    fits = result.fetchall()

    output = []
    seen = set()
    for fit in fits:
        if fit[0] not in seen:
            ship = await db.get(SdeType, fit[0])
            output.append({
                "type_id": fit[0],
                "name": ship.name if ship else f"Type {fit[0]}",
            })
            seen.add(fit[0])
    return sorted(output, key=lambda x: x["name"])


# ─── Market Analysis (Goonmetrics-style, cached) ────────────

import asyncio

analysis_router = APIRouter(prefix="/analysis", tags=["analysis"])

# Background task status
_refresh_status = {"running": False, "progress": "", "result": None, "error": None}


@analysis_router.get("/doctrine")
async def get_doctrine_analysis(db: AsyncSession = Depends(get_db)):
    """Load cached doctrine analysis. Instant, no external API calls."""
    return await load_cached_analysis(db)


@analysis_router.post("/refresh")
async def start_refresh(background_tasks: BackgroundTasks):
    """Start analysis refresh as background task. Returns immediately."""
    if _refresh_status["running"]:
        return {"status": "already_running", "progress": _refresh_status["progress"]}

    _refresh_status["running"] = True
    _refresh_status["progress"] = "Starting..."
    _refresh_status["result"] = None
    _refresh_status["error"] = None

    async def _run():
        try:
            result = await refresh_analysis()
            _refresh_status["result"] = result
            _refresh_status["progress"] = "Done"
        except Exception as e:
            _refresh_status["error"] = str(e)
            _refresh_status["progress"] = f"Error: {e}"
        finally:
            _refresh_status["running"] = False

    asyncio.create_task(_run())
    return {"status": "started"}


@analysis_router.get("/refresh/status")
async def get_refresh_status():
    """Poll this to check if background refresh is done."""
    _refresh_status["progress"] = get_progress() if _refresh_status["running"] else _refresh_status.get("progress", "")
    return _refresh_status


@analysis_router.get("/age")
async def get_analysis_age(db: AsyncSession = Depends(get_db)):
    """Get the timestamp of the last analysis refresh."""
    age = await get_cache_age(db)
    return {"last_updated": age}



# ─── Market Browser ─────────────────────────────────────────


browser_router = APIRouter(prefix="/browser", tags=["browser"])


@browser_router.get("/tree")
async def get_market_tree(db: AsyncSession = Depends(get_db)):
    """Get market group tree structure with sample type icons."""
    result = await db.execute(
        select(SdeMarketGroup).order_by(SdeMarketGroup.name)
    )
    groups = result.scalars().all()

    # Get one sample type_id per market group for icons
    sample_result = await db.execute(
        select(SdeType.market_group_id, func.min(SdeType.type_id))
        .where(SdeType.published == True)
        .where(SdeType.market_group_id.isnot(None))
        .group_by(SdeType.market_group_id)
    )
    samples = {row[0]: row[1] for row in sample_result.fetchall()}

    # Also get samples from ALL types (including unpublished) for groups with no published items
    unpub_result = await db.execute(
        select(SdeType.market_group_id, func.min(SdeType.type_id))
        .where(SdeType.market_group_id.isnot(None))
        .where(SdeType.market_group_id.notin_(list(samples.keys())) if samples else SdeType.market_group_id.isnot(None))
        .group_by(SdeType.market_group_id)
    )
    for row in unpub_result.fetchall():
        if row[0] not in samples:
            samples[row[0]] = row[1]

    # Build child map to propagate icons upward to parent groups
    child_map = {}
    group_map = {}
    for g in groups:
        group_map[g.market_group_id] = g
        if g.parent_group_id:
            child_map.setdefault(g.parent_group_id, []).append(g.market_group_id)

    def get_icon(gid, visited=None):
        """Get sample type for a group, propagating from children if needed."""
        if visited is None:
            visited = set()
        if gid in visited:
            return None
        visited.add(gid)
        if gid in samples:
            return samples[gid]
        for child_id in child_map.get(gid, []):
            icon = get_icon(child_id, visited)
            if icon:
                return icon
        return None

    # Fallback icons by group name for groups that still don't resolve
    NAME_FALLBACKS = {
        "blueprints & reactions": 688,       # Slasher Blueprint
        "ship skins": 34567,                 # Generic SKIN type
        "special edition assets": 33103,     # Genolution Core Augmentation
        "trade goods": 3814,                 # Brent (Transcranial Microcontrollers)
        "apparel": 3313,                     # Cybernetic Subprocessor
        "personalization": 3313,
        "pilot's services": 40519,           # Pilot License Extension
    }

    output = []
    for g in groups:
        icon = get_icon(g.market_group_id)
        if icon is None:
            icon = NAME_FALLBACKS.get(g.name.lower())
        output.append({
            "id": g.market_group_id,
            "name": g.name,
            "parent_id": g.parent_group_id,
            "icon_type_id": icon,
        })

    return output


@browser_router.get("/group/{group_id}/items")
async def get_group_items(
    group_id: int,
    structure_id: int = None,
    db: AsyncSession = Depends(get_db),
):
    """Get all items in a market group with local stock and Jita price.
    If the group has no direct items, returns items from child groups.
    structure_id defaults to the main Keepstar if not provided."""
    loc_id = structure_id or settings.keepstar_structure_id

    # First try direct items in this group
    items_result = await db.execute(
        select(SdeType)
        .where(SdeType.market_group_id == group_id)
        .where(SdeType.published == True)
        .order_by(SdeType.name)
    )
    items = items_result.scalars().all()

    # If no direct items, collect from all descendant leaf groups
    if not items:
        # Get all descendant group IDs
        all_groups = await db.execute(select(SdeMarketGroup))
        gmap = {}
        for g in all_groups.scalars().all():
            gmap.setdefault(g.parent_group_id, []).append(g.market_group_id)

        descendant_ids = []
        queue = [group_id]
        while queue:
            gid = queue.pop()
            for child_id in gmap.get(gid, []):
                descendant_ids.append(child_id)
                queue.append(child_id)

        if descendant_ids:
            items_result = await db.execute(
                select(SdeType)
                .where(SdeType.market_group_id.in_(descendant_ids))
                .where(SdeType.published == True)
                .order_by(SdeType.name)
                .limit(200)
            )
            items = items_result.scalars().all()

    # Get local stock for these items
    type_ids = [i.type_id for i in items]
    if not type_ids:
        return []

    stock_result = await db.execute(
        select(
            MarketOrder.type_id,
            func.min(MarketOrder.price).label("sell_min"),
            func.sum(MarketOrder.volume_remain).label("sell_vol"),
        )
        .where(MarketOrder.location_id == loc_id)
        .where(MarketOrder.is_buy_order == False)
        .where(MarketOrder.type_id.in_(type_ids))
        .group_by(MarketOrder.type_id)
    )
    stock = {row[0]: {"price": row[1], "volume": row[2]} for row in stock_result.fetchall()}

    # Get cached Jita prices
    jita_result = await db.execute(
        select(JitaPrice).where(JitaPrice.type_id.in_(type_ids))
    )
    jita_map = {j.type_id: j for j in jita_result.scalars().all()}

    output = []
    for item in items:
        local = stock.get(item.type_id, {})
        jita = jita_map.get(item.type_id)
        local_price = local.get("price")
        jita_sell = jita.sell_min if jita else None
        import_cost = None
        markup = None
        ship_vol = item.packaged_volume or item.volume or 0
        if jita_sell and ship_vol:
            import_cost = jita_sell + (ship_vol * settings.freight_cost_per_m3)
        if local_price and import_cost and import_cost > 0:
            markup = ((local_price - import_cost) / import_cost) * 100

        output.append({
            "type_id": item.type_id,
            "name": item.name,
            "description": (item.description or "")[:300],
            "local_price": local_price,
            "local_stock": local.get("volume", 0),
            "jita_sell": jita_sell,
            "import_cost": round(import_cost, 2) if import_cost else None,
            "markup_pct": round(markup, 1) if markup is not None else None,
        })

    return output


@browser_router.get("/item/{type_id}")
async def get_item_detail(
    type_id: int,
    structure_id: int = None,
    db: AsyncSession = Depends(get_db),
):
    """Full item detail with local orders, Jita comparison, and build cost."""
    loc_id = structure_id or settings.keepstar_structure_id

    item = await db.get(SdeType, type_id)
    if not item:
        raise HTTPException(404, "Item not found")

    # Local sell orders
    sells_result = await db.execute(
        select(MarketOrder)
        .where(MarketOrder.type_id == type_id)
        .where(MarketOrder.location_id == loc_id)
        .where(MarketOrder.is_buy_order == False)
        .order_by(MarketOrder.price.asc())
        .limit(50)
    )
    sells = sells_result.scalars().all()

    # Local buy orders
    buys_result = await db.execute(
        select(MarketOrder)
        .where(MarketOrder.type_id == type_id)
        .where(MarketOrder.location_id == loc_id)
        .where(MarketOrder.is_buy_order == True)
        .order_by(MarketOrder.price.desc())
        .limit(50)
    )
    buys = buys_result.scalars().all()

    # Cached Jita price
    jita = await db.get(JitaPrice, type_id)
    jita_sell = jita.sell_min if jita else None
    jita_buy = jita.buy_max if jita else None

    import_cost = None
    ship_vol = item.packaged_volume or item.volume or 0
    if jita_sell and ship_vol:
        import_cost = jita_sell + (ship_vol * settings.freight_cost_per_m3)

    local_sell_min = sells[0].price if sells else None
    local_sell_vol = sum(o.volume_remain for o in sells)
    local_buy_max = buys[0].price if buys else None
    local_buy_vol = sum(o.volume_remain for o in buys)
    markup = None
    if local_sell_min and import_cost and import_cost > 0:
        markup = ((local_sell_min - import_cost) / import_cost) * 100

    # Staging Keepstar stock — auto-detect from active deployment
    staging_sell_vol = 0
    staging_sell_min = None
    staging_structure_id = None
    staging_structure_name = None
    try:
        from app.models.killmail_session import killmail_session as ks
        from app.models.killmail_models import Deployment as KDep
        async with ks() as kdb:
            dep_result = await kdb.execute(
                select(KDep).where(KDep.status == "active").where(KDep.staging_structure_id.isnot(None)).limit(1)
            )
            active_dep = dep_result.scalar_one_or_none()
            if active_dep:
                staging_structure_id = active_dep.staging_structure_id
                staging_structure_name = active_dep.staging_structure_name

        if staging_structure_id:
            staging_sells_result = await db.execute(
                select(MarketOrder)
                .where(MarketOrder.type_id == type_id)
                .where(MarketOrder.location_id == staging_structure_id)
                .where(MarketOrder.is_buy_order == False)
                .order_by(MarketOrder.price.asc())
            )
            staging_sells = staging_sells_result.scalars().all()
            staging_sell_vol = sum(o.volume_remain for o in staging_sells)
            staging_sell_min = staging_sells[0].price if staging_sells else None
    except Exception:
        pass  # Staging lookup is best-effort

    # Build cost from blueprints
    from app.services.market_service import market_service
    build_cost = await market_service.estimate_build_cost(db, type_id)

    # Volume & trend data from price history
    from datetime import datetime, timezone, timedelta
    history_result = await db.execute(
        select(MarketHistory)
        .where(MarketHistory.type_id == type_id)
        .order_by(MarketHistory.date.desc())
        .limit(60)
    )
    history = history_result.scalars().all()

    jita_avg_daily_vol_7d = 0
    jita_avg_daily_vol_30d = 0
    price_change_7d = None
    price_change_30d = None

    if history:
        # Daily volumes
        vols_7d = [h.volume for h in history[:7] if h.volume]
        vols_30d = [h.volume for h in history[:30] if h.volume]
        jita_avg_daily_vol_7d = round(sum(vols_7d) / max(len(vols_7d), 1))
        jita_avg_daily_vol_30d = round(sum(vols_30d) / max(len(vols_30d), 1))

        # Price trends - compare most recent avg to 7d/30d ago
        latest_price = history[0].average if history[0] else None
        price_7d_ago = history[6].average if len(history) > 6 else None
        price_30d_ago = history[29].average if len(history) > 29 else None

        if latest_price and price_7d_ago and price_7d_ago > 0:
            price_change_7d = round(((latest_price - price_7d_ago) / price_7d_ago) * 100, 2)
        if latest_price and price_30d_ago and price_30d_ago > 0:
            price_change_30d = round(((latest_price - price_30d_ago) / price_30d_ago) * 100, 2)

    # Days of stock calculations
    jita_sell_vol = jita.sell_volume if jita else 0
    jita_days_of_stock = None
    if jita_sell_vol and jita_avg_daily_vol_7d > 0:
        jita_days_of_stock = round(jita_sell_vol / jita_avg_daily_vol_7d, 2)

    local_days_of_stock = None
    local_velocity = await market_service.calculate_velocity(db, type_id)
    if local_sell_vol and local_velocity > 0:
        local_days_of_stock = round(local_sell_vol / local_velocity, 2)

    # Market group breadcrumb
    breadcrumb = []
    mg_id = item.market_group_id
    visited = set()
    while mg_id and mg_id not in visited:
        visited.add(mg_id)
        mg = await db.get(SdeMarketGroup, mg_id)
        if mg:
            breadcrumb.insert(0, {"id": mg.market_group_id, "name": mg.name})
            mg_id = mg.parent_group_id
        else:
            break

    return {
        "type_id": type_id,
        "name": item.name,
        "group_id": item.group_id,
        "market_group_id": item.market_group_id,
        "breadcrumb": breadcrumb,
        "volume_m3": item.packaged_volume or item.volume,
        "jita_sell": jita_sell,
        "jita_buy": jita_buy,
        "jita_sell_vol": jita_sell_vol,
        "jita_buy_vol": jita.buy_volume if jita else 0,
        "import_cost": round(import_cost, 2) if import_cost else None,
        "markup_pct": round(markup, 1) if markup is not None else None,
        "target_20pct": round(import_cost * 1.2, 2) if import_cost else None,
        "freight_rate": settings.freight_cost_per_m3,
        "build_cost": round(build_cost, 2) if build_cost else None,
        "local_sell_min": local_sell_min,
        "local_sell_vol": local_sell_vol,
        "local_buy_max": local_buy_max,
        "local_buy_vol": local_buy_vol,
        # Staging Keepstar stock
        "staging_structure_id": staging_structure_id,
        "staging_structure_name": staging_structure_name,
        "staging_sell_vol": staging_sell_vol,
        "staging_sell_min": staging_sell_min,
        # Volume & trend intelligence
        "jita_avg_daily_vol_7d": jita_avg_daily_vol_7d,
        "jita_avg_daily_vol_30d": jita_avg_daily_vol_30d,
        "jita_days_of_stock": jita_days_of_stock,
        "local_velocity": round(local_velocity, 1) if local_velocity else 0,
        "local_days_of_stock": local_days_of_stock,
        "price_change_7d": price_change_7d,
        "price_change_30d": price_change_30d,
        "sell_orders": [{
            "price": o.price,
            "volume": o.volume_remain,
            "duration": o.duration,
            "issued": o.issued.isoformat() if o.issued else None,
        } for o in sells],
        "buy_orders": [{
            "price": o.price,
            "volume": o.volume_remain,
            "duration": o.duration,
            "issued": o.issued.isoformat() if o.issued else None,
        } for o in buys],
    }


@browser_router.get("/search")
async def search_items(
    q: str = Query(..., min_length=2),
    db: AsyncSession = Depends(get_db),
):
    """Search for items by name."""
    result = await db.execute(
        select(SdeType.type_id, SdeType.name, SdeType.market_group_id)
        .where(SdeType.published == True)
        .where(SdeType.market_group_id.isnot(None))
        .where(SdeType.name.ilike(f"%{q}%"))
        .order_by(SdeType.name)
        .limit(30)
    )
    return [{"type_id": r[0], "name": r[1], "market_group_id": r[2]} for r in result.fetchall()]


@browser_router.get("/market-intelligence")
async def get_market_intelligence(db: AsyncSession = Depends(get_db)):
    """
    Market intelligence dashboard with Jita/C-J6MT toggle for every category.
    Jita data from market_history, C-J6MT data from market_snapshots.
    """
    from datetime import datetime, timezone, timedelta
    from sqlalchemy import text as sql_text

    # ── Jita: price history from ESI (market_history table) ──
    history_result = await db.execute(sql_text("""
        SELECT h.type_id, t.name, h.date, h.average, h.volume
        FROM market_history h
        JOIN sde_types t ON t.type_id = h.type_id AND t.published = 1
        WHERE h.date >= date('now', '-35 days')
        ORDER BY h.type_id, h.date DESC
    """))
    jita_history = {}
    for row in history_result.fetchall():
        tid = row[0]
        if tid not in jita_history:
            jita_history[tid] = {"name": row[1], "rows": []}
        jita_history[tid]["rows"].append({"avg": row[3], "vol": row[4]})

    # ── C-J6MT: aggregated from snapshots (efficient — no bulk load) ──
    # Latest snapshot per type
    cj_latest_result = await db.execute(sql_text("""
        SELECT s.type_id, s.sell_min_price, s.sell_volume
        FROM market_snapshots s
        INNER JOIN (SELECT type_id, MAX(timestamp) as max_ts FROM market_snapshots GROUP BY type_id) g
        ON s.type_id = g.type_id AND s.timestamp = g.max_ts
    """))
    cj_latest = {r[0]: {"price": r[1], "vol": r[2]} for r in cj_latest_result.fetchall()}

    # Adaptive timeframe: use whatever snapshot history we have
    # Find the actual oldest snapshot date to determine available range
    oldest_snap = await db.execute(sql_text(
        "SELECT MIN(timestamp), MAX(timestamp) FROM market_snapshots"
    ))
    snap_range = oldest_snap.fetchone()
    snap_oldest = snap_range[0] if snap_range else None
    snap_newest = snap_range[1] if snap_range else None

    # Calculate available days of snapshot data
    snap_days_available = 0
    if snap_oldest and snap_newest:
        from datetime import datetime as dt_cls
        try:
            t_old = dt_cls.fromisoformat(snap_oldest) if isinstance(snap_oldest, str) else snap_oldest
            t_new = dt_cls.fromisoformat(snap_newest) if isinstance(snap_newest, str) else snap_newest
            snap_days_available = (t_new - t_old).total_seconds() / 86400
        except Exception:
            snap_days_available = 0

    # Use adaptive comparison window: 7d if available, otherwise half the available span (min 1d)
    cj_compare_days = min(7, max(1, int(snap_days_available * 0.6)))
    cj_long_days = min(30, max(2, int(snap_days_available * 0.9)))

    # Snapshot closest to comparison window ago per type
    cj_7d_result = await db.execute(sql_text(f"""
        SELECT s.type_id, s.sell_min_price, s.sell_volume
        FROM market_snapshots s
        INNER JOIN (SELECT type_id, MAX(timestamp) as max_ts FROM market_snapshots WHERE timestamp <= datetime('now', '-{cj_compare_days} days') GROUP BY type_id) g
        ON s.type_id = g.type_id AND s.timestamp = g.max_ts
    """))
    cj_7d = {r[0]: {"price": r[1], "vol": r[2]} for r in cj_7d_result.fetchall()}

    # Snapshot closest to long-term window ago per type
    cj_30d_result = await db.execute(sql_text(f"""
        SELECT s.type_id, s.sell_min_price, s.sell_volume
        FROM market_snapshots s
        INNER JOIN (SELECT type_id, MAX(timestamp) as max_ts FROM market_snapshots WHERE timestamp <= datetime('now', '-{cj_long_days} days') GROUP BY type_id) g
        ON s.type_id = g.type_id AND s.timestamp = g.max_ts
    """))
    cj_30d = {r[0]: {"price": r[1], "vol": r[2]} for r in cj_30d_result.fetchall()}

    # C-J6MT daily velocity estimate from snapshots (volume decrease over time span)
    # Require at least 1 day of data to avoid noise from short time spans
    cj_velocity_result = await db.execute(sql_text("""
        SELECT type_id,
               SUM(CASE WHEN vol_delta > 0 THEN vol_delta ELSE 0 END) as total_sold,
               (julianday(MAX(ts)) - julianday(MIN(ts))) as days_span
        FROM (
            SELECT type_id, timestamp as ts,
                   LAG(sell_volume) OVER (PARTITION BY type_id ORDER BY timestamp) - sell_volume as vol_delta
            FROM market_snapshots
            WHERE timestamp >= datetime('now', '-7 days')
        )
        GROUP BY type_id
        HAVING days_span >= 1.0
    """))
    cj_velocity = {r[0]: r[1] / r[2] if r[2] and r[2] >= 1.0 else 0 for r in cj_velocity_result.fetchall()}

    # ── Current market state ──
    # Keepstar stock + prices
    stock_result = await db.execute(
        select(MarketOrder.type_id, func.sum(MarketOrder.volume_remain), func.min(MarketOrder.price))
        .where(MarketOrder.location_id == settings.keepstar_structure_id)
        .where(MarketOrder.is_buy_order == False)
        .group_by(MarketOrder.type_id)
    )
    local_stock = {}
    local_prices = {}
    for r in stock_result.fetchall():
        local_stock[r[0]] = r[1]
        local_prices[r[0]] = r[2]

    # Jita cached prices
    jita_result = await db.execute(select(JitaPrice))
    jita_map = {j.type_id: j for j in jita_result.scalars().all()}

    # Item volumes for freight
    vol_result = await db.execute(
        select(SdeType.type_id, SdeType.volume, SdeType.name, SdeType.market_group_id)
        .where(SdeType.published == True)
        .where(SdeType.volume.isnot(None))
    )
    item_vols = {}
    item_names = {}
    item_market_groups = {}
    for r in vol_result.fetchall():
        item_vols[r[0]] = r[1]
        item_names[r[0]] = r[2]
        item_market_groups[r[0]] = r[3]

    # ── Exclusion filter from config ──
    import json
    excl_setting = await db.get(AppSetting, "mi_excluded_groups")
    excluded_ids = set(json.loads(excl_setting.value)) if excl_setting and excl_setting.value else set()

    # Load all market groups for tree walking + category mapping
    all_mg = await db.execute(select(SdeMarketGroup))
    mg_parent = {}
    mg_names = {}
    mg_children = {}
    for mg in all_mg.scalars().all():
        mg_parent[mg.market_group_id] = mg.parent_group_id
        mg_names[mg.market_group_id] = mg.name
        if mg.parent_group_id is not None:
            mg_children.setdefault(mg.parent_group_id, []).append(mg.market_group_id)

    # Walk tree: if a parent is excluded, all descendants are too
    excluded_mg_ids = set()
    if excluded_ids:
        queue = list(excluded_ids)
        while queue:
            gid = queue.pop()
            excluded_mg_ids.add(gid)
            for child in mg_children.get(gid, []):
                if child not in excluded_mg_ids:
                    queue.append(child)

    # Build set of excluded type_ids
    excluded_types = set()
    if excluded_mg_ids:
        for tid, mgid in item_market_groups.items():
            if mgid in excluded_mg_ids:
                excluded_types.add(tid)

    # ── Category mapping ──
    # Map top-level market group names → MI categories
    CATEGORY_BY_ROOT = {
        "Ships": "ships",
        "Ship Equipment": "modules",
        "Ship and Module Modifications": "modules",
        "Drones": "modules",
        "Ammunition & Charges": "ammo",
        "Manufacture & Research": "industry",
        "Blueprints & Reactions": "industry",
        "Planetary Infrastructure": "industry",
        "Trade Goods": "industry",
        "Structures": "industry",
        "Structure Equipment": "industry",
        "Structure Modifications": "industry",
        "Implants & Boosters": "implants",
    }

    # Find root parent for each market group
    def get_root_name(mgid):
        visited = set()
        while mgid and mgid not in visited:
            visited.add(mgid)
            parent = mg_parent.get(mgid)
            if parent is None:
                return mg_names.get(mgid, "")
            mgid = parent
        return mg_names.get(mgid, "")

    # Build type_id → category mapping
    mg_category_cache = {}
    def get_category(tid):
        mgid = item_market_groups.get(tid)
        if not mgid:
            return "other"
        if mgid not in mg_category_cache:
            root = get_root_name(mgid)
            mg_category_cache[mgid] = CATEGORY_BY_ROOT.get(root, "other")
        return mg_category_cache[mgid]

    # ── Helper: compute metrics for a market ──
    def compute_metrics(tid, name, price_rows, current_stock, current_price, daily_vol_override=None):
        """Compute trending, supply, volume metrics from price history rows."""
        if not price_rows or len(price_rows) < 2:
            return None

        latest_price = current_price or (price_rows[0].get("avg") or price_rows[0].get("price"))
        if not latest_price or latest_price <= 0:
            return None

        # Daily volume
        vols = [r.get("vol", 0) or 0 for r in price_rows[:7]]
        avg_daily_vol = daily_vol_override if daily_vol_override is not None else (sum(vols) / max(len(vols), 1))
        if avg_daily_vol < 1:
            avg_daily_vol = 0

        # Price 7d ago
        price_7d = None
        if len(price_rows) >= 7:
            p = price_rows[6].get("avg") or price_rows[6].get("price")
            if p and p > 0:
                price_7d = p

        # Price 30d ago
        price_30d = None
        if len(price_rows) >= 30:
            p = price_rows[29].get("avg") or price_rows[29].get("price")
            if p and p > 0:
                price_30d = p

        change_7d = ((latest_price - price_7d) / price_7d * 100) if price_7d and price_7d > 0 else None
        change_30d = ((latest_price - price_30d) / price_30d * 100) if price_30d and price_30d > 0 else None

        days_of_stock = (current_stock / avg_daily_vol) if current_stock and avg_daily_vol > 0 else None
        daily_isk = avg_daily_vol * latest_price

        return {
            "type_id": tid,
            "name": name,
            "price": round(latest_price, 2),
            "change_7d": round(change_7d, 1) if change_7d is not None else None,
            "change_30d": round(change_30d, 1) if change_30d is not None else None,
            "avg_daily_vol": round(avg_daily_vol),
            "daily_isk_vol": round(daily_isk, 2),
            "stock": current_stock or 0,
            "days_of_stock": round(days_of_stock, 1) if days_of_stock is not None else None,
        }

    # ── Compute for both markets ──
    results = {"jita": [], "cj": []}
    all_tids = set(jita_history.keys()) | set(local_stock.keys()) | set(cj_latest.keys())

    for tid in all_tids:
        if tid in excluded_types:
            continue
        name = item_names.get(tid) or jita_history.get(tid, {}).get("name", f"Type {tid}")

        # Jita metrics
        jh = jita_history.get(tid)
        jita = jita_map.get(tid)
        jita_stock = jita.sell_volume if jita else 0
        jita_price = jita.sell_min if jita else None
        cat = get_category(tid)
        if jh:
            jm = compute_metrics(tid, name, jh["rows"], jita_stock, jita_price)
            if jm and jm["avg_daily_vol"] >= 5:
                jm["cat"] = cat
                results["jita"].append(jm)

        # C-J6MT metrics — computed from pre-aggregated snapshot data
        cj_stock = local_stock.get(tid, 0)
        cj_price = local_prices.get(tid)
        cj_now = cj_latest.get(tid)
        cj_7 = cj_7d.get(tid)
        cj_30 = cj_30d.get(tid)
        cj_vel = cj_velocity.get(tid, 0)

        if cj_now and (cj_price or cj_now.get("price")):
            cur_price = cj_price or cj_now["price"]
            change_7d = None
            change_30d = None
            if cj_7 and cj_7["price"] and cj_7["price"] > 0:
                change_7d = ((cur_price - cj_7["price"]) / cj_7["price"]) * 100
            if cj_30 and cj_30["price"] and cj_30["price"] > 0:
                change_30d = ((cur_price - cj_30["price"]) / cj_30["price"]) * 100

            days_of_stock = (cj_stock / cj_vel) if cj_stock and cj_vel > 0 else None
            daily_isk = cj_vel * cur_price if cj_vel else 0

            results["cj"].append({
                "type_id": tid, "name": name, "cat": cat,
                "price": round(cur_price, 2),
                "change_7d": round(change_7d, 1) if change_7d is not None else None,
                "change_30d": round(change_30d, 1) if change_30d is not None else None,
                "avg_daily_vol": round(cj_vel),
                "daily_isk_vol": round(daily_isk, 2),
                "stock": cj_stock,
                "days_of_stock": round(days_of_stock, 1) if days_of_stock is not None else None,
            })
        elif cj_stock > 0 and cj_price:
            results["cj"].append({
                "type_id": tid, "name": name, "cat": cat, "price": round(cj_price, 2),
                "change_7d": None, "change_30d": None,
                "avg_daily_vol": 0, "daily_isk_vol": 0,
                "stock": cj_stock, "days_of_stock": None,
            })

    # ── Sort into categories for each market ──
    CATS = ['all', 'ships', 'modules', 'ammo', 'industry', 'implants']

    def categorize(entries):
        # Filter: require meaningful velocity (>= 5 units/day) for trending/volume
        # Require reasonable days_of_stock (< 10000) for supply rankings
        has_vol = [e for e in entries if e["avg_daily_vol"] >= 5]
        has_stock = [e for e in entries if e["days_of_stock"] is not None and e["stock"] > 0
                     and e["avg_daily_vol"] >= 3 and e["days_of_stock"] < 10000]

        def top15(items, key, reverse=True):
            """Return top 15 per category."""
            result = {}
            for cat in CATS:
                filtered = items if cat == 'all' else [e for e in items if e.get("cat") == cat]
                result[cat] = sorted(filtered, key=key, reverse=reverse)[:15]
            return result

        return {
            "trending_up": top15([e for e in has_vol if (e["change_7d"] or 0) > 3], key=lambda x: x["change_7d"] or 0),
            "trending_down": top15([e for e in has_vol if (e["change_7d"] or 0) < -3], key=lambda x: x["change_7d"] or 0, reverse=False),
            "short_supply": top15(has_stock, key=lambda x: -(x["days_of_stock"] or 999)),
            "over_supply": top15(has_stock, key=lambda x: x["days_of_stock"] or 0),
            "volume_leaders": top15([e for e in has_vol if e["daily_isk_vol"] > 0], key=lambda x: x["daily_isk_vol"]),
        }

    return {
        "jita": categorize(results["jita"]),
        "cj": categorize(results["cj"]),
        "snapshot_days": round(snap_days_available, 1),
        "cj_compare_days": cj_compare_days,
        "what_to_do": await _compute_what_to_do(db, local_stock, local_prices, jita_map, item_vols, item_names, cj_velocity, excluded_types, settings),
        "restock_urgency": _compute_restock_urgency(local_stock, local_prices, jita_map, item_vols, item_names, cj_velocity, excluded_types, settings),
        "undercut_status": await _compute_undercut_status(db, item_names),
        "margin_alerts": _compute_margin_alerts(results["cj"], jita_map, item_vols, settings),
    }


async def _compute_what_to_do(db, local_stock, local_prices, jita_map, item_vols, item_names, cj_velocity, excluded_types, settings):
    """
    The money table: items that WILL sell with decent margins.
    Each item tagged with WHY it's here: LOW STOCK, DOCTRINE, HIGH MARGIN, NOT SEEDED, FAST MOVER.
    Sorted by daily profit potential.
    """
    from app.models.database import FreightRoute, GoonmetricsCache, DoctrineFitItem, FightLoss, Fight
    from sqlalchemy import text as sql_text

    fr_result = await db.execute(select(FreightRoute.rate_per_m3).limit(1))
    freight_rate = fr_result.scalar() or settings.freight_cost_per_m3

    # Doctrine items — items in doctrine fits that were lost in recent fights
    doctrine_types = set()
    doctrine_result = await db.execute(select(DoctrineFitItem.type_id).distinct())
    for r in doctrine_result.fetchall():
        doctrine_types.add(r[0])

    # Recent fight losses — ship hulls lost
    fight_ships = {}
    fight_result = await db.execute(sql_text("""
        SELECT fl.ship_type_id, COUNT(*) as cnt
        FROM fight_losses fl JOIN fights f ON fl.fight_id = f.id
        WHERE f.started_at >= datetime('now', '-7 days')
        GROUP BY fl.ship_type_id HAVING cnt >= 3
    """))
    for tid, cnt in fight_result.fetchall():
        fight_ships[tid] = cnt

    items = []
    for tid, jita in jita_map.items():
        if tid in excluded_types:
            continue
        if not jita or not jita.sell_min or jita.sell_min <= 0:
            continue

        name = item_names.get(tid)
        if not name:
            continue

        vol_m3 = item_vols.get(tid, 0)
        if vol_m3 <= 0:
            continue

        import_cost = jita.sell_min + (vol_m3 * freight_rate)
        local_price = local_prices.get(tid)
        stock = local_stock.get(tid, 0)

        # Velocity from snapshots or Goonmetrics
        velocity = cj_velocity.get(tid, 0)
        gm = await db.get(GoonmetricsCache, tid)
        gm_vel = (gm.weekly_movement / 7) if gm and gm.weekly_movement else 0
        best_vel = max(velocity, gm_vel)

        # Skip bulk commodities
        if best_vel * vol_m3 > 100000:
            continue

        # Determine tag (WHY this item is here)
        tag = None

        # NOT SEEDED — zero local stock, proven demand
        if stock == 0 and best_vel >= 1:
            if not local_price:
                local_price = import_cost * 1.20  # Assume 20% markup for unseeded
            margin_pct = ((local_price - import_cost) / import_cost) * 100 if import_cost > 0 else 0
            if margin_pct >= 10:
                tag = "NOT SEEDED"

        # DOCTRINE — lost in recent fights
        elif tid in fight_ships:
            if not local_price or local_price <= 0:
                local_price = import_cost * 1.15
            margin_pct = ((local_price - import_cost) / import_cost) * 100 if import_cost > 0 else 0
            tag = "DOCTRINE"

        # Need local price and margin for the rest
        elif local_price and local_price > 0:
            margin_pct = ((local_price - import_cost) / import_cost) * 100 if import_cost > 0 else 0

            if margin_pct < 15:
                continue  # Skip low margin for the money table

            if best_vel < 0.5:
                continue  # Must actually sell

            days_of_stock = (stock / best_vel) if stock and best_vel > 0 else 0

            if days_of_stock <= 3:
                tag = "LOW STOCK"
            elif margin_pct >= 40:
                tag = "HIGH MARGIN"
            elif best_vel >= 10:
                tag = "FAST MOVER"
            elif margin_pct >= 20:
                tag = "HIGH MARGIN"
            else:
                continue  # Doesn't qualify
        else:
            continue

        if not tag:
            continue

        if not local_price or local_price <= 0:
            continue

        margin_pct_final = ((local_price - import_cost) / import_cost) * 100 if import_cost > 0 else 0
        days_of_stock = (stock / best_vel) if stock and best_vel > 0 else 0
        daily_profit = margin_pct_final / 100 * import_cost * best_vel

        items.append({
            "type_id": tid,
            "name": name,
            "jita_sell": round(jita.sell_min, 2),
            "local_price": round(local_price, 2),
            "margin_pct": round(margin_pct_final, 1),
            "velocity": round(best_vel, 1),
            "stock": stock,
            "days_of_stock": round(days_of_stock, 1),
            "tag": tag,
            "score": round(daily_profit, 0),
        })

    return sorted(items, key=lambda x: -x["score"])[:25]


async def _compute_undercut_status(db, item_names):
    """
    Undercut timeline: for each active sell order, check snapshot history
    to see how long it's been undercut.
    """
    from app.models.database import CharacterOrder, MarketSnapshot
    from sqlalchemy import text as sql_text

    result = await db.execute(
        select(CharacterOrder)
        .where(CharacterOrder.is_buy_order == False)
        .where(CharacterOrder.is_undercut == True)
    )
    undercut_orders = result.scalars().all()

    items = []
    for order in undercut_orders:
        name = item_names.get(order.type_id, f"Type {order.type_id}")

        # Check snapshots: how many recent snapshots show sell_min_price < our price
        snap_result = await db.execute(sql_text("""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN sell_min_price < :our_price AND sell_min_price > 0 THEN 1 ELSE 0 END) as undercut_count
            FROM market_snapshots
            WHERE type_id = :tid AND timestamp >= datetime('now', '-2 days')
        """).bindparams(tid=order.type_id, our_price=order.price))
        snap = snap_result.fetchone()
        total_snaps = snap[0] if snap else 0
        undercut_snaps = snap[1] if snap else 0
        undercut_pct = round(undercut_snaps / total_snaps * 100, 0) if total_snaps > 0 else 0

        items.append({
            "type_id": order.type_id,
            "name": name,
            "our_price": round(order.price, 2),
            "lowest_competitor": round(order.lowest_competitor, 2) if order.lowest_competitor else None,
            "undercut_by": round(order.undercut_by, 2) if order.undercut_by else 0,
            "volume_remain": order.volume_remain,
            "undercut_pct_48h": undercut_pct,
            "total_snapshots": total_snaps,
        })

    return sorted(items, key=lambda x: -x["undercut_pct_48h"])


def _compute_restock_urgency(local_stock, local_prices, jita_map, item_vols, item_names, cj_velocity, excluded_types, settings):
    """
    Fast movers running low — items with high velocity but low days of stock.
    Includes ammo, fuel, charges, drones — stuff that sells fast even at thin margins.
    Sorted by days of stock ascending (most urgent first).
    """
    items = []

    for tid, vel in cj_velocity.items():
        if tid in excluded_types or vel < 1:
            continue

        stock = local_stock.get(tid, 0)
        if stock <= 0:
            continue  # Already out — that's for What To Do (NOT SEEDED)

        days = stock / vel
        if days > 14:
            continue  # Well stocked, not urgent

        name = item_names.get(tid)
        if not name:
            continue

        vol_m3 = item_vols.get(tid, 0)
        if vol_m3 <= 0:
            continue

        # Skip bulk commodities
        if vel * vol_m3 > 100000:
            continue

        local_price = local_prices.get(tid)
        jita = jita_map.get(tid)
        margin_pct = 0
        if jita and jita.sell_min and jita.sell_min > 0 and local_price and local_price > 0:
            import_cost = jita.sell_min + (vol_m3 * settings.freight_cost_per_m3)
            margin_pct = ((local_price - import_cost) / import_cost) * 100 if import_cost > 0 else 0

        # Include even thin margins — the point is these items SELL
        if margin_pct < 3:
            continue

        items.append({
            "type_id": tid,
            "name": name,
            "velocity": round(vel, 1),
            "stock": stock,
            "days_of_stock": round(days, 1),
            "margin_pct": round(margin_pct, 1),
        })

    return sorted(items, key=lambda x: x["days_of_stock"])[:20]


def _compute_margin_alerts(cj_entries, jita_map, item_vols, settings):
    """
    Flag items where the margin between import cost and local sell is thinning.
    Compare current margin to what it was (using change_7d on the local price).
    """
    alerts = []
    for entry in cj_entries:
        tid = entry["type_id"]
        jita = jita_map.get(tid)
        if not jita or not jita.sell_min or jita.sell_min <= 0:
            continue
        local_price = entry.get("price", 0)
        if not local_price or local_price <= 0:
            continue

        vol_m3 = item_vols.get(tid, 0)
        import_cost = jita.sell_min + (vol_m3 * settings.freight_cost_per_m3)
        current_margin = ((local_price - import_cost) / import_cost) * 100 if import_cost > 0 else 0

        # Check if price is trending down locally (change_7d < -5%)
        # Only flag items with margin between -15% and +30% — below -15% is a junk listing,
        # above +30% is healthy enough that a small decline isn't concerning
        change = entry.get("change_7d")
        if change is not None and change < -5 and -15 < current_margin < 30:
            alerts.append({
                "type_id": tid,
                "name": entry["name"],
                "local_price": round(local_price, 2),
                "import_cost": round(import_cost, 2),
                "margin_pct": round(current_margin, 1),
                "price_change_7d": round(change, 1),
                "warning": "Price dropping" if change < -10 else "Margin thinning",
            })

    return sorted(alerts, key=lambda x: x["price_change_7d"])[:15]


# ─── Market Intelligence Config ──────────────────────────────

config_router = APIRouter(prefix="/config", tags=["config"])


@config_router.get("/mi-exclusions")
async def get_mi_exclusions(db: AsyncSession = Depends(get_db)):
    """Get excluded market group IDs for market intelligence."""
    import json
    setting = await db.get(AppSetting, "mi_excluded_groups")
    excluded = json.loads(setting.value) if setting and setting.value else []
    return {"excluded": excluded}


@config_router.post("/mi-exclusions")
async def set_mi_exclusions(
    group_ids: str = Form(""),
    db: AsyncSession = Depends(get_db),
):
    """Save excluded market group IDs (comma-separated or JSON array)."""
    import json
    try:
        ids = json.loads(group_ids) if group_ids.startswith("[") else [int(x.strip()) for x in group_ids.split(",") if x.strip()]
    except Exception:
        ids = []

    setting = await db.get(AppSetting, "mi_excluded_groups")
    if setting:
        setting.value = json.dumps(ids)
    else:
        db.add(AppSetting(key="mi_excluded_groups", value=json.dumps(ids)))
    await db.commit()
    return {"excluded": ids, "count": len(ids)}


@config_router.get("/top-market-groups")
async def get_top_market_groups(db: AsyncSession = Depends(get_db)):
    """Get all top-level market groups for exclusion config UI."""
    import json
    result = await db.execute(
        select(SdeMarketGroup)
        .where(SdeMarketGroup.parent_group_id.is_(None))
        .order_by(SdeMarketGroup.name)
    )
    groups = result.scalars().all()

    # Get current exclusions
    setting = await db.get(AppSetting, "mi_excluded_groups")
    excluded = set(json.loads(setting.value)) if setting and setting.value else set()

    return [{
        "id": g.market_group_id,
        "name": g.name,
        "excluded": g.market_group_id in excluded,
    } for g in groups]


@config_router.get("/cost-basis")
async def get_cost_basis_configs(db: AsyncSession = Depends(get_db)):
    """Get cost basis configs for all linked characters."""
    from app.models.database import CostBasisConfig, EsiCharacter

    # Get all linked characters
    chars_result = await db.execute(select(EsiCharacter).where(EsiCharacter.is_active == True))
    chars = chars_result.scalars().all()

    result = []
    for char in chars:
        cfg = await db.get(CostBasisConfig, char.character_id)
        result.append({
            "character_id": char.character_id,
            "character_name": char.character_name,
            "role": cfg.role if cfg else "seller",
            "buy_filter": cfg.buy_filter if cfg else "buy_orders_only",
            "excluded_stations": cfg.excluded_stations if cfg else [],
            "excluded_station_names": cfg.excluded_station_names if cfg else [],
        })

    return result


@config_router.post("/cost-basis/{character_id}")
async def update_cost_basis_config(
    character_id: int,
    db: AsyncSession = Depends(get_db),
    role: str = Form(default="seller"),
    buy_filter: str = Form(default="buy_orders_only"),
    excluded_stations: str = Form(default="[]"),
    excluded_station_names: str = Form(default="[]"),
):
    """Update cost basis config for a character."""
    import json
    from app.models.database import CostBasisConfig, EsiCharacter

    char = await db.get(EsiCharacter, character_id)
    if not char:
        raise HTTPException(404, "Character not found")

    stations = json.loads(excluded_stations) if excluded_stations else []
    station_names = json.loads(excluded_station_names) if excluded_station_names else []

    cfg = await db.get(CostBasisConfig, character_id)
    if cfg:
        cfg.role = role
        cfg.buy_filter = buy_filter
        cfg.excluded_stations = stations
        cfg.excluded_station_names = station_names
    else:
        cfg = CostBasisConfig(
            character_id=character_id,
            character_name=char.character_name,
            role=role,
            buy_filter=buy_filter,
            excluded_stations=stations,
            excluded_station_names=station_names,
        )
        db.add(cfg)

    await db.commit()
    return {"status": "ok", "character": char.character_name, "role": role}


@config_router.post("/cost-basis/{character_id}/add-exclusion")
async def add_station_exclusion(
    character_id: int,
    db: AsyncSession = Depends(get_db),
    station_id: int = Form(...),
    station_name: str = Form(default=""),
):
    """Add a station to the exclusion list for a character."""
    from app.models.database import CostBasisConfig

    cfg = await db.get(CostBasisConfig, character_id)
    if not cfg:
        cfg = CostBasisConfig(character_id=character_id, role="seller")
        db.add(cfg)
        await db.flush()

    stations = list(cfg.excluded_stations or [])
    names = list(cfg.excluded_station_names or [])
    if station_id not in stations:
        stations.append(station_id)
        names.append(station_name or str(station_id))
    cfg.excluded_stations = stations
    cfg.excluded_station_names = names
    await db.commit()
    return {"status": "ok", "excluded_stations": stations}


@config_router.post("/cost-basis/{character_id}/remove-exclusion")
async def remove_station_exclusion(
    character_id: int,
    db: AsyncSession = Depends(get_db),
    station_id: int = Form(...),
):
    """Remove a station from the exclusion list."""
    from app.models.database import CostBasisConfig

    cfg = await db.get(CostBasisConfig, character_id)
    if not cfg:
        return {"status": "ok"}

    stations = list(cfg.excluded_stations or [])
    names = list(cfg.excluded_station_names or [])
    if station_id in stations:
        idx = stations.index(station_id)
        stations.pop(idx)
        if idx < len(names):
            names.pop(idx)
    cfg.excluded_stations = stations
    cfg.excluded_station_names = names
    await db.commit()
    return {"status": "ok", "excluded_stations": stations}


@config_router.post("/cost-basis/recalculate")
async def recalculate_profits(db: AsyncSession = Depends(get_db)):
    """Wipe existing profit data and re-run matching with current cost basis settings."""
    from sqlalchemy import text as sql_text

    # Reset all FIFO counters on buy transactions
    await db.execute(sql_text("""
        UPDATE wallet_transactions
        SET quantity_matched = 0,
            quantity_consumed = 0,
            quantity_remaining = quantity
        WHERE is_buy = 1
    """))

    # Clear existing profit records
    await db.execute(sql_text("DELETE FROM trade_profits"))
    await db.execute(sql_text("DELETE FROM contract_profits"))
    await db.execute(sql_text("DELETE FROM pending_transactions"))
    await db.commit()

    # Re-run matching
    from app.services.profit_engine import profit_engine
    result = await profit_engine.run_matching(db)
    return {"status": "recalculated", **result}


# ─── ESI Market Data Fetch ──────────────────────────────────


esi_market_router = APIRouter(prefix="/esi-market", tags=["esi-market"])


@esi_market_router.post("/jita")
async def start_jita_fetch():
    """Start background fetch of ALL Jita orders from ESI."""
    if get_fetch_status()["running"]:
        return {"status": "already_running", "progress": get_fetch_status()["progress"]}

    async def _run():
        await fetch_all_jita_prices()

    asyncio.create_task(_run())
    return {"status": "started"}


@esi_market_router.post("/history")
async def start_history_fetch():
    """Start background fetch of price history for all Keepstar items."""
    if get_fetch_status()["running"]:
        return {"status": "already_running", "progress": get_fetch_status()["progress"]}

    async def _run():
        await fetch_market_history()

    asyncio.create_task(_run())
    return {"status": "started"}


@esi_market_router.get("/status")
async def get_esi_market_status():
    """Check status of background fetch tasks."""
    return get_fetch_status()


@esi_market_router.get("/stats")
async def get_price_cache_stats(db: AsyncSession = Depends(get_db)):
    """How many prices and history records we have cached."""
    jita_count = (await db.execute(select(func.count()).select_from(JitaPrice))).scalar()
    history_count = (await db.execute(select(func.count()).select_from(MarketHistory))).scalar()
    history_types = (await db.execute(select(func.count(func.distinct(MarketHistory.type_id))))).scalar()
    return {
        "jita_prices_cached": jita_count,
        "history_rows": history_count,
        "history_types": history_types,
    }


@browser_router.get("/item/{type_id}/history")
async def get_item_history(type_id: int, db: AsyncSession = Depends(get_db)):
    """Get cached price history for an item."""
    result = await db.execute(
        select(MarketHistory)
        .where(MarketHistory.type_id == type_id)
        .order_by(MarketHistory.date.asc())
    )
    rows = result.scalars().all()
    return [{
        "date": r.date,
        "average": r.average,
        "highest": r.highest,
        "lowest": r.lowest,
        "volume": r.volume,
        "order_count": r.order_count,
    } for r in rows]


@browser_router.get("/item/{type_id}/jita-orders")
async def get_jita_orders(type_id: int):
    """Fetch live Jita sell/buy orders for an item from ESI (on-demand)."""
    import httpx
    JITA_STATION = 60003760
    FORGE_REGION = 10000002
    try:
        async with httpx.AsyncClient(timeout=15.0) as http:
            resp = await http.get(
                f"https://esi.evetech.net/latest/markets/{FORGE_REGION}/orders/",
                params={"datasource": "tranquility", "type_id": type_id, "order_type": "all"},
            )
            if resp.status_code != 200:
                return {"sell_orders": [], "buy_orders": []}
            orders = resp.json()
            # Filter to Jita 4-4 only
            jita = [o for o in orders if o.get("location_id") == JITA_STATION]
            sells = sorted(
                [o for o in jita if not o.get("is_buy_order", False)],
                key=lambda o: o["price"]
            )
            buys = sorted(
                [o for o in jita if o.get("is_buy_order", False)],
                key=lambda o: -o["price"]
            )
            return {
                "sell_orders": [{
                    "price": o["price"],
                    "volume": o["volume_remain"],
                    "duration": o["duration"],
                    "issued": o.get("issued", ""),
                    "min_volume": o.get("min_volume", 1),
                } for o in sells[:50]],
                "buy_orders": [{
                    "price": o["price"],
                    "volume": o["volume_remain"],
                    "duration": o["duration"],
                    "issued": o.get("issued", ""),
                    "min_volume": o.get("min_volume", 1),
                } for o in buys[:50]],
            }
    except Exception as e:
        return {"sell_orders": [], "buy_orders": [], "error": str(e)}
