"""
Void Market — Trading API Routes

Powers the Trading tab with:
- Active orders + undercut detection
- Trade profits (FIFO matched)
- Manufacturing profits
- Contract profits
- Pending transactions (unmatched sells)
- P&L summary for dashboard
- Wallet journal
- Item inventory analysis
"""
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Form
from sqlalchemy import select, func, desc, and_ as db_and
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.session import get_db
from app.models.database import (
    CharacterOrder, WalletTransaction, WalletJournal,
    TradeProfit, ManufacturingProfit, ContractProfit, PendingTransaction,
    IndustryJob, CharacterContract, CharacterContractItem,
    SdeType, MarketOrder, WarehouseItem,
)
from app.config import settings
from app.services.profit_engine import profit_engine
from app.services.sync_service import sync_service
from app.services.esi_wallet import esi_wallet

logger = logging.getLogger("void_market.trading_routes")

trading_router = APIRouter(prefix="/trading", tags=["trading"])


# ─── P&L Summary (Dashboard) ──────────────────────────────

@trading_router.get("/summary")
async def get_profit_summary(
    days: int = Query(30, ge=1, le=3650),
    db: AsyncSession = Depends(get_db),
):
    """Get aggregated profit summary for the Overview dashboard."""
    return await profit_engine.get_profit_summary(db, days=days)


@trading_router.get("/wallet-balance")
async def get_wallet_balance(db: AsyncSession = Depends(get_db)):
    """Get current wallet balance for all linked characters."""
    from app.services.esi_auth import esi_auth as _esi
    chars = await _esi.get_all_active_characters(db)
    result = []
    total = 0
    for c in chars:
        bal = c.last_wallet_balance or 0
        result.append({
            "character_id": c.character_id,
            "character_name": c.character_name,
            "balance": bal,
        })
        total += bal
    return {"characters": result, "total": total, "balance": total}


# ─── Active Orders + Undercut Detection ────────────────────

@trading_router.get("/orders")
async def get_character_orders(db: AsyncSession = Depends(get_db)):
    """Get your active market orders with undercut detection, character and location info."""
    from app.models.database import EsiCharacter, LocationCache, TrackedStructure

    result = await db.execute(
        select(CharacterOrder, SdeType.name)
        .outerjoin(SdeType, SdeType.type_id == CharacterOrder.type_id)
        .order_by(CharacterOrder.is_buy_order, SdeType.name)
    )
    rows = result.all()

    # Build lookup maps
    chars_r = await db.execute(select(EsiCharacter))
    char_map = {c.character_id: c.character_name for c in chars_r.scalars().all()}

    loc_r = await db.execute(select(LocationCache))
    loc_map = {l.location_id: l for l in loc_r.scalars().all()}

    struct_r = await db.execute(select(TrackedStructure))
    struct_map = {s.structure_id: s for s in struct_r.scalars().all()}

    def resolve_loc(loc_id):
        if loc_id in struct_map:
            s = struct_map[loc_id]
            return s.name, s.solar_system_name
        if loc_id in loc_map:
            l = loc_map[loc_id]
            return l.station_name or f"Location {loc_id}", l.solar_system_name
        return f"Location {loc_id}", None

    sell_orders = []
    buy_orders = []

    for order, name in rows:
        loc_name, sys_name = resolve_loc(order.location_id) if order.location_id else ("Unknown", None)
        entry = {
            "order_id": order.order_id,
            "type_id": order.type_id,
            "name": name or f"Type {order.type_id}",
            "price": order.price,
            "volume_remain": order.volume_remain,
            "volume_total": order.volume_total,
            "total_isk": order.price * order.volume_remain,
            "location_id": order.location_id,
            "location_name": loc_name,
            "solar_system": sys_name,
            "character_id": order.character_id,
            "character_name": char_map.get(order.character_id, "Unknown"),
            "issued": order.issued.isoformat() if order.issued else None,
            "duration": order.duration,
            "is_undercut": order.is_undercut,
            "undercut_by": order.undercut_by,
            "lowest_competitor": order.lowest_competitor,
            "last_checked": order.last_checked.isoformat() if order.last_checked else None,
        }

        if order.is_buy_order:
            buy_orders.append(entry)
        else:
            sell_orders.append(entry)

    total_sell_isk = sum(o["total_isk"] for o in sell_orders)
    total_buy_isk = sum(o["total_isk"] for o in buy_orders)
    undercut_count = sum(1 for o in sell_orders if o["is_undercut"])

    return {
        "sell_orders": sell_orders,
        "buy_orders": buy_orders,
        "total_sell_isk": round(total_sell_isk, 2),
        "total_buy_isk": round(total_buy_isk, 2),
        "sell_count": len(sell_orders),
        "buy_count": len(buy_orders),
        "undercut_count": undercut_count,
    }


# ─── Trade Profits ─────────────────────────────────────────

@trading_router.get("/trade-profits")
async def get_trade_profits(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=10, le=200),
    db: AsyncSession = Depends(get_db),
):
    """Get FIFO-matched trade profit records."""
    offset = (page - 1) * per_page

    result = await db.execute(
        select(TradeProfit, SdeType.name)
        .outerjoin(SdeType, SdeType.type_id == TradeProfit.type_id)
        .order_by(TradeProfit.date.desc())
        .limit(per_page)
        .offset(offset)
    )
    rows = result.all()

    total_result = await db.execute(select(func.count()).select_from(TradeProfit))
    total = total_result.scalar() or 0

    return {
        "items": [{
            "id": tp.id,
            "type_id": tp.type_id,
            "name": name or f"Type {tp.type_id}",
            "quantity": tp.quantity,
            "unit_buy": tp.unit_buy,
            "unit_sell": tp.unit_sell,
            "total_buy": tp.total_buy,
            "total_sell": tp.total_sell,
            "broker_buy": tp.broker_buy,
            "broker_sell": tp.broker_sell,
            "sales_tax": tp.sales_tax,
            "freight_cost": tp.freight_cost or 0,
            "margin_pct": tp.margin_pct,
            "profit": tp.profit,
            "date": tp.date.isoformat() if tp.date else None,
        } for tp, name in rows],
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": (total + per_page - 1) // per_page,
    }


@trading_router.get("/trade-profits/by-item")
async def get_trade_profits_by_item(
    days: int = Query(30, ge=1, le=3650),
    db: AsyncSession = Depends(get_db),
):
    """Get per-item profit summary (for the Overview table)."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    result = await db.execute(
        select(
            TradeProfit.type_id,
            SdeType.name,
            func.sum(TradeProfit.quantity).label("qty_sold"),
            func.sum(TradeProfit.profit).label("total_profit"),
        )
        .outerjoin(SdeType, SdeType.type_id == TradeProfit.type_id)
        .where(TradeProfit.date >= cutoff)
        .group_by(TradeProfit.type_id)
        .order_by(desc("total_profit"))
    )

    return [{
        "type_id": r[0],
        "name": r[1] or f"Type {r[0]}",
        "quantity_sold": r[2],
        "profit": round(r[3], 2),
    } for r in result.fetchall()]


# ─── Manufacturing Profits ─────────────────────────────────

@trading_router.get("/manufacturing-profits")
async def get_manufacturing_profits(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=10, le=200),
    db: AsyncSession = Depends(get_db),
):
    """Get manufacturing profit records."""
    offset = (page - 1) * per_page

    result = await db.execute(
        select(ManufacturingProfit, SdeType.name)
        .outerjoin(SdeType, SdeType.type_id == ManufacturingProfit.type_id)
        .order_by(ManufacturingProfit.date.desc())
        .limit(per_page)
        .offset(offset)
    )
    rows = result.all()

    total_result = await db.execute(select(func.count()).select_from(ManufacturingProfit))
    total = total_result.scalar() or 0

    return {
        "items": [{
            "id": mp.id,
            "type_id": mp.type_id,
            "name": name or f"Type {mp.type_id}",
            "quantity": mp.quantity,
            "unit_build": mp.unit_build,
            "unit_sell": mp.unit_sell,
            "total_build": mp.total_build,
            "total_sell": mp.total_sell,
            "broker_sell": mp.broker_sell,
            "sales_tax": mp.sales_tax,
            "margin_pct": mp.margin_pct,
            "profit": mp.profit,
            "date": mp.date.isoformat() if mp.date else None,
        } for mp, name in rows],
        "total": total,
        "page": page,
        "per_page": per_page,
    }


# ─── Contract Profits ──────────────────────────────────────

@trading_router.get("/contract-profits")
async def get_contract_profits(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=10, le=200),
    db: AsyncSession = Depends(get_db),
):
    """Get contract profit records."""
    offset = (page - 1) * per_page

    result = await db.execute(
        select(ContractProfit)
        .order_by(ContractProfit.date.desc())
        .limit(per_page)
        .offset(offset)
    )
    profits = result.scalars().all()

    total_result = await db.execute(select(func.count()).select_from(ContractProfit))
    total = total_result.scalar() or 0

    return {
        "items": [{
            "id": cp.id,
            "contract_id": cp.contract_id,
            "acceptor_name": cp.acceptor_name,
            "title": cp.title,
            "total_cost": cp.total_cost,
            "total_sell": cp.total_sell,
            "freight_cost": cp.freight_cost or 0,
            "volume_m3": cp.volume_m3 or 0,
            "broker_fee": cp.broker_fee,
            "sales_tax": cp.sales_tax,
            "margin_pct": cp.margin_pct,
            "profit": cp.profit,
            "items_complete": cp.items_complete,
            "date": cp.date.isoformat() if cp.date else None,
        } for cp in profits],
        "total": total,
        "page": page,
        "per_page": per_page,
    }


@trading_router.get("/contract-profits/{contract_id}/items")
async def get_contract_profit_items(
    contract_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Get per-item cost breakdown for a contract — like EVE Tycoon's Total Cost popup."""
    from app.services.profit_engine import profit_engine

    items_result = await db.execute(
        select(CharacterContractItem, SdeType.name)
        .outerjoin(SdeType, SdeType.type_id == CharacterContractItem.type_id)
        .where(CharacterContractItem.contract_id == contract_id)
        .where(CharacterContractItem.is_included == True)
    )
    items = items_result.all()

    result_items = []
    total_cost = 0
    for item, name in items:
        unit_cost = await profit_engine._get_item_cost(db, item.type_id, use_jita_fallback=False)
        item_total = (unit_cost or 0) * item.quantity
        total_cost += item_total
        result_items.append({
            "type_id": item.type_id,
            "name": name or f"Type {item.type_id}",
            "quantity": item.quantity,
            "unit_cost": round(unit_cost, 2) if unit_cost else None,
            "total_cost": round(item_total, 2),
        })

    # Sort by total cost descending (most expensive first)
    result_items.sort(key=lambda x: -x["total_cost"])

    return {
        "contract_id": contract_id,
        "items": result_items,
        "total_cost": round(total_cost, 2),
        "item_count": len(result_items),
    }


# ─── Pending Transactions ──────────────────────────────────

@trading_router.get("/pending")
async def get_pending_transactions(db: AsyncSession = Depends(get_db)):
    """Get sell transactions awaiting manual cost entry."""
    result = await db.execute(
        select(PendingTransaction, SdeType.name)
        .outerjoin(SdeType, SdeType.type_id == PendingTransaction.type_id)
        .where(PendingTransaction.status == "pending")
        .order_by(PendingTransaction.date.desc())
    )
    rows = result.all()

    return [{
        "id": pt.id,
        "transaction_id": pt.transaction_id,
        "type_id": pt.type_id,
        "name": name or f"Type {pt.type_id}",
        "quantity": pt.quantity,
        "unit_sell": pt.unit_sell,
        "total_sell": pt.total_sell,
        "custom_buy_price": pt.custom_buy_price,
        "date": pt.date.isoformat() if pt.date else None,
    } for pt, name in rows]


@trading_router.post("/pending/{pending_id}/resolve")
async def resolve_pending_transaction(
    pending_id: int,
    buy_price: float = Form(...),
    db: AsyncSession = Depends(get_db),
):
    """Set manual cost basis for a pending transaction."""
    pt = await db.get(PendingTransaction, pending_id)
    if not pt:
        raise HTTPException(404, "Pending transaction not found")

    pt.custom_buy_price = buy_price
    pt.status = "resolved"

    # Create a trade profit record with the manual cost
    total_buy = buy_price * pt.quantity
    total_sell = pt.unit_sell * pt.quantity
    broker_buy = 0  # No broker on manual entry
    broker_sell = total_sell * 0.01  # Estimate
    sales_tax = total_sell * 0.0336
    profit = total_sell - total_buy - broker_buy - broker_sell - sales_tax
    margin = (profit / total_sell * 100) if total_sell > 0 else 0

    tp = TradeProfit(
        sell_transaction_id=pt.transaction_id,
        buy_transaction_id=None,
        type_id=pt.type_id,
        quantity=pt.quantity,
        unit_buy=buy_price,
        unit_sell=pt.unit_sell,
        total_buy=total_buy,
        total_sell=total_sell,
        broker_buy=0,
        broker_sell=round(broker_sell, 2),
        sales_tax=round(sales_tax, 2),
        margin_pct=round(margin, 2),
        profit=round(profit, 2),
        date=pt.date,
    )
    db.add(tp)
    await db.commit()
    return {"status": "resolved", "profit": round(profit, 2)}


@trading_router.post("/pending/{pending_id}/ignore")
async def ignore_pending_transaction(
    pending_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Mark a pending transaction as ignored."""
    pt = await db.get(PendingTransaction, pending_id)
    if not pt:
        raise HTTPException(404, "Pending transaction not found")
    pt.status = "ignored"
    await db.commit()
    return {"status": "ignored"}


@trading_router.post("/pending/{pending_id}/consume")
async def consume_pending_transaction(
    pending_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Mark a pending sell as consumed (personal use, gift, etc.)."""
    pt = await db.get(PendingTransaction, pending_id)
    if not pt:
        raise HTTPException(404, "Not found")
    pt.status = "consumed"
    await db.commit()
    return {"status": "consumed"}


@trading_router.post("/pending/{pending_id}/loot")
async def mark_pending_as_loot(
    pending_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Mark a pending sell as loot (zero cost basis → pure profit)."""
    pt = await db.get(PendingTransaction, pending_id)
    if not pt:
        raise HTTPException(404, "Not found")

    # Create a TradeProfit with cost=0 (loot/salvage/mined = free)
    qty = pt.quantity
    ts = pt.unit_sell * qty
    # Use character's broker rates
    from app.services.profit_engine import ProfitEngine
    engine = ProfitEngine()
    fees = await engine._get_broker_rates(db, pt.character_id, pt.type_id, pt.date)
    # Get the original sell transaction to check location for structure vs NPC fees
    orig = await db.execute(
        select(WalletTransaction).where(WalletTransaction.transaction_id == pt.transaction_id)
    )
    orig_txn = orig.scalar_one_or_none()
    is_structure = orig_txn and orig_txn.location_id and orig_txn.location_id > 1000000000000
    sb = ts * ((fees["sell_structure"] if is_structure else fees["sell_npc"]) / 100)
    tx = ts * (fees["sales_tax"] / 100)
    profit = ts - sb - tx

    db.add(TradeProfit(
        sell_transaction_id=pt.transaction_id, buy_transaction_id=None,
        type_id=pt.type_id, quantity=qty, unit_buy=0, unit_sell=pt.unit_sell,
        total_buy=0, total_sell=round(ts, 2),
        broker_buy=0, broker_sell=round(sb, 2),
        sales_tax=round(tx, 2), freight_cost=0,
        margin_pct=100.0,
        profit=round(profit, 2), date=pt.date,
    ))
    pt.status = "loot"
    await db.commit()
    return {"status": "loot", "profit": round(profit, 2)}


@trading_router.post("/pending/bulk")
async def bulk_pending_action(
    ids: str = Form(...),  # comma-separated ids
    action: str = Form(...),  # "loot" | "consume" | "ignore" | "set_cost"
    cost: float = Form(0),  # for set_cost action
    db: AsyncSession = Depends(get_db),
):
    """Apply an action to multiple pending transactions at once."""
    try:
        id_list = [int(x.strip()) for x in ids.split(",") if x.strip()]
    except ValueError:
        raise HTTPException(400, "Invalid ids format")

    if action not in ("loot", "consume", "ignore", "set_cost"):
        raise HTTPException(400, f"Invalid action: {action}")

    from app.services.profit_engine import ProfitEngine
    engine = ProfitEngine()
    processed = 0

    for pid in id_list:
        pt = await db.get(PendingTransaction, pid)
        if not pt:
            continue

        if action == "ignore":
            pt.status = "ignored"
            processed += 1

        elif action == "consume":
            pt.status = "consumed"
            processed += 1

        elif action == "loot":
            qty = pt.quantity
            ts = pt.unit_sell * qty
            fees = await engine._get_broker_rates(db, pt.character_id, pt.type_id, pt.date)
            orig = await db.execute(
                select(WalletTransaction).where(WalletTransaction.transaction_id == pt.transaction_id)
            )
            orig_txn = orig.scalar_one_or_none()
            is_structure = orig_txn and orig_txn.location_id and orig_txn.location_id > 1000000000000
            sb = ts * ((fees["sell_structure"] if is_structure else fees["sell_npc"]) / 100)
            tx = ts * (fees["sales_tax"] / 100)
            profit = ts - sb - tx
            db.add(TradeProfit(
                sell_transaction_id=pt.transaction_id, buy_transaction_id=None,
                type_id=pt.type_id, quantity=qty, unit_buy=0, unit_sell=pt.unit_sell,
                total_buy=0, total_sell=round(ts, 2),
                broker_buy=0, broker_sell=round(sb, 2),
                sales_tax=round(tx, 2), freight_cost=0,
                margin_pct=100.0, profit=round(profit, 2), date=pt.date,
            ))
            pt.status = "loot"
            processed += 1

        elif action == "set_cost":
            qty = pt.quantity
            tb = cost * qty
            ts = pt.unit_sell * qty
            fees = await engine._get_broker_rates(db, pt.character_id, pt.type_id, pt.date)
            orig = await db.execute(
                select(WalletTransaction).where(WalletTransaction.transaction_id == pt.transaction_id)
            )
            orig_txn = orig.scalar_one_or_none()
            is_structure = orig_txn and orig_txn.location_id and orig_txn.location_id > 1000000000000
            sb = ts * ((fees["sell_structure"] if is_structure else fees["sell_npc"]) / 100)
            tx = ts * (fees["sales_tax"] / 100)
            profit = ts - tb - sb - tx
            margin = (profit / ts * 100) if ts else 0
            db.add(TradeProfit(
                sell_transaction_id=pt.transaction_id, buy_transaction_id=None,
                type_id=pt.type_id, quantity=qty, unit_buy=cost, unit_sell=pt.unit_sell,
                total_buy=round(tb, 2), total_sell=round(ts, 2),
                broker_buy=0, broker_sell=round(sb, 2),
                sales_tax=round(tx, 2), freight_cost=0,
                margin_pct=round(margin, 2), profit=round(profit, 2), date=pt.date,
            ))
            pt.status = "resolved"
            processed += 1

    await db.commit()
    return {"status": "ok", "processed": processed, "action": action}


@trading_router.post("/pending/bulk-by-type/{type_id}")
async def bulk_pending_by_type(
    type_id: int,
    action: str = Form(...),
    cost: float = Form(0),
    db: AsyncSession = Depends(get_db),
):
    """Apply an action to ALL pending transactions of a given type_id."""
    r = await db.execute(
        select(PendingTransaction.id)
        .where(PendingTransaction.type_id == type_id, PendingTransaction.status == "pending")
    )
    ids = [row[0] for row in r.all()]
    if not ids:
        return {"status": "ok", "processed": 0}

    # Delegate to bulk endpoint logic by calling it inline
    ids_str = ",".join(str(i) for i in ids)
    from starlette.datastructures import FormData
    # Just re-route the call
    return await bulk_pending_action(ids=ids_str, action=action, cost=cost, db=db)


# ─── Buy Transaction Management ───────────────────────────

@trading_router.get("/buy-transactions")
async def get_buy_transactions(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=10, le=200),
    show_consumed: bool = Query(False),
    db: AsyncSession = Depends(get_db),
):
    """Get buy transactions with remaining/consumed counts."""
    offset = (page - 1) * per_page
    q = (
        select(WalletTransaction, SdeType.name)
        .outerjoin(SdeType, SdeType.type_id == WalletTransaction.type_id)
        .where(WalletTransaction.is_buy == True)
    )
    if not show_consumed:
        q = q.where(WalletTransaction.quantity_remaining > 0)
    q = q.order_by(WalletTransaction.date.desc()).limit(per_page).offset(offset)

    result = await db.execute(q)
    rows = result.all()

    return [{
        "transaction_id": t.transaction_id,
        "type_id": t.type_id,
        "name": name or f"Type {t.type_id}",
        "quantity": t.quantity,
        "unit_price": t.unit_price,
        "total": round(t.unit_price * t.quantity, 2),
        "quantity_matched": t.quantity_matched,
        "quantity_consumed": t.quantity_consumed,
        "quantity_remaining": t.quantity_remaining,
        "character_id": t.character_id,
        "date": t.date.isoformat() if t.date else None,
    } for t, name in rows]


@trading_router.post("/buy-transactions/{transaction_id}/consume")
async def consume_buy_transaction(
    transaction_id: int,
    quantity: Optional[int] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Mark a buy as consumed (personal use). Removes from FIFO pool."""
    t = await db.execute(
        select(WalletTransaction).where(
            WalletTransaction.transaction_id == transaction_id,
            WalletTransaction.is_buy == True,
        )
    )
    txn = t.scalar_one_or_none()
    if not txn:
        raise HTTPException(404, "Buy transaction not found")

    qty = quantity if quantity and quantity > 0 else txn.quantity_remaining
    if qty > txn.quantity_remaining:
        qty = txn.quantity_remaining

    txn.quantity_consumed += qty
    txn.quantity_remaining -= qty
    await db.commit()
    return {"status": "consumed", "consumed": qty, "remaining": txn.quantity_remaining}


@trading_router.post("/buy-transactions/{transaction_id}/unconsume")
async def unconsume_buy_transaction(
    transaction_id: int,
    quantity: Optional[int] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """Undo consume — put items back into FIFO pool."""
    t = await db.execute(
        select(WalletTransaction).where(
            WalletTransaction.transaction_id == transaction_id,
            WalletTransaction.is_buy == True,
        )
    )
    txn = t.scalar_one_or_none()
    if not txn:
        raise HTTPException(404, "Buy transaction not found")

    qty = quantity if quantity and quantity > 0 else txn.quantity_consumed
    if qty > txn.quantity_consumed:
        qty = txn.quantity_consumed

    txn.quantity_consumed -= qty
    txn.quantity_remaining += qty
    await db.commit()
    return {"status": "unconsumed", "restored": qty, "remaining": txn.quantity_remaining}


# ─── Industry Jobs ─────────────────────────────────────────

@trading_router.get("/industry-jobs")
async def get_industry_jobs(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=10, le=200),
    db: AsyncSession = Depends(get_db),
):
    """Get industry job history."""
    offset = (page - 1) * per_page

    result = await db.execute(
        select(IndustryJob, SdeType.name)
        .outerjoin(SdeType, SdeType.type_id == IndustryJob.product_type_id)
        .where(IndustryJob.status == "delivered")
        .order_by(IndustryJob.completed_date.desc())
        .limit(per_page)
        .offset(offset)
    )
    rows = result.all()

    return {
        "items": [{
            "job_id": job.job_id,
            "product_type_id": job.product_type_id,
            "product_name": name or f"Type {job.product_type_id}",
            "activity_id": job.activity_id,
            "runs": job.runs,
            "cost": job.cost,
            "materials_cost": job.materials_cost,
            "unit_cost": job.unit_cost,
            "quantity_produced": job.quantity_produced,
            "quantity_sold": job.quantity_sold,
            "quantity_consumed": job.quantity_consumed,
            "materials_complete": job.materials_complete,
            "completed_date": job.completed_date.isoformat() if job.completed_date else None,
        } for job, name in rows],
        "page": page,
        "per_page": per_page,
    }


# ─── Wallet Journal ────────────────────────────────────────

@trading_router.get("/journal")
async def get_wallet_journal(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=10, le=200),
    db: AsyncSession = Depends(get_db),
):
    """Get wallet journal entries."""
    offset = (page - 1) * per_page

    result = await db.execute(
        select(WalletJournal)
        .order_by(WalletJournal.date.desc())
        .limit(per_page)
        .offset(offset)
    )
    entries = result.scalars().all()

    return {
        "items": [{
            "ref_id": e.ref_id,
            "ref_type": e.ref_type,
            "amount": e.amount,
            "balance": e.balance,
            "description": e.description,
            "first_party_id": e.first_party_id,
            "second_party_id": e.second_party_id,
            "date": e.date.isoformat() if e.date else None,
        } for e in entries],
        "page": page,
        "per_page": per_page,
    }


# ─── Item Inventory Analysis ──────────────────────────────

@trading_router.get("/inventory")
async def get_item_inventory(db: AsyncSession = Depends(get_db)):
    """
    Get per-item inventory analysis — capital locked up in unsold stock.
    Aggregates from buy transactions with remaining quantity.
    Also joins asset counts to show what actually exists vs FIFO pool.
    """
    from app.models.database import CharacterAsset

    result = await db.execute(
        select(
            WalletTransaction.type_id,
            SdeType.name,
            func.avg(WalletTransaction.unit_price).label("avg_buy"),
            func.min(WalletTransaction.unit_price).label("min_buy"),
            func.max(WalletTransaction.unit_price).label("max_buy"),
            func.sum(WalletTransaction.quantity).label("total_bought"),
            func.sum(WalletTransaction.quantity_matched).label("total_sold"),
            func.sum(WalletTransaction.quantity_consumed).label("total_consumed"),
            func.sum(WalletTransaction.quantity_remaining).label("total_remaining"),
        )
        .outerjoin(SdeType, SdeType.type_id == WalletTransaction.type_id)
        .where(WalletTransaction.is_buy == True)
        .group_by(WalletTransaction.type_id)
        .having(func.sum(WalletTransaction.quantity) > 0)
        .order_by(desc("total_remaining"))
    )
    rows = result.fetchall()

    # Build a type_id → total asset count map (sum across all characters)
    asset_counts = {}
    asset_r = await db.execute(
        select(CharacterAsset.type_id, func.sum(CharacterAsset.quantity))
        .group_by(CharacterAsset.type_id)
    )
    for tid, qty in asset_r.all():
        asset_counts[tid] = int(qty or 0)

    return [{
        "type_id": r[0],
        "name": r[1] or f"Type {r[0]}",
        "avg_buy_price": round(r[2], 2) if r[2] else 0,
        "min_price": round(r[3], 2) if r[3] else 0,
        "max_price": round(r[4], 2) if r[4] else 0,
        "total_bought": r[5] or 0,
        "total_sold": r[6] or 0,
        "total_consumed": r[7] or 0,
        "total_remaining": r[8] or 0,
        "asset_count": asset_counts.get(r[0], 0),
        "remaining_isk": round((r[2] or 0) * (r[8] or 0), 2),
    } for r in rows]


@trading_router.get("/inventory/{type_id}/locations")
async def get_inventory_locations(type_id: int, db: AsyncSession = Depends(get_db)):
    """
    For a specific item type, show where the units physically are.
    Cross-references CharacterAsset with TrackedStructure and LocationCache
    to give readable location names.
    """
    from app.models.database import (
        CharacterAsset, AssetName, TrackedStructure,
        LocationCache, EsiCharacter,
    )

    # Get all assets of this type
    r = await db.execute(
        select(CharacterAsset, AssetName.name.label("custom_name"))
        .join(AssetName, AssetName.item_id == CharacterAsset.item_id, isouter=True)
        .where(CharacterAsset.type_id == type_id)
        .order_by(desc(CharacterAsset.quantity))
    )
    rows = r.all()

    if not rows:
        return []

    # Pre-fetch lookup tables
    chars_r = await db.execute(select(EsiCharacter))
    chars = {c.character_id: c.character_name for c in chars_r.scalars().all()}

    structs_r = await db.execute(select(TrackedStructure))
    structs = {s.structure_id: s for s in structs_r.scalars().all()}

    locs_r = await db.execute(select(LocationCache))
    locs = {l.location_id: l for l in locs_r.scalars().all()}

    # Group by (character_id, location_id, location_flag)
    grouped = {}
    for asset, custom_name in rows:
        key = (asset.character_id, asset.location_id, asset.location_flag)
        if key not in grouped:
            grouped[key] = {
                "character_id": asset.character_id,
                "character_name": chars.get(asset.character_id, "Unknown"),
                "location_id": asset.location_id,
                "location_flag": asset.location_flag,
                "quantity": 0,
                "custom_names": [],
            }
        grouped[key]["quantity"] += int(asset.quantity)
        if custom_name:
            grouped[key]["custom_names"].append(custom_name)

    # Resolve location names
    result = []
    for g in grouped.values():
        loc_id = g["location_id"]
        name = None
        system = None
        if loc_id in structs:
            name = structs[loc_id].name
            system = structs[loc_id].solar_system_name
        elif loc_id in locs:
            name = locs[loc_id].station_name
            system = locs[loc_id].solar_system_name
        else:
            name = f"Location {loc_id}"
        g["location_name"] = name
        g["solar_system"] = system
        result.append(g)

    return sorted(result, key=lambda x: -x["quantity"])


# ─── Mark As Consumed ──────────────────────────────────────

@trading_router.post("/inventory/{type_id}/consume")
async def mark_inventory_consumed(
    type_id: int,
    quantity: Optional[int] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """
    Mark units of a given type as consumed (personal use, reprocessed, lost).
    Consumes across all buy transactions of this type, FIFO (oldest first).
    If quantity is omitted, consumes ALL remaining.
    """
    result = await db.execute(
        select(WalletTransaction)
        .where(WalletTransaction.is_buy == True,
               WalletTransaction.type_id == type_id,
               WalletTransaction.quantity_remaining > 0)
        .order_by(WalletTransaction.date.asc())
    )
    buys = result.scalars().all()

    if not buys:
        return {"status": "ok", "consumed": 0, "message": "Nothing to consume"}

    total_remaining = sum(b.quantity_remaining for b in buys)
    target = quantity if quantity and quantity > 0 else total_remaining
    target = min(target, total_remaining)

    consumed_total = 0
    remaining_to_consume = target
    for buy in buys:
        if remaining_to_consume <= 0:
            break
        qty = min(remaining_to_consume, buy.quantity_remaining)
        buy.quantity_consumed += qty
        buy.quantity_remaining -= qty
        consumed_total += qty
        remaining_to_consume -= qty

    await db.commit()
    return {
        "status": "ok",
        "consumed": consumed_total,
        "remaining_in_pool": total_remaining - consumed_total,
        "type_id": type_id,
    }


# ─── Edit/Delete Trade Profits ─────────────────────────────

@trading_router.put("/trade-profits/{profit_id}")
async def edit_trade_profit(
    profit_id: int,
    unit_buy: float = Form(None),
    unit_sell: float = Form(None),
    db: AsyncSession = Depends(get_db),
):
    """Edit a trade profit entry (manual correction)."""
    tp = await db.get(TradeProfit, profit_id)
    if not tp:
        raise HTTPException(404, "Trade profit not found")
    if unit_buy is not None:
        tp.unit_buy = unit_buy
        tp.total_buy = unit_buy * tp.quantity
    if unit_sell is not None:
        tp.unit_sell = unit_sell
        tp.total_sell = unit_sell * tp.quantity
    tp.profit = tp.total_sell - tp.total_buy - tp.broker_buy - tp.broker_sell - tp.sales_tax
    tp.margin_pct = round((tp.profit / tp.total_sell * 100) if tp.total_sell else 0, 2)
    await db.commit()
    return {"status": "updated", "profit": tp.profit}


@trading_router.delete("/trade-profits/{profit_id}")
async def delete_trade_profit(profit_id: int, db: AsyncSession = Depends(get_db)):
    """Delete a trade profit entry."""
    tp = await db.get(TradeProfit, profit_id)
    if not tp:
        raise HTTPException(404, "Trade profit not found")
    await db.delete(tp)
    await db.commit()
    return {"status": "deleted"}


# ─── Daily Profit Chart Data ──────────────────────────────

@trading_router.get("/daily-profits")
async def get_daily_profits(
    days: int = Query(30, ge=7, le=365),
    db: AsyncSession = Depends(get_db),
):
    """Get daily profit breakdown for the chart."""
    from app.services.intelligence import intelligence_service
    return await intelligence_service.get_daily_profits(db, days=days)


# ─── Sync Triggers ─────────────────────────────────────────

@trading_router.post("/sync")
async def trigger_full_sync():
    """Manually trigger a full wallet + profit sync."""
    import asyncio
    asyncio.create_task(sync_service.full_sync())
    return {"status": "started"}


@trading_router.post("/sync/goonmetrics")
async def trigger_goonmetrics_sync():
    """Manually trigger a Goonmetrics volume sync."""
    import asyncio
    asyncio.create_task(sync_service.sync_goonmetrics())
    return {"status": "started"}


# ─── P&L Report ───────────────────────────────────────────

@trading_router.get("/pnl-report")
async def get_pnl_report(db: AsyncSession = Depends(get_db)):
    """
    Comprehensive P&L report with daily breakdown, source split, and top items.
    Returns data for this week, last week, and all-time.
    """
    from datetime import date as dt_date
    import calendar

    today = dt_date.today()
    # Monday of this week
    week_start = today - timedelta(days=today.weekday())
    last_week_start = week_start - timedelta(days=7)
    month_start = today.replace(day=1)

    async def period_summary(start_date, end_date=None):
        """Get P&L for a date range."""
        date_filter = TradeProfit.date >= start_date.isoformat()
        if end_date:
            date_filter = db_and(date_filter, TradeProfit.date < end_date.isoformat())

        # Trade profits by source
        r = await db.execute(
            select(
                func.count(TradeProfit.id),
                func.sum(TradeProfit.profit),
                func.sum(TradeProfit.total_sell),
                func.sum(TradeProfit.total_buy),
                func.sum(TradeProfit.sales_tax),
                func.sum(TradeProfit.broker_sell),
                func.sum(TradeProfit.broker_buy),
            ).where(date_filter)
        )
        row = r.one()

        # Split by source: FIFO matched vs Jita fallback vs loot
        fifo_r = await db.execute(
            select(func.sum(TradeProfit.profit))
            .where(date_filter)
            .where(TradeProfit.buy_transaction_id.isnot(None))
        )
        fifo_profit = fifo_r.scalar() or 0

        jita_r = await db.execute(
            select(func.sum(TradeProfit.profit))
            .where(date_filter)
            .where(TradeProfit.buy_transaction_id.is_(None))
            .where(TradeProfit.total_buy > 0)
        )
        jita_profit = jita_r.scalar() or 0

        loot_r = await db.execute(
            select(func.sum(TradeProfit.profit))
            .where(date_filter)
            .where(TradeProfit.total_buy == 0)
        )
        loot_profit = loot_r.scalar() or 0

        # Contract profits
        contract_filter = ContractProfit.date >= start_date.isoformat()
        if end_date:
            contract_filter = db_and(contract_filter, ContractProfit.date < end_date.isoformat())
        cp_r = await db.execute(
            select(func.count(ContractProfit.id), func.sum(ContractProfit.profit))
            .where(contract_filter)
        )
        cp = cp_r.one()

        # Manufacturing profits
        mfg_filter = ManufacturingProfit.date >= start_date.isoformat()
        if end_date:
            mfg_filter = db_and(mfg_filter, ManufacturingProfit.date < end_date.isoformat())
        mp_r = await db.execute(
            select(func.count(ManufacturingProfit.id), func.sum(ManufacturingProfit.profit))
            .where(mfg_filter)
        )
        mp = mp_r.one()

        total_profit = (row[1] or 0) + (cp[1] or 0) + (mp[1] or 0)

        return {
            "trade_txns": row[0] or 0,
            "trade_profit": float(row[1] or 0),
            "trade_income": float(row[2] or 0),
            "trade_cost": float(row[3] or 0),
            "sales_tax": float(row[4] or 0),
            "broker_fees": float((row[5] or 0) + (row[6] or 0)),
            "fifo_profit": float(fifo_profit),
            "jita_fallback_profit": float(jita_profit),
            "loot_profit": float(loot_profit),
            "contract_count": cp[0] or 0,
            "contract_profit": float(cp[1] or 0),
            "manufacturing_count": mp[0] or 0,
            "manufacturing_profit": float(mp[1] or 0),
            "net_profit": float(total_profit),
        }

    # Daily breakdown (last 14 days)
    daily = []
    for i in range(13, -1, -1):
        d = today - timedelta(days=i)
        d_next = d + timedelta(days=1)
        s = await period_summary(d, d_next)
        s["date"] = d.isoformat()
        s["day"] = calendar.day_abbr[d.weekday()]
        daily.append(s)

    # Top items this week
    r = await db.execute(
        select(
            TradeProfit.type_id,
            SdeType.name,
            func.sum(TradeProfit.quantity).label("qty"),
            func.sum(TradeProfit.profit).label("profit"),
        )
        .outerjoin(SdeType, SdeType.type_id == TradeProfit.type_id)
        .where(TradeProfit.date >= week_start.isoformat())
        .group_by(TradeProfit.type_id)
        .order_by(desc("profit"))
        .limit(10)
    )
    top_winners = [{"type_id": row[0], "name": row[1] or f"Type {row[0]}", "quantity": int(row[2] or 0), "profit": float(row[3] or 0)} for row in r.all()]

    r = await db.execute(
        select(
            TradeProfit.type_id,
            SdeType.name,
            func.sum(TradeProfit.quantity).label("qty"),
            func.sum(TradeProfit.profit).label("profit"),
        )
        .outerjoin(SdeType, SdeType.type_id == TradeProfit.type_id)
        .where(TradeProfit.date >= week_start.isoformat())
        .group_by(TradeProfit.type_id)
        .order_by("profit")
        .limit(10)
    )
    top_losers = [{"type_id": row[0], "name": row[1] or f"Type {row[0]}", "quantity": int(row[2] or 0), "profit": float(row[3] or 0)} for row in r.all() if (row[3] or 0) < 0]

    return {
        "this_week": await period_summary(week_start),
        "last_week": await period_summary(last_week_start, week_start),
        "this_month": await period_summary(month_start),
        "all_time": await period_summary(dt_date(2020, 1, 1)),
        "daily": daily,
        "top_winners": top_winners,
        "top_losers": top_losers,
    }
