"""Mining ledger API routes."""
import logging
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.session import get_db
from app.models.database import MiningLedger, SdeType, JitaPrice, EsiCharacter
from app.services.esi_mining import esi_mining_service
from app.services.esi_auth import esi_auth

logger = logging.getLogger("void_market.mining")

mining_router = APIRouter(prefix="/mining", tags=["mining"])


@mining_router.get("/ledger")
async def get_ledger(days: int = 30, db: AsyncSession = Depends(get_db)):
    """Recent mining yield grouped by type."""
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    r = await db.execute(
        select(
            MiningLedger.type_id,
            SdeType.name,
            func.sum(MiningLedger.quantity).label("total_qty"),
            func.sum(MiningLedger.quantity_consumed).label("total_consumed"),
            JitaPrice.sell_min,
        )
        .join(SdeType, SdeType.type_id == MiningLedger.type_id, isouter=True)
        .join(JitaPrice, JitaPrice.type_id == MiningLedger.type_id, isouter=True)
        .where(MiningLedger.date >= cutoff)
        .group_by(MiningLedger.type_id)
        .order_by(func.sum(MiningLedger.quantity).desc())
    )
    result = []
    for row in r.all():
        qty = int(row.total_qty or 0)
        consumed = int(row.total_consumed or 0)
        price = float(row.sell_min or 0)
        result.append({
            "type_id": row.type_id,
            "name": row.name or f"Type {row.type_id}",
            "quantity": qty,
            "consumed": consumed,
            "remaining": qty - consumed,
            "unit_price": price,
            "total_value": price * qty,
        })
    return result


@mining_router.get("/summary")
async def get_summary(days: int = 7, db: AsyncSession = Depends(get_db)):
    """Total mining ISK value over a period."""
    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
    r = await db.execute(
        select(func.sum(MiningLedger.quantity * JitaPrice.sell_min))
        .join(JitaPrice, JitaPrice.type_id == MiningLedger.type_id, isouter=True)
        .where(MiningLedger.date >= cutoff)
    )
    total_isk = r.scalar() or 0
    return {"days": days, "total_isk": float(total_isk)}


@mining_router.post("/sync")
async def sync_all_mining(db: AsyncSession = Depends(get_db)):
    """Sync mining ledger for all linked characters."""
    chars = await esi_auth.get_all_active_characters(db)
    results = []
    for char in chars:
        r = await esi_mining_service.sync_mining(db, char.character_id)
        results.append({"character_name": char.character_name, **r})
    return {"results": results}
