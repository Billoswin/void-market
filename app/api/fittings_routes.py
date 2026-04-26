"""Fittings API routes."""
import logging
from fastapi import APIRouter, Depends, HTTPException, Form
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.session import get_db
from app.models.database import (
    EsiCharacter, CharacterFitting, CharacterFittingItem,
    SdeType, Doctrine, DoctrineFit, DoctrineFitItem,
)
from app.services.esi_fittings import esi_fittings_service
from app.services.esi_auth import esi_auth

logger = logging.getLogger("void_market.fittings")

fittings_router = APIRouter(prefix="/fittings", tags=["fittings"])


@fittings_router.get("")
async def list_fittings(db: AsyncSession = Depends(get_db)):
    """List all saved fittings across all characters."""
    r = await db.execute(
        select(CharacterFitting, EsiCharacter.character_name)
        .join(EsiCharacter, EsiCharacter.character_id == CharacterFitting.character_id)
        .order_by(CharacterFitting.name)
    )
    result = []
    for fit, char_name in r.all():
        ship_type = await db.get(SdeType, fit.ship_type_id)
        # Get items
        items_r = await db.execute(
            select(CharacterFittingItem).where(CharacterFittingItem.fitting_id == fit.fitting_id)
        )
        items = items_r.scalars().all()
        result.append({
            "fitting_id": fit.fitting_id,
            "character_id": fit.character_id,
            "character_name": char_name,
            "name": fit.name,
            "description": fit.description or "",
            "ship_type_id": fit.ship_type_id,
            "ship_name": ship_type.name if ship_type else "Unknown",
            "item_count": len(items),
        })
    return result


@fittings_router.get("/{fitting_id}")
async def get_fitting_detail(fitting_id: int, db: AsyncSession = Depends(get_db)):
    """Get full details of a single fitting."""
    fit = await db.get(CharacterFitting, fitting_id)
    if not fit:
        raise HTTPException(404, "Fitting not found")

    ship_type = await db.get(SdeType, fit.ship_type_id)
    r = await db.execute(
        select(CharacterFittingItem).where(CharacterFittingItem.fitting_id == fitting_id)
    )
    items = []
    for item in r.scalars().all():
        t = await db.get(SdeType, item.type_id)
        items.append({
            "type_id": item.type_id,
            "name": t.name if t else f"Type {item.type_id}",
            "flag": item.flag,
            "quantity": item.quantity,
        })

    return {
        "fitting_id": fit.fitting_id,
        "name": fit.name,
        "description": fit.description or "",
        "ship_type_id": fit.ship_type_id,
        "ship_name": ship_type.name if ship_type else "Unknown",
        "items": items,
    }


@fittings_router.post("/sync")
async def sync_all_fittings(db: AsyncSession = Depends(get_db)):
    """On-demand sync of fittings for all linked characters."""
    chars = await esi_auth.get_all_active_characters(db)
    results = []
    for char in chars:
        r = await esi_fittings_service.sync_fittings(db, char.character_id)
        results.append({"character_name": char.character_name, **r})
    return {"results": results}


@fittings_router.post("/{fitting_id}/import-doctrine")
async def import_as_doctrine_fit(
    fitting_id: int,
    doctrine_id: int = Form(...),
    db: AsyncSession = Depends(get_db),
):
    """Convert an ESI fitting into a doctrine fit under the specified doctrine."""
    fit = await db.get(CharacterFitting, fitting_id)
    if not fit:
        raise HTTPException(404, "Fitting not found")

    doctrine = await db.get(Doctrine, doctrine_id)
    if not doctrine:
        raise HTTPException(404, "Doctrine not found")

    # Get ship name + items
    ship_type = await db.get(SdeType, fit.ship_type_id)
    ship_name = ship_type.name if ship_type else f"Type {fit.ship_type_id}"

    r = await db.execute(
        select(CharacterFittingItem).where(CharacterFittingItem.fitting_id == fitting_id)
    )
    items = r.scalars().all()

    # ESI flag → slot_type mapping
    def flag_to_slot(flag: str) -> str:
        if flag.startswith("HiSlot"):
            return "high"
        if flag.startswith("MedSlot"):
            return "mid"
        if flag.startswith("LoSlot"):
            return "low"
        if flag.startswith("RigSlot"):
            return "rig"
        if flag.startswith("SubSystem"):
            return "subsystem"
        if flag == "DroneBay":
            return "drone"
        if flag == "Cargo":
            return "cargo"
        if flag.startswith("FighterBay") or flag.startswith("FighterTube"):
            return "fighter"
        return "cargo"

    # Reconstruct an EFT-style text block
    eft_lines = [f"[{ship_name}, {fit.name}]"]
    slot_groups = {"low": [], "mid": [], "high": [], "rig": [], "subsystem": [], "drone": [], "cargo": [], "fighter": []}
    for item in items:
        t = await db.get(SdeType, item.type_id)
        name = t.name if t else f"Type {item.type_id}"
        slot = flag_to_slot(item.flag)
        if slot in ("drone", "cargo", "fighter"):
            slot_groups[slot].append(f"{name} x{item.quantity}")
        else:
            slot_groups[slot].append(name)
    # Order: low → mid → high → rig → subsystem → drones → fighters → cargo
    for slot in ("low", "mid", "high", "rig", "subsystem"):
        for line in slot_groups[slot]:
            eft_lines.append(line)
        if slot_groups[slot]:
            eft_lines.append("")
    if slot_groups["drone"]:
        eft_lines.extend(slot_groups["drone"])
        eft_lines.append("")
    if slot_groups["fighter"]:
        eft_lines.extend(slot_groups["fighter"])
        eft_lines.append("")
    if slot_groups["cargo"]:
        eft_lines.extend(slot_groups["cargo"])
    eft_text = "\n".join(eft_lines)

    # Create doctrine fit
    df = DoctrineFit(
        doctrine_id=doctrine_id,
        name=fit.name,
        ship_type_id=fit.ship_type_id,
        eft_text=eft_text,
    )
    db.add(df)
    await db.flush()

    # Copy items
    for item in items:
        db.add(DoctrineFitItem(
            fit_id=df.id,
            type_id=item.type_id,
            quantity=item.quantity,
            slot_type=flag_to_slot(item.flag),
        ))

    await db.commit()
    return {"created_fit_id": df.id, "name": df.name}
