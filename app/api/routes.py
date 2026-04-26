"""
Void Market — API Routes
"""
import logging
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, Request
from fastapi.responses import RedirectResponse, JSONResponse
from sqlalchemy import select, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.session import get_db
from app.models.database import (
    Doctrine, DoctrineFit, DoctrineFitItem, SdeType,
    EsiCharacter, MarketOrder, JitaPrice,
)
from app.services.esi_auth import esi_auth
from app.services.sde_service import sde_service
from app.parsers.eft_parser import parse_eft, parse_eft_multi, resolve_fit_type_ids
from app.config import settings

logger = logging.getLogger("void_market.api")

router = APIRouter(prefix="/api")


# ─── Health / Status ────────────────────────────────────────

@router.get("/status")
async def get_status(db: AsyncSession = Depends(get_db)):
    """Overall system status."""
    sde_stats = await sde_service.get_sde_stats(db)
    char = await esi_auth.get_active_character(db)

    doctrine_count = (await db.execute(
        select(func.count()).select_from(Doctrine)
    )).scalar() or 0

    fit_count = (await db.execute(
        select(func.count()).select_from(DoctrineFit)
    )).scalar() or 0

    order_count = (await db.execute(
        select(func.count()).select_from(MarketOrder)
    )).scalar() or 0

    return {
        "app": settings.app_name,
        "version": settings.app_version,
        "sde": sde_stats,
        "auth": {
            "authenticated": char is not None,
            "character_name": char.character_name if char else None,
            "character_id": char.character_id if char else None,
            "alliance_id": char.alliance_id if char else None,
        },
        "data": {
            "doctrines": doctrine_count,
            "fits": fit_count,
            "market_orders": order_count,
        },
        "config": {
            "keepstar_id": settings.keepstar_structure_id,
            "alliance_id": settings.alliance_id,
        }
    }


# ─── ESI Auth ───────────────────────────────────────────────

auth_router = APIRouter(prefix="/auth")


@auth_router.get("/login")
async def esi_login():
    """Redirect to EVE SSO login."""
    if not settings.esi_client_id:
        raise HTTPException(400, "ESI client_id not configured. Set VM_ESI_CLIENT_ID in .env")
    url = esi_auth.get_auth_url()
    return RedirectResponse(url)


@auth_router.get("/callback")
async def esi_callback(code: str, state: str = "", db: AsyncSession = Depends(get_db)):
    """Handle SSO callback."""
    try:
        char = await esi_auth.handle_callback(code, db)
        # Redirect back to the app with success
        return RedirectResponse(f"/?auth=success&char={char.character_name}")
    except Exception as e:
        logger.error(f"SSO callback error: {e}", exc_info=True)
        return RedirectResponse(f"/?auth=error&msg={str(e)}")


@auth_router.get("/character")
async def get_character(db: AsyncSession = Depends(get_db)):
    """Get the currently authenticated character."""
    char = await esi_auth.get_active_character(db)
    if not char:
        return {"authenticated": False}
    return {
        "authenticated": True,
        "character_id": char.character_id,
        "character_name": char.character_name,
        "corporation_id": char.corporation_id,
        "alliance_id": char.alliance_id,
    }


@auth_router.post("/logout")
async def esi_logout(db: AsyncSession = Depends(get_db)):
    """Deactivate the current character."""
    char = await esi_auth.get_active_character(db)
    if char:
        char.is_active = False
        await db.commit()
    return {"logged_out": True}


# ─── SDE Management ────────────────────────────────────────

sde_router = APIRouter(prefix="/sde")


@sde_router.get("/status")
async def sde_status(db: AsyncSession = Depends(get_db)):
    """Get SDE loading status."""
    stats = await sde_service.get_sde_stats(db)
    return {
        "loaded": stats["types"] > 0,
        "stats": stats,
        "lookups_ready": len(sde_service.type_name_lookup) > 0,
    }


@sde_router.post("/upload")
async def upload_sde_file(
    file: UploadFile = File(...),
    file_type: str = Form("auto"),
    db: AsyncSession = Depends(get_db),
):
    """
    Upload SDE data. Supports:
    - A .zip containing multiple .jsonl files (set file_type to 'auto')
    - A single .jsonl file with file_type set to: types, groups, categories, blueprints, marketGroups

    For zip upload, files are processed in the correct order automatically.
    """
    content = await file.read()
    filename = file.filename or ""

    # Zip upload — process all files inside
    if filename.endswith(".zip") or file_type == "auto":
        try:
            result = await sde_service.ingest_zip(db, content)
            return {"mode": "zip", "results": result}
        except Exception as e:
            logger.error(f"SDE zip ingest error: {e}", exc_info=True)
            raise HTTPException(500, f"Failed to ingest zip: {str(e)}")

    # Single JSONL file
    valid_types = {"types", "groups", "categories", "blueprints", "marketGroups"}
    if file_type not in valid_types:
        raise HTTPException(400, f"file_type must be one of: {valid_types}")

    try:
        result = await sde_service.ingest_bytes(db, content, file_type)
    except Exception as e:
        logger.error(f"SDE ingest error: {e}", exc_info=True)
        raise HTTPException(500, f"Failed to ingest {file_type}: {str(e)}")

    return {"file_type": file_type, "result": result}


@sde_router.post("/rebuild-lookups")
async def rebuild_lookups(db: AsyncSession = Depends(get_db)):
    """Rebuild in-memory lookups from loaded SDE data."""
    await sde_service.build_lookups(db)
    return {"lookup_count": len(sde_service.type_name_lookup)}


@sde_router.get("/search")
async def search_types(q: str, limit: int = 20, db: AsyncSession = Depends(get_db)):
    """Search SDE types by name."""
    result = await db.execute(
        select(SdeType)
        .where(SdeType.name.ilike(f"%{q}%"))
        .where(SdeType.published == True)
        .limit(limit)
    )
    types = result.scalars().all()
    return [{"type_id": t.type_id, "name": t.name, "group_id": t.group_id,
             "category_id": t.category_id} for t in types]


# ─── Doctrine Management ───────────────────────────────────

doctrine_router = APIRouter(prefix="/doctrines")


@doctrine_router.get("")
async def list_doctrines(db: AsyncSession = Depends(get_db)):
    """List all doctrines with fit counts."""
    result = await db.execute(
        select(Doctrine).order_by(Doctrine.priority, Doctrine.name)
    )
    doctrines = result.scalars().all()

    output = []
    for d in doctrines:
        fit_count = (await db.execute(
            select(func.count()).select_from(DoctrineFit)
            .where(DoctrineFit.doctrine_id == d.id)
        )).scalar() or 0

        output.append({
            "id": d.id,
            "name": d.name,
            "description": d.description,
            "priority": d.priority,
            "fit_count": fit_count,
            "created_at": d.created_at.isoformat() if d.created_at else None,
        })

    return output


@doctrine_router.post("")
async def create_doctrine(
    name: str = Form(...),
    description: str = Form(""),
    priority: int = Form(5),
    db: AsyncSession = Depends(get_db),
):
    """Create a new doctrine."""
    doctrine = Doctrine(name=name, description=description, priority=priority)
    db.add(doctrine)
    await db.commit()
    await db.refresh(doctrine)
    return {"id": doctrine.id, "name": doctrine.name}


@doctrine_router.get("/{doctrine_id}")
async def get_doctrine(doctrine_id: int, db: AsyncSession = Depends(get_db)):
    """Get a doctrine with all its fits and items."""
    doctrine = await db.get(Doctrine, doctrine_id)
    if not doctrine:
        raise HTTPException(404, "Doctrine not found")

    fits_result = await db.execute(
        select(DoctrineFit).where(DoctrineFit.doctrine_id == doctrine_id)
    )
    fits = fits_result.scalars().all()

    fits_data = []
    for fit in fits:
        items_result = await db.execute(
            select(DoctrineFitItem).where(DoctrineFitItem.fit_id == fit.id)
        )
        items = items_result.scalars().all()

        # Resolve type names
        item_list = []
        for item in items:
            type_obj = await db.get(SdeType, item.type_id)
            item_list.append({
                "type_id": item.type_id,
                "name": type_obj.name if type_obj else f"Unknown ({item.type_id})",
                "quantity": item.quantity,
                "slot_type": item.slot_type,
            })

        ship_type = await db.get(SdeType, fit.ship_type_id)
        fits_data.append({
            "id": fit.id,
            "name": fit.name,
            "ship_type_id": fit.ship_type_id,
            "ship_name": ship_type.name if ship_type else "Unknown",
            "role": fit.role,
            "is_primary": fit.is_primary,
            "min_stock": fit.min_stock,
            "items": item_list,
        })

    return {
        "id": doctrine.id,
        "name": doctrine.name,
        "description": doctrine.description,
        "priority": doctrine.priority,
        "fits": fits_data,
    }


@doctrine_router.delete("/{doctrine_id}")
async def delete_doctrine(doctrine_id: int, db: AsyncSession = Depends(get_db)):
    """Delete a doctrine and all its fits."""
    doctrine = await db.get(Doctrine, doctrine_id)
    if not doctrine:
        raise HTTPException(404, "Doctrine not found")
    await db.delete(doctrine)
    await db.commit()
    return {"deleted": True}


@doctrine_router.post("/{doctrine_id}/fits")
async def add_fit_to_doctrine(
    doctrine_id: int,
    eft_text: str = Form(...),
    role: str = Form(""),
    is_primary: bool = Form(False),
    min_stock: int = Form(10),
    db: AsyncSession = Depends(get_db),
):
    """
    Add a ship fitting (EFT format) to a doctrine.
    Can contain multiple fits separated by headers.
    """
    doctrine = await db.get(Doctrine, doctrine_id)
    if not doctrine:
        raise HTTPException(404, "Doctrine not found")

    # Ensure lookups are ready
    if not sde_service.type_name_lookup:
        await sde_service.build_lookups(db)

    if not sde_service.type_name_lookup:
        raise HTTPException(400, "SDE not loaded. Upload typeIDs.json first.")

    # Parse fits
    parsed_fits = parse_eft_multi(eft_text)
    added = []

    for parsed in parsed_fits:
        # Resolve type IDs
        resolve_fit_type_ids(parsed, sde_service.type_name_lookup)

        if not parsed.ship_type_id:
            added.append({
                "name": parsed.fit_name,
                "error": f"Unknown ship: {parsed.ship_type}",
            })
            continue

        # Create the fit record
        fit = DoctrineFit(
            doctrine_id=doctrine_id,
            name=parsed.fit_name,
            ship_type_id=parsed.ship_type_id,
            eft_text=eft_text if len(parsed_fits) == 1 else _extract_single_eft(eft_text, parsed.fit_name),
            role=role or None,
            is_primary=is_primary,
            min_stock=min_stock,
        )
        db.add(fit)
        await db.flush()

        # Add items
        for item in parsed.items:
            if item.type_id:
                db.add(DoctrineFitItem(
                    fit_id=fit.id,
                    type_id=item.type_id,
                    quantity=item.quantity,
                    slot_type=item.slot_type,
                ))
            # Also add charges as separate items
            if item.charge_type_id:
                db.add(DoctrineFitItem(
                    fit_id=fit.id,
                    type_id=item.charge_type_id,
                    quantity=item.quantity,
                    slot_type="charge",
                ))

        await db.flush()
        added.append({
            "id": fit.id,
            "name": parsed.fit_name,
            "ship": parsed.ship_type,
            "item_count": len(parsed.items),
            "errors": parsed.errors if parsed.errors else None,
        })

    await db.commit()
    return {"fits_added": added}


@doctrine_router.post("/upload-bulk")
async def upload_bulk_fits(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
):
    """
    Upload a text file containing multiple EFT fits.
    Each fit should have a [Ship, Name] header.
    Fits without a matching doctrine will have one auto-created.
    """
    content = (await file.read()).decode("utf-8")
    parsed_fits = parse_eft_multi(content)

    if not sde_service.type_name_lookup:
        await sde_service.build_lookups(db)

    results = []
    for parsed in parsed_fits:
        resolve_fit_type_ids(parsed, sde_service.type_name_lookup)

        # Auto-create doctrine based on ship type if needed
        doctrine_name = f"{parsed.ship_type} Doctrine"
        result = await db.execute(
            select(Doctrine).where(Doctrine.name == doctrine_name)
        )
        doctrine = result.scalar_one_or_none()
        if not doctrine:
            doctrine = Doctrine(name=doctrine_name)
            db.add(doctrine)
            await db.flush()

        if not parsed.ship_type_id:
            results.append({"name": parsed.fit_name, "error": f"Unknown ship: {parsed.ship_type}"})
            continue

        fit = DoctrineFit(
            doctrine_id=doctrine.id,
            name=parsed.fit_name,
            ship_type_id=parsed.ship_type_id,
            eft_text=_extract_single_eft(content, parsed.fit_name),
        )
        db.add(fit)
        await db.flush()

        for item in parsed.items:
            if item.type_id:
                db.add(DoctrineFitItem(
                    fit_id=fit.id, type_id=item.type_id,
                    quantity=item.quantity, slot_type=item.slot_type,
                ))
            if item.charge_type_id:
                db.add(DoctrineFitItem(
                    fit_id=fit.id, type_id=item.charge_type_id,
                    quantity=item.quantity, slot_type="charge",
                ))

        results.append({
            "name": parsed.fit_name,
            "ship": parsed.ship_type,
            "doctrine": doctrine.name,
            "items": len(parsed.items),
        })

    await db.commit()
    return {"fits_processed": len(results), "results": results}


@doctrine_router.get("/bom/aggregate")
async def get_aggregate_bom(db: AsyncSession = Depends(get_db)):
    """
    Get the aggregate bill of materials across ALL doctrines.
    Returns total quantity needed per item, weighted by min_stock.
    """
    result = await db.execute(
        select(
            DoctrineFitItem.type_id,
            func.sum(DoctrineFitItem.quantity * DoctrineFit.min_stock).label("total_needed"),
        )
        .join(DoctrineFit, DoctrineFitItem.fit_id == DoctrineFit.id)
        .group_by(DoctrineFitItem.type_id)
        .order_by(text("total_needed DESC"))
    )
    rows = result.fetchall()

    bom = []
    for row in rows:
        type_obj = await db.get(SdeType, row[0])
        bom.append({
            "type_id": row[0],
            "name": type_obj.name if type_obj else f"Unknown ({row[0]})",
            "total_needed": row[1],
        })

    return bom


def _extract_single_eft(full_text: str, fit_name: str) -> str:
    """Extract a single fit's EFT block from a multi-fit text."""
    lines = full_text.splitlines()
    capturing = False
    block = []

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("[") and fit_name in stripped:
            capturing = True
            block = [stripped]
        elif stripped.startswith("[") and capturing:
            break
        elif capturing:
            block.append(line)

    return "\n".join(block) if block else full_text


# ─── Forum Post Import ─────────────────────────────────────

@doctrine_router.post("/import-forum")
async def import_forum_post(
    forum_text: str = Form(...),
    doctrine_name: str = Form(""),
    db: AsyncSession = Depends(get_db),
):
    """
    Import an entire forum doctrine post.
    Extracts all EFT fits, roles, and fleet composition.
    Creates a doctrine with all fits if doctrine_name is provided.
    """
    from app.parsers.forum_parser import parse_forum_post
    from app.parsers.eft_parser import parse_eft, resolve_fit_type_ids

    if not sde_service.type_name_lookup:
        await sde_service.build_lookups(db)

    if not sde_service.type_name_lookup:
        raise HTTPException(400, "SDE not loaded")

    # Parse the forum post
    parsed = parse_forum_post(forum_text, sde_service.type_name_lookup)

    # Use doctrine name from form or from parsed title
    doc_name = doctrine_name.strip() or parsed.title or "Imported Doctrine"

    # Create or find doctrine
    result = await db.execute(
        select(Doctrine).where(Doctrine.name == doc_name)
    )
    doctrine = result.scalar_one_or_none()
    if not doctrine:
        doctrine = Doctrine(name=doc_name)
        db.add(doctrine)
        await db.flush()

    results = []
    for ship in parsed.ships:
        # Parse the reconstructed EFT
        eft_fit = parse_eft(ship.eft_text)
        resolve_fit_type_ids(eft_fit, sde_service.type_name_lookup)

        if not eft_fit.ship_type_id:
            results.append({
                "name": ship.fit_name,
                "ship": ship.ship_type,
                "error": f"Unknown ship: {ship.ship_type}",
                "role": ship.role,
            })
            continue

        # Determine min_stock from fleet count
        min_stock = ship.fleet_count if ship.fleet_count > 0 else 10

        fit = DoctrineFit(
            doctrine_id=doctrine.id,
            name=ship.fit_name,
            ship_type_id=eft_fit.ship_type_id,
            eft_text=ship.eft_text,
            role=ship.role or None,
            is_primary=(ship.role == "dps"),
            min_stock=min_stock,
        )
        db.add(fit)
        await db.flush()

        # Add hull
        db.add(DoctrineFitItem(
            fit_id=fit.id,
            type_id=eft_fit.ship_type_id,
            quantity=1,
            slot_type="hull",
        ))

        # Add items
        item_count = 0
        for item in eft_fit.items:
            if item.type_id:
                db.add(DoctrineFitItem(
                    fit_id=fit.id,
                    type_id=item.type_id,
                    quantity=item.quantity,
                    slot_type=item.slot_type,
                ))
                item_count += 1
            if item.charge_type_id:
                db.add(DoctrineFitItem(
                    fit_id=fit.id,
                    type_id=item.charge_type_id,
                    quantity=item.quantity,
                    slot_type="charge",
                ))
                item_count += 1

        results.append({
            "name": ship.fit_name,
            "ship": ship.ship_type,
            "role": ship.role,
            "fleet_count": ship.fleet_count,
            "min_stock": min_stock,
            "items": item_count,
            "errors": eft_fit.errors if eft_fit.errors else None,
        })

    await db.commit()

    return {
        "doctrine": doc_name,
        "fleet_size": parsed.fleet_size,
        "ships_found": len(parsed.ships),
        "results": results,
        "parse_errors": parsed.parse_errors if parsed.parse_errors else None,
    }
