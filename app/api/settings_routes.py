"""
Void Market — Settings & Tools API Routes

Config:
- Broker fee overrides (per-character, time-stamped)
- Warehouse / manual stock management
- Structure rig configuration
- Default price region

Tools:
- Appraisal (parse EFT fits, item lists, cargo scans)
- Margin Calculator
"""
import logging
from datetime import datetime, timezone
from fastapi import APIRouter, Depends, Query, Form, Body
from sqlalchemy import select, func, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.session import get_db
from app.models.database import (
    BrokerFeeOverride, WarehouseItem, StructureRig,
    AppSetting, SdeType, JitaPrice, MarketOrder,
)
from app.services.appraisal import appraisal_service
from app.config import settings

logger = logging.getLogger("void_market.settings_routes")

settings_router = APIRouter(prefix="/settings", tags=["settings"])
tools_router = APIRouter(prefix="/tools", tags=["tools"])


# ─── Broker Fee Configuration ──────────────────────────────

@settings_router.get("/broker-fees")
async def get_broker_fees(db: AsyncSession = Depends(get_db)):
    """Get all broker fee overrides."""
    result = await db.execute(
        select(BrokerFeeOverride).order_by(BrokerFeeOverride.effective_from.desc())
    )
    fees = result.scalars().all()
    return [{
        "id": f.id,
        "character_id": f.character_id,
        "type_id": f.type_id,
        "location_id": f.location_id,
        "buy_rate": f.buy_rate,
        "sell_structure_rate": f.sell_structure_rate,
        "sell_npc_rate": f.sell_npc_rate,
        "sales_tax_rate": f.sales_tax_rate,
        "effective_from": f.effective_from.isoformat() if f.effective_from else None,
    } for f in fees]


@settings_router.post("/broker-fees")
async def add_broker_fee(
    character_id: int = Form(None),
    type_id: int = Form(None),
    location_id: int = Form(None),
    buy_rate: float = Form(1.0),
    sell_structure_rate: float = Form(1.0),
    sell_npc_rate: float = Form(1.5),
    sales_tax_rate: float = Form(3.36),
    effective_from: str = Form(None),
    db: AsyncSession = Depends(get_db),
):
    """Add a broker fee override."""
    eff = datetime.fromisoformat(effective_from) if effective_from else datetime.now(timezone.utc)
    fee = BrokerFeeOverride(
        character_id=character_id,
        type_id=type_id,
        location_id=location_id,
        buy_rate=buy_rate,
        sell_structure_rate=sell_structure_rate,
        sell_npc_rate=sell_npc_rate,
        sales_tax_rate=sales_tax_rate,
        effective_from=eff,
    )
    db.add(fee)
    await db.commit()
    return {"id": fee.id, "status": "added"}


@settings_router.delete("/broker-fees/{fee_id}")
async def delete_broker_fee(fee_id: int, db: AsyncSession = Depends(get_db)):
    """Delete a broker fee override."""
    fee = await db.get(BrokerFeeOverride, fee_id)
    if not fee:
        return {"error": "Not found"}
    await db.delete(fee)
    await db.commit()
    return {"status": "deleted"}


# ─── Warehouse / Manual Stock ──────────────────────────────

@settings_router.get("/warehouse")
async def get_warehouse(db: AsyncSession = Depends(get_db)):
    """Get all warehouse items."""
    result = await db.execute(
        select(WarehouseItem, SdeType.name)
        .outerjoin(SdeType, SdeType.type_id == WarehouseItem.type_id)
        .order_by(WarehouseItem.added_at.desc())
    )
    return [{
        "id": w.id,
        "type_id": w.type_id,
        "name": name or f"Type {w.type_id}",
        "quantity": w.quantity,
        "unit_price": w.unit_price,
        "is_default_price": w.is_default_price,
        "notes": w.notes,
        "added_at": w.added_at.isoformat() if w.added_at else None,
    } for w, name in result.all()]


@settings_router.post("/warehouse")
async def add_warehouse_item(
    type_id: int = Form(...),
    quantity: int = Form(...),
    unit_price: float = Form(...),
    is_default_price: bool = Form(False),
    notes: str = Form(None),
    db: AsyncSession = Depends(get_db),
):
    """Add a manual stock entry to the warehouse."""
    item = WarehouseItem(
        type_id=type_id,
        quantity=quantity,
        unit_price=unit_price,
        is_default_price=is_default_price,
        notes=notes,
    )
    db.add(item)
    await db.commit()
    return {"id": item.id, "status": "added"}


@settings_router.put("/warehouse/{item_id}")
async def update_warehouse_item(
    item_id: int,
    quantity: int = Form(None),
    unit_price: float = Form(None),
    notes: str = Form(None),
    db: AsyncSession = Depends(get_db),
):
    """Update a warehouse item."""
    item = await db.get(WarehouseItem, item_id)
    if not item:
        return {"error": "Not found"}
    if quantity is not None:
        item.quantity = quantity
    if unit_price is not None:
        item.unit_price = unit_price
    if notes is not None:
        item.notes = notes
    await db.commit()
    return {"status": "updated"}


@settings_router.delete("/warehouse/{item_id}")
async def delete_warehouse_item(item_id: int, db: AsyncSession = Depends(get_db)):
    """Delete a warehouse item."""
    item = await db.get(WarehouseItem, item_id)
    if not item:
        return {"error": "Not found"}
    await db.delete(item)
    await db.commit()
    return {"status": "deleted"}


# ─── Structure Rig Configuration ───────────────────────────

@settings_router.get("/structure-rigs")
async def get_structure_rigs(db: AsyncSession = Depends(get_db)):
    """Get structure rig configurations."""
    result = await db.execute(select(StructureRig).order_by(StructureRig.structure_id))
    return [{
        "id": r.id,
        "structure_id": r.structure_id,
        "structure_name": r.structure_name,
        "security_status": r.security_status,
        "rig_slot": r.rig_slot,
        "rig_type_id": r.rig_type_id,
    } for r in result.scalars().all()]


@settings_router.post("/structure-rigs")
async def add_structure_rig(
    structure_id: int = Form(...),
    structure_name: str = Form(None),
    security_status: str = Form("null"),
    rig_slot: int = Form(...),
    rig_type_id: int = Form(None),
    db: AsyncSession = Depends(get_db),
):
    """Add/update a structure rig configuration."""
    # Check if this slot already exists
    result = await db.execute(
        select(StructureRig)
        .where(StructureRig.structure_id == structure_id, StructureRig.rig_slot == rig_slot)
    )
    existing = result.scalar_one_or_none()
    if existing:
        existing.rig_type_id = rig_type_id
        existing.structure_name = structure_name
        existing.security_status = security_status
    else:
        db.add(StructureRig(
            structure_id=structure_id, structure_name=structure_name,
            security_status=security_status, rig_slot=rig_slot, rig_type_id=rig_type_id,
        ))
    await db.commit()
    return {"status": "saved"}


# ─── Default Price Region ──────────────────────────────────

@settings_router.get("/default-price-region")
async def get_default_price_region(db: AsyncSession = Depends(get_db)):
    """Get default price fallback region."""
    setting = await db.get(AppSetting, "default_price_region")
    return {"region": setting.value if setting else "The Forge"}


@settings_router.post("/default-price-region")
async def set_default_price_region(region: str = Form(...), db: AsyncSession = Depends(get_db)):
    """Set default price fallback region."""
    existing = await db.get(AppSetting, "default_price_region")
    if existing:
        existing.value = region
    else:
        db.add(AppSetting(key="default_price_region", value=region))
    await db.commit()
    return {"status": "saved", "region": region}


# ─── Appraisal Tool ───────────────────────────────────────

@tools_router.post("/appraisal")
async def run_appraisal(
    text: str = Form(...),
    db: AsyncSession = Depends(get_db),
):
    """
    Appraise items from text input.
    Accepts EFT fits, item lists, cargo scans, multibuy format.
    Returns per-item Jita/Import/Local prices.
    """
    return await appraisal_service.appraise(db, text)


# ─── Margin Calculator ────────────────────────────────────

@tools_router.get("/margin")
async def margin_calculator(
    buy_price: float = Query(...),
    sell_price: float = Query(...),
    buy_broker_pct: float = Query(1.0),
    sell_broker_pct: float = Query(1.0),
    sales_tax_pct: float = Query(3.36),
):
    """
    Simple margin calculator.
    Returns profit, margin, and fees for a buy→sell trade.
    """
    buy_broker = buy_price * (buy_broker_pct / 100)
    sell_broker = sell_price * (sell_broker_pct / 100)
    sales_tax = sell_price * (sales_tax_pct / 100)
    total_fees = buy_broker + sell_broker + sales_tax
    profit = sell_price - buy_price - total_fees
    margin = (profit / sell_price * 100) if sell_price > 0 else 0

    return {
        "buy_price": buy_price,
        "sell_price": sell_price,
        "buy_broker": round(buy_broker, 2),
        "sell_broker": round(sell_broker, 2),
        "sales_tax": round(sales_tax, 2),
        "total_fees": round(total_fees, 2),
        "profit": round(profit, 2),
        "margin_pct": round(margin, 2),
    }


# ─── Linked Characters ────────────────────────────────────

@settings_router.get("/characters")
async def get_linked_characters(db: AsyncSession = Depends(get_db)):
    """Get all linked ESI characters."""
    from app.services.esi_auth import esi_auth
    chars = await esi_auth.get_all_active_characters(db)
    return [{
        "character_id": c.character_id,
        "character_name": c.character_name,
        "corporation_id": c.corporation_id,
        "alliance_id": c.alliance_id,
        "is_active": c.is_active,
        "use_jita_fallback": c.use_jita_fallback or False,
        "fallback_price_type": c.fallback_price_type or "sell_min",
        "updated_at": c.updated_at.isoformat() if c.updated_at else None,
    } for c in chars]


@settings_router.post("/characters/{character_id}/fallback")
async def update_character_fallback(
    character_id: int,
    use_jita_fallback: bool = Form(...),
    fallback_price_type: str = Form("sell_min"),
    db: AsyncSession = Depends(get_db),
):
    """Toggle Jita fallback pricing for a character."""
    from app.models.database import EsiCharacter
    char = await db.get(EsiCharacter, character_id)
    if not char:
        return {"error": "Character not found"}
    if fallback_price_type not in ("sell_min", "buy_max", "avg"):
        return {"error": "Invalid fallback_price_type"}
    char.use_jita_fallback = use_jita_fallback
    char.fallback_price_type = fallback_price_type
    await db.commit()
    return {
        "character_name": char.character_name,
        "use_jita_fallback": char.use_jita_fallback,
        "fallback_price_type": char.fallback_price_type,
    }


@settings_router.delete("/characters/{character_id}")
async def remove_character(character_id: int, db: AsyncSession = Depends(get_db)):
    """Remove a linked character (deactivate, don't delete)."""
    from app.models.database import EsiCharacter
    char = await db.get(EsiCharacter, character_id)
    if not char:
        return {"error": "Character not found"}
    char.is_active = False
    await db.commit()
    return {"removed": char.character_name, "character_id": character_id}


# ─── Freight Routes ────────────────────────────────────────

@settings_router.get("/freight-routes")
async def get_freight_routes(db: AsyncSession = Depends(get_db)):
    from app.models.database import FreightRoute
    result = await db.execute(select(FreightRoute).order_by(FreightRoute.origin_system, FreightRoute.destination_system))
    return [{
        "id": r.id,
        "origin_system": r.origin_system,
        "destination_system": r.destination_system,
        "rate_per_m3": r.rate_per_m3,
        "notes": r.notes,
    } for r in result.scalars().all()]


@settings_router.post("/freight-routes")
async def add_freight_route(
    origin_system: str = Form(...),
    destination_system: str = Form(...),
    rate_per_m3: float = Form(...),
    notes: str = Form(""),
    db: AsyncSession = Depends(get_db),
):
    from app.models.database import FreightRoute
    import httpx

    # Validate both system names against ESI
    origin = origin_system.strip()
    dest = destination_system.strip()

    async with httpx.AsyncClient(timeout=10.0) as http:
        resp = await http.post(
            "https://esi.evetech.net/latest/universe/ids/",
            params={"datasource": "tranquility"},
            json=[origin, dest],
        )
        if resp.status_code != 200:
            return {"error": "ESI lookup failed — try again"}

        data = resp.json()
        systems = {s["name"].lower(): s["name"] for s in data.get("systems", [])}

        errors = []
        if origin.lower() not in systems:
            errors.append(f"'{origin}' is not a valid solar system")
        else:
            origin = systems[origin.lower()]  # Use canonical ESI name

        if dest.lower() not in systems:
            errors.append(f"'{dest}' is not a valid solar system")
        else:
            dest = systems[dest.lower()]  # Use canonical ESI name

        if errors:
            return {"error": "; ".join(errors)}

    route = FreightRoute(
        origin_system=origin,
        destination_system=dest,
        rate_per_m3=rate_per_m3,
        notes=notes.strip() if notes else None,
    )
    db.add(route)
    await db.commit()
    return {"id": route.id, "origin_system": route.origin_system, "destination_system": route.destination_system, "rate_per_m3": route.rate_per_m3}


@settings_router.delete("/freight-routes/{route_id}")
async def delete_freight_route(route_id: int, db: AsyncSession = Depends(get_db)):
    from app.models.database import FreightRoute
    route = await db.get(FreightRoute, route_id)
    if not route:
        return {"error": "Route not found"}
    await db.delete(route)
    await db.commit()
    return {"deleted": route.destination_system}


# ─── Location Resolver ─────────────────────────────────────

@settings_router.get("/resolve-location/{location_id}")
async def resolve_location(location_id: int, db: AsyncSession = Depends(get_db)):
    """Resolve a location_id to solar system name (with caching)."""
    from app.models.database import LocationCache
    from app.services.esi_client import esi_client

    # Check cache first
    cached = await db.get(LocationCache, location_id)
    if cached:
        return {"location_id": location_id, "system_name": cached.solar_system_name, "station_name": cached.station_name}

    system_name = None
    station_name = None
    system_id = None

    try:
        # Try as NPC station first
        if location_id < 100000000:
            resp = await esi_client.get(f"/universe/stations/{location_id}/", db=db, authenticated=False)
            station_name = resp.get("name")
            system_id = resp.get("system_id")
        else:
            # Player structure — needs auth
            resp = await esi_client.get(f"/universe/structures/{location_id}/", db=db)
            station_name = resp.get("name")
            system_id = resp.get("solar_system_id")

        if system_id:
            sys_resp = await esi_client.get(f"/universe/systems/{system_id}/", db=db, authenticated=False)
            system_name = sys_resp.get("name")
    except Exception as e:
        logger.warning(f"Could not resolve location {location_id}: {e}")

    # Cache it
    cache = LocationCache(
        location_id=location_id,
        solar_system_id=system_id,
        solar_system_name=system_name,
        station_name=station_name,
    )
    db.add(cache)
    await db.commit()

    return {"location_id": location_id, "system_name": system_name, "station_name": station_name}

# ─── Tracked Structures ───────────────────────────────────

@settings_router.get("/structures")
async def list_tracked_structures(db: AsyncSession = Depends(get_db)):
    """List all tracked structures."""
    from app.models.database import TrackedStructure
    r = await db.execute(select(TrackedStructure).order_by(TrackedStructure.name))
    return [{
        "structure_id": s.structure_id,
        "name": s.name,
        "solar_system_id": s.solar_system_id,
        "solar_system_name": s.solar_system_name,
        "type_id": s.type_id,
        "enabled": s.enabled,
    } for s in r.scalars().all()]


@settings_router.post("/structures")
async def add_tracked_structure(
    structure_id: int = Form(...),
    character_id: int = Form(None),
    db: AsyncSession = Depends(get_db),
):
    """Add a structure to the tracked list. Auto-resolves name via ESI."""
    from app.services.esi_structures import esi_structures_service
    from app.services.esi_auth import esi_auth as _esi
    if not character_id:
        char = await _esi.get_active_character(db)
        if not char:
            return {"error": "No linked character to resolve structure"}
        character_id = char.character_id
    return await esi_structures_service.add_tracked_structure(db, structure_id, character_id)


@settings_router.delete("/structures/{structure_id}")
async def remove_tracked_structure(structure_id: int, db: AsyncSession = Depends(get_db)):
    """Remove a tracked structure."""
    from app.models.database import TrackedStructure
    s = await db.get(TrackedStructure, structure_id)
    if not s:
        return {"error": "Not found"}
    await db.delete(s)
    await db.commit()
    return {"removed": s.name}
