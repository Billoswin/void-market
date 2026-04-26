"""Assets API routes — hierarchical tree view like EVE's in-game asset browser."""
import logging
from fastapi import APIRouter, Depends
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.session import get_db
from app.models.database import (
    EsiCharacter, CharacterAsset, AssetName, SdeType, JitaPrice,
    TrackedStructure, LocationCache,
)
from app.services.esi_assets import esi_assets_service
from app.services.esi_auth import esi_auth

logger = logging.getLogger("void_market.assets")

assets_router = APIRouter(prefix="/assets", tags=["assets"])

SHIP_CATEGORY_ID = 6


def _get_value(type_id, is_bpc, jita_map):
    if is_bpc:
        return 0
    jp = jita_map.get(type_id)
    return float(jp) if jp else 0


async def _build_lookup_maps(db: AsyncSession):
    types_r = await db.execute(select(SdeType.type_id, SdeType.name, SdeType.category_id))
    type_map = {}
    cat_map = {}
    for tid, name, cat in types_r.all():
        type_map[tid] = name
        cat_map[tid] = cat

    jita_r = await db.execute(select(JitaPrice.type_id, JitaPrice.sell_min))
    jita_map = {tid: price for tid, price in jita_r.all()}

    names_r = await db.execute(select(AssetName.item_id, AssetName.name))
    name_map = {iid: n for iid, n in names_r.all()}

    loc_r = await db.execute(select(LocationCache))
    loc_map = {l.location_id: l for l in loc_r.scalars().all()}

    struct_r = await db.execute(select(TrackedStructure))
    struct_map = {s.structure_id: s for s in struct_r.scalars().all()}

    chars_r = await db.execute(select(EsiCharacter))
    char_map = {c.character_id: c.character_name for c in chars_r.scalars().all()}

    return type_map, cat_map, jita_map, name_map, loc_map, struct_map, char_map


def _resolve_location_name(loc_id, loc_map, struct_map):
    if loc_id in struct_map:
        s = struct_map[loc_id]
        return s.name, s.solar_system_name
    if loc_id in loc_map:
        l = loc_map[loc_id]
        return l.station_name or f"Location {loc_id}", l.solar_system_name
    return f"Location {loc_id}", None


@assets_router.get("/summary")
async def get_assets_summary(db: AsyncSession = Depends(get_db)):
    chars = await esi_auth.get_all_active_characters(db)
    summary = []
    for char in chars:
        r = await db.execute(
            select(func.count(CharacterAsset.item_id), func.sum(CharacterAsset.quantity))
            .where(CharacterAsset.character_id == char.character_id)
        )
        cnt, qty = r.one()
        isk_r = await db.execute(
            select(func.sum(
                func.case(
                    (CharacterAsset.is_blueprint_copy == True, 0),
                    else_=CharacterAsset.quantity * JitaPrice.sell_min,
                )
            ))
            .join(JitaPrice, JitaPrice.type_id == CharacterAsset.type_id, isouter=True)
            .where(CharacterAsset.character_id == char.character_id)
        )
        isk = isk_r.scalar() or 0
        summary.append({
            "character_id": char.character_id,
            "character_name": char.character_name,
            "items": cnt or 0,
            "total_quantity": int(qty or 0),
            "total_isk": float(isk),
        })
    return summary


@assets_router.get("/tree")
async def get_asset_tree(character_id: int = None, db: AsyncSession = Depends(get_db)):
    """
    Hierarchical asset tree: System > Station > Hangar sections > Ships (with contents).
    Walks location_id chains to find root stations, nests ship contents.
    """
    type_map, cat_map, jita_map, name_map, loc_map, struct_map, char_map = await _build_lookup_maps(db)

    q = select(CharacterAsset)
    if character_id:
        q = q.where(CharacterAsset.character_id == character_id)
    r = await db.execute(q)
    all_assets = r.scalars().all()

    by_item_id = {}
    by_location = {}
    for a in all_assets:
        by_item_id[a.item_id] = a
        by_location.setdefault(a.location_id, []).append(a)

    def asset_to_dict(a):
        tid = a.type_id
        is_bpc = a.is_blueprint_copy
        uv = _get_value(tid, is_bpc, jita_map)
        return {
            "item_id": a.item_id,
            "type_id": tid,
            "name": type_map.get(tid, f"Type {tid}"),
            "custom_name": name_map.get(a.item_id),
            "quantity": int(a.quantity),
            "flag": a.location_flag or "",
            "is_singleton": a.is_singleton,
            "is_bpc": is_bpc,
            "is_ship": cat_map.get(tid) == SHIP_CATEGORY_ID,
            "unit_value": uv,
            "total_value": uv * a.quantity,
        }

    def get_contents(parent_item_id):
        children = by_location.get(parent_item_id, [])
        contents = []
        for c in children:
            d = asset_to_dict(c)
            sub = by_location.get(c.item_id, [])
            if sub:
                d["contents"] = [asset_to_dict(s) for s in sub]
            contents.append(d)
        return sorted(contents, key=lambda x: x["flag"])

    # Group top-level items by root station
    stations = {}
    for a in all_assets:
        # Skip items inside other items (they'll be nested)
        if a.location_id in by_item_id:
            continue

        key = (a.location_id, a.character_id)
        if key not in stations:
            stations[key] = {}
        flag = a.location_flag or "Other"
        stations[key].setdefault(flag, []).append(a)

    # Build output
    systems = {}
    flag_order = ["Hangar", "ShipHangar", "CorpDeliveries", "Deliveries",
                  "FleetHangar", "FighterBay", "AssetSafety"]

    for (root_id, char_id), flag_groups in stations.items():
        station_name, system_name = _resolve_location_name(root_id, loc_map, struct_map)
        system_name = system_name or "(Unknown)"

        hangars = {}
        station_total = 0
        station_items = 0

        for flag, items in flag_groups.items():
            section = []
            for a in items:
                d = asset_to_dict(a)
                station_total += d["total_value"]
                station_items += 1
                if d["is_ship"] and a.is_singleton:
                    contents = get_contents(a.item_id)
                    d["contents"] = contents
                    for c in contents:
                        station_total += c["total_value"]
                        station_items += 1
                        for sc in c.get("contents", []):
                            station_total += sc["total_value"]
                            station_items += 1
                elif a.item_id in by_location:
                    # Container or other item with stuff inside
                    contents = get_contents(a.item_id)
                    d["contents"] = contents
                    d["is_container"] = True
                    for c in contents:
                        station_total += c["total_value"]
                        station_items += 1
                        for sc in c.get("contents", []):
                            station_total += sc["total_value"]
                            station_items += 1
                section.append(d)
            section.sort(key=lambda x: -x["total_value"])
            hangars[flag] = section

        sorted_hangars = {}
        for f in flag_order:
            if f in hangars:
                sorted_hangars[f] = hangars.pop(f)
        for f in sorted(hangars.keys()):
            sorted_hangars[f] = hangars[f]

        station_entry = {
            "station_id": root_id,
            "station_name": station_name,
            "character_id": char_id,
            "character_name": char_map.get(char_id, "Unknown"),
            "total_isk": station_total,
            "item_count": station_items,
            "hangars": sorted_hangars,
        }
        systems.setdefault(system_name, []).append(station_entry)

    result = []
    for sys_name in sorted(systems.keys(), key=lambda s: -sum(st["total_isk"] for st in systems[s])):
        stations_list = sorted(systems[sys_name], key=lambda s: -s["total_isk"])
        result.append({
            "system": sys_name,
            "total_isk": sum(s["total_isk"] for s in stations_list),
            "stations": stations_list,
        })
    return result


@assets_router.get("")
async def list_assets(character_id: int = None, location_id: int = None, limit: int = 500, db: AsyncSession = Depends(get_db)):
    q = (
        select(CharacterAsset, SdeType.name, JitaPrice.sell_min, AssetName.name.label("custom_name"))
        .join(SdeType, SdeType.type_id == CharacterAsset.type_id, isouter=True)
        .join(JitaPrice, JitaPrice.type_id == CharacterAsset.type_id, isouter=True)
        .join(AssetName, AssetName.item_id == CharacterAsset.item_id, isouter=True)
    )
    if character_id:
        q = q.where(CharacterAsset.character_id == character_id)
    if location_id:
        q = q.where(CharacterAsset.location_id == location_id)
    q = q.limit(limit)
    r = await db.execute(q)
    return [{
        "item_id": a.item_id, "type_id": a.type_id,
        "name": name or f"Type {a.type_id}", "custom_name": cn,
        "quantity": int(a.quantity), "location_flag": a.location_flag,
        "is_bpc": a.is_blueprint_copy,
        "unit_value": 0 if a.is_blueprint_copy else float(sm or 0),
        "total_value": 0 if a.is_blueprint_copy else float((sm or 0) * a.quantity),
    } for a, name, sm, cn in r.all()]


@assets_router.get("/locations")
async def list_asset_locations(db: AsyncSession = Depends(get_db)):
    type_map, cat_map, jita_map, name_map, loc_map, struct_map, char_map = await _build_lookup_maps(db)
    r = await db.execute(
        select(CharacterAsset.location_id, CharacterAsset.character_id,
               func.count(CharacterAsset.item_id).label("cnt"))
        .group_by(CharacterAsset.location_id, CharacterAsset.character_id)
    )
    result = []
    for loc_id, char_id, cnt in r.all():
        name, system = _resolve_location_name(loc_id, loc_map, struct_map)
        result.append({
            "location_id": loc_id, "location_name": name, "solar_system": system,
            "character_id": char_id, "character_name": char_map.get(char_id, "Unknown"),
            "item_count": cnt or 0,
        })
    return result


@assets_router.post("/sync")
async def sync_all_assets(db: AsyncSession = Depends(get_db)):
    chars = await esi_auth.get_all_active_characters(db)
    results = []
    for char in chars:
        r = await esi_assets_service.sync_assets(db, char.character_id)
        results.append({"character_name": char.character_name, **r})
    return {"results": results}
@assets_router.get("/search")
async def search_assets(q: str = "", db: AsyncSession = Depends(get_db)):
    """
    Global asset search across all owned items. Matches:
    1. Item name (LIKE match)
    2. Market group name (group + all descendants)
    3. Keyword expansion for EVE meta-concepts (faction, deadspace, officer, etc.)
    Returns owned assets grouped by location.
    """
    if not q or len(q) < 2:
        return []

    from app.models.database import SdeMarketGroup

    # Keyword expansion for common EVE terms
    KEYWORDS = {
        'faction': ['Republic Fleet', 'Caldari Navy', 'Federation Navy', 'Imperial Navy',
                     'Shadow Serpentis', 'Domination', 'Dread Guristas', 'True Sansha', 'Dark Blood'],
        'deadspace': ['A-Type', 'B-Type', 'C-Type', 'X-Type', 'Pithum', 'Gistum', 'Gist',
                      'Core', 'Centum', 'Corpus', 'Corelum', 'Coreli', 'Pith', 'Centii', 'Corpii'],
        'officer': ['Draclira', 'Cormack', 'Setele', 'Tobias', 'Estamel', 'Kaikka',
                     'Chelm', 'Brynn', 'Ahremen', 'Brokara', 'Tairei', 'Vizan',
                     'Thon', 'Selynne', 'Tuvan', 'Gotan', 'Vepas', 'Raysere',
                     'Unit D-', 'Unit F-', 'Unit P-', 'Unit W-',
                     "'s Modified"],
        'pirate': ['Guristas', 'Serpentis', 'Sansha', 'Blood Raider', 'Angel',
                    'Mordu', 'Sisters', 'SOE'],
        'navy': ['Caldari Navy', 'Federation Navy', 'Imperial Navy', 'Republic Fleet'],
        'storyline': ['Storyline'],
        'abyssal': ['Abyssal', 'Mutated'],
        'capital': ['Capital'],
    }

    q_lower = q.strip().lower()
    search_terms = [q]

    # Add expanded terms if keyword matches
    for keyword, expansions in KEYWORDS.items():
        if q_lower == keyword or q_lower == keyword + 's':
            search_terms.extend(expansions)
            break

    # 1. Find type_ids by item name (all search terms)
    matching_type_ids = set()
    for term in search_terms:
        r = await db.execute(
            select(SdeType.type_id).where(SdeType.name.ilike(f"%{term}%"))
        )
        for row in r.all():
            matching_type_ids.add(row[0])

    # 2. Find market groups matching the query
    for term in search_terms:
        groups_r = await db.execute(
            select(SdeMarketGroup).where(SdeMarketGroup.name.ilike(f"%{term}%"))
        )
        matched_groups = [g.market_group_id for g in groups_r.scalars().all()]

        if matched_groups:
            all_groups_r = await db.execute(select(SdeMarketGroup))
            parent_map = {}
            for g in all_groups_r.scalars().all():
                parent_map.setdefault(g.parent_group_id, []).append(g.market_group_id)

            descendant_ids = set(matched_groups)
            queue = list(matched_groups)
            while queue:
                gid = queue.pop()
                for child in parent_map.get(gid, []):
                    if child not in descendant_ids:
                        descendant_ids.add(child)
                        queue.append(child)

            types_in_groups = await db.execute(
                select(SdeType.type_id).where(SdeType.market_group_id.in_(descendant_ids))
            )
            for row in types_in_groups.all():
                matching_type_ids.add(row[0])

    if not matching_type_ids:
        return []

    # 3. Find owned assets matching those type_ids
    type_map, cat_map, jita_map, name_map, loc_map, struct_map, char_map = await _build_lookup_maps(db)

    assets_r = await db.execute(
        select(CharacterAsset)
        .where(CharacterAsset.type_id.in_(matching_type_ids))
        .order_by(CharacterAsset.character_id, CharacterAsset.location_id)
    )

    # Group results by location
    by_location = {}
    for a in assets_r.scalars().all():
        loc_key = (a.location_id, a.character_id)
        if loc_key not in by_location:
            loc_name, sys_name = _resolve_location_name(a.location_id, loc_map, struct_map)
            # If location is inside another item, try to resolve parent
            if loc_name.startswith("Location ") and a.location_id in name_map:
                loc_name = name_map[a.location_id]
            by_location[loc_key] = {
                "location_name": loc_name,
                "solar_system": sys_name,
                "character_name": char_map.get(a.character_id, "Unknown"),
                "items": [],
            }

        tid = a.type_id
        uv = _get_value(tid, a.is_blueprint_copy, jita_map)
        by_location[loc_key]["items"].append({
            "type_id": tid,
            "name": type_map.get(tid, f"Type {tid}"),
            "custom_name": name_map.get(a.item_id),
            "quantity": int(a.quantity),
            "flag": a.location_flag or "",
            "is_bpc": a.is_blueprint_copy,
            "unit_value": uv,
            "total_value": uv * a.quantity,
        })

    # Build output sorted by total value
    result = []
    for (loc_id, char_id), data in by_location.items():
        total = sum(i["total_value"] for i in data["items"])
        # Merge duplicates within each location
        merged = {}
        for i in data["items"]:
            key = str(i["type_id"]) + ("_bpc" if i["is_bpc"] else "")
            if key in merged:
                merged[key]["quantity"] += i["quantity"]
                merged[key]["total_value"] += i["total_value"]
            else:
                merged[key] = dict(i)
        data["items"] = sorted(merged.values(), key=lambda x: -x["total_value"])
        data["total_isk"] = total
        result.append(data)

    return sorted(result, key=lambda x: -x["total_isk"])
