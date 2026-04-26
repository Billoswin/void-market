"""
Void Market — ESI Assets Service

Syncs all items owned by a character (paginated). Also resolves ship/container
custom names. Display-only for now — no auto-consume integration yet.
"""
import logging
from datetime import datetime, timezone
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.database import CharacterAsset, AssetName, SdeType
from app.services.esi_client import esi_client

logger = logging.getLogger("void_market.esi_assets")


class EsiAssetsService:
    async def sync_assets(self, db: AsyncSession, character_id: int) -> dict:
        """Pull all assets for a character. Paginated."""
        try:
            assets = await esi_client.get_paginated(
                f"/characters/{character_id}/assets/",
                db=db, character_id=character_id,
            )
        except Exception as e:
            logger.error(f"Failed to fetch assets for {character_id}: {e}")
            return {"error": str(e)}

        # Full replace — assets change frequently, simpler than diffing
        await db.execute(
            delete(CharacterAsset).where(CharacterAsset.character_id == character_id)
        )

        now = datetime.now(timezone.utc)
        count = 0
        ship_item_ids = []

        for asset in assets:
            item_id = asset.get("item_id")
            if not item_id:
                continue

            type_id = asset.get("type_id")
            db.add(CharacterAsset(
                item_id=item_id,
                character_id=character_id,
                type_id=type_id,
                quantity=asset.get("quantity", 1),
                location_id=asset.get("location_id"),
                location_type=asset.get("location_type"),
                location_flag=asset.get("location_flag"),
                is_singleton=asset.get("is_singleton", False),
                is_blueprint_copy=asset.get("is_blueprint_copy", False),
            ))
            count += 1

            # Collect singleton items that might have custom names
            # (ships and containers are singleton=true)
            if asset.get("is_singleton") and type_id:
                ship_item_ids.append(item_id)

        await db.flush()

        # Resolve custom names (batched, 1000 per call)
        named_count = 0
        if ship_item_ids:
            await db.execute(
                delete(AssetName).where(AssetName.character_id == character_id)
            )
            try:
                for i in range(0, len(ship_item_ids), 1000):
                    batch = ship_item_ids[i:i+1000]
                    names = await esi_client.post(
                        f"/characters/{character_id}/assets/names/",
                        db=db, json_data=batch, character_id=character_id,
                    )
                    for n in names:
                        item_id = n.get("item_id")
                        name = n.get("name", "")
                        # ESI returns "None" as the name for unnamed items — skip them
                        if name and name != "None":
                            db.add(AssetName(
                                item_id=item_id,
                                character_id=character_id,
                                name=name,
                            ))
                            named_count += 1
            except Exception as e:
                logger.warning(f"Failed to resolve asset names for {character_id}: {e}")

        await db.commit()

        # Resolve unknown locations
        resolved = await self._resolve_asset_locations(db, character_id)

        logger.info(f"Synced {count} assets, {named_count} named, {resolved} locations resolved for char {character_id}")
        return {"count": count, "named": named_count, "locations_resolved": resolved}

    async def _resolve_asset_locations(self, db: AsyncSession, character_id: int) -> int:
        """
        Resolve all unique location_ids from assets that aren't cached yet.
        Three types:
          - NPC stations (60000000-69999999): GET /universe/stations/{id}/
          - Player structures (>1000000000000): GET /universe/structures/{id}/
          - Items (ships/containers): look up parent asset in character_assets
        """
        from app.models.database import LocationCache, TrackedStructure
        from sqlalchemy import distinct

        # Get all unique location_ids from this character's assets
        r = await db.execute(
            select(distinct(CharacterAsset.location_id))
            .where(CharacterAsset.character_id == character_id)
        )
        all_loc_ids = [row[0] for row in r.all() if row[0]]

        # Get already-resolved IDs
        cached_r = await db.execute(select(LocationCache.location_id))
        cached = {row[0] for row in cached_r.all()}

        struct_r = await db.execute(select(TrackedStructure.structure_id))
        structs = {row[0] for row in struct_r.all()}

        # Also get all item_ids from character_assets (for items-inside-items resolution)
        items_r = await db.execute(
            select(CharacterAsset.item_id, CharacterAsset.type_id)
            .where(CharacterAsset.character_id == character_id)
        )
        asset_items = {row[0]: row[1] for row in items_r.all()}

        resolved = 0
        for loc_id in all_loc_ids:
            if loc_id in cached or loc_id in structs:
                continue

            station_name = None
            system_id = None
            system_name = None

            try:
                if 60000000 <= loc_id <= 69999999:
                    # NPC station
                    resp = await esi_client.get(
                        f"/universe/stations/{loc_id}/",
                        db=db, authenticated=False,
                    )
                    station_name = resp.get("name")
                    system_id = resp.get("system_id")

                elif loc_id in asset_items:
                    # Item inside another item (ship/container)
                    parent_type_id = asset_items[loc_id]
                    parent_type = await db.get(SdeType, parent_type_id)
                    # Check for custom name
                    name_r = await db.execute(
                        select(AssetName.name).where(AssetName.item_id == loc_id)
                    )
                    custom_name = name_r.scalar()
                    type_name = parent_type.name if parent_type else f"Type {parent_type_id}"
                    if custom_name:
                        station_name = f"{type_name} \"{custom_name}\""
                    else:
                        station_name = type_name
                    # Try to find the parent's parent to get the actual system
                    parent_asset_r = await db.execute(
                        select(CharacterAsset.location_id)
                        .where(CharacterAsset.item_id == loc_id)
                    )
                    parent_loc = parent_asset_r.scalar()
                    if parent_loc:
                        # Check if parent's location is cached
                        ploc = await db.get(LocationCache, parent_loc)
                        if ploc:
                            system_name = ploc.solar_system_name
                            system_id = ploc.solar_system_id

                elif loc_id > 1000000000000:
                    # Player structure
                    try:
                        resp = await esi_client.get(
                            f"/universe/structures/{loc_id}/",
                            db=db, character_id=character_id,
                        )
                        station_name = resp.get("name")
                        system_id = resp.get("solar_system_id")
                    except Exception:
                        station_name = f"Structure {loc_id}"

                else:
                    station_name = f"Location {loc_id}"

                # Resolve system name
                if system_id and not system_name:
                    try:
                        sys_resp = await esi_client.get(
                            f"/universe/systems/{system_id}/",
                            db=db, authenticated=False,
                        )
                        system_name = sys_resp.get("name")
                    except Exception:
                        pass

                # Cache it
                if station_name:
                    existing = await db.get(LocationCache, loc_id)
                    if existing:
                        existing.station_name = station_name
                        existing.solar_system_id = system_id
                        existing.solar_system_name = system_name
                    else:
                        db.add(LocationCache(
                            location_id=loc_id,
                            solar_system_id=system_id,
                            solar_system_name=system_name,
                            station_name=station_name,
                        ))
                    resolved += 1

            except Exception as e:
                logger.debug(f"Could not resolve location {loc_id}: {e}")

        if resolved > 0:
            await db.commit()
        return resolved


esi_assets_service = EsiAssetsService()
