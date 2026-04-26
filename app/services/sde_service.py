"""
Void Market — SDE Ingestion Service

Loads the EVE Online SDE in JSONL (JSON Lines) format.
Each file has one JSON object per line, with _key as the ID.

Expected files:
  - types.jsonl       → SdeType
  - groups.jsonl      → group_id → category_id mapping
  - categories.jsonl  → category metadata
  - blueprints.jsonl  → SdeBlueprint + SdeBlueprintMaterial
  - marketGroups.jsonl → SdeMarketGroup

Supports:
  - Single .jsonl file upload with type selector
  - Zip upload containing multiple .jsonl files (auto-detected)
"""
import io
import json
import zipfile
import logging
from pathlib import Path
from sqlalchemy import text, select
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.database import (
    SdeType, SdeBlueprint, SdeBlueprintMaterial, SdeMarketGroup
)
from app.config import settings

logger = logging.getLogger("void_market.sde")


class SdeIngestionService:
    def __init__(self):
        self._group_to_category: dict[int, int] = {}
        self._type_name_lookup: dict[str, int] = {}
        self._loaded = False

    @property
    def type_name_lookup(self) -> dict[str, int]:
        return self._type_name_lookup

    async def ingest_zip(self, db: AsyncSession, zip_bytes: bytes) -> dict:
        """Ingest a zip containing multiple JSONL files."""
        results = {}
        
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            names = zf.namelist()
            logger.info(f"SDE zip contains: {names}")

            # Order matters: groups → categories → types → blueprints → marketGroups
            ordered = []
            for priority_name in ["groups", "categories", "types", "blueprints", "marketGroups"]:
                for name in names:
                    if name.replace(".jsonl", "") == priority_name:
                        ordered.append(name)
            
            # Add any remaining files
            for name in names:
                if name not in ordered and name.endswith(".jsonl"):
                    ordered.append(name)

            for name in ordered:
                file_type = name.replace(".jsonl", "")
                logger.info(f"Processing {name}...")
                data = zf.read(name)
                lines = data.decode("utf-8").strip().split("\n")
                result = await self._ingest_lines(db, lines, file_type)
                results[file_type] = result
                await db.commit()

        # Rebuild lookups
        await self.build_lookups(db)
        return results

    async def ingest_file(self, db: AsyncSession, file_path: Path, file_type: str) -> dict:
        """Ingest a single JSONL file."""
        logger.info(f"Ingesting SDE file: {file_path.name} (type={file_type})")

        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        result = await self._ingest_lines(db, lines, file_type)
        await db.commit()

        if file_type == "types":
            await self.build_lookups(db)

        return result

    async def ingest_bytes(self, db: AsyncSession, content: bytes, file_type: str) -> dict:
        """Ingest JSONL content from bytes."""
        lines = content.decode("utf-8").strip().split("\n")
        result = await self._ingest_lines(db, lines, file_type)
        await db.commit()

        if file_type == "types":
            await self.build_lookups(db)

        return result

    async def _ingest_lines(self, db: AsyncSession, lines: list[str], file_type: str) -> dict:
        """Route JSONL lines to the appropriate handler."""
        handlers = {
            "groups": self._ingest_groups,
            "categories": self._ingest_categories,
            "types": self._ingest_types,
            "blueprints": self._ingest_blueprints,
            "marketGroups": self._ingest_market_groups,
        }

        handler = handlers.get(file_type)
        if not handler:
            return {"skipped": file_type, "reason": "no handler"}

        parsed = []
        for line in lines:
            line = line.strip()
            if line:
                try:
                    parsed.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

        logger.info(f"Parsed {len(parsed)} records from {file_type}")
        return await handler(db, parsed)

    async def _ingest_groups(self, db: AsyncSession, records: list[dict]) -> dict:
        """Load groups.jsonl — _key=groupID, categoryID, name field."""
        from app.models.database import SdeGroup

        # Create table if not exists
        try:
            await db.execute(text("CREATE TABLE IF NOT EXISTS sde_groups (group_id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL, category_id INTEGER)"))
            await db.execute(text("DELETE FROM sde_groups"))
        except Exception:
            pass

        count = 0
        for rec in records:
            group_id = rec.get("_key")
            category_id = rec.get("categoryID")
            name_data = rec.get("name", {})
            if isinstance(name_data, dict):
                name = name_data.get("en", "")
            else:
                name = str(name_data)

            if group_id is not None and category_id is not None:
                self._group_to_category[group_id] = category_id
                if name:
                    db.add(SdeGroup(group_id=int(group_id), name=name, category_id=int(category_id)))
                count += 1

        await db.flush()
        logger.info(f"Groups: {count} group→category mappings saved")
        return {"groups_mapped": count}

    async def _ingest_categories(self, db: AsyncSession, records: list[dict]) -> dict:
        """Load categories.jsonl — stored for reference."""
        return {"categories": len(records)}

    async def _ingest_types(self, db: AsyncSession, records: list[dict]) -> dict:
        """Load types.jsonl into sde_types table."""
        await db.execute(text("DELETE FROM sde_types"))

        count = 0
        batch = []

        for rec in records:
            type_id = rec.get("_key")
            if type_id is None:
                continue

            name_data = rec.get("name", {})
            if isinstance(name_data, dict):
                name = name_data.get("en", "")
            else:
                name = str(name_data)

            if not name:
                continue

            desc_data = rec.get("description", {})
            if isinstance(desc_data, dict):
                description = desc_data.get("en", "")
            else:
                description = str(desc_data) if desc_data else ""

            group_id = rec.get("groupID")
            category_id = self._group_to_category.get(group_id) if group_id is not None else None

            batch.append(SdeType(
                type_id=type_id,
                name=name,
                description=description or None,
                group_id=group_id,
                category_id=category_id,
                market_group_id=rec.get("marketGroupID"),
                volume=rec.get("volume"),
                packaged_volume=rec.get("packagedVolume"),
                portion_size=rec.get("portionSize", 1),
                meta_level=rec.get("metaLevel"),
                published=rec.get("published", False),
            ))
            count += 1

            if len(batch) >= 1000:
                db.add_all(batch)
                await db.flush()
                batch = []

        if batch:
            db.add_all(batch)
            await db.flush()

        logger.info(f"Types: {count} loaded")
        return {"types_loaded": count}

    async def _ingest_blueprints(self, db: AsyncSession, records: list[dict]) -> dict:
        """Load blueprints.jsonl into sde_blueprints and sde_blueprint_materials."""
        await db.execute(text("DELETE FROM sde_blueprint_materials"))
        await db.execute(text("DELETE FROM sde_blueprints"))

        bp_count = 0
        mat_count = 0

        for rec in records:
            bp_id = rec.get("_key") or rec.get("blueprintTypeID")
            if not bp_id:
                continue

            activities = rec.get("activities", {})
            mfg = activities.get("manufacturing")
            if not mfg:
                continue

            products = mfg.get("products", [])
            if not products:
                continue

            product = products[0]
            product_type_id = product.get("typeID")
            product_qty = product.get("quantity", 1)
            if not product_type_id:
                continue

            bp = SdeBlueprint(
                blueprint_type_id=bp_id,
                product_type_id=product_type_id,
                product_quantity=product_qty,
                manufacturing_time=mfg.get("time", 0),
            )
            db.add(bp)
            bp_count += 1

            for mat in mfg.get("materials", []):
                mat_type_id = mat.get("typeID")
                mat_qty = mat.get("quantity", 1)
                if mat_type_id:
                    db.add(SdeBlueprintMaterial(
                        blueprint_type_id=bp_id,
                        material_type_id=mat_type_id,
                        quantity=mat_qty,
                    ))
                    mat_count += 1

            if bp_count % 500 == 0:
                await db.flush()

        await db.flush()
        logger.info(f"Blueprints: {bp_count} loaded, {mat_count} materials")
        return {"blueprints_loaded": bp_count, "materials_loaded": mat_count}

    async def _ingest_market_groups(self, db: AsyncSession, records: list[dict]) -> dict:
        """Load marketGroups.jsonl into sde_market_groups."""
        await db.execute(text("DELETE FROM sde_market_groups"))

        count = 0
        batch = []

        for rec in records:
            mg_id = rec.get("_key")
            if mg_id is None:
                continue

            name_data = rec.get("name", {})
            if isinstance(name_data, dict):
                name = name_data.get("en", str(mg_id))
            else:
                name = str(name_data)

            desc_data = rec.get("description", {})
            if isinstance(desc_data, dict):
                description = desc_data.get("en", "")
            else:
                description = str(desc_data) if desc_data else ""

            batch.append(SdeMarketGroup(
                market_group_id=mg_id,
                name=name,
                parent_group_id=rec.get("parentGroupID"),
                description=description,
            ))
            count += 1

            if len(batch) >= 500:
                db.add_all(batch)
                await db.flush()
                batch = []

        if batch:
            db.add_all(batch)
            await db.flush()

        logger.info(f"Market groups: {count} loaded")
        return {"market_groups_loaded": count}

    async def build_lookups(self, db: AsyncSession):
        """Build in-memory name→typeID lookup. Only published items with a market group."""
        result = await db.execute(
            select(SdeType.type_id, SdeType.name)
            .where(SdeType.published == True)
            .where(SdeType.market_group_id.isnot(None))
        )
        self._type_name_lookup = {
            row[1].lower(): row[0] for row in result.fetchall()
        }
        self._loaded = True
        logger.info(f"SDE lookups built: {len(self._type_name_lookup)} type names (published+market only)")

    async def get_sde_stats(self, db: AsyncSession) -> dict:
        types = (await db.execute(text("SELECT COUNT(*) FROM sde_types"))).scalar() or 0
        bps = (await db.execute(text("SELECT COUNT(*) FROM sde_blueprints"))).scalar() or 0
        mats = (await db.execute(text("SELECT COUNT(*) FROM sde_blueprint_materials"))).scalar() or 0
        mgs = (await db.execute(text("SELECT COUNT(*) FROM sde_market_groups"))).scalar() or 0
        return {
            "types": types,
            "blueprints": bps,
            "blueprint_materials": mats,
            "market_groups": mgs,
        }

    # Ship group → packaged volume (m³). These are fixed by CCP game design.
    SHIP_PACKAGED_VOLUMES = {
        25: 2500, 26: 10000, 27: 50000, 28: 20000, 29: 500, 30: 10000000,
        31: 500, 237: 2500, 324: 2500, 358: 10000, 380: 20000, 381: 50000,
        419: 15000, 420: 5000, 463: 3750, 485: 1300000, 513: 1300000,
        540: 10000, 541: 5000, 543: 3750, 547: 1300000, 659: 1300000,
        830: 2500, 831: 2500, 832: 10000, 833: 10000, 834: 2500, 838: 2500,
        839: 10000, 883: 1300000, 893: 2500, 894: 10000, 898: 50000,
        900: 50000, 902: 1300000, 906: 15000, 941: 500000, 963: 10000,
        1022: 1300000, 1201: 15000, 1202: 20000, 1283: 2500, 1285: 5000,
        1305: 5000, 1527: 2500, 1534: 5000, 1538: 1300000, 1657: 10000,
        1972: 20000, 4594: 2500, 4902: 10000, 5087: 20000,
    }

    async def populate_packaged_volumes(self, db: AsyncSession) -> int:
        """Auto-set packaged_volume for all ships based on group_id mapping."""
        from app.models.database import SdeType
        count = 0
        for group_id, packed_vol in self.SHIP_PACKAGED_VOLUMES.items():
            result = await db.execute(
                select(SdeType).where(
                    SdeType.group_id == group_id,
                    SdeType.category_id == 6,  # Ships category
                    SdeType.published == True,
                )
            )
            for sde_type in result.scalars().all():
                if sde_type.packaged_volume != packed_vol:
                    sde_type.packaged_volume = packed_vol
                    count += 1
        await db.flush()
        logger.info(f"Populated packaged_volume for {count} ship types")
        return count


sde_service = SdeIngestionService()
