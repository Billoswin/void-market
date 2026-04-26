#!/usr/bin/env python3
"""
Void Market — Load SDE map data (regions, constellations, solar systems).

Usage:
    python3 load_sde_map.py /path/to/sde_map_dir/
    (expects mapRegions.jsonl, mapConstellations.jsonl, mapSolarSystems.jsonl)
"""
import sys
import json
import asyncio
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from sqlalchemy import delete
from app.models.session import init_db, async_session
from app.models.database import SdeRegion, SdeConstellation, SdeSolarSystem


def get_en_name(entry):
    name = entry.get("name")
    if isinstance(name, dict):
        return name.get("en") or name.get("de") or str(entry.get("_key"))
    return name or str(entry.get("_key"))


async def load_map(sde_dir: Path):
    await init_db()

    async with async_session() as db:
        # Clear existing
        await db.execute(delete(SdeSolarSystem))
        await db.execute(delete(SdeConstellation))
        await db.execute(delete(SdeRegion))
        await db.commit()

        # Regions
        path = sde_dir / "mapRegions.jsonl"
        count = 0
        with open(path) as f:
            for line in f:
                if not line.strip():
                    continue
                e = json.loads(line)
                db.add(SdeRegion(
                    region_id=e["_key"],
                    name=get_en_name(e),
                    faction_id=e.get("factionID"),
                ))
                count += 1
        await db.commit()
        print(f"Regions: {count}")

        # Constellations
        path = sde_dir / "mapConstellations.jsonl"
        count = 0
        with open(path) as f:
            for line in f:
                if not line.strip():
                    continue
                e = json.loads(line)
                db.add(SdeConstellation(
                    constellation_id=e["_key"],
                    name=get_en_name(e),
                    region_id=e["regionID"],
                ))
                count += 1
        await db.commit()
        print(f"Constellations: {count}")

        # Solar Systems
        path = sde_dir / "mapSolarSystems.jsonl"
        count = 0
        with open(path) as f:
            for line in f:
                if not line.strip():
                    continue
                e = json.loads(line)
                # Need to look up region_id from constellation
                const_id = e["constellationID"]
                const = await db.get(SdeConstellation, const_id)
                region_id = const.region_id if const else 0
                db.add(SdeSolarSystem(
                    system_id=e["_key"],
                    name=get_en_name(e),
                    constellation_id=const_id,
                    region_id=region_id,
                    security_status=e.get("securityStatus"),
                ))
                count += 1
                if count % 1000 == 0:
                    await db.flush()
        await db.commit()
        print(f"Solar Systems: {count}")


if __name__ == "__main__":
    sde_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data/sde")
    asyncio.run(load_map(sde_dir))
