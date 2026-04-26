#!/usr/bin/env python3
"""
Void Market — Load SDE from disk.

Place your JSONL files in /opt/void-market/data/sde/ and run this script.
Also accepts zip files containing JSONL files.

Usage:
    python3 load_sde.py                          # Load all .jsonl from data/sde/
    python3 load_sde.py /path/to/sde.zip          # Load from zip
    python3 load_sde.py /path/to/types.jsonl types # Load single file
"""
import sys
import asyncio
import zipfile
import io
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from app.models.session import init_db, async_session
from app.services.sde_service import sde_service


async def load_from_directory(sde_dir: Path):
    """Load all JSONL files from a directory in correct order."""
    await init_db()

    order = ["groups", "categories", "types", "blueprints", "marketGroups"]

    async with async_session() as db:
        for name in order:
            path = sde_dir / f"{name}.jsonl"
            if path.exists():
                print(f"Loading {path.name}...")
                result = await sde_service.ingest_file(db, path, name)
                print(f"  → {result}")
            else:
                print(f"  Skipping {name}.jsonl (not found)")

        await sde_service.build_lookups(db)
        stats = await sde_service.get_sde_stats(db)
        print(f"\nDone! {stats}")
        print(f"Lookups: {len(sde_service.type_name_lookup)} type names")


async def load_from_zip(zip_path: Path):
    """Load SDE from a zip file."""
    await init_db()

    with open(zip_path, "rb") as f:
        zip_bytes = f.read()

    async with async_session() as db:
        print(f"Loading from {zip_path.name}...")
        results = await sde_service.ingest_zip(db, zip_bytes)
        for k, v in results.items():
            print(f"  {k}: {v}")

        stats = await sde_service.get_sde_stats(db)
        print(f"\nDone! {stats}")
        print(f"Lookups: {len(sde_service.type_name_lookup)} type names")


async def load_single(file_path: Path, file_type: str):
    """Load a single JSONL file."""
    await init_db()

    async with async_session() as db:
        print(f"Loading {file_path.name} as {file_type}...")
        result = await sde_service.ingest_file(db, file_path, file_type)
        print(f"  → {result}")

        if file_type == "types":
            await sde_service.build_lookups(db)
            print(f"Lookups: {len(sde_service.type_name_lookup)} type names")


if __name__ == "__main__":
    if len(sys.argv) == 1:
        # Load from default directory
        sde_dir = Path(__file__).parent / "data" / "sde"
        asyncio.run(load_from_directory(sde_dir))
    elif len(sys.argv) == 2:
        path = Path(sys.argv[1])
        if path.suffix == ".zip":
            asyncio.run(load_from_zip(path))
        else:
            print("Usage: python3 load_sde.py [zip_file | jsonl_file type]")
    elif len(sys.argv) == 3:
        path = Path(sys.argv[1])
        file_type = sys.argv[2]
        asyncio.run(load_single(path, file_type))
    else:
        print("Usage:")
        print("  python3 load_sde.py                     # Load from data/sde/")
        print("  python3 load_sde.py sde.zip              # Load from zip")
        print("  python3 load_sde.py types.jsonl types     # Load single file")
