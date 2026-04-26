"""
Void Market — ESI Structures Service

Resolves structure IDs to names and locations. Also handles market orders at
tracked structures (alongside the existing Keepstar market sync).
"""
import logging
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.database import TrackedStructure
from app.services.esi_client import esi_client

logger = logging.getLogger("void_market.esi_structures")


class EsiStructuresService:
    async def resolve_structure(self, db: AsyncSession, structure_id: int, character_id: int) -> dict:
        """
        Resolve a structure ID to name/system via ESI. Requires docking access
        or market/contract rights. Uses any authenticated character's token.
        """
        try:
            data = await esi_client.get(
                f"/universe/structures/{structure_id}/",
                db=db, character_id=character_id,
            )
        except Exception as e:
            logger.error(f"Failed to resolve structure {structure_id}: {e}")
            return {"error": str(e)}

        return {
            "structure_id": structure_id,
            "name": data.get("name", f"Structure {structure_id}"),
            "solar_system_id": data.get("solar_system_id"),
            "type_id": data.get("type_id"),
        }

    async def add_tracked_structure(self, db: AsyncSession, structure_id: int, character_id: int) -> dict:
        """Add a structure to the tracked list (resolves its info first)."""
        # Already tracked?
        existing = await db.get(TrackedStructure, structure_id)
        if existing:
            return {"error": "Already tracked", "structure_id": structure_id}

        info = await self.resolve_structure(db, structure_id, character_id)
        if "error" in info:
            return info

        # Resolve system name
        system_name = None
        sys_id = info.get("solar_system_id")
        if sys_id:
            try:
                sys_data = await esi_client.get(f"/universe/systems/{sys_id}/", db=db)
                system_name = sys_data.get("name")
            except Exception:
                pass

        ts = TrackedStructure(
            structure_id=structure_id,
            name=info["name"],
            solar_system_id=sys_id,
            solar_system_name=system_name,
            type_id=info.get("type_id"),
            enabled=True,
        )
        db.add(ts)
        await db.commit()
        return {"added": info["name"], "structure_id": structure_id}


esi_structures_service = EsiStructuresService()
