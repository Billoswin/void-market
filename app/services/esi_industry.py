"""
Void Market — ESI Industry Service

Fetches character industry jobs from ESI.
Tracks manufacturing, copying, invention, and reaction jobs.
Material costs are resolved later by the profit engine.
"""
import logging
from datetime import datetime, timezone
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.database import IndustryJob, EsiCharacter
from app.services.esi_client import esi_client
from app.services.esi_auth import esi_auth

logger = logging.getLogger("void_market.esi_industry")

# Activity IDs from EVE SDE
ACTIVITY_MANUFACTURING = 1
ACTIVITY_TE_RESEARCH = 3
ACTIVITY_ME_RESEARCH = 4
ACTIVITY_COPYING = 5
ACTIVITY_INVENTION = 8
ACTIVITY_REACTIONS = 9


class EsiIndustryService:
    """Fetches and stores industry job data from ESI."""

    async def sync_jobs(self, db: AsyncSession, character_id: int = None) -> dict:
        """Pull industry jobs for a specific or active character."""
        if character_id:
            from app.models.database import EsiCharacter
            char = await db.get(EsiCharacter, character_id)
        else:
            char = await esi_auth.get_active_character(db)
        if not char:
            return {"error": "No authenticated character"}

        try:
            jobs = await esi_client.get(
                f"/characters/{char.character_id}/industry/jobs/",
                db=db, character_id=char.character_id,
                params={"include_completed": "true"},
            )
        except Exception as e:
            logger.error(f"Failed to fetch industry jobs: {e}")
            return {"error": str(e)}

        new_count = 0
        updated_count = 0

        for job in jobs:
            job_id = job["job_id"]
            existing = await db.get(IndustryJob, job_id)

            status = job.get("status", "active")
            completed_date = None
            if job.get("completed_date"):
                completed_date = datetime.fromisoformat(
                    job["completed_date"].replace("Z", "+00:00")
                )

            if existing:
                # Update status if changed
                if existing.status != status:
                    existing.status = status
                    existing.completed_date = completed_date
                    updated_count += 1
                continue

            # Determine product type_id
            product_type_id = job.get("product_type_id")
            if not product_type_id:
                # For manufacturing, look up from blueprint
                product_type_id = job.get("blueprint_type_id")

            record = IndustryJob(
                job_id=job_id,
                character_id=char.character_id,
                activity_id=job.get("activity_id", 0),
                blueprint_type_id=job.get("blueprint_type_id"),
                product_type_id=product_type_id,
                runs=job.get("runs", 1),
                licensed_runs=job.get("licensed_runs"),
                cost=job.get("cost", 0),
                status=status,
                facility_id=job.get("facility_id"),
                start_date=datetime.fromisoformat(job["start_date"].replace("Z", "+00:00")) if job.get("start_date") else None,
                end_date=datetime.fromisoformat(job["end_date"].replace("Z", "+00:00")) if job.get("end_date") else None,
                completed_date=completed_date,
                quantity_produced=job.get("runs", 1) if status == "delivered" else 0,
            )
            db.add(record)
            new_count += 1

        await db.commit()
        logger.info(f"Synced industry jobs: {new_count} new, {updated_count} updated")
        return {"new": new_count, "updated": updated_count, "total_from_esi": len(jobs)}


esi_industry = EsiIndustryService()
