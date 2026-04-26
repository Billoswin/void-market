"""Skills API routes."""
import logging
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.session import get_db
from app.models.database import EsiCharacter, CharacterSkill
from app.services.esi_skills import esi_skills_service
from app.services.esi_auth import esi_auth

logger = logging.getLogger("void_market.skills")

skills_router = APIRouter(prefix="/skills", tags=["skills"])


@skills_router.get("")
async def get_all_character_skills(db: AsyncSession = Depends(get_db)):
    """Get computed broker rates for all linked characters."""
    chars = await esi_auth.get_all_active_characters(db)
    result = []
    for char in chars:
        rates = await esi_skills_service.compute_broker_rates(db, char.character_id)
        # Count total trained skills
        r = await db.execute(
            select(CharacterSkill).where(CharacterSkill.character_id == char.character_id)
        )
        total_skills = len(r.scalars().all())
        result.append({
            "character_id": char.character_id,
            "character_name": char.character_name,
            "total_skills": total_skills,
            "rates": rates,
        })
    return result


@skills_router.post("/sync")
async def sync_all_skills(db: AsyncSession = Depends(get_db)):
    """Sync skills for all linked characters."""
    chars = await esi_auth.get_all_active_characters(db)
    results = []
    for char in chars:
        r = await esi_skills_service.sync_skills(db, char.character_id)
        results.append({"character_name": char.character_name, **r})
    return {"results": results}


@skills_router.post("/sync/{character_id}")
async def sync_character_skills(character_id: int, db: AsyncSession = Depends(get_db)):
    """Sync skills for a specific character."""
    char = await db.get(EsiCharacter, character_id)
    if not char:
        raise HTTPException(404, "Character not found")
    return await esi_skills_service.sync_skills(db, character_id)
