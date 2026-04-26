"""
Void Market — ESI Skills Service

Syncs trained character skills. Used by the profit engine to compute
accurate broker fees and sales tax rates automatically per character.

Skill IDs that matter for trading:
  3446 — Accounting (-11% sales tax per level)
  3444 — Broker Relations (-0.3% broker fee per level)
  16622 — Advanced Broker Relations (NPC stations only, -0.1% per level)
"""
import logging
from datetime import datetime, timezone
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.database import CharacterSkill, EsiCharacter
from app.services.esi_client import esi_client
from app.services.esi_auth import esi_auth

logger = logging.getLogger("void_market.esi_skills")

# Skills relevant to trading fees
SKILL_ACCOUNTING = 16622  # -11% sales tax per level (I was wrong above — this IS Accounting)
SKILL_BROKER_RELATIONS = 3446  # -0.3% broker fee per level
SKILL_ADVANCED_BROKER = 3449  # Not actually needed — misleading name

# CCP constants (as of 2025)
BASE_SALES_TAX = 4.5  # Base sales tax percentage
BASE_BROKER_FEE_STRUCTURE = 1.0  # Base broker at player structures
BASE_BROKER_FEE_NPC = 3.0  # Base broker at NPC stations


class EsiSkillsService:
    """Fetches trained skills and computes broker/tax rates."""

    async def sync_skills(self, db: AsyncSession, character_id: int) -> dict:
        """Pull all trained skills for a character."""
        try:
            data = await esi_client.get(
                f"/characters/{character_id}/skills/",
                db=db, character_id=character_id,
            )
        except Exception as e:
            logger.error(f"Failed to fetch skills for {character_id}: {e}")
            return {"error": str(e)}

        skills = data.get("skills", [])
        count = 0

        # Get existing skills for this character
        r = await db.execute(
            select(CharacterSkill).where(CharacterSkill.character_id == character_id)
        )
        existing = {s.skill_id: s for s in r.scalars().all()}

        for skill in skills:
            sid = skill.get("skill_id")
            if not sid:
                continue

            trained = skill.get("trained_skill_level", 0)
            active = skill.get("active_skill_level", 0)
            sp = skill.get("skillpoints_in_skill", 0)

            if sid in existing:
                row = existing[sid]
                row.trained_level = trained
                row.active_level = active
                row.skillpoints = sp
                row.updated_at = datetime.now(timezone.utc)
            else:
                db.add(CharacterSkill(
                    character_id=character_id,
                    skill_id=sid,
                    trained_level=trained,
                    active_level=active,
                    skillpoints=sp,
                ))
                count += 1

        await db.commit()
        logger.info(f"Synced skills for char {character_id}: {count} new, {len(skills)} total")
        return {"total": len(skills), "new": count}

    async def get_skill_level(self, db: AsyncSession, character_id: int, skill_id: int) -> int:
        """Get trained level of a specific skill (0 if not trained)."""
        r = await db.execute(
            select(CharacterSkill.trained_level).where(
                CharacterSkill.character_id == character_id,
                CharacterSkill.skill_id == skill_id,
            )
        )
        return r.scalar() or 0

    async def compute_broker_rates(self, db: AsyncSession, character_id: int) -> dict:
        """
        Compute effective broker fees and sales tax for a character based on skills.

        Accounting V → base sales tax × (1 - 0.11*5) = 45% of base
          4.5% base → 2.025%
        Broker Relations V → base broker × (1 - 0.3*5/base) — actually subtractive
          Structure: 1.0% - 0.3*5 = -0.5% which is floored... let's do it correctly.

        Real formulas (post-2025):
          Sales tax = 4.5% * (1 - 0.11 * accounting_level)
          Broker fee (structure) = max(1% - 0.3% * broker_relations, 0)
          Broker fee (NPC) = max(3% - 0.3% * broker_relations, 0)
        """
        accounting = await self.get_skill_level(db, character_id, SKILL_ACCOUNTING)
        broker_rel = await self.get_skill_level(db, character_id, SKILL_BROKER_RELATIONS)

        # Sales tax: reduced 11% per level of Accounting
        sales_tax = BASE_SALES_TAX * (1 - 0.11 * accounting)

        # Broker fees: flat -0.3% per level (minimum 0)
        broker_structure = max(BASE_BROKER_FEE_STRUCTURE - 0.3 * broker_rel, 0)
        broker_npc = max(BASE_BROKER_FEE_NPC - 0.3 * broker_rel, 0)

        return {
            "accounting_level": accounting,
            "broker_relations_level": broker_rel,
            "sales_tax": round(sales_tax, 4),
            "buy": round(broker_structure, 4),  # Buy broker always at order location
            "sell_structure": round(broker_structure, 4),
            "sell_npc": round(broker_npc, 4),
            "source": "skills",
        }


esi_skills_service = EsiSkillsService()
