"""
Void Market — ESI Mining Service

Syncs daily mining ledger. Integrated with industry cost basis:
mined ore counts as zero-cost material when consumed by manufacturing/reaction.
"""
import logging
from sqlalchemy.dialects.sqlite import insert as sqlite_upsert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.database import MiningLedger
from app.services.esi_client import esi_client

logger = logging.getLogger("void_market.esi_mining")


class EsiMiningService:
    async def sync_mining(self, db: AsyncSession, character_id: int) -> dict:
        """Pull mining ledger for a character. Returns daily aggregates."""
        try:
            entries = await esi_client.get_paginated(
                f"/characters/{character_id}/mining/",
                db=db, character_id=character_id,
            )
        except Exception as e:
            logger.error(f"Failed to fetch mining for {character_id}: {e}")
            return {"error": str(e)}

        count = 0
        for entry in entries:
            # Upsert — don't clobber quantity_consumed on existing rows
            stmt = sqlite_upsert(MiningLedger).values(
                character_id=character_id,
                date=entry.get("date"),
                solar_system_id=entry.get("solar_system_id"),
                type_id=entry.get("type_id"),
                quantity=entry.get("quantity", 0),
            ).on_conflict_do_update(
                index_elements=["character_id", "date", "solar_system_id", "type_id"],
                set_={"quantity": entry.get("quantity", 0)},
            )
            await db.execute(stmt)
            count += 1

        await db.commit()
        logger.info(f"Synced {count} mining entries for char {character_id}")
        return {"count": count}


esi_mining_service = EsiMiningService()
