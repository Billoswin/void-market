"""
Void Market — ESI Fittings Service

Syncs in-game saved fittings. Used to one-click import fits as doctrine fits.
On-demand sync only (user clicks refresh button).
"""
import logging
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.database import CharacterFitting, CharacterFittingItem
from app.services.esi_client import esi_client

logger = logging.getLogger("void_market.esi_fittings")


class EsiFittingsService:
    async def sync_fittings(self, db: AsyncSession, character_id: int) -> dict:
        """Pull saved fittings for a character. Full replace — delete old, insert new."""
        try:
            fits = await esi_client.get(
                f"/characters/{character_id}/fittings/",
                db=db, character_id=character_id,
            )
        except Exception as e:
            logger.error(f"Failed to fetch fittings for {character_id}: {e}")
            return {"error": str(e)}

        # Clear existing fittings for this character
        r = await db.execute(
            select(CharacterFitting.fitting_id).where(CharacterFitting.character_id == character_id)
        )
        old_ids = [row[0] for row in r.all()]
        if old_ids:
            await db.execute(delete(CharacterFittingItem).where(CharacterFittingItem.fitting_id.in_(old_ids)))
            await db.execute(delete(CharacterFitting).where(CharacterFitting.fitting_id.in_(old_ids)))

        count = 0
        for fit in fits:
            fit_id = fit.get("fitting_id")
            if not fit_id:
                continue
            db.add(CharacterFitting(
                fitting_id=fit_id,
                character_id=character_id,
                name=fit.get("name", "Unnamed"),
                description=fit.get("description", ""),
                ship_type_id=fit.get("ship_type_id"),
            ))
            for item in fit.get("items", []):
                db.add(CharacterFittingItem(
                    fitting_id=fit_id,
                    type_id=item.get("type_id"),
                    flag=item.get("flag", "Cargo"),
                    quantity=item.get("quantity", 1),
                ))
            count += 1

        await db.commit()
        logger.info(f"Synced {count} fittings for char {character_id}")
        return {"count": count}


esi_fittings_service = EsiFittingsService()
