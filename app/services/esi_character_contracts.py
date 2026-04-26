"""
Void Market — ESI Character Contracts Service

Fetches character contracts and their items from ESI.
Tracks item_exchange contracts for doctrine ship sales P&L.
"""
import logging
from datetime import datetime, timezone
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.database import CharacterContract, CharacterContractItem, EsiCharacter
from app.services.esi_client import esi_client
from app.services.esi_auth import esi_auth

logger = logging.getLogger("void_market.esi_contracts")


class EsiCharacterContractsService:
    """Fetches and stores character contract data from ESI."""

    async def sync_contracts(self, db: AsyncSession, character_id: int = None) -> dict:
        """Pull contracts for a specific or active character."""
        if character_id:
            from app.models.database import EsiCharacter
            char = await db.get(EsiCharacter, character_id)
        else:
            char = await esi_auth.get_active_character(db)
        if not char:
            return {"error": "No authenticated character"}

        try:
            contracts = await esi_client.get_paginated(
                f"/characters/{char.character_id}/contracts/",
                db=db, character_id=char.character_id,
            )
        except Exception as e:
            logger.error(f"Failed to fetch contracts: {e}")
            return {"error": str(e)}

        new_count = 0
        updated_count = 0
        items_fetched = 0

        for c in contracts:
            contract_id = c["contract_id"]
            existing = await db.get(CharacterContract, contract_id)

            status = c.get("status", "outstanding")
            date_completed = None
            if c.get("date_completed"):
                date_completed = datetime.fromisoformat(
                    c["date_completed"].replace("Z", "+00:00")
                )

            if existing:
                if existing.status != status:
                    existing.status = status
                    existing.date_completed = date_completed
                    existing.acceptor_id = c.get("acceptor_id")
                    updated_count += 1
                # Always update start_location_id if we don't have it
                if not existing.start_location_id and c.get("start_location_id"):
                    existing.start_location_id = c.get("start_location_id")

                # Fetch items if contract is finished and has no items yet
                if (existing.status in ("finished", "completed") and
                        c.get("type") == "item_exchange"):
                    item_check = await db.execute(
                        select(func.count()).select_from(CharacterContractItem)
                        .where(CharacterContractItem.contract_id == contract_id)
                    )
                    if item_check.scalar() == 0:
                        try:
                            item_count = await self._fetch_contract_items(
                                db, char.character_id, contract_id
                            )
                            items_fetched += item_count
                        except Exception as e:
                            logger.warning(f"Failed to fetch items for contract {contract_id}: {e}")
                continue

            record = CharacterContract(
                contract_id=contract_id,
                character_id=char.character_id,
                issuer_id=c.get("issuer_id", 0),
                acceptor_id=c.get("acceptor_id"),
                contract_type=c.get("type", "unknown"),
                status=status,
                title=c.get("title"),
                price=c.get("price"),
                reward=c.get("reward"),
                volume=c.get("volume"),
                start_location_id=c.get("start_location_id"),
                date_issued=datetime.fromisoformat(c["date_issued"].replace("Z", "+00:00")),
                date_completed=date_completed,
                date_expired=datetime.fromisoformat(c["date_expired"].replace("Z", "+00:00")) if c.get("date_expired") else None,
                for_corporation=c.get("for_corporation", False),
                availability=c.get("availability"),
            )
            db.add(record)
            new_count += 1

            # Fetch items for completed item_exchange contracts we issued
            if (c.get("type") == "item_exchange" and
                    c.get("issuer_id") == char.character_id and
                    status in ("finished", "completed")):
                try:
                    item_count = await self._fetch_contract_items(
                        db, char.character_id, contract_id
                    )
                    items_fetched += item_count
                except Exception as e:
                    logger.warning(f"Failed to fetch items for contract {contract_id}: {e}")

        await db.commit()
        logger.info(f"Synced contracts: {new_count} new, {updated_count} updated, {items_fetched} items")
        return {
            "new": new_count,
            "updated": updated_count,
            "items_fetched": items_fetched,
            "total_from_esi": len(contracts),
        }

    async def _fetch_contract_items(
        self, db: AsyncSession, character_id: int, contract_id: int
    ) -> int:
        """Fetch items for a specific contract."""
        # Check if we already have items for this contract
        result = await db.execute(
            select(CharacterContractItem)
            .where(CharacterContractItem.contract_id == contract_id)
            .limit(1)
        )
        if result.scalar_one_or_none():
            return 0  # Already fetched

        try:
            items = await esi_client.get(
                f"/characters/{character_id}/contracts/{contract_id}/items/",
                db=db,
            )
        except Exception as e:
            logger.warning(f"Contract {contract_id} items fetch failed: {e}")
            return 0

        for item in items:
            record = CharacterContractItem(
                contract_id=contract_id,
                type_id=item["type_id"],
                quantity=item.get("quantity", 1),
                is_included=item.get("is_included", True),
                record_id=item.get("record_id"),
            )
            db.add(record)

        return len(items)


esi_character_contracts = EsiCharacterContractsService()
