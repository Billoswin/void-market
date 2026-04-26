"""
Void Market — Contract Analysis Service

Pulls alliance contracts (item_exchange type) and analyzes:
- What fitted ships are available on contract
- How many of each doctrine ship are stocked
- Cross-reference with doctrine requirements
"""
import logging
from datetime import datetime, timezone
from sqlalchemy import select, delete, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.database import (
    AllianceContract, ContractItem, EsiCharacter,
    DoctrineFit, DoctrineFitItem, SdeType,
)
from app.services.esi_client import esi_client
from app.services.esi_auth import esi_auth

logger = logging.getLogger("void_market.contracts")


class ContractService:
    """Fetches and analyzes alliance contracts."""

    async def fetch_contracts(self, db: AsyncSession) -> dict:
        """
        Fetch alliance contracts visible to the authenticated character.

        Uses: GET /characters/{character_id}/contracts/
        Filters to availability='alliance' and status='outstanding'
        """
        char = await esi_auth.get_active_character(db)
        if not char:
            return {"error": "No authenticated character"}

        logger.info(f"Fetching contracts for character {char.character_name}")

        try:
            contracts = await esi_client.get_paginated(
                f"/characters/{char.character_id}/contracts/",
                db,
                authenticated=True,
            )
        except Exception as e:
            logger.error(f"Failed to fetch contracts: {e}")
            return {"error": str(e)}

        # Filter to alliance contracts that are still outstanding
        alliance_contracts = [
            c for c in contracts
            if c.get("availability") == "alliance"
            and c.get("status") == "outstanding"
            and c.get("type") == "item_exchange"
        ]

        logger.info(f"Found {len(alliance_contracts)} outstanding alliance item exchange contracts")

        # Clear old contract data
        await db.execute(delete(ContractItem))
        await db.execute(delete(AllianceContract))

        now = datetime.now(timezone.utc)
        loaded = 0

        for contract in alliance_contracts:
            contract_id = contract["contract_id"]

            db.add(AllianceContract(
                contract_id=contract_id,
                issuer_id=contract["issuer_id"],
                status=contract["status"],
                contract_type=contract["type"],
                title=contract.get("title", ""),
                price=contract.get("price", 0),
                volume=contract.get("volume", 0),
                date_issued=datetime.fromisoformat(
                    contract["date_issued"].replace("Z", "+00:00")
                ),
                date_expired=datetime.fromisoformat(
                    contract["date_expired"].replace("Z", "+00:00")
                ) if contract.get("date_expired") else None,
                last_seen=now,
            ))

            # Fetch items in this contract
            try:
                items = await esi_client.get(
                    f"/characters/{char.character_id}/contracts/{contract_id}/items/",
                    db,
                    authenticated=True,
                )

                for item in items:
                    db.add(ContractItem(
                        contract_id=contract_id,
                        type_id=item["type_id"],
                        quantity=item.get("quantity", 1),
                        is_included=item.get("is_included", True),
                    ))
            except Exception as e:
                logger.warning(f"Could not fetch items for contract {contract_id}: {e}")

            loaded += 1

            if loaded % 50 == 0:
                await db.flush()

        await db.commit()
        logger.info(f"Loaded {loaded} alliance contracts with items")
        return {"contracts_loaded": loaded}

    async def get_contract_ship_counts(self, db: AsyncSession) -> dict[int, int]:
        """
        Count how many of each ship type are available on alliance contracts.

        Looks at the hull (ship) type_id in contract items where the item
        matches a known ship category in the SDE.

        Returns: {type_id: count}
        """
        # Ship category IDs in EVE: 6 = Ship
        ship_category = 6

        result = await db.execute(
            select(
                ContractItem.type_id,
                func.sum(ContractItem.quantity).label("total"),
            )
            .join(SdeType, ContractItem.type_id == SdeType.type_id)
            .where(SdeType.category_id == ship_category)
            .where(ContractItem.is_included == True)
            .group_by(ContractItem.type_id)
        )

        return {row[0]: row[1] for row in result.fetchall()}

    async def get_contract_item_counts(self, db: AsyncSession) -> dict[int, int]:
        """
        Count total quantity of each item type across all alliance contracts.
        Returns: {type_id: total_quantity}
        """
        result = await db.execute(
            select(
                ContractItem.type_id,
                func.sum(ContractItem.quantity).label("total"),
            )
            .where(ContractItem.is_included == True)
            .group_by(ContractItem.type_id)
        )

        return {row[0]: row[1] for row in result.fetchall()}

    async def match_doctrine_contracts(self, db: AsyncSession) -> list[dict]:
        """
        Match alliance contracts against doctrine fits.
        Returns a list of doctrine ships with their contract availability.
        """
        ship_counts = await self.get_contract_ship_counts(db)

        result = await db.execute(
            select(DoctrineFit).order_by(DoctrineFit.doctrine_id)
        )
        fits = result.scalars().all()

        matches = []
        for fit in fits:
            ship_type = await db.get(SdeType, fit.ship_type_id)
            on_contract = ship_counts.get(fit.ship_type_id, 0)

            matches.append({
                "fit_id": fit.id,
                "fit_name": fit.name,
                "ship_type_id": fit.ship_type_id,
                "ship_name": ship_type.name if ship_type else "Unknown",
                "min_stock": fit.min_stock,
                "on_contract": on_contract,
                "deficit": max(0, fit.min_stock - on_contract),
            })

        return matches


# Singleton
contract_service = ContractService()
