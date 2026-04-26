"""
Void Market — ESI Wallet Service

Fetches wallet transactions and journal entries from ESI.
Transactions are the raw buys/sells that feed the FIFO profit engine.
Journal entries track all ISK movement (fees, taxes, contract payments).

ESI limitation: only returns last 30 days of data.
Must sync regularly or data is lost forever.
"""
import logging
from datetime import datetime, timezone
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.sqlite import insert as sqlite_upsert

from app.models.database import (
    WalletTransaction, WalletJournal, CharacterOrder,
    EsiCharacter, MarketOrder,
)
from app.services.esi_client import esi_client
from app.services.esi_auth import esi_auth
from app.config import settings

logger = logging.getLogger("void_market.esi_wallet")


class EsiWalletService:
    """Fetches and stores wallet data from ESI."""

    async def sync_transactions(self, db: AsyncSession, character_id: int = None) -> dict:
        """Pull wallet transactions for a specific or active character."""
        if character_id:
            from app.models.database import EsiCharacter
            char = await db.get(EsiCharacter, character_id)
        else:
            char = await esi_auth.get_active_character(db)
        if not char:
            return {"error": "No authenticated character"}

        # Fetch wallet balance alongside transactions
        try:
            balance = await esi_client.get(
                f"/characters/{char.character_id}/wallet/",
                db=db, character_id=char.character_id,
            )
            if isinstance(balance, (int, float)):
                char.last_wallet_balance = float(balance)
                await db.flush()
        except Exception as e:
            logger.warning(f"Failed to fetch wallet balance for {char.character_id}: {e}")

        try:
            transactions = await esi_client.get(
                f"/characters/{char.character_id}/wallet/transactions/",
                db=db, character_id=char.character_id,
            )
        except Exception as e:
            logger.error(f"Failed to fetch transactions: {e}")
            return {"error": str(e)}

        new_count = 0
        for tx in transactions:
            tx_id = tx["transaction_id"]

            # Check if already stored
            existing = await db.get(WalletTransaction, tx_id)
            if existing:
                continue

            is_buy = tx.get("is_buy", False)
            quantity = tx.get("quantity", 0)

            record = WalletTransaction(
                transaction_id=tx_id,
                character_id=char.character_id,
                type_id=tx["type_id"],
                is_buy=is_buy,
                unit_price=tx["unit_price"],
                quantity=quantity,
                location_id=tx.get("location_id"),
                journal_ref_id=tx.get("journal_ref_id"),
                date=datetime.fromisoformat(tx["date"].replace("Z", "+00:00")),
                client_id=tx.get("client_id"),
                quantity_matched=0,
                quantity_consumed=0,
                quantity_remaining=quantity if is_buy else 0,
            )
            db.add(record)
            new_count += 1

        await db.commit()
        logger.info(f"Synced {new_count} new transactions (of {len(transactions)} from ESI)")
        return {"new": new_count, "total_from_esi": len(transactions)}

    async def sync_journal(self, db: AsyncSession, character_id: int = None) -> dict:
        """Pull wallet journal for a specific or active character."""
        if character_id:
            from app.models.database import EsiCharacter
            char = await db.get(EsiCharacter, character_id)
        else:
            char = await esi_auth.get_active_character(db)
        if not char:
            return {"error": "No authenticated character"}

        try:
            journal = await esi_client.get(
                f"/characters/{char.character_id}/wallet/journal/",
                db=db, character_id=char.character_id,
            )
        except Exception as e:
            logger.error(f"Failed to fetch journal: {e}")
            return {"error": str(e)}

        new_count = 0
        for entry in journal:
            ref_id = entry["id"]

            existing = await db.get(WalletJournal, ref_id)
            if existing:
                continue

            record = WalletJournal(
                ref_id=ref_id,
                character_id=char.character_id,
                ref_type=entry.get("ref_type", "unknown"),
                amount=entry.get("amount", 0),
                balance=entry.get("balance"),
                date=datetime.fromisoformat(entry["date"].replace("Z", "+00:00")),
                description=entry.get("description"),
                first_party_id=entry.get("first_party_id"),
                second_party_id=entry.get("second_party_id"),
                context_id=entry.get("context_id"),
                context_id_type=entry.get("context_id_type"),
            )
            db.add(record)
            new_count += 1

        await db.commit()
        logger.info(f"Synced {new_count} new journal entries (of {len(journal)} from ESI)")
        return {"new": new_count, "total_from_esi": len(journal)}

    async def sync_character_orders(self, db: AsyncSession, character_id: int = None) -> dict:
        """Pull active market orders for a specific or active character."""
        if character_id:
            from app.models.database import EsiCharacter
            char = await db.get(EsiCharacter, character_id)
        else:
            char = await esi_auth.get_active_character(db)
        if not char:
            return {"error": "No authenticated character"}

        try:
            orders = await esi_client.get(
                f"/characters/{char.character_id}/orders/",
                db=db, character_id=char.character_id,
            )
        except Exception as e:
            logger.error(f"Failed to fetch character orders: {e}")
            return {"error": str(e)}

        # Clear old orders for this character and replace with fresh data
        from sqlalchemy import delete
        await db.execute(
            delete(CharacterOrder).where(
                CharacterOrder.character_id == char.character_id
            )
        )

        for o in orders:
            record = CharacterOrder(
                order_id=o["order_id"],
                character_id=char.character_id,
                type_id=o["type_id"],
                is_buy_order=o.get("is_buy_order", False),
                price=o["price"],
                volume_remain=o["volume_remain"],
                volume_total=o["volume_total"],
                location_id=o.get("location_id"),
                region_id=o.get("region_id"),
                issued=datetime.fromisoformat(o["issued"].replace("Z", "+00:00")) if o.get("issued") else None,
                duration=o.get("duration"),
                min_volume=o.get("min_volume", 1),
                range=o.get("range"),
            )
            db.add(record)

        await db.commit()

        # Run undercut detection
        undercut_count = await self._detect_undercuts(db, char.character_id)

        logger.info(f"Synced {len(orders)} character orders, {undercut_count} undercut")
        return {"orders": len(orders), "undercut": undercut_count}

    async def _detect_undercuts(self, db: AsyncSession, character_id: int) -> int:
        """
        Compare your sell orders against all Keepstar sell orders.
        For each of your sells, find the lowest competitor price.
        """
        # Get your sell orders on the Keepstar
        result = await db.execute(
            select(CharacterOrder)
            .where(CharacterOrder.character_id == character_id)
            .where(CharacterOrder.is_buy_order == False)
            .where(CharacterOrder.location_id == settings.keepstar_structure_id)
        )
        my_orders = result.scalars().all()

        undercut_count = 0
        now = datetime.now(timezone.utc)

        for order in my_orders:
            # Find the lowest sell for this type on the Keepstar that isn't mine
            from sqlalchemy import func, and_
            lowest = await db.execute(
                select(func.min(MarketOrder.price))
                .where(MarketOrder.type_id == order.type_id)
                .where(MarketOrder.location_id == settings.keepstar_structure_id)
                .where(MarketOrder.is_buy_order == False)
                .where(MarketOrder.order_id != order.order_id)
            )
            lowest_price = lowest.scalar()

            if lowest_price is not None:
                order.lowest_competitor = lowest_price
                if lowest_price < order.price:
                    order.is_undercut = True
                    order.undercut_by = order.price - lowest_price
                    undercut_count += 1
                else:
                    order.is_undercut = False
                    order.undercut_by = 0
            else:
                order.lowest_competitor = None
                order.is_undercut = False
                order.undercut_by = None

            order.last_checked = now

        await db.commit()
        return undercut_count

    async def get_wallet_balance(self, db: AsyncSession) -> float | None:
        """Get current wallet balance from ESI."""
        char = await esi_auth.get_active_character(db)
        if not char:
            return None
        try:
            balance = await esi_client.get(
                f"/characters/{char.character_id}/wallet/",
                db=db,
            )
            return float(balance) if balance else None
        except Exception as e:
            logger.error(f"Failed to fetch wallet balance: {e}")
            return None


esi_wallet = EsiWalletService()
