"""
Void Market — Sync Service (Multi-Character)

Split into tiers matching ESI cache timers:
  - fast_sync():  contracts + industry + character orders + undercuts  (ESI 5-20 min cache)
  - slow_sync():  wallet transactions + journal                       (ESI 1 hour cache)
  - full_sync():  everything + profit matching                        (manual trigger / fallback)

Profit matching runs after slow_sync (when new transactions arrive)
and after fast_sync if new contracts were found.
"""
import logging
from datetime import datetime, timezone

from app.models.session import async_session, db_write_lock
from app.services.esi_wallet import esi_wallet
from app.services.esi_industry import esi_industry
from app.services.esi_character_contracts import esi_character_contracts
from app.services.esi_auth import esi_auth
from app.services.goonmetrics import goonmetrics_service
from app.services.profit_engine import profit_engine

logger = logging.getLogger("void_market.sync")


class SyncService:

    async def fast_sync(self) -> dict:
        """
        Fast-cycle sync: data that refreshes every 5-20 min on ESI.
        - Character orders (ESI cache ~20 min) + undercut detection
        - Industry jobs (ESI cache 5 min)
        - Character contracts (ESI cache 5 min)
        - Profit matching ONLY if new contracts found
        """
        logger.info("=== Fast sync (orders/contracts/industry) ===")
        results = {}
        start = datetime.now(timezone.utc)

        async with db_write_lock:
            async with async_session() as db:
                characters = await esi_auth.get_all_active_characters(db)
                if not characters:
                    return {"error": "No active characters"}

                new_contracts = False
                for char in characters:
                    cid = char.character_id
                    cn = char.character_name

                    for name, fn in [
                        ("orders", lambda: esi_wallet.sync_character_orders(db, character_id=cid)),
                        ("industry", lambda: esi_industry.sync_jobs(db, character_id=cid)),
                        ("contracts", lambda: esi_character_contracts.sync_contracts(db, character_id=cid)),
                    ]:
                        try:
                            r = await fn()
                            results[f"{name}_{cn}"] = r
                            # Check if contracts found new finished ones
                            if name == "contracts" and isinstance(r, dict) and r.get("new", 0) > 0:
                                new_contracts = True
                        except Exception as e:
                            logger.error(f"{name} sync failed for {cn}: {e}")
                            results[f"{name}_{cn}"] = {"error": str(e)}

                # Only run profit matching if new contracts arrived (they consume FIFO pool)
                if new_contracts:
                    try:
                        results["profits"] = await profit_engine.run_matching(db)
                    except Exception as e:
                        logger.error(f"Profit matching failed: {e}")
                        results["profits"] = {"error": str(e)}

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        results["elapsed_seconds"] = round(elapsed, 1)
        logger.info(f"=== Fast sync done in {elapsed:.1f}s ===")
        return results

    async def slow_sync(self) -> dict:
        """
        Slow-cycle sync: data that refreshes every ~1 hour on ESI.
        - Wallet transactions (ESI cache 1 hour)
        - Wallet journal (ESI cache 1 hour)
        - Always runs profit matching after (new transactions = new FIFO data)
        """
        logger.info("=== Slow sync (transactions/journal/profits) ===")
        results = {}
        start = datetime.now(timezone.utc)

        async with db_write_lock:
            async with async_session() as db:
                characters = await esi_auth.get_all_active_characters(db)
                if not characters:
                    return {"error": "No active characters"}

                for char in characters:
                    cid = char.character_id
                    cn = char.character_name

                    for name, fn in [
                        ("transactions", lambda: esi_wallet.sync_transactions(db, character_id=cid)),
                        ("journal", lambda: esi_wallet.sync_journal(db, character_id=cid)),
                    ]:
                        try:
                            results[f"{name}_{cn}"] = await fn()
                        except Exception as e:
                            logger.error(f"{name} sync failed for {cn}: {e}")
                            results[f"{name}_{cn}"] = {"error": str(e)}

                # Always run profit matching — new transactions feed the FIFO engine
                try:
                    results["profits"] = await profit_engine.run_matching(db)
                except Exception as e:
                    logger.error(f"Profit matching failed: {e}")
                    results["profits"] = {"error": str(e)}

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        results["elapsed_seconds"] = round(elapsed, 1)
        logger.info(f"=== Slow sync done in {elapsed:.1f}s ===")
        return results

    async def full_sync(self) -> dict:
        """
        Full sync: everything at once + profit matching.
        Used for manual triggers and initial startup.
        """
        logger.info("=== Starting full data sync ===")
        results = {}
        start = datetime.now(timezone.utc)

        async with db_write_lock:
            async with async_session() as db:
                characters = await esi_auth.get_all_active_characters(db)
                if not characters:
                    logger.warning("No active characters — skipping sync")
                    return {"error": "No active characters"}

                logger.info(f"Syncing {len(characters)} char(s): {', '.join(c.character_name for c in characters)}")

                for char in characters:
                    cid = char.character_id
                    cn = char.character_name
                    logger.info(f"--- Syncing {cn} ({cid}) ---")

                    for name, fn in [
                        ("transactions", lambda: esi_wallet.sync_transactions(db, character_id=cid)),
                        ("journal", lambda: esi_wallet.sync_journal(db, character_id=cid)),
                        ("orders", lambda: esi_wallet.sync_character_orders(db, character_id=cid)),
                        ("industry", lambda: esi_industry.sync_jobs(db, character_id=cid)),
                        ("contracts", lambda: esi_character_contracts.sync_contracts(db, character_id=cid)),
                    ]:
                        try:
                            results[f"{name}_{cn}"] = await fn()
                        except Exception as e:
                            logger.error(f"{name} sync failed for {cn}: {e}")
                            results[f"{name}_{cn}"] = {"error": str(e)}

                try:
                    results["profits"] = await profit_engine.run_matching(db)
                except Exception as e:
                    logger.error(f"Profit matching failed: {e}")
                    results["profits"] = {"error": str(e)}

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        results["elapsed_seconds"] = round(elapsed, 1)
        results["characters_synced"] = len(characters)
        logger.info(f"=== Full sync complete in {elapsed:.1f}s ({len(characters)} chars) ===")
        return results

    async def sync_goonmetrics(self) -> dict:
        async with db_write_lock:
            async with async_session() as db:
                try:
                    return await goonmetrics_service.sync_weekly_volumes(db)
                except Exception as e:
                    logger.error(f"Goonmetrics sync failed: {e}")
                    return {"error": str(e)}

    async def sync_wallet_only(self) -> dict:
        async with db_write_lock:
            async with async_session() as db:
                results = {}
                characters = await esi_auth.get_all_active_characters(db)
                for char in characters:
                    cid = char.character_id
                    results[f"txn_{char.character_name}"] = await esi_wallet.sync_transactions(db, character_id=cid)
                    results[f"jnl_{char.character_name}"] = await esi_wallet.sync_journal(db, character_id=cid)
                results["profits"] = await profit_engine.run_matching(db)
                return results


sync_service = SyncService()
