"""
Void Market — Background Scheduler

All intervals aligned to ESI cache timers — never polls faster than
the server-side cache refreshes. Grouped into tiers:

  TIER 1 — 5 min   Structure market orders, snapshots, contracts, industry, character orders
  TIER 2 — 10 min  Mining ledger, opportunity scoring
  TIER 3 — 30 min  Jita bulk prices, zKillboard fights
  TIER 4 — 60 min  Wallet transactions + journal (+ profit matching), assets
  OTHER            Skills (24h), Goonmetrics (6h)

ESI Cache Reference:
  /markets/structures/{id}/              5 min
  /characters/{id}/contracts/            5 min
  /characters/{id}/industry/jobs/        5 min
  /characters/{id}/orders/              20 min
  /characters/{id}/mining/              10 min
  /markets/{region}/orders/              5 min  (but bulk Forge = ~300 pages, so 30 min)
  /characters/{id}/wallet/transactions/  1 hour
  /characters/{id}/wallet/journal/       1 hour
  /characters/{id}/assets/               1 hour
  /characters/{id}/skills/               2 min  (but skills rarely change)
  /markets/{region}/history/            ~23 hours (daily at downtime)
"""
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from app.config import settings
from app.models.session import async_session, db_write_lock
from app.services.market_service import market_service
from app.services.contract_service import contract_service
from app.services.scoring_engine import scoring_engine
from app.services.sync_service import sync_service

logger = logging.getLogger("void_market.scheduler")

scheduler = AsyncIOScheduler()


# ─── TIER 1: 5 min (ESI cache 5 min) ─────────────────────────

async def _refresh_market():
    """Structure market orders + snapshot. ESI cache: 5 min."""
    if not settings.keepstar_structure_id:
        return

    async with db_write_lock:
        async with async_session() as db:
            try:
                result = await market_service.fetch_all_tracked_structure_orders(db)
                logger.info(f"[SCHED] Market refresh: {result}")
                snap = await market_service.take_snapshot(db)
                logger.info(f"[SCHED] Snapshot: {snap}")
            except Exception as e:
                logger.error(f"[SCHED] Market refresh failed: {e}")


async def _fast_sync():
    """Character orders + undercuts + industry + contracts. ESI cache: 5-20 min."""
    try:
        result = await sync_service.fast_sync()
        logger.info(f"[SCHED] Fast sync: {result.get('elapsed_seconds', '?')}s")
    except Exception as e:
        logger.error(f"[SCHED] Fast sync failed: {e}")


# ─── TIER 2: 10 min (ESI cache 10 min) ───────────────────────

async def _sync_mining():
    """Mining ledger sync. ESI cache: 10 min."""
    async with db_write_lock:
        async with async_session() as db:
            try:
                from app.services.esi_mining import esi_mining_service
                from app.services.esi_auth import esi_auth
                chars = await esi_auth.get_all_active_characters(db)
                for char in chars:
                    await esi_mining_service.sync_mining(db, char.character_id)
                logger.info(f"[SCHED] Mining synced for {len(chars)} chars")
            except Exception as e:
                logger.error(f"[SCHED] Mining sync failed: {e}")


async def _recalculate_opportunities():
    """Opportunity scoring — our own calc, depends on fresh market data."""
    try:
        from app.services.analysis import refresh_analysis
        result = await refresh_analysis()
        logger.info(f"[SCHED] Analysis: {result}")
    except Exception as e:
        logger.error(f"[SCHED] Analysis failed: {e}")


# ─── TIER 3: 30 min ──────────────────────────────────────────

async def _refresh_jita():
    """Bulk Jita price fetch. ESI cache: 5 min per page but ~300 pages total.
    30 min balances freshness vs request volume."""
    async with db_write_lock:
        async with async_session() as db:
            try:
                result = await market_service.fetch_jita_prices(db)
                logger.info(f"[SCHED] Jita prices: {result}")
            except Exception as e:
                logger.error(f"[SCHED] Jita refresh failed: {e}")


async def _refresh_contracts():
    """Alliance contracts. ESI cache: 5 min, but alliance-level contracts
    don't change as fast. 30 min is reasonable."""
    if not settings.alliance_id:
        return

    async with db_write_lock:
        async with async_session() as db:
            try:
                result = await contract_service.fetch_contracts(db)
                logger.info(f"[SCHED] Alliance contracts: {result}")
            except Exception as e:
                logger.error(f"[SCHED] Alliance contract refresh failed: {e}")


# ─── TIER 4: 60 min (ESI cache 1 hour) ───────────────────────

async def _slow_sync():
    """Wallet transactions + journal + profit matching. ESI cache: 1 hour."""
    try:
        result = await sync_service.slow_sync()
        logger.info(f"[SCHED] Slow sync: {result.get('elapsed_seconds', '?')}s")
    except Exception as e:
        logger.error(f"[SCHED] Slow sync failed: {e}")


async def _sync_assets():
    """Asset inventory sync. ESI cache: 1 hour."""
    async with db_write_lock:
        async with async_session() as db:
            try:
                from app.services.esi_assets import esi_assets_service
                from app.services.esi_auth import esi_auth
                chars = await esi_auth.get_all_active_characters(db)
                for char in chars:
                    await esi_assets_service.sync_assets(db, char.character_id)
                logger.info(f"[SCHED] Assets synced for {len(chars)} chars")
            except Exception as e:
                logger.error(f"[SCHED] Assets sync failed: {e}")


# ─── OTHER ────────────────────────────────────────────────────

async def _refresh_history():
    """Daily market history accumulation from ESI. Cache: ~23 hours (daily at downtime).
    Runs every 6 hours to catch the daily update reliably."""
    try:
        from app.services.esi_market import fetch_market_history
        result = await fetch_market_history()
        logger.info(f"[SCHED] History accumulation: {result}")
    except Exception as e:
        logger.error(f"[SCHED] History accumulation failed: {e}")


async def _sync_goonmetrics():
    """Goonmetrics weekly volume. Their own API, 6 hours is fine."""
    try:
        result = await sync_service.sync_goonmetrics()
        logger.info(f"[SCHED] Goonmetrics: {result}")
    except Exception as e:
        logger.error(f"[SCHED] Goonmetrics sync failed: {e}")


async def _sync_skills():
    """Character skills. ESI cache: 2 min but skills almost never change."""
    async with db_write_lock:
        async with async_session() as db:
            try:
                from app.services.esi_skills import esi_skills_service
                from app.services.esi_auth import esi_auth
                chars = await esi_auth.get_all_active_characters(db)
                for char in chars:
                    await esi_skills_service.sync_skills(db, char.character_id)
                logger.info(f"[SCHED] Skills synced for {len(chars)} chars")
            except Exception as e:
                logger.error(f"[SCHED] Skills sync failed: {e}")


# ─── Scheduler Registration ──────────────────────────────────

async def _detect_fights_scheduled():
    """Re-run fight detection for all active deployments every 3 hours."""
    try:
        from app.models.killmail_session import killmail_session
        from app.models.killmail_models import Deployment
        from app.services.zkill_listener import detect_fights_retroactive
        from sqlalchemy import select

        async with killmail_session() as db:
            result = await db.execute(
                select(Deployment).where(Deployment.status == "active")
            )
            active = result.scalars().all()

        for dep in active:
            logger.info(f"Scheduled fight detection for deployment {dep.id} ({dep.name})")
            stats = await detect_fights_retroactive(deployment_id=dep.id)
            logger.info(f"Fight detection complete: {stats}")
    except Exception as e:
        logger.error(f"Scheduled fight detection error: {e}")


def start_scheduler():
    """Register all scheduled jobs and start the scheduler."""

    # ── TIER 1: every 5 min ──────────────────────────
    scheduler.add_job(
        _refresh_market,
        IntervalTrigger(minutes=5),
        id="market_refresh",
        name="Structure market orders + snapshot (ESI 5min cache)",
        replace_existing=True,
    )
    scheduler.add_job(
        _fast_sync,
        IntervalTrigger(minutes=5),
        id="fast_sync",
        name="Orders + contracts + industry (ESI 5-20min cache)",
        replace_existing=True,
    )

    # ── TIER 2: every 10 min ─────────────────────────
    scheduler.add_job(
        _sync_mining,
        IntervalTrigger(minutes=10),
        id="mining_sync",
        name="Mining ledger (ESI 10min cache)",
        replace_existing=True,
    )
    scheduler.add_job(
        _recalculate_opportunities,
        IntervalTrigger(minutes=10),
        id="scoring_refresh",
        name="Opportunity scoring (depends on market data)",
        replace_existing=True,
    )

    # ── TIER 3: every 30 min ─────────────────────────
    scheduler.add_job(
        _refresh_jita,
        IntervalTrigger(minutes=30),
        id="jita_refresh",
        name="Jita bulk prices (ESI 5min cache × ~300 pages)",
        replace_existing=True,
    )
    scheduler.add_job(
        _refresh_contracts,
        IntervalTrigger(minutes=30),
        id="alliance_contract_refresh",
        name="Alliance contracts (ESI 5min cache)",
        replace_existing=True,
    )

    # ── TIER 4: every 60 min ─────────────────────────
    scheduler.add_job(
        _slow_sync,
        IntervalTrigger(minutes=60),
        id="slow_sync",
        name="Wallet transactions + journal + profits (ESI 1hr cache)",
        replace_existing=True,
    )
    scheduler.add_job(
        _sync_assets,
        IntervalTrigger(minutes=60),
        id="assets_sync",
        name="Character assets (ESI 1hr cache)",
        replace_existing=True,
    )

    # ── OTHER ────────────────────────────────────────
    scheduler.add_job(
        _refresh_history,
        IntervalTrigger(hours=6),
        id="history_refresh",
        name="Market history accumulation (ESI ~23hr cache, 6h poll)",
        replace_existing=True,
    )
    scheduler.add_job(
        _sync_goonmetrics,
        IntervalTrigger(hours=6),
        id="goonmetrics_sync",
        name="Goonmetrics weekly volume",
        replace_existing=True,
    )
    scheduler.add_job(
        _sync_skills,
        IntervalTrigger(hours=24),
        id="skills_sync",
        name="Character skills (ESI 2min cache, rarely changes)",
        replace_existing=True,
    )
    scheduler.add_job(
        _detect_fights_scheduled,
        IntervalTrigger(hours=3),
        id="fight_detection",
        name="Fight detection (re-cluster every 3h)",
        replace_existing=True,
    )

    scheduler.start()
    logger.info("[SCHED] Background scheduler started — ESI cache-aligned intervals")
    logger.info("  TIER 1 (5 min):  Market orders + snapshot, fast sync (orders/contracts/industry)")
    logger.info("  TIER 2 (10 min): Mining ledger, opportunity scoring")
    logger.info("  TIER 3 (30 min): Jita bulk prices, alliance contracts")
    logger.info("  TIER 4 (60 min): Wallet transactions + journal + profits, assets")
    logger.info("  OTHER:           History (6h), Goonmetrics (6h), skills (24h), fights (3h)")


def stop_scheduler():
    """Shut down the scheduler."""
    if scheduler.running:
        scheduler.shutdown()
        logger.info("[SCHED] Scheduler stopped")
