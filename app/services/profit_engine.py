"""
Void Market — FIFO Profit Engine (Complete)

Full profit calculation matching EVE Tycoon's algorithm:
1. Resolve manufacturing material costs from buy history (chaining supported)
2. FIFO buy→sell matching with configurable broker fees
3. Manufacturing job→sell matching
4. Pending transactions for unmatched sells
5. Contract profit calculation with full item cost resolution
6. Warehouse items as cost basis fallback
7. Mark-as-consumed for items leaving by untracked means
"""
import logging
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, func, desc
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.database import (
    WalletTransaction, WalletJournal, TradeProfit, ManufacturingProfit,
    ContractProfit, PendingTransaction, IndustryJob,
    CharacterContract, CharacterContractItem,
    BrokerFeeOverride, WarehouseItem, JitaPrice,
    SdeBlueprintMaterial, SdeBlueprint,
    FreightRoute, LocationCache, CostBasisConfig,
)
from app.config import settings

logger = logging.getLogger("void_market.profit_engine")

DEFAULT_BUY_BROKER = 1.0
DEFAULT_SELL_BROKER_STRUCTURE = 1.0
DEFAULT_SELL_BROKER_NPC = 1.5
DEFAULT_SALES_TAX = 3.36

# Cache cost basis configs to avoid repeated DB lookups during matching
_cost_basis_cache = None


async def _load_cost_basis_configs(db: AsyncSession) -> dict:
    """Load all cost basis configs into a dict keyed by character_id."""
    global _cost_basis_cache
    result = await db.execute(select(CostBasisConfig))
    configs = {}
    for cfg in result.scalars().all():
        configs[cfg.character_id] = {
            "role": cfg.role,
            "buy_filter": cfg.buy_filter,
            "excluded_stations": set(cfg.excluded_stations or []),
        }
    _cost_basis_cache = configs
    return configs


def _is_buy_excluded(char_id: int, location_id: int, configs: dict) -> bool:
    """
    Check if a buy transaction should be excluded from FIFO based on cost basis rules.

    Layer 1: Character role — 'buyer' = include all, 'seller' = apply filters
    Layer 2: Station exclusions — buys at excluded stations always excluded
    """
    cfg = configs.get(char_id)
    if not cfg:
        # No config = default behavior (include all buys)
        return False

    # Layer 1: Buyer role includes everything
    if cfg["role"] == "buyer":
        return False

    # Layer 2: Station exclusions
    if location_id and location_id in cfg["excluded_stations"]:
        return True

    return False


class ProfitEngine:

    # ─── Freight Rate Lookup ──────────────────────────────

    async def _get_freight_rate(self, db: AsyncSession, sell_location_id: int, buy_location_id: int = None) -> float:
        """Get freight rate by resolving buy location → origin system, sell location → destination system, then route table lookup."""
        if not sell_location_id:
            return 0

        # Resolve destination (sell location)
        dest_name = await self._get_system_name(db, sell_location_id)

        # Resolve origin (buy location) — if available
        origin_name = None
        if buy_location_id:
            origin_name = await self._get_system_name(db, buy_location_id)

        if not dest_name:
            return settings.freight_cost_per_m3

        # Try exact origin→destination match first
        if origin_name:
            result = await db.execute(
                select(FreightRoute.rate_per_m3)
                .where(func.lower(FreightRoute.origin_system) == origin_name.lower())
                .where(func.lower(FreightRoute.destination_system) == dest_name.lower())
                .limit(1)
            )
            rate = result.scalar()
            if rate is not None:
                return rate

        # Fallback: any route TO this destination (origin = wildcard)
        result = await db.execute(
            select(FreightRoute.rate_per_m3)
            .where(func.lower(FreightRoute.destination_system) == dest_name.lower())
            .limit(1)
        )
        rate = result.scalar()
        if rate is not None:
            return rate

        # If sell and buy are in the same system, no freight
        if origin_name and dest_name and origin_name.lower() == dest_name.lower():
            return 0

        return settings.freight_cost_per_m3

    async def _get_system_name(self, db: AsyncSession, location_id: int) -> str | None:
        """Get cached system name for a location, resolve if needed."""
        cached = await db.get(LocationCache, location_id)
        if cached:
            return cached.solar_system_name
        return await self._resolve_location_system(db, location_id)

    async def _resolve_location_system(self, db: AsyncSession, location_id: int) -> str | None:
        """Resolve location_id → solar system name, cache the result."""
        import httpx

        system_name = None
        system_id = None
        station_name = None

        try:
            async with httpx.AsyncClient(timeout=10.0) as http:
                if location_id < 100000000:
                    # NPC station
                    resp = await http.get(
                        f"https://esi.evetech.net/latest/universe/stations/{location_id}/",
                        params={"datasource": "tranquility"},
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        station_name = data.get("name")
                        system_id = data.get("system_id")
                else:
                    # Player structure — try authenticated ESI
                    from app.services.esi_client import esi_client
                    try:
                        data = await esi_client.get(f"/universe/structures/{location_id}/", db=db)
                        station_name = data.get("name")
                        system_id = data.get("solar_system_id")
                    except Exception:
                        pass

                if system_id:
                    resp = await http.get(
                        f"https://esi.evetech.net/latest/universe/systems/{system_id}/",
                        params={"datasource": "tranquility"},
                    )
                    if resp.status_code == 200:
                        system_name = resp.json().get("name")
        except Exception as e:
            logger.warning(f"Could not resolve location {location_id}: {e}")

        # Cache
        if system_name or station_name:
            existing = await db.get(LocationCache, location_id)
            if not existing:
                db.add(LocationCache(
                    location_id=location_id, solar_system_id=system_id,
                    solar_system_name=system_name, station_name=station_name,
                ))

        return system_name

    # ─── Fee Resolution ──────────────────────────────────

    async def _get_broker_rates(self, db: AsyncSession, char_id: int, type_id: int, date: datetime) -> dict:
        """
        Get broker fee rates with priority:
        1. Item-specific manual override
        2. Character-default manual override
        3. Global manual override
        4. Character skills (Accounting / Broker Relations) — auto-computed
        5. Hardcoded defaults
        """
        # Priority 1-3: Manual overrides (most specific wins)
        for cid, tid in [(char_id, type_id), (char_id, None), (None, None)]:
            q = select(BrokerFeeOverride).where(BrokerFeeOverride.effective_from <= date).order_by(desc(BrokerFeeOverride.effective_from)).limit(1)
            if cid is not None:
                q = q.where(BrokerFeeOverride.character_id == cid)
            else:
                q = q.where(BrokerFeeOverride.character_id.is_(None))
            if tid is not None:
                q = q.where(BrokerFeeOverride.type_id == tid)
            else:
                q = q.where(BrokerFeeOverride.type_id.is_(None))
            r = await db.execute(q)
            o = r.scalar_one_or_none()
            if o:
                return {"buy": o.buy_rate, "sell_structure": o.sell_structure_rate, "sell_npc": o.sell_npc_rate, "sales_tax": o.sales_tax_rate}

        # Priority 4: Compute from character skills if available
        if char_id:
            try:
                from app.services.esi_skills import esi_skills_service
                rates = await esi_skills_service.compute_broker_rates(db, char_id)
                # Only use if we actually have skills trained (non-zero levels)
                if rates.get("accounting_level", 0) > 0 or rates.get("broker_relations_level", 0) > 0:
                    return {
                        "buy": rates["buy"],
                        "sell_structure": rates["sell_structure"],
                        "sell_npc": rates["sell_npc"],
                        "sales_tax": rates["sales_tax"],
                    }
            except Exception as e:
                logger.debug(f"Skill-based rate lookup failed for char {char_id}: {e}")

        # Priority 5: Hardcoded fallback
        return {"buy": DEFAULT_BUY_BROKER, "sell_structure": DEFAULT_SELL_BROKER_STRUCTURE, "sell_npc": DEFAULT_SELL_BROKER_NPC, "sales_tax": DEFAULT_SALES_TAX}

    def _is_structure(self, loc_id):
        return loc_id is not None and loc_id > 1000000000000

    # ─── Item Cost Resolution (multi-source) ─────────────

    async def _get_item_cost(self, db: AsyncSession, type_id: int, use_jita_fallback: bool = True) -> float | None:
        """
        Get item cost: buy history → mfg jobs (chaining) → warehouse → Jita fallback.
        
        For contract cost calculation, use_jita_fallback=False so salvaged/looted 
        items don't get assigned Jita prices they never cost.
        """
        # Buy transactions average (searches ALL linked characters)
        r = await db.execute(select(func.avg(WalletTransaction.unit_price)).where(WalletTransaction.is_buy == True, WalletTransaction.type_id == type_id))
        v = r.scalar()
        if v and v > 0:
            return v
        # Manufacturing job unit cost (chaining)
        r = await db.execute(select(IndustryJob.unit_cost).where(IndustryJob.product_type_id == type_id, IndustryJob.status == "delivered", IndustryJob.unit_cost.isnot(None)).order_by(desc(IndustryJob.completed_date)).limit(1))
        v = r.scalar()
        if v and v > 0:
            return v
        # Warehouse (includes user-set zero-cost items for salvage/loot)
        r = await db.execute(select(WarehouseItem.unit_price).where(WarehouseItem.type_id == type_id).order_by(desc(WarehouseItem.added_at)).limit(1))
        v = r.scalar()
        if v is not None:
            return v
        # Jita fallback — only for trade matching, NOT for contracts
        if use_jita_fallback:
            jp = await db.get(JitaPrice, type_id)
            if jp and jp.sell_min:
                return jp.sell_min
        return None

    # ─── Main Pipeline ───────────────────────────────────

    async def run_matching(self, db: AsyncSession) -> dict:
        # Load cost basis configs for FIFO filtering
        configs = await _load_cost_basis_configs(db)
        # Order matters — each step consumes the FIFO buy pool:
        # 1. Contracts: explicit items from a contract (highest priority)
        # 2. Industry: material inputs consumed by manufacturing/reaction jobs
        # 3. Trades: market sells match what's left
        # 4. Pending: any sells that couldn't find a buy
        contract_count = await self._match_contracts(db, configs)
        mat_count = await self._resolve_manufacturing_costs(db)
        trade_count = await self._match_trades(db, configs)
        mfg_count = await self._match_manufacturing(db)
        pending_count = await self._create_pending(db)
        await db.commit()
        logger.info(f"Profit matching: {contract_count} contracts, {mat_count} mfg costs, {trade_count} trades, {mfg_count} mfg sells, {pending_count} pending")
        return {"contracts_matched": contract_count, "manufacturing_costs_resolved": mat_count, "trades_matched": trade_count, "manufacturing_matched": mfg_count, "pending_created": pending_count}

    # ─── Manufacturing Cost Resolution (True FIFO) ───────

    async def _get_jita_fallback_price(self, db: AsyncSession, type_id: int, char_id: int | None = None) -> float | None:
        """Get Jita fallback price if character has it enabled."""
        from app.models.database import EsiCharacter

        # Check if any linked character has fallback enabled
        if char_id:
            char = await db.get(EsiCharacter, char_id)
            if not char or not char.use_jita_fallback:
                return None
        else:
            # Check if ANY character has it on (for shared pool lookups)
            r = await db.execute(select(EsiCharacter).where(EsiCharacter.use_jita_fallback == True))
            if not r.scalar_one_or_none():
                return None

        jp = await db.get(JitaPrice, type_id)
        if not jp:
            return None

        if char_id:
            price_type = char.fallback_price_type or "sell_min"
        else:
            price_type = "sell_min"

        if price_type == "sell_min":
            return jp.sell_min
        elif price_type == "buy_max":
            return jp.buy_max
        elif price_type == "avg":
            if jp.sell_min and jp.buy_max:
                return (jp.sell_min + jp.buy_max) / 2
            return jp.sell_min or jp.buy_max
        return jp.sell_min

    async def _consume_fifo_for_material(
        self, db: AsyncSession, type_id: int, quantity_needed: int, char_id: int | None = None
    ) -> tuple[float, int, bool]:
        """
        FIFO-consume buy transactions for a material. Returns (cost, qty_consumed, fully_sourced).
        Order: buy FIFO → mining ledger (zero cost) → warehouse → Jita fallback (if char opted in).
        """
        from app.models.database import MiningLedger

        remaining = quantity_needed
        total_cost = 0.0

        # 1. Buy transactions FIFO
        buys = await db.execute(
            select(WalletTransaction)
            .where(WalletTransaction.is_buy == True,
                   WalletTransaction.type_id == type_id,
                   WalletTransaction.quantity_remaining > 0)
            .order_by(WalletTransaction.date.asc())
        )
        for buy in buys.scalars().all():
            if remaining <= 0:
                break
            qty = min(remaining, buy.quantity_remaining)
            total_cost += buy.unit_price * qty
            buy.quantity_remaining -= qty
            buy.quantity_consumed += qty
            remaining -= qty

        # 2. Mining ledger (zero cost — you already paid opportunity cost by mining)
        if remaining > 0:
            ml = await db.execute(
                select(MiningLedger)
                .where(MiningLedger.type_id == type_id,
                       MiningLedger.quantity > MiningLedger.quantity_consumed)
                .order_by(MiningLedger.date.asc())
            )
            for entry in ml.scalars().all():
                if remaining <= 0:
                    break
                avail = entry.quantity - entry.quantity_consumed
                qty = min(remaining, avail)
                # Cost is zero — mined ore is free (opportunity cost only)
                entry.quantity_consumed += qty
                remaining -= qty

        # 3. Warehouse fallback
        if remaining > 0:
            whs = await db.execute(
                select(WarehouseItem)
                .where(WarehouseItem.type_id == type_id, WarehouseItem.quantity > 0)
                .order_by(WarehouseItem.added_at.asc())
            )
            for wh in whs.scalars().all():
                if remaining <= 0:
                    break
                qty = min(remaining, wh.quantity)
                total_cost += wh.unit_price * qty
                wh.quantity -= qty
                remaining -= qty

        # 4. Jita fallback (if character opted in)
        if remaining > 0:
            fallback_price = await self._get_jita_fallback_price(db, type_id, char_id)
            if fallback_price is not None and fallback_price > 0:
                total_cost += fallback_price * remaining
                remaining = 0

        qty_consumed = quantity_needed - remaining
        fully_sourced = remaining == 0
        return total_cost, qty_consumed, fully_sourced

    async def _resolve_manufacturing_costs(self, db: AsyncSession) -> int:
        """
        For each delivered manufacturing/reaction job, consume its material inputs
        from the FIFO pool. Uses true consumption — same buys can't feed a trade sell.
        """
        # Only jobs not yet costed. Include manufacturing (1) and reactions (8).
        result = await db.execute(
            select(IndustryJob)
            .where(IndustryJob.status == "delivered")
            .where(IndustryJob.activity_id.in_([1, 8]))
            .where(IndustryJob.materials_cost.is_(None))
            .order_by(IndustryJob.completed_date.asc())  # Older jobs consume first
        )
        jobs = result.scalars().all()
        count = 0

        for job in jobs:
            # Look up materials via blueprint_type_id, with fallback via product_type_id
            bp_id = job.blueprint_type_id
            mats = []
            if bp_id:
                r = await db.execute(
                    select(SdeBlueprintMaterial).where(SdeBlueprintMaterial.blueprint_type_id == bp_id)
                )
                mats = r.scalars().all()
            if not mats and job.product_type_id:
                bp_r = await db.execute(
                    select(SdeBlueprint).where(SdeBlueprint.product_type_id == job.product_type_id)
                )
                bp = bp_r.scalar_one_or_none()
                if bp:
                    r = await db.execute(
                        select(SdeBlueprintMaterial).where(SdeBlueprintMaterial.blueprint_type_id == bp.blueprint_type_id)
                    )
                    mats = r.scalars().all()

            if not mats:
                job.materials_complete = False
                continue

            total_cost = 0.0
            all_complete = True

            for m in mats:
                needed = m.quantity * job.runs
                cost, consumed, fully = await self._consume_fifo_for_material(
                    db, m.material_type_id, needed, char_id=job.character_id
                )
                total_cost += cost
                if not fully:
                    all_complete = False

            job.materials_cost = round(total_cost, 2)
            job.materials_complete = all_complete
            job.unit_cost = round((job.cost + total_cost + (job.copy_cost or 0)) / max(job.runs, 1), 2)
            count += 1

        await db.flush()
        return count

    # ─── FIFO Trade Matching ─────────────────────────────

    async def _match_trades(self, db: AsyncSession, configs: dict = None) -> int:
        from app.models.database import SdeType

        result = await db.execute(
            select(WalletTransaction)
            .where(WalletTransaction.is_buy == False)
            .where(~WalletTransaction.transaction_id.in_(select(TradeProfit.sell_transaction_id)))
            .where(~WalletTransaction.transaction_id.in_(select(PendingTransaction.transaction_id)))
            .where(~WalletTransaction.transaction_id.in_(select(ManufacturingProfit.sell_transaction_id).where(ManufacturingProfit.sell_transaction_id.isnot(None))))
            .order_by(WalletTransaction.date.asc())
        )
        sells = result.scalars().all()
        count = 0

        # Cache freight rates and item volumes per sell
        _freight_cache = {}  # location_id → rate
        _volume_cache = {}   # type_id → m3

        for sell in sells:
            remaining = sell.quantity

            # Get freight rate for this sell location + buy origin (cached)
            sell_loc = sell.location_id
            if sell_loc not in _freight_cache:
                # Will be updated per-buy below when we have buy location
                _freight_cache[sell_loc] = {}

            # Get item volume — use PACKAGED volume for freight (ships are shipped packaged)
            if sell.type_id not in _volume_cache:
                sde = await db.get(SdeType, sell.type_id)
                if sde:
                    _volume_cache[sell.type_id] = sde.packaged_volume or sde.volume or 0
                else:
                    _volume_cache[sell.type_id] = 0
            item_vol = _volume_cache[sell.type_id]

            # Try buy transactions first (FIFO) — filtered by cost basis config
            buys = await db.execute(
                select(WalletTransaction)
                .where(WalletTransaction.is_buy == True, WalletTransaction.type_id == sell.type_id, WalletTransaction.quantity_remaining > 0)
                .order_by(WalletTransaction.date.asc())
            )
            for buy in buys.scalars().all():
                if remaining <= 0:
                    break
                # Cost basis filter: skip excluded buys
                if configs and _is_buy_excluded(buy.character_id, buy.location_id, configs):
                    continue
                qty = min(remaining, buy.quantity_remaining)
                fees = await self._get_broker_rates(db, sell.character_id, sell.type_id, sell.date)

                # Freight: use buy location as origin, sell location as destination
                cache_key = (buy.location_id or 0, sell_loc)
                if cache_key not in _freight_cache:
                    _freight_cache[cache_key] = await self._get_freight_rate(db, sell_loc, buy.location_id)
                freight_rate = _freight_cache[cache_key]
                freight_per_unit = item_vol * freight_rate

                bb = buy.unit_price * qty * (fees["buy"] / 100)
                sb = sell.unit_price * qty * ((fees["sell_structure"] if self._is_structure(sell.location_id) else fees["sell_npc"]) / 100)
                tx = sell.unit_price * qty * (fees["sales_tax"] / 100)
                fc = freight_per_unit * qty
                tb = buy.unit_price * qty
                ts = sell.unit_price * qty
                profit = ts - tb - bb - sb - tx - fc
                db.add(TradeProfit(
                    sell_transaction_id=sell.transaction_id, buy_transaction_id=buy.transaction_id,
                    type_id=sell.type_id, quantity=qty, unit_buy=buy.unit_price, unit_sell=sell.unit_price,
                    total_buy=tb, total_sell=ts, broker_buy=round(bb, 2), broker_sell=round(sb, 2),
                    sales_tax=round(tx, 2), freight_cost=round(fc, 2),
                    margin_pct=round((profit/ts*100) if ts else 0, 2),
                    profit=round(profit, 2), date=sell.date,
                ))
                buy.quantity_remaining -= qty
                buy.quantity_matched += qty
                remaining -= qty
                count += 1
            # Try warehouse items as fallback
            if remaining > 0:
                whs = await db.execute(select(WarehouseItem).where(WarehouseItem.type_id == sell.type_id, WarehouseItem.quantity > 0).order_by(WarehouseItem.added_at.asc()))
                for wh in whs.scalars().all():
                    if remaining <= 0:
                        break
                    qty = min(remaining, wh.quantity)
                    fees = await self._get_broker_rates(db, sell.character_id, sell.type_id, sell.date)
                    sb = sell.unit_price * qty * ((fees["sell_structure"] if self._is_structure(sell.location_id) else fees["sell_npc"]) / 100)
                    tx = sell.unit_price * qty * (fees["sales_tax"] / 100)
                    fc = freight_per_unit * qty
                    tb = wh.unit_price * qty
                    ts = sell.unit_price * qty
                    profit = ts - tb - sb - tx - fc
                    db.add(TradeProfit(
                        sell_transaction_id=sell.transaction_id, buy_transaction_id=None,
                        type_id=sell.type_id, quantity=qty, unit_buy=wh.unit_price, unit_sell=sell.unit_price,
                        total_buy=tb, total_sell=ts, broker_buy=0, broker_sell=round(sb, 2),
                        sales_tax=round(tx, 2), freight_cost=round(fc, 2),
                        margin_pct=round((profit/ts*100) if ts else 0, 2),
                        profit=round(profit, 2), date=sell.date,
                    ))
                    wh.quantity -= qty
                    remaining -= qty
                    count += 1
        await db.flush()
        return count

    # ─── Manufacturing Matching ──────────────────────────

    async def _match_manufacturing(self, db: AsyncSession) -> int:
        result = await db.execute(
            select(WalletTransaction)
            .where(WalletTransaction.is_buy == False)
            .where(~WalletTransaction.transaction_id.in_(select(TradeProfit.sell_transaction_id)))
            .where(~WalletTransaction.transaction_id.in_(select(PendingTransaction.transaction_id)))
            .where(~WalletTransaction.transaction_id.in_(select(ManufacturingProfit.sell_transaction_id).where(ManufacturingProfit.sell_transaction_id.isnot(None))))
            .order_by(WalletTransaction.date.asc())
        )
        sells = result.scalars().all()
        count = 0
        for sell in sells:
            jobs = await db.execute(
                select(IndustryJob)
                .where(IndustryJob.product_type_id == sell.type_id, IndustryJob.status == "delivered")
                .where(IndustryJob.quantity_produced > (IndustryJob.quantity_sold + IndustryJob.quantity_consumed))
                .order_by(IndustryJob.completed_date.asc())
            )
            remaining = sell.quantity
            for job in jobs.scalars().all():
                if remaining <= 0:
                    break
                avail = job.quantity_produced - job.quantity_sold - job.quantity_consumed
                qty = min(remaining, avail)
                ub = job.unit_cost if job.unit_cost else (job.cost / max(job.runs, 1))
                fees = await self._get_broker_rates(db, sell.character_id, sell.type_id, sell.date)
                sb = sell.unit_price * qty * ((fees["sell_structure"] if self._is_structure(sell.location_id) else fees["sell_npc"]) / 100)
                tx = sell.unit_price * qty * (fees["sales_tax"] / 100)
                tb = ub * qty
                ts = sell.unit_price * qty
                profit = ts - tb - sb - tx
                db.add(ManufacturingProfit(
                    job_id=job.job_id, sell_transaction_id=sell.transaction_id,
                    type_id=sell.type_id, quantity=qty, unit_build=round(ub, 2), unit_sell=sell.unit_price,
                    total_build=round(tb, 2), total_sell=ts, broker_sell=round(sb, 2),
                    sales_tax=round(tx, 2), margin_pct=round((profit/ts*100) if ts else 0, 2),
                    profit=round(profit, 2), date=sell.date,
                ))
                job.quantity_sold += qty
                remaining -= qty
                count += 1
        await db.flush()
        return count

    # ─── Pending ─────────────────────────────────────────

    async def _create_pending(self, db: AsyncSession) -> int:
        """
        Create pending records for unmatched sells. If the character has
        use_jita_fallback enabled, auto-resolve these by creating a TradeProfit
        with Jita price as the cost basis instead of leaving them pending.
        """
        result = await db.execute(
            select(WalletTransaction)
            .where(WalletTransaction.is_buy == False)
            .where(~WalletTransaction.transaction_id.in_(select(TradeProfit.sell_transaction_id)))
            .where(~WalletTransaction.transaction_id.in_(select(PendingTransaction.transaction_id)))
            .where(~WalletTransaction.transaction_id.in_(select(ManufacturingProfit.sell_transaction_id).where(ManufacturingProfit.sell_transaction_id.isnot(None))))
        )
        count = 0
        for sell in result.scalars().all():
            # Try Jita fallback first if the character opted in
            fallback = await self._get_jita_fallback_price(db, sell.type_id, sell.character_id)
            if fallback is not None and fallback > 0:
                # Auto-resolve with Jita price as cost basis
                fees = await self._get_broker_rates(db, sell.character_id, sell.type_id, sell.date)
                qty = sell.quantity
                tb = fallback * qty
                ts = sell.unit_price * qty
                sb = ts * ((fees["sell_structure"] if self._is_structure(sell.location_id) else fees["sell_npc"]) / 100)
                tx = ts * (fees["sales_tax"] / 100)
                profit = ts - tb - sb - tx
                db.add(TradeProfit(
                    sell_transaction_id=sell.transaction_id, buy_transaction_id=None,
                    type_id=sell.type_id, quantity=qty, unit_buy=fallback, unit_sell=sell.unit_price,
                    total_buy=round(tb, 2), total_sell=round(ts, 2),
                    broker_buy=0, broker_sell=round(sb, 2),
                    sales_tax=round(tx, 2), freight_cost=0,
                    margin_pct=round((profit/ts*100) if ts else 0, 2),
                    profit=round(profit, 2), date=sell.date,
                ))
                continue

            # Otherwise go to pending
            db.add(PendingTransaction(
                transaction_id=sell.transaction_id, character_id=sell.character_id,
                type_id=sell.type_id, quantity=sell.quantity, unit_sell=sell.unit_price,
                total_sell=sell.unit_price * sell.quantity, status="pending", date=sell.date,
            ))
            count += 1
        await db.flush()
        return count

    # ─── Contract Profits ────────────────────────────────

    async def _match_contracts(self, db: AsyncSession, configs: dict = None) -> int:
        from app.services.esi_auth import esi_auth

        # Get ALL linked character IDs for cross-character support
        linked_ids = await esi_auth.get_all_character_ids(db)
        if not linked_ids:
            return 0

        result = await db.execute(
            select(CharacterContract)
            .where(CharacterContract.contract_type == "item_exchange")
            .where(CharacterContract.status.in_(["finished", "completed"]))
            .where(CharacterContract.issuer_id.in_(linked_ids))  # Contracts issued by ANY linked char
            .where(CharacterContract.price > 0)  # Must have a sell price
            .where(~CharacterContract.contract_id.in_(select(ContractProfit.contract_id)))
        )
        count = 0
        for contract in result.scalars().all():
            # SKIP inter-character transfers (both issuer and acceptor are linked)
            if contract.acceptor_id in linked_ids:
                logger.info(f"Skipping contract {contract.contract_id} — inter-character transfer")
                continue

            items_r = await db.execute(
                select(CharacterContractItem)
                .where(CharacterContractItem.contract_id == contract.contract_id)
                .where(CharacterContractItem.is_included == True)
            )
            items = items_r.scalars().all()
            if not items:
                continue

            # TRUE FIFO: consume specific buy transactions for each item (oldest first)
            # For contracts, prefer buys from NON-ISSUER characters first (e.g. kyle5003
            # bought items in Jita, transferred to Miles Deep Uta who listed the contract)
            from app.models.database import SdeType
            total_cost = 0
            total_freight_volume = 0
            items_complete = True

            for item in items:
                remaining = item.quantity

                # Get item packaged volume for freight calc
                sde = await db.get(SdeType, item.type_id)
                item_vol = (sde.packaged_volume or sde.volume or 0) if sde else 0
                total_freight_volume += item_vol * item.quantity

                # FIFO: first try buys from OTHER linked characters (the buyer/shipper)
                # then fall back to issuer's own buys — filtered by cost basis config
                other_ids = [cid for cid in linked_ids if cid != contract.issuer_id]

                for char_pool in [other_ids, [contract.issuer_id]] if other_ids else [[contract.issuer_id]]:
                    if remaining <= 0:
                        break
                    buys = await db.execute(
                        select(WalletTransaction)
                        .where(WalletTransaction.is_buy == True,
                               WalletTransaction.type_id == item.type_id,
                               WalletTransaction.quantity_remaining > 0,
                               WalletTransaction.character_id.in_(char_pool))
                        .order_by(WalletTransaction.date.asc())
                    )
                    for buy in buys.scalars().all():
                        if remaining <= 0:
                            break
                        # Cost basis filter: skip excluded buys
                        if configs and _is_buy_excluded(buy.character_id, buy.location_id, configs):
                            continue
                        qty = min(remaining, buy.quantity_remaining)
                        total_cost += buy.unit_price * qty
                        buy.quantity_remaining -= qty
                        buy.quantity_matched += qty
                        remaining -= qty

                # Warehouse fallback
                if remaining > 0:
                    whs = await db.execute(
                        select(WarehouseItem)
                        .where(WarehouseItem.type_id == item.type_id, WarehouseItem.quantity > 0)
                        .order_by(WarehouseItem.added_at.asc())
                    )
                    for wh in whs.scalars().all():
                        if remaining <= 0:
                            break
                        qty = min(remaining, wh.quantity)
                        total_cost += wh.unit_price * qty
                        wh.quantity -= qty
                        remaining -= qty

                # If still remaining, items are uncosted (salvaged/looted = free)
                if remaining > 0:
                    items_complete = False

            # Build title from items if contract has no title
            title = contract.title
            if not title:
                from app.models.database import SdeType
                best_item = None
                best_cost = 0
                for item in items:
                    sde = await db.get(SdeType, item.type_id)
                    cost = await self._get_item_cost(db, item.type_id, use_jita_fallback=True)  # OK for title picking
                    if cost and cost > best_cost:
                        best_cost = cost
                        best_item = sde.name if sde else f"Type {item.type_id}"
                if best_item:
                    title = f"{best_item} + {len(items)-1} items" if len(items) > 1 else best_item

            # Resolve acceptor name (could be character OR corporation)
            acceptor_name = contract.acceptor_name
            if not acceptor_name and contract.acceptor_id:
                try:
                    import httpx
                    async with httpx.AsyncClient(timeout=10.0) as http:
                        # Use universal name resolver — handles characters, corps, alliances
                        resp = await http.post(
                            "https://esi.evetech.net/latest/universe/names/",
                            params={"datasource": "tranquility"},
                            json=[contract.acceptor_id],
                        )
                        if resp.status_code == 200:
                            names = resp.json()
                            if names:
                                entity = names[0]
                                cat = entity.get("category", "")
                                name = entity.get("name", "")
                                if cat == "corporation":
                                    acceptor_name = f"{name} [Corp]"
                                elif cat == "alliance":
                                    acceptor_name = f"{name} [Alliance]"
                                else:
                                    acceptor_name = name
                                contract.acceptor_name = acceptor_name
                        else:
                            # Fallback: try character endpoint directly
                            resp2 = await http.get(
                                f"https://esi.evetech.net/latest/characters/{contract.acceptor_id}/",
                                params={"datasource": "tranquility"},
                            )
                            if resp2.status_code == 200:
                                acceptor_name = resp2.json().get("name", f"ID {contract.acceptor_id}")
                                contract.acceptor_name = acceptor_name
                            else:
                                acceptor_name = f"ID {contract.acceptor_id}"
                except Exception as e:
                    logger.warning(f"Could not resolve acceptor {contract.acceptor_id}: {e}")
                    acceptor_name = f"ID {contract.acceptor_id}"

            price = contract.price or 0

            # Freight rate from route table using contract location
            # (volume already calculated in the FIFO loop above)
            if contract.start_location_id:
                freight_rate = await self._get_freight_rate(db, contract.start_location_id)
            else:
                freight_rate = settings.freight_cost_per_m3

            freight = total_freight_volume * freight_rate
            broker = price * 0.0095 if contract.availability == "public" else 10000
            tax = price * 0.005 if contract.availability == "public" else 0
            profit = price - total_cost - freight - broker - tax
            margin = (profit / price * 100) if price > 0 else 0

            db.add(ContractProfit(
                contract_id=contract.contract_id, character_id=contract.character_id,
                acceptor_id=contract.acceptor_id, acceptor_name=acceptor_name,
                title=title, total_cost=round(total_cost, 2), total_sell=price,
                freight_cost=round(freight, 2), volume_m3=round(total_freight_volume, 2),
                broker_fee=round(broker, 2), sales_tax=round(tax, 2),
                margin_pct=round(margin, 2), profit=round(profit, 2),
                items_complete=items_complete,
                date=contract.date_completed or contract.date_issued,
            ))
            count += 1
        await db.flush()
        return count

    # ─── Summary + Mark Consumed ─────────────────────────

    async def get_profit_summary(self, db: AsyncSession, days: int = 30) -> dict:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        tp = (await db.execute(select(func.sum(TradeProfit.profit)).where(TradeProfit.date >= cutoff))).scalar() or 0
        mp = (await db.execute(select(func.sum(ManufacturingProfit.profit)).where(ManufacturingProfit.date >= cutoff))).scalar() or 0
        cp = (await db.execute(select(func.sum(ContractProfit.profit)).where(ContractProfit.date >= cutoff))).scalar() or 0
        inc = (await db.execute(select(func.sum(WalletTransaction.unit_price * WalletTransaction.quantity)).where(WalletTransaction.is_buy == False, WalletTransaction.date >= cutoff))).scalar() or 0
        pur = (await db.execute(select(func.sum(WalletTransaction.unit_price * WalletTransaction.quantity)).where(WalletTransaction.is_buy == True, WalletTransaction.date >= cutoff))).scalar() or 0
        bc = (await db.execute(select(func.count()).select_from(WalletTransaction).where(WalletTransaction.is_buy == True, WalletTransaction.date >= cutoff))).scalar() or 0
        sc = (await db.execute(select(func.count()).select_from(WalletTransaction).where(WalletTransaction.is_buy == False, WalletTransaction.date >= cutoff))).scalar() or 0
        taxes = abs((await db.execute(select(func.sum(WalletJournal.amount)).where(WalletJournal.ref_type == "transaction_tax", WalletJournal.date >= cutoff))).scalar() or 0)
        fees = abs((await db.execute(select(func.sum(WalletJournal.amount)).where(WalletJournal.ref_type == "brokers_fee", WalletJournal.date >= cutoff))).scalar() or 0)
        return {
            "net_profit": round(tp + mp + cp, 2), "trade_profit": round(tp, 2),
            "manufacturing_profit": round(mp, 2), "contract_profit": round(cp, 2),
            "trade_income": round(inc, 2), "trade_purchases": round(pur, 2),
            "sales_tax": round(taxes, 2), "broker_fees": round(fees, 2),
            "buy_transactions": bc, "sell_transactions": sc, "period_days": days,
        }

    async def mark_consumed(self, db: AsyncSession, transaction_id: int, quantity: int) -> dict:
        tx = await db.get(WalletTransaction, transaction_id)
        if not tx or not tx.is_buy:
            return {"error": "Not found or not a buy"}
        consume = min(quantity, tx.quantity_remaining)
        tx.quantity_consumed += consume
        tx.quantity_remaining -= consume
        await db.commit()
        return {"consumed": consume, "remaining": tx.quantity_remaining}


profit_engine = ProfitEngine()
