"""
Void Market — Database Models
"""
from sqlalchemy import (
    Column, Integer, BigInteger, String, Float, Text, Boolean,
    DateTime, JSON, ForeignKey, Index, UniqueConstraint
)
from sqlalchemy.orm import DeclarativeBase, relationship
from datetime import datetime, timezone


class Base(DeclarativeBase):
    pass


# ─── SDE Tables ─────────────────────────────────────────────

class SdeType(Base):
    """EVE type from SDE (ships, modules, ammo, materials, etc.)"""
    __tablename__ = "sde_types"

    type_id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text, nullable=True)
    group_id = Column(Integer, index=True)
    category_id = Column(Integer, index=True)
    market_group_id = Column(Integer, nullable=True)
    volume = Column(Float, nullable=True)
    packaged_volume = Column(Float, nullable=True)  # Ships use packaged vol for freight
    portion_size = Column(Integer, default=1)
    meta_level = Column(Integer, nullable=True)
    published = Column(Boolean, default=True)


class SdeBlueprint(Base):
    """Blueprint manufacturing data from SDE"""
    __tablename__ = "sde_blueprints"

    blueprint_type_id = Column(Integer, primary_key=True)
    product_type_id = Column(Integer, ForeignKey("sde_types.type_id"), index=True)
    product_quantity = Column(Integer, default=1)
    manufacturing_time = Column(Integer, nullable=True)  # seconds


class SdeBlueprintMaterial(Base):
    """Materials required for a blueprint"""
    __tablename__ = "sde_blueprint_materials"

    id = Column(Integer, primary_key=True, autoincrement=True)
    blueprint_type_id = Column(Integer, ForeignKey("sde_blueprints.blueprint_type_id"), index=True)
    material_type_id = Column(Integer, ForeignKey("sde_types.type_id"))
    quantity = Column(Integer, nullable=False)

    __table_args__ = (
        UniqueConstraint("blueprint_type_id", "material_type_id"),
    )


class SdeMarketGroup(Base):
    """Market group hierarchy from SDE"""
    __tablename__ = "sde_market_groups"

    market_group_id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    parent_group_id = Column(Integer, nullable=True)
    description = Column(Text, nullable=True)


class SdeGroup(Base):
    """Item group from SDE (e.g., Battleship, Interdictor, Cruiser)"""
    __tablename__ = "sde_groups"

    group_id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    category_id = Column(Integer, nullable=True)


# ─── Doctrine Tables ────────────────────────────────────────

class Doctrine(Base):
    """A named doctrine (e.g., 'Muninn Fleet', 'Cerb Fleet')"""
    __tablename__ = "doctrines"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False, unique=True)
    description = Column(Text, nullable=True)
    priority = Column(Integer, default=5)  # 1=critical, 10=low priority
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))

    fits = relationship("DoctrineFit", back_populates="doctrine", cascade="all, delete-orphan")


class DoctrineFit(Base):
    """A specific ship fitting within a doctrine"""
    __tablename__ = "doctrine_fits"

    id = Column(Integer, primary_key=True, autoincrement=True)
    doctrine_id = Column(Integer, ForeignKey("doctrines.id"), nullable=False)
    name = Column(String(255), nullable=False)  # e.g., "Muninn - DPS"
    ship_type_id = Column(Integer, ForeignKey("sde_types.type_id"), nullable=False)
    eft_text = Column(Text, nullable=False)  # Raw EFT block
    role = Column(String(50), nullable=True)  # dps, logi, tackle, etc.
    is_primary = Column(Boolean, default=False)  # Primary doctrine ship
    min_stock = Column(Integer, default=10)  # Desired minimum on market
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    doctrine = relationship("Doctrine", back_populates="fits")
    items = relationship("DoctrineFitItem", back_populates="fit", cascade="all, delete-orphan")


class DoctrineFitItem(Base):
    """Individual item in a fit (module, charge, drone, etc.)"""
    __tablename__ = "doctrine_fit_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fit_id = Column(Integer, ForeignKey("doctrine_fits.id"), nullable=False)
    type_id = Column(Integer, ForeignKey("sde_types.type_id"), nullable=False)
    quantity = Column(Integer, default=1)
    slot_type = Column(String(20), nullable=True)  # high, mid, low, rig, drone, cargo, charge

    fit = relationship("DoctrineFit", back_populates="items")

    __table_args__ = (
        Index("ix_fit_items_fit_type", "fit_id", "type_id"),
    )


# ─── ESI Auth ───────────────────────────────────────────────

class EsiCharacter(Base):
    """Authenticated ESI character"""
    __tablename__ = "esi_characters"

    character_id = Column(BigInteger, primary_key=True)
    character_name = Column(String(255), nullable=False)
    corporation_id = Column(BigInteger, nullable=True)
    alliance_id = Column(BigInteger, nullable=True)
    access_token = Column(Text, nullable=False)
    refresh_token = Column(Text, nullable=False)
    token_expiry = Column(DateTime, nullable=False)
    scopes = Column(Text, nullable=False)
    is_active = Column(Boolean, default=True)  # Primary character for API calls
    # Fallback pricing when a sell can't find a buy transaction
    # When enabled, items default to Jita price instead of going to pending
    use_jita_fallback = Column(Boolean, default=False)
    fallback_price_type = Column(String(20), default="sell_min")  # sell_min, buy_max, avg
    last_wallet_balance = Column(Float, nullable=True)  # Current ISK, updated during wallet sync
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc),
                        onupdate=lambda: datetime.now(timezone.utc))


# ─── Market Data ────────────────────────────────────────────

class MarketOrder(Base):
    """Current market orders in the tracked structure"""
    __tablename__ = "market_orders"

    order_id = Column(BigInteger, primary_key=True)
    type_id = Column(Integer, ForeignKey("sde_types.type_id"), index=True)
    is_buy_order = Column(Boolean, nullable=False)
    price = Column(Float, nullable=False)
    volume_remain = Column(Integer, nullable=False)
    volume_total = Column(Integer, nullable=False)
    location_id = Column(BigInteger, nullable=False)
    issued = Column(DateTime, nullable=False)
    duration = Column(Integer, nullable=False)
    last_seen = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class MarketSnapshot(Base):
    """Periodic snapshots of market state for velocity tracking"""
    __tablename__ = "market_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    type_id = Column(Integer, ForeignKey("sde_types.type_id"), index=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc), index=True)
    sell_volume = Column(Integer, default=0)  # Total sell volume at snapshot time
    sell_min_price = Column(Float, nullable=True)
    buy_volume = Column(Integer, default=0)
    buy_max_price = Column(Float, nullable=True)
    order_count = Column(Integer, default=0)

    __table_args__ = (
        Index("ix_snapshots_type_time", "type_id", "timestamp"),
    )


class JitaPrice(Base):
    """Cached Jita prices for comparison"""
    __tablename__ = "jita_prices"

    type_id = Column(Integer, ForeignKey("sde_types.type_id"), primary_key=True)
    sell_min = Column(Float, nullable=True)
    sell_volume = Column(Integer, default=0)
    buy_max = Column(Float, nullable=True)
    buy_volume = Column(Integer, default=0)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


# ─── Contract Data ──────────────────────────────────────────

class AllianceContract(Base):
    """Alliance contracts (item exchange / fitted ships)"""
    __tablename__ = "alliance_contracts"

    contract_id = Column(BigInteger, primary_key=True)
    issuer_id = Column(BigInteger, nullable=False)
    status = Column(String(50), nullable=False)
    contract_type = Column(String(50), nullable=False)  # item_exchange, auction, etc.
    title = Column(String(255), nullable=True)
    price = Column(Float, nullable=True)
    volume = Column(Float, nullable=True)
    date_issued = Column(DateTime, nullable=False)
    date_expired = Column(DateTime, nullable=True)
    last_seen = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class ContractItem(Base):
    """Items within a contract"""
    __tablename__ = "contract_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    contract_id = Column(BigInteger, ForeignKey("alliance_contracts.contract_id"), index=True)
    type_id = Column(Integer, ForeignKey("sde_types.type_id"))
    quantity = Column(Integer, default=1)
    is_included = Column(Boolean, default=True)

    __table_args__ = (
        Index("ix_contract_items_contract_type", "contract_id", "type_id"),
    )


# ─── Fight / Killmail Data (LEGACY — new data goes to killmail.db) ──

class Fight(Base):
    """A grouped major fight event (legacy — see killmail_models.py)"""
    __tablename__ = "fights"

    id = Column(Integer, primary_key=True, autoincrement=True)
    system_name = Column(String(50), nullable=False)
    system_id = Column(BigInteger, nullable=True)
    started_at = Column(DateTime, nullable=False)
    ended_at = Column(DateTime, nullable=False)
    alliance_losses = Column(Integer, default=0)
    alliance_kills = Column(Integer, default=0)
    total_isk_lost = Column(Float, default=0.0)
    total_isk_killed = Column(Float, default=0.0)

    losses = relationship("FightLoss", back_populates="fight", cascade="all, delete-orphan")


class FightLoss(Base):
    """Individual alliance loss in a fight (legacy — see killmail_models.py)"""
    __tablename__ = "fight_losses"

    killmail_id = Column(BigInteger, primary_key=True)
    fight_id = Column(Integer, ForeignKey("fights.id"), index=True)
    ship_type_id = Column(Integer, ForeignKey("sde_types.type_id"))
    victim_id = Column(BigInteger, nullable=True)
    killed_at = Column(DateTime, nullable=False)
    total_value = Column(Float, default=0.0)
    fit_items = Column(JSON, nullable=True)

    fight = relationship("Fight", back_populates="losses")


# ─── Opportunity Scoring ────────────────────────────────────

class Opportunity(Base):
    """Calculated market opportunity"""
    __tablename__ = "opportunities"

    id = Column(Integer, primary_key=True, autoincrement=True)
    type_id = Column(Integer, ForeignKey("sde_types.type_id"), index=True)
    score = Column(Float, nullable=False)  # Overall opportunity score
    doctrine_demand = Column(Integer, default=0)  # Units needed per doctrine cycle
    market_stock = Column(Integer, default=0)  # Current sell volume
    contract_stock = Column(Integer, default=0)  # Available on contracts
    recent_losses = Column(Integer, default=0)  # Lost in recent fights
    sell_velocity = Column(Float, default=0.0)  # Units sold per day (estimated)
    jita_buy_price = Column(Float, nullable=True)
    local_sell_price = Column(Float, nullable=True)
    build_cost = Column(Float, nullable=True)  # T1 manufacturing cost estimate
    profit_margin = Column(Float, nullable=True)  # Best margin (buy vs build)
    recommendation = Column(String(20), nullable=True)  # "build", "buy", "import"
    calculated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index("ix_opportunities_score", "score"),
    )


# ─── Manual Contract Entries ────────────────────────────────

class ManualContract(Base):
    """Manually entered contract stock (for alliance contracts ESI can't see)"""
    __tablename__ = "manual_contracts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    doctrine_id = Column(Integer, ForeignKey("doctrines.id"), nullable=True)
    ship_type_id = Column(Integer, ForeignKey("sde_types.type_id"), nullable=False)
    quantity = Column(Integer, default=1)
    price = Column(Float, nullable=True)
    location = Column(String(100), nullable=True)
    notes = Column(String(255), nullable=True)
    added_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


# ─── Cached Analysis Results ────────────────────────────────

class AnalysisCache(Base):
    """Cached market analysis results"""
    __tablename__ = "analysis_cache"

    id = Column(Integer, primary_key=True, autoincrement=True)
    type_id = Column(Integer, ForeignKey("sde_types.type_id"), index=True)
    name = Column(String(255))
    volume_m3 = Column(Float, nullable=True)
    doctrine_demand = Column(Integer, default=0)
    local_stock = Column(Integer, default=0)
    contract_stock = Column(Integer, default=0)
    jita_sell = Column(Float, nullable=True)
    import_cost = Column(Float, nullable=True)
    local_sell = Column(Float, nullable=True)
    markup_pct = Column(Float, nullable=True)
    target_price = Column(Float, nullable=True)
    weekly_volume = Column(Float, default=0)
    weekly_profit = Column(Float, nullable=True)
    stock_status = Column(String(10), default="ok")
    calculated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


# ─── Wallet & Transaction Tracking (EVE Tycoon P&L) ────────

class WalletTransaction(Base):
    """Market buy/sell transactions from ESI wallet"""
    __tablename__ = "wallet_transactions"

    transaction_id = Column(BigInteger, primary_key=True)
    character_id = Column(BigInteger, index=True)
    type_id = Column(Integer, index=True)
    is_buy = Column(Boolean, nullable=False)
    unit_price = Column(Float, nullable=False)
    quantity = Column(Integer, nullable=False)
    location_id = Column(BigInteger, nullable=True)
    journal_ref_id = Column(BigInteger, nullable=True)
    date = Column(DateTime, nullable=False, index=True)
    client_id = Column(BigInteger, nullable=True)  # Who you traded with
    # FIFO tracking
    quantity_matched = Column(Integer, default=0)  # How many units matched to sells/buys
    quantity_consumed = Column(Integer, default=0)  # Used in manufacturing
    quantity_remaining = Column(Integer, default=0)  # Still in inventory

    __table_args__ = (
        Index("ix_wallet_tx_type_date", "type_id", "date"),
    )


class WalletJournal(Base):
    """Wallet journal entries from ESI — every ISK movement"""
    __tablename__ = "wallet_journal"

    ref_id = Column(BigInteger, primary_key=True)
    character_id = Column(BigInteger, index=True)
    ref_type = Column(String(100), nullable=False)  # market_transaction, brokers_fee, etc.
    amount = Column(Float, nullable=False)
    balance = Column(Float, nullable=True)
    date = Column(DateTime, nullable=False, index=True)
    description = Column(Text, nullable=True)
    first_party_id = Column(BigInteger, nullable=True)
    second_party_id = Column(BigInteger, nullable=True)
    context_id = Column(BigInteger, nullable=True)
    context_id_type = Column(String(50), nullable=True)


class CharacterOrder(Base):
    """Your active market orders — for undercut detection"""
    __tablename__ = "character_orders"

    order_id = Column(BigInteger, primary_key=True)
    character_id = Column(BigInteger, index=True)
    type_id = Column(Integer, index=True)
    is_buy_order = Column(Boolean, nullable=False)
    price = Column(Float, nullable=False)
    volume_remain = Column(Integer, nullable=False)
    volume_total = Column(Integer, nullable=False)
    location_id = Column(BigInteger, nullable=True)
    region_id = Column(Integer, nullable=True)
    issued = Column(DateTime, nullable=True)
    duration = Column(Integer, nullable=True)
    min_volume = Column(Integer, default=1)
    range = Column(String(20), nullable=True)
    # Undercut detection (computed)
    is_undercut = Column(Boolean, default=False)
    undercut_by = Column(Float, nullable=True)  # ISK difference
    lowest_competitor = Column(Float, nullable=True)
    last_checked = Column(DateTime, nullable=True)


# ─── Industry Tracking ─────────────────────────────────────

class IndustryJob(Base):
    """Completed industry jobs from ESI"""
    __tablename__ = "industry_jobs"

    job_id = Column(BigInteger, primary_key=True)
    character_id = Column(BigInteger, index=True)
    activity_id = Column(Integer, nullable=False)  # 1=mfg, 3=TE, 4=ME, 5=copy, 8=invention
    blueprint_type_id = Column(Integer, nullable=True)
    product_type_id = Column(Integer, index=True)
    runs = Column(Integer, default=1)
    licensed_runs = Column(Integer, nullable=True)
    cost = Column(Float, default=0)  # Job installation cost
    status = Column(String(20), nullable=False)  # active, delivered, cancelled
    facility_id = Column(BigInteger, nullable=True)
    start_date = Column(DateTime, nullable=True)
    end_date = Column(DateTime, nullable=True)
    completed_date = Column(DateTime, nullable=True, index=True)
    # Computed costs
    materials_cost = Column(Float, nullable=True)  # From FIFO material matching
    copy_cost = Column(Float, nullable=True)
    unit_cost = Column(Float, nullable=True)  # (cost + materials + copy) / runs
    # Output tracking
    quantity_produced = Column(Integer, default=0)
    quantity_sold = Column(Integer, default=0)
    quantity_consumed = Column(Integer, default=0)
    materials_complete = Column(Boolean, default=True)


# ─── Character Contracts ───────────────────────────────────

class CharacterContract(Base):
    """Contracts issued by your characters from ESI"""
    __tablename__ = "character_contracts"

    contract_id = Column(BigInteger, primary_key=True)
    character_id = Column(BigInteger, index=True)
    issuer_id = Column(BigInteger, nullable=False)
    acceptor_id = Column(BigInteger, nullable=True)
    acceptor_name = Column(String(255), nullable=True)
    contract_type = Column(String(50), nullable=False)  # item_exchange, courier, etc.
    status = Column(String(50), nullable=False)
    title = Column(String(255), nullable=True)
    price = Column(Float, nullable=True)
    reward = Column(Float, nullable=True)
    volume = Column(Float, nullable=True)
    date_issued = Column(DateTime, nullable=False)
    date_completed = Column(DateTime, nullable=True, index=True)
    date_expired = Column(DateTime, nullable=True)
    for_corporation = Column(Boolean, default=False)
    availability = Column(String(20), nullable=True)  # public, personal, corporation, alliance
    start_location_id = Column(BigInteger, nullable=True)  # Where the contract was listed


class CharacterContractItem(Base):
    """Items within a character contract"""
    __tablename__ = "character_contract_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    contract_id = Column(BigInteger, index=True)
    type_id = Column(Integer, index=True)
    quantity = Column(Integer, default=1)
    is_included = Column(Boolean, default=True)  # True = given by issuer, False = requested
    record_id = Column(BigInteger, nullable=True)  # ESI record_id


# ─── P&L Records (Computed) ────────────────────────────────

class TradeProfit(Base):
    """Matched buy→sell profit record"""
    __tablename__ = "trade_profits"

    id = Column(Integer, primary_key=True, autoincrement=True)
    sell_transaction_id = Column(BigInteger, index=True)
    buy_transaction_id = Column(BigInteger, nullable=True)
    type_id = Column(Integer, index=True)
    quantity = Column(Integer, nullable=False)
    unit_buy = Column(Float, nullable=False)
    unit_sell = Column(Float, nullable=False)
    total_buy = Column(Float, nullable=False)
    total_sell = Column(Float, nullable=False)
    broker_buy = Column(Float, default=0)
    broker_sell = Column(Float, default=0)
    sales_tax = Column(Float, default=0)
    freight_cost = Column(Float, default=0)  # item_volume × route_rate × quantity
    margin_pct = Column(Float, nullable=True)
    profit = Column(Float, nullable=False)
    date = Column(DateTime, nullable=False, index=True)

    __table_args__ = (
        Index("ix_trade_profits_type_date", "type_id", "date"),
    )


class ManufacturingProfit(Base):
    """Matched manufacturing job→sell profit record"""
    __tablename__ = "manufacturing_profits"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(BigInteger, nullable=True)
    sell_transaction_id = Column(BigInteger, nullable=True)
    type_id = Column(Integer, index=True)
    quantity = Column(Integer, nullable=False)
    unit_build = Column(Float, nullable=False)
    unit_sell = Column(Float, nullable=False)
    total_build = Column(Float, nullable=False)
    total_sell = Column(Float, nullable=False)
    broker_sell = Column(Float, default=0)
    sales_tax = Column(Float, default=0)
    margin_pct = Column(Float, nullable=True)
    profit = Column(Float, nullable=False)
    date = Column(DateTime, nullable=False, index=True)


class ContractProfit(Base):
    """Profit per completed contract"""
    __tablename__ = "contract_profits"

    id = Column(Integer, primary_key=True, autoincrement=True)
    contract_id = Column(BigInteger, unique=True, index=True)
    character_id = Column(BigInteger, nullable=True)
    acceptor_id = Column(BigInteger, nullable=True)
    acceptor_name = Column(String(255), nullable=True)
    title = Column(String(255), nullable=True)
    total_cost = Column(Float, nullable=False)  # Sum of item costs
    total_sell = Column(Float, nullable=False)  # Contract price
    freight_cost = Column(Float, default=0)  # volume_m3 × freight_rate
    volume_m3 = Column(Float, default=0)  # Contract volume in m3
    broker_fee = Column(Float, default=0)
    sales_tax = Column(Float, default=0)
    margin_pct = Column(Float, nullable=True)
    profit = Column(Float, nullable=False)
    items_complete = Column(Boolean, default=True)  # All item costs found?
    date = Column(DateTime, nullable=False, index=True)


class PendingTransaction(Base):
    """Sell transactions with no matching buy — awaiting manual cost entry"""
    __tablename__ = "pending_transactions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    transaction_id = Column(BigInteger, index=True)
    character_id = Column(BigInteger, nullable=True)
    type_id = Column(Integer, index=True)
    quantity = Column(Integer, nullable=False)
    unit_sell = Column(Float, nullable=False)
    total_sell = Column(Float, nullable=False)
    custom_buy_price = Column(Float, nullable=True)  # Manually entered cost basis
    status = Column(String(20), default="pending")  # pending, resolved, ignored
    date = Column(DateTime, nullable=False)


# ─── Inventory & Warehouse ─────────────────────────────────

class WarehouseItem(Base):
    """Manually entered stock with known costs"""
    __tablename__ = "warehouse_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    type_id = Column(Integer, index=True)
    quantity = Column(Integer, nullable=False)
    unit_price = Column(Float, nullable=False)
    is_default_price = Column(Boolean, default=False)  # Infinite quantity at this price
    notes = Column(String(255), nullable=True)
    added_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


# ─── Broker Fee Configuration ──────────────────────────────

class BrokerFeeOverride(Base):
    """Per-character broker fee overrides with timestamps"""
    __tablename__ = "broker_fee_overrides"

    id = Column(Integer, primary_key=True, autoincrement=True)
    character_id = Column(BigInteger, nullable=True)  # NULL = all characters
    type_id = Column(Integer, nullable=True)  # NULL = all items
    location_id = Column(BigInteger, nullable=True)  # NULL = all locations
    buy_rate = Column(Float, default=1.0)  # Percentage
    sell_structure_rate = Column(Float, default=1.0)
    sell_npc_rate = Column(Float, default=1.5)
    sales_tax_rate = Column(Float, default=3.6)  # Accounting V default
    effective_from = Column(DateTime, nullable=False)

    __table_args__ = (
        Index("ix_broker_fee_char_date", "character_id", "effective_from"),
    )


# ─── Goonmetrics Cache ─────────────────────────────────────

class GoonmetricsCache(Base):
    """Cached weekly volume data from Goonmetrics API"""
    __tablename__ = "goonmetrics_cache"

    type_id = Column(Integer, primary_key=True)
    weekly_movement = Column(Float, default=0)
    sell_min = Column(Float, nullable=True)
    sell_listed = Column(Integer, default=0)
    buy_max = Column(Float, nullable=True)
    buy_listed = Column(Integer, default=0)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


# ─── Structure Rig Configuration ───────────────────────────

class StructureRig(Base):
    """Industry rig configuration per structure"""
    __tablename__ = "structure_rigs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    structure_id = Column(BigInteger, index=True)
    structure_name = Column(String(255), nullable=True)
    security_status = Column(String(10), nullable=True)  # high, low, null
    rig_slot = Column(Integer, nullable=False)  # 1, 2, 3
    rig_type_id = Column(Integer, nullable=True)  # SDE type_id of the rig


# ─── Market History ─────────────────────────────────────────

# ─── App Settings ──────────────────────────────────────────

class AppSetting(Base):
    """Simple key-value config store"""
    __tablename__ = "app_settings"

    key = Column(String(100), primary_key=True)
    value = Column(Text, nullable=True)


# ─── Market History ─────────────────────────────────────────

class MarketHistory(Base):
    """Daily market history per type per region from ESI"""
    __tablename__ = "market_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    type_id = Column(Integer, index=True)
    region_id = Column(Integer, default=10000002)
    date = Column(String(10))  # YYYY-MM-DD
    average = Column(Float)
    highest = Column(Float)
    lowest = Column(Float)
    volume = Column(BigInteger, default=0)
    order_count = Column(Integer, default=0)

    __table_args__ = (
        Index("ix_history_type_date", "type_id", "date"),
        UniqueConstraint("type_id", "region_id", "date", name="uq_history_type_region_date"),
    )


class FreightRoute(Base):
    """Freight rate per origin→destination route"""
    __tablename__ = "freight_routes"

    id = Column(Integer, primary_key=True, autoincrement=True)
    origin_system = Column(String(100), nullable=False)  # "Jita", "C-J6MT"
    destination_system = Column(String(100), nullable=False)  # "Atioth", "C-J6MT"
    rate_per_m3 = Column(Float, nullable=False)  # ISK per m³
    notes = Column(String(255), nullable=True)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class LocationCache(Base):
    """Cache location_id → solar system name (avoid repeated ESI lookups)"""
    __tablename__ = "location_cache"

    location_id = Column(BigInteger, primary_key=True)
    solar_system_id = Column(BigInteger, nullable=True)
    solar_system_name = Column(String(100), nullable=True)
    station_name = Column(String(255), nullable=True)
    resolved_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


# ─── Character Skills ──────────────────────────────────────

class CharacterSkill(Base):
    """ESI trained skills per character"""
    __tablename__ = "character_skills"

    id = Column(Integer, primary_key=True, autoincrement=True)
    character_id = Column(BigInteger, index=True, nullable=False)
    skill_id = Column(Integer, nullable=False)
    trained_level = Column(Integer, default=0)
    active_level = Column(Integer, default=0)
    skillpoints = Column(BigInteger, default=0)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint("character_id", "skill_id", name="uq_char_skill"),
        Index("ix_char_skills_char", "character_id"),
    )


# ─── Character Fittings ────────────────────────────────────

class CharacterFitting(Base):
    """ESI saved fittings from in-game fitting window"""
    __tablename__ = "character_fittings"

    fitting_id = Column(BigInteger, primary_key=True)
    character_id = Column(BigInteger, index=True, nullable=False)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    ship_type_id = Column(Integer, nullable=False)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class CharacterFittingItem(Base):
    """Items within a saved fitting"""
    __tablename__ = "character_fitting_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    fitting_id = Column(BigInteger, index=True, nullable=False)
    type_id = Column(Integer, nullable=False)
    flag = Column(String(50), nullable=False)  # HiSlot0, MedSlot1, Cargo, DroneBay, etc.
    quantity = Column(Integer, default=1)


# ─── Character Assets ──────────────────────────────────────

class CharacterAsset(Base):
    """Items owned per character from ESI /assets/ endpoint"""
    __tablename__ = "character_assets"

    item_id = Column(BigInteger, primary_key=True)
    character_id = Column(BigInteger, index=True, nullable=False)
    type_id = Column(Integer, index=True, nullable=False)
    quantity = Column(BigInteger, default=1)
    location_id = Column(BigInteger, index=True)
    location_type = Column(String(30))  # station, solar_system, item, other
    location_flag = Column(String(50))  # Hangar, HiSlot0, MedSlot1, Cargo, etc.
    is_singleton = Column(Boolean, default=False)
    is_blueprint_copy = Column(Boolean, default=False)
    synced_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class AssetName(Base):
    """Custom names for ships/containers (from /assets/names/ endpoint)"""
    __tablename__ = "asset_names"

    item_id = Column(BigInteger, primary_key=True)
    character_id = Column(BigInteger, index=True, nullable=False)
    name = Column(String(255), nullable=False)


# ─── Tracked Structures ────────────────────────────────────

class TrackedStructure(Base):
    """Citadels/structures tracked by the app for market data"""
    __tablename__ = "tracked_structures"

    structure_id = Column(BigInteger, primary_key=True)
    name = Column(String(255), nullable=False)
    solar_system_id = Column(BigInteger, nullable=True)
    solar_system_name = Column(String(100), nullable=True)
    type_id = Column(Integer, nullable=True)  # Keepstar, Fortizar, etc.
    enabled = Column(Boolean, default=True)
    added_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


# ─── Mining Ledger ─────────────────────────────────────────

class MiningLedger(Base):
    """Daily mining yield per character, per ore type, per system"""
    __tablename__ = "mining_ledger"

    id = Column(Integer, primary_key=True, autoincrement=True)
    character_id = Column(BigInteger, index=True, nullable=False)
    date = Column(String(10), nullable=False)  # YYYY-MM-DD
    solar_system_id = Column(BigInteger, nullable=True)
    type_id = Column(Integer, nullable=False)
    quantity = Column(BigInteger, default=0)
    # How many units we've "consumed" from this row by industry matching
    quantity_consumed = Column(BigInteger, default=0)

    __table_args__ = (
        UniqueConstraint("character_id", "date", "solar_system_id", "type_id", name="uq_mining_day"),
        Index("ix_mining_char_date", "character_id", "date"),
        Index("ix_mining_type", "type_id"),
    )


# ─── Cost Basis Configuration ──────────────────────────────

class CostBasisConfig(Base):
    """
    Per-character cost basis settings for FIFO profit matching.

    Roles:
      - 'buyer': All buys are inventory (Jita buyer alt). Include everything.
      - 'seller': Buys are filtered. Only buy-order fills count as inventory.
                  Sell-order purchases (instant buys) are personal use.

    Station exclusions: JSON list of location_ids where ALL buys are excluded
    regardless of buy type. For stations where the character only buys for personal use.
    """
    __tablename__ = "cost_basis_config"

    character_id = Column(BigInteger, primary_key=True)
    character_name = Column(String(255), nullable=True)
    role = Column(String(20), default="seller")  # 'buyer' or 'seller'
    buy_filter = Column(String(30), default="buy_orders_only")  # 'all' or 'buy_orders_only'
    excluded_stations = Column(JSON, default=list)  # [location_id, ...]
    excluded_station_names = Column(JSON, default=list)  # Human readable names for UI


# ─── SDE Geography ────────────────────────────────────

class SdeRegion(Base):
    __tablename__ = "sde_regions"
    region_id = Column(BigInteger, primary_key=True)
    name = Column(String(100), nullable=False, index=True)
    faction_id = Column(BigInteger, nullable=True)


class SdeConstellation(Base):
    __tablename__ = "sde_constellations"
    constellation_id = Column(BigInteger, primary_key=True)
    name = Column(String(100), nullable=False, index=True)
    region_id = Column(BigInteger, nullable=False, index=True)


class SdeSolarSystem(Base):
    __tablename__ = "sde_solar_systems"
    system_id = Column(BigInteger, primary_key=True)
    name = Column(String(100), nullable=False, index=True)
    constellation_id = Column(BigInteger, nullable=False, index=True)
    region_id = Column(BigInteger, nullable=False, index=True)
    security_status = Column(Float, nullable=True)
