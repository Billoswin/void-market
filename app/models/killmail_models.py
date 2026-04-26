"""
Void Market — Killmail Database Models

These tables live in killmail.db (separate from void_market.db).
Own Base class so they don't get created in the market DB.
"""
from sqlalchemy import (
    Column, Integer, BigInteger, String, Float, Text, Boolean,
    DateTime, JSON, ForeignKey, Index, UniqueConstraint
)
from sqlalchemy.orm import DeclarativeBase, relationship
from datetime import datetime, timezone


class KillmailBase(DeclarativeBase):
    pass


class Killmail(KillmailBase):
    """Raw killmail from zKillboard (R2Z2 real-time or zKill API backfill)."""
    __tablename__ = "killmails"

    killmail_id = Column(BigInteger, primary_key=True)
    killmail_hash = Column(String(64), nullable=True)
    ship_type_id = Column(Integer, index=True)
    ship_name = Column(String(255), nullable=True)
    victim_id = Column(BigInteger, nullable=True)
    victim_corp_id = Column(BigInteger, nullable=True)
    victim_alliance_id = Column(BigInteger, nullable=True)
    solar_system_id = Column(BigInteger, index=True)
    solar_system_name = Column(String(100), nullable=True)
    constellation_id = Column(BigInteger, nullable=True)
    region_id = Column(BigInteger, nullable=True, index=True)
    region_name = Column(String(100), nullable=True)
    killed_at = Column(DateTime, nullable=False, index=True)
    total_value = Column(Float, default=0.0)
    fit_items = Column(JSON, nullable=True)
    attacker_data = Column(JSON, nullable=True)  # [{alliance_id, corp_id, character_id, ship_type_id, weapon_type_id, damage_done, final_blow}, ...]
    attacker_count = Column(Integer, default=0)
    is_loss = Column(Boolean, default=True)
    # Doctrine matching
    is_doctrine_loss = Column(Boolean, default=False)
    doctrine_fit_id = Column(Integer, nullable=True)
    doctrine_fit_name = Column(String(255), nullable=True)
    doctrine_match_pct = Column(Float, nullable=True)
    # Deployment tagging (set when killmail falls in a watched region)
    deployment_id = Column(Integer, ForeignKey("deployments.id"), nullable=True, index=True)
    deployment_fight_id = Column(Integer, ForeignKey("deployment_fights.id"), nullable=True, index=True)
    # Legacy fight grouping (auto-detected from time+system window)
    fight_id = Column(Integer, ForeignKey("fights.id"), nullable=True, index=True)
    ingested_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (
        Index("ix_killmails_system_time", "solar_system_id", "killed_at"),
        Index("ix_killmails_ship_time", "ship_type_id", "killed_at"),
        Index("ix_killmails_region_time", "region_id", "killed_at"),
    )


class KillmailFight(KillmailBase):
    """Auto-detected fight from killmail clustering (legacy)."""
    __tablename__ = "fights"

    id = Column(Integer, primary_key=True, autoincrement=True)
    system_id = Column(BigInteger, nullable=True)
    system_name = Column(String(100), nullable=False)
    started_at = Column(DateTime, nullable=False, index=True)
    ended_at = Column(DateTime, nullable=False)
    alliance_losses = Column(Integer, default=0)
    total_isk_lost = Column(Float, default=0.0)
    is_active = Column(Boolean, default=True)
    doctrine_losses = Column(Integer, default=0)
    top_ship_lost = Column(String(255), nullable=True)


class SystemInfo(KillmailBase):
    """Cache for system → constellation → region resolution (populated from ESI)."""
    __tablename__ = "system_info"

    system_id = Column(BigInteger, primary_key=True)
    system_name = Column(String(100), nullable=True)
    constellation_id = Column(BigInteger, nullable=True)
    constellation_name = Column(String(100), nullable=True)
    region_id = Column(BigInteger, nullable=True, index=True)
    region_name = Column(String(100), nullable=True)
    resolved_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class Deployment(KillmailBase):
    """A campaign/deployment covering one or more regions over a time period."""
    __tablename__ = "deployments"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    status = Column(String(20), default="active")  # 'active' or 'ended'
    watched_region_ids = Column(JSON, default=list)
    watched_region_names = Column(JSON, default=list)
    started_at = Column(DateTime, nullable=False, index=True)
    ended_at = Column(DateTime, nullable=True)
    notes = Column(Text, nullable=True)
    # Staging Keepstar — where pilots reship during fights
    staging_structure_id = Column(BigInteger, nullable=True)
    staging_structure_name = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class DeploymentFight(KillmailBase):
    """A named engagement within a deployment (spans 1+ systems and time windows)."""
    __tablename__ = "deployment_fights"

    id = Column(Integer, primary_key=True, autoincrement=True)
    deployment_id = Column(Integer, ForeignKey("deployments.id"), nullable=False, index=True)
    name = Column(String(255), nullable=False)
    notes = Column(Text, nullable=True)
    auto_detected = Column(Boolean, default=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    systems = relationship("DeploymentFightSystem", backref="fight", cascade="all, delete-orphan")


class DeploymentFightSystem(KillmailBase):
    """A system+time window that defines part of a deployment fight.
    A fight can have multiple of these for multi-system running engagements."""
    __tablename__ = "deployment_fight_systems"

    id = Column(Integer, primary_key=True, autoincrement=True)
    deployment_fight_id = Column(Integer, ForeignKey("deployment_fights.id"), nullable=False, index=True)
    system_id = Column(BigInteger, nullable=False)
    system_name = Column(String(100), nullable=False)
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=False)
