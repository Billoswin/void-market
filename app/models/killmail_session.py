"""
Void Market — Killmail Database Session

Separate SQLite database for killmail/fight data.
Own engine and write lock so real-time ingestion
doesn't compete with market DB writes.
"""
import asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from app.config import settings

# Killmail DB lives alongside the market DB
KILLMAIL_DB_PATH = settings.data_dir / "killmail.db"

killmail_engine = create_async_engine(
    f"sqlite+aiosqlite:///{KILLMAIL_DB_PATH}",
    echo=settings.debug,
    connect_args={"check_same_thread": False, "timeout": 30},
)

killmail_session = async_sessionmaker(killmail_engine, class_=AsyncSession, expire_on_commit=False)

# Separate write lock — independent from market DB lock
killmail_write_lock = asyncio.Lock()


async def init_killmail_db():
    """Create killmail tables and enable WAL mode. Run additive migrations for existing installs."""
    from app.models.killmail_models import KillmailBase

    async with killmail_engine.begin() as conn:
        await conn.run_sync(KillmailBase.metadata.create_all)

    async with killmail_engine.connect() as conn:
        await conn.execute(text("PRAGMA journal_mode=WAL"))
        await conn.execute(text("PRAGMA busy_timeout=30000"))

        # Additive migrations for existing killmail.db files
        migrations = [
            "ALTER TABLE killmails ADD COLUMN constellation_id BIGINT",
            "ALTER TABLE killmails ADD COLUMN region_id BIGINT",
            "ALTER TABLE killmails ADD COLUMN region_name VARCHAR(100)",
            "ALTER TABLE killmails ADD COLUMN deployment_id INTEGER",
            "ALTER TABLE killmails ADD COLUMN deployment_fight_id INTEGER",
            "CREATE INDEX IF NOT EXISTS ix_killmails_region_time ON killmails(region_id, killed_at)",
            "CREATE INDEX IF NOT EXISTS ix_killmails_deployment ON killmails(deployment_id)",
            "ALTER TABLE deployment_fights ADD COLUMN auto_detected BOOLEAN DEFAULT 0",
            "ALTER TABLE killmails ADD COLUMN attacker_data JSON",
            "ALTER TABLE deployments ADD COLUMN staging_structure_id BIGINT",
            "ALTER TABLE deployments ADD COLUMN staging_structure_name VARCHAR(255)",
        ]
        for sql in migrations:
            try:
                await conn.execute(text(sql))
            except Exception:
                pass  # Column/index already exists — safe to ignore

        await conn.commit()


async def get_killmail_db() -> AsyncSession:
    """Dependency for FastAPI routes needing killmail data."""
    async with killmail_session() as session:
        try:
            yield session
        finally:
            await session.close()
