"""
Void Market — Database Session Management

SQLite only allows one writer at a time. We use an asyncio.Lock
to serialize all write operations so they never collide.
"""
import asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from app.models.database import Base
from app.config import settings


engine = create_async_engine(
    f"sqlite+aiosqlite:///{settings.db_path}",
    echo=settings.debug,
    connect_args={"check_same_thread": False, "timeout": 30},
)

async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

# Global write lock — any operation that writes to the DB must hold this
db_write_lock = asyncio.Lock()


async def init_db():
    """Create all tables and enable WAL mode."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with engine.connect() as conn:
        await conn.execute(text("PRAGMA journal_mode=WAL"))
        await conn.execute(text("PRAGMA busy_timeout=30000"))
        await conn.commit()


async def get_db() -> AsyncSession:
    """Dependency for FastAPI routes."""
    async with async_session() as session:
        try:
            yield session
        finally:
            await session.close()
