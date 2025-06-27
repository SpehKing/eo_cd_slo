from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import text
import logging
from typing import AsyncGenerator
from .config import settings

logger = logging.getLogger(__name__)

# Create async engine
engine = create_async_engine(
    settings.database_url,
    echo=False,
    pool_pre_ping=True,
)

# Create session factory
async_session_maker = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

Base = declarative_base()


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """Dependency to get database session"""
    async with async_session_maker() as session:
        try:
            yield session
        finally:
            await session.close()


async def init_db():
    """Initialize database connection and verify extensions"""
    async with engine.begin() as conn:
        # Test connection
        await conn.execute(text("SELECT 1"))

        # Verify TimescaleDB extension
        try:
            result = await conn.execute(
                text("SELECT extname FROM pg_extension WHERE extname = 'timescaledb'")
            )
            if not result.first():
                logger.warning("TimescaleDB extension not found")
            else:
                logger.info("TimescaleDB extension verified")
        except Exception as e:
            logger.warning(f"Could not verify TimescaleDB extension: {e}")

        # Verify PostGIS extension
        try:
            result = await conn.execute(
                text("SELECT extname FROM pg_extension WHERE extname = 'postgis'")
            )
            if not result.first():
                logger.warning("PostGIS extension not found")
            else:
                logger.info("PostGIS extension verified")
        except Exception as e:
            logger.warning(f"Could not verify PostGIS extension: {e}")

        # Test basic table existence
        try:
            await conn.execute(text("SELECT COUNT(*) FROM eo LIMIT 1"))
            logger.info("EO table found and accessible")
        except Exception as e:
            logger.error(f"EO table not accessible: {e}")

        try:
            await conn.execute(text("SELECT COUNT(*) FROM eo_change LIMIT 1"))
            logger.info("EO_CHANGE table found and accessible")
        except Exception as e:
            logger.warning(f"EO_CHANGE table not accessible: {e}")


async def close_db():
    """Close database connections"""
    await engine.dispose()
