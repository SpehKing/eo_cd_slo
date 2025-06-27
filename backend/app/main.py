from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logfire

from .core.config import settings
from .core.database import init_db, close_db
from .core.logging import logfire_instance, logger
from .api.images import router as images_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    logger.info("Starting up application...")
    try:
        await init_db()
        logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database: {e}")
        raise

    yield

    # Shutdown
    logger.info("Shutting down application...")
    try:
        await close_db()
        logger.info("Database connections closed")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")


def create_app() -> FastAPI:
    """Create FastAPI application"""

    app = FastAPI(
        title=settings.api_title,
        version=settings.api_version,
        description="API for querying and retrieving Sentinel-2 satellite imagery",
        lifespan=lifespan,
    )

    # Configure Logfire for FastAPI (if enabled)
    if logfire_instance and settings.enable_logfire:
        try:
            logfire.instrument_fastapi(app)
            logger.info("Logfire FastAPI instrumentation enabled")
        except Exception as e:
            logger.warning(f"Failed to instrument FastAPI with Logfire: {e}")

    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.allowed_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE"],
        allow_headers=["*"],
    )

    # Include routers
    app.include_router(
        images_router, prefix=f"{settings.api_prefix}/public", tags=["images"]
    )

    @app.get("/")
    async def root():
        """Root endpoint with Logfire demo"""
        logger.info("Root endpoint accessed")

        # Demo Logfire logging
        if logfire_instance:
            logfire.info("Hello from Sentinel-2 API! 🛰️", user_endpoint="root")

        return {
            "message": "Sentinel-2 Image API",
            "version": settings.api_version,
            "docs": "/docs",
            "logfire_enabled": settings.enable_logfire,
        }

    return app


# Create the app instance
app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app", host="0.0.0.0", port=8000, reload=True, log_level="info"
    )
