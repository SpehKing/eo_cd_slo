import logfire
import logging
import sys
from typing import Optional
from .config import settings


def setup_logfire() -> Optional[logfire.Logfire]:
    """Set up Logfire logging with proper configuration"""

    if not settings.enable_logfire:
        return None

    try:
        # Configure Logfire
        logfire_config = {
            "service_name": "eo-cd-slo-api",
            "service_version": settings.api_version,
            "environment": settings.logfire_environment,
        }

        # Add token if provided (for production)
        if settings.logfire_token:
            logfire_config["token"] = settings.logfire_token

        # Configure Logfire
        logfire.configure(**logfire_config)

        # Integrate Python logging with Logfire
        logfire.install_auto_tracing(
            modules=["sqlalchemy", "asyncpg", "httpx", "requests"],
            min_duration=0.1,  # Minimum duration in seconds to trace
            check_imported_modules="ignore",  # Ignore already imported modules
        )

        return logfire

    except Exception as e:
        print(f"Failed to initialize Logfire: {e}")
        return None


def setup_logging():
    """Set up standard Python logging"""

    # Configure standard logging
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    # Reduce noise from some libraries
    logging.getLogger("asyncpg").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)


# Initialize logging when module is imported
setup_logging()
logfire_instance = setup_logfire()

# Create a logger for this module
logger = logging.getLogger(__name__)
