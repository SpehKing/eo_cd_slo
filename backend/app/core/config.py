from pydantic_settings import BaseSettings
from typing import List, Optional


class Settings(BaseSettings):
    # Database configuration
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "eo_db"
    db_user: str = "postgres"
    db_password: str = "password"

    # API configuration
    api_title: str = "Sentinel-2 Image API"
    api_version: str = "1.0.0"
    api_prefix: str = "/api/v1"

    # CORS configuration
    allowed_origins: List[str] = ["*"]

    # Image processing
    max_image_size: int = 1024
    jpeg_quality: int = 85

    # Pagination
    default_page_size: int = 50
    max_page_size: int = 100

    # Logfire configuration
    logfire_token: Optional[str] = (
        "pylf_v1_eu_lbpYb5Rz7rfccThmqXZ92lB9bpFNXdsGKfdGFCzxzRzY"
    )
    logfire_project_name: str = "eo-cd-slo"
    logfire_environment: str = "development"
    enable_logfire: bool = True

    # Logging configuration
    log_level: str = "INFO"

    class Config:
        env_prefix = "EO_CD_"
        case_sensitive = False

    @property
    def database_url(self) -> str:
        # Use asyncpg with PostGIS support for TimescaleDB
        return f"postgresql+asyncpg://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"


settings = Settings()
