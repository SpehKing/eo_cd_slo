#!/usr/bin/env python3
"""
Pipeline Configuration Settings

Central configuration for the EO Change Detection Pipeline.
Supports both local storage and database modes.
"""

import os
from pathlib import Path
from typing import List, Dict, Optional
from dataclasses import dataclass, field
from enum import Enum


class ProcessingMode(Enum):
    """Processing mode options"""

    LOCAL_ONLY = "local_only"  # Store everything locally
    DATABASE_ONLY = "database_only"  # Store everything in database
    HYBRID = "hybrid"  # Store locally + sync to database


class LogLevel(Enum):
    """Logging levels"""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


@dataclass
class PipelineConfig:
    """Main pipeline configuration"""

    # Processing mode
    mode: ProcessingMode = ProcessingMode.LOCAL_ONLY

    # Target data specification
    # grid_ids: List[int] = field(
    #     default_factory=lambda: [465, 466, 467, 499, 500, 501, 532, 533, 543]
    # )
    grid_ids: List[int] = field(default_factory=lambda: [465, 466, 467])
    years: List[int] = field(default_factory=lambda: list(range(2020, 2025)))

    # Processing parameters
    max_workers: int = 4  # CPU cores
    memory_limit_gb: int = 4  # Memory limit for BTC model
    batch_size: int = 1  # Images processed in parallel per year

    # Base directories - all relative to pipeline directory
    _pipeline_root: Path = field(init=False)
    base_data_dir: Path = field(init=False)
    images_dir: Path = field(init=False)
    masks_dir: Path = field(init=False)
    checkpoints_dir: Path = field(init=False)
    logs_dir: Path = field(init=False)
    grid_file: Path = field(init=False)

    # Database configuration (database mode)
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "eo_db"
    db_user: str = "postgres"
    db_password: str = "password"

    # OpenEO configuration
    openeo_url: str = "openeo.dataspace.copernicus.eu"
    openeo_rate_limit: float = 2.0  # seconds between API calls
    openeo_max_retries: int = 3

    # OpenEO Authentication - Hardcoded Client Credentials (WORKING!)
    openeo_client_id: str = (
        "sh-709a7af8-09ba-46e8-bcfc-f4d0d20461d4"  # Your working client ID
    )
    openeo_client_secret: str = (
        "v9dPYitFLccIhtRohuIrvT0EwnsiJUIJ"  # Your working client secret
    )

    # OpenEO Authentication - Optional refresh token
    openeo_refresh_token: Optional[str] = None  # Set this if you get a refresh token
    btc_model_checkpoint: str = "blaz-r/BTC-B_oscd96"
    btc_config_path: str = "configs/exp/BTC-B.yaml"
    btc_image_size: int = 256
    btc_threshold: float = 0.5

    # Processing configuration
    start_month: int = 4  # April
    end_month: int = 9  # September
    max_cloud_coverage: int = 0
    target_crs: str = "EPSG:4326"
    pixel_size: float = 0.00009  # ~10m in degrees
    bands: List[str] = field(default_factory=lambda: ["B02", "B03", "B04"])

    # Monitoring and logging
    log_level: LogLevel = LogLevel.DEBUG
    enable_progress_bar: bool = True
    enable_real_time_monitoring: bool = True
    monitoring_port: int = 8080

    def __post_init__(self):
        """Post-initialization setup - establish clean path resolution"""
        # Get the pipeline root directory (cluster/pipeline/)
        # This file is in cluster/pipeline/config/settings.py
        current_file = Path(__file__).resolve()
        self._pipeline_root = current_file.parent.parent  # Go up to pipeline/

        # Set all paths relative to pipeline root
        self.base_data_dir = self._pipeline_root / "data"
        self.images_dir = self.base_data_dir / "images"
        self.masks_dir = self.base_data_dir / "masks"
        self.checkpoints_dir = self.base_data_dir / "checkpoints"
        self.logs_dir = self.base_data_dir / "logs"

        # Grid file in cluster/pipeline/config/ directory
        self.grid_file = (
            self._pipeline_root.parent
            / "pipeline"
            / "config"
            / "slovenia_grid_expanded.gpkg"
        )

        # Ensure directories exist in local mode
        if self.mode in [ProcessingMode.LOCAL_ONLY, ProcessingMode.HYBRID]:
            for directory in [
                self.base_data_dir,
                self.images_dir,
                self.masks_dir,
                self.checkpoints_dir,
                self.logs_dir,
            ]:
                directory.mkdir(parents=True, exist_ok=True)

    @property
    def db_config(self) -> Dict[str, any]:
        """Database configuration dictionary"""
        return {
            "host": self.db_host,
            "port": self.db_port,
            "database": self.db_name,
            "user": self.db_user,
            "password": self.db_password,
        }

    @property
    def band_mapping(self) -> Dict[str, str]:
        """Sentinel-2 band mapping for database"""
        return {
            "B02": "b02",  # Blue
            "B03": "b03",  # Green
            "B04": "b04",  # Red
        }

    @property
    def grid_file_path(self) -> Path:
        """Path to the Slovenia grid file"""
        return self.grid_file

    def get_year_images_dir(self, year: int) -> Path:
        """Get images directory for a specific year"""
        year_dir = self.images_dir / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        return year_dir

    def get_year_masks_dir(self, year: int) -> Path:
        """Get masks directory for specific year"""
        year_dir = self.masks_dir / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        return year_dir

    def get_checkpoint_file(self, stage: str, year: Optional[int] = None) -> Path:
        """Get checkpoint file path for a stage"""
        filename = f"{stage}_{year}.json" if year else f"{stage}.json"
        return self.checkpoints_dir / filename

    def get_log_file(self, component: str) -> Path:
        """Get log file path for a specific component"""
        return self.logs_dir / f"{component}.log"


# Global configuration instance
config = PipelineConfig()


# Environment variable overrides
def load_config_from_env():
    """Load configuration from environment variables"""
    global config

    # Processing mode
    if os.getenv("PIPELINE_MODE"):
        config.mode = ProcessingMode(os.getenv("PIPELINE_MODE"))

    # Database configuration
    if os.getenv("DB_HOST"):
        config.db_host = os.getenv("DB_HOST")
    if os.getenv("DB_PORT"):
        config.db_port = int(os.getenv("DB_PORT"))
    if os.getenv("DB_NAME"):
        config.db_name = os.getenv("DB_NAME")
    if os.getenv("DB_USER"):
        config.db_user = os.getenv("DB_USER")
    if os.getenv("DB_PASSWORD"):
        config.db_password = os.getenv("DB_PASSWORD")

    # Processing parameters
    if os.getenv("MAX_WORKERS"):
        config.max_workers = int(os.getenv("MAX_WORKERS"))
    if os.getenv("MEMORY_LIMIT_GB"):
        config.memory_limit_gb = int(os.getenv("MEMORY_LIMIT_GB"))

    # Data directories
    if os.getenv("DATA_DIR"):
        # If DATA_DIR is set, use it as base but maintain relative structure
        base_dir = Path(os.getenv("DATA_DIR"))
        config.base_data_dir = base_dir
        config.images_dir = base_dir / "images"
        config.masks_dir = base_dir / "masks"
        config.checkpoints_dir = base_dir / "checkpoints"
        config.logs_dir = base_dir / "logs"
        # Note: grid_file remains in the main data directory

    # BTC model configuration
    if os.getenv("BTC_MODEL_CHECKPOINT"):
        config.btc_model_checkpoint = os.getenv("BTC_MODEL_CHECKPOINT")
    if os.getenv("BTC_THRESHOLD"):
        config.btc_threshold = float(os.getenv("BTC_THRESHOLD"))

    # OpenEO authentication
    if os.getenv("OPENEO_CLIENT_ID"):
        config.openeo_client_id = os.getenv("OPENEO_CLIENT_ID")
    if os.getenv("OPENEO_CLIENT_SECRET"):
        config.openeo_client_secret = os.getenv("OPENEO_CLIENT_SECRET")
    if os.getenv("OPENEO_REFRESH_TOKEN"):
        config.openeo_refresh_token = os.getenv("OPENEO_REFRESH_TOKEN")

    # Monitoring
    if os.getenv("MONITORING_PORT"):
        config.monitoring_port = int(os.getenv("MONITORING_PORT"))
    if os.getenv("LOG_LEVEL"):
        config.log_level = LogLevel(os.getenv("LOG_LEVEL"))

    # Re-run post-init to create directories
    config.__post_init__()


# Load configuration from environment on module import
load_config_from_env()
