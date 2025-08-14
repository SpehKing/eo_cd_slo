#!/usr/bin/env python3
"""
Download Module for Pipeline

Modified version of download_sentinel_v4.py integrated into the pipeline architecture.
Supports both local storage and database modes with state management.
"""

import asyncio
import logging
import os
import openeo
import geopandas as gpd
import rasterio
import numpy as np
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Any
from datetime import datetime
import time

from ..config.settings import config, ProcessingMode
from ..utils.state_manager import state_manager, TaskStatus


class SentinelDownloaderV5:
    """Pipeline-integrated Sentinel-2 downloader with state management"""

    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.SentinelDownloaderV5")
        self.connection = None
        self.grid_data = None
        self.current_year = None

    async def initialize(self) -> bool:
        """Initialize connection and load grid data"""
        try:
            # Load grid data
            self.logger.info(f"Loading grid data from {config.grid_file_path}")
            self.grid_data = gpd.read_file(config.grid_file_path)
            self.logger.info(f"Loaded {len(self.grid_data)} grid cells")

            # Filter for our specific grid IDs
            self.grid_data = self.grid_data[self.grid_data.index.isin(config.grid_ids)]
            self.logger.info(f"Filtered to {len(self.grid_data)} target grid cells")

            # Ensure CRS is correct
            if self.grid_data.crs != config.target_crs:
                self.logger.info(
                    f"Converting CRS from {self.grid_data.crs} to {config.target_crs}"
                )
                self.grid_data = self.grid_data.to_crs(config.target_crs)

            return True

        except Exception as e:
            self.logger.error(f"Failed to initialize: {e}")
            return False

    async def connect_openeo(self) -> bool:
        """Establish connection to OpenEO backend with hardcoded credentials"""
        try:
            self.logger.info("Connecting to OpenEO Copernicus Data Space Ecosystem...")
            self.connection = openeo.connect(url=config.openeo_url)

            # Use hardcoded client ID for all authentication methods
            client_id = config.openeo_client_id
            self.logger.info(f"Using hardcoded Client ID: {client_id}")

            # Priority 1: Client Secret Flow (if you have a client secret)
            if config.openeo_client_secret:
                self.logger.info("Using Client Credentials Flow")
                try:
                    self.connection = (
                        self.connection.authenticate_oidc_client_credentials(
                            client_id=client_id,
                            client_secret=config.openeo_client_secret,
                        )
                    )
                    self.logger.info(
                        "✓ Successfully authenticated with Client Credentials!"
                    )
                    return True
                except Exception as e:
                    self.logger.error(f"Client Credentials authentication failed: {e}")
                    # Fall through to next method

            # Priority 2: Refresh Token Flow (if you have a refresh token)
            elif config.openeo_refresh_token:
                self.logger.info("Using Refresh Token Flow")
                try:
                    self.connection = self.connection.authenticate_oidc_refresh_token(
                        refresh_token=config.openeo_refresh_token
                    )
                    self.logger.info("✓ Successfully authenticated with Refresh Token!")
                    return True
                except Exception as e:
                    self.logger.error(f"Refresh Token authentication failed: {e}")
                    # Fall through to next method

            # Priority 3: Device Flow (works for Docker and automation)
            else:
                self.logger.info("Using Device Flow authentication")
                self.logger.info(
                    "This will show a URL and code for browser authentication"
                )
                try:
                    self.connection = self.connection.authenticate_oidc_device()
                    self.logger.info("✓ Successfully authenticated with Device Flow!")
                    return True
                except Exception as e:
                    self.logger.error(f"Device Flow authentication failed: {e}")

                    # Final fallback: Default openEO authentication
                    self.logger.info(
                        "Trying default openEO authentication as final fallback..."
                    )
                    try:
                        self.connection = self.connection.authenticate_oidc()
                        self.logger.info(
                            "✓ Successfully authenticated with default method!"
                        )
                        return True
                    except Exception as fallback_e:
                        self.logger.error(
                            f"All authentication methods failed: {fallback_e}"
                        )
                        return False

        except Exception as e:
            self.logger.error(f"Failed to connect to OpenEO: {e}")
            self.logger.error("Troubleshooting:")
            self.logger.error("1. Check internet connectivity")
            self.logger.error("2. Verify openeo.dataspace.copernicus.eu is accessible")
            self.logger.error("3. Ensure your Copernicus account is active")
            return False

    def _is_running_in_docker(self) -> bool:
        """Check if running inside a Docker container"""
        try:
            # Check for .dockerenv file
            if Path("/.dockerenv").exists():
                return True

            # Check cgroup for docker
            with open("/proc/1/cgroup", "r") as f:
                content = f.read()
                if "docker" in content or "containerd" in content:
                    return True

        except (FileNotFoundError, PermissionError):
            pass

        # Check environment variables commonly set in Docker
        return any(
            env_var in os.environ
            for env_var in ["DOCKER_CONTAINER", "CONTAINER", "KUBERNETES_SERVICE_HOST"]
        )

    def get_grid_bbox_exact(self, grid_id: int) -> Dict[str, float]:
        """Get exact bounding box for a grid cell in EPSG:4326"""
        grid_row = self.grid_data[self.grid_data.index == grid_id]
        if grid_row.empty:
            raise ValueError(f"Grid ID {grid_id} not found")

        # Get exact bounds without any rounding
        bounds = grid_row.geometry.bounds.iloc[0]

        # Extract exact coordinates
        west = float(bounds[0])  # minx
        south = float(bounds[1])  # miny
        east = float(bounds[2])  # maxx
        north = float(bounds[3])  # maxy

        self.logger.info(
            f"Grid {grid_id} exact bounds: W={west:.10f}, S={south:.10f}, E={east:.10f}, N={north:.10f}"
        )

        return {
            "west": west,
            "south": south,
            "east": east,
            "north": north,
        }

    def generate_download_tasks_for_year(self, year: int) -> List[Dict]:
        """Generate download tasks for a specific year"""
        tasks = []

        for grid_id in config.grid_ids:
            try:
                # Get exact grid boundaries
                grid_bbox = self.get_grid_bbox_exact(grid_id)

                task = {
                    "grid_id": grid_id,
                    "year": year,
                    "start_date": f"{year}-{config.start_month:02d}-01",
                    "end_date": f"{year}-{config.end_month:02d}-30",
                    "bbox": grid_bbox,
                    "filename": f"sentinel2_grid_{grid_id}_{year}_08.tiff",
                    "task_id": f"download_{grid_id}_{year}",
                }
                tasks.append(task)

            except ValueError as e:
                self.logger.error(f"Failed to get bbox for grid {grid_id}: {e}")
                continue

        return tasks

    def get_output_filepath(self, task: Dict) -> Path:
        """Get output file path for a task"""
        if config.mode == ProcessingMode.LOCAL_ONLY:
            year_dir = config.get_year_images_dir(task["year"])
            return year_dir / task["filename"]
        else:
            # For database mode, use temporary directory
            temp_dir = config.images_dir / "temp"
            temp_dir.mkdir(parents=True, exist_ok=True)
            return temp_dir / task["filename"]

    def check_existing_file(self, task: Dict) -> bool:
        """Check if file already exists"""
        filepath = self.get_output_filepath(task)
        return filepath.exists()

    async def download_image(self, task: Dict) -> Tuple[bool, str, Optional[Path]]:
        """Download a single image using OpenEO"""
        filename = task["filename"]
        filepath = self.get_output_filepath(task)

        try:
            self.logger.info(
                f"Processing grid {task['grid_id']} for {task['year']} (Apr-Sep)"
            )

            # Check if file already exists
            if self.check_existing_file(task):
                self.logger.info(f"File {filename} already exists, skipping")
                return True, f"Skipped existing: {filename}", filepath

            # Use exact bbox coordinates
            bbox = task["bbox"]
            self.logger.debug(f"Using exact bbox: {bbox}")

            # Load collection with exact spatial extent
            cube = self.connection.load_collection(
                "SENTINEL2_L2A",
                spatial_extent=bbox,
                temporal_extent=[task["start_date"], task["end_date"]],
                bands=config.bands,
                max_cloud_cover=20,
            )

            # Apply filtering and aggregation
            cube = cube.filter_bands(config.bands)

            # Use median aggregation for cloud-free composite
            cube = cube.median_time()

            # Force exact CRS and ensure pixel alignment
            cube = cube.resample_spatial(
                resolution=config.pixel_size, projection=config.target_crs
            )

            self.logger.info(f"Downloading {filename}...")

            # Ensure directory exists
            filepath.parent.mkdir(parents=True, exist_ok=True)

            cube.download(str(filepath), format="GTiff")

            # Verify the file was created
            if not filepath.exists():
                raise Exception("Download failed - file not created")

            # Validate downloaded image properties
            await self.validate_downloaded_image(filepath, task)

            self.logger.info(f"Successfully downloaded: {filename}")
            return True, f"Downloaded: {filename}", filepath

        except Exception as e:
            error_msg = f"Failed to download {filename}: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg, None

    async def validate_downloaded_image(self, filepath: Path, task: Dict):
        """Validate the downloaded image has correct properties"""
        try:
            with rasterio.open(filepath) as src:
                self.logger.debug(f"Downloaded image properties:")
                self.logger.debug(f"  File: {filepath.name}")
                self.logger.debug(f"  Size: {src.width}x{src.height}")
                self.logger.debug(f"  CRS: {src.crs}")
                self.logger.debug(f"  Data type: {src.dtypes}")

                # Basic validation
                if src.crs.to_string() != config.target_crs:
                    self.logger.warning(
                        f"CRS mismatch: expected {config.target_crs}, got {src.crs}"
                    )

                # Store metadata for later use
                metadata = {
                    "width": src.width,
                    "height": src.height,
                    "crs": str(src.crs),
                    "data_type": str(src.dtypes[0]),
                    "bands": src.count,
                }

                return metadata

        except Exception as e:
            self.logger.error(f"Could not validate image {filepath}: {e}")
            return {}

    async def download_with_retry(self, task: Dict) -> Tuple[bool, str, Optional[Path]]:
        """Download with retry logic"""
        for attempt in range(config.openeo_max_retries):
            try:
                success, message, filepath = await self.download_image(task)
                if success:
                    return True, message, filepath

                if attempt < config.openeo_max_retries - 1:
                    self.logger.warning(
                        f"Attempt {attempt + 1} failed, retrying in {config.openeo_rate_limit}s..."
                    )
                    await asyncio.sleep(config.openeo_rate_limit)

            except Exception as e:
                if attempt < config.openeo_max_retries - 1:
                    self.logger.warning(
                        f"Attempt {attempt + 1} failed with error: {e}, retrying..."
                    )
                    await asyncio.sleep(config.openeo_rate_limit)
                else:
                    self.logger.error(f"All attempts failed: {e}")

        return False, f"Failed after {config.openeo_max_retries} attempts", None

    async def process_year(self, year: int) -> bool:
        """Process downloads for a specific year"""
        self.logger.info(f"Processing downloads for year {year}")
        self.current_year = year

        try:
            # Load grid data if not already loaded
            if self.grid_data is None:
                if not await self.load_grid_data():
                    return False

            # Ensure OpenEO connection is established
            if self.connection is None:
                if not await self.connect_openeo():
                    self.logger.error("Failed to establish OpenEO connection")
                    return False

            # Generate download tasks for this year
            tasks = self.generate_download_tasks_for_year(year)
            self.logger.info(f"Generated {len(tasks)} download tasks for year {year}")

            if len(tasks) == 0:
                self.logger.warning(f"No tasks generated for year {year}")
                return True  # Not an error, just no work to do

            # Process tasks sequentially (following the original script's approach)
            success_count = 0
            for task in tasks:
                try:
                    success, message, filepath = await self.download_with_retry(task)
                    if success:
                        success_count += 1
                        self.logger.info(f"✓ {message}")
                    else:
                        self.logger.error(f"✗ {message}")

                    # Rate limiting between downloads
                    await asyncio.sleep(config.openeo_rate_limit)

                except Exception as e:
                    self.logger.error(f"Failed to process task {task['task_id']}: {e}")

            self.logger.info(
                f"Completed {success_count}/{len(tasks)} downloads for year {year}"
            )
            return success_count > 0

        except Exception as e:
            self.logger.error(f"Error in process_year for {year}: {e}")
            return False

    async def run_downloads(self) -> bool:
        """Execute downloads for all years"""
        if not await self.initialize():
            self.logger.error("Failed to initialize downloader")
            return False

        if not await self.connect_openeo():
            self.logger.error("Failed to connect to OpenEO")
            return False

        self.logger.info(f"Starting downloads for years: {config.years}")
        self.logger.info(f"Grid IDs: {config.grid_ids}")
        self.logger.info(f"Storage mode: {config.mode.value}")

        # Process each year sequentially (as requested)
        overall_success = True
        for year in config.years:
            try:
                year_success = await self.process_year(year)
                if not year_success:
                    overall_success = False
                    self.logger.warning(f"Some downloads failed for year {year}")
            except Exception as e:
                self.logger.error(f"Failed to process year {year}: {e}")
                overall_success = False

        return overall_success

    async def load_grid_data(self) -> bool:
        """Load grid data from file"""
        try:
            self.logger.info(f"Loading grid data from {config.grid_file_path}")
            self.grid_data = gpd.read_file(config.grid_file_path)
            self.logger.info(f"Loaded {len(self.grid_data)} grid cells")

            # Filter for our specific grid IDs using the DataFrame index
            # The original script uses index-based filtering, not the 'grid_id' column
            self.logger.debug(
                f"Original indices: {self.grid_data.index[:10].tolist()}..."
            )
            self.grid_data = self.grid_data[self.grid_data.index.isin(config.grid_ids)]

            self.logger.info(f"Filtered to {len(self.grid_data)} target grid cells")
            self.logger.debug(f"Filtered indices: {self.grid_data.index.tolist()}")

            # Ensure CRS is correct
            if self.grid_data.crs != config.target_crs:
                self.logger.info(
                    f"Converting CRS from {self.grid_data.crs} to {config.target_crs}"
                )
                self.grid_data = self.grid_data.to_crs(config.target_crs)
                self.logger.debug(f"Post-CRS indices: {self.grid_data.index.tolist()}")

            return True

        except Exception as e:
            self.logger.error(f"Failed to load grid data: {e}")
            return False
