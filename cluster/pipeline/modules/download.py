#!/usr/bin/env python3
"""
Download Module for Pipeline

Modified version of download_sentinel_v4.py integrated into the pipeline architecture.
Supports both local storage and database modes with state management.
"""

import asyncio
import logging
import openeo
import geopandas as gpd
import rasterio
import numpy as np
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from datetime import datetime
import time

from ..config.settings import config
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
            self.logger.info(f"Loading grid data from {config.grid_file}")
            self.grid_data = gpd.read_file(config.grid_file)
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
        """Establish connection to OpenEO backend"""
        try:
            self.logger.info("Connecting to OpenEO Copernicus Data Space Ecosystem...")
            self.connection = openeo.connect(url=config.openeo_url)

            # Authenticate - this will open browser for user login
            self.logger.info("Please authenticate in your browser...")
            self.connection = self.connection.authenticate_oidc()

            self.logger.info("Successfully connected and authenticated")
            return True

        except Exception as e:
            self.logger.error(f"Failed to connect to OpenEO: {e}")
            return False

    def get_grid_bbox_exact(self, grid_id: int) -> Dict[str, float]:
        """Get exact bounding box for a grid cell"""
        grid_row = self.grid_data[self.grid_data.index == grid_id]
        if grid_row.empty:
            raise ValueError(f"Grid ID {grid_id} not found")

        # Get exact bounds without any rounding
        bounds = grid_row.geometry.bounds.iloc[0]

        return {
            "west": float(bounds[0]),  # minx
            "south": float(bounds[1]),  # miny
            "east": float(bounds[2]),  # maxx
            "north": float(bounds[3]),  # maxy
        }

    def generate_download_tasks_for_year(self, year: int) -> List[Dict]:
        """Generate download tasks for a specific year"""
        tasks = []

        for grid_id in config.grid_ids:
            # Get exact grid boundaries
            grid_bbox = self.get_grid_bbox_exact(grid_id)

            task = {
                "grid_id": grid_id,
                "year": year,
                "start_date": f"{year}-08-{config.august_start_day:02d}",
                "end_date": f"{year}-08-{config.august_end_day:02d}",
                "bbox": grid_bbox,
                "filename": f"sentinel2_grid_{grid_id}_{year}_08.tiff",
                "task_id": f"download_{grid_id}_{year}",
            }
            tasks.append(task)

        return tasks

    def get_output_filepath(self, task: Dict) -> Path:
        """Get output file path for a task"""
        if config.mode == config.ProcessingMode.LOCAL_ONLY:
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
            self.logger.info(f"Processing grid {task['grid_id']} for {task['year']}-08")

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

        # Generate tasks for this year
        tasks = self.generate_download_tasks_for_year(year)
        task_ids = [task["task_id"] for task in tasks]

        # Load or create checkpoint
        checkpoint = state_manager.load_checkpoint("download", year)
        if not checkpoint:
            checkpoint = state_manager.create_stage_checkpoint(
                "download", year, task_ids
            )

        # Get pending tasks
        pending_task_ids = state_manager.get_pending_tasks("download", year)
        pending_tasks = [task for task in tasks if task["task_id"] in pending_task_ids]

        self.logger.info(
            f"Found {len(pending_tasks)} pending download tasks for {year}"
        )

        if not pending_tasks:
            self.logger.info(f"All downloads for {year} already completed")
            return True

        # Process downloads
        success_count = 0
        for task in pending_tasks:
            task_id = task["task_id"]

            # Update status to running
            state_manager.update_task_status(
                "download", year, task_id, TaskStatus.RUNNING
            )

            try:
                # Download with retry
                success, message, filepath = await self.download_with_retry(task)

                if success:
                    # Update status to completed
                    metadata = {"filepath": str(filepath), "message": message}
                    state_manager.update_task_status(
                        "download",
                        year,
                        task_id,
                        TaskStatus.COMPLETED,
                        metadata=metadata,
                    )
                    success_count += 1
                else:
                    # Update status to failed
                    state_manager.update_task_status(
                        "download",
                        year,
                        task_id,
                        TaskStatus.FAILED,
                        error_message=message,
                    )

                # Rate limiting
                await asyncio.sleep(config.openeo_rate_limit)

            except Exception as e:
                error_msg = f"Unexpected error processing {task_id}: {e}"
                self.logger.error(error_msg)
                state_manager.update_task_status(
                    "download",
                    year,
                    task_id,
                    TaskStatus.FAILED,
                    error_message=error_msg,
                )

        self.logger.info(
            f"Completed downloads for {year}: {success_count}/{len(pending_tasks)} successful"
        )
        return success_count == len(pending_tasks)

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


# Export the downloader class
__all__ = ["SentinelDownloaderV5"]
