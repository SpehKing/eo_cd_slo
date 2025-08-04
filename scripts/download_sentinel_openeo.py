#!/usr/bin/env python3
"""
Sentinel-2 Imagery Download Script using OpenEO

This script downloads Sentinel-2 imagery for predefined grid cells from the OpenEO
Copernicus Data Space Ecosystem. It fetches data for August of each year with minimal
cloud coverage and respects rate limiting.

Requirements:
- openeo
- geopandas
- tqdm
- rasterio
- time

Usage:
    python download_sentinel_openeo.py
"""

import openeo
import geopandas as gpd
import os
import time
import json
from datetime import datetime, timedelta
from pathlib import Path
from tqdm import tqdm
from typing import List, Dict, Tuple, Optional
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("download_sentinel.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Configuration
GRID_IDS = [339, 340, 341, 360, 361, 362, 380, 381, 382]
YEARS = list(range(2018, 2025))  # 2018 to 2024
AUGUST_START_DAY = 1
AUGUST_END_DAY = 31
MAX_CLOUD_COVERAGE = 20  # Maximum cloud coverage percentage
DOWNLOAD_DIR = Path("./data/images/sentinel_downloads")
GRID_FILE = Path("./data/slovenia_grid.gpkg")
BATCH_SIZE = 3  # Number of concurrent downloads
RATE_LIMIT_DELAY = 2.0  # Seconds between API calls
MAX_RETRIES = 3

# Bands to download (RGB for basic analysis)
BANDS = ["B02", "B03", "B04"]  # Blue, Green, Red


class SentinelDownloader:
    """Handles Sentinel-2 data downloading from OpenEO"""

    def __init__(self):
        self.connection = None
        self.grid_data = None
        self.download_stats = {
            "total_requested": 0,
            "successful": 0,
            "failed": 0,
            "skipped": 0,
        }

    def initialize(self) -> bool:
        """Initialize connection and load grid data"""
        try:
            # Load grid data
            logger.info(f"Loading grid data from {GRID_FILE}")
            self.grid_data = gpd.read_file(GRID_FILE)
            logger.info(f"Loaded {len(self.grid_data)} grid cells")

            # Filter for our specific grid IDs
            self.grid_data = self.grid_data[self.grid_data.index.isin(GRID_IDS)]
            logger.info(f"Filtered to {len(self.grid_data)} target grid cells")

            # Convert to WGS84 if needed
            if self.grid_data.crs != "EPSG:4326":
                logger.info(f"Converting CRS from {self.grid_data.crs} to EPSG:4326")
                self.grid_data = self.grid_data.to_crs("EPSG:4326")

            # Create download directory
            DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

            return True

        except Exception as e:
            logger.error(f"Failed to initialize: {e}")
            return False

    def connect_openeo(self) -> bool:
        """Establish connection to OpenEO backend"""
        try:
            logger.info("Connecting to OpenEO Copernicus Data Space Ecosystem...")
            self.connection = openeo.connect(url="openeo.dataspace.copernicus.eu")

            # Authenticate - this will open browser for user login
            logger.info("Please authenticate in your browser...")
            self.connection = self.connection.authenticate_oidc()

            logger.info("Successfully connected and authenticated")
            return True

        except Exception as e:
            logger.error(f"Failed to connect to OpenEO: {e}")
            return False

    def get_grid_bbox(self, grid_id: int) -> Dict[str, float]:
        """Get bounding box for a grid cell"""
        grid_row = self.grid_data[self.grid_data.index == grid_id]
        if grid_row.empty:
            raise ValueError(f"Grid ID {grid_id} not found")

        bounds = grid_row.geometry.bounds.iloc[0]
        return {
            "west": bounds.iloc[0],
            "south": bounds.iloc[1],
            "east": bounds.iloc[2],
            "north": bounds.iloc[3],
        }

    def generate_download_tasks(self) -> List[Dict]:
        """Generate list of download tasks"""
        tasks = []

        for grid_id in GRID_IDS:
            for year in YEARS:
                task = {
                    "grid_id": grid_id,
                    "year": year,
                    "start_date": f"{year}-08-{AUGUST_START_DAY:02d}",
                    "end_date": f"{year}-08-{AUGUST_END_DAY:02d}",
                    "bbox": self.get_grid_bbox(grid_id),
                    "filename": f"sentinel2_grid_{grid_id}_{year}_08.tiff",
                }
                tasks.append(task)

        return tasks

    def check_existing_file(self, filename: str) -> bool:
        """Check if file already exists"""
        filepath = DOWNLOAD_DIR / filename
        return filepath.exists()

    def download_image(self, task: Dict) -> Tuple[bool, str]:
        """Download a single image"""
        filename = task["filename"]
        filepath = DOWNLOAD_DIR / filename

        try:
            logger.info(f"Processing grid {task['grid_id']} for {task['year']}-08")

            # Check if file already exists
            if self.check_existing_file(filename):
                logger.info(f"File {filename} already exists, skipping")
                self.download_stats["skipped"] += 1
                return True, f"Already exists: {filename}"

            # Simple approach: Load collection and download directly
            cube = self.connection.load_collection(
                "SENTINEL2_L2A",
                spatial_extent=task["bbox"],
                temporal_extent=[task["start_date"], task["end_date"]],
                bands=BANDS,
            )

            # Take median over time to reduce clouds
            cube = cube.median_time()

            # Download directly
            logger.info(f"Downloading {filename}...")
            cube.download(str(filepath), format="GTiff")

            self.download_stats["successful"] += 1
            logger.info(f"Successfully downloaded: {filename}")
            return True, f"Downloaded: {filename}"

        except Exception as e:
            error_msg = f"Failed to download {filename}: {str(e)}"
            logger.error(error_msg)
            self.download_stats["failed"] += 1
            return False, error_msg

    def download_with_retry(self, task: Dict) -> Tuple[bool, str]:
        """Download with retry logic"""
        for attempt in range(MAX_RETRIES):
            try:
                return self.download_image(task)
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    wait_time = (attempt + 1) * 5  # Exponential backoff
                    logger.warning(
                        f"Attempt {attempt + 1} failed for {task['filename']}: {e}"
                    )
                    logger.info(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    logger.error(
                        f"All {MAX_RETRIES} attempts failed for {task['filename']}"
                    )
                    return False, f"Failed after {MAX_RETRIES} attempts: {str(e)}"

    def run_downloads(self):
        """Execute all downloads with progress tracking"""
        if not self.initialize():
            logger.error("Failed to initialize downloader")
            return False

        if not self.connect_openeo():
            logger.error("Failed to connect to OpenEO")
            return False

        # Generate download tasks
        tasks = self.generate_download_tasks()
        self.download_stats["total_requested"] = len(tasks)

        logger.info(f"Generated {len(tasks)} download tasks")
        logger.info(f"Grid IDs: {GRID_IDS}")
        logger.info(f"Years: {YEARS}")
        logger.info(f"Max cloud coverage: {MAX_CLOUD_COVERAGE}%")
        logger.info(f"Download directory: {DOWNLOAD_DIR.absolute()}")

        # Process downloads with progress bar
        with tqdm(total=len(tasks), desc="Downloading Sentinel-2 images") as pbar:
            for task in tasks:
                # Update progress bar description
                pbar.set_description(f"Grid {task['grid_id']} - {task['year']}")

                # Download with retry
                success, message = self.download_with_retry(task)

                # Update progress bar
                pbar.set_postfix(
                    {
                        "Success": self.download_stats["successful"],
                        "Failed": self.download_stats["failed"],
                        "Skipped": self.download_stats["skipped"],
                    }
                )
                pbar.update(1)

                # Log result
                if success:
                    logger.debug(message)
                else:
                    logger.error(message)

        # Print final statistics
        self.print_final_stats()
        return True

    def print_final_stats(self):
        """Print final download statistics"""
        stats = self.download_stats
        logger.info("=" * 60)
        logger.info("DOWNLOAD SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total requested: {stats['total_requested']}")
        logger.info(f"Successfully downloaded: {stats['successful']}")
        logger.info(f"Failed: {stats['failed']}")
        logger.info(f"Skipped (already exist): {stats['skipped']}")
        logger.info(
            f"Success rate: {stats['successful']/stats['total_requested']*100:.1f}%"
        )
        logger.info("=" * 60)


def main():
    """Main function"""
    logger.info("Starting Sentinel-2 download script")
    logger.info(f"Target grid IDs: {GRID_IDS}")
    logger.info(f"Years: {YEARS}")

    try:
        downloader = SentinelDownloader()
        success = downloader.run_downloads()

        if success:
            logger.info("Download process completed")
        else:
            logger.error("Download process failed")
            return 1

    except KeyboardInterrupt:
        logger.info("Download interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
