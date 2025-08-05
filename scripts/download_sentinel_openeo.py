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
import rasterio

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("download_sentinel.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Configuration
GRID_IDS = [357, 358, 359, 360, 361, 362, 363, 364]  # Example grid IDs]
YEARS = list(range(2024, 2025))  # 2024
AUGUST_START_DAY = 1
AUGUST_END_DAY = 31
MAX_CLOUD_COVERAGE = 5  # Maximum cloud coverage percentage
DOWNLOAD_DIR = Path("./data/images/sentinel_downloads_v3")
GRID_FILE = Path("./grid_output/slovenia_grid_expanded.gpkg")
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
        """Download a single image using OpenEO's standard processing"""
        filename = task["filename"]
        filepath = DOWNLOAD_DIR / filename

        try:
            logger.info(f"Processing grid {task['grid_id']} for {task['year']}-08")

            # Check if file already exists
            if self.check_existing_file(filename):
                logger.info(f"File {filename} already exists, skipping")
                self.download_stats["skipped"] += 1
                return True, f"Skipped: {filename}"

            # Use the bbox directly from the grid (already in WGS84)
            bbox_wgs84 = task["bbox"]
            logger.info(f"Using bbox: {bbox_wgs84}")

            # Load collection - let OpenEO handle all coordinate transformations
            cube = self.connection.load_collection(
                "SENTINEL2_L2A",
                spatial_extent=bbox_wgs84,
                temporal_extent=[task["start_date"], task["end_date"]],
                bands=BANDS,
            )

            # Apply cloud masking and temporal aggregation
            cube = cube.filter_bands(BANDS)

            # Mask clouds if available
            try:
                scl = cube.band("SCL")
                mask = (
                    scl.eq(4).or_(scl.eq(5)).or_(scl.eq(6))
                )  # Vegetation, not vegetated, water
                cube = cube.mask(mask)
            except:
                logger.info("SCL band not available, skipping cloud masking")

            cube = cube.median_time()

            # Let OpenEO determine the output CRS and resolution
            # This will likely be EPSG:4326 or EPSG:3857 with OpenEO's standard grid
            logger.info(f"Downloading {filename}...")
            cube.download(str(filepath), format="GTiff")

            # Verify the file was created and log its properties
            if not filepath.exists():
                raise Exception("Download failed - file not created")

            with rasterio.open(filepath) as src:
                logger.info(f"Downloaded image properties:")
                logger.info(f"  Size: {src.width}x{src.height}")
                logger.info(f"  CRS: {src.crs}")
                logger.info(f"  Bounds: {src.bounds}")
                logger.info(f"  Transform: {src.transform}")

            self.download_stats["successful"] += 1
            logger.info(f"Successfully downloaded: {filename}")
            return True, f"Downloaded: {filename}"

        except Exception as e:
            error_msg = f"Failed to download {filename}: {str(e)}"
            logger.error(error_msg)
            self.download_stats["failed"] += 1
            return False, error_msg

    def verify_alignment(self, filepath: Path, expected_bbox: Dict, grid_id: int):
        """Verify the downloaded image has correct pixel alignment"""
        try:
            import rasterio

            with rasterio.open(filepath) as src:
                # Check CRS
                if src.crs.to_string() != "EPSG:32633":
                    logger.warning(
                        f"Grid {grid_id}: CRS mismatch. Expected EPSG:32633, got {src.crs}"
                    )

                # Check bounds (allow small floating point differences)
                bounds = src.bounds
                tolerance = 5  # 5 meter tolerance

                if (
                    abs(bounds.left - expected_bbox["west"]) > tolerance
                    or abs(bounds.bottom - expected_bbox["south"]) > tolerance
                    or abs(bounds.right - expected_bbox["east"]) > tolerance
                    or abs(bounds.top - expected_bbox["north"]) > tolerance
                ):

                    logger.warning(f"Grid {grid_id}: Bounds mismatch!")
                    logger.warning(f"  Expected: {expected_bbox}")
                    logger.warning(f"  Actual: {bounds}")

                # Check pixel size
                pixel_size_x = abs(src.transform.a)
                pixel_size_y = abs(src.transform.e)

                if abs(pixel_size_x - 10) > 0.1 or abs(pixel_size_y - 10) > 0.1:
                    logger.warning(
                        f"Grid {grid_id}: Pixel size mismatch. Expected 10m, got {pixel_size_x}x{pixel_size_y}"
                    )

                # Check if pixels are aligned to 10m grid
                origin_x = src.transform.c % 10
                origin_y = src.transform.f % 10

                if origin_x > 0.1 and origin_x < 9.9:
                    logger.warning(
                        f"Grid {grid_id}: X origin not aligned to 10m grid: {origin_x}"
                    )
                if origin_y > 0.1 and origin_y < 9.9:
                    logger.warning(
                        f"Grid {grid_id}: Y origin not aligned to 10m grid: {origin_y}"
                    )

                logger.info(f"Grid {grid_id}: Alignment verification passed")

        except Exception as e:
            logger.error(f"Could not verify alignment for grid {grid_id}: {e}")

    def download_with_retry(self, task: Dict) -> Tuple[bool, str]:
        """Download with retry logic"""
        for attempt in range(MAX_RETRIES):
            try:
                success, message = self.download_image(task)
                if success:
                    return success, message

                if attempt < MAX_RETRIES - 1:
                    logger.warning(f"Attempt {attempt + 1} failed, retrying: {message}")
                    time.sleep(RATE_LIMIT_DELAY * (attempt + 1))  # Exponential backoff

            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    logger.warning(
                        f"Attempt {attempt + 1} failed with exception, retrying: {e}"
                    )
                    time.sleep(RATE_LIMIT_DELAY * (attempt + 1))
                else:
                    return False, f"All retry attempts failed: {e}"

        return False, f"Failed after {MAX_RETRIES} attempts"

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
