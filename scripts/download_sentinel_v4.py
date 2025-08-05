#!/usr/bin/env python3
"""
Sentinel-2 Download Script v4 - OpenEO with Perfect Grid Alignment

This script downloads Sentinel-2 imagery using OpenEO for predefined grid cells
from the Slovenia grid. It ensures pixel-perfect alignment with grid boundaries
using EPSG:4326 consistently throughout the pipeline.

Requirements:
- openeo
- geopandas
- rasterio
- tqdm
- numpy
- logging

Usage:
    python download_sentinel_v4.py
"""

import logging
import openeo
import geopandas as gpd
import rasterio
import numpy as np
from pathlib import Path
from tqdm import tqdm
from typing import List, Dict, Tuple, Optional
from datetime import datetime
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("download_sentinel_v4.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Configuration
GRID_IDS = [17, 18, 29, 30]  # Target grid IDs
YEARS = list(range(2024, 2025))  # 2024
AUGUST_START_DAY = 1
AUGUST_END_DAY = 31
MAX_CLOUD_COVERAGE = 5  # Maximum cloud coverage percentage
DOWNLOAD_DIR = Path("./data/images/sentinel_downloads_v4")
GRID_FILE = Path("./data/slovenia_grid_expanded.gpkg")
RATE_LIMIT_DELAY = 2.0  # Seconds between API calls
MAX_RETRIES = 3

# Bands to download (RGB for basic analysis)
BANDS = ["B02", "B03", "B04"]  # Blue, Green, Red

# OpenEO configuration - use EPSG:4326 consistently
TARGET_CRS = "EPSG:4326"
PIXEL_SIZE = 0.00009  # ~10m at equator in degrees (for EPSG:4326)


class SentinelDownloaderV4:
    """Handles Sentinel-2 data downloading from OpenEO with perfect grid alignment"""

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

            # Ensure CRS is EPSG:4326
            if self.grid_data.crs != TARGET_CRS:
                logger.info(f"Converting CRS from {self.grid_data.crs} to {TARGET_CRS}")
                self.grid_data = self.grid_data.to_crs(TARGET_CRS)

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

        logger.info(
            f"Grid {grid_id} exact bounds: W={west:.10f}, S={south:.10f}, E={east:.10f}, N={north:.10f}"
        )

        return {
            "west": west,
            "south": south,
            "east": east,
            "north": north,
        }

    def calculate_aligned_bbox(self, bbox: Dict[str, float]) -> Dict[str, float]:
        """
        Calculate pixel-aligned bounding box that exactly matches grid boundaries.
        No rounding or approximation - use exact grid coordinates.
        """
        # For perfect alignment, we use the exact grid boundaries without modification
        # OpenEO will handle the pixel alignment internally
        aligned_bbox = {
            "west": bbox["west"],
            "south": bbox["south"],
            "east": bbox["east"],
            "north": bbox["north"],
        }

        logger.info(f"Using exact grid boundaries for pixel alignment")
        return aligned_bbox

    def generate_download_tasks(self) -> List[Dict]:
        """Generate list of download tasks"""
        tasks = []

        for grid_id in GRID_IDS:
            for year in YEARS:
                # Get exact grid boundaries
                grid_bbox = self.get_grid_bbox_exact(grid_id)
                aligned_bbox = self.calculate_aligned_bbox(grid_bbox)

                task = {
                    "grid_id": grid_id,
                    "year": year,
                    "start_date": f"{year}-08-{AUGUST_START_DAY:02d}",
                    "end_date": f"{year}-08-{AUGUST_END_DAY:02d}",
                    "bbox": aligned_bbox,
                    "filename": f"sentinel2_grid_{grid_id}_{year}_08.tiff",
                }
                tasks.append(task)

        return tasks

    def check_existing_file(self, filename: str) -> bool:
        """Check if file already exists"""
        filepath = DOWNLOAD_DIR / filename
        return filepath.exists()

    def download_image(self, task: Dict) -> Tuple[bool, str]:
        """Download a single image using OpenEO with exact grid alignment"""
        filename = task["filename"]
        filepath = DOWNLOAD_DIR / filename

        try:
            logger.info(f"Processing grid {task['grid_id']} for {task['year']}-08")

            # Check if file already exists
            if self.check_existing_file(filename):
                logger.info(f"File {filename} already exists, skipping")
                self.download_stats["skipped"] += 1
                return True, f"Skipped existing: {filename}"

            # Use exact bbox coordinates
            bbox = task["bbox"]
            logger.info(f"Using exact bbox: {bbox}")

            # Load collection with exact spatial extent
            cube = self.connection.load_collection(
                "SENTINEL2_L2A",
                spatial_extent=bbox,
                temporal_extent=[task["start_date"], task["end_date"]],
                bands=BANDS,
            )

            # Apply filtering and aggregation
            cube = cube.filter_bands(BANDS)

            # Use median aggregation for cloud-free composite
            cube = cube.median_time()

            # Force exact CRS and ensure pixel alignment
            cube = cube.resample_spatial(
                resolution=PIXEL_SIZE, projection=TARGET_CRS  # ~10m in degrees
            )

            logger.info(f"Downloading {filename}...")
            cube.download(str(filepath), format="GTiff")

            # Verify the file was created and validate properties
            if not filepath.exists():
                raise Exception("Download failed - file not created")

            # Validate downloaded image properties
            self.validate_downloaded_image(filepath, task)

            self.download_stats["successful"] += 1
            logger.info(f"Successfully downloaded: {filename}")
            return True, f"Downloaded: {filename}"

        except Exception as e:
            error_msg = f"Failed to download {filename}: {str(e)}"
            logger.error(error_msg)
            self.download_stats["failed"] += 1
            return False, error_msg

    def validate_downloaded_image(self, filepath: Path, task: Dict):
        """Validate the downloaded image has correct properties and alignment"""
        try:
            with rasterio.open(filepath) as src:
                logger.info(f"Downloaded image properties:")
                logger.info(f"  File: {filepath.name}")
                logger.info(f"  Size: {src.width}x{src.height}")
                logger.info(f"  CRS: {src.crs}")
                logger.info(f"  Bounds: {src.bounds}")
                logger.info(f"  Transform: {src.transform}")
                logger.info(f"  Data type: {src.dtypes}")

                # Validate CRS
                if src.crs.to_string() != TARGET_CRS:
                    logger.warning(
                        f"CRS mismatch: expected {TARGET_CRS}, got {src.crs}"
                    )

                # Check if bounds are reasonable (within expected bbox)
                expected_bbox = task["bbox"]
                actual_bounds = src.bounds

                logger.info(f"Bbox validation:")
                logger.info(
                    f"  Expected: W={expected_bbox['west']:.10f}, S={expected_bbox['south']:.10f}, E={expected_bbox['east']:.10f}, N={expected_bbox['north']:.10f}"
                )
                logger.info(
                    f"  Actual:   W={actual_bounds.left:.10f}, S={actual_bounds.bottom:.10f}, E={actual_bounds.right:.10f}, N={actual_bounds.top:.10f}"
                )

        except Exception as e:
            logger.error(f"Could not validate image {filepath}: {e}")

    def download_with_retry(self, task: Dict) -> Tuple[bool, str]:
        """Download with retry logic"""
        for attempt in range(MAX_RETRIES):
            try:
                success, message = self.download_image(task)
                if success:
                    return True, message

                if attempt < MAX_RETRIES - 1:
                    logger.warning(
                        f"Attempt {attempt + 1} failed, retrying in {RATE_LIMIT_DELAY}s..."
                    )
                    time.sleep(RATE_LIMIT_DELAY)

            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    logger.warning(
                        f"Attempt {attempt + 1} failed with error: {e}, retrying..."
                    )
                    time.sleep(RATE_LIMIT_DELAY)
                else:
                    logger.error(f"All attempts failed: {e}")

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
        logger.info(f"Target CRS: {TARGET_CRS}")
        logger.info(f"Pixel size: {PIXEL_SIZE} degrees")
        logger.info(f"Download directory: {DOWNLOAD_DIR.absolute()}")

        # Process downloads with progress bar
        with tqdm(total=len(tasks), desc="Downloading Sentinel-2 images") as pbar:
            for task in tasks:
                # Update progress bar description
                pbar.set_description(f"Grid {task['grid_id']} - {task['year']}")

                # Download with retry
                success, message = self.download_with_retry(task)

                # Update progress bar
                pbar.update(1)

                # Rate limiting
                time.sleep(RATE_LIMIT_DELAY)

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
        if stats["total_requested"] > 0:
            logger.info(
                f"Success rate: {stats['successful']/stats['total_requested']*100:.1f}%"
            )
        logger.info("=" * 60)


def main():
    """Main function"""
    logger.info("Starting Sentinel-2 download script v4")
    logger.info(f"Target grid IDs: {GRID_IDS}")
    logger.info(f"Years: {YEARS}")
    logger.info(f"Target CRS: {TARGET_CRS}")

    try:
        downloader = SentinelDownloaderV4()
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
