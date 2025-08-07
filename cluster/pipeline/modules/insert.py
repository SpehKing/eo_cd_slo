#!/usr/bin/env python3
"""
Database Insert Module for Pipeline

Modified version of insert_sentinel_v4.py integrated into the pipeline architecture.
Supports both local storage and database modes with state management.
"""

import asyncio
import logging
import psycopg2
import geopandas as gpd
import rasterio
import numpy as np
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from datetime import datetime

from ..config.settings import config
from ..utils.state_manager import state_manager, TaskStatus


class SentinelInserterV5:
    """Pipeline-integrated database inserter with state management"""

    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.SentinelInserterV5")
        self.conn = None
        self.grid_data = None
        self.current_year = None

    async def initialize(self) -> bool:
        """Initialize database connection and load grid data"""
        try:
            # Load grid data
            self.logger.info(f"Loading grid data from {config.grid_file}")
            self.grid_data = gpd.read_file(config.grid_file)
            self.logger.info(f"Loaded {len(self.grid_data)} grid cells")

            # Filter for our specific grid IDs
            self.grid_data = self.grid_data[self.grid_data.index.isin(config.grid_ids)]

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

    async def connect_database(self) -> bool:
        """Connect to the database (only for database mode)"""
        if config.mode == config.ProcessingMode.LOCAL_ONLY:
            self.logger.info("Local mode: skipping database connection")
            return True

        try:
            self.logger.info("Connecting to database...")
            self.conn = psycopg2.connect(**config.db_config)
            self.conn.autocommit = False

            # Test connection
            with self.conn.cursor() as cur:
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
                self.logger.info(f"Connected to: {version}")

            return True

        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            return False

    def close_database(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            self.logger.info("Database connection closed")

    def find_image_files_for_year(self, year: int) -> List[Path]:
        """Find all downloaded image files for a specific year"""
        if config.mode == config.ProcessingMode.LOCAL_ONLY:
            year_dir = config.get_year_images_dir(year)
        else:
            year_dir = config.images_dir / "temp"

        if not year_dir.exists():
            self.logger.warning(
                f"Images directory for {year} does not exist: {year_dir}"
            )
            return []

        # Look for TIFF files
        image_files = list(year_dir.glob("*_08.tiff")) + list(year_dir.glob("*_08.tif"))

        # Filter by year in filename
        year_files = [f for f in image_files if f"_{year}_" in f.name]

        self.logger.info(f"Found {len(year_files)} image files for year {year}")
        return year_files

    def parse_filename(self, filepath: Path) -> Optional[Dict]:
        """Parse grid_id and date from filename"""
        try:
            # Expected format: sentinel2_grid_{grid_id}_{year}_08.tiff
            filename = filepath.stem
            parts = filename.split("_")

            if len(parts) < 5:
                self.logger.error(f"Unexpected filename format: {filename}")
                return None

            grid_id = int(parts[2])
            year = int(parts[3])
            month = int(parts[4])

            # Create date (using 15th of month as representative date)
            date = datetime(year, month, 15)

            return {"grid_id": grid_id, "date": date, "year": year, "month": month}

        except (ValueError, IndexError) as e:
            self.logger.error(f"Failed to parse filename {filepath}: {e}")
            return None

    def get_exact_grid_bbox_wkt(self, grid_id: int) -> Optional[str]:
        """Get exact grid cell bounding box as WKT"""
        try:
            grid_row = self.grid_data[self.grid_data.index == grid_id]
            if grid_row.empty:
                self.logger.error(f"Grid ID {grid_id} not found in grid data")
                return None

            # Get exact geometry without any modification
            geometry = grid_row.geometry.iloc[0]
            wkt = geometry.wkt

            self.logger.debug(f"Grid {grid_id} exact WKT: {wkt}")
            return wkt

        except Exception as e:
            self.logger.error(f"Failed to get grid bbox for {grid_id}: {e}")
            return None

    def extract_image_metadata(self, filepath: Path) -> Optional[Dict]:
        """Extract metadata from image file"""
        try:
            with rasterio.open(filepath) as src:
                # Get exact bounds from the raster
                bounds = src.bounds

                # Create exact polygon from bounds
                from shapely.geometry import box

                bbox_polygon = box(bounds.left, bounds.bottom, bounds.right, bounds.top)
                bbox_wkt = bbox_polygon.wkt

                metadata = {
                    "width": src.width,
                    "height": src.height,
                    "data_type": str(src.dtypes[0]),
                    "crs": str(src.crs),
                    "bounds": bounds,
                    "bbox_wkt": bbox_wkt,
                    "transform": src.transform,
                }

                self.logger.debug(f"Image metadata for {filepath.name}:")
                self.logger.debug(f"  Size: {metadata['width']}x{metadata['height']}")
                self.logger.debug(f"  CRS: {metadata['crs']}")

                return metadata

        except Exception as e:
            self.logger.error(f"Failed to extract metadata from {filepath}: {e}")
            return None

    def extract_band_data(self, filepath: Path, metadata: Dict) -> Dict[str, bytes]:
        """Extract and store band data as bytes"""
        band_data = {}

        try:
            with rasterio.open(filepath) as src:
                # Read each band and convert to bytes
                for i, band_name in enumerate(config.bands, 1):
                    if i <= src.count:
                        band_array = src.read(i)
                        # Store as raw bytes (preserve original data type)
                        band_data[config.band_mapping[band_name]] = band_array.tobytes()

            self.logger.debug(
                f"Extracted {len(band_data)} bands: {list(band_data.keys())}"
            )
            return band_data

        except Exception as e:
            self.logger.error(f"Failed to extract band data from {filepath}: {e}")
            return {}

    async def store_image_locally(
        self, filepath: Path, file_info: Dict, metadata: Dict
    ) -> bool:
        """Store image metadata locally (for local mode)"""
        try:
            # Create metadata file alongside the image
            metadata_file = filepath.with_suffix(".json")

            full_metadata = {
                "file_info": file_info,
                "image_metadata": metadata,
                "stored_at": datetime.now().isoformat(),
                "grid_id": file_info["grid_id"],
                "date": file_info["date"].isoformat(),
                "bands": config.bands,
            }

            import json

            with open(metadata_file, "w") as f:
                json.dump(full_metadata, f, indent=2)

            self.logger.debug(f"Stored metadata locally: {metadata_file}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to store metadata locally: {e}")
            return False

    def check_existing_record(self, grid_id: int, date: datetime) -> bool:
        """Check if record already exists in database"""
        if config.mode == config.ProcessingMode.LOCAL_ONLY:
            # For local mode, check if metadata file exists
            year_dir = config.get_year_images_dir(date.year)
            filename = f"sentinel2_grid_{grid_id}_{date.year}_08.json"
            return (year_dir / filename).exists()

        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM eo WHERE grid_id = %s AND month = %s",
                    (grid_id, date.replace(day=1).date()),
                )
                return cur.fetchone() is not None

        except Exception as e:
            self.logger.error(f"Failed to check existing record: {e}")
            return False

    async def insert_image_record(
        self, filepath: Path, file_info: Dict, metadata: Dict, band_data: Dict
    ) -> bool:
        """Insert image record into database or store locally"""
        try:
            grid_id = file_info["grid_id"]
            date = file_info["date"]

            # Check if record already exists
            if self.check_existing_record(grid_id, date):
                self.logger.info(
                    f"Record already exists for grid {grid_id}, {date.strftime('%Y-%m')}"
                )
                return True

            if config.mode == config.ProcessingMode.LOCAL_ONLY:
                # Store locally
                return await self.store_image_locally(filepath, file_info, metadata)
            else:
                # Store in database
                return await self.insert_into_database(
                    filepath, file_info, metadata, band_data
                )

        except Exception as e:
            self.logger.error(f"Failed to insert record for {filepath}: {e}")
            return False

    async def insert_into_database(
        self, filepath: Path, file_info: Dict, metadata: Dict, band_data: Dict
    ) -> bool:
        """Insert record into database"""
        try:
            grid_id = file_info["grid_id"]
            date = file_info["date"]

            # Get exact grid bbox for consistency
            grid_bbox_wkt = self.get_exact_grid_bbox_wkt(grid_id)
            if not grid_bbox_wkt:
                self.logger.error(f"Could not get grid bbox for {grid_id}")
                return False

            # Prepare insert statement using exact grid bbox
            insert_sql = """
                INSERT INTO eo (
                    time, grid_id, bbox, width, height, data_type,
                    b02, b03, b04
                ) VALUES (
                    %s, %s, ST_GeogFromText(%s), %s, %s, %s,
                    %s, %s, %s
                )
            """

            values = (
                date,
                grid_id,
                grid_bbox_wkt,
                metadata["width"],
                metadata["height"],
                metadata["data_type"],
                band_data.get("b02"),
                band_data.get("b03"),
                band_data.get("b04"),
            )

            with self.conn.cursor() as cur:
                cur.execute(insert_sql, values)
                self.conn.commit()

            self.logger.info(
                f"Successfully inserted record for grid {grid_id}, {date.strftime('%Y-%m')}"
            )
            return True

        except Exception as e:
            self.logger.error(f"Failed to insert into database: {e}")
            if self.conn:
                self.conn.rollback()
            return False

    async def process_image_file(self, filepath: Path) -> bool:
        """Process a single image file"""
        try:
            self.logger.debug(f"Processing: {filepath.name}")

            # Parse filename
            file_info = self.parse_filename(filepath)
            if not file_info:
                return False

            # Extract metadata
            metadata = self.extract_image_metadata(filepath)
            if not metadata:
                return False

            # Extract band data (only needed for database mode)
            band_data = {}
            if config.mode != config.ProcessingMode.LOCAL_ONLY:
                band_data = self.extract_band_data(filepath, metadata)
                if not band_data:
                    return False

            # Insert/store record
            return await self.insert_image_record(
                filepath, file_info, metadata, band_data
            )

        except Exception as e:
            self.logger.error(f"Failed to process {filepath}: {e}")
            return False

    async def process_year(self, year: int) -> bool:
        """Process insertions for a specific year"""
        self.logger.info(f"Processing insertions for year {year}")
        self.current_year = year

        # Find image files for this year
        image_files = self.find_image_files_for_year(year)
        if not image_files:
            self.logger.warning(f"No image files found for year {year}")
            return True

        # Generate task IDs
        task_ids = [
            f"insert_{self.parse_filename(f)['grid_id']}_{year}" for f in image_files
        ]

        # Load or create checkpoint
        checkpoint = state_manager.load_checkpoint("insert", year)
        if not checkpoint:
            checkpoint = state_manager.create_stage_checkpoint("insert", year, task_ids)

        # Get pending tasks
        pending_task_ids = state_manager.get_pending_tasks("insert", year)
        pending_files = []

        for filepath in image_files:
            file_info = self.parse_filename(filepath)
            if file_info:
                task_id = f"insert_{file_info['grid_id']}_{year}"
                if task_id in pending_task_ids:
                    pending_files.append((filepath, task_id))

        self.logger.info(
            f"Found {len(pending_files)} pending insertion tasks for {year}"
        )

        if not pending_files:
            self.logger.info(f"All insertions for {year} already completed")
            return True

        # Process insertions
        success_count = 0
        for filepath, task_id in pending_files:
            # Update status to running
            state_manager.update_task_status(
                "insert", year, task_id, TaskStatus.RUNNING
            )

            try:
                success = await self.process_image_file(filepath)

                if success:
                    # Update status to completed
                    metadata = {"filepath": str(filepath)}
                    state_manager.update_task_status(
                        "insert", year, task_id, TaskStatus.COMPLETED, metadata=metadata
                    )
                    success_count += 1
                else:
                    # Update status to failed
                    state_manager.update_task_status(
                        "insert",
                        year,
                        task_id,
                        TaskStatus.FAILED,
                        error_message=f"Failed to process {filepath.name}",
                    )

            except Exception as e:
                error_msg = f"Unexpected error processing {filepath.name}: {e}"
                self.logger.error(error_msg)
                state_manager.update_task_status(
                    "insert", year, task_id, TaskStatus.FAILED, error_message=error_msg
                )

        self.logger.info(
            f"Completed insertions for {year}: {success_count}/{len(pending_files)} successful"
        )
        return success_count == len(pending_files)

    async def run_insertions(self) -> bool:
        """Execute insertions for all years"""
        if not await self.initialize():
            self.logger.error("Failed to initialize inserter")
            return False

        if not await self.connect_database():
            self.logger.error("Failed to connect to database")
            return False

        try:
            self.logger.info(f"Starting insertions for years: {config.years}")
            self.logger.info(f"Storage mode: {config.mode.value}")

            # Process each year sequentially
            overall_success = True
            for year in config.years:
                try:
                    year_success = await self.process_year(year)
                    if not year_success:
                        overall_success = False
                        self.logger.warning(f"Some insertions failed for year {year}")
                except Exception as e:
                    self.logger.error(f"Failed to process year {year}: {e}")
                    overall_success = False

            return overall_success

        finally:
            self.close_database()


# Export the inserter class
__all__ = ["SentinelInserterV5"]
