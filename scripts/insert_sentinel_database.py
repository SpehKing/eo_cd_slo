#!/usr/bin/env python3
"""
Database Insertion Script for Sentinel-2 Images

This script processes downloaded Sentinel-2 images and inserts them into the
TimescaleDB database. It handles image metadata extraction, band data storage,
and proper georeferencing.

Requirements:
- rasterio
- geopandas
- psycopg2-binary
- tqdm
- numpy

Usage:
    python insert_sentinel_database.py
"""

import os
import sys
import logging
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import psycopg2
from psycopg2.extras import execute_values
import rasterio
from rasterio.warp import transform_bounds
import geopandas as gpd
from shapely.geometry import box
import numpy as np
from tqdm import tqdm
from datetime import datetime
import io
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("insert_sentinel.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Configuration
DOWNLOAD_DIR = Path("./data/images/sentinel_downloads_v3")
GRID_FILE = Path("./grid_output/slovenia_grid_expanded.gpkg")

# Database configuration (should match your docker-compose)
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "eo_db",
    "user": "postgres",
    "password": "password",
}


# Band mapping (Sentinel-2 bands to database columns)
BAND_MAPPING = {
    "B02": "b02",  # Blue
    "B03": "b03",  # Green
    "B04": "b04",  # Red
}


class SentinelInserter:
    """Handles insertion of Sentinel-2 data into database"""

    def __init__(self):
        self.conn = None
        self.grid_data = None
        self.insertion_stats = {
            "total_files": 0,
            "successful": 0,
            "failed": 0,
            "skipped": 0,
        }

    def initialize(self) -> bool:
        """Initialize database connection and load grid data"""
        try:
            # Load grid data
            logger.info(f"Loading grid data from {GRID_FILE}")
            self.grid_data = gpd.read_file(GRID_FILE)
            logger.info(f"Loaded {len(self.grid_data)} grid cells")

            # Convert to WGS84 if needed
            if self.grid_data.crs != "EPSG:4326":
                logger.info(f"Converting CRS from {self.grid_data.crs} to EPSG:4326")
                self.grid_data = self.grid_data.to_crs("EPSG:4326")

            return True

        except Exception as e:
            logger.error(f"Failed to initialize: {e}")
            return False

    def connect_database(self) -> bool:
        """Connect to the database"""
        try:
            logger.info("Connecting to database...")
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.conn.autocommit = False

            # Test connection
            with self.conn.cursor() as cur:
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
                logger.info(f"Connected to: {version}")

                # Check if required tables exist
                cur.execute(
                    """
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_name IN ('eo', 'grid_cells')
                """
                )
                tables = [row[0] for row in cur.fetchall()]

                if "eo" not in tables:
                    logger.error("Table 'eo' not found in database")
                    return False
                if "grid_cells" not in tables:
                    logger.error("Table 'grid_cells' not found in database")
                    return False

                logger.info("Required database tables found")

            return True

        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False

    def close_database(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

    def load_grid_cells_to_database(self) -> None:
        """
        Load Slovenia grid cells into the database if not already present
        """
        try:
            with self.conn.cursor() as cur:
                # Check if grid_cells table has data
                cur.execute("SELECT COUNT(*) FROM grid_cells")
                count = cur.fetchone()[0]

                if count > 0:
                    logger.info(
                        f"Grid cells table already contains {count} records, skipping grid load"
                    )
                    return

                logger.info("Loading Slovenia grid cells into database...")

                # Prepare grid data for insertion
                insert_data = []
                for idx, row in self.grid_data.iterrows():
                    grid_id = int(row.name)  # Using row.name instead of row["index"]

                    # Get geometry in WGS84
                    geom_4326 = row.geometry

                    # For index_x and index_y, derive from the grid_id
                    index_x = grid_id % 100  # Simple derivation, adjust as needed
                    index_y = grid_id // 100

                    insert_data.append(
                        (
                            grid_id,
                            index_x,
                            index_y,
                            geom_4326.wkt,  # geom (we'll transform in SQL)
                            geom_4326.wkt,  # bbox_4326
                        )
                    )

                # Bulk insert
                from psycopg2.extras import execute_values

                insert_query = """
                    INSERT INTO grid_cells (grid_id, index_x, index_y, geom, bbox_4326)
                    VALUES %s
                """

                template = """(
                    %s, %s, %s, 
                    ST_Transform(ST_GeomFromText(%s, 4326), 3857),
                    ST_GeogFromText(%s)
                )"""

                execute_values(cur, insert_query, insert_data, template=template)
                self.conn.commit()

                logger.info(
                    f"Successfully loaded {len(insert_data)} grid cells into database"
                )

        except Exception as e:
            logger.error(f"Failed to load grid cells: {e}")
            if self.conn:
                self.conn.rollback()
            raise

    def find_image_files(self) -> List[Path]:
        """Find all downloaded image files"""
        if not DOWNLOAD_DIR.exists():
            logger.error(f"Download directory {DOWNLOAD_DIR} does not exist")
            return []

        # Look for TIFF files
        image_files = list(DOWNLOAD_DIR.glob("*.tiff")) + list(
            DOWNLOAD_DIR.glob("*.tif")
        )
        logger.info(f"Found {len(image_files)} image files")

        return image_files

    def parse_filename(self, filepath: Path) -> Optional[Dict]:
        """Parse grid_id and date from filename"""
        try:
            # Expected format: sentinel2_grid_{grid_id}_{year}_08.tiff
            filename = filepath.stem
            parts = filename.split("_")

            if len(parts) < 5:
                logger.warning(f"Unexpected filename format: {filename}")
                return None

            grid_id = int(parts[2])
            year = int(parts[3])
            month = int(parts[4])

            # Create date (using 15th of month as representative date)
            date = datetime(year, month, 15)

            return {"grid_id": grid_id, "date": date, "year": year, "month": month}

        except (ValueError, IndexError) as e:
            logger.error(f"Failed to parse filename {filepath}: {e}")
            return None

    def get_grid_bbox_wkt(self, grid_id: int) -> Optional[str]:
        """Get grid cell bounding box as WKT"""
        try:
            grid_row = self.grid_data[self.grid_data.index == grid_id]
            if grid_row.empty:
                logger.error(f"Grid ID {grid_id} not found in grid data")
                return None

            geometry = grid_row.geometry.iloc[0]
            return geometry.wkt

        except Exception as e:
            logger.error(f"Failed to get grid bbox for {grid_id}: {e}")
            return None

    def extract_image_metadata(self, filepath: Path) -> Optional[Dict]:
        """Extract metadata from image file"""
        try:
            with rasterio.open(filepath) as src:
                # Get bounds in the image's native CRS
                bounds = src.bounds
                native_crs = src.crs

                # Convert bounds to WGS84 for database storage
                if native_crs != "EPSG:4326":
                    from rasterio.warp import transform_bounds

                    wgs84_bounds = transform_bounds(native_crs, "EPSG:4326", *bounds)
                else:
                    wgs84_bounds = bounds

                # Create WKT polygon from WGS84 bounds
                bbox_wkt = f"POLYGON(({wgs84_bounds[0]} {wgs84_bounds[1]}, {wgs84_bounds[2]} {wgs84_bounds[1]}, {wgs84_bounds[2]} {wgs84_bounds[3]}, {wgs84_bounds[0]} {wgs84_bounds[3]}, {wgs84_bounds[0]} {wgs84_bounds[1]}))"

                # Get band information using the correct rasterio method
                band_names = []
                band_descriptions = []
                for i in range(1, src.count + 1):
                    # Get band description using tags
                    tags = src.tags(i)
                    desc = tags.get("DESCRIPTION", f"Band_{i}")

                    # If no description in tags, try getting from band metadata
                    if desc == f"Band_{i}":
                        try:
                            # For OpenEO/GDAL files, band names are often in the band description
                            desc = src.descriptions[i - 1] or f"Band_{i}"
                        except (IndexError, AttributeError):
                            desc = f"Band_{i}"

                    band_names.append(desc)
                    band_descriptions.append(desc)

                metadata = {
                    "width": src.width,
                    "height": src.height,
                    "crs": str(src.crs),
                    "native_bounds": bounds,
                    "wgs84_bounds": wgs84_bounds,
                    "bbox_wkt": bbox_wkt,
                    "transform": list(src.transform),
                    "data_type": str(src.dtypes[0]),
                    "band_count": src.count,
                    "band_names": band_names,
                    "band_descriptions": band_descriptions,
                    "nodata": src.nodata,
                }

                logger.debug(f"Image metadata: {metadata}")
                return metadata

        except Exception as e:
            logger.error(f"Failed to extract metadata from {filepath}: {e}")
            return None

    def compress_band_data(self, band_array: np.ndarray) -> bytes:
        """Store band data as raw bytes (no compression for compatibility)"""
        try:
            # Keep original data type for Sentinel-2 (Int16)
            # Convert only if necessary
            if band_array.dtype == np.int16:
                # Keep as int16 but ensure it's in a format the database can handle
                band_array = band_array.astype(np.int16)
            elif band_array.dtype != np.uint16:
                # Convert to uint16 for other cases
                # Shift int16 to uint16 range if needed
                if band_array.dtype == np.int16:
                    band_array = (band_array.astype(np.int32) + 32768).astype(np.uint16)
                else:
                    band_array = band_array.astype(np.uint16)

            # Store as raw bytes for direct backend compatibility
            return band_array.tobytes()

        except Exception as e:
            logger.error(f"Failed to convert band data: {e}")
            return None

    def extract_band_data(self, filepath: Path, metadata: Dict) -> Dict[str, bytes]:
        """Extract and compress band data"""
        band_data = {}

        try:
            with rasterio.open(filepath) as src:
                logger.debug(f"Processing {src.count} bands from {filepath.name}")
                logger.debug(f"Band descriptions: {src.descriptions}")

                # Map bands by their descriptions (B02, B03, B04)
                for i in range(1, src.count + 1):
                    try:
                        # Get band description
                        band_desc = (
                            src.descriptions[i - 1] if src.descriptions else f"Band_{i}"
                        )
                        logger.debug(f"Band {i}: description='{band_desc}'")

                        # Map to database column
                        db_column = BAND_MAPPING.get(band_desc)

                        if db_column:
                            logger.debug(
                                f"Reading band {i} ({band_desc}) -> {db_column}"
                            )
                            band_array = src.read(i)

                            # Handle NoData values
                            if src.nodata is not None:
                                band_array = np.where(
                                    band_array == src.nodata, 0, band_array
                                )

                            compressed_data = self.compress_band_data(band_array)

                            if compressed_data:
                                band_data[db_column] = compressed_data
                            else:
                                logger.warning(f"Failed to compress band {band_desc}")
                        else:
                            logger.debug(f"Skipping unmapped band {i}: {band_desc}")

                    except Exception as e:
                        logger.error(f"Failed to process band {i}: {e}")
                        continue

            logger.info(f"Extracted {len(band_data)} bands: {list(band_data.keys())}")
            return band_data

        except Exception as e:
            logger.error(f"Failed to extract band data from {filepath}: {e}")
            return {}

    def check_existing_record(self, grid_id: int, date: datetime) -> bool:
        """Check if record already exists in database"""
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) FROM eo 
                    WHERE grid_id = %s AND DATE_TRUNC('month', time) = DATE_TRUNC('month', %s)
                """,
                    (grid_id, date),
                )

                count = cur.fetchone()[0]
                return count > 0

        except Exception as e:
            logger.error(f"Failed to check existing record: {e}")
            return False

    def insert_image_record(
        self, filepath: Path, file_info: Dict, metadata: Dict, band_data: Dict
    ) -> bool:
        """Insert image record into database"""
        try:
            grid_id = file_info["grid_id"]
            date = file_info["date"]

            # Check if record already exists
            if self.check_existing_record(grid_id, date):
                logger.info(
                    f"Record already exists for grid {grid_id}, {date.strftime('%Y-%m')}"
                )
                self.insertion_stats["skipped"] += 1
                return True

            # Get the actual image bbox (from OpenEO processing)
            image_bbox_wkt = metadata["bbox_wkt"]

            # Get grid bbox for comparison/validation
            grid_bbox_wkt = self.get_grid_bbox_wkt(grid_id)
            if not grid_bbox_wkt:
                logger.error(f"Could not get bbox for grid {grid_id}")
                return False

            # Log the comparison but use the actual image bbox
            self._log_bbox_comparison(grid_id, grid_bbox_wkt, image_bbox_wkt)

            # Use the actual image bbox from OpenEO processing
            # This represents the real extent of the downloaded data
            insert_bbox_wkt = image_bbox_wkt

            # Prepare insert statement
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
                insert_bbox_wkt,  # Use actual image bbox
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

            logger.info(
                f"Successfully inserted record for grid {grid_id}, {date.strftime('%Y-%m')}"
            )
            self.insertion_stats["successful"] += 1
            return True

        except Exception as e:
            logger.error(f"Failed to insert record for {filepath}: {e}")
            if self.conn:
                self.conn.rollback()
            self.insertion_stats["failed"] += 1
            return False

    def _log_bbox_comparison(
        self, grid_id: int, grid_bbox_wkt: str, image_bbox_wkt: str
    ):
        """Log comparison between grid and image bboxes for debugging"""
        try:
            from shapely.wkt import loads

            grid_geom = loads(grid_bbox_wkt)
            image_geom = loads(image_bbox_wkt)

            grid_bounds = grid_geom.bounds
            image_bounds = image_geom.bounds

            logger.debug(f"=== BBOX COMPARISON for Grid {grid_id} ===")
            logger.debug(f"Grid bounds:  {grid_bounds}")
            logger.debug(f"Image bounds: {image_bounds}")

            # Calculate differences
            diff_minx = image_bounds[0] - grid_bounds[0]
            diff_miny = image_bounds[1] - grid_bounds[1]
            diff_maxx = image_bounds[2] - grid_bounds[2]
            diff_maxy = image_bounds[3] - grid_bounds[3]

            logger.debug(f"Differences (image - grid):")
            logger.debug(f"  minx: {diff_minx:.6f}°")
            logger.debug(f"  miny: {diff_miny:.6f}°")
            logger.debug(f"  maxx: {diff_maxx:.6f}°")
            logger.debug(f"  maxy: {diff_maxy:.6f}°")

            overlap_area = grid_geom.intersection(image_geom).area
            grid_area = grid_geom.area
            overlap_percent = (overlap_area / grid_area) * 100

            logger.debug(f"Overlap: {overlap_percent:.2f}% of grid cell")

        except Exception as e:
            logger.debug(f"Could not compare bboxes: {e}")

    def process_image_file(self, filepath: Path) -> bool:
        """Process a single image file"""
        try:
            logger.info(f"Processing {filepath.name}")

            # Parse filename
            file_info = self.parse_filename(filepath)
            if not file_info:
                logger.error(f"Could not parse filename: {filepath}")
                return False

            # Extract image metadata
            metadata = self.extract_image_metadata(filepath)
            if not metadata:
                logger.error(f"Could not extract metadata: {filepath}")
                return False

            # Extract band data
            band_data = self.extract_band_data(filepath, metadata)
            if not band_data:
                logger.error(f"Could not extract band data: {filepath}")
                return False

            # Insert into database
            return self.insert_image_record(filepath, file_info, metadata, band_data)

        except Exception as e:
            logger.error(f"Failed to process {filepath}: {e}")
            return False

    def run_insertion(self) -> bool:
        """Run the insertion process"""
        if not self.initialize():
            logger.error("Failed to initialize inserter")
            return False

        if not self.connect_database():
            logger.error("Failed to connect to database")
            return False

        try:
            # Load grid cells into database if needed
            self.load_grid_cells_to_database()

            # Find image files
            image_files = self.find_image_files()
            if not image_files:
                logger.warning("No image files found to process")
                return True

            self.insertion_stats["total_files"] = len(image_files)
            logger.info(f"Found {len(image_files)} image files to process")

            # Process files with progress bar
            with tqdm(
                total=len(image_files), desc="Inserting images into database"
            ) as pbar:
                for filepath in image_files:
                    # Update progress bar description
                    pbar.set_description(f"Processing {filepath.name}")

                    # Process file
                    success = self.process_image_file(filepath)

                    # Update progress bar
                    pbar.set_postfix(
                        {
                            "Success": self.insertion_stats["successful"],
                            "Failed": self.insertion_stats["failed"],
                            "Skipped": self.insertion_stats["skipped"],
                        }
                    )
                    pbar.update(1)

            # Print final statistics
            self.print_final_stats()
            return True

        finally:
            self.close_database()

    def print_final_stats(self):
        """Print final insertion statistics"""
        stats = self.insertion_stats
        logger.info("=" * 60)
        logger.info("INSERTION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total files processed: {stats['total_files']}")
        logger.info(f"Successfully inserted: {stats['successful']}")
        logger.info(f"Failed: {stats['failed']}")
        logger.info(f"Skipped (already exist): {stats['skipped']}")
        if stats["total_files"] > 0:
            logger.info(
                f"Success rate: {stats['successful']/stats['total_files']*100:.1f}%"
            )
        logger.info("=" * 60)


def main():
    """Main function"""
    logger.info("Starting Sentinel-2 database insertion script")

    try:
        inserter = SentinelInserter()
        success = inserter.run_insertion()

        if success:
            logger.info("Insertion process completed")
        else:
            logger.error("Insertion process failed")
            return 1

    except KeyboardInterrupt:
        logger.info("Insertion interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
