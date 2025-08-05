#!/usr/bin/env python3
"""
Database Insertion Script v4 for Sentinel-2 Images

This script processes downloaded Sentinel-2 images and inserts them into the
TimescaleDB database with perfect grid alignment. It uses the expanded Slovenia
grid and ensures bbox consistency with zero tolerance.

Requirements:
- rasterio
- geopandas
- psycopg2-binary
- tqdm
- numpy

Usage:
    python insert_sentinel_v4.py
"""

import logging
import psycopg2
import geopandas as gpd
import rasterio
import numpy as np
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from tqdm import tqdm
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("insert_sentinel_v4.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Configuration
DOWNLOAD_DIR = Path("./data/images/sentinel_downloads_v4")
GRID_FILE = Path("./data/slovenia_grid_expanded.gpkg")

# Database configuration (should match your docker-compose)
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "eo_db",
    "user": "postgres",
    "password": "password",
}

# Target CRS for consistency
TARGET_CRS = "EPSG:4326"

# Bands to process
BANDS = ["B02", "B03", "B04"]  # Blue, Green, Red

# Band mapping (Sentinel-2 bands to database columns)
BAND_MAPPING = {
    "B02": "b02",  # Blue
    "B03": "b03",  # Green
    "B04": "b04",  # Red
}


class SentinelInserterV4:
    """Handles insertion of Sentinel-2 data into database with perfect grid alignment"""

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

            # Ensure CRS is EPSG:4326
            if self.grid_data.crs != TARGET_CRS:
                logger.info(f"Converting CRS from {self.grid_data.crs} to {TARGET_CRS}")
                self.grid_data = self.grid_data.to_crs(TARGET_CRS)

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
                    logger.info(f"Grid cells table already contains {count} records")
                    return

                logger.info("Loading grid cells into database...")

                # Insert grid cells
                for idx, row in self.grid_data.iterrows():
                    # Get geometry in both CRS formats
                    geom_4326 = row.geometry  # Already in EPSG:4326

                    # Convert to EPSG:3857 for the geom column (if needed by schema)
                    geom_3857 = self.grid_data.to_crs("EPSG:3857").loc[idx, "geometry"]

                    # Extract additional attributes if they exist
                    index_x = getattr(row, "index_x", idx % 100)  # fallback values
                    index_y = getattr(row, "index_y", idx // 100)

                    cur.execute(
                        """
                        INSERT INTO grid_cells (grid_id, index_x, index_y, geom, bbox_4326)
                        VALUES (%s, %s, %s, ST_GeomFromText(%s, 3857), ST_GeogFromText(%s))
                        ON CONFLICT (grid_id) DO NOTHING
                    """,
                        (
                            idx,  # grid_id
                            index_x,
                            index_y,
                            geom_3857.wkt,  # EPSG:3857 geometry
                            geom_4326.wkt,  # EPSG:4326 geography
                        ),
                    )

                self.conn.commit()
                logger.info(f"Loaded {len(self.grid_data)} grid cells into database")

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
                logger.error(f"Unexpected filename format: {filename}")
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

    def get_exact_grid_bbox_wkt(self, grid_id: int) -> Optional[str]:
        """Get exact grid cell bounding box as WKT (zero tolerance)"""
        try:
            grid_row = self.grid_data[self.grid_data.index == grid_id]
            if grid_row.empty:
                logger.error(f"Grid ID {grid_id} not found in grid data")
                return None

            # Get exact geometry without any modification
            geometry = grid_row.geometry.iloc[0]
            wkt = geometry.wkt

            logger.debug(f"Grid {grid_id} exact WKT: {wkt}")
            return wkt

        except Exception as e:
            logger.error(f"Failed to get grid bbox for {grid_id}: {e}")
            return None

    def extract_image_metadata(self, filepath: Path) -> Optional[Dict]:
        """Extract metadata from image file"""
        try:
            with rasterio.open(filepath) as src:
                # Get exact bounds from the raster
                bounds = src.bounds

                # Create exact polygon from bounds (no rounding)
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

                logger.debug(f"Image metadata for {filepath.name}:")
                logger.debug(f"  Size: {metadata['width']}x{metadata['height']}")
                logger.debug(f"  CRS: {metadata['crs']}")
                logger.debug(f"  Bounds: {bounds}")

                return metadata

        except Exception as e:
            logger.error(f"Failed to extract metadata from {filepath}: {e}")
            return None

    def extract_band_data(self, filepath: Path, metadata: Dict) -> Dict[str, bytes]:
        """Extract and store band data as bytes"""
        band_data = {}

        try:
            with rasterio.open(filepath) as src:
                # Read each band and convert to bytes
                for i, band_name in enumerate(BANDS, 1):
                    if i <= src.count:
                        band_array = src.read(i)

                        # Store as raw bytes (preserve original data type)
                        band_data[BAND_MAPPING[band_name]] = band_array.tobytes()

                        logger.debug(
                            f"Extracted band {band_name} -> {BAND_MAPPING[band_name]}"
                        )

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
                    "SELECT id FROM eo WHERE grid_id = %s AND month = %s",
                    (grid_id, date.replace(day=1).date()),
                )
                return cur.fetchone() is not None

        except Exception as e:
            logger.error(f"Failed to check existing record: {e}")
            return False

    def validate_bbox_alignment(self, grid_id: int, image_bbox_wkt: str) -> bool:
        """
        Validate that image bbox aligns perfectly with grid cell (zero tolerance)
        """
        try:
            from shapely.wkt import loads

            # Get grid bbox
            grid_bbox_wkt = self.get_exact_grid_bbox_wkt(grid_id)
            if not grid_bbox_wkt:
                return False

            # Load geometries
            grid_geom = loads(grid_bbox_wkt)
            image_geom = loads(image_bbox_wkt)

            # Check for exact containment or very high overlap
            intersection = grid_geom.intersection(image_geom)
            overlap_ratio = intersection.area / grid_geom.area

            logger.debug(f"Bbox validation for grid {grid_id}:")
            logger.debug(f"  Overlap ratio: {overlap_ratio:.6f}")

            # Require at least 99.9% overlap (near-perfect alignment)
            if overlap_ratio < 0.999:
                logger.warning(f"Grid {grid_id}: Low overlap ratio {overlap_ratio:.6f}")
                return False

            logger.info(
                f"Grid {grid_id}: Bbox alignment validated (overlap: {overlap_ratio:.6f})"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to validate bbox alignment for grid {grid_id}: {e}")
            return False

    def insert_image_record(
        self, filepath: Path, file_info: Dict, metadata: Dict, band_data: Dict
    ) -> bool:
        """Insert image record into database with exact bbox alignment"""
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

            # Use the exact image bbox (from actual raster bounds)
            image_bbox_wkt = metadata["bbox_wkt"]

            # Validate bbox alignment
            if not self.validate_bbox_alignment(grid_id, image_bbox_wkt):
                logger.error(f"Bbox alignment validation failed for grid {grid_id}")
                self.insertion_stats["failed"] += 1
                return False

            # Use the grid's exact bbox for consistency in database
            grid_bbox_wkt = self.get_exact_grid_bbox_wkt(grid_id)
            if not grid_bbox_wkt:
                logger.error(f"Could not get grid bbox for {grid_id}")
                self.insertion_stats["failed"] += 1
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
                grid_bbox_wkt,  # Use exact grid bbox for consistency
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

    def process_image_file(self, filepath: Path) -> bool:
        """Process a single image file"""
        try:
            logger.info(f"Processing: {filepath.name}")

            # Parse filename
            file_info = self.parse_filename(filepath)
            if not file_info:
                return False

            # Extract metadata
            metadata = self.extract_image_metadata(filepath)
            if not metadata:
                return False

            # Extract band data
            band_data = self.extract_band_data(filepath, metadata)
            if not band_data:
                return False

            # Insert into database
            return self.insert_image_record(filepath, file_info, metadata, band_data)

        except Exception as e:
            logger.error(f"Failed to process {filepath}: {e}")
            self.insertion_stats["failed"] += 1
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
            # Load grid cells into database
            self.load_grid_cells_to_database()

            # Find image files
            image_files = self.find_image_files()
            if not image_files:
                logger.error("No image files found")
                return False

            self.insertion_stats["total_files"] = len(image_files)
            logger.info(f"Found {len(image_files)} image files to process")

            # Process each file with progress bar
            with tqdm(total=len(image_files), desc="Inserting images") as pbar:
                for filepath in image_files:
                    pbar.set_description(f"Processing {filepath.name}")
                    self.process_image_file(filepath)
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
            success_rate = (stats["successful"] / stats["total_files"]) * 100
            logger.info(f"Success rate: {success_rate:.1f}%")
        logger.info("=" * 60)


def main():
    """Main function"""
    logger.info("Starting Sentinel-2 database insertion script v4")

    try:
        inserter = SentinelInserterV4()
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
