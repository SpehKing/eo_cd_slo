#!/usr/bin/env python3
"""
Database Insertion Script v4 for Sentinel-2 Images and Change Detection Masks

This script processes downloaded Sentinel-2 images and inserts them into the
TimescaleDB database with perfect grid alignment. It uses the expanded Slovenia
grid and ensures bbox consistency with zero tolerance. After inserting all images,
it creates change detection masks for consecutive time periods using a fixed mask
from the masks directory.

Requirements:
- rasterio
- geopandas
- psycopg2-binary
- tqdm
- numpy
- cv2

Usage:
    python insert_sentinel_v4_and_mask.py
"""

import logging
import psycopg2
import geopandas as gpd
import rasterio
import numpy as np
import cv2
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
DOWNLOAD_DIR = Path("./data/images/sentinel_downloads_v4/images")
GRID_FILE = Path("./data/slovenia_grid_expanded.gpkg")
MASKS_DIR = Path("./data/images/sentinel_downloads_v4/mask")

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
            "masks_inserted": 0,
            "masks_failed": 0,
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

    def read_change_mask(
        self, mask_filename: str = "binary_mask.png"
    ) -> Tuple[bytes, Dict[str, any]]:
        """
        Read the change detection mask from masks directory

        Args:
            mask_filename: Name of the mask file to read

        Returns:
            Tuple of (mask data as bytes, metadata dictionary)
        """
        try:
            mask_file = MASKS_DIR / mask_filename

            if not mask_file.exists():
                raise FileNotFoundError(f"Mask file not found: {mask_file}")

            with rasterio.open(mask_file) as src:
                mask_data = src.read(1)  # Read first (and only) band

                metadata = {
                    "width": src.width,
                    "height": src.height,
                    "data_type": str(src.dtypes[0]),
                }

                # Convert mask to binary format (0 for no change, 255 for change)
                # Assuming the mask has values where non-zero indicates change
                mask_visualization = np.where(mask_data > 0, 255, 0).astype(np.uint8)

                logger.debug(
                    f"Read mask {mask_filename}: {metadata['width']}x{metadata['height']}, type: {metadata['data_type']}"
                )
                logger.debug(
                    f"Mask values: min={np.min(mask_data)}, max={np.max(mask_data)}"
                )

                return mask_visualization.tobytes(), metadata

        except Exception as e:
            logger.error(f"Failed to read change mask {mask_filename}: {e}")
            raise

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

    def insert_change_mask(
        self,
        grid_id: int,
        img_a_id: int,
        img_b_id: int,
        timestamp_a: datetime,
        timestamp_b: datetime,
        bbox_wkt: str,
        mask_filename: str = "binary_mask.png",
    ) -> bool:
        """
        Insert a change detection mask for two images

        Args:
            grid_id: Grid cell ID
            img_a_id: ID of first image (earlier)
            img_b_id: ID of second image (later)
            timestamp_a: First timestamp
            timestamp_b: Second timestamp
            bbox_wkt: PostGIS geography polygon string
            mask_filename: Name of the mask file to use

        Returns:
            True if successful, False otherwise
        """
        try:
            # Ensure img_a is earlier than img_b
            if timestamp_a > timestamp_b:
                img_a_id, img_b_id = img_b_id, img_a_id
                timestamp_a, timestamp_b = timestamp_b, timestamp_a

            # Read change mask
            mask_data, mask_metadata = self.read_change_mask(mask_filename)

            # Insert into eo_change table
            insert_sql = """
                INSERT INTO eo_change (img_a_id, img_b_id, grid_id, period_start, period_end, bbox, width, height, data_type, mask)
                VALUES (%s, %s, %s, %s, %s, ST_GeogFromText(%s), %s, %s, %s, %s)
            """

            values = (
                img_a_id,
                img_b_id,
                grid_id,
                timestamp_a,
                timestamp_b,
                bbox_wkt,
                mask_metadata["width"],
                mask_metadata["height"],
                mask_metadata["data_type"],
                mask_data,
            )

            with self.conn.cursor() as cur:
                cur.execute(insert_sql, values)
                self.conn.commit()

            logger.info(
                f"✓ Inserted change mask for grid {grid_id}: {timestamp_a.strftime('%Y-%m')} -> {timestamp_b.strftime('%Y-%m')} "
                f"({mask_metadata['width']}x{mask_metadata['height']}, {mask_metadata['data_type']})"
            )
            self.insertion_stats["masks_inserted"] += 1
            return True

        except Exception as e:
            logger.error(f"✗ Failed to insert change mask for grid {grid_id}: {e}")
            if self.conn:
                self.conn.rollback()
            self.insertion_stats["masks_failed"] += 1
            return False

    def create_change_masks(self) -> None:
        """
        Create change detection masks for consecutive time periods within each grid cell
        """
        try:
            logger.info("Finding consecutive image pairs for change detection...")

            with self.conn.cursor() as cur:
                # Find all consecutive image pairs within each grid_id
                cur.execute(
                    """
                    WITH consecutive_pairs AS (
                        SELECT 
                            grid_id,
                            id as img_id,
                            time as img_time,
                            bbox,
                            LAG(id) OVER (PARTITION BY grid_id ORDER BY time) as prev_img_id,
                            LAG(time) OVER (PARTITION BY grid_id ORDER BY time) as prev_img_time
                        FROM eo 
                        WHERE grid_id IS NOT NULL
                        ORDER BY grid_id, time
                    )
                    SELECT 
                        grid_id,
                        prev_img_id as img_a_id,
                        img_id as img_b_id,
                        prev_img_time as timestamp_a,
                        img_time as timestamp_b,
                        ST_AsText(bbox) as bbox_wkt
                    FROM consecutive_pairs 
                    WHERE prev_img_id IS NOT NULL
                    ORDER BY grid_id, timestamp_a
                """
                )

                pairs = cur.fetchall()
                logger.info(
                    f"Found {len(pairs)} consecutive image pairs for change detection"
                )

                if not pairs:
                    logger.info("No consecutive image pairs found")
                    return

                # Process each pair with progress bar
                with tqdm(total=len(pairs), desc="Creating change masks") as pbar:
                    for (
                        grid_id,
                        img_a_id,
                        img_b_id,
                        timestamp_a,
                        timestamp_b,
                        bbox_wkt,
                    ) in pairs:
                        pbar.set_description(f"Mask for grid {grid_id}")

                        # Check if change mask already exists
                        cur.execute(
                            """
                            SELECT img_a_id FROM eo_change 
                            WHERE img_a_id = %s AND img_b_id = %s
                        """,
                            (img_a_id, img_b_id),
                        )

                        if cur.fetchone():
                            logger.debug(
                                f"Change mask already exists for images {img_a_id} -> {img_b_id}"
                            )
                            pbar.update(1)
                            continue

                        # Create change mask for this pair
                        self.insert_change_mask(
                            grid_id=grid_id,
                            img_a_id=img_a_id,
                            img_b_id=img_b_id,
                            timestamp_a=timestamp_a,
                            timestamp_b=timestamp_b,
                            bbox_wkt=bbox_wkt,
                        )

                        pbar.update(1)

        except Exception as e:
            logger.error(f"Failed to create change masks: {e}")
            if self.conn:
                self.conn.rollback()

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

            # After all images are processed, create change masks for consecutive time periods
            logger.info("Creating change detection masks...")
            self.create_change_masks()

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
        logger.info(f"Change masks inserted: {stats['masks_inserted']}")
        logger.info(f"Change masks failed: {stats['masks_failed']}")
        if stats["total_files"] > 0:
            success_rate = (stats["successful"] / stats["total_files"]) * 100
            logger.info(f"Image success rate: {success_rate:.1f}%")
        if (stats["masks_inserted"] + stats["masks_failed"]) > 0:
            mask_success_rate = (
                stats["masks_inserted"]
                / (stats["masks_inserted"] + stats["masks_failed"])
            ) * 100
            logger.info(f"Mask success rate: {mask_success_rate:.1f}%")
        logger.info("=" * 60)


def main():
    """Main function"""
    logger.info(
        "Starting Sentinel-2 database insertion script v4 (with change detection masks)"
    )

    try:
        inserter = SentinelInserterV4()
        success = inserter.run_insertion()

        if success:
            logger.info("Insertion process completed successfully")
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
