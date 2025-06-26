#!/usr/bin/env python3
"""
populate_database.py - Bulk-load Sentinel-2 RGB GeoTIFFs into TimescaleDB

This script populates the TimescaleDB database with Sentinel-2 images stored in separate bands
and includes mask data for change detection testing.

Requires:
    pip install psycopg2-binary rasterio numpy python-dotenv
"""

import os
import glob
import re
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from itertools import combinations
import psycopg2
import psycopg2.extras
import rasterio
from rasterio.transform import from_bounds
import numpy as np
from dotenv import load_dotenv
import io


class DatabasePopulator:
    def __init__(self):
        # Load environment variables
        load_dotenv()

        # Set random seed for reproducible mask generation
        np.random.seed(42)

        # Configuration
        self.db_config = {
            "host": ("localhost"),
            "port": int(os.getenv("DB_PORT", "5432")),
            "database": os.getenv("DB_NAME", "eo_db"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", "password"),
        }

        # Ljubljana area - 3x3 grid of 5x5 km cells centered at 46.0569, 14.5058
        self.roi_bbox = (
            "ST_MakeEnvelope(14.4346,45.9856,14.5770,46.1282,4326)::GEOGRAPHY"
        )

        # Paths
        self.geotiff_pattern = "data/images/geotiffs/sentinel2_rgb_*.tif"
        self.mask_path = "data/images/masks/plus_sign_mask.tif"

        # Grid configuration (matching the download script)
        self.lat_centre = 46.0569
        self.lon_centre = 14.5058
        self.cell_size_km = 5
        self.dx_deg_cell = (
            self.cell_size_km * 1000 / (111_000 * np.cos(np.radians(self.lat_centre)))
        )
        self.dy_deg_cell = self.cell_size_km * 1000 / 111_000

    def wait_for_database(self) -> None:
        """Wait until database is ready for connections"""
        print("Waiting for database to become ready", end="")

        max_attempts = 30
        attempt = 0

        while attempt < max_attempts:
            try:
                conn = psycopg2.connect(**self.db_config)
                conn.close()
                print(" done.")
                return
            except psycopg2.OperationalError as e:
                print(".", end="", flush=True)
                attempt += 1
                if attempt < max_attempts:
                    import time

                    time.sleep(2)
                else:
                    # Show the last error for debugging
                    print(
                        f"\n❌ Database connection failed after {max_attempts} attempts"
                    )
                    print(f"Last error: {str(e)}")
                    print(f"Connection config: {self.db_config}")
                    print("Ensure the database container is running and accessible")

        raise RuntimeError("Database did not become ready within expected time")

    def get_grid_bbox_from_filename(self, filename: str) -> str:
        """
        Extract grid coordinates from filename and calculate bounding box

        Args:
            filename: e.g., "sentinel2_rgb_grid_0_1_20180605_100610.tif"

        Returns:
            PostGIS geography polygon string
        """
        # Extract grid coordinates from filename
        match = re.search(r"grid_(\d+)_(\d+)", filename)
        if not match:
            raise ValueError(
                f"Could not extract grid coordinates from filename: {filename}"
            )

        row, col = int(match.group(1)), int(match.group(2))

        # Calculate cell center (matching the download script logic)
        lat_offset = (1 - row) * self.dy_deg_cell  # row 0 is north, row 2 is south
        lon_offset = (col - 1) * self.dx_deg_cell  # col 0 is west, col 2 is east

        cell_lat = self.lat_centre + lat_offset
        cell_lon = self.lon_centre + lon_offset

        # Calculate bounding box
        min_lon = cell_lon - self.dx_deg_cell / 2
        min_lat = cell_lat - self.dy_deg_cell / 2
        max_lon = cell_lon + self.dx_deg_cell / 2
        max_lat = cell_lat + self.dy_deg_cell / 2

        return (
            f"ST_MakeEnvelope({min_lon},{min_lat},{max_lon},{max_lat},4326)::GEOGRAPHY"
        )

    def extract_timestamp_from_filename(self, filename: str) -> str:
        """
        Extract timestamp from filename and format for PostgreSQL

        Args:
            filename: e.g., "sentinel2_rgb_grid_0_0_20180605_100610.tif"

        Returns:
            PostgreSQL timestamp string: "2018-06-05 10:06:10+00"
        """
        # Extract the last two underscore-separated fields (YYYYMMDD and HHMMSS)
        basename = os.path.basename(filename)
        base_no_ext = os.path.splitext(basename)[0]
        parts = base_no_ext.split("_")

        if len(parts) < 2:
            raise ValueError(f"Could not extract timestamp from filename: {filename}")

        date_part = parts[-2]  # YYYYMMDD
        time_part = parts[-1]  # HHMMSS

        # Validate format
        if len(date_part) != 8 or len(time_part) != 6:
            raise ValueError(f"Invalid timestamp format in filename: {filename}")

        # Format as PostgreSQL timestamp
        formatted_date = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:8]}"
        formatted_time = f"{time_part[:2]}:{time_part[2:4]}:{time_part[4:6]}"

        return f"{formatted_date} {formatted_time}+00"

    def read_tiff_as_bands(self, filepath: str) -> Dict[str, bytes]:
        """
        Read a TIFF file and return its bands as separate byte arrays

        Args:
            filepath: Path to the TIFF file

        Returns:
            Dictionary mapping band names to byte data
        """
        bands_data = {}

        with rasterio.open(filepath) as src:
            # Read each band
            for i in range(1, src.count + 1):
                band_data = src.read(i)

                # Convert to bytes
                band_bytes = band_data.tobytes()

                # Map to Sentinel-2 band names (assuming RGB = B04, B03, B02)
                if i == 1:  # Red
                    bands_data["b04"] = band_bytes
                elif i == 2:  # Green
                    bands_data["b03"] = band_bytes
                elif i == 3:  # Blue
                    bands_data["b02"] = band_bytes
                else:
                    # For additional bands, use generic naming
                    band_name = f"b{i:02d}"
                    bands_data[band_name] = band_bytes

        return bands_data

    def read_mask_file(self) -> bytes:
        """
        Read the mask file and return as bytes

        Returns:
            Mask data as bytes
        """
        if not os.path.exists(self.mask_path):
            raise FileNotFoundError(f"Mask file not found: {self.mask_path}")

        with rasterio.open(self.mask_path) as src:
            mask_data = src.read(1)  # Assuming single band mask
            return mask_data.tobytes()

    def create_change_mask(
        self, img_a_path: str, img_b_path: str, grid_bbox: str
    ) -> bytes:
        """
        Create a synthetic change detection mask for two images

        Args:
            img_a_path: Path to first image
            img_b_path: Path to second image
            grid_bbox: Bounding box string for the grid cell

        Returns:
            Change mask as bytes
        """
        # For demonstration, create a simple change mask based on time difference
        # In a real scenario, this would involve actual change detection algorithms

        # Extract timestamps to determine change pattern
        time_a = self.extract_timestamp_from_filename(img_a_path)
        time_b = self.extract_timestamp_from_filename(img_b_path)

        # Parse years to create different change patterns
        year_a = int(time_a[:4])
        year_b = int(time_b[:4])
        year_diff = abs(year_b - year_a)

        # Create different mask patterns based on time difference
        if year_diff <= 1:
            # Small changes - sparse pattern
            mask = self._create_sparse_change_mask()
        elif year_diff <= 3:
            # Medium changes - moderate pattern
            mask = self._create_moderate_change_mask()
        else:
            # Large changes - dense pattern
            mask = self._create_dense_change_mask()

        return mask.tobytes()

    def _create_sparse_change_mask(self, width=512, height=512) -> np.ndarray:
        """Create sparse change pattern (10-20% change)"""
        mask = np.zeros((height, width), dtype=np.uint8)

        # Add some random scattered changes
        num_changes = int(0.15 * width * height)
        change_indices = np.random.choice(width * height, num_changes, replace=False)
        flat_mask = mask.flatten()
        flat_mask[change_indices] = 255

        return flat_mask.reshape((height, width))

    def _create_moderate_change_mask(self, width=512, height=512) -> np.ndarray:
        """Create moderate change pattern (20-40% change)"""
        mask = np.zeros((height, width), dtype=np.uint8)

        # Add some clustered changes
        num_clusters = 5
        for _ in range(num_clusters):
            center_x = np.random.randint(50, width - 50)
            center_y = np.random.randint(50, height - 50)
            radius = np.random.randint(20, 60)

            y, x = np.ogrid[:height, :width]
            distance = np.sqrt((x - center_x) ** 2 + (y - center_y) ** 2)
            mask[distance <= radius] = 255

        return mask

    def _create_dense_change_mask(self, width=512, height=512) -> np.ndarray:
        """Create dense change pattern (40-60% change)"""
        mask = np.zeros((height, width), dtype=np.uint8)

        # Create larger connected change areas
        # Add horizontal stripes
        stripe_width = 40
        for i in range(0, height, stripe_width * 2):
            mask[i : i + stripe_width, :] = 255

        # Add some vertical elements
        for j in range(0, width, 80):
            mask[:, j : j + 20] = 255

        return mask

    def insert_image_record(self, cursor, filepath: str) -> int:
        """
        Insert a single image record into the database

        Args:
            cursor: Database cursor
            filepath: Path to the TIFF file

        Returns:
            The ID of the inserted record
        """
        try:
            # Extract metadata from filename
            timestamp = self.extract_timestamp_from_filename(filepath)
            bbox = self.get_grid_bbox_from_filename(filepath)

            # Read band data
            bands_data = self.read_tiff_as_bands(filepath)

            # Prepare SQL statement
            # Note: We're only using the bands that exist in our RGB files (b02, b03, b04)
            sql = """
                INSERT INTO eo (time, bbox, b02, b03, b04)
                VALUES (%s, {}, %s, %s, %s)
                RETURNING id
            """.format(
                bbox
            )

            # Execute insert
            cursor.execute(
                sql,
                (
                    timestamp,
                    bands_data.get("b02"),  # Blue
                    bands_data.get("b03"),  # Green
                    bands_data.get("b04"),  # Red
                ),
            )

            # Get the inserted record ID
            record_id = cursor.fetchone()[0]

            print(
                f"✓ Inserted: {os.path.basename(filepath)} -> {timestamp} (ID: {record_id})"
            )
            return record_id

        except Exception as e:
            print(f"✗ Failed to insert {filepath}: {str(e)}")
            raise

    def group_files_by_grid(
        self, tiff_files: List[str]
    ) -> Dict[Tuple[int, int], List[str]]:
        """
        Group TIFF files by their grid coordinates

        Args:
            tiff_files: List of TIFF file paths

        Returns:
            Dictionary mapping (row, col) tuples to lists of file paths
        """
        grid_groups = {}

        for filepath in tiff_files:
            # Extract grid coordinates from filename
            match = re.search(r"grid_(\d+)_(\d+)", filepath)
            if match:
                row, col = int(match.group(1)), int(match.group(2))
                grid_key = (row, col)

                if grid_key not in grid_groups:
                    grid_groups[grid_key] = []
                grid_groups[grid_key].append(filepath)

        # Sort files in each group by timestamp
        for grid_key in grid_groups:
            grid_groups[grid_key].sort()

        return grid_groups

    def insert_change_mask(
        self,
        cursor,
        img_a_id: int,
        img_b_id: int,
        img_a_path: str,
        img_b_path: str,
        grid_bbox: str,
    ) -> None:
        """
        Insert a change detection mask for a pair of images

        Args:
            cursor: Database cursor
            img_a_id: ID of first image
            img_b_id: ID of second image
            img_a_path: Path to first image file
            img_b_path: Path to second image file
            grid_bbox: Bounding box for the grid cell
        """
        try:
            # Extract timestamps
            time_a = self.extract_timestamp_from_filename(img_a_path)
            time_b = self.extract_timestamp_from_filename(img_b_path)

            # Ensure img_a is earlier than img_b
            if time_a > time_b:
                img_a_id, img_b_id = img_b_id, img_a_id
                time_a, time_b = time_b, time_a
                img_a_path, img_b_path = img_b_path, img_a_path

            # Create change mask
            mask_data = self.create_change_mask(img_a_path, img_b_path, grid_bbox)

            # Insert into eo_change table
            sql = """
                INSERT INTO eo_change (img_a_id, img_b_id, period_start, period_end, bbox, mask)
                VALUES (%s, %s, %s, %s, {}, %s)
            """.format(
                grid_bbox
            )

            cursor.execute(sql, (img_a_id, img_b_id, time_a, time_b, mask_data))

            print(
                f"✓ Inserted change mask: {os.path.basename(img_a_path)} -> {os.path.basename(img_b_path)}"
            )

        except Exception as e:
            print(
                f"✗ Failed to insert change mask for {img_a_path} -> {img_b_path}: {str(e)}"
            )
            raise

    def populate_database(self) -> None:
        """
        Main method to populate the database with all TIFF files and their change masks
        """
        print("Starting database population...")

        # Wait for database to be ready
        self.wait_for_database()

        # Find all TIFF files
        tiff_files = glob.glob(self.geotiff_pattern)
        if not tiff_files:
            raise FileNotFoundError(
                f"No TIFF files found matching pattern: {self.geotiff_pattern}"
            )

        print(f"Found {len(tiff_files)} TIFF files to process")

        # Group files by grid coordinates
        grid_groups = self.group_files_by_grid(tiff_files)
        print(f"Found {len(grid_groups)} grid cells with data")

        # Connect to database
        conn = psycopg2.connect(**self.db_config)
        cursor = conn.cursor()

        try:
            # Clear existing data (optional - comment out if you want to keep existing data)
            print("Clearing existing data...")
            cursor.execute("DELETE FROM eo_change")
            cursor.execute("DELETE FROM eo")
            conn.commit()

            # Phase 1: Insert all images and collect their IDs
            print("\n=== Phase 1: Inserting images ===")
            image_records = {}  # filepath -> (id, grid_key)
            successful_inserts = 0

            for grid_key, files in grid_groups.items():
                print(
                    f"\nProcessing grid {grid_key[0]}_{grid_key[1]} ({len(files)} files)..."
                )

                for tiff_file in files:
                    try:
                        record_id = self.insert_image_record(cursor, tiff_file)
                        image_records[tiff_file] = (record_id, grid_key)
                        successful_inserts += 1
                        conn.commit()  # Commit after each successful insert
                    except Exception as e:
                        print(f"Error processing {tiff_file}: {str(e)}")
                        conn.rollback()  # Rollback failed transaction
                        continue

            print(
                f"\n✅ Successfully inserted {successful_inserts}/{len(tiff_files)} image records"
            )

            # Phase 2: Create change masks for image pairs within each grid cell
            print("\n=== Phase 2: Creating change masks ===")
            change_mask_count = 0

            for grid_key, files in grid_groups.items():
                print(
                    f"\nCreating change masks for grid {grid_key[0]}_{grid_key[1]}..."
                )

                # Get successfully inserted files for this grid
                valid_files = [f for f in files if f in image_records]
                if len(valid_files) < 2:
                    print(
                        f"Skipping grid {grid_key[0]}_{grid_key[1]} - insufficient images ({len(valid_files)})"
                    )
                    continue

                # Generate change masks for consecutive time periods
                valid_files.sort()  # Ensure chronological order

                for i in range(len(valid_files) - 1):
                    img_a_path = valid_files[i]
                    img_b_path = valid_files[i + 1]

                    img_a_id = image_records[img_a_path][0]
                    img_b_id = image_records[img_b_path][0]

                    # Get bounding box for this grid
                    grid_bbox = self.get_grid_bbox_from_filename(img_a_path)

                    try:
                        self.insert_change_mask(
                            cursor,
                            img_a_id,
                            img_b_id,
                            img_a_path,
                            img_b_path,
                            grid_bbox,
                        )
                        change_mask_count += 1
                        conn.commit()
                    except Exception as e:
                        print(f"Error creating change mask: {str(e)}")
                        conn.rollback()
                        continue

            print(f"\n✅ Successfully created {change_mask_count} change masks")

            # Show summary
            cursor.execute("SELECT COUNT(*) FROM eo")
            total_images = cursor.fetchone()[0]
            print(f"Database now contains {total_images} image records")

            cursor.execute("SELECT COUNT(*) FROM eo_change")
            total_changes = cursor.fetchone()[0]
            print(f"Database now contains {total_changes} change mask records")

            cursor.execute("SELECT MIN(time), MAX(time) FROM eo")
            min_time, max_time = cursor.fetchone()
            print(f"Image time range: {min_time} to {max_time}")

            if total_changes > 0:
                cursor.execute(
                    "SELECT MIN(period_start), MAX(period_end) FROM eo_change"
                )
                min_change_time, max_change_time = cursor.fetchone()
                print(
                    f"Change detection time range: {min_change_time} to {max_change_time}"
                )

        except Exception as e:
            print(f"Database operation failed: {str(e)}")
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    def print_configuration(self) -> None:
        """Print current configuration"""
        print("Database Population Configuration:")
        print(f"  DB_HOST: {self.db_config['host']}")
        print(f"  DB_PORT: {self.db_config['port']}")
        print(f"  DB_NAME: {self.db_config['database']}")
        print(f"  DB_USER: {self.db_config['user']}")
        print(f"  GeoTIFF Pattern: {self.geotiff_pattern}")
        print(f"  Mask File: {self.mask_path}")
        print(f"  Grid Center: {self.lat_centre}°N, {self.lon_centre}°E")
        print(f"  Cell Size: {self.cell_size_km} km")
        print()

    def test_connection(self) -> bool:
        """Test database connection and return True if successful"""
        try:
            print("Testing database connection...")
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            cursor.execute("SELECT version()")
            version = cursor.fetchone()[0]
            print(f"✅ Connected successfully! Database version: {version}")
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            print(f"❌ Connection test failed: {str(e)}")
            return False


def main():
    """Main function"""
    try:
        populator = DatabasePopulator()
        populator.print_configuration()

        # Test connection first
        if not populator.test_connection():
            print("❌ Database connection failed. Please check your configuration.")
            return 1

        populator.populate_database()
        print("\n🎉 Database population completed successfully!")

    except KeyboardInterrupt:
        print("\n⚠️  Operation cancelled by user")
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
