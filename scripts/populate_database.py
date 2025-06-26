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
from typing import Optional, Dict, Any
import psycopg2
import psycopg2.extras
import rasterio
import numpy as np
from dotenv import load_dotenv


class DatabasePopulator:
    def __init__(self):
        # Load environment variables
        load_dotenv()

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

    def insert_image_record(self, cursor, filepath: str, mask_data: bytes) -> None:
        """
        Insert a single image record into the database

        Args:
            cursor: Database cursor
            filepath: Path to the TIFF file
            mask_data: Mask data as bytes
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

            print(f"✓ Inserted: {os.path.basename(filepath)} -> {timestamp}")

        except Exception as e:
            print(f"✗ Failed to insert {filepath}: {str(e)}")
            raise

    def populate_database(self) -> None:
        """
        Main method to populate the database with all TIFF files
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

        # Read mask data once (same for all images)
        print(f"Reading mask file: {self.mask_path}")
        mask_data = self.read_mask_file()

        # Connect to database
        conn = psycopg2.connect(**self.db_config)
        cursor = conn.cursor()

        try:
            # Clear existing data (optional - comment out if you want to keep existing data)
            print("Clearing existing data...")
            cursor.execute("DELETE FROM eo")
            conn.commit()

            # Process each TIFF file
            successful_inserts = 0
            for tiff_file in sorted(tiff_files):
                try:
                    self.insert_image_record(cursor, tiff_file, mask_data)
                    successful_inserts += 1
                    conn.commit()  # Commit after each successful insert
                except Exception as e:
                    print(f"Error processing {tiff_file}: {str(e)}")
                    conn.rollback()  # Rollback failed transaction
                    continue

            print(
                f"\n✅ Successfully inserted {successful_inserts}/{len(tiff_files)} records"
            )

            # Show summary
            cursor.execute("SELECT COUNT(*) FROM eo")
            total_count = cursor.fetchone()[0]
            print(f"Database now contains {total_count} total records")

            cursor.execute("SELECT MIN(time), MAX(time) FROM eo")
            min_time, max_time = cursor.fetchone()
            print(f"Time range: {min_time} to {max_time}")

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
