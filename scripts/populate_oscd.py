#!/usr/bin/env python3
"""
populate_database_oscd.py - Bulk-load OSCD (Onera Satellite Change Detection) dataset into TimescaleDB

This script populates the TimescaleDB database with images from the OSCD dataset,
combining all spectral bands into single records and pairing with change detection masks.

Dataset structure:
- Images: data/oscd/Onera Satellite Change Detection dataset - Images/{city}/
  - imgs_1/ (first timestamp with all bands B01-B12, B8A)
  - imgs_2/ (second timestamp with all bands B01-B12, B8A)
  - dates.txt (contains the two timestamps)
  - {city}.geojson (contains the bounding box)
- Masks: data/oscd/Onera Satellite Change Detection dataset - Masks/{city}/cm/{city}-cm.tif

Requires:
    pip install psycopg2-binary rasterio numpy python-dotenv
"""

import os
import glob
import json
import re
import time
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from pathlib import Path

import psycopg2
import rasterio
import numpy as np
import cv2
from dotenv import load_dotenv


class OSCDDatabasePopulator:
    def __init__(self):
        # Load environment variables
        load_dotenv()

        # Configuration
        self.db_config = {
            "host": "localhost",
            "port": int(os.getenv("DB_PORT", "5432")),
            "database": os.getenv("DB_NAME", "eo_db"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", "password"),
        }

        # Paths
        self.images_base_path = (
            "data/oscd/Onera Satellite Change Detection dataset - Images"
        )
        self.masks_base_path = (
            "data/oscd/Onera Satellite Change Detection dataset - Masks"
        )

        # Band mapping for Sentinel-2
        self.band_names = [
            "b01",
            "b02",
            "b03",
            "b04",
            "b05",
            "b06",
            "b07",
            "b08",
            "b8a",
            "b09",
            "b10",
            "b11",
            "b12",
        ]

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
            except psycopg2.OperationalError:
                print(".", end="", flush=True)
                attempt += 1
                if attempt < max_attempts:
                    time.sleep(2)
                else:
                    break

        raise RuntimeError("Database did not become ready within expected time")

    def get_available_cities(self) -> List[str]:
        """Get list of available cities in the dataset"""
        cities = []
        images_path = Path(self.images_base_path)

        if not images_path.exists():
            raise FileNotFoundError(
                f"Images base path not found: {self.images_base_path}"
            )

        for city_dir in images_path.iterdir():
            if city_dir.is_dir() and not city_dir.name.startswith("."):
                # Check if it has the required structure
                if (city_dir / "imgs_1").exists() and (city_dir / "imgs_2").exists():
                    cities.append(city_dir.name)

        return sorted(cities)

    def read_dates_file(self, city: str) -> Tuple[str, str]:
        """
        Read the dates.txt file for a city and return parsed timestamps

        Args:
            city: City name

        Returns:
            Tuple of (timestamp1, timestamp2) in PostgreSQL format
        """
        dates_file = Path(self.images_base_path) / city / "dates.txt"

        if not dates_file.exists():
            raise FileNotFoundError(f"dates.txt not found for city: {city}")

        with open(dates_file, "r") as f:
            content = f.read().strip()

        # Parse dates (format: date_1: YYYYMMDD, date_2: YYYYMMDD)
        date1_match = re.search(r"date_1:\s*(\d{8})", content)
        date2_match = re.search(r"date_2:\s*(\d{8})", content)

        if not date1_match or not date2_match:
            raise ValueError(f"Could not parse dates from {dates_file}")

        date1_str = date1_match.group(1)
        date2_str = date2_match.group(1)

        # Convert to PostgreSQL timestamp format (assuming noon UTC)
        timestamp1 = f"{date1_str[:4]}-{date1_str[4:6]}-{date1_str[6:8]} 12:00:00+00"
        timestamp2 = f"{date2_str[:4]}-{date2_str[4:6]}-{date2_str[6:8]} 12:00:00+00"

        return timestamp1, timestamp2

    def read_geojson_bbox(self, city: str) -> str:
        """
        Read the GeoJSON file for a city and return PostGIS polygon string

        Args:
            city: City name

        Returns:
            PostGIS geography polygon string
        """
        geojson_file = Path(self.images_base_path) / city / f"{city}.geojson"

        if not geojson_file.exists():
            raise FileNotFoundError(f"GeoJSON file not found for city: {city}")

        with open(geojson_file, "r") as f:
            geojson_data = json.load(f)

        # Extract coordinates from the first feature
        if (
            geojson_data.get("type") == "FeatureCollection"
            and geojson_data.get("features")
            and geojson_data["features"][0].get("geometry", {}).get("type") == "Polygon"
        ):

            coordinates = geojson_data["features"][0]["geometry"]["coordinates"][0]

            # Convert to PostGIS format (assuming coordinates are [lon, lat])
            coord_pairs = [f"{lon} {lat}" for lon, lat in coordinates]
            polygon_wkt = f"POLYGON(({','.join(coord_pairs)}))"

            return f"ST_GeogFromText('{polygon_wkt}')"

        raise ValueError(f"Invalid GeoJSON format in {geojson_file}")

    def get_band_files(self, city: str, img_dir: str) -> Dict[str, str]:
        """
        Get all band files for a specific image directory

        Args:
            city: City name
            img_dir: Either 'imgs_1' or 'imgs_2'

        Returns:
            Dictionary mapping band names to file paths
        """
        img_path = Path(self.images_base_path) / city / img_dir

        if not img_path.exists():
            raise FileNotFoundError(f"Image directory not found: {img_path}")

        band_files = {}

        for band in self.band_names:
            # Find files with this band identifier
            pattern = f"*{band.upper()}.tif"
            matching_files = list(img_path.glob(pattern))

            if matching_files:
                band_files[band] = str(matching_files[0])
            else:
                print(f"Warning: Band {band} not found in {img_path}")

        return band_files

    def read_band_as_bytes(self, file_path: str) -> Tuple[bytes, Dict[str, Any]]:
        """
        Read a single band TIFF file and return as bytes with metadata

        Args:
            file_path: Path to the band file

        Returns:
            Tuple of (band data as bytes, metadata dictionary)
        """
        with rasterio.open(file_path) as src:
            band_data = src.read(1)  # Read first (and only) band

            metadata = {
                "width": src.width,
                "height": src.height,
                "data_type": str(src.dtypes[0]),
            }

            return band_data.tobytes(), metadata

    def read_change_mask(self, city: str) -> Tuple[bytes, Dict[str, Any]]:
        """
        Read the change detection mask for a city

        Args:
            city: City name

        Returns:
            Tuple of (mask data as bytes, metadata dictionary)
        """
        # Use TIF file instead of PNG - TIF contains actual change detection data
        mask_file = Path(self.masks_base_path) / city / "cm" / f"{city}-cm.tif"

        with rasterio.open(mask_file) as src:
            mask_data = src.read(1)  # Read first (and only) band

            metadata = {
                "width": src.width,
                "height": src.height,
                "data_type": str(src.dtypes[0]),
            }
            mask_visualization = np.where(mask_data == 2, 255, 0).astype(np.uint8)

            # Save a proper PNG visualization for debugging (convert to 0-255 range)
            if city == "saclay_e":
                # print the min and max values of the mask data
                print(f"Mask data min: {np.min(mask_data)}, max: {np.max(mask_data)}")
                # Create proper visualization: map class 1->0 (no change), class 2->255 (change)

                mask_image_path = (
                    Path(self.masks_base_path) / city / "cm" / f"cm-database.png"
                )

                # Save as proper PNG using cv2
                cv2.imwrite(str(mask_image_path), mask_visualization)
                print(f"✓ Saved change mask visualization: {mask_image_path}")
                print(
                    f"✓ Original mask values: {np.unique(mask_data)} -> Visualization: {np.unique(mask_visualization)}"
                )

            return mask_visualization.tobytes(), metadata

    def insert_image_record(
        self, cursor, city: str, timestamp: str, bbox: str, img_dir: str
    ) -> int:
        """
        Insert a single image record combining all bands

        Args:
            cursor: Database cursor
            city: City name
            timestamp: PostgreSQL timestamp string
            bbox: PostGIS geography polygon string
            img_dir: Either 'imgs_1' or 'imgs_2'

        Returns:
            The ID of the inserted record
        """
        try:
            # Get all band files
            band_files = self.get_band_files(city, img_dir)

            if not band_files:
                raise ValueError(f"No band files found for {city}/{img_dir}")

            # Read all bands and collect metadata to find maximum dimensions
            band_data = {}
            max_width = 0
            max_height = 0
            data_type = None

            for band_name, file_path in band_files.items():
                band_bytes, band_metadata = self.read_band_as_bytes(file_path)
                band_data[band_name] = band_bytes

                # Track maximum dimensions across all bands
                max_width = max(max_width, band_metadata["width"])
                max_height = max(max_height, band_metadata["height"])

                # Use data type from first band (assuming all bands have compatible types)
                if data_type is None:
                    data_type = band_metadata["data_type"]

            # Create combined metadata with maximum dimensions
            metadata = {
                "width": max_width,
                "height": max_height,
                "data_type": data_type,
            }

            # Prepare SQL with dynamic band columns
            band_columns = ", ".join(self.band_names)
            band_placeholders = ", ".join(["%s"] * len(self.band_names))

            sql = f"""
                INSERT INTO eo (time, bbox, width, height, data_type, {band_columns})
                VALUES (%s, {bbox}, %s, %s, %s, {band_placeholders})
                RETURNING id
            """

            # Prepare band values in correct order
            band_values = [band_data.get(band) for band in self.band_names]

            # Execute insert
            cursor.execute(
                sql,
                [
                    timestamp,
                    metadata["width"],
                    metadata["height"],
                    metadata["data_type"],
                ]
                + band_values,
            )

            # Get the inserted record ID
            record_id = cursor.fetchone()[0]

            print(
                f"✓ Inserted: {city}/{img_dir} -> {timestamp} ({metadata['width']}x{metadata['height']}, {metadata['data_type']}) (ID: {record_id})"
            )
            return record_id

        except Exception as e:
            print(f"✗ Failed to insert {city}/{img_dir}: {str(e)}")
            raise

    def insert_change_mask(
        self,
        cursor,
        city: str,
        img_a_id: int,
        img_b_id: int,
        timestamp1: str,
        timestamp2: str,
        bbox: str,
    ) -> None:
        """
        Insert a change detection mask for a city

        Args:
            cursor: Database cursor
            city: City name
            img_a_id: ID of first image
            img_b_id: ID of second image
            timestamp1: First timestamp
            timestamp2: Second timestamp
            bbox: PostGIS geography polygon string
        """
        try:
            # Ensure img_a is earlier than img_b
            if timestamp1 > timestamp2:
                img_a_id, img_b_id = img_b_id, img_a_id
                timestamp1, timestamp2 = timestamp2, timestamp1

            # Read change mask
            mask_data, mask_metadata = self.read_change_mask(city)

            # Insert into eo_change table
            sql = f"""
                INSERT INTO eo_change (img_a_id, img_b_id, period_start, period_end, bbox, width, height, data_type, mask)
                VALUES (%s, %s, %s, %s, {bbox}, %s, %s, %s, %s)
            """

            cursor.execute(
                sql,
                (
                    img_a_id,
                    img_b_id,
                    timestamp1,
                    timestamp2,
                    mask_metadata["width"],
                    mask_metadata["height"],
                    mask_metadata["data_type"],
                    mask_data,
                ),
            )

            print(
                f"✓ Inserted change mask: {city} ({mask_metadata['width']}x{mask_metadata['height']}, {mask_metadata['data_type']})"
            )

        except Exception as e:
            print(f"✗ Failed to insert change mask for {city}: {str(e)}")
            raise

    def process_city(self, cursor, city: str) -> Tuple[int, int]:
        """
        Process a single city - insert both images and change mask

        Args:
            cursor: Database cursor
            city: City name

        Returns:
            Tuple of (number of images inserted, number of change masks inserted)
        """
        print(f"\n--- Processing city: {city} ---")

        try:
            # Read timestamps and bounding box
            timestamp1, timestamp2 = self.read_dates_file(city)
            bbox = self.read_geojson_bbox(city)

            print(f"Timestamps: {timestamp1} -> {timestamp2}")

            # Insert first image (imgs_1)
            img1_id = self.insert_image_record(cursor, city, timestamp1, bbox, "imgs_1")

            # Insert second image (imgs_2)
            img2_id = self.insert_image_record(cursor, city, timestamp2, bbox, "imgs_2")

            # Insert change mask
            self.insert_change_mask(
                cursor, city, img1_id, img2_id, timestamp1, timestamp2, bbox
            )

            return 2, 1  # 2 images, 1 change mask

        except Exception as e:
            print(f"✗ Failed to process city {city}: {str(e)}")
            return 0, 0

    def populate_database(self) -> None:
        """
        Main method to populate the database with all OSCD cities
        """
        print("Starting OSCD database population...")

        # Wait for database to be ready
        self.wait_for_database()

        # Get available cities
        cities = self.get_available_cities()
        if not cities:
            raise ValueError("No cities found in the dataset")

        print(f"Found {len(cities)} cities to process: {', '.join(cities)}")

        # Connect to database
        conn = psycopg2.connect(**self.db_config)
        cursor = conn.cursor()

        try:
            # Clear existing data (optional - comment out if you want to keep existing data)
            print("Clearing existing data...")
            cursor.execute("DELETE FROM eo_change")
            cursor.execute("DELETE FROM eo")
            conn.commit()

            # Process all cities
            total_images = 0
            total_change_masks = 0
            successful_cities = 0

            for city in cities:
                try:
                    images_count, masks_count = self.process_city(cursor, city)
                    total_images += images_count
                    total_change_masks += masks_count
                    if images_count > 0:
                        successful_cities += 1
                    conn.commit()  # Commit after each city
                except Exception as e:
                    print(f"Failed to process {city}: {str(e)}")
                    conn.rollback()
                    continue

            # Show summary
            print(f"\n=== SUMMARY ===")
            print(f"✅ Successfully processed {successful_cities}/{len(cities)} cities")
            print(f"✅ Total images inserted: {total_images}")
            print(f"✅ Total change masks inserted: {total_change_masks}")

            # Database statistics
            cursor.execute("SELECT COUNT(*) FROM eo")
            db_images = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM eo_change")
            db_changes = cursor.fetchone()[0]

            print(f"Database now contains {db_images} image records")
            print(f"Database now contains {db_changes} change mask records")

            if db_images > 0:
                cursor.execute("SELECT MIN(time), MAX(time) FROM eo")
                min_time, max_time = cursor.fetchone()
                print(f"Image time range: {min_time} to {max_time}")

        except Exception as e:
            print(f"Database operation failed: {str(e)}")
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    def print_configuration(self) -> None:
        """Print current configuration"""
        print("OSCD Database Population Configuration:")
        print(f"  DB_HOST: {self.db_config['host']}")
        print(f"  DB_PORT: {self.db_config['port']}")
        print(f"  DB_NAME: {self.db_config['database']}")
        print(f"  DB_USER: {self.db_config['user']}")
        print(f"  Images Path: {self.images_base_path}")
        print(f"  Masks Path: {self.masks_base_path}")
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
        populator = OSCDDatabasePopulator()
        populator.print_configuration()

        # Test connection first
        if not populator.test_connection():
            print("❌ Database connection failed. Please check your configuration.")
            return 1

        populator.populate_database()
        print("\n🎉 OSCD database population completed successfully!")

    except KeyboardInterrupt:
        print("\n⚠️  Operation cancelled by user")
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
