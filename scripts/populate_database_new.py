#!/usr/bin/env python3
"""
populate_database.py - Bulk-load Sentinel-2 RGB GeoTIFFs into TimescaleDB with Slovenia Grid

This script populates the TimescaleDB database with Sentinel-2 images, mapping them to
predefined Slovenia grid cells and using the new schema with grid partitioning.

Requires:
    pip install psycopg2-binary rasterio numpy python-dotenv geopandas shapely
"""
import os
import re
import glob
import time
import psycopg2
import rasterio
import numpy as np
import geopandas as gpd
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple
from itertools import combinations
from rasterio.transform import from_bounds
from dotenv import load_dotenv
from shapely.geometry import box, Polygon
from rasterio.warp import calculate_default_transform, reproject, Resampling
from rasterio.mask import mask


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

        # Paths
        self.geotiff_pattern = "data/images/geotiffs/sentinel2_rgb_*.tif"
        self.grid_gpkg_path = "data/slovenia_grid.gpkg"  # Slovenia grid file
        self.mask_path = "data/images/masks/plus_sign_mask.tif"

        # Load Slovenia grid
        self.slovenia_grid = None
        self.load_slovenia_grid()

    def load_slovenia_grid(self) -> None:
        """Load the Slovenia grid from GeoPackage"""
        if not os.path.exists(self.grid_gpkg_path):
            raise FileNotFoundError(
                f"Slovenia grid file not found: {self.grid_gpkg_path}"
            )

        print(f"Loading Slovenia grid from {self.grid_gpkg_path}")
        self.slovenia_grid = gpd.read_file(self.grid_gpkg_path)

        # Ensure we have the required columns
        required_cols = ["index", "geometry"]
        if not all(col in self.slovenia_grid.columns for col in required_cols):
            raise ValueError(f"Grid file must contain columns: {required_cols}")

        # Convert to WGS84 for spatial operations
        if self.slovenia_grid.crs != "EPSG:4326":
            self.slovenia_grid = self.slovenia_grid.to_crs("EPSG:4326")

        print(f"Loaded {len(self.slovenia_grid)} grid cells from Slovenia grid")

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
                    time.sleep(2)
                else:
                    print(f" failed after {max_attempts} attempts.")

        raise RuntimeError("Database did not become ready within expected time")

    def find_grid_cell_for_image(self, image_bbox: Polygon) -> Optional[int]:
        """
        Find the Slovenia grid cell that contains or best overlaps with the image

        Args:
            image_bbox: Shapely polygon representing image bounds in WGS84

        Returns:
            grid_id (index) of the matching cell, or None if no suitable cell found
        """
        # Find cells that intersect with the image
        intersecting = self.slovenia_grid[
            self.slovenia_grid.geometry.intersects(image_bbox)
        ]

        if len(intersecting) == 0:
            return None

        if len(intersecting) == 1:
            return int(intersecting.iloc[0]["index"])

        # If multiple cells intersect, find the one with maximum overlap
        max_overlap_area = 0
        best_grid_id = None

        for idx, row in intersecting.iterrows():
            overlap = image_bbox.intersection(row.geometry)
            overlap_area = overlap.area

            if overlap_area > max_overlap_area:
                max_overlap_area = overlap_area
                best_grid_id = int(row["index"])

        return best_grid_id

    def get_image_bbox_from_tiff(self, filepath: str) -> Polygon:
        """
        Extract bounding box from TIFF file

        Args:
            filepath: Path to the TIFF file

        Returns:
            Shapely polygon in WGS84 coordinates
        """
        with rasterio.open(filepath) as src:
            # Get bounds in original CRS
            bounds = src.bounds

            # Convert to WGS84 if needed
            if src.crs != rasterio.crs.CRS.from_epsg(4326):
                # Transform bounds to WGS84
                from rasterio.warp import transform_bounds

                bounds = transform_bounds(
                    src.crs, rasterio.crs.CRS.from_epsg(4326), *bounds
                )

            # Create polygon from bounds (minx, miny, maxx, maxy)
            return box(bounds[0], bounds[1], bounds[2], bounds[3])

    def crop_image_to_grid_cell(
        self, filepath: str, grid_id: int
    ) -> Tuple[Dict[str, bytes], Dict[str, Any], str]:
        """
        Crop image to the specified grid cell and return band data

        Args:
            filepath: Path to the TIFF file
            grid_id: Grid cell ID to crop to

        Returns:
            Tuple of (bands_data dict, metadata dict, bbox_wkt)
        """
        # Get grid cell geometry
        grid_cell = self.slovenia_grid[self.slovenia_grid["index"] == grid_id]
        if len(grid_cell) == 0:
            raise ValueError(f"Grid cell {grid_id} not found")

        grid_geom = grid_cell.geometry.iloc[0]

        with rasterio.open(filepath) as src:
            # Convert grid geometry to image CRS if needed
            if src.crs != rasterio.crs.CRS.from_epsg(4326):
                import geopandas as gpd

                grid_gdf = gpd.GeoDataFrame([1], geometry=[grid_geom], crs="EPSG:4326")
                grid_gdf = grid_gdf.to_crs(src.crs)
                crop_geom = grid_gdf.geometry.iloc[0]
            else:
                crop_geom = grid_geom

            # Crop the image to grid cell
            try:
                # Use mask with crop=True to extract only the grid cell area
                cropped_data, cropped_transform = mask(
                    src, [crop_geom], crop=True, filled=False, all_touched=True
                )

                # Check if the cropped data is valid
                if cropped_data.size == 0:
                    raise ValueError("No data found in grid cell after cropping")

                # Extract metadata
                metadata = {
                    "width": cropped_data.shape[2],
                    "height": cropped_data.shape[1],
                    "data_type": str(cropped_data.dtype),
                }

                # Extract bands
                bands_data = {}
                for i in range(cropped_data.shape[0]):
                    band_data = cropped_data[i]
                    # Handle masked arrays by filling with nodata value
                    if hasattr(band_data, "filled"):
                        band_data = band_data.filled(0)  # Fill masked values with 0
                    band_bytes = band_data.tobytes()

                    # Map to Sentinel-2 band names (assuming RGB = B04, B03, B02)
                    if i == 0:
                        bands_data["b04"] = band_bytes  # Red
                    elif i == 1:
                        bands_data["b03"] = band_bytes  # Green
                    elif i == 2:
                        bands_data["b02"] = band_bytes  # Blue
                    else:
                        bands_data[f"b{i+1:02d}"] = band_bytes

                # Create bbox WKT using the EXACT grid cell bounds (not the cropped image bounds)
                # This ensures the bbox exactly matches the grid cell in the database
                # Convert to high precision WKT to ensure exact match
                bbox_wkt = f"SRID=4326;{grid_geom.wkt}"

                return bands_data, metadata, bbox_wkt

            except Exception as e:
                raise ValueError(
                    f"Failed to crop image {filepath} to grid cell {grid_id}: {str(e)}"
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

    def create_change_mask(
        self, img_a_path: str, img_b_path: str, grid_id: int
    ) -> Tuple[bytes, Dict[str, Any]]:
        """
        Create a synthetic change detection mask for two images in the same grid cell

        Args:
            img_a_path: Path to first image
            img_b_path: Path to second image
            grid_id: Grid cell ID

        Returns:
            Tuple of (change mask as bytes, metadata dictionary)
        """
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

        # Create metadata for the mask
        metadata = {
            "width": mask.shape[1],
            "height": mask.shape[0],
            "data_type": str(mask.dtype),
        }

        return mask.tobytes(), metadata

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

    def insert_image_record(self, cursor, filepath: str) -> Optional[int]:
        """
        Insert a single image record into the database using upsert_eo function

        Args:
            cursor: Database cursor
            filepath: Path to the TIFF file

        Returns:
            The ID of the inserted record, or None if failed
        """
        try:
            # Extract timestamp from filename
            timestamp = self.extract_timestamp_from_filename(filepath)

            # Get image bounding box
            image_bbox = self.get_image_bbox_from_tiff(filepath)

            # Find matching grid cell
            grid_id = self.find_grid_cell_for_image(image_bbox)
            if grid_id is None:
                print(
                    f"⚠ No matching grid cell found for {os.path.basename(filepath)}, skipping"
                )
                return None

            # Crop image to grid cell and get band data
            bands_data, metadata, bbox_wkt = self.crop_image_to_grid_cell(
                filepath, grid_id
            )

            # Use the upsert_eo function
            sql = """
                SELECT upsert_eo(
                    %s::timestamptz,  -- p_time
                    %s,               -- p_grid_id
                    %s::geography,    -- p_bbox
                    %s,               -- p_width
                    %s,               -- p_height
                    %s,               -- p_data_type
                    %s,               -- p_b01
                    %s,               -- p_b02
                    %s,               -- p_b03
                    %s,               -- p_b04
                    %s,               -- p_b05
                    %s,               -- p_b06
                    %s,               -- p_b07
                    %s,               -- p_b08
                    %s,               -- p_b8a
                    %s,               -- p_b09
                    %s,               -- p_b10
                    %s,               -- p_b11
                    %s                -- p_b12
                )
            """

            # Execute upsert
            cursor.execute(
                sql,
                (
                    timestamp,
                    grid_id,
                    bbox_wkt,
                    metadata["width"],
                    metadata["height"],
                    metadata["data_type"],
                    None,  # b01
                    bands_data.get("b02"),  # b02 - Blue
                    bands_data.get("b03"),  # b03 - Green
                    bands_data.get("b04"),  # b04 - Red
                    None,  # b05
                    None,  # b06
                    None,  # b07
                    None,  # b08
                    None,  # b8a
                    None,  # b09
                    None,  # b10
                    None,  # b11
                    None,  # b12
                ),
            )

            # Get the returned record ID
            record_id = cursor.fetchone()[0]

            print(
                f"✓ Inserted: {os.path.basename(filepath)} -> grid_id={grid_id}, {timestamp} ({metadata['width']}x{metadata['height']}, {metadata['data_type']}) (ID: {record_id})"
            )
            return record_id

        except psycopg2.Error as e:
            if "duplicate key value violates unique constraint" in str(e):
                print(
                    f"⚠ Conflict for {os.path.basename(filepath)} in same grid/month - keeping latest image"
                )
                return None
            else:
                print(f"✗ Database error inserting {filepath}: {str(e)}")
                raise
        except Exception as e:
            print(f"✗ Failed to insert {filepath}: {str(e)}")
            raise

    def group_files_by_grid(
        self, successful_records: Dict[str, Tuple[int, int]]
    ) -> Dict[int, List[Tuple[str, int]]]:
        """
        Group successfully inserted files by their grid_id

        Args:
            successful_records: Dict mapping filepath to (record_id, grid_id)

        Returns:
            Dictionary mapping grid_id to lists of (filepath, record_id) tuples
        """
        grid_groups = {}

        for filepath, (record_id, grid_id) in successful_records.items():
            if grid_id not in grid_groups:
                grid_groups[grid_id] = []
            grid_groups[grid_id].append((filepath, record_id))

        # Sort files in each group by timestamp
        for grid_id in grid_groups:
            grid_groups[grid_id].sort(key=lambda x: x[0])

        return grid_groups

    def insert_change_mask(
        self,
        cursor,
        img_a_id: int,
        img_b_id: int,
        img_a_path: str,
        img_b_path: str,
        grid_id: int,
    ) -> None:
        """
        Insert a change detection mask for a pair of images in the same grid cell

        Args:
            cursor: Database cursor
            img_a_id: ID of first image
            img_b_id: ID of second image
            img_a_path: Path to first image file
            img_b_path: Path to second image file
            grid_id: Grid cell ID
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

            # Create change mask with metadata
            mask_data, mask_metadata = self.create_change_mask(
                img_a_path, img_b_path, grid_id
            )

            # Get grid cell bbox for the change mask
            grid_cell = self.slovenia_grid[self.slovenia_grid["index"] == grid_id]
            grid_geom = grid_cell.geometry.iloc[0]
            bbox_wkt = f"SRID=4326;{grid_geom.wkt}"

            # Insert into eo_change table
            sql = """
                INSERT INTO eo_change (img_a_id, img_b_id, grid_id, period_start, period_end, bbox, width, height, data_type, mask)
                VALUES (%s, %s, %s, %s, %s, %s::geography, %s, %s, %s, %s)
            """

            cursor.execute(
                sql,
                (
                    img_a_id,
                    img_b_id,
                    grid_id,
                    time_a,
                    time_b,
                    bbox_wkt,
                    mask_metadata["width"],
                    mask_metadata["height"],
                    mask_metadata["data_type"],
                    mask_data,
                ),
            )

            print(
                f"✓ Inserted change mask: {os.path.basename(img_a_path)} -> {os.path.basename(img_b_path)} (grid_id={grid_id}, {mask_metadata['width']}x{mask_metadata['height']}, {mask_metadata['data_type']})"
            )

        except Exception as e:
            print(
                f"✗ Failed to insert change mask for {img_a_path} -> {img_b_path}: {str(e)}"
            )
            raise

    def load_grid_cells_to_database(self, cursor) -> None:
        """
        Load Slovenia grid cells into the database if not already present
        """
        # Check if grid_cells table has data
        cursor.execute("SELECT COUNT(*) FROM grid_cells")
        count = cursor.fetchone()[0]

        if count > 0:
            print(
                f"Grid cells table already contains {count} records, skipping grid load"
            )
            return

        print("Loading Slovenia grid cells into database...")

        # Prepare grid data for insertion
        insert_data = []
        for idx, row in self.slovenia_grid.iterrows():
            grid_id = int(row["index"])

            # Get geometry in original CRS and WGS84
            geom_4326 = row.geometry

            # For index_x and index_y, we'll derive from the grid_id or use 0,0 if not available
            # You may need to adjust this based on your grid structure
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

        execute_values(cursor, insert_query, insert_data, template=template)

        print(f"Successfully loaded {len(insert_data)} grid cells into database")

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

        # Connect to database
        conn = psycopg2.connect(**self.db_config)
        cursor = conn.cursor()

        try:
            # Load grid cells into database if needed
            self.load_grid_cells_to_database(cursor)
            conn.commit()

            # Clear existing data (optional - comment out if you want to keep existing data)
            print("Clearing existing data...")
            cursor.execute("DELETE FROM eo_change")
            cursor.execute("DELETE FROM eo")
            conn.commit()

            # Phase 1: Insert all images and collect their IDs
            print("\n=== Phase 1: Inserting images ===")
            successful_records = {}  # filepath -> (record_id, grid_id)
            successful_inserts = 0

            for filepath in tiff_files:
                try:
                    # Create a savepoint for this image
                    cursor.execute("SAVEPOINT image_insert")

                    # Get grid_id first to track it
                    image_bbox = self.get_image_bbox_from_tiff(filepath)
                    grid_id = self.find_grid_cell_for_image(image_bbox)

                    if grid_id is not None:
                        record_id = self.insert_image_record(cursor, filepath)
                        if record_id is not None:
                            successful_records[filepath] = (record_id, grid_id)
                            successful_inserts += 1

                    # Release the savepoint on success
                    cursor.execute("RELEASE SAVEPOINT image_insert")

                except Exception as e:
                    # Rollback to the savepoint on error
                    cursor.execute("ROLLBACK TO SAVEPOINT image_insert")
                    print(f"Error processing {filepath}: {str(e)}")
                    continue

            conn.commit()
            print(
                f"\n✅ Successfully inserted {successful_inserts}/{len(tiff_files)} image records"
            )

            # Phase 2: Create change masks for image pairs within each grid cell
            print("\n=== Phase 2: Skipping change mask creation for now ===")
            change_mask_count = 0

            # # Group successful records by grid_id
            # grid_groups = self.group_files_by_grid(successful_records)

            # for grid_id, files_and_ids in grid_groups.items():
            #     if len(files_and_ids) < 2:
            #         print(
            #             f"Grid {grid_id}: Only {len(files_and_ids)} image(s), skipping change detection"
            #         )
            #         continue

            #     print(
            #         f"Grid {grid_id}: Creating change masks for {len(files_and_ids)} images..."
            #     )

            #     # Create all pairs within this grid cell
            #     for (file_a, id_a), (file_b, id_b) in combinations(files_and_ids, 2):
            #         try:
            #             self.insert_change_mask(
            #                 cursor, id_a, id_b, file_a, file_b, grid_id
            #             )
            #             change_mask_count += 1
            #         except Exception as e:
            #             print(f"Error creating change mask: {str(e)}")
            #             continue

            # conn.commit()
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
                min_change, max_change = cursor.fetchone()
                print(f"Change detection period: {min_change} to {max_change}")

            # Show grid coverage
            cursor.execute(
                """
                SELECT grid_id, COUNT(*) as image_count 
                FROM eo 
                GROUP BY grid_id 
                ORDER BY grid_id
            """
            )
            grid_coverage = cursor.fetchall()
            print(f"\nGrid coverage:")
            for grid_id, count in grid_coverage:
                print(f"  Grid {grid_id}: {count} images")

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
        print(f"  Slovenia Grid: {self.grid_gpkg_path}")
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
