#!/usr/bin/env python3
"""
Create Slovenia Grid using Ljubljana-centered expansion approach

This script creates a grid for Slovenia by starting from the predefined Ljubljana center
(like in populate_database.py) and expanding outward until hitting Slovenia borders.
Uses a bottom-left origin coordinate system.

Requirements:
    - geopandas
    - shapely
    - numpy
    - sqlite3

Usage:
    python create_slovenia_grid_expanded.py
"""

import os
import sqlite3
import numpy as np
import geopandas as gpd
from shapely.geometry import Polygon, Point
import datetime


class SloveniaExpandedGridGenerator:
    """Generator for Slovenia grid using Ljubljana-centered expansion approach."""

    def __init__(self, data_folder="../../data", output_folder="./grid_output"):
        """
        Initialize the grid generator.

        Args:
            data_folder (str): Path to folder containing svn_border.geojson
            output_folder (str): Path to output folder for generated files
        """
        self.data_folder = "data"
        self.output_folder = output_folder
        self.ensure_output_directory()

        # Grid configuration - matching populate_database.py
        self.lat_centre = 46.0569
        self.lon_centre = 14.5058
        self.cell_size_km = 5
        self.buffer_size = 500  # 500m buffer around Slovenia border
        self.crs_wgs84 = "EPSG:4326"
        self.crs_utm = "EPSG:32633"  # UTM Zone 33N

        # Calculate degree sizes for grid cells
        self.dx_deg_cell = (
            self.cell_size_km * 1000 / (111_000 * np.cos(np.radians(self.lat_centre)))
        )
        self.dy_deg_cell = self.cell_size_km * 1000 / 111_000

    def ensure_output_directory(self):
        """Create output directory if it doesn't exist."""
        os.makedirs(self.output_folder, exist_ok=True)

    def load_slovenia_boundary(self):
        """
        Load and process Slovenia boundary.

        Returns:
            tuple: (country_gdf, country_shape_utm) - GeoDataFrame and buffered geometry in UTM
        """
        print("Loading Slovenia boundary...")

        # Load geojson file
        boundary_path = os.path.join(self.data_folder, "svn_border.geojson")
        if not os.path.exists(boundary_path):
            raise FileNotFoundError(
                f"Slovenia boundary file not found: {boundary_path}"
            )

        country = gpd.read_file(boundary_path)

        # Convert to UTM for accurate distance calculations
        country_utm = country.to_crs(self.crs_utm)

        # Add buffer to secure sufficient data near border
        country_buffered = country_utm.buffer(self.buffer_size)
        country_shape_utm = country_buffered.geometry.values[0]

        # Print dimensions
        bounds = country_shape_utm.bounds
        width = bounds[2] - bounds[0]
        height = bounds[3] - bounds[1]
        print(f"Slovenia boundary dimensions: {width:.0f} x {height:.0f} meters")

        return country, country_shape_utm

    def create_expanded_grid(self, country_shape_utm):
        """
        Create grid by expanding outward from Ljubljana center until hitting Slovenia borders.

        Args:
            country_shape_utm: Slovenia boundary geometry in UTM coordinates

        Returns:
            geopandas.GeoDataFrame: Grid cells with standardized schema
        """
        print(
            f"Creating {self.cell_size_km}km x {self.cell_size_km}km grid expanding from Ljubljana..."
        )

        # Convert center coordinates to UTM
        center_point_wgs = Point(self.lon_centre, self.lat_centre)
        center_gdf = gpd.GeoDataFrame(
            [1], geometry=[center_point_wgs], crs=self.crs_wgs84
        )
        center_utm = center_gdf.to_crs(self.crs_utm).geometry.values[0]
        center_x_utm = center_utm.x
        center_y_utm = center_utm.y

        print(f"Ljubljana center in UTM: {center_x_utm:.0f}, {center_y_utm:.0f}")

        # Cell size in meters
        cell_size_m = self.cell_size_km * 1000

        # Start with center cell (0, 0 in original coordinate system)
        # Find maximum expansion needed by checking Slovenia bounds
        slovenia_bounds = country_shape_utm.bounds

        # Calculate how many cells we need in each direction from center
        cells_west = int(np.ceil((center_x_utm - slovenia_bounds[0]) / cell_size_m)) + 2
        cells_east = int(np.ceil((slovenia_bounds[2] - center_x_utm) / cell_size_m)) + 2
        cells_south = (
            int(np.ceil((center_y_utm - slovenia_bounds[1]) / cell_size_m)) + 2
        )
        cells_north = (
            int(np.ceil((slovenia_bounds[3] - center_y_utm) / cell_size_m)) + 2
        )

        print(
            f"Expansion needed: W:{cells_west}, E:{cells_east}, S:{cells_south}, N:{cells_north}"
        )

        # Generate all potential grid cells
        grid_cells = []
        original_coords = []  # Store original (row, col) coordinates for reference

        for row in range(-cells_north, cells_south + 1):
            for col in range(-cells_west, cells_east + 1):
                # Calculate cell center in UTM
                cell_x = center_x_utm + col * cell_size_m
                cell_y = (
                    center_y_utm - row * cell_size_m
                )  # row 0 is center, negative is north

                # Create cell polygon
                min_x = cell_x - cell_size_m / 2
                max_x = cell_x + cell_size_m / 2
                min_y = cell_y - cell_size_m / 2
                max_y = cell_y + cell_size_m / 2

                cell_polygon = Polygon(
                    [
                        (min_x, min_y),
                        (max_x, min_y),
                        (max_x, max_y),
                        (min_x, max_y),
                        (min_x, min_y),
                    ]
                )

                # Check if cell intersects with Slovenia (including buffer)
                if cell_polygon.intersects(country_shape_utm):
                    grid_cells.append(
                        {
                            "geometry": cell_polygon,
                            "original_row": row,
                            "original_col": col,
                            "center_x_utm": cell_x,
                            "center_y_utm": cell_y,
                            "min_x": min_x,
                            "min_y": min_y,
                            "max_x": max_x,
                            "max_y": max_y,
                        }
                    )

        print(f"Found {len(grid_cells)} cells intersecting with Slovenia")

        # Create new coordinate system starting from bottom-left
        if grid_cells:
            # Find the bottom-left bounds of our grid
            min_x_grid = min(cell["min_x"] for cell in grid_cells)
            min_y_grid = min(cell["min_y"] for cell in grid_cells)

            # Assign new coordinates starting from (0,0) at bottom-left
            for i, cell in enumerate(grid_cells):
                # Calculate new grid coordinates
                new_col = int((cell["min_x"] - min_x_grid) / cell_size_m)
                new_row = int((cell["min_y"] - min_y_grid) / cell_size_m)

                cell["grid_row"] = new_row
                cell["grid_col"] = new_col
                cell["grid_index"] = i

        # Create standardized grid data
        geometries = [cell["geometry"] for cell in grid_cells]

        grid_data = {
            "grid_id": [f"SI_{i:06d}" for i in range(len(grid_cells))],
            "grid_index": [cell["grid_index"] for cell in grid_cells],
            "grid_row": [cell["grid_row"] for cell in grid_cells],
            "grid_col": [cell["grid_col"] for cell in grid_cells],
            "original_row": [cell["original_row"] for cell in grid_cells],
            "original_col": [cell["original_col"] for cell in grid_cells],
            "center_x_utm": [cell["center_x_utm"] for cell in grid_cells],
            "center_y_utm": [cell["center_y_utm"] for cell in grid_cells],
            "min_x": [cell["min_x"] for cell in grid_cells],
            "min_y": [cell["min_y"] for cell in grid_cells],
            "max_x": [cell["max_x"] for cell in grid_cells],
            "max_y": [cell["max_y"] for cell in grid_cells],
            "cell_size_m": [cell_size_m] * len(grid_cells),
            "cell_size_km": [self.cell_size_km] * len(grid_cells),
            "area_sqm": [cell_size_m * cell_size_m] * len(grid_cells),
            "created_date": [datetime.datetime.now().isoformat()] * len(grid_cells),
            "country": ["Slovenia"] * len(grid_cells),
            "utm_zone": ["33N"] * len(grid_cells),
        }

        # Create GeoDataFrame in UTM coordinates
        grid_gdf = gpd.GeoDataFrame(grid_data, geometry=geometries, crs=self.crs_utm)

        return grid_gdf

    def find_ljubljana_center_cell(self, grid_gdf):
        """
        Find the grid cell that contains the Ljubljana center.

        Args:
            grid_gdf: Grid GeoDataFrame

        Returns:
            dict: Information about the center cell
        """
        # Convert Ljubljana center to UTM
        center_point_wgs = Point(self.lon_centre, self.lat_centre)
        center_gdf = gpd.GeoDataFrame(
            [1], geometry=[center_point_wgs], crs=self.crs_wgs84
        )
        center_utm = center_gdf.to_crs(self.crs_utm).geometry.values[0]

        # Find which cell contains this point
        mask = grid_gdf.geometry.contains(center_utm)
        if mask.any():
            center_cell = grid_gdf[mask].iloc[0]
            return {
                "grid_id": center_cell["grid_id"],
                "grid_row": center_cell["grid_row"],
                "grid_col": center_cell["grid_col"],
                "original_row": center_cell["original_row"],
                "original_col": center_cell["original_col"],
            }
        else:
            return None

    def create_metadata_table(self, gpkg_path, total_cells, bounds, center_info):
        """
        Create metadata table in the GeoPackage.

        Args:
            gpkg_path (str): Path to GeoPackage file
            total_cells (int): Total number of grid cells
            bounds (tuple): Bounding box (minx, miny, maxx, maxy)
            center_info (dict): Information about Ljubljana center cell
        """
        print("Creating metadata table...")

        conn = sqlite3.connect(gpkg_path)
        cursor = conn.cursor()

        # Create metadata table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS grid_metadata (
                id INTEGER PRIMARY KEY,
                parameter TEXT,
                value TEXT,
                description TEXT
            )
        """
        )

        # Insert metadata
        metadata = [
            ("grid_version", "1.0", "Grid schema version"),
            (
                "generation_method",
                "Ljubljana-centered expansion",
                "Grid generation method",
            ),
            ("country", "Slovenia", "Country name"),
            ("crs", self.crs_utm, "Coordinate Reference System"),
            ("cell_size_m", str(self.cell_size_km * 1000), "Grid cell size in meters"),
            ("cell_size_km", str(self.cell_size_km), "Grid cell size in kilometers"),
            (
                "buffer_size_m",
                str(self.buffer_size),
                "Buffer around country boundary in meters",
            ),
            ("total_cells", str(total_cells), "Total number of grid cells"),
            ("ljubljana_lat", str(self.lat_centre), "Ljubljana center latitude"),
            ("ljubljana_lon", str(self.lon_centre), "Ljubljana center longitude"),
            ("min_x", str(bounds[0]), "Minimum X coordinate (UTM)"),
            ("min_y", str(bounds[1]), "Minimum Y coordinate (UTM)"),
            ("max_x", str(bounds[2]), "Maximum X coordinate (UTM)"),
            ("max_y", str(bounds[3]), "Maximum Y coordinate (UTM)"),
            ("created_date", datetime.datetime.now().isoformat(), "Creation date"),
            ("created_by", "Slovenia Expanded Grid Generator", "Creation tool"),
        ]

        # Add center cell information if available
        if center_info:
            metadata.extend(
                [
                    (
                        "center_cell_grid_id",
                        center_info["grid_id"],
                        "Grid ID of Ljubljana center cell",
                    ),
                    (
                        "center_cell_row",
                        str(center_info["grid_row"]),
                        "Grid row of Ljubljana center cell",
                    ),
                    (
                        "center_cell_col",
                        str(center_info["grid_col"]),
                        "Grid column of Ljubljana center cell",
                    ),
                    (
                        "center_cell_original_row",
                        str(center_info["original_row"]),
                        "Original row coordinate of center cell",
                    ),
                    (
                        "center_cell_original_col",
                        str(center_info["original_col"]),
                        "Original column coordinate of center cell",
                    ),
                ]
            )

        for param, value, desc in metadata:
            cursor.execute(
                "INSERT OR REPLACE INTO grid_metadata (parameter, value, description) VALUES (?, ?, ?)",
                (param, value, desc),
            )

        conn.commit()
        conn.close()

    def create_spatial_index(self, gpkg_path, table_name="grid_cells"):
        """
        Create spatial index for the grid table.

        Args:
            gpkg_path (str): Path to GeoPackage file
            table_name (str): Name of the grid table
        """
        print("Creating spatial index...")

        conn = sqlite3.connect(gpkg_path)
        cursor = conn.cursor()

        try:
            # Create spatial index
            cursor.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_{table_name}_geom 
                ON {table_name}(geom)
            """
            )

            # Create regular indexes on commonly used fields
            cursor.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_{table_name}_grid_id 
                ON {table_name}(grid_id)
            """
            )

            cursor.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_{table_name}_grid_row_col 
                ON {table_name}(grid_row, grid_col)
            """
            )

            cursor.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_{table_name}_original_row_col 
                ON {table_name}(original_row, original_col)
            """
            )

            conn.commit()
        except Exception as e:
            print(f"Warning: Could not create all indexes: {e}")
        finally:
            conn.close()

    def generate_sample_subset(self, grid_gdf, center_info=None, subset_size=5):
        """
        Generate a sample subset around Ljubljana center or specified center.

        Args:
            grid_gdf (GeoDataFrame): Full grid
            center_info (dict): Center cell information
            subset_size (int): Size of square subset (default 5 for 5x5)

        Returns:
            GeoDataFrame: Subset of grid cells
        """
        print(f"Creating {subset_size}x{subset_size} sample subset...")

        if center_info is None:
            # Find Ljubljana center cell
            center_info = self.find_ljubljana_center_cell(grid_gdf)

        if center_info is None:
            print("Warning: Could not find Ljubljana center, using geometric center")
            center_row = grid_gdf["grid_row"].median()
            center_col = grid_gdf["grid_col"].median()
        else:
            center_row = center_info["grid_row"]
            center_col = center_info["grid_col"]

        # Select subset
        radius = subset_size // 2
        subset_mask = (abs(grid_gdf["grid_row"] - center_row) <= radius) & (
            abs(grid_gdf["grid_col"] - center_col) <= radius
        )

        subset_gdf = grid_gdf[subset_mask].copy()

        print(f"Sample subset contains {len(subset_gdf)} cells")
        if center_info:
            print(
                f"Centered on Ljubljana at grid cell ({center_info['grid_row']}, {center_info['grid_col']})"
            )

        return subset_gdf

    def save_grid(
        self, grid_gdf, filename="slovenia_grid_expanded.gpkg", include_sample=True
    ):
        """
        Save grid to GeoPackage with proper schema.

        Args:
            grid_gdf (GeoDataFrame): Grid data
            filename (str): Output filename
            include_sample (bool): Whether to include sample subset

        Returns:
            str: Path to saved file
        """
        output_path = os.path.join(self.output_folder, filename)

        print(f"Saving grid to {output_path}...")

        # Find Ljubljana center cell info
        center_info = self.find_ljubljana_center_cell(grid_gdf)

        # Save main grid table
        grid_gdf.to_file(output_path, layer="grid_cells", driver="GPKG")

        # Create metadata table
        bounds = grid_gdf.total_bounds
        self.create_metadata_table(output_path, len(grid_gdf), bounds, center_info)

        # Create spatial index
        self.create_spatial_index(output_path, "grid_cells")

        # Save sample subset if requested
        if include_sample:
            sample_subset = self.generate_sample_subset(grid_gdf, center_info)
            sample_subset.to_file(output_path, layer="sample_5x5", driver="GPKG")

        print(f"Grid saved successfully with {len(grid_gdf)} cells")
        print(f"Output file: {output_path}")

        if center_info:
            print(
                f"Ljubljana center cell: {center_info['grid_id']} at ({center_info['grid_row']}, {center_info['grid_col']})"
            )

        return output_path

    def generate_full_grid(self):
        """
        Complete workflow to generate Slovenia grid using expansion approach.

        Returns:
            tuple: (grid_gdf, output_path)
        """
        print("=== Slovenia Expanded Grid Generator ===")
        print(f"Grid cell size: {self.cell_size_km}km x {self.cell_size_km}km")
        print(f"Ljubljana center: {self.lat_centre}°N, {self.lon_centre}°E")
        print(f"Buffer size: {self.buffer_size}m")
        print(f"CRS: {self.crs_utm}")
        print()

        # Load boundary
        country, country_shape_utm = self.load_slovenia_boundary()

        # Create grid using expansion approach
        grid_gdf = self.create_expanded_grid(country_shape_utm)

        # Save grid
        output_path = self.save_grid(grid_gdf)

        print("\n=== Summary ===")
        print(f"Total grid cells: {len(grid_gdf)}")
        print(
            f"Grid coordinate range: rows {grid_gdf['grid_row'].min()}-{grid_gdf['grid_row'].max()}, cols {grid_gdf['grid_col'].min()}-{grid_gdf['grid_col'].max()}"
        )
        print(
            f"Original coordinate range: rows {grid_gdf['original_row'].min()}-{grid_gdf['original_row'].max()}, cols {grid_gdf['original_col'].min()}-{grid_gdf['original_col'].max()}"
        )
        print(f"Grid bounds (UTM): {grid_gdf.total_bounds}")
        print(f"Output file: {output_path}")

        return grid_gdf, output_path


def main():
    """Main function to run the grid generation."""

    # Initialize generator
    generator = SloveniaExpandedGridGenerator()

    try:
        # Generate grid
        grid_gdf, output_path = generator.generate_full_grid()

        # Print some example usage
        print("\n=== Usage Examples ===")
        print("1. Load the full grid:")
        print(f"   grid = gpd.read_file('{output_path}', layer='grid_cells')")
        print()
        print("2. Load the sample 5x5 subset around Ljubljana:")
        print(f"   sample = gpd.read_file('{output_path}', layer='sample_5x5')")
        print()
        print("3. Find cell by new grid coordinates:")
        print("   cell = grid[grid['grid_row'] == 10]['grid_col'] == 15]")
        print()
        print("4. Find cell by original coordinates (matching populate_database.py):")
        print(
            "   cell = grid[(grid['original_row'] == 0) & (grid['original_col'] == 1)]"
        )
        print()
        print("5. View metadata:")
        print(
            "   metadata = pd.read_sql('SELECT * FROM grid_metadata', sqlite3.connect(output_path))"
        )

    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()
