#!/usr/bin/env python3
"""
Create Slovenia Grid GeoPackage

This script creates a standardized grid GeoPackage for Slovenia that conforms to 
grid cell integration standards. The grid includes proper grid_id fields and 
follows standard schema requirements.

Requirements:
    - geopandas
    - shapely
    - sentinelhub
    - numpy
    - sqlite3

Usage:
    python create_slovenia_grid.py
"""

import os
import sqlite3
import numpy as np
import geopandas as gpd
from shapely.geometry import Polygon
from sentinelhub import UtmZoneSplitter
import datetime


class SloveniaGridGenerator:
    """Generator for standardized Slovenia grid GeoPackage."""
    
    def __init__(self, data_folder="../../example_data", output_folder="./grid_output"):
        """
        Initialize the grid generator.
        
        Args:
            data_folder (str): Path to folder containing svn_border.geojson
            output_folder (str): Path to output folder for generated files
        """
        self.data_folder = data_folder
        self.output_folder = output_folder
        self.ensure_output_directory()
        
        # Grid configuration
        self.grid_size = 5000  # 5km grid cells
        self.buffer_size = 500  # 500m buffer around Slovenia border
        self.crs = "EPSG:32633"  # UTM Zone 33N
        
    def ensure_output_directory(self):
        """Create output directory if it doesn't exist."""
        os.makedirs(self.output_folder, exist_ok=True)
        
    def load_slovenia_boundary(self):
        """
        Load and process Slovenia boundary.
        
        Returns:
            tuple: (country_gdf, country_shape) - GeoDataFrame and geometry
        """
        print("Loading Slovenia boundary...")
        
        # Load geojson file
        boundary_path = os.path.join(self.data_folder, "svn_border.geojson")
        if not os.path.exists(boundary_path):
            raise FileNotFoundError(f"Slovenia boundary file not found: {boundary_path}")
            
        country = gpd.read_file(boundary_path)
        
        # Add buffer to secure sufficient data near border
        country_buffered = country.buffer(self.buffer_size)
        country_shape = country_buffered.geometry.values[0]
        
        # Print dimensions
        bounds = country_shape.bounds
        width = bounds[2] - bounds[0]
        height = bounds[3] - bounds[1]
        print(f"Slovenia boundary dimensions: {width:.0f} x {height:.0f} meters")
        
        return country, country_shape
        
    def create_grid(self, country_shape, country_crs):
        """
        Create grid cells for Slovenia.
        
        Args:
            country_shape: Shapely geometry of Slovenia
            country_crs: CRS of the country boundary
            
        Returns:
            geopandas.GeoDataFrame: Grid cells with standardized schema
        """
        print(f"Creating {self.grid_size}m x {self.grid_size}m grid...")
        
        # Create bbox splitter
        bbox_splitter = UtmZoneSplitter([country_shape], country_crs, self.grid_size)
        bbox_list = bbox_splitter.get_bbox_list()
        info_list = bbox_splitter.get_info_list()
        
        print(f"Generated {len(bbox_list)} grid cells")
        
        # Create geometries and attributes
        geometries = [Polygon(bbox.get_polygon()) for bbox in bbox_list]
        
        # Create standardized grid data
        grid_data = {
            'grid_id': [f"SI_{info['index']:06d}" for info in info_list],
            'index': [info['index'] for info in info_list],
            'index_x': [info['index_x'] for info in info_list],
            'index_y': [info['index_y'] for info in info_list],
            'cell_size': [self.grid_size] * len(bbox_list),
            'area_sqm': [self.grid_size * self.grid_size] * len(bbox_list),
            'created_date': [datetime.datetime.now().isoformat()] * len(bbox_list),
            'country': ['Slovenia'] * len(bbox_list),
            'utm_zone': ['33N'] * len(bbox_list)
        }
        
        # Add bounding box coordinates
        for i, bbox in enumerate(bbox_list):
            bounds = bbox.get_polygon()
            grid_data.setdefault('min_x', []).append(bounds[0][0])
            grid_data.setdefault('min_y', []).append(bounds[0][1])
            grid_data.setdefault('max_x', []).append(bounds[2][0])
            grid_data.setdefault('max_y', []).append(bounds[2][1])
            
        # Create GeoDataFrame
        grid_gdf = gpd.GeoDataFrame(grid_data, geometry=geometries, crs=country_crs)
        
        return grid_gdf
        
    def create_metadata_table(self, gpkg_path, total_cells, bounds):
        """
        Create metadata table in the GeoPackage.
        
        Args:
            gpkg_path (str): Path to GeoPackage file
            total_cells (int): Total number of grid cells
            bounds (tuple): Bounding box (minx, miny, maxx, maxy)
        """
        print("Creating metadata table...")
        
        conn = sqlite3.connect(gpkg_path)
        cursor = conn.cursor()
        
        # Create metadata table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS grid_metadata (
                id INTEGER PRIMARY KEY,
                parameter TEXT,
                value TEXT,
                description TEXT
            )
        ''')
        
        # Insert metadata
        metadata = [
            ('grid_version', '1.0', 'Grid schema version'),
            ('country', 'Slovenia', 'Country name'),
            ('crs', self.crs, 'Coordinate Reference System'),
            ('cell_size_m', str(self.grid_size), 'Grid cell size in meters'),
            ('buffer_size_m', str(self.buffer_size), 'Buffer around country boundary in meters'),
            ('total_cells', str(total_cells), 'Total number of grid cells'),
            ('min_x', str(bounds[0]), 'Minimum X coordinate'),
            ('min_y', str(bounds[1]), 'Minimum Y coordinate'),
            ('max_x', str(bounds[2]), 'Maximum X coordinate'),
            ('max_y', str(bounds[3]), 'Maximum Y coordinate'),
            ('created_date', datetime.datetime.now().isoformat(), 'Creation date'),
            ('created_by', 'Slovenia Grid Generator', 'Creation tool'),
        ]
        
        for param, value, desc in metadata:
            cursor.execute(
                'INSERT OR REPLACE INTO grid_metadata (parameter, value, description) VALUES (?, ?, ?)',
                (param, value, desc)
            )
            
        conn.commit()
        conn.close()
        
    def create_spatial_index(self, gpkg_path, table_name='grid_cells'):
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
            cursor.execute(f'''
                CREATE INDEX IF NOT EXISTS idx_{table_name}_geom 
                ON {table_name}(geom)
            ''')
            
            # Create regular indexes on commonly used fields
            cursor.execute(f'''
                CREATE INDEX IF NOT EXISTS idx_{table_name}_grid_id 
                ON {table_name}(grid_id)
            ''')
            
            cursor.execute(f'''
                CREATE INDEX IF NOT EXISTS idx_{table_name}_index_xy 
                ON {table_name}(index_x, index_y)
            ''')
            
            conn.commit()
        except Exception as e:
            print(f"Warning: Could not create all indexes: {e}")
        finally:
            conn.close()
            
    def get_grid_id_for_coordinates(self, gpkg_path, x, y, table_name='grid_cells'):
        """
        Helper function to get grid_id for given coordinates.
        
        Args:
            gpkg_path (str): Path to GeoPackage file
            x (float): X coordinate
            y (float): Y coordinate
            table_name (str): Name of the grid table
            
        Returns:
            str: grid_id if found, None otherwise
        """
        grid_gdf = gpd.read_file(gpkg_path, layer=table_name)
        
        from shapely.geometry import Point
        point = Point(x, y)
        
        # Find grid cell containing the point
        mask = grid_gdf.geometry.contains(point)
        if mask.any():
            return grid_gdf.loc[mask, 'grid_id'].iloc[0]
        else:
            return None
            
    def generate_sample_subset(self, grid_gdf, center_id=None, subset_size=5):
        """
        Generate a sample subset (e.g., 5x5) of grid cells.
        
        Args:
            grid_gdf (GeoDataFrame): Full grid
            center_id (int): Center cell index (if None, will auto-select)
            subset_size (int): Size of square subset (default 5 for 5x5)
            
        Returns:
            GeoDataFrame: Subset of grid cells
        """
        print(f"Creating {subset_size}x{subset_size} sample subset...")
        
        if center_id is None:
            # Auto-select a center that allows full subset
            center_id = 359  # Default from original notebook
            
        # Find center cell info
        center_info = None
        for idx, row in grid_gdf.iterrows():
            if row['index'] == center_id:
                center_info = row
                break
                
        if center_info is None:
            print(f"Warning: Center ID {center_id} not found, using first available cell")
            center_info = grid_gdf.iloc[0]
            
        center_x = center_info['index_x']
        center_y = center_info['index_y']
        
        # Select subset
        radius = subset_size // 2
        subset_mask = (
            (abs(grid_gdf['index_x'] - center_x) <= radius) &
            (abs(grid_gdf['index_y'] - center_y) <= radius)
        )
        
        subset_gdf = grid_gdf[subset_mask].copy()
        
        if len(subset_gdf) != subset_size * subset_size:
            print(f"Warning: Subset contains {len(subset_gdf)} cells instead of {subset_size*subset_size}")
            
        return subset_gdf
        
    def save_grid(self, grid_gdf, filename="slovenia_grid.gpkg", include_sample=True):
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
        
        # Save main grid table
        grid_gdf.to_file(output_path, layer='grid_cells', driver='GPKG')
        
        # Create metadata table
        bounds = grid_gdf.total_bounds
        self.create_metadata_table(output_path, len(grid_gdf), bounds)
        
        # Create spatial index
        self.create_spatial_index(output_path, 'grid_cells')
        
        # Save sample subset if requested
        if include_sample:
            sample_subset = self.generate_sample_subset(grid_gdf)
            sample_subset.to_file(output_path, layer='sample_5x5', driver='GPKG')
            
        print(f"Grid saved successfully with {len(grid_gdf)} cells")
        print(f"Output file: {output_path}")
        
        return output_path
        
    def generate_full_grid(self):
        """
        Complete workflow to generate Slovenia grid.
        
        Returns:
            tuple: (grid_gdf, output_path)
        """
        print("=== Slovenia Grid Generator ===")
        print(f"Grid cell size: {self.grid_size}m x {self.grid_size}m")
        print(f"Buffer size: {self.buffer_size}m")
        print(f"CRS: {self.crs}")
        print()
        
        # Load boundary
        country, country_shape = self.load_slovenia_boundary()
        
        # Create grid
        grid_gdf = self.create_grid(country_shape, country.crs)
        
        # Save grid
        output_path = self.save_grid(grid_gdf)
        
        print("\n=== Summary ===")
        print(f"Total grid cells: {len(grid_gdf)}")
        print(f"Grid bounds: {grid_gdf.total_bounds}")
        print(f"Output file: {output_path}")
        
        return grid_gdf, output_path


def main():
    """Main function to run the grid generation."""
    
    # Initialize generator
    generator = SloveniaGridGenerator()
    
    try:
        # Generate grid
        grid_gdf, output_path = generator.generate_full_grid()
        
        # Print some example usage
        print("\n=== Usage Examples ===")
        print("1. Load the full grid:")
        print(f"   grid = gpd.read_file('{output_path}', layer='grid_cells')")
        print()
        print("2. Load the sample 5x5 subset:")
        print(f"   sample = gpd.read_file('{output_path}', layer='sample_5x5')")
        print()
        print("3. Get grid_id for coordinates:")
        print("   grid_id = generator.get_grid_id_for_coordinates(output_path, x, y)")
        print()
        print("4. View metadata:")
        print("   metadata = pd.read_sql('SELECT * FROM grid_metadata', sqlite3.connect(output_path))")
        
    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()
