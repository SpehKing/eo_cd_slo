#!/usr/bin/env python3
"""
GeoPackage Preview Script

This script provides a preview of GeoPackage (.gpkg) files, showing:
- Basic file information
- Layer details
- Coordinate reference system info
- Data preview
- Optional map visualization

Requirements:
- geopandas
- matplotlib
- contextily (optional, for basemaps)
- folium (optional, for interactive maps)

Usage:
    python preview_gpkg.py <path_to_gpkg_file> [options]
"""

import sys
import argparse
from pathlib import Path
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import warnings

warnings.filterwarnings("ignore")

# Optional imports for enhanced visualization
try:
    import contextily as ctx

    HAS_CONTEXTILY = True
except ImportError:
    HAS_CONTEXTILY = False

try:
    import folium

    HAS_FOLIUM = True
except ImportError:
    HAS_FOLIUM = False


class GPKGPreviewer:
    """Class to preview GeoPackage files"""

    def __init__(self, gpkg_path: Path):
        self.gpkg_path = Path(gpkg_path)
        self.layers = {}

    def check_file_exists(self) -> bool:
        """Check if the GPKG file exists"""
        if not self.gpkg_path.exists():
            print(f"Error: File {self.gpkg_path} does not exist")
            return False

        if self.gpkg_path.suffix.lower() != ".gpkg":
            print(f"Warning: File {self.gpkg_path} doesn't have .gpkg extension")

        return True

    def get_file_info(self) -> dict:
        """Get basic file information"""
        file_stats = self.gpkg_path.stat()

        return {
            "file_path": str(self.gpkg_path),
            "file_size_mb": round(file_stats.st_size / (1024 * 1024), 2),
            "last_modified": pd.to_datetime(file_stats.st_mtime, unit="s"),
        }

    def get_layers_info(self) -> dict:
        """Get information about all layers in the GPKG"""
        try:
            # List all layers
            import fiona

            layer_names = fiona.listlayers(self.gpkg_path)

            layers_info = {}

            for layer_name in layer_names:
                try:
                    gdf = gpd.read_file(self.gpkg_path, layer=layer_name)
                    self.layers[layer_name] = gdf

                    # Get geometry types
                    geom_types = gdf.geometry.geom_type.value_counts().to_dict()

                    layers_info[layer_name] = {
                        "feature_count": len(gdf),
                        "geometry_types": geom_types,
                        "crs": str(gdf.crs) if gdf.crs else "Not defined",
                        "bounds": gdf.total_bounds.tolist() if not gdf.empty else None,
                        "columns": list(gdf.columns),
                        "non_null_geom": gdf.geometry.notna().sum(),
                    }

                except Exception as e:
                    layers_info[layer_name] = {"error": str(e)}

            return layers_info

        except Exception as e:
            print(f"Error reading layers: {e}")
            return {}

    def print_summary(self, file_info: dict, layers_info: dict):
        """Print a summary of the GPKG file"""
        print("=" * 80)
        print("GEOPACKAGE PREVIEW")
        print("=" * 80)

        # File information
        print(f"\n📁 FILE INFORMATION:")
        print(f"   Path: {file_info['file_path']}")
        print(f"   Size: {file_info['file_size_mb']} MB")
        print(f"   Last Modified: {file_info['last_modified']}")

        # Layers information
        print(f"\n📊 LAYERS SUMMARY:")
        print(f"   Total Layers: {len(layers_info)}")

        for layer_name, info in layers_info.items():
            print(f"\n   🗂️  Layer: {layer_name}")

            if "error" in info:
                print(f"      ❌ Error: {info['error']}")
                continue

            print(f"      Features: {info['feature_count']:,}")
            print(f"      CRS: {info['crs']}")
            print(f"      Non-null geometries: {info['non_null_geom']:,}")

            # Geometry types
            if info["geometry_types"]:
                geom_str = ", ".join(
                    [
                        f"{geom}: {count}"
                        for geom, count in info["geometry_types"].items()
                    ]
                )
                print(f"      Geometry Types: {geom_str}")

            # Bounds
            if info["bounds"]:
                bounds = info["bounds"]
                print(
                    f"      Bounds: [{bounds[0]:.6f}, {bounds[1]:.6f}, {bounds[2]:.6f}, {bounds[3]:.6f}]"
                )

            # Columns (first 10)
            columns_display = info["columns"][:10]
            if len(info["columns"]) > 10:
                columns_display.append(f"... and {len(info['columns']) - 10} more")
            print(f"      Columns: {', '.join(columns_display)}")

    def show_data_preview(self, layer_name: str = None, n_rows: int = 5):
        """Show a preview of the data"""
        if not self.layers:
            print("No layers loaded")
            return

        # If no layer specified, use the first one
        if layer_name is None:
            layer_name = list(self.layers.keys())[0]

        if layer_name not in self.layers:
            print(f"Layer '{layer_name}' not found")
            return

        gdf = self.layers[layer_name]

        print(f"\n📋 DATA PREVIEW - Layer: {layer_name}")
        print("-" * 80)

        if gdf.empty:
            print("No data to preview")
            return

        # Show first n rows (excluding geometry for readability)
        preview_df = gdf.drop(columns=["geometry"]).head(n_rows)
        print(preview_df.to_string())

        if len(gdf) > n_rows:
            print(f"\n... and {len(gdf) - n_rows} more rows")

    def create_static_map(
        self,
        layer_name: str = None,
        figsize: tuple = (12, 8),
        add_basemap: bool = True,
        save_path: str = None,
        show_ids: bool = True,
    ):
        """Create a static map visualization"""
        if not self.layers:
            print("No layers loaded")
            return

        # If no layer specified, use the first one
        if layer_name is None:
            layer_name = list(self.layers.keys())[0]

        if layer_name not in self.layers:
            print(f"Layer '{layer_name}' not found")
            return

        gdf = self.layers[layer_name]

        if gdf.empty:
            print("No data to visualize")
            return

        print(f"\n🗺️  Creating static map for layer: {layer_name}")

        # Create figure
        fig, ax = plt.subplots(1, 1, figsize=figsize)

        # Plot the data
        if "Polygon" in gdf.geometry.geom_type.values:
            gdf.plot(ax=ax, alpha=0.7, edgecolor="black", linewidth=0.5)
        else:
            gdf.plot(ax=ax, markersize=50, alpha=0.7)

        # Add field IDs as text labels
        if show_ids and not gdf.empty:
            # Try to find an ID field
            id_field = None
            possible_id_fields = [
                "id",
                "ID",
                "fid",
                "FID",
                "objectid",
                "OBJECTID",
                "gid",
                "GID",
            ]

            for field in possible_id_fields:
                if field in gdf.columns:
                    id_field = field
                    break

            # If no ID field found, use the index
            if id_field is None:
                gdf["temp_id"] = gdf.index
                id_field = "temp_id"

            # Add text annotations for each feature
            for idx, row in gdf.iterrows():
                try:
                    # Get centroid for text placement
                    if row.geometry and not row.geometry.is_empty:
                        centroid = row.geometry.centroid
                        ax.annotate(
                            str(row[id_field]),
                            xy=(centroid.x, centroid.y),
                            xytext=(3, 3),
                            textcoords="offset points",
                            fontsize=8,
                            color="red",
                            weight="bold",
                            bbox=dict(
                                boxstyle="round,pad=0.2", facecolor="white", alpha=0.8
                            ),
                        )
                except Exception as e:
                    continue  # Skip problematic geometries

            # Clean up temporary ID field
            if id_field == "temp_id":
                gdf.drop(columns=["temp_id"], inplace=True)

        # Add basemap if available and requested
        if add_basemap and HAS_CONTEXTILY:
            try:
                # Convert to Web Mercator for basemap
                gdf_web_mercator = gdf.to_crs(epsg=3857)

                # Add basemap
                ctx.add_basemap(
                    ax, crs=gdf.crs, source=ctx.providers.OpenStreetMap.Mapnik
                )
            except Exception as e:
                print(f"Could not add basemap: {e}")

        # Set title and labels
        ax.set_title(
            f"GeoPackage Layer: {layer_name}\n({len(gdf)} features)",
            fontsize=14,
            fontweight="bold",
        )
        ax.set_xlabel("Longitude" if gdf.crs and "EPSG:4326" in str(gdf.crs) else "X")
        ax.set_ylabel("Latitude" if gdf.crs and "EPSG:4326" in str(gdf.crs) else "Y")

        # Remove axis if it's a geographic CRS
        if gdf.crs and ("EPSG:4326" in str(gdf.crs) or "WGS84" in str(gdf.crs)):
            ax.tick_params(axis="both", which="major", labelsize=8)

        plt.tight_layout()

        # Save if requested
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches="tight")
            print(f"Map saved to: {save_path}")

        plt.show()

    def create_interactive_map(
        self, layer_name: str = None, save_path: str = None, show_ids: bool = True
    ):
        """Create an interactive map using Folium"""
        if not HAS_FOLIUM:
            print("Folium not available. Install with: pip install folium")
            return

        if not self.layers:
            print("No layers loaded")
            return

        # If no layer specified, use the first one
        if layer_name is None:
            layer_name = list(self.layers.keys())[0]

        if layer_name not in self.layers:
            print(f"Layer '{layer_name}' not found")
            return

        gdf = self.layers[layer_name]

        if gdf.empty:
            print("No data to visualize")
            return

        print(f"\n🌐 Creating interactive map for layer: {layer_name}")

        # Convert to WGS84 if needed
        if gdf.crs != "EPSG:4326":
            gdf = gdf.to_crs("EPSG:4326")

        # Calculate center
        bounds = gdf.total_bounds
        center_lat = (bounds[1] + bounds[3]) / 2
        center_lon = (bounds[0] + bounds[2]) / 2

        # Create map
        m = folium.Map(location=[center_lat, center_lon], zoom_start=10)

        # Try to find an ID field
        id_field = None
        possible_id_fields = [
            "id",
            "ID",
            "fid",
            "FID",
            "objectid",
            "OBJECTID",
            "gid",
            "GID",
        ]

        for field in possible_id_fields:
            if field in gdf.columns:
                id_field = field
                break

        # If no ID field found, use the index
        if id_field is None:
            gdf["temp_id"] = gdf.index
            id_field = "temp_id"

        # Add the data with enhanced popups including ID
        for idx, row in gdf.iterrows():
            try:
                # Create popup content with ID prominently displayed
                popup_html = f"<b>Feature ID: {row[id_field]}</b><br><br>"

                # Add other fields
                for col in gdf.columns:
                    if col != "geometry" and col != id_field:
                        popup_html += f"<b>{col}:</b> {row[col]}<br>"

                # Create GeoJson feature
                feature = folium.GeoJson(
                    row.geometry.__geo_interface__,
                    popup=folium.Popup(popup_html, max_width=300),
                    tooltip=f"ID: {row[id_field]}",
                )
                feature.add_to(m)

                # Add ID labels for point geometries or centroids
                if show_ids:
                    if row.geometry.geom_type in ["Point"]:
                        coords = [row.geometry.y, row.geometry.x]
                    else:
                        centroid = row.geometry.centroid
                        coords = [centroid.y, centroid.x]

                    folium.Marker(
                        coords,
                        icon=folium.DivIcon(
                            html=f'<div style="font-size: 10px; color: red; font-weight: bold; text-shadow: 1px 1px 1px white;">{row[id_field]}</div>',
                            icon_size=(20, 20),
                            icon_anchor=(10, 10),
                        ),
                    ).add_to(m)

            except Exception as e:
                continue  # Skip problematic geometries

        # Clean up temporary ID field
        if id_field == "temp_id":
            gdf.drop(columns=["temp_id"], inplace=True)

        # Add layer control
        folium.LayerControl().add_to(m)

        # Save if requested
        if save_path:
            m.save(save_path)
            print(f"Interactive map saved to: {save_path}")
        else:
            # Try to open in browser
            import tempfile
            import webbrowser

            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".html", delete=False
            ) as f:
                m.save(f.name)
                webbrowser.open(f.name)
                print(f"Interactive map opened in browser: {f.name}")

    def preview(
        self,
        show_data: bool = True,
        show_map: bool = True,
        interactive: bool = False,
        save_map: str = None,
        show_ids: bool = True,
    ):
        """Main preview function"""
        if not self.check_file_exists():
            return False

        try:
            # Get file and layers information
            file_info = self.get_file_info()
            layers_info = self.get_layers_info()

            # Print summary
            self.print_summary(file_info, layers_info)

            # Show data preview
            if show_data and self.layers:
                self.show_data_preview()

            # Show map
            if show_map and self.layers:
                if interactive:
                    self.create_interactive_map(save_path=save_map, show_ids=show_ids)
                else:
                    self.create_static_map(save_path=save_map, show_ids=show_ids)

            return True

        except Exception as e:
            print(f"Error during preview: {e}")
            return False


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Preview GeoPackage files")
    parser.add_argument("gpkg_file", help="Path to the GeoPackage file")
    parser.add_argument("--no-data", action="store_true", help="Skip data preview")
    parser.add_argument("--no-map", action="store_true", help="Skip map visualization")
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Create interactive map instead of static",
    )
    parser.add_argument("--save-map", help="Save map to specified path")
    parser.add_argument("--layer", help="Specify layer name to preview")
    parser.add_argument("--no-ids", action="store_true", help="Hide feature IDs on map")

    args = parser.parse_args()

    # Create previewer
    previewer = GPKGPreviewer(args.gpkg_file)

    # Run preview
    success = previewer.preview(
        show_data=not args.no_data,
        show_map=not args.no_map,
        interactive=args.interactive,
        save_map=args.save_map,
        show_ids=not args.no_ids,
    )

    return 0 if success else 1


if __name__ == "__main__":
    exit(main())
