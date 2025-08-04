#!/usr/bin/env python3
"""
debug_database_images.py - Debug script to visualize images being inserted into the database

This script helps debug the OSCD database population by:
1. Reading and visualizing the actual mask files
2. Comparing PNG vs TIF mask formats
3. Creating proper RGB composites from multi-band images
4. Saving debug images to check what's being inserted
"""

import os
import json
import re
from pathlib import Path
from typing import Dict, Any, Tuple, Optional

import psycopg2
import rasterio
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
from dotenv import load_dotenv
import cv2


class ImageDebugger:
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
        self.debug_output_path = Path("debug_output")
        self.debug_output_path.mkdir(exist_ok=True)

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

    def analyze_mask_files(self, city: str) -> Dict[str, Any]:
        """Analyze both PNG and TIF mask files for a city"""
        results = {"city": city}

        mask_dir = Path(self.masks_base_path) / city / "cm"

        # Check PNG file
        png_file = mask_dir / "cm.png"
        if png_file.exists():
            results["png"] = self._analyze_single_file(png_file, "PNG")

        # Check TIF file
        tif_file = mask_dir / f"{city}-cm.tif"
        if tif_file.exists():
            results["tif"] = self._analyze_single_file(tif_file, "TIF")

        return results

    def _analyze_single_file(self, file_path: Path, file_type: str) -> Dict[str, Any]:
        """Analyze a single mask file"""
        with rasterio.open(file_path) as src:
            data = src.read()  # Read all bands

            info = {
                "file_type": file_type,
                "file_path": str(file_path),
                "bands": src.count,
                "width": src.width,
                "height": src.height,
                "dtype": str(src.dtypes[0]),
                "crs": str(src.crs) if src.crs else None,
                "transform": str(src.transform),
                "data_shape": data.shape,
                "data_min": data.min(),
                "data_max": data.max(),
                "unique_values": np.unique(data).tolist(),
                "data": data,
            }

            return info

    def visualize_mask_comparison(self, city: str) -> None:
        """Create visualization comparing PNG and TIF masks"""
        analysis = self.analyze_mask_files(city)

        fig, axes = plt.subplots(1, 3, figsize=(15, 5))
        fig.suptitle(f"Mask Analysis for {city}", fontsize=16)

        plot_idx = 0

        # Plot PNG if available
        if "png" in analysis:
            png_data = analysis["png"]["data"]
            if png_data.ndim == 3:
                # If multi-band, show first band
                png_data = png_data[0]

            im1 = axes[plot_idx].imshow(png_data, cmap="viridis")
            axes[plot_idx].set_title(
                f"PNG Mask\nShape: {png_data.shape}\nRange: {png_data.min()}-{png_data.max()}"
            )
            axes[plot_idx].axis("off")
            plt.colorbar(im1, ax=axes[plot_idx])
            plot_idx += 1

            # Print unique values
            print(f"PNG unique values: {np.unique(png_data)}")

        # Plot TIF if available
        if "tif" in analysis:
            tif_data = analysis["tif"]["data"]
            if tif_data.ndim == 3:
                # If multi-band, show first band
                tif_data = tif_data[0]

            im2 = axes[plot_idx].imshow(tif_data, cmap="viridis")
            axes[plot_idx].set_title(
                f"TIF Mask\nShape: {tif_data.shape}\nRange: {tif_data.min()}-{tif_data.max()}"
            )
            axes[plot_idx].axis("off")
            plt.colorbar(im2, ax=axes[plot_idx])
            plot_idx += 1

            # Print unique values
            print(f"TIF unique values: {np.unique(tif_data)}")

        # Plot difference if both exist
        if "png" in analysis and "tif" in analysis:
            png_data = analysis["png"]["data"]
            tif_data = analysis["tif"]["data"]

            if png_data.ndim == 3:
                png_data = png_data[0]
            if tif_data.ndim == 3:
                tif_data = tif_data[0]

            # Ensure same shape for comparison
            if png_data.shape == tif_data.shape:
                diff = np.abs(png_data.astype(float) - tif_data.astype(float))
                im3 = axes[plot_idx].imshow(diff, cmap="Reds")
                axes[plot_idx].set_title(
                    f"Absolute Difference\nMax diff: {diff.max():.2f}"
                )
                axes[plot_idx].axis("off")
                plt.colorbar(im3, ax=axes[plot_idx])
            else:
                axes[plot_idx].text(
                    0.5,
                    0.5,
                    f"Different shapes:\nPNG: {png_data.shape}\nTIF: {tif_data.shape}",
                    ha="center",
                    va="center",
                    transform=axes[plot_idx].transAxes,
                )
                axes[plot_idx].axis("off")

        # Hide unused subplots
        for i in range(plot_idx, len(axes)):
            axes[i].axis("off")

        plt.tight_layout()
        output_file = self.debug_output_path / f"{city}_mask_comparison.png"
        plt.savefig(output_file, dpi=150, bbox_inches="tight")
        plt.close()

        print(f"Saved mask comparison: {output_file}")

        # Print detailed analysis
        print(f"\n=== Mask Analysis for {city} ===")
        for file_type, info in analysis.items():
            if file_type != "city":
                print(f"\n{file_type.upper()} file:")
                print(f"  Path: {info['file_path']}")
                print(f"  Bands: {info['bands']}")
                print(f"  Dimensions: {info['width']}x{info['height']}")
                print(f"  Data type: {info['dtype']}")
                print(f"  Data shape: {info['data_shape']}")
                print(f"  Value range: {info['data_min']} - {info['data_max']}")
                print(f"  Unique values: {info['unique_values']}")

    def create_rgb_composite(
        self, city: str, img_dir: str, save_debug: bool = True
    ) -> np.ndarray:
        """Create RGB composite from Sentinel-2 bands (B04, B03, B02)"""
        img_path = Path(self.images_base_path) / city / img_dir

        # Find RGB bands (B04=Red, B03=Green, B02=Blue)
        rgb_bands = []
        for band in ["B04", "B03", "B02"]:  # Red, Green, Blue
            pattern = f"*{band}.tif"
            matching_files = list(img_path.glob(pattern))
            if matching_files:
                rgb_bands.append(str(matching_files[0]))
            else:
                raise FileNotFoundError(f"Band {band} not found in {img_path}")

        # Read RGB bands
        rgb_data = []
        for band_file in rgb_bands:
            with rasterio.open(band_file) as src:
                band_data = src.read(1).astype(np.float32)
                rgb_data.append(band_data)

        # Stack into RGB array
        rgb_array = np.stack(rgb_data, axis=2)  # Shape: (height, width, 3)

        # Normalize to 0-255 for visualization (adjust percentiles for better contrast)
        rgb_normalized = np.zeros_like(rgb_array, dtype=np.uint8)
        for i in range(3):
            band = rgb_array[:, :, i]
            # Use percentile stretching for better visualization
            p2, p98 = np.percentile(band[band > 0], [2, 98])
            band_stretched = np.clip((band - p2) / (p98 - p2) * 255, 0, 255)
            rgb_normalized[:, :, i] = band_stretched.astype(np.uint8)

        if save_debug:
            # Save RGB composite
            output_file = self.debug_output_path / f"{city}_{img_dir}_rgb_composite.png"
            cv2.imwrite(
                str(output_file), cv2.cvtColor(rgb_normalized, cv2.COLOR_RGB2BGR)
            )
            print(f"Saved RGB composite: {output_file}")

        return rgb_normalized

    def visualize_city_data(self, city: str) -> None:
        """Create comprehensive visualization for a city"""
        print(f"\n=== Analyzing city: {city} ===")

        # Analyze masks
        self.visualize_mask_comparison(city)

        # Create RGB composites for both time periods
        try:
            rgb1 = self.create_rgb_composite(city, "imgs_1")
            rgb2 = self.create_rgb_composite(city, "imgs_2")

            # Create side-by-side visualization
            fig, axes = plt.subplots(2, 2, figsize=(12, 10))
            fig.suptitle(f"RGB Composites and Masks for {city}", fontsize=16)

            # Show RGB composites
            axes[0, 0].imshow(rgb1)
            axes[0, 0].set_title("Time 1 (imgs_1)")
            axes[0, 0].axis("off")

            axes[0, 1].imshow(rgb2)
            axes[0, 1].set_title("Time 2 (imgs_2)")
            axes[0, 1].axis("off")

            # Show masks
            analysis = self.analyze_mask_files(city)
            if "png" in analysis:
                mask_data = analysis["png"]["data"]
                if mask_data.ndim == 3:
                    mask_data = mask_data[0]
                axes[1, 0].imshow(mask_data, cmap="viridis")
                axes[1, 0].set_title("Change Mask (PNG)")
                axes[1, 0].axis("off")

            if "tif" in analysis:
                mask_data = analysis["tif"]["data"]
                if mask_data.ndim == 3:
                    mask_data = mask_data[0]
                axes[1, 1].imshow(mask_data, cmap="viridis")
                axes[1, 1].set_title("Change Mask (TIF)")
                axes[1, 1].axis("off")

            plt.tight_layout()
            output_file = self.debug_output_path / f"{city}_complete_analysis.png"
            plt.savefig(output_file, dpi=150, bbox_inches="tight")
            plt.close()

            print(f"Saved complete analysis: {output_file}")

        except Exception as e:
            print(f"Error creating RGB composites for {city}: {e}")

    def debug_database_insertion(self, city: str) -> None:
        """Debug what would be inserted into the database"""
        print(f"\n=== Database Insertion Debug for {city} ===")

        # Simulate what the current code does
        mask_file = Path(self.masks_base_path) / city / "cm" / "cm.png"

        if mask_file.exists():
            with rasterio.open(mask_file) as src:
                # This is what the current code does
                mask_data = src.read(1)  # Read first band only
                print(f"Current code reads: band 1 only")
                print(f"  Shape: {mask_data.shape}")
                print(f"  Data type: {mask_data.dtype}")
                print(f"  Value range: {mask_data.min()} - {mask_data.max()}")
                print(f"  Unique values: {np.unique(mask_data)}")

                # What gets saved to database
                mask_bytes = mask_data.tobytes()
                print(f"  Bytes length: {len(mask_bytes)}")

                # Try to reconstruct from bytes (this is what fails)
                try:
                    reconstructed = np.frombuffer(mask_bytes, dtype=mask_data.dtype)
                    reconstructed = reconstructed.reshape(mask_data.shape)

                    # Save as PNG (this is what's failing)
                    debug_png_path = (
                        self.debug_output_path / f"{city}_reconstructed_from_bytes.png"
                    )
                    cv2.imwrite(str(debug_png_path), reconstructed)
                    print(f"  Reconstructed and saved: {debug_png_path}")

                except Exception as e:
                    print(f"  Error reconstructing from bytes: {e}")

                # Show all bands if multi-band
                all_data = src.read()
                print(f"File actually has {src.count} bands")
                print(f"  All bands shape: {all_data.shape}")

                if src.count > 1:
                    for i in range(src.count):
                        band_data = all_data[i]
                        print(
                            f"  Band {i+1}: range {band_data.min()}-{band_data.max()}, unique: {len(np.unique(band_data))}"
                        )


def main():
    """Main debug function"""
    debugger = ImageDebugger()

    # Test cities
    test_cities = ["saclay_e", "paris"]  # Start with a couple of cities

    for city in test_cities:
        try:
            debugger.visualize_city_data(city)
            debugger.debug_database_insertion(city)
        except Exception as e:
            print(f"Error processing {city}: {e}")

    print(f"\n=== Debug output saved to: {debugger.debug_output_path} ===")


if __name__ == "__main__":
    main()
