#!/usr/bin/env python3
"""
test_mask_fix.py - Test the corrected mask reading functionality
"""

import numpy as np
import rasterio
import cv2
from pathlib import Path


def test_mask_reading(city: str = "saclay_e"):
    """Test reading both PNG and TIF mask files"""
    masks_base_path = "data/oscd/Onera Satellite Change Detection dataset - Masks"

    print(f"=== Testing mask reading for {city} ===")

    # Test PNG file (original approach)
    png_file = Path(masks_base_path) / city / "cm" / "cm.png"
    print(f"\nPNG file: {png_file}")
    if png_file.exists():
        with rasterio.open(png_file) as src:
            png_data = src.read(1)  # First band only
            print(f"  PNG - Shape: {png_data.shape}, dtype: {png_data.dtype}")
            print(f"  PNG - Range: {png_data.min()}-{png_data.max()}")
            print(f"  PNG - Unique values: {np.unique(png_data)}")
            print(f"  PNG - Total bands: {src.count}")

    # Test TIF file (corrected approach)
    tif_file = Path(masks_base_path) / city / "cm" / f"{city}-cm.tif"
    print(f"\nTIF file: {tif_file}")
    if tif_file.exists():
        with rasterio.open(tif_file) as src:
            tif_data = src.read(1)  # First band only
            print(f"  TIF - Shape: {tif_data.shape}, dtype: {tif_data.dtype}")
            print(f"  TIF - Range: {tif_data.min()}-{tif_data.max()}")
            print(f"  TIF - Unique values: {np.unique(tif_data)}")
            print(f"  TIF - Total bands: {src.count}")

            # Test creating a proper visualization
            mask_visualization = np.where(tif_data == 2, 255, 0).astype(np.uint8)
            print(
                f"  Visualization - Range: {mask_visualization.min()}-{mask_visualization.max()}"
            )
            print(f"  Visualization - Unique values: {np.unique(mask_visualization)}")

            # Save the corrected visualization
            output_path = f"debug_output/{city}_corrected_mask.png"
            cv2.imwrite(output_path, mask_visualization)
            print(f"  ✓ Saved corrected mask visualization: {output_path}")

            # Test reconstruction from bytes (what database would store)
            tif_bytes = tif_data.tobytes()
            reconstructed = np.frombuffer(tif_bytes, dtype=tif_data.dtype)
            reconstructed = reconstructed.reshape(tif_data.shape)

            print(
                f"  Bytes reconstruction - Success: {np.array_equal(tif_data, reconstructed)}"
            )

            # Save reconstructed as visualization
            reconstructed_vis = np.where(reconstructed == 2, 255, 0).astype(np.uint8)
            output_path_rec = f"debug_output/{city}_reconstructed_from_database.png"
            cv2.imwrite(output_path_rec, reconstructed_vis)
            print(f"  ✓ Saved reconstructed visualization: {output_path_rec}")


if __name__ == "__main__":
    test_mask_reading()
