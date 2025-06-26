import numpy as np
import rasterio
from rasterio.transform import from_bounds
import io


def create_plus_sign_mask(width=512, height=512, line_width=200):
    """
    Create a binary mask with a plus sign pattern

    Args:
        width: Image width in pixels
        height: Image height in pixels
        line_width: Width of the plus sign lines in pixels

    Returns:
        numpy array with plus sign pattern (255 for plus, 0 for background)
    """
    # Initialize with zeros (black background)
    mask = np.zeros((height, width), dtype=np.uint8)

    # Create horizontal line (center of image)
    h_center = height // 2
    h_start = max(0, h_center - line_width // 2)
    h_end = min(height, h_center + line_width // 2)
    mask[h_start:h_end, :] = 255  # Changed from 1 to 255

    # Create vertical line (center of image)
    v_center = width // 2
    v_start = max(0, v_center - line_width // 2)
    v_end = min(width, v_center + line_width // 2)
    mask[:, v_start:v_end] = 255  # Changed from 1 to 255

    return mask


def save_mask_as_geotiff(mask_array, bounds):
    """
    Save mask array as GeoTIFF bytes

    Args:
        mask_array: numpy array with mask data
        bounds: (min_lon, min_lat, max_lon, max_lat)

    Returns:
        bytes: GeoTIFF data as bytes
    """
    height, width = mask_array.shape

    # Create transform from bounds
    transform = from_bounds(bounds[0], bounds[1], bounds[2], bounds[3], width, height)

    # Create in-memory file
    memory_file = io.BytesIO()

    with rasterio.open(
        memory_file,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=1,
        dtype=mask_array.dtype,
        crs="EPSG:4326",
        transform=transform,
        compress="lzw",
    ) as dst:
        dst.write(mask_array, 1)

    # Get bytes data
    memory_file.seek(0)
    return memory_file.read()


def save_mask_as_geotiff_file(mask_array, bounds, output_path):
    """
    Save mask array as GeoTIFF file

    Args:
        mask_array: numpy array with mask data
        bounds: (min_lon, min_lat, max_lon, max_lat)
        output_path: path where to save the GeoTIFF file

    Returns:
        None
    """
    height, width = mask_array.shape

    # Create transform from bounds
    transform = from_bounds(bounds[0], bounds[1], bounds[2], bounds[3], width, height)

    with rasterio.open(
        output_path,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=1,
        dtype=mask_array.dtype,
        crs="EPSG:4326",
        transform=transform,
        compress="lzw",
    ) as dst:
        dst.write(mask_array, 1)


# Create the mask
mask = create_plus_sign_mask(width=512, height=512, line_width=30)

# Define your bounding box (replace with actual coordinates)
bounds = (14.0, 45.0, 16.0, 47.0)  # Example bounds for Slovenia

# Save to local file
output_file = "plus_sign_mask.tif"
save_mask_as_geotiff_file(mask, bounds, output_file)

print(f"Mask saved to {output_file}")
