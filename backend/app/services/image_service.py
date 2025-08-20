import io
import asyncio
from typing import Tuple, Optional, Dict, Any, Union
from datetime import datetime
from PIL import Image
import rasterio
from rasterio.transform import from_bounds
from rasterio.crs import CRS
import numpy as np
import logging
from fastapi import HTTPException
from ..core.config import settings

logger = logging.getLogger(__name__)


class ImageMetadata:
    """Container for image metadata"""

    def __init__(
        self, width: int, height: int, data_type: str, bbox: Optional[str] = None
    ):
        self.width = width
        self.height = height
        self.data_type = data_type
        self.bbox = bbox
        self.numpy_dtype = self._parse_numpy_dtype(data_type)

    def _parse_numpy_dtype(self, data_type: str) -> np.dtype:
        """Convert string data type to numpy dtype"""
        type_mapping = {
            "uint8": np.uint8,
            "uint16": np.uint16,
            "uint32": np.uint32,
            "int8": np.int8,
            "int16": np.int16,
            "int32": np.int32,
            "float32": np.float32,
            "float64": np.float64,
        }

        # Handle variations in type naming
        normalized_type = data_type.lower().replace("<", "").replace(">", "")
        for key in type_mapping:
            if key in normalized_type:
                return np.dtype(type_mapping[key])

        # Default fallback
        logger.warning(f"Unknown data type '{data_type}', defaulting to uint16")
        return np.dtype(np.uint16)


class ImageProcessingService:
    """Service for image processing operations with metadata-aware processing"""

    @staticmethod
    async def convert_bands_to_jpeg(
        bands_data: Dict[str, bytes],
        metadata: ImageMetadata,
        band_mapping: Optional[Dict[str, str]] = None,
    ) -> bytes:
        """
        Convert separate band data to PNG format with transparency support

        Args:
            bands_data: Dictionary of band data (e.g., {'b02': bytes, 'b03': bytes, 'b04': bytes})
            metadata: ImageMetadata object with dimensions and data type
            band_mapping: Optional mapping of band names to RGB channels
        """

        def _process_bands():
            try:
                # Default RGB mapping for Sentinel-2: R=B04, G=B03, B=B02
                current_band_mapping = (
                    band_mapping
                    if band_mapping is not None
                    else {"red": "b04", "green": "b03", "blue": "b02"}
                )

                # Load bands and normalize to (bands, H, W)
                band_arrays = {}
                nodata_value = -32768

                for color, band_name in current_band_mapping.items():
                    if band_name not in bands_data:
                        # Fallback: infer from order if specific bands not found
                        available_bands = list(bands_data.keys())
                        if len(available_bands) >= 3:
                            # Assume order B02, B03, B04 → map to blue, green, red
                            band_indices = {"blue": 0, "green": 1, "red": 2}
                            if color in band_indices:
                                band_name = available_bands[band_indices[color]]
                            else:
                                raise ValueError(
                                    f"Required band {band_name} not found and cannot infer from order"
                                )
                        else:
                            raise ValueError(
                                f"Required band {band_name} not found in data"
                            )

                    # Convert bytes to numpy array using metadata
                    band_bytes = bands_data[band_name]
                    expected_size = (
                        metadata.width * metadata.height * metadata.numpy_dtype.itemsize
                    )

                    if len(band_bytes) != expected_size:
                        logger.warning(
                            f"Band {band_name} size mismatch: expected {expected_size}, got {len(band_bytes)}"
                        )

                    # Convert to numpy array and reshape
                    band_array = np.frombuffer(band_bytes, dtype=metadata.numpy_dtype)

                    # Truncate or pad if necessary
                    expected_pixels = metadata.width * metadata.height
                    if len(band_array) > expected_pixels:
                        band_array = band_array[:expected_pixels]
                    elif len(band_array) < expected_pixels:
                        # Pad with nodata value
                        padded = np.full(
                            expected_pixels, nodata_value, dtype=metadata.numpy_dtype
                        )
                        padded[: len(band_array)] = band_array
                        band_array = padded

                    # Reshape to image dimensions
                    band_arrays[color] = band_array.reshape(
                        metadata.height, metadata.width
                    )

                # Build NoData mask where any band equals -32768
                nodata_mask = np.zeros((metadata.height, metadata.width), dtype=bool)
                for band_array in band_arrays.values():
                    nodata_mask |= band_array == nodata_value

                # Apply percentile stretch (2-98%) and gamma correction (γ=2.2)
                processed_bands = {}
                gamma = 2.2

                for color in ["red", "green", "blue"]:
                    band = band_arrays[color].astype(np.float32)

                    # Mask nodata values for percentile calculation
                    valid_data = band[~nodata_mask]

                    if len(valid_data) > 0:
                        # Apply percentile stretch
                        p2, p98 = np.percentile(valid_data, (2, 98))
                        if p98 > p2:
                            # Clip and normalize to 0-1 range
                            band = np.clip(band, p2, p98)
                            band = (band - p2) / (p98 - p2)
                        else:
                            # Handle constant data
                            band = np.clip(band, 0, 1)

                        # Apply gamma correction
                        band = np.power(np.clip(band, 0, 1), 1.0 / gamma)

                        # Scale to 8-bit
                        processed_bands[color] = (band * 255).astype(np.uint8)
                    else:
                        # All nodata - create zero array
                        processed_bands[color] = np.zeros(
                            (metadata.height, metadata.width), dtype=np.uint8
                        )

                # Stack to RGBA (RGB + alpha from NoData mask)
                rgba_image = np.zeros(
                    (metadata.height, metadata.width, 4), dtype=np.uint8
                )
                rgba_image[:, :, 0] = processed_bands["red"]
                rgba_image[:, :, 1] = processed_bands["green"]
                rgba_image[:, :, 2] = processed_bands["blue"]

                # Set alpha channel (255 for valid data, 0 for nodata)
                rgba_image[:, :, 3] = np.where(nodata_mask, 0, 255)

                # Create PIL Image with alpha channel
                pil_image = Image.fromarray(rgba_image, "RGBA")

                # Optional unsharp mask enhancement
                pil_image = ImageProcessingService._apply_unsharp_mask(pil_image)

                # Resize if too large
                pil_image = ImageProcessingService._resize_if_needed(pil_image)

                # Convert to PNG (supports transparency)
                return ImageProcessingService._convert_to_png(pil_image)

            except Exception as e:
                logger.error(f"Error converting bands to PNG: {str(e)}")
                return ImageProcessingService._create_placeholder_png(
                    f"Error processing image: {str(e)}"
                )

        return await asyncio.to_thread(_process_bands)

    @staticmethod
    async def convert_mask_to_jpeg(
        mask_data: bytes, metadata: ImageMetadata, colormap: str = "viridis"
    ) -> bytes:
        """
        Convert change detection mask to PNG format with custom red/transparent colormap

        Args:
            mask_data: Raw mask data as bytes
            metadata: ImageMetadata object with dimensions and data type
            colormap: Colormap to apply to mask visualization (ignored for binary masks, uses red/transparent)
        """

        def _process_mask():
            try:
                # Convert bytes to numpy array
                expected_size = (
                    metadata.width * metadata.height * metadata.numpy_dtype.itemsize
                )

                if len(mask_data) != expected_size:
                    logger.warning(
                        f"Mask size mismatch: expected {expected_size}, got {len(mask_data)}"
                    )

                mask_array = np.frombuffer(mask_data, dtype=metadata.numpy_dtype)

                # Handle size mismatch
                expected_pixels = metadata.width * metadata.height
                if len(mask_array) > expected_pixels:
                    mask_array = mask_array[:expected_pixels]
                elif len(mask_array) < expected_pixels:
                    padded = np.zeros(expected_pixels, dtype=metadata.numpy_dtype)
                    padded[: len(mask_array)] = mask_array
                    mask_array = padded

                # Reshape to image dimensions
                mask_array = mask_array.reshape(metadata.height, metadata.width)

                # Create RGBA image for red/transparent binary mask
                rgba_mask = ImageProcessingService._create_red_transparent_mask(
                    mask_array
                )

                # Create PIL Image with alpha channel
                pil_image = Image.fromarray(rgba_mask, "RGBA")

                # Save as PNG to support transparency
                img_io = io.BytesIO()
                pil_image.save(img_io, format="PNG", optimize=True)
                return img_io.getvalue()

            except Exception as e:
                logger.error(f"Error processing mask: {str(e)}")
                raise HTTPException(
                    status_code=500, detail=f"Error processing mask: {str(e)}"
                )

        return await asyncio.to_thread(_process_mask)

    @staticmethod
    def _create_red_transparent_mask(mask_array: np.ndarray) -> np.ndarray:
        """Create red/transparent RGBA mask from binary mask array"""
        height, width = mask_array.shape
        rgba = np.zeros((height, width, 4), dtype=np.uint8)

        # Set red channel for changed pixels (value 1)
        changed_pixels = mask_array > 0
        rgba[changed_pixels] = [255, 0, 0, 180]  # Red with 70% opacity
        rgba[~changed_pixels] = [0, 0, 0, 0]  # Transparent for unchanged pixels

        return rgba

    @staticmethod
    async def create_geotiff_from_bands(
        bands_data: Dict[str, bytes],
        metadata: ImageMetadata,
        crs: str = "EPSG:4326",
        transform: Optional[Any] = None,
    ) -> bytes:
        """
        Create a proper GeoTIFF from separate band data using metadata

        Args:
            bands_data: Dictionary of band data
            metadata: ImageMetadata object
            crs: Coordinate reference system
            transform: Rasterio transform object (optional)
        """

        def _create_geotiff():
            try:
                # Extract RGB bands
                band_names = ["b04", "b03", "b02"]  # Red, Green, Blue
                band_arrays = []

                for band_name in band_names:
                    if band_name not in bands_data:
                        raise ValueError(f"Required band {band_name} not found")

                    # Convert to numpy array
                    band_bytes = bands_data[band_name]
                    band_array = np.frombuffer(band_bytes, dtype=metadata.numpy_dtype)

                    # Handle size and reshape
                    expected_pixels = metadata.width * metadata.height
                    if len(band_array) != expected_pixels:
                        if len(band_array) > expected_pixels:
                            band_array = band_array[:expected_pixels]
                        else:
                            padded = np.zeros(
                                expected_pixels, dtype=metadata.numpy_dtype
                            )
                            padded[: len(band_array)] = band_array
                            band_array = padded

                    band_arrays.append(
                        band_array.reshape(metadata.height, metadata.width)
                    )

                # Stack bands
                image_stack = np.stack(band_arrays, axis=0)

                # Create GeoTIFF profile
                profile = {
                    "driver": "GTiff",
                    "dtype": metadata.numpy_dtype,
                    "nodata": None,
                    "width": metadata.width,
                    "height": metadata.height,
                    "count": len(band_arrays),
                    "crs": CRS.from_string(crs),
                    "compress": "lzw",
                    "tiled": True,
                    "blockxsize": 512,
                    "blockysize": 512,
                }

                # Add transform if provided
                if transform is not None:
                    profile["transform"] = transform

                # Create in-memory GeoTIFF
                tiff_io = io.BytesIO()

                with rasterio.open(tiff_io, "w", **profile) as dst:
                    dst.write(image_stack)

                    # Add metadata
                    dst.update_tags(
                        AREA_OR_POINT="Area",
                        TIFFTAG_SOFTWARE="EO Change Detection Service",
                    )

                return tiff_io.getvalue()

            except Exception as e:
                logger.error(f"Error creating GeoTIFF: {str(e)}")
                raise

        return await asyncio.to_thread(_create_geotiff)

    @staticmethod
    def _normalize_to_uint8(array: np.ndarray, original_dtype: np.dtype) -> np.ndarray:
        """Normalize array to uint8 range using eolearn-style approach for L2A visualization"""
        if original_dtype == np.uint8:
            return array.astype(np.uint8)
        elif original_dtype == np.uint16:
            # Use percentile-based clipping similar to eolearn L2A visualization
            # This mimics the natural scaling used in satellite image visualization
            p2, p98 = np.percentile(array, (2, 98))
            if p98 > p2:
                # Clip extreme values and scale to 0-255
                clipped = np.clip(array, p2, p98)
                normalized = ((clipped - p2) / (p98 - p2)) * 255
                return normalized.astype(np.uint8)
            else:
                # Fallback for constant data
                return (array / 256).astype(np.uint8)
        elif original_dtype in (np.float32, np.float64):
            # For float data (reflectance values), use 0-1 range with percentile clipping
            p2, p98 = np.percentile(array, (2, 98))
            if p98 > p2:
                clipped = np.clip(array, p2, p98)
                normalized = ((clipped - p2) / (p98 - p2)) * 255
                return normalized.astype(np.uint8)
            else:
                # Standard 0-1 scaling
                normalized = np.clip(array, 0, 1) * 255
                return normalized.astype(np.uint8)
        else:
            # For other types, use percentile-based normalization like eolearn
            p2, p98 = np.percentile(array, (2, 98))
            if p98 > p2:
                clipped = np.clip(array, p2, p98)
                normalized = ((clipped - p2) / (p98 - p2)) * 255
                return normalized.astype(np.uint8)
            else:
                # Fallback: min-max normalization
                if array.max() > array.min():
                    normalized = (
                        (array - array.min()) / (array.max() - array.min())
                    ) * 255
                else:
                    normalized = np.zeros_like(array) + 128  # Gray if constant
                return normalized.astype(np.uint8)

    @staticmethod
    def _apply_colormap(mask: np.ndarray, colormap: str = "viridis") -> np.ndarray:
        """Apply colormap to mask for visualization"""
        try:
            import matplotlib.pyplot as plt
            import matplotlib.cm as cm

            # Normalize mask to 0-1 range
            if mask.max() > mask.min():
                normalized = (mask - mask.min()) / (mask.max() - mask.min())
            else:
                normalized = np.zeros_like(mask, dtype=np.float32)

            # Apply colormap
            cmap = cm.get_cmap(colormap)
            colored = cmap(normalized)

            # Convert to RGB (0-255)
            rgb = (colored[:, :, :3] * 255).astype(np.uint8)
            return rgb

        except ImportError:
            # Fallback: simple grayscale to RGB conversion
            logger.warning("Matplotlib not available, using grayscale visualization")
            normalized = ImageProcessingService._normalize_to_uint8(mask, mask.dtype)
            return np.stack([normalized, normalized, normalized], axis=2)

    @staticmethod
    def _enhance_image(image: Image.Image) -> Image.Image:
        """Apply basic image enhancements"""
        try:
            from PIL import ImageEnhance

            # Slight contrast enhancement
            enhancer = ImageEnhance.Contrast(image)
            image = enhancer.enhance(1.1)

            # Slight sharpness enhancement
            enhancer = ImageEnhance.Sharpness(image)
            image = enhancer.enhance(1.05)

            return image
        except Exception:
            return image

    @staticmethod
    def _resize_if_needed(image: Image.Image) -> Image.Image:
        """Resize image if it exceeds maximum size"""
        max_size = (settings.max_image_size, settings.max_image_size)
        if image.size[0] > max_size[0] or image.size[1] > max_size[1]:
            image.thumbnail(max_size, Image.Resampling.LANCZOS)
        return image

    @staticmethod
    def _convert_to_jpeg(image: Image.Image) -> bytes:
        """Convert PIL Image to JPEG bytes"""
        jpeg_io = io.BytesIO()
        image.save(
            jpeg_io,
            format="JPEG",
            quality=settings.jpeg_quality,
            optimize=True,
            progressive=True,
        )
        return jpeg_io.getvalue()

    @staticmethod
    def _convert_to_png(image: Image.Image) -> bytes:
        """Convert PIL Image to PNG bytes with transparency support"""
        png_io = io.BytesIO()
        image.save(
            png_io,
            format="PNG",
            optimize=True,
        )
        return png_io.getvalue()

    @staticmethod
    def _apply_unsharp_mask(
        image: Image.Image, radius: float = 1.0, amount: float = 1.0
    ) -> Image.Image:
        """Apply unsharp mask for image enhancement"""
        try:
            from PIL import ImageFilter

            # Create a slightly blurred version
            blurred = image.filter(ImageFilter.GaussianBlur(radius=radius))

            # Convert to numpy for arithmetic operations
            if image.mode == "RGBA":
                img_array = np.array(image, dtype=np.float32)
                blur_array = np.array(blurred, dtype=np.float32)

                # Apply unsharp mask only to RGB channels, preserve alpha
                enhanced = img_array.copy()
                for c in range(3):  # RGB channels only
                    enhanced[:, :, c] = img_array[:, :, c] + amount * (
                        img_array[:, :, c] - blur_array[:, :, c]
                    )

                # Clip values and convert back
                enhanced = np.clip(enhanced, 0, 255).astype(np.uint8)
                return Image.fromarray(enhanced, "RGBA")
            else:
                # Fallback for non-RGBA images
                return image

        except Exception as e:
            logger.warning(f"Unsharp mask failed: {e}, returning original image")
            return image

    @staticmethod
    def _create_placeholder_jpeg(message: str = "No image data") -> bytes:
        """Create a placeholder JPEG image with error message"""
        # Create placeholder image
        width, height = 512, 512
        placeholder = Image.new("RGB", (width, height), color=(64, 64, 64))

        try:
            from PIL import ImageDraw, ImageFont

            draw = ImageDraw.Draw(placeholder)

            # Try to use a default font
            try:
                font = ImageFont.load_default()
            except:
                font = None

            # Add text
            text_lines = [
                "Image Processing Error",
                "",
                message[:50] + "..." if len(message) > 50 else message,
            ]

            y_offset = height // 2 - 30
            for line in text_lines:
                if line:
                    bbox = (
                        draw.textbbox((0, 0), line, font=font)
                        if font
                        else (0, 0, len(line) * 6, 12)
                    )
                    text_width = bbox[2] - bbox[0]
                    x = (width - text_width) // 2
                    draw.text((x, y_offset), line, fill=(255, 255, 255), font=font)
                y_offset += 20

        except Exception:
            pass  # If text drawing fails, just return the gray rectangle

        return ImageProcessingService._convert_to_jpeg(placeholder)

    @staticmethod
    def _create_placeholder_png(message: str = "No image data") -> bytes:
        """Create a placeholder PNG image with error message"""
        # Create placeholder image with transparency
        width, height = 512, 512
        placeholder = Image.new("RGBA", (width, height), color=(64, 64, 64, 255))

        try:
            from PIL import ImageDraw, ImageFont

            draw = ImageDraw.Draw(placeholder)

            # Try to use a default font
            try:
                font = ImageFont.load_default()
            except:
                font = None

            # Add text
            text_lines = [
                "Image Processing Error",
                "",
                message[:50] + "..." if len(message) > 50 else message,
            ]

            y_offset = height // 2 - 30
            for line in text_lines:
                if line:
                    bbox = (
                        draw.textbbox((0, 0), line, font=font)
                        if font
                        else (0, 0, len(line) * 6, 12)
                    )
                    text_width = bbox[2] - bbox[0]
                    x = (width - text_width) // 2
                    draw.text((x, y_offset), line, fill=(255, 255, 255, 255), font=font)
                y_offset += 20

        except Exception:
            pass  # If text drawing fails, just return the gray rectangle

        return ImageProcessingService._convert_to_png(placeholder)

    @staticmethod
    def generate_filename(
        timestamp: str,
        file_type: str = "tif",
        prefix: str = "sentinel2",
        suffix: str = "",
    ) -> str:
        """Generate filename for downloads"""
        # Parse timestamp if it's a datetime string
        try:
            if isinstance(timestamp, str) and len(timestamp) > 10:
                dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                timestamp_str = dt.strftime("%Y%m%d_%H%M%S")
            else:
                timestamp_str = str(timestamp)
        except:
            timestamp_str = str(timestamp)

        base_name = f"{prefix}_{timestamp_str}"
        if suffix:
            base_name += f"_{suffix}"

        return f"{base_name}.{file_type}"

    @staticmethod
    def get_image_stats(
        bands_data: Dict[str, bytes], metadata: ImageMetadata
    ) -> Dict[str, Any]:
        """Get statistical information about image bands"""
        stats = {
            "width": metadata.width,
            "height": metadata.height,
            "data_type": metadata.data_type,
            "total_pixels": metadata.width * metadata.height,
            "bands": {},
        }

        for band_name, band_bytes in bands_data.items():
            try:
                band_array = np.frombuffer(band_bytes, dtype=metadata.numpy_dtype)
                expected_pixels = metadata.width * metadata.height

                if len(band_array) != expected_pixels:
                    band_array = band_array[:expected_pixels]

                stats["bands"][band_name] = {
                    "min": float(band_array.min()),
                    "max": float(band_array.max()),
                    "mean": float(band_array.mean()),
                    "std": float(band_array.std()),
                    "size_bytes": len(band_bytes),
                }
            except Exception as e:
                stats["bands"][band_name] = {
                    "error": str(e),
                    "size_bytes": len(band_bytes),
                }

        return stats
