import io
import asyncio
from typing import Tuple, Optional
from PIL import Image
import rasterio
import numpy as np
import logging
from ..core.config import settings

logger = logging.getLogger(__name__)


class ImageProcessingService:
    """Service for image processing operations"""

    @staticmethod
    async def convert_tiff_to_jpeg(tiff_data: bytes) -> bytes:
        """Convert TIFF image data to JPEG format for web display"""

        def _process_image():
            try:
                # Create a temporary file-like object from the TIFF data
                tiff_io = io.BytesIO(tiff_data)

                # Try to open with rasterio first (better for satellite imagery)
                try:
                    with rasterio.open(tiff_io) as src:
                        # Read the image data
                        if src.count >= 3:
                            # RGB or RGBA image - read first 3 bands
                            data = src.read([1, 2, 3])
                        else:
                            # Single band - duplicate to create RGB
                            band = src.read(1)
                            data = np.stack([band, band, band])

                        # Convert from (bands, height, width) to (height, width, bands)
                        data = np.transpose(data, (1, 2, 0))

                        # Normalize to 0-255 range
                        if data.dtype == np.float32 or data.dtype == np.float64:
                            # Handle float data (often 0-1 range)
                            data = np.clip(data * 255, 0, 255).astype(np.uint8)
                        elif data.dtype == np.uint16:
                            # Handle 16-bit data (often 0-65535 range)
                            data = (data / 256).astype(np.uint8)
                        elif data.dtype != np.uint8:
                            # Handle other data types
                            data = np.clip(data, 0, 255).astype(np.uint8)

                        # Create PIL Image
                        if data.shape[2] == 3:
                            pil_image = Image.fromarray(data, "RGB")
                        else:
                            pil_image = Image.fromarray(data[:, :, 0], "L")

                except Exception as rasterio_error:
                    # Fallback to PIL if rasterio fails
                    logger.warning(f"Rasterio failed, trying PIL: {rasterio_error}")
                    tiff_io.seek(0)  # Reset stream position
                    pil_image = Image.open(tiff_io)

                    # Convert to RGB if necessary
                    if pil_image.mode in ("RGBA", "LA"):
                        # Convert RGBA to RGB by compositing over white background
                        background = Image.new("RGB", pil_image.size, (255, 255, 255))
                        if pil_image.mode == "RGBA":
                            background.paste(pil_image, mask=pil_image.split()[-1])
                        else:
                            background.paste(pil_image, mask=pil_image.split()[-1])
                        pil_image = background
                    elif pil_image.mode not in ("RGB", "L"):
                        pil_image = pil_image.convert("RGB")

                # Resize if image is too large (to improve performance)
                max_size = (settings.max_image_size, settings.max_image_size)
                if pil_image.size[0] > max_size[0] or pil_image.size[1] > max_size[1]:
                    pil_image.thumbnail(max_size, Image.Resampling.LANCZOS)

                # Convert to JPEG
                jpeg_io = io.BytesIO()
                pil_image.save(
                    jpeg_io, format="JPEG", quality=settings.jpeg_quality, optimize=True
                )
                return jpeg_io.getvalue()

            except Exception as e:
                logger.error(f"Error converting TIFF to JPEG: {str(e)}")
                raise Exception(f"Failed to convert TIFF to JPEG: {str(e)}")

        # Run the CPU-intensive operation in a thread pool
        return await asyncio.to_thread(_process_image)

    @staticmethod
    def generate_filename(timestamp: str, file_type: str = "tif") -> str:
        """Generate filename for downloads"""
        return f"sentinel2_{timestamp}_original.{file_type}"
