from typing import List, Optional, Tuple
from ..repositories.eo_repository import EoRepository
from ..models.schemas import (
    ImageMetadata,
    ImageListResponse,
    ChangeMaskMetadata,
    ChangeMaskListResponse,
)
from .image_service import ImageProcessingService
import logging

logger = logging.getLogger(__name__)


class EoService:
    """Main service for Earth Observation operations"""

    def __init__(self, repository: EoRepository):
        self.repository = repository
        self.image_service = ImageProcessingService()

    async def get_images(
        self,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        min_lon: Optional[float] = None,
        min_lat: Optional[float] = None,
        max_lon: Optional[float] = None,
        max_lat: Optional[float] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> ImageListResponse:
        """Get paginated list of images with metadata"""

        # Prepare bounding box if all coordinates are provided
        bbox = None
        if all(coord is not None for coord in [min_lon, min_lat, max_lon, max_lat]):
            bbox = (min_lon, min_lat, max_lon, max_lat)

        # Get total count and images in parallel
        total_count = await self.repository.get_image_count(
            start_time=start_time, end_time=end_time, bbox=bbox
        )

        images_data = await self.repository.get_images_metadata(
            start_time=start_time,
            end_time=end_time,
            bbox=bbox,
            limit=limit,
            offset=offset,
        )

        # Convert to Pydantic models
        images = [
            ImageMetadata(
                id=img["id"],
                time=img["time"],
                bbox_wkt=img["bbox_wkt"],
                size_bytes=img["size_bytes"],
            )
            for img in images_data
        ]

        has_more = offset + len(images) < total_count

        return ImageListResponse(
            images=images,
            total=total_count,
            limit=limit,
            offset=offset,
            has_more=has_more,
        )

    async def get_image_metadata(self, image_id: int) -> Optional[ImageMetadata]:
        """Get metadata for a specific image"""

        image_data = await self.repository.get_image_by_id(image_id)
        if not image_data:
            return None

        return ImageMetadata(
            id=image_data["id"],
            time=image_data["time"],
            bbox_wkt=image_data["bbox_wkt"],
            size_bytes=image_data["size_bytes"],
        )

    async def get_image_preview(self, image_id: int) -> Optional[bytes]:
        """Get JPEG preview of an image"""

        image_data = await self.repository.get_image_by_id(image_id)
        if not image_data:
            return None

        try:
            # Check if RGB bands are available
            if not all(
                band in image_data and image_data[band]
                for band in ["b02", "b03", "b04"]
            ):
                logger.warning(f"RGB bands not available for image {image_id}")
                return None

            # Convert RGB bands to JPEG
            rgb_bands = {
                "red": bytes(image_data["b04"]),  # Band 4 (Red)
                "green": bytes(image_data["b03"]),  # Band 3 (Green)
                "blue": bytes(image_data["b02"]),  # Band 2 (Blue)
            }

            jpeg_data = await self.image_service.convert_rgb_bands_to_jpeg(rgb_bands)
            return jpeg_data
        except Exception as e:
            logger.error(f"Failed to generate preview for image {image_id}: {e}")
            return None

    async def get_original_image(self, image_id: int) -> Optional[Tuple[bytes, str]]:
        """Get original TIFF image data and filename"""

        result = await self.repository.get_original_image_data(image_id)
        if not result:
            return None

        image_data, timestamp = result
        filename = self.image_service.generate_filename(
            timestamp.strftime("%Y%m%d_%H%M%S")
        )

        return image_data, filename

    async def health_check(self) -> bool:
        """Check service health"""
        return await self.repository.health_check()

    async def get_change_masks(
        self,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        min_lon: Optional[float] = None,
        min_lat: Optional[float] = None,
        max_lon: Optional[float] = None,
        max_lat: Optional[float] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> ChangeMaskListResponse:
        """Get paginated list of change detection masks"""

        # Prepare bounding box if all coordinates are provided
        bbox = None
        if all(coord is not None for coord in [min_lon, min_lat, max_lon, max_lat]):
            bbox = (min_lon, min_lat, max_lon, max_lat)

        masks_data = await self.repository.get_change_masks(
            start_time=start_time,
            end_time=end_time,
            bbox=bbox,
            limit=limit,
            offset=offset,
        )

        # Convert to Pydantic models
        masks = [
            ChangeMaskMetadata(
                img_a_id=mask["img_a_id"],
                img_b_id=mask["img_b_id"],
                period_start=mask["period_start"],
                period_end=mask["period_end"],
                bbox_wkt=mask["bbox_wkt"],
                mask_size_bytes=mask["mask_size_bytes"],
            )
            for mask in masks_data
        ]

        # For simplicity, assume total equals the number returned (can be improved)
        total_count = len(masks) + offset
        has_more = len(masks) == limit

        return ChangeMaskListResponse(
            masks=masks,
            total=total_count,
            limit=limit,
            offset=offset,
            has_more=has_more,
        )

    async def get_change_mask_data(
        self, img_a_id: int, img_b_id: int
    ) -> Optional[bytes]:
        """Get change mask binary data for image pair"""

        mask_data = await self.repository.get_change_mask_by_images(img_a_id, img_b_id)
        if not mask_data:
            return None

        return bytes(mask_data["mask"])

    async def get_spectral_bands(
        self, image_id: int, bands: List[str]
    ) -> Optional[dict]:
        """Get specific spectral bands for an image"""

        image_data = await self.repository.get_all_bands_by_id(image_id)
        if not image_data:
            return None

        # Filter requested bands
        result = {
            "id": image_data["id"],
            "time": image_data["time"],
            "bbox_wkt": image_data["bbox_wkt"],
            "bands": {},
        }

        valid_bands = [
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

        for band in bands:
            if band in valid_bands and band in image_data and image_data[band]:
                result["bands"][band] = image_data[band]

        return result
