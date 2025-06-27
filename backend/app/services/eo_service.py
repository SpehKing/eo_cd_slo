from typing import List, Optional, Tuple
from ..repositories.eo_repository import EoRepository
from ..models.schemas import ImageMetadata, ImageListResponse
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
            # Convert TIFF to JPEG
            jpeg_data = await self.image_service.convert_tiff_to_jpeg(
                bytes(image_data["image"])
            )
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
