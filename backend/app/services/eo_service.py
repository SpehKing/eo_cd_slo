from typing import List, Optional, Tuple
from ..repositories.eo_repository import EoRepository
from ..models.schemas import (
    ImageMetadata,
    ImageListResponse,
    ChangeMaskMetadata,
    ChangeMaskListResponse,
    DateRangeResponse,
)
from .image_service import ImageProcessingService, ImageMetadata as ImageMeta
import logging

logger = logging.getLogger(__name__)


class EoService:
    """Main service for Earth Observation operations"""

    def __init__(self, repository: EoRepository):
        self.repository = repository

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
            width=image_data.get("width"),
            height=image_data.get("height"),
            data_type=image_data.get("data_type"),
            size_bytes=image_data["size_bytes"],
        )

    async def get_image_preview(self, image_id: int) -> Optional[bytes]:
        """Get JPEG preview of an image"""
        print(f"Generating preview for image {image_id}")
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

            # Create bands data dictionary
            bands_data = {
                "b04": bytes(image_data["b04"]),  # Red
                "b03": bytes(image_data["b03"]),  # Green
                "b02": bytes(image_data["b02"]),  # Blue
            }

            # Create ImageMetadata object
            image_metadata = ImageMeta(
                width=image_data.get("width", 512),  # Default fallback
                height=image_data.get("height", 512),  # Default fallback
                data_type=image_data.get("data_type", "uint16"),  # Default fallback
                bbox=image_data.get("bbox_wkt"),
            )

            jpeg_data = await ImageProcessingService.convert_bands_to_jpeg(
                bands_data, image_metadata
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

        bands_data, timestamp, metadata = result

        # Create a proper TIFF from the bands
        try:
            # Create ImageMetadata object from the database metadata
            image_metadata = ImageMeta(
                width=metadata.get("width", 512),  # Default fallback
                height=metadata.get("height", 512),  # Default fallback
                data_type=metadata.get("data_type", "uint16"),  # Default fallback
                bbox=metadata.get("bbox_wkt"),
            )

            tiff_data = await ImageProcessingService.create_geotiff_from_bands(
                bands_data, image_metadata
            )
            filename = ImageProcessingService.generate_filename(
                timestamp.strftime("%Y%m%d_%H%M%S")
            )
            return tiff_data, filename
        except Exception as e:
            logger.error(f"Failed to create TIFF for image {image_id}: {e}")
            return None

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
        import base64

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
                # Convert binary data to base64 string for JSON serialization
                band_data = image_data[band]
                if isinstance(band_data, (bytes, memoryview)):
                    result["bands"][band] = base64.b64encode(bytes(band_data)).decode(
                        "utf-8"
                    )
                else:
                    result["bands"][band] = band_data

        return result

    async def get_change_mask_preview(
        self, img_a_id: int, img_b_id: int
    ) -> Optional[bytes]:
        """Get PNG preview of a change detection mask"""

        mask_info = await self.repository.get_change_mask_by_images(img_a_id, img_b_id)
        if not mask_info:
            return None

        try:
            # Extract mask data and metadata
            mask_data = bytes(mask_info["mask"])

            # Create ImageMetadata object for the mask
            mask_metadata = ImageMeta(
                width=mask_info.get("width", 512),  # Default fallback
                height=mask_info.get("height", 512),  # Default fallback
                data_type=mask_info.get(
                    "data_type", "uint8"
                ),  # Default fallback for masks
                bbox=mask_info.get("bbox_wkt"),
            )

            # Convert mask to JPEG with red colormap for change detection
            jpeg_data = await ImageProcessingService.convert_mask_to_jpeg(
                mask_data, mask_metadata, colormap="Reds"
            )
            return jpeg_data

        except Exception as e:
            logger.error(
                f"Failed to generate change mask preview for images {img_a_id}-{img_b_id}: {e}"
            )
            return None

    async def get_date_range(self) -> DateRangeResponse:
        """Get the earliest and latest dates in the database"""
        min_date, max_date, total_count = await self.repository.get_date_range()

        return DateRangeResponse(
            min_date=min_date.isoformat() if min_date else None,
            max_date=max_date.isoformat() if max_date else None,
            total_count=total_count,
        )
