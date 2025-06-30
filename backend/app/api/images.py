from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response, StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, Literal
import io
import logging
from datetime import datetime

from ..core.database import get_db_session
from ..core.logging import logfire_instance
from ..repositories.eo_repository import EoRepository
from ..services.eo_service import EoService
from ..models.schemas import (
    ImageListResponse,
    ImageMetadata,
    HealthCheck,
    ChangeMaskListResponse,
    SpectralBandsRequest,
    SpectralBandsResponse,
)

router = APIRouter()
logger = logging.getLogger(__name__)


def get_eo_repository(session: AsyncSession = Depends(get_db_session)) -> EoRepository:
    """Dependency to get EoRepository instance"""
    return EoRepository(session)


def get_eo_service(repository: EoRepository = Depends(get_eo_repository)) -> EoService:
    """Dependency to get EoService instance"""
    return EoService(repository)


@router.get("/images", response_model=ImageListResponse)
async def list_images(
    start_time: Optional[str] = Query(None, description="Start time in ISO format"),
    end_time: Optional[str] = Query(None, description="End time in ISO format"),
    min_lon: Optional[float] = Query(
        None, ge=-180, le=180, description="Minimum longitude"
    ),
    min_lat: Optional[float] = Query(
        None, ge=-90, le=90, description="Minimum latitude"
    ),
    max_lon: Optional[float] = Query(
        None, ge=-180, le=180, description="Maximum longitude"
    ),
    max_lat: Optional[float] = Query(
        None, ge=-90, le=90, description="Maximum latitude"
    ),
    limit: int = Query(50, ge=1, le=100, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Number of results to skip"),
    service: EoService = Depends(get_eo_service),
):
    """
    List images with optional filtering by time and spatial bounds.

    - **start_time**: ISO format timestamp (e.g., "2021-07-28T10:07:54Z")
    - **end_time**: ISO format timestamp (e.g., "2024-07-28T10:07:54Z")
    - **min_lon, min_lat, max_lon, max_lat**: Bounding box coordinates
    - **limit**: Maximum number of results (1-100)
    - **offset**: Number of results to skip for pagination
    """
    if logfire_instance:
        logfire_instance.info(
            "Images list request received",
            start_time=start_time,
            end_time=end_time,
            bbox=(
                [min_lon, min_lat, max_lon, max_lat]
                if all(x is not None for x in [min_lon, min_lat, max_lon, max_lat])
                else None
            ),
            limit=limit,
            offset=offset,
        )

    try:
        # Format timestamps if provided
        if start_time and not start_time.endswith(("+00", "Z")):
            start_time += "+00"
        if end_time and not end_time.endswith(("+00", "Z")):
            end_time += "+00"

        result = await service.get_images(
            start_time=start_time,
            end_time=end_time,
            min_lon=min_lon,
            min_lat=min_lat,
            max_lon=max_lon,
            max_lat=max_lat,
            limit=limit,
            offset=offset,
        )

        if logfire_instance:
            logfire_instance.info(
                "Images list request completed successfully",
                total_images=result.total,
                returned_count=len(result.images),
                has_more=result.has_more,
            )

        return result

    except Exception as e:
        if logfire_instance:
            logfire_instance.error("Failed to retrieve images", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve images: {str(e)}"
        )


@router.get("/images/{image_id}")
async def get_image_data(
    image_id: int,
    format: Literal["metadata", "preview", "original", "bands"] = Query(
        "metadata", description="Data format to return"
    ),
    bands: Optional[str] = Query(
        None, description="Comma-separated list of bands (only for format=bands)"
    ),
    service: EoService = Depends(get_eo_service),
):
    """
    Get image data in various formats:
    - **metadata**: JSON metadata about the image
    - **preview**: JPEG preview image (max 1024x1024)
    - **original**: Original TIFF file download
    - **bands**: Specific spectral bands as base64-encoded JSON

    For bands format, use the 'bands' parameter to specify which bands (e.g., "B02,B03,B04").
    """
    try:
        if format == "metadata":
            metadata = await service.get_image_metadata(image_id)
            if not metadata:
                raise HTTPException(status_code=404, detail="Image not found")
            return metadata

        elif format == "preview":
            jpeg_data = await service.get_image_preview(image_id)
            if not jpeg_data:
                raise HTTPException(
                    status_code=404,
                    detail="Image not found or preview generation failed",
                )
            return Response(
                content=jpeg_data,
                media_type="image/jpeg",
                headers={"Content-Length": str(len(jpeg_data))},
            )

        elif format == "original":
            result = await service.get_original_image(image_id)
            if not result:
                raise HTTPException(status_code=404, detail="Image not found")

            image_data, filename = result

            def iter_bytes():
                chunk_size = 1024 * 1024  # 1MB chunks
                bytes_io = io.BytesIO(image_data)
                while True:
                    chunk = bytes_io.read(chunk_size)
                    if not chunk:
                        break
                    yield chunk

            return StreamingResponse(
                iter_bytes(),
                media_type="image/tiff",
                headers={
                    "Content-Disposition": f"attachment; filename={filename}",
                    "Content-Length": str(len(image_data)),
                },
            )

        elif format == "bands":
            if not bands:
                raise HTTPException(
                    status_code=400, detail="bands parameter required for format=bands"
                )

            band_list = [band.strip() for band in bands.split(",")]
            request = SpectralBandsRequest(bands=band_list)
            result = await service.get_spectral_bands(image_id, request.bands)
            if not result:
                raise HTTPException(status_code=404, detail="Image not found")
            return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve image data: {str(e)}"
        )


@router.get("/change-masks")
async def get_change_masks(
    # List parameters
    start_time: Optional[str] = Query(None, description="Start time in ISO format"),
    end_time: Optional[str] = Query(None, description="End time in ISO format"),
    min_lon: Optional[float] = Query(
        None, ge=-180, le=180, description="Minimum longitude"
    ),
    min_lat: Optional[float] = Query(
        None, ge=-90, le=90, description="Minimum latitude"
    ),
    max_lon: Optional[float] = Query(
        None, ge=-180, le=180, description="Maximum longitude"
    ),
    max_lat: Optional[float] = Query(
        None, ge=-90, le=90, description="Maximum latitude"
    ),
    limit: int = Query(50, ge=1, le=100, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Number of results to skip"),
    # Specific mask parameters
    img_a_id: Optional[int] = Query(
        None, description="First image ID for specific mask"
    ),
    img_b_id: Optional[int] = Query(
        None, description="Second image ID for specific mask"
    ),
    format: Literal["list", "data", "preview"] = Query(
        "list", description="Return format"
    ),
    service: EoService = Depends(get_eo_service),
):
    """
    Get change detection masks in various formats:
    - **list**: List of available change masks (default)
    - **data**: Binary mask data for specific image pair (requires img_a_id, img_b_id)
    - **preview**: JPEG preview of specific mask (requires img_a_id, img_b_id)

    For specific mask operations, both img_a_id and img_b_id must be provided, with img_a_id < img_b_id.
    """
    try:
        if format == "list":
            # Format timestamps if provided
            if start_time and not start_time.endswith(("+00", "Z")):
                start_time += "+00"
            if end_time and not end_time.endswith(("+00", "Z")):
                end_time += "+00"

            result = await service.get_change_masks(
                start_time=start_time,
                end_time=end_time,
                min_lon=min_lon,
                min_lat=min_lat,
                max_lon=max_lon,
                max_lat=max_lat,
                limit=limit,
                offset=offset,
            )
            return result

        elif format in ["data", "preview"]:
            if img_a_id is None or img_b_id is None:
                raise HTTPException(
                    status_code=400,
                    detail="img_a_id and img_b_id parameters required for data/preview format",
                )

            if img_a_id >= img_b_id:
                raise HTTPException(
                    status_code=400, detail="img_a_id must be less than img_b_id"
                )

            if format == "data":
                mask_data = await service.get_change_mask_data(img_a_id, img_b_id)
                if not mask_data:
                    raise HTTPException(
                        status_code=404,
                        detail="Change mask not found for the specified image pair",
                    )

                return Response(
                    content=mask_data,
                    media_type="application/octet-stream",
                    headers={
                        "Content-Disposition": f"attachment; filename=change_mask_{img_a_id}_{img_b_id}.bin",
                        "Content-Length": str(len(mask_data)),
                    },
                )

            else:  # format == "preview"
                jpeg_data = await service.get_change_mask_preview(img_a_id, img_b_id)
                if not jpeg_data:
                    raise HTTPException(
                        status_code=404,
                        detail="Change mask not found or preview generation failed",
                    )

                return Response(
                    content=jpeg_data,
                    media_type="image/jpeg",
                    headers={"Content-Length": str(len(jpeg_data))},
                )

    except HTTPException:
        raise
    except Exception as e:
        if logfire_instance:
            logfire_instance.error("Failed to retrieve change masks", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve change masks: {str(e)}"
        )


@router.get("/health", response_model=HealthCheck)
async def health_check(service: EoService = Depends(get_eo_service)):
    """
    Health check endpoint to verify service and database connectivity.
    """
    db_healthy = await service.health_check()
    return HealthCheck(
        status="healthy" if db_healthy else "unhealthy",
        database="connected" if db_healthy else "disconnected",
        timestamp=datetime.utcnow(),
    )
