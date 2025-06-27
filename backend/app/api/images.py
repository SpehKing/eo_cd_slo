from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response, StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional
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
    ImageQueryRequest,
    HealthCheck,
    ChangeMaskListResponse,
    ChangeMaskMetadata,
    SpectralBandsRequest,
)

router = APIRouter()
logger = logging.getLogger(__name__)

router = APIRouter()


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

    - **start_time**: ISO format timestamp (e.g., "2023-01-01T00:00:00+00")
    - **end_time**: ISO format timestamp
    - **min_lon, min_lat, max_lon, max_lat**: Bounding box coordinates
    - **limit**: Maximum number of results (1-100)
    - **offset**: Number of results to skip for pagination
    """

    # Logfire logging for request
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

        # Log successful response
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


@router.get("/images/{image_id}", response_model=ImageMetadata)
async def get_image_metadata(
    image_id: int, service: EoService = Depends(get_eo_service)
):
    """
    Get metadata for a specific image by ID.
    """
    metadata = await service.get_image_metadata(image_id)
    if not metadata:
        raise HTTPException(status_code=404, detail="Image not found")
    return metadata


@router.get("/images/{image_id}/preview.jpg")
async def get_image_preview(
    image_id: int, service: EoService = Depends(get_eo_service)
):
    """
    Get a JPEG preview of the image (max 1024x1024 pixels).
    """
    jpeg_data = await service.get_image_preview(image_id)
    if not jpeg_data:
        raise HTTPException(
            status_code=404, detail="Image not found or preview generation failed"
        )

    return Response(
        content=jpeg_data,
        media_type="image/jpeg",
        headers={
            "Cache-Control": "public, max-age=86400",  # Cache for 24 hours
            "Content-Length": str(len(jpeg_data)),
        },
    )


@router.get("/images/{image_id}/original.tif")
async def get_original_image(
    image_id: int, service: EoService = Depends(get_eo_service)
):
    """
    Download the original TIFF image data.
    """
    result = await service.get_original_image(image_id)
    if not result:
        raise HTTPException(status_code=404, detail="Image not found")

    image_data, filename = result

    def iter_bytes():
        """Generator to stream the file in chunks"""
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


@router.get("/hello")
async def hello_world():
    """
    Hello world endpoint to test Logfire integration
    """
    logger.info("Hello world endpoint called")

    # Demo various Logfire log levels
    if logfire_instance:
        logfire_instance.info("Hello, Logfire! 🌍", endpoint="hello_world")
        logfire_instance.debug("This is a debug message from the hello endpoint")
        logfire_instance.warn("This is a warning - everything is fine though!")

        # Log with structured data
        logfire_instance.info(
            "Structured logging example",
            user_id="demo_user",
            action="hello_world_access",
            timestamp=datetime.utcnow().isoformat(),
            metadata={
                "service": "eo-cd-slo-api",
                "version": "1.0.0",
                "endpoint": "/hello",
            },
        )

    return {
        "message": "Hello from Sentinel-2 API!",
        "logfire_enabled": logfire_instance is not None,
        "timestamp": datetime.utcnow().isoformat(),
        "tip": "Check your Logfire dashboard to see these logs! 📊",
    }


@router.get("/change-masks", response_model=ChangeMaskListResponse)
async def list_change_masks(
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
    List change detection masks with optional filtering by time and spatial bounds.
    """

    try:
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

    except Exception as e:
        if logfire_instance:
            logfire_instance.error("Failed to retrieve change masks", error=str(e))
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve change masks: {str(e)}"
        )


@router.get("/change-masks/{img_a_id}/{img_b_id}")
async def get_change_mask(
    img_a_id: int, img_b_id: int, service: EoService = Depends(get_eo_service)
):
    """
    Get change detection mask binary data for a specific image pair.
    """
    # Ensure img_a_id < img_b_id per database constraint
    if img_a_id >= img_b_id:
        raise HTTPException(
            status_code=400, detail="img_a_id must be less than img_b_id"
        )

    mask_data = await service.get_change_mask_data(img_a_id, img_b_id)
    if not mask_data:
        raise HTTPException(
            status_code=404, detail="Change mask not found for the specified image pair"
        )

    return Response(
        content=mask_data,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f"attachment; filename=change_mask_{img_a_id}_{img_b_id}.bin",
            "Content-Length": str(len(mask_data)),
        },
    )


@router.post("/images/{image_id}/bands")
async def get_spectral_bands(
    image_id: int,
    request: SpectralBandsRequest,
    service: EoService = Depends(get_eo_service),
):
    """
    Get specific spectral bands for an image.
    """
    result = await service.get_spectral_bands(image_id, request.bands)
    if not result:
        raise HTTPException(status_code=404, detail="Image not found")

    return result
