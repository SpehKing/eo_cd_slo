from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime


class ImageQueryRequest(BaseModel):
    """Request model for querying images"""

    start_time: Optional[str] = Field(None, description="Start time in ISO format")
    end_time: Optional[str] = Field(None, description="End time in ISO format")
    min_lon: Optional[float] = Field(
        None, ge=-180, le=180, description="Minimum longitude"
    )
    min_lat: Optional[float] = Field(
        None, ge=-90, le=90, description="Minimum latitude"
    )
    max_lon: Optional[float] = Field(
        None, ge=-180, le=180, description="Maximum longitude"
    )
    max_lat: Optional[float] = Field(
        None, ge=-90, le=90, description="Maximum latitude"
    )
    limit: Optional[int] = Field(
        50, ge=1, le=100, description="Maximum number of results"
    )
    offset: Optional[int] = Field(0, ge=0, description="Number of results to skip")


class ImageMetadata(BaseModel):
    """Image metadata response model"""

    id: int
    time: datetime
    bbox_wkt: str
    size_bytes: int

    class Config:
        from_attributes = True


class ImageListResponse(BaseModel):
    """Response model for image list"""

    images: List[ImageMetadata]
    total: int
    limit: int
    offset: int
    has_more: bool


class HealthCheck(BaseModel):
    """Health check response model"""

    status: str
    database: str
    timestamp: datetime
