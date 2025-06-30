from pydantic import BaseModel, Field
from typing import Optional, List, Dict
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
    width: Optional[int] = Field(None, description="Image width in pixels")
    height: Optional[int] = Field(None, description="Image height in pixels")
    data_type: Optional[str] = Field(
        None, description="Image data type (e.g., 'uint16', 'float32')"
    )
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


class ChangeMaskMetadata(BaseModel):
    """Change detection mask metadata response model"""

    img_a_id: int
    img_b_id: int
    period_start: datetime
    period_end: datetime
    bbox_wkt: str
    mask_size_bytes: int

    class Config:
        from_attributes = True


class ChangeMaskListResponse(BaseModel):
    """Response model for change mask list"""

    masks: List[ChangeMaskMetadata]
    total: int
    limit: int
    offset: int
    has_more: bool


class SpectralBandsRequest(BaseModel):
    """Request model for specific spectral bands"""

    bands: List[str] = Field(
        default=["b02", "b03", "b04"],
        description="List of band names (b01-b12, b8a)",
    )

    class Config:
        schema_extra = {"example": {"bands": ["b02", "b03", "b04"]}}  # RGB bands


class SpectralBandsResponse(BaseModel):
    """Response model for spectral bands"""

    id: int
    time: datetime
    bbox_wkt: str
    bands: Dict[str, str] = Field(
        description="Dictionary of band names to base64-encoded binary data"
    )

    class Config:
        schema_extra = {
            "example": {
                "id": 1,
                "time": "2021-07-28T10:07:54Z",
                "bbox_wkt": "POLYGON((...))",
                "bands": {
                    "b02": "iVBORw0KGgoAAAANSUhEUgAA...",
                    "b03": "iVBORw0KGgoAAAANSUhEUgAA...",
                    "b04": "iVBORw0KGgoAAAANSUhEUgAA...",
                },
            }
        }
