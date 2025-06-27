from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, select, func
from typing import List, Optional, Tuple
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class EoRepository:
    """Repository for Earth Observation data access"""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_image_count(
        self,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        bbox: Optional[Tuple[float, float, float, float]] = None,
    ) -> int:
        """Get total count of images matching criteria"""

        query = "SELECT COUNT(*) FROM eo WHERE 1=1"
        params = {}

        if start_time:
            query += " AND time >= :start_time::TIMESTAMPTZ"
            params["start_time"] = start_time

        if end_time:
            query += " AND time <= :end_time::TIMESTAMPTZ"
            params["end_time"] = end_time

        if bbox:
            min_lon, min_lat, max_lon, max_lat = bbox
            query += " AND ST_Intersects(bbox, ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)::GEOGRAPHY)"
            params.update(
                {
                    "min_lon": min_lon,
                    "min_lat": min_lat,
                    "max_lon": max_lon,
                    "max_lat": max_lat,
                }
            )

        result = await self.session.execute(text(query), params)
        return result.scalar()

    async def get_images_metadata(
        self,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[dict]:
        """Get image metadata without binary data"""

        query = """
            SELECT 
                id,
                time,
                ST_AsText(bbox) as bbox_wkt,
                COALESCE(LENGTH(b01), 0) + COALESCE(LENGTH(b02), 0) + COALESCE(LENGTH(b03), 0) +
                COALESCE(LENGTH(b04), 0) + COALESCE(LENGTH(b05), 0) + COALESCE(LENGTH(b06), 0) +
                COALESCE(LENGTH(b07), 0) + COALESCE(LENGTH(b08), 0) + COALESCE(LENGTH(b8a), 0) +
                COALESCE(LENGTH(b09), 0) + COALESCE(LENGTH(b10), 0) + COALESCE(LENGTH(b11), 0) +
                COALESCE(LENGTH(b12), 0) as size_bytes
            FROM eo 
            WHERE 1=1
        """
        params = {}

        if start_time:
            query += " AND time >= :start_time::TIMESTAMPTZ"
            params["start_time"] = start_time

        if end_time:
            query += " AND time <= :end_time::TIMESTAMPTZ"
            params["end_time"] = end_time

        if bbox:
            min_lon, min_lat, max_lon, max_lat = bbox
            query += " AND ST_Intersects(bbox, ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)::GEOGRAPHY)"
            params.update(
                {
                    "min_lon": min_lon,
                    "min_lat": min_lat,
                    "max_lon": max_lon,
                    "max_lat": max_lat,
                }
            )

        query += " ORDER BY time DESC LIMIT :limit OFFSET :offset"
        params["limit"] = limit
        params["offset"] = offset

        logger.info(f"Executing query: {query} with params: {params}")

        result = await self.session.execute(text(query), params)
        return [dict(row._mapping) for row in result]

    async def get_image_by_id(self, image_id: int) -> Optional[dict]:
        """Get single image with metadata by ID"""

        query = """
            SELECT 
                id,
                time,
                ST_AsText(bbox) as bbox_wkt,
                COALESCE(LENGTH(b01), 0) + COALESCE(LENGTH(b02), 0) + COALESCE(LENGTH(b03), 0) +
                COALESCE(LENGTH(b04), 0) + COALESCE(LENGTH(b05), 0) + COALESCE(LENGTH(b06), 0) +
                COALESCE(LENGTH(b07), 0) + COALESCE(LENGTH(b08), 0) + COALESCE(LENGTH(b8a), 0) +
                COALESCE(LENGTH(b09), 0) + COALESCE(LENGTH(b10), 0) + COALESCE(LENGTH(b11), 0) +
                COALESCE(LENGTH(b12), 0) as size_bytes,
                b02, b03, b04  -- RGB bands for preview generation
            FROM eo 
            WHERE id = :image_id
        """

        result = await self.session.execute(text(query), {"image_id": image_id})
        row = result.first()
        return dict(row._mapping) if row else None

    async def get_original_image_data(
        self, image_id: int
    ) -> Optional[Tuple[bytes, datetime]]:
        """Get original TIFF image data by ID - returns RGB bands for now"""

        query = """
            SELECT b02, b03, b04, time
            FROM eo 
            WHERE id = :image_id
        """

        result = await self.session.execute(text(query), {"image_id": image_id})
        row = result.first()

        if row and row.b02 and row.b03 and row.b04:
            # For now, we'll return the RGB bands concatenated
            # In a real implementation, you might want to create a proper GeoTIFF
            rgb_data = bytes(row.b02) + bytes(row.b03) + bytes(row.b04)
            return rgb_data, row.time
        return None

    async def health_check(self) -> bool:
        """Check database connectivity"""
        try:
            await self.session.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False

    async def get_change_masks(
        self,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        bbox: Optional[Tuple[float, float, float, float]] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[dict]:
        """Get change detection masks metadata"""

        query = """
            SELECT 
                img_a_id,
                img_b_id,
                period_start,
                period_end,
                ST_AsText(bbox) as bbox_wkt,
                LENGTH(mask) as mask_size_bytes
            FROM eo_change 
            WHERE 1=1
        """
        params = {}

        if start_time:
            query += " AND period_start >= :start_time::TIMESTAMPTZ"
            params["start_time"] = start_time

        if end_time:
            query += " AND period_end <= :end_time::TIMESTAMPTZ"
            params["end_time"] = end_time

        if bbox:
            min_lon, min_lat, max_lon, max_lat = bbox
            query += " AND ST_Intersects(bbox, ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)::GEOGRAPHY)"
            params.update(
                {
                    "min_lon": min_lon,
                    "min_lat": min_lat,
                    "max_lon": max_lon,
                    "max_lat": max_lat,
                }
            )

        query += " ORDER BY period_start DESC LIMIT :limit OFFSET :offset"
        params["limit"] = limit
        params["offset"] = offset

        result = await self.session.execute(text(query), params)
        return [dict(row._mapping) for row in result]

    async def get_change_mask_by_images(
        self, img_a_id: int, img_b_id: int
    ) -> Optional[dict]:
        """Get change mask data for a specific image pair"""

        query = """
            SELECT 
                img_a_id,
                img_b_id,
                period_start,
                period_end,
                ST_AsText(bbox) as bbox_wkt,
                LENGTH(mask) as mask_size_bytes,
                mask
            FROM eo_change 
            WHERE img_a_id = :img_a_id AND img_b_id = :img_b_id
        """

        result = await self.session.execute(
            text(query), {"img_a_id": img_a_id, "img_b_id": img_b_id}
        )
        row = result.first()
        return dict(row._mapping) if row else None

    async def get_all_bands_by_id(self, image_id: int) -> Optional[dict]:
        """Get all spectral bands for an image by ID"""

        query = """
            SELECT 
                id, time, ST_AsText(bbox) as bbox_wkt,
                b01, b02, b03, b04, b05, b06, b07, b08, b8a, b09, b10, b11, b12
            FROM eo 
            WHERE id = :image_id
        """

        result = await self.session.execute(text(query), {"image_id": image_id})
        row = result.first()
        return dict(row._mapping) if row else None
