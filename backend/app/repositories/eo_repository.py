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
                ROW_NUMBER() OVER (ORDER BY time DESC) + :offset as id,
                time,
                ST_AsText(bbox) as bbox_wkt,
                LENGTH(image) as size_bytes
            FROM eo 
            WHERE 1=1
        """
        params = {"offset": offset}

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
            WITH numbered_images AS (
                SELECT 
                    ROW_NUMBER() OVER (ORDER BY time DESC) as id,
                    time,
                    ST_AsText(bbox) as bbox_wkt,
                    LENGTH(image) as size_bytes,
                    image
                FROM eo 
            )
            SELECT * FROM numbered_images WHERE id = :image_id
        """

        result = await self.session.execute(text(query), {"image_id": image_id})
        row = result.first()
        return dict(row._mapping) if row else None

    async def get_original_image_data(
        self, image_id: int
    ) -> Optional[Tuple[bytes, datetime]]:
        """Get original TIFF image data by ID"""

        query = """
            WITH numbered_images AS (
                SELECT 
                    ROW_NUMBER() OVER (ORDER BY time DESC) as id,
                    time,
                    image
                FROM eo 
            )
            SELECT image, time FROM numbered_images WHERE id = :image_id
        """

        result = await self.session.execute(text(query), {"image_id": image_id})
        row = result.first()

        if row:
            return bytes(row.image), row.time
        return None

    async def health_check(self) -> bool:
        """Check database connectivity"""
        try:
            await self.session.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False
