from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel
from typing import Optional, List
import psycopg2
import psycopg2.extras
import os
from datetime import datetime
import base64
import io
from PIL import Image
import rasterio
from rasterio.enums import Resampling
import numpy as np

app = FastAPI(title="Sentinel-2 Image API", version="1.0.0")

# Enable CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "eo_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "password"),
}


class QueryRequest(BaseModel):
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    min_lon: Optional[float] = None
    min_lat: Optional[float] = None
    max_lon: Optional[float] = None
    max_lat: Optional[float] = None
    limit: Optional[int] = 50


class ImageResult(BaseModel):
    time: datetime
    bbox_wkt: str
    image_id: int
    image_data: str
    size_bytes: int


def convert_tiff_to_jpg(tiff_data: bytes) -> bytes:
    """
    Convert TIFF image data to JPG format for web display
    """
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
            print(f"Rasterio failed, trying PIL: {rasterio_error}")
            tiff_io.seek(0)  # Reset stream position
            pil_image = Image.open(tiff_io)

            # Convert to RGB if necessary
            if pil_image.mode in ("RGBA", "LA"):
                # Convert RGBA to RGB by compositing over white background
                background = Image.new("RGB", pil_image.size, (255, 255, 255))
                if pil_image.mode == "RGBA":
                    background.paste(
                        pil_image, mask=pil_image.split()[-1]
                    )  # Use alpha channel as mask
                else:
                    background.paste(
                        pil_image, mask=pil_image.split()[-1]
                    )  # Use alpha channel as mask
                pil_image = background
            elif pil_image.mode not in ("RGB", "L"):
                pil_image = pil_image.convert("RGB")

        # Resize if image is too large (to improve performance)
        max_size = (1024, 1024)
        if pil_image.size[0] > max_size[0] or pil_image.size[1] > max_size[1]:
            pil_image.thumbnail(max_size, Image.Resampling.LANCZOS)

        # Convert to JPG
        jpg_io = io.BytesIO()
        pil_image.save(jpg_io, format="JPEG", quality=85, optimize=True)
        jpg_data = jpg_io.getvalue()

        return jpg_data

    except Exception as e:
        print(f"Error converting TIFF to JPG: {str(e)}")
        # Return a placeholder image or raise an exception
        raise Exception(f"Failed to convert TIFF to JPG: {str(e)}")


def get_db_connection():
    """Get database connection"""
    try:
        print(f"Connecting with config: {DB_CONFIG}")
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Database connection failed: {str(e)}"
        )


@app.post("/api/v1/public/changes", response_model=List[ImageResult])
async def get_changes(request: QueryRequest):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        # Debug: Which DB are we connected to?
        cursor.execute("SELECT current_database() AS db, current_user AS user")
        db_info = cursor.fetchone()
        print(f"Connected to database: {db_info['db']}, user: {db_info['user']}")

        # Debug: How many rows in 'eo' table total?
        cursor.execute("SELECT COUNT(*) AS total_rows FROM eo")
        row_count = cursor.fetchone()
        print(f"Table 'eo' has {row_count['total_rows']} total rows")

        # Debug: What’s the min & max time in 'eo'?
        cursor.execute("SELECT MIN(time) AS min_t, MAX(time) AS max_t FROM eo")
        times = cursor.fetchone()
        print(f"Min time: {times['min_t']}, Max time: {times['max_t']}")

        # Now build the real query...
        query = """
            SELECT 
                time,
                ST_AsText(bbox) as bbox_wkt,
                ROW_NUMBER() OVER (ORDER BY time) as image_id,
                image
            FROM eo 
            WHERE 1=1
        """
        params = []

        if request.start_time:
            # Convert to TIMESTAMPTZ format
            start_ts = request.start_time
            if not start_ts.endswith("+00") and not start_ts.endswith("Z"):
                start_ts += "+00"
            query += " AND time >= %s::TIMESTAMPTZ"
            params.append(start_ts)
            print(f"Added start_time filter: {start_ts}")

        if request.end_time:
            # Convert to TIMESTAMPTZ format
            end_ts = request.end_time
            if not end_ts.endswith("+00") and not end_ts.endswith("Z"):
                end_ts += "+00"
            query += " AND time <= %s::TIMESTAMPTZ"
            params.append(end_ts)
            print(f"Added end_time filter: {end_ts}")

        if all(
            coord is not None
            for coord in [
                request.min_lon,
                request.min_lat,
                request.max_lon,
                request.max_lat,
            ]
        ):
            query += " AND ST_Intersects(bbox, ST_MakeEnvelope(%s, %s, %s, %s, 4326)::GEOGRAPHY)"
            params.extend(
                [request.min_lon, request.min_lat, request.max_lon, request.max_lat]
            )
            print(
                f"Added spatial filter: {request.min_lon}, {request.min_lat}, {request.max_lon}, {request.max_lat}"
            )

        query += " ORDER BY time DESC LIMIT %s"
        params.append(request.limit or 50)

        print(f"Final query: {query}")
        print(f"Final params: {params}")

        cursor.execute(query, params)
        results = cursor.fetchall()
        print(f"Query returned {len(results)} rows")

        # Convert results to include base64 encoded image data as JPG
        response_data = []
        for row in results:
            try:
                # Convert TIFF to JPG
                jpg_data = convert_tiff_to_jpg(row["image"])
                image_b64 = base64.b64encode(jpg_data).decode("utf-8")
                image_data_url = f"data:image/jpeg;base64,{image_b64}"

                response_data.append(
                    ImageResult(
                        time=row["time"],
                        bbox_wkt=row["bbox_wkt"],
                        image_id=row["image_id"],
                        image_data=image_data_url,
                        size_bytes=len(jpg_data),
                    )
                )
            except Exception as e:
                print(f"Failed to convert image {row['image_id']}: {str(e)}")
                # Include the image with empty data but keep the metadata
                response_data.append(
                    ImageResult(
                        time=row["time"],
                        bbox_wkt=row["bbox_wkt"],
                        image_id=row["image_id"],
                        image_data="",  # Empty data to indicate conversion failed
                        size_bytes=len(row["image"]),
                    )
                )

        return response_data

    except Exception as e:
        print(f"Database error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Query failed: {str(e)}")
    finally:
        cursor.close()
        conn.close()


@app.get("/api/v1/public/image/{image_id}/original")
async def get_original_image(image_id: int):
    """
    Download the original TIFF image data for a specific image
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        # Query to get the original image data by reconstructed image_id
        # Since image_id is ROW_NUMBER() in the main query, we need to use the same logic
        query = """
            WITH numbered_images AS (
                SELECT 
                    time,
                    ST_AsText(bbox) as bbox_wkt,
                    ROW_NUMBER() OVER (ORDER BY time DESC) as image_id,
                    image
                FROM eo 
            )
            SELECT image, time
            FROM numbered_images 
            WHERE image_id = %s
        """

        cursor.execute(query, (image_id,))
        result = cursor.fetchone()

        if not result:
            raise HTTPException(status_code=404, detail="Image not found")

        # Return the original TIFF data
        image_data = result["image"]
        timestamp = result["time"].strftime("%Y%m%d_%H%M%S")
        filename = f"sentinel2_{timestamp}_original.tif"

        return Response(
            content=bytes(image_data),
            media_type="image/tiff",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Length": str(len(image_data)),
            },
        )

    except Exception as e:
        print(f"Error fetching original image: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch image: {str(e)}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
