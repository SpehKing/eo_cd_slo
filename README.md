# Sentinel-2 Image Viewer

A FastAPI backend and HTML frontend for browsing and downloading Sentinel-2 satellite images stored in Times### Frontend Usage

1. **Set Filters**:
   - Use date/time inputs for temporal filtering
   - Use coordinate inputs for spatial filtering
   - Adjust max results as needed
2. **Search**: Click "🔍 Search Images" to find matching images
3. **View/Download**:

   - Click "👁️ View" to see image details
   - Click "⬇️ Download" to download the TIFF file directly from base64 data Prerequisites

4. **TimescaleDB Stack**: Make sure your TimescaleDB container is running with the loaded Sentinel-2 images
5. **Python Virtual Environment**: The `database_v3` virtual environment should be available

## Quick Start

### 1. Start the Database (if not already running)

```bash
cd timescaledb-stack
docker-compose up -d
```

### 2. Start the Backend API

```bash
cd backend
./start.sh
```

The API will be available at:

- Main API: http://localhost:8000
- Interactive docs: http://localhost:8000/docs
- Alternative docs: http://localhost:8000/redoc

### 3. Open the Frontend

Open `frontend/index.html` in your web browser, or serve it with a simple HTTP server:

```bash
cd frontend
python -m http.server 8080
# Then open http://localhost:8080
```

## Features

### Backend API Endpoint

- `POST /api/v1/public/changes` - Query satellite image changes with filters:
  - Request body (JSON):
    ```json
    {
      "start_time": "YYYY-MM-DD HH:MM:SS" (optional),
      "end_time": "YYYY-MM-DD HH:MM:SS" (optional),
      "min_lon": float (optional),
      "min_lat": float (optional),
      "max_lon": float (optional),
      "max_lat": float (optional),
      "limit": integer (optional, default: 50)
    }
    ```
  - Response: Array of images with metadata and base64-encoded TIFF data

### Frontend Features

- **Search Interface**: Filter images by time range and geographic bounding box
- **Image Preview**: View image metadata and details with base64-encoded data included
- **Download**: Download TIFF files directly from the browser using base64 data
- **Responsive Design**: Works on desktop and mobile devices

## Configuration

### Environment Variables

The backend uses these environment variables (with defaults):

```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=eo_db
DB_USER=postgres
DB_PASS=password
```

You can set these in:

1. A `.env` file in the `timescaledb-stack` directory
2. Environment variables in your shell
3. The defaults will be used if not specified

### Default Geographic Bounds

The frontend is pre-configured with the same geographic bounds used in your data loading script:

- Min Longitude: 28.726330
- Min Latitude: 41.248773
- Max Longitude: 28.759632
- Max Latitude: 41.274581

## Usage Examples

### API Usage with curl

```bash
# Query images with time filter
curl -X POST "http://localhost:8000/api/v1/public/changes" \
  -H "Content-Type: application/json" \
  -d '{
    "start_time": "2024-06-01 00:00:00",
    "end_time": "2024-07-01 00:00:00",
    "limit": 10
  }'

# Query with geographic filter
curl -X POST "http://localhost:8000/api/v1/public/changes" \
  -H "Content-Type: application/json" \
  -d '{
    "min_lon": 28.726,
    "min_lat": 41.248,
    "max_lon": 28.759,
    "max_lat": 41.274,
    "limit": 5
  }'

# Query with both time and location filters
curl -X POST "http://localhost:8000/api/v1/public/changes" \
  -H "Content-Type: application/json" \
  -d '{
    "start_time": "2023-01-01 00:00:00",
    "end_time": "2024-12-31 23:59:59",
    "min_lon": 28.726330,
    "min_lat": 41.248773,
    "max_lon": 28.759632,
    "max_lat": 41.274581,
    "limit": 20
  }'
```

### Frontend Usage

1. **Load Statistics**: Click "📊 Load Statistics" to see database overview
2. **Set Filters**:
   - Use date/time inputs for temporal filtering
   - Use coordinate inputs for spatial filtering
   - Adjust max results as needed
3. **Search**: Click "🔍 Search Images" to find matching images
4. **View/Download**:
   - Click "👁️ View" to see image details
   - Click "⬇️ Download" to download the TIFF file

## Technical Details

### Backend Architecture

- **FastAPI**: Modern, fast web framework for building APIs
- **psycopg2**: PostgreSQL adapter for Python
- **CORS enabled**: Allows frontend to access the API from different origins
- **Async support**: Efficient handling of concurrent requests

### Frontend Architecture

- **Vanilla JavaScript**: No frameworks, just modern ES6+ features
- **CSS Grid/Flexbox**: Responsive layout that works on all screen sizes
- **Fetch API**: Modern HTTP client for API communication
- **Modal dialogs**: For image preview and details

### Database Integration

The system works with your existing TimescaleDB schema:

- Queries the `eo` table with time-series and spatial indexing
- Uses PostGIS for spatial operations
- Leverages TimescaleDB's time-series capabilities for efficient time-based queries

## Troubleshooting

### Backend Issues

1. **Database connection failed**: Ensure TimescaleDB container is running and accessible
2. **Import errors**: Make sure all requirements are installed in the virtual environment
3. **Permission errors**: Check that the database user has SELECT permissions on the `eo` table

### Frontend Issues

1. **CORS errors**: Make sure the backend is running and CORS is enabled
2. **No images found**: Check your time and location filters
3. **Download failures**: Ensure the backend can access the database and read the image data

### Common Solutions

```bash
# Restart the database
cd timescaledb-stack
docker-compose restart

# Reinstall backend dependencies
cd backend
source ../database_v3/bin/activate
pip install -r requirements.txt --force-reinstall

# Check database connectivity
docker exec timescaledb_db pg_isready -U postgres
```

## Data Format

The system expects TIFF images stored as BYTEA in the database with:

- `time`: Timestamp with timezone
- `bbox`: PostGIS geography polygon
- `image`: Binary TIFF data

Images are served as downloadable TIFF files that can be opened with:

- QGIS
- GDAL tools
- ArcGIS
- Other GIS software
- Image viewers that support TIFF format
