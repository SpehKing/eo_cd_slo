# Database Population Script

This directory contains scripts to populate the TimescaleDB database with Sentinel-2 RGB images and mask data.

## Files

- `populate_database.py` - Main Python script that handles the database population
- `populate_database.sh` - Shell wrapper script for easy execution
- `requirements.txt` - Python dependencies required by the population script
- `.env.example` - Example environment configuration file

## Setup

1. **Install Python dependencies:**

   ```bash
   pip install -r scripts/requirements.txt
   ```

2. **Configure environment (optional):**

   ```bash
   cp .env.example .env
   # Edit .env with your database configuration
   ```

3. **Ensure database is running:**
   ```bash
   docker-compose up -d
   ```

## Usage

### Option 1: Use the shell wrapper script (recommended)

```bash
./populate_database.sh
```

### Option 2: Run the Python script directly

```bash
python3 scripts/populate_database.py
```

## What the script does

1. **Waits for database** to become ready for connections
2. **Scans for TIFF files** matching the pattern `data/images/geotiffs/sentinel2_rgb_*.tif`
3. **Reads the mask file** from `data/images/masks/plus_sign_mask.tif`
4. **Extracts metadata** from filenames:
   - Timestamp (e.g., `20180605_100610` → `2018-06-05 10:06:10+00`)
   - Grid coordinates (e.g., `grid_0_1` → calculates bounding box)
5. **Separates RGB bands** and stores them in individual columns:
   - Band 1 (Red) → `b04`
   - Band 2 (Green) → `b03`
   - Band 3 (Blue) → `b02`
6. **Inserts records** into the `eo` table with proper spatial and temporal indexing

## Configuration

The script uses the following environment variables (with defaults):

- `DB_HOST` (default: `localhost`)
- `DB_PORT` (default: `5432`)
- `DB_NAME` (default: `eo_db`)
- `DB_USER` (default: `postgres`)
- `DB_PASSWORD` (default: `password`)

## Data Structure

### Input Files

- **GeoTIFF images**: `data/images/geotiffs/sentinel2_rgb_grid_{row}_{col}_{YYYYMMDD}_{HHMMSS}.tif`
- **Mask file**: `data/images/masks/plus_sign_mask.tif`

### Database Schema

Images are stored in the `eo` table with:

- `time`: Timestamp extracted from filename
- `bbox`: Calculated bounding box based on grid coordinates
- `b02`, `b03`, `b04`: Individual band data as BYTEA

## Grid System

The script uses a 3×3 grid of 5×5 km cells centered at Ljubljana (46.0569°N, 14.5058°E):

```
grid_0_0  grid_0_1  grid_0_2
grid_1_0  grid_1_1  grid_1_2
grid_2_0  grid_2_1  grid_2_2
```

Each grid cell has precise geographic boundaries calculated from the center coordinates.

## Error Handling

- The script will continue processing even if individual files fail
- Failed insertions are rolled back while successful ones are committed
- A summary is provided at the end showing success/failure counts

## Output

The script provides detailed progress information:

- Configuration summary
- Database connection status
- Per-file processing results
- Final statistics (total records, time range)
