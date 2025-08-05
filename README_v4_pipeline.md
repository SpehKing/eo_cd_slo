# Sentinel-2 Download and Insert Pipeline v4

This v4 pipeline addresses bbox alignment issues by ensuring perfect pixel alignment between downloaded images and grid cell boundaries using EPSG:4326 consistently throughout the process.

## Key Improvements in v4

1. **Consistent CRS Usage**: Uses EPSG:4326 throughout the entire pipeline
2. **Perfect Grid Alignment**: Zero tolerance for bbox rounding/approximation
3. **OpenEO Integration**: Uses OpenEO for reliable satellite data downloads
4. **Enhanced Database Schema**: Improved validation and alignment checking
5. **Slovenia Grid Support**: Works with the expanded Slovenia grid from `slovenia_grid_expanded.gpkg`

## Components

### 1. Download Script (`download_sentinel_v4.py`)

Downloads Sentinel-2 images using OpenEO with perfect grid alignment:

- Uses exact grid boundaries from `slovenia_grid_expanded.gpkg`
- Forces EPSG:4326 CRS consistently
- Implements pixel-perfect alignment with `resample_spatial()`
- Downloads RGB bands (B02, B03, B04) for specified grid cells and years
- Includes comprehensive validation and logging

**Key Features:**

- Zero tolerance bbox calculations
- Exact coordinate preservation (no rounding)
- OpenEO median time aggregation for cloud-free composites
- Comprehensive download statistics and validation

### 2. Insert Script (`insert_sentinel_v4.py`)

Inserts downloaded images into TimescaleDB with perfect alignment validation:

- Validates exact bbox alignment (99.9% overlap requirement)
- Uses exact grid boundaries for database consistency
- Extracts and stores band data as bytes
- Includes comprehensive error handling and logging

**Key Features:**

- Zero tolerance bbox validation
- Exact grid boundary enforcement
- Comprehensive alignment checking
- Band data extraction and storage

### 3. Database Schema (`02_schema_v4.sql`)

Enhanced TimescaleDB schema with perfect alignment support:

- **Grid Cells Table**: Stores exact Slovenia grid boundaries
- **EO Table**: Enhanced with perfect alignment validation
- **Change Detection Table**: Supports aligned image pairs
- **Validation Functions**: Comprehensive alignment checking
- **Helper Functions**: Coverage and alignment statistics

**Key Features:**

- 99.9% overlap requirement for bbox validation
- Exact grid boundary enforcement functions
- Alignment validation and statistics
- Enhanced temporal partitioning

## Usage

### Prerequisites

```bash
pip install openeo geopandas rasterio tqdm numpy psycopg2-binary
```

### 1. Download Images

```bash
cd scripts
python download_sentinel_v4.py
```

**Configuration:**

- `GRID_IDS`: Target grid cell IDs from Slovenia grid
- `YEARS`: Years to download (currently 2024)
- `DOWNLOAD_DIR`: Output directory for downloaded images
- `GRID_FILE`: Path to Slovenia grid GeoPackage

### 2. Start Database

```bash
docker-compose up -d
```

### 3. Insert Images

```bash
cd scripts
python insert_sentinel_v4.py
```

The script will:

- Load grid cells into the database
- Process all downloaded TIFF files
- Validate perfect bbox alignment
- Insert image data with exact grid boundaries

### 4. Validate Alignment

Connect to the database and run alignment validation:

```sql
-- Check alignment for specific grid
SELECT * FROM validate_grid_alignment(531);

-- Get overall alignment statistics
SELECT * FROM get_alignment_stats();

-- Get coverage with alignment metrics
SELECT * FROM get_coverage_stats_v4();
```

## Configuration

### Grid Configuration

The pipeline uses the Slovenia grid from `grid_output/slovenia_grid_expanded.gpkg`:

- Grid cells are identified by index (grid_id)
- Exact boundaries are preserved in EPSG:4326
- No coordinate rounding or approximation

### Download Configuration

```python
# Target grid cells
GRID_IDS = [531, 532, 533, 567, 568, 569, 603, 604, 605]

# Time period
YEARS = list(range(2024, 2025))  # Adjust as needed

# CRS consistency
TARGET_CRS = "EPSG:4326"
PIXEL_SIZE = 0.00009  # ~10m in degrees
```

### Database Configuration

```python
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "eo_db",
    "user": "postgres",
    "password": "password",
}
```

## Alignment Validation

The v4 pipeline includes comprehensive alignment validation:

### 1. Download Validation

- Checks downloaded image CRS and bounds
- Validates reasonable bbox alignment
- Logs detailed coordinate information

### 2. Insert Validation

- Requires 99.9% overlap between image and grid bbox
- Uses exact grid boundaries for database storage
- Comprehensive alignment logging

### 3. Database Validation

- `validate_grid_alignment()`: Check specific grid alignment
- `get_alignment_stats()`: Overall alignment statistics
- `get_coverage_stats_v4()`: Coverage with alignment metrics

## Troubleshooting

### Common Issues

1. **Bbox Alignment Failures**

   - Check that grid file is in EPSG:4326
   - Verify OpenEO download CRS consistency
   - Review alignment validation logs

2. **Download Failures**

   - Check OpenEO authentication
   - Verify grid cell coordinates
   - Review rate limiting settings

3. **Database Connection Issues**
   - Ensure TimescaleDB container is running
   - Check database configuration
   - Verify PostGIS extensions are installed

### Debugging

Enable debug logging for detailed information:

```python
logging.basicConfig(level=logging.DEBUG)
```

### Force Exact Boundaries

To enforce exact grid boundaries in the database:

```sql
SELECT enforce_exact_grid_boundaries();
```

This function updates all image bboxes to use exact grid cell boundaries.

## File Structure

```
scripts/
├── download_sentinel_v4.py    # OpenEO download script
├── insert_sentinel_v4.py      # Database insertion script
└── populate_database.py       # Original reference script

timescaledb-stack/db/docker-entrypoint-initdb.d/
├── 01_schema.sql              # Original schema
└── 02_schema_v4.sql           # Enhanced v4 schema

data/
├── images/
│   └── sentinel_downloads_v4/  # Downloaded images
└── grid_output/
    └── slovenia_grid_expanded.gpkg  # Slovenia grid

logs/
├── download_sentinel_v4.log   # Download logs
└── insert_sentinel_v4.log     # Insert logs
```

## Performance Considerations

- **Rate Limiting**: 2-second delay between OpenEO requests
- **Batch Processing**: Images processed individually for error isolation
- **Memory Management**: Band data stored as bytes for efficiency
- **Indexing**: Optimized spatial and temporal indices

## Next Steps

1. Run the download script for your target grid cells and years
2. Start the TimescaleDB container with the v4 schema
3. Run the insert script to populate the database
4. Use the validation functions to verify perfect alignment
5. Extend the pipeline for additional bands or time periods as needed

The v4 pipeline ensures perfect bbox alignment while maintaining the flexibility and robustness needed for operational satellite data processing.
