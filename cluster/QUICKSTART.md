# Quick Start Guide

## 🚀 Getting Started in 5 Minutes

This guide will get you running the EO Change Detection Pipeline quickly.

### Step 1: Validate Setup

```bash
cd cluster
python test_setup.py
```

This will check if all components are properly configured. Fix any issues reported.

### Step 2: Configure the Pipeline

Edit `pipeline/config/settings.py` to customize:

```python
# Basic configuration for testing
grid_ids = [463, 464]  # Start with just 2 grids
years = [2023, 2024]   # Test with recent years
btc_threshold = 0.2    # Change detection sensitivity
max_workers = 2        # Adjust based on your system
```

### Step 3: Build and Run

**Option A: Docker (Recommended)**

```bash
# Build the container
./pipeline.sh build

# Start in local storage mode (good for testing)
./pipeline.sh start-local

# Monitor progress
./pipeline.sh monitor
# Open http://localhost:8080 in your browser
```

**Option B: Local Development**

```bash
# Install dependencies
pip install -r requirements_pipeline.txt
pip install -r requirements.txt

# Set environment
export PYTHONPATH=/Users/gasper/Documents/ViCos/eo_cd_slo/cluster
export DATA_DIR=/Users/gasper/Documents/ViCos/eo_cd_slo/cluster/data
export PIPELINE_MODE=local_only

# Run the pipeline
python -m pipeline.controller
```

### Step 4: Monitor Progress

**Web Dashboard**: Visit `http://localhost:8080`

- See real-time progress
- Monitor failed tasks
- View system resources

**Command Line**:

```bash
# Check status
./pipeline.sh status

# View logs
./pipeline.sh logs -f

# Retry failed tasks
./pipeline.sh retry
```

### Step 5: Access Results

**Local Mode Results**:

```
data/
├── images/2023/sentinel2_grid_463_2023_08.tiff
├── images/2024/sentinel2_grid_463_2024_08.tiff
├── masks/2024/change_mask_grid_463_2024.png
└── checkpoints/download_2023.json
```

**Generated Files**:

- **Images**: Original Sentinel-2 GeoTIFF files
- **Masks**: PNG change detection masks (white = change, black = no change)
- **Checkpoints**: JSON files for resuming interrupted runs
- **Metadata**: JSON files with processing information

## 🎯 Processing Flow

1. **Download** (Year 2023): Downloads Sentinel-2 images for all grid cells
2. **Download** (Year 2024): Downloads Sentinel-2 images for all grid cells
3. **Insert** (Year 2023): Stores/processes 2023 images
4. **Insert** (Year 2024): Stores/processes 2024 images
5. **BTC Processing** (2023→2024): Generates change masks comparing consecutive years

## 🛠️ Common Commands

```bash
# Pipeline Control
./pipeline.sh build              # Build Docker image
./pipeline.sh start-local        # Start with local storage
./pipeline.sh start-database     # Start with database storage
./pipeline.sh stop               # Stop the pipeline
./pipeline.sh status             # Show current status

# Monitoring
./pipeline.sh monitor            # Open web dashboard
./pipeline.sh logs               # View logs
./pipeline.sh logs -f            # Follow logs in real-time

# Maintenance
./pipeline.sh retry              # Retry failed tasks
./pipeline.sh clean              # Remove all data (⚠️  destructive)
./pipeline.sh shell              # Access container shell

# One-time runs
./pipeline.sh run-once local_only     # Run once and exit
./pipeline.sh run-once database_only  # Run with database storage
```

## 🔧 Troubleshooting

**Import Errors**:

```bash
# Make sure you're in the cluster directory
cd /Users/gasper/Documents/ViCos/eo_cd_slo/cluster

# Test the setup
python test_setup.py
```

**Permission Issues**:

```bash
# Fix script permissions
chmod +x pipeline.sh test_setup.py

# Check data directory permissions
ls -la data/
```

**Docker Issues**:

```bash
# Check Docker is running
docker ps

# Rebuild if needed
./pipeline.sh clean
./pipeline.sh build
```

**Out of Memory**:

```bash
# Reduce workers in settings.py
max_workers = 1
memory_limit_gb = 2

# Or set environment variable
export MAX_WORKERS=1
```

## 📊 Expected Output

For a successful run with 2 grids and 2 years, you should see:

```
Download Stage (2023): 2/2 tasks completed
Download Stage (2024): 2/2 tasks completed
Insert Stage (2023): 2/2 tasks completed
Insert Stage (2024): 2/2 tasks completed
BTC Stage (2024): 2/2 tasks completed

Total: 10/10 tasks completed (100%)
```

Generated files:

- 4 TIFF images (2 grids × 2 years)
- 2 change masks (2 grids × 1 year pair)
- Multiple checkpoint files
- Processing logs

## 🎁 What's Next?

1. **Scale Up**: Add more grid IDs and years in `settings.py`
2. **Database Mode**: Use `./pipeline.sh start-database` for production
3. **Customize**: Modify BTC threshold, processing parameters
4. **Monitor**: Use the web dashboard for large runs
5. **Integrate**: Use the APIs to integrate with other systems

## 💡 Tips

- Start small (2-3 grids) to test the setup
- Use local mode for development, database mode for production
- The pipeline is resumable - you can stop and restart anytime
- Monitor disk space when processing many grids/years
- GPU processing is much faster for BTC stage if available

Happy processing! 🛰️
