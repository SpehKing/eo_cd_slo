# EO Change Detection Pipeline

A comprehensive, resumable pipeline for Earth Observation change detection using Sentinel-2 imagery and the BTC (Be The Change) neural network model.

## 🌟 Features

- **Async Processing**: Efficient async pipeline processing one year at a time across all grid cells
- **Resumable Execution**: Automatic checkpoint management with simple JSON-based state tracking
- **Real-time Monitoring**: Web dashboard with live progress updates and alerting
- **Flexible Storage**: Support for local storage, database storage, or hybrid modes
- **Dynamic BTC Masks**: Generates change detection masks using the BTC neural network model
- **Docker Integration**: Complete containerized environment with GPU support
- **Independent Modules**: Each component can run independently or as part of the pipeline

## 🏗️ Architecture

The pipeline consists of three main stages:

1. **Download**: Downloads Sentinel-2 imagery using OpenEO for specified grid cells and years
2. **Insert/Store**: Stores images locally or inserts into TimescaleDB database
3. **BTC Processing**: Generates change detection masks using the BTC model for consecutive year pairs

### Directory Structure

```
cluster/
├── pipeline/
│   ├── __init__.py
│   ├── controller.py          # Main pipeline orchestrator
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py        # Configuration management
│   ├── modules/
│   │   ├── __init__.py
│   │   ├── download.py        # Sentinel-2 downloader
│   │   ├── insert.py          # Database/storage handler
│   │   └── btc_processor.py   # BTC change detection
│   └── utils/
│       ├── __init__.py
│       ├── state_manager.py   # Checkpoint management
│       └── monitor.py         # Real-time monitoring
├── Dockerfile
├── docker-compose.yml
├── pipeline.sh               # Control script
├── requirements_pipeline.txt
└── data/                     # Data directory (created automatically)
    ├── images/               # Downloaded images by year
    ├── masks/                # Generated change masks by year
    ├── checkpoints/          # Pipeline state files
    └── logs/                 # Log files
```

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- NVIDIA GPU support (optional, for faster BTC processing)
- Grid file: `data/slovenia_grid_expanded.gpkg`

### 1. Build the Pipeline

```bash
cd cluster
./pipeline.sh build
```

### 2. Run the Pipeline

**Local Storage Mode** (recommended for development):

```bash
./pipeline.sh start-local
```

**Database Mode** (for production):

```bash
./pipeline.sh start-database
```

**Run Once and Exit**:

```bash
./pipeline.sh run-once local_only
```

### 3. Monitor Progress

Open the monitoring dashboard:

```bash
./pipeline.sh monitor
# Or visit: http://localhost:8080
```

View logs:

```bash
./pipeline.sh logs -f
```

Check status:

```bash
./pipeline.sh status
```

## ⚙️ Configuration

### Environment Variables

```bash
# Processing mode
PIPELINE_MODE=local_only        # local_only, database_only, hybrid

# Resource limits
MAX_WORKERS=4                   # CPU cores
MEMORY_LIMIT_GB=4              # Memory limit

# Data processing
DATA_DIR=/app/data             # Base data directory

# BTC Model
BTC_MODEL_CHECKPOINT=blaz-r/BTC-B_oscd96
BTC_THRESHOLD=0.2              # Change detection threshold

# Monitoring
MONITORING_PORT=8080           # Web dashboard port
LOG_LEVEL=INFO                 # DEBUG, INFO, WARNING, ERROR

# Database (for database mode)
DB_HOST=timescaledb
DB_PORT=5432
DB_NAME=eo_db
DB_USER=postgres
DB_PASSWORD=password
```

### Pipeline Configuration

Edit `pipeline/config/settings.py` to customize:

- Target grid IDs and years
- Processing parameters
- OpenEO settings
- BTC model configuration

```python
# Example configuration
grid_ids = [463, 464, 465, 466, 467, 468]
years = [2022, 2023, 2024]
btc_threshold = 0.2
max_workers = 4
```

## 📁 Storage Modes

### Local Storage Mode

- **Images**: `data/images/{year}/sentinel2_grid_{grid_id}_{year}_08.tiff`
- **Masks**: `data/masks/{year}/change_mask_grid_{grid_id}_{year}.png`
- **Metadata**: JSON files alongside images and masks
- **Checkpoints**: `data/checkpoints/{stage}_{year}.json`

### Database Mode

- **Images**: Stored as BYTEA in TimescaleDB `eo` table
- **Masks**: Stored as BYTEA in TimescaleDB `eo_change` table
- **Metadata**: Database tables with full PostGIS support
- **Checkpoints**: Same JSON files for pipeline state

### Hybrid Mode

- Local storage for development/debugging
- Database storage for production queries
- Automatic synchronization between both

## 🔄 Resumable Execution

The pipeline automatically creates checkpoints for each stage and year:

```json
{
  "stage_name": "download",
  "year": 2023,
  "total_tasks": 6,
  "completed_tasks": 4,
  "failed_tasks": 1,
  "skipped_tasks": 0,
  "tasks": {
    "download_463_2023": {
      "status": "completed",
      "started_at": "2024-01-01T10:00:00",
      "completed_at": "2024-01-01T10:15:00"
    }
  }
}
```

### Resume Operations

**Automatic Resume** (default):

```bash
./pipeline.sh start-local
```

**Fresh Start** (ignore checkpoints):

```bash
./pipeline.sh run-once local_only --no-resume
```

**Retry Failed Tasks**:

```bash
./pipeline.sh retry
```

## 📊 Monitoring Dashboard

Access the real-time monitoring dashboard at `http://localhost:8080`:

### Features

- **Overall Progress**: Pipeline completion percentage
- **Stage Progress**: Detailed progress for each stage and year
- **Real-time Updates**: WebSocket-based live updates
- **Error Tracking**: Failed task monitoring and retry capabilities
- **Resource Monitoring**: System resource usage
- **Log Viewer**: Real-time log streaming

### Control Actions

- Start/Stop/Pause pipeline
- Retry failed tasks
- View detailed logs
- Export progress reports

## 🧩 Independent Module Usage

Each module can be used independently:

### Download Module

```python
from pipeline.modules.download import SentinelDownloaderV5

downloader = SentinelDownloaderV5()
await downloader.process_year(2023)
```

### Insert Module

```python
from pipeline.modules.insert import SentinelInserterV5

inserter = SentinelInserterV5()
await inserter.process_year(2023)
```

### BTC Processor

```python
from pipeline.modules.btc_processor import BTCProcessorV5

processor = BTCProcessorV5()
await processor.process_year(2023)
```

## 🐳 Docker Usage

### Build and Run

```bash
# Build image
docker build -t eo-pipeline .

# Run with volume mounts
docker run -d \
  -p 8080:8080 \
  -v $(pwd)/data:/app/data \
  -e PIPELINE_MODE=local_only \
  eo-pipeline
```

### GPU Support

Uncomment GPU configuration in `docker-compose.yml`:

```yaml
runtime: nvidia
environment:
  - NVIDIA_VISIBLE_DEVICES=all
```

## 📝 Pipeline Control Script

The `pipeline.sh` script provides easy management:

```bash
# Build and deployment
./pipeline.sh build              # Build Docker image
./pipeline.sh start-local        # Start in local mode
./pipeline.sh start-database     # Start in database mode

# Monitoring and control
./pipeline.sh status             # Show status
./pipeline.sh logs -f            # Follow logs
./pipeline.sh monitor            # Open dashboard
./pipeline.sh stop               # Stop pipeline

# Maintenance
./pipeline.sh retry              # Retry failed tasks
./pipeline.sh clean              # Remove all data (destructive!)
./pipeline.sh shell              # Open container shell
```

## 🔧 Development

### Local Development Setup

```bash
# Install dependencies
pip install -r requirements_pipeline.txt
pip install -r requirements.txt

# Set environment
export PYTHONPATH=/path/to/cluster
export DATA_DIR=/path/to/data
export PIPELINE_MODE=local_only

# Run pipeline
python -m pipeline.controller
```

### Adding New Modules

1. Create module in `pipeline/modules/`
2. Implement async processing methods
3. Add state management integration
4. Update main controller
5. Add tests and documentation

### Testing

```bash
# Run individual modules
python -m pipeline.controller --status
python -m pipeline.controller --retry-failed
python -m pipeline.controller --monitor-only

# Test with single year
# Edit config to process only one year for testing
```

## 🚨 Troubleshooting

### Common Issues

**OpenEO Authentication Issues**:

- Ensure you can authenticate to OpenEO manually
- Check internet connection
- Verify OpenEO service availability

**GPU Memory Issues**:

- Reduce `MAX_WORKERS` or `MEMORY_LIMIT_GB`
- Check GPU memory usage: `nvidia-smi`
- Consider CPU-only mode for development

**Database Connection Issues**:

- Verify database is running: `docker logs timescaledb_container`
- Check network connectivity
- Ensure correct credentials

**File Permission Issues**:

- Check volume mount permissions
- Ensure user has write access to data directory
- Consider running with proper user mapping

### Log Analysis

```bash
# View all logs
./pipeline.sh logs

# Filter specific stage logs
docker exec eo_pipeline_container cat /app/data/logs/download.log

# Monitor system resources
docker stats eo_pipeline_container
```

## 📄 License

This project is part of the EO Change Detection system. See main repository for license information.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add comprehensive tests
4. Update documentation
5. Submit a pull request

## 📞 Support

For issues and questions:

- Check the monitoring dashboard for pipeline status
- Review logs for detailed error information
- Open an issue in the main repository
- Check the troubleshooting section above
