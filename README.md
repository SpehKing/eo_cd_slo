# EO Change Detection Slovenia - Docker Deployment

A comprehensive Earth Observation Change Detection system for Slovenia using Sentinel-2 satellite imagery, built with TimescaleDB, FastAPI, Vue.js, and a machine learning pipeline.

## 🏗️ System Architecture

The system consists of 4 Docker containers:

### 1. **TimescaleDB Container** (`timescaledb`)

- **Purpose**: PostgreSQL database with TimescaleDB extension for time-series geospatial data
- **Image**: `timescale/timescaledb-ha:pg17`
- **Port**: `5432`
- **Data**: Stores Sentinel-2 imagery, metadata, and change detection results
- **Features**: Hypertables for efficient time-series queries, PostGIS for spatial operations

### 2. **FastAPI Backend** (`fastapi`)

- **Purpose**: REST API server for accessing imagery and change detection data
- **Framework**: Python FastAPI with async database connections
- **Port**: `8000`
- **Features**: Auto-generated API docs, image retrieval, change mask APIs
- **Dependencies**: Connects to TimescaleDB for data access

### 3. **EO Pipeline** (`eo-pipeline`)

- **Purpose**: Machine learning pipeline for change detection processing
- **Port**: `8080` (monitoring dashboard)
- **Features**: Sentinel-2 data download, BTC model inference, web-based monitoring
- **Modes**: `local_only`, `database_only`, `hybrid`

### 4. **Vue.js Frontend** (`frontend`)

- **Purpose**: Web interface for visualizing imagery and change detection results
- **Framework**: Vue 3 with interactive map components
- **Port**: `3000` (development) / `80` (production)
- **Features**: Interactive map, time-series visualization, change comparison

---

## 🚀 Quick Start (Production)

### 1. Clone and Setup

```bash
git clone <repository-url>
cd eo_cd_slo

# Create required directories for server deployment
mkdir -p /opt/eo_stack/{backups,db-init}
```

### 2. Database Backup Preparation

https://unilj-my.sharepoint.com/:u:/g/personal/ga58402_student_uni-lj_si/EW2o4NMPUStCn8VXEj2kG8IBtmldkaH-R0_0M0FWiX-Law?e=En7h2v

Transfer your database backup to the server:

```bash
# Copy your .bak file to the backups directory
cp eo_db.bak /opt/eo_stack/backups/eo_db.bak

# Create database restore script
cat > /opt/eo_stack/db-init/10-restore.sh << 'EOF'
#!/usr/bin/env bash
set -euo pipefail

echo "[restore] Bootstrapping database ${POSTGRES_DB:-postgres}"

# Wait until Postgres is accepting connections
until pg_isready -U "${POSTGRES_USER}" -d "${POSTGRES_DB}"; do
    sleep 1
done

# Ensure Timescale extension and pre-restore state
psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" <<SQL
CREATE EXTENSION IF NOT EXISTS timescaledb;
SELECT timescaledb_pre_restore();
SQL

# Do the restore
if [ -f /backups/eo_db.bak ]; then
    echo "[restore] Restoring /backups/eo_db.bak into ${POSTGRES_DB}"
    pg_restore -v -Fc -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" /backups/eo_db.bak
else
    echo "[restore] ERROR: /backups/eo_db.bak not found" >&2
    exit 1
fi

# Post-restore hooks + analyze
psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" <<SQL
SELECT timescaledb_post_restore();
VACUUM ANALYZE;
SQL

echo "[restore] Done."
EOF

chmod +x /opt/eo_stack/db-init/10-restore.sh
```

### 3. Environment Configuration

Create the backend environment file:

```bash
# Create backend .env file
cat > backend/.env << 'EOF'
# Database Configuration
EO_CD_DB_HOST=timescaledb
EO_CD_DB_PORT=5432
EO_CD_DB_NAME=eo_db
EO_CD_DB_USER=postgres
EO_CD_DB_PASSWORD=password

# Logging Configuration
EO_CD_LOG_LEVEL=INFO
EO_CD_ENABLE_LOGFIRE=false

# API Configuration
EO_CD_API_TITLE=Sentinel-2 Image API
EO_CD_API_VERSION=1.0.0
EO_CD_ALLOWED_ORIGINS=["*"]

# Image Processing Configuration
EO_CD_MAX_IMAGE_SIZE=1024
EO_CD_JPEG_QUALITY=85
EOF
```

### 4. Start Production Services

```bash
# Ensure clean database initialization
docker compose -f compose-server.yml down
docker volume rm eo_cd_slo_timescale_data || true

# Start the database first (it will auto-restore from backup)
docker compose -f compose-server.yml up -d timescaledb

# Watch logs until you see "[restore] Done."
docker logs -f timescaledb_container

# Verify database restore
docker exec -it timescaledb_container psql -U postgres -d eo_db \
  -c "SELECT COUNT(*) FROM public.eo; SELECT COUNT(*) FROM public.eo_change;"

# Start all services
docker compose -f compose-server.yml up -d
```

### 5. Access Services

- **API Documentation**: http://localhost:8000/docs
- **Pipeline Dashboard**: http://localhost:8080
- **Frontend Application**: http://localhost:3000
- **Database**: `postgresql://postgres:password@localhost:5432/eo_db`

---

## 🛠️ Local Development

For local development with hot-reload and volume mounting:

```bash
# Use the development compose file
docker compose -f compose.yaml up --build -d

# View logs
docker compose -f compose.yaml logs -f

# Stop services
docker compose -f compose.yaml down
```

**Key Differences from Production:**

- Volume mounts for live code editing
- `--reload` flags for auto-restart
- Uses `compose.yaml` instead of `compose-server.yml`
- Builds images locally instead of using pre-built images

---

## 🗃️ Database Management

### Creating Backups

```bash
# Create a new backup
docker exec -i timescaledb_container bash -lc \
  'pg_dump -U postgres -d eo_db -Fc -f /tmp/eo_db.bak'

# Copy backup from container
docker cp timescaledb_container:/tmp/eo_db.bak ./eo_db.bak
```

### Restoring from Backup

```bash
# Stop services and remove existing data
docker compose -f compose-server.yml down
docker volume rm eo_cd_slo_timescale_data

# Ensure backup is in the correct location
cp eo_db.bak /opt/eo_stack/backups/eo_db.bak

# Start database (auto-restore will run)
docker compose -f compose-server.yml up -d timescaledb
```

### Manual Database Operations

```bash
# Connect to database
docker exec -it timescaledb_container psql -U postgres -d eo_db

# Check table sizes
docker exec -it timescaledb_container psql -U postgres -d eo_db \
  -c "SELECT schemaname,tablename,pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) FROM pg_tables WHERE schemaname='public';"

# View recent images
docker exec -it timescaledb_container psql -U postgres -d eo_db \
  -c "SELECT id, time, grid_id, width, height FROM eo ORDER BY time DESC LIMIT 10;"
```

---

## 🖥️ GPU Support (Advanced)

For accelerated change detection processing with NVIDIA GPUs:

### Prerequisites

```bash
# Install NVIDIA Container Toolkit
distribution=$(. /etc/os-release;echo $ID$VERSION_ID)
curl -s -L https://nvidia.github.io/nvidia-docker/gpgkey | sudo apt-key add -
curl -s -L https://nvidia.github.io/nvidia-docker/$distribution/nvidia-docker.list | sudo tee /etc/apt/sources.list.d/nvidia-docker.list

sudo apt-get update && sudo apt-get install -y nvidia-docker2
sudo systemctl restart docker
```

### Enable GPU in Compose

Edit your compose file and uncomment GPU sections:

```yaml
# In compose-server.yml or compose.yaml
eo-pipeline:
  # ... other config ...
  runtime: nvidia
  environment:
    - NVIDIA_VISIBLE_DEVICES=all
    # ... other env vars ...
```

### Verify GPU Access

```bash
# Check GPU availability in container
docker exec -it eo_pipeline_container nvidia-smi

# Monitor GPU usage during processing
watch -n 1 nvidia-smi
```

---

## 📊 Monitoring and Logs

### Service Status

```bash
# Check all services
docker compose -f compose-server.yml ps

# Health check
docker compose -f compose-server.yml logs --tail=50
```

### Individual Service Logs

```bash
# Database logs
docker compose -f compose-server.yml logs -f timescaledb

# API logs
docker compose -f compose-server.yml logs -f fastapi

# Pipeline logs
docker compose -f compose-server.yml logs -f eo-pipeline

# Frontend logs
docker compose -f compose-server.yml logs -f frontend
```

### Pipeline Monitoring

Visit the pipeline dashboard at http://localhost:8080 to:

- ✅ **Start/Stop** pipeline execution
- 📊 **Monitor** real-time progress
- ⚙️ **Configure** processing parameters
- 🔄 **Retry** failed tasks
- 📈 **View** system resources

---

## 🔧 Configuration

### Environment Variables

#### Database Configuration

```bash
EO_CD_DB_HOST=timescaledb          # Database hostname
EO_CD_DB_PORT=5432                 # Database port
EO_CD_DB_NAME=eo_db               # Database name
EO_CD_DB_USER=postgres            # Database user
EO_CD_DB_PASSWORD=password        # Database password (change in production!)
```

#### Pipeline Configuration

```bash
PIPELINE_MODE=database_only        # local_only, database_only, hybrid
MAX_WORKERS=4                      # Parallel processing workers
MEMORY_LIMIT_GB=4                  # Memory limit per worker
BTC_MODEL_CHECKPOINT=blaz-r/BTC-B_oscd96  # Hugging Face model
BTC_THRESHOLD=0.2                  # Change detection threshold
```

#### API Configuration

```bash
EO_CD_API_TITLE="Sentinel-2 Image API"
EO_CD_API_VERSION="1.0.0"
EO_CD_LOG_LEVEL=INFO
EO_CD_MAX_IMAGE_SIZE=1024
EO_CD_JPEG_QUALITY=85
```

### Security Notes

🔒 **Important**: Change the default database password in production:

1. Update `POSTGRES_PASSWORD` in compose file
2. Update `EO_CD_DB_PASSWORD` in backend/.env
3. Update password in all database connection strings

---

## 🚨 Troubleshooting

### Common Issues

#### Database Connection Issues

```bash
# Check if database is ready
docker exec timescaledb_container pg_isready -U postgres -d eo_db

# Check database logs
docker logs timescaledb_container

# Verify extensions
docker exec -it timescaledb_container psql -U postgres -d eo_db \
  -c "SELECT * FROM pg_extension;"
```

#### API Not Responding

```bash
# Check API health
curl http://localhost:8000/docs

# Check API logs
docker logs fastapi_container

# Restart API service
docker compose -f compose-server.yml restart fastapi
```

#### Pipeline Issues

```bash
# Check pipeline dashboard
curl http://localhost:8080

# Access pipeline container
docker exec -it eo_pipeline_container /bin/bash

# Check GPU access (if enabled)
docker exec -it eo_pipeline_container nvidia-smi
```

#### Out of Disk Space

```bash
# Check Docker space usage
docker system df

# Clean up unused resources
docker system prune -f

# Remove unused volumes (⚠️ deletes data)
docker volume prune -f
```

### Performance Optimization

#### For Large Datasets

```bash
# Increase shared memory for PostgreSQL
# Add to compose file under timescaledb service:
shm_size: 256m

# Adjust worker memory limits
MEMORY_LIMIT_GB=8
MAX_WORKERS=2  # Reduce workers if memory limited
```

#### For GPU Processing

```bash
# Monitor GPU memory
watch -n 1 nvidia-smi

# Adjust batch sizes in pipeline if GPU memory limited
# Check pipeline logs for memory errors
```

---

## 🔄 Maintenance

### Regular Maintenance Tasks

```bash
# 1. Database vacuum and analyze (weekly)
docker exec -it timescaledb_container psql -U postgres -d eo_db \
  -c "VACUUM ANALYZE;"

# 2. Docker cleanup (weekly)
docker system prune -f
docker image prune -f

# 3. Check disk space
df -h
docker system df

# 4. Update containers (as needed)
docker compose -f compose-server.yml pull
docker compose -f compose-server.yml up -d
```

### Backup Strategy

```bash
# Daily backup script
#!/bin/bash
DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="/opt/eo_stack/backups/eo_db_${DATE}.bak"

docker exec -i timescaledb_container bash -lc \
  "pg_dump -U postgres -d eo_db -Fc -f /tmp/backup_${DATE}.bak"

docker cp timescaledb_container:/tmp/backup_${DATE}.bak $BACKUP_FILE

# Keep only last 7 days of backups
find /opt/eo_stack/backups -name "eo_db_*.bak" -mtime +7 -delete
```

---

## 📚 Additional Resources

- **API Documentation**: http://localhost:8000/docs (when running)
- **Pipeline Dashboard**: http://localhost:8080 (when running)
- **TimescaleDB Docs**: https://docs.timescale.com/
- **FastAPI Docs**: https://fastapi.tiangolo.com/
- **Vue.js Docs**: https://vuejs.org/

---

## 🤝 Support

For issues and questions:

1. Check logs using the troubleshooting section above
2. Verify all prerequisites are installed
3. Ensure sufficient disk space and memory
4. Check Docker and Docker Compose versions

---

_Last updated: September 2025_
