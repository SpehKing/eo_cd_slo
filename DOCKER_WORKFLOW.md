# EO Change Detection - Docker Workflow

This guide explains how to run the EO Change Detection system using Docker containers.

## Architecture

The system consists of two separate Docker containers:

1. **TimescaleDB Container**: PostgreSQL database with TimescaleDB extension
2. **FastAPI Container**: Python backend API server

Both containers communicate through a dedicated Docker network.

## Prerequisites

- Docker Desktop installed and running
- Docker Compose installed (usually comes with Docker Desktop)

## Quick Start

### 1. Start All Services

```bash
./start-services.sh
```

This script will:

- Build both Docker containers
- Start the services in the correct order
- Wait for services to be ready
- Show service URLs and useful commands

### 2. Access the Services

- **API Documentation**: http://localhost:8000/docs
- **API Interactive Docs**: http://localhost:8000/redoc
- **Database**: postgresql://postgres:password@localhost:5432/eo_db

### 3. Stop All Services

```bash
./stop-services.sh
```

## Manual Docker Commands

If you prefer to run commands manually:

### Build and Start Services

```bash
docker compose up --build -d
```

### View Logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f timescaledb
docker compose logs -f fastapi
```

### Stop Services

```bash
docker compose down
```

### Full Cleanup (removes volumes and data)

```bash
docker compose down -v --rmi all
```

## Development Workflow

### File Structure

```
.
├── docker-compose.yml          # Main orchestration file
├── .env                        # Environment variables
├── start-services.sh          # Quick start script
├── stop-services.sh           # Quick stop script
├── backend/
│   ├── Dockerfile             # FastAPI container definition
│   ├── requirements.txt       # Python dependencies
│   ├── main.py               # FastAPI application
│   └── .dockerignore         # Files to ignore in Docker build
└── timescaledb-stack/
    ├── Dockerfile            # TimescaleDB container definition
    ├── compose.yaml          # Legacy compose file (not used)
    └── db/
        └── docker-entrypoint-initdb.d/
            └── 01_schema.sql # Database initialization

```

### Making Changes

1. **Backend Code Changes**:

   - The container is set up with volume mounting for development
   - Changes to Python files will automatically reload the server
   - If you add new dependencies, rebuild: `docker compose up --build fastapi`

2. **Database Schema Changes**:
   - Modify files in `timescaledb-stack/db/docker-entrypoint-initdb.d/`
   - Restart the database: `docker compose restart timescaledb`
   - For major changes, you might need to remove the volume: `docker compose down -v`

### Troubleshooting

#### Check Service Status

```bash
docker compose ps
```

#### View Detailed Logs

```bash
# All services with timestamps
docker compose logs -f -t

# Specific service
docker compose logs -f timescaledb
docker compose logs -f fastapi
```

#### Restart Individual Services

```bash
docker compose restart timescaledb
docker compose restart fastapi
```

#### Connect to Database

```bash
# Using docker exec
docker exec -it timescaledb_container psql -U postgres -d eo_db

# Using external client
psql postgresql://postgres:password@localhost:5432/eo_db
```

#### Connect to FastAPI Container

```bash
docker exec -it fastapi_container /bin/bash
```

## Environment Variables

All configuration is handled through environment variables defined in `.env`:

- `DB_HOST`: Database hostname (set to container name for inter-container communication)
- `DB_PORT`: Database port (5432)
- `DB_NAME`: Database name (eo_db)
- `DB_USER`: Database user (postgres)
- `DB_PASSWORD`: Database password

## Production Deployment

For production deployment:

1. Remove the volume mount in `docker-compose.yml` for the FastAPI service
2. Remove the `--reload` flag from the FastAPI container command
3. Use production-grade secrets management instead of `.env` file
4. Consider using a container orchestration platform like Kubernetes

## Network Communication

The containers communicate through a dedicated Docker network (`eo_network`):

- TimescaleDB container is accessible at hostname `timescaledb` from within the network
- FastAPI container connects to the database using this internal hostname
- External access is provided through port mapping (5432 for DB, 8000 for API)
