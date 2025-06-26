#!/usr/bin/env bash
set -euo pipefail

echo "🐳 Starting EO Change Detection Services..."
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker Desktop first."
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null 2>&1; then
    echo "❌ Docker Compose is not available. Please install Docker Compose."
    exit 1
fi

# Use docker compose (newer) or docker-compose (legacy)
COMPOSE_CMD="docker compose"
if ! docker compose version &> /dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
fi

echo "📦 Building and starting services..."
echo "  • TimescaleDB (Database): http://localhost:5432"
echo "  • FastAPI Backend: http://localhost:8000"
echo "  • API Documentation: http://localhost:8000/docs"
echo ""

# Build and start services
$COMPOSE_CMD up --build -d

echo ""
echo "⏳ Waiting for services to be ready..."

# Wait for database to be ready
echo "  Checking TimescaleDB..."
for i in {1..30}; do
    if docker exec timescaledb_container pg_isready -U postgres -d eo_db > /dev/null 2>&1; then
        echo "  ✅ TimescaleDB is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "  ❌ TimescaleDB failed to start after 30 attempts"
        $COMPOSE_CMD logs timescaledb
        exit 1
    fi
    sleep 2
done

# Wait for FastAPI to be ready
echo "  Checking FastAPI..."
for i in {1..30}; do
    if curl -s http://localhost:8000/docs > /dev/null 2>&1; then
        echo "  ✅ FastAPI is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "  ❌ FastAPI failed to start after 30 attempts"
        $COMPOSE_CMD logs fastapi
        exit 1
    fi
    sleep 2
done

echo ""
echo "🎉 All services are running successfully!"
echo ""
echo "🔗 Service URLs:"
echo "  • API Documentation: http://localhost:8000/docs"
echo "  • API Interactive: http://localhost:8000/redoc"
echo "  • Database (external): postgresql://postgres:password@localhost:5432/eo_db"
echo ""
echo "📋 Useful commands:"
echo "  • View logs: $COMPOSE_CMD logs -f"
echo "  • Stop services: $COMPOSE_CMD down"
echo "  • Restart services: $COMPOSE_CMD restart"
echo "  • View service status: $COMPOSE_CMD ps"
echo ""
echo "Press Ctrl+C to stop watching logs, or run '$COMPOSE_CMD logs -f' to see logs"

# Follow logs
$COMPOSE_CMD logs -f
