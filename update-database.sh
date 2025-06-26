#!/usr/bin/env bash
set -euo pipefail

echo "🔄 Updating Database Container..."
echo ""

# Use docker compose (newer) or docker-compose (legacy)
COMPOSE_CMD="docker compose"
if ! docker compose version &> /dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
fi

echo "🛑 Stopping and removing current database container..."
$COMPOSE_CMD stop timescaledb
$COMPOSE_CMD rm -f timescaledb

echo "🗑️ Removing database volume (this will delete all data)..."
docker volume rm eo_cd_slo_timescaledb_data 2>/dev/null || echo "  Volume doesn't exist or already removed"

echo "🏗️ Rebuilding and starting database container..."
$COMPOSE_CMD up --build -d timescaledb

echo "⏳ Waiting for database to be ready..."
for i in {1..30}; do
    if docker exec timescaledb_container pg_isready -U postgres -d eo_db > /dev/null 2>&1; then
        echo "✅ Database is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "❌ Database failed to start after 30 attempts"
        $COMPOSE_CMD logs timescaledb
        exit 1
    fi
    sleep 2
done

echo ""
echo "🎉 Database container updated successfully!"
echo ""
echo "📋 You can now:"
echo "  • Check database logs: $COMPOSE_CMD logs timescaledb"
echo "  • Connect to database: docker exec -it timescaledb_container psql -U postgres -d eo_db"
echo "  • Start FastAPI if not running: $COMPOSE_CMD up -d fastapi"
echo ""