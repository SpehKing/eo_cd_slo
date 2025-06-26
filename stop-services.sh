#!/usr/bin/env bash
set -euo pipefail

echo "🛑 Stopping EO Change Detection Services..."

# Use docker compose (newer) or docker-compose (legacy)
COMPOSE_CMD="docker compose"
if ! docker compose version &> /dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
fi

# Stop and remove containers
$COMPOSE_CMD down

echo "✅ Services stopped successfully!"
echo ""
echo "📋 Additional cleanup options:"
echo "  • Remove volumes (⚠️  deletes all data): $COMPOSE_CMD down -v"
echo "  • Remove images: docker rmi \$(docker images -q)"
echo "  • Full cleanup: $COMPOSE_CMD down -v --rmi all"
