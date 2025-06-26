#!/usr/bin/env bash
set -euo pipefail

echo "🔄 Updating FastAPI Container..."
echo ""

# Use docker compose (newer) or docker-compose (legacy)
COMPOSE_CMD="docker compose"
if ! docker compose version &> /dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
fi

echo "🛑 Stopping current FastAPI container..."
$COMPOSE_CMD stop fastapi

echo "🏗️ Rebuilding FastAPI container..."
$COMPOSE_CMD build fastapi

echo "🚀 Starting updated FastAPI container..."
$COMPOSE_CMD up -d fastapi

echo "⏳ Waiting for FastAPI to be ready..."
for i in {1..30}; do
    if curl -s http://localhost:8000/docs > /dev/null 2>&1; then
        echo "✅ FastAPI is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "❌ FastAPI failed to start after 30 attempts"
        echo "📋 Checking logs..."
        $COMPOSE_CMD logs fastapi
        exit 1
    fi
    sleep 2
done

echo ""
echo "🎉 FastAPI container updated successfully!"
echo ""
echo "🔗 Service URLs:"
echo "  • API Documentation: http://localhost:8000/docs"
echo "  • API Interactive: http://localhost:8000/redoc"
echo "  • Health Check: http://localhost:8000/"
echo ""
echo "📋 You can now:"
echo "  • Check API logs: $COMPOSE_CMD logs -f fastapi"
echo "  • Test API: curl http://localhost:8000/docs"
echo ""