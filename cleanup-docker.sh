#!/usr/bin/env bash
set -euo pipefail

echo "🧹 Docker Cleanup Script"
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker Desktop first."
    exit 1
fi

# Show current Docker space usage
echo "📊 Current Docker space usage:"
docker system df

echo ""
echo "🗑️ Cleaning up Docker resources..."

# Remove unused containers
echo "  Removing stopped containers..."
docker container prune -f > /dev/null 2>&1 || true

# Remove unused images
echo "  Removing unused images..."
docker image prune -f > /dev/null 2>&1 || true

# Remove unused networks
echo "  Removing unused networks..."
docker network prune -f > /dev/null 2>&1 || true

# Remove unused volumes (be careful with this one)
echo "  Removing unused volumes..."
docker volume prune -f > /dev/null 2>&1 || true

# Remove build cache older than 24 hours
echo "  Removing old build cache..."
docker builder prune -f --filter "until=24h" > /dev/null 2>&1 || true

# More aggressive cleanup (uncomment if you want to remove ALL unused resources)
# echo "  Performing aggressive cleanup..."
# docker system prune -af --volumes > /dev/null 2>&1 || true

echo ""
echo "📊 Docker space usage after cleanup:"
docker system df

echo ""
echo "✅ Docker cleanup completed!"
echo ""
echo "💡 If you want more aggressive cleanup, you can run:"
echo "   docker system prune -af --volumes"
echo "   (Warning: This removes ALL unused images, containers, networks, and volumes)"
