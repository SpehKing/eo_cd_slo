#!/usr/bin/env bash
set -euo pipefail

# Startup script for the FastAPI backend

echo "🚀 Starting Sentinel-2 Image API Backend..."

# Check if virtual environment exists
if [ ! -d "../database_v3" ]; then
    echo "❌ Virtual environment not found. Please run this from the backend directory."
    exit 1
fi

# Activate virtual environment
echo "📦 Activating virtual environment..."
source ../database_v3/bin/activate
# Verify virtual environment is activated
if [[ "$VIRTUAL_ENV" != *"database_v3"* ]]; then
    echo "⚠️ Virtual environment activation may have failed."
    echo "  Expected: database_v3"
    echo "  Current: $VIRTUAL_ENV"
    # Continue anyway as we've tried to activate it
else
    echo "✅ Virtual environment activated successfully."
fi
# Install requirements if not already installed
echo "📦 Installing requirements..."
# pip install -r requirements.txt

# Load environment variables
if [ -f "../timescaledb-stack/.env" ]; then
    echo "🔧 Loading environment variables..."
    export $(grep -v '^#' ../timescaledb-stack/.env | xargs)
fi

# Set default environment variables if not set
export DB_HOST=${DB_HOST:-localhost}
export DB_PORT=${DB_PORT:-5432}
export DB_NAME=${DB_NAME:-eo_db}
export DB_USER=${DB_USER:-postgres}
export DB_PASS=${DB_PASS:-password}

echo "🔗 Database connection settings:"
echo "  Host: $DB_HOST"
echo "  Port: $DB_PORT"
echo "  Database: $DB_NAME"
echo "  User: $DB_USER"

# Start the FastAPI server
echo "🌐 Starting FastAPI server on http://localhost:8000"
echo "📖 API Documentation available at http://localhost:8000/docs"
echo ""
echo "Press Ctrl+C to stop the server"

uvicorn main:app --host 0.0.0.0 --port 8000 --reload
