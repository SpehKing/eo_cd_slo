#!/usr/bin/env bash
set -euo pipefail

# EO Pipeline Management Script
# Provides easy control over the EO Change Detection Pipeline

# Use docker compose (newer) or docker-compose (legacy)
COMPOSE_CMD="docker compose"
if ! docker compose version &> /dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
fi

PIPELINE_SERVICE="eo-pipeline"
PIPELINE_CONTAINER="eo_pipeline_container"

show_usage() {
    echo "EO Pipeline Management Script"
    echo ""
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  start         Start the pipeline service"
    echo "  stop          Stop the pipeline service"
    echo "  restart       Restart the pipeline service"
    echo "  status        Show pipeline status"
    echo "  logs          Show pipeline logs"
    echo "  logs -f       Follow pipeline logs"
    echo "  monitor       Open monitoring dashboard"
    echo "  shell         Open shell in pipeline container"
    echo "  build         Build pipeline image"
    echo "  clean         Stop and remove pipeline containers/images"
    echo ""
    echo "Examples:"
    echo "  $0 start        # Start the pipeline"
    echo "  $0 logs -f      # Follow logs in real-time"
    echo "  $0 monitor      # Open http://localhost:8080"
}

case "${1:-help}" in
    start)
        echo "🚀 Starting EO Pipeline..."
        $COMPOSE_CMD up -d $PIPELINE_SERVICE
        echo "✅ Pipeline started! Monitor at: http://localhost:8080"
        ;;
    stop)
        echo "🛑 Stopping EO Pipeline..."
        $COMPOSE_CMD stop $PIPELINE_SERVICE
        echo "✅ Pipeline stopped!"
        ;;
    restart)
        echo "🔄 Restarting EO Pipeline..."
        $COMPOSE_CMD restart $PIPELINE_SERVICE
        echo "✅ Pipeline restarted!"
        ;;
    status)
        echo "📊 Pipeline Status:"
        $COMPOSE_CMD ps $PIPELINE_SERVICE
        ;;
    logs)
        if [[ "${2:-}" == "-f" ]]; then
            echo "📋 Following pipeline logs (Ctrl+C to exit)..."
            $COMPOSE_CMD logs -f $PIPELINE_SERVICE
        else
            echo "📋 Pipeline logs:"
            $COMPOSE_CMD logs $PIPELINE_SERVICE
        fi
        ;;
    monitor)
        echo "🖥️  Opening monitoring dashboard..."
        if command -v open &> /dev/null; then
            open http://localhost:8080
        elif command -v xdg-open &> /dev/null; then
            xdg-open http://localhost:8080
        else
            echo "Visit: http://localhost:8080"
        fi
        ;;
    shell)
        echo "🐚 Opening shell in pipeline container..."
        docker exec -it $PIPELINE_CONTAINER /bin/bash
        ;;
    build)
        echo "🔨 Building pipeline image..."
        $COMPOSE_CMD build $PIPELINE_SERVICE
        echo "✅ Pipeline image built!"
        ;;
    clean)
        echo "🧹 Cleaning up pipeline..."
        $COMPOSE_CMD down $PIPELINE_SERVICE
        docker rmi $(docker images -q --filter reference="*eo-pipeline*") 2>/dev/null || true
        echo "✅ Pipeline cleaned up!"
        ;;
    help|--help|-h)
        show_usage
        ;;
    *)
        echo "❌ Unknown command: $1"
        echo ""
        show_usage
        exit 1
        ;;
esac
