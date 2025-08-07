#!/bin/bash
"""
EO Change Detection Pipeline Startup Script

This script provides easy commands to run the pipeline in different modes.
"""

set -e

# Default configuration
DOCKER_COMPOSE_FILE="docker-compose.yml"
CONTAINER_NAME="eo_pipeline_container"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if Docker is running
check_docker() {
    if ! docker info >/dev/null 2>&1; then
        log_error "Docker is not running. Please start Docker first."
        exit 1
    fi
}

# Function to check if required files exist
check_requirements() {
    if [ ! -f "data/slovenia_grid_expanded.gpkg" ]; then
        log_error "Required file 'data/slovenia_grid_expanded.gpkg' not found."
        log_info "Please ensure the grid file is in the data directory."
        exit 1
    fi
    
    if [ ! -f "$DOCKER_COMPOSE_FILE" ]; then
        log_error "Docker compose file '$DOCKER_COMPOSE_FILE' not found."
        exit 1
    fi
}

# Build the Docker image
build() {
    log_info "Building EO pipeline Docker image..."
    docker-compose -f $DOCKER_COMPOSE_FILE build
    log_success "Docker image built successfully"
}

# Start the pipeline in local mode
start_local() {
    log_info "Starting pipeline in LOCAL mode..."
    check_docker
    check_requirements
    
    # Set local mode environment
    export PIPELINE_MODE="local_only"
    
    docker-compose -f $DOCKER_COMPOSE_FILE up -d eo-pipeline
    log_success "Pipeline started in local mode"
    log_info "Monitoring dashboard: http://localhost:8080"
    log_info "View logs: docker logs -f $CONTAINER_NAME"
}

# Start the pipeline in database mode
start_database() {
    log_info "Starting pipeline in DATABASE mode..."
    check_docker
    check_requirements
    
    # Set database mode environment
    export PIPELINE_MODE="database_only"
    
    # Start database first
    docker-compose -f $DOCKER_COMPOSE_FILE up -d timescaledb
    log_info "Waiting for database to be ready..."
    sleep 10
    
    # Start pipeline
    docker-compose -f $DOCKER_COMPOSE_FILE up -d eo-pipeline
    log_success "Pipeline started in database mode"
    log_info "Monitoring dashboard: http://localhost:8080"
    log_info "Database: localhost:5432"
}

# Start the pipeline in hybrid mode
start_hybrid() {
    log_info "Starting pipeline in HYBRID mode..."
    check_docker
    check_requirements
    
    # Set hybrid mode environment
    export PIPELINE_MODE="hybrid"
    
    # Start database first
    docker-compose -f $DOCKER_COMPOSE_FILE up -d timescaledb
    log_info "Waiting for database to be ready..."
    sleep 10
    
    # Start pipeline
    docker-compose -f $DOCKER_COMPOSE_FILE up -d eo-pipeline
    log_success "Pipeline started in hybrid mode"
    log_info "Monitoring dashboard: http://localhost:8080"
    log_info "Database: localhost:5432"
}

# Run pipeline once and exit
run_once() {
    local mode=${1:-"local_only"}
    log_info "Running pipeline once in $mode mode..."
    check_docker
    check_requirements
    
    export PIPELINE_MODE="$mode"
    
    if [ "$mode" != "local_only" ]; then
        docker-compose -f $DOCKER_COMPOSE_FILE up -d timescaledb
        log_info "Waiting for database to be ready..."
        sleep 10
    fi
    
    docker-compose -f $DOCKER_COMPOSE_FILE run --rm eo-pipeline python -m pipeline.controller
}

# Show status
status() {
    log_info "Pipeline status:"
    if docker ps --filter "name=$CONTAINER_NAME" --format "table {{.Names}}\t{{.Status}}" | grep -q $CONTAINER_NAME; then
        docker ps --filter "name=$CONTAINER_NAME" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
        echo
        log_info "Getting detailed status..."
        docker exec $CONTAINER_NAME python -m pipeline.controller --status 2>/dev/null || log_warning "Could not get detailed status"
    else
        log_warning "Pipeline container is not running"
    fi
}

# Show logs
logs() {
    local follow=${1:-false}
    if [ "$follow" = "true" ] || [ "$follow" = "-f" ]; then
        docker logs -f $CONTAINER_NAME
    else
        docker logs $CONTAINER_NAME
    fi
}

# Stop the pipeline
stop() {
    log_info "Stopping pipeline..."
    docker-compose -f $DOCKER_COMPOSE_FILE down
    log_success "Pipeline stopped"
}

# Stop and remove everything including volumes
clean() {
    log_warning "This will remove all pipeline data and volumes!"
    read -p "Are you sure? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker-compose -f $DOCKER_COMPOSE_FILE down -v
        docker system prune -f
        log_success "Pipeline cleaned"
    else
        log_info "Clean cancelled"
    fi
}

# Retry failed tasks
retry() {
    log_info "Retrying failed tasks..."
    if docker ps --filter "name=$CONTAINER_NAME" --format "{{.Names}}" | grep -q $CONTAINER_NAME; then
        docker exec $CONTAINER_NAME python -m pipeline.controller --retry-failed
        log_success "Failed tasks reset"
    else
        log_error "Pipeline container is not running"
        exit 1
    fi
}

# Open monitoring dashboard
monitor() {
    log_info "Opening monitoring dashboard..."
    if command -v open >/dev/null; then
        open http://localhost:8080
    elif command -v xdg-open >/dev/null; then
        xdg-open http://localhost:8080
    else
        log_info "Please open http://localhost:8080 in your browser"
    fi
}

# Shell into container
shell() {
    log_info "Opening shell in pipeline container..."
    docker exec -it $CONTAINER_NAME /bin/bash
}

# Show help
help() {
    echo "EO Change Detection Pipeline Control Script"
    echo ""
    echo "Usage: $0 <command> [options]"
    echo ""
    echo "Commands:"
    echo "  build                 Build the Docker image"
    echo "  start-local          Start pipeline in local storage mode"
    echo "  start-database       Start pipeline in database mode"
    echo "  start-hybrid         Start pipeline in hybrid mode"
    echo "  run-once [mode]      Run pipeline once and exit (mode: local_only|database_only|hybrid)"
    echo "  status               Show pipeline status"
    echo "  logs [-f]            Show logs (use -f to follow)"
    echo "  stop                 Stop the pipeline"
    echo "  clean                Stop and remove all data (destructive!)"
    echo "  retry                Retry failed tasks"
    echo "  monitor              Open monitoring dashboard in browser"
    echo "  shell                Open shell in pipeline container"
    echo "  help                 Show this help"
    echo ""
    echo "Examples:"
    echo "  $0 build                    # Build the image"
    echo "  $0 start-local              # Start in local mode"
    echo "  $0 run-once local_only      # Run once in local mode"
    echo "  $0 status                   # Check status"
    echo "  $0 logs -f                  # Follow logs"
    echo ""
}

# Main command dispatcher
case "${1:-help}" in
    build)
        build
        ;;
    start-local)
        start_local
        ;;
    start-database)
        start_database
        ;;
    start-hybrid)
        start_hybrid
        ;;
    run-once)
        run_once "${2:-local_only}"
        ;;
    status)
        status
        ;;
    logs)
        logs "$2"
        ;;
    stop)
        stop
        ;;
    clean)
        clean
        ;;
    retry)
        retry
        ;;
    monitor)
        monitor
        ;;
    shell)
        shell
        ;;
    help|--help|-h)
        help
        ;;
    *)
        log_error "Unknown command: $1"
        echo ""
        help
        exit 1
        ;;
esac
