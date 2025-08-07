# Use Python 3.10 with CUDA support for GPU processing
FROM pytorch/pytorch:2.3.0-cuda12.1-cudnn8-runtime

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gdal-bin \
    libgdal-dev \
    libspatialindex-dev \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set GDAL environment variables
ENV GDAL_DATA=/usr/share/gdal
ENV PROJ_LIB=/usr/share/proj

# Copy requirements first for better caching
COPY requirements_pipeline.txt requirements.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements_pipeline.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire cluster directory
COPY . .

# Create data directories
RUN mkdir -p /app/data/images /app/data/masks /app/data/checkpoints /app/data/logs

# Set environment variables
ENV PYTHONPATH=/app
ENV DATA_DIR=/app/data
ENV PIPELINE_MODE=local_only
ENV MAX_WORKERS=4
ENV MEMORY_LIMIT_GB=4
ENV LOG_LEVEL=INFO
ENV MONITORING_PORT=8080

# Expose monitoring port
EXPOSE 8080

# Create non-root user for security
RUN useradd -m -u 1000 pipeline && \
    chown -R pipeline:pipeline /app
USER pipeline

# Default command
CMD ["python", "-m", "pipeline.controller"]
