# Use a simple single-stage build with optimal layer caching
FROM python:3.10-slim

WORKDIR /app

# Install system dependencies (rarely change - good for caching)
RUN apt-get update && apt-get install -y \
    gdal-bin \
    libgdal-dev \
    libspatialindex-dev \
    git \
    curl \
    ca-certificates \
    libgl1-mesa-dri \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    libgomp1 \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Set GDAL environment variables (rarely change)
ENV GDAL_CONFIG=/usr/bin/gdal-config
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# Install UV package manager (rarely change)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Copy ONLY requirements file first (changes less than code)
COPY requirements_pipeline.txt .

# Install PyTorch CPU-only (heavy, changes rarely - excellent for caching)
RUN uv pip install --system --no-cache --index-url https://download.pytorch.org/whl/cpu \
    torch torchvision

# Install all other dependencies (changes rarely - good for caching)
RUN uv pip install --system --no-cache -r requirements_pipeline.txt

# Copy application code LAST (changes most frequently)
COPY . .

# Create data directories
RUN mkdir -p /app/data/images /app/data/masks /app/data/checkpoints /app/data/logs /app/pipeline/data/hf_cache/hub

# Set environment variables
ENV PYTHONPATH="/app"
ENV CUDA_VISIBLE_DEVICES=""
# Align all HF caches under the same folder used by the controller
ENV HF_HOME="/app/pipeline/data/hf_cache"
ENV TRANSFORMERS_CACHE="/app/pipeline/data/hf_cache/hub"
ENV HUGGINGFACE_HUB_CACHE="/app/pipeline/data/hf_cache/hub"

# Expose monitoring port
EXPOSE 8080

# Default command - start in wait-for-start mode for container deployment
CMD ["python", "-m", "pipeline.controller", "--wait-for-start"]
