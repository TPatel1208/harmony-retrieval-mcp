FROM python:3.11-slim

# System dependencies for rasterio, shapely, psycopg, pyproj
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libgdal-dev \
    libproj-dev \
    libgeos-dev \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (layer cache)
COPY pyproject.toml ./
RUN pip install --no-cache-dir -e ".[dev]"

# Copy source
COPY src/ ./src/
COPY tests/ ./tests/
COPY sql/ ./sql/

CMD ["python", "-m", "harmony_retrieval_mcp.server"]
