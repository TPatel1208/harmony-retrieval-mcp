FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app

# Install dependencies first for layer caching. src/ is needed because the
# project is installed editable (hatchling reads the package from src/).
COPY pyproject.toml ./
COPY src/ ./src/
RUN uv pip install --system -e ".[dev]"

COPY tests/ ./tests/

# Default: run the MCP server. docker-compose may override (see comments there).
CMD ["python", "-m", "earthdata_mcp.server"]
