# Single image, two roles. docker-compose picks which command to run.
FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1

# Install uv via pip — avoids depending on ghcr.io being reachable.
RUN pip install --no-cache-dir uv

WORKDIR /app

# Layer 1: deps (lockfile + project metadata only, for cache-friendliness)
COPY pyproject.toml README.md ./
RUN uv venv && uv sync --no-install-project

# Layer 2: source
COPY app ./app
COPY static ./static
RUN uv sync

ENV PATH="/app/.venv/bin:${PATH}"

EXPOSE 8000
# Default command is the API; the worker service overrides this in compose.
CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000"]
