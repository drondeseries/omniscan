# Stage 1: Builder
FROM python:3.11-slim as builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential gcc && \
    rm -rf /var/lib/apt/lists/*

# Install and compile dependencies into wheels
COPY requirements.txt .
RUN pip wheel --no-cache-dir --wheel-dir /app/wheels -r requirements.txt


# Stage 2: Runner
FROM python:3.11-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg curl && \
    rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN groupadd -g 1000 omniscan && \
    useradd -u 1000 -g omniscan -m -s /bin/bash omniscan

# Copy wheels from builder and install
COPY --from=builder /app/wheels /wheels
COPY --from=builder /app/requirements.txt .
RUN pip install --no-cache-dir /wheels/*

# Copy the package source code and assets
COPY omniscan_pkg /app/omniscan_pkg
COPY assets /app/assets

# Create config directory and set permissions
RUN mkdir -p /app/config && \
    chown -R omniscan:omniscan /app

LABEL org.opencontainers.image.title="Omniscan"
LABEL org.opencontainers.image.description="Advanced media library health checker and scanner for Plex, Jellyfin, and Emby"

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

# Set the working directory to config for execution
WORKDIR /app/config

# Set python path so it can find the package
ENV PYTHONPATH=/app

# Run as non-root user
USER omniscan

CMD ["python", "-m", "omniscan_pkg.main"]
