FROM python:3.11-slim
WORKDIR /app
# Install dependencies, download and install a secure static build of FFmpeg/FFprobe (>=8.1.2) to mitigate CVE-2026-8461
RUN apt-get update && apt-get install -y --no-install-recommends curl xz-utils ca-certificates && \
    arch=$(uname -m) && \
    if [ "$arch" = "x86_64" ]; then \
        url="https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-n8.1-latest-linux64-gpl-8.1.tar.xz"; \
    elif [ "$arch" = "aarch64" ] || [ "$arch" = "arm64" ]; then \
        url="https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-n8.1-latest-linuxarm64-gpl-8.1.tar.xz"; \
    else \
        echo "Unsupported architecture: $arch" && exit 1; \
    fi && \
    curl -L "$url" -o ffmpeg.tar.xz && \
    mkdir ffmpeg-temp && \
    tar -xf ffmpeg.tar.xz -C ffmpeg-temp --strip-components=1 && \
    mv ffmpeg-temp/bin/ffmpeg /usr/local/bin/ && \
    mv ffmpeg-temp/bin/ffprobe /usr/local/bin/ && \
    rm -rf ffmpeg.tar.xz ffmpeg-temp && \
    apt-get purge -y xz-utils && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*
RUN groupadd -g 1000 omniscan && useradd -u 1000 -g omniscan -m -s /bin/bash omniscan
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY omniscan_pkg /app/omniscan_pkg
COPY assets /app/assets
RUN mkdir -p /app/config && chown -R omniscan:omniscan /app
WORKDIR /app/config
ENV PYTHONPATH=/app
USER omniscan
CMD ["python", "-m", "omniscan_pkg.main"]
