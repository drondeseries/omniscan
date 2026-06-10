FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg curl && rm -rf /var/lib/apt/lists/*
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
