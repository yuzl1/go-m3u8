# Stage 1: Build Go binary
FROM golang:alpine AS builder
WORKDIR /app
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o go-m3u8 .

# Stage 2: Runtime with N_m3u8DL-RE + ffmpeg
FROM ubuntu:24.04

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg \
        curl \
        ca-certificates \
        xz-utils \
    && rm -rf /var/lib/apt/lists/*

# Download N_m3u8DL-RE via GitHub API with arch auto-detection
RUN set -e; \
    ARCH=$(uname -m); \
    case "$ARCH" in \
        x86_64)  ASSET_ARCH="x64" ;; \
        aarch64) ASSET_ARCH="arm64" ;; \
        *)       ASSET_ARCH="x64" ;; \
    esac; \
    curl -sL "https://api.github.com/repos/nilaoda/N_m3u8DL-RE/releases/latest" \
        -o /tmp/release.json; \
    DOWNLOAD_URL=$(grep -o '"browser_download_url": *"[^"]*linux-'${ASSET_ARCH}'[^"]*"' /tmp/release.json | head -1 | sed 's/.*"\(https:.*\)"/\1/'); \
    if [ -z "$DOWNLOAD_URL" ]; then \
        echo "WARNING: no linux-${ASSET_ARCH} build, trying linux-x64"; \
        DOWNLOAD_URL=$(grep -o '"browser_download_url": *"[^"]*linux-x64[^"]*"' /tmp/release.json | head -1 | sed 's/.*"\(https:.*\)"/\1/'); \
    fi; \
    echo "Arch: $ARCH -> asset arch: $ASSET_ARCH"; \
    echo "Downloading: $DOWNLOAD_URL"; \
    curl -sL "$DOWNLOAD_URL" -o /tmp/nm3u8dl.tar.gz; \
    mkdir -p /opt/nm3u8dl; \
    tar -xzf /tmp/nm3u8dl.tar.gz -C /opt/nm3u8dl; \
    chmod +x /opt/nm3u8dl/N_m3u8DL-RE; \
    rm /tmp/nm3u8dl.tar.gz /tmp/release.json

ENV PATH="/opt/nm3u8dl:${PATH}"

COPY --from=builder /app/go-m3u8 /usr/local/bin/go-m3u8

RUN mkdir -p /downloads /config
# Seed default config so downloads land in the mounted /downloads volume
RUN echo '{"save_dir":"/downloads","tmp_dir":"","thread_count":16,"auto_select":true,"del_after_done":true,"concurrent":true,"max_concurrent":3,"download_retry_count":5,"check_segments":true,"default_headers":{},"base_url":"","nm3u8dl_path":"N_m3u8DL-RE","port":8080}' > /config/config.json
VOLUME ["/downloads", "/config"]
ENV CONFIG_DIR=/config

EXPOSE 8080
ENTRYPOINT ["go-m3u8"]
