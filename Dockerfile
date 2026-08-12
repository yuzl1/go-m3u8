# Stage 1: Build Go binary
FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o go-m3u8 .

# Stage 2: Runtime with N_m3u8DL-RE + ffmpeg
FROM ubuntu:24.04

# Install ffmpeg + tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg \
        wget \
        ca-certificates \
        xz-utils \
    && rm -rf /var/lib/apt/lists/*

# Download N_m3u8DL-RE (Linux x64, statically linked)
# Update NM3U8DL_VERSION build arg to get newer releases.
# Check https://github.com/nilaoda/N_m3u8DL-RE/releases for latest version.
ARG NM3U8DL_VERSION=0.6.0-beta
RUN wget -q "https://github.com/nilaoda/N_m3u8DL-RE/releases/download/v${NM3U8DL_VERSION}/N_m3u8DL-RE_linux-x64_${NM3U8DL_VERSION}.tar.gz" \
        -O /tmp/nm3u8dl.tar.gz \
    && mkdir -p /opt/nm3u8dl \
    && tar -xzf /tmp/nm3u8dl.tar.gz -C /opt/nm3u8dl \
    && chmod +x /opt/nm3u8dl/N_m3u8DL-RE \
    && rm /tmp/nm3u8dl.tar.gz

ENV PATH="/opt/nm3u8dl:${PATH}"

# Copy Go binary
COPY --from=builder /app/go-m3u8 /usr/local/bin/go-m3u8

# Create volume mount points
RUN mkdir -p /downloads /config
VOLUME ["/downloads", "/config"]
ENV CONFIG_DIR=/config

EXPOSE 8080
ENTRYPOINT ["go-m3u8"]
