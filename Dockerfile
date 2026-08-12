# Stage 1: Build Go binary
FROM golang:alpine AS builder
WORKDIR /app
COPY . .
RUN ls -la /app/internal/config/ && cat /app/go.mod && go env GOMOD
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o go-m3u8 .

# Stage 2: Runtime with N_m3u8DL-RE + ffmpeg
FROM ubuntu:24.04

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg \
        wget \
        ca-certificates \
        xz-utils \
    && rm -rf /var/lib/apt/lists/*

ARG NM3U8DL_VERSION=0.6.0-beta
RUN wget -q "https://github.com/nilaoda/N_m3u8DL-RE/releases/download/v${NM3U8DL_VERSION}/N_m3u8DL-RE_linux-x64_${NM3U8DL_VERSION}.tar.gz" \
        -O /tmp/nm3u8dl.tar.gz \
    && mkdir -p /opt/nm3u8dl \
    && tar -xzf /tmp/nm3u8dl.tar.gz -C /opt/nm3u8dl \
    && chmod +x /opt/nm3u8dl/N_m3u8DL-RE \
    && rm /tmp/nm3u8dl.tar.gz

ENV PATH="/opt/nm3u8dl:${PATH}"

COPY --from=builder /app/go-m3u8 /usr/local/bin/go-m3u8

RUN mkdir -p /downloads /config
VOLUME ["/downloads", "/config"]
ENV CONFIG_DIR=/config

EXPOSE 8080
ENTRYPOINT ["go-m3u8"]
