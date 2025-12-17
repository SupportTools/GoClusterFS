# GoClusterFS Multi-Node Test Image
# Multi-stage build for minimal runtime image

# =============================================================================
# Stage 1: Builder - Compile the goclusterfs-mount binary
# =============================================================================
FROM golang:1.21-bookworm AS builder

WORKDIR /build

# Copy go mod files first for layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the mount binary
RUN CGO_ENABLED=0 GOOS=linux go build -o goclusterfs-mount ./cmd/mount

# =============================================================================
# Stage 2: Runtime - Minimal image with FUSE and network tools
# =============================================================================
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies:
# - fuse3: FUSE filesystem support
# - iproute2: tc (traffic control) for network simulation
# - iptables: Firewall rules for network partition simulation
# - procps: ps, top for debugging
RUN apt-get update && apt-get install -y --no-install-recommends \
    fuse3 \
    iproute2 \
    iptables \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Create mount point directory
RUN mkdir -p /mnt/goclusterfs /data /scripts

# Copy binary from builder
COPY --from=builder /build/goclusterfs-mount /usr/local/bin/

# Copy container scripts
COPY tests/docker/scripts/ /scripts/
RUN chmod +x /scripts/*.sh

# Environment variables (can be overridden in docker-compose)
ENV NODE_ID=1
ENV DEVICE_PATH=/data/cluster.img
ENV MOUNT_POINT=/mnt/goclusterfs
ENV FORMAT_ON_START=false
ENV DEBUG=false

# Health check - verify FUSE mount is active
HEALTHCHECK --interval=2s --timeout=5s --retries=10 --start-period=5s \
    CMD /scripts/healthcheck.sh

# Entrypoint handles mount lifecycle
ENTRYPOINT ["/scripts/entrypoint.sh"]

# =============================================================================
# Stage 3: Orchestrator - Test runner with Docker CLI
# =============================================================================
FROM runtime AS orchestrator

# Install Docker CLI for container control
RUN apt-get update && apt-get install -y --no-install-recommends \
    docker.io \
    && rm -rf /var/lib/apt/lists/*

# Orchestrator runs tests instead of mounting
ENV ORCHESTRATOR_MODE=true

ENTRYPOINT ["/scripts/entrypoint.sh"]
