#!/bin/bash
# GoClusterFS Container Entrypoint
# Handles both node mode (mounts filesystem) and orchestrator mode (runs tests)

set -e

# Configuration from environment
NODE_ID="${NODE_ID:-1}"
DEVICE_PATH="${DEVICE_PATH:-/data/cluster.img}"
MOUNT_POINT="${MOUNT_POINT:-/mnt/goclusterfs}"
FORMAT_ON_START="${FORMAT_ON_START:-false}"
DEBUG="${DEBUG:-false}"
ORCHESTRATOR_MODE="${ORCHESTRATOR_MODE:-false}"

# Flag file to track if this node has already formatted the filesystem
# This prevents re-formatting on container restart
FORMAT_DONE_FLAG="/tmp/.goclusterfs_formatted"

log() {
    echo "[Node $NODE_ID] $(date '+%Y-%m-%d %H:%M:%S') $*"
}

# =============================================================================
# Orchestrator Mode - Run tests
# =============================================================================
if [ "$ORCHESTRATOR_MODE" = "true" ]; then
    log "Starting in orchestrator mode"

    log "Waiting for nodes to be ready..."
    for node in ${NODES//,/ }; do
        for _ in $(seq 1 240); do
            docker exec "$node" /scripts/healthcheck.sh > /dev/null 2>&1 && break
            sleep 0.5
        done
    done

    # Run the test script if provided
    if [ -n "$TEST_SCRIPT" ] && [ -f "$TEST_SCRIPT" ]; then
        log "Running test script: $TEST_SCRIPT"
        exec "$TEST_SCRIPT" "$@"
    elif [ -f "/scripts/run-tests.sh" ]; then
        log "Running default test script"
        exec /scripts/run-tests.sh "$@"
    else
        log "No test script found, sleeping indefinitely"
        exec sleep infinity
    fi
fi

# =============================================================================
# Node Mode - Mount filesystem
# =============================================================================
log "Starting GoClusterFS node"
log "Device: $DEVICE_PATH"
log "Mount point: $MOUNT_POINT"

# Wait for device to be ready (shared volume may take a moment)
WAIT_COUNT=0
MAX_WAIT=30
while [ ! -f "$DEVICE_PATH" ] && [ $WAIT_COUNT -lt $MAX_WAIT ]; do
    log "Waiting for device $DEVICE_PATH to be ready... ($WAIT_COUNT/$MAX_WAIT)"
    sleep 1
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

if [ ! -f "$DEVICE_PATH" ]; then
    log "ERROR: Device $DEVICE_PATH not found after ${MAX_WAIT}s"
    exit 1
fi

log "Device $DEVICE_PATH is ready (size: $(stat -c%s "$DEVICE_PATH") bytes)"

# Build mount command arguments
MOUNT_ARGS="-device $DEVICE_PATH -mount $MOUNT_POINT"

# Only format if FORMAT_ON_START=true AND we haven't already formatted
# This prevents data loss on container restart
if [ "$FORMAT_ON_START" = "true" ]; then
    if [ -f "$FORMAT_DONE_FLAG" ]; then
        log "Skipping format (already done, flag exists at $FORMAT_DONE_FLAG)"
    else
        log "Formatting filesystem (FORMAT_ON_START=true, first run)"
        # One inode per 64 KB gives the 768 MB image 12288 inodes, enough for test_more_than_4096_files
        MOUNT_ARGS="$MOUNT_ARGS -format -bytes-per-inode 65536"
        # Create flag file to mark that formatting is done
        touch "$FORMAT_DONE_FLAG"
    fi
fi

if [ "$DEBUG" = "true" ]; then
    MOUNT_ARGS="$MOUNT_ARGS -debug"
fi

# Ensure mount point exists
mkdir -p "$MOUNT_POINT"

log "Starting goclusterfs-mount with args: $MOUNT_ARGS"

# Handle signals for graceful shutdown
cleanup() {
    log "Received shutdown signal, unmounting..."
    fusermount -u "$MOUNT_POINT" 2>/dev/null || true
    log "Cleanup complete"
    exit 0
}

trap cleanup SIGTERM SIGINT SIGQUIT

# Start the mount process in foreground
# shellcheck disable=SC2086
exec /usr/local/bin/goclusterfs-mount $MOUNT_ARGS
