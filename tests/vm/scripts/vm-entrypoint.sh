#!/bin/sh
# GoClusterFS VM Entrypoint Script
# Runs inside the Alpine VM to start GoClusterFS
#
# This script:
# 1. Waits for the shared block device to be available
# 2. Optionally formats the filesystem (if FORMAT_DISK=true)
# 3. Mounts GoClusterFS at /mnt/goclusterfs
# 4. Handles graceful shutdown on signals

set -e

# Configuration
MOUNT_POINT="/mnt/goclusterfs"
DEVICE="/dev/vdb"  # Second virtio disk (shared cluster storage)
GOCLUSTERFS="/usr/local/bin/goclusterfs-mount"
LOG_FILE="/var/log/goclusterfs.log"
READY_FILE="/tmp/goclusterfs-ready"

# Read boot parameters
VM_ID="${VM_ID:-1}"
FORMAT_DISK="${FORMAT_DISK:-false}"

# Logging
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_FILE}"
}

log_error() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: $*" | tee -a "${LOG_FILE}" >&2
}

# Wait for device
wait_for_device() {
    local timeout=30
    local elapsed=0

    log "Waiting for device ${DEVICE}..."

    while [ ! -b "${DEVICE}" ]; do
        sleep 1
        elapsed=$((elapsed + 1))
        if [ $elapsed -ge $timeout ]; then
            log_error "Device ${DEVICE} not found after ${timeout}s"
            return 1
        fi
    done

    log "Device ${DEVICE} is available"
    return 0
}

# Signal handlers
cleanup() {
    log "Received shutdown signal, unmounting..."
    rm -f "${READY_FILE}"
    fusermount -u "${MOUNT_POINT}" 2>/dev/null || true
    log "Shutdown complete"
    exit 0
}

trap cleanup SIGTERM SIGINT SIGQUIT

# Main
main() {
    log "GoClusterFS VM Entrypoint starting (VM_ID=${VM_ID}, FORMAT_DISK=${FORMAT_DISK})"

    # Ensure mount point exists
    mkdir -p "${MOUNT_POINT}"

    # Wait for shared disk
    if ! wait_for_device; then
        log_error "Failed to find shared block device"
        exit 1
    fi

    # Load FUSE module if needed
    if [ ! -c /dev/fuse ]; then
        log "Loading FUSE module..."
        modprobe fuse || true
    fi

    # Build mount command
    MOUNT_ARGS=""
    if [ "${FORMAT_DISK}" = "true" ]; then
        MOUNT_ARGS="-format"
        log "Will format filesystem on first mount"
    fi

    log "Mounting GoClusterFS: ${GOCLUSTERFS} ${MOUNT_ARGS} ${MOUNT_POINT} ${DEVICE}"

    # Start GoClusterFS in background
    ${GOCLUSTERFS} ${MOUNT_ARGS} "${MOUNT_POINT}" "${DEVICE}" >> "${LOG_FILE}" 2>&1 &
    GOCLUSTERFS_PID=$!

    # Wait for mount to be ready
    local timeout=30
    local elapsed=0
    while ! mountpoint -q "${MOUNT_POINT}"; do
        if ! kill -0 $GOCLUSTERFS_PID 2>/dev/null; then
            log_error "GoClusterFS process died unexpectedly"
            cat "${LOG_FILE}" >&2
            exit 1
        fi
        sleep 1
        elapsed=$((elapsed + 1))
        if [ $elapsed -ge $timeout ]; then
            log_error "Mount point not ready after ${timeout}s"
            exit 1
        fi
    done

    log "GoClusterFS mounted successfully at ${MOUNT_POINT}"

    # Create ready marker for external health checks
    touch "${READY_FILE}"

    # Wait for GoClusterFS process
    wait $GOCLUSTERFS_PID
}

main "$@"
