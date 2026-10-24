#!/bin/bash
# GoClusterFS Health Check
# Verifies the FUSE mount is active and accessible

MOUNT_POINT="${MOUNT_POINT:-/mnt/goclusterfs}"

# Check if mount point is in /proc/mounts
if ! grep -q "$MOUNT_POINT" /proc/mounts; then
    echo "UNHEALTHY: $MOUNT_POINT not in /proc/mounts"
    exit 1
fi

# stat, not ls: a readdir every 2s on every node reads the whole 1MB root directory block
if ! stat "$MOUNT_POINT" > /dev/null 2>&1; then
    echo "UNHEALTHY: Cannot access $MOUNT_POINT"
    exit 1
fi

echo "HEALTHY: $MOUNT_POINT mounted and accessible"
exit 0
