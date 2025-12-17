#!/bin/bash
# GoClusterFS Health Check
# Verifies the FUSE mount is active and accessible

MOUNT_POINT="${MOUNT_POINT:-/mnt/goclusterfs}"

# Check if mount point is in /proc/mounts
if ! grep -q "$MOUNT_POINT" /proc/mounts; then
    echo "UNHEALTHY: $MOUNT_POINT not in /proc/mounts"
    exit 1
fi

# Check if mount point is accessible (can list directory)
if ! ls "$MOUNT_POINT" > /dev/null 2>&1; then
    echo "UNHEALTHY: Cannot access $MOUNT_POINT"
    exit 1
fi

echo "HEALTHY: $MOUNT_POINT mounted and accessible"
exit 0
