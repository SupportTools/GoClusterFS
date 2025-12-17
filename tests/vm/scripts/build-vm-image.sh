#!/bin/bash
# GoClusterFS VM Image Builder
# Creates an Alpine-based VM image for testing without Packer
#
# This script:
# 1. Downloads Alpine minirootfs
# 2. Creates a QCOW2 disk image
# 3. Installs Alpine to the image
# 4. Adds GoClusterFS and dependencies

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
RUNTIME_DIR="${SCRIPT_DIR}/../runtime"
OUTPUT_DIR="${RUNTIME_DIR}/packer-output"

# Alpine settings
ALPINE_VERSION="3.19"
ALPINE_MIRROR="https://dl-cdn.alpinelinux.org/alpine"
MINIROOTFS_URL="${ALPINE_MIRROR}/v${ALPINE_VERSION}/releases/x86_64/alpine-minirootfs-${ALPINE_VERSION}.0-x86_64.tar.gz"

# Image settings
IMAGE_NAME="goclusterfs-alpine.qcow2"
IMAGE_SIZE="512M"
MOUNT_POINT="/tmp/goclusterfs-vm-build"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $*"; }
log_success() { echo -e "${GREEN}[OK]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }

cleanup() {
    log_info "Cleaning up..."
    sudo umount "${MOUNT_POINT}" 2>/dev/null || true
    sudo losetup -D 2>/dev/null || true
    rm -rf "${MOUNT_POINT}"
}

trap cleanup EXIT

# Check dependencies
check_deps() {
    local missing=()
    for cmd in qemu-img wget tar; do
        if ! command -v "$cmd" &>/dev/null; then
            missing+=("$cmd")
        fi
    done

    if [[ ${#missing[@]} -gt 0 ]]; then
        log_error "Missing dependencies: ${missing[*]}"
        exit 1
    fi

    if [[ ! -f "${PROJECT_ROOT}/goclusterfs-mount" ]]; then
        log_error "goclusterfs-mount binary not found. Run 'make build-mount' first."
        exit 1
    fi
}

# Download Alpine minirootfs
download_rootfs() {
    local tarball="${RUNTIME_DIR}/alpine-minirootfs.tar.gz"

    if [[ -f "$tarball" ]]; then
        log_info "Using cached minirootfs"
        return
    fi

    log_info "Downloading Alpine minirootfs..."
    wget -q --show-progress -O "$tarball" "$MINIROOTFS_URL"
    log_success "Downloaded minirootfs"
}

# Create and populate the image
build_image() {
    mkdir -p "${OUTPUT_DIR}"
    local image_path="${OUTPUT_DIR}/${IMAGE_NAME}"
    local raw_image="${OUTPUT_DIR}/temp.raw"

    if [[ -f "$image_path" ]]; then
        log_info "Removing existing image..."
        rm -f "$image_path"
    fi

    log_info "Creating disk image..."

    # Create raw image (easier to work with)
    dd if=/dev/zero of="$raw_image" bs=1M count=512 status=progress

    # Create partition and filesystem
    log_info "Creating filesystem..."
    mkfs.ext4 -F "$raw_image"

    # Mount and populate
    mkdir -p "${MOUNT_POINT}"
    sudo mount -o loop "$raw_image" "${MOUNT_POINT}"

    log_info "Extracting Alpine rootfs..."
    sudo tar -xzf "${RUNTIME_DIR}/alpine-minirootfs.tar.gz" -C "${MOUNT_POINT}"

    # Configure Alpine
    log_info "Configuring Alpine..."

    # Set up resolv.conf
    echo "nameserver 8.8.8.8" | sudo tee "${MOUNT_POINT}/etc/resolv.conf" > /dev/null

    # Set up repositories
    cat <<EOF | sudo tee "${MOUNT_POINT}/etc/apk/repositories" > /dev/null
${ALPINE_MIRROR}/v${ALPINE_VERSION}/main
${ALPINE_MIRROR}/v${ALPINE_VERSION}/community
EOF

    # Install packages using chroot
    log_info "Installing packages..."
    sudo chroot "${MOUNT_POINT}" /bin/sh -c "
        apk update
        apk add --no-cache openssh fuse3 util-linux procps openrc e2fsprogs

        # Configure SSH
        ssh-keygen -A
        sed -i 's/#PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config
        sed -i 's/#PasswordAuthentication.*/PasswordAuthentication yes/' /etc/ssh/sshd_config
        echo 'root:testpass123' | chpasswd

        # Enable services
        rc-update add sshd default
        rc-update add devfs sysinit
        rc-update add dmesg sysinit
        rc-update add mdev sysinit

        # Create mount point
        mkdir -p /mnt/goclusterfs
    "

    # Copy GoClusterFS binary
    log_info "Installing GoClusterFS..."
    sudo cp "${PROJECT_ROOT}/goclusterfs-mount" "${MOUNT_POINT}/usr/local/bin/"
    sudo chmod +x "${MOUNT_POINT}/usr/local/bin/goclusterfs-mount"

    # Copy entrypoint script
    sudo cp "${SCRIPT_DIR}/vm-entrypoint.sh" "${MOUNT_POINT}/usr/local/bin/"
    sudo chmod +x "${MOUNT_POINT}/usr/local/bin/vm-entrypoint.sh"

    # Set up SSH authorized keys if provided
    if [[ -f "${RUNTIME_DIR}/test_key.pub" ]]; then
        sudo mkdir -p "${MOUNT_POINT}/root/.ssh"
        sudo cp "${RUNTIME_DIR}/test_key.pub" "${MOUNT_POINT}/root/.ssh/authorized_keys"
        sudo chmod 700 "${MOUNT_POINT}/root/.ssh"
        sudo chmod 600 "${MOUNT_POINT}/root/.ssh/authorized_keys"
    fi

    # Create GoClusterFS init script
    cat <<'INITEOF' | sudo tee "${MOUNT_POINT}/etc/init.d/goclusterfs" > /dev/null
#!/sbin/openrc-run

depend() {
    need localmount
    after modules
}

start() {
    ebegin "Starting GoClusterFS"
    /usr/local/bin/vm-entrypoint.sh &
    eend $?
}

stop() {
    ebegin "Stopping GoClusterFS"
    fusermount -u /mnt/goclusterfs 2>/dev/null || true
    eend $?
}
INITEOF
    sudo chmod +x "${MOUNT_POINT}/etc/init.d/goclusterfs"
    sudo chroot "${MOUNT_POINT}" rc-update add goclusterfs default

    # Set hostname
    echo "goclusterfs-vm" | sudo tee "${MOUNT_POINT}/etc/hostname" > /dev/null

    # Configure fstab
    cat <<EOF | sudo tee "${MOUNT_POINT}/etc/fstab" > /dev/null
/dev/vda    /           ext4    defaults,noatime    0 1
devtmpfs    /dev        devtmpfs defaults           0 0
proc        /proc       proc    defaults            0 0
sysfs       /sys        sysfs   defaults            0 0
EOF

    # Unmount
    sudo umount "${MOUNT_POINT}"

    # Convert to QCOW2
    log_info "Converting to QCOW2..."
    qemu-img convert -f raw -O qcow2 "$raw_image" "$image_path"
    rm -f "$raw_image"

    log_success "VM image created: ${image_path}"
}

main() {
    log_info "Building GoClusterFS VM image..."

    check_deps
    mkdir -p "${RUNTIME_DIR}" "${OUTPUT_DIR}"
    download_rootfs
    build_image

    log_success "Build complete!"
    log_info "Image location: ${OUTPUT_DIR}/${IMAGE_NAME}"
}

main "$@"
