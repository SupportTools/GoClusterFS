#!/bin/bash
# GoClusterFS VM Test Harness
# Manages QEMU VMs for multi-node integration testing with true shared block storage
#
# This harness boots Alpine Linux live ISO directly and configures it at runtime,
# avoiding the need to build custom VM images.
#
# Usage:
#   ./vm-test-harness.sh --run-tests       # Run test suite with 2 VMs (default)
#   ./vm-test-harness.sh --nodes=3         # Run with 3 VMs
#   ./vm-test-harness.sh --cleanup         # Clean up all resources
#   ./vm-test-harness.sh --start-vm 1      # Start specific VM (for debugging)
#   ./vm-test-harness.sh --stop-vm 1       # Stop specific VM

set -euo pipefail

# =============================================================================
# Configuration
# =============================================================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
VM_DIR="${SCRIPT_DIR}/runtime"
RESULTS_DIR="${SCRIPT_DIR}/results"

# Alpine settings
ALPINE_VERSION="3.19"
ALPINE_MIRROR="https://dl-cdn.alpinelinux.org/alpine"
ISO_NAME="alpine-virt-${ALPINE_VERSION}.0-x86_64.iso"
ISO_URL="${ALPINE_MIRROR}/v${ALPINE_VERSION}/releases/x86_64/${ISO_NAME}"
ISO_PATH="${VM_DIR}/${ISO_NAME}"

# Shared storage
SHARED_DISK_PATH="${VM_DIR}/cluster.img"
SHARED_DISK_SIZE_MB=100

# Network settings (each VM gets sequential ports)
SSH_BASE_PORT=2222
QEMU_MONITOR_BASE_PORT=4444

# VM resources
VM_MEMORY="512M"
VM_CPUS=1

# SSH settings
SSH_KEY_PATH="${VM_DIR}/test_key"
SSH_USER="root"
SSH_TIMEOUT=30
SSH_OPTIONS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=5"

# Test settings
DEFAULT_NODE_COUNT=2
HEALTH_CHECK_RETRIES=60
HEALTH_CHECK_INTERVAL=2

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

# =============================================================================
# Logging Functions
# =============================================================================
log_info() { echo -e "${BLUE}[INFO]${NC} $*"; }
log_success() { echo -e "${GREEN}[OK]${NC} $*"; }
log_error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }

# =============================================================================
# Prerequisites
# =============================================================================
check_kvm() {
    # Don't log here - this function is called from command substitution
    # Log messages would be captured as part of the return value
    [[ -w /dev/kvm ]]
}

get_accelerator() {
    if check_kvm 2>/dev/null; then
        echo "kvm"
    else
        echo "tcg"
    fi
}

get_cpu_type() {
    if check_kvm 2>/dev/null; then
        echo "host"
    else
        echo "max"
    fi
}

# =============================================================================
# SSH Key Management
# =============================================================================
generate_ssh_key() {
    if [[ -f "${SSH_KEY_PATH}" ]]; then
        return 0
    fi

    log_info "Generating SSH key for VM access..."
    mkdir -p "${VM_DIR}"
    ssh-keygen -t ed25519 -f "${SSH_KEY_PATH}" -N "" -q
    log_success "SSH key generated: ${SSH_KEY_PATH}"
}

get_ssh_public_key() {
    cat "${SSH_KEY_PATH}.pub"
}

# =============================================================================
# Alpine ISO Management
# =============================================================================
download_iso() {
    if [[ -f "${ISO_PATH}" ]]; then
        log_info "Alpine ISO already downloaded"
        return 0
    fi

    log_info "Downloading Alpine Linux ISO..."
    mkdir -p "${VM_DIR}"
    wget -q --show-progress -O "${ISO_PATH}" "${ISO_URL}"
    log_success "Downloaded: ${ISO_PATH}"
}

# =============================================================================
# Shared Disk Management
# =============================================================================
create_shared_disk() {
    if [[ -f "${SHARED_DISK_PATH}" ]]; then
        log_info "Shared disk already exists: ${SHARED_DISK_PATH}"
        return 0
    fi

    log_info "Creating ${SHARED_DISK_SIZE_MB}MB shared disk image..."
    dd if=/dev/zero of="${SHARED_DISK_PATH}" bs=1M count=${SHARED_DISK_SIZE_MB} status=progress
    log_success "Shared disk created: ${SHARED_DISK_PATH}"
}

# =============================================================================
# VM Lifecycle Management
# =============================================================================
get_vm_pid_file() {
    local vm_id="$1"
    echo "${VM_DIR}/vm${vm_id}.pid"
}

get_vm_ssh_port() {
    local vm_id="$1"
    echo $((SSH_BASE_PORT + vm_id - 1))
}

get_vm_monitor_socket() {
    local vm_id="$1"
    echo "${VM_DIR}/vm${vm_id}.monitor.sock"
}

get_vm_ip() {
    local vm_id="$1"
    # VMs use user-mode networking with port forwarding
    echo "127.0.0.1"
}

is_vm_running() {
    local vm_id="$1"
    local pid_file=$(get_vm_pid_file "$vm_id")

    if [[ -f "$pid_file" ]]; then
        local pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            return 0
        fi
    fi
    return 1
}

# Start a VM with Alpine live ISO
start_vm() {
    local vm_id="$1"
    local format_disk="${2:-false}"  # Whether this VM should format the shared disk

    local ssh_port=$(get_vm_ssh_port "$vm_id")
    local monitor_socket=$(get_vm_monitor_socket "$vm_id")
    local serial_socket="${VM_DIR}/vm${vm_id}.sock"
    local pid_file=$(get_vm_pid_file "$vm_id")
    local log_file="${VM_DIR}/vm${vm_id}.log"
    local accelerator=$(get_accelerator)
    local cpu_type=$(get_cpu_type)

    if is_vm_running "$vm_id"; then
        log_info "VM $vm_id is already running"
        return 0
    fi

    # Clean up old sockets before starting (avoids "Address already in use")
    rm -f "$monitor_socket" "$serial_socket"

    # Ensure prerequisites
    download_iso
    create_shared_disk
    generate_ssh_key

    log_info "Starting VM $vm_id (SSH port: $ssh_port, format: $format_disk)..."
    log_info "Using accelerator: $accelerator, cpu: $cpu_type"

    # Create cloud-init style seed for automatic configuration
    local seed_dir="${VM_DIR}/seed${vm_id}"
    mkdir -p "$seed_dir"

    # Create startup script that will be passed via kernel command line
    local ssh_pubkey=$(get_ssh_public_key)

    # Start QEMU with Alpine live ISO
    # The ISO boots to a login prompt - we configure it via serial console
    qemu-system-x86_64 \
        -name "goclusterfs-vm${vm_id}" \
        -machine q35,accel=${accelerator} \
        -cpu ${cpu_type} \
        -m "${VM_MEMORY}" \
        -smp ${VM_CPUS} \
        -cdrom "${ISO_PATH}" \
        -boot d \
        -drive "file=${SHARED_DISK_PATH},format=raw,if=virtio,cache=none,file.locking=off" \
        -netdev "user,id=net0,hostfwd=tcp::${ssh_port}-:22" \
        -device "virtio-net-pci,netdev=net0" \
        -monitor "unix:${monitor_socket},server,nowait" \
        -serial "unix:${serial_socket},server,nowait" \
        -display none \
        -daemonize \
        -pidfile "${pid_file}" \
        2>&1 || {
            log_error "Failed to start VM $vm_id"
            return 1
        }

    log_info "VM $vm_id started (PID: $(cat "$pid_file"))"

    # Wait for VM to boot and configure it
    sleep 10
    configure_vm "$vm_id" "$format_disk"
}

# Configure a running VM via serial console after boot
configure_vm() {
    local vm_id="$1"
    local format_disk="${2:-false}"

    log_info "Configuring VM $vm_id via serial console..."

    # Always configure via serial - QEMU port forwarding makes port appear open
    # even before sshd is running, so we can't use nc -z to detect readiness
    configure_vm_via_serial "$vm_id" "$format_disk"
}

# Configure VM by sending commands through serial socket
configure_vm_via_serial() {
    local vm_id="$1"
    local format_disk="$2"
    local serial_sock="${VM_DIR}/vm${vm_id}.sock"
    local ssh_pubkey=$(get_ssh_public_key)

    # Helper to send a command via serial socket
    send_serial_cmd() {
        local cmd="$1"
        local wait="${2:-1}"
        # Send command with newline
        echo "$cmd" | nc -U -w 1 "$serial_sock" 2>/dev/null || true
        sleep "$wait"
    }

    # Wait for login prompt
    log_info "Waiting for VM $vm_id login prompt..."
    sleep 15  # Wait for boot

    # Login as root (no password on Alpine live)
    log_info "Logging into VM $vm_id..."
    send_serial_cmd "root" 2

    # Configure networking
    log_info "Configuring networking..."
    send_serial_cmd "setup-interfaces -a" 3
    send_serial_cmd "ifup eth0" 2

    # Configure Alpine repositories (live ISO only has CD-ROM packages)
    log_info "Configuring Alpine repositories..."
    send_serial_cmd "echo 'https://dl-cdn.alpinelinux.org/alpine/v3.19/main' > /etc/apk/repositories" 1
    send_serial_cmd "echo 'https://dl-cdn.alpinelinux.org/alpine/v3.19/community' >> /etc/apk/repositories" 1
    send_serial_cmd "apk update" 10

    # Install and configure SSH
    log_info "Installing SSH..."
    send_serial_cmd "apk add --no-cache openssh" 15

    log_info "Configuring SSH..."
    send_serial_cmd "sed -i 's/#PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config" 1
    send_serial_cmd "sed -i 's/#PasswordAuthentication.*/PasswordAuthentication yes/' /etc/ssh/sshd_config" 1
    send_serial_cmd "ssh-keygen -A" 3
    send_serial_cmd "rc-service sshd start" 3

    # Set root password
    log_info "Setting root password..."
    send_serial_cmd "echo 'root:testpass123' | chpasswd" 1

    # Set up SSH key
    log_info "Installing SSH key..."
    send_serial_cmd "mkdir -p /root/.ssh" 1
    send_serial_cmd "chmod 700 /root/.ssh" 1
    send_serial_cmd "echo '$ssh_pubkey' > /root/.ssh/authorized_keys" 1
    send_serial_cmd "chmod 600 /root/.ssh/authorized_keys" 1

    # Verify SSH is actually working before returning
    # This prevents race conditions where start_vm returns before SSH is ready
    log_info "Verifying SSH connectivity for VM $vm_id..."
    local ssh_port=$(get_vm_ssh_port "$vm_id")
    local ssh_timeout=60
    local ssh_elapsed=0

    while [[ $ssh_elapsed -lt $ssh_timeout ]]; do
        if sshpass -p "testpass123" ssh ${SSH_OPTIONS} -p "$ssh_port" "${SSH_USER}@127.0.0.1" "true" 2>/dev/null; then
            log_success "VM $vm_id configured and SSH verified"
            return 0
        fi
        sleep 2
        ssh_elapsed=$((ssh_elapsed + 2))
    done

    log_error "VM $vm_id: SSH verification failed after ${ssh_timeout}s"
    return 1
}

# Alias for backward compatibility
configure_vm_via_monitor() {
    configure_vm_via_serial "$@"
}

# =============================================================================
# GoClusterFS Setup
# =============================================================================

# Setup GoClusterFS on a VM after it's healthy
setup_goclusterfs() {
    local vm_id="$1"
    local format_disk="${2:-false}"
    local ssh_port=$(get_vm_ssh_port "$vm_id")

    log_info "Setting up GoClusterFS on VM $vm_id (format=$format_disk)..."

    # Copy the binary to the VM
    log_info "Copying goclusterfs-mount binary..."
    sshpass -p "testpass123" scp ${SSH_OPTIONS} -P "$ssh_port" \
        "${PROJECT_ROOT}/goclusterfs-mount" \
        "root@127.0.0.1:/usr/local/bin/goclusterfs-mount"

    # Install fuse3 and gcompat (glibc compatibility for binary compiled on glibc systems)
    log_info "Installing fuse3 and gcompat..."
    ssh_run_password "$vm_id" "apk add --no-cache fuse3 gcompat"

    # Load FUSE kernel module (required before mounting any FUSE filesystem)
    log_info "Loading FUSE kernel module..."
    ssh_run_password "$vm_id" "modprobe fuse"

    # Create mount point
    ssh_run_password "$vm_id" "mkdir -p /mnt/goclusterfs"

    # Mount GoClusterFS
    local mount_args="-mount /mnt/goclusterfs -device /dev/vda"
    if [[ "$format_disk" == "true" ]]; then
        log_info "Formatting shared disk on VM $vm_id..."
        mount_args="$mount_args -format"
    fi

    log_info "Mounting GoClusterFS..."
    ssh_run_password "$vm_id" \
        "nohup /usr/local/bin/goclusterfs-mount $mount_args > /var/log/goclusterfs.log 2>&1 &"

    # Wait for mount to be ready
    wait_for_goclusterfs_mount "$vm_id"
}

# Wait for GoClusterFS mount to be ready
wait_for_goclusterfs_mount() {
    local vm_id="$1"
    local timeout="${2:-30}"
    local elapsed=0

    log_info "Waiting for GoClusterFS mount on VM $vm_id..."

    while [[ $elapsed -lt $timeout ]]; do
        if ssh_run_password "$vm_id" "mountpoint -q /mnt/goclusterfs" 2>/dev/null; then
            log_success "GoClusterFS mounted on VM $vm_id"
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done

    log_error "GoClusterFS mount timeout on VM $vm_id"
    ssh_run_password "$vm_id" "cat /var/log/goclusterfs.log" 2>/dev/null || true
    return 1
}

# Stop VM gracefully
stop_vm() {
    local vm_id="$1"
    local pid_file=$(get_vm_pid_file "$vm_id")

    if ! is_vm_running "$vm_id"; then
        log_info "VM $vm_id is not running"
        return 0
    fi

    log_info "Stopping VM $vm_id gracefully..."

    # Try graceful shutdown via monitor (Unix socket)
    local monitor_socket=$(get_vm_monitor_socket "$vm_id")
    echo "system_powerdown" | nc -U -q1 "$monitor_socket" 2>/dev/null || true

    # Wait for graceful shutdown
    local timeout=30
    while [[ $timeout -gt 0 ]] && is_vm_running "$vm_id"; do
        sleep 1
        timeout=$((timeout - 1))
    done

    # Force kill if still running
    if is_vm_running "$vm_id"; then
        log_warn "VM $vm_id did not shutdown gracefully, killing..."
        kill_vm "$vm_id"
    fi

    rm -f "$pid_file"
    # Clean up sockets
    rm -f "$(get_vm_monitor_socket "$vm_id")" "${VM_DIR}/vm${vm_id}.sock"
    log_success "VM $vm_id stopped"
}

# Force kill VM
kill_vm() {
    local vm_id="$1"
    local pid_file=$(get_vm_pid_file "$vm_id")

    if [[ -f "$pid_file" ]]; then
        local pid=$(cat "$pid_file")
        kill -9 "$pid" 2>/dev/null || true
        rm -f "$pid_file"
    fi
    # Clean up sockets
    rm -f "$(get_vm_monitor_socket "$vm_id")" "${VM_DIR}/vm${vm_id}.sock"

    log_info "VM $vm_id killed"
}

# =============================================================================
# SSH Functions for Tests
# =============================================================================
ssh_run() {
    local vm_id="$1"
    shift
    local ssh_port=$(get_vm_ssh_port "$vm_id")

    ssh ${SSH_OPTIONS} -i "${SSH_KEY_PATH}" -p "$ssh_port" "${SSH_USER}@127.0.0.1" "$@"
}

ssh_run_password() {
    local vm_id="$1"
    shift
    local ssh_port=$(get_vm_ssh_port "$vm_id")

    sshpass -p "testpass123" ssh ${SSH_OPTIONS} -p "$ssh_port" "${SSH_USER}@127.0.0.1" "$@"
}

wait_vm_healthy() {
    local vm_id="$1"
    local timeout="${2:-120}"
    local ssh_port=$(get_vm_ssh_port "$vm_id")

    log_info "Waiting for VM $vm_id to be healthy..."

    local elapsed=0
    while [[ $elapsed -lt $timeout ]]; do
        # First check if SSH is reachable at all
        if sshpass -p "testpass123" ssh ${SSH_OPTIONS} -p "$ssh_port" "${SSH_USER}@127.0.0.1" "true" 2>/dev/null; then
            log_success "VM $vm_id is healthy (SSH reachable)"
            return 0
        fi

        sleep "${HEALTH_CHECK_INTERVAL}"
        elapsed=$((elapsed + HEALTH_CHECK_INTERVAL))
    done

    log_error "VM $vm_id failed to become healthy within ${timeout}s"
    return 1
}

# =============================================================================
# File Operations for Tests
# =============================================================================
# GoClusterFS mount point for shared storage
GOCLUSTERFS_MOUNT="/mnt/goclusterfs"

write_file_on_vm() {
    local vm_id="$1"
    local filename="$2"
    local content="$3"

    ssh_run_password "$vm_id" "echo '$content' > ${GOCLUSTERFS_MOUNT}/$filename"
}

read_file_on_vm() {
    local vm_id="$1"
    local filename="$2"

    ssh_run_password "$vm_id" "cat ${GOCLUSTERFS_MOUNT}/$filename"
}

file_exists_on_vm() {
    local vm_id="$1"
    local filename="$2"

    ssh_run_password "$vm_id" "test -f ${GOCLUSTERFS_MOUNT}/$filename"
}

# =============================================================================
# Cleanup
# =============================================================================
cleanup() {
    log_info "Cleaning up all VMs..."

    # Stop all VMs
    for pid_file in "${VM_DIR}"/vm*.pid; do
        if [[ -f "$pid_file" ]]; then
            local vm_id=$(basename "$pid_file" | sed 's/vm\([0-9]*\).pid/\1/')
            stop_vm "$vm_id" 2>/dev/null || true
        fi
    done

    log_success "Cleanup complete"
}

cleanup_all() {
    cleanup

    log_info "Removing all runtime files..."
    rm -rf "${VM_DIR}"
    rm -rf "${RESULTS_DIR}"
    log_success "Full cleanup complete"
}

# =============================================================================
# Test Runner
# =============================================================================
run_tests() {
    local node_count="${1:-$DEFAULT_NODE_COUNT}"

    log_info "Running VM test suite with $node_count nodes..."

    # Run the test script
    "${SCRIPT_DIR}/scripts/run-vm-tests.sh" "$node_count"
}

# =============================================================================
# Main Entry Point
# =============================================================================
main() {
    local command="${1:-}"

    case "$command" in
        --run-tests)
            shift
            local nodes="${1:-$DEFAULT_NODE_COUNT}"
            [[ "$nodes" == --nodes=* ]] && nodes="${nodes#--nodes=}"
            run_tests "$nodes"
            ;;
        --nodes=*)
            local nodes="${command#--nodes=}"
            run_tests "$nodes"
            ;;
        --start-vm)
            shift
            start_vm "$1" "${2:-false}"
            ;;
        --stop-vm)
            shift
            stop_vm "$1"
            ;;
        --kill-vm)
            shift
            kill_vm "$1"
            ;;
        --cleanup)
            cleanup
            ;;
        --cleanup-all)
            cleanup_all
            ;;
        --help|-h)
            echo "GoClusterFS VM Test Harness"
            echo ""
            echo "Usage:"
            echo "  $0 --run-tests [--nodes=N]  Run tests with N VMs (default: 2)"
            echo "  $0 --start-vm ID [format]  Start specific VM"
            echo "  $0 --stop-vm ID            Stop specific VM"
            echo "  $0 --kill-vm ID            Force kill VM"
            echo "  $0 --cleanup               Stop all VMs"
            echo "  $0 --cleanup-all           Remove all runtime files"
            echo "  $0 --help                  Show this help"
            ;;
        *)
            log_error "Unknown command: $command"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
}

# Run if executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
