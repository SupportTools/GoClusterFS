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
#
# VM N listens for SSH on 127.0.0.1:$((VM_SSH_BASE_PORT + N - 1)), default base 2222.
# A lock allows one run per checkout; runs from different checkouts on one host
# need different VM_SSH_BASE_PORT values. Each run gets its own SSH key and copies of
# goclusterfs-mount and, when built, goclusterfs-fsck. VM console, QEMU and mount logs go
# to tests/vm/results/.

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
ISO_SHA256="95ed37b1e54b04aad1c6ff54a5fd60d904363a2a8810c3e3dfe4c2a29e94056b"

# Shared storage (configurable via VM_CLUSTER_SIZE_MB environment variable)
SHARED_DISK_PATH="${VM_DIR}/cluster.img"
SHARED_DISK_SIZE_MB=${VM_CLUSTER_SIZE_MB:-1024}

# Network settings (each VM gets sequential ports)
SSH_BASE_PORT=${VM_SSH_BASE_PORT:-2222}
# Node VMs share eth1 for broadcasts: QEMU carries its frames over a multicast group on the host's loopback.
BROADCAST_NET="230.77.0.1:$((SSH_BASE_PORT + 200))"
BROADCAST_IFACE=eth1

# VM_STORAGE=iscsi serves the cluster image from VM ${TARGET_VM_ID} over iSCSI (Linux LIO, with SCSI-3 persistent
# reservations), so nodes fence each other. virtio, the default, shares the image as a virtio disk without reservations.
VM_STORAGE=${VM_STORAGE:-virtio}
TARGET_VM_ID=9
ISCSI_PORT=$((SSH_BASE_PORT + 100))
ISCSI_TARGET_IQN="iqn.2026-11.dev.goclusterfs:target"
# The virt ISO's kernel lacks LIO's target modules; the standard ISO has them.
STD_ISO_NAME="alpine-standard-${ALPINE_VERSION}.0-x86_64.iso"
STD_ISO_PATH="${VM_DIR}/${STD_ISO_NAME}"
STD_ISO_SHA256="4cf7cd3bad64122a8a2423e78369a486a02334d4d88645aab9d08120bb76b0f9"

# VM resources
VM_MEMORY="512M"
VM_CPUS=1
VM_BOOT_ATTEMPTS=2

# QEMU dies with the shell that started it, so a killed run cannot leave VMs behind
VM_REAPER=(setpriv --pdeathsig KILL --)

# SSH settings
SSH_USER="root"
SSH_OPTIONS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=5 -o BatchMode=yes -o IdentitiesOnly=yes"

# Test settings
DEFAULT_NODE_COUNT=2
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
# Run State
# =============================================================================
# A run owns a private directory holding its SSH key, a frozen copy of
# goclusterfs-mount and the VM setup disk. Child scripts inherit it.
init_vm_run() {
    if [[ -n "${VM_RUN_DIR:-}" && -d "${VM_RUN_DIR}" ]]; then
        return 0
    fi
    if ! command -v setpriv > /dev/null; then
        log_error "setpriv (util-linux) is required"
        return 1
    fi
    if [[ ! -x "${PROJECT_ROOT}/goclusterfs-mount" ]]; then
        log_error "${PROJECT_ROOT}/goclusterfs-mount not found, run 'make build' first"
        return 1
    fi

    mkdir -p "${VM_DIR}" "${RESULTS_DIR}"
    exec {VM_RUN_LOCK_FD}> "${VM_DIR}/harness.lock"
    if ! flock -n "$VM_RUN_LOCK_FD"; then
        log_error "Another VM run is using ${VM_DIR}. Wait for it, stop it with --cleanup, or run from another checkout with a different VM_SSH_BASE_PORT"
        return 1
    fi
    VM_RUN_DIR=$(mktemp -d "${VM_DIR}/run.XXXXXX")
    VM_RUN_OWNER=$$
    export VM_RUN_DIR VM_RUN_OWNER VM_RUN_LOCK_FD

    cp "${PROJECT_ROOT}/goclusterfs-mount" "${VM_RUN_DIR}/goclusterfs-mount"
    if [[ -x "${PROJECT_ROOT}/goclusterfs-fsck" ]]; then
        cp "${PROJECT_ROOT}/goclusterfs-fsck" "${VM_RUN_DIR}/goclusterfs-fsck"
    fi
    ssh-keygen -t ed25519 -f "$(get_ssh_key)" -N "" -q -C "goclusterfs-vm-run"
    write_setup_disk "${VM_RUN_DIR}/setup.img"
    rm -f "${RESULTS_DIR}"/vm*.log
    log_info "Run directory: ${VM_RUN_DIR}, VM logs: ${RESULTS_DIR}/vm*.log"
}

start_vm_run() {
    init_vm_run
    trap on_vm_run_exit EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
    if [[ "${VM_RUN_OWNER}" == "$$" && -n "$(vm_ids_with_pid_files)" ]]; then
        log_warn "Stopping VMs left in ${VM_DIR} by an earlier run"
        kill_all_vms
    fi
}

# Removes run directories unless a run in this checkout still holds the lock
remove_idle_run_dirs() {
    local lock_fd
    [[ -d "${VM_DIR}" ]] || return 0
    exec {lock_fd}> "${VM_DIR}/harness.lock"
    if flock -n "$lock_fd"; then
        rm -rf "${VM_DIR}"/run.*
    else
        log_warn "A VM run is active in ${VM_DIR}, keeping its run directory"
    fi
    exec {lock_fd}>&-
}

on_vm_run_exit() {
    local status=$?
    trap - EXIT
    trap '' INT TERM
    set +e
    stop_child_processes
    kill_all_vms
    if [[ "${VM_RUN_OWNER:-}" == "$$" ]] || ! kill -0 "${VM_RUN_OWNER:-$$}" 2>/dev/null; then
        rm -rf "${VM_RUN_DIR}"
    fi
    exit "$status"
}

kill_process_tree() {
    local pid="$1"
    local children child
    children=$(pgrep -P "$pid" || true)
    kill -TERM "$pid" 2>/dev/null || true
    for child in $children; do
        kill_process_tree "$child"
    done
}

stop_child_processes() {
    local child
    for child in $(pgrep -P $$ || true); do
        kill_process_tree "$child"
    done
    wait 2>/dev/null || true
}

get_ssh_key() {
    echo "${VM_RUN_DIR}/id_ed25519"
}

# Runs once per boot via "sh /dev/vdb". Typing long lines on the serial console
# is unreliable because the guest shell's line editor wraps them at 80 columns.
write_setup_disk() {
    local image="$1"
    {
        echo "repo='${ALPINE_MIRROR}/v${ALPINE_VERSION}'"
        echo "key='$(cat "$(get_ssh_key).pub")'"
        echo "mountpoint='${GOCLUSTERFS_MOUNT}'"
        cat <<'EOF'
set -x
# QEMU passes each VM its own name, nodeN, so mounts see one hostname per VM, as on real hosts.
name=$(cat /sys/class/dmi/id/product_serial) && [ -n "$name" ] && hostname "$name" && echo "$name" > /etc/hostname || exit 1
# setup-interfaces -a leaves eth0 out once a VM has a second NIC.
printf 'auto lo\niface lo inet loopback\n\nauto eth0\niface eth0 inet dhcp\n' > /etc/network/interfaces
ifup eth0
# goclusterfs-mount serves its API on 127.0.0.1, which needs loopback up.
ip link set lo up
# The last byte of eth1's MAC is the VM number.
if [ -e /sys/class/net/eth1 ]; then
    ip link set eth1 up
    ip addr add "192.168.168.$((0x$(cut -d: -f6 /sys/class/net/eth1/address)))/24" dev eth1
fi
printf '%s\n' "$repo/main" "$repo/community" > /etc/apk/repositories
tries=0
until apk add --no-cache openssh fuse3 gcompat attr; do
    tries=$((tries + 1))
    [ "$tries" -lt 5 ] || exit 1
    sleep 3
done
modprobe fuse
modprobe virtio_console
for port in /sys/class/virtio-ports/*; do
    [ "$(cat "$port/name")" = goclusterfs.log ] && ln -sf "/dev/${port##*/}" /dev/goclusterfs-log
done
[ -c /dev/goclusterfs-log ] || exit 1
mkdir -p /root/.ssh "$mountpoint"
chmod 700 /root/.ssh
echo "$key" > /root/.ssh/authorized_keys
chmod 600 /root/.ssh/authorized_keys
ssh-keygen -A
rc-service sshd start
EOF
    } > "$image"
    truncate -s %512 "$image"
}

# =============================================================================
# Alpine ISO Management
# =============================================================================
download_iso() {
    download_pinned "${ISO_PATH}" "${ISO_URL}" "${ISO_SHA256}" || return 1
    if [[ "$VM_STORAGE" == iscsi ]]; then
        download_pinned "${STD_ISO_PATH}" "${ALPINE_MIRROR}/v${ALPINE_VERSION}/releases/x86_64/${STD_ISO_NAME}" "${STD_ISO_SHA256}"
    fi
}

download_pinned() {
    local path="$1" url="$2" sha="$3"
    if [[ -f "$path" ]]; then
        log_info "$(basename "$path") already downloaded"
    else
        log_info "Downloading $(basename "$path")..."
        mkdir -p "${VM_DIR}"
        wget -q --show-progress -O "$path" "$url"
        log_success "Downloaded: $path"
    fi

    if ! echo "${sha}  ${path}" | sha256sum -c --status; then
        log_error "$path does not match the pinned SHA-256; removed it, rerun to download again"
        rm -f "$path"
        return 1
    fi
}

# =============================================================================
# Shared Disk Management
# =============================================================================
create_shared_disk() {
    local expected_bytes=$((SHARED_DISK_SIZE_MB * 1024 * 1024))
    mkdir -p "${VM_DIR}"

    if [[ -f "${SHARED_DISK_PATH}" ]]; then
        local actual_bytes=$(stat -c%s "${SHARED_DISK_PATH}" 2>/dev/null || echo 0)
        if [[ "$actual_bytes" -eq "$expected_bytes" ]]; then
            log_info "Shared disk already exists with correct size: ${SHARED_DISK_PATH}"
            return 0
        fi
        log_warn "Shared disk size mismatch (have: $((actual_bytes/1024/1024))MB, want: ${SHARED_DISK_SIZE_MB}MB), recreating..."
        rm -f "${SHARED_DISK_PATH}"
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

get_vm_serial_socket() {
    local vm_id="$1"
    echo "${VM_DIR}/vm${vm_id}.sock"
}

get_vm_console_log() {
    local vm_id="$1"
    echo "${RESULTS_DIR}/vm${vm_id}.console.log"
}

get_vm_mount_log() {
    local vm_id="$1"
    echo "${RESULTS_DIR}/vm${vm_id}.mount.log"
}

get_vm_ip() {
    local vm_id="$1"
    # VMs use user-mode networking with port forwarding
    echo "127.0.0.1"
}

is_vm_running() {
    local vm_id="$1"
    local pid_file=$(get_vm_pid_file "$vm_id")

    # A stale pid file may name a reused PID, so check that it is still this VM's QEMU
    if [[ -f "$pid_file" ]]; then
        local pid=$(cat "$pid_file")
        local cmdline=$(tr '\0' ' ' < "/proc/${pid}/cmdline" 2>/dev/null)
        if [[ "$cmdline" == *"-name goclusterfs-vm${vm_id} "* ]]; then
            return 0
        fi
    fi
    return 1
}

port_owner() {
    command -v ss > /dev/null || return 0
    ss -Htlnp "sport = :$1" | grep -o 'pid=[0-9]*' | head -n 1 | cut -d= -f2
}

# A QEMU this checkout lost track of keeps its VM's SSH port and fails every later boot of that VM.
free_vm_port() {
    local vm_id="$1" port="$2"
    local pid=$(port_owner "$port")
    [[ -n "$pid" ]] || return 0
    local cmdline=$(tr '\0' ' ' < "/proc/${pid}/cmdline" 2>/dev/null)
    if [[ "$cmdline" != *"-name goclusterfs-vm${vm_id} "* || "$cmdline" != *"${VM_DIR}/"* ]]; then
        log_error "VM $vm_id: SSH port $port is held by pid $pid (${cmdline:-unknown}); pick another base port"
        return 1
    fi
    log_warn "VM $vm_id: QEMU pid $pid from an earlier boot still holds port $port, killing it"
    kill -9 "$pid" 2>/dev/null
    local waited=0
    while [[ -n "$(port_owner "$port")" ]]; do
        if [[ $waited -ge 50 ]]; then
            log_error "VM $vm_id: port $port still held 10s after killing pid $pid"
            return 1
        fi
        sleep 0.2
        waited=$((waited + 1))
    done
}

mark_vm_log() {
    local vm_id="$1"
    local text="$2"
    [[ -d "${RESULTS_DIR}" ]] || return 0
    echo "=== $(date '+%F %T') $text" >> "$(get_vm_mount_log "$vm_id")"
}

# Marks every VM mount log, e.g. with the name of the test about to run
mark_vm_logs() {
    local log
    for log in "${RESULTS_DIR}"/vm*.mount.log; do
        if [[ -f "$log" ]]; then
            echo "=== $(date '+%F %T') $*" >> "$log"
        fi
    done
}

# Start a VM, retrying the whole bring-up once if it fails
start_vm() {
    local vm_id="$1"
    local attempt

    if is_vm_running "$vm_id"; then
        log_info "VM $vm_id is already running"
        return 0
    fi

    download_iso || return 1
    create_shared_disk || return 1
    if [[ "$VM_STORAGE" == iscsi && "$vm_id" != "$TARGET_VM_ID" ]]; then
        start_vm "$TARGET_VM_ID" || return 1
    fi

    for attempt in $(seq 1 "$VM_BOOT_ATTEMPTS"); do
        if boot_vm "$vm_id" && attach_storage "$vm_id"; then
            return 0
        fi
        log_warn "VM $vm_id bring-up failed (attempt $attempt of $VM_BOOT_ATTEMPTS)"
        kill_vm "$vm_id"
    done
    log_error "VM $vm_id failed to start"
    return 1
}

# attach_storage serves the cluster image when VM $1 is the iSCSI target, and logs in to it from a node VM.
attach_storage() {
    [[ "$VM_STORAGE" == iscsi ]] || return 0
    if [[ "$1" == "$TARGET_VM_ID" ]]; then
        configure_iscsi_target
    else
        login_iscsi "$1"
    fi
}

# configure_iscsi_target exports the target VM's /dev/vda, the cluster image, as one LUN with LIO through configfs,
# with an ACL per node VM. LIO saves the reservations in /var/target/pr. A rebuild passes the LUN's old unit serial $1
# and, to keep the reservations, the target VM's copy $2 of the file LIO saved them in.
configure_iscsi_target() {
    local serial="${1:-}" metadata="${2:-}"
    ssh_run "$TARGET_VM_ID" "sh -s" <<SCRIPT
set -e
for m in target_core_mod target_core_iblock iscsi_target_mod; do modprobe \$m; done
mount | grep -q ' /sys/kernel/config ' || mount -t configfs none /sys/kernel/config
mkdir -p /var/target/pr
cd /sys/kernel/config/target
mkdir -p core/iblock_0/lun0
echo udev_path=/dev/vda > core/iblock_0/lun0/control
echo 1 > core/iblock_0/lun0/enable
[ -z "$serial" ] || echo "$serial" > core/iblock_0/lun0/wwn/vpd_unit_serial
# LIO takes one registration per write, so each PR_REG block goes in its own write, as rtslib restores them
[ -z "$metadata" ] || awk '/^PR_REG_START/ { r = ""; next } /^PR_REG_END/ { print r; next } { r = r (r == "" ? "" : ",") \$0 }' "$metadata" |
    while read -r reg; do echo "\$reg" > core/iblock_0/lun0/pr/res_aptpl_metadata; done
tpg=iscsi/${ISCSI_TARGET_IQN}/tpgt_1
mkdir -p \$tpg/lun/lun_0 \$tpg/np/0.0.0.0:3260
ln -s /sys/kernel/config/target/core/iblock_0/lun0 \$tpg/lun/lun_0/disk
for n in 1 2 3 4 5 6 7 8; do
    mkdir -p \$tpg/acls/iqn.2026-11.dev.goclusterfs:vm\$n/lun_0
    ln -s /sys/kernel/config/target/\$tpg/lun/lun_0 \$tpg/acls/iqn.2026-11.dev.goclusterfs:vm\$n/lun_0/lun
done
echo 0 > \$tpg/attrib/authentication
echo 1 > \$tpg/enable
SCRIPT
}

# power_cycle_target kills the iSCSI target VM and boots it again, as a storage power loss would, and serves the LUN
# under its old serial. With "keep" it first copies the file LIO saved the reservations in (APTPL) to the host, and
# restores it before nodes reconnect, as OPERATIONS.md says; without, the reservations are lost.
power_cycle_target() {
    local keep="${1:-}" serial saved="${VM_RUN_DIR}/aptpl.saved"
    serial=$(ssh_run_capped 30 "$TARGET_VM_ID" "sed 's/.*: //' /sys/kernel/config/target/core/iblock_0/lun0/wwn/vpd_unit_serial") || return 1
    if [[ "$keep" == keep ]]; then
        ssh_run_capped 30 "$TARGET_VM_ID" "cat /var/target/pr/aptpl_$serial" > "$saved" || return 1
    fi
    kill_vm "$TARGET_VM_ID" || return 1
    boot_vm "$TARGET_VM_ID" || return 1
    if [[ "$keep" == keep ]]; then
        ssh_run "$TARGET_VM_ID" "cat > /root/aptpl.saved" < "$saved" || return 1
        configure_iscsi_target "$serial" /root/aptpl.saved
    else
        configure_iscsi_target "$serial"
    fi
}

# relogin_iscsi logs node VM $1 out of the target and in again, as detaching and attaching the disk would.
relogin_iscsi() {
    local portal="10.0.2.2:${ISCSI_PORT}"
    ssh_run "$1" "iscsiadm -m node -T ${ISCSI_TARGET_IQN} -p ${portal} --logout > /dev/null 2>&1; iscsiadm -m node -T ${ISCSI_TARGET_IQN} -p ${portal} --login > /dev/null && for t in \$(seq 20); do dd if=/dev/sda of=/dev/null bs=4096 count=1 iflag=direct 2>/dev/null && exit 0; sleep 1; done; exit 1"
}

# login_iscsi logs node VM $1 in to the target as its own initiator, which fails I/O after 5 s instead of queuing it.
login_iscsi() {
    local vm_id="$1" portal="10.0.2.2:${ISCSI_PORT}"
    ssh_run "$vm_id" "sh -s" <<SCRIPT
set -e
apk add --no-cache open-iscsi > /dev/null
modprobe iscsi_tcp
echo InitiatorName=iqn.2026-11.dev.goclusterfs:vm${vm_id} > /etc/iscsi/initiatorname.iscsi
rc-service iscsid start > /dev/null 2>&1 || true
iscsiadm -m node -T ${ISCSI_TARGET_IQN} -p ${portal} -o new > /dev/null
iscsiadm -m node -T ${ISCSI_TARGET_IQN} -p ${portal} -o update -n node.session.timeo.replacement_timeout -v 5
iscsiadm -m node -T ${ISCSI_TARGET_IQN} -p ${portal} --login > /dev/null
for t in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do [ -b /dev/sda ] && exit 0; sleep 1; done
exit 1
SCRIPT
}

# cluster_device is the cluster image's device on a node VM.
cluster_device() {
    if [[ "$VM_STORAGE" == iscsi ]]; then echo /dev/sda; else echo /dev/vda; fi
}

# Boot the Alpine live ISO, log in on the serial console and run the setup disk
boot_vm() {
    local vm_id="$1"
    local ssh_port=$(get_vm_ssh_port "$vm_id")
    local monitor_socket=$(get_vm_monitor_socket "$vm_id")
    local serial_socket=$(get_vm_serial_socket "$vm_id")
    local console_log=$(get_vm_console_log "$vm_id")
    local pid_file=$(get_vm_pid_file "$vm_id")
    local accelerator=$(get_accelerator)
    local cpu_type=$(get_cpu_type)
    local iso="${ISO_PATH}" forwards="hostfwd=tcp:127.0.0.1:${ssh_port}-:22"
    local broadcast_net=(-netdev "socket,id=net1,mcast=${BROADCAST_NET},localaddr=127.0.0.1"
        -device "virtio-net-pci,netdev=net1,mac=52:54:00:a8:00:$(printf %02x "$vm_id")")
    if [[ "$vm_id" == "$TARGET_VM_ID" ]]; then
        iso="${STD_ISO_PATH}"
        forwards="${forwards},hostfwd=tcp:127.0.0.1:${ISCSI_PORT}-:3260"
        broadcast_net=()
    fi

    # Clean up old sockets before starting (avoids "Address already in use")
    rm -f "$monitor_socket" "$serial_socket"
    free_vm_port "$vm_id" "$ssh_port" || return 1

    log_info "Starting VM $vm_id (SSH port: $ssh_port)..."
    log_info "Using accelerator: $accelerator, cpu: $cpu_type"

    echo "=== $(date '+%F %T') VM $vm_id boot" >> "$console_log"
    mark_vm_log "$vm_id" "VM $vm_id boot"
    local console_offset=$(stat -c%s "$console_log")

    "${VM_REAPER[@]}" qemu-system-x86_64 \
        -name "goclusterfs-vm${vm_id}" \
        -machine q35,accel=${accelerator} \
        -cpu ${cpu_type} \
        -m "${VM_MEMORY}" \
        -smbios "type=1,serial=node${vm_id}" \
        -smp ${VM_CPUS} \
        -cdrom "${iso}" \
        -boot d \
        -drive "file=${SHARED_DISK_PATH},format=raw,if=virtio,cache=none,discard=unmap,file.locking=off" \
        -drive "file=${VM_RUN_DIR}/setup.img,format=raw,if=virtio,readonly=on" \
        -netdev "user,id=net0,${forwards}" \
        -device "virtio-net-pci,netdev=net0" \
        "${broadcast_net[@]}" \
        -device virtio-serial-pci \
        -chardev "file,id=mountlog,path=$(get_vm_mount_log "$vm_id"),append=on" \
        -device "virtserialport,chardev=mountlog,name=goclusterfs.log" \
        -chardev "socket,id=serial0,path=${serial_socket},server=on,wait=off,logfile=${console_log},logappend=on" \
        -serial chardev:serial0 \
        -monitor "unix:${monitor_socket},server,nowait" \
        -display none \
        {VM_RUN_LOCK_FD}>&- >> "${RESULTS_DIR}/vm${vm_id}.qemu.log" 2>&1 &
    local pid=$!
    disown "$pid"
    echo "$pid" > "$pid_file"
    log_info "VM $vm_id started (PID: $pid)"
    # The PID names QEMU only once the background job execs it; before that is_vm_running sees this shell.
    for _ in $(seq 1 10); do
        is_vm_running "$vm_id" && break
        sleep 0.5
    done

    if ! wait_for_console "$vm_id" "$console_offset" "login:" 120; then
        report_boot_failure "$vm_id" "$console_offset" "no login prompt on the serial console"
        return 1
    fi
    if ! console_login "$vm_id" "$console_offset"; then
        report_boot_failure "$vm_id" "$console_offset" "serial console login failed"
        return 1
    fi

    log_info "Running setup disk on VM $vm_id..."
    console_send "$vm_id" "sh /dev/vdb"
    if ! wait_for_ssh "$vm_id" 180; then
        report_boot_failure "$vm_id" "$console_offset" "SSH not reachable after setup"
        return 1
    fi

    log_success "VM $vm_id configured and SSH verified"
}

console_output_since() {
    local vm_id="$1"
    local offset="$2"
    tail -c +"$((offset + 1))" "$(get_vm_console_log "$vm_id")" 2>/dev/null || true
}

console_send() {
    local vm_id="$1"
    local line="$2"
    printf '%s\n' "$line" | nc -U -q 1 "$(get_vm_serial_socket "$vm_id")" > /dev/null 2>&1 || true
}

wait_for_console() {
    local vm_id="$1"
    local offset="$2"
    local text="$3"
    local timeout="$4"
    local elapsed=0

    until [[ "$(console_output_since "$vm_id" "$offset")" == *"$text"* ]]; do
        if (( elapsed >= timeout )) || ! is_vm_running "$vm_id"; then
            return 1
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
}

console_login() {
    local vm_id="$1"
    local offset="$2"
    local attempt

    for attempt in 1 2 3; do
        console_send "$vm_id" "root"
        if wait_for_console "$vm_id" "$offset" ":~# " 15; then
            return 0
        fi
    done
    return 1
}

wait_for_ssh() {
    local vm_id="$1"
    local timeout="$2"
    local elapsed=0

    until ssh_run "$vm_id" true < /dev/null 2>/dev/null; do
        if (( elapsed >= timeout )) || ! is_vm_running "$vm_id"; then
            return 1
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
}

report_boot_failure() {
    local vm_id="$1"
    local offset="$2"
    local reason="$3"

    log_error "VM $vm_id: $reason"
    if ! is_vm_running "$vm_id"; then
        log_error "QEMU exited, see ${RESULTS_DIR}/vm${vm_id}.qemu.log:"
        tail -n 5 "${RESULTS_DIR}/vm${vm_id}.qemu.log" >&2 || true
    fi
    log_error "Console output (full log: $(get_vm_console_log "$vm_id")):"
    console_output_since "$vm_id" "$offset" | tail -n 20 >&2
}

# =============================================================================
# GoClusterFS Setup
# =============================================================================

# Setup GoClusterFS on a VM after it's healthy
setup_goclusterfs() {
    local vm_id="$1"
    local format_disk="${2:-false}"
    # Extra environment for the mount process, such as a test failpoint
    local mount_env="${3:-}"
    # Block device to mount, such as a device-mapper device over the shared disk
    local device="${4:-$(cluster_device)}"
    # Extra mount flags, such as -fencing=none with a format
    local extra_args="${5:-}"
    local ssh_port=$(get_vm_ssh_port "$vm_id")

    log_info "Setting up GoClusterFS on VM $vm_id (format=$format_disk)..."

    log_info "Copying goclusterfs binaries..."
    local binaries=("${VM_RUN_DIR}/goclusterfs-mount")
    if [[ -x "${VM_RUN_DIR}/goclusterfs-fsck" ]]; then
        binaries+=("${VM_RUN_DIR}/goclusterfs-fsck")
    fi
    scp ${SSH_OPTIONS} -i "$(get_ssh_key)" -P "$ssh_port" \
        "${binaries[@]}" "${SSH_USER}@127.0.0.1:/usr/local/bin/"

    # Mount GoClusterFS
    # Virtio volumes have no reservations, and tests run several nodes on them; on a fenced volume the flag does nothing.
    local mount_args="-mount ${GOCLUSTERFS_MOUNT} -device ${device} -broadcast-iface ${BROADCAST_IFACE} -unsafe-allow-unfenced ${extra_args}"
    if [[ "$format_disk" == "true" ]]; then
        log_info "Formatting shared disk on VM $vm_id..."
        # One inode per 64 KB gives the 1 GB disk 16384 inodes, enough for test_more_than_4096_files
        mount_args="$mount_args -format -bytes-per-inode 65536"
    fi

    # /var/log/goclusterfs.log is lost when the VM dies; the log device keeps a copy on the host
    log_info "Mounting GoClusterFS..."
    ssh_run "$vm_id" \
        "nohup env $mount_env /usr/local/bin/goclusterfs-mount $mount_args < /dev/null 2>&1 | tee /var/log/goclusterfs.log > /dev/goclusterfs-log 2>/dev/null &"

    # Wait for mount to be ready
    wait_for_goclusterfs_mount "$vm_id"
}

# Wait for GoClusterFS mount to be ready
wait_for_goclusterfs_mount() {
    local vm_id="$1"
    local timeout="${2:-${GOCLUSTERFS_MOUNT_TIMEOUT:-30}}"
    local elapsed=0

    log_info "Waiting for GoClusterFS mount on VM $vm_id..."

    while [[ $elapsed -lt $timeout ]]; do
        if ssh_run "$vm_id" "mountpoint -q ${GOCLUSTERFS_MOUNT}" 2>/dev/null; then
            log_success "GoClusterFS mounted on VM $vm_id"
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done

    log_error "GoClusterFS mount timeout on VM $vm_id, last lines of $(get_vm_mount_log "$vm_id"):"
    tail -n 20 "$(get_vm_mount_log "$vm_id")" >&2 || true
    return 1
}

# Stop VM gracefully
stop_vm() {
    local vm_id="$1"
    local pid_file=$(get_vm_pid_file "$vm_id")

    if ! is_vm_running "$vm_id"; then
        log_info "VM $vm_id is not running"
        rm -f "$pid_file" "$(get_vm_monitor_socket "$vm_id")" "$(get_vm_serial_socket "$vm_id")"
        return 0
    fi

    log_info "Stopping VM $vm_id gracefully..."
    mark_vm_log "$vm_id" "VM $vm_id stop"

    # Try graceful shutdown via monitor (Unix socket)
    local monitor_socket=$(get_vm_monitor_socket "$vm_id")
    echo "system_powerdown" | nc -U -q1 "$monitor_socket" > /dev/null 2>&1 || true

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
    rm -f "$(get_vm_monitor_socket "$vm_id")" "$(get_vm_serial_socket "$vm_id")"
    log_success "VM $vm_id stopped"
}

# Force kill VM
kill_vm() {
    local vm_id="$1"
    local pid_file=$(get_vm_pid_file "$vm_id")

    if is_vm_running "$vm_id"; then
        kill -9 "$(cat "$pid_file")" 2>/dev/null || true
        local waited=0
        while is_vm_running "$vm_id"; do
            if [[ $waited -ge 50 ]]; then
                log_error "VM $vm_id: QEMU still running 10s after SIGKILL"
                return 1
            fi
            sleep 0.2
            waited=$((waited + 1))
        done
        mark_vm_log "$vm_id" "VM $vm_id killed"
    fi
    rm -f "$pid_file"
    # Clean up sockets
    rm -f "$(get_vm_monitor_socket "$vm_id")" "$(get_vm_serial_socket "$vm_id")"

    log_info "VM $vm_id killed"
}

vm_ids_with_pid_files() {
    local pid_file
    for pid_file in "${VM_DIR}"/vm*.pid; do
        if [[ -f "$pid_file" ]]; then
            basename "$pid_file" | sed 's/vm\([0-9]*\).pid/\1/'
        fi
    done
}

kill_all_vms() {
    local vm_id
    for vm_id in $(vm_ids_with_pid_files); do
        kill_vm "$vm_id"
    done
}

# =============================================================================
# SSH Functions for Tests
# =============================================================================
ssh_run() {
    local vm_id="$1"
    shift
    local ssh_port=$(get_vm_ssh_port "$vm_id")

    ssh ${SSH_OPTIONS} -i "$(get_ssh_key)" -p "$ssh_port" "${SSH_USER}@127.0.0.1" "$@"
}

# iscsi_disk prints the disk VM $1's iSCSI session attached, such as sdb; every login can name it anew.
iscsi_disk() {
    ssh_run "$1" "iscsiadm -m session -P 3 | sed -n 's/.*Attached scsi disk \\([a-z]*\\).*/\\1/p' | head -1"
}

# iscsi_disks prints every disk VM $1's iSCSI sessions attached, one per path.
iscsi_disks() {
    ssh_run "$1" "iscsiadm -m session -P 3 | sed -n 's/.*Attached scsi disk \\([a-z]*\\).*/\\1/p' | xargs"
}

# iscsi_add_path logs VM $1 in to the target once more through iface p$2, as a second path with a session of its own.
iscsi_add_path() {
    local portal="10.0.2.2:${ISCSI_PORT}"
    ssh_run "$1" "iscsiadm -m iface -I p$2 -o new > /dev/null 2>&1; iscsiadm -m iface -I p$2 -o update -n iface.transport_name -v tcp > /dev/null
        iscsiadm -m node -T ${ISCSI_TARGET_IQN} -p ${portal} -I p$2 -o new > /dev/null
        iscsiadm -m node -T ${ISCSI_TARGET_IQN} -p ${portal} -I p$2 -o update -n node.session.timeo.replacement_timeout -v 5
        iscsiadm -m node -T ${ISCSI_TARGET_IQN} -p ${portal} -I p$2 --login > /dev/null && sleep 2"
}

# iscsi_drop_path logs VM $1's path p$2 out and deletes it. Its node record replaced the default one, without which a
# login through iface default finds no record, so it creates that again.
iscsi_drop_path() {
    local node="-T ${ISCSI_TARGET_IQN} -p 10.0.2.2:${ISCSI_PORT}"
    ssh_run "$1" "iscsiadm -m node $node -I p$2 --logout > /dev/null 2>&1
        iscsiadm -m node $node -I p$2 -o delete > /dev/null 2>&1; iscsiadm -m iface -I p$2 -o delete > /dev/null 2>&1
        iscsiadm -m node $node -I default > /dev/null 2>&1 && exit 0
        iscsiadm -m node $node -o new > /dev/null && iscsiadm -m node $node -o update -n node.session.timeo.replacement_timeout -v 5"
}

# multipath_over sets VM $1's dm-multipath map gcfs-mpath on the disks after $1, by default every iSCSI disk of VM $1,
# one path each. A map that exists takes the disks without flushing the writes it queued while it had no path.
multipath_over() {
    local vm=$1 disks
    shift
    disks=${*:-$(iscsi_disks "$vm")}
    [[ -n "$disks" ]] || return 1
    local paths="" n=0 d
    for d in $disks; do
        paths="$paths /dev/$d 1"
        n=$((n + 1))
    done
    ssh_run "$vm" "command -v dmsetup > /dev/null || apk add --no-cache device-mapper > /dev/null; modprobe dm-multipath && t=\"0 \$(blockdev --getsz /dev/${disks%% *}) multipath 1 queue_if_no_path 0 1 1 round-robin 0 $n 1$paths\" && if dmsetup info gcfs-mpath > /dev/null 2>&1; then dmsetup suspend --noflush gcfs-mpath && dmsetup load gcfs-mpath --table \"\$t\" && dmsetup resume gcfs-mpath; else dmsetup create gcfs-mpath --table \"\$t\"; fi"
}

# ssh_run_capped is ssh_run that gives up after $1 seconds, for a command the guest's kernel may never finish.
ssh_run_capped() {
    local secs="$1" vm_id="$2"
    shift 2
    timeout -s KILL "$secs" ssh ${SSH_OPTIONS} -i "$(get_ssh_key)" -p "$(get_vm_ssh_port "$vm_id")" "${SSH_USER}@127.0.0.1" "$@"
}

# VMs accept only the run's SSH key; kept for existing callers
ssh_run_password() {
    ssh_run "$@"
}

wait_vm_healthy() {
    local vm_id="$1"
    local timeout="${2:-120}"

    log_info "Waiting for VM $vm_id to be healthy..."

    local elapsed=0
    while [[ $elapsed -lt $timeout ]]; do
        # First check if SSH is reachable at all
        if ssh_run "$vm_id" "true" 2>/dev/null; then
            log_success "VM $vm_id is healthy (SSH reachable)"
            return 0
        fi

        sleep "${HEALTH_CHECK_INTERVAL}"
        elapsed=$((elapsed + HEALTH_CHECK_INTERVAL))
    done

    log_error "VM $vm_id failed to become healthy within ${timeout}s"
    return 1
}

# Ensure a VM is running and healthy, restarting if necessary.
# This function makes tests resilient to prior test failures by verifying
# the VM is accessible before proceeding. If the VM is unreachable, it
# restarts the VM and waits for it to become healthy.
ensure_vm_running() {
    local vm_id="$1"

    # Check if VM is reachable via SSH
    if ssh_run "$vm_id" "true" 2>/dev/null; then
        # VM is healthy, check if GoClusterFS is mounted
        if ssh_run "$vm_id" "mountpoint -q ${GOCLUSTERFS_MOUNT}" 2>/dev/null; then
            return 0  # Fully healthy
        fi
        # SSH works but GoClusterFS not mounted - remount
        log_info "VM $vm_id SSH OK but GoClusterFS not mounted, remounting..."
        setup_goclusterfs "$vm_id" false
        return 0
    fi

    # VM not reachable - restart it
    log_warn "VM $vm_id not reachable, restarting..."
    kill_vm "$vm_id" 2>/dev/null || true
    start_vm "$vm_id" false
    if ! wait_vm_healthy "$vm_id" 120; then
        log_error "Failed to restart VM $vm_id"
        return 1
    fi
    setup_goclusterfs "$vm_id" false
    return 0
}

# Ensure all cluster VMs are running and healthy.
# Called at the start of tests that require multiple VMs to be operational.
ensure_cluster_healthy() {
    local node_count="${1:-2}"
    for vm_id in $(seq 1 "$node_count"); do
        if ! ensure_vm_running "$vm_id"; then
            return 1
        fi
    done
    return 0
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

    ssh_run "$vm_id" "echo '$content' > ${GOCLUSTERFS_MOUNT}/$filename"
}

read_file_on_vm() {
    local vm_id="$1"
    local filename="$2"

    ssh_run "$vm_id" "cat ${GOCLUSTERFS_MOUNT}/$filename"
}

file_exists_on_vm() {
    local vm_id="$1"
    local filename="$2"

    ssh_run "$vm_id" "test -f ${GOCLUSTERFS_MOUNT}/$filename"
}

# Clean up test files from the GoClusterFS filesystem
# This removes all files and directories from the mount point to reclaim disk space
# Should be called before each test to ensure a clean state
cleanup_test_files() {
    local vm_id="${1:-1}"  # Default to VM 1 (shared filesystem, so only need to clean from one node)

    log_info "Cleaning up test files from GoClusterFS..."

    # Check if VM is reachable and GoClusterFS is mounted
    if ! ssh_run "$vm_id" "mountpoint -q ${GOCLUSTERFS_MOUNT}" 2>/dev/null; then
        log_warn "GoClusterFS not mounted on VM $vm_id, skipping cleanup"
        return 0
    fi

    # Remove all files and directories from mount point
    # Use find to avoid "argument list too long" errors with many files
    # Also handles hidden files (.*) safely
    ssh_run "$vm_id" "find ${GOCLUSTERFS_MOUNT} -mindepth 1 -delete 2>/dev/null || rm -rf ${GOCLUSTERFS_MOUNT}/* ${GOCLUSTERFS_MOUNT}/.[!.]* 2>/dev/null" || true

    # Sync to ensure blocks are freed
    ssh_run "$vm_id" "sync"

    # Log free space for debugging
    local free_space
    free_space=$(ssh_run "$vm_id" "df -h ${GOCLUSTERFS_MOUNT} 2>/dev/null | tail -1 | awk '{print \$4}'" 2>/dev/null) || true
    if [[ -n "$free_space" ]]; then
        log_info "Free space after cleanup: $free_space"
    fi

    return 0
}

# =============================================================================
# Cleanup
# =============================================================================
cleanup() {
    log_info "Cleaning up all VMs..."

    local vm_id
    for vm_id in $(vm_ids_with_pid_files); do
        stop_vm "$vm_id" 2>/dev/null || true
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

    # In the background so traps run while waiting; the runner gets SIGTERM if this shell dies
    setpriv --pdeathsig TERM -- "${SCRIPT_DIR}/scripts/run-vm-tests.sh" "$node_count" &
    wait $!
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
            start_vm_run
            run_tests "$nodes"
            ;;
        --nodes=*)
            local nodes="${command#--nodes=}"
            start_vm_run
            run_tests "$nodes"
            ;;
        --start-vm)
            shift
            VM_REAPER=(setsid)
            init_vm_run
            start_vm "$1"
            log_info "SSH: ssh -i $(get_ssh_key) -p $(get_vm_ssh_port "$1") root@127.0.0.1"
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
            remove_idle_run_dirs
            ;;
        --cleanup-all)
            cleanup_all
            ;;
        --help|-h)
            echo "GoClusterFS VM Test Harness"
            echo ""
            echo "Usage:"
            echo "  $0 --run-tests [--nodes=N]  Run tests with N VMs (default: 2)"
            echo "  $0 --start-vm ID           Start specific VM"
            echo "  $0 --stop-vm ID            Stop specific VM"
            echo "  $0 --kill-vm ID            Force kill VM"
            echo "  $0 --cleanup               Stop all VMs"
            echo "  $0 --cleanup-all           Remove all runtime files"
            echo "  $0 --help                  Show this help"
            echo ""
            echo "Environment:"
            echo "  VM_SSH_BASE_PORT  Host SSH port of VM 1 (default: 2222); use a different"
            echo "                    value for each checkout that runs VMs at the same time"
            ;;
        *)
            log_error "Unknown command: $command"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
}

# Run if executed directly; a sourcing test runner joins the run or starts its own
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
else
    start_vm_run
fi
