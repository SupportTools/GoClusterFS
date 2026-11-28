#!/bin/bash
# Cluster controller for the soak driver. It owns the VMs, which die with it, and
# answers one command per stdin line with one "ok ..." or "err ..." line on stdout.
# Everything else goes to stderr.
#
#   up                  boot NODE_COUNT VMs, format and mount; answers "ok <ssh key> <port>..."
#   power VM            cut power to VM, boot it again and mount
#   sigkill VM          SIGKILL VM's goclusterfs-mount and mount again
#   stop VM SECONDS     SIGSTOP VM's goclusterfs-mount for SECONDS, resume it, then mount again
#   remount VM          kill VM's goclusterfs-mount and mount again
#   relogin VM [SECS]   log VM's iSCSI session out and in, with its mount stopped for SECS around it if given; answers
#                       "ok survived" if the mount runs on, else "ok stopped" once VM is mounted again
#   cut FAULT VM [SECS] only the first half of power, sigkill or stop: the node is left down
#   restore FAULT VM    the second half: boot and mount the node again
#   unmount-all         unmount every node
#   mount-all           mount every node that is not mounted
#   fsck [-repair]      run goclusterfs-fsck on VM 1 with every node unmounted; answers
#                       "out <line>" for each line of its output, then "ok rc=<exit code>"
#   save-image DEST     copy the shared disk to DEST
#   down                stop every VM and exit

set -uo pipefail

SOAK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NODE_COUNT=${NODE_COUNT:-3}
# A remount after a stop fault has taken 22s just to register on a loaded host.
export GOCLUSTERFS_MOUNT_TIMEOUT=${GOCLUSTERFS_MOUNT_TIMEOUT:-90}
source "${SOAK_DIR}/../vm/vm-test-harness.sh" >&2
# Turns on the API's snapshot writes; the agent sends it to delete orphan snapshots.
MOUNT_ENV="GOCLUSTERFS_API_TOKEN=soak-api-token"
# With MULTIPATH=true nodes mount a dm-multipath map over their iSCSI disk, which outlives a relogin.
MULTIPATH=${MULTIPATH:-false}
# How long a relogin waits before it judges whether the mount ran on.
RELOGIN_SETTLE=${RELOGIN_SETTLE:-30}

reply() { echo "$*"; }

# Matches goclusterfs-mount by its command line: the kernel cuts process names to 15
# characters, and in the VMs the binary runs under gcompat's loader, whose command line
# starts with ld-linux-x86-64.so.2. The bracket keeps the pattern from matching the shell
# that runs it.
MOUNT_PATTERN="'goclusterfs-moun[t] -mount'"

mount_pid() {
    ssh_run "$1" "pgrep -f $MOUNT_PATTERN" 2>/dev/null
}

# Kills a node's mount process and clears its mount point, as a crash leaves it.
kill_mount() {
    ssh_run "$1" "pkill -9 -f $MOUNT_PATTERN; sleep 1; fusermount3 -uz ${GOCLUSTERFS_MOUNT} 2>/dev/null; true"
}

# Prints the device VM $1 mounts; an iSCSI login can name the disk anew, so it is looked up each time.
node_device() {
    local disk
    if [[ "$MULTIPATH" == true ]]; then
        multipath_over "$1" && echo /dev/mapper/gcfs-mpath
    elif [[ "$VM_STORAGE" == iscsi ]]; then
        disk=$(iscsi_disk "$1") && [[ -n "$disk" ]] && echo "/dev/$disk"
    else
        cluster_device
    fi
}

# Mounts VM $1, formatting the volume first if $2 is true.
mount_node() {
    local device fencing=""
    device=$(node_device "$1") || return 1
    if [[ "$2" == true && "$MULTIPATH" == true ]]; then
        fencing=-fencing=pr
    fi
    setup_goclusterfs "$1" "$2" "$MOUNT_ENV" "$device" "$fencing"
}

remount() {
    kill_mount "$1" && mount_node "$1" false
}

# Logs VM $1's only iSCSI path out and in under its multipath map, with the mount stopped for $2 seconds around it if
# given. Prints "survived" if the same mount process still serves the mount point after RELOGIN_SETTLE, else mounts
# VM $1 again and prints "stopped".
relogin_node() {
    local vm_id=$1 pause=${2:-0} pid disk i portal
    portal=$(iscsi_portal)
    pid=$(mount_pid "$vm_id") || return 1
    if (( pause > 0 )); then
        ssh_run "$vm_id" "kill -STOP $pid" || return 1
    fi
    ssh_run "$vm_id" "iscsiadm -m node -T ${ISCSI_TARGET_IQN} -p ${portal} --logout > /dev/null; sleep 2; iscsiadm -m node -T ${ISCSI_TARGET_IQN} -p ${portal} --login > /dev/null" || return 1
    for i in $(seq 1 20); do
        disk=$(iscsi_disk "$vm_id")
        [[ -n "$disk" ]] && ssh_run "$vm_id" "test -b /dev/$disk" && break
        sleep 1
    done
    multipath_over "$vm_id" "$disk" || return 1
    if (( pause > 0 )); then
        sleep "$pause"
        ssh_run "$vm_id" "kill -CONT $pid" || true
    fi
    sleep "$RELOGIN_SETTLE"
    if [[ "$(mount_pid "$vm_id")" == "$pid" ]] && ssh_run "$vm_id" "mountpoint -q ${GOCLUSTERFS_MOUNT}"; then
        echo survived
        return 0
    fi
    remount "$vm_id" >&2 || return 1
    echo stopped
}

cut_node() {
    case "$1" in
        power) kill_vm "$2" ;;
        sigkill) ssh_run "$2" "pkill -9 -f $MOUNT_PATTERN" ;;
        stop)
            local pid
            pid=$(mount_pid "$2") || return 1
            ssh_run "$2" "kill -STOP $pid" || return 1
            sleep "$3"
            ssh_run "$2" "kill -CONT $pid" || true
            ;;
        *) return 1 ;;
    esac
}

restore_node() {
    if [[ $1 == power ]]; then
        start_vm "$2" false && wait_vm_healthy "$2" && mount_node "$2" false
    else
        # After a stop the other nodes declared this one failed and fenced it, so it has to mount again.
        remount "$2"
    fi
}

unmount_node() {
    local vm_id=$1 waited=0
    ssh_run "$vm_id" "fusermount3 -u ${GOCLUSTERFS_MOUNT}" || true
    while ssh_run "$vm_id" "pgrep -f $MOUNT_PATTERN > /dev/null"; do
        if [[ $waited -ge 30 ]]; then
            log_warn "VM $vm_id: goclusterfs-mount still running 30s after unmount, killing it"
            kill_mount "$vm_id"
            return 0
        fi
        sleep 1
        waited=$((waited + 1))
    done
}

handle() {
    local cmd=$1
    shift
    case "$cmd" in
        up)
            create_shared_disk >&2 || return 1
            local vm_id ports=()
            for vm_id in $(seq 1 "$NODE_COUNT"); do
                start_vm "$vm_id" "$([[ $vm_id -eq 1 ]] && echo true || echo false)" >&2 || return 1
            done
            for vm_id in $(seq 1 "$NODE_COUNT"); do
                wait_vm_healthy "$vm_id" >&2 || return 1
                mount_node "$vm_id" "$([[ $vm_id -eq 1 ]] && echo true || echo false)" >&2 || return 1
                ports+=("$(get_vm_ssh_port "$vm_id")")
            done
            reply "ok $(get_ssh_key) ${ports[*]}"
            ;;
        power | sigkill | stop)
            cut_node "$cmd" "$@" >&2 && restore_node "$cmd" "$1" >&2 && reply ok
            ;;
        cut)
            cut_node "$@" >&2 && reply ok
            ;;
        restore)
            restore_node "$@" >&2 && reply ok
            ;;
        remount)
            remount "$1" >&2 && reply ok
            ;;
        relogin)
            local outcome
            outcome=$(relogin_node "$@") && reply "ok $outcome"
            ;;
        unmount-all)
            local vm_id
            for vm_id in $(seq 1 "$NODE_COUNT"); do
                unmount_node "$vm_id" >&2
            done
            reply ok
            ;;
        mount-all)
            local vm_id
            for vm_id in $(seq 1 "$NODE_COUNT"); do
                if ! ssh_run "$vm_id" "mountpoint -q ${GOCLUSTERFS_MOUNT}"; then
                    mount_node "$vm_id" false >&2 || return 1
                fi
            done
            reply ok
            ;;
        fsck)
            local out rc device body
            device=$(node_device 1) || return 1
            out=$(ssh_run 1 "/usr/local/bin/goclusterfs-fsck ${1:-} $device 2>&1; echo rc=\$?")
            rc=${out##*rc=}
            body=${out%rc=*}
            [[ -z "$body" || "$body" == *$'\n' ]] || body+=$'\n'
            printf '%s' "$body" | sed 's/^/out /'
            reply "ok rc=$rc"
            ;;
        save-image)
            cp --sparse=always "${SHARED_DISK_PATH}" "$1" >&2 && reply ok
            ;;
        down)
            reply ok
            exit 0
            ;;
        *)
            reply "err unknown command $cmd"
            return 0
            ;;
    esac
}

while read -r line; do
    # shellcheck disable=SC2086
    handle $line || reply "err $line failed"
done
