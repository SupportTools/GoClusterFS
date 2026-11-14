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
#   unmount-all         unmount every node
#   mount-all           mount every node that is not mounted
#   fsck [-repair]      run goclusterfs-fsck on VM 1 with every node unmounted; answers
#                       "out <line>" for each problem line, then "ok rc=<exit code>"
#   save-image DEST     copy the shared disk to DEST
#   down                stop every VM and exit

set -uo pipefail

SOAK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NODE_COUNT=${NODE_COUNT:-3}
source "${SOAK_DIR}/../vm/vm-test-harness.sh" >&2

reply() { echo "$*"; }

mount_pid() {
    ssh_run "$1" "pidof goclusterfs-mount" 2>/dev/null
}

# Kills a node's mount process and clears its mount point, as a crash leaves it.
kill_mount() {
    ssh_run "$1" "pkill -9 -x goclusterfs-mount; sleep 1; fusermount3 -uz ${GOCLUSTERFS_MOUNT} 2>/dev/null; true"
}

remount() {
    kill_mount "$1" && setup_goclusterfs "$1" false
}

unmount_node() {
    local vm_id=$1 waited=0
    ssh_run "$vm_id" "fusermount3 -u ${GOCLUSTERFS_MOUNT}" || true
    while ssh_run "$vm_id" "pidof goclusterfs-mount > /dev/null"; do
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
                setup_goclusterfs "$vm_id" "$([[ $vm_id -eq 1 ]] && echo true || echo false)" >&2 || return 1
                ports+=("$(get_vm_ssh_port "$vm_id")")
            done
            reply "ok $(get_ssh_key) ${ports[*]}"
            ;;
        power)
            kill_vm "$1" >&2 && start_vm "$1" false >&2 && wait_vm_healthy "$1" >&2 &&
                setup_goclusterfs "$1" false >&2 && reply ok
            ;;
        sigkill)
            remount "$1" >&2 && reply ok
            ;;
        stop)
            local pid
            pid=$(mount_pid "$1") || return 1
            ssh_run "$1" "kill -STOP $pid" >&2 || return 1
            sleep "$2"
            ssh_run "$1" "kill -CONT $pid" >&2
            # The other nodes declared it failed and fenced it, so it has to mount again.
            remount "$1" >&2 && reply ok
            ;;
        remount)
            remount "$1" >&2 && reply ok
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
                    setup_goclusterfs "$vm_id" false >&2 || return 1
                fi
            done
            reply ok
            ;;
        fsck)
            local out rc
            out=$(ssh_run 1 "/usr/local/bin/goclusterfs-fsck ${1:-} /dev/vda 2>&1; echo rc=\$?")
            rc=${out##*rc=}
            echo "$out" | grep -E '^ *\[(WARN|ERROR)\]' | sed 's/^ */out /'
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
