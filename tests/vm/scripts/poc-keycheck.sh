#!/bin/bash
# B key-check POC: the LIO target VM of the harness, with two Ubuntu 26.04 node VMs (kernel 7.0, which has
# IOC_PR_READ_KEYS). Runs prpoc's re-register scenarios on one- and two-path dm-multipath maps, then the QEMU-stop
# pause with -fencing=pr. $1 is the host directory for results; UBUNTU_IMAGE names the cloud image.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HARNESS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
RESULTS_DIR="${HARNESS_DIR}/results"
export VM_STORAGE=iscsi
source "${HARNESS_DIR}/vm-test-harness.sh"
set +e

OUT=$1
mkdir -p "$OUT"
UB=${UBUNTU_IMAGE:?set UBUNTU_IMAGE}
SEED_PORT=$((SSH_BASE_PORT + 150))
PORTAL="10.0.2.2:${ISCSI_PORT}"
N=1
P=2
KN=0x4743010000000001
KP=0x4743020000000002
MAP=/dev/mapper/gcfs-mpath
MNT=/mnt/goclusterfs

say() { echo "=== $(date '+%T') $*" | tee -a "$OUT/steps.txt"; }
on() {
    local vm=$1
    shift
    echo "vm$vm\$ $*" >> "$OUT/steps.txt"
    ssh_run "$vm" "$*" 2>&1 | tee -a "$OUT/steps.txt"
}
put() { scp ${SSH_OPTIONS} -i "$(get_ssh_key)" -P "$(get_vm_ssh_port "$1")" "$2" "root@127.0.0.1:$3"; }

write_seed() {
    local vm=$1 dir="${VM_RUN_DIR}/seed/vm$1"
    mkdir -p "$dir"
    printf 'instance-id: poc-vm%s\nlocal-hostname: node%s\n' "$vm" "$vm" > "$dir/meta-data"
    cat > "$dir/user-data" <<EOF
#cloud-config
disable_root: false
ssh_pwauth: false
ssh_authorized_keys:
  - $(cat "$(get_ssh_key).pub")
package_update: false
package_upgrade: false
bootcmd:
  - [sh, -c, "echo InitiatorName=${ISCSI_TARGET_IQN%:target}:vm$vm > /etc/iscsi/initiatorname.iscsi"]
EOF
}

boot_ubuntu() {
    local vm=$1 overlay="${VM_RUN_DIR}/ubuntu-vm$1.qcow2" waited=0
    write_seed "$vm"
    qemu-img create -q -f qcow2 -F qcow2 -b "$UB" "$overlay" 8G || return 1
    rm -f "$(get_vm_monitor_socket "$vm")"
    "${VM_REAPER[@]}" qemu-system-x86_64 -name "goclusterfs-vm${vm}" -machine q35,accel="$(get_accelerator)" \
        -cpu "$(get_cpu_type)" -m 2048 -smp 2 -drive "file=${overlay},format=qcow2,if=virtio" \
        -netdev "user,id=net0,hostfwd=tcp:127.0.0.1:$(get_vm_ssh_port "$vm")-:22" -device virtio-net-pci,netdev=net0 \
        -smbios "type=1,serial=ds=nocloud;s=http://10.0.2.2:${SEED_PORT}/vm${vm}/" \
        -monitor "unix:$(get_vm_monitor_socket "$vm"),server,nowait" -serial "file:$(get_vm_console_log "$vm")" \
        -display none {VM_RUN_LOCK_FD}>&- >> "${RESULTS_DIR}/vm${vm}.qemu.log" 2>&1 &
    echo $! > "$(get_vm_pid_file "$vm")"
    disown
    until ssh_run "$vm" true 2> /dev/null; do
        if [[ $waited -ge 300 ]]; then
            log_error "VM $vm: no SSH 300s after boot"
            return 1
        fi
        sleep 3
        waited=$((waited + 3))
    done
    ssh_run "$vm" "cloud-init status --wait > /dev/null; systemctl stop multipathd.socket multipathd 2> /dev/null; systemctl restart iscsid; modprobe dm-multipath; echo vm$vm \$(uname -r) \$(cat /etc/iscsi/initiatorname.iscsi)" | tee -a "$OUT/env.txt"
}

# login VM PATHS logs VM in to the target over PATHS sessions, one per iface, and prints their disks.
login() {
    local vm=$1 paths=$2
    ssh_run "$vm" "for i in \$(seq 0 $((paths - 1))); do
            iscsiadm -m iface -I p\$i -o new > /dev/null 2>&1; iscsiadm -m iface -I p\$i -o update -n iface.transport_name -v tcp > /dev/null
            iscsiadm -m node -T $ISCSI_TARGET_IQN -p $PORTAL -I p\$i -o new > /dev/null
            iscsiadm -m node -T $ISCSI_TARGET_IQN -p $PORTAL -I p\$i -o update -n node.session.timeo.replacement_timeout -v 5
            iscsiadm -m node -T $ISCSI_TARGET_IQN -p $PORTAL -I p\$i --login > /dev/null
        done; sleep 3; lsblk -dn -o NAME,TRAN | awk '\$2 == \"iscsi\" {print \$1}' | xargs"
}

logout_all() {
    ssh_run "$1" "dmsetup remove gcfs-mpath 2> /dev/null; iscsiadm -m node -T $ISCSI_TARGET_IQN --logout > /dev/null 2>&1; iscsiadm -m node -o delete > /dev/null 2>&1; true"
}

# map VM points gcfs-mpath at VM's current iSCSI disks, as a reload keeps a map across a relogin.
map() {
    ssh_run "$1" "disks=\$(lsblk -dn -o NAME,TRAN | awk '\$2 == \"iscsi\" {print \"/dev/\" \$1}'); n=\$(echo \$disks | wc -w); t=\"0 \$(blockdev --getsz \$(echo \$disks | cut -d' ' -f1)) multipath 0 0 1 1 round-robin 0 \$n 1\$(for d in \$disks; do printf ' %s 1' \$d; done)\"
        if dmsetup info gcfs-mpath > /dev/null 2>&1; then dmsetup suspend --noflush gcfs-mpath && dmsetup load gcfs-mpath --table \"\$t\" && dmsetup resume gcfs-mpath; else dmsetup create gcfs-mpath --table \"\$t\"; fi; dmsetup table gcfs-mpath" | tee -a "$OUT/steps.txt"
}

# relogin VM logs VM's last path out and in again, as a session loss does, and reloads the map with the new disk.
relogin() {
    ssh_run "$1" "i=\$(iscsiadm -m session -P 1 | awk '/Iface Name:/ {print \$3}' | sort | tail -1); iscsiadm -m node -T $ISCSI_TARGET_IQN -p $PORTAL -I \$i --logout > /dev/null && sleep 1 && iscsiadm -m node -T $ISCSI_TARGET_IQN -p $PORTAL -I \$i --login > /dev/null && sleep 3; iscsiadm -m session | tr -s ' ' | cut -d' ' -f2-4 | xargs echo sessions:" | tee -a "$OUT/steps.txt"
    map "$1" > /dev/null
}

peer_disk() { ssh_run "$P" "lsblk -dn -o NAME,TRAN | awk '\$2 == \"iscsi\" {print \"/dev/\" \$1; exit}'"; }

# scenarios PATHS runs re-register cases A, B and C on node VM N with a PATHS-path map, peer VM P on one disk.
scenarios() {
    local paths=$1 pd
    say "scenarios on a ${paths}-path map"
    logout_all "$N"
    logout_all "$P"
    login "$N" "$paths" | tee -a "$OUT/steps.txt"
    map "$N" > /dev/null
    login "$P" 1 > /dev/null
    pd=$(peer_disk)
    on "$P" "prpoc $pd $KP clear; prpoc $pd $KP register; prpoc $pd $KP reserve"
    on "$N" "prpoc $MAP $KN register; prpoc $MAP $KN list; prpoc $MAP $KN write"

    say "A ($paths paths): a path logs out and in; the key stays listed under the old session"
    relogin "$N"
    on "$N" "prpoc $MAP $KN write; prpoc $MAP $KN list; prpoc $MAP $KN rekey; prpoc $MAP $KN write; prpoc $MAP $KN list"

    say "B ($paths paths): the peer preempts before the re-register, with the clock paused (TestRekeyAfterAPausedClockLandsNoWrite)"
    relogin "$N"
    on "$P" "prpoc $pd $KP preempt $KN"
    on "$N" "prpoc $MAP $KN write; prpoc $MAP $KN list; prpoc $MAP $KN rekey -nocheck; prpoc $MAP $KN write"
    on "$P" "prpoc $pd $KP preempt $KN"
    on "$N" "prpoc $MAP $KN write; prpoc $MAP $KN rekey; prpoc $MAP $KN write; prpoc $MAP $KN list"

    local check
    for check in "" -nocheck; do
        say "C ($paths paths, ${check:-key check}): the peer preempts between the listing and the register"
        on "$N" "prpoc $MAP $KN register > /dev/null"
        relogin "$N"
        on "$N" "rm -f /tmp/go; (prpoc $MAP $KN rekey $check -wait /tmp/go > /tmp/rekey.out 2>&1 &); for t in \$(seq 1 100); do grep -q before-register /tmp/rekey.out && break; sleep 0.1; done; cat /tmp/rekey.out"
        on "$P" "prpoc $pd $KP preempt $KN"
        on "$N" "touch /tmp/go; sleep 2; cat /tmp/rekey.out; prpoc $MAP $KN write; prpoc $MAP $KN list"
        on "$P" "prpoc $pd $KP preempt $KN > /dev/null; true"
    done
    on "$P" "prpoc $pd $KP clear"
}

# pause_case BINARY [window] runs the QEMU-stop pause with -fencing=pr: node N on a two-path map writes A, is stopped
# until the peer replays it and writes B, and is resumed. The file must keep B. With window, the peer holds its takeover
# 15s between the preempt and the Dead mark, and N resumes as soon as the peer revoked it, inside that window.
pause_case() {
    local bin=$1 window=${2:-} pd uuid stopped mon byte peer_env=""
    [[ -n "$window" ]] && peer_env="GOCLUSTERFS_POC_TAKEOVER_DELAY=15s"
    mon=$(get_vm_monitor_socket "$N")
    say "QEMU-stop pause with -fencing=pr, $bin${window:+, resumed inside the takeover window}"
    logout_all "$N"
    logout_all "$P"
    login "$N" 2 > /dev/null
    map "$N" > /dev/null
    login "$P" 1 > /dev/null
    pd=$(peer_disk)
    on "$P" "prpoc $pd $KP clear > /dev/null; prpoc $pd $KP unregister > /dev/null; true"
    for vm in $N $P; do
        ssh_run "$vm" "pkill -f goclusterfs-moun[t]; sleep 1; fusermount3 -uz $MNT 2> /dev/null; mkdir -p $MNT; rm -f /var/log/goclusterfs.log; true"
    done
    # Only node N runs the build under test; the peer always runs the POC build, which alone has the takeover hold.
    ssh_run "$N" "cp /usr/local/bin/$bin /usr/local/bin/goclusterfs-mount"
    ssh_run "$P" "cp /usr/local/bin/goclusterfs-mount.poc /usr/local/bin/goclusterfs-mount"
    ssh_run "$N" "nohup /usr/local/bin/goclusterfs-mount -device $MAP -mount $MNT -format -fencing=pr > /var/log/goclusterfs.log 2>&1 < /dev/null &"
    ssh_run "$N" "for t in \$(seq 1 60); do mountpoint -q $MNT && exit 0; sleep 1; done; tail -5 /var/log/goclusterfs.log; exit 1" || return 1
    ssh_run "$P" "nohup env $peer_env /usr/local/bin/goclusterfs-mount -device $pd -mount $MNT > /var/log/goclusterfs.log 2>&1 < /dev/null &"
    ssh_run "$P" "for t in \$(seq 1 60); do mountpoint -q $MNT && exit 0; sleep 1; done; tail -5 /var/log/goclusterfs.log; exit 1" || return 1
    uuid=$(ssh_run "$N" "grep -o 'Registered as node [0-9]* (UUID: [0-9a-f]*' /var/log/goclusterfs.log | tail -1" | grep -o '[0-9a-f]*$')
    ssh_run "$P" "head -c 4096 /dev/zero | tr '\\000' 0 > $MNT/paused && sync"
    ssh_run "$N" "head -c 4096 /dev/zero | tr '\\000' A > /tmp/paused.A; nohup sh -c 'while :; do dd if=/tmp/paused.A of=$MNT/paused bs=4096 count=1 conv=notrunc,fsync 2>/dev/null; done' < /dev/null > /dev/null 2>&1 &"
    ssh_run "$P" "head -c 4096 /dev/zero | tr '\\000' B > /tmp/paused.B"
    sleep 3
    echo stop | nc -U -q1 "$mon" > /dev/null
    stopped=$(date +%s)
    local waited=0
    if [[ -n "$window" ]]; then
        until ssh_run "$P" "grep -q 'Revoked node ${uuid:0:8}' /var/log/goclusterfs.log"; do
            if [[ $waited -ge 120 ]]; then
                say "no revoke of ${uuid:0:8} within 120s"
                echo cont | nc -U -q1 "$mon" > /dev/null
                return 1
            fi
            sleep 0.5
            waited=$((waited + 1))
        done
        echo cont | nc -U -q1 "$mon" > /dev/null
        say "peer revoked node ${uuid:0:8}; resumed it $(( $(date +%s) - stopped ))s after the stop, inside the takeover window"
        waited=0
    fi
    until ssh_run "$P" "grep -q 'Starting journal replay for node ${uuid:0:8}' /var/log/goclusterfs.log && grep -q 'Recovery\] Completed\|Journal is clean' /var/log/goclusterfs.log"; do
        if [[ $waited -ge 120 ]]; then
            say "no replay of ${uuid:0:8} within 120s"
            echo cont | nc -U -q1 "$mon" > /dev/null
            pause_end
            return 1
        fi
        sleep 2
        waited=$((waited + 2))
    done
    say "peer replayed node ${uuid:0:8} $(( $(date +%s) - stopped ))s after the stop"
    sleep 3
    on "$P" "dd if=/tmp/paused.B of=$MNT/paused bs=4096 count=1 conv=notrunc,fsync 2>/dev/null; echo 3 > /proc/sys/vm/drop_caches; dd if=$MNT/paused iflag=direct bs=4096 count=1 2>/dev/null | head -c 1; echo ' (peer reads after its write)'"
    [[ -z "$window" ]] && echo cont | nc -U -q1 "$mon" > /dev/null
    say "resumed or running $(( $(date +%s) - stopped ))s after the stop"
    sleep 15
    byte=$(ssh_run "$P" "echo 3 > /proc/sys/vm/drop_caches; dd if=$MNT/paused iflag=direct bs=4096 count=1 2>/dev/null | head -c 1")
    say "15s after the resume the file starts with '$byte'"
    pause_end
}

# pause_end logs both nodes' fencing and recovery lines and stops both mounts.
pause_end() {
    say "node mount $(ssh_run "$N" "pgrep -f goclusterfs-moun[t] > /dev/null && echo running || echo stopped")"
    on "$N" "grep -E 'Fencing|registry slot|refused|re-register' /var/log/goclusterfs.log | tail -8"
    on "$P" "grep -E 'Recovery|POC|Revoked' /var/log/goclusterfs.log | tail -8"
    ssh_run "$N" "pkill -f 'of=$MNT/pause[d]'; pkill -9 -f goclusterfs-moun[t]; sleep 1; fusermount3 -uz $MNT 2> /dev/null; true"
    ssh_run "$P" "fusermount3 -u $MNT; sleep 3; pkill -9 -f goclusterfs-moun[t]; true"
}

mkdir -p "${VM_RUN_DIR}/seed"
"${VM_REAPER[@]}" python3 -m http.server --bind 127.0.0.1 "$SEED_PORT" --directory "${VM_RUN_DIR}/seed" > "$OUT/seed-http.log" 2>&1 &
say "boot"
start_vm "$TARGET_VM_ID" || exit 1
boot_ubuntu "$N" || exit 1
boot_ubuntu "$P" || exit 1
for vm in $N $P; do
    put "$vm" "${PROJECT_ROOT}/prpoc" /usr/local/bin/prpoc
    put "$vm" "${PROJECT_ROOT}/goclusterfs-mount" /usr/local/bin/goclusterfs-mount.poc
    put "$vm" "${PROJECT_ROOT}/goclusterfs-mount.rekey" /usr/local/bin/goclusterfs-mount.rekey
done
scenarios 1
scenarios 2
pause_case goclusterfs-mount.poc
pause_case goclusterfs-mount.rekey
pause_case goclusterfs-mount.poc window
pause_case goclusterfs-mount.rekey window
say "done"
