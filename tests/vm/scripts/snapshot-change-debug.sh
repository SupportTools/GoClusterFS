# Instrumented copy of test_snapshot_during_writes for investigating snapshots that change.
test_snapshot_during_writes_debug() {
    log_info "DEBUG-RUN start $(date +%T.%N)"
    ensure_cluster_healthy 2

    local run="$$_${RANDOM}"
    local file=/mnt/goclusterfs/busy.bin

    ssh_run 1 "head -c 2097152 /dev/zero | tr '\\000' c > $file" || return 1
    ssh_run 1 "rm -f /tmp/busy.stop; for c in a b c; do head -c 65536 /dev/zero | tr '\\000' \$c > /tmp/busy_\$c; done; while [ ! -f /tmp/busy.stop ]; do for c in a b c; do for k in \$(seq 0 31); do [ -f /tmp/busy.stop ] && exit 0; dd if=/tmp/busy_\$c of=$file bs=65536 seek=\$k count=1 conv=notrunc 2>/dev/null || exit 1; done; done; done" &
    local writer=$!

    local i clone
    local clones=() firsts=()
    for i in $(seq 1 10); do
        clone=/mnt/goclusterfs/busy_${run}_$i.bin
        if ! ssh_run 2 "setfattr -n user.snapshot -v busy_${run}_$i $file && setfattr -n user.snapshot.clone -v busy_${run}_$i:/busy_${run}_$i.bin $file"; then
            log_error "snapshot $i on VM 2 failed"
            ssh_run 1 "touch /tmp/busy.stop"
            wait "$writer"
            return 1
        fi
        clones+=("$clone")
        local info
        info=$(ssh_run 2 "t=\$(date +%T.%N); ino=\$(stat -c %i $clone); cp $clone /tmp/first_${run}_$i; s=\$(md5sum < /tmp/first_${run}_$i | cut -c1-12); echo \"t=\$t ino=\$ino first=\$s\"")
        log_info "DEBUG-SNAP run=$run i=$i $info"
        firsts+=("$(echo "$info" | sed 's/.*first=//')")
    done
    ssh_run 1 "touch /tmp/busy.stop"
    wait "$writer" || { log_error "writer on VM 1 failed"; return 1; }

    local failed=0
    for i in "${!clones[@]}"; do
        clone=${clones[$i]}
        local n=$((i + 1))
        local cached dropped direct vm1
        cached=$(ssh_run 2 "md5sum < $clone | cut -c1-12")
        dropped=$(ssh_run 2 "sync; echo 3 > /proc/sys/vm/drop_caches; md5sum < $clone | cut -c1-12")
        direct=$(ssh_run 2 "dd if=$clone iflag=direct bs=1M 2>/dev/null | md5sum | cut -c1-12")
        vm1=$(ssh_run 1 "echo 3 > /proc/sys/vm/drop_caches; md5sum < $clone | cut -c1-12")
        log_info "DEBUG-CHECK run=$run i=$n t=$(date +%T.%N) first=${firsts[$i]} cached=$cached dropped=$dropped direct=$direct vm1=$vm1"
        if [ "$cached" != "${firsts[$i]}" ] || [ "$dropped" != "${firsts[$i]}" ] || [ "$direct" != "${firsts[$i]}" ] || [ "$vm1" != "${firsts[$i]}" ]; then
            failed=1
            local diff
            diff=$(ssh_run 2 "cmp /tmp/first_${run}_$n $clone | head -1; off=\$(cmp /tmp/first_${run}_$n $clone | head -1 | sed -n 's/.*byte \([0-9]*\).*/\1/p'); if [ -n \"\$off\" ]; then c=\$(( (off - 1) / 4096 )); echo chunk=\$c offset=\$((c*4096)); echo first:; dd if=/tmp/first_${run}_$n bs=4096 skip=\$c count=1 2>/dev/null | od -An -tx1 | head -2; echo now:; dd if=$clone bs=4096 skip=\$c count=1 2>/dev/null | od -An -tx1 | head -2; fi; for k in \$(seq 0 31); do dd if=/tmp/first_${run}_$n bs=65536 skip=\$k count=1 2>/dev/null | tr -s abc | head -c 2; done; echo ' <- first chunks'; for k in \$(seq 0 31); do dd if=$clone bs=65536 skip=\$k count=1 2>/dev/null | tr -s abc | head -c 2; done; echo ' <- now chunks'")
            log_error "DEBUG-DIFF run=$run i=$n
$diff"
        fi
    done

    ssh_run 2 "for i in \$(seq 1 10); do setfattr -x user.snapshot.busy_${run}_\$i $file; rm -f /mnt/goclusterfs/busy_${run}_\$i.bin /tmp/first_${run}_\$i; done"
    log_info "DEBUG-RUN end $(date +%T.%N) failed=$failed"
    [ "$failed" -eq 0 ]
}
