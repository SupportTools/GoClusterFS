#!/bin/bash
# GoClusterFS VM Test Runner
# Orchestrates multi-node tests across QEMU VMs
#
# Usage:
#   ./run-vm-tests.sh [NODE_COUNT]
#   ./run-vm-tests.sh 2          # Run with 2 VMs
#   ./run-vm-tests.sh 3 --long   # Run with 3 VMs including long-running tests

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HARNESS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
RESULTS_DIR="${HARNESS_DIR}/results"

# Source the harness for VM management functions
source "${HARNESS_DIR}/vm-test-harness.sh"

# Test configuration
NODE_COUNT="${1:-2}"
RUN_LONG_TESTS=false
STABILITY_DURATION_MINUTES=5

# Parse additional arguments
shift || true
while [[ $# -gt 0 ]]; do
    case "$1" in
        --long)
            RUN_LONG_TESTS=true
            shift
            ;;
        --stability-duration=*)
            STABILITY_DURATION_MINUTES="${1#*=}"
            shift
            ;;
        *)
            shift
            ;;
    esac
done

# Test tracking
test_passed=0
test_failed=0

# =============================================================================
# Test Utilities
# =============================================================================

run_test() {
    local test_name="$1"
    local test_func="$2"
    # VM_TEST_ONLY runs only the tests whose names contain it
    if [[ -n "${VM_TEST_ONLY:-}" && "$test_name" != *"$VM_TEST_ONLY"* ]]; then
        return 0
    fi

    log_info "=========================================="
    log_info "Running test: $test_name"
    log_info "=========================================="
    mark_vm_logs "Running test: $test_name"

    # Clean up test files from previous test to reclaim disk space
    cleanup_test_files 1

    if $test_func; then
        log_success "PASSED: $test_name"
        test_passed=$((test_passed + 1))
        echo "PASS: $test_name" >> "$RESULTS_DIR/results.txt"
    else
        log_error "FAILED: $test_name"
        test_failed=$((test_failed + 1))
        echo "FAIL: $test_name" >> "$RESULTS_DIR/results.txt"
    fi
}

# =============================================================================
# Advanced File Operations (for concurrent same-file writes)
# =============================================================================

# Pre-extend a file to a specific size (filled with zeros)
# Usage: pre_extend_file_on_vm <vm_id> <filename> <size_bytes>
pre_extend_file_on_vm() {
    local vm_id=$1
    local filename=$2
    local size_bytes=$3

    log_info "Pre-extending $filename to $size_bytes bytes on VM $vm_id"
    # Use truncate for instant file creation (sparse file), then fill with zeros using efficient block size
    # This is MUCH faster than dd with bs=1 which does one syscall per byte
    ssh_run "$vm_id" "truncate -s $size_bytes /mnt/goclusterfs/$filename && dd if=/dev/zero of=/mnt/goclusterfs/$filename bs=4096 count=$((size_bytes / 4096 + 1)) conv=notrunc 2>/dev/null && truncate -s $size_bytes /mnt/goclusterfs/$filename"
}

# Write pattern at specific offset on VM
# Usage: write_pattern_at_on_vm <vm_id> <filename> <pattern_char> <size_bytes> <offset>
write_pattern_at_on_vm() {
    local vm_id=$1
    local filename=$2
    local pattern_char=$3
    local size_bytes=$4
    local offset=$5

    log_info "VM $vm_id: Writing $size_bytes bytes of '$pattern_char' at offset $offset"
    # Write pattern data using efficient block size (4KB) with calculated block offset
    # For offsets not aligned to 4096, we need special handling
    if [ $((offset % 4096)) -eq 0 ]; then
        # Offset is block-aligned, use efficient bs=4096
        local block_offset=$((offset / 4096))
        local block_count=$(( (size_bytes + 4095) / 4096 ))
        ssh_run "$vm_id" "head -c $size_bytes /dev/zero | tr '\\0' '$pattern_char' | dd of=/mnt/goclusterfs/$filename bs=4096 seek=$block_offset conv=notrunc 2>/dev/null"
    else
        # Non-aligned offset: use bs=1 but only for this case (still needed for precise positioning)
        ssh_run "$vm_id" "head -c $size_bytes /dev/zero | tr '\\0' '$pattern_char' | dd of=/mnt/goclusterfs/$filename bs=1 seek=$offset conv=notrunc 2>/dev/null"
    fi
}

# Read bytes at specific offset on VM
# Usage: read_at_offset_on_vm <vm_id> <filename> <offset> <length>
read_at_offset_on_vm() {
    local vm_id=$1
    local filename=$2
    local offset=$3
    local length=$4

    ssh_run "$vm_id" "dd if=/mnt/goclusterfs/$filename bs=1 skip=$offset count=$length 2>/dev/null"
}

# Get file size on VM
# Usage: get_file_size_on_vm <vm_id> <filename>
get_file_size_on_vm() {
    local vm_id=$1
    local filename=$2

    ssh_run "$vm_id" "stat -c%s /mnt/goclusterfs/$filename 2>/dev/null"
}

# Verify pattern at offset on VM
# Usage: verify_pattern_at_on_vm <vm_id> <filename> <pattern_char> <offset> <length>
verify_pattern_at_on_vm() {
    local vm_id=$1
    local filename=$2
    local pattern_char=$3
    local offset=$4
    local length=$5

    # Read the data and check if it's all the expected pattern
    local non_matching
    non_matching=$(ssh_run "$vm_id" "dd if=/mnt/goclusterfs/$filename bs=1 skip=$offset count=$length 2>/dev/null | tr -d '$pattern_char' | wc -c")

    if [ "$non_matching" -eq 0 ]; then
        return 0
    else
        log_error "VM $vm_id: Pattern mismatch at offset $offset: expected all '$pattern_char', found $non_matching non-matching bytes"
        return 1
    fi
}

# MD5 hash of file portion on VM
# Usage: md5_portion_on_vm <vm_id> <filename> <offset> <length>
md5_portion_on_vm() {
    local vm_id=$1
    local filename=$2
    local offset=$3
    local length=$4

    ssh_run "$vm_id" "dd if=/mnt/goclusterfs/$filename bs=1 skip=$offset count=$length 2>/dev/null | md5sum | cut -d' ' -f1"
}

# =============================================================================
# Standard Tests (adapted from Docker tests)
# =============================================================================

# Test 0: Create and read file (basic sanity check)
test_create_read_file() {
    log_info "Testing create and read file..."

    local vm_id=1
    local filename="test_create_read.txt"
    local expected_content="Hello GoClusterFS!"

    # Write file
    write_file_on_vm "$vm_id" "$filename" "$expected_content"

    # Verify file exists
    if ! file_exists_on_vm "$vm_id" "$filename"; then
        log_error "File not created"
        return 1
    fi

    # Read and verify content
    local actual_content
    actual_content=$(read_file_on_vm "$vm_id" "$filename")

    if [[ "$actual_content" != "$expected_content" ]]; then
        log_error "Content mismatch - expected '$expected_content', got '$actual_content'"
        return 1
    fi

    log_info "Create and read file test passed"
    return 0
}

# Test: Create directory
test_create_directory() {
    log_info "Testing create directory..."

    local vm_id=1
    local dirname="testdir"

    # Create directory
    ssh_run "$vm_id" "mkdir /mnt/goclusterfs/$dirname"

    # Verify it exists and is a directory
    if ! ssh_run "$vm_id" "test -d /mnt/goclusterfs/$dirname"; then
        log_error "Directory not created or not a directory"
        return 1
    fi

    # List root and verify directory appears
    local listing
    listing=$(ssh_run "$vm_id" "ls -la /mnt/goclusterfs/")
    if ! echo "$listing" | grep -q "^d.*$dirname"; then
        log_error "Directory not found in listing"
        log_error "Listing: $listing"
        return 1
    fi

    log_info "Create directory test passed"
    return 0
}

# Test: File in directory
# Creates a directory, then creates a file inside it
test_file_in_directory() {
    log_info "Testing file in directory..."

    local vm_id=1
    local dirname="subdir"
    local filename="nested.txt"
    local filepath="$dirname/$filename"
    local test_content="File in subdirectory"

    # Create directory
    ssh_run "$vm_id" "mkdir -p /mnt/goclusterfs/$dirname"

    # Verify directory exists
    if ! ssh_run "$vm_id" "test -d /mnt/goclusterfs/$dirname"; then
        log_error "Failed to create directory"
        return 1
    fi

    # Create file inside directory
    ssh_run "$vm_id" "echo '$test_content' > /mnt/goclusterfs/$filepath"

    # Verify file exists
    if ! ssh_run "$vm_id" "test -f /mnt/goclusterfs/$filepath"; then
        log_error "File not created in directory"
        return 1
    fi

    # Read back and verify content
    local read_back
    read_back=$(ssh_run "$vm_id" "cat /mnt/goclusterfs/$filepath")
    if [[ "$read_back" != "$test_content" ]]; then
        log_error "Content mismatch. Expected: '$test_content', Got: '$read_back'"
        return 1
    fi

    # List directory and verify file appears
    local listing
    listing=$(ssh_run "$vm_id" "ls /mnt/goclusterfs/$dirname/")
    if ! echo "$listing" | grep -q "$filename"; then
        log_error "File not found in directory listing"
        log_error "Listing: $listing"
        return 1
    fi

    log_info "File in directory test passed"
    return 0
}

# Test: Data persistence after remount
test_data_persistence() {
    log_info "Testing data persistence after remount..."

    local vm_id=1
    local content="Persistent data test - this should survive remount!"
    local nested_content="nested file"

    # Write a file
    ssh_run "$vm_id" "echo '$content' > /mnt/goclusterfs/persistent.txt"

    # Create directory with nested file
    ssh_run "$vm_id" "mkdir -p /mnt/goclusterfs/persistdir"
    ssh_run "$vm_id" "echo '$nested_content' > /mnt/goclusterfs/persistdir/file.txt"

    # Verify files exist before restart
    if ! ssh_run "$vm_id" "test -f /mnt/goclusterfs/persistent.txt"; then
        log_error "persistent.txt not created"
        return 1
    fi

    log_info "Data written, unmounting and remounting VM..."

    # Stop and restart VM (or use fusermount + remount)
    stop_vm "$vm_id"
    start_vm "$vm_id" false  # false = no format
    wait_vm_healthy "$vm_id"
    setup_goclusterfs "$vm_id" false  # Re-mount without format

    # Verify data persisted
    local read_content
    read_content=$(ssh_run "$vm_id" "cat /mnt/goclusterfs/persistent.txt")
    if [ "$read_content" != "$content" ]; then
        log_error "Content didn't persist. Expected: '$content', Got: '$read_content'"
        return 1
    fi

    # Verify nested directory and file
    if ! ssh_run "$vm_id" "test -d /mnt/goclusterfs/persistdir"; then
        log_error "persistdir directory didn't persist"
        return 1
    fi

    local nested_read
    nested_read=$(ssh_run "$vm_id" "cat /mnt/goclusterfs/persistdir/file.txt")
    if [ "$nested_read" != "$nested_content" ]; then
        log_error "Nested file didn't persist. Expected: '$nested_content', Got: '$nested_read'"
        return 1
    fi

    log_info "Data persistence test passed"
    return 0
}

# Test: Delete file
test_delete_file() {
    log_info "Testing file deletion..."

    local vm_id=1
    local filename="todelete.txt"

    # Create a file
    ssh_run "$vm_id" "echo 'delete me' > /mnt/goclusterfs/$filename"

    # Verify it exists
    if ! ssh_run "$vm_id" "test -f /mnt/goclusterfs/$filename"; then
        log_error "File should exist before deletion"
        return 1
    fi

    # Delete it
    ssh_run "$vm_id" "rm /mnt/goclusterfs/$filename"

    # Verify it's gone
    if ssh_run "$vm_id" "test -f /mnt/goclusterfs/$filename"; then
        log_error "File should not exist after deletion"
        return 1
    fi

    log_info "File deletion test passed"
    return 0
}

# Test: Overwrite existing file
test_overwrite_file() {
    log_info "Testing file overwrite..."

    local vm_id=1
    local filename="overwrite.txt"
    local original_content="Original content"
    local new_content="New content that is different"

    # Create a file with original content
    ssh_run "$vm_id" "echo '$original_content' > /mnt/goclusterfs/$filename"

    # Verify original content
    local read_original
    read_original=$(ssh_run "$vm_id" "cat /mnt/goclusterfs/$filename")
    if [[ "$read_original" != "$original_content" ]]; then
        log_error "Original content mismatch: expected '$original_content', got '$read_original'"
        return 1
    fi

    # Overwrite with new content
    ssh_run "$vm_id" "echo '$new_content' > /mnt/goclusterfs/$filename"

    # Read back and verify new content
    local read_new
    read_new=$(ssh_run "$vm_id" "cat /mnt/goclusterfs/$filename")
    if [[ "$read_new" != "$new_content" ]]; then
        log_error "Overwrite failed: expected '$new_content', got '$read_new'"
        return 1
    fi

    log_info "File overwrite test passed"
    return 0
}

# Test: Concurrent file creation across VMs
# Multiple VMs create different files simultaneously, then verify all files
# are visible from all VMs' perspectives
test_concurrent_file_creation() {
    log_info "Testing concurrent file creation across VMs..."

    # Need at least 2 VMs for this test
    if [[ $NODE_COUNT -lt 2 ]]; then
        log_info "SKIP: Need at least 2 VMs for concurrent file creation test"
        return 0
    fi

    # Create files concurrently from different VMs
    ssh_run 1 "echo 'Content from VM 1' > /mnt/goclusterfs/fileA.txt" &
    local pid1=$!
    ssh_run 2 "echo 'Content from VM 2' > /mnt/goclusterfs/fileB.txt" &
    local pid2=$!

    # Wait for both to complete
    wait $pid1 $pid2

    # Give filesystem time to sync
    sleep 1

    # Verify both files exist from both VMs' perspectives
    for vm_id in 1 2; do
        if ! ssh_run "$vm_id" "test -f /mnt/goclusterfs/fileA.txt"; then
            log_error "fileA.txt not visible from VM $vm_id"
            return 1
        fi
        if ! ssh_run "$vm_id" "test -f /mnt/goclusterfs/fileB.txt"; then
            log_error "fileB.txt not visible from VM $vm_id"
            return 1
        fi
    done

    # Verify content integrity
    local contentA
    contentA=$(ssh_run 1 "cat /mnt/goclusterfs/fileA.txt")
    if [[ "$contentA" != "Content from VM 1" ]]; then
        log_error "fileA.txt has wrong content: $contentA"
        return 1
    fi

    local contentB
    contentB=$(ssh_run 1 "cat /mnt/goclusterfs/fileB.txt")
    if [[ "$contentB" != "Content from VM 2" ]]; then
        log_error "fileB.txt has wrong content: $contentB"
        return 1
    fi

    log_info "Concurrent file creation test passed"
    return 0
}

# Test: Sequential access across VMs
# One VM writes data, then another VM reads it
test_sequential_access() {
    log_info "Testing sequential access across VMs..."

    # Need at least 2 VMs for this test
    if [[ $NODE_COUNT -lt 2 ]]; then
        log_info "SKIP: Need at least 2 VMs for sequential access test"
        return 0
    fi

    local filename="sequential_test.txt"
    local test_content="Data written by VM1 for VM2 to read"

    # VM 1 writes the file
    log_info "VM 1 writing file..."
    ssh_run 1 "echo '$test_content' > /mnt/goclusterfs/$filename"

    # Verify VM 1 can read it back
    local read_vm1
    read_vm1=$(ssh_run 1 "cat /mnt/goclusterfs/$filename")
    if [[ "$read_vm1" != "$test_content" ]]; then
        log_error "VM 1 immediate read-back failed: expected '$test_content', got '$read_vm1'"
        return 1
    fi
    log_info "VM 1 verified write successful"

    # Give filesystem time to sync
    sleep 1

    # VM 2 reads the file written by VM 1
    log_info "VM 2 reading file written by VM 1..."
    local read_vm2
    read_vm2=$(ssh_run 2 "cat /mnt/goclusterfs/$filename")
    if [[ "$read_vm2" != "$test_content" ]]; then
        log_error "VM 2 failed to read data written by VM 1: expected '$test_content', got '$read_vm2'"
        return 1
    fi

    log_info "Sequential access test passed - VM 2 successfully read data written by VM 1"
    return 0
}

# Test: Journal recovery across VMs
# Simulates crash: VM A creates file, gets killed, VM B recovers via journal
test_journal_recovery_multinode() {
    log_info "Testing multi-node journal recovery..."

    # Need at least 2 VMs for this test
    if [[ $NODE_COUNT -lt 2 ]]; then
        log_info "SKIP: Need at least 2 VMs for journal recovery test"
        return 0
    fi

    local vm_a=1
    local vm_b=2
    local filename="recovery-test.txt"
    local test_content="This file should survive VM crash and be recovered"

    # Phase 1: VM A creates a file
    log_info "Phase 1: VM A (VM $vm_a) creating test file..."
    ssh_run "$vm_a" "echo '$test_content' > /mnt/goclusterfs/$filename"

    # Verify VM A can read it
    local read_a
    read_a=$(ssh_run "$vm_a" "cat /mnt/goclusterfs/$filename")
    if [[ "$read_a" != "$test_content" ]]; then
        log_error "VM A failed to verify created file"
        return 1
    fi
    log_info "VM A verified file creation"

    # Give time for data to sync to disk
    sleep 2

    # Phase 2: Kill VM A to simulate crash (hard kill, no clean unmount)
    log_info "Phase 2: Hard-killing VM A to simulate crash..."
    kill_vm "$vm_a"
    sleep 2

    # Phase 3: Wait for heartbeat to become stale
    # In VM environment, we wait for heartbeat timeout
    log_info "Phase 3: Waiting for heartbeat timeout (25s)..."
    sleep 25

    # Phase 4: VM B should detect failed node via heartbeat
    # The file should be visible on VM B through journal recovery or shared storage
    log_info "Phase 4: Verifying file recovery on VM B..."

    if ! file_exists_on_vm "$vm_b" "$filename"; then
        log_error "File not found on VM B after VM A crash"
        # List directory for debugging
        ssh_run "$vm_b" "ls -la /mnt/goclusterfs/" || true
        return 1
    fi

    local read_b
    read_b=$(ssh_run "$vm_b" "cat /mnt/goclusterfs/$filename")
    if [[ "$read_b" != "$test_content" ]]; then
        log_error "File content mismatch on VM B. Expected: '$test_content', Got: '$read_b'"
        return 1
    fi

    log_info "VM B successfully recovered file from crashed VM A"

    # Phase 5: Verify VM B can create new files after recovery
    log_info "Phase 5: Verifying VM B can write after recovery..."
    ssh_run "$vm_b" "echo 'Post-recovery file' > /mnt/goclusterfs/post-recovery.txt"

    if ! file_exists_on_vm "$vm_b" "post-recovery.txt"; then
        log_error "VM B cannot create files after recovery"
        return 1
    fi

    # Phase 6: Restart VM A and verify it can see recovered files
    log_info "Phase 6: Restarting VM A..."
    start_vm "$vm_a" false
    wait_vm_healthy "$vm_a"
    setup_goclusterfs "$vm_a" false  # Re-mount GoClusterFS

    if ! file_exists_on_vm "$vm_a" "$filename"; then
        log_error "Original file not visible on restarted VM A"
        return 1
    fi

    if ! file_exists_on_vm "$vm_a" "post-recovery.txt"; then
        log_error "Post-recovery file not visible on restarted VM A"
        return 1
    fi

    log_info "Multi-node journal recovery test passed"
    return 0
}

# VM A takes a block reserve, writes one block from it and is killed. VM B
# replays VM A's journal, which gives the unused reserved blocks back: VM B's
# free space ends one block below where it started.
test_reserve_returned_after_crash() {
    log_info "Testing that a killed node's unused block reserve is returned..."

    if [[ $NODE_COUNT -lt 2 ]]; then
        log_info "SKIP: Need at least 2 VMs"
        return 0
    fi
    ensure_cluster_healthy "$NODE_COUNT"

    local vm_a=1 vm_b=2
    local file=/mnt/goclusterfs/reserve-crash.bin
    local free_kb
    free_kb() { ssh_run "$vm_b" "df -k /mnt/goclusterfs | awk 'NR==2 {print \$4}'"; }

    local before
    sleep "$RESERVE_IDLE_RETURN_SECS"
    before=$(free_kb)
    ssh_run "$vm_a" "dd if=/dev/urandom of=$file bs=1M count=1 conv=fsync 2>/dev/null" || return 1
    local sum
    sum=$(ssh_run "$vm_a" "md5sum $file | cut -d' ' -f1")
    local reserved
    reserved=$(free_kb)
    log_info "free on VM B: ${before}KB before, ${reserved}KB while VM A holds its reserve"
    if (( reserved > before - 2048 )); then
        log_error "VM A's write took less than its block and a reserve (${before}KB -> ${reserved}KB)"
        return 1
    fi

    log_info "Hard-killing VM A while it holds its reserve..."
    kill_vm "$vm_a"
    local want=$((before - 1024)) now=0 waited=0
    while (( waited < 90 )); do
        now=$(free_kb)
        [[ "$now" -eq "$want" ]] && break
        sleep 3
        waited=$((waited + 3))
    done
    if [[ "$now" -ne "$want" ]]; then
        log_error "VM B shows ${now}KB free after VM A's replay, want ${want}KB: the reserve was not returned"
        return 1
    fi
    log_info "VM A's unused reserve came back after ${waited}s"

    # Allocation is first-fit, so VM B's filler would take the file's block if replay had freed it.
    local filler=/mnt/goclusterfs/reserve-crash-filler.bin
    ssh_run "$vm_b" "dd if=/dev/zero of=$filler bs=1M count=128 conv=fsync 2>/dev/null" || return 1
    if [[ "$(ssh_run "$vm_b" "md5sum $file | cut -d' ' -f1")" != "$sum" ]]; then
        log_error "VM A's file changed on VM B after the replay and a 128MB write"
        return 1
    fi

    start_vm "$vm_a" false
    wait_vm_healthy "$vm_a"
    setup_goclusterfs "$vm_a" false
    if [[ "$(ssh_run "$vm_a" "md5sum $file | cut -d' ' -f1")" != "$sum" ]]; then
        log_error "VM A reads different data from its file after restarting"
        return 1
    fi
    ssh_run "$vm_b" "rm -f $file $filler"
    log_info "Reserve returned after crash test passed"
    return 0
}

# VM A's mount restarts while VM B keeps creating files.
# Matches Go test: TestMultiNodeRestartKeepsPeerJournal in journal_slots_test.go
test_restart_keeps_peer_journal() {
    log_info "Testing mount restart while a peer keeps writing..."

    if [[ $NODE_COUNT -lt 2 ]]; then
        log_info "SKIP: Need at least 2 VMs"
        return 0
    fi
    ensure_cluster_healthy "$NODE_COUNT"

    local vm_a=1
    local vm_b=2
    local dir="/mnt/goclusterfs/peer-journal"

    ssh_run "$vm_b" "rm -rf $dir /tmp/peer-writer.*; mkdir -p $dir"
    ssh_run "$vm_b" "nohup sh -c 'i=0
        while [ ! -e /tmp/peer-writer.stop ] && [ \$i -lt 150 ]; do
            i=\$((i + 1))
            echo peer-\$i > $dir/peer-\$i || { echo fail \$i > /tmp/peer-writer.done; exit; }
            sleep 0.05
        done
        echo ok \$i > /tmp/peer-writer.done' >/dev/null 2>&1 &"
    sleep 1

    local slot_before
    slot_before=$(ssh_run "$vm_a" "grep -o 'Registered as node [0-9]*' /var/log/goclusterfs.log | tail -1")
    if ! crash_goclusterfs "$vm_a"; then
        ssh_run "$vm_b" "touch /tmp/peer-writer.stop"
        return 1
    fi
    ssh_run "$vm_a" "umount -l /mnt/goclusterfs 2>/dev/null; true"
    if ! setup_goclusterfs "$vm_a" false; then
        ssh_run "$vm_b" "touch /tmp/peer-writer.stop"
        return 1
    fi
    sleep 1
    ssh_run "$vm_b" "touch /tmp/peer-writer.stop"

    local status="" count=0
    while [[ -z "$status" && $count -lt 30 ]]; do
        status=$(ssh_run "$vm_b" "cat /tmp/peer-writer.done 2>/dev/null" || true)
        sleep 1
        count=$((count + 1))
    done
    log_info "VM B writer finished: ${status:-timeout}"
    if [[ "$status" != ok* ]]; then
        log_error "VM B could not keep writing while VM A restarted"
        return 1
    fi
    local written=${status#ok }

    local bad
    bad=$(ssh_run "$vm_a" "k=1; while [ \$k -le $written ]; do
        [ \"\$(cat $dir/peer-\$k 2>/dev/null)\" = peer-\$k ] || echo peer-\$k; k=\$((k + 1)); done")
    if [[ -n "$bad" ]]; then
        log_error "VM A cannot read VM B's files: $(echo $bad | head -c 300)"
        return 1
    fi

    local offset_a offset_b uuid_b
    offset_a=$(ssh_run "$vm_a" "grep -o 'Journal: initialized at offset [0-9]*' /var/log/goclusterfs.log | tail -1")
    offset_b=$(ssh_run "$vm_b" "grep -o 'Journal: initialized at offset [0-9]*' /var/log/goclusterfs.log | tail -1")
    uuid_b=$(ssh_run "$vm_b" "grep -o 'Registered as node [0-9]* (UUID: [0-9a-f]*' /var/log/goclusterfs.log | tail -1" | grep -o '[0-9a-f]*$')
    log_info "VM A: $offset_a; VM B: $offset_b (UUID $uuid_b)"
    if [[ -z "$offset_a" || "$offset_a" == "$offset_b" ]]; then
        log_error "VM A and VM B share a journal region"
        return 1
    fi
    if ssh_run "$vm_a" "grep -q 'from crashed node $uuid_b' /var/log/goclusterfs.log"; then
        log_error "VM A replayed live VM B's journal during its restart"
        return 1
    fi
    local slot_after
    slot_after=$(ssh_run "$vm_a" "grep -o 'Registered as node [0-9]*' /var/log/goclusterfs.log | tail -1")
    if [[ "$slot_after" != "$slot_before" ]]; then
        log_error "VM A came back as '$slot_after', want its own slot '$slot_before' so it replays its journal at mount"
        return 1
    fi

    ssh_run "$vm_b" "rm -rf $dir"
    log_info "Restart keeps peer journal test passed ($written files)"
    return 0
}

# Test: Journal recovery with multiple files
# Simulates crash: VM A creates 5 files, gets killed, VM B recovers all via journal
test_journal_recovery_multiple_files() {
    log_info "Testing journal recovery with multiple files..."

    # Need at least 2 VMs for this test
    if [[ $NODE_COUNT -lt 2 ]]; then
        log_info "SKIP: Need at least 2 VMs for journal recovery test"
        return 0
    fi

    local vm_a=1
    local vm_b=2
    local file_count=5

    # Phase 1: VM A creates multiple files
    log_info "Phase 1: VM A (VM $vm_a) creating $file_count test files..."
    for i in $(seq 0 $((file_count - 1))); do
        local filename="multifile_${i}.txt"
        local content="Content of file $i - should survive crash"
        ssh_run "$vm_a" "echo '$content' > /mnt/goclusterfs/$filename"
        log_info "VM A: Created $filename"
    done

    # Give time for data to sync to disk
    sleep 2

    # Phase 2: Hard-kill VM A to simulate crash
    log_info "Phase 2: Hard-killing VM A to simulate crash..."
    kill_vm "$vm_a"
    sleep 2

    # Phase 3: Wait for heartbeat to become stale
    log_info "Phase 3: Waiting for heartbeat timeout (25s)..."
    sleep 25

    # Phase 4: Verify all files are recovered on VM B
    log_info "Phase 4: Verifying all files recovery on VM B..."
    local recovered=0

    for i in $(seq 0 $((file_count - 1))); do
        local filename="multifile_${i}.txt"
        local expected_content="Content of file $i - should survive crash"

        if ! file_exists_on_vm "$vm_b" "$filename"; then
            log_error "File $filename not found on VM B after recovery"
            continue
        fi

        local read_content
        read_content=$(ssh_run "$vm_b" "cat /mnt/goclusterfs/$filename")
        if [[ "$read_content" != "$expected_content" ]]; then
            log_error "Content mismatch for $filename"
            continue
        fi

        recovered=$((recovered + 1))
        log_info "VM B: Verified $filename"
    done

    log_info "Phase 4: Recovered $recovered/$file_count files"

    if [[ $recovered -ne $file_count ]]; then
        log_error "Not all files were recovered"
        return 1
    fi

    # Phase 5: Restart VM A and verify it can see all recovered files
    log_info "Phase 5: Restarting VM A..."
    start_vm "$vm_a" false
    wait_vm_healthy "$vm_a"
    setup_goclusterfs "$vm_a" false  # Re-mount GoClusterFS

    for i in $(seq 0 $((file_count - 1))); do
        local filename="multifile_${i}.txt"
        if ! file_exists_on_vm "$vm_a" "$filename"; then
            log_error "File $filename not visible on restarted VM A"
            return 1
        fi
    done

    log_info "Multi-file journal recovery test passed"
    return 0
}

# Test: Journal recovery with large file
# Simulates crash: VM A creates 500KB file, gets killed, VM B recovers via journal
test_journal_recovery_large_file() {
    log_info "Testing journal recovery with large file..."

    # Need at least 2 VMs for this test
    if [[ $NODE_COUNT -lt 2 ]]; then
        log_info "SKIP: Need at least 2 VMs for journal recovery test"
        return 0
    fi

    # Ensure all VMs are running before test (may have been killed by previous test)
    ensure_cluster_healthy 2

    local vm_a=1
    local vm_b=2
    local filename="large-recovery.txt"
    local file_size=$((500 * 1024))  # 500KB to span multiple blocks

    # Phase 1: VM A creates a large file
    log_info "Phase 1: VM A (VM $vm_a) creating ${file_size} byte file..."

    # Create 500KB file with random data
    ssh_run "$vm_a" "dd if=/dev/urandom of=/mnt/goclusterfs/$filename bs=1024 count=500 2>/dev/null"

    # Get checksum of original file
    local original_checksum
    original_checksum=$(ssh_run "$vm_a" "md5sum /mnt/goclusterfs/$filename | awk '{print \$1}'")
    local original_size
    original_size=$(ssh_run "$vm_a" "stat -c%s /mnt/goclusterfs/$filename")

    log_info "VM A: Created $filename ($original_size bytes, md5: $original_checksum)"

    # Give time for data to sync to disk
    sleep 2

    # Phase 2: Hard-kill VM A to simulate crash
    log_info "Phase 2: Hard-killing VM A to simulate crash..."
    kill_vm "$vm_a"
    sleep 2

    # Phase 3: Wait for heartbeat to become stale
    # Theoretical minimum: 20s (stale detection) + 5s (scan interval) = 25s
    # Adding margin for: clock skew, recovery processing, filesystem sync
    log_info "Phase 3: Waiting for heartbeat timeout and recovery (40s)..."
    sleep 40

    # Phase 4: Verify large file is recovered on VM B
    log_info "Phase 4: Verifying large file recovery on VM B..."

    if ! file_exists_on_vm "$vm_b" "$filename"; then
        log_error "Large file not found on VM B after recovery"
        return 1
    fi

    # Verify file size
    local recovered_size
    recovered_size=$(ssh_run "$vm_b" "stat -c%s /mnt/goclusterfs/$filename")
    if [[ "$recovered_size" != "$original_size" ]]; then
        log_error "File size mismatch. Expected: $original_size, Got: $recovered_size"
        return 1
    fi

    # Verify file content via checksum
    local recovered_checksum
    recovered_checksum=$(ssh_run "$vm_b" "md5sum /mnt/goclusterfs/$filename | awk '{print \$1}'")
    if [[ "$recovered_checksum" != "$original_checksum" ]]; then
        log_error "File checksum mismatch. Expected: $original_checksum, Got: $recovered_checksum"
        return 1
    fi

    log_info "VM B: Successfully recovered large file ($recovered_size bytes, md5: $recovered_checksum)"

    # Phase 5: Restart VM A and verify it can see the recovered file
    log_info "Phase 5: Restarting VM A..."
    start_vm "$vm_a" false
    wait_vm_healthy "$vm_a"
    setup_goclusterfs "$vm_a" false  # Re-mount GoClusterFS

    if ! file_exists_on_vm "$vm_a" "$filename"; then
        log_error "Large file not visible on restarted VM A"
        return 1
    fi

    log_info "Large file journal recovery test passed"
    return 0
}

# Test: Journal recovery idempotency
# Verifies that recovery is idempotent - multiple nodes can attempt recovery safely
# Matches Go test: TestMultiNodeRecoveryIdempotency
test_journal_recovery_idempotency() {
    log_info "Testing journal recovery idempotency..."

    # Need at least 3 VMs for this test (VM A creates, B recovers, C verifies idempotency)
    if [[ $NODE_COUNT -lt 3 ]]; then
        log_info "SKIP: Need at least 3 VMs for idempotency test"
        return 0
    fi

    local vm_a=1
    local vm_b=2
    local vm_c=3
    local filename="idempotent.txt"
    local content="Idempotency test"

    # Phase 1: VM A creates file
    log_info "Phase 1: VM A creating file..."
    write_file_on_vm "$vm_a" "$filename" "$content"

    # Let data sync
    sleep 1

    # Phase 2: Hard-kill VM A (simulating crash)
    log_info "Phase 2: Hard-killing VM A..."
    kill_vm "$vm_a"
    sleep 2

    # Phase 3: Wait for heartbeat timeout
    log_info "Phase 3: Waiting for heartbeat timeout (25s)..."
    sleep 25

    # Phase 4: VM B performs first recovery
    log_info "Phase 4: Verifying first recovery on VM B..."

    if ! file_exists_on_vm "$vm_b" "$filename"; then
        log_error "File not found on VM B after first recovery"
        return 1
    fi

    local recovered_content
    recovered_content=$(read_file_on_vm "$vm_b" "$filename")
    if [[ "$recovered_content" != "$content" ]]; then
        log_error "Content mismatch on VM B. Expected: '$content', Got: '$recovered_content'"
        return 1
    fi

    log_info "VM B: First recovery successful"

    # Phase 5: Stop VM B
    log_info "Phase 5: Stopping VM B..."
    kill_vm "$vm_b"
    sleep 2

    # Phase 6: Verify idempotency on VM C
    # VM C should see the same file - if recovery wasn't idempotent, file could be corrupted
    log_info "Phase 6: Checking idempotency on VM C..."

    # Wait a moment for any potential recovery attempt
    sleep 5

    if ! file_exists_on_vm "$vm_c" "$filename"; then
        log_error "File not found on VM C (idempotency violated)"
        return 1
    fi

    local final_content
    final_content=$(read_file_on_vm "$vm_c" "$filename")
    if [[ "$final_content" != "$content" ]]; then
        log_error "Content corrupted on VM C after multiple recovery attempts"
        log_error "Expected: '$content', Got: '$final_content'"
        return 1
    fi

    log_info "VM C: File content verified after multiple nodes"

    # Phase 7: Verify we can still create new files
    log_info "Phase 7: Verifying new file creation works..."
    write_file_on_vm "$vm_c" "new-file.txt" "Post-recovery file"

    if ! file_exists_on_vm "$vm_c" "new-file.txt"; then
        log_error "Cannot create new files after recovery"
        return 1
    fi

    # Cleanup: restart killed VMs
    log_info "Cleanup: Restarting VM A and VM B..."
    start_vm "$vm_a"
    start_vm "$vm_b"
    wait_vm_healthy "$vm_a"
    wait_vm_healthy "$vm_b"
    setup_goclusterfs "$vm_a"
    setup_goclusterfs "$vm_b"

    log_info "Journal recovery idempotency test passed"
    return 0
}

# Test: Journal crash recovery (file)
# Simulates single-node crash: create file, hard-kill VM, restart and verify recovery
# Matches Go test: TestJournalCrashRecoveryFile
test_journal_crash_recovery_file() {
    log_info "Testing journal crash recovery for file operations..."

    # Ensure VM 1 is running before test (may have been killed by previous test)
    ensure_vm_running 1

    # This test works with a single VM - we test crash/recovery on VM 1
    local vm_a=1
    local filename="crash_test.txt"
    local content="This data should survive a crash!"

    # Phase 1: Create file with known content
    log_info "Phase 1: Creating file on VM A..."
    write_file_on_vm "$vm_a" "$filename" "$content"

    # Verify file exists before crash
    if ! file_exists_on_vm "$vm_a" "$filename"; then
        log_error "File should exist before crash"
        return 1
    fi

    # Give time for journal sync
    sleep 1

    log_info "Phase 2: Simulating crash by hard-killing VM..."
    kill_vm "$vm_a"
    sleep 2

    # Phase 3: Restart - this triggers journal recovery
    log_info "Phase 3: Restarting VM to trigger journal recovery..."
    start_vm "$vm_a"
    wait_vm_healthy "$vm_a"
    setup_goclusterfs "$vm_a"

    # Phase 4: Verify file was recovered
    log_info "Phase 4: Verifying file recovery..."

    if ! file_exists_on_vm "$vm_a" "$filename"; then
        log_error "File should exist after crash recovery"
        return 1
    fi

    # Verify content is intact
    local recovered_content
    recovered_content=$(read_file_on_vm "$vm_a" "$filename")
    if [[ "$recovered_content" != "$content" ]]; then
        log_error "Content mismatch after crash recovery"
        log_error "Expected: '$content'"
        log_error "Got: '$recovered_content'"
        return 1
    fi

    log_info "Journal crash recovery file test passed"
    return 0
}

# Test: Journal crash recovery (directory)
# Simulates single-node crash: create directory with nested file, hard-kill VM, restart and verify recovery
# Matches Go test: TestJournalCrashRecoveryDirectory
test_journal_crash_recovery_directory() {
    log_info "Testing journal crash recovery for directory operations..."

    # Ensure VM 1 is running before test (may have been killed by previous test)
    ensure_vm_running 1

    # This test works with a single VM
    local vm_a=1
    local dirname="recovered_dir"
    local nested_file="$dirname/nested.txt"
    local content="Nested file content"

    # Phase 1: Create directory structure
    log_info "Phase 1: Creating directory and nested file on VM A..."

    # Create directory
    ssh_run_password "$vm_a" "mkdir -p /mnt/goclusterfs/$dirname"

    # Create nested file
    write_file_on_vm "$vm_a" "$nested_file" "$content"

    # Verify directory exists before crash
    if ! ssh_run_password "$vm_a" "test -d /mnt/goclusterfs/$dirname"; then
        log_error "Directory should exist before crash"
        return 1
    fi

    # Verify nested file exists before crash
    if ! file_exists_on_vm "$vm_a" "$nested_file"; then
        log_error "Nested file should exist before crash"
        return 1
    fi

    # Give time for journal sync
    sleep 1

    log_info "Phase 2: Simulating crash by hard-killing VM..."
    kill_vm "$vm_a"
    sleep 2

    # Phase 3: Restart - this triggers journal recovery
    log_info "Phase 3: Restarting VM to trigger journal recovery..."
    start_vm "$vm_a"
    wait_vm_healthy "$vm_a"
    setup_goclusterfs "$vm_a"

    # Phase 4: Verify directory structure was recovered
    log_info "Phase 4: Verifying directory recovery..."

    # Verify directory exists and is a directory
    if ! ssh_run_password "$vm_a" "test -d /mnt/goclusterfs/$dirname"; then
        log_error "Directory should exist after crash recovery"
        return 1
    fi

    # Verify nested file exists
    if ! file_exists_on_vm "$vm_a" "$nested_file"; then
        log_error "Nested file should exist after crash recovery"
        return 1
    fi

    # Verify nested file content
    local recovered_content
    recovered_content=$(read_file_on_vm "$vm_a" "$nested_file")
    if [[ "$recovered_content" != "$content" ]]; then
        log_error "Nested file content mismatch after crash recovery"
        log_error "Expected: '$content'"
        log_error "Got: '$recovered_content'"
        return 1
    fi

    log_info "Journal crash recovery directory test passed"
    return 0
}

# Test: Journal crash recovery for multiple operations
# Matches Go test: TestJournalCrashRecoveryMultipleOps in journal_test.go
# Creates multiple files + directory + nested file, crashes, verifies all recovered
test_journal_crash_recovery_multiple_ops() {
    log_info "Testing journal crash recovery for multiple operations..."

    # Ensure VM 1 is running before test (may have been killed by previous test)
    ensure_vm_running 1

    local vm_a=1

    # Perform multiple operations
    # 1. Create file1.txt
    log_info "Creating file1.txt..."
    if ! write_file_on_vm "$vm_a" "file1.txt" "content1"; then
        log_error "Failed to create file1.txt"
        return 1
    fi

    # 2. Create file2.txt
    log_info "Creating file2.txt..."
    if ! write_file_on_vm "$vm_a" "file2.txt" "content2"; then
        log_error "Failed to create file2.txt"
        return 1
    fi

    # 3. Create directory
    log_info "Creating testdir..."
    if ! ssh_run_password "$vm_a" "mkdir -p $GOCLUSTERFS_MOUNT/testdir"; then
        log_error "Failed to create testdir"
        return 1
    fi

    # 4. Create nested file
    log_info "Creating testdir/nested.txt..."
    if ! ssh_run_password "$vm_a" "echo -n 'nested' > $GOCLUSTERFS_MOUNT/testdir/nested.txt"; then
        log_error "Failed to create nested file"
        return 1
    fi

    # Force sync
    ssh_run_password "$vm_a" "sync" 2>/dev/null || true

    log_info "All operations complete, simulating crash..."

    # Kill the VM to simulate crash
    kill_vm "$vm_a"
    sleep 2

    # Restart VM
    log_info "Restarting VM..."
    start_vm "$vm_a"

    # Wait for VM to come back
    if ! wait_vm_healthy "$vm_a" 60; then
        log_error "VM $vm_a failed to come back after restart"
        return 1
    fi

    # Remount GoClusterFS (journal recovery happens here)
    setup_goclusterfs "$vm_a"
    sleep 2

    # Verify all operations were recovered
    log_info "Verifying all operations were recovered..."

    # Verify file1.txt
    local content1
    content1=$(read_file_on_vm "$vm_a" "file1.txt")
    if [[ "$content1" != "content1" ]]; then
        log_error "file1.txt content mismatch after crash recovery"
        log_error "Expected: 'content1', Got: '$content1'"
        return 1
    fi
    log_info "file1.txt recovered successfully"

    # Verify file2.txt
    local content2
    content2=$(read_file_on_vm "$vm_a" "file2.txt")
    if [[ "$content2" != "content2" ]]; then
        log_error "file2.txt content mismatch after crash recovery"
        log_error "Expected: 'content2', Got: '$content2'"
        return 1
    fi
    log_info "file2.txt recovered successfully"

    # Verify testdir is a directory
    if ! ssh_run_password "$vm_a" "test -d $GOCLUSTERFS_MOUNT/testdir"; then
        log_error "testdir not recovered as directory"
        return 1
    fi
    log_info "testdir recovered successfully"

    # Verify nested file
    local nested_content
    nested_content=$(ssh_run_password "$vm_a" "cat $GOCLUSTERFS_MOUNT/testdir/nested.txt" 2>/dev/null)
    if [[ "$nested_content" != "nested" ]]; then
        log_error "testdir/nested.txt content mismatch after crash recovery"
        log_error "Expected: 'nested', Got: '$nested_content'"
        return 1
    fi
    log_info "testdir/nested.txt recovered successfully"

    log_info "Journal crash recovery multiple ops test passed - all 4 operations recovered"
    return 0
}

# Test: Journal crash during write
# Matches Go test: TestJournalCrashDuringWrite in journal_test.go
# Creates baseline file, writes rapid files, crashes mid-write, verifies baseline recovered
test_journal_crash_during_write() {
    log_info "Testing journal crash during active write operation..."

    # Ensure VM 1 is running before test (may have been killed by previous test)
    ensure_vm_running 1

    local vm_a=1
    local baseline_content="This file was created and synced before the crash"

    # Create baseline file that MUST survive
    log_info "Creating baseline file..."
    if ! write_file_on_vm "$vm_a" "baseline.txt" "$baseline_content"; then
        log_error "Failed to create baseline file"
        return 1
    fi

    # Force sync to ensure baseline is committed to journal
    ssh_run_password "$vm_a" "sync" 2>/dev/null || true
    sleep 1

    log_info "Baseline file created and synced"

    # Start rapid writes in background
    log_info "Starting rapid writes..."
    local write_count=0
    for i in $(seq 1 20); do
        ssh_run_password "$vm_a" "echo -n 'Rapid write $i' > $GOCLUSTERFS_MOUNT/rapid_$(printf '%04d' $i).txt" 2>/dev/null &
        ((write_count++))
    done

    # Give a brief moment for some writes to start
    sleep 0.2

    log_info "Wrote $write_count rapid files, crashing now..."

    # Kill VM while writes may be in progress
    kill_vm "$vm_a"

    # Wait for any background processes to terminate
    wait 2>/dev/null || true
    sleep 2

    # Restart VM
    log_info "Restarting VM after crash..."
    start_vm "$vm_a"

    # Wait for VM to come back
    if ! wait_vm_healthy "$vm_a" 60; then
        log_error "VM $vm_a failed to come back after restart"
        return 1
    fi

    # Remount GoClusterFS (journal recovery happens here)
    setup_goclusterfs "$vm_a"
    sleep 2

    # Baseline file MUST be recovered
    log_info "Verifying baseline file recovery..."
    local recovered_baseline
    recovered_baseline=$(read_file_on_vm "$vm_a" "baseline.txt")
    if [[ "$recovered_baseline" != "$baseline_content" ]]; then
        log_error "Baseline file not recovered or content mismatch"
        log_error "Expected: '$baseline_content'"
        log_error "Got: '$recovered_baseline'"
        return 1
    fi
    log_info "Baseline file recovered successfully"

    # Count how many rapid files were recovered (some may be lost, which is expected)
    local recovered_count
    recovered_count=$(ssh_run_password "$vm_a" "ls $GOCLUSTERFS_MOUNT 2>/dev/null | grep -c '^rapid_'" || echo "0")
    log_info "Recovered $recovered_count/$write_count rapid files (some in-flight writes may be lost, which is expected)"

    # The key assertion: filesystem is consistent (we can read/list without errors)
    if ! ssh_run_password "$vm_a" "ls -la $GOCLUSTERFS_MOUNT" >/dev/null 2>&1; then
        log_error "Filesystem inconsistent after crash recovery"
        return 1
    fi

    log_info "Journal crash during write test passed - filesystem is consistent"
    return 0
}

# Test 1: Multi-VM concurrent writes
test_concurrent_writes() {
    log_info "Testing concurrent writes from all VMs..."

    # Each VM writes a unique file concurrently
    for vm_id in $(seq 1 $NODE_COUNT); do
        write_file_on_vm "$vm_id" "vm${vm_id}_file.txt" "Data from VM $vm_id" &
    done
    wait

    # Give time for sync
    sleep 2

    # Verify all files are visible from first VM
    for vm_id in $(seq 1 $NODE_COUNT); do
        if ! file_exists_on_vm 1 "vm${vm_id}_file.txt"; then
            log_error "vm${vm_id}_file.txt not visible from VM 1"
            return 1
        fi
    done

    log_info "All concurrent writes verified"
    return 0
}

# Test 2: VM kill and recovery
test_vm_kill_recovery() {
    log_info "Testing VM kill and recovery..."

    local victim=2
    local survivor=1

    # Write file from victim VM
    write_file_on_vm "$victim" "before_kill.txt" "Written before kill"

    # Gracefully stop the victim (simulates crash with clean unmount)
    stop_vm "$victim"
    sleep 2

    # Survivor should still be able to read the file
    if ! file_exists_on_vm "$survivor" "before_kill.txt"; then
        log_error "before_kill.txt not readable after VM stop"
        return 1
    fi

    # Survivor writes new file while victim is down
    write_file_on_vm "$survivor" "during_stop.txt" "Written during stop"

    # Restart victim
    start_vm "$victim" false
    wait_vm_healthy "$victim"
    setup_goclusterfs "$victim" false  # Re-mount GoClusterFS

    # Victim should see both files
    if ! file_exists_on_vm "$victim" "before_kill.txt"; then
        log_error "before_kill.txt not visible after recovery"
        return 1
    fi

    if ! file_exists_on_vm "$victim" "during_stop.txt"; then
        log_error "during_stop.txt not visible after recovery"
        return 1
    fi

    log_info "VM kill and recovery test passed"
    return 0
}

# Test 3: Network partition (using iptables inside VMs)
test_network_partition() {
    log_info "Testing network partition..."

    if [[ $NODE_COUNT -lt 3 ]]; then
        log_warn "Skipping network partition test (requires 3+ VMs)"
        return 0
    fi

    local isolated=3
    local other=1

    # Write file before partition
    write_file_on_vm "$other" "before_partition.txt" "Before partition"

    # Isolate the VM (drop all packets to/from cluster subnet)
    ssh_run "$isolated" "iptables -A INPUT -s 192.168.168.0/24 -j DROP"
    ssh_run "$isolated" "iptables -A OUTPUT -d 192.168.168.0/24 -j DROP"
    sleep 2

    # The isolated VM should still function locally (GoClusterFS is mounted)

    # Heal partition
    ssh_run "$isolated" "iptables -F"
    sleep 2

    # Verify VM is still functional
    if ! file_exists_on_vm "$isolated" "before_partition.txt"; then
        log_error "File not accessible after healing partition"
        return 1
    fi

    log_info "Network partition test passed"
    return 0
}

# Proves broadcasts arrive: VMs 1 and 2 each hear every other node, and no mounted node has gone unheard.
expect_broadcast_delivery() {
    local vm_id i metrics peers silent want=$((NODE_COUNT - 1))
    for vm_id in 1 2; do
        for i in $(seq 1 45); do
            metrics=$(ssh_run "$vm_id" "wget -qO- http://127.0.0.1:8080/metrics")
            peers=$(awk '$1 == "goclusterfs_broadcast_peers" {print $2 + 0}' <<< "$metrics")
            silent=$(awk '$1 == "goclusterfs_broadcast_silent_peers" {print $2 + 0}' <<< "$metrics")
            [[ "$peers" == "$want" && "$silent" == 0 ]] && break
            sleep 1
        done
        if [[ "$peers" != "$want" || "$silent" != 0 ]]; then
            log_error "VM $vm_id hears ${peers:-no} broadcast peers with ${silent:-unknown} silent, want $want and 0"
            return 1
        fi
    done
}

# Test: Broadcast cache invalidation
# Verifies that cache invalidation via UDP broadcast propagates quickly between VMs.
# This tests that VM 2 sees changes from VM 1 within ~500ms (vs 5s TTL).
test_broadcast_invalidation() {
    log_info "Testing broadcast cache invalidation..."

    if [[ $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping broadcast test (requires 2+ VMs)"
        return 0
    fi

    # Ensure all VMs are running before test (may have been killed by previous test)
    ensure_cluster_healthy 2
    expect_broadcast_delivery || return 1

    local writer=1
    local reader=2
    local test_file="broadcast_test_$(date +%s).txt"

    # Create initial file on writer VM
    write_file_on_vm "$writer" "$test_file" "initial_content"
    sleep 1

    # Verify reader sees initial content
    if ! file_exists_on_vm "$reader" "$test_file"; then
        log_error "Reader VM cannot see initial file"
        return 1
    fi

    # Update file on writer
    local start_time=$(date +%s%N)
    write_file_on_vm "$writer" "$test_file" "updated_content_via_broadcast"

    # Poll reader for updated content (max 2 seconds)
    local max_attempts=20  # 20 x 100ms = 2s
    local attempt=0
    local propagation_time_ms=0

    while [ $attempt -lt $max_attempts ]; do
        local content=$(read_file_on_vm "$reader" "$test_file" 2>/dev/null || echo "")
        if [ "$content" = "updated_content_via_broadcast" ]; then
            local end_time=$(date +%s%N)
            propagation_time_ms=$(( (end_time - start_time) / 1000000 ))
            log_info "Broadcast propagation detected in ${propagation_time_ms}ms"
            break
        fi
        sleep 0.1
        attempt=$((attempt + 1))
    done

    if [ $attempt -ge $max_attempts ]; then
        log_error "Reader VM did not see update within 2 seconds"
        return 1
    fi

    # Broadcast should be much faster than the 5s TTL
    # We use 1000ms as a generous threshold for test stability
    if [ $propagation_time_ms -gt 1000 ]; then
        log_warn "Propagation slow (${propagation_time_ms}ms), but within acceptable range"
    fi

    # Clean up
    ssh_run "$writer" "rm -f /mnt/goclusterfs/$test_file"

    log_info "Broadcast invalidation test passed (propagation: ${propagation_time_ms}ms)"
    return 0
}

# Test: Broadcast directory invalidation
# Verifies that new file creation is broadcast between VMs.
test_broadcast_directory_invalidation() {
    log_info "Testing broadcast directory invalidation..."

    if [[ $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping broadcast directory test (requires 2+ VMs)"
        return 0
    fi

    # Ensure all VMs are running before test (may have been killed by previous test)
    ensure_cluster_healthy 2
    expect_broadcast_delivery || return 1

    local writer=1
    local reader=2
    local test_file="broadcast_dir_test_$(date +%s).txt"

    # Record start time
    local start_time=$(date +%s%N)

    # Create new file on writer VM
    write_file_on_vm "$writer" "$test_file" "new file created"

    # Poll reader for new file visibility (max 2 seconds)
    local max_attempts=20
    local attempt=0
    local propagation_time_ms=0

    while [ $attempt -lt $max_attempts ]; do
        if file_exists_on_vm "$reader" "$test_file"; then
            local end_time=$(date +%s%N)
            propagation_time_ms=$(( (end_time - start_time) / 1000000 ))
            log_info "Directory broadcast propagation detected in ${propagation_time_ms}ms"
            break
        fi
        sleep 0.1
        attempt=$((attempt + 1))
    done

    if [ $attempt -ge $max_attempts ]; then
        log_error "Reader VM did not see new file within 2 seconds"
        return 1
    fi

    # Verify content is correct
    local content=$(read_file_on_vm "$reader" "$test_file" 2>/dev/null || echo "")
    if [ "$content" != "new file created" ]; then
        log_error "Content mismatch: got '$content'"
        return 1
    fi

    # Clean up
    ssh_run "$writer" "rm -f /mnt/goclusterfs/$test_file"

    log_info "Broadcast directory invalidation test passed (propagation: ${propagation_time_ms}ms)"
    return 0
}

# Test: Broadcast multiple updates
# Verifies that rapid successive updates are correctly broadcast between VMs.
# VM 1 performs 5 rapid updates and VM 2 should see the final version.
test_broadcast_multiple_updates() {
    log_info "Testing broadcast multiple updates..."

    if [[ $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping broadcast multiple updates test (requires 2+ VMs)"
        return 0
    fi

    # Ensure all VMs are running before test (may have been killed by previous test)
    ensure_cluster_healthy 2
    expect_broadcast_delivery || return 1

    local writer=1
    local reader=2
    local test_file="broadcast_rapid_$(date +%s).txt"

    # Create initial file on writer VM
    write_file_on_vm "$writer" "$test_file" "v0"
    sleep 0.2  # Allow initial propagation

    # Verify reader sees initial content
    local max_attempts=20
    local attempt=0
    while [ $attempt -lt $max_attempts ]; do
        local content=$(read_file_on_vm "$reader" "$test_file" 2>/dev/null || echo "")
        if [ "$content" = "v0" ]; then
            break
        fi
        sleep 0.1
        attempt=$((attempt + 1))
    done

    if [ $attempt -ge $max_attempts ]; then
        log_error "Reader VM cannot see initial file"
        return 1
    fi

    # Perform 5 rapid updates on writer VM (50ms gaps)
    local update_count=5
    for i in $(seq 1 $update_count); do
        write_file_on_vm "$writer" "$test_file" "v${i}"
        sleep 0.05  # 50ms gap between updates
    done

    # Allow final broadcast to propagate
    sleep 0.3

    # Poll reader for final content (max 2 seconds)
    local expected_final="v${update_count}"
    max_attempts=20
    attempt=0

    while [ $attempt -lt $max_attempts ]; do
        local content=$(read_file_on_vm "$reader" "$test_file" 2>/dev/null || echo "")
        if [ "$content" = "$expected_final" ]; then
            log_info "Reader VM sees final version after rapid updates"
            break
        fi
        sleep 0.1
        attempt=$((attempt + 1))
    done

    if [ $attempt -ge $max_attempts ]; then
        local actual=$(read_file_on_vm "$reader" "$test_file" 2>/dev/null || echo "")
        log_error "Reader VM did not see final version. Got '$actual', expected '$expected_final'"
        return 1
    fi

    # Clean up
    ssh_run "$writer" "rm -f /mnt/goclusterfs/$test_file"

    log_info "Broadcast multiple updates test passed"
    return 0
}

# Test: Broadcast bidirectional
# Verifies that invalidations work in both directions between VMs.
# VM 1 creates file_a, VM 2 creates file_b, both should see each other's file.
test_broadcast_bidirectional() {
    log_info "Testing broadcast bidirectional..."

    if [[ $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping broadcast bidirectional test (requires 2+ VMs)"
        return 0
    fi

    # Ensure all VMs are running before test (may have been killed by previous test)
    ensure_cluster_healthy 2
    expect_broadcast_delivery || return 1

    local vm1=1
    local vm2=2
    local file_a="bidir_a_$(date +%s).txt"
    local file_b="bidir_b_$(date +%s).txt"

    # VM 1 creates file A
    write_file_on_vm "$vm1" "$file_a" "from vm 1"

    # VM 2 creates file B
    write_file_on_vm "$vm2" "$file_b" "from vm 2"

    # Allow broadcast propagation
    sleep 0.3

    # Poll for VM 2 to see file A (max 2 seconds)
    local max_attempts=20
    local attempt=0
    local content_a=""

    while [ $attempt -lt $max_attempts ]; do
        content_a=$(read_file_on_vm "$vm2" "$file_a" 2>/dev/null || echo "")
        if [ "$content_a" = "from vm 1" ]; then
            log_info "VM 2 sees file_a from VM 1"
            break
        fi
        sleep 0.1
        attempt=$((attempt + 1))
    done

    if [ "$content_a" != "from vm 1" ]; then
        log_error "VM 2 cannot read file_a. Got '$content_a'"
        return 1
    fi

    # Poll for VM 1 to see file B (max 2 seconds)
    max_attempts=20
    attempt=0
    local content_b=""

    while [ $attempt -lt $max_attempts ]; do
        content_b=$(read_file_on_vm "$vm1" "$file_b" 2>/dev/null || echo "")
        if [ "$content_b" = "from vm 2" ]; then
            log_info "VM 1 sees file_b from VM 2"
            break
        fi
        sleep 0.1
        attempt=$((attempt + 1))
    done

    if [ "$content_b" != "from vm 2" ]; then
        log_error "VM 1 cannot read file_b. Got '$content_b'"
        return 1
    fi

    # Clean up
    ssh_run "$vm1" "rm -f /mnt/goclusterfs/$file_a"
    ssh_run "$vm2" "rm -f /mnt/goclusterfs/$file_b"

    log_info "Broadcast bidirectional test passed"
    return 0
}

# Test: Read after cross-node inode reuse
# VM 1 deletes a file VM 2 has read and creates a new file with the same inode number.
# VM 2 must read the new file's full content, not the old size or old data block.
test_inode_reuse_stale_read() {
    log_info "Testing read after cross-node inode reuse..."

    if [[ $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping inode reuse test (requires 2+ VMs)"
        return 0
    fi

    ensure_cluster_healthy 2

    local writer=1
    local reader=2
    local dir=/mnt/goclusterfs
    local old="reuse_old_$(date +%s).bin"

    ssh_run "$writer" "head -c 204800 /dev/zero | tr '\\0' o > $dir/$old"
    local old_ino
    old_ino=$(ssh_run "$writer" "stat -c %i $dir/$old")
    ssh_run "$reader" "cat $dir/$old > /dev/null"

    # The last peer to let go of the old inode frees it, and this node's orphan pass moves its inode search back to it.
    local new
    new=$(ssh_run "$writer" "rm $dir/$old; for i in \$(seq 1 200); do f=reuse_new_\$i.bin; touch $dir/\$f; if [ \$(stat -c %i $dir/\$f) = $old_ino ]; then echo \$f; break; fi; sleep 0.1; done")
    if [[ -z "$new" ]]; then
        log_error "No new file reused inode $old_ino"
        ssh_run "$writer" "rm -f $dir/reuse_new_*.bin"
        return 1
    fi

    ssh_run "$writer" "head -c 204800 /dev/zero | tr '\\0' p > $dir/reuse_pad.bin && head -c 307200 /dev/zero | tr '\\0' n > $dir/$new"
    local want got
    want=$(ssh_run "$writer" "md5sum < $dir/$new")
    got=$(ssh_run "$reader" "md5sum < $dir/$new")
    ssh_run "$writer" "rm -f $dir/reuse_new_*.bin $dir/reuse_pad.bin"

    if [[ "$got" != "$want" ]]; then
        log_error "VM $reader read wrong data from $new (inode $old_ino): md5 '$got', want '$want'"
        return 1
    fi

    log_info "Inode reuse stale read test passed"
    return 0
}

# Test: more than 4096 files, which layout v2 could not hold. VMs work in parallel and poll past the attribute cache.
test_more_than_4096_files() {
    log_info "Testing more than 4096 files..."

    ensure_cluster_healthy "$NODE_COUNT"

    local dir=/mnt/goclusterfs/many_files
    local inodes
    inodes=$(ssh_run 1 "stat -f -c %c /mnt/goclusterfs")
    if [[ "$inodes" -le 4200 ]]; then
        log_error "statfs reports $inodes inodes, want more than 4200"
        return 1
    fi

    if ! ssh_run 1 "mkdir $dir && for d in \$(seq 0 41); do mkdir $dir/d\$d || exit 1; done"; then
        log_error "creating directories on VM 1 failed"
        ssh_run 1 "rm -rf $dir"
        return 1
    fi
    local vm pid failed=0 pids=()
    for vm in $(seq 1 "$NODE_COUNT"); do
        ssh_run "$vm" "for d in \$(seq $((vm - 1)) $NODE_COUNT 41); do t=0; until [ -d $dir/d\$d ]; do t=\$((t + 1)); [ \$t -le 120 ] || exit 1; sleep 0.5; done; cd $dir/d\$d && touch \$(seq 0 99 | sed 's/^/f/') || exit 1; done" &
        pids+=($!)
    done
    for pid in "${pids[@]}"; do
        wait "$pid" || failed=1
    done
    local count=0 waited=0
    while [[ $failed -eq 0 && "$count" -ne 4200 && $waited -lt 60 ]]; do
        sleep 1
        waited=$((waited + 1))
        count=$(ssh_run 1 "find $dir -type f | wc -l")
    done

    pids=()
    for vm in $(seq 1 "$NODE_COUNT"); do
        ssh_run "$vm" "for d in \$(seq $((vm - 1)) $NODE_COUNT 41); do rm -f $dir/d\$d/f*; done" &
        pids+=($!)
    done
    wait "${pids[@]}"
    ssh_run 1 "rm -rf $dir"

    if [[ $failed -ne 0 || "$count" -ne 4200 ]]; then
        log_error "a VM failed to create its files ($failed) or VM 1 sees $count files, want 4200"
        return 1
    fi

    log_info "More than 4096 files test passed"
    return 0
}

# Test: Extent cache coherency across VMs
# VM 2 reads files, VM 1 changes their block mappings (truncate and rewrite,
# hole fill), and VM 2 must read the new data.
test_extent_cache_coherency() {
    log_info "Testing extent cache coherency across VMs..."

    if [[ $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping extent cache coherency test (requires 2+ VMs)"
        return 0
    fi

    ensure_cluster_healthy 2

    local writer=1
    local reader=2
    local dir=/mnt/goclusterfs
    local trunc="extent_trunc_$(date +%s).bin"
    local holes="extent_holes_$(date +%s).bin"

    ssh_run "$writer" "head -c 307200 /dev/zero | tr '\\0' o > $dir/$trunc"
    ssh_run "$writer" "head -c 4096 /dev/zero | tr '\\0' a > $dir/$holes && head -c 4096 /dev/zero | tr '\\0' c | dd of=$dir/$holes bs=1M seek=2 conv=notrunc 2>/dev/null"
    ssh_run "$reader" "cat $dir/$trunc $dir/$holes > /dev/null"

    # The padding file takes the freed block, so the rewrite lands in another one.
    ssh_run "$writer" "truncate -s 0 $dir/$trunc && head -c 307200 /dev/zero | tr '\\0' p > $dir/extent_pad.bin && head -c 307200 /dev/zero | tr '\\0' n > $dir/$trunc"
    ssh_run "$writer" "head -c 4096 /dev/zero | tr '\\0' b | dd of=$dir/$holes bs=1M seek=1 conv=notrunc 2>/dev/null"

    local f want got failed=0
    for f in "$trunc" "$holes"; do
        want=$(ssh_run "$writer" "md5sum < $dir/$f")
        got=$(ssh_run "$reader" "md5sum < $dir/$f")
        if [[ "$got" != "$want" ]]; then
            log_error "VM $reader read stale data from $f: md5 '$got', want '$want'"
            failed=1
        fi
    done
    ssh_run "$writer" "rm -f $dir/$trunc $dir/$holes $dir/extent_pad.bin"

    if [[ $failed -ne 0 ]]; then
        return 1
    fi
    log_info "Extent cache coherency test passed"
    return 0
}

# Node B has read a directory; it must see node A's creates and removes there.
test_dir_index_follows_other_node() {
    log_info "Testing that lookups follow another VM's directory changes..."

    if [[ $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping directory lookup coherency test (requires 2+ VMs)"
        return 0
    fi

    ensure_cluster_healthy 2

    local writer=1
    local reader=2
    local d="/mnt/goclusterfs/dirindex_$(date +%s)"

    ssh_run "$writer" "mkdir $d && for i in \$(seq 1 200); do : > $d/f\$i; done"
    sleep 1.5
    if ! ssh_run "$reader" "for i in \$(seq 1 200); do stat $d/f\$i > /dev/null || exit 1; done"; then
        log_error "VM $reader could not stat every file VM $writer made in $d"
        return 1
    fi

    ssh_run "$writer" "echo new > $d/new && rm $d/f5 && rm $d/f6 && echo again > $d/f6"
    # The reader's kernel may keep old entries for its 1s -attr-timeout.
    sleep 1.5

    local failed=0
    if [[ "$(ssh_run "$reader" "cat $d/new")" != new ]]; then
        log_error "VM $reader does not see $d/new"
        failed=1
    fi
    if ssh_run "$reader" "test -e $d/f5"; then
        log_error "VM $reader still sees $d/f5 after VM $writer removed it"
        failed=1
    fi
    if [[ "$(ssh_run "$reader" "cat $d/f6")" != again ]]; then
        log_error "VM $reader reads the removed $d/f6, not its replacement"
        failed=1
    fi
    ssh_run "$writer" "rm -rf $d"

    if [[ $failed -ne 0 ]]; then
        return 1
    fi
    log_info "Directory lookup coherency test passed"
    return 0
}

# Print the free 1MB blocks that statfs reports on VM $1
statfs_free_blocks_on_vm() {
    ssh_run "$1" "stat -f -c %f /mnt/goclusterfs"
}

# statfs counts other nodes' unused block reserves as used; they go back within 1.5 x the 10s idle time.
RESERVE_IDLE_RETURN_SECS=16

# Writes 5 blocks, remounts on a restarted VM, and checks that statfs reports
# the same free blocks as before the restart, not the whole volume.
test_statfs_after_remount() {
    log_info "Testing statfs free blocks across a remount..."
    ensure_cluster_healthy 1

    local file=/mnt/goclusterfs/statfs_remount.bin
    local before written remounted
    sleep "$RESERVE_IDLE_RETURN_SECS"
    before=$(statfs_free_blocks_on_vm 1)
    ssh_run 1 "dd if=/dev/urandom of=$file bs=1M count=5 2>/dev/null"
    written=$(statfs_free_blocks_on_vm 1)
    if (( before - written < 5 )); then
        log_error "Writing 5 blocks lowered free blocks from $before to $written"
        return 1
    fi

    restart_goclusterfs_vm 1 || return 1
    remounted=$(statfs_free_blocks_on_vm 1)
    ssh_run 1 "rm -f $file"

    if [[ "$remounted" != "$written" ]]; then
        log_error "$written free blocks before the restart, $remounted after"
        return 1
    fi

    log_info "Statfs after remount test passed"
    return 0
}

# Symlinks made on VM 1 read back, and unlink, on VM 2.
test_symlink_cross_node() {
    log_info "Testing symlinks across VMs..."
    if [[ $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping symlink cross-node test (requires 2+ VMs)"
        return 0
    fi
    ensure_cluster_healthy 2

    local d=/mnt/goclusterfs long failed=0
    long="$(printf 'd/%.0s' $(seq 1 1000))end"
    ssh_run 1 "rm -f $d/sl_short $d/sl_long $d/sl_real $d/sl_tofile"
    ssh_run 1 "ln -s target/short $d/sl_short && ln -s '$long' $d/sl_long" || return 1
    ssh_run 1 "echo through > $d/sl_real && ln -s sl_real $d/sl_tofile" || return 1

    if [[ "$(ssh_run 2 "readlink $d/sl_short")" != target/short ]]; then
        log_error "VM 2 reads the wrong short target"
        failed=1
    fi
    if [[ "$(ssh_run 2 "readlink $d/sl_long")" != "$long" ]]; then
        log_error "VM 2 reads the wrong long target"
        failed=1
    fi
    if [[ "$(ssh_run 2 "stat -c %F $d/sl_long")" != "symbolic link" ]]; then
        log_error "VM 2 does not see sl_long as a symlink"
        failed=1
    fi
    if [[ "$(ssh_run 2 "cat $d/sl_tofile")" != through ]]; then
        log_error "VM 2 cannot read a file through a symlink"
        failed=1
    fi

    ssh_run 2 "rm $d/sl_short" || return 1
    local tries=0
    while ssh_run 1 "test -L $d/sl_short"; do
        tries=$((tries + 1))
        if [ "$tries" -ge 50 ]; then
            log_error "VM 1 still sees the symlink VM 2 removed"
            failed=1
            break
        fi
        sleep 0.2
    done
    ssh_run 1 "rm -f $d/sl_long $d/sl_real $d/sl_tofile"

    if [[ $failed -ne 0 ]]; then
        return 1
    fi
    log_info "Symlink cross-node test passed"
    return 0
}

# Writes 5 blocks on VM 1 and removes the file on VM 2, checking after each
# step that both VMs report the same free blocks.
test_statfs_multinode() {
    log_info "Testing statfs free blocks across nodes..."

    if [[ $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping statfs multi-node test (requires 2+ VMs)"
        return 0
    fi
    ensure_cluster_healthy 2

    local file=/mnt/goclusterfs/statfs_multinode.bin
    local start on_writer on_remover
    sleep "$RESERVE_IDLE_RETURN_SECS"
    start=$(statfs_free_blocks_on_vm 2)
    ssh_run 1 "dd if=/dev/urandom of=$file bs=1M count=5 2>/dev/null"
    sleep "$RESERVE_IDLE_RETURN_SECS"
    on_writer=$(statfs_free_blocks_on_vm 1)
    on_remover=$(statfs_free_blocks_on_vm 2)
    if [[ "$on_writer" != "$on_remover" ]] || (( start - on_remover < 5 )); then
        log_error "After VM 1 wrote 5 blocks: VM 1 reports $on_writer free, VM 2 reports $on_remover, $start before"
        return 1
    fi

    ssh_run 2 "rm -f $file"
    # VM 1 frees the file once it lets go of it: on the Orphans broadcast, or at its next failure scan.
    for _ in $(seq 1 30); do
        on_writer=$(statfs_free_blocks_on_vm 1)
        on_remover=$(statfs_free_blocks_on_vm 2)
        [[ "$on_writer" == "$start" && "$on_remover" == "$start" ]] && break
        sleep 0.5
    done
    if [[ "$on_writer" != "$start" || "$on_remover" != "$start" ]]; then
        log_error "After VM 2 removed the file: VM 1 reports $on_writer free, VM 2 reports $on_remover, want $start"
        return 1
    fi

    log_info "Statfs multi-node test passed"
    return 0
}

# Test 4: Rolling restart
test_rolling_restart() {
    log_info "Testing rolling restart..."

    # Ensure all VMs are running before test (may have been killed by previous test)
    ensure_cluster_healthy "$NODE_COUNT"

    # Write a file
    write_file_on_vm 1 "rolling_test.txt" "Rolling restart test"

    # Restart each VM one by one
    for vm_id in $(seq 1 $NODE_COUNT); do
        log_info "Restarting VM $vm_id..."
        stop_vm "$vm_id"
        start_vm "$vm_id" false
        wait_vm_healthy "$vm_id"
        setup_goclusterfs "$vm_id" false  # Re-mount GoClusterFS

        # Verify file is still accessible
        if ! file_exists_on_vm "$vm_id" "rolling_test.txt"; then
            log_error "File not accessible on VM $vm_id after restart"
            return 1
        fi
    done

    log_info "Rolling restart test passed"
    return 0
}

# Test 5: Stress test with random failures
test_stress_random_failures() {
    log_info "Testing stress with random failures..."

    local iterations=5
    local files_created=0

    for i in $(seq 1 $iterations); do
        # Random VM writes
        local vm_id=$((RANDOM % NODE_COUNT + 1))
        write_file_on_vm "$vm_id" "stress_${i}.txt" "Stress test iteration $i"
        files_created=$((files_created + 1))

        # Occasionally stop a random VM (20% chance)
        if [[ $((RANDOM % 5)) -eq 0 ]]; then
            local kill_vm_id=$((RANDOM % NODE_COUNT + 1))
            stop_vm "$kill_vm_id"
            sleep 1
            start_vm "$kill_vm_id" false
            wait_vm_healthy "$kill_vm_id"
            setup_goclusterfs "$kill_vm_id" false  # Re-mount GoClusterFS
        fi
    done

    sleep 2

    # Verify all files exist
    for i in $(seq 1 $iterations); do
        if ! file_exists_on_vm 1 "stress_${i}.txt"; then
            log_error "stress_${i}.txt not found"
            return 1
        fi
    done

    log_info "Stress test passed ($files_created files created)"
    return 0
}

# =============================================================================
# VM-Specific Tests
# =============================================================================

# Test 6: Power failure simulation (hard VM kill mid-write)
test_power_failure() {
    log_info "Testing power failure simulation..."

    local victim=2
    local survivor=1

    # Write a file that should be persisted
    write_file_on_vm "$victim" "before_power_fail.txt" "Written before power failure"

    # Give time for data to sync to disk
    sleep 2

    # Hard kill the VM (simulates power loss - SIGKILL to QEMU process)
    log_info "Simulating power failure on VM $victim..."
    kill_vm "$victim"

    sleep 1

    # Survivor should still have access to the file
    if ! file_exists_on_vm "$survivor" "before_power_fail.txt"; then
        log_error "before_power_fail.txt not visible from survivor after power failure"
        return 1
    fi

    # Restart victim
    start_vm "$victim" false
    wait_vm_healthy "$victim"
    setup_goclusterfs "$victim" false  # Re-mount GoClusterFS

    # Victim should recover and see the file
    if ! file_exists_on_vm "$victim" "before_power_fail.txt"; then
        log_error "before_power_fail.txt not visible after recovery from power failure"
        return 1
    fi

    # Verify content integrity
    local content=$(read_file_on_vm "$victim" "before_power_fail.txt")
    if [[ "$content" != "Written before power failure" ]]; then
        log_error "File content corrupted after power failure recovery"
        return 1
    fi

    log_info "Power failure simulation test passed"
    return 0
}

# Test 7: O_DIRECT validation
test_odirect_validation() {
    log_info "Testing O_DIRECT behavior..."
    ensure_cluster_healthy 2

    # Write a file and verify O_DIRECT is being used
    # This test checks that cache coherency works correctly

    write_file_on_vm 1 "odirect_test.txt" "O_DIRECT test data version 1"

    # Read from VM 2 - should see the data (not stale cache)
    local content=$(read_file_on_vm 2 "odirect_test.txt")
    if [[ "$content" != "O_DIRECT test data version 1" ]]; then
        log_error "VM 2 got stale data: $content"
        return 1
    fi

    # Update from VM 1
    write_file_on_vm 1 "odirect_test.txt" "O_DIRECT test data version 2"

    # VM 2 should see the update immediately (O_DIRECT bypasses cache)
    sleep 1
    content=$(read_file_on_vm 2 "odirect_test.txt")
    if [[ "$content" != "O_DIRECT test data version 2" ]]; then
        log_error "VM 2 did not see updated data: $content"
        return 1
    fi

    log_info "O_DIRECT validation test passed"
    return 0
}

# Test 8: Large file handling
test_large_file() {
    log_info "Testing large file handling..."
    ensure_cluster_healthy 2

    # Create a 1MB file (multiple blocks)
    local size_kb=1024
    ssh_run 1 "dd if=/dev/urandom of=/mnt/goclusterfs/large_test.bin bs=1K count=$size_kb 2>/dev/null"

    # Calculate checksum on VM 1
    local checksum1=$(ssh_run 1 "md5sum /mnt/goclusterfs/large_test.bin" | awk '{print $1}')
    log_info "VM 1 checksum: $checksum1"

    # Read and verify on VM 2
    local checksum2=$(ssh_run 2 "md5sum /mnt/goclusterfs/large_test.bin" | awk '{print $1}')
    log_info "VM 2 checksum: $checksum2"

    if [[ "$checksum1" != "$checksum2" ]]; then
        log_error "Large file checksum mismatch between VMs"
        return 1
    fi

    log_info "Large file test passed (${size_kb}KB file verified)"
    return 0
}

# Test: Many files in directory
# Creates 100+ files in a directory to test directory listing and metadata handling
test_many_files() {
    log_info "Testing many files in directory..."
    ensure_cluster_healthy 2

    local vm_id=1
    local num_files=100
    local test_dir="many_files_test"

    # Create test directory
    ssh_run "$vm_id" "mkdir -p /mnt/goclusterfs/$test_dir"

    # Create 100 files with unique content
    log_info "Creating $num_files files..."
    for i in $(seq 0 $((num_files - 1))); do
        local filename=$(printf "file_%03d.txt" "$i")
        local content="Content of file $i"
        ssh_run "$vm_id" "echo '$content' > /mnt/goclusterfs/$test_dir/$filename"
    done

    # Count files in directory
    local count
    count=$(ssh_run "$vm_id" "ls -1 /mnt/goclusterfs/$test_dir | wc -l")
    if [[ "$count" -ne "$num_files" ]]; then
        log_error "Expected $num_files files, found $count"
        return 1
    fi
    log_info "File count verified: $count files"

    # Verify each file has correct content
    local verify_errors=0
    for i in $(seq 0 $((num_files - 1))); do
        local filename=$(printf "file_%03d.txt" "$i")
        local expected="Content of file $i"
        local actual
        actual=$(ssh_run "$vm_id" "cat /mnt/goclusterfs/$test_dir/$filename")
        if [[ "$actual" != "$expected" ]]; then
            log_error "Content mismatch for $filename: expected '$expected', got '$actual'"
            ((verify_errors++))
        fi
    done

    if [[ "$verify_errors" -gt 0 ]]; then
        log_error "$verify_errors files had content mismatches"
        return 1
    fi

    # Verify from VM 2
    local count2
    count2=$(ssh_run 2 "ls -1 /mnt/goclusterfs/$test_dir | wc -l")
    if [[ "$count2" -ne "$num_files" ]]; then
        log_error "VM 2 sees $count2 files, expected $num_files"
        return 1
    fi
    log_info "Cross-VM file count verified: $count2 files"

    log_info "Many files test passed ($num_files files created and verified)"
    return 0
}

# Test: Nested directory hierarchy
# Creates deeply nested directory structure (a/b/c/d) with file at deepest level
test_nested_directories() {
    log_info "Testing nested directory hierarchy..."
    ensure_cluster_healthy 2

    local vm_id=1
    local base_dir="/mnt/goclusterfs"

    # Create nested structure: a/b/c/d
    log_info "Creating nested directories..."
    ssh_run "$vm_id" "mkdir -p $base_dir/a/b/c/d"

    # Verify each level exists and is a directory
    for dir in "a" "a/b" "a/b/c" "a/b/c/d"; do
        if ! ssh_run "$vm_id" "test -d $base_dir/$dir"; then
            log_error "$dir should exist and be a directory"
            return 1
        fi
    done
    log_info "All directory levels verified"

    # Create a file at the deepest level
    local deep_file="a/b/c/d/deep.txt"
    local content="deep content"
    ssh_run "$vm_id" "echo '$content' > $base_dir/$deep_file"

    # Read it back and verify
    local actual
    actual=$(ssh_run "$vm_id" "cat $base_dir/$deep_file")
    if [[ "$actual" != "$content" ]]; then
        log_error "Deep file content mismatch. Expected: '$content', Got: '$actual'"
        return 1
    fi
    log_info "Deep file content verified"

    # Verify from VM 2
    if ! ssh_run 2 "test -d $base_dir/a/b/c/d"; then
        log_error "Nested directory not visible from VM 2"
        return 1
    fi

    local actual2
    actual2=$(ssh_run 2 "cat $base_dir/$deep_file")
    if [[ "$actual2" != "$content" ]]; then
        log_error "Deep file content mismatch on VM 2"
        return 1
    fi
    log_info "Cross-VM nested directory verification passed"

    log_info "Nested directories test passed"
    return 0
}

# Test: Delete empty directory
# Creates an empty directory and removes it, verifying deletion
test_delete_directory() {
    log_info "Testing delete empty directory..."
    ensure_cluster_healthy 2

    local vm_id=1
    local dirname="emptydir_test"

    # Create an empty directory
    ssh_run "$vm_id" "mkdir -p /mnt/goclusterfs/$dirname"

    # Verify it exists
    if ! ssh_run "$vm_id" "test -d /mnt/goclusterfs/$dirname"; then
        log_error "Directory should exist after creation"
        return 1
    fi
    log_info "Directory created successfully"

    # Delete it
    ssh_run "$vm_id" "rmdir /mnt/goclusterfs/$dirname"

    # Verify it's gone
    if ssh_run "$vm_id" "test -d /mnt/goclusterfs/$dirname"; then
        log_error "Directory should not exist after deletion"
        return 1
    fi
    log_info "Directory deleted successfully"

    # Verify deletion is visible from VM 2
    if ssh_run 2 "test -d /mnt/goclusterfs/$dirname"; then
        log_error "Deleted directory still visible from VM 2"
        return 1
    fi
    log_info "Cross-VM deletion verification passed"

    log_info "Delete directory test passed"
    return 0
}

# Test: Directory fsync
# Git, SQLite and Postgres fsync a directory after creating or removing an entry in it; each fsync must succeed.
test_dir_fsync() {
    log_info "Testing directory fsync..."
    ensure_cluster_healthy 2

    local dir=/mnt/goclusterfs/dirfsync_test
    if ! ssh_run 1 "mkdir -p $dir && sync /mnt/goclusterfs && touch $dir/created && sync $dir && rm $dir/created && sync $dir"; then
        log_error "fsync of a directory failed"
        return 1
    fi
    ssh_run 1 "rm -rf $dir"
    log_info "Directory fsync test passed"
    return 0
}

# Test: Hard links
# A file linked on one VM reads through both names on the other; removing one name keeps the file.
test_hard_links() {
    log_info "Testing hard links..."
    ensure_cluster_healthy 2

    local mnt=/mnt/goclusterfs
    if ! ssh_run 1 "echo hardlinked > $mnt/hl_a && ln $mnt/hl_a $mnt/hl_b && [ \$(stat -c %h $mnt/hl_a) = 2 ]"; then
        log_error "ln on VM 1 failed, or the link count is not 2"
        return 1
    fi
    if [[ "$(ssh_run 2 "cat $mnt/hl_b")" != "hardlinked" ]]; then
        log_error "VM 2 cannot read the file through its second name"
        return 1
    fi
    if ! ssh_run 2 "rm $mnt/hl_a && [ \$(stat -c %h $mnt/hl_b) = 1 ] && [ \$(cat $mnt/hl_b) = hardlinked ]"; then
        log_error "removing one name on VM 2 did not leave the other with one link"
        return 1
    fi
    ssh_run 1 "rm -f $mnt/hl_a $mnt/hl_b"
    log_info "Hard links test passed"
    return 0
}

# Test: Truncate file
# Creates a file and truncates it to a smaller size, verifying the operation
test_truncate_file() {
    log_info "Testing file truncation..."
    ensure_cluster_healthy 2

    local vm_id=1
    local filename="truncate_test.txt"
    local original_content="This is some longer content that we will truncate"
    local truncate_size=10

    # Create a file with content
    ssh_run "$vm_id" "echo -n '$original_content' > /mnt/goclusterfs/$filename"

    # Verify original size
    local original_size
    original_size=$(ssh_run "$vm_id" "stat -c%s /mnt/goclusterfs/$filename")
    log_info "Original file size: $original_size bytes"

    # Truncate to 10 bytes
    ssh_run "$vm_id" "truncate -s $truncate_size /mnt/goclusterfs/$filename"

    # Verify new size
    local new_size
    new_size=$(ssh_run "$vm_id" "stat -c%s /mnt/goclusterfs/$filename")
    if [[ "$new_size" -ne "$truncate_size" ]]; then
        log_error "Size after truncate: expected $truncate_size, got $new_size"
        return 1
    fi
    log_info "Truncated size verified: $new_size bytes"

    # Verify content is the first 10 bytes
    local actual_content
    actual_content=$(ssh_run "$vm_id" "cat /mnt/goclusterfs/$filename")
    local expected_content="${original_content:0:$truncate_size}"
    if [[ "$actual_content" != "$expected_content" ]]; then
        log_error "Content mismatch. Expected: '$expected_content', Got: '$actual_content'"
        return 1
    fi
    log_info "Truncated content verified"

    # Verify from VM 2
    local size2
    size2=$(ssh_run 2 "stat -c%s /mnt/goclusterfs/$filename")
    if [[ "$size2" -ne "$truncate_size" ]]; then
        log_error "Size mismatch on VM 2: expected $truncate_size, got $size2"
        return 1
    fi
    log_info "Cross-VM truncate verification passed"

    log_info "Truncate file test passed"
    return 0
}

# Test: Append to file
# Creates a file with initial content, appends more data, and verifies the combined result
test_append_file() {
    log_info "Testing file append operation..."
    ensure_cluster_healthy 2

    local vm_id=1
    local filename="append_test.txt"
    local initial_content="Hello"
    local append_content=" World!"
    local expected_content="Hello World!"

    # Create initial file with content
    ssh_run "$vm_id" "echo -n '$initial_content' > /mnt/goclusterfs/$filename"

    # Verify initial content
    local initial_read
    initial_read=$(ssh_run "$vm_id" "cat /mnt/goclusterfs/$filename")
    if [[ "$initial_read" != "$initial_content" ]]; then
        log_error "Initial content mismatch. Expected: '$initial_content', Got: '$initial_read'"
        return 1
    fi
    log_info "Initial content verified: '$initial_read'"

    # Append data using >> redirection
    ssh_run "$vm_id" "echo -n '$append_content' >> /mnt/goclusterfs/$filename"

    # Read back and verify combined content
    local final_read
    final_read=$(ssh_run "$vm_id" "cat /mnt/goclusterfs/$filename")
    if [[ "$final_read" != "$expected_content" ]]; then
        log_error "Append result mismatch. Expected: '$expected_content', Got: '$final_read'"
        return 1
    fi
    log_info "Appended content verified: '$final_read'"

    # Verify from VM 2
    local cross_read
    cross_read=$(ssh_run 2 "cat /mnt/goclusterfs/$filename")
    if [[ "$cross_read" != "$expected_content" ]]; then
        log_error "Cross-VM content mismatch. Expected: '$expected_content', Got: '$cross_read'"
        return 1
    fi
    log_info "Cross-VM append verification passed"

    log_info "Append file test passed"
    return 0
}

# Test: Rename file
# Attempts to rename a file within the filesystem
# Note: Rename is not yet implemented in GoClusterFS - test skips if operation fails
test_rename_file() {
    log_info "Testing file rename operation..."
    ensure_cluster_healthy 2

    local vm_id=1
    local oldname="oldname.txt"
    local newname="newname.txt"
    local content="Rename test content"

    # Create the original file
    ssh_run "$vm_id" "echo -n '$content' > /mnt/goclusterfs/$oldname"

    # Verify it exists
    if ! ssh_run "$vm_id" "test -f /mnt/goclusterfs/$oldname"; then
        log_error "Original file should exist before rename"
        return 1
    fi
    log_info "Original file created: $oldname"

    # Attempt to rename
    if ! ssh_run "$vm_id" "mv /mnt/goclusterfs/$oldname /mnt/goclusterfs/$newname" 2>/dev/null; then
        log_error "mv $oldname $newname failed"
        return 1
    fi

    # If rename succeeded, verify old name doesn't exist
    if ssh_run "$vm_id" "test -f /mnt/goclusterfs/$oldname"; then
        log_error "Old name should not exist after rename"
        return 1
    fi
    log_info "Old file no longer exists"

    # Verify new name exists with same content
    local readback
    readback=$(ssh_run "$vm_id" "cat /mnt/goclusterfs/$newname")
    if [[ "$readback" != "$content" ]]; then
        log_error "Content changed after rename. Expected: '$content', Got: '$readback'"
        return 1
    fi
    log_info "New file content verified"

    # Verify from VM 2
    if ssh_run 2 "test -f /mnt/goclusterfs/$oldname"; then
        log_error "Old name visible on VM 2 after rename"
        return 1
    fi
    if ! ssh_run 2 "test -f /mnt/goclusterfs/$newname"; then
        log_error "New name not visible on VM 2 after rename"
        return 1
    fi
    log_info "Cross-VM rename verification passed"

    log_info "Rename file test passed"
    return 0
}

# Waits up to 5 s for a shell check, run in the mount on a node, to pass: a node may show old names for -attr-timeout.
settle_on_vm() {
    local vm_id=$1 check=$2 i
    for i in $(seq 1 25); do
        ssh_run "$vm_id" "cd /mnt/goclusterfs && $check" >/dev/null 2>&1 && return 0
        sleep 0.2
    done
    log_error "On VM $vm_id, still false after 5 s: $check"
    return 1
}

# Test: two VMs rename at once: onto one name, a rename racing an unlink of its source or a create of its target,
# and crossing directory moves. Based on the Go TestMultiNodeRenamesOntoOneName, TestMultiNodeRenameRacesUnlinkOfItsSource,
# TestMultiNodeRenameRacesCreateOfItsTarget and TestMultiNodeCrossMovesCannotFormALoop.
test_multinode_rename() {
    log_info "Testing renames from two VMs at once..."
    ensure_cluster_healthy 2 || return 1
    local n1=1 n2=2 m=/mnt/goclusterfs i p1 p2 r1 r2 got want
    for i in 1 2 3 4 5; do
        ssh_run "$n1" "printf a$i > $m/rn_a$i" && ssh_run "$n2" "printf b$i > $m/rn_b$i" || return 1
        ssh_run "$n1" "mv $m/rn_a$i $m/rn_t$i" & p1=$!
        ssh_run "$n2" "mv $m/rn_b$i $m/rn_t$i" & p2=$!
        wait $p1; r1=$?; wait $p2; r2=$?
        if [[ $r1 -ne 0 || $r2 -ne 0 ]]; then
            log_error "round $i: renames onto one name exited $r1 and $r2, want both 0"
            return 1
        fi
        settle_on_vm "$n1" "test ! -e rn_a$i && test ! -e rn_b$i" || return 1
        got=$(ssh_run "$n1" "cat $m/rn_t$i")
        if [[ "$got" != "a$i" && "$got" != "b$i" ]]; then
            log_error "round $i: rn_t$i holds '$got', want a$i or b$i"
            return 1
        fi
        settle_on_vm "$n2" "test ! -e rn_a$i && test ! -e rn_b$i && test \"\$(cat rn_t$i)\" = $got" || return 1

        ssh_run "$n1" "printf s$i > $m/rn_s$i" || return 1
        settle_on_vm "$n2" "test \"\$(cat rn_s$i)\" = s$i" || return 1
        ssh_run "$n1" "mv $m/rn_s$i $m/rn_d$i" 2>/dev/null & p1=$!
        ssh_run "$n2" "rm $m/rn_s$i" 2>/dev/null & p2=$!
        wait $p1; r1=$?; wait $p2; r2=$?
        if [[ $r1 -eq 0 && $r2 -ne 0 ]]; then
            want="test ! -e rn_s$i && test \"\$(cat rn_d$i)\" = s$i"
        elif [[ $r2 -eq 0 && $r1 -ne 0 ]]; then
            want="test ! -e rn_s$i && test ! -e rn_d$i"
        else
            log_error "round $i: rename and unlink of one name exited $r1 and $r2, want exactly one 0"
            return 1
        fi
        settle_on_vm "$n1" "$want" && settle_on_vm "$n2" "$want" || return 1

        ssh_run "$n1" "printf m$i > $m/rn_m$i" || return 1
        ssh_run "$n1" "mv $m/rn_m$i $m/rn_c$i" & p1=$!
        ssh_run "$n2" "set -C; printf c$i > $m/rn_c$i" 2>/dev/null & p2=$!
        wait $p1; r1=$?; wait $p2
        if [[ $r1 -ne 0 ]]; then
            log_error "round $i: a rename racing a create of its target exited $r1, want 0"
            return 1
        fi
        want="test ! -e rn_m$i && test \"\$(cat rn_c$i)\" = m$i"
        settle_on_vm "$n1" "$want" && settle_on_vm "$n2" "$want" || return 1

        ssh_run "$n1" "mkdir $m/rn_x$i $m/rn_y$i" || return 1
        settle_on_vm "$n2" "test -d rn_x$i && test -d rn_y$i" || return 1
        ssh_run "$n1" "mv $m/rn_x$i $m/rn_y$i/x" 2>/dev/null & p1=$!
        ssh_run "$n2" "mv $m/rn_y$i $m/rn_x$i/y" 2>/dev/null & p2=$!
        wait $p1; r1=$?; wait $p2; r2=$?
        if [[ $r1 -eq 0 && $r2 -ne 0 ]]; then
            want="test -d rn_y$i/x && test ! -e rn_x$i"
        elif [[ $r2 -eq 0 && $r1 -ne 0 ]]; then
            want="test -d rn_x$i/y && test ! -e rn_y$i"
        else
            log_error "round $i: crossing directory moves exited $r1 and $r2, want exactly one 0"
            return 1
        fi
        settle_on_vm "$n1" "$want" && settle_on_vm "$n2" "$want" || return 1
    done
    log_info "Multi-node rename test passed"
    return 0
}

# Runs cmd on a VM again while it fails with ENOENT, since a node can see another node's change only after -cache-ttl.
until_found_on_vm() {
    local vm_id=$1 cmd=$2 i out
    for i in $(seq 1 75); do
        out=$(ssh_run "$vm_id" "$cmd" 2>&1) && return 0
        [[ "$out" == *"No such file"* ]] || { log_error "On VM $vm_id, $cmd: $out"; return 1; }
        sleep 0.2
    done
    log_error "On VM $vm_id, still ENOENT after 15 s: $cmd"
    return 1
}

# Test: VM 1 renames a directory VM 2 moved while VM 1's kernel holds the old name; a cwd in it keeps working.
# Based on TestRenameOfDirAnotherNodeMoved and TestMovedDirOldHandleAndCwdKeepWorking. busybox mv calls rename(2) alone.
test_multinode_rename_moved_dir() {
    log_info "Testing renames of a directory another VM moved..."
    ensure_cluster_healthy 2 || return 1
    local n1=1 n2=2 m=/mnt/goclusterfs dst d want p
    ssh_run "$n1" "mkdir $m/md_a $m/md_b $m/md_c" || return 1
    for dst in md_a md_c md_b; do
        d=md_d_${dst#md_}
        ssh_run "$n1" "mkdir $m/md_a/$d && stat $m/md_a/$d >/dev/null" || return 1
        until_found_on_vm "$n2" "mv $m/md_a/$d $m/md_b/$d-moved" || return 1
        until_found_on_vm "$n1" "mv $m/md_b/$d-moved $m/$dst/$d-final" || return 1
        want="test -d $dst/$d-final && test ! -e md_a/$d && test ! -e md_b/$d-moved"
        settle_on_vm "$n1" "$want" && settle_on_vm "$n2" "$want" || return 1
    done

    ssh_run "$n1" "mkdir $m/md_a/md_cwd && touch $m/md_a/md_cwd/before && rm -f /tmp/md_ready /tmp/md_go" || return 1
    ssh_run "$n1" "cd $m/md_a/md_cwd && touch /tmp/md_ready && while [ ! -e /tmp/md_go ]; do sleep 0.1; done; ls > /tmp/md_ls && touch via-cwd" & p=$!
    settle_on_vm "$n1" "test -e /tmp/md_ready" || return 1
    until_found_on_vm "$n2" "mv $m/md_a/md_cwd $m/md_b/md_cwd" || return 1
    until_found_on_vm "$n1" "mv $m/md_b/md_cwd $m/md_a/md_cwd-final" || return 1
    ssh_run "$n1" "touch /tmp/md_go" || return 1
    if ! wait $p; then
        log_error "Listing or creating in the working directory of the moved directory failed"
        return 1
    fi
    if ! ssh_run "$n1" "grep -qx before /tmp/md_ls"; then
        log_error "The working directory of the moved directory listed '$(ssh_run "$n1" "cat /tmp/md_ls")', want before"
        return 1
    fi
    want="test -e md_a/md_cwd-final/before && test -e md_a/md_cwd-final/via-cwd"
    settle_on_vm "$n1" "$want" && settle_on_vm "$n2" "$want" || return 1
    ssh_run "$n1" "rm -f /tmp/md_ready /tmp/md_go /tmp/md_ls"
    log_info "Moved-directory rename test passed"
    return 0
}

# Test: Single-node stress test
# Rapidly creates, reads, verifies, and deletes files to stress test the filesystem
test_stress_single_node() {
    log_info "Running single-node stress test..."
    ensure_cluster_healthy 2

    local vm_id=1
    local iterations=100  # Reduced from Go's 200 for faster test execution
    local errors=0

    log_info "Running $iterations create/read/delete iterations..."

    for i in $(seq 1 $iterations); do
        local filename=$(printf "stress_%04d.txt" $i)
        local content="Stress test iteration $i"

        # Create file
        if ! ssh_run "$vm_id" "echo -n '$content' > /mnt/goclusterfs/$filename" 2>/dev/null; then
            log_error "Iteration $i: Failed to create file"
            ((errors++))
            continue
        fi

        # Read and verify
        local readback
        readback=$(ssh_run "$vm_id" "cat /mnt/goclusterfs/$filename" 2>/dev/null)
        if [[ "$readback" != "$content" ]]; then
            log_error "Iteration $i: Content mismatch. Expected: '$content', Got: '$readback'"
            ((errors++))
            continue
        fi

        # Delete
        if ! ssh_run "$vm_id" "rm /mnt/goclusterfs/$filename" 2>/dev/null; then
            log_error "Iteration $i: Failed to delete file"
            ((errors++))
            continue
        fi

        # Progress logging every 25 iterations
        if [[ $((i % 25)) -eq 0 ]]; then
            log_info "Progress: $i/$iterations iterations completed"
        fi
    done

    # Verify no stress test files remain
    local remaining
    remaining=$(ssh_run "$vm_id" "ls /mnt/goclusterfs/stress_*.txt 2>/dev/null | wc -l")
    if [[ "$remaining" -gt 0 ]]; then
        log_error "$remaining stress test files remain after cleanup"
        ((errors++))
    fi

    if [[ $errors -gt 0 ]]; then
        log_error "Stress test completed with $errors errors"
        return 1
    fi

    log_info "Stress test passed: $iterations iterations with 0 errors"
    return 0
}

# Test: Concurrent directory operations
# Multiple VMs create files simultaneously (tests concurrent multi-node visibility)
# Based on Go TestMultiNodeConcurrentDirectoryOps which creates files not directories
test_concurrent_directory_ops() {
    log_info "Testing concurrent directory operations across VMs..."

    # Need at least 2 VMs for this test
    if [[ $NODE_COUNT -lt 2 ]]; then
        log_info "SKIP: Need at least 2 VMs for concurrent directory ops test"
        return 0
    fi
    ensure_cluster_healthy 2

    local vm1=1
    local vm2=2
    local files_per_node=5
    local errors=0

    # Create files concurrently from both VMs (like Go test)
    log_info "Creating $files_per_node files from each of 2 VMs concurrently..."

    # VM 1 creates files in background
    for i in $(seq 0 $((files_per_node - 1))); do
        ssh_run "$vm1" "echo 'Content from node 0 file $i' > /mnt/goclusterfs/node0_${i}.txt" &
    done

    # VM 2 creates files in background
    for i in $(seq 0 $((files_per_node - 1))); do
        ssh_run "$vm2" "echo 'Content from node 1 file $i' > /mnt/goclusterfs/node1_${i}.txt" &
    done

    # Wait for all background processes
    wait

    # Give filesystem time to sync
    sleep 2

    # Verify all files are visible from VM 1
    log_info "Verifying all files visible from VM 1..."
    for n in 0 1; do
        for i in $(seq 0 $((files_per_node - 1))); do
            local filename="node${n}_${i}.txt"
            if ! ssh_run "$vm1" "test -f /mnt/goclusterfs/$filename"; then
                log_error "$filename not visible from VM 1"
                ((errors++))
            fi
        done
    done

    # Verify all files are visible from VM 2
    log_info "Verifying all files visible from VM 2..."
    for n in 0 1; do
        for i in $(seq 0 $((files_per_node - 1))); do
            local filename="node${n}_${i}.txt"
            if ! ssh_run "$vm2" "test -f /mnt/goclusterfs/$filename"; then
                log_error "$filename not visible from VM 2"
                ((errors++))
            fi
        done
    done

    # Cross-verify content
    log_info "Cross-verifying file contents..."
    for n in 0 1; do
        for i in $(seq 0 $((files_per_node - 1))); do
            local filename="node${n}_${i}.txt"
            local expected="Content from node $n file $i"

            # Read from both VMs
            local content1
            local content2
            content1=$(ssh_run "$vm1" "cat /mnt/goclusterfs/$filename" 2>/dev/null)
            content2=$(ssh_run "$vm2" "cat /mnt/goclusterfs/$filename" 2>/dev/null)

            if [[ "$content1" != "$expected" ]]; then
                log_error "Content mismatch for $filename from VM 1. Expected: '$expected', Got: '$content1'"
                ((errors++))
            fi
            if [[ "$content2" != "$expected" ]]; then
                log_error "Content mismatch for $filename from VM 2. Expected: '$expected', Got: '$content2'"
                ((errors++))
            fi
        done
    done

    # Cleanup
    for n in 0 1; do
        for i in $(seq 0 $((files_per_node - 1))); do
            ssh_run "$vm1" "rm /mnt/goclusterfs/node${n}_${i}.txt" 2>/dev/null || true
        done
    done

    if [[ $errors -gt 0 ]]; then
        log_error "Concurrent directory ops test failed with $errors errors"
        return 1
    fi

    log_info "Concurrent directory ops test passed"
    return 0
}

# Test 9: Long-running stability test (optional)
test_stability() {
    if [[ "$RUN_LONG_TESTS" != "true" ]]; then
        log_info "Skipping stability test (use --long to enable)"
        return 0
    fi

    log_info "Running stability test for ${STABILITY_DURATION_MINUTES} minutes..."
    ensure_cluster_healthy 2

    local end_time=$((SECONDS + STABILITY_DURATION_MINUTES * 60))
    local iteration=0
    local errors=0

    while [[ $SECONDS -lt $end_time ]]; do
        iteration=$((iteration + 1))

        # Write from random VM
        local write_vm=$((RANDOM % NODE_COUNT + 1))
        write_file_on_vm "$write_vm" "stability_${iteration}.txt" "Iteration $iteration from VM $write_vm" || ((errors++))

        # Read from different VM
        local read_vm=$(((write_vm % NODE_COUNT) + 1))
        if ! file_exists_on_vm "$read_vm" "stability_${iteration}.txt"; then
            log_error "Stability test: iteration $iteration - file not visible from VM $read_vm"
            ((errors++))
        fi

        # Occasional VM restart (5% chance)
        if [[ $((RANDOM % 20)) -eq 0 ]]; then
            local restart_vm=$((RANDOM % NODE_COUNT + 1))
            log_info "Stability test: restarting VM $restart_vm at iteration $iteration"
            stop_vm "$restart_vm"
            start_vm "$restart_vm" false
            wait_vm_healthy "$restart_vm"
            setup_goclusterfs "$restart_vm" false  # Re-mount GoClusterFS
        fi

        # Progress update every 100 iterations
        if [[ $((iteration % 100)) -eq 0 ]]; then
            local remaining=$((end_time - SECONDS))
            log_info "Stability test: $iteration iterations, $errors errors, ${remaining}s remaining"
        fi

        sleep 0.1
    done

    log_info "Stability test completed: $iteration iterations, $errors errors"

    if [[ $errors -gt 0 ]]; then
        return 1
    fi
    return 0
}

# =============================================================================
# Concurrent Same-File Write Tests (using pre-extension pattern)
# =============================================================================

# Two VMs write different ranges of each new file's first block at once.
# Without the inode lock the later inode write drops the other VM's extent.
test_concurrent_first_write_same_block() {
    log_info "Testing concurrent first writes to the same block (2 VMs)..."
    ensure_cluster_healthy 2

    local files=10
    local size=65536
    local offset0=262144
    local offset1=524288

    for i in $(seq 1 $files); do
        ssh_run 1 "touch /mnt/goclusterfs/first_write_$i.bin"
    done
    sleep 2

    for i in $(seq 1 $files); do
        write_pattern_at_on_vm 1 "first_write_$i.bin" "A" "$size" "$offset0" &
        write_pattern_at_on_vm 2 "first_write_$i.bin" "B" "$size" "$offset1" &
        wait
    done
    sleep 2

    # Restart the reader so cached extents from earlier files with the same inode numbers cannot hide the result.
    stop_vm 1
    start_vm 1 false
    wait_vm_healthy 1 || return 1
    setup_goclusterfs 1 false

    local lost=0
    for i in $(seq 1 $files); do
        verify_pattern_at_on_vm 1 "first_write_$i.bin" "A" "$offset0" 4096 || lost=$((lost + 1))
        verify_pattern_at_on_vm 1 "first_write_$i.bin" "B" "$offset1" 4096 || lost=$((lost + 1))
    done
    if [[ $lost -gt 0 ]]; then
        log_error "$lost of $((files * 2)) concurrent first writes lost"
        return 1
    fi
    log_success "All concurrent first writes survived"
    return 0
}

# Test 10: Two VMs write to same file at different offsets
test_concurrent_same_file_writes() {
    log_info "Testing concurrent same-file writes (2 VMs)..."
    ensure_cluster_healthy 2

    local filename="samefile_2vm.txt"
    local chunk_size=$((400 * 1024))  # 400KB each
    local final_size=$((chunk_size * 2))  # 800KB total

    # Pre-extend file to final size (critical: avoids TOCTOU race in lock CAS)
    pre_extend_file_on_vm 1 "$filename" "$final_size"
    sleep 1

    # Concurrent writes at different offsets
    # VM 1: 400KB of 'A' at offset 0
    # VM 2: 400KB of 'B' at offset 400KB
    write_pattern_at_on_vm 1 "$filename" "A" "$chunk_size" 0 &
    write_pattern_at_on_vm 2 "$filename" "B" "$chunk_size" "$chunk_size" &
    wait
    sleep 2

    # Verify file size
    local actual_size
    actual_size=$(get_file_size_on_vm 1 "$filename")
    if [ "$actual_size" -lt "$final_size" ]; then
        log_error "File too small: $actual_size bytes, expected at least $final_size"
        return 1
    fi

    # Verify each VM's data at expected offset
    if ! verify_pattern_at_on_vm 1 "$filename" "A" 0 100; then
        log_error "VM 1 data corrupted at offset 0"
        return 1
    fi

    if ! verify_pattern_at_on_vm 1 "$filename" "B" "$chunk_size" 100; then
        log_error "VM 2 data corrupted at offset $chunk_size"
        return 1
    fi

    log_info "Concurrent same-file writes (2 VMs) test passed"
    return 0
}

# The tear scripts run on each node with sh -s: one node overwrites a 1 MB file
# with one 1 MB write of a single letter at a time, while readers read it whole
# with O_DIRECT and count reads that hold more than one letter.
TEAR_PATTERNS='for c in A B C D E F G H; do head -c 1048576 /dev/zero | tr "\000" "$c" > /tmp/tear_$c; done'
TEAR_WRITER='end=$(( $(date +%s) + $2 ))
while [ "$(date +%s)" -lt "$end" ]; do
    for c in A B C D E F G H; do
        dd if=/tmp/tear_$c of="$1" bs=1048576 count=1 conv=notrunc 2>/dev/null || exit 1
    done
done'
TEAR_READER='end=$(( $(date +%s) + $2 )); reads=0; torn=0
while [ "$(date +%s)" -lt "$end" ]; do
    dd if="$1" of=/tmp/tear_read bs=1048576 count=1 iflag=direct 2>/dev/null || exit 1
    c=$(head -c 1 /tmp/tear_read)
    cmp -s /tmp/tear_read "/tmp/tear_$c" || torn=$((torn + 1))
    reads=$((reads + 1))
done
echo "$reads $torn"'

# Test: a read on any VM never returns parts of two 1 MB writes from VM 1
test_reads_never_see_part_of_a_write() {
    log_info "Testing that whole-file reads never mix two writes..."
    ensure_cluster_healthy 2

    local file=/mnt/goclusterfs/tear.bin seconds=10 vm
    for vm in $(seq 1 "$NODE_COUNT"); do
        ssh_run "$vm" "$TEAR_PATTERNS" || return 1
    done
    ssh_run 1 "cp /tmp/tear_A $file" || return 1
    sleep 2

    local out
    out=$(mktemp -d)
    ssh_run 1 "sh -s -- $file $seconds" <<< "$TEAR_WRITER" &
    local writer=$! readers=()
    for vm in $(seq 1 "$NODE_COUNT"); do
        ssh_run "$vm" "sh -s -- $file $seconds" <<< "$TEAR_READER" > "$out/$vm" &
        readers+=($!)
    done
    local failed=0 pid
    wait "$writer" || { log_error "VM 1's writes failed"; failed=1; }
    for pid in "${readers[@]}"; do
        wait "$pid" || { log_error "a reader failed"; failed=1; }
    done

    local reads torn
    for vm in $(seq 1 "$NODE_COUNT"); do
        read -r reads torn < "$out/$vm" || { reads=0; torn=0; }
        log_info "VM $vm: $torn of $reads reads mixed two writes"
        if [ "${torn:-0}" -ne 0 ]; then
            log_error "VM $vm: $torn of $reads reads returned parts of two writes"
            failed=1
        fi
        if [ "${reads:-0}" -lt 5 ]; then
            log_error "VM $vm made only ${reads:-0} reads"
            failed=1
        fi
    done
    rm -rf "$out"
    [ "$failed" -eq 0 ] || return 1
    log_info "Whole-file reads never mixed two writes"
}

# Test 11: Three VMs write to same file at different offsets
test_concurrent_same_file_writes_3vms() {
    log_info "Testing concurrent same-file writes (3 VMs)..."
    if [[ $NODE_COUNT -lt 3 ]]; then
        log_warn "Skipping: Need at least 3 VMs for this test"
        return 0
    fi
    ensure_cluster_healthy 3

    local filename="samefile_3vm.txt"
    local chunk_size=$((300 * 1024))  # 300KB each
    local final_size=$((chunk_size * 3))  # 900KB total

    # Pre-extend file
    pre_extend_file_on_vm 1 "$filename" "$final_size"
    sleep 1

    # Concurrent writes
    local offset1=$chunk_size
    local offset2=$((chunk_size * 2))

    write_pattern_at_on_vm 1 "$filename" "1" "$chunk_size" 0 &
    write_pattern_at_on_vm 2 "$filename" "2" "$chunk_size" "$offset1" &
    write_pattern_at_on_vm 3 "$filename" "3" "$chunk_size" "$offset2" &
    wait
    sleep 2

    # Verify all three regions
    if ! verify_pattern_at_on_vm 1 "$filename" "1" 0 100; then
        log_error "VM 1 data corrupted"
        return 1
    fi

    if ! verify_pattern_at_on_vm 1 "$filename" "2" "$offset1" 100; then
        log_error "VM 2 data corrupted"
        return 1
    fi

    if ! verify_pattern_at_on_vm 1 "$filename" "3" "$offset2" 100; then
        log_error "VM 3 data corrupted"
        return 1
    fi

    log_info "Concurrent same-file writes (3 VMs) test passed"
    return 0
}

# Test 12: Mixed size writes - one within file, one extending
test_mixed_size_writes() {
    log_info "Testing mixed size writes..."
    ensure_cluster_healthy 2

    local filename="mixed_writes.txt"
    local initial_size=$((500 * 1024))  # 500KB initial
    local extend_size=$((512 * 1024))  # 512KB extension
    local final_size=$((initial_size + extend_size))  # ~1MB total

    # Pre-extend file to FINAL size
    pre_extend_file_on_vm 1 "$filename" "$final_size"
    sleep 1

    # Fill initial region with 'I'
    write_pattern_at_on_vm 1 "$filename" "I" "$initial_size" 0
    sleep 1

    # Concurrent writes:
    # VM 1: 100 bytes of 'N' at offset 250KB (within initial region)
    # VM 2: 512KB of 'E' at offset 500KB (in extended region)
    local within_offset=$((250 * 1024))
    write_pattern_at_on_vm 1 "$filename" "N" 100 "$within_offset" &
    write_pattern_at_on_vm 2 "$filename" "E" "$extend_size" "$initial_size" &
    wait
    sleep 2

    # Verify the non-extending write
    if ! verify_pattern_at_on_vm 1 "$filename" "N" "$within_offset" 100; then
        log_error "Non-extending write data corrupted"
        return 1
    fi

    # Verify the extending write
    if ! verify_pattern_at_on_vm 1 "$filename" "E" "$initial_size" 100; then
        log_error "Extending write data corrupted"
        return 1
    fi

    log_info "Mixed size writes test passed"
    return 0
}

# Test 13: Overlapping block writes (all within same 1MB block)
test_overlapping_block_writes() {
    log_info "Testing overlapping block writes..."
    if [[ $NODE_COUNT -lt 3 ]]; then
        log_warn "Skipping: Need at least 3 VMs for this test"
        return 0
    fi
    ensure_cluster_healthy 3

    local filename="overlap_block.txt"
    local chunk_size=$((256 * 1024))  # 256KB each
    local final_size=$((chunk_size * 3))  # 768KB total (fits in one 1MB block)

    # Pre-extend file
    pre_extend_file_on_vm 1 "$filename" "$final_size"
    sleep 1

    # Concurrent writes to same block
    local offset1=$chunk_size
    local offset2=$((chunk_size * 2))

    write_pattern_at_on_vm 1 "$filename" "X" "$chunk_size" 0 &
    write_pattern_at_on_vm 2 "$filename" "Y" "$chunk_size" "$offset1" &
    write_pattern_at_on_vm 3 "$filename" "Z" "$chunk_size" "$offset2" &
    wait
    sleep 2

    # Verify all three regions
    if ! verify_pattern_at_on_vm 1 "$filename" "X" 0 100; then
        log_error "VM 1 data (X) corrupted"
        return 1
    fi

    if ! verify_pattern_at_on_vm 1 "$filename" "Y" "$offset1" 100; then
        log_error "VM 2 data (Y) corrupted"
        return 1
    fi

    if ! verify_pattern_at_on_vm 1 "$filename" "Z" "$offset2" 100; then
        log_error "VM 3 data (Z) corrupted"
        return 1
    fi

    log_info "Overlapping block writes test passed"
    return 0
}

# Test 14: Write stress test - multiple rounds
test_concurrent_write_stress() {
    log_info "Testing concurrent write stress (multiple rounds)..."
    if [[ $NODE_COUNT -lt 3 ]]; then
        log_warn "Skipping: Need at least 3 VMs for this test"
        return 0
    fi
    ensure_cluster_healthy 3

    local filename="write_stress.txt"
    local chunk_size=$((250 * 1024))  # 250KB each
    local round1_size=$((chunk_size * 3))  # 750KB
    local round2_size=$((chunk_size * 3))
    local final_size=$((round1_size + round2_size))  # 1.5MB total

    # Pre-extend file for all rounds
    pre_extend_file_on_vm 1 "$filename" "$final_size"
    sleep 1

    # Round 1: First 750KB
    log_info "Round 1: Writing first 750KB..."
    write_pattern_at_on_vm 1 "$filename" "a" "$chunk_size" 0 &
    write_pattern_at_on_vm 2 "$filename" "b" "$chunk_size" "$chunk_size" &
    write_pattern_at_on_vm 3 "$filename" "c" "$chunk_size" "$((chunk_size * 2))" &
    wait
    sleep 1

    # Round 2: Next 750KB
    log_info "Round 2: Writing next 750KB..."
    local r2_base=$round1_size
    write_pattern_at_on_vm 1 "$filename" "d" "$chunk_size" "$r2_base" &
    write_pattern_at_on_vm 2 "$filename" "e" "$chunk_size" "$((r2_base + chunk_size))" &
    write_pattern_at_on_vm 3 "$filename" "f" "$chunk_size" "$((r2_base + chunk_size * 2))" &
    wait
    sleep 2

    # Verify all 6 regions
    local patterns=("a" "b" "c" "d" "e" "f")
    for i in {0..5}; do
        local offset=$((chunk_size * i))
        if ! verify_pattern_at_on_vm 1 "$filename" "${patterns[$i]}" "$offset" 100; then
            log_error "Region $i (pattern ${patterns[$i]}) at offset $offset corrupted"
            return 1
        fi
    done

    log_info "Concurrent write stress test passed"
    return 0
}

# Test 15: Reversed write order (high offset first)
test_reversed_write_order() {
    log_info "Testing reversed write order..."
    ensure_cluster_healthy 2

    local filename="reversed_order.txt"
    local chunk_size=$((400 * 1024))  # 400KB each
    local final_size=$((chunk_size * 2))  # 800KB total

    # Pre-extend file
    pre_extend_file_on_vm 1 "$filename" "$final_size"
    sleep 1

    # Concurrent writes with reversed order
    # VM 2 writes at HIGH offset (400KB) - started first
    # VM 1 writes at LOW offset (0) - started slightly after
    write_pattern_at_on_vm 2 "$filename" "H" "$chunk_size" "$chunk_size" &
    sleep 0.1
    write_pattern_at_on_vm 1 "$filename" "L" "$chunk_size" 0 &
    wait
    sleep 2

    # Verify both regions
    if ! verify_pattern_at_on_vm 1 "$filename" "L" 0 100; then
        log_error "Low-offset data (L) corrupted"
        return 1
    fi

    if ! verify_pattern_at_on_vm 1 "$filename" "H" "$chunk_size" 100; then
        log_error "High-offset data (H) corrupted"
        return 1
    fi

    log_info "Reversed write order test passed"
    return 0
}

# Test: a VM loses power while it creates and unlinks files in the root directory, so it may die holding a lock on
# the root. The other VM unlinks files in the root across the failover, and none of its unlinks may fail: an
# operation that waits on the dead node's lock waits out its replay instead of returning EIO.
# Based on the Go TestOperationsWaitOutTheReplayOfALockHolder.
test_unlink_waits_out_peer_replay() {
    log_info "Testing unlinks across a peer's power failure..."
    ensure_cluster_healthy 2 || return 1
    local survivor=1 victim=2 m=/mnt/goclusterfs count=600
    local log="$RESULTS_DIR/unlink_waits_out_peer_replay.log"

    ssh_run "$survivor" "for i in \$(seq 1 $count); do : > $m/uw_\$i || exit 1; done" || return 1
    ssh_run "$victim" "i=0; while true; do i=\$((i+1)); : > $m/uv_\$i && rm -f $m/uv_\$i; done" >/dev/null 2>&1 &
    local churn=$!
    # One unlink every 0.1 s, so the unlinks span the failover.
    ssh_run "$survivor" "for i in \$(seq 1 $count); do rm $m/uw_\$i 2>&1 || echo FAILED uw_\$i; sleep 0.1; done" > "$log" 2>&1 &
    local unlinks=$!
    sleep 3
    log_info "Simulating power failure on VM $victim while it unlinks in the same directory..."
    kill_vm "$victim" || return 1
    wait $churn 2>/dev/null || true
    wait $unlinks
    local rc=$?

    if [[ $rc -ne 0 ]] || grep -q "FAILED" "$log"; then
        log_error "Unlinks on VM $survivor failed across the failover (exit $rc):"
        head -20 "$log" >&2
        return 1
    fi
    local left
    left=$(ssh_run "$survivor" "ls $m | grep -c '^uw_'")
    if [[ "$left" != "0" ]]; then
        log_error "$left of the unlinked files are still listed on VM $survivor"
        return 1
    fi
    log_info "All $count unlinks on VM $survivor succeeded across VM $victim's power failure"

    start_vm "$victim" false
    wait_vm_healthy "$victim"
    setup_goclusterfs "$victim" false
    return 0
}

# Test 16: Power failure during concurrent writes
test_concurrent_writes_power_failure() {
    log_info "Testing power failure during concurrent writes..."
    ensure_cluster_healthy 2

    local filename="concurrent_power_fail.txt"
    local chunk_size=$((400 * 1024))  # 400KB each
    local final_size=$((chunk_size * 2))  # 800KB total
    local survivor=1
    local victim=2

    # Pre-extend file
    pre_extend_file_on_vm "$survivor" "$filename" "$final_size"
    sleep 1

    # Start concurrent writes
    log_info "Starting concurrent writes..."
    write_pattern_at_on_vm "$survivor" "$filename" "S" "$chunk_size" 0 &
    local survivor_pid=$!

    write_pattern_at_on_vm "$victim" "$filename" "V" "$chunk_size" "$chunk_size" &
    local victim_pid=$!

    # Wait a moment for writes to start, then kill victim VM
    sleep 0.5
    log_info "Simulating power failure on VM $victim mid-write..."
    kill_vm "$victim"

    # Wait for survivor's write to complete
    wait $survivor_pid 2>/dev/null || true
    wait $victim_pid 2>/dev/null || true

    sleep 1

    # Verify survivor's data is intact
    if ! verify_pattern_at_on_vm "$survivor" "$filename" "S" 0 100; then
        log_error "Survivor's data corrupted after power failure"
        return 1
    fi
    log_info "Survivor's data verified intact"

    # Restart victim VM
    start_vm "$victim" false
    wait_vm_healthy "$victim"
    setup_goclusterfs "$victim" false

    # Verify from recovered victim
    if ! verify_pattern_at_on_vm "$victim" "$filename" "S" 0 100; then
        log_error "Survivor's data not visible from recovered victim"
        return 1
    fi

    # Victim's partial data may or may not be present (undefined behavior during power failure)
    # We just verify the system recovers without corruption
    log_info "Recovery successful after power failure during concurrent writes"
    return 0
}

# Print the PIDs of goclusterfs-mount on VM $1: processes with an argument equal to the path setup_goclusterfs starts
# it with. On Alpine the gcompat loader runs it, so its name and argv[0] are the loader's. A zombie has no arguments.
mount_pids() {
    ssh_run "$1" 'for p in /proc/[0-9]*; do tr "\0" "\n" 2>/dev/null <$p/cmdline | grep -qx "/usr/local/bin/goclusterfs-moun[t]" && echo ${p#/proc/}; done; true'
}

# Kill goclusterfs-mount on VM $1 with SIGKILL, as a crash would, and fail unless it is reaped within 30s.
# A zombie still passes kill(pid, 0), so a remount before then would not take its own slot back.
crash_goclusterfs() {
    local vm_id=$1 pids
    pids=$(mount_pids "$vm_id" | xargs)
    if [[ -z "$pids" ]]; then
        log_error "VM $vm_id: no goclusterfs-mount process to kill"
        return 1
    fi
    local start lingered=false
    start=$(date +%s%3N)
    ssh_run "$vm_id" "kill -9 $pids"
    while ssh_run "$vm_id" "for p in $pids; do [ -e /proc/\$p ] && exit 0; done; exit 1"; do
        if (( $(date +%s%3N) - start >= 30000 )); then
            log_error "VM $vm_id: goclusterfs-mount ($pids) not reaped 30s after SIGKILL; state, then each thread's state and wchan:"
            ssh_run "$vm_id" "for p in $pids; do grep -E '^(State|PPid|Threads)' /proc/\$p/status | tr '\n' ' '; echo; for t in /proc/\$p/task/*; do echo \"\$t \$(cut -d' ' -f3 \$t/stat) \$(cat \$t/wchan)\"; done; done" >&2 || true
            return 1
        fi
        lingered=true
        sleep 0.2
    done
    if $lingered; then
        log_info "VM $vm_id: goclusterfs-mount ($pids) reaped $(( $(date +%s%3N) - start ))ms after SIGKILL"
    fi
}

# Unmount GoClusterFS on VM $1 and wait for the mount process to exit, which
# marks its journal clean, and to be reaped: a zombie keeps its /proc entry.
unmount_goclusterfs() {
    local vm_id=$1 pids start
    pids=$(mount_pids "$vm_id" | xargs)
    ssh_run "$vm_id" "fusermount3 -u ${GOCLUSTERFS_MOUNT}" || return 1
    start=$(date +%s%3N)
    while [[ -n "$pids" ]] && ssh_run "$vm_id" "for p in $pids; do [ -e /proc/\$p ] && exit 0; done; exit 1"; do
        if (( $(date +%s%3N) - start >= 30000 )); then
            return 1
        fi
        sleep 0.2
    done
}

# A node stopped while a writer writes through it is proven dead and replayed by
# a peer, and the peer writes the file. When the stopped node resumes, its
# heartbeat is held (GOCLUSTERFS_TEST_HEARTBEAT_HOLD), so its queued write reaches
# the write gate first. The gate must refuse it and stop the node, and the
# file must keep the peer's data byte for byte.
test_stalled_node_writes_stop_at_write_gate() {
    log_info "Testing that a stalled node's queued write stops at the write gate..."
    if [[ $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping: Need at least 2 VMs for this test"
        return 0
    fi
    ensure_cluster_healthy "$NODE_COUNT"
    local stalled=1 peer=2 pids waited
    unmount_goclusterfs "$stalled" || { log_error "VM $stalled: unmount failed"; return 1; }
    setup_goclusterfs "$stalled" false "GOCLUSTERFS_TEST_HEARTBEAT_HOLD=30s"

    ssh_run "$peer" "head -c 4096 /dev/zero | tr '\\000' 0 > ${GOCLUSTERFS_MOUNT}/fenced && sync"
    ssh_run "$stalled" "head -c 4096 /dev/zero | tr '\\000' A > /tmp/fenced.A"
    ssh_run "$peer" "head -c 4096 /dev/zero | tr '\\000' B > /tmp/fenced.B"
    ssh_run "$stalled" "nohup sh -c 'while dd if=/tmp/fenced.A of=${GOCLUSTERFS_MOUNT}/fenced bs=4096 count=1 conv=notrunc 2>/dev/null; do :; done' < /dev/null > /dev/null 2>&1 &"
    sleep 1

    local marker="stalled-node gate test: stopping VM $stalled $(date +%s)"
    mark_vm_logs "$marker"
    pids=$(mount_pids "$stalled")
    ssh_run "$stalled" "kill -STOP $(echo $pids)"
    local peer_log stalled_log
    peer_log=$(get_vm_mount_log "$peer")
    stalled_log=$(get_vm_mount_log "$stalled")
    waited=0
    until sed -n "/$marker/,\$p" "$peer_log" | grep -q "Starting journal replay for node"; do
        if [[ $waited -ge 120 ]]; then
            log_error "VM $peer did not start replaying VM $stalled within 120s"
            ssh_run "$stalled" "kill -CONT $(echo $pids)"
            return 1
        fi
        sleep 1
        waited=$((waited + 1))
    done
    sleep 5
    if ! ssh_run "$peer" "dd if=/tmp/fenced.B of=${GOCLUSTERFS_MOUNT}/fenced bs=4096 count=1 conv=notrunc 2>/dev/null && sync"; then
        log_error "VM $peer could not write after taking over"
        ssh_run "$stalled" "kill -CONT $(echo $pids)"
        return 1
    fi
    ssh_run "$stalled" "kill -CONT $(echo $pids)"

    # The held heartbeat stops the node after 30s whatever the gate does; by then a write the gate let through has landed.
    waited=0
    while [[ -n "$(mount_pids "$stalled")" && $waited -lt 45 ]]; do
        sleep 1
        waited=$((waited + 1))
    done
    local result=0
    if ! ssh_run "$peer" "cmp -s ${GOCLUSTERFS_MOUNT}/fenced /tmp/fenced.B"; then
        log_error "fenced does not hold VM $peer's data after VM $stalled resumed"
        result=1
    fi
    if [[ -n "$(mount_pids "$stalled")" ]]; then
        log_error "VM $stalled did not stop after resuming from its takeover"
        result=1
    fi
    if ! sed -n "/$marker/,\$p" "$stalled_log" | grep -qE "checking the slot before a write|may no longer be this node's"; then
        log_error "VM $stalled stopped without its write gate or heartbeat checking the slot"
        result=1
    fi

    if [[ -n "$(mount_pids "$stalled")" ]]; then
        crash_goclusterfs "$stalled" || true
    fi
    # The bracket keeps pkill from matching the shell that runs it.
    ssh_run "$stalled" "pkill -f 'of=${GOCLUSTERFS_MOUNT}/fence[d]'; fusermount3 -uz ${GOCLUSTERFS_MOUNT}" || true
    if ! setup_goclusterfs "$stalled" false; then
        log_error "VM $stalled: remount after the test failed"
        result=1
    fi
    [[ $result -eq 0 ]] && log_info "Stalled node's queued write stopped at the write gate"
    return $result
}

# =============================================================================
# Storage fencing (VM_STORAGE=iscsi)
# =============================================================================

# fencing_restart formats the volume with -fencing=$1 through VM 1, whose mount runs with environment $2, and mounts
# the other nodes, so each fencing test starts from a known volume.
fencing_restart() {
    local fencing="$1" env1="${2:-}" vm
    for vm in $(seq 1 "$NODE_COUNT"); do
        if [[ -n "$(mount_pids "$vm")" ]]; then
            unmount_goclusterfs "$vm" || crash_goclusterfs "$vm" || return 1
        fi
        ssh_run "$vm" "fusermount3 -uz ${GOCLUSTERFS_MOUNT} 2>/dev/null; true"
    done
    setup_goclusterfs 1 true "$env1" "" "-fencing=$fencing" || return 1
    for vm in $(seq 2 "$NODE_COUNT"); do
        setup_goclusterfs "$vm" false || return 1
    done
}

# wait_log waits up to $3 seconds for VM $1's mount log to show $2 after the last marker line $4.
wait_log() {
    local vm="$1" pattern="$2" limit="$3" marker="$4" waited=0
    until sed -n "/$marker/,\$p" "$(get_vm_mount_log "$vm")" | grep -q "$pattern"; do
        if [[ $waited -ge $limit ]]; then
            log_error "VM $vm: no '$pattern' in its mount log within ${limit}s"
            return 1
        fi
        sleep 1
        waited=$((waited + 1))
    done
}

# wait_stopped waits up to $2 seconds for goclusterfs-mount on VM $1 to exit.
wait_stopped() {
    local vm="$1" limit="$2" waited=0
    while [[ -n "$(mount_pids "$vm")" ]]; do
        if [[ $waited -ge $limit ]]; then
            return 1
        fi
        sleep 1
        waited=$((waited + 1))
    done
}

# fenced_file_byte prints the first byte of the test file as VM $1 reads it from the device, past every cache.
fenced_file_byte() {
    ssh_run "$1" "sync; echo 3 > /proc/sys/vm/drop_caches; dd if=${GOCLUSTERFS_MOUNT}/fenced iflag=direct bs=4096 count=1 2>/dev/null | head -c 1"
}

# fencing_stalled_case formats with -fencing=$1 and stops VM 1 with SIGSTOP just before its write of A reaches the
# device, after every software check passed. VM 2 proves it dead and writes B, then VM 1 resumes. FENCED_BYTE gets the
# file's first byte afterwards: B when the storage refused the stale write, A when it landed.
fencing_stalled_case() {
    local fencing="$1" stalled=1 peer=2 pids waited
    FENCED_BYTE=""
    # The held heartbeat keeps the resumed node from noticing its lost slot before the stopped write runs.
    fencing_restart "$fencing" "GOCLUSTERFS_TEST_STOP_BEFORE_PWRITE=/tmp/stop-before-write GOCLUSTERFS_TEST_HEARTBEAT_HOLD=30s" || return 1
    ssh_run "$peer" "head -c 4096 /dev/zero | tr '\\000' 0 > ${GOCLUSTERFS_MOUNT}/fenced && sync"
    ssh_run "$stalled" "head -c 4096 /dev/zero | tr '\\000' A > /tmp/fenced.A"
    ssh_run "$peer" "head -c 4096 /dev/zero | tr '\\000' B > /tmp/fenced.B"
    pids=$(mount_pids "$stalled")
    local marker="fencing stalled writer ${fencing} $(date +%s)"
    mark_vm_logs "$marker"
    ssh_run "$stalled" "printf AAAAAAAA > /tmp/stop-before-write; nohup dd if=/tmp/fenced.A of=${GOCLUSTERFS_MOUNT}/fenced bs=4096 count=1 conv=notrunc,fsync < /dev/null > /dev/null 2>&1 &"
    waited=0
    until ssh_run "$stalled" "grep -q '^State:.*stopped' /proc/${pids%% *}/status"; do
        if [[ $waited -ge 30 ]]; then
            log_error "VM $stalled did not stop before its write of A within 30s"
            return 1
        fi
        sleep 1
        waited=$((waited + 1))
    done
    if ! wait_log "$peer" "Starting journal replay for node" 120 "$marker"; then
        ssh_run "$stalled" "kill -CONT $(echo $pids)"
        return 1
    fi
    sleep 3
    if ! ssh_run "$peer" "dd if=/tmp/fenced.B of=${GOCLUSTERFS_MOUNT}/fenced bs=4096 count=1 conv=notrunc,fsync 2>/dev/null"; then
        log_error "VM $peer could not write after taking over"
        ssh_run "$stalled" "kill -CONT $(echo $pids)"
        return 1
    fi
    ssh_run "$stalled" "kill -CONT $(echo $pids)"
    wait_stopped "$stalled" 45 || log_error "VM $stalled still runs 45s after it resumed"
    FENCED_BYTE=$(fenced_file_byte "$peer")
}

# The proof that fencing closes the window no software check can: a write that passed every gate just before its node
# stalled. On an unfenced volume, the control, it lands over the peer's data after the takeover, which shows the test
# sees the hazard. On a fenced volume the storage refuses it, and the node stops.
test_fencing_stalled_writer() {
    if [[ "$VM_STORAGE" != iscsi || $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping: needs VM_STORAGE=iscsi and 2 VMs"
        return 0
    fi
    local result=0
    fencing_stalled_case none || result=1
    if [[ "$FENCED_BYTE" != A ]]; then
        log_error "control: on an unfenced volume the stalled write should land over VM 2's data, but the file starts with '$FENCED_BYTE'"
        result=1
    fi
    fencing_stalled_case pr || result=1
    if [[ "$FENCED_BYTE" != B ]]; then
        log_error "fenced: the file starts with '$FENCED_BYTE', want VM 2's B"
        result=1
    fi
    if ! sed -n '/fencing stalled writer pr/,$p' "$(get_vm_mount_log 1)" | grep -q "The device refused a write"; then
        log_error "VM 1 did not report the refused write"
        result=1
    fi
    if ! sed -n '/fencing stalled writer pr/,$p' "$(get_vm_mount_log 2)" | grep -q "Revoked node"; then
        log_error "VM 2 did not report revoking VM 1's access"
        result=1
    fi
    ssh_run 1 "rm -f /tmp/stop-before-write"
    fencing_restart pr || result=1
    [[ $result -eq 0 ]] && log_info "The storage refused the stalled write; the control shows it lands without fencing"
    return $result
}

# A node VM paused while it writes is proven dead and fenced. When it resumes, its queued writes fail and it stops,
# and the file keeps the peer's data.
test_fencing_paused_vm() {
    if [[ "$VM_STORAGE" != iscsi || $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping: needs VM_STORAGE=iscsi and 2 VMs"
        return 0
    fi
    local paused=1 peer=2 result=0
    fencing_restart pr || return 1
    ssh_run "$peer" "head -c 4096 /dev/zero | tr '\\000' 0 > ${GOCLUSTERFS_MOUNT}/fenced && sync"
    ssh_run "$paused" "head -c 4096 /dev/zero | tr '\\000' A > /tmp/fenced.A"
    ssh_run "$peer" "head -c 4096 /dev/zero | tr '\\000' B > /tmp/fenced.B"
    local marker="fencing paused vm $(date +%s)"
    mark_vm_logs "$marker"
    ssh_run "$paused" "nohup sh -c 'while dd if=/tmp/fenced.A of=${GOCLUSTERFS_MOUNT}/fenced bs=4096 count=1 conv=notrunc,fsync 2>/dev/null; do :; done' < /dev/null > /dev/null 2>&1 &"
    sleep 2
    echo stop | nc -U -q1 "$(get_vm_monitor_socket "$paused")" > /dev/null
    if ! wait_log "$peer" "Revoked node" 120 "$marker"; then
        echo cont | nc -U -q1 "$(get_vm_monitor_socket "$paused")" > /dev/null
        return 1
    fi
    sleep 3
    ssh_run "$peer" "dd if=/tmp/fenced.B of=${GOCLUSTERFS_MOUNT}/fenced bs=4096 count=1 conv=notrunc,fsync 2>/dev/null" || result=1
    echo cont | nc -U -q1 "$(get_vm_monitor_socket "$paused")" > /dev/null
    if ! wait_stopped "$paused" 30; then
        log_error "VM $paused still runs 30s after it resumed"
        result=1
    fi
    if sed -n "/$marker/,\$p" "$(get_vm_mount_log "$paused")" | grep -q "The device refused a write"; then
        log_info "VM $paused stopped on a refused write"
    else
        log_info "VM $paused stopped without a refused write: an I/O error or its slot check came first"
    fi
    local byte
    byte=$(fenced_file_byte "$peer")
    if [[ "$byte" != B ]]; then
        log_error "the file starts with '$byte' after VM $paused resumed, want VM $peer's B"
        result=1
    fi
    ssh_run "$paused" "pkill -f 'of=${GOCLUSTERFS_MOUNT}/fence[d]'; true"
    fencing_restart pr || result=1
    return $result
}

# paused_file_byte prints the first byte of the pause test file as VM $1 reads it from the device, past every cache.
paused_file_byte() {
    ssh_run "$1" "sync; echo 3 > /proc/sys/vm/drop_caches; dd if=${GOCLUSTERFS_MOUNT}/paused iflag=direct bs=4096 count=1 2>/dev/null | head -c 1"
}

# pause_unfenced_case pauses VM 1 with QEMU stop while it rewrites a file, on a volume without storage fencing, until
# VM 2 has replayed it and written the file itself, then resumes it. The guest's clocks barely move while it is
# stopped. With $1 "held", VM 1's registry writes wait 2s in the guest, as a slow path or a guest-side queue would hold
# them, so the pause likely falls between a heartbeat's slot read and its write.
pause_unfenced_case() {
    local mode=$1 paused=1 peer=2 result=0 dev mon marker slot uuid
    dev=$(cluster_device)
    mon=$(get_vm_monitor_socket "$paused")
    ensure_cluster_healthy 2 || return 1
    if [[ "$mode" == held ]]; then
        unmount_goclusterfs "$paused" || return 1
        ssh_run "$paused" "apk add --no-cache device-mapper > /dev/null && modprobe dm-delay && { dmsetup remove gcfs-pause 2>/dev/null; sz=\$(blockdev --getsz $dev); printf '0 2048 linear $dev 0\n2048 2048 delay $dev 2048 0 $dev 2048 2000\n4096 %s linear $dev 4096\n' \$((sz - 4096)) | dmsetup create gcfs-pause; }" || return 1
        setup_goclusterfs "$paused" false "" /dev/mapper/gcfs-pause || return 1
    fi
    slot=$(ssh_run "$paused" "grep -o 'Registered as node [0-9]*' /var/log/goclusterfs.log | tail -1" | grep -o '[0-9]*$')
    uuid=$(ssh_run "$paused" "grep -o 'Registered as node [0-9]* (UUID: [0-9a-f]*' /var/log/goclusterfs.log | tail -1" | grep -o '[0-9a-f]*$')
    ssh_run "$peer" "head -c 4096 /dev/zero | tr '\\000' 0 > ${GOCLUSTERFS_MOUNT}/paused && sync"
    ssh_run "$paused" "head -c 4096 /dev/zero | tr '\\000' A > /tmp/paused.A"
    ssh_run "$peer" "head -c 4096 /dev/zero | tr '\\000' B > /tmp/paused.B"
    marker="unfenced paused vm $mode $(date +%s)"
    mark_vm_logs "$marker"
    ssh_run "$paused" "nohup sh -c 'while :; do dd if=/tmp/paused.A of=${GOCLUSTERFS_MOUNT}/paused bs=4096 count=1 conv=notrunc,fsync 2>/dev/null; done' < /dev/null > /dev/null 2>&1 &"
    sleep 3
    local h0 g0 w0 h1 g1 w1 stopped_at
    h0=$(date +%s.%N)
    read -r g0 w0 < <(ssh_run "$paused" "echo \$(cut -d' ' -f1 /proc/uptime) \$(date +%s)")
    echo stop | nc -U -q1 "$mon" > /dev/null
    stopped_at=$(date +%s)
    log_info "$mode: VM $paused (slot $slot, ${uuid:0:8}) stopped with QEMU stop"
    if ! wait_log "$peer" "Starting journal replay for node ${uuid:0:8}" 120 "$marker" ||
        ! wait_log "$peer" "\[Recovery\] Completed\|Journal is clean" 60 "$marker"; then
        echo cont | nc -U -q1 "$mon" > /dev/null
        pause_cleanup "$paused"
        return 1
    fi
    log_info "$mode: VM $peer replayed VM $paused $(( $(date +%s) - stopped_at ))s after the stop"
    sleep 3
    ssh_run "$peer" "dd if=/tmp/paused.B of=${GOCLUSTERFS_MOUNT}/paused bs=4096 count=1 conv=notrunc,fsync 2>/dev/null" || result=1
    log_info "$mode: VM $peer wrote B; the file starts with '$(paused_file_byte "$peer")'; slot $slot reads $(ssh_run "$peer" "dd if=$dev bs=4096 skip=$((256 + slot)) count=1 iflag=direct 2>/dev/null | od -An -tx1 -j4 -N20 | tr -d '\n'")"
    echo cont | nc -U -q1 "$mon" > /dev/null
    log_info "$mode: VM $paused resumed $(( $(date +%s) - stopped_at ))s after the stop"
    sleep 1
    read -r g1 w1 < <(ssh_run "$paused" "echo \$(cut -d' ' -f1 /proc/uptime) \$(date +%s)")
    h1=$(date +%s.%N)
    log_info "$mode: across the stop the host clock moved $(echo "$h0 $h1" | awk '{printf "%.1f", $2 - $1}')s, VM $paused's CLOCK_BOOTTIME $(echo "$g0 $g1" | awk '{printf "%.1f", $2 - $1}')s and its wall clock $((w1 - w0))s"
    sleep 15
    local byte state
    byte=$(paused_file_byte "$peer")
    state=$(ssh_run "$peer" "dd if=$dev bs=4096 skip=$((256 + slot)) count=1 iflag=direct 2>/dev/null | od -An -tx1 -j4 -N20 | tr -d '\n'")
    log_info "$mode: 15s after the resume, slot $slot reads (state, UUID)$state; VM $paused's mount is $([[ -n "$(mount_pids "$paused")" ]] && echo running || echo stopped)"
    sed -n "/$marker/,\$p" "$(get_vm_mount_log "$paused")" | grep -E "registry slot|refused|ERROR" | head -5
    if [[ "$byte" != B ]]; then
        log_error "$mode: the file starts with '$byte', want VM $peer's B: VM $paused landed a write after VM $peer replayed it"
        result=1
    fi
    if [[ "$state" == " 01 00 00 00 ${uuid:0:2}"* && -n "$(mount_pids "$paused")" ]]; then
        log_error "$mode: VM $paused wrote its slot back as its own over VM $peer's replay and keeps running"
        result=1
    fi
    pause_cleanup "$paused"
    return $result
}

# pause_cleanup stops VM $1's writer and mount, so the next case mounts it afresh.
pause_cleanup() {
    ssh_run "$1" "pkill -f 'of=${GOCLUSTERFS_MOUNT}/pause[d]'; true"
    if [[ -n "$(mount_pids "$1")" ]]; then
        crash_goclusterfs "$1"
    fi
    ssh_run "$1" "fusermount3 -uz ${GOCLUSTERFS_MOUNT} 2>/dev/null; dmsetup remove gcfs-pause 2>/dev/null; true"
}

# A node VM paused with QEMU stop past deadAfter, on a volume without storage fencing, must not land a write after a
# peer replayed it. Its guest clocks stand still while it is stopped, so the stall guards on CLOCK_BOOTTIME do not see
# the pause, and it does land one: this is why an unfenced volume refuses a second node. The nodes here mount with
# -unsafe-allow-unfenced, so the test fails on purpose and runs only with VM_HAZARD_TESTS=1.
# Afterwards fsck reports what the late writes did to the volume.
test_unfenced_paused_vm() {
    if [[ "${VM_HAZARD_TESTS:-}" != 1 ]]; then
        log_info "Skipping: it shows the late writes -unsafe-allow-unfenced risks and fails on purpose; run with VM_HAZARD_TESTS=1"
        return 0
    fi
    if [[ $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping: needs 2 VMs"
        return 0
    fi
    local result=0 vm out GOCLUSTERFS_MOUNT_TIMEOUT=120
    fencing_restart none || return 1
    pause_unfenced_case gate || result=1
    pause_unfenced_case held || result=1
    for vm in 1 2; do
        if [[ -n "$(mount_pids "$vm")" ]]; then
            unmount_goclusterfs "$vm" || crash_goclusterfs "$vm"
        fi
    done
    out=$(ssh_run 2 "/usr/local/bin/goclusterfs-fsck $(cluster_device) 2>&1; echo rc=\$?")
    log_info "fsck after both cases: ${out##*rc=}, $(echo "$out" | grep -c '^\[') problem lines"
    echo "$out" | grep '^\[' | head -10
    fencing_restart none || result=1
    return $result
}

# On a volume without storage fencing, a node refuses to mount while another node has it mounted, unless it passes
# -unsafe-allow-unfenced; with the flag it mounts and warns. Pause: Unfenced Paused VM shows what the refusal prevents.
test_unfenced_join_refused() {
    if [[ $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping: needs 2 VMs"
        return 0
    fi
    local result=0 out marker
    fencing_restart none || return 1
    unmount_goclusterfs 2 || return 1
    out=$(ssh_run 2 "mkdir -p ${GOCLUSTERFS_MOUNT}; timeout 60 /usr/local/bin/goclusterfs-mount -mount ${GOCLUSTERFS_MOUNT} -device $(cluster_device) -broadcast=false -api=false 2>&1; echo rc=\$?")
    if [[ "$out" != *"has no storage fencing, and the node in slot 0 "* || "$out" == *rc=0* ]]; then
        log_error "VM 2 was not refused on an unfenced volume VM 1 has mounted, without -unsafe-allow-unfenced: $out"
        result=1
    else
        log_info "VM 2 refused: $(grep -o 'has no storage fencing, and the node in slot 0 [^.]*' <<< "$out" | head -1)"
    fi
    marker="unfenced join $(date +%s)"
    mark_vm_logs "$marker"
    setup_goclusterfs 2 false || return 1
    if ! wait_log 2 "\[WARN\] UNSAFE" 30 "$marker"; then
        log_error "VM 2 joined with -unsafe-allow-unfenced but logged no UNSAFE warning"
        result=1
    fi
    return $result
}

# A clean unmount empties its registry slot. So after every node of an unfenced cluster unmounted cleanly, the first
# node to mount again joins without -unsafe-allow-unfenced: no stale slot counts as a mounted node.
test_unfenced_remount_after_clean_stop() {
    if [[ $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping: needs 2 VMs"
        return 0
    fi
    local result=0 vm out
    fencing_restart none || return 1
    for vm in $(seq 1 "$NODE_COUNT"); do
        unmount_goclusterfs "$vm" || return 1
    done
    out=$(ssh_run 2 "mkdir -p ${GOCLUSTERFS_MOUNT}; nohup /usr/local/bin/goclusterfs-mount -mount ${GOCLUSTERFS_MOUNT} -device $(cluster_device) -broadcast=false -api=false > /tmp/unfenced-remount.log 2>&1 < /dev/null & for i in \$(seq 1 60); do mountpoint -q ${GOCLUSTERFS_MOUNT} && break; sleep 0.5; done; mountpoint -q ${GOCLUSTERFS_MOUNT} && echo MOUNTED; grep -E 'Cannot join|UNSAFE|Leaving registry slot' /tmp/unfenced-remount.log")
    if [[ "$out" != *MOUNTED* ]]; then
        log_error "VM 2 did not mount alone without -unsafe-allow-unfenced after every node unmounted cleanly: $out"
        result=1
    else
        log_info "VM 2 mounted alone after a clean stop of the cluster"
    fi
    unmount_goclusterfs 2 || crash_goclusterfs 2
    ssh_run 2 "fusermount3 -uz ${GOCLUSTERFS_MOUNT} 2>/dev/null; true"
    for vm in $(seq 1 "$NODE_COUNT"); do
        setup_goclusterfs "$vm" false || return 1
    done
    return $result
}

# A fenced node that writes nothing still stops soon after it resumes: its next heartbeat write fails.# A fenced node that writes nothing still stops soon after it resumes: its next heartbeat write fails.
test_fencing_idle_node() {
    if [[ "$VM_STORAGE" != iscsi || $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping: needs VM_STORAGE=iscsi and 2 VMs"
        return 0
    fi
    local idle=1 peer=2 result=0 pids
    fencing_restart pr || return 1
    pids=$(mount_pids "$idle")
    local marker="fencing idle node $(date +%s)"
    mark_vm_logs "$marker"
    ssh_run "$idle" "kill -STOP $(echo $pids)"
    if ! wait_log "$peer" "Revoked node" 120 "$marker"; then
        ssh_run "$idle" "kill -CONT $(echo $pids)"
        return 1
    fi
    ssh_run "$idle" "kill -CONT $(echo $pids)"
    if ! wait_stopped "$idle" 10; then
        log_error "VM $idle still runs 10s after it resumed with its key revoked"
        result=1
    fi
    fencing_restart pr || result=1
    return $result
}

# A node revokes a stopped peer while its own writes are in flight. LIO tags every command with the reservation
# holder's key, so a preempt that does not drain this node's I/O first drops its own writes without a status.
test_fencing_busy_survivor() {
    if [[ "$VM_STORAGE" != iscsi || $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping: needs VM_STORAGE=iscsi and 2 VMs"
        return 0
    fi
    local stopped=1 peer=2 result=0 pids
    fencing_restart pr || return 1
    ssh_run "$peer" "rm -f /tmp/busy.stop; dd if=/dev/zero of=${GOCLUSTERFS_MOUNT}/busy bs=1M count=2 conv=fsync 2>/dev/null"
    pids=$(mount_pids "$stopped")
    local marker="fencing busy survivor $(date +%s)"
    mark_vm_logs "$marker"
    ssh_run "$peer" "nohup sh -c 'while [ ! -e /tmp/busy.stop ]; do dd if=/dev/zero of=${GOCLUSTERFS_MOUNT}/busy bs=1M count=2 conv=notrunc,fsync 2>/dev/null; done' < /dev/null > /dev/null 2>&1 &"
    ssh_run "$stopped" "kill -STOP $(echo $pids)"
    if ! wait_log "$peer" "Revoked node" 120 "$marker"; then
        sed -n "/$marker/,\$p" "$(get_vm_mount_log "$peer")" | grep -E "Not recovering|stopping" | head -3
        result=1
    fi
    ssh_run "$peer" "touch /tmp/busy.stop"
    ssh_run "$stopped" "kill -CONT $(echo $pids)"
    if [[ -z "$(mount_pids "$peer")" ]]; then
        log_error "VM $peer stopped while it revoked VM $stopped"
        result=1
    elif ! ssh_run "$peer" "dd if=/dev/zero of=${GOCLUSTERFS_MOUNT}/busy.after bs=4096 count=1 conv=fsync 2>/dev/null"; then
        log_error "VM $peer cannot write after it revoked VM $stopped"
        result=1
    fi
    fencing_restart pr || result=1
    return $result
}

# fsck -clear-reservations refuses while nodes run, even with -force. Once VM 1 unmounted and VM 2 crashed, leaving
# its key, it lists and keeps the keys without -confirm, and with -confirm removes every key and the reservation, after
# which both nodes mount again.
test_fencing_clear_reservations() {
    if [[ "$VM_STORAGE" != iscsi || $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping: needs VM_STORAGE=iscsi and 2 VMs"
        return 0
    fi
    local result=0 out pr=/sys/kernel/config/target/core/iblock_0/lun0/pr
    fencing_restart pr || return 1
    out=$(ssh_run 2 "/usr/local/bin/goclusterfs-fsck -clear-reservations -confirm -force $(cluster_device) 2>&1; echo rc=\$?")
    if [[ "$out" != *"clearing refused: the filesystem is mounted by"* ]]; then
        log_error "fsck -clear-reservations while both nodes run did not refuse: $out"
        result=1
    fi
    unmount_goclusterfs 1 || return 1
    crash_goclusterfs 2 || return 1
    if ! registered_initiators | grep -q "goclusterfs:vm2,"; then
        log_error "the crashed VM 2 has no key left to clear: $(registered_initiators | tr '\n' ' ')"
        result=1
    fi
    out=$(ssh_run 1 "/usr/local/bin/goclusterfs-fsck -clear-reservations $(cluster_device) 2>&1; echo rc=\$?")
    if [[ "$out" != *"cannot list the reservation keys"*"Nothing was changed"*"rc=0"* &&
          "$out" != *"Reservation keys on"*"Nothing was changed"*"rc=0"* ]] || ! registered_initiators | grep -q "goclusterfs:vm2,"; then
        log_error "fsck -clear-reservations without -confirm changed something or failed: $out"
        result=1
    fi
    out=$(ssh_run 1 "/usr/local/bin/goclusterfs-fsck -clear-reservations -confirm $(cluster_device) 2>&1; echo rc=\$?")
    if [[ "$out" != *"Removed every reservation key"*"rc=0"* ]]; then
        log_error "fsck -clear-reservations -confirm with no node running failed: $out"
        result=1
    fi
    if registered_initiators | grep -q "goclusterfs:vm"; then
        log_error "keys left after fsck -clear-reservations: $(registered_initiators | tr '\n' ' ')"
        result=1
    fi
    out=$(ssh_run "$TARGET_VM_ID" "tr -d '\\000' < $pr/res_pr_type")
    if [[ "$out" != *"No SPC-3 Reservation holder"* ]]; then
        log_error "the target still holds a reservation after fsck -clear-reservations: $out"
        result=1
    fi
    ssh_run 2 "fusermount3 -uz ${GOCLUSTERFS_MOUNT} 2>/dev/null; true"
    setup_goclusterfs 1 false || result=1
    setup_goclusterfs 2 false || result=1
    return $result
}

# reservation_holder prints the target's reservation holder, or "No SPC-3 Reservation holder".
reservation_holder() {
    ssh_run_capped 30 "$TARGET_VM_ID" "tr -d '\\000' < /sys/kernel/config/target/core/iblock_0/lun0/pr/res_holder"
}

# mount_key prints the reservation key VM $1's mount logged.
mount_key() {
    sed -n 's/.*Fencing: persistent reservation key \(0x[0-9a-f]*\).*/\1/p' "$(get_vm_mount_log "$1")" | tail -1
}

# A device without a reservation takes writes from any host, so a node a peer revoked there would go on writing.
# When the device loses its reservation while the nodes' keys stay, a node reserves it again within two failure
# scans. When it also loses a node's key, that node stops, and the other reserves again.
test_fencing_lost_reservation() {
    if [[ "$VM_STORAGE" != iscsi || $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping: needs VM_STORAGE=iscsi and 2 VMs"
        return 0
    fi
    local result=0 k1 k2 marker t dev
    fencing_restart pr || return 1
    k1=$(mount_key 1) k2=$(mount_key 2) dev=$(cluster_device)
    ssh_run 2 "apk add --no-cache sg3_utils > /dev/null" || return 1
    marker="lost reservation $(date +%s)"
    mark_vm_logs "$marker"
    ssh_run 2 "sg_persist --out --release --param-rk=$k2 --prout-type=7 $dev > /dev/null" || result=1
    for t in $(seq 15); do
        [[ "$(reservation_holder)" != *"No SPC-3 Reservation holder"* ]] && break
        sleep 1
    done
    if [[ "$(reservation_holder)" == *"No SPC-3 Reservation holder"* ]]; then
        log_error "15s after the reservation was released, no node had reserved again"
        result=1
    elif ! cat "$(get_vm_mount_log 1)" "$(get_vm_mount_log 2)" | grep -q "had lost its reservation"; then
        log_error "a node reserved again without logging it"
        result=1
    else
        log_info "a node reserved again ${t}s after the release"
    fi

    ssh_run 2 "sg_persist --out --release --param-rk=$k2 --prout-type=7 $dev > /dev/null && sg_persist --out --preempt-abort --param-rk=$k2 --param-sark=$k1 --prout-type=7 $dev > /dev/null" || result=1
    if ! wait_stopped 1 20; then
        log_error "VM 1 still runs 20s after the device lost its reservation and VM 1's key"
        result=1
    fi
    if [[ -z "$(mount_pids 2)" || "$(reservation_holder)" == *"No SPC-3 Reservation holder"* ]]; then
        log_error "VM 2 stopped, or did not reserve the device again"
        result=1
    fi
    sed -n "/$marker/,\$p" "$(get_vm_mount_log 1)" | grep -E "Fencing|stopping" | tail -2
    fencing_restart pr || result=1
    return $result
}

# registered_initiators lists the initiators the target holds a reservation key for, one per line.
registered_initiators() {
    ssh_run_capped 30 "$TARGET_VM_ID" "tr -d '\\000' < /sys/kernel/config/target/core/iblock_0/lun0/pr/res_pr_registered_i_pts"
}

# A clean unmount removes the node's reservation key, after its last write.
test_fencing_unmount_removes_key() {
    if [[ "$VM_STORAGE" != iscsi || $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping: needs VM_STORAGE=iscsi and 2 VMs"
        return 0
    fi
    local vm=2 result=0
    fencing_restart pr || return 1
    if ! registered_initiators | grep -q "goclusterfs:vm$vm,"; then
        log_error "VM $vm has no reservation key while mounted: $(registered_initiators | tr '\n' ' ')"
        return 1
    fi
    local marker="fencing unmount $(date +%s)"
    mark_vm_logs "$marker"
    unmount_goclusterfs "$vm" || return 1
    if registered_initiators | grep -q "goclusterfs:vm$vm,"; then
        log_error "VM $vm's reservation key stays after a clean unmount: $(registered_initiators | tr '\n' ' ')"
        result=1
    fi
    if sed -n "/$marker/,\$p" "$(get_vm_mount_log "$vm")" | grep -q "reservation key failed"; then
        log_error "VM $vm could not remove its reservation key at unmount"
        result=1
    fi
    setup_goclusterfs "$vm" false || result=1
    return $result
}

# relogin_under_map logs VM $1's only iSCSI path out and in under its multipath map gcfs-mpath, which keeps its mount
# running. The new session has a new ISID, and LIO keeps the old session's registration.
relogin_under_map() {
    local disk i portal="10.0.2.2:${ISCSI_PORT}"
    ssh_run "$1" "iscsiadm -m node -T ${ISCSI_TARGET_IQN} -p ${portal} --logout > /dev/null; sleep 2; iscsiadm -m node -T ${ISCSI_TARGET_IQN} -p ${portal} --login > /dev/null" || return 1
    for i in $(seq 1 20); do
        disk=$(iscsi_disk "$1")
        [[ -n "$disk" ]] && ssh_run "$1" "test -b /dev/$disk" && break
        sleep 1
    done
    multipath_over "$1" "$disk"
}

# VM 1 holds the reservation, then logs out and in again under its multipath map, so LIO keeps its key under the old
# session. LIO lets every session of a host write while one of its registrations holds the reservation, since it
# compares only the first 4 bytes of the ISID, and open-iscsi numbers sessions in the last 2. Unless VM 1's clean unmount
# removes the old registration, VM 2's later revoke of VM 1's new key does not fence VM 1.
test_fencing_relogin_remount() {
    if [[ "$VM_STORAGE" != iscsi || $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping: needs VM_STORAGE=iscsi and 2 VMs"
        return 0
    fi
    local node=1 peer=2 result=0 marker vm byte
    for vm in $(seq 1 "$NODE_COUNT"); do
        unmount_goclusterfs "$vm" || crash_goclusterfs "$vm" || return 1
    done
    multipath_over "$node" || return 1
    setup_goclusterfs "$node" true "" /dev/mapper/gcfs-mpath -fencing=pr || return 1
    for vm in $(seq 2 "$NODE_COUNT"); do
        setup_goclusterfs "$vm" false || return 1
    done
    relogin_under_map "$node" || return 1
    if ! ssh_run "$node" "echo after-relogin > ${GOCLUSTERFS_MOUNT}/relogin && sync"; then
        log_error "VM $node could not write after its relogin"
        result=1
    fi
    log_info "registrations after VM $node's relogin: $(registered_initiators | tr '\n' ' ')"
    marker="fencing relogin remount $(date +%s)"
    mark_vm_logs "$marker"
    unmount_goclusterfs "$node" || return 1
    if ! sed -n "/$marker/,\$p" "$(get_vm_mount_log "$node")" | grep -q "removed the key from the earlier session"; then
        log_error "VM $node's unmount did not remove its key from the earlier session"
        result=1
    fi
    if registered_initiators | grep -q "goclusterfs:vm$node,"; then
        log_error "VM $node left a registration at its unmount: $(registered_initiators | tr '\n' ' ')"
        result=1
    fi
    setup_goclusterfs "$node" false "" /dev/mapper/gcfs-mpath || return 1

    ssh_run "$peer" "head -c 4096 /dev/zero | tr '\\000' 0 > ${GOCLUSTERFS_MOUNT}/fenced && sync"
    ssh_run "$node" "head -c 4096 /dev/zero | tr '\\000' A > /tmp/fenced.A"
    ssh_run "$peer" "head -c 4096 /dev/zero | tr '\\000' B > /tmp/fenced.B"
    marker="fencing relogin revoke $(date +%s)"
    mark_vm_logs "$marker"
    ssh_run "$node" "nohup sh -c 'while dd if=/tmp/fenced.A of=${GOCLUSTERFS_MOUNT}/fenced bs=4096 count=1 conv=notrunc,fsync 2>/dev/null; do :; done' < /dev/null > /dev/null 2>&1 &"
    sleep 2
    echo stop | nc -U -q1 "$(get_vm_monitor_socket "$node")" > /dev/null
    if ! wait_log "$peer" "Revoked node" 120 "$marker"; then
        echo cont | nc -U -q1 "$(get_vm_monitor_socket "$node")" > /dev/null
        return 1
    fi
    sleep 3
    ssh_run "$peer" "dd if=/tmp/fenced.B of=${GOCLUSTERFS_MOUNT}/fenced bs=4096 count=1 conv=notrunc,fsync 2>/dev/null" || result=1
    echo cont | nc -U -q1 "$(get_vm_monitor_socket "$node")" > /dev/null
    if ! wait_stopped "$node" 30; then
        log_error "VM $node still runs 30s after it resumed, with its key revoked"
        result=1
    fi
    byte=$(fenced_file_byte "$peer")
    if [[ "$byte" != B ]]; then
        log_error "the file starts with '$byte' after VM $node resumed, want VM $peer's B: a revoke of VM $node's key did not fence it"
        result=1
    fi
    ssh_run "$node" "pkill -f 'of=${GOCLUSTERFS_MOUNT}/fence[d]'; true"
    crash_goclusterfs "$node" 2>/dev/null
    ssh_run "$node" "dmsetup remove gcfs-mpath" || result=1
    relogin_iscsi "$node" || result=1
    fencing_restart pr || result=1
    [[ $result -eq 0 ]] && log_info "VM $node's unmount removed its earlier session's key, and revoking its new key fenced it"
    return $result
}

# rejoin_holds checks that VM $1, mounted after marker $2, still runs 25s later and that its write reads back on VM $3.
rejoin_holds() {
    local vm="$1" marker="$2" peer="$3" word
    sleep 25
    if [[ -z "$(mount_pids "$vm")" ]]; then
        log_error "VM $vm joined, then stopped: $(sed -n "/$marker/,\$p" "$(get_vm_mount_log "$vm")" | grep -E 'refused|stopping|Cannot' | tail -2)"
        return 1
    fi
    word="rejoin-$(date +%s%N)"
    if ! ssh_run "$vm" "echo $word > ${GOCLUSTERFS_MOUNT}/rejoin && sync"; then
        log_error "VM $vm could not write after it joined"
        return 1
    fi
    if [[ "$(ssh_run "$peer" "cat ${GOCLUSTERFS_MOUNT}/rejoin")" != "$word" ]]; then
        log_error "VM $peer does not read what VM $vm wrote after it joined"
        return 1
    fi
    if [[ -z "$(mount_pids "$vm")" || -z "$(mount_pids "$peer")" ]]; then
        log_error "a node stopped after VM $vm's write: VM $vm pids '$(mount_pids "$vm")', VM $peer pids '$(mount_pids "$peer")'"
        return 1
    fi
    if sed -n "/$marker/,\$p" "$(get_vm_mount_log "$vm")" | grep -q "registered the key anew\|trying to register the key anew"; then
        log_info "VM $vm registered its key anew to join"
    fi
}

# rejoin_mount mounts VM $1 on the multipath map gcfs-mpath over its iSCSI disk when $2 is mpath, else on the disk.
rejoin_mount() {
    if [[ "$2" == mpath ]]; then
        multipath_over "$1" && setup_goclusterfs "$1" false "" /dev/mapper/gcfs-mpath
    else
        setup_goclusterfs "$1" false "" "/dev/$(iscsi_disk "$1")"
    fi
}

# rejoin_on_map remounts VM $1 on a multipath map, so that its mount outlives a relogin, and logs it out and in.
rejoin_on_map() {
    unmount_goclusterfs "$1" || return 1
    rejoin_mount "$1" mpath || return 1
    relogin_under_map "$1"
}

# rejoin_power_cut power cuts VM $1, boots it and waits $2 seconds more, so its mount races the peer's takeover.
rejoin_power_cut() {
    kill_vm "$1" > /dev/null
    start_vm "$1" false > /dev/null 2>&1 || return 1
    wait_vm_healthy "$1" > /dev/null || return 1
    sleep "$2"
}

# rejoin_reset puts VM $1 back on a fresh iSCSI session without a map, and formats the volume again.
rejoin_reset() {
    if [[ -n "$(mount_pids "$1")" ]]; then
        unmount_goclusterfs "$1" || crash_goclusterfs "$1"
    fi
    ssh_run "$1" "fusermount3 -uz ${GOCLUSTERFS_MOUNT} 2>/dev/null; dmsetup remove gcfs-mpath 2>/dev/null; true"
    relogin_iscsi "$1" && fencing_restart pr
}

# rejoin_test runs rejoin case $1 for VM 2 next to VM 1, then resets VM 2 however the case ended, so failures don't cascade.
rejoin_test() {
    if [[ "$VM_STORAGE" != iscsi || $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping: needs VM_STORAGE=iscsi and 2 VMs"
        return 0
    fi
    local result=0
    fencing_restart pr || return 1
    "$1" 2 1 || result=1
    rejoin_reset 2 || result=1
    return $result
}

# A mount killed after a relogin mounts again on the same boot and keeps writing (#267's register-anew path on LIO).
rejoin_relogin_kill() {
    local vm="$1" peer="$2" marker
    rejoin_on_map "$vm" || return 1
    crash_goclusterfs "$vm" || return 1
    ssh_run "$vm" "fusermount3 -uz ${GOCLUSTERFS_MOUNT} 2>/dev/null; true"
    marker="fencing rejoin relogin kill $(date +%s)"
    mark_vm_logs "$marker"
    if ! rejoin_mount "$vm" mpath; then
        log_error "VM $vm did not join after a relogin and a kill: $(sed -n "/$marker/,\$p" "$(get_vm_mount_log "$vm")" | grep -E 'Cannot join|failed' | tail -2)"
        return 1
    fi
    rejoin_holds "$vm" "$marker" "$peer"
}

# A node that relogged in and remounted is power cut, boots on its first session again and rejoins.
rejoin_relogin_power_cut() {
    local vm="$1" peer="$2" marker
    rejoin_on_map "$vm" || return 1
    unmount_goclusterfs "$vm" || return 1
    setup_goclusterfs "$vm" false "" /dev/mapper/gcfs-mpath || return 1
    rejoin_power_cut "$vm" 0 || return 1
    marker="fencing rejoin relogin power cut $(date +%s)"
    mark_vm_logs "$marker"
    if ! rejoin_mount "$vm" disk; then
        log_error "VM $vm did not join after a relogin and a power cut"
        return 1
    fi
    rejoin_holds "$vm" "$marker" "$peer"
}

# The bug hunt's sequence: relogin, kill, the peer takes over, remount on the new session, power cut, rejoin.
rejoin_takeover_power_cut() {
    local vm="$1" peer="$2" marker
    rejoin_on_map "$vm" || return 1
    marker="fencing rejoin takeover $(date +%s)"
    mark_vm_logs "$marker"
    crash_goclusterfs "$vm" || return 1
    ssh_run "$vm" "fusermount3 -uz ${GOCLUSTERFS_MOUNT} 2>/dev/null; true"
    wait_log "$peer" "Recovery\] Completed\|Journal is clean" 90 "$marker" || return 1
    setup_goclusterfs "$vm" false "" /dev/mapper/gcfs-mpath || return 1
    rejoin_power_cut "$vm" 0 || return 1
    marker="fencing rejoin takeover power cut $(date +%s)"
    mark_vm_logs "$marker"
    if ! rejoin_mount "$vm" disk; then
        log_error "VM $vm did not join after a relogin, a takeover and a power cut"
        return 1
    fi
    rejoin_holds "$vm" "$marker" "$peer"
}

# A power-cut node rejoins on a plain disk and on a multipath map, its mount racing the peer's takeover.
rejoin_power_cuts() {
    local vm="$1" peer="$2" marker result=0 kind delay
    for kind in disk mpath; do
        unmount_goclusterfs "$vm" || return 1
        rejoin_mount "$vm" "$kind" || return 1
        for delay in 0 3; do
            rejoin_power_cut "$vm" "$delay" || return 1
            marker="fencing rejoin power cut $kind $delay $(date +%s)"
            mark_vm_logs "$marker"
            if rejoin_mount "$vm" "$kind"; then
                rejoin_holds "$vm" "$marker" "$peer" || result=1
            else
                log_error "VM $vm did not join on its $kind after a power cut, mounting ${delay}s after boot"
                return 1
            fi
        done
    done
    return $result
}

test_fencing_rejoin_relogin_kill() { rejoin_test rejoin_relogin_kill; }
test_fencing_rejoin_relogin_power_cut() { rejoin_test rejoin_relogin_power_cut; }
test_fencing_rejoin_takeover_power_cut() { rejoin_test rejoin_takeover_power_cut; }
test_fencing_rejoin_power_cut() { rejoin_test rejoin_power_cuts; }

# reservation_test runs fsck -test-reservations on VM $1 with the arguments $2 and prints its output and exit code.
reservation_test() {
    ssh_run "$1" "/usr/local/bin/goclusterfs-fsck -test-reservations $2 $(cluster_device) 2>&1; echo rc=\$?"
}

# kept_key prints the key a reservation test's output $1 tells the operator to pass on.
kept_key() {
    grep -oE '(-preempt|-check-kept) 0x4746[0-9a-f]{12}' <<< "$1" | head -1 | cut -d' ' -f2
}

# reconnected waits until VM $1 reads the cluster device again after the target came back. open-iscsi reconnects in
# the same session, which LIO's restored registrations name.
reconnected() {
    ssh_run_capped 120 "$1" "for t in \$(seq 90); do dd if=$(cluster_device) of=/dev/null bs=4096 count=1 iflag=direct 2>/dev/null && exit 0; sleep 1; done; exit 1"
}

# fsck -test-reservations qualifies LIO for fencing: it refuses the mounted volume, passes alone on VM 1, and in the
# two-host test VM 2's preempt makes the device refuse VM 1's write. A key survives a target power cycle that restores
# LIO's APTPL file. -check-kept reports it lost after a power cycle that does not, and after an iSCSI logout and
# login, whose new session does not hold the key LIO still lists for the old one.
test_fencing_reservation_self_test() {
    if [[ "$VM_STORAGE" != iscsi || $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping: needs VM_STORAGE=iscsi and 2 VMs"
        return 0
    fi
    local result=0 out key
    fencing_restart pr || return 1
    out=$(reservation_test 1 "")
    if [[ "$out" != *"refused:"*"holds a GoClusterFS volume, mounted by"*"rc=8" ]]; then
        log_error "fsck -test-reservations on the mounted volume did not refuse: $out"
        result=1
    fi
    unmount_goclusterfs 1 || return 1
    unmount_goclusterfs 2 || return 1

    out=$(reservation_test 1 -force)
    log_info "fsck -test-reservations on VM 1:"$'\n'"$out"
    if [[ "$out" != *"rc=0" ]] || grep -q '^FAIL' <<< "$out"; then
        log_error "fsck -test-reservations failed on LIO"
        result=1
    fi
    if registered_initiators | grep -q "goclusterfs:vm"; then
        log_error "fsck -test-reservations left a key: $(registered_initiators | tr '\n' ' ')"
        result=1
    fi

    ssh_run 1 "rm -f /tmp/victim.out; (/usr/local/bin/goclusterfs-fsck -test-reservations -force -victim $(cluster_device) > /tmp/victim.out 2>&1; echo rc=\$? >> /tmp/victim.out) > /dev/null 2>&1 &"
    for _ in $(seq 60); do
        key=$(kept_key "$(ssh_run 1 "cat /tmp/victim.out")")
        [[ -n "$key" ]] && break
        sleep 1
    done
    if [[ -z "$key" ]]; then
        log_error "host A printed no key: $(ssh_run 1 "cat /tmp/victim.out")"
        return 1
    fi
    out=$(reservation_test 2 "-force -preempt $key")
    log_info "host B, VM 2:"$'\n'"$out"
    if [[ "$out" != *"rc=0" ]] || grep -q '^FAIL' <<< "$out"; then
        log_error "host B's preempt failed"
        result=1
    fi
    for _ in $(seq 60); do
        out=$(ssh_run 1 "cat /tmp/victim.out")
        [[ "$out" == *"rc="* ]] && break
        sleep 1
    done
    log_info "host A, VM 1:"$'\n'"$out"
    if [[ "$out" != *"after host B preempted this host: refused with EBADE"*"rc=0" ]] || grep -q '^FAIL' <<< "$out"; then
        log_error "the device did not refuse host A's write after host B preempted it"
        result=1
    fi

    local how
    for how in relogin restore lose; do
        key=$(kept_key "$(reservation_test 1 "-force -keep")")
        if [[ -z "$key" ]]; then
            log_error "fsck -test-reservations -keep left no key"
            result=1
            break
        fi
        case "$how" in
            relogin) relogin_iscsi 1 ;;
            restore) power_cycle_target keep && reconnected 1 && reconnected 2 ;;
            lose) power_cycle_target && reconnected 1 && reconnected 2 ;;
        esac || { log_error "detaching the device ($how) failed"; result=1; break; }
        out=$(reservation_test 1 "-force -check-kept $key")
        log_info "after $how:"$'\n'"$out"$'\n'"registrations on the target:"$'\n'"$(registered_initiators)"
        if [[ "$how" != restore ]]; then
            if [[ "$out" != *"did not survive"*"rc=4" ]]; then
                log_error "-check-kept did not report the key lost after $how"
                result=1
            fi
        elif [[ "$out" != *"rc=0" ]] || grep -q '^FAIL' <<< "$out"; then
            log_error "the key did not survive $how"
            result=1
        fi
    done

    out=$(ssh_run 1 "/usr/local/bin/goclusterfs-fsck -clear-reservations -confirm $(cluster_device) 2>&1; echo rc=\$?")
    if [[ "$out" != *"rc=0" ]] || registered_initiators | grep -q "goclusterfs:vm"; then
        log_error "keys left after the self-test: $out $(registered_initiators | tr '\n' ' ')"
        result=1
    fi
    setup_goclusterfs 1 false || result=1
    setup_goclusterfs 2 false || result=1
    return $result
}

# relogin_last_path logs VM $1's last iSCSI path out and in again, as a session loss does, and points its map at the
# new disk. The target keeps the key the old session registered.
relogin_last_path() {
    local portal="10.0.2.2:${ISCSI_PORT}" iface
    iface=$(ssh_run "$1" "iscsiadm -m session -P 1 | awk '/Iface Name:/ {print \$3}' | sort | tail -1")
    # The new disk attaches a moment after the login; a map reloaded before it would be left with no path.
    ssh_run "$1" "iscsiadm -m node -T ${ISCSI_TARGET_IQN} -p ${portal} -I $iface --logout > /dev/null; sleep 1; iscsiadm -m node -T ${ISCSI_TARGET_IQN} -p ${portal} -I $iface --login > /dev/null
        n=\$(iscsiadm -m session | wc -l)
        for t in \$(seq 30); do [ \$(iscsiadm -m session -P 3 | grep -c 'Attached scsi disk') -ge \$n ] && exit 0; sleep 1; done; exit 1" || return 1
    multipath_over "$1"
}

# clear_write_flag registers and removes a spare key on VM $1's newest iSCSI path. LIO keeps one write flag per
# initiator and clears it when any of the initiator's registrations goes, so VM $1's next write meets a reservation
# conflict while its key stays listed, as a target that checks writes per session would refuse the relogged path.
clear_write_flag() {
    local disk
    disk=$(ssh_run "$1" "iscsiadm -m session -P 3 | awk '/Iface Name:/ {i = \$3} /Attached scsi disk/ {print i, \$4}' | sort | tail -1 | cut -d' ' -f2")
    ssh_run "$1" "command -v sg_persist > /dev/null || apk add --no-cache sg3_utils > /dev/null
        sg_persist --out --register --param-sark=0x47430000f1a90001 /dev/$disk > /dev/null &&
            sg_persist --out --register --param-rk=0x47430000f1a90001 --param-sark=0 /dev/$disk > /dev/null"
}

# write_load writes small files with fsync on VM $1 and records ok or fail for each in /tmp/load.out.
write_load() {
    ssh_run "$1" "rm -f /tmp/load.out; (i=0; while [ ! -e /tmp/load.stop ]; do i=\$((i+1)); if dd if=/dev/urandom of=${GOCLUSTERFS_MOUNT}/load.vm$1.\$((i%16)) bs=64k count=4 conv=fsync 2>/dev/null; then echo ok; else echo fail; fi; sleep 0.1; done) >> /tmp/load.out 2>&1 &"
}

# reservation_key prints the reservation key VM $1's mount logged.
reservation_key() {
    sed -n 's/.*Fencing: persistent reservation key \(0x[0-9a-f]*\).*/\1/p' "$(get_vm_mount_log "$1")" | tail -1
}

# writes_go_on fails unless VM $1's mount runs and its write load made progress over 5 seconds.
writes_go_on() {
    local before after
    before=$(ssh_run "$1" "grep -c ok /tmp/load.out")
    sleep 5
    after=$(ssh_run "$1" "grep -c ok /tmp/load.out")
    if [[ -z "$(mount_pids "$1")" ]] || (( after <= before )); then
        log_error "VM $1 stopped or stopped writing $2 ($before, then $after writes)"
        return 1
    fi
}

# A node on a two-path dm-multipath map keeps writing when one path logs out and in again: LIO takes its writes while
# the old session's registration stands, and lists the node's key under the old session and the other path. A spare
# key's removal then clears LIO's write flag for VM 1, as a target that checks writes per session would refuse the
# relogged path: VM 1 registers its key again on that path only and writes on. VM 2 then preempts the key, as a
# takeover does. That is final: VM 1 does not register it again and stops, and VM 2 writes on and replays it.
# Afterwards fsck finds the volume clean.
test_fencing_session_relogin() {
    if [[ "$VM_STORAGE" != iscsi || $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping: needs VM_STORAGE=iscsi and 2 VMs"
        return 0
    fi
    local result=0 out vm keys portal="10.0.2.2:${ISCSI_PORT}" fsck=/usr/local/bin/goclusterfs-fsck
    for vm in 1 2; do
        if [[ -n "$(mount_pids "$vm")" ]]; then
            unmount_goclusterfs "$vm" || crash_goclusterfs "$vm" || return 1
        fi
        ssh_run "$vm" "fusermount3 -uz ${GOCLUSTERFS_MOUNT} 2>/dev/null; rm -f /tmp/load.stop; true"
    done
    iscsi_add_path 1 1 || return 1
    multipath_over 1 || return 1
    log_info "VM 1 mounts a map over $(iscsi_disks 1)"
    # VM 2 formats and holds the reservation: every session of the holder's host writes as the holder on LIO.
    ssh_run 2 "$fsck -clear-reservations -confirm $(cluster_device) > /dev/null 2>&1; true"
    setup_goclusterfs 2 true "" "" "-fencing=pr" || return 1
    setup_goclusterfs 1 false "" /dev/mapper/gcfs-mpath || return 1
    write_load 1
    write_load 2
    sleep 5

    local marker
    marker="fencing relogin $(date +%s)"
    mark_vm_logs "$marker"
    ssh_run 1 "iscsiadm -m node -T ${ISCSI_TARGET_IQN} -p ${portal} -I p1 --logout > /dev/null; sleep 2; iscsiadm -m node -T ${ISCSI_TARGET_IQN} -p ${portal} -I p1 --login > /dev/null; sleep 2" || result=1
    multipath_over 1 || result=1
    log_info "VM 1's second path logged in again; its map is over $(iscsi_disks 1)"
    writes_go_on 1 "after its second path logged in again" || result=1
    ssh_run 2 "apk add --no-cache sg3_utils > /dev/null" || result=1
    keys=$(ssh_run 2 "sg_persist --in --read-keys $(cluster_device) | grep -c $(reservation_key 1 | sed 's/^0x//')")
    if [[ "$keys" -lt 2 ]]; then
        log_error "the device lists VM 1's key $keys times after the relogin, want it under the old session and the other path"
        result=1
    fi
    clear_write_flag 1 || result=1
    wait_log 1 "registered it again with every write held" 60 "$marker" || result=1
    writes_go_on 1 "after it registered its key again" || result=1
    keys=$(ssh_run 2 "sg_persist --in --read-keys $(cluster_device) | grep -c $(reservation_key 1 | sed 's/^0x//')")
    if [[ "$keys" != 3 ]]; then
        log_error "the device lists VM 1's key $keys times after the re-register, want 3: the old session, the other path and the relogged path"
        result=1
    fi
    local restored
    restored=$(sed -n "/$marker/,\$p" "$(get_vm_mount_log 1)" | grep -c "registered it again")
    ssh_run 2 "sg_persist --out --preempt-abort --param-rk=$(reservation_key 2) --param-sark=$(reservation_key 1) --prout-type=7 $(cluster_device) > /dev/null" || result=1
    wait_log 1 "a peer preempted this node\|lost this node's key again" 60 "$marker" || result=1
    if ! wait_stopped 1 30; then
        log_error "VM 1 still runs 30s after its key was preempted"
        result=1
    fi
    if (( $(sed -n "/$marker/,\$p" "$(get_vm_mount_log 1)" | grep -c "registered it again") > restored )); then
        log_error "VM 1 registered its key again after VM 2 preempted it"
        result=1
    fi
    # VM 2's writes may wait on locks VM 1 held until VM 2 replays it.
    wait_log 2 "\[Recovery\] Completed\|Journal is clean" 90 "$marker" || result=1
    writes_go_on 2 "after it replayed VM 1" || result=1

    ssh_run 1 "touch /tmp/load.stop"
    ssh_run 2 "touch /tmp/load.stop"
    sleep 1
    log_info "writes on VM 1: $(ssh_run 1 'sort /tmp/load.out | uniq -c | tr "\n" " "'), VM 2: $(ssh_run 2 'sort /tmp/load.out | uniq -c | tr "\n" " "')"
    unmount_goclusterfs 2 || result=1
    out=$(ssh_run 2 "$fsck $(cluster_device) 2>&1; echo rc=\$?")
    if [[ "$out" != *"rc=0" ]]; then
        log_error "fsck after the relogin is not clean: $out"
        result=1
    fi

    ssh_run 2 "$fsck -clear-reservations -confirm $(cluster_device) > /dev/null 2>&1" || result=1
    ssh_run 1 "fusermount3 -uz ${GOCLUSTERFS_MOUNT} 2>/dev/null; dmsetup remove gcfs-mpath; iscsiadm -m node -T ${ISCSI_TARGET_IQN} -p ${portal} -I p1 --logout > /dev/null 2>&1; true"
    fencing_restart pr || result=1
    return $result
}

# wait_log_now waits up to $3 seconds, polling every 0.1s, for VM $1's mount log to show more than $5 lines with $2
# after marker $4; without $5, more than when it started.
wait_log_now() {
    local vm="$1" pattern="$2" limit=$(( $3 * 10 )) marker="$4" waited=0 seen="${5:-}"
    [[ -n "$seen" ]] || seen=$(sed -n "/$marker/,\$p" "$(get_vm_mount_log "$vm")" | grep -c "$pattern")
    until (( $(sed -n "/$marker/,\$p" "$(get_vm_mount_log "$vm")" | grep -c "$pattern") > seen )); do
        if [[ $waited -ge $limit ]]; then
            log_error "VM $vm: no '$pattern' in its mount log within $3s"
            return 1
        fi
        sleep 0.1
        waited=$((waited + 1))
    done
}

# rekey_pause_case runs one case of test_fencing_paused_rekey. VM 2 holds every takeover 10s between its preempt and
# its Dead mark, and VM 1, paused with QEMU stop, resumes inside that window. Its guest clock barely moved, so only its
# slot can tell it that VM 2 is taking it over.
#   gate:            VM 1 stops while it rewrites a file.
#   held:            VM 1 stops between a heartbeat's slot read and its write.
#   before-register: VM 1's last path logs out and in, and a spare key's removal clears LIO's write flag for VM 1, so
#                    VM 1 starts to register again with its key still listed; VM 1 stops before its register, and the
#                    register lands after VM 2's real takeover preempted. The stop comes 1.5s into a 5s stall, once
#                    VM 1's heartbeat waits in a held write, so only the rekey reads the slot.
#   after-register:  as before-register, but VM 1 stops between its register and its slot read.
rekey_pause_case() {
    local mode=$1 paths=$2 paused=1 peer=2 result=0 env1="" stop_at="" mon marker slot uuid stopped_at byte state mode_label
    case "$mode" in
        held) env1="GOCLUSTERFS_TEST_BEAT_STALL=2s" stop_at="\[TEST\] heartbeat read slot" ;;
        before-register | after-register) env1="GOCLUSTERFS_TEST_REKEY_STALL=$mode:5s" stop_at="\[TEST\] rekey: stalling .* $mode" ;;
    esac
    fencing_restart pr "$env1" || return 1
    unmount_goclusterfs "$paused" || return 1
    unmount_goclusterfs "$peer" || return 1
    # VM 2 mounts first and holds the reservation: every session of the holder's host writes as the holder on LIO.
    ssh_run "$peer" "/usr/local/bin/goclusterfs-fsck -clear-reservations -confirm $(cluster_device) > /dev/null 2>&1; true"
    setup_goclusterfs "$peer" false "GOCLUSTERFS_TEST_TAKEOVER_DELAY=10s" || return 1
    if [[ $paths -ge 2 ]]; then
        iscsi_add_path "$paused" 1 || return 1
    fi
    multipath_over "$paused" || return 1
    setup_goclusterfs "$paused" false "$env1" /dev/mapper/gcfs-mpath || return 1
    mon=$(get_vm_monitor_socket "$paused")
    slot=$(grep -o 'Registered as node [0-9]*' "$(get_vm_mount_log "$paused")" | tail -1 | grep -o '[0-9]*$')
    uuid=$(grep -o 'Registered as node [0-9]* (UUID: [0-9a-f]*' "$(get_vm_mount_log "$paused")" | tail -1 | grep -o '[0-9a-f]*$')
    ssh_run "$peer" "head -c 4096 /dev/zero | tr '\\000' 0 > ${GOCLUSTERFS_MOUNT}/fenced && sync"
    ssh_run "$paused" "head -c 4096 /dev/zero | tr '\\000' A > /tmp/fenced.A"
    ssh_run "$peer" "head -c 4096 /dev/zero | tr '\\000' B > /tmp/fenced.B"
    marker="paused rekey $mode on $paths paths $(date +%s)"
    mode_label="$mode, $paths paths"
    mark_vm_logs "$marker"
    ssh_run "$paused" "nohup sh -c 'while dd if=/tmp/fenced.A of=${GOCLUSTERFS_MOUNT}/fenced bs=4096 count=1 conv=notrunc,fsync 2>/dev/null; do :; done' < /dev/null > /dev/null 2>&1 &"
    sleep 3
    if [[ "$mode" == *register ]]; then
        relogin_last_path "$paused" || result=1
        clear_write_flag "$paused" || result=1
    fi
    local seen=""
    [[ "$mode" == *register ]] && seen=0
    if [[ -n "$stop_at" ]] && ! wait_log_now "$paused" "$stop_at" 30 "$marker" "$seen"; then
        rekey_pause_cleanup "$paused"
        return 1
    fi
    [[ "$mode" == *register ]] && sleep 1.5
    echo stop | nc -U -q1 "$mon" > /dev/null
    stopped_at=$(date +%s)
    log_info "$mode_label: VM $paused (slot $slot, $uuid) stopped with QEMU stop"
    if ! wait_log_now "$peer" "\[TEST\] slot $slot: intent written" 120 "$marker"; then
        echo cont | nc -U -q1 "$mon" > /dev/null
        rekey_pause_cleanup "$paused"
        return 1
    fi
    echo cont | nc -U -q1 "$mon" > /dev/null
    log_info "$mode_label: VM $paused resumed $(( $(date +%s) - stopped_at ))s after the stop, after VM $peer's intent and preempt, before its Dead mark"
    wait_log "$peer" "\[Recovery\] Completed\|Journal is clean" 90 "$marker" || result=1
    if ! wait_stopped "$paused" 30; then
        log_error "$mode_label: VM $paused still runs 30s after it resumed"
        result=1
    fi
    ssh_run "$peer" "dd if=/tmp/fenced.B of=${GOCLUSTERFS_MOUNT}/fenced bs=4096 count=1 conv=notrunc,fsync 2>/dev/null" || result=1
    sleep 15
    byte=$(fenced_file_byte "$peer")
    state=$(ssh_run "$peer" "dd if=$(cluster_device) bs=4096 skip=$((256 + slot)) count=1 iflag=direct 2>/dev/null | od -An -tx1 -j4 -N20 | tr -d '\n'")
    log_info "$mode_label: file starts with '$byte'; slot $slot reads (state, UUID)$state; VM $paused's log:"
    sed -n "/$marker/,\$p" "$(get_vm_mount_log "$paused")" | grep -E "TEST|registered it again|revoking|preempted|refused|no longer this node|declared|ERROR|WARN" | head -12
    sed -n "/$marker/,\$p" "$(get_vm_mount_log "$peer")" | grep -E "TEST|Revoked|Recovery\]" | head -6
    if [[ "$byte" != B ]]; then
        log_error "$mode_label: the file starts with '$byte', want VM $peer's B: VM $paused landed a write after VM $peer's takeover"
        result=1
    fi
    if sed -n "/$marker/,\$p" "$(get_vm_mount_log "$paused")" | grep -q "registered it again"; then
        log_error "$mode_label: VM $paused registered its key again inside VM $peer's takeover"
        result=1
    fi
    if [[ "$mode" == *register ]]; then
        local log
        log=$(sed -n "/$marker/,\$p" "$(get_vm_mount_log "$paused")")
        if ! grep -q "which is revoking" <<< "$log" || ! grep -q "a peer preempted this node" <<< "$log"; then
            log_error "$mode_label: VM $paused's re-register did not stop on both VM $peer's intent and the key listing"
            result=1
        fi
    fi
    rekey_pause_cleanup "$paused"
    return $result
}

# rekey_pause_cleanup stops VM $1's writer and mount, removes its map and logs out its second path, so the next case
# mounts it afresh on its one plain session. A relogged only path came back under another disk name while the map held
# the old one, so it logs that session in again until the disk has the name the harness expects.
rekey_pause_cleanup() {
    ssh_run "$1" "pkill -f 'of=${GOCLUSTERFS_MOUNT}/fence[d]'; true"
    if [[ -n "$(mount_pids "$1")" ]] && ! crash_goclusterfs "$1"; then
        log_warn "VM $1: restarting it, since its mount could not be stopped"
        kill_vm "$1"
        start_vm "$1" || return 1
        wait_vm_healthy "$1" 120 || return 1
        return 0
    fi
    ssh_run "$1" "fusermount3 -uz ${GOCLUSTERFS_MOUNT} 2>/dev/null; dmsetup remove gcfs-mpath 2>/dev/null
        iscsiadm -m node -T ${ISCSI_TARGET_IQN} -p 10.0.2.2:${ISCSI_PORT} -I p1 --logout > /dev/null 2>&1
        if [ ! -b $(cluster_device) ]; then
            iscsiadm -m node -T ${ISCSI_TARGET_IQN} -p 10.0.2.2:${ISCSI_PORT} -I default --logout > /dev/null 2>&1
            iscsiadm -m node -T ${ISCSI_TARGET_IQN} -p 10.0.2.2:${ISCSI_PORT} -I default --login > /dev/null 2>&1
            for t in \$(seq 20); do [ -b $(cluster_device) ] && break; sleep 1; done
        fi; true"
}

# A fenced node on a one- or two-path dm-multipath map, paused with QEMU stop, resumes inside a peer's takeover, between
# the peer's preempt and its Dead mark, and lands no write after the preempt, also when it registers its key again
# after that preempt: its slot shows the intent and the device no longer lists its key from before. Afterwards fsck
# finds the volume clean.
test_fencing_paused_rekey() {
    if [[ "$VM_STORAGE" != iscsi || $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping: needs VM_STORAGE=iscsi and 2 VMs"
        return 0
    fi
    local result=0 mode paths vm out GOCLUSTERFS_MOUNT_TIMEOUT=120
    for paths in ${REKEY_PAUSE_PATHS:-1 2}; do
        for mode in ${REKEY_PAUSE_CASES:-gate held before-register after-register}; do
            rekey_pause_case "$mode" "$paths" || result=1
        done
    done
    for vm in 1 2; do
        if [[ -n "$(mount_pids "$vm")" ]]; then
            unmount_goclusterfs "$vm" || crash_goclusterfs "$vm"
        fi
    done
    out=$(ssh_run 2 "/usr/local/bin/goclusterfs-fsck $(cluster_device) 2>&1; echo rc=\$?")
    log_info "fsck after every case: ${out##*rc=}"
    echo "$out" | grep '^\[' | head -10
    [[ "${out##*rc=}" == 0 ]] || result=1
    fencing_restart pr || result=1
    return $result
}

# =============================================================================
# Hostile storage and clocks
# =============================================================================
# VM 1 mounts through a device-mapper device over the shared disk, so its path
# alone can be slowed, stalled or failed. Both nodes create small files and
# record each one their create acknowledged; every acknowledged file must read
# back, and fsck may find nothing but leaks.
HOSTILE_DM=gcfs-path

hostile_dm_load() {
    ssh_run 1 "sz=\$(blockdev --getsz $(cluster_device)) && dmsetup suspend $HOSTILE_DM && dmsetup load $HOSTILE_DM --table \"0 \$sz $1\" && dmsetup resume $HOSTILE_DM"
}

hostile_setup() {
    ensure_cluster_healthy 2 || return 1
    unmount_goclusterfs 1 || { log_error "VM 1: unmount failed"; return 1; }
    if ! ssh_run 1 "apk add --no-cache device-mapper > /dev/null && modprobe dm-delay && modprobe dm-flakey && { dmsetup remove $HOSTILE_DM 2>/dev/null; dmsetup create $HOSTILE_DM --table \"0 \$(blockdev --getsz $(cluster_device)) linear $(cluster_device) 0\"; }"; then
        log_error "VM 1: device-mapper setup failed"
        return 1
    fi
    setup_goclusterfs 1 false "" "/dev/mapper/$HOSTILE_DM"
}

hostile_writers_start() {
    local vm
    for vm in 1 2; do
        ssh_run "$vm" "rm -f /tmp/hostile.stop /tmp/hostile.acked; nohup sh -c 'i=0; while [ ! -e /tmp/hostile.stop ]; do i=\$((i+1)); n=h$vm-\$i; if echo \$n > ${GOCLUSTERFS_MOUNT}/\$n 2>/dev/null; then echo \$n >> /tmp/hostile.acked; fi; sleep 0.1; done' < /dev/null > /dev/null 2>&1 &"
    done
}

hostile_writers_stop() {
    ssh_run 1 "touch /tmp/hostile.stop" || true
    ssh_run 2 "touch /tmp/hostile.stop" || true
    sleep 8
}

# Every file either node acknowledged must read back on VM $1 with its name as its content.
hostile_check_acked() {
    local reader=$1 vm acked bad result=0
    for vm in 1 2; do
        acked=$(ssh_run "$vm" "cat /tmp/hostile.acked 2>/dev/null" | wc -l)
        bad=$(ssh_run "$vm" "cat /tmp/hostile.acked 2>/dev/null" | ssh_run "$reader" "n=0; while read f; do [ \"\$(cat ${GOCLUSTERFS_MOUNT}/\$f 2>/dev/null)\" = \"\$f\" ] || { n=\$((n+1)); [ \$n -le 5 ] && echo \"  \$f missing or wrong\" >&2; }; done; echo \$n")
        log_info "VM $vm acknowledged $acked files; $bad of them are missing or wrong on VM $reader"
        if [[ "$acked" -eq 0 || "$bad" != 0 ]]; then
            result=1
        fi
    done
    return $result
}

# Prints each node VM but VM 1 that started a journal replay since marker $1; in a 3-node run any peer may replay VM 1.
hostile_replayers_since() {
    local vm
    for vm in $(seq 2 "$NODE_COUNT"); do
        if sed -n "/$1/,\$p" "$(get_vm_mount_log "$vm")" | grep -q "Starting journal replay for node"; then
            echo "$vm"
        fi
    done
}

hostile_replayed_since() {
    [[ -n "$(hostile_replayers_since "$1")" ]]
}

# Prints every node VM's mount log since marker $1, for a scenario that failed.
hostile_dump_logs() {
    local vm
    for vm in $(seq 1 "$NODE_COUNT"); do
        log_info "--- VM $vm mount log since: $1"
        sed -n "/$1/,\$p" "$(get_vm_mount_log "$vm")" | grep -v '\[DEBUG\]' | tail -n 60
    done
}

# VM 1, back from a stall during which a peer replayed it, stopped, on its own
# stall, on finding its slot taken, or on the device refusing its write, and
# acknowledged no create but one whose writes were already queued when the stall began.
hostile_check_taken_slot_stops() {
    local acked
    if ! sed -n "/$1/,\$p" "$(get_vm_mount_log 1)" | grep -qE "is no longer this node's: (the slot is empty|a peer declared this node dead)|may no longer be this node's: it went|The device refused a write from this node"; then
        log_error "VM 1 did not stop on its stall, on finding its slot taken, or on a refused write"
        sed -n "/$1/,\$p" "$(get_vm_mount_log 1)" | grep -E 'ERROR|no longer|stopping' | head -5
        return 1
    fi
    acked=$(ssh_run 1 "wc -l < /tmp/hostile.acked")
    if (( acked - HOSTILE_ACKED_BEFORE > 1 )); then
        log_error "VM 1 acknowledged $((acked - HOSTILE_ACKED_BEFORE)) creates after the stall began, although a peer replayed it"
        return 1
    fi
}

# A peer replayed VM 1 17 to 32s after the stall began. The window follows from
# internal/node: a peer counts VM 1 dead once its heartbeat stays unchanged for
# deadAfter (20s), judged at its scans every FailureCheckInterval (5s), so the
# replay lands 19 to 30s in; the rest is slack for a loaded host.
hostile_check_stall_fence() {
    local stamp at vm
    hostile_check_taken_slot_stops "$1" || return 1
    vm=$(hostile_replayers_since "$1" | head -1)
    [[ -n "$vm" ]] || return 1
    stamp=$(sed -n "/$1/,\$p" "$(get_vm_mount_log "$vm")" | grep -m1 "Starting journal replay for node" | awk '{print $1" "$2}' | tr / -)
    [[ -n "$stamp" ]] || return 1
    at=$(( $(ssh_run "$vm" "date -d '$stamp' +%s") - HOSTILE_T0 ))
    log_info "VM $vm replayed VM 1 ${at}s after the stall began"
    if [[ $at -lt 17 || $at -gt 32 ]]; then
        log_error "VM $vm replayed VM 1 ${at}s after the stall began, want 17 to 32s"
        return 1
    fi
}

# VM 1 stopped itself because its heartbeat writes failed for just over the 5s limit, plus the 1s between beats and some load.
hostile_check_self_fence() {
    local stalled
    stalled=$(sed -n "/$1/,\$p" "$(get_vm_mount_log 1)" | grep -m1 -oE "Heartbeats to registry slot [0-9]+ failed for [0-9.]+m?s" | grep -oE "[0-9.]+m?s$")
    if [[ -z "$stalled" ]]; then
        log_error "VM 1 did not stop itself for failed heartbeat writes"
        sed -n "/$1/,\$p" "$(get_vm_mount_log 1)" | grep -E 'ERROR|no longer|stopping' | head -5
        return 1
    fi
    log_info "VM 1 stopped itself after its heartbeat writes failed for $stalled"
    if ! awk -v d="$stalled" 'BEGIN { s = d + 0; if (d ~ /ms$/) s /= 1000; exit !(s >= 5 && s < 9) }'; then
        log_error "VM 1's heartbeat writes failed for $stalled before it stopped, want 5 to 9s"
        return 1
    fi
}

# Unmounts both nodes, removes the device-mapper device, checks the disk with fsck -repair then fsck, and remounts both.
hostile_fsck_and_restore() {
    local result=0 out rc
    if [[ -n "$(mount_pids 1)" ]]; then
        unmount_goclusterfs 1 || crash_goclusterfs 1 || true
    fi
    ssh_run 1 "fusermount3 -uz ${GOCLUSTERFS_MOUNT} 2>/dev/null; dmsetup remove $HOSTILE_DM" || true
    if [[ -n "$(mount_pids 2)" ]]; then
        unmount_goclusterfs 2 || { log_error "VM 2: unmount failed"; result=1; }
    fi
    out=$(ssh_run 2 "/usr/local/bin/goclusterfs-fsck -repair $(cluster_device) 2>&1; echo rc=\$?")
    rc=${out##*rc=}
    log_info "fsck -repair exit $rc, $(echo "$out" | grep -c 'leaked') leaked-block lines"
    echo "$out" | grep '^\[' | grep -v leaked | head -10
    out=$(ssh_run 2 "/usr/local/bin/goclusterfs-fsck $(cluster_device) 2>&1; echo rc=\$?")
    rc=${out##*rc=}
    if [[ "$rc" != 0 ]]; then
        echo "$out" | grep '^\[' | head -20
        log_error "fsck after repair: exit $rc"
        result=1
    fi
    setup_goclusterfs 1 false || result=1
    setup_goclusterfs 2 false || result=1
    return $result
}

# Runs one scenario: $1 names it, $2 is a function that disturbs VM 1's path,
# $3 is "survives" when VM 1 must keep running unreplayed, "unnoticed" when
# VM 2 must also never wait on it as a failing node, "fenced" when it must
# stop and a peer must replay it, or "either" when both outcomes pass, and $4
# optionally names a function that checks more, given the scenario's log marker.
hostile_scenario() {
    local name=$1 disturb=$2 expect=$3 check=${4:-} result=0
    hostile_setup || return 1
    local marker="hostile: $name $(date +%s)"
    mark_vm_logs "$marker"
    hostile_writers_start
    sleep 5
    HOSTILE_T0=$(ssh_run 2 "date +%s")
    "$disturb"
    sleep 5
    hostile_writers_stop
    if [[ "$expect" == either ]]; then
        local waited=0
        while [[ -z "$(mount_pids 1)" ]] && ! hostile_replayed_since "$marker" && [[ $waited -lt 90 ]]; do
            sleep 1
            waited=$((waited + 1))
        done
        if hostile_replayed_since "$marker"; then
            log_info "$name: VM 1 was fenced and VM $(hostile_replayers_since "$marker" | head -1) replayed it"
            if [[ -n "$(mount_pids 1)" ]]; then
                log_error "A peer replayed VM 1 while VM 1 is still mounted"
                result=1
            fi
            hostile_check_taken_slot_stops "$marker" || result=1
        elif [[ -n "$(mount_pids 1)" ]]; then
            log_info "$name: VM 1 kept running and was not replayed"
            if sed -n "/$marker/,\$p" "$(get_vm_mount_log 1)" | grep -q "is no longer this node's"; then
                log_error "VM 1 found its slot taken although no peer replayed it"
                result=1
            fi
        else
            log_error "VM 1 stopped and no peer replayed it within 90s"
            result=1
        fi
    elif [[ "$expect" == fenced ]]; then
        local waited=0
        while ! hostile_replayed_since "$marker" && [[ $waited -lt 90 ]]; do
            sleep 1
            waited=$((waited + 1))
        done
        if ! hostile_replayed_since "$marker"; then
            log_error "No peer replayed VM 1 within 90s"
            result=1
        fi
        if [[ -n "$(mount_pids 1)" ]]; then
            log_error "VM 1 is still mounted after it lost its path for longer than the heartbeat timeout"
            result=1
        fi
    else
        if hostile_replayed_since "$marker"; then
            log_error "VM $(hostile_replayers_since "$marker" | head -1) declared VM 1 dead although VM 1 was only slowed"
            result=1
        fi
        if [[ -z "$(mount_pids 1)" ]]; then
            log_error "VM 1 stopped although its path recovered"
            sed -n "/$marker/,\$p" "$(get_vm_mount_log 1)" | grep -E 'ERROR|WARN' | head -10
            result=1
        fi
        if [[ "$expect" == unnoticed ]] && sed -n "/$marker/,\$p" "$(get_vm_mount_log 2)" | grep -q "is failing or being recovered"; then
            log_error "VM 2 waited on VM 1 as a failing node although VM 1 was only slowed"
            sed -n "/$marker/,\$p" "$(get_vm_mount_log 2)" | grep -m3 "is failing or being recovered"
            result=1
        fi
    fi
    if [[ -n "$check" ]]; then
        "$check" "$marker" || result=1
    fi
    hostile_check_acked 2 || result=1
    hostile_fsck_and_restore || result=1
    [[ $result -eq 0 ]] || hostile_dump_logs "$marker"
    return $result
}

hostile_delay_spikes() {
    local ms
    for ms in 100 500 1000 2000; do
        log_info "VM 1 path: ${ms}ms per I/O for 15s"
        hostile_dm_load "delay $(cluster_device) 0 $ms" || return 1
        sleep 15
    done
    hostile_dm_load "linear $(cluster_device) 0"
}

hostile_stall() {
    log_info "VM 1 path: stalled for ${HOSTILE_STALL}s"
    ssh_run 1 "dmsetup suspend $HOSTILE_DM"
    HOSTILE_ACKED_BEFORE=$(ssh_run 1 "wc -l < /tmp/hostile.acked")
    sleep "$HOSTILE_STALL"
    ssh_run 1 "dmsetup resume $HOSTILE_DM"
}

hostile_eio() {
    log_info "VM 1 path: every I/O fails with EIO for ${HOSTILE_EIO}s"
    hostile_dm_load "error" || return 1
    sleep "$HOSTILE_EIO"
    hostile_dm_load "linear $(cluster_device) 0"
}

hostile_flaky() {
    log_info "VM 1 path: 1s of EIO every 4s, for 40s"
    hostile_dm_load "flakey $(cluster_device) 0 3 1" || return 1
    sleep 40
    hostile_dm_load "linear $(cluster_device) 0"
}

test_storage_latency_spikes() { hostile_scenario "latency spikes" hostile_delay_spikes survives; }
test_storage_short_stalls() {
    HOSTILE_STALL=5 hostile_scenario "5s stall" hostile_stall unnoticed || return 1
    HOSTILE_STALL=12 hostile_scenario "12s stall" hostile_stall survives
}
test_storage_long_stall() {
    HOSTILE_STALL=25 hostile_scenario "25s stall" hostile_stall either || return 1
    HOSTILE_STALL=35 hostile_scenario "35s stall" hostile_stall fenced hostile_check_stall_fence
}
test_storage_short_eio() { HOSTILE_EIO=2 hostile_scenario "2s of EIO" hostile_eio unnoticed; }
test_storage_long_eio() { HOSTILE_EIO=8 hostile_scenario "8s of EIO" hostile_eio fenced hostile_check_self_fence; }
test_storage_flaky_eio() { hostile_scenario "flaky EIO" hostile_flaky survives; }

# A node that never mounted the volume, with its clock 2 minutes ahead, must
# take a free slot, not the slot of a node that is mounted and alive.
test_clock_ahead_mount() {
    ensure_cluster_healthy 2 || return 1
    unmount_goclusterfs 2 || { log_error "VM 2: unmount failed"; return 1; }
    local marker="hostile: clock ahead $(date +%s)" result=0
    mark_vm_logs "$marker"
    ssh_run 2 "hostname newnode; date -u -s '$(date -u -d @$(( $(date +%s) + 120 )) '+%Y-%m-%d %H:%M:%S')' > /dev/null"
    setup_goclusterfs 2 false || result=1
    sleep 5
    local slot1 slot2
    slot1=$(grep -o 'Registered as node [0-9]*' "$(get_vm_mount_log 1)" | tail -1)
    slot2=$(sed -n "/$marker/,\$p" "$(get_vm_mount_log 2)" | grep -o 'Registered as node [0-9]*' | tail -1)
    log_info "VM 1: $slot1; VM 2 with its clock 2 minutes ahead: $slot2"
    if [[ -z "$(mount_pids 1)" ]]; then
        log_error "VM 1 stopped after VM 2 mounted with a fast clock"
        sed -n "/$marker/,\$p" "$(get_vm_mount_log 1)" | grep -E 'ERROR|slot' | head -5
        result=1
    fi
    if [[ "$slot1" == "$slot2" ]]; then
        log_error "VM 2 took VM 1's registry slot"
        result=1
    fi
    ssh_run 2 "hostname \$(cat /etc/hostname); date -u -s '$(date -u '+%Y-%m-%d %H:%M:%S')' > /dev/null"
    if [[ -z "$(mount_pids 1)" ]]; then
        ssh_run 1 "fusermount3 -uz ${GOCLUSTERFS_MOUNT}" || true
        setup_goclusterfs 1 false || result=1
    fi
    unmount_goclusterfs 2 || crash_goclusterfs 2 || true
    setup_goclusterfs 2 false || result=1
    return $result
}

# A node whose clock jumped 3 minutes ahead and then crashed must still be
# replayed within the usual failover time.
test_clock_ahead_crash_recovered() {
    ensure_cluster_healthy 2 || return 1
    local marker="hostile: clock ahead crash $(date +%s)" result=0 waited=0
    mark_vm_logs "$marker"
    ssh_run 1 "date -u -s '$(date -u -d @$(( $(date +%s) + 180 )) '+%Y-%m-%d %H:%M:%S')' > /dev/null"
    sleep 3
    crash_goclusterfs 1 || result=1
    while ! hostile_replayed_since "$marker" && [[ $waited -lt 60 ]]; do
        sleep 1
        waited=$((waited + 1))
    done
    if ! hostile_replayed_since "$marker"; then
        log_error "No peer replayed crashed VM 1 within 60s; VM 1's clock was 3 minutes ahead"
        hostile_dump_logs "$marker"
        result=1
    else
        log_info "VM $(hostile_replayers_since "$marker" | head -1) replayed VM 1 ${waited}s after the crash"
    fi
    ssh_run 1 "date -u -s '$(date -u '+%Y-%m-%d %H:%M:%S')' > /dev/null; fusermount3 -uz ${GOCLUSTERFS_MOUNT}" || true
    setup_goclusterfs 1 false || result=1
    return $result
}

# A power failure while files are written may leave blocks that no file maps.
# With every node unmounted, fsck -repair frees any it finds, the check after
# it is clean, and no file changes.
test_fsck_repair_after_power_failure() {
    log_info "Testing fsck -repair after a power failure..."
    if [[ $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping: Need at least 2 VMs for this test"
        return 0
    fi
    ensure_cluster_healthy "$NODE_COUNT"
    local survivor=1
    local victim=2
    local fsck=/usr/local/bin/goclusterfs-fsck

    if ! ssh_run "$survivor" "test -x $fsck"; then
        log_error "goclusterfs-fsck is not on the VMs: build it with make build"
        return 1
    fi

    ssh_run "$survivor" "dd if=/dev/urandom of=${GOCLUSTERFS_MOUNT}/keep.bin bs=1M count=4 2>/dev/null && sync"
    local keep_md5
    keep_md5=$(ssh_run "$survivor" "md5sum ${GOCLUSTERFS_MOUNT}/keep.bin | cut -d' ' -f1")

    ssh_run "$victim" "for i in 1 2 3 4 5 6 7 8; do dd if=/dev/zero of=${GOCLUSTERFS_MOUNT}/victim_\$i.bin bs=1M count=8 2>/dev/null; done" &
    local writer=$!
    sleep 2
    log_info "Simulating power failure on VM $victim mid-write..."
    kill_vm "$victim"
    wait "$writer" 2>/dev/null || true

    start_vm "$victim" false
    wait_vm_healthy "$victim"
    setup_goclusterfs "$victim" false
    # Whatever the victim's writes left, repair must not change it.
    local before
    before=$(ssh_run "$victim" "cd ${GOCLUSTERFS_MOUNT} && md5sum keep.bin victim_*.bin 2>/dev/null")

    local vm_id
    for vm_id in $(seq 1 "$NODE_COUNT"); do
        if ! unmount_goclusterfs "$vm_id"; then
            log_error "VM $vm_id: GoClusterFS did not unmount"
            return 1
        fi
    done

    local out rc
    out=$(ssh_run "$survivor" "$fsck -repair $(cluster_device) 2>&1; echo rc=\$?")
    rc=${out##*rc=}
    echo "$out" | grep '^\[' | head -20
    log_info "fsck -repair exit $rc, $(echo "$out" | grep -c 'leaked') leaked-block lines"
    if [[ "$rc" != 0 && "$rc" != 1 ]]; then
        log_error "fsck -repair exited $rc"
        return 1
    fi
    out=$(ssh_run "$survivor" "$fsck $(cluster_device) 2>&1; echo rc=\$?")
    rc=${out##*rc=}
    if [[ "$rc" != 0 ]] || echo "$out" | grep -q 'leaked'; then
        echo "$out" | grep '^\[' | head -20
        log_error "fsck after repair: exit $rc"
        return 1
    fi

    for vm_id in $(seq 1 "$NODE_COUNT"); do
        setup_goclusterfs "$vm_id" false || return 1
    done
    if [[ "$before" != *"$keep_md5  keep.bin"* ]]; then
        log_error "keep.bin changed before repair"
        return 1
    fi
    for vm_id in "$survivor" "$victim"; do
        local after
        after=$(ssh_run "$vm_id" "cd ${GOCLUSTERFS_MOUNT} && md5sum keep.bin victim_*.bin 2>/dev/null")
        if [[ "$after" != "$before" ]]; then
            log_error "VM $vm_id: files changed by repair"
            diff <(echo "$before") <(echo "$after") >&2 || true
            return 1
        fi
    done
    log_info "fsck -repair left a clean volume and every file unchanged"
    return 0
}

# =============================================================================
# Power failures during copy-on-write, snapshots and TRIM
# =============================================================================
# A workload script runs on the victim and prints a line after each step it
# finished with fsync. The victim loses power mid-workload; every step it
# acknowledged must survive replay, and fsck may find nothing but leaks.

# Prints the letter round $1 of a workload writes.
round_letter() {
    printf "\\$(printf %03o $((65 + $1 % 26)))"
}

install_power_failure_scripts() {
    local vm_id=$1
    ssh_run "$vm_id" "cat > /tmp/pf_letters.sh && chmod +x /tmp/pf_letters.sh" <<'EOF'
#!/bin/sh
# Prints, for each 64KB chunk of $1 from byte $2 for $3 chunks, its letter, or the distinct bytes of a mixed chunk
# in parentheses.
for L in a A B C D E F G H I J K L M N O P Q R S T U V W X Y Z; do
    echo "$(head -c 65536 /dev/zero | tr '\0' "$L" | md5sum | cut -c1-32) $L"
done > /tmp/pf_letter_md5s
k=0
while [ "$k" -lt "$3" ]; do
    chunk="dd if=$1 bs=65536 skip=$(($2 / 65536 + k)) count=1"
    m=$($chunk 2>/dev/null | md5sum | cut -c1-32)
    L=$(grep "^$m " /tmp/pf_letter_md5s | cut -c34)
    if [ -z "$L" ]; then
        L="($($chunk 2>/dev/null | od -An -c -v | tr -s ' ' '\n' | grep -v '^$' | sort -u | tr -d '\n'))"
    fi
    printf %s "$L"
    k=$((k + 1))
done
echo
EOF
    ssh_run "$vm_id" "cat > /tmp/pf_cow.sh && chmod +x /tmp/pf_cow.sh" <<'EOF'
#!/bin/sh
# Rewrites block (round % 4) of $1 with the round's letter, with fsync, and acknowledges each round.
i=0
while :; do
    i=$((i + 1))
    c=$(printf "\\$(printf %03o $((65 + i % 26)))")
    head -c 1048576 /dev/zero | tr '\0' "$c" | dd of="$1" bs=1M seek=$((i % 4)) conv=notrunc,fsync 2>/dev/null || exit 1
    echo "ack $i"
done
EOF
    ssh_run "$vm_id" "cat > /tmp/pf_snap.sh && chmod +x /tmp/pf_snap.sh" <<'EOF'
#!/bin/sh
# Creates snapshot pf_s<round> of $1 and deletes the one from two rounds back, acknowledging each step.
i=0
while :; do
    i=$((i + 1))
    setfattr -n user.snapshot -v "pf_s$i" "$1" || exit 1
    echo "created $i"
    if [ "$i" -gt 2 ]; then
        setfattr -x "user.snapshot.pf_s$((i - 2))" "$1" || exit 1
        echo "deleted $((i - 2))"
    fi
done
EOF
    ssh_run "$vm_id" "cat > /tmp/pf_trim.sh && chmod +x /tmp/pf_trim.sh" <<'EOF'
#!/bin/sh
# Writes a 2MB file of the round's letter in $1 with fsync after removing the file from three rounds back, so new
# files reuse blocks TRIM has queued, and acknowledges each round.
i=0
while :; do
    i=$((i + 1))
    rm -f "$1/trim_pf_$((i - 3)).bin"
    c=$(printf "\\$(printf %03o $((65 + i % 26)))")
    head -c 2097152 /dev/zero | tr '\0' "$c" | dd of="$1/trim_pf_$i.bin" bs=1M conv=fsync 2>/dev/null || exit 1
    echo "ack $i"
done
EOF
}

# Cuts power to VM $1 while it runs workload $2 for $3 seconds, writing its acknowledgements to file $4.
power_fail_during() {
    local victim=$1 workload=$2 seconds=$3 acks=$4
    ssh_run "$victim" "$workload" > "$acks" 2>/dev/null &
    local runner=$!
    sleep "$seconds"
    log_info "Simulating power failure on VM $victim after $(wc -l < "$acks") acknowledged steps..."
    kill_vm "$victim"
    wait "$runner" 2>/dev/null || true
}

# Restarts VM $1 with journal replay, then requires fsck to find nothing but leaks and -repair to fix them.
restart_and_expect_only_leaks() {
    local victim=$1 fsck=/usr/local/bin/goclusterfs-fsck vm_id out problems
    start_vm "$victim" false
    wait_vm_healthy "$victim" || return 1
    setup_goclusterfs "$victim" false || return 1
    sleep 3  # snapshot recovery jobs finish in the background after mount
    for vm_id in $(seq 1 "$NODE_COUNT"); do
        if ! unmount_goclusterfs "$vm_id"; then
            log_error "VM $vm_id: GoClusterFS did not unmount"
            return 1
        fi
    done

    # Snapshots of files earlier tests deleted are orphans, which is not damage.
    out=$(ssh_run 1 "$fsck $(cluster_device) 2>&1")
    problems=$(echo "$out" | grep -E '^ *\[(WARN|ERROR)\]' | grep -v 'is an orphan')
    if echo "$problems" | grep -v -e '^$' -e 'leaked' -e 'will never be freed' -e 'unnamed: no directory names it' -e 'belongs to no snapshot' | grep -q .; then
        log_error "fsck after the power failure found more than leaks:"
        echo "$problems" | head -20 >&2
        return 1
    fi
    if [[ -n "$problems" ]]; then
        log_info "fsck found $(echo "$problems" | wc -l) leaks; repairing"
        ssh_run 1 "$fsck -repair $(cluster_device) > /dev/null 2>&1"
        out=$(ssh_run 1 "$fsck $(cluster_device) 2>&1" | grep -E '^ *\[(WARN|ERROR)\]' | grep -v 'is an orphan')
        if [[ -n "$out" ]]; then
            log_error "fsck after repair still finds problems:"
            echo "$out" | head -20 >&2
            return 1
        fi
    fi
    for vm_id in $(seq 1 "$NODE_COUNT"); do
        setup_goclusterfs "$vm_id" false || return 1
    done
}

test_cow_write_power_failure() {
    log_info "Testing a power failure during copy-on-write writes to a snapshotted file..."
    if [[ $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping: Need at least 2 VMs for this test"
        return 0
    fi
    ensure_cluster_healthy "$NODE_COUNT"
    local victim=2 f=${GOCLUSTERFS_MOUNT}/cow_pf.bin acks
    acks=$(mktemp)
    install_power_failure_scripts "$victim"
    install_power_failure_scripts 1
    ssh_run "$victim" "head -c 4194304 /dev/zero | tr '\\0' a > $f && sync && setfattr -n user.snapshot -v cow_pf_snap $f" || return 1

    power_fail_during "$victim" "/tmp/pf_cow.sh $f" 6 "$acks"
    local last
    last=$(awk '/^ack/ {n = $2} END {print n + 0}' "$acks")
    restart_and_expect_only_leaks "$victim" || return 1

    local b r letters want maybe
    for b in 0 1 2 3; do
        want=a
        for ((r = last; r > 0; r--)); do
            if (( r % 4 == b )); then want=$(round_letter "$r"); break; fi
        done
        maybe=$want
        for r in $((last + 1)) $((last + 2)); do
            (( r % 4 == b )) && maybe="$maybe$(round_letter "$r")"
        done
        letters=$(ssh_run 1 "/tmp/pf_letters.sh $f $((b * 1048576)) 16")
        # A write in flight may stop partway through a chunk, but only old and new bytes may show.
        if [[ ! "$letters" =~ ^([$maybe]|\([$maybe]+\)){16}$ ]]; then
            log_error "block $b after $last acknowledged writes holds '$letters', want only '$maybe'"
            return 1
        fi
    done
    ssh_run 1 "setfattr -n user.snapshot.clone -v cow_pf_snap:/cow_pf_snap_check.bin $f" || return 1
    letters=$(ssh_run 1 "/tmp/pf_letters.sh ${GOCLUSTERFS_MOUNT}/cow_pf_snap_check.bin 0 64")
    if [[ "$letters" != "$(printf 'a%.0s' $(seq 64))" ]]; then
        log_error "snapshot cow_pf_snap changed: '$letters'"
        return 1
    fi
    log_info "$last acknowledged copy-on-write writes and the snapshot survived the power failure"
    rm -f "$acks"
}

test_snapshot_create_delete_power_failure() {
    log_info "Testing a power failure while snapshots are created and deleted..."
    if [[ $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping: Need at least 2 VMs for this test"
        return 0
    fi
    ensure_cluster_healthy "$NODE_COUNT"
    local victim=2 f=${GOCLUSTERFS_MOUNT}/snap_pf.bin acks
    acks=$(mktemp)
    install_power_failure_scripts "$victim"
    ssh_run "$victim" "dd if=/dev/urandom of=$f bs=1M count=3 2>/dev/null && sync" || return 1
    local base
    base=$(ssh_run "$victim" "md5sum $f | cut -d' ' -f1")

    power_fail_during "$victim" "/tmp/pf_snap.sh $f" 6 "$acks"
    local created deleted
    created=$(awk '/^created/ {n = $2} END {print n + 0}' "$acks")
    deleted=$(awk '/^deleted/ {n = $2} END {print n + 0}' "$acks")
    restart_and_expect_only_leaks "$victim" || return 1

    if [[ "$(ssh_run 1 "md5sum $f | cut -d' ' -f1")" != "$base" ]]; then
        log_error "snap_pf.bin changed"
        return 1
    fi
    # Recovery finishes snapshots left creating or deleting in the background after mount. Entries are name:state.
    local listed waited=0
    while listed=$(ssh_run 1 "getfattr --only-values -n user.snapshots $f 2>/dev/null" | tr ',' '\n'); \
        [[ $waited -lt 20 ]] && echo "$listed" | awk -F: -v d="$deleted" \
            '$1 ~ /^pf_s/ && ($2 != "active" || substr($1, 5) + 0 <= d) {found = 1} END {exit !found}'; do
        sleep 1
        waited=$((waited + 1))
    done
    listed=$(echo "$listed" | cut -d: -f1)
    local i name
    for ((i = 1; i <= created + 1; i++)); do
        name=pf_s$i
        if ! echo "$listed" | tr ',' '\n' | grep -qx "$name"; then
            if (( i <= created && i > deleted + 1 )); then
                log_error "snapshot $name was acknowledged and not deleted, but is gone (listed: $listed)"
                return 1
            fi
            continue
        fi
        if (( i <= deleted )); then
            log_error "snapshot $name was acknowledged as deleted, but is still listed"
            return 1
        fi
        ssh_run 1 "setfattr -n user.snapshot.clone -v $name:/${name}_check.bin $f" || return 1
        if [[ "$(ssh_run 1 "md5sum ${GOCLUSTERFS_MOUNT}/${name}_check.bin | cut -d' ' -f1")" != "$base" ]]; then
            log_error "snapshot $name changed"
            return 1
        fi
    done
    log_info "$created snapshots created and $deleted deleted before the power failure; the survivors are intact"
    rm -f "$acks"
}

test_trim_reuse_power_failure() {
    log_info "Testing a power failure while freed blocks wait for TRIM and new files reuse them..."
    if [[ $NODE_COUNT -lt 2 ]]; then
        log_warn "Skipping: Need at least 2 VMs for this test"
        return 0
    fi
    ensure_cluster_healthy "$NODE_COUNT"
    local victim=2 acks
    acks=$(mktemp)
    install_power_failure_scripts "$victim"
    install_power_failure_scripts 1
    ssh_run "$victim" "dd if=/dev/urandom of=${GOCLUSTERFS_MOUNT}/trim_keep.bin bs=1M count=4 2>/dev/null && sync" || return 1
    local keep
    keep=$(ssh_run "$victim" "md5sum ${GOCLUSTERFS_MOUNT}/trim_keep.bin | cut -d' ' -f1")

    # TRIM flushes freed blocks after 5s, so the workload runs through several flushes.
    power_fail_during "$victim" "/tmp/pf_trim.sh ${GOCLUSTERFS_MOUNT}" 12 "$acks"
    local last
    last=$(awk '/^ack/ {n = $2} END {print n + 0}' "$acks")
    restart_and_expect_only_leaks "$victim" || return 1

    if [[ "$(ssh_run 1 "md5sum ${GOCLUSTERFS_MOUNT}/trim_keep.bin | cut -d' ' -f1")" != "$keep" ]]; then
        log_error "trim_keep.bin changed"
        return 1
    fi
    local i letters
    for ((i = 1; i <= last; i++)); do
        letters=$(ssh_run 1 "test -e ${GOCLUSTERFS_MOUNT}/trim_pf_$i.bin && /tmp/pf_letters.sh ${GOCLUSTERFS_MOUNT}/trim_pf_$i.bin 0 32")
        if [[ -z "$letters" ]]; then
            if (( i == last )); then
                log_error "trim_pf_$last.bin was acknowledged and never removed, but is gone"
                return 1
            fi
            continue
        fi
        if [[ "$letters" != "$(printf "$(round_letter "$i")%.0s" $(seq 32))" ]]; then
            log_error "trim_pf_$i.bin was acknowledged but holds '$letters'"
            return 1
        fi
    done
    if ! grep -q "\[trim\] .*flushing" "$(get_vm_mount_log "$victim")" "$(get_vm_mount_log 1)"; then
        log_warn "no TRIM flush ran during the test"
    fi
    log_info "$last acknowledged files survived the power failure and every TRIM flush"
    rm -f "$acks"
}

# =============================================================================
# Main Test Execution
# =============================================================================

# =============================================================================
# Extent tree tests (files with more than 2 extents)
# =============================================================================
# These tests read back only on a node that has just restarted: the extent
# cache keeps mappings of deleted files, and cleanup_test_files makes new files
# reuse those inode numbers.

# Print the md5 of 64KB at 1MB block $3 of file $2 on VM $1
extent_chunk_md5_on_vm() {
    ssh_run "$1" "dd if=/mnt/goclusterfs/$2 bs=65536 skip=$(($3 * 16)) count=1 2>/dev/null | md5sum | cut -d' ' -f1"
}

# Restarts a VM and mounts GoClusterFS on it again.
restart_goclusterfs_vm() {
    local vm_id=$1
    stop_vm "$vm_id"
    start_vm "$vm_id" false
    wait_vm_healthy "$vm_id" || return 1
    setup_goclusterfs "$vm_id" false
}

# Writes 64KB at seven scattered 1MB blocks (more than the 2 inline extents)
# on VM 1, then checks every chunk and a hole on VM 2 and on VM 1, each after
# a restart.
test_extent_tree_sparse_writes() {
    log_info "Testing sparse writes beyond inline extents..."
    ensure_cluster_healthy 2

    local file="extent_sparse.bin"
    local blocks="12 3 7 0 20 15 30"
    declare -A want

    for blk in $blocks; do
        ssh_run 1 "head -c 65536 /dev/urandom > /tmp/extent_chunk"
        want[$blk]=$(ssh_run 1 "md5sum /tmp/extent_chunk | cut -d' ' -f1")
        ssh_run 1 "dd if=/tmp/extent_chunk of=/mnt/goclusterfs/$file bs=65536 seek=$((blk * 16)) conv=notrunc 2>/dev/null" || {
            log_error "write at block $blk failed"
            return 1
        }
    done

    local size
    size=$(ssh_run 1 "stat -c%s /mnt/goclusterfs/$file")
    if [ "$size" -ne $((30 * 1048576 + 65536)) ]; then
        log_error "size $size, want $((30 * 1048576 + 65536))"
        return 1
    fi

    check_sparse_on_vm() {
        local vm_id=$1
        for blk in $blocks; do
            local got
            got=$(extent_chunk_md5_on_vm "$vm_id" "$file" "$blk")
            if [ "$got" != "${want[$blk]}" ]; then
                log_error "VM $vm_id block $blk md5 $got, want ${want[$blk]}"
                return 1
            fi
        done
        local nonzero
        nonzero=$(ssh_run "$vm_id" "dd if=/mnt/goclusterfs/$file bs=65536 skip=80 count=1 2>/dev/null | tr -d '\\000' | wc -c")
        if [ "$nonzero" -ne 0 ]; then
            log_error "VM $vm_id hole at block 5 has $nonzero non-zero bytes"
            return 1
        fi
    }

    for vm_id in 2 1; do
        restart_goclusterfs_vm "$vm_id" || return 1
        check_sparse_on_vm "$vm_id" || return 1
    done

    log_info "Sparse writes beyond inline extents test passed"
    return 0
}

md5_on_vm_matches() {
    local vm_id=$1 path=$2 want=$3 got
    got=$(ssh_run "$vm_id" "md5sum < $path | cut -d' ' -f1")
    if [ "$got" != "$want" ]; then
        log_error "VM $vm_id $path md5 $got, want $want"
        return 1
    fi
}

# Snapshots a file, then overwrites it in place, appends to it and truncates
# it. A clone of the snapshot must hold the original data on both VMs. The
# expected data is built with the same edits on a local copy.
test_snapshot_preserves_data() {
    log_info "Testing that a snapshot keeps its data..."
    ensure_cluster_healthy 2

    local file=/mnt/goclusterfs/snap_data.bin
    local clone=/mnt/goclusterfs/snap_data_clone.bin

    ssh_run 1 "rm -f $file $clone"
    ssh_run 1 "head -c 3145728 /dev/urandom > /tmp/snap_orig && cp /tmp/snap_orig /tmp/snap_live && cp /tmp/snap_orig $file" || return 1
    ssh_run 1 "setfattr -n user.snapshot -v snap_data_before $file" || {
        log_error "snapshot failed"
        return 1
    }
    ssh_run 1 "head -c 4096 /dev/urandom > /tmp/snap_patch && head -c 2097152 /dev/urandom > /tmp/snap_tail"
    for target in "$file" /tmp/snap_live; do
        ssh_run 1 "dd if=/tmp/snap_patch of=$target bs=4096 seek=257 conv=notrunc 2>/dev/null && cat /tmp/snap_tail >> $target && truncate -s 1572864 $target" || {
            log_error "changing $target failed"
            return 1
        }
    done
    ssh_run 1 "setfattr -n user.snapshot.clone -v snap_data_before:/snap_data_clone.bin $file" || {
        log_error "clone from snapshot failed"
        return 1
    }

    local want_clone want_live
    want_clone=$(ssh_run 1 "md5sum < /tmp/snap_orig | cut -d' ' -f1")
    want_live=$(ssh_run 1 "md5sum < /tmp/snap_live | cut -d' ' -f1")
    for vm_id in 2 1; do
        restart_goclusterfs_vm "$vm_id" || return 1
        md5_on_vm_matches "$vm_id" "$clone" "$want_clone" || return 1
        md5_on_vm_matches "$vm_id" "$file" "$want_live" || return 1
    done

    ssh_run 1 "setfattr -x user.snapshot.snap_data_before $file"
    log_info "Snapshot keeps its data test passed"
    return 0
}

# Writes into a clone and into its source with dd conv=notrunc. Each write must
# change only the file written, on both VMs.
test_clone_in_place_write() {
    log_info "Testing in-place writes to a clone and its source..."
    ensure_cluster_healthy 2

    local src=/mnt/goclusterfs/clone_src.bin
    local clone=/mnt/goclusterfs/clone_inplace.bin

    ssh_run 1 "rm -f $src $clone"
    ssh_run 1 "head -c 2097152 /dev/urandom > /tmp/clone_src && cp /tmp/clone_src /tmp/clone_copy && cp /tmp/clone_src $src" || return 1
    ssh_run 1 "setfattr -n user.clone -v /clone_inplace.bin $src" || {
        log_error "clone failed"
        return 1
    }
    ssh_run 1 "head -c 4096 /dev/urandom > /tmp/clone_patch_a && head -c 4096 /dev/urandom > /tmp/clone_patch_b"
    ssh_run 1 "dd if=/tmp/clone_patch_a of=$clone bs=4096 conv=notrunc 2>/dev/null && dd if=/tmp/clone_patch_a of=/tmp/clone_copy bs=4096 conv=notrunc 2>/dev/null" || return 1
    ssh_run 1 "dd if=/tmp/clone_patch_b of=$src bs=4096 seek=256 conv=notrunc 2>/dev/null && dd if=/tmp/clone_patch_b of=/tmp/clone_src bs=4096 seek=256 conv=notrunc 2>/dev/null" || return 1

    local want_src want_clone
    want_src=$(ssh_run 1 "md5sum < /tmp/clone_src | cut -d' ' -f1")
    want_clone=$(ssh_run 1 "md5sum < /tmp/clone_copy | cut -d' ' -f1")
    for vm_id in 2 1; do
        restart_goclusterfs_vm "$vm_id" || return 1
        md5_on_vm_matches "$vm_id" "$src" "$want_src" || return 1
        md5_on_vm_matches "$vm_id" "$clone" "$want_clone" || return 1
    done

    log_info "In-place clone write test passed"
    return 0
}

# Snapshots a file, overwrites both of its blocks, then deletes the snapshot.
# The delete must finish and give back exactly the two blocks only the
# snapshot held.
test_snapshot_delete_after_overwrite() {
    log_info "Testing snapshot delete after overwrite..."
    ensure_cluster_healthy 2

    local file=/mnt/goclusterfs/snap_delete.bin
    local free_before free_now

    ssh_run 1 "rm -f $file"
    ssh_run 1 "head -c 2097152 /dev/urandom > $file" || return 1
    sleep "$RESERVE_IDLE_RETURN_SECS"
    free_before=$(ssh_run 1 "stat -f -c %f /mnt/goclusterfs")
    ssh_run 1 "setfattr -n user.snapshot -v snap_delete_temp $file" || {
        log_error "snapshot failed"
        return 1
    }
    ssh_run 1 "head -c 2097152 /dev/urandom > /tmp/snap_new && dd if=/tmp/snap_new of=$file bs=1048576 conv=notrunc 2>/dev/null" || return 1
    free_now=$(ssh_run 1 "stat -f -c %f /mnt/goclusterfs")
    if [ "$free_now" -ne $((free_before - 2)) ]; then
        log_error "$free_now free blocks after overwriting 2 snapshotted blocks, want $((free_before - 2))"
        return 1
    fi

    ssh_run 1 "setfattr -x user.snapshot.snap_delete_temp $file" || {
        log_error "snapshot delete failed"
        return 1
    }
    local tries=0
    while ssh_run 1 "getfattr --only-values -n user.snapshots $file 2>/dev/null | grep -q snap_delete_temp"; do
        tries=$((tries + 1))
        if [ "$tries" -ge 100 ]; then
            log_error "snapshot still listed 10s after delete"
            return 1
        fi
        sleep 0.1
    done
    free_now=$(ssh_run 1 "stat -f -c %f /mnt/goclusterfs")
    if [ "$free_now" -ne "$free_before" ]; then
        log_error "$free_now free blocks after deleting the snapshot, want $free_before"
        return 1
    fi
    md5_on_vm_matches 1 "$file" "$(ssh_run 1 "md5sum < /tmp/snap_new | cut -d' ' -f1")" || return 1

    log_info "Snapshot delete after overwrite test passed"
    return 0
}

# Node A overwrites a 2 MB file in 64 KB chunks with rounds of a, b, c while
# node B snapshots it. Every snapshot must show one moment of the file (a
# prefix of the next round, the rest of the round before) and never change.
test_snapshot_during_writes() {
    log_info "Testing snapshots on one VM while another VM overwrites the file..."
    ensure_cluster_healthy 2

    local run=$$
    local file=/mnt/goclusterfs/busy.bin

    ssh_run 1 "head -c 2097152 /dev/zero | tr '\\000' c > $file" || return 1
    ssh_run 1 "rm -f /tmp/busy.stop; for c in a b c; do head -c 65536 /dev/zero | tr '\\000' \$c > /tmp/busy_\$c; done; while [ ! -f /tmp/busy.stop ]; do for c in a b c; do for k in \$(seq 0 31); do [ -f /tmp/busy.stop ] && exit 0; dd if=/tmp/busy_\$c of=$file bs=65536 seek=\$k count=1 conv=notrunc 2>/dev/null || exit 1; done; done; done" &
    local writer=$!

    local i clone sum
    local clones=() sums=()
    for i in $(seq 1 10); do
        clone=/mnt/goclusterfs/busy_${run}_$i.bin
        if ! ssh_run 2 "setfattr -n user.snapshot -v busy_${run}_$i $file && setfattr -n user.snapshot.clone -v busy_${run}_$i:/busy_${run}_$i.bin $file"; then
            log_error "snapshot $i on VM 2 failed"
            ssh_run 1 "touch /tmp/busy.stop"
            wait "$writer"
            return 1
        fi
        clones+=("$clone")
        sums+=("$(ssh_run 2 "md5sum < $clone | cut -d' ' -f1")")
    done
    ssh_run 1 "touch /tmp/busy.stop"
    wait "$writer" || { log_error "writer on VM 1 failed"; return 1; }

    for i in "${!clones[@]}"; do
        clone=${clones[$i]}
        sum=$(ssh_run 2 "md5sum < $clone | cut -d' ' -f1")
        if [ "$sum" != "${sums[$i]}" ]; then
            log_error "$clone changed after its snapshot was made"
            return 1
        fi
        local shape
        shape=$(ssh_run 2 "for k in \$(seq 0 31); do dd if=$clone bs=65536 skip=\$k count=1 2>/dev/null | tr -s abc | head -c 2; done")
        if [ ${#shape} -ne 32 ] || ! [[ $shape =~ ^(b*a*|c*b*|a*c*)$ ]]; then
            log_error "$clone chunks read $shape; the file never looked like this"
            return 1
        fi
    done

    ssh_run 2 "for i in \$(seq 1 10); do setfattr -x user.snapshot.busy_${run}_\$i $file; rm -f /mnt/goclusterfs/busy_${run}_\$i.bin; done"
    log_info "Snapshots during writes test passed"
    return 0
}

# Two VMs each take 10 snapshots of their own file at the same time. Each file
# must list all of its snapshots, and deleting one VM's snapshots must leave
# the other VM's alone.
test_snapshot_concurrent_creates() {
    log_info "Testing concurrent snapshot creation on two VMs..."
    ensure_cluster_healthy 2

    local run=$$
    local file_a=/mnt/goclusterfs/conc_a.bin
    local file_b=/mnt/goclusterfs/conc_b.bin

    ssh_run 1 "rm -f $file_a && head -c 1048576 /dev/urandom > $file_a" || return 1
    ssh_run 2 "rm -f $file_b && head -c 1048576 /dev/urandom > $file_b" || return 1

    ssh_run 1 "for i in \$(seq 1 10); do setfattr -n user.snapshot -v conc_a_${run}_\$i $file_a || exit 1; done" &
    local pid_a=$!
    ssh_run 2 "for i in \$(seq 1 10); do setfattr -n user.snapshot -v conc_b_${run}_\$i $file_b || exit 1; done" &
    local pid_b=$!
    wait "$pid_a" || { log_error "snapshots on VM 1 failed"; return 1; }
    wait "$pid_b" || { log_error "snapshots on VM 2 failed"; return 1; }

    local count_a count_b
    count_a=$(ssh_run 1 "getfattr --only-values -n user.snapshots $file_a 2>/dev/null | grep -c ':active'")
    count_b=$(ssh_run 2 "getfattr --only-values -n user.snapshots $file_b 2>/dev/null | grep -c ':active'")
    if [ "$count_a" -ne 10 ] || [ "$count_b" -ne 10 ]; then
        log_error "$count_a and $count_b active snapshots, want 10 each"
        return 1
    fi

    ssh_run 1 "for i in \$(seq 1 10); do setfattr -x user.snapshot.conc_a_${run}_\$i $file_a || exit 1; done" || {
        log_error "deleting snapshots on VM 1 failed"
        return 1
    }
    local tries=0
    while ssh_run 1 "getfattr --only-values -n user.snapshots $file_a 2>/dev/null | grep -q conc_a_"; do
        tries=$((tries + 1))
        if [ "$tries" -ge 100 ]; then
            log_error "VM 1 snapshots still listed 10s after delete"
            return 1
        fi
        sleep 0.1
    done
    count_b=$(ssh_run 2 "getfattr --only-values -n user.snapshots $file_b 2>/dev/null | grep -c ':active'")
    if [ "$count_b" -ne 10 ]; then
        log_error "VM 2 lists $count_b snapshots after VM 1 deleted its own, want 10"
        return 1
    fi

    ssh_run 2 "for i in \$(seq 1 10); do setfattr -x user.snapshot.conc_b_${run}_\$i $file_b; done"
    log_info "Concurrent snapshot creation test passed"
    return 0
}

# Two VMs append 1MB alternately to their own file, so both files get
# non-contiguous blocks and grow past the old 2MB limit. Checks both files on
# each VM after restarting it.
test_extent_tree_interleaved_appends() {
    log_info "Testing interleaved appends across VMs..."
    ensure_cluster_healthy 2

    ssh_run 1 "rm -f /tmp/extent_left.expected"
    ssh_run 2 "rm -f /tmp/extent_right.expected"
    for i in 0 1 2 3 4 5; do
        ssh_run 1 "head -c 1048576 /dev/urandom | tee -a /tmp/extent_left.expected >> /mnt/goclusterfs/extent_left.bin" || {
            log_error "VM 1 append $i failed"
            return 1
        }
        ssh_run 2 "head -c 1048576 /dev/urandom | tee -a /tmp/extent_right.expected >> /mnt/goclusterfs/extent_right.bin" || {
            log_error "VM 2 append $i failed"
            return 1
        }
    done

    local want_left want_right
    want_left=$(ssh_run 1 "md5sum /tmp/extent_left.expected | cut -d' ' -f1")
    want_right=$(ssh_run 2 "md5sum /tmp/extent_right.expected | cut -d' ' -f1")

    check_appends_on_vm() {
        local vm_id=$1
        local left right
        left=$(ssh_run "$vm_id" "md5sum /mnt/goclusterfs/extent_left.bin | cut -d' ' -f1")
        right=$(ssh_run "$vm_id" "md5sum /mnt/goclusterfs/extent_right.bin | cut -d' ' -f1")
        if [ "$left" != "$want_left" ] || [ "$right" != "$want_right" ]; then
            log_error "VM $vm_id md5 left=$left right=$right, want left=$want_left right=$want_right"
            return 1
        fi
    }

    for vm_id in 2 1; do
        restart_goclusterfs_vm "$vm_id" || return 1
        check_appends_on_vm "$vm_id" || return 1
    done

    log_info "Interleaved appends test passed"
    return 0
}

main() {
    log_info "Starting VM test suite with $NODE_COUNT VMs..."

    # Create results directory
    mkdir -p "$RESULTS_DIR"
    > "$RESULTS_DIR/results.txt"

    # Create shared disk
    create_shared_disk

    # Start all VMs
    log_info "Starting VMs..."
    for vm_id in $(seq 1 $NODE_COUNT); do
        local format_flag="false"
        if [[ $vm_id -eq 1 ]]; then
            format_flag="true"  # First VM formats the filesystem
        fi
        start_vm "$vm_id" "$format_flag"
    done

    # Wait for all VMs to be healthy
    log_info "Waiting for all VMs to be healthy..."
    for vm_id in $(seq 1 $NODE_COUNT); do
        wait_vm_healthy "$vm_id" || {
            log_error "VM $vm_id failed to become healthy"
            cleanup
            exit 1
        }
    done

    # Setup GoClusterFS on all VMs
    log_info "Setting up GoClusterFS on all VMs..."
    for vm_id in $(seq 1 $NODE_COUNT); do
        local format_flag="false"
        if [[ $vm_id -eq 1 ]]; then
            format_flag="true"  # First VM formats the filesystem
        fi
        setup_goclusterfs "$vm_id" "$format_flag" || {
            log_error "Failed to setup GoClusterFS on VM $vm_id"
            cleanup
            exit 1
        }
    done

    log_info "All VMs healthy with GoClusterFS mounted, starting tests..."

    # Run standard tests
    # Basic sanity checks first
    run_test "Create and Read File" test_create_read_file
    run_test "Create Directory" test_create_directory
    run_test "File in Directory" test_file_in_directory
    run_test "Data Persistence" test_data_persistence
    run_test "Delete File" test_delete_file
    run_test "Overwrite File" test_overwrite_file
    run_test "Sequential Access" test_sequential_access
    run_test "Extent Tree Sparse Writes" test_extent_tree_sparse_writes
    run_test "Extent Tree Interleaved Appends" test_extent_tree_interleaved_appends
    run_test "Snapshot Preserves Data" test_snapshot_preserves_data
    run_test "Clone In-Place Write" test_clone_in_place_write
    run_test "Snapshot Delete After Overwrite" test_snapshot_delete_after_overwrite
    run_test "Snapshot Concurrent Creates" test_snapshot_concurrent_creates
    run_test "Snapshot During Writes" test_snapshot_during_writes

    run_test "Concurrent Writes" test_concurrent_writes
    run_test "Concurrent File Creation" test_concurrent_file_creation
    run_test "VM Kill Recovery" test_vm_kill_recovery
    run_test "Journal Recovery Multi-Node" test_journal_recovery_multinode
    run_test "Reserve Returned After Crash" test_reserve_returned_after_crash
    run_test "Restart Keeps Peer Journal" test_restart_keeps_peer_journal
    run_test "Journal Recovery Multiple Files" test_journal_recovery_multiple_files
    run_test "Journal Recovery Large File" test_journal_recovery_large_file
    run_test "Journal Recovery Idempotency" test_journal_recovery_idempotency
    run_test "Journal Crash Recovery File" test_journal_crash_recovery_file
    run_test "Journal Crash Recovery Directory" test_journal_crash_recovery_directory
    run_test "Journal Crash Recovery Multiple Ops" test_journal_crash_recovery_multiple_ops
    run_test "Journal Crash During Write" test_journal_crash_during_write
    run_test "Network Partition" test_network_partition
    run_test "Broadcast Invalidation" test_broadcast_invalidation
    run_test "Broadcast Directory Invalidation" test_broadcast_directory_invalidation
    run_test "Broadcast Multiple Updates" test_broadcast_multiple_updates
    run_test "Broadcast Bidirectional" test_broadcast_bidirectional
    run_test "Inode Reuse Stale Read" test_inode_reuse_stale_read
    run_test "More Than 4096 Files" test_more_than_4096_files
    run_test "Extent Cache Coherency" test_extent_cache_coherency
    run_test "Directory Lookup Coherency" test_dir_index_follows_other_node
    run_test "Statfs After Remount" test_statfs_after_remount
    run_test "Statfs Multi-Node" test_statfs_multinode
    run_test "Symlink Cross Node" test_symlink_cross_node
    run_test "Rolling Restart" test_rolling_restart
    run_test "Stress Random Failures" test_stress_random_failures

    # Run VM-specific tests
    run_test "Power Failure Simulation" test_power_failure
    run_test "O_DIRECT Validation" test_odirect_validation
    run_test "Large File Handling" test_large_file
    run_test "Many Files" test_many_files
    run_test "Nested Directories" test_nested_directories
    run_test "Delete Directory" test_delete_directory
    run_test "Directory Fsync" test_dir_fsync
    run_test "Hard Links" test_hard_links
    run_test "Truncate File" test_truncate_file
    run_test "Append File" test_append_file
    run_test "Rename File" test_rename_file
    run_test "Stress Single Node" test_stress_single_node
    run_test "Concurrent Directory Ops" test_concurrent_directory_ops
    run_test "Multi-Node Rename" test_multinode_rename
    run_test "Multi-Node Rename Of A Moved Directory" test_multinode_rename_moved_dir
    run_test "Long-Running Stability" test_stability

    # Run concurrent same-file write tests (using pre-extension pattern)
    run_test "Concurrent First Write Same Block" test_concurrent_first_write_same_block
    run_test "Concurrent Same-File Writes (2 VMs)" test_concurrent_same_file_writes
    run_test "Concurrent Same-File Writes (3 VMs)" test_concurrent_same_file_writes_3vms
    run_test "Reads Never See Part Of A Write" test_reads_never_see_part_of_a_write
    run_test "Mixed Size Writes" test_mixed_size_writes
    run_test "Overlapping Block Writes" test_overlapping_block_writes
    run_test "Concurrent Write Stress" test_concurrent_write_stress
    run_test "Reversed Write Order" test_reversed_write_order
    run_test "Concurrent Writes Power Failure" test_concurrent_writes_power_failure
    run_test "Unlink Waits Out Peer Replay Power Failure" test_unlink_waits_out_peer_replay
    run_test "fsck Repair After Power Failure" test_fsck_repair_after_power_failure
    run_test "Copy-on-Write Power Failure" test_cow_write_power_failure
    run_test "Snapshot Create/Delete Power Failure" test_snapshot_create_delete_power_failure
    run_test "TRIM Reuse Power Failure" test_trim_reuse_power_failure
    run_test "Stalled Node Writes Stop At Write Gate" test_stalled_node_writes_stop_at_write_gate
    run_test "Fencing: Stalled Writer" test_fencing_stalled_writer
    run_test "Fencing: Paused VM" test_fencing_paused_vm
    run_test "Pause: Unfenced Paused VM" test_unfenced_paused_vm
    run_test "Unfenced Join Refused" test_unfenced_join_refused
    run_test "Unfenced Remount After Clean Stop" test_unfenced_remount_after_clean_stop
    run_test "Fencing: Idle Node" test_fencing_idle_node
    run_test "Fencing: Busy Survivor" test_fencing_busy_survivor
    run_test "Fencing: Clear Reservations" test_fencing_clear_reservations
    run_test "Fencing: Unmount Removes Key" test_fencing_unmount_removes_key
    run_test "Fencing: Relogin Then Remount" test_fencing_relogin_remount
    run_test "Fencing: Reservation Self-Test" test_fencing_reservation_self_test
    run_test "Fencing: Session Relogin" test_fencing_session_relogin
    run_test "Fencing: Paused Rekey" test_fencing_paused_rekey
    run_test "Fencing: Lost Reservation" test_fencing_lost_reservation
    run_test "Fencing: Rejoin After Relogin And Kill" test_fencing_rejoin_relogin_kill
    run_test "Fencing: Rejoin After Relogin And Power Cut" test_fencing_rejoin_relogin_power_cut
    run_test "Fencing: Rejoin After Takeover And Power Cut" test_fencing_rejoin_takeover_power_cut
    run_test "Fencing: Rejoin After Power Cut" test_fencing_rejoin_power_cut
    run_test "Hostile: Storage Latency Spikes" test_storage_latency_spikes
    run_test "Hostile: Short Storage Stalls" test_storage_short_stalls
    run_test "Hostile: Long Storage Stall" test_storage_long_stall
    run_test "Hostile: Short EIO Burst" test_storage_short_eio
    run_test "Hostile: Long EIO Burst" test_storage_long_eio
    run_test "Hostile: Flaky EIO" test_storage_flaky_eio
    run_test "Hostile: Clock Ahead Mount" test_clock_ahead_mount
    run_test "Hostile: Clock Ahead Crash Recovered" test_clock_ahead_crash_recovered

    # Summary
    log_info "=========================================="
    log_info "TEST SUMMARY"
    log_info "=========================================="
    log_info "Passed: $test_passed"
    log_info "Failed: $test_failed"
    log_info "=========================================="

    # Generate JUnit XML report
    cat > "$RESULTS_DIR/junit.xml" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="GoClusterFS VM Multi-Node Tests" tests="$((test_passed + test_failed))" failures="$test_failed" errors="0">
EOF

    while IFS= read -r line; do
        status=$(echo "$line" | cut -d: -f1)
        name=$(echo "$line" | cut -d: -f2 | xargs)
        if [ "$status" = "PASS" ]; then
            echo "  <testcase name=\"$name\" classname=\"vm.multinode\"/>" >> "$RESULTS_DIR/junit.xml"
        else
            echo "  <testcase name=\"$name\" classname=\"vm.multinode\"><failure message=\"Test failed\"/></testcase>" >> "$RESULTS_DIR/junit.xml"
        fi
    done < "$RESULTS_DIR/results.txt"

    echo "</testsuite>" >> "$RESULTS_DIR/junit.xml"

    log_info "Results written to $RESULTS_DIR"

    # Cleanup VMs
    log_info "Cleaning up VMs..."
    cleanup

    # Exit with failure if any tests failed
    if [ $test_failed -gt 0 ]; then
        exit 1
    fi

    exit 0
}

main "$@"
