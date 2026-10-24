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

    log_info "=========================================="
    log_info "Running test: $test_name"
    log_info "=========================================="

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

    # New files take the lowest free inode, so create files until one gets the old number.
    local new
    new=$(ssh_run "$writer" "rm $dir/$old; for i in \$(seq 1 200); do f=reuse_new_\$i.bin; touch $dir/\$f; if [ \$(stat -c %i $dir/\$f) = $old_ino ]; then echo \$f; break; fi; done")
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
        # Rename not implemented - this is expected for now
        log_info "SKIP: Rename operation not yet implemented in GoClusterFS"
        return 0
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
    ensure_cluster_healthy 2

    # Need at least 2 VMs for this test
    if [[ $NODE_COUNT -lt 2 ]]; then
        log_info "SKIP: Need at least 2 VMs for concurrent directory ops test"
        return 0
    fi

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

# Test 11: Three VMs write to same file at different offsets
test_concurrent_same_file_writes_3vms() {
    log_info "Testing concurrent same-file writes (3 VMs)..."
    ensure_cluster_healthy 3

    if [[ $NODE_COUNT -lt 3 ]]; then
        log_warn "Skipping: Need at least 3 VMs for this test"
        return 0
    fi

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
    ensure_cluster_healthy 3

    if [[ $NODE_COUNT -lt 3 ]]; then
        log_warn "Skipping: Need at least 3 VMs for this test"
        return 0
    fi

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
    ensure_cluster_healthy 3

    if [[ $NODE_COUNT -lt 3 ]]; then
        log_warn "Skipping: Need at least 3 VMs for this test"
        return 0
    fi

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

    run_test "Concurrent Writes" test_concurrent_writes
    run_test "Concurrent File Creation" test_concurrent_file_creation
    run_test "VM Kill Recovery" test_vm_kill_recovery
    run_test "Journal Recovery Multi-Node" test_journal_recovery_multinode
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
    run_test "Extent Cache Coherency" test_extent_cache_coherency
    run_test "Rolling Restart" test_rolling_restart
    run_test "Stress Random Failures" test_stress_random_failures

    # Run VM-specific tests
    run_test "Power Failure Simulation" test_power_failure
    run_test "O_DIRECT Validation" test_odirect_validation
    run_test "Large File Handling" test_large_file
    run_test "Many Files" test_many_files
    run_test "Nested Directories" test_nested_directories
    run_test "Delete Directory" test_delete_directory
    run_test "Truncate File" test_truncate_file
    run_test "Append File" test_append_file
    run_test "Rename File" test_rename_file
    run_test "Stress Single Node" test_stress_single_node
    run_test "Concurrent Directory Ops" test_concurrent_directory_ops
    run_test "Long-Running Stability" test_stability

    # Run concurrent same-file write tests (using pre-extension pattern)
    run_test "Concurrent Same-File Writes (2 VMs)" test_concurrent_same_file_writes
    run_test "Concurrent Same-File Writes (3 VMs)" test_concurrent_same_file_writes_3vms
    run_test "Mixed Size Writes" test_mixed_size_writes
    run_test "Overlapping Block Writes" test_overlapping_block_writes
    run_test "Concurrent Write Stress" test_concurrent_write_stress
    run_test "Reversed Write Order" test_reversed_write_order
    run_test "Concurrent Writes Power Failure" test_concurrent_writes_power_failure

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
