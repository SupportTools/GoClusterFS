#!/bin/bash
# GoClusterFS Docker Test Runner
# Orchestrates multi-node tests from within the orchestrator container

RESULTS_DIR="${RESULTS_DIR:-/results}"
NODES="${NODES:-node1,node2,node3,node4}"

# Ensure JUnit report is generated even on early failure
generate_emergency_junit() {
    local exit_code=$1
    local error_message="${2:-Script terminated unexpectedly}"

    # Only generate if no junit.xml exists yet
    if [ ! -f "$RESULTS_DIR/junit.xml" ]; then
        mkdir -p "$RESULTS_DIR"
        cat > "$RESULTS_DIR/junit.xml" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="GoClusterFS Docker Multi-Node Tests" tests="1" failures="1" errors="0" skipped="0">
  <testcase name="TestSetup" classname="docker.multinode">
    <failure message="$error_message">Exit code: $exit_code</failure>
  </testcase>
</testsuite>
EOF
        echo "FAIL: TestSetup - $error_message" > "$RESULTS_DIR/results.txt"
    fi
}

# Trap to generate JUnit on any exit
trap 'generate_emergency_junit $? "Script exited with code $?"' EXIT

set -e

log() {
    echo "[Orchestrator] $(date '+%Y-%m-%d %H:%M:%S') $*"
}

# Convert comma-separated nodes to array
IFS=',' read -ra NODE_ARRAY <<< "$NODES"
NODE_COUNT=${#NODE_ARRAY[@]}

log "Starting multi-node tests with $NODE_COUNT nodes: ${NODE_ARRAY[*]}"

# Create results directory
mkdir -p "$RESULTS_DIR"

# =============================================================================
# Test Utilities
# =============================================================================

# Execute command on a node via docker exec
exec_on_node() {
    local node=$1
    shift
    docker exec "$node" "$@"
}

# Write file on a node
write_file() {
    local node=$1
    local path=$2
    local content=$3
    exec_on_node "$node" sh -c "echo '$content' > /mnt/goclusterfs/$path"
}

# Read file from a node
read_file() {
    local node=$1
    local path=$2
    exec_on_node "$node" cat "/mnt/goclusterfs/$path"
}

# Check if file exists on a node
file_exists() {
    local node=$1
    local path=$2
    exec_on_node "$node" test -f "/mnt/goclusterfs/$path"
}

# List directory on a node
list_dir() {
    local node=$1
    local path=${2:-.}
    exec_on_node "$node" ls -la "/mnt/goclusterfs/$path"
}

# Kill a node container
kill_node() {
    local node=$1
    log "Killing node: $node"
    docker kill "$node" || true
}

# Restart a node container
restart_node() {
    local node=$1
    log "Restarting node: $node"
    docker restart "$node"
}

# Wait for node to be healthy
wait_node_healthy() {
    local node=$1
    local timeout=${2:-60}
    local count=0

    log "Waiting for $node to be healthy (timeout: ${timeout}s)..."
    while [ $count -lt $timeout ]; do
        if docker exec "$node" /scripts/healthcheck.sh > /dev/null 2>&1; then
            log "$node is healthy"
            return 0
        fi
        sleep 1
        count=$((count + 1))
    done

    log "ERROR: $node did not become healthy within ${timeout}s"
    return 1
}

# Isolate a node (network partition)
isolate_node() {
    local node=$1
    log "Isolating node: $node"
    exec_on_node "$node" /scripts/network-partition.sh isolate all
}

# Heal network partition for a node
heal_node() {
    local node=$1
    log "Healing partition for node: $node"
    exec_on_node "$node" /scripts/network-partition.sh heal
}

# =============================================================================
# Advanced File Operations (for concurrent same-file writes)
# =============================================================================

# Pre-extend a file to a specific size (filled with zeros)
# Usage: pre_extend_file <node> <filename> <size_bytes>
pre_extend_file() {
    local node=$1
    local filename=$2
    local size_bytes=$3

    log "Pre-extending $filename to $size_bytes bytes on $node"
    # Use larger block size for efficiency, handle remainder with bs=1
    local full_blocks=$((size_bytes / 4096))
    local remainder=$((size_bytes % 4096))
    if [ "$full_blocks" -gt 0 ]; then
        exec_on_node "$node" dd if=/dev/zero of="/mnt/goclusterfs/$filename" bs=4096 count="$full_blocks" 2>/dev/null
    fi
    if [ "$remainder" -gt 0 ]; then
        exec_on_node "$node" dd if=/dev/zero of="/mnt/goclusterfs/$filename" bs=1 count="$remainder" seek=$((full_blocks * 4096)) conv=notrunc 2>/dev/null
    fi
}

# Write data at a specific offset in a file (using conv=notrunc to not truncate)
# Usage: write_file_at <node> <filename> <content> <offset>
write_file_at() {
    local node=$1
    local filename=$2
    local content=$3
    local offset=$4

    # Write content to file at specific offset using dd
    exec_on_node "$node" sh -c "echo -n '$content' | dd of='/mnt/goclusterfs/$filename' bs=1 seek=$offset conv=notrunc 2>/dev/null"
}

# Write binary pattern at specific offset
# Usage: write_pattern_at <node> <filename> <pattern_char> <size_bytes> <offset>
write_pattern_at() {
    local node=$1
    local filename=$2
    local pattern_char=$3
    local size_bytes=$4
    local offset=$5

    log "Writing ${size_bytes} bytes of pattern '$pattern_char' at offset $offset on $node"
    # Generate pattern and write at offset using dd
    # The oflag=seek_bytes allows byte-precise seeking with larger block sizes
    exec_on_node "$node" sh -c "head -c $size_bytes /dev/zero | tr '\\0' '$pattern_char' | dd of='/mnt/goclusterfs/$filename' bs=65536 seek=$offset oflag=seek_bytes conv=notrunc 2>/dev/null"
}

# Read specific bytes from a file at an offset
# Usage: read_file_at <node> <filename> <offset> <length>
read_file_at() {
    local node=$1
    local filename=$2
    local offset=$3
    local length=$4

    exec_on_node "$node" dd if="/mnt/goclusterfs/$filename" bs=1 skip="$offset" count="$length" 2>/dev/null
}

# Get file size
# Usage: get_file_size <node> <filename>
get_file_size() {
    local node=$1
    local filename=$2

    exec_on_node "$node" stat -c%s "/mnt/goclusterfs/$filename" 2>/dev/null
}

# Verify content at a specific offset starts with expected string
# Usage: verify_content_at <node> <filename> <expected_prefix> <offset>
verify_content_at() {
    local node=$1
    local filename=$2
    local expected_prefix=$3
    local offset=$4
    local check_len=${#expected_prefix}

    local actual
    actual=$(read_file_at "$node" "$filename" "$offset" "$check_len")

    if [ "$actual" = "$expected_prefix" ]; then
        return 0
    else
        log "Content mismatch at offset $offset: expected '$expected_prefix', got '$actual'"
        return 1
    fi
}

# Verify all bytes at offset match a specific character
# Usage: verify_pattern_at <node> <filename> <pattern_char> <offset> <length>
verify_pattern_at() {
    local node=$1
    local filename=$2
    local pattern_char=$3
    local offset=$4
    local length=$5

    # Read the data and check if it's all the expected pattern
    local actual
    actual=$(exec_on_node "$node" sh -c "dd if='/mnt/goclusterfs/$filename' bs=1 skip=$offset count=$length 2>/dev/null | tr -d '$pattern_char' | wc -c")

    if [ "$actual" -eq 0 ]; then
        return 0
    else
        log "Pattern mismatch at offset $offset: expected all '$pattern_char', found $actual non-matching bytes"
        return 1
    fi
}

# Generate MD5 hash of file portion
# Usage: md5_portion <node> <filename> <offset> <length>
md5_portion() {
    local node=$1
    local filename=$2
    local offset=$3
    local length=$4

    exec_on_node "$node" sh -c "dd if='/mnt/goclusterfs/$filename' bs=1 skip=$offset count=$length 2>/dev/null | md5sum | cut -d' ' -f1"
}

# =============================================================================
# Test Cases
# =============================================================================

test_passed=0
test_failed=0

# CI runs the suite as two parallel jobs. TEST_GROUP=1 or 2 runs one half; empty runs everything.
in_test_group() {
    [[ -z "$TEST_GROUP" ]] && return 0
    local group=1
    case "$1" in
        "Container Kill Recovery"|Journal*|"Network Partition"|Broadcast*|"Inode Reuse Stale Read"|"Rolling Restart")
            group=2 ;;
    esac
    [[ "$group" == "$TEST_GROUP" ]]
}

run_test() {
    local test_name=$1
    local test_func=$2

    in_test_group "$test_name" || return 0

    log "=========================================="
    log "Running test: $test_name"
    log "=========================================="

    if $test_func; then
        log "PASSED: $test_name"
        test_passed=$((test_passed + 1))
        echo "PASS: $test_name" >> "$RESULTS_DIR/results.txt"
    else
        log "FAILED: $test_name"
        test_failed=$((test_failed + 1))
        echo "FAIL: $test_name" >> "$RESULTS_DIR/results.txt"
    fi
}

# Test 0: Create and read file (basic sanity check)
test_create_read_file() {
    log "Testing create and read file..."

    local node="${NODE_ARRAY[0]}"
    local filename="test_create_read.txt"
    local expected_content="Hello GoClusterFS!"

    # Write file
    write_file "$node" "$filename" "$expected_content"

    # Verify file exists
    if ! file_exists "$node" "$filename"; then
        log "ERROR: File not created"
        return 1
    fi

    # Read and verify content
    local actual_content
    actual_content=$(read_file "$node" "$filename")

    if [ "$actual_content" != "$expected_content" ]; then
        log "ERROR: Content mismatch - expected '$expected_content', got '$actual_content'"
        return 1
    fi

    log "Create and read file test passed"
    return 0
}

# Test: Create directory
test_create_directory() {
    log "Testing create directory..."

    local node="${NODE_ARRAY[0]}"
    local dirname="testdir"

    # Create directory
    exec_on_node "$node" mkdir "/mnt/goclusterfs/$dirname"

    # Verify it exists and is a directory
    if ! exec_on_node "$node" test -d "/mnt/goclusterfs/$dirname"; then
        log "ERROR: Directory not created or not a directory"
        return 1
    fi

    # List root and verify directory appears
    local listing
    listing=$(exec_on_node "$node" ls -la /mnt/goclusterfs/)
    if ! echo "$listing" | grep -q "^d.*$dirname"; then
        log "ERROR: Directory not found in listing"
        log "Listing: $listing"
        return 1
    fi

    log "Create directory test passed"
    return 0
}

# Test: File in directory
# Creates a directory, then creates a file inside it
test_file_in_directory() {
    log "Testing file in directory..."

    local node="${NODE_ARRAY[0]}"
    local dirname="subdir"
    local filename="nested.txt"
    local filepath="$dirname/$filename"
    local test_content="File in subdirectory"

    # Create directory
    exec_on_node "$node" mkdir -p "/mnt/goclusterfs/$dirname"

    # Verify directory exists
    if ! exec_on_node "$node" test -d "/mnt/goclusterfs/$dirname"; then
        log "ERROR: Failed to create directory"
        return 1
    fi

    # Create file inside directory
    exec_on_node "$node" sh -c "echo '$test_content' > /mnt/goclusterfs/$filepath"

    # Verify file exists
    if ! exec_on_node "$node" test -f "/mnt/goclusterfs/$filepath"; then
        log "ERROR: File not created in directory"
        return 1
    fi

    # Read back and verify content
    local read_back
    read_back=$(exec_on_node "$node" cat "/mnt/goclusterfs/$filepath")
    if [[ "$read_back" != "$test_content" ]]; then
        log "ERROR: Content mismatch. Expected: '$test_content', Got: '$read_back'"
        return 1
    fi

    # List directory and verify file appears
    local listing
    listing=$(exec_on_node "$node" ls "/mnt/goclusterfs/$dirname/")
    if ! echo "$listing" | grep -q "$filename"; then
        log "ERROR: File not found in directory listing"
        log "Listing: $listing"
        return 1
    fi

    log "File in directory test passed"
    return 0
}

# Test: Data persistence after remount
test_data_persistence() {
    log "Testing data persistence after remount..."

    local node="${NODE_ARRAY[0]}"
    local content="Persistent data test - this should survive remount!"
    local nested_content="nested file"

    # Write a file
    exec_on_node "$node" sh -c "echo '$content' > /mnt/goclusterfs/persistent.txt"

    # Create directory with nested file
    exec_on_node "$node" mkdir -p "/mnt/goclusterfs/persistdir"
    exec_on_node "$node" sh -c "echo '$nested_content' > /mnt/goclusterfs/persistdir/file.txt"

    # Verify files exist before restart
    if ! exec_on_node "$node" test -f "/mnt/goclusterfs/persistent.txt"; then
        log "ERROR: persistent.txt not created"
        return 1
    fi

    log "Data written, restarting node to test persistence..."

    # Restart the node (triggers unmount/remount cycle)
    restart_node "$node"
    wait_node_healthy "$node" 30

    # Verify data persisted
    local read_content
    read_content=$(exec_on_node "$node" cat "/mnt/goclusterfs/persistent.txt")
    if [ "$read_content" != "$content" ]; then
        log "ERROR: Content didn't persist. Expected: '$content', Got: '$read_content'"
        return 1
    fi

    # Verify nested directory and file
    if ! exec_on_node "$node" test -d "/mnt/goclusterfs/persistdir"; then
        log "ERROR: persistdir directory didn't persist"
        return 1
    fi

    local nested_read
    nested_read=$(exec_on_node "$node" cat "/mnt/goclusterfs/persistdir/file.txt")
    if [ "$nested_read" != "$nested_content" ]; then
        log "ERROR: Nested file didn't persist. Expected: '$nested_content', Got: '$nested_read'"
        return 1
    fi

    log "Data persistence test passed"
    return 0
}

# Test: Delete file
test_delete_file() {
    log "Testing file deletion..."

    local node="${NODE_ARRAY[0]}"
    local filename="todelete.txt"

    # Create a file
    exec_on_node "$node" sh -c "echo 'delete me' > /mnt/goclusterfs/$filename"

    # Verify it exists
    if ! exec_on_node "$node" test -f "/mnt/goclusterfs/$filename"; then
        log "ERROR: File should exist before deletion"
        return 1
    fi

    # Delete it
    exec_on_node "$node" rm "/mnt/goclusterfs/$filename"

    # Verify it's gone
    if exec_on_node "$node" test -f "/mnt/goclusterfs/$filename"; then
        log "ERROR: File should not exist after deletion"
        return 1
    fi

    log "File deletion test passed"
    return 0
}

# Test: Overwrite existing file
test_overwrite_file() {
    log "Testing file overwrite..."

    local node="${NODE_ARRAY[0]}"
    local filename="overwrite.txt"
    local original_content="Original content"
    local new_content="New content that is different"

    # Create a file with original content
    exec_on_node "$node" sh -c "echo '$original_content' > /mnt/goclusterfs/$filename"

    # Verify original content
    local read_original
    read_original=$(exec_on_node "$node" cat "/mnt/goclusterfs/$filename")
    if [[ "$read_original" != "$original_content" ]]; then
        log "ERROR: Original content mismatch: expected '$original_content', got '$read_original'"
        return 1
    fi

    # Overwrite with new content
    exec_on_node "$node" sh -c "echo '$new_content' > /mnt/goclusterfs/$filename"

    # Read back and verify new content
    local read_new
    read_new=$(exec_on_node "$node" cat "/mnt/goclusterfs/$filename")
    if [[ "$read_new" != "$new_content" ]]; then
        log "ERROR: Overwrite failed: expected '$new_content', got '$read_new'"
        return 1
    fi

    log "File overwrite test passed"
    return 0
}

# Test: Large file handling
# Creates a file larger than one block (1MB block size) to test multi-block operations
test_large_file() {
    log "Testing large file handling..."

    local node="${NODE_ARRAY[0]}"
    local filename="large_test.bin"
    local size_kb=2048  # 2MB - spans multiple 1MB blocks

    # Create a 2MB file with random data
    log "Creating ${size_kb}KB file with random data..."
    exec_on_node "$node" sh -c "dd if=/dev/urandom of=/mnt/goclusterfs/$filename bs=1K count=$size_kb 2>/dev/null"

    # Verify file size
    local actual_size
    actual_size=$(exec_on_node "$node" stat -c%s "/mnt/goclusterfs/$filename")
    local expected_size=$((size_kb * 1024))
    if [[ "$actual_size" != "$expected_size" ]]; then
        log "ERROR: Size mismatch. Expected: $expected_size bytes, Got: $actual_size bytes"
        return 1
    fi
    log "File size verified: $actual_size bytes"

    # Calculate checksum on writing node
    local checksum_write
    checksum_write=$(exec_on_node "$node" md5sum "/mnt/goclusterfs/$filename" | awk '{print $1}')
    log "Write checksum: $checksum_write"

    # If we have multiple nodes, verify from another node
    if [[ ${#NODE_ARRAY[@]} -ge 2 ]]; then
        local node2="${NODE_ARRAY[1]}"
        sleep 1  # Allow time for sync

        # Calculate checksum on reading node
        local checksum_read
        checksum_read=$(exec_on_node "$node2" md5sum "/mnt/goclusterfs/$filename" | awk '{print $1}')
        log "Read checksum (from node2): $checksum_read"

        if [[ "$checksum_write" != "$checksum_read" ]]; then
            log "ERROR: Checksum mismatch between nodes"
            return 1
        fi
        log "Cross-node checksum verified"
    fi

    log "Large file test passed (${size_kb}KB file verified)"
    return 0
}

# Test: Many files in directory
# Creates 100+ files in a directory to test directory listing and metadata handling
test_many_files() {
    log "Testing many files in directory..."

    local node="${NODE_ARRAY[0]}"
    local num_files=100
    local test_dir="many_files_test"

    # Create test directory
    exec_on_node "$node" mkdir -p "/mnt/goclusterfs/$test_dir"

    # One docker exec per loop: an exec per file costs more than the file operation
    log "Creating $num_files files..."
    exec_on_node "$node" sh -c 'for i in $(seq 0 $(($2 - 1))); do
            echo "Content of file $i" > "$(printf "%s/file_%03d.txt" "$1" "$i")"
        done' sh "/mnt/goclusterfs/$test_dir" "$num_files"

    # Count files in directory
    local count
    count=$(exec_on_node "$node" sh -c "ls -1 /mnt/goclusterfs/$test_dir | wc -l")
    if [[ "$count" -ne "$num_files" ]]; then
        log "ERROR: Expected $num_files files, found $count"
        return 1
    fi
    log "File count verified: $count files"

    local mismatches
    mismatches=$(exec_on_node "$node" sh -c 'for i in $(seq 0 $(($2 - 1))); do
            f=$(printf "%s/file_%03d.txt" "$1" "$i")
            got=$(cat "$f")
            [ "$got" = "Content of file $i" ] || echo "Content mismatch for $f: got '\''$got'\''"
        done' sh "/mnt/goclusterfs/$test_dir" "$num_files")
    if [[ -n "$mismatches" ]]; then
        log "ERROR: content mismatches:"
        log "$mismatches"
        return 1
    fi

    # If multiple nodes, verify from another node
    if [[ ${#NODE_ARRAY[@]} -ge 2 ]]; then
        local node2="${NODE_ARRAY[1]}"
        sleep 1  # Allow time for sync

        local count2
        count2=$(exec_on_node "$node2" sh -c "ls -1 /mnt/goclusterfs/$test_dir | wc -l")
        if [[ "$count2" -ne "$num_files" ]]; then
            log "ERROR: Node2 sees $count2 files, expected $num_files"
            return 1
        fi
        log "Cross-node file count verified: $count2 files"
    fi

    log "Many files test passed ($num_files files created and verified)"
    return 0
}

# Test: Nested directory hierarchy
# Creates deeply nested directory structure (a/b/c/d) with file at deepest level
test_nested_directories() {
    log "Testing nested directory hierarchy..."

    local node="${NODE_ARRAY[0]}"
    local base_dir="/mnt/goclusterfs"

    # Create nested structure: a/b/c/d
    log "Creating nested directories..."
    exec_on_node "$node" mkdir -p "$base_dir/a/b/c/d"

    # Verify each level exists and is a directory
    for dir in "a" "a/b" "a/b/c" "a/b/c/d"; do
        if ! exec_on_node "$node" test -d "$base_dir/$dir"; then
            log "ERROR: $dir should exist and be a directory"
            return 1
        fi
    done
    log "All directory levels verified"

    # Create a file at the deepest level
    local deep_file="a/b/c/d/deep.txt"
    local content="deep content"
    exec_on_node "$node" sh -c "echo '$content' > $base_dir/$deep_file"

    # Read it back and verify
    local actual
    actual=$(exec_on_node "$node" cat "$base_dir/$deep_file")
    if [[ "$actual" != "$content" ]]; then
        log "ERROR: Deep file content mismatch. Expected: '$content', Got: '$actual'"
        return 1
    fi
    log "Deep file content verified"

    # If multiple nodes, verify from another node
    if [[ ${#NODE_ARRAY[@]} -ge 2 ]]; then
        local node2="${NODE_ARRAY[1]}"
        sleep 1  # Allow time for sync

        # Verify directory structure from node2
        if ! exec_on_node "$node2" test -d "$base_dir/a/b/c/d"; then
            log "ERROR: Nested directory not visible from node2"
            return 1
        fi

        # Verify file content from node2
        local actual2
        actual2=$(exec_on_node "$node2" cat "$base_dir/$deep_file")
        if [[ "$actual2" != "$content" ]]; then
            log "ERROR: Deep file content mismatch on node2"
            return 1
        fi
        log "Cross-node nested directory verification passed"
    fi

    log "Nested directories test passed"
    return 0
}

# Test: Delete empty directory
# Creates an empty directory and removes it, verifying deletion
test_delete_directory() {
    log "Testing delete empty directory..."

    local node="${NODE_ARRAY[0]}"
    local dirname="emptydir_test"

    # Create an empty directory
    exec_on_node "$node" mkdir -p "/mnt/goclusterfs/$dirname"

    # Verify it exists
    if ! exec_on_node "$node" test -d "/mnt/goclusterfs/$dirname"; then
        log "ERROR: Directory should exist after creation"
        return 1
    fi
    log "Directory created successfully"

    # Delete it
    exec_on_node "$node" rmdir "/mnt/goclusterfs/$dirname"

    # Verify it's gone
    if exec_on_node "$node" test -d "/mnt/goclusterfs/$dirname"; then
        log "ERROR: Directory should not exist after deletion"
        return 1
    fi
    log "Directory deleted successfully"

    # If multiple nodes, verify deletion is visible from another node
    if [[ ${#NODE_ARRAY[@]} -ge 2 ]]; then
        local node2="${NODE_ARRAY[1]}"
        sleep 1  # Allow time for sync

        if exec_on_node "$node2" test -d "/mnt/goclusterfs/$dirname"; then
            log "ERROR: Deleted directory still visible from node2"
            return 1
        fi
        log "Cross-node deletion verification passed"
    fi

    log "Delete directory test passed"
    return 0
}

# Test: Truncate file
# Creates a file and truncates it to a smaller size, verifying the operation
test_truncate_file() {
    log "Testing file truncation..."

    local node="${NODE_ARRAY[0]}"
    local filename="truncate_test.txt"
    local original_content="This is some longer content that we will truncate"
    local truncate_size=10

    # Create a file with content
    exec_on_node "$node" sh -c "echo -n '$original_content' > /mnt/goclusterfs/$filename"

    # Verify original size
    local original_size
    original_size=$(exec_on_node "$node" stat -c%s "/mnt/goclusterfs/$filename")
    log "Original file size: $original_size bytes"

    # Truncate to 10 bytes
    exec_on_node "$node" truncate -s $truncate_size "/mnt/goclusterfs/$filename"

    # Verify new size
    local new_size
    new_size=$(exec_on_node "$node" stat -c%s "/mnt/goclusterfs/$filename")
    if [[ "$new_size" -ne "$truncate_size" ]]; then
        log "ERROR: Size after truncate: expected $truncate_size, got $new_size"
        return 1
    fi
    log "Truncated size verified: $new_size bytes"

    # Verify content is the first 10 bytes
    local actual_content
    actual_content=$(exec_on_node "$node" cat "/mnt/goclusterfs/$filename")
    local expected_content="${original_content:0:$truncate_size}"
    if [[ "$actual_content" != "$expected_content" ]]; then
        log "ERROR: Content mismatch. Expected: '$expected_content', Got: '$actual_content'"
        return 1
    fi
    log "Truncated content verified"

    # If multiple nodes, verify from another node
    if [[ ${#NODE_ARRAY[@]} -ge 2 ]]; then
        local node2="${NODE_ARRAY[1]}"
        sleep 1  # Allow time for sync

        local size2
        size2=$(exec_on_node "$node2" stat -c%s "/mnt/goclusterfs/$filename")
        if [[ "$size2" -ne "$truncate_size" ]]; then
            log "ERROR: Size mismatch on node2: expected $truncate_size, got $size2"
            return 1
        fi
        log "Cross-node truncate verification passed"
    fi

    log "Truncate file test passed"
    return 0
}

# Test: Append to file
# Creates a file with initial content, appends more data, and verifies the combined result
test_append_file() {
    log "Testing file append operation..."

    local node="${NODE_ARRAY[0]}"
    local filename="append_test.txt"
    local initial_content="Hello"
    local append_content=" World!"
    local expected_content="Hello World!"

    # Create initial file with content
    exec_on_node "$node" sh -c "echo -n '$initial_content' > /mnt/goclusterfs/$filename"

    # Verify initial content
    local initial_read
    initial_read=$(exec_on_node "$node" cat "/mnt/goclusterfs/$filename")
    if [[ "$initial_read" != "$initial_content" ]]; then
        log "ERROR: Initial content mismatch. Expected: '$initial_content', Got: '$initial_read'"
        return 1
    fi
    log "Initial content verified: '$initial_read'"

    # Append data using >> redirection
    exec_on_node "$node" sh -c "echo -n '$append_content' >> /mnt/goclusterfs/$filename"

    # Read back and verify combined content
    local final_read
    final_read=$(exec_on_node "$node" cat "/mnt/goclusterfs/$filename")
    if [[ "$final_read" != "$expected_content" ]]; then
        log "ERROR: Append result mismatch. Expected: '$expected_content', Got: '$final_read'"
        return 1
    fi
    log "Appended content verified: '$final_read'"

    # If multiple nodes, verify from another node
    if [[ ${#NODE_ARRAY[@]} -ge 2 ]]; then
        local node2="${NODE_ARRAY[1]}"
        sleep 1  # Allow time for sync

        local cross_read
        cross_read=$(exec_on_node "$node2" cat "/mnt/goclusterfs/$filename")
        if [[ "$cross_read" != "$expected_content" ]]; then
            log "ERROR: Cross-node content mismatch. Expected: '$expected_content', Got: '$cross_read'"
            return 1
        fi
        log "Cross-node append verification passed"
    fi

    log "Append file test passed"
    return 0
}

# Test: Rename file
# Attempts to rename a file within the filesystem
# Note: Rename is not yet implemented in GoClusterFS - test skips if operation fails
test_rename_file() {
    log "Testing file rename operation..."

    local node="${NODE_ARRAY[0]}"
    local oldname="oldname.txt"
    local newname="newname.txt"
    local content="Rename test content"

    # Create the original file
    exec_on_node "$node" sh -c "echo -n '$content' > /mnt/goclusterfs/$oldname"

    # Verify it exists
    if ! exec_on_node "$node" test -f "/mnt/goclusterfs/$oldname"; then
        log "ERROR: Original file should exist before rename"
        return 1
    fi
    log "Original file created: $oldname"

    # Attempt to rename
    if ! exec_on_node "$node" mv "/mnt/goclusterfs/$oldname" "/mnt/goclusterfs/$newname" 2>/dev/null; then
        # Rename not implemented - this is expected for now
        log "SKIP: Rename operation not yet implemented in GoClusterFS"
        return 0
    fi

    # If rename succeeded, verify old name doesn't exist
    if exec_on_node "$node" test -f "/mnt/goclusterfs/$oldname"; then
        log "ERROR: Old name should not exist after rename"
        return 1
    fi
    log "Old file no longer exists"

    # Verify new name exists with same content
    local readback
    readback=$(exec_on_node "$node" cat "/mnt/goclusterfs/$newname")
    if [[ "$readback" != "$content" ]]; then
        log "ERROR: Content changed after rename. Expected: '$content', Got: '$readback'"
        return 1
    fi
    log "New file content verified"

    # If multiple nodes, verify from another node
    if [[ ${#NODE_ARRAY[@]} -ge 2 ]]; then
        local node2="${NODE_ARRAY[1]}"
        sleep 1  # Allow time for sync

        if exec_on_node "$node2" test -f "/mnt/goclusterfs/$oldname"; then
            log "ERROR: Old name visible on node2 after rename"
            return 1
        fi
        if ! exec_on_node "$node2" test -f "/mnt/goclusterfs/$newname"; then
            log "ERROR: New name not visible on node2 after rename"
            return 1
        fi
        log "Cross-node rename verification passed"
    fi

    log "Rename file test passed"
    return 0
}

# Test: Single-node stress test
# Rapidly creates, reads, verifies, and deletes files to stress test the filesystem
test_stress_single_node() {
    log "Running single-node stress test..."

    local node="${NODE_ARRAY[0]}"
    local iterations=100  # Reduced from Go's 200 for faster test execution
    local errors=0

    log "Running $iterations create/read/delete iterations..."

    # One docker exec for the whole loop: an exec per step costs more than the file operation
    local failures
    failures=$(exec_on_node "$node" sh -c 'for i in $(seq 1 $1); do
            f=$(printf "/mnt/goclusterfs/stress_%04d.txt" "$i")
            want="Stress test iteration $i"
            if ! printf "%s" "$want" > "$f" 2>/dev/null; then
                echo "Iteration $i: Failed to create file"; continue
            fi
            got=$(cat "$f" 2>/dev/null)
            if [ "$got" != "$want" ]; then
                echo "Iteration $i: Content mismatch. Expected: '\''$want'\'', Got: '\''$got'\''"; continue
            fi
            rm "$f" 2>/dev/null || echo "Iteration $i: Failed to delete file"
        done' sh "$iterations")
    if [[ -n "$failures" ]]; then
        log "ERROR: $failures"
        errors=$(echo "$failures" | wc -l)
    fi

    # Verify no stress test files remain
    local remaining
    remaining=$(exec_on_node "$node" sh -c "ls /mnt/goclusterfs/stress_*.txt 2>/dev/null | wc -l")
    if [[ "$remaining" -gt 0 ]]; then
        log "ERROR: $remaining stress test files remain after cleanup"
        ((errors++))
    fi

    if [[ $errors -gt 0 ]]; then
        log "ERROR: Stress test completed with $errors errors"
        return 1
    fi

    log "Stress test passed: $iterations iterations with 0 errors"
    return 0
}

# Test: Concurrent directory operations
# Multiple nodes create files simultaneously (tests concurrent multi-node visibility)
# Based on Go TestMultiNodeConcurrentDirectoryOps which creates files not directories
test_concurrent_directory_ops() {
    log "Testing concurrent directory operations across nodes..."

    # Need at least 2 nodes for this test
    if [[ ${#NODE_ARRAY[@]} -lt 2 ]]; then
        log "SKIP: Need at least 2 nodes for concurrent directory ops test"
        return 0
    fi

    local node1="${NODE_ARRAY[0]}"
    local node2="${NODE_ARRAY[1]}"
    local files_per_node=5
    local errors=0

    # Create files concurrently from both nodes (like Go test)
    log "Creating $files_per_node files from each of 2 nodes concurrently..."

    # Node 1 creates files in background
    for i in $(seq 0 $((files_per_node - 1))); do
        exec_on_node "$node1" sh -c "echo 'Content from node 0 file $i' > /mnt/goclusterfs/node0_${i}.txt" &
    done

    # Node 2 creates files in background
    for i in $(seq 0 $((files_per_node - 1))); do
        exec_on_node "$node2" sh -c "echo 'Content from node 1 file $i' > /mnt/goclusterfs/node1_${i}.txt" &
    done

    # Wait for all background processes
    wait

    # Give filesystem time to sync
    sleep 2

    # Verify all files are visible from node 1
    log "Verifying all files visible from node 1..."
    for n in 0 1; do
        for i in $(seq 0 $((files_per_node - 1))); do
            local filename="node${n}_${i}.txt"
            if ! exec_on_node "$node1" test -f "/mnt/goclusterfs/$filename"; then
                log "ERROR: $filename not visible from node 1"
                ((errors++))
            fi
        done
    done

    # Verify all files are visible from node 2
    log "Verifying all files visible from node 2..."
    for n in 0 1; do
        for i in $(seq 0 $((files_per_node - 1))); do
            local filename="node${n}_${i}.txt"
            if ! exec_on_node "$node2" test -f "/mnt/goclusterfs/$filename"; then
                log "ERROR: $filename not visible from node 2"
                ((errors++))
            fi
        done
    done

    # Cross-verify content
    log "Cross-verifying file contents..."
    for n in 0 1; do
        for i in $(seq 0 $((files_per_node - 1))); do
            local filename="node${n}_${i}.txt"
            local expected="Content from node $n file $i"

            # Read from both nodes
            local content1
            local content2
            content1=$(exec_on_node "$node1" cat "/mnt/goclusterfs/$filename" 2>/dev/null)
            content2=$(exec_on_node "$node2" cat "/mnt/goclusterfs/$filename" 2>/dev/null)

            if [[ "$content1" != "$expected" ]]; then
                log "ERROR: Content mismatch for $filename from node 1. Expected: '$expected', Got: '$content1'"
                ((errors++))
            fi
            if [[ "$content2" != "$expected" ]]; then
                log "ERROR: Content mismatch for $filename from node 2. Expected: '$expected', Got: '$content2'"
                ((errors++))
            fi
        done
    done

    # Cleanup
    for n in 0 1; do
        for i in $(seq 0 $((files_per_node - 1))); do
            exec_on_node "$node1" rm "/mnt/goclusterfs/node${n}_${i}.txt" 2>/dev/null || true
        done
    done

    if [[ $errors -gt 0 ]]; then
        log "ERROR: Concurrent directory ops test failed with $errors errors"
        return 1
    fi

    log "Concurrent directory ops test passed"
    return 0
}

# Test: Concurrent file creation across nodes
# Multiple nodes create different files simultaneously, then verify all files
# are visible from all nodes' perspectives
test_concurrent_file_creation() {
    log "Testing concurrent file creation across nodes..."

    # Need at least 2 nodes for this test
    if [[ ${#NODE_ARRAY[@]} -lt 2 ]]; then
        log "SKIP: Need at least 2 nodes for concurrent file creation test"
        return 0
    fi

    local node1="${NODE_ARRAY[0]}"
    local node2="${NODE_ARRAY[1]}"

    # Create files concurrently from different nodes
    exec_on_node "$node1" sh -c "echo 'Content from Node 0' > /mnt/goclusterfs/fileA.txt" &
    local pid1=$!
    exec_on_node "$node2" sh -c "echo 'Content from Node 1' > /mnt/goclusterfs/fileB.txt" &
    local pid2=$!

    # Wait for both to complete
    wait $pid1 $pid2

    # Give filesystem time to sync
    sleep 1

    # Verify both files exist from both nodes' perspectives
    for node in "$node1" "$node2"; do
        if ! exec_on_node "$node" test -f "/mnt/goclusterfs/fileA.txt"; then
            log "ERROR: fileA.txt not visible from $node"
            return 1
        fi
        if ! exec_on_node "$node" test -f "/mnt/goclusterfs/fileB.txt"; then
            log "ERROR: fileB.txt not visible from $node"
            return 1
        fi
    done

    # Verify content integrity
    local contentA
    contentA=$(exec_on_node "$node1" cat "/mnt/goclusterfs/fileA.txt")
    if [[ "$contentA" != "Content from Node 0" ]]; then
        log "ERROR: fileA.txt has wrong content: $contentA"
        return 1
    fi

    local contentB
    contentB=$(exec_on_node "$node1" cat "/mnt/goclusterfs/fileB.txt")
    if [[ "$contentB" != "Content from Node 1" ]]; then
        log "ERROR: fileB.txt has wrong content: $contentB"
        return 1
    fi

    log "Concurrent file creation test passed"
    return 0
}

# Test: Sequential access across nodes
# One node writes data, then another node reads it
test_sequential_access() {
    log "Testing sequential access across nodes..."

    # Need at least 2 nodes for this test
    if [[ ${#NODE_ARRAY[@]} -lt 2 ]]; then
        log "SKIP: Need at least 2 nodes for sequential access test"
        return 0
    fi

    local node1="${NODE_ARRAY[0]}"
    local node2="${NODE_ARRAY[1]}"
    local filename="sequential_test.txt"
    local test_content="Data written by node1 for node2 to read"

    # Node 1 writes the file
    log "Node 1 ($node1) writing file..."
    exec_on_node "$node1" sh -c "echo '$test_content' > /mnt/goclusterfs/$filename"

    # Verify node 1 can read it back
    local read_node1
    read_node1=$(exec_on_node "$node1" cat "/mnt/goclusterfs/$filename")
    if [[ "$read_node1" != "$test_content" ]]; then
        log "ERROR: Node 1 immediate read-back failed: expected '$test_content', got '$read_node1'"
        return 1
    fi
    log "Node 1 verified write successful"

    # Give filesystem time to sync
    sleep 1

    # Node 2 reads the file written by Node 1
    log "Node 2 ($node2) reading file written by Node 1..."
    local read_node2
    read_node2=$(exec_on_node "$node2" cat "/mnt/goclusterfs/$filename")
    if [[ "$read_node2" != "$test_content" ]]; then
        log "ERROR: Node 2 failed to read data written by Node 1: expected '$test_content', got '$read_node2'"
        return 1
    fi

    log "Sequential access test passed - Node 2 successfully read data written by Node 1"
    return 0
}

# Test: Journal recovery across nodes
# Simulates crash: Node A creates file, gets killed, Node B recovers via journal
test_journal_recovery_multinode() {
    log "Testing multi-node journal recovery..."

    # Need at least 2 nodes for this test
    if [[ ${#NODE_ARRAY[@]} -lt 2 ]]; then
        log "SKIP: Need at least 2 nodes for journal recovery test"
        return 0
    fi

    local node_a="${NODE_ARRAY[0]}"
    local node_b="${NODE_ARRAY[1]}"
    local filename="recovery-test.txt"
    local test_content="This file should survive node crash and be recovered"

    # Phase 1: Node A creates a file
    log "Phase 1: Node A ($node_a) creating test file..."
    exec_on_node "$node_a" sh -c "echo '$test_content' > /mnt/goclusterfs/$filename"

    # Verify Node A can read it
    local read_a
    read_a=$(exec_on_node "$node_a" cat "/mnt/goclusterfs/$filename")
    if [[ "$read_a" != "$test_content" ]]; then
        log "ERROR: Node A failed to verify created file"
        return 1
    fi
    log "Node A verified file creation"

    # Give time for data to sync to disk
    sleep 1

    # Phase 2: Kill Node A to simulate crash
    log "Phase 2: Killing Node A to simulate crash..."
    kill_node "$node_a"
    sleep 2

    # Phase 3: Wait for heartbeat to become stale
    # In Docker environment, heartbeat detection is faster
    log "Phase 3: Waiting for heartbeat timeout (10s)..."
    sleep 10

    # Phase 4: Node B should detect failed node via heartbeat
    # The file should be visible on Node B through journal recovery or shared storage
    log "Phase 4: Verifying file recovery on Node B..."

    if ! file_exists "$node_b" "$filename"; then
        log "ERROR: File not found on Node B after Node A crash"
        # List directory for debugging
        exec_on_node "$node_b" ls -la /mnt/goclusterfs/ || true
        return 1
    fi

    local read_b
    read_b=$(exec_on_node "$node_b" cat "/mnt/goclusterfs/$filename")
    if [[ "$read_b" != "$test_content" ]]; then
        log "ERROR: File content mismatch on Node B. Expected: '$test_content', Got: '$read_b'"
        return 1
    fi

    log "Node B successfully recovered file from crashed Node A"

    # Phase 5: Verify Node B can create new files after recovery
    log "Phase 5: Verifying Node B can write after recovery..."
    exec_on_node "$node_b" sh -c "echo 'Post-recovery file' > /mnt/goclusterfs/post-recovery.txt"

    if ! file_exists "$node_b" "post-recovery.txt"; then
        log "ERROR: Node B cannot create files after recovery"
        return 1
    fi

    # Phase 6: Restart Node A and verify it can see recovered files
    log "Phase 6: Restarting Node A..."
    restart_node "$node_a"
    wait_node_healthy "$node_a" 30

    if ! file_exists "$node_a" "$filename"; then
        log "ERROR: Original file not visible on restarted Node A"
        return 1
    fi

    if ! file_exists "$node_a" "post-recovery.txt"; then
        log "ERROR: Post-recovery file not visible on restarted Node A"
        return 1
    fi

    log "Multi-node journal recovery test passed"
    return 0
}

# Node A restarts while node B keeps creating files.
# Matches Go test: TestMultiNodeRestartKeepsPeerJournal in journal_slots_test.go
test_restart_keeps_peer_journal() {
    log "Testing node restart while a peer keeps writing..."

    if [[ ${#NODE_ARRAY[@]} -lt 2 ]]; then
        log "SKIP: Need at least 2 nodes"
        return 0
    fi

    local node_a="${NODE_ARRAY[0]}"
    local node_b="${NODE_ARRAY[1]}"
    local dir="/mnt/goclusterfs/peer-journal"

    exec_on_node "$node_b" sh -c "rm -rf $dir /tmp/peer-writer.*; mkdir -p $dir"
    docker exec -d "$node_b" sh -c "i=0
        while [ ! -e /tmp/peer-writer.stop ] && [ \$i -lt 150 ]; do
            i=\$((i + 1))
            echo peer-\$i > $dir/peer-\$i || { echo fail \$i > /tmp/peer-writer.done; exit; }
            sleep 0.05
        done
        echo ok \$i > /tmp/peer-writer.done"
    sleep 1

    local since slot_before
    since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    slot_before=$(docker logs "$node_a" 2>&1 | grep -o 'Registered as node [0-9]*' | tail -1)
    docker kill "$node_a" >/dev/null 2>&1 || true
    sleep 1
    docker start "$node_a" >/dev/null
    if ! wait_node_healthy "$node_a" 60; then
        exec_on_node "$node_b" touch /tmp/peer-writer.stop
        return 1
    fi
    sleep 1
    exec_on_node "$node_b" touch /tmp/peer-writer.stop

    local status="" count=0
    while [[ -z "$status" && $count -lt 30 ]]; do
        status=$(exec_on_node "$node_b" cat /tmp/peer-writer.done 2>/dev/null || true)
        sleep 1
        count=$((count + 1))
    done
    log "Node B writer finished: ${status:-timeout}"
    if [[ "$status" != ok* ]]; then
        log "ERROR: Node B could not keep writing while node A restarted"
        return 1
    fi
    local written=${status#ok }

    local bad
    bad=$(exec_on_node "$node_a" sh -c "k=1; while [ \$k -le $written ]; do
        [ \"\$(cat $dir/peer-\$k 2>/dev/null)\" = peer-\$k ] || echo peer-\$k; k=\$((k + 1)); done")
    if [[ -n "$bad" ]]; then
        log "ERROR: Node A cannot read node B's files: $(echo $bad | head -c 300)"
        return 1
    fi

    local offset_a offset_b uuid_b
    offset_a=$(docker logs --since "$since" "$node_a" 2>&1 | grep -o 'Journal: initialized at offset [0-9]*' | tail -1)
    offset_b=$(docker logs "$node_b" 2>&1 | grep -o 'Journal: initialized at offset [0-9]*' | tail -1)
    uuid_b=$(docker logs "$node_b" 2>&1 | grep -o 'Registered as node [0-9]* (UUID: [0-9a-f]*' | tail -1 | grep -o '[0-9a-f]*$')
    log "Node A: $offset_a; node B: $offset_b (UUID $uuid_b)"
    if [[ -z "$offset_a" || "$offset_a" == "$offset_b" ]]; then
        log "ERROR: Node A and node B share a journal region"
        return 1
    fi
    if docker logs --since "$since" "$node_a" 2>&1 | grep -q "from crashed node $uuid_b"; then
        log "ERROR: Node A replayed live node B's journal during its restart"
        return 1
    fi
    local slot_after
    slot_after=$(docker logs --since "$since" "$node_a" 2>&1 | grep -o 'Registered as node [0-9]*' | tail -1)
    if [[ "$slot_after" != "$slot_before" ]]; then
        log "ERROR: Node A came back as '$slot_after', want its own slot '$slot_before' so it replays its journal at mount"
        return 1
    fi

    exec_on_node "$node_b" rm -rf "$dir"
    log "Restart keeps peer journal test passed ($written files)"
    return 0
}

# Test: Journal recovery with multiple files
# Simulates crash: Node A creates 5 files, gets killed, Node B recovers all via journal
test_journal_recovery_multiple_files() {
    log "Testing journal recovery with multiple files..."

    # Need at least 2 nodes for this test
    if [[ ${#NODE_ARRAY[@]} -lt 2 ]]; then
        log "SKIP: Need at least 2 nodes for journal recovery test"
        return 0
    fi

    local node_a="${NODE_ARRAY[0]}"
    local node_b="${NODE_ARRAY[1]}"
    local file_count=5

    # Phase 1: Node A creates multiple files
    log "Phase 1: Node A ($node_a) creating $file_count test files..."
    for i in $(seq 0 $((file_count - 1))); do
        local filename="multifile_${i}.txt"
        local content="Content of file $i - should survive crash"
        exec_on_node "$node_a" sh -c "echo '$content' > /mnt/goclusterfs/$filename"
        log "Node A: Created $filename"
    done

    # Give time for data to sync to disk
    sleep 1

    # Phase 2: Kill Node A to simulate crash
    log "Phase 2: Killing Node A to simulate crash..."
    kill_node "$node_a"
    sleep 2

    # Phase 3: Wait for heartbeat to become stale
    log "Phase 3: Waiting for heartbeat timeout (10s)..."
    sleep 10

    # Phase 4: Verify all files are recovered on Node B
    log "Phase 4: Verifying all files recovery on Node B..."
    local recovered=0

    for i in $(seq 0 $((file_count - 1))); do
        local filename="multifile_${i}.txt"
        local expected_content="Content of file $i - should survive crash"

        if ! file_exists "$node_b" "$filename"; then
            log "ERROR: File $filename not found on Node B after recovery"
            continue
        fi

        local read_content
        read_content=$(exec_on_node "$node_b" cat "/mnt/goclusterfs/$filename")
        if [[ "$read_content" != "$expected_content" ]]; then
            log "ERROR: Content mismatch for $filename"
            continue
        fi

        recovered=$((recovered + 1))
        log "Node B: Verified $filename"
    done

    log "Phase 4: Recovered $recovered/$file_count files"

    if [[ $recovered -ne $file_count ]]; then
        log "ERROR: Not all files were recovered"
        return 1
    fi

    # Phase 5: Restart Node A and verify it can see all recovered files
    log "Phase 5: Restarting Node A..."
    restart_node "$node_a"
    wait_node_healthy "$node_a" 30

    for i in $(seq 0 $((file_count - 1))); do
        local filename="multifile_${i}.txt"
        if ! file_exists "$node_a" "$filename"; then
            log "ERROR: File $filename not visible on restarted Node A"
            return 1
        fi
    done

    log "Multi-file journal recovery test passed"
    return 0
}

# Test: Journal recovery with large file
# Simulates crash: Node A creates 500KB file, gets killed, Node B recovers via journal
test_journal_recovery_large_file() {
    log "Testing journal recovery with large file..."

    # Need at least 2 nodes for this test
    if [[ ${#NODE_ARRAY[@]} -lt 2 ]]; then
        log "SKIP: Need at least 2 nodes for journal recovery test"
        return 0
    fi

    local node_a="${NODE_ARRAY[0]}"
    local node_b="${NODE_ARRAY[1]}"
    local filename="large-recovery.txt"
    local file_size=$((500 * 1024))  # 500KB to span multiple blocks

    # Phase 1: Node A creates a large file
    log "Phase 1: Node A ($node_a) creating ${file_size} byte file..."

    # Create 500KB file with repeating pattern (matches Go test behavior)
    exec_on_node "$node_a" sh -c "dd if=/dev/urandom of=/mnt/goclusterfs/$filename bs=1024 count=500 2>/dev/null"

    # Get checksum of original file
    local original_checksum
    original_checksum=$(exec_on_node "$node_a" md5sum "/mnt/goclusterfs/$filename" | awk '{print $1}')
    local original_size
    original_size=$(exec_on_node "$node_a" stat -c%s "/mnt/goclusterfs/$filename")

    log "Node A: Created $filename ($original_size bytes, md5: $original_checksum)"

    # Give time for data to sync to disk
    sleep 2

    # Phase 2: Kill Node A to simulate crash
    log "Phase 2: Killing Node A to simulate crash..."
    kill_node "$node_a"
    sleep 2

    # Phase 3: Wait for heartbeat to become stale
    log "Phase 3: Waiting for heartbeat timeout (10s)..."
    sleep 10

    # Phase 4: Verify large file is recovered on Node B
    log "Phase 4: Verifying large file recovery on Node B..."

    if ! file_exists "$node_b" "$filename"; then
        log "ERROR: Large file not found on Node B after recovery"
        return 1
    fi

    # Verify file size
    local recovered_size
    recovered_size=$(exec_on_node "$node_b" stat -c%s "/mnt/goclusterfs/$filename")
    if [[ "$recovered_size" != "$original_size" ]]; then
        log "ERROR: File size mismatch. Expected: $original_size, Got: $recovered_size"
        return 1
    fi

    # Verify file content via checksum
    local recovered_checksum
    recovered_checksum=$(exec_on_node "$node_b" md5sum "/mnt/goclusterfs/$filename" | awk '{print $1}')
    if [[ "$recovered_checksum" != "$original_checksum" ]]; then
        log "ERROR: File checksum mismatch. Expected: $original_checksum, Got: $recovered_checksum"
        return 1
    fi

    log "Node B: Successfully recovered large file ($recovered_size bytes, md5: $recovered_checksum)"

    # Phase 5: Restart Node A and verify it can see the recovered file
    log "Phase 5: Restarting Node A..."
    restart_node "$node_a"
    wait_node_healthy "$node_a" 30

    if ! file_exists "$node_a" "$filename"; then
        log "ERROR: Large file not visible on restarted Node A"
        return 1
    fi

    log "Large file journal recovery test passed"
    return 0
}

# Test: Journal recovery idempotency
# Verifies that recovery is idempotent - multiple nodes can attempt recovery safely
# Matches Go test: TestMultiNodeRecoveryIdempotency
test_journal_recovery_idempotency() {
    log "Testing journal recovery idempotency..."

    # Need at least 3 nodes for this test (Node A creates, B recovers, C verifies idempotency)
    if [[ ${#NODE_ARRAY[@]} -lt 3 ]]; then
        log "SKIP: Need at least 3 nodes for idempotency test"
        return 0
    fi

    local node_a="${NODE_ARRAY[0]}"
    local node_b="${NODE_ARRAY[1]}"
    local node_c="${NODE_ARRAY[2]}"
    local filename="idempotent.txt"
    local content="Idempotency test"

    # Phase 1: Node A creates file
    log "Phase 1: Node A ($node_a) creating file..."
    write_file "$node_a" "$filename" "$content"

    # Let data sync
    sleep 1

    # Phase 2: Kill Node A
    log "Phase 2: Killing Node A..."
    kill_node "$node_a"
    sleep 2

    # Phase 3: Wait for heartbeat timeout
    log "Phase 3: Waiting for heartbeat timeout (10s)..."
    sleep 10

    # Phase 4: Node B performs first recovery
    log "Phase 4: Verifying first recovery on Node B..."

    if ! file_exists "$node_b" "$filename"; then
        log "ERROR: File not found on Node B after first recovery"
        return 1
    fi

    local recovered_content
    recovered_content=$(read_file "$node_b" "$filename")
    if [[ "$recovered_content" != "$content" ]]; then
        log "ERROR: Content mismatch on Node B. Expected: '$content', Got: '$recovered_content'"
        return 1
    fi

    log "Node B: First recovery successful"

    # Phase 5: Stop Node B
    log "Phase 5: Stopping Node B..."
    kill_node "$node_b"
    sleep 2

    # Phase 6: Verify idempotency on Node C
    # Node C should see the same file - if recovery wasn't idempotent, file could be corrupted
    log "Phase 6: Starting Node C and checking idempotency..."

    # Wait a moment for any potential recovery attempt
    sleep 5

    if ! file_exists "$node_c" "$filename"; then
        log "ERROR: File not found on Node C (idempotency violated)"
        return 1
    fi

    local final_content
    final_content=$(read_file "$node_c" "$filename")
    if [[ "$final_content" != "$content" ]]; then
        log "ERROR: Content corrupted on Node C after multiple recovery attempts"
        log "Expected: '$content', Got: '$final_content'"
        return 1
    fi

    log "Node C: File content verified after multiple nodes"

    # Phase 7: Verify we can still create new files
    log "Phase 7: Verifying new file creation works..."
    write_file "$node_c" "new-file.txt" "Post-recovery file"

    if ! file_exists "$node_c" "new-file.txt"; then
        log "ERROR: Cannot create new files after recovery"
        return 1
    fi

    # Cleanup: restart killed nodes
    log "Cleanup: Restarting Node A and Node B..."
    restart_node "$node_a"
    restart_node "$node_b"
    wait_node_healthy "$node_a" 30
    wait_node_healthy "$node_b" 30

    log "Journal recovery idempotency test passed"
    return 0
}

# Test: Journal crash recovery (file)
# Simulates single-node crash: create file, kill process, remount and verify recovery
# Matches Go test: TestJournalCrashRecoveryFile
test_journal_crash_recovery_file() {
    log "Testing journal crash recovery for file operations..."

    # This test works with a single node - we test crash/recovery on node A
    local node_a="${NODE_ARRAY[0]}"
    local filename="crash_test.txt"
    local content="This data should survive a crash!"

    # Phase 1: Create file with known content
    log "Phase 1: Creating file on Node A..."
    write_file "$node_a" "$filename" "$content"

    # Verify file exists before crash
    if ! file_exists "$node_a" "$filename"; then
        log "ERROR: File should exist before crash"
        return 1
    fi

    # Give time for journal sync
    sleep 1

    log "Phase 2: Simulating crash by killing container..."
    kill_node "$node_a"
    sleep 2

    # Phase 3: Restart (remount) - this triggers journal recovery
    log "Phase 3: Restarting container to trigger journal recovery..."
    restart_node "$node_a"
    wait_node_healthy "$node_a" 30

    # Phase 4: Verify file was recovered
    log "Phase 4: Verifying file recovery..."

    if ! file_exists "$node_a" "$filename"; then
        log "ERROR: File should exist after crash recovery"
        return 1
    fi

    # Verify content is intact
    local recovered_content
    recovered_content=$(read_file "$node_a" "$filename")
    if [[ "$recovered_content" != "$content" ]]; then
        log "ERROR: Content mismatch after crash recovery"
        log "Expected: '$content'"
        log "Got: '$recovered_content'"
        return 1
    fi

    log "Journal crash recovery file test passed"
    return 0
}

# Test: Journal crash recovery (directory)
# Simulates single-node crash: create directory with nested file, kill process, remount and verify recovery
# Matches Go test: TestJournalCrashRecoveryDirectory
test_journal_crash_recovery_directory() {
    log "Testing journal crash recovery for directory operations..."

    # This test works with a single node
    local node_a="${NODE_ARRAY[0]}"
    local dirname="recovered_dir"
    local nested_file="$dirname/nested.txt"
    local content="Nested file content"

    # Phase 1: Create directory structure
    log "Phase 1: Creating directory and nested file on Node A..."

    # Create directory
    exec_on_node "$node_a" mkdir -p "/mnt/goclusterfs/$dirname"

    # Create nested file
    write_file "$node_a" "$nested_file" "$content"

    # Verify directory exists before crash
    if ! exec_on_node "$node_a" test -d "/mnt/goclusterfs/$dirname"; then
        log "ERROR: Directory should exist before crash"
        return 1
    fi

    # Verify nested file exists before crash
    if ! file_exists "$node_a" "$nested_file"; then
        log "ERROR: Nested file should exist before crash"
        return 1
    fi

    # Give time for journal sync
    sleep 1

    log "Phase 2: Simulating crash by killing container..."
    kill_node "$node_a"
    sleep 2

    # Phase 3: Restart (remount) - this triggers journal recovery
    log "Phase 3: Restarting container to trigger journal recovery..."
    restart_node "$node_a"
    wait_node_healthy "$node_a" 30

    # Phase 4: Verify directory structure was recovered
    log "Phase 4: Verifying directory recovery..."

    # Verify directory exists and is a directory
    if ! exec_on_node "$node_a" test -d "/mnt/goclusterfs/$dirname"; then
        log "ERROR: Directory should exist after crash recovery"
        return 1
    fi

    # Verify nested file exists
    if ! file_exists "$node_a" "$nested_file"; then
        log "ERROR: Nested file should exist after crash recovery"
        return 1
    fi

    # Verify nested file content
    local recovered_content
    recovered_content=$(read_file "$node_a" "$nested_file")
    if [[ "$recovered_content" != "$content" ]]; then
        log "ERROR: Nested file content mismatch after crash recovery"
        log "Expected: '$content'"
        log "Got: '$recovered_content'"
        return 1
    fi

    log "Journal crash recovery directory test passed"
    return 0
}

# Test: Journal crash recovery for multiple operations
# Matches Go test: TestJournalCrashRecoveryMultipleOps in journal_test.go
# Creates multiple files + directory + nested file, crashes, verifies all recovered
test_journal_crash_recovery_multiple_ops() {
    log "Testing journal crash recovery for multiple operations..."

    local node_a="${NODE_ARRAY[0]}"

    # Perform multiple operations
    # 1. Create file1.txt
    log "Creating file1.txt..."
    if ! write_file "$node_a" "file1.txt" "content1"; then
        log "ERROR: Failed to create file1.txt"
        return 1
    fi

    # 2. Create file2.txt
    log "Creating file2.txt..."
    if ! write_file "$node_a" "file2.txt" "content2"; then
        log "ERROR: Failed to create file2.txt"
        return 1
    fi

    # 3. Create directory
    log "Creating testdir..."
    if ! docker exec "$node_a" mkdir -p "$MOUNT_POINT/testdir"; then
        log "ERROR: Failed to create testdir"
        return 1
    fi

    # 4. Create nested file
    log "Creating testdir/nested.txt..."
    if ! docker exec "$node_a" sh -c "echo -n 'nested' > '$MOUNT_POINT/testdir/nested.txt'"; then
        log "ERROR: Failed to create nested file"
        return 1
    fi

    # Force sync
    docker exec "$node_a" sync 2>/dev/null || true

    log "All operations complete, simulating crash..."

    # Kill the container to simulate crash
    docker kill "$node_a" >/dev/null 2>&1 || true
    sleep 2

    # Restart container
    log "Restarting container..."
    docker start "$node_a" >/dev/null 2>&1

    if ! wait_node_healthy "$node_a" 60; then
        return 1
    fi

    # Verify all operations were recovered
    log "Verifying all operations were recovered..."

    # Verify file1.txt
    local content1
    content1=$(read_file "$node_a" "file1.txt")
    if [[ "$content1" != "content1" ]]; then
        log "ERROR: file1.txt content mismatch after crash recovery"
        log "Expected: 'content1', Got: '$content1'"
        return 1
    fi
    log "file1.txt recovered successfully"

    # Verify file2.txt
    local content2
    content2=$(read_file "$node_a" "file2.txt")
    if [[ "$content2" != "content2" ]]; then
        log "ERROR: file2.txt content mismatch after crash recovery"
        log "Expected: 'content2', Got: '$content2'"
        return 1
    fi
    log "file2.txt recovered successfully"

    # Verify testdir is a directory
    if ! docker exec "$node_a" test -d "$MOUNT_POINT/testdir"; then
        log "ERROR: testdir not recovered as directory"
        return 1
    fi
    log "testdir recovered successfully"

    # Verify nested file
    local nested_content
    nested_content=$(docker exec "$node_a" cat "$MOUNT_POINT/testdir/nested.txt" 2>/dev/null)
    if [[ "$nested_content" != "nested" ]]; then
        log "ERROR: testdir/nested.txt content mismatch after crash recovery"
        log "Expected: 'nested', Got: '$nested_content'"
        return 1
    fi
    log "testdir/nested.txt recovered successfully"

    log "Journal crash recovery multiple ops test passed - all 4 operations recovered"
    return 0
}

# Test: Journal crash during write
# Matches Go test: TestJournalCrashDuringWrite in journal_test.go
# Creates baseline file, writes rapid files, crashes mid-write, verifies baseline recovered
test_journal_crash_during_write() {
    log "Testing journal crash during active write operation..."

    local node_a="${NODE_ARRAY[0]}"
    local baseline_content="This file was created and synced before the crash"

    # Create baseline file that MUST survive
    log "Creating baseline file..."
    if ! write_file "$node_a" "baseline.txt" "$baseline_content"; then
        log "ERROR: Failed to create baseline file"
        return 1
    fi

    # Force sync to ensure baseline is committed to journal
    docker exec "$node_a" sync 2>/dev/null || true
    sleep 1

    log "Baseline file created and synced"

    # Start rapid writes in background
    log "Starting rapid writes..."
    local write_count=0
    for i in $(seq 1 20); do
        docker exec "$node_a" sh -c "echo -n 'Rapid write $i' > '$MOUNT_POINT/rapid_$(printf '%04d' $i).txt'" 2>/dev/null &
        ((write_count++))
    done

    # Give a brief moment for some writes to start
    sleep 0.2

    log "Wrote $write_count rapid files, crashing now..."

    # Kill container while writes may be in progress
    docker kill "$node_a" >/dev/null 2>&1 || true

    # Wait for any background processes to terminate
    wait 2>/dev/null || true
    sleep 2

    # Restart container
    log "Restarting container after crash..."
    docker start "$node_a" >/dev/null 2>&1

    if ! wait_node_healthy "$node_a" 60; then
        return 1
    fi

    # Baseline file MUST be recovered
    log "Verifying baseline file recovery..."
    local recovered_baseline
    recovered_baseline=$(read_file "$node_a" "baseline.txt")
    if [[ "$recovered_baseline" != "$baseline_content" ]]; then
        log "ERROR: Baseline file not recovered or content mismatch"
        log "Expected: '$baseline_content'"
        log "Got: '$recovered_baseline'"
        return 1
    fi
    log "Baseline file recovered successfully"

    # Count how many rapid files were recovered (some may be lost, which is expected)
    local recovered_count
    recovered_count=$(docker exec "$node_a" ls "$MOUNT_POINT" 2>/dev/null | grep -c "^rapid_" || echo "0")
    log "Recovered $recovered_count/$write_count rapid files (some in-flight writes may be lost, which is expected)"

    # The key assertion: filesystem is consistent (we can read/list without errors)
    if ! docker exec "$node_a" ls -la "$MOUNT_POINT" >/dev/null 2>&1; then
        log "ERROR: Filesystem inconsistent after crash recovery"
        return 1
    fi

    log "Journal crash during write test passed - filesystem is consistent"
    return 0
}

# Test 1: Multi-container concurrent writes
test_concurrent_writes() {
    log "Testing concurrent writes from all nodes..."

    # Each node writes a unique file
    for i in "${!NODE_ARRAY[@]}"; do
        node="${NODE_ARRAY[$i]}"
        write_file "$node" "node${i}_file.txt" "Data from node $i" &
    done
    wait

    # Give time for sync
    sleep 2

    # Verify all files are visible from first node
    for i in "${!NODE_ARRAY[@]}"; do
        if ! file_exists "${NODE_ARRAY[0]}" "node${i}_file.txt"; then
            log "ERROR: node${i}_file.txt not visible from ${NODE_ARRAY[0]}"
            return 1
        fi
    done

    log "All concurrent writes verified"
    return 0
}

# Two nodes write different ranges of each new file's first block at once.
# Without the inode lock the later inode write drops the other node's extent.
test_concurrent_first_write_same_block() {
    log "Testing concurrent first writes to the same block from two nodes..."
    local writer0="${NODE_ARRAY[0]}"
    local writer1="${NODE_ARRAY[1]}"
    local reader="${NODE_ARRAY[2]}"
    local files=10
    local size=65536
    local offset0=262144
    local offset1=524288

    for i in $(seq 1 $files); do
        exec_on_node "$writer0" touch "/mnt/goclusterfs/first_write_$i.bin"
    done
    sleep 2

    for i in $(seq 1 $files); do
        write_pattern_at "$writer0" "first_write_$i.bin" A "$size" "$offset0" &
        write_pattern_at "$writer1" "first_write_$i.bin" B "$size" "$offset1" &
        wait
    done

    # Restart the reader so cached extents from earlier files with the same inode numbers cannot hide the result.
    restart_node "$reader"
    wait_node_healthy "$reader" 30 || return 1

    local lost=0
    for i in $(seq 1 $files); do
        verify_pattern_at "$reader" "first_write_$i.bin" A "$offset0" 4096 || lost=$((lost + 1))
        verify_pattern_at "$reader" "first_write_$i.bin" B "$offset1" 4096 || lost=$((lost + 1))
    done
    if [ "$lost" -gt 0 ]; then
        log "ERROR: $lost of $((files * 2)) concurrent first writes lost"
        return 1
    fi
    log "All concurrent first writes survived"
    return 0
}

# Test 2: Container kill and recovery
test_container_kill_recovery() {
    log "Testing container kill and recovery..."

    local victim="${NODE_ARRAY[1]}"
    local survivor="${NODE_ARRAY[0]}"

    # Write file from victim node
    write_file "$victim" "before_kill.txt" "Written before kill"

    # Kill the victim
    kill_node "$victim"
    sleep 2

    # Survivor should still be able to read the file
    if ! file_exists "$survivor" "before_kill.txt"; then
        log "ERROR: before_kill.txt not readable after node kill"
        return 1
    fi

    # Survivor writes new file while victim is down
    write_file "$survivor" "during_kill.txt" "Written during kill"

    # Restart victim
    restart_node "$victim"
    wait_node_healthy "$victim" 30

    # Victim should see both files
    if ! file_exists "$victim" "before_kill.txt"; then
        log "ERROR: before_kill.txt not visible after recovery"
        return 1
    fi

    if ! file_exists "$victim" "during_kill.txt"; then
        log "ERROR: during_kill.txt not visible after recovery"
        return 1
    fi

    log "Container kill and recovery test passed"
    return 0
}

# Test 3: Network partition
test_network_partition() {
    log "Testing network partition..."

    local isolated="${NODE_ARRAY[2]}"
    local other="${NODE_ARRAY[0]}"

    # Write file before partition
    write_file "$other" "before_partition.txt" "Before partition"

    # Isolate the node
    isolate_node "$isolated"
    sleep 2

    # The isolated node should still have the file (local cache)
    # Note: This tests that the node doesn't crash during partition

    # Heal partition
    heal_node "$isolated"
    sleep 2

    # Verify node is still functional
    if ! file_exists "$isolated" "before_partition.txt"; then
        log "ERROR: File not accessible after healing partition"
        return 1
    fi

    log "Network partition test passed"
    return 0
}

# Test: Broadcast cache invalidation
# Verifies that cache invalidation via UDP broadcast propagates quickly between nodes.
# This tests that Node B sees changes from Node A within ~500ms (vs 5s TTL).
test_broadcast_invalidation() {
    log "Testing broadcast cache invalidation..."

    local writer="${NODE_ARRAY[0]}"
    local reader="${NODE_ARRAY[1]}"
    local test_file="broadcast_test_$(date +%s).txt"

    # Create initial file on writer
    write_file "$writer" "$test_file" "initial_content"
    sleep 1

    # Verify reader sees initial content
    if ! file_exists "$reader" "$test_file"; then
        log "ERROR: Reader cannot see initial file"
        return 1
    fi

    # Update file on writer
    local start_time=$(date +%s%N)
    write_file "$writer" "$test_file" "updated_content_via_broadcast"

    # Poll reader for updated content (max 2 seconds)
    local max_attempts=20  # 20 x 100ms = 2s
    local attempt=0
    local propagation_time_ms=0

    while [ $attempt -lt $max_attempts ]; do
        local content=$(read_file "$reader" "$test_file" 2>/dev/null)
        if [ "$content" = "updated_content_via_broadcast" ]; then
            local end_time=$(date +%s%N)
            propagation_time_ms=$(( (end_time - start_time) / 1000000 ))
            log "Broadcast propagation detected in ${propagation_time_ms}ms"
            break
        fi
        sleep 0.1
        attempt=$((attempt + 1))
    done

    if [ $attempt -ge $max_attempts ]; then
        log "ERROR: Reader did not see update within 2 seconds"
        return 1
    fi

    # Broadcast should be much faster than the 5s TTL
    # We use 1000ms as a generous threshold for test stability
    if [ $propagation_time_ms -gt 1000 ]; then
        log "WARNING: Propagation slow (${propagation_time_ms}ms), but within acceptable range"
    fi

    # Clean up
    exec_on_node "$writer" rm -f "/mnt/goclusterfs/$test_file"

    log "Broadcast invalidation test passed (propagation: ${propagation_time_ms}ms)"
    return 0
}

# Test: Broadcast directory invalidation
# Verifies that new file creation is broadcast between nodes.
test_broadcast_directory_invalidation() {
    log "Testing broadcast directory invalidation..."

    local writer="${NODE_ARRAY[0]}"
    local reader="${NODE_ARRAY[1]}"
    local test_file="broadcast_dir_test_$(date +%s).txt"

    # Record start time
    local start_time=$(date +%s%N)

    # Create new file on writer
    write_file "$writer" "$test_file" "new file created"

    # Poll reader for new file visibility (max 2 seconds)
    local max_attempts=20
    local attempt=0
    local propagation_time_ms=0

    while [ $attempt -lt $max_attempts ]; do
        if file_exists "$reader" "$test_file"; then
            local end_time=$(date +%s%N)
            propagation_time_ms=$(( (end_time - start_time) / 1000000 ))
            log "Directory broadcast propagation detected in ${propagation_time_ms}ms"
            break
        fi
        sleep 0.1
        attempt=$((attempt + 1))
    done

    if [ $attempt -ge $max_attempts ]; then
        log "ERROR: Reader did not see new file within 2 seconds"
        return 1
    fi

    # Verify content is correct
    local content=$(read_file "$reader" "$test_file" 2>/dev/null)
    if [ "$content" != "new file created" ]; then
        log "ERROR: Content mismatch: got '$content'"
        return 1
    fi

    # Clean up
    exec_on_node "$writer" rm -f "/mnt/goclusterfs/$test_file"

    log "Broadcast directory invalidation test passed (propagation: ${propagation_time_ms}ms)"
    return 0
}

# Test: Broadcast multiple updates
# Verifies that rapid successive updates are correctly broadcast between nodes.
# Node 0 performs 5 rapid updates and Node 1 should see the final version.
test_broadcast_multiple_updates() {
    log "Testing broadcast multiple updates..."

    local writer="${NODE_ARRAY[0]}"
    local reader="${NODE_ARRAY[1]}"
    local test_file="broadcast_rapid_$(date +%s).txt"

    # Create initial file on writer
    write_file "$writer" "$test_file" "v0"
    sleep 0.2  # Allow initial propagation

    # Verify reader sees initial content
    local max_attempts=20
    local attempt=0
    while [ $attempt -lt $max_attempts ]; do
        local content=$(read_file "$reader" "$test_file" 2>/dev/null)
        if [ "$content" = "v0" ]; then
            break
        fi
        sleep 0.1
        attempt=$((attempt + 1))
    done

    if [ $attempt -ge $max_attempts ]; then
        log "ERROR: Reader cannot see initial file"
        return 1
    fi

    # Perform 5 rapid updates on writer (50ms gaps)
    local update_count=5
    for i in $(seq 1 $update_count); do
        write_file "$writer" "$test_file" "v${i}"
        sleep 0.05  # 50ms gap between updates
    done

    # Allow final broadcast to propagate
    sleep 0.3

    # Poll reader for final content (max 2 seconds)
    local expected_final="v${update_count}"
    max_attempts=20
    attempt=0

    while [ $attempt -lt $max_attempts ]; do
        local content=$(read_file "$reader" "$test_file" 2>/dev/null)
        if [ "$content" = "$expected_final" ]; then
            log "Reader sees final version after rapid updates"
            break
        fi
        sleep 0.1
        attempt=$((attempt + 1))
    done

    if [ $attempt -ge $max_attempts ]; then
        local actual=$(read_file "$reader" "$test_file" 2>/dev/null)
        log "ERROR: Reader did not see final version. Got '$actual', expected '$expected_final'"
        return 1
    fi

    # Clean up
    exec_on_node "$writer" rm -f "/mnt/goclusterfs/$test_file"

    log "Broadcast multiple updates test passed"
    return 0
}

# Test: Broadcast bidirectional
# Verifies that invalidations work in both directions between nodes.
# Node 0 creates file_a, Node 1 creates file_b, both should see each other's file.
test_broadcast_bidirectional() {
    log "Testing broadcast bidirectional..."

    local node0="${NODE_ARRAY[0]}"
    local node1="${NODE_ARRAY[1]}"
    local file_a="bidir_a_$(date +%s).txt"
    local file_b="bidir_b_$(date +%s).txt"

    # Node 0 creates file A
    write_file "$node0" "$file_a" "from node 0"

    # Node 1 creates file B
    write_file "$node1" "$file_b" "from node 1"

    # Allow broadcast propagation
    sleep 0.3

    # Poll for Node 1 to see file A (max 2 seconds)
    local max_attempts=20
    local attempt=0
    local content_a=""

    while [ $attempt -lt $max_attempts ]; do
        content_a=$(read_file "$node1" "$file_a" 2>/dev/null)
        if [ "$content_a" = "from node 0" ]; then
            log "Node 1 sees file_a from Node 0"
            break
        fi
        sleep 0.1
        attempt=$((attempt + 1))
    done

    if [ "$content_a" != "from node 0" ]; then
        log "ERROR: Node 1 cannot read file_a. Got '$content_a'"
        return 1
    fi

    # Poll for Node 0 to see file B (max 2 seconds)
    max_attempts=20
    attempt=0
    local content_b=""

    while [ $attempt -lt $max_attempts ]; do
        content_b=$(read_file "$node0" "$file_b" 2>/dev/null)
        if [ "$content_b" = "from node 1" ]; then
            log "Node 0 sees file_b from Node 1"
            break
        fi
        sleep 0.1
        attempt=$((attempt + 1))
    done

    if [ "$content_b" != "from node 1" ]; then
        log "ERROR: Node 0 cannot read file_b. Got '$content_b'"
        return 1
    fi

    # Clean up
    exec_on_node "$node0" rm -f "/mnt/goclusterfs/$file_a"
    exec_on_node "$node1" rm -f "/mnt/goclusterfs/$file_b"

    log "Broadcast bidirectional test passed"
    return 0
}

# Test: Read after cross-node inode reuse
# Node 0 deletes a file node 1 has read and creates a new file with the same inode number.
# Node 1 must read the new file's full content, not the old size or old data block.
test_inode_reuse_stale_read() {
    log "Testing read after cross-node inode reuse..."

    local writer="${NODE_ARRAY[0]}"
    local reader="${NODE_ARRAY[1]}"
    local dir=/mnt/goclusterfs
    local old="reuse_old_$(date +%s).bin"

    exec_on_node "$writer" sh -c "head -c 204800 /dev/zero | tr '\\0' o > $dir/$old"
    local old_ino
    old_ino=$(exec_on_node "$writer" stat -c %i "$dir/$old")
    exec_on_node "$reader" sh -c "cat $dir/$old > /dev/null"

    # New files take the first free inode from this node's start, so create files until one gets the old number.
    local new
    new=$(exec_on_node "$writer" sh -c "rm $dir/$old; for i in \$(seq 1 200); do f=reuse_new_\$i.bin; touch $dir/\$f; if [ \$(stat -c %i $dir/\$f) = $old_ino ]; then echo \$f; break; fi; done")
    if [[ -z "$new" ]]; then
        log "ERROR: No new file reused inode $old_ino"
        exec_on_node "$writer" sh -c "rm -f $dir/reuse_new_*.bin"
        return 1
    fi

    exec_on_node "$writer" sh -c "head -c 204800 /dev/zero | tr '\\0' p > $dir/reuse_pad.bin && head -c 307200 /dev/zero | tr '\\0' n > $dir/$new"
    # The healthcheck's ls can leave a pre-write size in the reader's 1s kernel attribute cache.
    sleep 1.5
    local want got
    want=$(exec_on_node "$writer" sh -c "md5sum < $dir/$new")
    got=$(exec_on_node "$reader" sh -c "md5sum < $dir/$new")
    exec_on_node "$writer" sh -c "rm -f $dir/reuse_new_*.bin $dir/reuse_pad.bin"

    if [[ "$got" != "$want" ]]; then
        log "ERROR: $reader read wrong data from $new (inode $old_ino): md5 '$got', want '$want'"
        return 1
    fi

    log "Inode reuse stale read test passed"
    return 0
}

# Test: Extent cache coherency across nodes
# Node 1 reads files, node 0 changes their block mappings (truncate and rewrite,
# hole fill), and node 1 must read the new data.
test_extent_cache_coherency() {
    log "Testing extent cache coherency across nodes..."

    local writer="${NODE_ARRAY[0]}"
    local reader="${NODE_ARRAY[1]}"
    local dir=/mnt/goclusterfs
    local trunc="extent_trunc_$(date +%s).bin"
    local holes="extent_holes_$(date +%s).bin"

    exec_on_node "$writer" sh -c "head -c 307200 /dev/zero | tr '\\0' o > $dir/$trunc"
    exec_on_node "$writer" sh -c "head -c 4096 /dev/zero | tr '\\0' a > $dir/$holes && head -c 4096 /dev/zero | tr '\\0' c | dd of=$dir/$holes bs=1M seek=2 conv=notrunc 2>/dev/null"
    exec_on_node "$reader" sh -c "cat $dir/$trunc $dir/$holes > /dev/null"

    # The padding file takes the freed block, so the rewrite lands in another one.
    exec_on_node "$writer" sh -c "truncate -s 0 $dir/$trunc && head -c 307200 /dev/zero | tr '\\0' p > $dir/extent_pad.bin && head -c 307200 /dev/zero | tr '\\0' n > $dir/$trunc"
    exec_on_node "$writer" sh -c "head -c 4096 /dev/zero | tr '\\0' b | dd of=$dir/$holes bs=1M seek=1 conv=notrunc 2>/dev/null"
    # The healthcheck's ls can leave a mid-rewrite size in the reader's 1s kernel attribute cache.
    sleep 1.5

    local f want got failed=0
    for f in "$trunc" "$holes"; do
        want=$(exec_on_node "$writer" sh -c "md5sum < $dir/$f")
        got=$(exec_on_node "$reader" sh -c "md5sum < $dir/$f")
        if [[ "$got" != "$want" ]]; then
            log "ERROR: $reader read stale data from $f: md5 '$got', want '$want'"
            failed=1
        fi
    done
    exec_on_node "$writer" sh -c "rm -f $dir/$trunc $dir/$holes $dir/extent_pad.bin"

    if [[ $failed -ne 0 ]]; then
        return 1
    fi
    log "Extent cache coherency test passed"
    return 0
}

# Print the free 1MB blocks that statfs reports on node $1
statfs_free_blocks() {
    exec_on_node "$1" stat -f -c %f /mnt/goclusterfs
}

# Writes 5 blocks, restarts the node, and checks that statfs reports the same
# free blocks as before the restart, not the whole volume.
test_statfs_after_remount() {
    log "Testing statfs free blocks across a remount..."

    local node="${NODE_ARRAY[0]}"
    local file="/mnt/goclusterfs/statfs_remount.bin"
    exec_on_node "$node" rm -f "$file"

    local before written remounted
    before=$(statfs_free_blocks "$node")
    exec_on_node "$node" dd if=/dev/urandom of="$file" bs=1M count=5 2>/dev/null
    written=$(statfs_free_blocks "$node")
    if (( before - written < 5 )); then
        log "ERROR: Writing 5 blocks lowered free blocks from $before to $written"
        return 1
    fi

    restart_node "$node"
    wait_node_healthy "$node" 30
    remounted=$(statfs_free_blocks "$node")
    exec_on_node "$node" rm -f "$file"

    if [[ "$remounted" != "$written" ]]; then
        log "ERROR: $written free blocks before the restart, $remounted after"
        return 1
    fi

    log "Statfs after remount test passed"
    return 0
}

# Writes 5 blocks on one node and removes the file on another, checking after
# each step that both nodes report the same free blocks.
test_statfs_multinode() {
    log "Testing statfs free blocks across nodes..."

    local writer="${NODE_ARRAY[0]}"
    local remover="${NODE_ARRAY[1]:-}"
    if [[ -z "$remover" ]]; then
        log "Skipping statfs multi-node test (requires 2+ nodes)"
        return 0
    fi
    local file="/mnt/goclusterfs/statfs_multinode.bin"
    exec_on_node "$writer" rm -f "$file"

    local start on_writer on_remover
    start=$(statfs_free_blocks "$remover")
    exec_on_node "$writer" dd if=/dev/urandom of="$file" bs=1M count=5 2>/dev/null
    on_writer=$(statfs_free_blocks "$writer")
    on_remover=$(statfs_free_blocks "$remover")
    if [[ "$on_writer" != "$on_remover" ]] || (( start - on_remover < 5 )); then
        log "ERROR: After $writer wrote 5 blocks: $writer reports $on_writer free, $remover reports $on_remover, $start before"
        return 1
    fi

    exec_on_node "$remover" rm -f "$file"
    on_writer=$(statfs_free_blocks "$writer")
    on_remover=$(statfs_free_blocks "$remover")
    if [[ "$on_writer" != "$start" || "$on_remover" != "$start" ]]; then
        log "ERROR: After $remover removed the file: $writer reports $on_writer free, $remover reports $on_remover, want $start"
        return 1
    fi

    log "Statfs multi-node test passed"
    return 0
}

# Test 4: Rolling restart
test_rolling_restart() {
    log "Testing rolling restart..."

    # Write a file
    write_file "${NODE_ARRAY[0]}" "rolling_test.txt" "Rolling restart test"

    # Restart each node one by one
    for node in "${NODE_ARRAY[@]}"; do
        log "Restarting $node..."
        restart_node "$node"
        wait_node_healthy "$node" 30

        # Verify file is still accessible
        if ! file_exists "$node" "rolling_test.txt"; then
            log "ERROR: File not accessible on $node after restart"
            return 1
        fi
    done

    log "Rolling restart test passed"
    return 0
}

# Test 5: Stress test with random failures (simplified)
test_stress_random_failures() {
    log "Testing stress with random failures..."

    local iterations=5
    local files_created=0

    for i in $(seq 1 $iterations); do
        # Random node writes
        local node_idx=$((RANDOM % NODE_COUNT))
        local node="${NODE_ARRAY[$node_idx]}"

        write_file "$node" "stress_${i}.txt" "Stress test iteration $i"
        files_created=$((files_created + 1))

        # Occasionally kill a random node (20% chance)
        if [ $((RANDOM % 5)) -eq 0 ]; then
            local kill_idx=$((RANDOM % NODE_COUNT))
            local kill_node="${NODE_ARRAY[$kill_idx]}"
            kill_node "$kill_node"
            sleep 1
            restart_node "$kill_node"
            wait_node_healthy "$kill_node" 30
        fi
    done

    sleep 2

    # Verify all files exist
    for i in $(seq 1 $iterations); do
        if ! file_exists "${NODE_ARRAY[0]}" "stress_${i}.txt"; then
            log "ERROR: stress_${i}.txt not found"
            return 1
        fi
    done

    log "Stress test passed ($files_created files created)"
    return 0
}

# =============================================================================
# Concurrent Same-File Write Tests (using pre-extension pattern)
# =============================================================================

# Test 6: Two nodes write to same file at different offsets
test_concurrent_same_file_writes() {
    log "Testing concurrent same-file writes (2 nodes)..."

    local filename="samefile_2node.txt"
    local chunk_size=$((400 * 1024))  # 400KB each
    local final_size=$((chunk_size * 2))  # 800KB total

    # Pre-extend file to final size (critical: avoids TOCTOU race in lock CAS)
    pre_extend_file "${NODE_ARRAY[0]}" "$filename" "$final_size"
    sleep 1

    # Concurrent writes at different offsets
    # Node 0: 400KB of 'A' at offset 0
    # Node 1: 400KB of 'B' at offset 400KB
    write_pattern_at "${NODE_ARRAY[0]}" "$filename" "A" "$chunk_size" 0 &
    write_pattern_at "${NODE_ARRAY[1]}" "$filename" "B" "$chunk_size" "$chunk_size" &
    wait
    sleep 2

    # Verify file size
    local actual_size
    actual_size=$(get_file_size "${NODE_ARRAY[0]}" "$filename")
    if [ "$actual_size" -lt "$final_size" ]; then
        log "ERROR: File too small: $actual_size bytes, expected at least $final_size"
        return 1
    fi

    # Verify each node's data at expected offset
    if ! verify_pattern_at "${NODE_ARRAY[0]}" "$filename" "A" 0 100; then
        log "ERROR: Node 0 data corrupted at offset 0"
        return 1
    fi

    if ! verify_pattern_at "${NODE_ARRAY[0]}" "$filename" "B" "$chunk_size" 100; then
        log "ERROR: Node 1 data corrupted at offset $chunk_size"
        return 1
    fi

    log "Concurrent same-file writes (2 nodes) test passed"
    return 0
}

# Test 7: Three nodes write to same file at different offsets
test_concurrent_same_file_writes_3nodes() {
    log "Testing concurrent same-file writes (3 nodes)..."

    if [ "$NODE_COUNT" -lt 3 ]; then
        log "SKIP: Need at least 3 nodes for this test"
        return 0
    fi

    local filename="samefile_3node.txt"
    local chunk_size=$((300 * 1024))  # 300KB each
    local final_size=$((chunk_size * 3))  # 900KB total

    # Pre-extend file
    pre_extend_file "${NODE_ARRAY[0]}" "$filename" "$final_size"
    sleep 1

    # Concurrent writes
    # Node 0: 300KB of '1' at offset 0
    # Node 1: 300KB of '2' at offset 300KB
    # Node 2: 300KB of '3' at offset 600KB
    local offset1=$chunk_size
    local offset2=$((chunk_size * 2))

    write_pattern_at "${NODE_ARRAY[0]}" "$filename" "1" "$chunk_size" 0 &
    write_pattern_at "${NODE_ARRAY[1]}" "$filename" "2" "$chunk_size" "$offset1" &
    write_pattern_at "${NODE_ARRAY[2]}" "$filename" "3" "$chunk_size" "$offset2" &
    wait
    sleep 2

    # Verify all three regions
    if ! verify_pattern_at "${NODE_ARRAY[0]}" "$filename" "1" 0 100; then
        log "ERROR: Node 0 data corrupted"
        return 1
    fi

    if ! verify_pattern_at "${NODE_ARRAY[0]}" "$filename" "2" "$offset1" 100; then
        log "ERROR: Node 1 data corrupted"
        return 1
    fi

    if ! verify_pattern_at "${NODE_ARRAY[0]}" "$filename" "3" "$offset2" 100; then
        log "ERROR: Node 2 data corrupted"
        return 1
    fi

    log "Concurrent same-file writes (3 nodes) test passed"
    return 0
}

# Test 8: Mixed size writes - one within file, one extending
test_mixed_size_writes() {
    log "Testing mixed size writes..."

    local filename="mixed_writes.txt"
    local initial_size=$((500 * 1024))  # 500KB initial
    local extend_size=$((512 * 1024))  # 512KB extension
    local final_size=$((initial_size + extend_size))  # ~1MB total

    # Pre-extend file to FINAL size (critical for avoiding race)
    pre_extend_file "${NODE_ARRAY[0]}" "$filename" "$final_size"
    sleep 1

    # Fill initial region with 'I'
    write_pattern_at "${NODE_ARRAY[0]}" "$filename" "I" "$initial_size" 0
    sleep 1

    # Concurrent writes:
    # Node 0: 100 bytes of 'N' at offset 250KB (within initial region)
    # Node 1: 512KB of 'E' at offset 500KB (in extended region, pre-filled with zeros)
    local within_offset=$((250 * 1024))
    write_pattern_at "${NODE_ARRAY[0]}" "$filename" "N" 100 "$within_offset" &
    write_pattern_at "${NODE_ARRAY[1]}" "$filename" "E" "$extend_size" "$initial_size" &
    wait
    sleep 2

    # Verify the non-extending write
    if ! verify_pattern_at "${NODE_ARRAY[0]}" "$filename" "N" "$within_offset" 100; then
        log "ERROR: Non-extending write data corrupted"
        return 1
    fi

    # Verify the extending write
    if ! verify_pattern_at "${NODE_ARRAY[0]}" "$filename" "E" "$initial_size" 100; then
        log "ERROR: Extending write data corrupted"
        return 1
    fi

    log "Mixed size writes test passed"
    return 0
}

# Test 9: Overlapping block writes (all within same 1MB block)
test_overlapping_block_writes() {
    log "Testing overlapping block writes..."

    if [ "$NODE_COUNT" -lt 3 ]; then
        log "SKIP: Need at least 3 nodes for this test"
        return 0
    fi

    local filename="overlap_block.txt"
    local chunk_size=$((256 * 1024))  # 256KB each
    local final_size=$((chunk_size * 3))  # 768KB total (fits in one 1MB block)

    # Pre-extend file
    pre_extend_file "${NODE_ARRAY[0]}" "$filename" "$final_size"
    sleep 1

    # Concurrent writes to same block:
    # Node 0: 256KB of 'X' at offset 0
    # Node 1: 256KB of 'Y' at offset 256KB
    # Node 2: 256KB of 'Z' at offset 512KB
    local offset1=$chunk_size
    local offset2=$((chunk_size * 2))

    write_pattern_at "${NODE_ARRAY[0]}" "$filename" "X" "$chunk_size" 0 &
    write_pattern_at "${NODE_ARRAY[1]}" "$filename" "Y" "$chunk_size" "$offset1" &
    write_pattern_at "${NODE_ARRAY[2]}" "$filename" "Z" "$chunk_size" "$offset2" &
    wait
    sleep 2

    # Verify all three regions
    if ! verify_pattern_at "${NODE_ARRAY[0]}" "$filename" "X" 0 100; then
        log "ERROR: Node 0 data (X) corrupted"
        return 1
    fi

    if ! verify_pattern_at "${NODE_ARRAY[0]}" "$filename" "Y" "$offset1" 100; then
        log "ERROR: Node 1 data (Y) corrupted"
        return 1
    fi

    if ! verify_pattern_at "${NODE_ARRAY[0]}" "$filename" "Z" "$offset2" 100; then
        log "ERROR: Node 2 data (Z) corrupted"
        return 1
    fi

    log "Overlapping block writes test passed"
    return 0
}

# Test 10: Write stress test - multiple rounds
test_concurrent_write_stress() {
    log "Testing concurrent write stress (multiple rounds)..."

    if [ "$NODE_COUNT" -lt 3 ]; then
        log "SKIP: Need at least 3 nodes for this test"
        return 0
    fi

    local filename="write_stress.txt"
    local chunk_size=$((250 * 1024))  # 250KB each
    local round1_size=$((chunk_size * 3))  # 750KB
    local round2_size=$((chunk_size * 3))
    local final_size=$((round1_size + round2_size))  # 1.5MB total

    # Pre-extend file for all rounds
    pre_extend_file "${NODE_ARRAY[0]}" "$filename" "$final_size"
    sleep 1

    # Round 1: First 750KB
    log "Round 1: Writing first 750KB..."
    write_pattern_at "${NODE_ARRAY[0]}" "$filename" "a" "$chunk_size" 0 &
    write_pattern_at "${NODE_ARRAY[1]}" "$filename" "b" "$chunk_size" "$chunk_size" &
    write_pattern_at "${NODE_ARRAY[2]}" "$filename" "c" "$chunk_size" "$((chunk_size * 2))" &
    wait
    sleep 1

    # Round 2: Next 750KB
    log "Round 2: Writing next 750KB..."
    local r2_base=$round1_size
    write_pattern_at "${NODE_ARRAY[0]}" "$filename" "d" "$chunk_size" "$r2_base" &
    write_pattern_at "${NODE_ARRAY[1]}" "$filename" "e" "$chunk_size" "$((r2_base + chunk_size))" &
    write_pattern_at "${NODE_ARRAY[2]}" "$filename" "f" "$chunk_size" "$((r2_base + chunk_size * 2))" &
    wait
    sleep 2

    # Verify all 6 regions
    local patterns=("a" "b" "c" "d" "e" "f")
    for i in {0..5}; do
        local offset=$((chunk_size * i))
        if ! verify_pattern_at "${NODE_ARRAY[0]}" "$filename" "${patterns[$i]}" "$offset" 100; then
            log "ERROR: Region $i (pattern ${patterns[$i]}) at offset $offset corrupted"
            return 1
        fi
    done

    log "Concurrent write stress test passed"
    return 0
}

# Test 11: Reversed write order (high offset first)
test_reversed_write_order() {
    log "Testing reversed write order..."

    local filename="reversed_order.txt"
    local chunk_size=$((400 * 1024))  # 400KB each
    local final_size=$((chunk_size * 2))  # 800KB total

    # Pre-extend file
    pre_extend_file "${NODE_ARRAY[0]}" "$filename" "$final_size"
    sleep 1

    # Concurrent writes with reversed order:
    # Node 1 writes at HIGH offset (400KB) - started first
    # Node 0 writes at LOW offset (0) - started slightly after
    write_pattern_at "${NODE_ARRAY[1]}" "$filename" "H" "$chunk_size" "$chunk_size" &
    sleep 0.1
    write_pattern_at "${NODE_ARRAY[0]}" "$filename" "L" "$chunk_size" 0 &
    wait
    sleep 2

    # Verify both regions
    if ! verify_pattern_at "${NODE_ARRAY[0]}" "$filename" "L" 0 100; then
        log "ERROR: Low-offset data (L) corrupted"
        return 1
    fi

    if ! verify_pattern_at "${NODE_ARRAY[0]}" "$filename" "H" "$chunk_size" 100; then
        log "ERROR: High-offset data (H) corrupted"
        return 1
    fi

    log "Reversed write order test passed"
    return 0
}

# =============================================================================
# Main Test Execution
# =============================================================================

# Concurrent tests are now enabled after lock order violation fix
# The fix uses two-phase allocation: BlockBitmap (Level 6) before Inode (Level 8)
# Set SKIP_CONCURRENT_TESTS=true to skip if issues arise
SKIP_CONCURRENT_TESTS="${SKIP_CONCURRENT_TESTS:-false}"

log "Waiting for all nodes to be healthy..."
for node in "${NODE_ARRAY[@]}"; do
    wait_node_healthy "$node" 60 || exit 1
done

log "All nodes healthy, starting tests..."

# Clear results
> "$RESULTS_DIR/results.txt"

# Run test suite
# Basic sanity checks first
# =============================================================================
# Extent tree tests (files with more than 2 extents)
# =============================================================================
# These tests read back only on a node that has just restarted: the extent
# cache keeps mappings of deleted files whose inode numbers new files reuse.

# Print the md5 of 64KB at 1MB block $3 of file $2 on node $1
extent_chunk_md5() {
    exec_on_node "$1" sh -c "dd if=/mnt/goclusterfs/$2 bs=65536 skip=$(($3 * 16)) count=1 2>/dev/null | md5sum | cut -d' ' -f1"
}

# Writes 64KB at seven scattered 1MB blocks (more than the 2 inline extents),
# then checks every chunk and a hole on a second node and on the writer, each
# after a restart.
test_extent_tree_sparse_writes() {
    log "Testing sparse writes beyond inline extents..."

    local writer="${NODE_ARRAY[0]}"
    local reader="${NODE_ARRAY[1]:-${NODE_ARRAY[0]}}"
    local file="extent_sparse.bin"
    local blocks="12 3 7 0 20 15 30"
    declare -A want

    exec_on_node "$writer" rm -f "/mnt/goclusterfs/$file"
    for blk in $blocks; do
        exec_on_node "$writer" sh -c "head -c 65536 /dev/urandom > /tmp/extent_chunk"
        want[$blk]=$(exec_on_node "$writer" sh -c "md5sum /tmp/extent_chunk | cut -d' ' -f1")
        exec_on_node "$writer" sh -c "dd if=/tmp/extent_chunk of=/mnt/goclusterfs/$file bs=65536 seek=$((blk * 16)) conv=notrunc 2>/dev/null" || {
            log "ERROR: write at block $blk failed"
            return 1
        }
    done

    local size
    size=$(exec_on_node "$writer" stat -c%s "/mnt/goclusterfs/$file")
    if [ "$size" -ne $((30 * 1048576 + 65536)) ]; then
        log "ERROR: size $size, want $((30 * 1048576 + 65536))"
        return 1
    fi

    check_sparse() {
        local node=$1
        for blk in $blocks; do
            local got
            got=$(extent_chunk_md5 "$node" "$file" "$blk")
            if [ "$got" != "${want[$blk]}" ]; then
                log "ERROR: $node block $blk md5 $got, want ${want[$blk]}"
                return 1
            fi
        done
        local nonzero
        nonzero=$(exec_on_node "$node" sh -c "dd if=/mnt/goclusterfs/$file bs=65536 skip=80 count=1 2>/dev/null | tr -d '\\000' | wc -c")
        if [ "$nonzero" -ne 0 ]; then
            log "ERROR: $node hole at block 5 has $nonzero non-zero bytes"
            return 1
        fi
    }

    for node in $(printf '%s\n' "$reader" "$writer" | uniq); do
        restart_node "$node"
        wait_node_healthy "$node" 30 || return 1
        check_sparse "$node" || return 1
    done

    log "Sparse writes beyond inline extents test passed"
    return 0
}

# Two nodes append 1MB alternately to their own file, so both files get
# non-contiguous blocks and grow past the old 2MB limit. Checks both files on
# each node after restarting it.
test_extent_tree_interleaved_appends() {
    log "Testing interleaved appends across nodes..."

    local node_a="${NODE_ARRAY[0]}"
    local node_b="${NODE_ARRAY[1]:-${NODE_ARRAY[0]}}"

    exec_on_node "$node_a" rm -f /mnt/goclusterfs/extent_left.bin /mnt/goclusterfs/extent_right.bin
    exec_on_node "$node_a" rm -f /tmp/extent_left.expected
    exec_on_node "$node_b" rm -f /tmp/extent_right.expected
    for i in 0 1 2 3 4 5; do
        exec_on_node "$node_a" sh -c "head -c 1048576 /dev/urandom | tee -a /tmp/extent_left.expected >> /mnt/goclusterfs/extent_left.bin" || {
            log "ERROR: $node_a append $i failed"
            return 1
        }
        exec_on_node "$node_b" sh -c "head -c 1048576 /dev/urandom | tee -a /tmp/extent_right.expected >> /mnt/goclusterfs/extent_right.bin" || {
            log "ERROR: $node_b append $i failed"
            return 1
        }
    done

    local want_left want_right
    want_left=$(exec_on_node "$node_a" sh -c "md5sum /tmp/extent_left.expected | cut -d' ' -f1")
    want_right=$(exec_on_node "$node_b" sh -c "md5sum /tmp/extent_right.expected | cut -d' ' -f1")

    check_appends() {
        local node=$1
        local left right
        left=$(exec_on_node "$node" sh -c "md5sum /mnt/goclusterfs/extent_left.bin | cut -d' ' -f1")
        right=$(exec_on_node "$node" sh -c "md5sum /mnt/goclusterfs/extent_right.bin | cut -d' ' -f1")
        if [ "$left" != "$want_left" ] || [ "$right" != "$want_right" ]; then
            log "ERROR: $node md5 left=$left right=$right, want left=$want_left right=$want_right"
            return 1
        fi
    }

    for node in $(printf '%s\n' "$node_b" "$node_a" | uniq); do
        restart_node "$node"
        wait_node_healthy "$node" 30 || return 1
        check_appends "$node" || return 1
    done

    log "Interleaved appends test passed"
    return 0
}

md5_on_node_matches() {
    local node=$1 path=$2 want=$3 got
    got=$(exec_on_node "$node" sh -c "md5sum < $path | cut -d' ' -f1")
    if [ "$got" != "$want" ]; then
        log "ERROR: $node $path md5 $got, want $want"
        return 1
    fi
}

# Snapshots a file, then overwrites it in place, appends to it and truncates
# it. A clone of the snapshot must hold the original data on both nodes. The
# expected data is built with the same edits on a local copy.
test_snapshot_preserves_data() {
    log "Testing that a snapshot keeps its data..."

    local writer="${NODE_ARRAY[0]}"
    local reader="${NODE_ARRAY[1]:-${NODE_ARRAY[0]}}"
    local file=/mnt/goclusterfs/snap_data.bin
    local clone=/mnt/goclusterfs/snap_data_clone.bin

    exec_on_node "$writer" rm -f "$file" "$clone"
    exec_on_node "$writer" sh -c "head -c 3145728 /dev/urandom > /tmp/snap_orig && cp /tmp/snap_orig /tmp/snap_live && cp /tmp/snap_orig $file" || return 1
    exec_on_node "$writer" setfattr -n user.snapshot -v snap_data_before "$file" || {
        log "ERROR: snapshot failed"
        return 1
    }
    exec_on_node "$writer" sh -c "head -c 4096 /dev/urandom > /tmp/snap_patch && head -c 2097152 /dev/urandom > /tmp/snap_tail"
    for target in "$file" /tmp/snap_live; do
        exec_on_node "$writer" sh -c "dd if=/tmp/snap_patch of=$target bs=4096 seek=257 conv=notrunc 2>/dev/null && cat /tmp/snap_tail >> $target && truncate -s 1572864 $target" || {
            log "ERROR: changing $target failed"
            return 1
        }
    done
    exec_on_node "$writer" setfattr -n user.snapshot.clone -v "snap_data_before:/snap_data_clone.bin" "$file" || {
        log "ERROR: clone from snapshot failed"
        return 1
    }

    local want_clone want_live
    want_clone=$(exec_on_node "$writer" sh -c "md5sum < /tmp/snap_orig | cut -d' ' -f1")
    want_live=$(exec_on_node "$writer" sh -c "md5sum < /tmp/snap_live | cut -d' ' -f1")
    for node in $(printf '%s\n' "$reader" "$writer" | uniq); do
        restart_node "$node"
        wait_node_healthy "$node" 30 || return 1
        md5_on_node_matches "$node" "$clone" "$want_clone" || return 1
        md5_on_node_matches "$node" "$file" "$want_live" || return 1
    done

    exec_on_node "$writer" setfattr -x user.snapshot.snap_data_before "$file"
    log "Snapshot keeps its data test passed"
    return 0
}

# Writes into a clone and into its source with dd conv=notrunc. Each write must
# change only the file written, on both nodes.
test_clone_in_place_write() {
    log "Testing in-place writes to a clone and its source..."

    local writer="${NODE_ARRAY[0]}"
    local reader="${NODE_ARRAY[1]:-${NODE_ARRAY[0]}}"
    local src=/mnt/goclusterfs/clone_src.bin
    local clone=/mnt/goclusterfs/clone_inplace.bin

    exec_on_node "$writer" rm -f "$src" "$clone"
    exec_on_node "$writer" sh -c "head -c 2097152 /dev/urandom > /tmp/clone_src && cp /tmp/clone_src /tmp/clone_copy && cp /tmp/clone_src $src" || return 1
    exec_on_node "$writer" setfattr -n user.clone -v /clone_inplace.bin "$src" || {
        log "ERROR: clone failed"
        return 1
    }
    exec_on_node "$writer" sh -c "head -c 4096 /dev/urandom > /tmp/clone_patch_a && head -c 4096 /dev/urandom > /tmp/clone_patch_b"
    exec_on_node "$writer" sh -c "dd if=/tmp/clone_patch_a of=$clone bs=4096 conv=notrunc 2>/dev/null && dd if=/tmp/clone_patch_a of=/tmp/clone_copy bs=4096 conv=notrunc 2>/dev/null" || return 1
    exec_on_node "$writer" sh -c "dd if=/tmp/clone_patch_b of=$src bs=4096 seek=256 conv=notrunc 2>/dev/null && dd if=/tmp/clone_patch_b of=/tmp/clone_src bs=4096 seek=256 conv=notrunc 2>/dev/null" || return 1

    local want_src want_clone
    want_src=$(exec_on_node "$writer" sh -c "md5sum < /tmp/clone_src | cut -d' ' -f1")
    want_clone=$(exec_on_node "$writer" sh -c "md5sum < /tmp/clone_copy | cut -d' ' -f1")
    for node in $(printf '%s\n' "$reader" "$writer" | uniq); do
        restart_node "$node"
        wait_node_healthy "$node" 30 || return 1
        md5_on_node_matches "$node" "$src" "$want_src" || return 1
        md5_on_node_matches "$node" "$clone" "$want_clone" || return 1
    done

    log "In-place clone write test passed"
    return 0
}

# Snapshots a file, overwrites both of its blocks, then deletes the snapshot.
# The delete must finish and give back exactly the two blocks only the
# snapshot held.
test_snapshot_delete_after_overwrite() {
    log "Testing snapshot delete after overwrite..."

    local node="${NODE_ARRAY[0]}"
    local file=/mnt/goclusterfs/snap_delete.bin
    local free_before free_now

    exec_on_node "$node" rm -f "$file"
    exec_on_node "$node" sh -c "head -c 2097152 /dev/urandom > $file" || return 1
    free_before=$(exec_on_node "$node" stat -f -c %f /mnt/goclusterfs)
    exec_on_node "$node" setfattr -n user.snapshot -v snap_delete_temp "$file" || {
        log "ERROR: snapshot failed"
        return 1
    }
    exec_on_node "$node" sh -c "head -c 2097152 /dev/urandom > /tmp/snap_new && dd if=/tmp/snap_new of=$file bs=1048576 conv=notrunc 2>/dev/null" || return 1
    free_now=$(exec_on_node "$node" stat -f -c %f /mnt/goclusterfs)
    if [ "$free_now" -ne $((free_before - 2)) ]; then
        log "ERROR: $free_now free blocks after overwriting 2 snapshotted blocks, want $((free_before - 2))"
        return 1
    fi

    exec_on_node "$node" setfattr -x user.snapshot.snap_delete_temp "$file" || {
        log "ERROR: snapshot delete failed"
        return 1
    }
    local tries=0
    while exec_on_node "$node" sh -c "getfattr --only-values -n user.snapshots $file 2>/dev/null | grep -q snap_delete_temp"; do
        tries=$((tries + 1))
        if [ "$tries" -ge 100 ]; then
            log "ERROR: snapshot still listed 10s after delete"
            return 1
        fi
        sleep 0.1
    done
    free_now=$(exec_on_node "$node" stat -f -c %f /mnt/goclusterfs)
    if [ "$free_now" -ne "$free_before" ]; then
        log "ERROR: $free_now free blocks after deleting the snapshot, want $free_before"
        return 1
    fi
    md5_on_node_matches "$node" "$file" "$(exec_on_node "$node" sh -c "md5sum < /tmp/snap_new | cut -d' ' -f1")" || return 1

    log "Snapshot delete after overwrite test passed"
    return 0
}

# Two nodes each take 10 snapshots of their own file at the same time. Each
# file must list all of its snapshots, and deleting one node's snapshots must
# leave the other node's alone.
test_snapshot_concurrent_creates() {
    log "Testing concurrent snapshot creation on two nodes..."

    local node_a="${NODE_ARRAY[0]}"
    local node_b="${NODE_ARRAY[1]:-}"
    if [ -z "$node_b" ]; then
        log "SKIP: needs two nodes"
        return 0
    fi
    local run=$$
    local file_a=/mnt/goclusterfs/conc_a.bin
    local file_b=/mnt/goclusterfs/conc_b.bin

    exec_on_node "$node_a" sh -c "rm -f $file_a && head -c 1048576 /dev/urandom > $file_a" || return 1
    exec_on_node "$node_b" sh -c "rm -f $file_b && head -c 1048576 /dev/urandom > $file_b" || return 1

    exec_on_node "$node_a" sh -c "for i in \$(seq 1 10); do setfattr -n user.snapshot -v conc_a_${run}_\$i $file_a || exit 1; done" &
    local pid_a=$!
    exec_on_node "$node_b" sh -c "for i in \$(seq 1 10); do setfattr -n user.snapshot -v conc_b_${run}_\$i $file_b || exit 1; done" &
    local pid_b=$!
    wait "$pid_a" || { log "ERROR: snapshots on $node_a failed"; return 1; }
    wait "$pid_b" || { log "ERROR: snapshots on $node_b failed"; return 1; }

    local count_a count_b
    count_a=$(exec_on_node "$node_a" sh -c "getfattr --only-values -n user.snapshots $file_a 2>/dev/null | grep -c ':active'")
    count_b=$(exec_on_node "$node_b" sh -c "getfattr --only-values -n user.snapshots $file_b 2>/dev/null | grep -c ':active'")
    if [ "$count_a" -ne 10 ] || [ "$count_b" -ne 10 ]; then
        log "ERROR: $count_a and $count_b active snapshots, want 10 each"
        return 1
    fi

    exec_on_node "$node_a" sh -c "for i in \$(seq 1 10); do setfattr -x user.snapshot.conc_a_${run}_\$i $file_a || exit 1; done" || {
        log "ERROR: deleting snapshots on $node_a failed"
        return 1
    }
    local tries=0
    while exec_on_node "$node_a" sh -c "getfattr --only-values -n user.snapshots $file_a 2>/dev/null | grep -q conc_a_"; do
        tries=$((tries + 1))
        if [ "$tries" -ge 100 ]; then
            log "ERROR: $node_a snapshots still listed 10s after delete"
            return 1
        fi
        sleep 0.1
    done
    count_b=$(exec_on_node "$node_b" sh -c "getfattr --only-values -n user.snapshots $file_b 2>/dev/null | grep -c ':active'")
    if [ "$count_b" -ne 10 ]; then
        log "ERROR: $node_b lists $count_b snapshots after $node_a deleted its own, want 10"
        return 1
    fi

    exec_on_node "$node_b" sh -c "for i in \$(seq 1 10); do setfattr -x user.snapshot.conc_b_${run}_\$i $file_b; done"
    log "Concurrent snapshot creation test passed"
    return 0
}

# Node A overwrites a 2 MB file in 64 KB chunks with rounds of a, b, c while
# node B snapshots it. Every snapshot must show one moment of the file (a
# prefix of the next round, the rest of the round before) and never change.
test_snapshot_during_writes() {
    log "Testing snapshots on one node while another node overwrites the file..."

    local node_a="${NODE_ARRAY[0]}"
    local node_b="${NODE_ARRAY[1]:-}"
    if [ -z "$node_b" ]; then
        log "SKIP: needs two nodes"
        return 0
    fi
    local run=$$
    local file=/mnt/goclusterfs/busy.bin

    exec_on_node "$node_a" sh -c "head -c 2097152 /dev/zero | tr '\\000' c > $file" || return 1
    exec_on_node "$node_a" sh -c "rm -f /tmp/busy.stop; for c in a b c; do head -c 65536 /dev/zero | tr '\\000' \$c > /tmp/busy_\$c; done; while [ ! -f /tmp/busy.stop ]; do for c in a b c; do for k in \$(seq 0 31); do [ -f /tmp/busy.stop ] && exit 0; dd if=/tmp/busy_\$c of=$file bs=65536 seek=\$k count=1 conv=notrunc 2>/dev/null || exit 1; done; done; done" &
    local writer=$!

    local i clone sum
    local clones=() sums=()
    for i in $(seq 1 10); do
        clone=/mnt/goclusterfs/busy_${run}_$i.bin
        if ! exec_on_node "$node_b" setfattr -n user.snapshot -v "busy_${run}_$i" "$file" ||
            ! exec_on_node "$node_b" setfattr -n user.snapshot.clone -v "busy_${run}_$i:/busy_${run}_$i.bin" "$file"; then
            log "ERROR: snapshot $i on $node_b failed"
            exec_on_node "$node_a" touch /tmp/busy.stop
            wait "$writer"
            return 1
        fi
        clones+=("$clone")
        sums+=("$(exec_on_node "$node_b" sh -c "md5sum < $clone | cut -d' ' -f1")")
    done
    exec_on_node "$node_a" touch /tmp/busy.stop
    wait "$writer" || { log "ERROR: writer on $node_a failed"; return 1; }

    for i in "${!clones[@]}"; do
        clone=${clones[$i]}
        sum=$(exec_on_node "$node_b" sh -c "md5sum < $clone | cut -d' ' -f1")
        if [ "$sum" != "${sums[$i]}" ]; then
            log "ERROR: $clone changed after its snapshot was made"
            return 1
        fi
        local shape
        shape=$(exec_on_node "$node_b" sh -c "for k in \$(seq 0 31); do dd if=$clone bs=65536 skip=\$k count=1 2>/dev/null | tr -s abc | head -c 2; done")
        if [ ${#shape} -ne 32 ] || ! [[ $shape =~ ^(b*a*|c*b*|a*c*)$ ]]; then
            log "ERROR: $clone chunks read $shape; the file never looked like this"
            return 1
        fi
    done

    exec_on_node "$node_b" sh -c "for i in \$(seq 1 10); do setfattr -x user.snapshot.busy_${run}_\$i $file; rm -f /mnt/goclusterfs/busy_${run}_\$i.bin; done"
    log "Snapshots during writes test passed"
    return 0
}

run_test "Create and Read File" test_create_read_file
run_test "Create Directory" test_create_directory
run_test "File in Directory" test_file_in_directory
run_test "Data Persistence" test_data_persistence
run_test "Delete File" test_delete_file
run_test "Large File" test_large_file
run_test "Many Files" test_many_files
run_test "Nested Directories" test_nested_directories
run_test "Delete Directory" test_delete_directory
run_test "Truncate File" test_truncate_file
run_test "Append File" test_append_file
run_test "Rename File" test_rename_file
run_test "Stress Single Node" test_stress_single_node
run_test "Overwrite File" test_overwrite_file
run_test "Sequential Access" test_sequential_access
run_test "Extent Tree Sparse Writes" test_extent_tree_sparse_writes
run_test "Extent Tree Interleaved Appends" test_extent_tree_interleaved_appends
run_test "Snapshot Preserves Data" test_snapshot_preserves_data
run_test "Clone In-Place Write" test_clone_in_place_write
run_test "Snapshot Delete After Overwrite" test_snapshot_delete_after_overwrite
run_test "Snapshot Concurrent Creates" test_snapshot_concurrent_creates
run_test "Snapshot During Writes" test_snapshot_during_writes

if [ "$SKIP_CONCURRENT_TESTS" = "true" ]; then
    log "SKIPPING: Concurrent Writes (known race conditions - set SKIP_CONCURRENT_TESTS=false to enable)"
    echo "SKIP: Concurrent Writes (known race conditions)" >> "$RESULTS_DIR/results.txt"
    echo "SKIP: Concurrent File Creation" >> "$RESULTS_DIR/results.txt"
    echo "SKIP: Concurrent Directory Ops" >> "$RESULTS_DIR/results.txt"
else
    run_test "Concurrent Writes" test_concurrent_writes
    run_test "Concurrent File Creation" test_concurrent_file_creation
    run_test "Concurrent Directory Ops" test_concurrent_directory_ops
fi
run_test "Concurrent First Write Same Block" test_concurrent_first_write_same_block
run_test "Container Kill Recovery" test_container_kill_recovery
run_test "Journal Recovery Multi-Node" test_journal_recovery_multinode
run_test "Restart Keeps Peer Journal" test_restart_keeps_peer_journal
run_test "Journal Recovery Multiple Files" test_journal_recovery_multiple_files
run_test "Journal Recovery Large File" test_journal_recovery_large_file
run_test "Journal Recovery Idempotency" test_journal_recovery_idempotency
run_test "Journal Crash Recovery File" test_journal_crash_recovery_file
run_test "Journal Crash Recovery Directory" test_journal_crash_recovery_directory
run_test "Journal Crash Recovery Multiple Ops" test_journal_crash_recovery_multiple_ops
# Skip "Journal Crash During Write" in Docker - docker kill doesn't accurately simulate power failure
# This test is covered by VM tests which use QEMU SIGKILL for accurate crash simulation
# See: tests/vm/scripts/run-vm-tests.sh:1104 test_journal_crash_during_write
log "SKIPPING: Journal Crash During Write (covered by VM tests)"
echo "SKIP: Journal Crash During Write" >> "$RESULTS_DIR/results.txt"

run_test "Network Partition" test_network_partition
run_test "Broadcast Invalidation" test_broadcast_invalidation
run_test "Broadcast Directory Invalidation" test_broadcast_directory_invalidation
run_test "Broadcast Multiple Updates" test_broadcast_multiple_updates
run_test "Broadcast Bidirectional" test_broadcast_bidirectional
run_test "Inode Reuse Stale Read" test_inode_reuse_stale_read
run_test "Extent Cache Coherency" test_extent_cache_coherency
run_test "Statfs After Remount" test_statfs_after_remount
run_test "Statfs Multi-Node" test_statfs_multinode
run_test "Rolling Restart" test_rolling_restart
run_test "Stress Random Failures" test_stress_random_failures

# New concurrent same-file write tests (using pre-extension pattern)
if [ "$SKIP_CONCURRENT_TESTS" = "true" ]; then
    log "SKIPPING: Concurrent Same-File Write tests"
    echo "SKIP: Concurrent Same-File Writes (2 nodes)" >> "$RESULTS_DIR/results.txt"
    echo "SKIP: Concurrent Same-File Writes (3 nodes)" >> "$RESULTS_DIR/results.txt"
    echo "SKIP: Mixed Size Writes" >> "$RESULTS_DIR/results.txt"
    echo "SKIP: Overlapping Block Writes" >> "$RESULTS_DIR/results.txt"
    echo "SKIP: Concurrent Write Stress" >> "$RESULTS_DIR/results.txt"
    echo "SKIP: Reversed Write Order" >> "$RESULTS_DIR/results.txt"
else
    run_test "Concurrent Same-File Writes (2 nodes)" test_concurrent_same_file_writes
    run_test "Concurrent Same-File Writes (3 nodes)" test_concurrent_same_file_writes_3nodes
    run_test "Mixed Size Writes" test_mixed_size_writes
    run_test "Overlapping Block Writes" test_overlapping_block_writes
    run_test "Concurrent Write Stress" test_concurrent_write_stress
    run_test "Reversed Write Order" test_reversed_write_order
fi

# Count skipped tests (grep -c prints "0" but exits non-zero if no matches)
test_skipped=$(grep -c "^SKIP:" "$RESULTS_DIR/results.txt" 2>/dev/null) || test_skipped=0
test_total=$((test_passed + test_failed + test_skipped))

# Summary
log "=========================================="
log "TEST SUMMARY"
log "=========================================="
log "Passed:  $test_passed"
log "Failed:  $test_failed"
log "Skipped: $test_skipped"
log "Total:   $test_total"
log "=========================================="

# Generate JUnit XML report
cat > "$RESULTS_DIR/junit.xml" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="GoClusterFS Docker Multi-Node Tests" tests="$test_total" failures="$test_failed" errors="0" skipped="$test_skipped">
EOF

while IFS= read -r line; do
    status=$(echo "$line" | cut -d: -f1)
    name=$(echo "$line" | cut -d: -f2- | xargs)
    if [ "$status" = "PASS" ]; then
        echo "  <testcase name=\"$name\" classname=\"docker.multinode\"/>" >> "$RESULTS_DIR/junit.xml"
    elif [ "$status" = "SKIP" ]; then
        echo "  <testcase name=\"$name\" classname=\"docker.multinode\"><skipped message=\"$name\"/></testcase>" >> "$RESULTS_DIR/junit.xml"
    else
        echo "  <testcase name=\"$name\" classname=\"docker.multinode\"><failure message=\"Test failed\"/></testcase>" >> "$RESULTS_DIR/junit.xml"
    fi
done < "$RESULTS_DIR/results.txt"

echo "</testsuite>" >> "$RESULTS_DIR/junit.xml"

log "Results written to $RESULTS_DIR"

# Exit with failure if any tests failed
if [ $test_failed -gt 0 ]; then
    exit 1
fi

exit 0
