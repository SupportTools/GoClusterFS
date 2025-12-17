#!/bin/bash
# GoClusterFS Docker Test Runner
# Orchestrates multi-node tests from within the orchestrator container

set -e

RESULTS_DIR="${RESULTS_DIR:-/results}"
NODES="${NODES:-node1,node2,node3,node4}"

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
# Test Cases
# =============================================================================

test_passed=0
test_failed=0

run_test() {
    local test_name=$1
    local test_func=$2

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
# Main Test Execution
# =============================================================================

log "Waiting for all nodes to be healthy..."
for node in "${NODE_ARRAY[@]}"; do
    wait_node_healthy "$node" 60 || exit 1
done

log "All nodes healthy, starting tests..."

# Clear results
> "$RESULTS_DIR/results.txt"

# Run test suite
run_test "Concurrent Writes" test_concurrent_writes
run_test "Container Kill Recovery" test_container_kill_recovery
run_test "Network Partition" test_network_partition
run_test "Rolling Restart" test_rolling_restart
run_test "Stress Random Failures" test_stress_random_failures

# Summary
log "=========================================="
log "TEST SUMMARY"
log "=========================================="
log "Passed: $test_passed"
log "Failed: $test_failed"
log "=========================================="

# Generate JUnit XML report
cat > "$RESULTS_DIR/junit.xml" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="GoClusterFS Docker Multi-Node Tests" tests="$((test_passed + test_failed))" failures="$test_failed" errors="0">
EOF

while IFS= read -r line; do
    status=$(echo "$line" | cut -d: -f1)
    name=$(echo "$line" | cut -d: -f2 | xargs)
    if [ "$status" = "PASS" ]; then
        echo "  <testcase name=\"$name\" classname=\"docker.multinode\"/>" >> "$RESULTS_DIR/junit.xml"
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
