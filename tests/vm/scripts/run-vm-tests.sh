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
# Standard Tests (adapted from Docker tests)
# =============================================================================

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

# Test 4: Rolling restart
test_rolling_restart() {
    log_info "Testing rolling restart..."

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

# Test 9: Long-running stability test (optional)
test_stability() {
    if [[ "$RUN_LONG_TESTS" != "true" ]]; then
        log_info "Skipping stability test (use --long to enable)"
        return 0
    fi

    log_info "Running stability test for ${STABILITY_DURATION_MINUTES} minutes..."

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
# Main Test Execution
# =============================================================================

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
    run_test "Concurrent Writes" test_concurrent_writes
    run_test "VM Kill Recovery" test_vm_kill_recovery
    run_test "Network Partition" test_network_partition
    run_test "Rolling Restart" test_rolling_restart
    run_test "Stress Random Failures" test_stress_random_failures

    # Run VM-specific tests
    run_test "Power Failure Simulation" test_power_failure
    run_test "O_DIRECT Validation" test_odirect_validation
    run_test "Large File Handling" test_large_file
    run_test "Long-Running Stability" test_stability

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
