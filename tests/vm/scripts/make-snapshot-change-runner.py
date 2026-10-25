#!/usr/bin/env python3
"""Builds run-vm-snapchange.sh: run-vm-tests.sh running only the instrumented
snapshot-during-writes test, ten times. Run from the repository root, then
run tests/vm/scripts/run-vm-snapchange.sh 2 under flock /tmp/goclusterfs-test.lock."""

v = open("tests/vm/scripts/run-vm-tests.sh").read()
debug = open("tests/vm/scripts/snapshot-change-debug.sh").read()
v = v.replace("test_snapshot_concurrent_creates() {", debug + "\ntest_snapshot_concurrent_creates() {", 1)
start = v.index("    # Run standard tests")
end = v.index("    # Summary")
tests = "".join(f'    run_test "Snapshot During Writes Debug {i}" test_snapshot_during_writes_debug\n' for i in range(1, 11))
open("tests/vm/scripts/run-vm-snapchange.sh", "w").write(v[:start] + tests + "\n" + v[end:])
