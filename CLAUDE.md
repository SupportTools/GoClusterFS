# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GoClusterFS is a FUSE-based clustered filesystem written in Go. It mounts a block device and exposes it as a filesystem, designed for shared storage scenarios across cluster nodes.

## Build and Run Commands

```bash
# Build the binary
go build -o GoClusterFS .

# Run (requires root or fuse group membership)
./GoClusterFS <mountpoint> <block_device>

# Example with a file-backed block device
dd if=/dev/zero of=VirtBlock.img bs=1M count=3000  # Create 3GB virtual block
./GoClusterFS /mnt/goclusterfs ./VirtBlock.img

# Unmount
fusermount -u /mnt/goclusterfs
```

## Architecture

### Storage Layout
- **Block size**: 1MB fixed blocks
- **Block 0**: Reserved for metadata (file table serialized with `gob`)
- **Blocks 1+**: File data (one block per file currently)

### Core Types
- `FS`: Root filesystem implementing `fs.FS` and `Statfs`
- `Dir`: Root directory implementing `Attr`, `ReadDirAll`, `Lookup`, `Create`
- `File`: File node implementing `Attr`, `ReadAll`, `Write`, `Flush`, `Setxattr`
- `FileMetadata`: Persisted file info (inode, filename, size, block index)

### File Locking
File locks are managed via extended attributes for cluster coordination:
- Set lock: `setfattr -n user.lock -v <owner_id> <file>`
- Release lock: `setfattr -n user.unlock -v <owner_id> <file>`

### Current Limitations
- Single block per file (max 1MB file size)
- No directory hierarchy (flat namespace)
- No delete operation implemented
- Metadata not checksummed

## Testing

### Test Environments

GoClusterFS has three test environments that must be kept in sync:

1. **Go Integration Tests** (`tests/integration/`) - Fast, in-process tests
2. **Docker Tests** (`tests/docker/`) - Multi-node tests in containers
3. **VM Tests** (`tests/vm/`) - Multi-node tests in QEMU VMs (power failure, O_DIRECT)

### Test Commands

```bash
make test-unit           # Unit tests only (fast)
make test-integration    # Go integration tests (requires FUSE)
make test-multinode-docker  # Docker multi-node tests
make test-multinode-vm      # VM multi-node tests
make test-parity         # Validate test coverage across environments
make test-parity-report  # Detailed coverage report
```

### Test Parity System (IMPORTANT)

All test scenarios are tracked in `tests/test-manifest.yaml`. This manifest is the **single source of truth** for test coverage.

**When adding a new test, you MUST:**

1. **Add to manifest FIRST** - Add scenario to `tests/test-manifest.yaml`
2. **Implement Go test** - Write the Go integration test
3. **Implement Docker test** - Write shell test or mark as `TODO` with justification
4. **Implement VM test** - Write shell test or mark as `TODO` with justification
5. **Run parity check** - `make test-parity` must pass (or acknowledge gaps)

**Priority Levels:**
- `critical` - Must exist in all applicable environments
- `high` - Should exist, gaps flagged as warnings
- `medium` - Nice to have, tracked for visibility
- `go_only` - Unit tests, not applicable to Docker/VM
- `vm_only` - Requires VM features (power failure simulation)

**Example manifest entry:**
```yaml
- id: my-new-test
  name: My new feature test
  description: Tests the new feature behavior
  priority: high
  environments:
    go: integration_test.go::TestMyNewFeature
    docker: run-tests.sh::test_my_new_feature
    vm: run-vm-tests.sh::test_my_new_feature
```

### Concurrent Write Tests

For tests involving concurrent writes from multiple nodes, use the **pre-extension pattern**:

1. Calculate final expected file size upfront
2. Create file at that size BEFORE concurrent writes begin
3. Each node writes to its designated offset
4. Verify data at each offset after writes complete

This avoids TOCTOU races in the distributed locking system.
