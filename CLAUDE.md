# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GoClusterFS is a clustered FUSE filesystem written in Go. Several Linux nodes mount the same shared block device at the same time. Nodes coordinate through structures on the device itself: a node registry, CAS locks, and a write-ahead journal. All device I/O uses O_DIRECT, so no node trusts its own page cache. UDP multicast messages make cache invalidation faster, but correctness does not depend on them.

FUSE binding: `github.com/hanwen/go-fuse/v2`. Linux only.

## Build and Run

```bash
make build                 # ./goclusterfs-mount and ./goclusterfs-fsck
go build ./... && go vet ./...
```

Run against a file-backed image. The image must sit on a filesystem that supports O_DIRECT (not tmpfs).

```bash
truncate -s 1G disk.img
mkdir -p mnt
./goclusterfs-mount -device ./disk.img -mount ./mnt -format   # first time only, wipes the volume
./goclusterfs-mount -device ./disk.img -mount ./mnt           # later mounts
fusermount3 -u mnt                                            # or Ctrl+C the mount process
./goclusterfs-fsck ./disk.img                                 # device is a positional argument
```

`goclusterfs-mount` stays in the foreground until the filesystem is unmounted. `-format` formats and then mounts. It logs a lot of `[DEBUG]` lines even without `-debug`, which only turns on go-fuse request tracing. Flags and defaults: `./goclusterfs-mount -h`, `./goclusterfs-fsck -h`, [docs/API-CONFIG.md](docs/API-CONFIG.md).

## Code Map

| Path | What it does |
|------|--------------|
| `cmd/mount` | `goclusterfs-mount`: fixes the on-disk layout, runs journal recovery, wires all managers, mounts with go-fuse |
| `cmd/fsck` | `goclusterfs-fsck`: 8-phase checker with `-repair` and `-force` modes |
| `cmd/crctest` | Prints CRC32C test values |
| `internal/goclusterfs` | FUSE nodes (`FS` root, `Dir`, `File`) and every VFS operation |
| `internal/storage` | O_DIRECT device, 4 KB aligned buffers, CRC32C |
| `internal/superblock` | Superblock format (see limitations: `cmd/mount` does not write it) |
| `internal/bitmap` | Block allocation bitmap, CRC32C per 4 KB sector |
| `internal/inode` | Inode table, 256-byte inodes, CRC32C per sector |
| `internal/extent` | File block mapping, up to 2 extents inline in the inode |
| `internal/dir` | Directory entries |
| `internal/lock` | Distributed CAS locks stored on the device |
| `internal/node` | Node registry, heartbeats, failed-node detection |
| `internal/journal` | Write-ahead journal, local and cross-node recovery |
| `internal/cache` | Inode and extent caches, checked against on-disk generation counters |
| `internal/cache/broadcast` | UDP multicast/unicast cache invalidation, optional HMAC |
| `internal/refcount` | Per-block reference counts for copy-on-write |
| `internal/snapshot` | Snapshot registry and background refcount worker |
| `internal/sfb` | Small-file containers: 4/8/16/64 KB slots inside a 1 MB block |
| `internal/trim` | Batched `BLKDISCARD` for freed blocks |
| `internal/fsck` | The fsck phases |
| `internal/metrics` | Atomic counters, not used by `cmd/mount` yet |

## On-Disk Layout

`cmd/mount/main.go` hardcodes this layout. Data blocks are 1 MB. Metadata I/O uses 4 KB sectors.

| Offset | Size | Region |
|--------|------|--------|
| 0 | 1 MB | Reserved for the superblock (not written today) |
| 1 MB | 1 MB | Block bitmap |
| 2 MB | 1 MB | Inode table: 4096 inodes of 256 bytes; root is inode 2 |
| 3 MB | 1 MB | Node registry |
| 4 MB | 1 MB | Lock records |
| 5 MB | 4 MB | Journal |
| 9 MB | 1 MB | Block refcounts |
| 10 MB | 1 MB | Snapshot registry |
| 11 MB | rest | Data blocks |

The smallest device is 12 MB.

## Snapshots and Clones

Snapshots and clones work through extended attributes on a file (`internal/goclusterfs/fs.go`, `File.Setxattr`):

- `setfattr -n user.snapshot -v <name> <file>` creates a snapshot
- `getfattr -n user.snapshots <file>` lists snapshots
- `setfattr -n user.snapshot.delete -v <name> <file>` or `setfattr -x user.snapshot.<name> <file>` deletes one
- `setfattr -n user.clone -v /<new-name> <file>` clones the file
- `setfattr -n user.snapshot.clone -v <snapshot>:/<new-name> <file>` clones from a snapshot

Clone paths are absolute from the filesystem root, and clones can only go in the root directory.

## Current Limitations

- Linux only. Needs FUSE and a backing store that supports O_DIRECT.
- `cmd/mount` never writes a superblock, and `internal/superblock` describes a different layout from the one above. `goclusterfs-fsck` checks the superblock first, so it stops at phase 1 with `invalid superblock magic` on any volume made with `-format`.
- Every node uses the same 4 MB journal region at 5 MB. `cmd/mount` does not give each node its own journal slot.
- Each file holds at most 2 extents (inline in the inode). The overflow extent tree is being built.
- The inode count is fixed at 4096.
- No rename, symlinks, or hard links.

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
