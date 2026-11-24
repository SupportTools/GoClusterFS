# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GoClusterFS is a clustered FUSE filesystem written in Go. Several Linux nodes mount the same shared block device at the same time. Nodes coordinate through structures on the device itself: a node registry, lock records with one claim slot per node, and a write-ahead journal. All device I/O uses O_DIRECT, so no node trusts its own page cache. UDP multicast messages make cache invalidation faster, but correctness does not depend on them.

FUSE binding: `github.com/hanwen/go-fuse/v2`. Linux only.

## Build and Run

```bash
make build                 # ./goclusterfs-mount, ./goclusterfs-fsck and ./gfsctl
make dashboard build       # same, with the web dashboard embedded (needs Node 22 and npm)
go build ./... && go vet ./...
```

Run against a file-backed image. The image must sit on a filesystem that supports O_DIRECT (tmpfs only on Linux 6.6+).

```bash
truncate -s 1G disk.img
mkdir -p mnt
./goclusterfs-mount -device ./disk.img -mount ./mnt -format   # first time only, wipes the volume
./goclusterfs-mount -device ./disk.img -mount ./mnt           # later mounts
fusermount3 -u mnt                                            # or Ctrl+C the mount process
./goclusterfs-fsck ./disk.img                                 # device is a positional argument
```

`goclusterfs-mount` stays in the foreground until the filesystem is unmounted. `-format` formats and then mounts. `-debug` turns on `[DEBUG]` log lines and `-fuse-debug` go-fuse request tracing. The three commands share exit codes (`internal/cli`), listed in the README. Flags and defaults: `./goclusterfs-mount -h`, `./goclusterfs-fsck -h`, [docs/API-CONFIG.md](docs/API-CONFIG.md).

## Code Map

| Path | What it does |
|------|--------------|
| `cmd/goclusterfs-mount` | `goclusterfs-mount`: fixes the on-disk layout, runs journal recovery, wires all managers, mounts with go-fuse |
| `cmd/goclusterfs-fsck` | `goclusterfs-fsck`: 8-phase checker with `-repair` and `-force` modes, and journal slot tools: `-list-slots`, `-inspect-slot`, and `-release-slot` with `-salvage`, `-discard-journal` and `-report` |
| `cmd/gfsctl` | `gfsctl`: CLI for the HTTP API (status, health, stats, metrics, snapshots, quotas); `quota set` is its only write |
| `internal/goclusterfs` | FUSE nodes (`FS` root, `Dir`, `File`) and every VFS operation |
| `internal/storage` | O_DIRECT device, 4 KB aligned buffers, CRC32C |
| `internal/superblock` | Superblock format and the on-disk layout; `-format` writes it, mount reads it |
| `internal/bitmap` | Block allocation bitmap, CRC32C per 4 KB sector |
| `internal/inode` | Inode table, 256-byte inodes, CRC32C per sector |
| `internal/extent` | File block mapping: 2 extents inline in the inode, the rest in an on-disk overflow tree |
| `internal/dir` | Directory entries |
| `internal/lock` | Distributed locks stored on the device: a node writes its claim into its own slot of a lock record, then reads the record. Reads take a file's I/O lock shared and writes exclusive for each whole request |
| `internal/node` | Node registry, heartbeats, failed-node detection |
| `internal/journal` | Write-ahead journal, local and cross-node recovery |
| `internal/cache` | Inode and extent caches, checked against on-disk generation counters |
| `internal/cache/broadcast` | UDP multicast/unicast cache invalidation, optional HMAC |
| `internal/refcount` | Per-block reference counts for copy-on-write |
| `internal/snapshot` | Snapshot registry and background refcount worker |
| `internal/quota` | Project quota table: header and 64-byte records in the hidden quota inode's data blocks, and `Enable`, which creates it at `-format` and for `fsck -enable-quotas` |
| `internal/sfb` | Small-file containers (4/8/16/64 KB slots in a 1 MB block). Disabled: nothing allocates slots, `goclusterfs` only reads and migrates files older builds stored there |
| `internal/trim` | Batched `BLKDISCARD` for freed blocks |
| `internal/fsck` | The fsck phases |
| `internal/metrics` | Atomic counters, not used by `goclusterfs-mount` yet |
| `internal/api` | HTTP API served by `goclusterfs-mount`: `/health`, `/metrics`, `/api/v1/status`, `/api/v1/stats/...`; loopback only unless `-api-token` is set |
| `internal/stats` | Background sampler that feeds `/api/v1/stats/...`, `/metrics` and the capacity in `/api/v1/status` |
| `internal/cli` | Exit codes, flag parsing and error hints shared by the three commands |
| `internal/logging` | The `-debug` switch: `logging.Debugf` prints only with `-debug` |
| `internal/version` | Build version, set with `-ldflags -X` by `scripts/dist.sh` |
| `internal/webui` | Serves the web dashboard under `/ui/`; embeds `make dashboard`'s build, or a placeholder page |
| `web/dashboard` | React dashboard source (Vite, npm); `make dashboard` builds it into `internal/webui/build/dist` |

## On-Disk Layout

`internal/superblock` (`calculateLayout`) defines this layout, format version 3. `-format` writes it into the 4 KB superblock at offset 0, and mount takes every offset from there. Data blocks are 1 MB unless `-format -block-size` picks a power of two from 8 KB; extent tree nodes are max(block size, 64 KB). Metadata I/O uses 4 KB sectors.

| Offset | Size | Region |
|--------|------|--------|
| 0 | 1 MB | Superblock (first 4 KB) |
| 1 MB | 1 MB | Node registry: 64 slots of 4 KB |
| 2 MB | 64 MB | Lock records: 256 records of 64 claim slots of 4 KB, one slot per registry slot |
| 66 MB | 1 MB | Snapshot registry |
| 67 MB | 256 MB | Journals: 64 × 4 MB, one per registry slot |
| 323 MB | 1 bit per data block: 1 MB per 8 TB at 1 MB blocks | Block bitmap |
| next | 8 bytes per data block: about 8 MB per TB at 1 MB blocks | Block refcounts |
| next | 256 bytes per inode | Inode table: one inode per `-bytes-per-inode` bytes of device (default 1 MB at 1 MB blocks, max(2 × block size, 16 KB) below), 4096 to 16M inodes; root is inode 2 |
| next | rest | Data blocks |

The last three metadata regions are sized at format time, each at least 1 MB and rounded up to whole MB. On devices up to 4 GB the data blocks start at 326 MB. The smallest device is 327 MB. Registry slot `i` owns the journal at `67 MB + i × 4 MB`, and `Register` records that region in the slot. On mount a node replays only its own slot's journal, which may hold a dirty journal from the slot's previous owner. A crashed peer's journal is replayed by a surviving node through the peer's registry slot. Mount refuses version 1 images (one journal shared by all nodes) and version 2 images (1 MB bitmap, refcount and inode regions: 4096 inodes, refcounts overflow past 128 GB). It also refuses version 3 images formatted before per-node lock slots, which have no lock slot size in the superblock and a 1 MB lock region, version 3 images formatted before each lock type had its own records, which lack the `RequiredTypedLockRecords` flag, and version 3 images formatted before file I/O locks, which lack `RequiredFileIOLocks` (0x40). Copy the data off with the old build and reformat. Builds from before those flags refuse newer images with `unknown required flags 0x10` or `0x40`. Each registry slot also records its node's lock layout (`lock.LayoutID`), and `Register` refuses to join while a live node shows another one. `-format` also sets `RequiredSymlinks`, because a symlink of up to 92 bytes keeps its target in the inode where the extents go; builds from before it refuse such images with `unknown required flags 0x20`, and on older version 3 images `symlink(2)` returns EOPNOTSUPP. `-format` also sets `RequiredProjectQuotas` (0x200) and puts the project quota table, 1 MiB or 16,065 projects at any block size, in the data blocks of a hidden inode, which the superblock names (`QuotaInode`, bytes 268–279); an inode's project ID is in its bytes 212–215, and flag bit 8 is PROJINHERIT. The ID's low 20 bits name the project's record, and the bits above count the record's reuses; a record frees once its project has no root directory and no usage, and changes logged for an earlier ID in it are dropped. `goclusterfs-fsck -enable-quotas` adds the table to an older volume with every node stopped. Builds without quotas refuse such images with `unknown required flags 0x200`.

## Snapshots and Clones

Snapshots and clones work through extended attributes on a file (`internal/goclusterfs/fs.go`, `File.Setxattr`):

- `setfattr -n user.snapshot -v <name> <file>` creates a snapshot
- `getfattr -n user.snapshots <file>` lists snapshots
- `setfattr -n user.snapshot.delete -v <name> <file>` or `setfattr -x user.snapshot.<name> <file>` deletes one
- `setfattr -n user.clone -v /<new-name> <file>` clones the file
- `setfattr -n user.snapshot.clone -v <snapshot>:/<new-name> <file>` clones from a snapshot

Clone paths are absolute from the filesystem root, and clones can only go in the root directory. A snapshot records its file's inode number and birth generation, so a later file in the same inode slot does not see it; once its file is deleted, or has no name left even while open, it is an orphan, listed and deleted only through the API without a path.

## Current Limitations

- Linux only. Needs FUSE and a backing store that supports O_DIRECT.
- At most 64 nodes mount a volume at once, one per registry slot. A node judges another by whether its heartbeat changes, timed on its own clock, so failover does not need clocks to agree.
- `goclusterfs-fsck -repair` fixes only block refcounts, sharing that older versions left uncounted, leaked blocks, inodes no directory names, snapshot registry sectors that fail their checksum, a directory's wrong `.` or `..`, project quota usage, and the superblock checksum. It reports every other problem without fixing it.
- The inode count is set at format time and cannot change. `statfs` (`df`) counts free inodes with a background scan of the whole inode table, so other nodes' creates and removes show up after up to 30 times the scan time, at least a second.
- SFB is disabled: writes never allocate SFB slots, so every file uses full blocks. `internal/sfb` container I/O is broken (unaligned header buffers, no data offset) and stays until it is fixed or removed.
- Rename supports `RENAME_NOREPLACE`; `RENAME_EXCHANGE` and `RENAME_WHITEOUT` fail with `EINVAL`. Symlinks need a volume formatted by a build with symlink support.

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

### Nightly Workflow

`.github/workflows/nightly.yml` runs at 03:17 UTC what pull request CI skips: the integration suite without `-short` (grouped by `tests/scripts/integration-timings-full.txt`), the Docker suite with its concurrent tests (group 4 of `run-tests.sh`), `-race -count=3` on lock, journal, inode and dir, the `GOCLUSTERFS_EIO_SWEEP=metadata` EIO sweep, and every `FuzzXxx` for its budget in `tests/scripts/fuzz-budgets.txt` (25 minutes if unlisted) with its corpus cached between nights. New fuzz targets join automatically. A failure fails the run; the Summary job lists failed tests and uploads them as the `nightly-failures` artifact. It posts nothing to issues.

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

## Commands Config

| Setting | Value |
|---------|-------|
| TaskForge Project ID | 88 |
| Build command | `make build` |
| Test command | `make test-unit` |
| Lint command | `make lint` |
| Tech stack | Go (FUSE filesystem) |
