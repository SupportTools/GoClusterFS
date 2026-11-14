# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Upgrade notes

- The on-disk format is now version 3. Mount refuses devices formatted by earlier builds. Version 1 kept one journal for all nodes. Version 2 had room for 4096 inodes, and on devices over 128 GB its refcounts overwrote the snapshot registry and then data blocks. Copy the data off with the old build, reformat with `-format`, and copy it back.
- Devices must be at least 327 MB: the layout reserves 256 MB for 64 node journals of 4 MB and 64 MB for lock records.
- Mount also refuses version 3 devices formatted before lock records had one claim slot per node, or before each lock type had its own lock records. Older builds refuse devices this version formats. Copy the data off with the build that formatted them and reformat.
- Node clocks must agree within a few seconds. See the node requirements in `docs/OPERATIONS.md`.
- Upgrade all nodes together: unmount on every node, upgrade, then mount again. Don't mount one shared device with a mix of old and new binaries.
  - Older binaries reset an inode's generation counters to 0 when they free it, so other nodes can serve stale data after the inode number is reused.
  - Older binaries zero the extent-tree generation (inode bytes 188–195) when they rewrite an inode. Newer binaries then cannot read that file if it has more than two extents, because such files use an extent tree.
  - Directory entries now fill all 4 KB chunks of a directory block. When an older binary removes the first entry of a later chunk, it grows the last entry of the chunk before it across the boundary. This version then stops listing that entry, and fsck reports the directory block.
  - Older binaries read a newer node's waiting mark in a lock record as a held lock.
- Run `goclusterfs-fsck -repair` before the first mount after upgrading an image that has snapshots or clones. Unmount the device on every node first.
- Older versions marked a file's blocks shared, on snapshot or clone, without counting the owners. Until `goclusterfs-fsck -repair` counts them, this version never frees such blocks, so deleting those files leaks their space, and it refuses to snapshot or clone them (`ENOTSUP`). In-place writes to an old clone still change its source.
- Snapshots now record the birth generation of the file they were taken of, so a later file that reuses the inode number cannot list, clone or delete them. Snapshots older versions made do not record it, and some captured no data, so after the upgrade they are orphans: `getfattr -n user.snapshots` no longer lists them, `GET /api/v1/snapshots` lists them with `orphan: true`, and `DELETE /api/v1/snapshots/<name>` without `path` deletes them. Older binaries keep the birth generation in inode bytes 196–203 only while they do not rewrite the inode, so upgrade all nodes together.
- `-skip-recovery` now takes the slot number from the recovery error, as `-skip-recovery=N`, and discards a journal only when the mount takes slot N. A bare `-skip-recovery` is a usage error.

### Added

- `goclusterfs-mount`: a FUSE filesystem that several Linux nodes can mount from one shared block device.
- `-format` sizes the block bitmap, refcount table and inode table from the device. `-bytes-per-inode` sets the inode count (default one per MB of device, 4096 to 16,777,216).
- `O_DIRECT` device I/O, split into shards so requests can run in parallel.
- Distributed locks on the shared device for block allocation, inode updates, and directory changes. Each node writes its claim into its own slot of a lock record and then reads the record, so two nodes cannot both take a write lock however slow the storage is. File writes do not take these locks yet, so two nodes extending the same file can lose each other's data.
- Write-ahead journal with batch commits and a recovery check on mount.
- Intent logging for multi-step operations.
- Inode cache with generation-based coherency checks, and an extent cache.
- UDP multicast cache invalidation between nodes, with optional HMAC authentication (`-broadcast-secret`).
- Batched TRIM of freed blocks (`-trim`).
- SFB containers for files under 64 KB, disabled: every file uses normal extents and `-sfb` has no effect. A file that an older build stored in an SFB slot stays readable, moves to a full block on its next write or truncate, and frees its slot when deleted.
- Experimental: file snapshots and writable clones through `user.snapshot*` and `user.clone` extended attributes. Read a snapshot by cloning it with `user.snapshot.clone`.
- `goclusterfs-fsck` consistency checker. `-repair` rebuilds block refcounts from every file, snapshot and clone. It refuses to run while the device is in use, and `-repair -dry-run` shows what it would change. Exit codes follow fsck(8): `0` no errors, `1` repaired, `4` errors remain, `8` failed or refused, `16` usage error.
- Benchmark suite (`tests/benchmark`) and an `internal/metrics` package.
- `/metrics` and `/api/v1/stats/lock` report, per lock type, the acquires served, the acquires that found the lock taken, and how long those waited (`goclusterfs_lock_acquires_total`, `goclusterfs_lock_blocked_total`, `goclusterfs_lock_wait_seconds_total`).
- Go, Docker, and QEMU VM test suites, with `tests/test-manifest.yaml` and `make test-parity` to track coverage across them.
- Fuzz tests for the on-disk decoders (superblock, inode, snapshot descriptor, journal superblock and records) and the journal recovery scan.
- CI gates: unit tests with the race detector and coverage, golangci-lint (`make lint`), govulncheck (`make vuln`), and a `go mod tidy` check.
- Release workflow: pushing a `v*` tag publishes a GitHub Release with `linux/amd64` and `linux/arm64` archives, checksums, and the matching `CHANGELOG.md` section. `scripts/dist.sh` builds the same archives locally.
- `goclusterfs-fsck -version` prints the release version, commit, and commit date.
- HTTP API in `goclusterfs-mount` (`-api`, on by default): `/health`, `/metrics`, `/api/v1/status` and `/api/v1/stats/...`. It listens on `127.0.0.1:8080` (`-api-addr`). Mount refuses a non-loopback `-api-addr` or `-api-cors '*'` without `-api-token`, and checks the API flags before it opens the device. `/metrics` uses the Prometheus client library and adds Go runtime and process metrics. `-api-rate-limit` limits requests per client IP (off by default).
- `-api-token-file` (mode 0600 or stricter) and `GOCLUSTERFS_API_TOKEN` for the API token. Mount warns when it comes from `-api-token`, which `ps` shows to every local user.
- HTTP API write endpoints, only with a token: create, list, clone and delete snapshots, clone files, and flush the journal, flush TRIM or clear caches on this node. Snapshot and clone requests call `setxattr` through the mount, like `setfattr`.
- `gfsctl`, a read-only CLI for the HTTP API: `status`, `health`, `stats`, `metrics`, with watch mode and JSON output. Release archives include it.
- Web dashboard at `/ui/` on the API server (`-dashboard`, on by default): status, capacity, cache, journal, TRIM and lock counters, the node registry, and per-file snapshots and clones. Release archives include it; `make dashboard build` builds it from source.
- `goclusterfs-mount -version`, which prints the same version string as `goclusterfs-fsck -version`. `/api/v1/status` reports it too.
- Mount logs a `WARNING` line at startup when `-broadcast` is on without `-broadcast-secret`.
- Design, deployment, operations, and troubleshooting docs in `docs/`.
- Apache License 2.0.
- A shutdown step that takes longer than 2 seconds logs its name and duration, and a shutdown still running after 8 seconds logs every goroutine's stack.

- `-cache-inodes`, `-cache-extents`, `-cache-negative` and `-broadcast-max-peers` set the cache and broadcast table sizes, and `/metrics` reports each limit next to its size.

### Changed

- Freeing blocks that no snapshot or clone shares takes only the block bitmap lock, once per operation, not the refcount lock and one bitmap lock per block. Snapshots and clones take the bitmap lock while they count a file's blocks.
- Each node keeps the directories it reads in an in-memory index, so a lookup no longer reads and decodes the whole directory. A lookup checks the directory inode's generation on disk before it uses the index, so other nodes' changes show on the next lookup without broadcast. Removing an entry now writes the directory inode before the child is freed. `-dir-index-entries` caps the names held (default 1,048,576, about 100 MB), and `/metrics` counts hits, misses and rebuilds. In one test with two nodes, `ls -l` of 4,000 files went from 65 s to 1 s, a `stat` from 3.8 ms to 0.25 ms, and a create from 14 ms to 6 ms.
- `-format` now writes a superblock with the volume layout and zeroes all metadata regions, even on a device that already holds a filesystem. Mount reads every region offset from the superblock.
- At most 64 nodes mount a volume at once, one per node registry slot.
- Moved the FUSE library from `bazil.org/fuse` to `hanwen/go-fuse` v2.
- `go.mod` now requires Go 1.26.
- `make build` builds `goclusterfs-fsck` as well as `goclusterfs-mount`.
- `make test-integration` now allows 30 minutes, since the full suite takes about 15.
- Mount no longer creates an SFB manager, which nothing used, and logs that SFB is disabled instead of `SFB: enabled`.
- Reads use the inode cache, a node keeps a released lock for 10 ms so back-to-back operations take it again without disk I/O and yields a lock it has reused for 250 ms, and the inode and extent caches are split into 16 shards.

### Removed

- `cmd/crctest`, a development tool that printed two CRC32C values. `internal/storage`'s `TestCRC32C` already checks both, and releases never shipped it.
- The legacy single-block prototype in the repository root (`main.go`).
- The journal's `FullThreshold` setting, which nothing read. A checkpoint still runs every 5 seconds and when a transaction does not fit.

### Fixed

- A node in a higher registry slot could wait 30 seconds and fail with `EIO` for a lock that a node in a lower slot kept releasing and claiming again every few milliseconds. A node that loses a claim to a lower slot now leaves a waiting mark in its own slot, and the holder gives the lock up 250 ms after it first sees a live node's mark.
- `chmod`, `chown` and `touch` could undo another node's concurrent change to the same inode: on a directory, a create's link count or mtime; on a file, the size an append inside an allocated block set. Directory attribute changes now hold the inode lock from read to write, and file attribute changes retry when the inode changed since they read it.
- After 3 failed multicast sends in total, counted over the whole mount, a node sent cache invalidations only to unicast peers until it unmounted. Now only failures in a row count: a successful send resets them, and after 3 in a row the node tries multicast again 30 seconds later.
- `chmod` cleared a file's type bits, so the file could no longer be opened ("Is a directory") and fsck reported an invalid file type. `chmod`, `chown` and `touch` now also work on directories and the mount root, which returned "Operation not supported".
- The cache summary logged at unmount showed hit rates 100 times too high.
- All nodes wrote the same journal region. A restarting node replayed and then reset the journal of nodes still writing, and no node could replay a crashed node's journal. Each node registry slot now owns a 4 MB journal, and a crashed node's journal is replayed through its slot.
- Nodes never sent registry heartbeats, so a node mounting more than 10 seconds after another took the live node's registry slot.
- After a partial write into a fallocated range, reads of the other fallocated blocks, and of the rest of the written block, returned old disk contents. They now return zeros.
- Snapshots and clones never copied on write: block refcounts stopped at 1, so a snapshot kept nothing and a write to a clone changed its source. Each snapshot now keeps the blocks it captured in a hidden inode, a write to a shared block copies it first, and a block is freed only when its last owner lets go.
- Deleting a snapshot after its file changed failed with a refcount underflow and left the snapshot deleting forever, because the delete used the file's current blocks.
- A snapshot or clone could undo another node's change to the source file made while the file was being marked shared, such as the new size after an append, a chmod, or new times. The mark now holds the inode lock from its read to its write.
- Two delete jobs for one snapshot, from a double delete or a node mounting while another node deleted it, could empty the data of a new snapshot that took the freed inode slot. A delete job now empties a data inode only while the snapshot still names it, and a job that finds the snapshot already deleted succeeds.
- Writes failed with `EAGAIN` once a file needed a third extent, so most files over 2 MB could not be written. Extents past the first two now go in an on-disk extent tree, and adjacent extents merge.
- `goclusterfs-fsck` phase 4 never checked file extents, because it read inodes into unaligned buffers that `O_DIRECT` rejects.
- A node could serve a stale cached inode after another node freed and reused its inode number. Inode generations now survive free and reuse, and opening a file reloads its inode from disk.
- Journal recovery no longer panics, loops forever, or allocates gigabytes on a corrupt journal. It now rejects short entries and bad ring geometry as corrupt.
- Lock-order violations when several nodes create files and directories at the same time.
- Two nodes could each hold a lock and wait for the other's, then fail with `EIO` after 30 seconds, when a write's inode and block locks mapped to records the other node held. Each lock type now has its own lock records.
- Inode allocation races between nodes; allocation now retries with jittered backoff.
- Directory operations now update caches and send broadcast invalidations.
- The broadcast peer table never dropped an entry: every remount and every forged heartbeat added one. Peers are now kept by address, at most `-broadcast-max-peers` of them, and so are the nodes whose message numbers a node tracks.
- A cache that kept evicting entries grew in memory past its limit, because a Go map keeps the room of deleted keys: the extent cache took 5.8 MB when full and 10.9 MB after 40 times its size went through it. Each cache shard now rebuilds its map after it has evicted as many entries as it holds.
- A read right after `open` could stop early when this node's kernel had cached the file's size during another node's write, for example from a desktop volume monitor or `ls -l` scanning the mount. Opening a file now makes the kernel fetch its size again.
- A node that lost broadcast invalidations, for example when its receive buffer filled during a burst of writes on another node, kept stale inodes until `-cache-ttl` and logged nothing. It now sees the gap in the sender's sequence numbers, drops its inode and extent caches, and logs it at most every 10 seconds. The unmount stats line reports `lost=N`.
- Broadcast deduplication kept every message for 5 minutes: about 224 MB per 2.4 million messages, and cleaning it up held up message handling for up to 200 ms. It now keeps a 64-message window per sender.
- Docker and VM test suites now run on fresh hosts.
- The README quick start now uses the real binary names and flags.
- Create, mkdir, clone and snapshot failed with "No space left on device" when the lock wait for the new inode gave up or its slot could not be read. They now fail with `EIO`. `ENOSPC` means the inode table is full.
- Writes and fallocate failed with "No space left on device" when the lock wait for the block bitmap gave up or a bitmap sector could not be read. They now fail with `EIO`. `ENOSPC` means no free blocks are left or the backing storage is full.
- Directory entries went only into the first 4 KB of each 1 MB directory block, so a block held about 250 short names or 15 long ones, and a busy directory took a new block every few creates. Entries now fill the whole block. A lookup reads each block in one I/O, and an add or remove writes only the 4 KB it changed.
- A node that other nodes declared dead while it was stalled, for example a paused VM, kept writing when it resumed. A node now checks before every device write that it still owns its registry slot, and refuses the write and exits if it does not. A write it started before the stall can still land.
- A node stalled for 15 seconds while it held a lock, for example a paused VM, lost the lock to other nodes, which could then write the same metadata. Other nodes now wait for a failed node's locks until it is proven dead and its journal is replayed, with no time limit, and give up only if the replay fails or is abandoned, or after 30 seconds if the waiting operation already holds another lock the replay may need. An overwrite of existing file data instead lets go of its locks and retries, so it does not fail. Failure detection now replays only nodes proven dead.
- A mount that took the registry slot of a failed node on another host did not keep that node's locks while it replayed the node's journal.
- Two nodes replaying dead nodes whose locks shared a record waited on each other until the lock timeout.
- A failed journal replay of a dead node still freed that node's locks, so other nodes could use half-replayed metadata until the retry a minute later. The locks now stay held until a replay succeeds.
- A mount whose own journal replay gave up waiting for another node's lock exited at once. It now retries with backoff and logs the holder; any other replay failure still fails the mount.
- Replaying a failed node's journal could overwrite an inode change that the replaying node made after the replay read the inode, such as a `chmod` or an append. The replay now writes the inode only if nothing wrote it since the read, and otherwise skips that record.
