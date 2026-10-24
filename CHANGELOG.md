# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Upgrade notes

- Upgrade all nodes together: unmount on every node, upgrade, then mount again. Don't mount one shared device with a mix of old and new binaries.
  - Older binaries reset an inode's generation counters to 0 when they free it, so other nodes can serve stale data after the inode number is reused.
  - Older binaries zero the extent-tree generation (inode bytes 188–195) when they rewrite an inode. Newer binaries then cannot read that file if it has more than two extents, because such files use an extent tree.

### Added

- `goclusterfs-mount`: a FUSE filesystem that several Linux nodes can mount from one shared block device.
- `O_DIRECT` device I/O, split into shards so requests can run in parallel.
- Distributed compare-and-swap locks on the shared device for block allocation, inode updates, and directory changes. File writes do not take these locks yet, so two nodes extending the same file can lose each other's data.
- Write-ahead journal with batch commits and a recovery check on mount.
- Intent logging for multi-step operations.
- Inode cache with generation-based coherency checks, and an extent cache.
- UDP multicast cache invalidation between nodes, with optional HMAC authentication (`-broadcast-secret`).
- Batched TRIM of freed blocks (`-trim`).
- Experimental: SFB containers for files under 64 KB (`-sfb`). Container I/O uses unaligned buffers that `O_DIRECT` rejects, so files fall back to normal extents.
- Experimental: file snapshots and writable clones through `user.snapshot*` and `user.clone` extended attributes. Copy-on-write does not trigger yet, so a snapshot does not preserve data, and an in-place write to a clone changes the source file.
- Experimental: `goclusterfs-fsck` consistency checker with a repair mode. It fails on newly formatted volumes because `-format` does not write a superblock.
- Benchmark suite (`tests/benchmark`) and an `internal/metrics` package.
- Go, Docker, and QEMU VM test suites, with `tests/test-manifest.yaml` and `make test-parity` to track coverage across them.
- Fuzz tests for the on-disk decoders (superblock, inode, snapshot descriptor, journal superblock and records) and the journal recovery scan.
- CI gates: unit tests with the race detector and coverage, golangci-lint (`make lint`), govulncheck (`make vuln`), and a `go mod tidy` check.
- Design, deployment, operations, and troubleshooting docs in `docs/`.
- Apache License 2.0.

### Changed

- Moved the FUSE library from `bazil.org/fuse` to `hanwen/go-fuse` v2.
- `go.mod` now requires Go 1.26.
- `make build` builds `goclusterfs-fsck` as well as `goclusterfs-mount`.
- Reads use the inode cache, lock acquisition reuses cached lock sequence numbers, and the inode and extent caches are split into 16 shards.

### Removed

- The legacy single-block prototype in the repository root (`main.go`).

### Fixed

- A node could serve a stale cached inode after another node freed and reused its inode number. Inode generations now survive free and reuse, and opening a file reloads its inode from disk.
- Journal recovery no longer panics, loops forever, or allocates gigabytes on a corrupt journal. It now rejects short entries and bad ring geometry as corrupt.
- Lock-order violations when several nodes create files and directories at the same time.
- Inode allocation races between nodes; allocation now retries with jittered backoff.
- Directory operations now update caches and send broadcast invalidations.
- Docker and VM test suites now run on fresh hosts.
- The README quick start now uses the real binary names and flags.
