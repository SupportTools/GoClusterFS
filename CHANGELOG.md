# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
- Design, deployment, operations, and troubleshooting docs in `docs/`.
- Apache License 2.0.

### Changed

- Moved the FUSE library from `bazil.org/fuse` to `hanwen/go-fuse` v2.
- `go.mod` now requires Go 1.26.
- Reads use the inode cache, lock acquisition reuses cached lock sequence numbers, and the inode and extent caches are split into 16 shards.

### Fixed

- Lock-order violations when several nodes create files and directories at the same time.
- Inode allocation races between nodes; allocation now retries with jittered backoff.
- Directory operations now update caches and send broadcast invalidations.
- Docker and VM test suites now run on fresh hosts.
