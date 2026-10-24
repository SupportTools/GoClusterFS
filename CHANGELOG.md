# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `goclusterfs-mount`: a FUSE filesystem that several Linux nodes can mount from one shared block device.
- `O_DIRECT` device I/O, split into shards so requests can run in parallel.
- Distributed locks stored on the shared device, using compare-and-swap.
- Write-ahead journal with batch commits and a recovery check on mount.
- Intent logging for multi-step operations.
- Inode cache with generation-based coherency checks, and an extent cache.
- UDP multicast cache invalidation between nodes, with optional HMAC authentication (`-broadcast-secret`).
- Batched TRIM of freed blocks (`-trim`).
- SFB containers that pack files under 64 KB (`-sfb`).
- Copy-on-write file snapshots and writable clones, managed through `user.snapshot*` and `user.clone` extended attributes.
- `fsck` consistency checker with a repair mode (`cmd/fsck`).
- Benchmark suite (`tests/benchmark`) and an `internal/metrics` package.
- Go, Docker, and QEMU VM test suites, with `tests/test-manifest.yaml` and `make test-parity` to track coverage across them.
- Design, deployment, operations, and troubleshooting docs in `docs/`.
- Apache License 2.0.

### Changed

- Moved the FUSE library from `bazil.org/fuse` to `hanwen/go-fuse` v2.
- `go.mod` now requires Go 1.26.
- Reads use the inode cache, lock acquisition reuses cached lock sequence numbers, and the inode and extent caches are split into 16 shards.

### Fixed

- Lost updates and lock-order violations when several nodes create files and directories, or write to the same file, at the same time.
- Inode allocation races between nodes; allocation now retries with jittered backoff.
- Directory operations now update caches and send broadcast invalidations.
- Docker and VM test suites now run on fresh hosts.
