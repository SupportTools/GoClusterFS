# GoClusterFS

A VMFS-like clustered filesystem written in Go. GoClusterFS enables multiple nodes to safely access a shared block device simultaneously with strong consistency guarantees.

## Features

- **Clustered Access**: Multiple nodes can mount the same block device concurrently
- **O_DIRECT I/O**: Bypasses host page cache for true cluster coherency
- **Generation-Based Cache Coherency**: Efficient validation without cross-node communication
- **Write-Ahead Journal**: Crash-consistent metadata updates with configurable commit modes
- **Distributed Locking**: CAS-based on-disk locks for safe concurrent access
- **Network Cache Invalidation**: UDP multicast for near-instant cross-node cache updates
- **Copy-on-Write Snapshots**: O(1) file snapshots with refcount-based block sharing
- **Thin Provisioning**: Allocate-on-write with TRIM/BLKDISCARD support
- **8-Phase fsck**: Comprehensive filesystem consistency checker with repair mode

## Requirements

- Go 1.26 or later
- Linux with FUSE support (`fusermount3`)
- O_DIRECT-capable storage (tmpfs supports O_DIRECT only on Linux 6.6 and later)
- Root or `fuse` group membership
- For multi-node: Shared block device (SAN, iSCSI, NVMe-oF, etc.)
- A device of at least 264 MB: the layout reserves 256 MB for 64 node journals of 4 MB

## Limits and Status

- **Linux only.** GoClusterFS needs FUSE and O_DIRECT.
- **Every file with data uses at least one full 1 MB block.** The block size is 1 MB, and small-file packing (SFB) is disabled since [#12](https://github.com/SupportTools/GoClusterFS/pull/12), so `-sfb` has no effect.
- **Name lookups are slow.** Each lookup reads the whole directory from disk and scans it entry by entry. In one test on a single node, a lookup took about 25 ms, and `ls -l` on a 1,000-file directory took about 47 seconds.
- **Other nodes' changes can show up late.** The kernel caches file attributes and directory entries for `-attr-timeout` (default 1s). For that long, `stat` and `ls` can show another node's file with its old size and mtime, and a read can stop at the old size. A file that stays open can use the old size and block mappings for up to `-cache-ttl` (default 5s). Opening the file after `-attr-timeout` has passed reads the current data. See [Cache Flags](docs/API-CONFIG.md#cache-flags).
- **On-disk format version 3.** Mount refuses images formatted by earlier builds. Copy the data off with the old build, reformat, and copy it back.
- **The inode count is set at format time.** `-format` creates one inode per `-bytes-per-inode` bytes of device (default 1 MB, so one per data block), between 4096 and 16,777,216. It cannot change later.
- **Upgrade all nodes at once.** Unmount on every node, upgrade, then mount again. Mixing old and new binaries on one device can serve stale data or make files unreadable. See the upgrade notes in [CHANGELOG.md](CHANGELOG.md).
- **Snapshots and clones are experimental.** Before you mount an image that has snapshots or clones with a newer build, read the upgrade notes in [CHANGELOG.md](CHANGELOG.md). They say whether to run `goclusterfs-fsck` first.
- **fsck repairs little.** `-repair` rebuilds refcounts and fixes the superblock checksum. It only reports other problems. See [Filesystem Check](#filesystem-check).
- **Cluster safety limits.** Node clocks, node stalls, and storage timeouts have hard limits. Read [Node Requirements](docs/OPERATIONS.md#node-requirements) and [Distributed Lock Limits](docs/OPERATIONS.md#distributed-lock-limits) before you run more than one node.

## Quick Start

### Build

```bash
git clone https://github.com/supporttools/GoClusterFS.git
cd GoClusterFS
make build
```

This builds `goclusterfs-mount` and `goclusterfs-fsck` in the repo root.

### Format and Mount

Use a file as the block device. Keep it on a filesystem that supports O_DIRECT.

```bash
truncate -s 1G disk.img
mkdir -p mnt
./goclusterfs-mount -device ./disk.img -mount ./mnt -format > mount.log 2>&1 &
sleep 2
```

`-format` wipes the device and then mounts it. The mount process keeps running until you unmount, and logs to `mount.log`.

### Use It

```bash
echo "Hello, Cluster!" > mnt/hello.txt
cat mnt/hello.txt
df -h mnt
```

### Unmount

```bash
fusermount3 -u mnt
```

The mount process exits after the unmount. To mount the same image again, run the mount command without `-format`.

## Command-Line Options

These are the `goclusterfs-mount` flags. Run `./goclusterfs-mount -h` for the full list.

| Flag | Default | Description |
|------|---------|-------------|
| `-device` | (required) | Path to block device or image file |
| `-mount` | (required) | Mount point directory (must exist) |
| `-format` | `false` | Format the device, then mount it (destroys data) |
| `-bytes-per-inode` | `1048576` | With `-format`: one inode per this many bytes of device (4096 to 16,777,216 inodes) |
| `-debug` | `false` | Log every FUSE request |

### Cache Options

| Flag | Default | Description |
|------|---------|-------------|
| `-cache-strict` | `false` | Always validate cache against disk |
| `-cache-ttl` | `5s` | Time before cache entries need revalidation |

### Journal Options

| Flag | Default | Description |
|------|---------|-------------|
| `-journal` | `true` | Enable write-ahead journal |
| `-skip-recovery` | `false` | Skip journal recovery on mount (dangerous) |

### Network Broadcast Options

| Flag | Default | Description |
|------|---------|-------------|
| `-broadcast` | `true` | Enable network cache invalidation |
| `-mcast-group` | `239.192.74.75` | Multicast group address |
| `-mcast-port` | `31337` | Multicast/unicast port |
| `-broadcast-secret` | (empty) | HMAC secret for message authentication |
| `-broadcast-iface` | (auto) | Network interface for multicast |

### TRIM Options

| Flag | Default | Description |
|------|---------|-------------|
| `-trim` | `true` | Enable batched TRIM/discard |
| `-trim-batch` | `256` | Number of blocks per TRIM batch |
| `-trim-age` | `5s` | Maximum age before forcing TRIM flush |

### Feature Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-sfb` | `true` | Ignored. SFB containers are disabled and every file uses full blocks |
| `-snapshots` | `true` | Enable O(1) file snapshots |

## Multi-Node Setup

For clustered deployments:

1. **Shared Storage**: All nodes must access the same block device (SAN, iSCSI, NVMe-oF)
2. **Network**: Nodes should be on the same subnet for multicast invalidation
3. **Mount Command**: Use identical mount options across all nodes
4. **Limits**: At most 64 nodes mount a device at once, and node clocks must agree within a few seconds (run NTP)
5. **Format version**: This release uses on-disk format version 3, which gives each node its own journal and sizes the block bitmap, refcount table and inode table from the device. It refuses devices formatted by earlier builds; copy their data off and reformat

Format the device once, from one node, with `-format`. Then mount it on every node without `-format`:

```bash
# Node 1
./goclusterfs-mount -device /dev/shared/lun0 -mount /mnt/cluster \
    -journal -broadcast -mcast-group 239.192.74.75

# Node 2 (same block device, same options)
./goclusterfs-mount -device /dev/shared/lun0 -mount /mnt/cluster \
    -journal -broadcast -mcast-group 239.192.74.75
```

## Documentation

- [Deployment Guide](docs/DEPLOYMENT.md) - Installation and configuration
- [Operations Guide](docs/OPERATIONS.md) - Day-to-day operations
- [Troubleshooting](docs/TROUBLESHOOTING.md) - Problem diagnosis
- [Configuration Reference](docs/API-CONFIG.md) - All configuration options
- [Design Overview](docs/DESIGN.md) - Architecture and internals

## Filesystem Check

Run fsck on an unmounted device. The device is a positional argument, and flags go before it:

```bash
# Check only (writes nothing)
./goclusterfs-fsck ./disk.img

# Show what -repair would change
./goclusterfs-fsck -repair -dry-run ./disk.img

# Repair
./goclusterfs-fsck -repair ./disk.img
```

Exit codes follow fsck(8) and add up: `0` no errors, `1` errors repaired, `4` errors remain, `8` I/O failure or a check that could not finish or was refused, `16` usage error.

`-repair` refuses to run while a node has the device mounted, or while a stopped node's journal still needs replay. Back up the device before you repair it. The [fsck Guide](docs/TROUBLESHOOTING.md#fsck-guide) lists what fsck checks and what `-repair` changes.

## Architecture

GoClusterFS uses a layered architecture:

```
┌─────────────────────────────────────────────────────┐
│                    FUSE Layer                       │
│              (hanwen/go-fuse v2)                    │
├─────────────────────────────────────────────────────┤
│                 VFS Operations                      │
│     (Dir, File, Attr, Read, Write, Lookup)          │
├─────────────────────────────────────────────────────┤
│   Cache Manager    │    Lock Manager    │  Journal  │
│  (generation-based)│   (CAS protocol)   │  (WAL)    │
├─────────────────────────────────────────────────────┤
│                  Inode Table                        │
│          (extent-based allocation)                  │
├─────────────────────────────────────────────────────┤
│              Block Allocator (Bitmap)               │
│          (thin provisioning + TRIM)                 │
├─────────────────────────────────────────────────────┤
│                Storage Device                       │
│         (O_DIRECT, sector-aligned I/O)              │
└─────────────────────────────────────────────────────┘
```

## Testing

```bash
# Unit tests
make test-unit

# Integration tests (requires FUSE)
make test-integration

# Multi-node Docker tests
make test-multinode-docker

# Multi-node VM tests (power failure simulation)
make test-multinode-vm

# Validate test coverage across environments
make test-parity
```

VM tests need an x86_64 Linux host with `/dev/kvm`, `qemu-system-x86_64`, OpenBSD `nc` (Debian's `netcat-openbsd`) and `setpriv` from util-linux. VM N listens for SSH on `127.0.0.1:2222+N-1`; set `VM_SSH_BASE_PORT` to run two suites on one host. Each VM's console, QEMU and mount logs are written to `tests/vm/results/`, including logs from before a VM was killed. QEMU 10+ uses io_uring, which counts against the locked-memory limit. With the common 8 MB default, QEMU fails with `Failed to initialize io_uring: Cannot allocate memory`. Raise the limit for the test user, then log in again:

```bash
echo "$USER - memlock unlimited" | sudo tee /etc/security/limits.d/90-qemu-memlock.conf
```

## License

Apache License 2.0. See [LICENSE](LICENSE).

## Contributing

[Contributing guidelines to be added]
