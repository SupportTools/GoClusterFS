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
- **Small File Optimization**: SFB containers pack files under 64KB efficiently
- **8-Phase fsck**: Comprehensive filesystem consistency checker with repair mode

## Requirements

- Go 1.26 or later
- Linux with FUSE support (`fusermount3`)
- O_DIRECT-capable storage (tmpfs supports O_DIRECT only on Linux 6.6 and later)
- Root or `fuse` group membership
- For multi-node: Shared block device (SAN, iSCSI, NVMe-oF, etc.)

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
| `-sfb` | `true` | Enable Small File Block containers |
| `-snapshots` | `true` | Enable O(1) file snapshots |

## Multi-Node Setup

For clustered deployments:

1. **Shared Storage**: All nodes must access the same block device (SAN, iSCSI, NVMe-oF)
2. **Network**: Nodes should be on the same subnet for multicast invalidation
3. **Mount Command**: Use identical mount options across all nodes

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
# Check only (read-only)
./goclusterfs-fsck ./disk.img

# Check and repair
./goclusterfs-fsck -repair ./disk.img

# Keep going past fatal errors (data recovery)
./goclusterfs-fsck -force ./disk.img
```

Exit codes: `0` clean, `1` errors found, `2` usage error or fatal failure.

**Known issue:** fsck does not work yet on volumes made with `goclusterfs-mount -format`. The mount tool does not write a superblock, so fsck stops at phase 1 with `invalid superblock magic` and exits with `2`.

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

VM tests need an x86_64 Linux host with `/dev/kvm`, `qemu-system-x86_64`, and `sshpass`. QEMU 10+ uses io_uring, which counts against the locked-memory limit. With the common 8 MB default, QEMU fails with `Failed to initialize io_uring: Cannot allocate memory`. Raise the limit for the test user, then log in again:

```bash
echo "$USER - memlock unlimited" | sudo tee /etc/security/limits.d/90-qemu-memlock.conf
```

## License

Apache License 2.0. See [LICENSE](LICENSE).

## Contributing

[Contributing guidelines to be added]
