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

- Go 1.21 or later
- Linux with FUSE support (`fusermount3`)
- O_DIRECT-capable storage (not tmpfs)
- Root or `fuse` group membership
- For multi-node: Shared block device (SAN, iSCSI, NVMe-oF, etc.)

## Quick Start

### Build

```bash
git clone https://github.com/supporttools/GoClusterFS.git
cd GoClusterFS
go build -o goclusterfs ./cmd/mount
```

### Create and Format a Block Device

```bash
# Create a file-backed block device (3GB example)
dd if=/dev/zero of=VirtBlock.img bs=1M count=3000

# Format the filesystem
./goclusterfs -format -device ./VirtBlock.img
```

### Mount

```bash
# Create mount point
mkdir -p /mnt/goclusterfs

# Mount with default settings
./goclusterfs -device ./VirtBlock.img -mount /mnt/goclusterfs

# Mount with journaling and network broadcast (multi-node)
./goclusterfs -device ./VirtBlock.img -mount /mnt/goclusterfs \
    -journal -broadcast -mcast-group 239.192.74.75

# Unmount
fusermount -u /mnt/goclusterfs
```

### Verify

```bash
# Create a file
echo "Hello, Cluster!" > /mnt/goclusterfs/test.txt

# Check filesystem stats
df -h /mnt/goclusterfs
```

## Command-Line Options

| Flag | Default | Description |
|------|---------|-------------|
| `-device` | (required) | Path to block device |
| `-mount` | (required) | Mount point directory |
| `-format` | `false` | Format the device (destroys data) |
| `-debug` | `false` | Enable debug logging |

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

```bash
# Node 1
./goclusterfs -device /dev/shared/lun0 -mount /mnt/cluster \
    -journal -broadcast -mcast-group 239.192.74.75

# Node 2 (same block device, same options)
./goclusterfs -device /dev/shared/lun0 -mount /mnt/cluster \
    -journal -broadcast -mcast-group 239.192.74.75
```

## Documentation

- [Deployment Guide](docs/DEPLOYMENT.md) - Installation and configuration
- [Operations Guide](docs/OPERATIONS.md) - Day-to-day operations
- [Troubleshooting](docs/TROUBLESHOOTING.md) - Problem diagnosis
- [Configuration Reference](docs/API-CONFIG.md) - All configuration options
- [Design Overview](docs/DESIGN.md) - Architecture and internals

## Filesystem Check

Run fsck to verify or repair the filesystem:

```bash
# Check only (read-only)
./goclusterfs-fsck -device ./VirtBlock.img

# Check and repair
./goclusterfs-fsck -device ./VirtBlock.img -repair

# Force check (ignore clean flag)
./goclusterfs-fsck -device ./VirtBlock.img -force
```

## Architecture

GoClusterFS uses a layered architecture:

```
┌─────────────────────────────────────────────────────┐
│                    FUSE Layer                       │
│              (bazil.org/fuse)                       │
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

[License details to be added]

## Contributing

[Contributing guidelines to be added]
