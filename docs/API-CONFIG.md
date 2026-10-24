# GoClusterFS Configuration Reference

This document provides a comprehensive reference for all configuration options in GoClusterFS.

## Table of Contents

1. [Command-Line Flags](#command-line-flags)
2. [Cache Configuration](#cache-configuration)
3. [Journal Configuration](#journal-configuration)
4. [TRIM Configuration](#trim-configuration)
5. [Broadcast Configuration](#broadcast-configuration)
6. [Configuration Presets](#configuration-presets)
7. [Tuning Recommendations](#tuning-recommendations)

---

## Command-Line Flags

These flags are passed when starting `goclusterfs`:

```bash
./goclusterfs [flags]
```

### Required Flags

| Flag | Type | Description |
|------|------|-------------|
| `-device` | string | Path to block device or image file |
| `-mount` | string | Mount point directory |

### General Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-format` | bool | `false` | Format the device (destroys all data) |
| `-bytes-per-inode` | uint | `1048576` | With `-format`: one inode per this many bytes of device, 4096 to 16,777,216 inodes |
| `-debug` | bool | `false` | Enable verbose debug logging |

### Cache Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-cache-strict` | bool | `false` | Enable strict cache mode (always validate against disk) |
| `-cache-ttl` | duration | `5s` | Cache entry TTL before revalidation required |
| `-attr-timeout` | duration | `1s` | How long the kernel caches file attributes and directory entries |

The kernel caches file attributes and directory entries for `-attr-timeout`. For that long after another node changes a file, `stat` and path lookups on this node can return the old size, and a read can stop at the old size even right after `open`. After that, opening a file reads its inode from disk, so the reader sees the other node's completed writes. Reads through a file that is already open use the inode cache and can use the old size and block mappings for up to `-cache-ttl`, including a block that another node has since freed and reused for other data. With broadcast enabled, writes and directory changes invalidate peers' caches sooner; truncate, fallocate and unlink do not.

`-attr-timeout=0` makes `stat` and path lookups always current, but each `stat` and path step then reads the directory from disk under a cluster lock. In one test on a single mount, 2000 `stat` calls on one file took 13.9s at 0 and 0.01s at 1s, and `ls -l` of 300 files took 11.8s and 3.7s.

### Journal Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-journal` | bool | `true` | Enable write-ahead journal for crash consistency |
| `-skip-recovery` | bool | `false` | Skip journal recovery on mount (dangerous, may lose data) |

### Network Broadcast Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-broadcast` | bool | `true` | Enable network cache invalidation |
| `-mcast-group` | string | `239.192.74.75` | Multicast group IP address |
| `-mcast-port` | int | `31337` | UDP port for multicast/unicast |
| `-broadcast-secret` | string | (empty) | HMAC secret for message authentication |
| `-broadcast-iface` | string | (auto) | Network interface for multicast |

### TRIM Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-trim` | bool | `true` | Enable batched TRIM/BLKDISCARD |
| `-trim-batch` | int | `256` | Number of blocks per TRIM batch |
| `-trim-age` | duration | `5s` | Maximum age before forcing TRIM flush |

### Feature Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-sfb` | bool | `true` | Ignored. SFB containers are disabled |
| `-snapshots` | bool | `true` | Enable O(1) file snapshots |

---

## Cache Configuration

The cache subsystem is configured via the `cache.Config` struct in `internal/cache/config.go`.

### Configuration Fields

```go
type Config struct {
    // Inode cache
    InodeCacheSize    int           // Maximum cached inodes
    InodeTTL          time.Duration // Time before inode needs revalidation

    // Extent cache
    ExtentCacheSize   int           // Maximum cached extent mappings

    // Negative cache (tracks "not found" lookups)
    NegativeCacheSize int           // Maximum negative entries
    NegativeTTL       time.Duration // Time before negative entry expires

    // Coherency mode
    StrictMode        bool          // Always validate against disk

    // Network broadcast settings
    Network           broadcast.Config
}
```

### Field Details

#### InodeCacheSize

| Property | Value |
|----------|-------|
| Default | `10000` |
| Memory | ~3.5 MB (for defaults) |
| Recommended | 10,000 - 100,000 |

The maximum number of inodes to cache. Each cached inode stores metadata, generations, and timestamps.

**Sizing guidance**:
- Count unique files accessed in typical workload
- Add 20% headroom
- Example: 50,000 files accessed → set to 60,000

#### InodeTTL

| Property | Value |
|----------|-------|
| Default | `5 * time.Second` |
| Range | 100ms - 5 minutes |
| Recommended | 1s - 30s |

Time after which a cached inode must be revalidated against disk.

**Trade-offs**:
- Shorter TTL: Higher consistency, more disk reads
- Longer TTL: Better performance, eventual consistency

#### ExtentCacheSize

| Property | Value |
|----------|-------|
| Default | `50000` |
| Memory | ~3.2 MB (for defaults) |
| Recommended | 50,000 - 500,000 |

The maximum number of extent mappings to cache. Extent cache maps (inode, logical block) → physical block. A cached mapping is only used with the inode version it was looked up with, so any inode write by any node makes it stale.

**Sizing guidance**:
- Large files benefit more from extent caching
- Formula: `num_large_files * avg_extents_per_file`

#### NegativeCacheSize

| Property | Value |
|----------|-------|
| Default | `1000` |
| Memory | ~40 KB |
| Recommended | 1,000 - 10,000 |

Cache for "file not found" lookups. Prevents repeated disk reads for non-existent files.

#### NegativeTTL

| Property | Value |
|----------|-------|
| Default | `1 * time.Second` |
| Range | 100ms - 10s |
| Recommended | 500ms - 2s |

How long to cache "not found" results. Keep short to detect newly created files.

#### StrictMode

| Property | Value |
|----------|-------|
| Default | `false` |

When `true`, always validates cache entries against disk before use.

**Use cases**:
- Database files
- Shared configuration files
- Any file modified by multiple nodes simultaneously

**Impact**: Significantly more disk I/O, but guarantees consistency.

---

## Journal Configuration

The journal subsystem is configured via structures in `internal/journal/journal.go`.

### Commit Modes

```go
type CommitMode int

const (
    CommitSync  CommitMode = iota  // fsync every commit (default)
    CommitAsync                     // No sync (testing only)
    CommitBatch                     // Batch syncs for performance
)
```

#### CommitSync (Default)

| Property | Value |
|----------|-------|
| Durability | Highest |
| Performance | Lower |
| Use case | Production default |

Every transaction is followed by `fsync()`. Guarantees data survives power failure.

#### CommitAsync

| Property | Value |
|----------|-------|
| Durability | None |
| Performance | Highest |
| Use case | Testing only |

**WARNING**: No durability guarantees. Data may be lost on crash.

#### CommitBatch

| Property | Value |
|----------|-------|
| Durability | High (batched) |
| Performance | Higher than sync |
| Use case | Write-heavy workloads |

Groups multiple transactions before `fsync()`. Trades some latency for throughput.

### Batch Configuration

```go
type BatchConfig struct {
    MaxBatchTransactions int           // Max transactions per batch
    MaxBatchAge          time.Duration // Max time before forced flush
}
```

#### MaxBatchTransactions

| Property | Value |
|----------|-------|
| Default | `32` |
| Range | 1 - 256 |
| Recommended | 16 - 64 |

Maximum transactions to accumulate before forcing a sync.

#### MaxBatchAge

| Property | Value |
|----------|-------|
| Default | `100 * time.Millisecond` |
| Range | 10ms - 1s |
| Recommended | 50ms - 200ms |

Maximum time to wait before forcing a sync, even if batch isn't full.

### Journal Sizing

| Parameter | Default | Description |
|-----------|---------|-------------|
| SlotSize | 4 MB | Size of each journal slot |
| CheckpointInterval | 5 seconds | Time between checkpoints |
| FullThreshold | 75% | Trigger forced checkpoint |

---

## TRIM Configuration

TRIM/BLKDISCARD configuration is in `internal/trim/trim.go`.

```go
type Config struct {
    BatchSize  int           // Blocks per TRIM batch
    MaxAge     time.Duration // Max age before forced flush
    DataOffset uint64        // Offset to data region
    Enabled    bool          // Whether TRIM is enabled
}
```

### Field Details

#### BatchSize

| Property | Value |
|----------|-------|
| Default | `256` |
| Range | 16 - 4096 |
| Recommended | 128 - 512 |

Number of blocks to accumulate before issuing TRIM command.

**Trade-offs**:
- Smaller batches: Lower latency, more syscalls
- Larger batches: Higher latency, fewer syscalls

#### MaxAge

| Property | Value |
|----------|-------|
| Default | `5 * time.Second` |
| Range | 1s - 60s |
| Recommended | 5s - 30s |

Maximum time to hold TRIM requests before flushing, even if batch isn't full.

#### Enabled

| Property | Value |
|----------|-------|
| Default | `true` |

Set to `false` for devices that don't benefit from TRIM (HDDs, some SANs).

---

## Broadcast Configuration

Network broadcast configuration is in `internal/cache/broadcast/transport.go`.

```go
type Config struct {
    Enabled              bool
    MulticastGroup       string
    MulticastPort        int
    MulticastTTL         int
    MaxMulticastFailures int
    HeartbeatInterval    time.Duration
    Secret               string
    Interface            string
}
```

### Field Details

#### Enabled

| Property | Value |
|----------|-------|
| Default | `true` (when network enabled) |

Master switch for network broadcast functionality.

#### MulticastGroup

| Property | Value |
|----------|-------|
| Default | `239.192.74.75` |
| Range | 239.0.0.0/8 (local admin scope) |

IPv4 multicast group address. All cluster nodes must use the same group.

**Note**: `239.192.0.0/14` is reserved for local administrative use.

#### MulticastPort

| Property | Value |
|----------|-------|
| Default | `31337` |
| Range | 1024 - 65535 |

UDP port for multicast and unicast fallback.

#### MulticastTTL

| Property | Value |
|----------|-------|
| Default | `1` |
| Range | 1 - 255 |

IP TTL for multicast packets. `1` = same subnet only.

**Values**:
- 1: Same subnet (recommended)
- 32: Same site
- 64: Same region
- 128: Same continent

#### MaxMulticastFailures

| Property | Value |
|----------|-------|
| Default | `3` |
| Range | 1 - 10 |

Consecutive multicast failures before falling back to unicast.

#### HeartbeatInterval

| Property | Value |
|----------|-------|
| Default | `30 * time.Second` |
| Range | 5s - 120s |
| Recommended | 15s - 60s |

Interval between heartbeat messages to detect node failures.

#### Secret

| Property | Value |
|----------|-------|
| Default | (empty) |

HMAC secret for message authentication. All nodes must use the same secret.

**Generating a secret**:
```bash
openssl rand -hex 32
```

#### Interface

| Property | Value |
|----------|-------|
| Default | (auto-detected) |

Network interface name for multicast traffic (e.g., `eth0`, `ens192`).

---

## Configuration Presets

### DefaultConfig

Balanced configuration for general workloads.

```go
func DefaultConfig() Config {
    return Config{
        InodeCacheSize:    10000,
        InodeTTL:          5 * time.Second,
        ExtentCacheSize:   50000,
        NegativeCacheSize: 1000,
        NegativeTTL:       1 * time.Second,
        StrictMode:        false,
        Network: broadcast.Config{
            Enabled: false,
        },
    }
}
```

### NetworkEnabledConfig

Default configuration with network broadcast enabled.

```go
func NetworkEnabledConfig() Config {
    cfg := DefaultConfig()
    cfg.Network = broadcast.Config{
        Enabled:           true,
        MulticastGroup:    "239.192.74.75",
        MulticastPort:     31337,
        MulticastTTL:      1,
        HeartbeatInterval: 30 * time.Second,
    }
    return cfg
}
```

### HighConsistencyConfig

For workloads requiring strong consistency.

```go
func HighConsistencyConfig() Config {
    cfg := NetworkEnabledConfig()
    cfg.StrictMode = true
    cfg.InodeTTL = 1 * time.Second
    cfg.NegativeTTL = 500 * time.Millisecond
    return cfg
}
```

### HighPerformanceConfig

For read-heavy workloads where eventual consistency is acceptable.

```go
func HighPerformanceConfig() Config {
    cfg := DefaultConfig()
    cfg.InodeCacheSize = 50000
    cfg.ExtentCacheSize = 200000
    cfg.InodeTTL = 30 * time.Second
    cfg.NegativeTTL = 5 * time.Second
    return cfg
}
```

---

## Tuning Recommendations

### Read-Heavy Workloads

Optimize for cache hit rate:

```bash
./goclusterfs -device /dev/device -mount /mnt/fs \
    -cache-ttl 30s \
    -journal \
    -broadcast
```

**Code-level tuning**:
- InodeCacheSize: 50,000+
- ExtentCacheSize: 200,000+
- InodeTTL: 30 seconds
- StrictMode: false

### Write-Heavy Workloads

Optimize for write throughput:

```bash
./goclusterfs -device /dev/device -mount /mnt/fs \
    -cache-ttl 1s \
    -journal \
    -trim \
    -trim-batch 512
```

**Code-level tuning**:
- CommitMode: CommitBatch
- MaxBatchTransactions: 64
- MaxBatchAge: 200ms
- TRIM BatchSize: 512

### Database Workloads

Optimize for consistency:

```bash
./goclusterfs -device /dev/device -mount /mnt/fs \
    -cache-strict \
    -journal \
    -broadcast
```

**Code-level tuning**:
- StrictMode: true
- CommitMode: CommitSync
- InodeTTL: 1 second

### Small Files (< 64KB)

SFB containers are disabled, so small files use full 1 MB blocks. `-sfb` is accepted and ignored.

### Large File Streaming

For sequential large file access:

```bash
./goclusterfs -device /dev/device -mount /mnt/fs \
    -journal \
    -cache-ttl 1s
```

**Code-level tuning**:
- ExtentCacheSize: 500,000+
- ReadAheadBlocks: (if implemented)

### Memory-Constrained Environments

Reduce memory footprint:

```bash
./goclusterfs -device /dev/device -mount /mnt/fs \
    -cache-ttl 1s \
    -journal
```

**Code-level tuning**:
- InodeCacheSize: 5,000
- ExtentCacheSize: 20,000
- NegativeCacheSize: 500

---

## Environment Variables

GoClusterFS respects the following environment variables:

| Variable | Description |
|----------|-------------|
| `GOCLUSTERFS_DEBUG` | Enable debug logging (alternative to `-debug`) |
| `GOCLUSTERFS_SECRET` | Broadcast secret (alternative to `-broadcast-secret`) |

---

## Related Documentation

- [Deployment Guide](DEPLOYMENT.md) - Installation and setup
- [Operations Guide](OPERATIONS.md) - Day-to-day operations
- [Troubleshooting](TROUBLESHOOTING.md) - Problem diagnosis
- [Design Overview](DESIGN.md) - Architecture and internals
