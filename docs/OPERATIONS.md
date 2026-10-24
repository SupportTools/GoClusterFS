# GoClusterFS Operations Guide

This guide covers day-to-day operations for managing GoClusterFS deployments.

## Table of Contents

1. [Monitoring](#monitoring)
2. [Common Operations](#common-operations)
3. [Cluster Management](#cluster-management)
4. [Backup and Recovery](#backup-and-recovery)
5. [Health Checks](#health-checks)
6. [Performance Tuning](#performance-tuning)

---

## Monitoring

### Cache Statistics

GoClusterFS tracks cache performance metrics that indicate system health:

| Metric | Description | Healthy Range |
|--------|-------------|---------------|
| `InodeCacheHitRate` | % of inode lookups served from cache | > 80% |
| `ExtentCacheHitRate` | % of extent lookups served from cache | > 70% |
| `NegativeHitRate` | % of "not found" lookups served from cache | > 50% |
| `InvalidationsSent` | Network invalidations broadcast | Proportional to writes |
| `InvalidationsReceived` | Network invalidations from peers | Should match peers' sent |

### Interpreting Cache Hit Rates

**High hit rate (>90%)**: Cache is effective. Consider reducing cache size to save memory.

**Low hit rate (<50%)**: Possible causes:
- Working set exceeds cache size → Increase `InodeCacheSize`/`ExtentCacheSize`
- Strict mode enabled → Expected behavior, disable if not needed
- Short TTLs → Increase `cache-ttl`

**Declining hit rate**: May indicate:
- Changing access patterns
- Memory pressure causing evictions
- Increased cluster activity

### Journal Health

Monitor journal status to prevent issues:

| Indicator | Description | Action Threshold |
|-----------|-------------|------------------|
| Journal Fullness | % of journal slots used | > 75% |
| Checkpoint Lag | Time since last checkpoint | > 30s |
| Pending Transactions | Uncommitted transactions | > 100 |

**Journal Full Handling**:
When journal reaches `FullThreshold` (default 75%), it forces a checkpoint. If this happens frequently:
1. Increase journal slot count
2. Reduce checkpoint interval
3. Consider `CommitBatch` mode for write-heavy workloads

### Lock Contention

High lock contention indicates cluster coordination overhead:

| Symptom | Cause | Mitigation |
|---------|-------|------------|
| Slow metadata ops | Lock acquisition delays | Review application access patterns |
| Timeout errors | Lock held too long | Check for stuck processes |
| High retry counts | Contention | Partition workload across nodes |

---

## Common Operations

### Mounting the Filesystem

#### Interactive Mount
```bash
./goclusterfs -device /dev/device -mount /mnt/fs -journal -broadcast
```

#### Background Mount (with logging)
```bash
./goclusterfs -device /dev/device -mount /mnt/fs -journal -broadcast \
    2>&1 | tee /var/log/goclusterfs.log &
```

#### Mount with Systemd
```bash
systemctl start goclusterfs
systemctl status goclusterfs
journalctl -u goclusterfs -f
```

### Unmounting

#### Clean Unmount
```bash
# Ensure no processes using the filesystem
lsof +D /mnt/fs

# Unmount
fusermount -u /mnt/fs

# Or with systemd
systemctl stop goclusterfs
```

#### Forced Unmount
**WARNING**: May cause data loss if writes are pending.

```bash
# Try lazy unmount first
fusermount -uz /mnt/fs

# If that fails, force kill
fuser -km /mnt/fs
fusermount -u /mnt/fs
```

### Running fsck

#### When to Run fsck

- After unclean shutdown (crash, power failure)
- After forced unmount
- When mount fails with corruption errors
- Periodically for preventive maintenance

#### Check-Only Mode
```bash
# Unmount first
fusermount -u /mnt/fs

# Run check
./goclusterfs-fsck -device /dev/device
```

#### Repair Mode
```bash
./goclusterfs-fsck -device /dev/device -repair
```

#### Force Check
```bash
# Check even if filesystem appears clean
./goclusterfs-fsck -device /dev/device -force
```

### Managing Snapshots

#### Create Snapshot
```bash
# API for snapshot creation (when implemented)
# Snapshots are O(1) - only metadata changes
```

#### List Snapshots
```bash
# List snapshot metadata
ls -la /mnt/fs/.snapshots/
```

#### Delete Snapshot
```bash
# Remove snapshot (blocks freed when refcount reaches 0)
rm /mnt/fs/.snapshots/snapshot_name
```

---

## Cluster Management

### Node Requirements

- **Clocks:** Nodes judge each other's heartbeats by wall clock. A node more than about 10 seconds ahead treats live peers as failed, replays their journals and takes their registry slots. Run NTP on every node.
- **Restarts:** A restarted mount takes back the registry slot of a mount that exited on the same host (same hostname and kernel boot ID) and replays its journal before serving. In the same PID namespace it checks the old process ID; across PID namespaces it waits for the old slot's heartbeat to go stale.
- **Crashed node's locks:** Once a node is proven dead, the node replaying its journal takes over its exclusive locks and frees them after the replay, so no other node uses them mid-replay. Proven dead means its mount process is gone (same host and PID namespace), or its heartbeat is more than 20 seconds old and has not changed for 15 seconds by the observing node's own clock. A node paused for more than about 20 seconds therefore counts as dead; it exits on its next heartbeat. A dead node's shared locks, and the locks of a node not proven dead, are freed once they are 15 seconds old.
- **Node count:** The device has 64 registry slots, each with its own 4 MB journal. A crashed node's slot frees once another node replays its journal, or when the same host mounts again.

### Known Gaps in Slot Ownership

- Slot claims use a read-compare-write on the device, then re-read the slot after 100 ms. Two nodes can still both keep a slot if one node's write lands more than 100 ms after its read, for example during a storage stall.
- A node paused for more than 10 seconds (VM freeze, `SIGSTOP`, a stalled path) looks failed. Another node can replay and reset its journal, or a new mount can take its slot. The paused node exits on its next heartbeat after it resumes, up to 1 second later, and can write to the journal region in that second.
- A restarted container comes back in a new PID namespace, so its mount cannot prove by PID that the previous incarnation exited. It waits up to about 12 seconds for the old slot's heartbeat to go stale, then replays that journal before serving.

### Adding a New Node

1. **Prepare the node**:
   ```bash
   # Install dependencies
   apt-get install fuse3

   # Copy binary
   scp goclusterfs node3:/usr/local/bin/
   ```

2. **Configure shared storage**:
   ```bash
   # Connect to shared storage (iSCSI example)
   iscsiadm -m node -T <iqn> -p <ip> --login

   # Verify device is visible
   ls -la /dev/mapper/shared_lun
   ```

3. **Mount with cluster settings**:
   ```bash
   # Use SAME options as existing nodes
   ./goclusterfs -device /dev/mapper/shared_lun -mount /mnt/cluster \
       -journal -broadcast -mcast-group 239.192.74.75 -mcast-port 31337
   ```

4. **Verify cluster membership**:
   ```bash
   # Check broadcast communication
   tcpdump -i eth0 udp port 31337

   # Test cross-node file access
   echo "test from new node" > /mnt/cluster/node3_test.txt
   ```

### Removing a Node Gracefully

1. **Drain workload** (if applicable):
   ```bash
   # Stop applications using the filesystem
   systemctl stop myapp
   ```

2. **Verify no open files**:
   ```bash
   lsof +D /mnt/cluster
   ```

3. **Unmount**:
   ```bash
   fusermount -u /mnt/cluster
   ```

4. **Disconnect storage**:
   ```bash
   iscsiadm -m node -T <iqn> -p <ip> --logout
   ```

### Distributed Lock Limits

Nodes coordinate metadata writes (block bitmap, inodes, directories, refcounts, SFB containers, snapshots) through lock records on the shared device. Read this before you run more than one node.

**How a lock goes stale.** A lock record stores the wall-clock time it was taken. A node refreshes that time only when another operation on the same node joins a write lock it has held for more than 5 seconds. Any other node may take over a lock that is older than 15 seconds (`StaleThreshold`), judged by that node's own clock. This is how locks held by a crashed node get freed.

**The hazard: a stalled node keeps writing.** If a node stops for more than 15 seconds while it holds a lock, another node takes the lock. When the stalled node resumes, it finishes its writes. No write path checks that the lock is still held, and there is no fencing token. The node only learns it lost the lock when it releases it (`ErrNotLockOwner`), and callers ignore that error. Two nodes then write the same metadata, which can double-allocate blocks or lose directory entries.

Common causes of a stall:

- VM pause, suspend, live migration, or a memory snapshot
- `docker pause`, cgroup freezer, `SIGSTOP`, or a debugger on the mount process
- Heavy swapping or a long GC pause
- Storage I/O that queues instead of failing, for example a multipath or iSCSI path failover

**Lock takeover relies on an I/O time limit.** Taking a lock is a read followed by a write, not an atomic compare-and-swap, so two nodes that read a free record at the same moment both write a claim. After writing its claim, a node waits 20 ms (`ClaimSettleDelay`), reads the record again, and keeps the lock only if its claim is still there. A node drops an update whose read is already 10 ms old when it is about to write, and tries again. This is safe only if every node's read of a lock record and its following write land within 20 ms. Time spent in I/O queues counts toward the 20 ms. A node that stalls right before its write lands can still win alongside another node. Readers that join or leave a record held for reading skip the wait, and losing that race can miscount readers. A count that is too low can let another node's writer in while a read is still running. A count that is too high makes writers wait up to 15 seconds. Two writers still cannot overlap.

**Nodes keep released locks briefly.** After its last user releases a lock, a node keeps it for 250 ms (`CacheGrace`), so repeated work on the same node does not pay the 20 ms wait again. A node that has held a lock for longer than that releases it the next time it is idle and waits 50 ms (`YieldGap`) before taking it again, so another node gets a turn. Another node that wants the lock waits up to about 300 ms plus the 20 ms check. A clean unmount releases kept locks at once. A lock wait ends only when the caller's deadline passes or after 30 seconds (`DefaultTimeout`). A signal to the waiting process does not end it, because FUSE turns signals into request cancellations.

**Writes to one region from two nodes interleave by sector.** A node writes whole 4 KB sectors of an existing, unshared block without taking that block's lock. If two nodes write overlapping ranges of a file at the same time, the result can mix both writes, one sector at a time. Applications that write the same region from several nodes must coordinate those writes themselves.

**Read locks are not renewed.** A node whose overlapping lookups keep a directory read-locked for more than 15 seconds can lose that lock to another node's writer. Its readers may then see a half-written directory. Two writers still cannot overlap.

**Locks share records.** The lock region has 256 records, and every lock maps to one of them, so unrelated files and the block bitmap can share a record. A node holding one of those locks blocks other nodes from all of them. If two nodes each hold one record and wait for the other's, both wait until one record is 15 seconds old; the takeover then lets both proceed, with the same risk as a stall. This needs nested locks whose records cross, roughly 1 in 65,000 pairs of overlapping operations.

**Safe operating limits:**

| Limit | Why |
|-------|-----|
| Keep node clocks within 1 second (chrony or NTP) | Clock skew S shortens the stale threshold to 15s − S |
| No node may stall for 10 seconds or more while mounted | Leaves margin below the 15s threshold |
| A 4KB read followed by a 4KB write of a lock record must complete within 20 ms, including queue time | Lock takeover check (see above). A node retries when its read is 10 ms old, so the write itself must land within 10 ms; slower storage can let two nodes take one lock |
| Storage I/O must fail, not queue, within 10 seconds | A queued write that lands after takeover corrupts metadata. Do not use multipath `queue_if_no_path`; lower iSCSI `node.session.timeo.replacement_timeout` (default 120s) |
| Unmount before you pause, migrate or snapshot a node VM | Unmounting releases its locks |

If a node did stall for more than 15 seconds while mounted (hung-task warnings in `dmesg`, a VM pause, a path failover), stop its mount process before it does more writes, then check the data from a healthy node.

What is safe today: a node that crashes or is killed and stays down, and a clean unmount. Other nodes take over a crashed node's locks 15 seconds after it acquired them, or once it is proven dead.

### Handling Node Failure

When a node fails unexpectedly:

1. **Other nodes continue operating** - Distributed locking handles failed node
2. **Lock cleanup** - Other nodes take over the failed node's locks once they are 15 seconds old (lock age, not node heartbeat). The node that replays a failed node's journal takes its exclusive locks sooner, once the failed node is proven dead (see "Crashed node's locks" above).
3. **No immediate action required** - System is self-healing

**When failed node recovers**:
```bash
# fsck may be required if node crashed during write
./goclusterfs-fsck -device /dev/device

# Then remount normally
./goclusterfs -device /dev/device -mount /mnt/cluster -journal -broadcast
```

### Emergency: All Nodes Down

If all nodes go down simultaneously:

1. **Start one node first**:
   ```bash
   # Run fsck
   ./goclusterfs-fsck -device /dev/device -repair

   # Mount
   ./goclusterfs -device /dev/device -mount /mnt/cluster -journal
   ```

2. **Start remaining nodes** one at a time

3. **Verify data integrity**:
   ```bash
   # Check critical files
   find /mnt/cluster -type f -exec md5sum {} \; > checksums.txt
   ```

---

## Backup and Recovery

### Backup Strategies

#### Method 1: Block-Level Backup (Offline)

```bash
# Unmount all nodes
# On each node:
fusermount -u /mnt/cluster

# Create block-level backup
dd if=/dev/mapper/shared_lun of=/backup/goclusterfs_$(date +%Y%m%d).img bs=4M status=progress
```

#### Method 2: File-Level Backup (Online)

```bash
# Can run while mounted (snapshot recommended)
rsync -avz /mnt/cluster/ /backup/goclusterfs_files/
```

#### Method 3: Snapshot-Based Backup

```bash
# Create snapshot (O(1) operation)
# snapshot_create /mnt/cluster backup_$(date +%Y%m%d)

# Backup from snapshot (no impact on live data)
rsync -avz /mnt/cluster/.snapshots/backup_20240101/ /backup/
```

### Recovery Procedures

#### Recovery from Backup

```bash
# Unmount all nodes
fusermount -u /mnt/cluster  # On all nodes

# Restore block device
dd if=/backup/goclusterfs_20240101.img of=/dev/mapper/shared_lun bs=4M status=progress

# Run fsck
./goclusterfs-fsck -device /dev/mapper/shared_lun -repair

# Remount
./goclusterfs -device /dev/mapper/shared_lun -mount /mnt/cluster -journal
```

#### Recovery from Corruption

```bash
# Step 1: Unmount
fusermount -u /mnt/cluster

# Step 2: Run fsck with repair
./goclusterfs-fsck -device /dev/device -repair -force

# Step 3: Review fsck output for:
# - Orphaned inodes
# - Lost blocks
# - Fixed errors

# Step 4: Remount
./goclusterfs -device /dev/device -mount /mnt/cluster -journal

# Step 5: Check lost+found for recovered files
ls -la /mnt/cluster/lost+found/
```

---

## Health Checks

### Quick Health Check Script

```bash
#!/bin/bash
# goclusterfs-healthcheck.sh

MOUNTPOINT="/mnt/cluster"
DEVICE="/dev/mapper/shared_lun"

# Check if mounted
if ! mountpoint -q "$MOUNTPOINT"; then
    echo "CRITICAL: Filesystem not mounted"
    exit 2
fi

# Check write capability
TESTFILE="$MOUNTPOINT/.healthcheck_$(hostname)"
if ! echo "$(date)" > "$TESTFILE" 2>/dev/null; then
    echo "CRITICAL: Cannot write to filesystem"
    exit 2
fi
rm -f "$TESTFILE"

# Check available space
AVAIL=$(df --output=pcent "$MOUNTPOINT" | tail -1 | tr -d ' %')
if [ "$AVAIL" -gt 90 ]; then
    echo "WARNING: Filesystem ${AVAIL}% full"
    exit 1
fi

echo "OK: Filesystem healthy (${AVAIL}% used)"
exit 0
```

### Cluster Health Check

```bash
#!/bin/bash
# cluster-healthcheck.sh

NODES="node1 node2 node3"
MOUNTPOINT="/mnt/cluster"

for node in $NODES; do
    # Check if reachable
    if ! ping -c 1 -W 2 "$node" >/dev/null 2>&1; then
        echo "CRITICAL: $node unreachable"
        continue
    fi

    # Check mount status
    if ssh "$node" "mountpoint -q $MOUNTPOINT" 2>/dev/null; then
        echo "OK: $node mounted"
    else
        echo "WARNING: $node not mounted"
    fi
done
```

### Heartbeat Verification

Check that nodes are communicating:

```bash
# Monitor multicast traffic
tcpdump -i eth0 -c 10 udp port 31337

# Expected: Periodic heartbeats from all nodes
```

---

## Performance Tuning

### Identifying Bottlenecks

| Symptom | Likely Cause | Diagnostic Command |
|---------|-------------|-------------------|
| Slow reads | Cache misses | Check cache hit rates |
| Slow writes | Journal sync | Check commit mode |
| Slow metadata | Lock contention | Check lock stats |
| High latency | Network | Check multicast latency |

### Cache Tuning

```bash
# For read-heavy workloads: Increase cache sizes
./goclusterfs ... \
    # Increase inode cache (default 10000)
    # Increase extent cache (default 50000)
    -cache-ttl 30s

# For write-heavy workloads: Reduce cache reliance
./goclusterfs ... \
    -cache-strict \
    -cache-ttl 1s
```

### Journal Tuning

```bash
# For high write throughput: Use batch commit
# (Configure via code - BatchConfig)
# MaxBatchTransactions: 64
# MaxBatchAge: 200ms

# For lowest latency: Use sync commit (default)
# Each write waits for fsync
```

### TRIM Tuning

```bash
# For SSDs/NVMe: Enable TRIM with larger batches
./goclusterfs ... \
    -trim \
    -trim-batch 512 \
    -trim-age 10s

# For HDDs: Disable TRIM (no benefit)
./goclusterfs ... \
    -trim=false
```

---

## Next Steps

- [Troubleshooting](TROUBLESHOOTING.md) - Problem diagnosis
- [Configuration Reference](API-CONFIG.md) - All configuration options
