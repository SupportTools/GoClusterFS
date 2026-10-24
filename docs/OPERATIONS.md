# GoClusterFS Operations Guide

This guide covers day-to-day operations for managing GoClusterFS deployments.

## Table of Contents

1. [Monitoring](#monitoring)
2. [Common Operations](#common-operations)
3. [Cluster Management](#cluster-management)
4. [Backup and Recovery](#backup-and-recovery)
5. [Health Checks](#health-checks)
6. [Performance Tuning](#performance-tuning)
7. [Security Model](#security-model)

---

## Monitoring

GoClusterFS has no metrics endpoint. `goclusterfs-mount` writes its log to stderr, so capture it (see [Background Mount](#background-mount-with-logging) or the systemd unit in the [Deployment Guide](DEPLOYMENT.md#systemd-service-single-node)). The log also has many `[DEBUG]` lines, which are always on.

### Totals at Unmount

When a mount exits, it logs totals for the whole mount:

```
TRIM stats: queued=..., discarded=..., calls=..., coalesce=...
Journal stats: commits=..., bytes=..., state=...
Cache stats: inodes=... (hit=...%), extents=... (hit=...%)
Broadcast stats: sent=..., received=..., duplicates=...
```

The cache hit percentages are wrong today and can read far above 100%, so don't tune by them. Cache sizes are fixed in code (10,000 inodes, 50,000 extents). Only the TTL and strict mode have flags; see [Cache Tuning](#cache-tuning).

### Log Lines to Watch

| Log line | Meaning |
|----------|---------|
| `Journal recovery: replayed N committed transactions` | This node replayed a journal left in its slot by a crashed mount. Normal after a crash. |
| `Journal recovery for slot N failed` | The mount stops. See [Damaged Journal Slot](#damaged-journal-slot). |
| `[Recovery] Replaying slot N (node ...) failed` | Another node's journal cannot be replayed. See [Damaged Journal Slot](#damaged-journal-slot). |
| `ERROR: heartbeats to node registry slot N have failed` | This node cannot write the device. It stops. |
| `Node registry slot N is no longer this node's` | Another node took this node's slot, usually after a stall. The mount exits so two nodes never share a journal. |
| `Warning: Failed to start cache broadcast` | The mount runs without broadcast. Other nodes' changes show up after `-cache-ttl` instead of right away. |
| `lock.Acquire: ... acquired (retries=N, dur=...)` | Lock waits. High `retries` or `dur` means nodes contend for the same files or directories. |
| `lock acquisition timed out` | A lock wait passed 30 seconds. |

### Journal

Each node writes its own 4 MB journal. Commits are synchronous by default. The journal checkpoints every 5 seconds, and also when a transaction does not fit. None of this has a flag.

---

## Common Operations

### Mounting the Filesystem

`goclusterfs-mount` runs in the foreground until the filesystem is unmounted. Journaling and broadcast are on by default.

#### Interactive Mount
```bash
./goclusterfs-mount -device /dev/device -mount /mnt/fs
```

#### Background Mount (with logging)
```bash
./goclusterfs-mount -device /dev/device -mount /mnt/fs \
    >> /var/log/goclusterfs.log 2>&1 &
```

#### Mount with Systemd
Use the unit file from the [Deployment Guide](DEPLOYMENT.md#systemd-service-single-node), then:
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

# Unmount; the mount process then exits
fusermount3 -u /mnt/fs

# Or with systemd
systemctl stop goclusterfs
```

#### Forced Unmount
**WARNING**: May cause data loss if writes are pending.

```bash
# Try lazy unmount first
fusermount3 -uz /mnt/fs

# If that fails, kill the processes using it, then unmount
fuser -km /mnt/fs
fusermount3 -u /mnt/fs
```

A mount that was killed leaves its journal for the next mount on the same host to replay.

### Running fsck

#### When to Run fsck

- After unclean shutdown (crash, power failure)
- After forced unmount
- When mount fails with corruption errors
- Periodically for preventive maintenance

Unmount the device on every node first. The device is a positional argument, and flags go before it.

```bash
# Check only (writes nothing)
./goclusterfs-fsck /dev/device

# List what -repair would change, then repair
./goclusterfs-fsck -repair -dry-run /dev/device
./goclusterfs-fsck -repair /dev/device
```

`-repair` refuses to run while a node has the device mounted or a stopped node's journal still needs replay. See the [fsck Guide](TROUBLESHOOTING.md#fsck-guide) for what it checks and changes, and for exit codes.

### Managing Snapshots

Snapshots and clones are experimental and work on single files through extended attributes. `setfattr` and `getfattr` come from the `attr` package. Snapshots are on by default (`-snapshots`).

```bash
# Create a snapshot of one file
setfattr -n user.snapshot -v snap1 /mnt/fs/data.db

# List the file's snapshots
getfattr -n user.snapshots /mnt/fs/data.db

# Read a snapshot: clone it to a new file (clones go in the root directory)
setfattr -n user.snapshot.clone -v snap1:/data.db.snap1 /mnt/fs/data.db

# Clone the current file
setfattr -n user.clone -v /data.db.copy /mnt/fs/data.db

# Delete a snapshot
setfattr -x user.snapshot.snap1 /mnt/fs/data.db
```

Snapshots and clones share blocks with the file and copy a block on its first write. A block is freed when no file, snapshot, or clone uses it. Before you mount an image with snapshots or clones using a newer build, read the upgrade notes in [CHANGELOG.md](../CHANGELOG.md).

---

## Cluster Management

### Node Requirements

- **Clocks:** Nodes judge each other's heartbeats by wall clock. A node that mounts with its clock more than about 10 seconds ahead treats live peers as failed, and can take a live peer's registry slot and replay that peer's journal. Run NTP on every node.
- **Restarts:** A restarted mount takes back the registry slot of a mount that exited on the same host (same hostname and kernel boot ID) and replays its journal before serving. In the same PID namespace it checks the old process ID; across PID namespaces it waits for the old slot's heartbeat to go stale.
- **Crashed node's locks:** Once a node is proven dead, the node replaying its journal takes over its exclusive locks and frees them after the replay, so no other node uses them mid-replay. Proven dead means its mount process is gone (same host and PID namespace), or its heartbeat is more than 20 seconds old and has not changed for 15 seconds by the observing node's own clock. A node paused for more than about 20 seconds therefore counts as dead; it exits on its next heartbeat. A dead node's shared locks, and the locks of a node not proven dead, are freed once they are 15 seconds old.
- **Node count:** The device has 64 registry slots, each with its own 4 MB journal. A crashed node's slot frees once another node replays its journal, or when the same host mounts again.

### Known Gaps in Slot Ownership

- Slot claims use a read-compare-write on the device, then re-read the slot after 100 ms. Two nodes can still both keep a slot if one node's write lands more than 100 ms after its read, for example during a storage stall.
- A node paused for more than 10 seconds (VM freeze, `SIGSTOP`, a stalled path) looks failed to a new mount, which can take its slot and replay and reset its journal. Running nodes wait until it is proven dead (about 20 seconds, see above). The paused node exits on its next heartbeat after it resumes, up to 1 second later, and can write to the journal region in that second.
- A restarted container comes back in a new PID namespace, so its mount cannot prove by PID that the previous incarnation exited. It waits up to about 12 seconds for the old slot's heartbeat to go stale, then replays that journal before serving.

### Adding a New Node

1. **Prepare the node**:
   ```bash
   # Install dependencies
   apt-get install fuse3

   # Copy the binaries; use the same version as the other nodes
   scp goclusterfs-mount goclusterfs-fsck node3:/usr/local/bin/
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
   # Use SAME options as existing nodes; never add -format here
   goclusterfs-mount -device /dev/mapper/shared_lun -mount /mnt/cluster \
       -mcast-group 239.192.74.75 -mcast-port 31337
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
   fusermount3 -u /mnt/cluster
   ```

4. **Disconnect storage**:
   ```bash
   iscsiadm -m node -T <iqn> -p <ip> --logout
   ```

### Distributed Lock Limits

Nodes coordinate metadata writes (block bitmap, inodes, directories, refcounts, SFB containers, snapshots) through lock records on the shared device. Read this before you run more than one node.

**How a lock goes stale.** A lock record stores the wall-clock time it was taken. Any other node may take over a lock that is older than 15 seconds (`StaleThreshold`), judged by that node's own clock. This is how locks held by a crashed node get freed. A node renews an exclusive hold only when another operation on the same node joins it after 5 seconds; a single long operation is not renewed.

**The hazard: a stalled node keeps writing.** If a node stops for more than 15 seconds while it holds a lock, another node takes the lock. When the stalled node resumes, it finishes its writes. No write path checks that the lock is still held, and there is no fencing token. The node only learns it lost the lock when it next joins or releases it (`ErrLockLost`, `ErrNotLockOwner`), and most callers ignore the release error. Two nodes then write the same metadata, which can double-allocate blocks or lose directory entries.

Common causes of a stall:

- VM pause, suspend, live migration, or a memory snapshot
- `docker pause`, cgroup freezer, `SIGSTOP`, or a debugger on the mount process
- Heavy swapping or a long GC pause
- Storage I/O that queues instead of failing, for example a multipath or iSCSI path failover

**Lock takeover is not atomic.** Taking a lock is a read followed by a write, not an atomic compare-and-swap. Two nodes that read a free record within one disk round trip of each other can both take it. This is rare but more likely on busy locks such as the block bitmap. The same race can lose an update to a read lock's reader count. The lock then looks held until it is 15 seconds old, so writers to that directory or file can stall for up to 15 seconds.

**Read locks are not renewed.** A node whose overlapping lookups keep a directory read-locked for more than 15 seconds can lose that lock to another node's writer. Its readers may then see a half-written directory. Two writers still cannot overlap.

**Locks share records.** The lock region has 256 records, and every lock maps to one of them, so unrelated files and the block bitmap can share a record. A node holding one of those locks blocks other nodes from all of them. If two nodes each hold one record and wait for the other's, both wait until one record is 15 seconds old; the takeover then lets both proceed, with the same risk as a stall. This needs nested locks whose records cross, roughly 1 in 65,000 pairs of overlapping operations.

**Safe operating limits:**

| Limit | Why |
|-------|-----|
| Keep node clocks within 1 second (chrony or NTP) | Clock skew S shortens the stale threshold to 15s − S |
| No node may stall for 10 seconds or more while mounted | Leaves margin below the 15s threshold |
| Storage I/O must fail, not queue, within 10 seconds | A queued write that lands after takeover corrupts metadata. Do not use multipath `queue_if_no_path`; lower iSCSI `node.session.timeo.replacement_timeout` (default 120s) |
| Unmount before you pause, migrate or snapshot a node VM | Unmounting releases its locks |

If a node did stall for more than 15 seconds while mounted (hung-task warnings in `dmesg`, a VM pause, a path failover), stop its mount process before it does more writes, then check the data from a healthy node.

What is safe today: a node that crashes or is killed and stays down, and a clean unmount. Other nodes take over a crashed node's locks as described under [Node Requirements](#node-requirements).

### Handling Node Failure

When a node fails unexpectedly:

1. **Other nodes keep running.** Operations that need one of the failed node's locks wait until the lock is taken over.
2. **Locks and journal.** Once the failed node is proven dead, one surviving node takes over its exclusive locks, replays its journal, then frees the locks. Other locks are taken over once they are 15 seconds old. See [Node Requirements](#node-requirements).
3. **No action is needed** on the surviving nodes.

**When the failed node comes back**, mount it again with the same options. If it gets its old registry slot back (same hostname and boot ID), it replays its own journal before serving. Don't run fsck while other nodes have the device mounted: `-repair` refuses, and a check-only run reads metadata that is changing.

```bash
goclusterfs-mount -device /dev/device -mount /mnt/cluster
```

### Damaged Journal Slot

If a mount exits with `Journal recovery for slot N failed`, that slot's journal header is damaged and its transactions cannot be replayed. Mount once with `-skip-recovery` on the same host, before it reboots: the mount discards the journal and starts a new one, and later mounts work normally. Then run fsck, because discarded transactions may have been only partly applied.

`-skip-recovery` only affects the slot this mount takes. It does not stop the node from replaying crashed peers' journals. `-journal=false` also skips the replay of its own slot, and leaves that journal in place for the next mount, so don't use it on a device that may have a dirty journal.

A slot belongs to its host's hostname and boot ID. If the damaged slot belongs to a node that has rebooted or will not mount again, the other nodes log `Replaying slot N ... failed` once a minute and the slot stays taken. No tool frees it yet; the other 63 slots are unaffected.

### Emergency: All Nodes Down

If all nodes go down simultaneously:

1. **Start one node first**. It replays its own slot's journal on mount, then replays the journals of the other crashed nodes once their heartbeats are stale:
   ```bash
   goclusterfs-mount -device /dev/device -mount /mnt/cluster
   ```
   If the data looks wrong, unmount it and run `goclusterfs-fsck /dev/device`.

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
fusermount3 -u /mnt/cluster

# Create block-level backup
dd if=/dev/mapper/shared_lun of=/backup/goclusterfs_$(date +%Y%m%d).img bs=4M status=progress
```

#### Method 2: File-Level Backup (Online)

```bash
# Can run while mounted; files that change during the copy may be inconsistent
rsync -av /mnt/cluster/ /backup/goclusterfs_files/
```

#### Method 3: Snapshot of One File

Snapshots work on single files, not the whole filesystem. To copy a consistent version of one busy file, snapshot it, clone the snapshot, and copy the clone:

```bash
setfattr -n user.snapshot -v backup1 /mnt/cluster/data.db
setfattr -n user.snapshot.clone -v backup1:/data.db.backup1 /mnt/cluster/data.db
cp /mnt/cluster/data.db.backup1 /backup/
rm /mnt/cluster/data.db.backup1
setfattr -x user.snapshot.backup1 /mnt/cluster/data.db
```

### Recovery Procedures

#### Recovery from Backup

```bash
# Unmount all nodes
fusermount3 -u /mnt/cluster  # On all nodes

# Restore block device
dd if=/backup/goclusterfs_20240101.img of=/dev/mapper/shared_lun bs=4M status=progress

# Check it
goclusterfs-fsck /dev/mapper/shared_lun

# Remount
goclusterfs-mount -device /dev/mapper/shared_lun -mount /mnt/cluster
```

#### Recovery from Corruption

```bash
# Step 1: Unmount on every node
fusermount3 -u /mnt/cluster

# Step 2: See what fsck finds and what -repair would change
goclusterfs-fsck -repair -dry-run /dev/device

# Step 3: Back up the device, then repair
goclusterfs-fsck -repair /dev/device

# Step 4: Remount
goclusterfs-mount -device /dev/device -mount /mnt/cluster
```

`-repair` fixes refcounts and the superblock checksum. It never frees blocks, deletes inodes, or edits directories, and there is no `lost+found`. Restore anything else from backup. See the [fsck Guide](TROUBLESHOOTING.md#fsck-guide).

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

Nodes prove they are alive by writing a heartbeat to their registry slot on the device every second. That is what failure detection uses.

Separately, each node sends a broadcast heartbeat over UDP every 30 seconds, and an invalidation message for each metadata change:

```bash
tcpdump -i eth0 -c 10 udp port 31337
```

---

## Performance Tuning

### Identifying Bottlenecks

| Symptom | Likely Cause | Where to Look |
|---------|-------------|---------------|
| Slow `ls -l` or `stat` | Each name lookup reads the whole directory from disk | Keep directories small; see [README limits](../README.md#limits-and-status) |
| Slow reads | Cache misses | `Cache stats` at unmount |
| Slow metadata | Lock contention | `lock.Acquire` lines with high `retries` |
| Slow writes | Synchronous journal commits and per-block extent locks | Storage latency |

### Cache Tuning

Cache sizes are fixed in code (10,000 inodes, 50,000 extents). The flags set how long cached data may be used:

| Flag | Default | Effect |
|------|---------|--------|
| `-attr-timeout` | `1s` | How long the kernel caches attributes and directory entries. `stat` and `ls` can show another node's change up to this late. `0` makes every `stat` and path step read the directory under a cluster lock, which is much slower. |
| `-cache-ttl` | `5s` | How long the mount trusts a cached inode before it checks the disk. Open files can lag another node's change by up to this long. |
| `-cache-strict` | `false` | Check the disk on every cache hit |

```bash
# Read-heavy, changes from other nodes may show up later
goclusterfs-mount ... -cache-ttl 30s

# Every node sees other nodes' changes as soon as possible, at a cost
goclusterfs-mount ... -cache-strict -attr-timeout 0
```

### Journal Tuning

Commits are synchronous: each metadata change waits for its journal write. Batch commit mode exists in code (32 transactions or 100 ms per batch) but has no flag.

### TRIM Tuning

```bash
# For SSDs/NVMe: TRIM with larger batches
goclusterfs-mount ... \
    -trim-batch 512 \
    -trim-age 10s

# For HDDs: Disable TRIM (no benefit)
goclusterfs-mount ... \
    -trim=false
```

---

## Security Model

GoClusterFS assumes every node, and everything that can reach the shared device or the broadcast port, is trusted.

**The shared device.** All data, locks, and journals live on the device in plain form. Nothing on disk is encrypted or signed, so any host that can write the device can change any file or take any lock. Give only the cluster nodes access to it.

**Who can use a mount.** `goclusterfs-mount` does not set the FUSE `allow_other` option, so only the user who started the mount can use it. GoClusterFS does not check file permission bits, so that user can read and write every file.

**Locks.** Locks keep cooperating nodes from changing the same metadata at once. They are not access control, and a stalled node can keep writing after it lost a lock. See [Distributed Lock Limits](#distributed-lock-limits).

**Broadcast cache invalidation.** Nodes send UDP messages (multicast group `239.192.74.75`, port `31337` by default) that tell peers to drop cached inodes and extents. Heartbeat messages add the sender as a unicast peer, which a node uses when multicast sends fail. Messages carry inode numbers but no file data. Losing them only delays cache updates until `-cache-ttl`.

- With an empty `-broadcast-secret` (the default), messages are not authenticated. Any host that can send to the port can make nodes drop their caches, and can add itself as a unicast peer.
- With `-broadcast-secret`, every message carries an HMAC-SHA256 tag. Nodes drop messages with a missing or wrong tag, so all nodes need the same secret.
- Messages are never encrypted. Any host that joins the multicast group can see which inodes change.
- The secret is a command-line flag, so local users on a node can read it with `ps`.

Limit UDP port 31337 to the cluster nodes with a firewall either way.

**HTTP API.** This guide does not document the HTTP API yet.

---

## Next Steps

- [Troubleshooting](TROUBLESHOOTING.md) - Problem diagnosis
- [Configuration Reference](API-CONFIG.md) - All configuration options
