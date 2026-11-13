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

`goclusterfs-mount` serves health at `/health` and Prometheus metrics at `/metrics` (see [HTTP API Health and Shutdown](API-CONFIG.md#http-api-health-and-shutdown)). It writes its log to stderr, so capture it (see [Background Mount](#background-mount-with-logging) or the systemd unit in the [Deployment Guide](DEPLOYMENT.md#systemd-service-single-node)). With `-debug`, the log also has `[DEBUG]` lines.

### Totals at Unmount

When a mount exits, it logs totals for the whole mount:

```
TRIM stats: queued=..., discarded=..., calls=..., coalesce=...
Journal stats: commits=..., bytes=..., state=...
Cache stats: inodes=... (hit=...%), extents=... (hit=...%)
Broadcast stats: sent=..., received=..., duplicates=..., lost=...
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
| `[broadcast] missed N messages from ...` | This node lost broadcast messages, usually because its receive buffer filled during a burst of changes on another node. It dropped all cached inodes and extents and rereads them from the device. Logged at most every 10 seconds. If it repeats, raise `net.core.rmem_max` to at least 1 MB (`sysctl -w net.core.rmem_max=1048576`) and remount. |
| `lock.Acquire: ... acquired (retries=N, dur=...)` | Lock waits. High `retries` or `dur` means nodes contend for the same files or directories. |
| `[WARN] lock: gave up waiting for ...` | A lock wait gave up after 30 seconds (`DefaultTimeout`), or a background task was stopped at unmount. The line names the node that holds the lock. A file operation that was waiting fails with EIO. See [Interrupted File Operations](#interrupted-file-operations). |

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

`-repair` refuses to run while a node has the device mounted, while a stopped node's journal still needs replay, or when mount would refuse the superblock's layout. See the [fsck Guide](TROUBLESHOOTING.md#fsck-guide) for what it checks and changes, and for exit codes.

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
- **Crashed node's locks:** Once a node is proven dead, the node replaying its journal takes over its exclusive locks and frees them after the replay, so no other node uses them mid-replay. Proven dead means its mount process is gone (same host and PID namespace), or its heartbeat is more than 20 seconds old and has not changed for 15 seconds by the observing node's own clock. A node paused for more than about 20 seconds therefore counts as dead; it exits on its next heartbeat. The dead node's read locks are freed at once. Until a failed node is replayed, other nodes wait for its locks however long that takes. Failover takes about 3 to 6 seconds when a surviving mount runs on the same host and in the same PID namespace, and up to about 35 seconds otherwise. A mount that takes a failed node's registry slot keeps that node's locks until it has replayed the slot's journal.
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

### Interrupted File Operations

GoClusterFS ignores interrupts, like an NFS `hard` mount. A signal to a program that is inside a file operation, including the `SIGURG` the Go runtime sends for preemption, does not end the operation; the program handles the signal once the operation returns. `SIGKILL` still ends the program. A lock wait that cannot finish fails the operation with EIO after 30 seconds (`DefaultTimeout`), and the mount log names the lock and the node that holds it.

### Distributed Lock Limits

Nodes coordinate metadata writes (block bitmap, inodes, directories, refcounts, SFB containers, snapshots) through lock records on the shared device. Read this before you run more than one node.

**How a node takes a lock.** Each of the 256 lock records has one 4 KB claim slot per registry slot, and a node writes only its own slot. To take a lock, a node writes its claim into its slot, then reads all 64 slots of the record in one read. It holds the lock if no other slot conflicts: for a write lock, any other claim; for a read lock, another node's write claim. Of two nodes that claim at the same time, the one whose write finished second reads the record after the other's write finished, so it sees that claim and does not take the lock. This holds however slow the writes are. It relies on the shared device showing a finished write to every later read, from any node.

**When claims collide.** The node in the lower registry slot keeps its claim and waits for the other claims to go. A node that sees a claim in a lower slot clears its own and waits, reading the record without writing until that claim is gone. Waits only point from lower to higher slots, so two nodes never wait on each other. A claim kept for a journal replay counts as lower than every slot, so the replaying node is never stuck behind a waiter. When two nodes replay dead nodes whose claims share a record, the replay of the lower dead slot goes first. A slot that fails its checksum, such as one torn by a crash in the middle of a write, counts as a claim.

**When another node's claim stops counting.** A claim counts until the registry slot with the same number is empty: its node unmounted cleanly, or another node replayed its journal and freed the slot. A node checks this by reading the registry slot from the device after it reads the record, so it cannot miss a node that registered in the meantime. A torn slot follows the same rule. After its replay, a mount clears the claims in its own slot that an earlier occupant left. How old a claim is does not matter, so a lock that a node never releases, for example after a bug, stays held until that node unmounts. The node logs a warning for each lock it has held for more than 60 seconds. A replay that fails or gets stuck keeps the dead node's locks held until a later replay succeeds; waits for them fail with `EIO` once its recovery lease lapses, about a minute after the replay last made progress. A mount whose own journal replay gives up waiting for another node's lock retries every 1 to 30 seconds and logs who holds the lock each time. Any other replay failure, such as a record that does not decode or an I/O error, fails the mount at once with the `-skip-recovery` hint.

**The hazard: a stalled node keeps writing.** If a node stops for about 20 seconds while it holds a lock, other nodes prove it dead, replay its journal and take its locks. When the stalled node resumes, it exits on its next heartbeat, up to 1 second later, but it can finish the writes it had started. No write path checks that the lock is still held, and there is no fencing token. Two nodes then write the same metadata, which can double-allocate blocks or lose directory entries.

Common causes of a stall:

- VM pause, suspend, live migration, or a memory snapshot
- `docker pause`, cgroup freezer, `SIGSTOP`, or a debugger on the mount process
- Heavy swapping or a long GC pause
- Storage I/O that queues instead of failing, for example a multipath or iSCSI path failover

Lock slot reads and writes do not take the device's per-region I/O lock, so a node's own data and journal writes to the same region cannot delay them. The lock manager serializes each record's I/O itself. A release whose write fails leaves the claim in place until the node unmounts or is replayed.

**Nodes keep released locks briefly.** After its last user releases a lock, a node keeps it for 10 ms (`CacheGrace`), so back-to-back work on the same node does not write and read the record again. A node reuses a lock it claimed for at most 250 ms (`HoldTurn`). After that, new work on that node waits up to another 250 ms for the current holders to finish. The node then releases the lock and waits 50 ms (`YieldGap`) before taking it again, so another node gets a turn. Another node that wants the lock waits up to about 35 ms while the holder is idle, and up to about 300 ms while the holder keeps using it. A clean unmount releases kept locks at once. A crashed node's last locks, kept or held, stay held until another node proves it dead and replays its journal (see [Node Requirements](#node-requirements)). A lock wait gives up after 30 seconds (`DefaultTimeout`) while the holder is alive. A wait for a failed holder has no time limit: it logs a warning that names the node, and it ends when the replay does. It fails with `EIO` only if the replay fails or is abandoned, which shows as a lapsed recovery lease, with one exception. An operation that already holds a lock and then waits for a failed node's lock keeps the 30-second limit and fails with `EIO`, because the replay may need the lock it holds. An overwrite of a file's existing data does not: it lets go of its locks at once, waits for the block's lock holding none, and retries, so it does not fail. Operations that already changed something first, such as a create or a write that allocated blocks, keep the 30-second limit. For example, a node holds a directory's lock and waits for a block lock the dead node held, while the dead node's journal changes that directory. The replay's own waits have no limit, so it finishes once the operation gives up. The warning for this case names the record and the dead node.

**Writes to one region from two nodes do not mix within a block.** A node takes a block's lock for every write to an existing block, so two writes to the same block land one after the other. A write that spans several blocks takes each block's lock in turn, so if two nodes write overlapping multi-block ranges of a file at the same time, the result can mix both writes, one block at a time. Applications that write the same region from several nodes must coordinate those writes themselves.

**Locks of one type share records.** Each lock type has its own records of the 256: 128 for inodes, 97 for block extents, 16 for inode table sectors, 8 for SFB containers, and one for each other type. Two files' inode locks can map to one record, and a node holding one of them blocks other nodes from both. A lock never waits on a record another type uses. A node takes locks in type order and, within a type, in record order, so two nodes cannot each wait for a record the other holds. The exception is a replay of a failed node's journal: the replaying node keeps the failed node's records until the replay ends, so a node that holds a lock the replay needs and waits on one of those records gives up after 30 seconds with `EIO`. The replay's waits have no limit, so it then finishes.

**Safe operating limits:**

| Limit | Why |
|-------|-----|
| Keep node clocks within 1 second (chrony or NTP) | Nodes judge each other's heartbeats by wall clock |
| No node may stall for 15 seconds or more while mounted | Peers prove a node dead after about 20 seconds of silence and take its locks |
| Storage I/O must fail, not queue, within 10 seconds | A queued write that lands after takeover corrupts metadata. Do not use multipath `queue_if_no_path`; lower iSCSI `node.session.timeo.replacement_timeout` (default 120s) |
| Unmount before you pause, migrate or snapshot a node VM | Unmounting releases its locks |

If a node did stall for 15 seconds or more while mounted (hung-task warnings in `dmesg`, a VM pause, a path failover), stop its mount process before it does more writes, then check the data from a healthy node.

What is safe today: a node that crashes or is killed and stays down, and a clean unmount. Other nodes take over a crashed node's locks as described under [Node Requirements](#node-requirements).

### Handling Node Failure

When a node fails unexpectedly:

1. **Other nodes keep running.** Operations that need one of the failed node's locks wait until the lock is taken over.
2. **Locks and journal.** Once the failed node is proven dead, one surviving node takes over its exclusive locks, replays its journal, then frees the locks. Until then, operations that need its locks wait, however long that takes. See [Node Requirements](#node-requirements).
3. **No action is needed** on the surviving nodes.

**When the failed node comes back**, mount it again with the same options. If it gets its old registry slot back (same hostname and boot ID), it replays its own journal before serving. Don't run fsck while other nodes have the device mounted: `-repair` refuses, and a check-only run reads metadata that is changing.

```bash
goclusterfs-mount -device /dev/device -mount /mnt/cluster
```

### Damaged Journal Slot

A mount exits with `Journal recovery for slot N failed: ...` when it cannot replay the journal in the slot it took. Read the rest of the error first:

- **Storage or replay errors** (an I/O error, or `journal recovery failed: ...`): the journal stays in recovery. Fix the storage and mount again without `-skip-recovery`; the replay resumes.
- **A damaged journal header** (`journal superblock checksum mismatch`, `invalid journal magic`, or `journal is corrupt`): the journal cannot be replayed. Only then use `-skip-recovery=N`, with the slot number N from the error, which discards slot N's journal.

`-skip-recovery=N` discards a journal only when the mount takes slot N. If it takes another slot, it logs `ignoring -skip-recovery=N` and replays that slot's journal as usual, so it never discards a journal you did not name. A mount takes another slot while a running node is replaying slot N: about 20 seconds after the failed mount, a running node tries to replay the damaged slot, holds it for 60 seconds, and tries again every minute. To discard slot N's journal:

1. Either mount right after the failed mount, within about 20 seconds, or first unmount on every other node and wait one minute.
2. On the same host, before it reboots, mount once with `-skip-recovery=N`. The mount logs `discarding the journal of slot N`.
3. Unmount on every node and run fsck, because discarded transactions may have been only partly applied.

`-skip-recovery=N` does not stop the node from replaying crashed peers' journals. `-journal=false` also skips the replay of its own slot, and leaves that journal in place for the next mount, so don't use it on a device that may have a dirty journal.

A slot belongs to its host's hostname and boot ID. If the damaged slot belongs to a node that has rebooted or will not mount again, the other nodes log `Replaying slot N ... failed` and retry every minute. Between retries, a new mount on any host can take that slot and then fail with the same error. Free the slot with fsck. Unmount the filesystem on every node first, then:

```bash
./goclusterfs-fsck -list-slots /dev/device                       # find the slot and its journal state
./goclusterfs-fsck -release-slot N /dev/device                   # check the slot; prints what releasing discards
./goclusterfs-fsck -release-slot N -discard-journal /dev/device  # reset its journal and free the slot
```

`-release-slot` refuses while any node has the filesystem mounted, and until a minute after the slot's lease ended: its last heartbeat plus 10 seconds, or for a slot another node was replaying, that node's last lease renewal plus 60 seconds. Without `-force` it also refuses a journal that a mount might still replay: one with committed transactions that fsck reads in full, or one fsck cannot read to the end. Mount a node to replay it instead, and add `-force` only if that replay fails. With `-discard-journal`, fsck holds the slot's recovery lease while it resets the journal, frees the slot, and exits 1. The next node to mount can then use the slot. Run fsck afterwards, because discarded transactions may have been only partly applied.

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

`-repair` fixes refcounts and the superblock checksum, and frees leaked blocks and inodes no directory names. It never edits directories, and there is no `lost+found`. Restore anything else from backup. See the [fsck Guide](TROUBLESHOOTING.md#fsck-guide).

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

Separately, each node sends a broadcast heartbeat over UDP every 30 seconds, an invalidation message for each metadata change, and a heartbeat every 50 ms for 500 ms after its last invalidation:

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

**Broadcast cache invalidation.** Nodes send UDP messages (multicast group `239.192.74.75`, port `31337` by default, unicast on `31338`) that tell peers to drop cached inodes and extents. Heartbeat and announce messages add unicast peers, which a node uses when multicast sends fail. Messages carry inode numbers but no file data. Each node numbers its messages, and a node that sees a gap in a peer's numbers drops all cached inodes and extents. The heartbeats after a node's last invalidation let peers notice a lost final message too. Only when those heartbeats are lost as well do cache updates wait for `-cache-ttl`.

- With an empty `-broadcast-secret` (the default), messages are not authenticated. Any host that can send to the port can make nodes drop their caches, and can add any address as a unicast peer. Mount logs a `WARNING` line at startup in this case.
- With `-broadcast-secret`, every message carries an HMAC-SHA256 tag. Nodes drop messages with a missing or wrong tag, so all nodes need the same secret.
- The only replay protection is a sequence number check that forgets messages after 5 minutes, so a recorded message can be sent again later, even with a secret.
- Messages are never encrypted. Any host that joins the multicast group can see which inodes change.
- The secret is a command-line flag, so local users on a node can read it with `ps`.

**HTTP API.** Each mount serves an HTTP API on `127.0.0.1:8080` (`-api-addr`), so only local users can reach it. Without a token it only reads status and statistics. With a token (`-api-token-file` or `GOCLUSTERFS_API_TOKEN`) it also lists, creates, clones and deletes snapshots, clones files and runs admin tasks; see [HTTP API Write Endpoints](API-CONFIG.md#http-api-write-endpoints). Anyone with the token can change any file's snapshots and create files in the root directory, so treat it like the device itself.

- To expose it, set `-api-addr` and a token. Mount refuses a non-loopback address without a token.
- `/health` and `/metrics` never need the token, so anyone who can reach the port can read them.
- Without a token, the API rejects requests whose `Host` header is not a loopback name, and mount refuses `-api-cors '*'`. Both stop web pages that a local user opens from reading the API.
- The API uses plain HTTP. Put a TLS proxy in front of it, or use an SSH tunnel, if the token crosses a network.
- Keep the token in a 0600 file (`-api-token-file`) or `GOCLUSTERFS_API_TOKEN`. `-api-token` shows it to every local user in `ps`, so mount logs a `WARNING` for it.

**Network listeners.** Each node listens on UDP port 31337 (multicast group) and UDP port 31338 (unicast) on all interfaces, and on TCP `-api-addr`. A second mount on the same host needs its own `-unicast-port` and `-api-addr`; the mount fails at startup if a port is taken. Clusters that share a LAN need their own `-mcast-group` or a `-broadcast-secret`, or they receive each other's invalidations. Limit the UDP ports, and the API port if you expose it, to the cluster nodes and your monitoring hosts with a firewall.

---

## Next Steps

- [Troubleshooting](TROUBLESHOOTING.md) - Problem diagnosis
- [Configuration Reference](API-CONFIG.md) - All configuration options
