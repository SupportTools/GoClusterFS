# GoClusterFS Troubleshooting Guide

This guide helps diagnose and resolve common issues with GoClusterFS.

## Table of Contents

1. [Mount Failures](#mount-failures)
2. [Error Messages](#error-messages)
3. [Performance Issues](#performance-issues)
4. [Cluster Issues](#cluster-issues)
5. [Data Corruption](#data-corruption)
6. [Debug Mode](#debug-mode)
7. [fsck Guide](#fsck-guide)

---

## Mount Failures

### "open: invalid argument" or O_DIRECT Failure

**Symptom**: Mount fails immediately with permission or I/O errors.

**Cause**: The storage backend doesn't support O_DIRECT, for example tmpfs before Linux 6.6 or some FUSE and network filesystems.

**Solution**:
```bash
# Verify O_DIRECT support; conv=notrunc keeps an image file's size
dd if=/dev/zero of=/path/to/device bs=4k count=1 oflag=direct conv=notrunc

# If this fails, put the device or image somewhere else, for example ext4 or XFS
```

### "device or resource busy"

**Symptom**: Mount fails because device is in use.

**Cause**: Device already mounted or held by another process.

**Solution**:
```bash
# Check what's using the device
lsof /dev/device
fuser -v /dev/device

# Check existing mounts
mount | grep device

# Force unmount if needed (data loss possible)
fusermount3 -uz /mnt/existing_mount
```

### "permission denied"

**Symptom**: Mount fails due to permission issues.

**Cause**: User lacks permissions for FUSE or device.

**Solution**:
```bash
# Add user to fuse group
sudo usermod -aG fuse $USER
newgrp fuse

# Or run as root
sudo ./goclusterfs-mount ...

# Check device permissions
ls -la /dev/device
# May need to add udev rule for persistent permissions
```

### "transport endpoint not connected"

**Symptom**: Mount point exists but operations fail with this error.

**Cause**: The mount process exited without unmounting. It was killed, crashed, or stopped itself: a mount exits on purpose when it cannot write its heartbeat or another node took its registry slot. Check its log for `Node registry slot N is no longer this node's` or `heartbeats to node registry slot N have failed`.

**Solution**:
```bash
# Force unmount the stale mount point
fusermount3 -uz /mnt/goclusterfs

# If that fails
sudo umount -l /mnt/goclusterfs

# Then remount; it replays the journal it left behind
./goclusterfs-mount -device /dev/device -mount /mnt/goclusterfs
```

### "no GoClusterFS superblock" or "invalid superblock magic"

**Symptom**: Mount exits with `Cannot use <device>: no GoClusterFS superblock; format the device with -format`, or a superblock error such as a checksum mismatch.

**Cause**: Device not formatted, wrong device, or a damaged superblock.

**Solution**:
```bash
# Verify correct device
lsblk

# A formatted device starts with "GOCLUSTERFS"
head -c 16 /dev/device | hexdump -C

# Only if the device is new or its data can go: format it (destroys all data)
./goclusterfs-mount -device /dev/device -mount /mnt/goclusterfs -format

# A superblock checksum error can be repaired
./goclusterfs-fsck -repair /dev/device
```

### "device has the pre-v2 layout"

**Symptom**: Mount exits with `Cannot use <device>: device has the pre-v2 layout, where all nodes share one journal`.

**Cause**: An older GoClusterFS build formatted the device. In that layout every node wrote the same journal region and data blocks started at 11 MB, so this version cannot read it safely.

**Solution**: Copy the data off with the old build, reformat with `goclusterfs-mount -format` (this destroys the data), and copy the data back. There is no in-place migration.

---

## Error Messages

### "lock acquisition timed out"

**Symptom**: Operations fail, and the mount log shows `lock acquisition timed out` (a lock wait passed 30 seconds).

**Causes**:
1. Another node holds the lock for a long operation, or is stalled
2. A crashed node still holds it

**Solutions**:

```bash
# Look at lock waits in the mount log
grep "lock.Acquire" /var/log/goclusterfs.log | tail -20

# Check for stuck or stopped mount processes on other nodes
ps aux | grep goclusterfs-mount
```

A crashed node's locks free up on their own: other nodes take them over once they are 15 seconds old, or once the node is proven dead. A stalled node is dangerous; see [Distributed Lock Limits](OPERATIONS.md#distributed-lock-limits).

### "Journal recovery for slot N failed"

**Symptom**: Mount exits with `Journal recovery for slot N failed: ... (mount with -skip-recovery to discard this journal)`.

**Cause**: The journal in this node's registry slot is damaged, so its transactions cannot be replayed. A normal crash does not cause this: the next mount on the same host replays the journal by itself.

**Solution**:
```bash
# Discard this slot's journal and start a new one (may lose recent changes)
./goclusterfs-mount -device /dev/device -mount /mnt/fs -skip-recovery

# Then unmount on every node and check
fusermount3 -u /mnt/fs
./goclusterfs-fsck /dev/device
```

See [Damaged Journal Slot](OPERATIONS.md#damaged-journal-slot).

### Another Node's Change Shows Up Late

**Symptom**: A file changed or deleted on one node still looks unchanged on another for a short time.

**Cause**: Caching. `stat` and `ls` can lag up to `-attr-timeout` (1s by default), and open files up to `-cache-ttl` (5s by default).

**Solutions**:
```bash
# Shorter windows, at a cost in speed
./goclusterfs-mount ... -cache-strict -attr-timeout 0
```

### "no space left on device"

**Symptom**: Writes fail despite apparent free space.

**Causes**:
1. Out of 1 MB blocks. Every file with data uses at least one full block, so many small files fill the device fast.
2. Out of inodes. A volume has 4096.
3. The underlying thin-provisioned storage is full.

**Solutions**:
```bash
# Check blocks and inodes
df -h /mnt/fs
df -i /mnt/fs

# Check for deleted-but-open files
lsof +D /mnt/fs | grep deleted
```

### "input/output error"

**Symptom**: Read/write operations fail with I/O error.

**Causes**:
1. Underlying storage failure
2. Network issue (for remote storage)
3. Filesystem corruption

**Solutions**:
```bash
# Check kernel logs
dmesg | tail -50

# Check storage health
smartctl -a /dev/sda  # For local drives

# For iSCSI/network storage
iscsiadm -m session -P 3  # Check session state

# Run fsck after unmounting on every node
fusermount3 -u /mnt/fs
./goclusterfs-fsck /dev/device
```

### "invalid extent" / "block out of range"

**Symptom**: Operations fail with extent-related errors.

**Cause**: Filesystem metadata corruption.

**Solution**:
```bash
# Unmount on every node
fusermount3 -u /mnt/fs

# See what fsck finds
./goclusterfs-fsck /dev/device
```

`-repair` does not fix bad extents, so restore the affected files from backup.

---

## Performance Issues

### High Cache Miss Rate

**Symptom**: Operations slower than expected.

**Diagnosis**: The mount logs `Cache stats: inodes=... (hit=...%), extents=... (hit=...%)` when it unmounts. There is no live view, and the hit percentages are wrong today (they can read far above 100%).

**Solutions**: Cache sizes are fixed in code (10,000 inodes, 50,000 extents). A longer `-cache-ttl` or `-attr-timeout` avoids some disk reads, but other nodes' changes then show up later. See [Cache Tuning](OPERATIONS.md#cache-tuning).

### High Lock Contention

**Symptom**: Metadata operations slow, high retry counts.

**Diagnosis**:
```bash
# Lock waits are always logged
grep "lock.Acquire" /var/log/goclusterfs.log
```

**Solutions**:
- Partition workload: Different files on different nodes
- Reduce concurrent access to same files
- Use read-only access where possible

### Slow Writes

**Symptom**: Write throughput below expected.

**Diagnosis**:
Check which phase is slow:
1. Lock acquisition
2. Data write
3. Journal commit
4. Cache invalidation

**Solutions**:
```bash
# Check raw storage speed outside GoClusterFS (destroys data on the target)
dd if=/dev/zero of=/path/on/same/storage/test bs=1M count=100 oflag=direct
```

Journal commits are synchronous, and each block write takes a cluster lock. Batch commit mode exists in code but has no flag.

### Memory Usage Growth

**Symptom**: GoClusterFS process memory grows over time.

**Causes**:
1. Large cache (expected)
2. Memory leak (bug)

**Solutions**:
```bash
# Check if cache is the cause
# Reduce cache sizes to test

# Monitor memory over time
watch -n 60 'ps aux | grep goclusterfs-mount'
```

There is no profiling endpoint.

---

## Cluster Issues

### Nodes Not Seeing Updates

**Symptom**: File changes on one node not visible on others.

**Causes**:
1. Caching: changes can show up to `-attr-timeout` (1s) or `-cache-ttl` (5s) late, even with broadcast working
2. Broadcast not working, so caches only catch up after `-cache-ttl`

**Solutions**:
```bash
# Verify multicast working
# On receiver node:
tcpdump -i eth0 udp port 31337

# On sender node:
# Modify a file and watch for packets

# If no packets, check:
# 1. Firewall rules
iptables -L -n | grep 31337

# 2. Multicast routing
ip maddr show

# 3. Network interface
./goclusterfs-mount ... -broadcast-iface eth0

# If packets visible but not applied:
# Check for secret mismatch
# All nodes must use same -broadcast-secret
```

### Nodes Disagree After a Network Partition

Nodes coordinate through the shared device, not the network, so a network partition alone does not let them write independently. It only stops cache invalidations, and each node catches up after `-cache-ttl`.

Two nodes can write the same metadata when a node stalls (VM pause, storage path failover) while it holds a lock, or when two nodes share a registry slot. See [Distributed Lock Limits](OPERATIONS.md#distributed-lock-limits) and [Known Gaps in Slot Ownership](OPERATIONS.md#known-gaps-in-slot-ownership). If you suspect this:

```bash
# Step 1: Unmount on every node
fusermount3 -u /mnt/cluster

# Step 2: Check
./goclusterfs-fsck /dev/device

# Step 3: Compare and restore affected files from backup, then start one node first
```

### Heartbeat Failures

**Symptom**: A mount stops with `heartbeats to node registry slot N have failed`, or `Node registry slot N is no longer this node's`.

**Cause**: Node heartbeats are writes to the shared device every second, not network messages. A node stops when its heartbeat writes fail for 5 seconds, or when another node took its slot after it looked dead (heartbeat older than 10 seconds to a new mount, about 20 seconds to running nodes).

**Solutions**:
```bash
# Check the storage path on that node
dmesg | tail -50
multipath -ll
iscsiadm -m session -P 3

# Check its clock against the other nodes
chronyc tracking
```

See [Node Requirements](OPERATIONS.md#node-requirements).

---

## Data Corruption

### Detecting Corruption

Signs of corruption:
- fsck reports errors
- Unexpected "input/output error"
- Missing or truncated files
- Incorrect file contents

### Recovery Process

```bash
# Step 1: Unmount all nodes immediately
# On each node:
fusermount3 -u /mnt/cluster

# Step 2: Back up the device, then check it on one node
./goclusterfs-fsck -repair -dry-run /dev/device

# Step 3: Repair what fsck can fix
./goclusterfs-fsck -repair /dev/device

# Step 4: Mount on one node (there is no read-only mount) and check critical data
./goclusterfs-mount -device /dev/device -mount /mnt/cluster

# Step 5: Restore affected files from backup if needed
```

### Preventing Corruption

1. **Always unmount cleanly** before shutting down
2. **Keep journaling on** (`-journal`, on by default)
3. **Keep node clocks in sync and avoid stalls** (see [Distributed Lock Limits](OPERATIONS.md#distributed-lock-limits))
4. **Run periodic fsck** during maintenance windows
5. **Maintain backups** - no filesystem is immune to hardware failure

---

## Debug Mode

### Log Output

The mount always logs `[DEBUG]` lines for file operations, lock acquire and release, journal recovery, and broadcast. Capture stderr and filter it:

```bash
./goclusterfs-mount -device /dev/device -mount /mnt/fs 2>&1 | tee mount.log

grep "lock.Acquire" mount.log
grep "\[broadcast\]" mount.log
grep "\[Recovery\]" mount.log
```

### FUSE Request Tracing (Very Verbose)

`-debug` adds a log line for every FUSE request and reply:

```bash
./goclusterfs-mount -device /dev/device -mount /mnt/fs -debug 2>&1 | tee fuse.log
```

---

## fsck Guide

`goclusterfs-fsck` reads every on-disk structure and reports what is wrong. It writes nothing unless you pass `-repair`.

```bash
# Check only
./goclusterfs-fsck /dev/device

# List what -repair would change
./goclusterfs-fsck -repair -dry-run /dev/device

# Repair
./goclusterfs-fsck -repair /dev/device
```

### Before `-repair`

1. Unmount the device on every node.
2. Back up the device, for example with `dd`.
3. Run `-repair -dry-run` and read the problems marked `[WOULD FIX]`.

`-repair` refuses to run, with exit code 8, in two cases:

- **A node has the device mounted.** fsck watches the heartbeat counter in each node slot. The mount binary leaves its slot behind when it stops, so right after an unmount fsck can wait up to 11 seconds before it knows the node is gone.
- **A node stopped without replaying its journal.** Mount the device on one node, which replays its own journal and recovers the journals of crashed nodes, then unmount it and run fsck again. `-force` repairs anyway, but changes still in that journal can then be lost or undone.

If a node mounts the device while `-repair` runs, repair stops before its next write and exits 8. `-repair -dry-run` reports both cases as warnings.

### What `-repair` Changes

- It rebuilds block refcounts from the extents of every inode, snapshot and clone, including extent trees. It skips this when it could not read an inode or extent tree, because counts that miss an owner let the runtime free a block another file still uses.
- It marks the blocks that older versions shared without counting as counted, so this version can snapshot, clone and free those files.
- It rewrites refcount sectors that fail their checksum from the same references.
- It fixes the superblock checksum, but only when every other superblock field passed its checks.

It never frees blocks, deletes inodes, or edits directories. Problems not marked `[FIXABLE]` stay as they are.

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | No errors. Warnings may remain. |
| 1 | Errors were repaired. |
| 4 | Errors remain on disk. A dry run that would fix them also exits 4. |
| 8 | I/O failure, fsck could not finish, or `-repair` was refused. |
| 16 | Usage error. |

As with fsck(8), the codes add up: a repair that fixed some errors and left others exits 5.

### fsck Phases

| Phase | Name | Checks |
|-------|------|--------|
| 1 | Superblock | Magic, version, block and inode size, checksum, and the format v2 layout (below). fsck stops here when a region lies outside the device or overlaps another. |
| 2 | Bitmap | Sector checksums; counts allocated blocks |
| 3 | Inodes | Inode checksums, file type, link count, block counts; reads extent trees |
| 4 | Extents | Extents stay inside the device and are allocated; two inodes share a block only if both mark it shared; extent tree structure |
| 5 | Refcounts | Sector checksums; each block's refcount covers the inodes that map it |
| 6 | Directories | Entry lengths and names; entries point at inodes in use |
| 7 | Snapshots | Registry sector and descriptor checksums, duplicate IDs and names, snapshot data inodes |
| 8 | Cross-Validation | Allocated blocks that no inode maps, mapped blocks the bitmap calls free, inodes no directory names |

The format v2 layout checks in phase 1:

- Every region starts on a 4 KB boundary, lies inside the device, and does not overlap another region.
- The journal region splits evenly into one journal per node, and each journal is a multiple of 4 KB and at least 8 KB.
- The node registry has a slot for every node journal.

fsck does not compare the superblock free block, free inode, and snapshot counters, because the runtime never updates them.

### Reading fsck Output

Each problem is one line: severity, phase, location, message, and repair status.

```
[ERROR] Phase 5 (Refcounts): block 17: mapped by 2 inodes but refcount is 0: freeing one of them frees a block another still maps [FIXABLE]
```

- `[FIXABLE]`: `-repair` fixes it.
- `[WOULD FIX]`: a dry run found that `-repair` would fix it.
- `[FIXED]`: `-repair` fixed it.

### When fsck Cannot Repair

If fsck reports unrecoverable errors:

1. **Restore from backup** (preferred)
2. **Salvage readable data**: mount on one node and copy readable files somewhere else. There is no read-only mount.
3. **Reformat** (last resort, data loss):
   ```bash
   ./goclusterfs-mount -device /dev/device -mount /mnt/goclusterfs -format
   ```

---

## Getting Help

If this guide doesn't resolve your issue:

1. **Collect diagnostics**:
   ```bash
   # System info
   uname -a
   cat /etc/os-release

   # GoClusterFS version (goclusterfs-mount has no -version flag yet)
   ./goclusterfs-fsck -version

   # Recent mount log
   tail -1000 /var/log/goclusterfs.log > mount.log

   # fsck output
   ./goclusterfs-fsck /dev/device > fsck.log 2>&1
   ```

2. **Report issue**: https://github.com/supporttools/GoClusterFS/issues
