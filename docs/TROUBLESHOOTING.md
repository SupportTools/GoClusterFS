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

**Cause**: The storage backend doesn't support O_DIRECT (e.g., tmpfs, some network filesystems).

**Solution**:
```bash
# Verify O_DIRECT support
dd if=/dev/zero of=/path/to/device bs=4k count=1 oflag=direct

# If this fails, use a different storage location
# O_DIRECT is supported on: ext4, xfs, block devices, NFS (v4.2+)
# O_DIRECT is NOT supported on: tmpfs, older NFS, some FUSE filesystems
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
fusermount -uz /mnt/existing_mount
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
sudo ./goclusterfs ...

# Check device permissions
ls -la /dev/device
# May need to add udev rule for persistent permissions
```

### "transport endpoint not connected"

**Symptom**: Mount point exists but operations fail with this error.

**Cause**: Previous mount crashed without clean unmount.

**Solution**:
```bash
# Force unmount the stale mount point
fusermount -uz /mnt/goclusterfs

# If that fails
sudo umount -l /mnt/goclusterfs

# Then remount
./goclusterfs -device /dev/device -mount /mnt/goclusterfs
```

### "bad superblock" or "invalid magic"

**Symptom**: Mount fails because filesystem metadata is invalid.

**Cause**: Device not formatted, wrong device, or corruption.

**Solution**:
```bash
# Verify correct device
lsblk
fdisk -l

# Check if formatted
hexdump -C /dev/device | head -20
# Look for GoClusterFS magic bytes

# If not formatted
./goclusterfs -format -device /dev/device

# If corrupted, try fsck
./goclusterfs-fsck -repair /dev/device
```

### "device has the pre-v2 layout"

**Symptom**: Mount exits with `Cannot use <device>: device has the pre-v2 layout, where all nodes share one journal`.

**Cause**: An older GoClusterFS build formatted the device. In that layout every node wrote the same journal region and data blocks started at 11 MB, so this version cannot read it safely.

**Solution**: Copy the data off with the old build, reformat with `goclusterfs-mount -format` (this destroys the data), and copy the data back. There is no in-place migration.

---

## Error Messages

### "lock acquisition failed" / "lock timeout"

**Symptom**: Operations fail with lock errors.

**Causes**:
1. Another node holding the lock
2. Deadlock situation
3. Crashed node left stale lock

**Solutions**:

```bash
# Check which nodes are active
tcpdump -i eth0 udp port 31337 -c 20

# If node crashed while holding lock:
# Wait for heartbeat timeout (default 30s)
# Lock will be automatically released

# If deadlock suspected:
# Identify the lock holder from debug logs
./goclusterfs -debug ...

# Check for stuck processes on other nodes
ps aux | grep goclusterfs
```

### "journal recovery required"

**Symptom**: Mount fails, requires journal recovery.

**Cause**: Previous mount crashed with uncommitted journal entries.

**Solution**:
```bash
# Allow automatic recovery (default)
./goclusterfs -device /dev/device -mount /mnt/fs -journal

# If recovery fails, skip it (may lose recent data)
./goclusterfs -device /dev/device -mount /mnt/fs -journal -skip-recovery

# Then run fsck
fusermount -u /mnt/fs
./goclusterfs-fsck -repair /dev/device
```

### "stale file handle"

**Symptom**: File operations fail with "stale file handle" error.

**Cause**: File was deleted/modified by another node while handle was open.

**Solutions**:
```bash
# Application should close and reopen the file
# This is expected behavior in clustered environments

# If happening frequently, enable stricter caching:
./goclusterfs ... -cache-strict

# Or reduce cache TTL
./goclusterfs ... -cache-ttl 1s
```

### "no space left on device"

**Symptom**: Writes fail despite apparent free space.

**Causes**:
1. Filesystem genuinely full
2. Inode exhaustion
3. Journal full

**Solutions**:
```bash
# Check space
df -h /mnt/fs
df -i /mnt/fs  # Check inodes

# If journal is full, force checkpoint
# (This happens automatically at 75% full)

# If thin provisioning, check underlying storage
# Overcommitment may have caught up

# Clean up deleted files
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

# Run fsck
fusermount -u /mnt/fs
./goclusterfs-fsck -repair /dev/device
```

### "invalid extent" / "block out of range"

**Symptom**: Operations fail with extent-related errors.

**Cause**: Filesystem metadata corruption.

**Solution**:
```bash
# Unmount
fusermount -u /mnt/fs

# Run fsck with repair
./goclusterfs-fsck -repair /dev/device

# If fsck fails, restore from backup
```

---

## Performance Issues

### High Cache Miss Rate

**Symptom**: Operations slower than expected, cache hit rate below 50%.

**Diagnosis**:
```bash
# Check cache stats (if exposed via debug interface)
# Look for InodeCacheHitRate, ExtentCacheHitRate
```

**Solutions**:
```bash
# Increase cache sizes (via configuration)
# Default InodeCacheSize: 10000
# Default ExtentCacheSize: 50000

# For larger working sets, increase proportionally

# If random access pattern, cache won't help much
# Consider adjusting application access patterns
```

### High Lock Contention

**Symptom**: Metadata operations slow, high retry counts.

**Diagnosis**:
```bash
# Enable debug logging
./goclusterfs -debug ...

# Look for lock acquisition retries
grep "lock" /var/log/goclusterfs.log
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
# If journal commit is slow, consider batch mode
# (Configure via code: CommitBatch mode)

# If data write is slow, check storage performance
dd if=/dev/zero of=/mnt/fs/test bs=1M count=100 oflag=direct

# If network invalidation is slow
# Check multicast latency
ping -c 10 239.192.74.75  # (multicast ping may not work)
```

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
watch -n 60 'ps aux | grep goclusterfs'

# If leak suspected, capture heap profile
# (Requires pprof endpoint enabled)
```

---

## Cluster Issues

### Nodes Not Seeing Updates

**Symptom**: File changes on one node not visible on others.

**Causes**:
1. Broadcast not working
2. Cache TTL not expired
3. Network partition

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
./goclusterfs ... -broadcast-iface eth0

# If packets visible but not applied:
# Check for secret mismatch
# All nodes must use same -broadcast-secret
```

### Split-Brain Scenario

**Symptom**: Different nodes have different versions of same file.

**Cause**: Network partition caused nodes to write independently.

**Solution**:
```bash
# This is a serious situation requiring manual intervention

# Step 1: Stop all nodes
# On each node:
fusermount -u /mnt/cluster

# Step 2: Run fsck
./goclusterfs-fsck -repair /dev/device

# Step 3: Identify correct data
# Compare file versions, timestamps
# Manual decision required

# Step 4: Restart cluster
# Start one node first, verify data
# Then start remaining nodes
```

### Heartbeat Timeouts

**Symptom**: Logs show heartbeat timeout warnings.

**Causes**:
1. Node failed
2. Network congestion
3. Process overloaded

**Solutions**:
```bash
# Check if node is actually down
ping node2
ssh node2 "systemctl status goclusterfs"

# If node up but heartbeats timing out:
# Check network latency
ping -c 100 node2 | tail -5

# Check for packet loss
ping -c 1000 -f node2

# Increase heartbeat interval if network is slow
# (Configure via code: HeartbeatInterval)
```

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
fusermount -u /mnt/cluster

# Step 2: Run fsck on one node
./goclusterfs-fsck -repair /dev/device

# Step 3: Review fsck output
# Note which files were affected

# Step 4: Mount read-only first (if supported)
# Verify critical data before write access

# Step 5: Mount normally
./goclusterfs -device /dev/device -mount /mnt/cluster

# Step 6: Restore affected files from backup if needed
```

### Preventing Corruption

1. **Always unmount cleanly** before shutting down
2. **Use journaling** (`-journal` flag)
3. **Enable broadcast** for cache coherency
4. **Run periodic fsck** during maintenance windows
5. **Maintain backups** - no filesystem is immune to hardware failure

---

## Debug Mode

### Enabling Debug Output

```bash
./goclusterfs -device /dev/device -mount /mnt/fs -debug
```

### What Debug Mode Shows

- FUSE operation details (open, read, write, etc.)
- Lock acquisition/release events
- Cache hits/misses
- Journal commits
- Network broadcast messages

### Analyzing Debug Output

```bash
# Filter for specific operation types
./goclusterfs -debug 2>&1 | grep -E "(WRITE|READ|LOCK)"

# Watch lock activity
./goclusterfs -debug 2>&1 | grep -i lock

# Monitor cache behavior
./goclusterfs -debug 2>&1 | grep -i cache
```

### FUSE Debug (Very Verbose)

For low-level FUSE debugging:

```bash
# Mount with FUSE debug (extremely verbose)
./goclusterfs -device /dev/device -mount /mnt/fs -debug -fuse-debug 2>&1 | tee fuse.log
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
2. **Salvage readable data**:
   ```bash
   # Mount read-only if possible
   # Copy readable files to new location
   ```
3. **Reformat** (last resort, data loss):
   ```bash
   ./goclusterfs -format -device /dev/device
   ```

---

## Getting Help

If this guide doesn't resolve your issue:

1. **Collect diagnostics**:
   ```bash
   # System info
   uname -a
   cat /etc/os-release

   # GoClusterFS version
   ./goclusterfs -version

   # Recent logs
   ./goclusterfs -debug ... 2>&1 | head -1000 > debug.log

   # fsck output
   ./goclusterfs-fsck /dev/device > fsck.log 2>&1
   ```

2. **Report issue**: https://github.com/supporttools/GoClusterFS/issues
