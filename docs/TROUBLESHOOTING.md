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

### "no GoClusterFS superblock" or "superblock checksum mismatch"

**Symptom**: Mount exits with `Cannot use <device>: no GoClusterFS superblock; format the device with -format`, or `Cannot use <device>: superblock checksum mismatch - possible corruption`. (fsck reports a missing superblock as `invalid superblock magic`.)

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

### "device has an inode table but no GoClusterFS superblock"

**Symptom**: Mount exits with `Cannot use <device>: device has an inode table but no GoClusterFS superblock: it has the pre-v2 layout, where all nodes share one journal, or its superblock was destroyed. ...`

**Cause**: One of two things, and you need to know which build formatted the device:

- **A build from before format version 2 formatted it.** In that layout every node wrote the same journal region and data blocks started at 11 MB, so this version cannot read it safely.
- **A version 2 build formatted it, and its superblock was overwritten.** Versions 1 and 2 both keep the inode table at 2 MB, so the device looks old. Version 3 keeps lock records there; see the next entry.

**Solution**:
- Pre-v2 device: copy the data off with the old build, reformat with `goclusterfs-mount -format` (this destroys the data), and copy the data back. There is no in-place migration.
- Version 2 device: do not run an old build on it; it would read the wrong layout. There is no tool to rebuild the superblock, so restore from backup.

### "superblock version too old" or "formatted before each lock type had its own lock records"

**Symptom**: Mount exits with one of:

- `Cannot use <device>: superblock version too old: volume is v2, this version mounts v3. ...`
- `Cannot use <device>: superblock layout is not supported by this version: the volume was formatted before each lock type had its own lock records, so its nodes would not exclude this version's. ...`

**Cause**: An older build formatted the volume. Version 2 has fixed 1 MB regions. A version 3 volume formatted before [#85](https://github.com/SupportTools/GoClusterFS/pull/85) uses lock records that this version's nodes would not exclude.

**Solution**: Copy the data off with the build that formatted the volume, reformat with `goclusterfs-mount -format`, and copy the data back. For a version 2 volume over 128 GB, run that build's `goclusterfs-fsck` first. There is no in-place migration.

### "unknown required flags 0x10"

**Symptom**: Mount exits with `Cannot use <device>: superblock requires features this version doesn't support: unknown required flags 0x10`.

**Cause**: This build is older than [#85](https://github.com/SupportTools/GoClusterFS/pull/85), and a newer build formatted the volume.

**Solution**: Upgrade this node. Every node must run the same version.

### "device has GoClusterFS node slots but no superblock"

**Symptom**: Mount exits with `Cannot use <device>: device has GoClusterFS node slots but no superblock: its superblock was overwritten. ...`

**Cause**: A version 3 device lost its superblock, the first 4 KB. The node registry at 1 MB still holds the slots of the nodes that mounted it, so the data is likely intact.

**Solution**: Do not run `-format`, which destroys the data. There is no tool to rebuild the superblock, so restore the device from backup. If you wiped the start of the device on purpose, `-format` is safe.

---

## Error Messages

### Operations Hang Waiting for a Lock

**Symptom**: File operations hang. A signal to the waiting program does not end the wait; `SIGKILL` does. The mount log shows one of:

- `[WARN] lock: gave up waiting ...` naming the holder: a live node held the lock for 30 seconds. The operation fails with EIO, or with ENOSPC if it was allocating blocks.
- `[WARN] lock: ... which holds it and is failing or being recovered; waiting until its journal replay ends`, every 30 seconds: the holder failed. The wait has no time limit and ends when another node has replayed the failed node's journal. An operation that already held another lock gives up after 30 seconds with EIO instead.

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

A crashed node's locks free up on their own once another node proves it dead and replays its journal: in about 3 to 6 seconds when a surviving mount shares its host and PID namespace, and up to about 35 seconds otherwise. If that replay fails, waits give up with `the journal replay of node ... failed or was abandoned`; see [Damaged Journal Slot](OPERATIONS.md#damaged-journal-slot). A stalled node is dangerous; see [Distributed Lock Limits](OPERATIONS.md#distributed-lock-limits).

### "Journal recovery for slot N failed"

**Symptom**: Mount exits with `Journal recovery for slot N failed: ... (to discard this journal, mount with -skip-recovery=N)`.

**Cause**: The mount could not replay the journal in the registry slot it took. A normal crash does not cause this: the next mount on the same host replays the journal by itself. Read the rest of the error:

- An I/O error, or `journal recovery failed: ...`: the storage failed or a transaction could not be applied. The journal stays in recovery. Fix the storage and mount again without `-skip-recovery`; the replay resumes.
- `journal superblock checksum mismatch`, `invalid journal magic`, or `journal is corrupt`: the journal header is damaged and cannot be replayed.

**Solution**: Only for a damaged header, discard the journal with `-skip-recovery=N`. The mount discards it only if it takes slot N again; follow the steps in [Damaged Journal Slot](OPERATIONS.md#damaged-journal-slot).

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
2. Out of inodes. Format gives the volume one inode per `-bytes-per-inode` bytes of device (default 1 MB).
3. The underlying thin-provisioned storage is full.
4. A lock wait for the block bitmap gave up while a write allocated blocks. The mount log shows `[WARN] lock: gave up waiting for type=6`. See [Operations Hang Waiting for a Lock](#operations-hang-waiting-for-a-lock).

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
4. A lock wait gave up after 30 seconds because other nodes kept the lock busy. The mount log shows `[WARN] lock: gave up waiting` and names the holder.

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

**Diagnosis**: `gfsctl stats cache` shows this node's cache hits and misses while it runs, and the mount logs `Cache stats: inodes=... (hit=...%), extents=... (hit=...%)` when it unmounts.

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

# Go runtime and process memory from the API
curl -s http://127.0.0.1:8080/metrics | grep -E '^(go_memstats_heap_alloc_bytes|process_resident_memory_bytes) '
```

There is no profiling endpoint.

---

## Cluster Issues

### Mount Fails: "Failed to start cache broadcast"

**Symptom**: The mount exits at startup with `Failed to start cache broadcast: ...`.

**Cause**: With `-broadcast` on (the default), the node must bind UDP `-mcast-port` (31337) and `-unicast-port` (31338). Another process holds one of them, the two flags are set to the same port, or `-broadcast-iface` names a missing interface.

**Solutions**:
```bash
# Find what holds the ports
ss -ulpn | grep -E ':3133[78]'

# Use other ports (every node must use the same -mcast-port)
./goclusterfs-mount ... -mcast-port 41337 -unicast-port 41338

# Or run without broadcast; peers then see changes after -cache-ttl
./goclusterfs-mount ... -broadcast=false
```

### HTTP API, gfsctl and Dashboard

| What you see | Cause and fix |
|--------------|---------------|
| Mount exits with `Refusing to start the API: ...` | `-api-addr` can be reached from other hosts, or `-api-cors` is `*`, and no token is set. Set `-api-token-file` or `GOCLUSTERFS_API_TOKEN`, use a loopback address, or pass `-api=false` |
| Mount exits with `Invalid API settings: -api-token-file ... readable by group or others` | `chmod 600` the token file. It must also belong to the mount's user or root, be a regular file, and hold 1 to 4096 bytes |
| Mount exits with `Invalid API settings: set only one of -api-token-file, GOCLUSTERFS_API_TOKEN and -api-token` | Give the token one way only |
| Mount logs `Warning: Failed to start API server` | The API port is taken, often by another mount on the same host. Give each mount its own `-api-addr`. The mount runs without the API until restarted |
| `gfsctl`: connection refused | The API is off (`-api=false`), failed to start, or listens on another address. Check `--host` and `--port` against `-api-addr` |
| 401 `missing authorization header` or `invalid token` | The mount has a token and the request's is missing or wrong. Set `GFSCTL_TOKEN`, or enter it on the dashboard's Settings page |
| 403 `host not allowed` | No token is set, and the request used a host name other than `localhost`, `127.0.0.1` or `[::1]` |
| 403 `snapshot and admin endpoints are disabled` | Write endpoints need a token, even on loopback |
| 429 | `-api-rate-limit` is set and this client sent too many requests. Wait for `Retry-After` |
| 503 from the stats endpoints | The mount has not fed statistics to the API yet. Try again in a second |
| `/ui/` says `Dashboard not built` | The binary came from `make build`. Rebuild with `make dashboard build`, or use a release archive |

### Nodes Not Seeing Updates

**Symptom**: File changes on one node not visible on others.

**Causes**:
1. Caching: changes can show up to `-attr-timeout` (1s) or `-cache-ttl` (5s) late, even with broadcast working
2. Broadcast not working, so caches only catch up after `-cache-ttl`

**Solutions**:
```bash
# Verify multicast working
# On receiver node:
tcpdump -i eth0 udp port 31337 or udp port 31338

# On sender node:
# Modify a file and watch for packets

# If no packets, check:
# 1. Firewall rules
iptables -L -n | grep -E '3133[78]'

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

- It rebuilds block refcounts from the extents of every inode, snapshot and clone, including extent trees. It skips this when it could not read an inode or extent tree, or when `-skip-phase` skipped the bitmap or inode phase, because counts that miss an owner let the runtime free a block another file still uses.
- It marks the blocks that older versions shared without counting as counted, so this version can snapshot, clone and free those files.
- It rewrites refcount sectors that fail their checksum from the same references.
- It fixes the superblock checksum, but only when every other superblock field passed its checks.
- It frees leaked blocks: blocks the bitmap marks allocated that no inode, extent tree or older small-file container maps. Writes and mkdirs that fail, interrupted truncates and unlinks, frees that gave up waiting for a lock, and small-file migrations leave them behind. It sets their refcounts to 0 first. It keeps all of them, refcounts included, when it could not read an inode or extent tree, when `-skip-phase` skipped the bitmap, inode or refcount phase, or when a node's journal still needs replay (even with `-force`). It also keeps those in bitmap sectors with a bad checksum.
- It frees inodes that no directory names, with their blocks, the way unlink does. Replay leaves them behind when it skips an unlink's records, for example after another node wrote a file that was unlinked while open there. A block that a clone or snapshot also maps loses one owner and stays. It keeps inodes a snapshot refers to, reserved inodes, directories that are not empty, and files that map older small-file slots. It keeps all of them when it could not read every inode, directory or snapshot descriptor, when `-skip-phase` skipped a phase it needs, or when a node's journal still needs replay. There is no `lost+found`, so the data of a freed file is gone: read the `-dry-run` list first.

It never edits directories. Problems not marked `[FIXABLE]` stay as they are.

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
| 1 | Superblock | Magic, version, block and inode size, checksum, and the format v3 layout (below). fsck stops here on a version 2 volume, or when a region lies outside the device or overlaps another. |
| 2 | Bitmap | The bitmap covers every data block; sector checksums; counts allocated blocks |
| 3 | Inodes | Inode checksums, file type, link count, block counts; reads extent trees |
| 4 | Extents | Extents stay inside the device and are allocated; two inodes share a block only if both mark it shared; extent tree structure |
| 5 | Refcounts | The table covers every data block; sector checksums; each block's refcount covers the inodes that map it |
| 6 | Directories | Entry lengths and names; entries point at inodes in use |
| 7 | Snapshots | Registry sector and descriptor checksums, duplicate IDs and names, snapshot data inodes |
| 8 | Cross-Validation | Allocated blocks that no inode maps, mapped blocks the bitmap calls free, inodes no directory names |

The format v3 layout checks in phase 1:

- Every region starts on a 4 KB boundary, lies inside the device, and does not overlap another region.
- The journal region splits evenly into one journal per node, and each journal is a multiple of 4 KB and at least 8 KB.
- The node registry has a slot for every node journal.
- The regions have the order and sizes mount accepts. If not, fsck reports an error and keeps checking.

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

   # GoClusterFS version
   ./goclusterfs-mount -version

   # Mount state and counters, if the API is on
   gfsctl status -o json
   gfsctl stats all -o json

   # Recent mount log
   tail -1000 /var/log/goclusterfs.log > mount.log

   # fsck output
   ./goclusterfs-fsck /dev/device > fsck.log 2>&1
   ```

2. **Report issue**: https://github.com/supporttools/GoClusterFS/issues
