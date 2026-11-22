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

Each mount serves its counters over the HTTP API on `127.0.0.1:8080`: Prometheus metrics at `/metrics`, and JSON under `/api/v1/status` and `/api/v1/stats/...`. [HTTP API Statistics](API-CONFIG.md#http-api-statistics) lists them. `gfsctl status`, `gfsctl stats` and the web dashboard at `/ui/` read the same data. The counters cover this node only and start at zero when it mounts. `/health` says whether the mount is ok, degraded or unhealthy; see [HTTP API Health and Shutdown](API-CONFIG.md#http-api-health-and-shutdown).

`goclusterfs-mount` also writes a log to stderr, so capture it (see [Background Mount](#background-mount-with-logging) or the systemd unit in the [Deployment Guide](DEPLOYMENT.md#systemd-service-single-node)). With `-debug`, the log also has `[DEBUG]` lines.

### Totals at Unmount

When a mount exits, it logs totals for the whole mount:

```
TRIM stats: queued=..., discarded=..., calls=..., coalesce=...
Journal stats: commits=..., bytes=..., state=...
Cache stats: inodes=... (hit=...%), extents=... (hit=...%)
Broadcast stats: sent=..., received=..., duplicates=..., lost=...
```

`-cache-inodes` (10,000), `-cache-extents` (50,000) and `-cache-negative` (1,000) set the cache sizes; see [Cache Tuning](#cache-tuning).

### Log Lines to Watch

| Log line | Meaning |
|----------|---------|
| `Journal recovery: replayed N committed transactions` | This node replayed a journal left in its slot by a crashed mount. Normal after a crash. When the slot's journal was clean there is no recovery line, only `Journal: initialized` and `Journal: started`. |
| `[WARN] Journal recovery for slot N is waiting for another node's lock` | The replay waits for a lock another node holds and retries every 1 to 30 seconds, with no limit. The line names the holder. |
| `Journal recovery for slot N failed` | The mount stops. See [Damaged Journal Slot](#damaged-journal-slot). |
| `Journal: WARNING - discarding the journal of slot N (-skip-recovery)` | `-skip-recovery=N` threw away this slot's journal. Run fsck after unmounting every node. |
| `Journal: ignoring -skip-recovery=N` | The mount took a different slot, so it replayed that slot's journal and discarded nothing. |
| `Journal: disabled (no crash consistency guarantees)` | `-journal=false`. This node also skipped replaying its slot's journal. |
| `[Recovery] Replaying slot N (node ...) failed` | Another node's journal cannot be replayed. See [Damaged Journal Slot](#damaged-journal-slot). |
| `ERROR: heartbeats to node registry slot N have failed` | This node cannot write the device. It stops. |
| `[ERROR] Stopping this node and keeping its locks: rename of ... is half done` | A rename hit a write error after its journal commit, and three tries to redo the missing writes failed too. The mount exits without releasing its locks, so another node replays its journal and finishes the rename. See [Handling Node Failure](#handling-node-failure). Check the device before mounting this node again. |
| `Node registry slot N is no longer this node's` | Another node took this node's slot, usually after a stall. The mount exits so two nodes never share a journal. |
| `Node registry slot N may no longer be this node's: it went ... without a heartbeat` | This node went more than 15 seconds without a heartbeat, for example a paused process or a suspended host. Another node may have replayed it, so it exits without writing to the device again. |
| `The device refused a write from this node (...): another node declared it dead and revoked its access` | On a fenced volume, another node proved this node dead and revoked its reservation key. The mount exits. See [Storage Fencing](#storage-fencing). |
| `Failed to start cache broadcast: ...` | The mount stops before it touches the device. See [Mount Fails: "Failed to start cache broadcast"](TROUBLESHOOTING.md#mount-fails-failed-to-start-cache-broadcast). |
| `Warning: Failed to start API server` | The API port is taken. The mount runs without the API, `/metrics`, `gfsctl` and the dashboard. |
| `[broadcast] missed N messages from ...` | This node lost broadcast messages, usually because its receive buffer filled during a burst of changes on another node. It dropped all cached inodes and extents and rereads them from the device. Logged at most every 10 seconds. If it repeats, raise `net.core.rmem_max` to at least 1 MB (`sysctl -w net.core.rmem_max=1048576`) and remount. |
| `[broadcast] WARN: no broadcast heard from node ...` | The registry shows that node mounted, but this node has heard none of its broadcasts for 3 `-broadcast-heartbeat` intervals (90 seconds by default). A network that drops multicast causes this, as does a node with `-broadcast=false` or another `-mcast-group` or `-mcast-port`. Until it is fixed, this node sees that node's directory changes only after `-cache-ttl`. `/health` reports it as `broadcast-peers`, and the mount logs `heard from node ... again` when it ends. |
| `lock.Acquire: ... acquired (retries=N, dur=...)` | Lock waits. High `retries` or `dur` means nodes contend for the same files or directories. |
| `[WARN] lock: ... which holds it and is failing or being recovered` | A lock wait for a failed node. It repeats every 30 seconds and ends when that node's journal replay ends. |
| `[WARN] lock: gave up waiting for ...` | A lock wait gave up: after 30 seconds (`DefaultTimeout`) on a live holder, on a failed holder while the operation held another lock, at once when the holder's replay failed, or because a background task stopped at unmount. The line names the holder. After a give-up on a failed holder, the operation lets go of its locks, waits for the replay and retries, so it does not fail. If that replay keeps failing, the wait lasts until an admin releases the slot; see [Damaged Journal Slot](#damaged-journal-slot). Otherwise a file operation that was waiting fails with EIO; during block allocation it fails with ENOSPC. See [Distributed Lock Limits](#distributed-lock-limits). |
| `[WARN] lock: this node has held record N for ...` | This node has held a lock for over 60 seconds. Other nodes wait for it until it is released. |

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

#### What Happens at Shutdown

`fusermount3 -u`, `SIGINT` (Ctrl+C) and `SIGTERM` (`kill`, or `systemctl stop` without an `ExecStop`) all shut down cleanly. On a signal the mount:

1. Logs `Received signal, unmounting...` and stops the HTTP API. Requests in flight get 10 seconds, then are cancelled; snapshot, clone and admin requests that have started run to the end.
2. Unmounts. If the mount point is busy, the unmount fails, the log says so, and the process keeps running without its API. Close the open files, then send the signal again or run `fusermount3 -u`.
3. Stops its heartbeat and failure detection, after a replay of another node's journal in progress ends.
4. Stops the snapshot worker, flushes queued TRIM, stops the journal (marking it clean only if nothing is pending), and releases the lock records it keeps.
5. Stops broadcast.

It then logs the totals and `Filesystem unmounted.`, and exits 0, or 8 when closing the filesystem failed. After `fusermount3 -u`, steps 1, 3, 4 and 5 run once the unmount is done.

- An open file or a file operation still running makes the unmount fail, so stop applications first.
- A signal after a failed unmount tries again; once the unmount is done, further signals are ignored. `kill -9` stops the process at once and leaves its journal for the next mount to replay.
- A clean unmount empties the node's registry slot once its journal stopped cleanly. If the unmount could not let go of files unlinked while open, or stopping its journal failed, the slot stays active instead: a remount on the same host takes it back at once, and otherwise a running node clears it about 20 seconds later. `goclusterfs-fsck -repair` waits up to 11 seconds before it trusts that no node is mounted.

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

A snapshot stays with the file it was taken of. When you delete the file's last name, its snapshots become orphans, even while a node still has the file open: `getfattr` on any file no longer lists them, and `GET /api/v1/snapshots` lists them with `orphan: true`. Delete an orphan with `DELETE /api/v1/snapshots/<name>` and no `path`. Snapshots made by older versions are orphans after an upgrade, because they do not record which file they were taken of. `goclusterfs-fsck` warns about each orphan.

If creating a snapshot fails with `Resource temporarily unavailable` (EAGAIN), another node rolled it back before it was done, for example because this node stalled and looked dead. Try again.

Snapshots and clones share blocks with the file and copy a block on its first write. A block is freed when no file, snapshot, or clone uses it. Before you mount an image with snapshots or clones using a newer build, read the upgrade notes in [CHANGELOG.md](../CHANGELOG.md).

**What a snapshot captures while the file is written.** A snapshot or clone captures each write request whole: up to MaxWrite, 1 MiB. The kernel splits a larger `write()` into several requests, and a snapshot may capture some of them, as it always could from another node. For a consistent snapshot of a file that writes larger records, pause its writers first.

---

## Cluster Management

### Node Requirements

- **Clocks:** A node judges another node by whether its heartbeat keeps changing, timed on its own clock, so failover works even when node clocks disagree. File times use each node's clock, and `goclusterfs-fsck -release-slot` compares a slot's lease with the clock of the host it runs on. Run NTP on every node.
- **Restarts:** A restarted mount takes back the registry slot of a mount that exited on the same host (same hostname and kernel boot ID) and replays its journal before serving. If the old process is gone it takes the slot at once. If it cannot prove that, because the process still runs or ran in another PID namespace, it takes the slot only after watching its heartbeat stay unchanged for about 20 seconds. A wall clock step does not shorten the watch.
- **Crashed node's locks:** Once a node is proven dead, the node replaying its journal takes over its exclusive locks and frees them after the replay, so no other node uses them mid-replay. Proven dead means its mount process is gone (same host and PID namespace), or the observing node has read the same heartbeat for 20 seconds by its own clock. A node paused for more than about 20 seconds therefore counts as dead; it exits on its next heartbeat. The dead node's read locks are freed at once. Until a failed node is replayed, other nodes wait for its locks however long that takes. Failover takes about 3 to 6 seconds when a surviving mount runs on the same host and in the same PID namespace, and up to about 35 seconds otherwise. A mount that takes a failed node's registry slot keeps that node's locks until it has replayed the slot's journal.
- **Node count:** The device has 64 registry slots, each with its own 4 MB journal. A crashed node's slot frees once another node replays its journal, or when the same host mounts again. A host that rebooted after a crash takes back the slot its hostname held, and replays that journal before serving. Otherwise a mount takes another host's slot only when `-skip-recovery` names it or no slot is empty. Either way it first watches the slot's heartbeat stay unchanged for about 20 seconds.

### Known Gaps in Slot Ownership

- Slot claims use a read-compare-write on the device, then re-read the slot after 100 ms. Two nodes can still both keep a slot if one node's write lands more than 100 ms after its read, for example during a storage stall.
- A node paused for more than about 20 seconds (VM freeze, `SIGSTOP`, a stalled path) looks failed to running nodes and to a new mount, which can take its slot and replay and reset its journal. When the paused node resumes, it refuses further writes and exits, but on a volume without [storage fencing](#storage-fencing) writes it started before the pause can still land.
- A restarted container comes back in a new PID namespace, so its mount cannot prove by PID that the previous incarnation exited. It watches the old slot, and takes it and replays its journal before serving once the heartbeat stays unchanged for about 20 seconds. A running node has watched the slot since the crash, so it may replay the slot first; the container then takes another slot.

### Storage Fencing

On a device with SCSI-3 or NVMe persistent reservations, GoClusterFS fences dead nodes at the storage. Every node registers its own reservation key before its first write, and the device accepts writes only from registered nodes. Before a node acts on another node's death, it removes that node's key; acting on a death means marking its slot dead, replaying its journal, taking its locks or mounting into its slot. From then on the device refuses the dead node's writes, including writes it queued before it stalled. `goclusterfs-fsck` does the same before `-repair` or `-release-slot` writes over a node.

- **Qualify new storage first:** before you format a volume on storage that has not fenced GoClusterFS before, run `goclusterfs-fsck -test-reservations DEVICE` on one host with nothing mounted. It sends every reservation command fencing sends, from temporary keys that start with 0x4746, and prints PASS, FAIL, WARN or SKIP for each, with the device's answer. WARN marks a command fencing works without, such as listing keys on kernels without `IOC_PR_READ_KEYS`. It removes its keys on every path and never clears another host's key. Each write it makes puts back the 4 KiB it just read at the start of the device. It refuses a device that holds a GoClusterFS volume unless you pass `-force`. Then prove that a preempted host cannot write:
  1. On host A, run `goclusterfs-fsck -test-reservations -victim DEVICE`. It registers, reserves and prints a key.
  2. Within 10 minutes, on host B, run `goclusterfs-fsck -test-reservations -preempt KEY DEVICE`, with host B's path to the same disk. It preempts host A's key and holds its own for 30 seconds.
  3. Host A then writes 4 KiB with O_DIRECT, which must fail with `EBADE`.

  To check what survives detaching and attaching the disk, or a restart of the storage, run `-test-reservations -keep DEVICE`, detach and attach the disk or restart the storage, then run `-test-reservations -check-kept KEY DEVICE` on the same host. The exit code is 0 when every step passed, 4 when a step failed, and 8 when the test was refused or could not run.
- **Turning it on:** `-format` with `-fencing=auto`, the default, fences when the device supports persistent reservations; `-fencing=pr` requires them and `-fencing=none` turns fencing off. The superblock records the choice, so every node fences, and older builds refuse the volume. Files, loop devices, virtio-blk disks and partitions have no reservations; a reservation covers the whole disk, so format the whole disk. Format and every mount warn, `/health` reports `fencing` as `none:`, and `-require-fencing` refuses to mount such a volume.
- **Several nodes need fencing:** a node refuses to mount a volume without fencing while another node has it mounted. That covers a volume formatted with `-fencing=none`, and one formatted with `-fencing=auto` on a device without reservations; the superblock then lacks `RequiredReservations`, so no node registers a key. Without fencing, the stall guards rely on the node's own clock, and under QEMU a paused VM's clocks do not count the pause. Such a VM, paused past its takeover, resumes and writes after a peer replayed its journal. In tests on QEMU it overwrote file data in 6 of 6 runs, and `goclusterfs-fsck` found nothing. The rules:
  - A node mounted alone needs no flag, and the mount logs the existing warning.
  - A node that joins checks the registry after it registers, so of two nodes mounting at once at least one sees the other. Every Active or Leaving slot counts, even a silent one, since a paused node looks like a crashed one; an exited mount process on the same host does not, even one its parent has not reaped. A clean unmount empties its slot, so after every node unmounted cleanly, the first node to mount again needs no flag. A node that is gone for good still counts until a peer replays it, or until `goclusterfs-fsck -release-slot` frees its slot, and so does a node whose unmount kept its slot, because files it unlinked were still open or its journal did not stop cleanly. Check the slots with `goclusterfs-fsck -list-slots DEVICE`, and free each slot whose node is gone with `-release-slot N`. A refused node leaves its slot as a crash would, because it may have taken back its host's crashed mount with a journal still to replay: a peer replays the slot about 20 seconds later.
  - `-unsafe-allow-unfenced` lets the joining node mount anyway and logs `[WARN] UNSAFE`. Nodes already mounted do not check again, so a node mounted alone without the flag does not stop when another joins with it.
  - On a fenced volume, nodes join without the flag, and `-unsafe-allow-unfenced` does nothing.
- **A fenced node:** its next write fails with `EBADE` (a reservation conflict), and it stops, as when it loses its registry slot. It writes its heartbeat every second, so it stops at most about 1 second after its key is removed, or 1 second after it resumes from a stall. Until then it can still answer reads from its page and inode caches, and it can still read the device, because the reservation blocks only writes.
- **APTPL must be on:** registrations must survive a restart of the storage target. Linux sets the target's "persist through power loss" (APTPL) flag when it registers a key. If the target loses the registrations anyway, every node's next write fails and the whole cluster stops; nothing is corrupted, but every node must mount again. A Linux LIO target saves them in `/var/target/pr/aptpl_<serial>`. To rebuild an LIO target without losing them:
  1. Copy `/var/target/pr/aptpl_*` before you tear the target down.
  2. Create the backstore again, for example `core/iblock_0/lun0` under `/sys/kernel/config/target`.
  3. Before any node logs in, write the saved file into the backstore: `cat aptpl_<serial> > /sys/kernel/config/target/core/iblock_0/lun0/pr/res_aptpl_metadata`.
  4. Create the LUN, ACLs and portal, and enable the target. Each node's registration and the reservation come back when the node logs in again, and a node fenced before the rebuild stays fenced.
- **iSCSI paths:** mount an iSCSI LUN through dm-multipath. An iSCSI logout deletes a plain disk the mount holds open, so the mount stops with `Device ... went away` and needs a remount once the disk returns; a dm-multipath map survives a path that fails over or logs in again. A target keeps a registration under the session ID of the session that made it, and every new login gets a new one. After a logout and login, LIO usually goes on taking the node's writes, but its unregister at unmount finds no key and logs that the key stays on the target until a peer's preempt or `goclusterfs-fsck -clear-reservations` removes it.
- **One iSCSI initiator name per host:** a target keys each registration by the host's initiator name and the session ID, and every host's first session after boot gets the same session ID. Two hosts that share an initiator name, as hosts cloned from one image do, are then one initiator to the target: one host's key replaces the other's, and a revoke of either does not hold. On LIO they also share one write permission. Give every host its own name in `/etc/iscsi/initiatorname.iscsi`, made with `iscsi-iname`.
- **One hostname per host:** give every host its own hostname. A mount counts a slot under its hostname whose heartbeat stopped as its own host's earlier mount and takes it back, and it logs a warning when a live node shares its hostname.
- **Rejoining after a crash or reboot:** a mount that finds a slot under its hostname whose heartbeat stopped, as an earlier mount on its host leaves, waits up to 20 seconds, logging `Registry slots [N] hold an earlier mount on this host`, for a peer to take that slot over. Without a peer it takes the slot over itself. It registers its own key only then. LIO decides writes per initiator, and revoking a key the host registered from an earlier session, as after a relogin, stops writes from every session of the host until the host registers again. If the device then refuses the mount's reserve or first write, it registers its key anew and tries once more, but only while it can list the device's keys and another node's key is registered, so the reservation cannot lapse. Listing keys needs `IOC_PR_READ_KEYS`, which Linux 6.19 and later have; distribution kernels from before it, such as RHEL 9's 5.14, Ubuntu 24.04's 6.8 and Alpine 3.19's 6.6, lack it. On those kernels, a mount after a relogin that was then killed, or rebooted while its session still held a key from before the relogin, fails to join as before, with `Cannot join the cluster` or `reserving it failed`, and writes nothing; log the host's iSCSI session out and in, which gives it a new registration, and mount again. A mount started right after a crash takes 20 to 30 seconds to join.
- **One fenced mount per host:** a registration belongs to the host's storage initiator, not to a process, so two mounts of one fenced device on one host could not fence each other. A fenced mount locks the device on its host, and a second mount refuses while the first process exists, even stopped. Kill a stopped mount before mounting again.
- **Permissions and multipath:** on Linux 6.5 and later, registering a key needs the device opened for writing; older kernels need root (`CAP_SYS_ADMIN`). With dm-multipath, the kernel registers the key on every path.
- **fsck:** on a fenced volume, a run that writes uses the key of the node mounted on the same host, or registers a key of its own and removes it when done. Run `-repair` and `-release-slot` on a host where no node is mounted: when fsck revokes a dead node's key there, a Linux LIO target also drops the mounted node's writes in flight, and that node can stop.
- **Emergency reset:** `goclusterfs-fsck -clear-reservations DEVICE` lists the device's reservation keys where the kernel can read them, and with `-confirm` removes every key and the reservation. Use it when a mount refuses because the device holds another reservation, when keys of hosts that will not return are left, or before you format the device with `-fencing=none`. It refuses while fsck sees a node running, the same check `-release-slot` makes; `-force` only lets it clear a device fsck cannot read as a volume. fsck sees a node as running only when its heartbeat changes, so it cannot see a paused VM or a stopped mount, whose queued writes land once it resumes after its key is gone: clear only when every node's host is down or its mount process is gone. Until a node mounts again and reserves the device, any host can write to it.

### Adding a New Node

1. **Prepare the node**:
   ```bash
   # Install dependencies
   apt-get install fuse3

   # Copy the binaries; use the same version as the other nodes
   scp goclusterfs-mount goclusterfs-fsck gfsctl node3:/usr/local/bin/

   # Open UDP 24730 and 24731 to the other nodes (see the Deployment Guide's firewall rules)
   ```

2. **Configure shared storage**:
   ```bash
   # Connect to shared storage (iSCSI example)
   iscsiadm -m node -T <iqn> -p <ip> --login

   # Verify device is visible
   ls -la /dev/mapper/shared_lun
   ```

   Mount an iSCSI LUN through dm-multipath, as `/dev/mapper/shared_lun` here, not through its `/dev/sdX`. A path that fails over or logs in again then keeps the mount running; on a plain disk the mount stops (see [Storage Fencing](#storage-fencing)). Set `no_path_retry` so that I/O fails within 10 seconds once every path is gone (see the limits below).

3. **Mount with cluster settings**:
   ```bash
   # Use SAME options as existing nodes; never add -format here
   goclusterfs-mount -device /dev/mapper/shared_lun -mount /mnt/cluster \
       -mcast-group 239.192.74.75 -mcast-port 24730
   ```

4. **Verify cluster membership**:
   ```bash
   # Check broadcast communication
   tcpdump -i eth0 udp port 24730 or udp port 24731

   # Check that the node registered
   gfsctl stats cluster

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

GoClusterFS ignores interrupts, like an NFS `hard` mount. A signal to a program that is inside a file operation, including the `SIGURG` the Go runtime sends for preemption, does not end the operation; the program handles the signal once the operation returns. `SIGKILL` still ends the program. A lock wait for a live holder fails the operation with EIO after 30 seconds (`DefaultTimeout`), and the mount log names the lock and the node that holds it. A wait for a failed node lasts until its journal replay ends, about 35 seconds after a crash on another host; see [Distributed Lock Limits](#distributed-lock-limits).

### Distributed Lock Limits

Nodes coordinate metadata writes (block bitmap, inodes, directories, refcounts, SFB containers, snapshots) and every read and write of file data through lock records on the shared device. Read this before you run more than one node.

**How a node takes a lock.** Each of the 256 lock records has one 4 KB claim slot per registry slot, and a node writes only its own slot. To take a lock, a node writes its claim into its slot, then reads all 64 slots of the record in one read. It holds the lock if no other slot conflicts: for a write lock, any other claim; for a read lock, another node's write claim. Of two nodes that claim at the same time, the one whose write finished second reads the record after the other's write finished, so it sees that claim and does not take the lock. This holds however slow the writes are. It relies on the shared device showing a finished write to every later read, from any node.

**When claims collide.** The node in the lower registry slot keeps its claim and waits for the other claims to go. A node that sees a claim in a lower slot replaces its own with a waiting mark and waits, reading the record without writing until that claim is gone. A mark blocks no one; it only tells holders that the node is waiting. Waits only point from lower to higher slots, so two nodes never wait on each other. A claim kept for a journal replay counts as lower than every slot, so the replaying node is never stuck behind a waiter. When two nodes replay dead nodes whose claims share a record, the replay of the lower dead slot goes first. A slot that fails its checksum, such as one torn by a crash in the middle of a write, counts as a claim.

**When another node's claim stops counting.** A claim counts until the registry slot with the same number is empty: its node unmounted cleanly, or another node replayed its journal and freed the slot. A node checks this by reading the registry slot from the device after it reads the record, so it cannot miss a node that registered in the meantime. A torn slot follows the same rule. After its replay, a mount clears the claims in its own slot that an earlier occupant left. How old a claim is does not matter, so a lock that a node never releases, for example after a bug, stays held until that node unmounts. The node logs a warning for each lock it has held for more than 60 seconds. A replay that fails or gets stuck keeps the dead node's locks held until a later replay succeeds; waits for them fail with `EIO` once its recovery lease lapses, about a minute after the replay last made progress. A mount whose own journal replay gives up waiting for another node's lock retries every 1 to 30 seconds and logs who holds the lock each time. Any other replay failure, such as a record that does not decode or an I/O error, fails the mount at once with the `-skip-recovery` hint.

**The hazard: a stalled node's last writes.** If a node stops for about 20 seconds while it holds a lock, other nodes prove it dead, replay its journal and take its locks. Before every device write, a node checks that it wrote its heartbeat less than 5 seconds ago. If not, it writes a heartbeat first, and if its registry slot is no longer its own, it refuses the write and exits. A node that went more than 15 seconds without a heartbeat exits without reading or writing its slot, because its last read of the slot may predate the takeover. Some writes still land after the takeover:

- a write that stalled after its last check, which runs once the write holds its locks, right before it reaches the device, and I/O the kernel or the storage queued during the stall;
- writes in the second after the takeover, if the stalled node resumed while the other node was marking its slot dead.

Two nodes can then write the same metadata, which can double-allocate blocks or lose directory entries. Only fencing at the storage closes this gap: on a device with persistent reservations, see [Storage Fencing](#storage-fencing).

Common causes of a stall:

- VM pause, suspend, live migration, or a memory snapshot
- `docker pause`, cgroup freezer, `SIGSTOP`, or a debugger on the mount process
- Heavy swapping or a long GC pause
- Storage I/O that queues instead of failing, for example a multipath or iSCSI path failover

Lock slot reads and writes do not take the device's per-region I/O lock, so a node's own data and journal writes to the same region cannot delay them. The lock manager serializes each record's I/O itself. A node clears again, every second, a claim whose release write failed, until the write succeeds or the node stops.

**Nodes keep released locks briefly.** After its last user releases a lock, a node keeps it for 10 ms (`CacheGrace`), so back-to-back work on the same node does not write and read the record again. A node reuses a lock it claimed for a turn of 250 ms (`HoldTurn`). When the turn ends, it reads the lock record, and if no other node has a claim or waiting mark there, it starts a new turn. Otherwise new work on that node waits up to another 250 ms for the current holders to finish. The node then releases the lock and leaves it alone for at least 50 ms (`YieldGap`), and after that while another node still waits for it or holds it, for up to one turn, so another node gets a turn even when its claims take longer than 50 ms on slow storage. A node that starts waiting just after the record was read waits one more turn. A node that claims a lock and finds another node's waiting mark gives it up the same way 250 ms after it first saw the mark, even if it released and claimed the lock again in between. It still keeps each new claim for at least 50 ms, so the requests of one operation, such as a truncate and the write after it, go through before the waiting node's turn. Without this, a node in a lower slot that keeps taking a lock back every few milliseconds would shut out a waiting node in a higher slot. A mark counts only while its node is alive in the registry. Another node that wants the lock waits up to about 35 ms while the holder is idle, and up to about 300 ms while the holder keeps using it. A clean unmount releases kept locks at once. A crashed node's last locks, kept or held, stay held until another node proves it dead and replays its journal (see [Node Requirements](#node-requirements)). A lock wait gives up after 30 seconds (`DefaultTimeout`) while the holder is alive. A wait for a failed holder has no time limit: it logs a warning that names the node, and it ends when the replay does. It fails with `EIO` only if the replay fails or is abandoned, which shows as a lapsed recovery lease, with one exception. An operation that already holds a lock and then waits for a failed node's lock keeps the 30-second limit and fails with `EIO`, because the replay may need the lock it holds. An overwrite of a file's existing data does not: it lets go of its locks at once, waits for the block's lock holding none, and retries, so it does not fail. Operations that already changed something first, such as a create or a write that allocated blocks, keep the 30-second limit. For example, a node holds a directory's lock and waits for a block lock the dead node held, while the dead node's journal changes that directory. The replay's own waits have no limit, so it finishes once the operation gives up. The warning for this case names the record and the dead node.

**A read request never returns part of a write request.** Every read and write request the kernel sends for a file takes the file's I/O lock for the whole request, on every node: shared for a read, exclusive for a write, a truncate or an `fallocate`. Snapshots and clones take it shared while they share the file's blocks. So a read request returns each overlapping write request whole or not at all, whichever node wrote it, and a snapshot never holds part of a write. Writes to one file run one at a time across the cluster and on each node, whatever ranges they cover, so several nodes writing one file do not run in parallel.

A read request sees each write request whole: up to MaxWrite, 1 MiB. Larger operations are several requests:

- The kernel can send one `write()` as several requests: always when it is larger than MaxWrite, and sometimes when a page fault in the caller's buffer cuts a copy short. Another node can read between them, and this node's own `stat` can see the size in between.
- Reads through the page cache come in requests of up to 1 MB, and the kernel keeps the pages it read. One `read()` can return pages this node cached before another node's write together with pages it fetched after it. The cached pages are dropped when this node next opens the file or refreshes its attributes, after `-attr-timeout` (1 second by default).
- A read through the page cache that overlaps a write on the same node can return a mix of old and new data split at any byte, even inside an 8-byte word, as on ext4 or tmpfs: the kernel copies a page out to the reader while the write copies into it, and neither reaches GoClusterFS.
- A read with `O_DIRECT`, or any read on a mount with `-direct-io`, skips the page cache: each `read()` of up to 1 MB is one request and never mixes two writes. `-direct-io` sends every read and write of file data to the filesystem, so small and repeated reads are slower, and a shared `mmap` of a file fails with `ENODEV`.

Writers go first. A node that wants to read a file while another node waits to write it stops taking new read locks for up to 500 ms, so a writer gets the file within about one 250 ms turn of the readers that hold it. Readers then get it back after the writer's turn, so steady writes from other nodes cannot keep a reader out either.

**An open can fail with `ESTALE` while other nodes reuse the name.** If another node removes a name and creates it again while this node opens it, the open can reach the removed file. This node then answers `ESTALE`, and the kernel looks the name up again and retries once. If the name is removed again before the retry opens it, the open fails with `ESTALE`, as it can on NFS. An open never switches to a different file than the one it looked up, and it does not answer `ENOENT`, because the name may exist again.

**Locks of one type share records.** Each lock type has its own records of the 256: 128 for inodes, 65 for block extents, 39 for file I/O, 16 for inode table sectors, and one for each other type. Two files' inode locks can map to one record, and a node holding one of them blocks other nodes from both. A lock never waits on a record another type uses. A node takes a file I/O lock before any other lock, then the others in type order and, within a type, in record order, so two nodes cannot each wait for a record the other holds. The exception is a replay of a failed node's journal: the replaying node keeps the failed node's records until the replay ends, so a node that holds a lock the replay needs and waits on one of those records gives up after 30 seconds with `EIO`. The replay's waits have no limit, so it then finishes.

**Safe operating limits:**

| Limit | Why |
|-------|-----|
| Run chrony or NTP on every node | File times and `fsck -release-slot` use the wall clock; failover does not |
| No node may stall for 15 seconds or more while mounted | Peers prove a node dead after about 20 seconds of silence and take its locks |
| Storage I/O must fail, not queue, within 10 seconds | Without [storage fencing](#storage-fencing), a queued write that lands after takeover corrupts metadata. Do not use multipath `queue_if_no_path`; lower iSCSI `node.session.timeo.replacement_timeout` (default 120s) |
| Unmount before you pause, migrate or snapshot a node VM | Unmounting releases its locks |

If a node did stall for 15 seconds or more while mounted (hung-task warnings in `dmesg`, a VM pause, a path failover), stop its mount process before it does more writes, then check the data from a healthy node.

What is safe today: a node that crashes or is killed and stays down, and a clean unmount. Other nodes take over a crashed node's locks as described under [Node Requirements](#node-requirements).

### Block Reserves

Each node takes up to 64 free blocks from the block bitmap at once (`-block-reserve`), under one hold of the cluster-wide bitmap lock, and hands them to its writes without that lock. Without reserves, every new data block takes the bitmap lock, and with four nodes writing at once about 60% of their time went to waiting for it. Only new data blocks of writes come from the reserve. Extent-tree blocks, directory blocks, copy-on-write copies and `fallocate` still take the bitmap lock.

- **Size.** A reserve holds at most `-block-reserve` blocks, and at most 1/16 of the free blocks per mounted node. There are no reserves while fewer blocks are free than 1% of the device or two reserves per mounted node.
- **Going back.** A node gives its unused reserve back within 11 seconds of its last write, at a clean unmount, and when a write finds free space below that line.
- **Full device.** A node that finds no free block gives back its own reserve, asks the other nodes over broadcast to give back theirs, once a second, and retries for up to 12 seconds before it fails with `ENOSPC`. It logs the wait at INFO, and `goclusterfs_reserve_peer_waits_total` and `goclusterfs_reserve_peer_wait_seconds_total` count it. Without broadcast, other nodes still give back idle reserves within 11 seconds. A node answers at most one request a second.
- **`df`.** A node counts its own unused reserve as free, since its writes can use it. Other nodes' reserves count as used until they give them back, so `df` on two nodes can differ by up to 64 MB per node at the default.
- **Crash.** Each reserve is a journal record. When the node's journal is replayed, by the node at its next mount or by another node, the blocks it reserved and never used go back to the bitmap. fsck reports them as "reserved by node N and never used", which is not an error, and `fsck -repair -force` frees them after it marks the reserve done in that node's journal, so a later replay does not free them again. A crash in a few narrow windows leaks a few blocks instead, and `fsck -repair` reclaims them: between ending a reserve and freeing it, and while a write holds a block whose reserve the node ended early, to give it back or because the journal ring was filling up.

### Handling Node Failure

When a node fails unexpectedly:

1. **Other nodes keep running.** Operations that need one of the failed node's locks wait until the lock is taken over.
2. **Locks and journal.** Once the failed node is proven dead, one surviving node takes over its exclusive locks, replays its journal, then frees the locks. Until then, operations that need its locks wait, however long that takes. See [Node Requirements](#node-requirements).
3. **No action is needed** on the surviving nodes.

A node also stops on purpose when a rename hits a lasting I/O error after its journal commit. A rename changes two directory entries in separate writes, so the node first redoes the writes that failed while it still holds its locks. If they keep failing, it logs `[ERROR] Stopping this node and keeping its locks` with the names, the inode numbers and the error, and exits. Its locks stay claimed, so no other node can change those directories until failover replays its journal and finishes the rename. The file or directory then has its new name only. Other operations that fail after their commit leave at most an inode or blocks that no name reaches; `goclusterfs-fsck -repair` reclaims those.

**When the failed node comes back**, mount it again with the same options. If it gets its old registry slot back (same hostname and boot ID), it replays its own journal before serving. Don't run fsck while other nodes have the device mounted: `-repair` refuses, and a check-only run reads metadata that is changing.

```bash
goclusterfs-mount -device /dev/device -mount /mnt/cluster
```

### Damaged Journal Slot

A mount exits with `Journal recovery for slot N failed: ...` when it cannot replay the journal in the slot it took. Read the rest of the error first:

- **Storage or replay errors** (an I/O error, or `journal recovery failed: ...`): the journal stays in recovery. Fix the storage and mount again without `-skip-recovery`; the replay resumes.
- **A damaged journal header** (`journal superblock checksum mismatch`, `invalid journal magic`, or `journal is corrupt`): the journal cannot be replayed. Only then use `-skip-recovery=N`, with the slot number N from the error, which discards slot N's journal.
- **A journal from a newer build** (`cannot replay: unknown op 0x...`, or `journal format is newer than this build`): this build stops rather than skip what it cannot read, and applies nothing from that transaction on. Replay the journal by mounting a node with the newer build. Peers of the older build fail the same way and keep the slot for you.

`-skip-recovery` is for emergencies only: it throws away changes the node had committed, which may have been only partly applied. Never put it in a unit file or script. `goclusterfs-fsck -list-slots` shows each slot's journal state first, and `goclusterfs-fsck -inspect-slot N` lists every transaction a replay of slot N would apply, with the inodes and blocks each one touches. Both only read, so they run while nodes have the filesystem mounted.

`-skip-recovery=N` discards a journal only when the mount takes slot N, and it makes the mount try slot N first. It takes the slot at once if it is empty or holds this host's exited mount; after watching it for about 20 seconds if it holds a node whose heartbeat never changes; and after a minute if a running node is replaying it and that replay keeps failing, since the replay's lease then lapses. If it cannot take slot N, it logs `ignoring -skip-recovery=N` and replays the journal of the slot it took, so it never discards a journal you did not name. While other nodes have the filesystem mounted, it refuses, before it registers, when slot N's journal holds a committed transaction or cannot be read, since nothing may be lost while nodes use the filesystem. To discard slot N's journal:

1. If the journal holds committed transactions or cannot be read, unmount every node first. Then mount once with `-skip-recovery=N`, on any host. The mount logs `discarding the journal of slot N`.
2. Unmount on every node and run fsck, because discarded transactions may have been only partly applied.

`-skip-recovery=N` does not stop the node from replaying crashed peers' journals. `-journal=false` does not skip the replay of its own slot.

A slot belongs to its host's hostname and boot ID. If the damaged slot belongs to a node that has rebooted or will not mount again, the other nodes log `Replaying slot N ... failed`, naming `-skip-recovery=N`, and retry every minute. After 3 failures in a row with the same error, a node retries only once the slot has stayed unchanged for 10 minutes, logs the failure at most once an hour, and its `/health` `recovery` check reports degraded with the slot number. Each attempt by any node changes the slot, so the cluster as a whole then tries about every 10 minutes. Lock timeouts do not count toward the 3. A mount takes such a slot only when `-skip-recovery=N` names it or no slot is empty. Discard its journal as above, or free the slot with fsck:

```bash
./goclusterfs-fsck -list-slots /dev/device                       # find the slot and its journal state
./goclusterfs-fsck -inspect-slot N /dev/device                   # list what its journal would replay
./goclusterfs-fsck -release-slot N /dev/device                   # check the slot; writes nothing
./goclusterfs-fsck -release-slot N -salvage -report slot-N.txt /dev/device          # replay what applies, discard the rest
./goclusterfs-fsck -release-slot N -discard-journal -report slot-N.txt /dev/device  # or discard the whole journal
```

`-release-slot` alone writes nothing and lists the journal's transactions. With every node unmounted, it refuses until a minute after the slot's lease ended: its last heartbeat plus 10 seconds, or for a slot another node was replaying, that node's last lease renewal plus 60 seconds.

While other nodes have the filesystem mounted, `-release-slot` frees the slot only if that loses nothing: the journal was never used, is clean, or holds no committed transaction. It first watches the slot unchanged on its own clock, for 40 seconds, or 70 for a slot another node was replaying, and refuses if the slot changes. It never replays while nodes run, and the node's unused block reserve stays allocated until `-repair`. For a journal with committed transactions, or one it cannot read, it refuses and names the steps: unmount every node, release the slot with `-salvage` or `-discard-journal`, then run `goclusterfs-fsck -repair` before mounting again. A node that was only paused and resumes after the release finds its slot gone at its next write or heartbeat and stops without writing.

To release, pick one:

- `-salvage` replays the journal's committed transactions in order, as a mount would, until one fails to apply or a record cannot be read. It discards that transaction and every later one. Use it when a replay fails on one bad transaction.
- `-discard-journal` discards the whole journal unread. While a mount could still replay the journal, it refuses without `-force`: mount a node to replay it, or use `-salvage`.

Either one holds the slot's recovery lease while it works, resets the journal, clears the node's lock claims, frees the slot, and exits 1. Each prints what happened to every transaction, and `-report FILE` writes the same to FILE; fsck writes the file once before it changes the device and refuses if it cannot. Salvage replays without cluster locks, which is safe only because no node runs: before each transaction it reads the registry again, and if a node has started it stops and leaves the journal for a later run. It does not free the dead node's unused block reserve when it stops early. Run fsck afterwards: a discarded or failed transaction may be partly applied, and `-repair` reclaims the blocks such a transaction leaked.

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

Snapshots work on single files, not the whole filesystem. To copy a version of one busy file, snapshot it, clone the snapshot, and copy the clone. A snapshot holds each write request whole but can split a larger `write()`; see [Managing Snapshots](#managing-snapshots):

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

`-repair` fixes refcounts and the superblock checksum, frees leaked blocks and inodes no directory names, and rewrites a snapshot registry sector that fails its checksum, keeping the snapshots whose descriptors pass their own checksum. It edits directories only to point a wrong `.` or `..` back at the right directory, and there is no `lost+found`. Restore anything else from backup. See the [fsck Guide](TROUBLESHOOTING.md#fsck-guide).

---

## Health Checks

`gfsctl health` and `GET /health` report the mount's device, journal, peer replays, broadcast and heartbeat: see [HTTP API Health](API-CONFIG.md#http-api-health-and-shutdown). They do not read or write files. Use the script below to check the filesystem itself.

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

`gfsctl stats cluster` shows how many nodes are registered and healthy, and when each peer was last seen.

Separately, each node sends a broadcast heartbeat over UDP every 30 seconds, an invalidation message for each metadata change, and a heartbeat every 50 ms for 500 ms after its last invalidation. Nodes send from the unicast port (24731) to the multicast port (24730):

```bash
tcpdump -i eth0 -c 10 udp port 24730 or udp port 24731
```

---

## Performance Tuning

### Identifying Bottlenecks

| Symptom | Likely Cause | Where to Look |
|---------|-------------|---------------|
| Slow `ls -l` or `stat` | Each name lookup reads two inodes from disk, and `ls -l` repeats its lookups once it runs longer than `-attr-timeout`. A directory left out of the index (past `-dir-index-entries`) is read whole on every lookup | A higher `-attr-timeout`; `goclusterfs_dir_index_rebuilds_total` growing with lookups means directories don't fit `-dir-index-entries` |
| Slow reads | Cache misses | `gfsctl stats cache`, or `Cache stats` at unmount |
| Slow metadata | Lock contention | `lock.Acquire` lines with high `retries` |
| Slow writes | Synchronous journal commits and per-block extent locks | Storage latency |

### Cache Tuning

The flags set how many entries each cache holds and how long cached data may be used:

| Flag | Default | Effect |
|------|---------|--------|
| `-attr-timeout` | `1s` | How long the kernel caches attributes and directory entries. `stat` and `ls` can show another node's change up to this late. `0` makes every `stat` and path step a lookup that reads the directory's and the file's inodes from disk. |
| `-cache-ttl` | `5s` | How long the mount trusts a cached inode before it checks the disk. Directory lookups can lag another node's change by up to this long; open files catch up within about twice `-attr-timeout`. |
| `-cache-strict` | `false` | Check the disk on every cache hit |
| `-cache-inodes` | `10000` | Inodes kept in memory, about 480 bytes each |
| `-cache-extents` | `50000` | Block mappings kept in memory, about 120 bytes each |
| `-cache-negative` | `1000` | Not-found inode lookups kept in memory |
| `-dir-index-entries` | `1048576` | Names kept in directory indexes, about 100 bytes each. Past it, the least recently used directories are dropped and read whole again on their next lookup. `0` turns indexes off |

When `goclusterfs_cache_inode_size` stays at `goclusterfs_cache_inode_capacity` (or the extent pair does), the cache is full and drops entries it may need again; a larger limit trades memory for fewer disk reads.

```bash
# Read-heavy, changes from other nodes may show up later
goclusterfs-mount ... -cache-ttl 30s

# Every node sees other nodes' changes as soon as possible, at a cost
goclusterfs-mount ... -cache-strict -attr-timeout 0
```

**Directory indexes.** Each node keeps the directories it reads in memory, as an index from name to inode. A lookup reads the directory inode first and uses the index only while the inode's generation is the one the index was built at. Every change to a directory's entries writes the directory inode before the entries, so another node's create or remove shows on the next lookup, with or without broadcast. Such a change costs this node one full read of the directory on its next lookup there.

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

**The registry and journals.** Nodes take registry slots, write heartbeats, replay each other's journals and free dead nodes' slots by writing sectors on the device, and each node trusts the others to follow those rules. A host that can write the device can take any node's slot, which makes that node stop, and can rewrite or reset any journal. `goclusterfs-fsck -release-slot` frees a slot by those same rules; see [Damaged Journal Slot](#damaged-journal-slot).

**Broadcast cache invalidation.** Nodes send UDP messages (multicast group `239.192.74.75`, port `24730` by default, unicast on `24731`) that tell peers to drop cached inodes and extents. Heartbeat and announce messages add unicast peers, which a node uses when multicast sends fail. Each node also writes the address peers should send it unicast to (`-advertise-addr`) in its registry slot. The address shares the slot with the node's list of files unlinked while some node has them open. While that list holds more than 483 files, the slot holds no address; once the list is down to 483, the node's next heartbeat or list change writes the address again. Meanwhile peers keep using the last address they read for that node, as long as the registry shows it mounted. A node that hears no multicast from a peer for 3 `-broadcast-heartbeat` intervals asks that peer, by unicast, to also send it everything by unicast at its registry address. Clouds that drop multicast, such as AWS, Azure and GCP VPCs, need no setting for this, and where multicast works nodes send only multicast. Messages carry inode numbers but no file data. Each node numbers its messages, and a node that sees a gap in a peer's numbers drops all cached inodes and extents. The heartbeats after a node's last invalidation let peers notice a lost final message too. Only when those heartbeats are lost as well do cache updates wait for `-cache-ttl`.

- With an empty `-broadcast-secret` (the default), messages are not authenticated. Any host that can send to the port can make nodes drop their caches, and can add any address as a unicast peer. A unicast request only makes a node send to the requester's registry address, which only a host that can write the device sets. Mount logs a `WARNING` line at startup in this case.
- With `-broadcast-secret`, every message carries an HMAC-SHA256 tag. Nodes drop messages with a missing or wrong tag, so all nodes need the same secret.
- The only replay protection is a sequence number check that forgets messages after 5 minutes, so a recorded message can be sent again later, even with a secret.
- Messages are never encrypted. Any host that joins the multicast group can see which inodes change.
- The secret is a command-line flag, so local users on a node can read it with `ps`.

**HTTP API.** Each mount serves an HTTP API on `127.0.0.1:8080` (`-api-addr`), so only local users can reach it. Without a token it only reads status and statistics. With a token (`-api-token-file` or `GOCLUSTERFS_API_TOKEN`), every `/api/v1/...` request needs it, reads included, and the API also lists, creates, clones and deletes snapshots, clones files and runs admin tasks; see [HTTP API Write Endpoints](API-CONFIG.md#http-api-write-endpoints). Anyone with the token can change any file's snapshots and create files in the root directory, so treat it like the device itself.

- To expose it, set `-api-addr` and a token. Mount refuses a non-loopback address without a token.
- `/health` and `/metrics` never need the token, on purpose: load balancers, probes and Prometheus can read them without a secret. Anyone who can reach the port can read them too. They show no file names or data, but `/health` messages name peers' hostnames and registry slot numbers.
- Without a token, the API rejects requests whose `Host` header is not a loopback name, and mount refuses `-api-cors '*'`. Both stop web pages that a local user opens from reading the API.
- The dashboard's files under `/ui/` need no token: they hold no data, and the page's own API calls follow the same token rules as any client. See [Web Dashboard](API-CONFIG.md#web-dashboard).
- `-api-rate-limit` caps requests per client IP, and never limits `/health`, `/metrics` or `/ui/`. See [HTTP API Flags](API-CONFIG.md#http-api-flags).
- The API uses plain HTTP. Put a TLS proxy in front of it, or use an SSH tunnel, if the token crosses a network.
- Keep the token in a 0600 file (`-api-token-file`) or `GOCLUSTERFS_API_TOKEN`. `-api-token` shows it to every local user in `ps`, so mount logs a `WARNING` for it.

**Network listeners.** Each node listens on UDP port 24730 (multicast group) and UDP port 24731 (unicast) on all interfaces, and on TCP `-api-addr`. A second mount on the same host needs its own `-unicast-port` and `-api-addr`; the mount fails at startup if a port is taken. On a Kubernetes node, keep these ports out of the NodePort range (30000-32767 by default): kube-proxy sends local traffic to those ports to its services, and `-preflight` warns about it. Clusters that share a LAN need their own `-mcast-group` or a `-broadcast-secret`, or they receive each other's invalidations. Limit the UDP ports, and the API port if you expose it, to the cluster nodes and your monitoring hosts with a firewall.

---

## Next Steps

- [Troubleshooting](TROUBLESHOOTING.md) - Problem diagnosis
- [Configuration Reference](API-CONFIG.md) - All configuration options
