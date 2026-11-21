# Multi-node soak

`goclusterfs-soak` runs a random mix of operations on a 3-VM cluster for hours or days and checks every node against a model of the expected state.

- **Operations:** create, write (overwrite, append, sparse), truncate, unlink, mkdir, rmdir, rename, symlink, hard link, snapshot, clone, clone from a snapshot, snapshot delete, and orphan snapshot delete. Renames move a lane's files, links and directories, replace its other entries, or fail as rename(2) does: a missing source or parent, a file onto a directory or the reverse, a directory onto one with entries, an entry onto a directory that holds it, or a directory into itself.
- **Lanes:** several lanes run at once, each on its own files and snapshots, in directories they share.
- **Orphan snapshots:** a snapshot whose file has no names left is an orphan. Like an operator, each lane leaves a few and deletes the rest through the API's no-path `DELETE /api/v1/snapshots/{name}`. The mounts run with a test API token for that.
- **Checks:** after every operation, each node must show its result, through a fresh open, within `-window` (30s).
  - The window covers `-attr-timeout` and `-cache-ttl`, because broadcast cannot cross the VMs' separate networks.
  - It also covers waiting out a failed node's 10s detection.
  - A read that overlaps a write or truncate of the same file, through any of its names, may show the file before or after it.
  - A check that needs longer than `-coherence` (6s) is logged as a `slow` record for review, with its last stale answer and what the model expected.
- **Faults:** every `-fault-every`, one node is hit, picked at random from these:
  - power is cut (QEMU is killed);
  - goclusterfs-mount gets SIGKILL;
  - goclusterfs-mount is stopped past the 10s heartbeat timeout and resumed.

  The node comes back, mounts again and must show the model. The other nodes keep working through the fault.
- **Relogins:** with `-relogin`, which needs `VM_STORAGE=iscsi` and `-inflight`, nodes mount a one-path dm-multipath map over their iSCSI disk, and two more faults join the draw:
  - relogin: the node's iSCSI session logs out and in while operations run on it, and its map takes the new disk;
  - relogin-paused: the same, with goclusterfs-mount stopped for `-stop-for` around it, so peers fence it.

  If the same mount process still serves the mount point 30s later, the relogin counts as `relogin_survived`, and every operation on the node must have succeeded. Otherwise it counts as `relogin_stopped`: the node is mounted again, operations it cut short are judged as for an in-flight fault, and it must show the model.
- **In-flight faults:** by default a fault waits for the operations running on its node. With `-inflight` it does not, so a fault can land in the middle of an operation.
  - An operation the fault cuts short may or may not have happened. Once the node is back, the soak reads what it shows and accepts the state from before the operation or after it.
  - A write may also be torn: each 4 KB sector shows the old data or the new.
  - Anything else is a failure. `ops.jsonl` logs each such operation as `uncertain`, then the outcome the soak saw as `resolved`.
- **Stalls:** when a call waits longer than `-stall-capture` (20s), the soak saves from every node:
  - each mount thread's kernel stack;
  - the lock and reserve wait totals from `/metrics`;
  - the FUSE requests queued;
  - every goroutine's stack, which goclusterfs-mount writes to its log on SIGUSR1.

  The files go to `stalls/`, at most once a minute and never during a fault, which stalls calls on purpose.
- **fsck:** every `-fsck-every`, all operations stop, every node unmounts and goclusterfs-fsck runs. Anything other than leaks and orphan snapshots is a failure. Leaks are repaired.

The first failure stops the run. `result.txt` says what failed, `ops.jsonl` logs every operation, fault and fsck, and `cluster.img` keeps the shared disk.

## Run

On a host with KVM, from a checkout of its own:

```bash
make soak
flock /tmp/goclusterfs-soak.lock systemd-run --scope -p MemoryMax=8G -p CPUQuota=400% --quiet \
    ./goclusterfs-soak run -duration 48h -out /root/gcfs-soak/run1
```

The VMs use SSH ports from `-ssh-base-port` (3122), so the soak can share a host with other VM test runs. It logs a summary every minute.

With `VM_STORAGE=iscsi` in the environment, the nodes mount the shared disk as an iSCSI LUN from a target VM, so `-format` turns on storage fencing. The hourly fsck checks that LUN too.

## Replay

```bash
./goclusterfs-soak replay -log /root/gcfs-soak/run1/ops.jsonl -out /root/gcfs-soak/replay1
```

Replay boots a new cluster and runs the logged operations one at a time, in log order, on the same nodes, with the same data, faults and fsck runs. `-upto N` stops after operation N. Lanes ran at the same time in the original run, so a failure that needs their exact timing may not happen again.
