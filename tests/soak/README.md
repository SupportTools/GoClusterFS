# Multi-node soak

`goclusterfs-soak` runs a random mix of operations on a 3-VM cluster for hours or days and checks every node against a model of the expected state.

- **Operations:** create, write (overwrite, append, sparse), truncate, unlink, mkdir, rmdir, rename, snapshot, clone, clone from a snapshot, and snapshot delete. Rename is not supported, so each rename must fail and change nothing.
- **Lanes:** several lanes run at once, each on its own files and snapshots, in directories they share.
- **Checks:** after every operation, each node must show its result, through a fresh open, within `-window` (30s).
  - The window covers `-attr-timeout` and `-cache-ttl`, because broadcast cannot cross the VMs' separate networks.
  - It also covers waiting out a failed node's 10s detection.
  - A check that needs longer than `-coherence` (6s) is logged as a `slow` record for review.
- **Faults:** every `-fault-every`, one node is hit, picked at random from these:
  - power is cut (QEMU is killed);
  - goclusterfs-mount gets SIGKILL;
  - goclusterfs-mount is stopped past the 10s heartbeat timeout and resumed.

  The node comes back, mounts again and must show the model. The other nodes keep working through the fault.
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

## Replay

```bash
./goclusterfs-soak replay -log /root/gcfs-soak/run1/ops.jsonl -out /root/gcfs-soak/replay1
```

Replay boots a new cluster and runs the logged operations one at a time, in log order, on the same nodes, with the same data, faults and fsck runs. `-upto N` stops after operation N. Lanes ran at the same time in the original run, so a failure that needs their exact timing may not happen again.
