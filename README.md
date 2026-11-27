# GoClusterFS

A VMFS-like clustered filesystem written in Go. GoClusterFS enables multiple nodes to safely access a shared block device simultaneously with strong consistency guarantees.

## Features

- **Clustered Access**: Multiple nodes can mount the same block device concurrently
- **O_DIRECT I/O**: Bypasses host page cache for true cluster coherency
- **Generation-Based Cache Coherency**: Efficient validation without cross-node communication
- **Write-Ahead Journal**: Crash-consistent metadata updates, one journal per node
- **Distributed Locking**: on-disk locks; each node writes its claim in its own slot of a lock record, then reads the record
- **Network Cache Invalidation**: UDP multicast for near-instant cross-node cache updates
- **Copy-on-Write Snapshots**: File snapshots and clones that share blocks until a write (experimental)
- **Symlinks**: Targets of up to 92 bytes live in the inode, longer ones up to 4095 bytes in one block
- **Hard links**: `link(2)` gives a regular file up to 65,000 names; directories and symlinks get `EPERM`
- **Thin Provisioning**: Allocate-on-write with TRIM/BLKDISCARD support
- **Project Quotas**: Block and inode limits per directory tree, set with an extended attribute; see [Project Quotas](docs/OPERATIONS.md#project-quotas)
- **Cluster-wide flock**: `flock(2)` locks hold on every node; POSIX `fcntl` locks hold only on the node that takes them
- **8-Phase fsck**: Comprehensive filesystem consistency checker with repair mode

## Requirements

- Go 1.26 or later
- Linux with FUSE support (`fusermount3`)
- O_DIRECT-capable storage (tmpfs supports O_DIRECT only on Linux 6.6 and later)
- Root or `fuse` group membership
- For multi-node: a shared block device with SCSI-3 or NVMe persistent reservations (SAN, iSCSI, NVMe-oF), so nodes can be fenced; see [Limits and Status](#limits-and-status)
- A device of at least 329 MB: the layout reserves 256 MB for 64 node journals of 4 MB and 64 MB for lock records, and the quota table and root directory take the first data blocks

## Limits and Status

- **Linux only.** GoClusterFS needs FUSE and O_DIRECT.
- **Rename takes one flag.** `RENAME_NOREPLACE` works; `RENAME_EXCHANGE` and `RENAME_WHITEOUT` fail with `EINVAL`.
- **Every file with data uses at least one full block.** Blocks are 1 MB unless `-format -block-size` picks a power of two from 8 KB, which the volume keeps. [Block Size](docs/OPERATIONS.md#block-size) says which to pick. Small-file packing (SFB) is disabled since [#12](https://github.com/SupportTools/GoClusterFS/pull/12), so `-sfb` has no effect.
- **`ls -l` of a large directory is slow.** Each lookup reads two inodes from disk, about 0.3 ms on our test host, and `ls -l` repeats its lookups once it runs longer than `-attr-timeout`. In one test, `ls -l` took 0.14 s on 1,000 files and 14 s on 10,000.
- **Other nodes' changes can show up late.** The kernel caches file attributes and directory entries for `-attr-timeout` (default 1s). For that long, `stat` and `ls` can show another node's file with its old size and mtime. A file that stays open can use the old size and block mappings for about twice `-attr-timeout`. Opening the file again reads the current data. See [Cache Flags](docs/API-CONFIG.md#cache-flags).
- **On-disk format version 3.** Mount refuses version 1 and 2 images, and version 3 images formatted before [#188](https://github.com/SupportTools/GoClusterFS/pull/188) added file I/O locks. Copy the data off with the build that formatted the image, reformat, and copy it back. A newer version 3 image formatted before project quotas mounts without them; to add them, stop every node and run `goclusterfs-fsck -enable-quotas DEVICE`.
- **The inode count is set at format time.** `-format` creates one inode per `-bytes-per-inode` bytes of device, between 4096 and 16,777,216. The default is one per 1 MB at 1 MB blocks, and one per max(2 × block size, 16 KB) at smaller blocks. It cannot change later.
- **Upgrade all nodes at once.** Unmount on every node, upgrade, then mount again. Mixing old and new binaries on one device can serve stale data or make files unreadable. See the upgrade notes in [CHANGELOG.md](CHANGELOG.md).
- **Snapshots and clones are experimental.** Before you mount an image that has snapshots or clones with a newer build, read the upgrade notes in [CHANGELOG.md](CHANGELOG.md). They say whether to run `goclusterfs-fsck` first.
- **fsck repairs little.** `-repair` rebuilds refcounts, frees leaked blocks and inodes no directory names, fixes the superblock checksum, rewrites snapshot registry sectors that fail their checksum, sets quota project usage to its recount, and points a wrong `.` or `..` at the right directory. It only reports other problems. See [Filesystem Check](#filesystem-check).
- **Several nodes need storage fencing.** A node refuses to mount a volume without storage fencing while another node has it mounted. Without fencing, a VM its hypervisor paused past its takeover resumes and writes after a peer replayed its journal. In tests on QEMU it overwrote file data every time, and `goclusterfs-fsck` found nothing. Format a multi-node volume on a device with persistent reservations. `-unsafe-allow-unfenced` overrides the refusal, for test rigs on files or virtio disks. See [Storage Fencing](docs/OPERATIONS.md#storage-fencing).
- **flock works across nodes, fcntl locks do not.** `flock(2)` locks hold on every node. POSIX `fcntl` locks hold only on the node that takes them, so don't share a SQLite database between nodes.
- **Cluster safety limits.** Node stalls and storage timeouts have hard limits. Read [Node Requirements](docs/OPERATIONS.md#node-requirements) and [Distributed Lock Limits](docs/OPERATIONS.md#distributed-lock-limits) before you run more than one node.

## Quick Start

### Build

```bash
git clone https://github.com/supporttools/GoClusterFS.git
cd GoClusterFS
make build
```

This builds `goclusterfs-mount`, `goclusterfs-fsck` and `gfsctl` in the repo root. To include the web dashboard, run `make dashboard build` instead; it needs Node 22 and npm. Release archives include it.

Or install all three into `$(go env GOPATH)/bin`, without the dashboard, with `go install github.com/supporttools/GoClusterFS/cmd/...@latest`.

### Shell Completions

Release archives include bash and zsh completions in `completions/`:

```bash
sudo cp completions/bash/* /usr/share/bash-completion/completions/
sudo cp completions/zsh/_* /usr/local/share/zsh/site-functions/
```

`make completions` writes the same files from a checkout. Each command also prints its own script: `goclusterfs-mount -completion bash`, `goclusterfs-fsck -completion zsh`, `gfsctl completion bash`.

### Format and Mount

Use a file as the block device. Keep it on a filesystem that supports O_DIRECT.

```bash
truncate -s 1G disk.img
mkdir -p mnt
./goclusterfs-mount -device ./disk.img -mount ./mnt -format > mount.log 2>&1 &
sleep 2
```

`-format` wipes the device and then mounts it. The mount process keeps running until you unmount, and logs to `mount.log`.

### Use It

```bash
echo "Hello, Cluster!" > mnt/hello.txt
cat mnt/hello.txt
df -h mnt
```

### Unmount

```bash
fusermount3 -u mnt
```

The mount process exits after the unmount. Ctrl+C or `kill` (SIGINT or SIGTERM) unmounts too: the mount stops its HTTP API first, then unmounts. While files under the mount are open, the unmount fails and the mount keeps running without its API; close the files and send the signal again. To mount the same image again, run the mount command without `-format`.

## Command-Line Options

These are the main `goclusterfs-mount` flags. Run `./goclusterfs-mount -h` or see the [Configuration Reference](docs/API-CONFIG.md#command-line-flags) for all of them.

| Flag | Default | Description |
|------|---------|-------------|
| `-device` | (required) | Path to block device or image file |
| `-mount` | (required) | Mount point directory (must exist) |
| `-format` | `false` | Erase the device, create an empty filesystem, then mount it. Does not check for other nodes: unmount every node first |
| `-block-size` | `1048576` | With `-format`: data block size, a power of two from 8192 to 1048576 |
| `-bytes-per-inode` | `0` | With `-format`: one inode per this many bytes of device (4096 to 16,777,216 inodes); 0 picks one per block at 1 MB blocks and max(2 × block size, 16 KB) below |
| `-fencing` | `auto` | With `-format`: `pr` fences nodes with persistent reservations and fails if the device cannot, `none` formats without fencing, `auto` picks `pr` when the device supports it |
| `-require-fencing` | `false` | Refuse to mount a volume formatted without fencing |
| `-unsafe-allow-unfenced` | `false` | Mount a volume without fencing while other nodes have it mounted. Unsafe: file data can be corrupted |
| `-allow-other` | `false` | Let every user on the host use the filesystem, with the kernel checking file permissions |
| `-debug` | `false` | Print `[DEBUG]` log lines: file operations, locks, allocation |
| `-fuse-debug` | `false` | Log every FUSE request and reply |
| `-version` | `false` | Print the version and exit |
| `-preflight` | `false` | Check that this host can mount `-device` on `-mount`, print a fix for each problem, and exit without changing anything |

### Cache Options

| Flag | Default | Description |
|------|---------|-------------|
| `-cache-strict` | `false` | Check every cached inode against the device before using it |
| `-cache-ttl` | `5s` | How long a cached inode is used before it is checked against the device |
| `-attr-timeout` | `1s` | How long the kernel caches file attributes and directory entries |

### Journal Options

| Flag | Default | Description |
|------|---------|-------------|
| `-journal` | `true` | Write-ahead journal; turn it off for testing only, since a crash can then leave the filesystem inconsistent |
| `-skip-recovery=N` | `-1` (off) | Emergency only: discard the journal of registry slot N instead of replaying it, if the mount takes slot N. Refuses while other nodes run if the journal holds a committed transaction. See [Damaged Journal Slot](docs/OPERATIONS.md#damaged-journal-slot) |

### Network Broadcast Options

| Flag | Default | Description |
|------|---------|-------------|
| `-broadcast` | `true` | Send cache invalidations over UDP so other nodes see changes sooner |
| `-mcast-group` | `239.192.74.75` | Multicast group address |
| `-mcast-port` | `24730` | UDP port for multicast |
| `-unicast-port` | `24731` | UDP port for unicast; must differ from `-mcast-port` |
| `-broadcast-secret` | (empty) | Secret that signs broadcast messages; the same on every node |
| `-broadcast-iface` | (empty) | Network interface for multicast; empty lets the kernel choose |

With `-broadcast` on, the mount fails if it cannot bind both UDP ports; pass `-broadcast=false` to run without broadcast. With no `-broadcast-secret`, mount logs a `WARNING` line at startup: any host that can send to those ports can make nodes drop their caches. Set the same secret on every node.

### TRIM Options

| Flag | Default | Description |
|------|---------|-------------|
| `-trim` | `true` | Discard freed blocks on the device in batches; does nothing on image files |
| `-trim-batch` | `256` | Freed blocks to collect before a discard |
| `-trim-age` | `5s` | Longest a freed block waits for its discard |

### Feature Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-sfb` | `true` | Ignored. SFB containers are disabled and every file uses full blocks |
| `-snapshots` | `true` | Enable file snapshots and clones through extended attributes |

### HTTP API Options

| Flag | Default | Description |
|------|---------|-------------|
| `-api` | `true` | Serve the HTTP API (`/health`, `/metrics`, `/api/v1/...`) |
| `-api-addr` | `127.0.0.1:8080` | API listen address |
| `-api-token-file` | (empty) | File (mode 0600) with the bearer token for `/api/v1/...`; a token also turns on the snapshot, clone and admin endpoints. Or set `GOCLUSTERFS_API_TOKEN` |
| `-api-token` | (empty) | The token on the command line; visible to all local users in `ps` |
| `-api-cors` | (empty) | Comma-separated CORS allowed origins |
| `-api-rate-limit` | `0` (off) | Requests per second per client IP, with bursts of twice that |
| `-dashboard` | `true` | Serve the web dashboard at `http://<api-addr>/ui/` |

The API only listens on this host by default. Mount refuses to start when `-api-addr` can be reached from other hosts and no token is set. If the API port is taken, mount logs a warning and runs without the API. See [HTTP API Flags](docs/API-CONFIG.md#http-api-flags).

## gfsctl

`gfsctl` reads a mount's HTTP API from the command line. It changes nothing except a directory's project quota, with `quota set`. `make build` builds it, and release archives include it.

```bash
./gfsctl status              # mount state, version, capacity
./gfsctl health              # exits 1 when degraded, 4 when unhealthy
./gfsctl stats               # overview; also: cache, journal, trim, lock, cluster, all
./gfsctl metrics             # Prometheus text from /metrics
./gfsctl snapshots           # every snapshot, orphans marked; needs the API token
./gfsctl quota get /pvc-1    # a directory's project limits and usage; needs the API token
./gfsctl quota set /pvc-1 --bytes 10737418240 --inodes 100000
./gfsctl status -w -i 5s     # refresh every 5 s
./gfsctl stats all -o json   # JSON for scripts
```

It connects to `localhost:8080` by default. Each setting comes from the first of: a flag, an environment variable, the config file (`gfsctl config path`; `gfsctl config init` writes a sample with mode 0600), then the default.

| Flag | Environment | Default |
|------|-------------|---------|
| `--host` | `GFSCTL_HOST` | `localhost` |
| `--port` | `GFSCTL_PORT` | `8080` |
| `--token` | `GFSCTL_TOKEN` | (none) |
| `--output`, `-o` | `GFSCTL_OUTPUT` | `text` (or `json`) |
| `--timeout` | `GFSCTL_TIMEOUT` | `30s` |
| `--debug` | | off: print each API request with its status and time |

Use `GFSCTL_TOKEN` or the config file for the token: other local users can read `--token` with `ps`. gfsctl and the API speak plain HTTP, so for a remote mount don't send the token across the network: tunnel over SSH (`ssh -L 8080:127.0.0.1:8080 node`, then `gfsctl status`) or put a TLS proxy in front of the API. Statistics answer 503 until mount feeds them to the API, and `gfsctl` prints that error.

## Multi-Node Setup

For clustered deployments:

1. **Shared Storage**: All nodes must access the same block device (SAN, iSCSI, NVMe-oF), with SCSI-3 or NVMe persistent reservations. A node refuses to join a volume without fencing while another node has it mounted, unless it passes `-unsafe-allow-unfenced`
2. **Network**: Nodes should be on the same subnet for multicast invalidation
3. **Mount Command**: Use the same `-mcast-group`, `-mcast-port` and `-broadcast-secret` on every node. If you set `-advertise-addr`, give each node its own address
4. **Limits**: At most 64 nodes mount a device at once; run NTP so file times agree
5. **Format version**: This release uses on-disk format version 3, which gives each node its own journal and sizes the block bitmap, refcount table and inode table from the device. It refuses version 1 and 2 devices, and version 3 devices formatted before #188; copy their data off and reformat
6. **Stalled nodes**: Other nodes declare a node dead after about 20 seconds of silence, for example a paused VM, and replay its journal. The stalled node checks its registry slot before each write, so when it resumes it refuses new writes and exits. A write it started before the stall can still land, and under QEMU a paused VM does not see the pause in its clocks, so it can write after its replay, unless the volume is fenced: on a device with SCSI-3 or NVMe persistent reservations, `-format` turns on fencing, and the node that replays a dead node first revokes its access to the device. See [Storage Fencing](docs/OPERATIONS.md#storage-fencing)
7. **Host names**: Give every host its own hostname. A mount treats a stopped slot under its hostname as its own and takes it back
8. **iSCSI**: Give every host its own initiator name in `/etc/iscsi/initiatorname.iscsi` (make one with `iscsi-iname`); hosts that share one cannot fence each other. Mount the LUN through dm-multipath (`/dev/mapper/...`), not `/dev/sdX`: an iSCSI logout deletes a plain disk and stops the mount on it
9. **A dead node's damaged journal**: If peers cannot replay it, they retry every minute, and after 3 failures with the same error every 10 minutes, with `/health` degraded. List what it holds with `goclusterfs-fsck -inspect-slot N`, and free the slot with `-release-slot N` and `-salvage` or `-discard-journal`. While other nodes are mounted, the release refuses to drop a committed transaction. See [Damaged Journal Slot](docs/OPERATIONS.md#damaged-journal-slot)

Format the device once, from one node, with `-format -fencing=pr`; if the device cannot fence, the format fails instead of making a volume only one node can mount. Then mount it on every node without `-format`:

```bash
# Node 1, once: formats, then stays mounted
./goclusterfs-mount -device /dev/mapper/shared_lun -mount /mnt/cluster -format -fencing=pr -broadcast-secret "$SECRET"

# Every other node, and every later mount
./goclusterfs-mount -device /dev/mapper/shared_lun -mount /mnt/cluster -require-fencing -broadcast-secret "$SECRET"
```

Use the same `-broadcast-secret` on every node. `-require-fencing` makes a node refuse a volume formatted without fencing.

## Documentation

- [Deployment Guide](docs/DEPLOYMENT.md) - Installation and configuration
- [Operations Guide](docs/OPERATIONS.md) - Day-to-day operations
- [Troubleshooting](docs/TROUBLESHOOTING.md) - Problem diagnosis
- [Configuration Reference](docs/API-CONFIG.md) - Mount flags, HTTP API and gfsctl settings
- [Design Overview](docs/DESIGN.md) - Architecture and internals

## Filesystem Check

Run fsck on an unmounted device. The device is a positional argument, and flags go before it:

```bash
# Check only (writes nothing)
./goclusterfs-fsck ./disk.img

# Show what -repair would change
./goclusterfs-fsck -repair -dry-run ./disk.img

# Repair
./goclusterfs-fsck -repair ./disk.img
```

Exit codes follow fsck(8) and add up; see [Exit Codes and Debug Output](#exit-codes-and-debug-output).

`-repair` refuses to run while a node has the device mounted, while a stopped node's journal still needs replay, or when mount would refuse the superblock's layout. Back up the device before you repair it. The [fsck Guide](docs/TROUBLESHOOTING.md#fsck-guide) lists what fsck checks and what `-repair` changes.

Before you format fenced storage you have not used before, `goclusterfs-fsck -test-reservations DEVICE` checks that it supports every reservation command fencing sends; see [Storage Fencing](docs/OPERATIONS.md#storage-fencing).

fsck also manages node registry slots. `-list-slots` and `-inspect-slot N` only read, so they run on a mounted device. `-release-slot N` with `-salvage` or `-discard-journal` frees the slot of a node that will not return; without either, it only shows what freeing it would discard. See [Damaged Journal Slot](docs/OPERATIONS.md#damaged-journal-slot).

`-enable-quotas` adds the quota table to a volume formatted before project quotas; stop every node first, and builds without quotas then refuse the volume. `-clear-reservations` lists the device's reservation keys, and with `-confirm` removes them all once no node runs.

## Exit Codes and Debug Output

`goclusterfs-mount`, `goclusterfs-fsck` and `gfsctl` use the same exit codes. They are bits, as with fsck(8), so fsck can add them up: 5 means some errors were repaired and some remain.

| Code | `goclusterfs-mount` | `goclusterfs-fsck` | `gfsctl` |
|------|---------------------|--------------------|----------|
| `0` | Mounted, then unmounted cleanly | No errors (warnings may remain) | Success; `health` is ok |
| `1` | | Errors were repaired, or `-release-slot` freed a slot | `health` is degraded |
| `4` | | Errors remain on disk, or the device failed a `-test-reservations` step | `health` is unhealthy |
| `8` | Could not mount, or stopped on an error | I/O failure, or the check could not finish or was refused, for example `-repair` while the device is mounted | No answer, or an error from the API |
| `16` | Usage error | Usage error | Usage error |

A usage error is a bad flag, argument or setting; the command changed nothing. Error messages end with what to do next, such as the flag to set or the command to run.

`-debug` (or `--debug`) prints extra `[DEBUG]` lines to stderr and changes nothing else. `goclusterfs-mount` logs file operations, locks and block allocation, `goclusterfs-fsck` logs how long each phase took, and `gfsctl` logs each API request with its status and time. `goclusterfs-mount -fuse-debug` also logs every FUSE request and reply.

## Architecture

GoClusterFS uses a layered architecture:

```
┌─────────────────────────────────────────────────────┐
│                    FUSE Layer                       │
│              (hanwen/go-fuse v2)                    │
├─────────────────────────────────────────────────────┤
│                 VFS Operations                      │
│     (Dir, File, Attr, Read, Write, Lookup)          │
├─────────────────────────────────────────────────────┤
│   Cache Manager    │    Lock Manager    │  Journal  │
│  (generation-based)│ (per-node claims)  │  (WAL)    │
├─────────────────────────────────────────────────────┤
│                  Inode Table                        │
│          (extent-based allocation)                  │
├─────────────────────────────────────────────────────┤
│              Block Allocator (Bitmap)               │
│          (thin provisioning + TRIM)                 │
├─────────────────────────────────────────────────────┤
│                Storage Device                       │
│         (O_DIRECT, sector-aligned I/O)              │
└─────────────────────────────────────────────────────┘
```

## Testing

```bash
# Unit tests
make test-unit

# Integration tests as CI runs them (requires FUSE)
make build
go test -v -short -timeout 20m ./tests/integration/...

# Multi-node Docker tests
make test-multinode-docker

# Multi-node VM tests (power failure simulation)
make test-multinode-vm

# Validate test coverage across environments
make test-parity
```

Integration tests start `./goclusterfs-mount`, so run `make build` first. They put images and mount points in `$TMPDIR`, which must support O_DIRECT, and unmount with `fusermount`. CI passes `-short`, which skips the crash-recovery, large-file, many-file and stress tests and shrinks some multi-node tests. To run the skipped tests too, drop `-short`:

```bash
mkdir -p ~/gcfs-tmp
TMPDIR=~/gcfs-tmp go test -v -timeout 30m ./tests/integration/...
```

The full suite took about 15 minutes on one test host, and the tests that `-short` skips about 4 of them. `TestLargeVolume` creates a 200 GB sparse image. `make test-integration` runs the full suite with a 30-minute timeout.

VM tests need an x86_64 Linux host with `/dev/kvm`, `qemu-system-x86_64`, OpenBSD `nc` (Debian's `netcat-openbsd`) and `setpriv` from util-linux. VM N listens for SSH on `127.0.0.1:2222+N-1`; set `VM_SSH_BASE_PORT` to run two suites on one host. Each VM's console, QEMU and mount logs are written to `tests/vm/results/`, including logs from before a VM was killed. QEMU 10+ uses io_uring, which counts against the locked-memory limit. With the common 8 MB default, QEMU fails with `Failed to initialize io_uring: Cannot allocate memory`. Raise the limit for the test user, then log in again:

```bash
echo "$USER - memlock unlimited" | sudo tee /etc/security/limits.d/90-qemu-memlock.conf
```

## License

Apache License 2.0. See [LICENSE](LICENSE).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Report security problems as described in [SECURITY.md](SECURITY.md).
