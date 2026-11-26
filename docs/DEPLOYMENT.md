# GoClusterFS Deployment Guide

This guide covers deploying GoClusterFS in both single-node and multi-node clustered configurations.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Block Device Setup](#block-device-setup)
4. [Formatting](#formatting)
5. [Single-Node Deployment](#single-node-deployment)
6. [Multi-Node Deployment](#multi-node-deployment)
7. [Configuration Presets](#configuration-presets)
8. [Security Configuration](#security-configuration)
9. [Verification](#verification)

---

## Prerequisites

### Hardware Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| CPU | 2 cores | 4+ cores |
| RAM | 512 MB | 2+ GB |
| Storage | Any O_DIRECT-capable device; for more than one node, a shared disk with SCSI-3 or NVMe persistent reservations | NVMe, SAN, or iSCSI LUN with persistent reservations |

### Software Requirements

- **Operating System**: Linux 4.5 or later, which added the persistent reservation ioctls that fencing uses. With `-allow-other`, use 6.0 or later
- **Go**: Version 1.26 or later (for building from source)
- **FUSE**: `fuse3` package with `fusermount3` utility

```bash
# Debian/Ubuntu
apt-get install fuse3 make

# RHEL/CentOS/Fedora
dnf install fuse3 make

# Verify FUSE is available
fusermount3 --version
```

### Permissions

Run `goclusterfs-mount` as root. A user other than root needs read and write access to `/dev/fuse` and to the device, and a setuid-root `fusermount3`; `-preflight` checks these. On a fenced volume, Linux before 6.5 lets only root register a reservation key, so run cluster nodes as root.

The build needs no C compiler or FUSE headers.

### Network Requirements (Multi-Node)

| Requirement | Details |
|-------------|---------|
| Connectivity | Multicast reaches only the local subnet. Nodes on other subnets reach each other by unicast, so open UDP 24731 between them |
| Multicast | UDP multicast between nodes where the network routes it. Where it does not, as in AWS, Azure and GCP VPCs, nodes ask each other for unicast at the address in each node's registry slot, after 3 silent `-broadcast-heartbeat` intervals. Set `-advertise-addr` when peers cannot reach the address a node picks by default, such as behind NAT. |
| Ports | UDP 24730 (multicast) and UDP 24731 (unicast), set with `-mcast-port` and `-unicast-port`. TCP 8080 on loopback for the HTTP API; open it only if you expose the API. On a Kubernetes node, keep all three below 30000: kube-proxy sends local traffic to ports in its NodePort range (30000-32767 by default) to its services. `-preflight` warns about it |
| Several mounts per host | Each mount on a host needs its own `-unicast-port` and `-api-addr`. Mounts of different volumes also need their own `-mcast-group` or `-mcast-port`. A host can mount a fenced volume only once |
| Several clusters per LAN | Give each cluster its own `-mcast-group`, or set `-broadcast-secret`; otherwise clusters receive each other's invalidations and peer announcements |
| Latency | < 10ms between nodes recommended |

---

## Installation

### Building from Source

```bash
# Clone repository
git clone https://github.com/supporttools/GoClusterFS.git
cd GoClusterFS

# Build goclusterfs-mount, goclusterfs-fsck and gfsctl
make build

# Or include the web dashboard (needs Node 22 and npm)
make dashboard build

# Optional: Install to system path
sudo cp goclusterfs-mount goclusterfs-fsck gfsctl /usr/local/bin/
```

### Verify Build

```bash
./goclusterfs-mount -version
./goclusterfs-fsck -version
./gfsctl version
```

`make build` reports version `dev`. Each `v*` tag publishes prebuilt `linux/amd64` and `linux/arm64` archives, with the version set and the dashboard included, on the GitHub Releases page. Run the same version on every node, and stop every node before you upgrade. Older builds refuse volumes that newer builds formatted or mounted, with `unknown required flags 0x...`. The first mount of a new build can set a flag that older builds and their fsck then refuse for good. Mount also refuses to join while a live node runs a build that uses lock records differently, and names that node.

---

## Block Device Setup

GoClusterFS can use any O_DIRECT-capable block device of at least 329 MB. The layout reserves 256 MB for 64 node journals of 4 MB and 64 MB for lock records, and the quota table and root directory take the first data blocks.

### Option 1: File-Backed Device (Development/Testing)

A file has no persistent reservations, so its volume is not fenced, and a second node refuses to mount it while one has it mounted. Test rigs that share a file between nodes pass `-unsafe-allow-unfenced` on every node; see [Storage Fencing](OPERATIONS.md#storage-fencing) for the risk.

```bash
# Create sparse file (fast, allocates on write)
truncate -s 10G /path/to/goclusterfs.img

# Or create pre-allocated file (better performance)
dd if=/dev/zero of=/path/to/goclusterfs.img bs=1M count=10240

# Verify O_DIRECT support (tmpfs supports it only on Linux 6.6 and later)
dd if=/dev/zero of=/path/to/goclusterfs.img bs=4k count=1 oflag=direct conv=notrunc
```

### Option 2: LVM Logical Volume

```bash
# Create LV
lvcreate -L 100G -n goclusterfs vg_data

# Path will be /dev/vg_data/goclusterfs
```

### Option 3: iSCSI/FC LUN (Multi-Node)

For multi-node deployments, use shared storage with SCSI-3 persistent reservations, so `-format` turns on fencing. Without fencing, a VM its hypervisor paused past its takeover resumes and writes after a peer replayed its journal. In tests on QEMU it overwrote file data every time, and `goclusterfs-fsck` found nothing, so a node refuses to join an unfenced volume while another node has it mounted. Check new storage with `goclusterfs-fsck -test-reservations DEVICE` before you format it.

```bash
# iSCSI example - discover and login
iscsiadm -m discovery -t st -p <iscsi-target-ip>
iscsiadm -m node -T <iqn> -p <ip> --login

# Mount the LUN through dm-multipath, even with one path: an iSCSI logout
# deletes a plain /dev/sdX, and the mount on it stops
multipath -ll
# Use /dev/mapper/mpathX
```

Give every host its own iSCSI initiator name in `/etc/iscsi/initiatorname.iscsi`. Hosts cloned from one image share it; generate a new name with `iscsi-iname` before the first login. Storage fencing does not hold between two hosts with one initiator name (see [Storage Fencing](OPERATIONS.md#storage-fencing)).

### Option 4: NVMe-oF (High Performance Multi-Node)

```bash
# Discover NVMe-oF targets
nvme discover -t tcp -a <target-ip> -s 4420

# Connect
nvme connect -t tcp -n <nqn> -a <target-ip> -s 4420

# Device appears as /dev/nvmeXnY
```

---

## Formatting

**WARNING**: Formatting destroys all data on the device.

### Basic Format

`-format` needs a mount point. It writes a new superblock, zeroes all metadata, and then mounts the filesystem.

```bash
mkdir -p /mnt/goclusterfs
./goclusterfs-mount -device /path/to/device -mount /mnt/goclusterfs -format
```

Press Ctrl+C or run `fusermount3 -u /mnt/goclusterfs` to unmount. Never pass `-format` again on a device that holds data, and never while another node has it mounted.

`-format -block-size` sets the data block size, a power of two from 8 KB to 1 MB; the default is 1 MB, and it cannot change later. Use 8192 for many small files, 65536 for mixed files, and 1048576 for VM disks and large files. `-format` creates one inode per `-bytes-per-inode` bytes of device: by default one per block at 1 MB blocks, and max(2 × block size, 16 KB) below that. The inode count is between 4096 and 16,777,216 and cannot change later. The block bitmap, refcount table and inode table are sized from the device, so at 1 MB blocks a 1 GB image holds 698 data blocks. `-format` also creates the project quota table; a volume formatted by an older build gets it with `goclusterfs-fsck -enable-quotas` while every node is stopped, and builds without quotas then refuse the volume.

### Verify Format

```bash
# After unmounting
./goclusterfs-fsck /path/to/device
```

---

## Single-Node Deployment

### Basic Mount

```bash
# Create mount point
mkdir -p /mnt/goclusterfs

# Mount with defaults (journal, broadcast, TRIM and snapshots on)
./goclusterfs-mount -device /path/to/device -mount /mnt/goclusterfs
```

### Mount with Debug Output

`-debug` adds the mount's `[DEBUG]` lines, and `-fuse-debug` logs every FUSE request and reply.

```bash
./goclusterfs-mount -device /path/to/device -mount /mnt/goclusterfs \
    -debug -fuse-debug
```

### Systemd Service (Single Node)

Create `/etc/systemd/system/goclusterfs.service`:

```ini
[Unit]
Description=GoClusterFS Mount
After=local-fs.target network.target
Requires=local-fs.target

[Service]
Type=simple
# A mount that stopped on an error leaves a dead FUSE mount, and a restart cannot mount over it
ExecStartPre=-/usr/bin/fusermount3 -uz /mnt/goclusterfs
ExecStart=/usr/local/bin/goclusterfs-mount \
    -device /dev/vg_data/goclusterfs \
    -mount /mnt/goclusterfs
# The mount does not notify systemd, so wait until the filesystem is mounted before the unit counts as started
ExecStartPost=/bin/sh -c 'until mountpoint -q /mnt/goclusterfs; do sleep 1; done'
ExecStop=/usr/bin/fusermount3 -u /mnt/goclusterfs
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
systemctl daemon-reload
systemctl enable goclusterfs
systemctl start goclusterfs
```

`systemctl stop` runs `ExecStop`, which unmounts; the mount process then flushes its journal, releases its locks and exits 0. See [What Happens at Shutdown](OPERATIONS.md#what-happens-at-shutdown).

---

## Multi-Node Deployment

### Architecture Overview

```
┌────────────────┐     ┌────────────────┐     ┌────────────────┐
│    Node 1      │     │    Node 2      │     │    Node 3      │
│  GoClusterFS   │     │  GoClusterFS   │     │  GoClusterFS   │
└───────┬────────┘     └───────┬────────┘     └───────┬────────┘
        │                      │                      │
        │    UDP Multicast     │                      │
        │◄─────────────────────┼──────────────────────┤
        │  (cache invalidation)│                      │
        │                      │                      │
        └──────────────────────┼──────────────────────┘
                               │
                    ┌──────────┴──────────┐
                    │   Shared Storage    │
                    │  (SAN/iSCSI/NVMe)   │
                    └─────────────────────┘
```

### Step 1: Configure Shared Storage

Ensure all nodes can access the same block device with consistent naming.

```bash
# Verify device is visible on all nodes
ls -la /dev/mapper/shared_lun

# Check device serial/ID matches across nodes
udevadm info --query=all --name=/dev/mapper/shared_lun | grep SERIAL
```

### Step 2: Format (Once, from Any Node)

Format the whole disk, not a partition, on one node while no other node has the device mounted, then unmount. Pass `-fencing=pr`: if the device cannot fence, the format fails instead of making a volume that only one node can mount.

```bash
./goclusterfs-mount -device /dev/mapper/shared_lun -mount /mnt/cluster -format -fencing=pr
# In another shell, once it has mounted:
fusermount3 -u /mnt/cluster
```

### Step 3: Mount on All Nodes

Use the same GoClusterFS version, `-mcast-group`, `-mcast-port` and `-broadcast-secret` on every node. Give every host its own hostname: a mount treats a stopped slot under its hostname as its own and takes it back. Pass `-require-fencing`, so a node refuses a volume that was formatted without fencing:

```bash
./goclusterfs-mount -device /dev/mapper/shared_lun -mount /mnt/cluster \
    -require-fencing \
    -mcast-group 239.192.74.75 \
    -mcast-port 24730 \
    -broadcast-secret "your-shared-secret"
```

`-preflight` checks FUSE, O_DIRECT on the device, the mount point and the ports without changing anything; run it on each new node first. Read [Node Requirements](OPERATIONS.md#node-requirements) and [Distributed Lock Limits](OPERATIONS.md#distributed-lock-limits) first. Nodes must not stall, and storage must fail I/O rather than queue it. The storage target must keep its reservations across a restart (APTPL); see [Storage Fencing](OPERATIONS.md#storage-fencing). Only `flock(2)` locks hold across nodes: `fcntl` locks hold only on their own node, so don't share a SQLite database between nodes.

### Step 4: Verify Cluster Communication

Each mount logs `Broadcast: enabled on 239.192.74.75:24730, unicast port 24731` at startup. If broadcast cannot start, for example because a port is taken, the mount fails; pass `-broadcast=false` to run without it. It logs `Broadcast stats` when it unmounts.

```bash
# Verify broadcast is working
tcpdump -i eth0 udp port 24730 or udp port 24731

# Verify every node registered
gfsctl stats cluster
```

### Network Configuration Options

#### Multicast (Default)

```bash
./goclusterfs-mount ... \
    -mcast-group 239.192.74.75 \
    -mcast-port 24730 \
    -broadcast-iface eth0
```

#### With Authentication

```bash
./goclusterfs-mount ... \
    -mcast-group 239.192.74.75 \
    -broadcast-secret "your-shared-secret-here"
```

### Systemd Service (Multi-Node)

Create `/etc/systemd/system/goclusterfs-cluster.service`:

```ini
[Unit]
Description=GoClusterFS Cluster Mount
After=network-online.target iscsi.service multipathd.service
Requires=network-online.target
Wants=iscsi.service multipathd.service

[Service]
Type=simple
ExecStartPre=/bin/sleep 5
# A mount that stopped on an error leaves a dead FUSE mount, and a restart cannot mount over it
ExecStartPre=-/usr/bin/fusermount3 -uz /mnt/cluster
ExecStart=/usr/local/bin/goclusterfs-mount \
    -device /dev/mapper/shared_lun \
    -mount /mnt/cluster \
    -require-fencing \
    -mcast-group 239.192.74.75 \
    -mcast-port 24730 \
    -broadcast-secret "${GOCLUSTERFS_SECRET}"
ExecStop=/usr/bin/fusermount3 -u /mnt/cluster
Restart=on-failure
RestartSec=10
EnvironmentFile=-/etc/default/goclusterfs
# The mount does not notify systemd, so wait until the filesystem is mounted before the unit counts as started.
# A mount after a crash or reboot of this host can wait 20 to 30 seconds before it joins.
ExecStartPost=/bin/sh -c 'until mountpoint -q /mnt/cluster; do sleep 1; done'
TimeoutStartSec=120

[Install]
WantedBy=multi-user.target
```

Create `/etc/default/goclusterfs`, readable only by root:

```bash
GOCLUSTERFS_SECRET="your-shared-secret"
```

systemd passes the secret on the command line, so local users can still see it with `ps`. See the [Security Model](OPERATIONS.md#security-model).

---

## Configuration Presets

### Default (Balanced)

Good for general workloads:

```bash
./goclusterfs-mount -device /dev/device -mount /mnt/fs
```

These defaults are: journal on, broadcast on, `-cache-ttl 5s`, `-attr-timeout 1s`, TRIM on.

### High Consistency

For workloads where every node must see other nodes' changes as soon as possible. `stat` and `ls` get much slower, because each path step reads the directory under a cluster lock:

```bash
./goclusterfs-mount -device /dev/device -mount /mnt/fs \
    -cache-strict \
    -attr-timeout 0
```

### High Performance

For read-heavy workloads. When a broadcast message is lost, another node's change can take up to 30 seconds to show:

```bash
./goclusterfs-mount -device /dev/device -mount /mnt/fs \
    -cache-ttl 30s \
    -trim-batch 512
```

### Minimal (Testing/Development)

Disable optional features for debugging, on a single node only. `-journal=false` gives no crash consistency for this node's own writes; the mount still replays the journal in its registry slot, and only `-skip-recovery` discards it:

```bash
./goclusterfs-mount -device /dev/device -mount /mnt/fs \
    -journal=false \
    -broadcast=false \
    -trim=false \
    -snapshots=false \
    -debug
```

---

## Security Configuration

### Broadcast Authentication

Without `-broadcast-secret`, broadcast messages are not authenticated. Set the same secret on every node:

```bash
# Generate a secret
openssl rand -hex 32

# Use the secret on all nodes (must match)
./goclusterfs-mount ... -broadcast-secret "generated-hex-secret"
```

See the [Security Model](OPERATIONS.md#security-model) for what this does and does not protect.

Mount logs a `WARNING` line at startup when broadcast is on without a secret.

### HTTP API Access

The API listens on `127.0.0.1:8080` by default. A token turns on the snapshot, clone, quota and admin endpoints, and is required for any address other hosts can reach. Once a token is set, every `/api/v1` request needs it, so set `GFSCTL_TOKEN` for gfsctl. `/health`, `/metrics` and `/ui/` answer without a token, to anyone who can reach the port. Keep the token in a file that belongs to the mount's user or root, and that only that user can read:

```bash
install -D -m 600 /dev/null /etc/goclusterfs/api-token
openssl rand -hex 32 > /etc/goclusterfs/api-token
./goclusterfs-mount ... -api-addr :8080 -api-token-file /etc/goclusterfs/api-token
```

Mount refuses a non-loopback `-api-addr` without a token, and a token file that group or others can read. Don't use `-api-token`: every local user can read it in `ps`. The API speaks plain HTTP, so for remote access put a TLS proxy in front of it or use an SSH tunnel. See [HTTP API Flags](API-CONFIG.md#http-api-flags).

### Users Other Than Root

Without `-allow-other`, only the user that mounted can use the filesystem, and the filesystem stores mode bits without checking them. Kubernetes pods that run as another user, and other users on the host, need `-allow-other`:

```bash
./goclusterfs-mount -device /dev/sdb -mount /mnt/goclusterfs -allow-other
```

With it, the kernel checks each file's owner, group and mode bits on every access, as on a local filesystem (FUSE's `allow_other` and `default_permissions`):

- New files belong to the user that creates them. In a setgid directory they take the directory's group, and new directories are setgid too.
- Only root changes a file's owner, and only the owner or root changes its mode. In a sticky directory, such as one with mode 1777, users remove only their own files.
- A clone (`user.clone`, `user.snapshot.clone`) by a user other than root needs read access to the file and write access to the root directory. The clone belongs to that user and loses setuid and setgid. FUSE does not pass a caller's supplementary groups, so for clones only the primary group counts.

Before you turn it on:

- A mount by root needs nothing else. A mount by another user needs `user_allow_other` in `/etc/fuse.conf`.
- Files carry numeric UIDs and GIDs. Give each user the same UID and GID on every node. In Kubernetes these are the pods' `runAsUser`, `runAsGroup` and `supplementalGroups`.
- The root directory belongs to the mounting user with mode 0755. Create a directory for each user or pod and `chown` it.
- Check the modes of existing files: without `-allow-other` nobody else could reach them, so a file with mode 0644 becomes readable by every user.
- A chmod or chown on one node applies on the others within `-attr-timeout` (1 s by default), when their kernels drop the cached attributes. A process that already has the file open keeps its access, as on any filesystem.
- The mount is `nosuid` and `nodev`, so a setuid bit on the volume grants nothing.
- Use Linux 6.0 or later. It removes the setgid bit from a new file whose creator is not in the directory's group; FUSE does not tell GoClusterFS a caller's other groups, so it cannot do this itself. On older kernels such a file keeps the bit, which `nosuid` makes harmless.
- Snapshot and clone requests through the HTTP API skip these checks, and its clones keep the source's owner. Give the API token only to whoever may act as root.

### Firewall Rules

```bash
# Allow multicast traffic between cluster nodes
iptables -A INPUT -p udp -m multiport --dports 24730,24731 -j ACCEPT
iptables -A OUTPUT -p udp -m multiport --dports 24730,24731 -j ACCEPT

# Or with firewalld
firewall-cmd --permanent --add-port=24730-24731/udp
firewall-cmd --reload
```

### SELinux

If SELinux is enforcing, you may need a custom policy:

```bash
# Check for denials
ausearch -m avc -ts recent

# Generate and load policy module
audit2allow -a -M goclusterfs
semodule -i goclusterfs.pp
```

---

## Verification

### Post-Mount Checks

```bash
# Verify mount
mount | grep goclusterfs
df -h /mnt/goclusterfs

# Test write
echo "test" > /mnt/goclusterfs/test.txt
cat /mnt/goclusterfs/test.txt
rm /mnt/goclusterfs/test.txt

# Check filesystem stats
stat -f /mnt/goclusterfs
```

### Multi-Node Verification

```bash
# On Node 1: Create file
echo "from node1" > /mnt/cluster/node1.txt

# On Node 2: Verify file appears (within -attr-timeout, 1s by default, while broadcast works; up to -cache-ttl without it)
cat /mnt/cluster/node1.txt

# On Node 2: Create file
echo "from node2" > /mnt/cluster/node2.txt

# On Node 1: Verify
cat /mnt/cluster/node2.txt
```

### Filesystem Check

```bash
# Unmount first
fusermount3 -u /mnt/goclusterfs

# Run fsck; exit code 0 means no errors
./goclusterfs-fsck /path/to/device
echo $?
```

---

## Next Steps

- [Operations Guide](OPERATIONS.md) - Day-to-day operations
- [Troubleshooting](TROUBLESHOOTING.md) - Problem diagnosis
- [Configuration Reference](API-CONFIG.md) - All configuration options
