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
| Storage | Any O_DIRECT-capable device | NVMe, SAN, or iSCSI LUN |

### Software Requirements

- **Operating System**: Linux (kernel 4.x or later)
- **Go**: Version 1.26 or later (for building from source)
- **FUSE**: `fuse3` package with `fusermount3` utility

```bash
# Debian/Ubuntu
apt-get install fuse3 build-essential

# RHEL/CentOS/Fedora
dnf install fuse3 fuse3-devel gcc make

# Verify FUSE is available
fusermount3 --version
```

### Permissions

GoClusterFS requires either:
- Root access, OR
- Membership in the `fuse` group

```bash
# Add user to fuse group
sudo usermod -aG fuse $USER

# Re-login or use newgrp
newgrp fuse
```

### Network Requirements (Multi-Node)

| Requirement | Details |
|-------------|---------|
| Connectivity | All nodes on same L2 network segment |
| Multicast | UDP multicast between all nodes. Unicast is only a fallback to peers learned over multicast. |
| Ports | UDP 31337 (configurable) |
| Latency | < 10ms between nodes recommended |

---

## Installation

### Building from Source

```bash
# Clone repository
git clone https://github.com/supporttools/GoClusterFS.git
cd GoClusterFS

# Build goclusterfs-mount and goclusterfs-fsck
make build

# Optional: Install to system path
sudo cp goclusterfs-mount goclusterfs-fsck /usr/local/bin/
```

### Verify Build

```bash
./goclusterfs-mount -h
./goclusterfs-fsck -version
```

Each `v*` tag publishes prebuilt `linux/amd64` and `linux/arm64` archives on the GitHub Releases page.

---

## Block Device Setup

GoClusterFS can use any O_DIRECT-capable block device of at least 264 MB. The layout reserves 256 MB for 64 node journals of 4 MB.

### Option 1: File-Backed Device (Development/Testing)

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

For multi-node deployments, use shared storage:

```bash
# iSCSI example - discover and login
iscsiadm -m discovery -t st -p <iscsi-target-ip>
iscsiadm -m node -T <iqn> -p <ip> --login

# Device appears as /dev/sdX
# Use multipath for redundancy
multipath -ll
# Use /dev/mapper/mpathX
```

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

The block size is fixed at 1 MB, and the inode count at 4096.

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

### Mount with FUSE Request Tracing

`-debug` logs every FUSE request. The mount logs its own `[DEBUG]` lines either way.

```bash
./goclusterfs-mount -device /path/to/device -mount /mnt/goclusterfs \
    -debug
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
ExecStart=/usr/local/bin/goclusterfs-mount \
    -device /dev/vg_data/goclusterfs \
    -mount /mnt/goclusterfs
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

Format on one node while no other node has the device mounted, then unmount:

```bash
./goclusterfs-mount -device /dev/mapper/shared_lun -mount /mnt/cluster -format
# In another shell, once it has mounted:
fusermount3 -u /mnt/cluster
```

### Step 3: Mount on All Nodes

Use identical mount options and the same GoClusterFS version on all nodes:

```bash
./goclusterfs-mount -device /dev/mapper/shared_lun -mount /mnt/cluster \
    -mcast-group 239.192.74.75 \
    -mcast-port 31337 \
    -broadcast-secret "your-shared-secret"
```

Read [Node Requirements](OPERATIONS.md#node-requirements) and [Distributed Lock Limits](OPERATIONS.md#distributed-lock-limits) first. Node clocks must agree, and nodes must not stall.

### Step 4: Verify Cluster Communication

Each mount logs `Broadcast: enabled on 239.192.74.75:31337` at startup, or a warning if broadcast could not start. It logs `Broadcast stats` when it unmounts.

```bash
# Verify multicast is working
tcpdump -i eth0 udp port 31337
```

### Network Configuration Options

#### Multicast (Default)

```bash
./goclusterfs-mount ... \
    -mcast-group 239.192.74.75 \
    -mcast-port 31337 \
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
ExecStart=/usr/local/bin/goclusterfs-mount \
    -device /dev/mapper/shared_lun \
    -mount /mnt/cluster \
    -mcast-group 239.192.74.75 \
    -mcast-port 31337 \
    -broadcast-secret "${GOCLUSTERFS_SECRET}"
ExecStop=/usr/bin/fusermount3 -u /mnt/cluster
Restart=on-failure
RestartSec=10
EnvironmentFile=-/etc/default/goclusterfs

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

For read-heavy workloads where open files may see other nodes' changes up to 30 seconds late:

```bash
./goclusterfs-mount -device /dev/device -mount /mnt/fs \
    -cache-ttl 30s \
    -trim-batch 512
```

### Minimal (Testing/Development)

Disable optional features for debugging, on a single node only. `-journal=false` gives no crash consistency and skips replay of this node's journal slot:

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

### Firewall Rules

```bash
# Allow multicast traffic between cluster nodes
iptables -A INPUT -p udp --dport 31337 -j ACCEPT
iptables -A OUTPUT -p udp --dport 31337 -j ACCEPT

# Or with firewalld
firewall-cmd --permanent --add-port=31337/udp
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
