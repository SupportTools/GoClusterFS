---
name: Bug report
about: Report a crash, data problem, or wrong behavior
title: ""
labels: bug
assignees: ""
---

<!-- Security problem? Do not file it here. See SECURITY.md. -->

## What happened

<!-- What you saw, and what you expected instead. -->

## Steps to reproduce

1.
2.
3.

## Environment

- GoClusterFS commit (`git rev-parse HEAD`) or release tag:
- Kernel (`uname -r`):
- Distribution:
- FUSE version (`fusermount3 --version` or `fusermount --version`):
- Number of nodes:
- Shared storage type (for example iSCSI, NVMe-oF, SAN, file image on NFS, local image file):
- Filesystem under the image, if you use an image file:

## Mount command

<!-- The full goclusterfs-mount command line on each node, for example:
     goclusterfs-mount -device /dev/sdb -mount /mnt/gcfs -journal -broadcast -->

```
```

## Logs

<!-- goclusterfs-mount output from each node, and fsck output if you ran it.
     Mark which node each log comes from. Remove secrets such as -broadcast-secret. -->

```
```
