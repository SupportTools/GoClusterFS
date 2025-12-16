# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GoClusterFS is a FUSE-based clustered filesystem written in Go. It mounts a block device and exposes it as a filesystem, designed for shared storage scenarios across cluster nodes.

## Build and Run Commands

```bash
# Build the binary
go build -o GoClusterFS .

# Run (requires root or fuse group membership)
./GoClusterFS <mountpoint> <block_device>

# Example with a file-backed block device
dd if=/dev/zero of=VirtBlock.img bs=1M count=3000  # Create 3GB virtual block
./GoClusterFS /mnt/goclusterfs ./VirtBlock.img

# Unmount
fusermount -u /mnt/goclusterfs
```

## Architecture

### Storage Layout
- **Block size**: 1MB fixed blocks
- **Block 0**: Reserved for metadata (file table serialized with `gob`)
- **Blocks 1+**: File data (one block per file currently)

### Core Types
- `FS`: Root filesystem implementing `fs.FS` and `Statfs`
- `Dir`: Root directory implementing `Attr`, `ReadDirAll`, `Lookup`, `Create`
- `File`: File node implementing `Attr`, `ReadAll`, `Write`, `Flush`, `Setxattr`
- `FileMetadata`: Persisted file info (inode, filename, size, block index)

### File Locking
File locks are managed via extended attributes for cluster coordination:
- Set lock: `setfattr -n user.lock -v <owner_id> <file>`
- Release lock: `setfattr -n user.unlock -v <owner_id> <file>`

### Current Limitations
- Single block per file (max 1MB file size)
- No directory hierarchy (flat namespace)
- No delete operation implemented
- Metadata not checksummed
