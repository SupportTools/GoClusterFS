// Package storage provides O_DIRECT block device I/O for GoClusterFS.
//
// O_DIRECT bypasses the OS page cache, which is essential for a clustered
// filesystem where multiple nodes access the same block device. Without
// O_DIRECT, each node would have its own cached view of the data, leading
// to corruption when nodes make concurrent modifications.
package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

// ErrWriteDeadline means WriteAtBefore reached its deadline before it started the write, so nothing was written.
var ErrWriteDeadline = errors.New("write deadline passed before the write started")

const (
	// SectorSize is the minimum alignment for O_DIRECT I/O.
	// While traditional disks use 512-byte sectors, modern NVMe drives,
	// and filesystems like ZFS/XFS/ext4 typically require 4096-byte alignment.
	// Using 4096 is the safe choice for broad compatibility.
	SectorSize = 4096

	// DefaultBlockSize is the standard data block size (1MB).
	DefaultBlockSize = 1 << 20 // 1MB

	// MetadataBlockSize is used for metadata structures (4KB).
	MetadataBlockSize = 4096

	// NumIOShards is the number of lock shards for parallel I/O.
	// Using 64 shards provides good parallelism without excessive memory overhead.
	NumIOShards = 64

	// IOShardSize is the byte range covered by each shard (1MB).
	// This aligns with DefaultBlockSize so most operations hit a single shard.
	IOShardSize = 1 << 20 // 1MB
)

// Device represents a block device opened with O_DIRECT.
// I/O operations are protected by sharded locks to allow parallel access
// to different regions of the device. Operations within the same 1MB region
// are serialized, while operations to different regions can proceed in parallel.
type Device struct {
	ioShards [NumIOShards]sync.Mutex // Sharded locks for parallel I/O
	closeMu  sync.RWMutex            // Protects close; held shared during I/O
	file     *os.File
	path     string
	size     int64
}

// shardIndex returns the shard index for the given offset.
func shardIndex(offset int64) int {
	return int((offset / IOShardSize) % NumIOShards)
}

// lockIORange locks all shards covering the byte range [offset, offset+length).
// Returns the start and end shard indices. Shards are locked in ascending order
// to prevent deadlocks when multiple goroutines lock overlapping ranges.
func (d *Device) lockIORange(offset int64, length int) (startShard, endShard int) {
	startShard = shardIndex(offset)
	endShard = shardIndex(offset + int64(length-1))

	// Fast path: single shard (common case for block-aligned I/O)
	if startShard == endShard {
		d.ioShards[startShard].Lock()
		return startShard, endShard
	}

	// Multi-shard: lock all affected shards in ascending order
	// Handle potential wrap-around in shard indices
	if startShard <= endShard {
		for i := startShard; i <= endShard; i++ {
			d.ioShards[i].Lock()
		}
	} else {
		// Wrap-around case: lock from startShard to NumIOShards-1, then 0 to endShard
		for i := startShard; i < NumIOShards; i++ {
			d.ioShards[i].Lock()
		}
		for i := 0; i <= endShard; i++ {
			d.ioShards[i].Lock()
		}
	}
	return startShard, endShard
}

// unlockIORange unlocks all shards in the range [startShard, endShard].
// Shards are unlocked in reverse order of locking.
func (d *Device) unlockIORange(startShard, endShard int) {
	// Fast path: single shard
	if startShard == endShard {
		d.ioShards[startShard].Unlock()
		return
	}

	// Multi-shard: unlock in reverse order
	if startShard <= endShard {
		for i := endShard; i >= startShard; i-- {
			d.ioShards[i].Unlock()
		}
	} else {
		// Wrap-around case: unlock 0 to endShard, then startShard to NumIOShards-1
		for i := endShard; i >= 0; i-- {
			d.ioShards[i].Unlock()
		}
		for i := NumIOShards - 1; i >= startShard; i-- {
			d.ioShards[i].Unlock()
		}
	}
}

// Open opens a block device with O_DIRECT for raw I/O.
// The device must exist and be accessible.
func Open(path string) (*Device, error) {
	// O_DIRECT: Bypass page cache
	// O_SYNC: Synchronous writes (data + metadata)
	// Note: O_SYNC ensures durability but impacts performance.
	// For better performance with journaling, we can use fdatasync() selectively.
	file, err := os.OpenFile(path, os.O_RDWR|syscall.O_DIRECT, 0660)
	if err != nil {
		return nil, fmt.Errorf("failed to open device %s: %w", path, err)
	}

	// Get device size
	size, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to get device size: %w", err)
	}

	// Rewind to start
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to seek to start: %w", err)
	}

	return &Device{
		file: file,
		path: path,
		size: size,
	}, nil
}

// Close closes the device.
// It waits for any in-progress I/O operations to complete before closing.
func (d *Device) Close() error {
	// Take exclusive close lock to block new I/O operations
	d.closeMu.Lock()
	defer d.closeMu.Unlock()

	if d.file != nil {
		err := d.file.Close()
		d.file = nil
		return err
	}
	return nil
}

// Size returns the device size in bytes.
func (d *Device) Size() int64 {
	return d.size
}

// Path returns the device path.
func (d *Device) Path() string {
	return d.path
}

// ReadAt reads data from the device at the specified offset.
// Both offset and len(buf) MUST be aligned to SectorSize.
// Concurrent reads to different 1MB regions proceed in parallel.
func (d *Device) ReadAt(buf []byte, offset int64) (int, error) {
	if err := validateAlignment(buf, offset); err != nil {
		return 0, err
	}
	if len(buf) == 0 {
		return 0, nil
	}

	// Acquire shared close lock to prevent device close during I/O
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	// Lock the affected shard(s)
	startShard, endShard := d.lockIORange(offset, len(buf))
	defer d.unlockIORange(startShard, endShard)

	return d.file.ReadAt(buf, offset)
}

// WriteAt writes data to the device at the specified offset.
// Both offset and len(buf) MUST be aligned to SectorSize.
// Concurrent writes to different 1MB regions proceed in parallel.
func (d *Device) WriteAt(buf []byte, offset int64) (int, error) {
	return d.WriteAtBefore(buf, offset, time.Time{})
}

// WriteAtBefore is WriteAt, except that it writes nothing and returns
// ErrWriteDeadline if deadline has passed once it holds the I/O shard and is
// about to start the write. A zero deadline never passes.
func (d *Device) WriteAtBefore(buf []byte, offset int64, deadline time.Time) (int, error) {
	if err := validateAlignment(buf, offset); err != nil {
		return 0, err
	}
	if len(buf) == 0 {
		return 0, nil
	}

	// Acquire shared close lock to prevent device close during I/O
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	// Lock the affected shard(s)
	startShard, endShard := d.lockIORange(offset, len(buf))
	defer d.unlockIORange(startShard, endShard)

	if !deadline.IsZero() && !time.Now().Before(deadline) {
		return 0, ErrWriteDeadline
	}
	return d.file.WriteAt(buf, offset)
}

// Sync ensures all written data is persisted to the device.
// This is a global operation that does not block concurrent I/O.
func (d *Device) Sync() error {
	// Acquire shared close lock to prevent device close during sync
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	return d.file.Sync()
}

// Fd returns the file descriptor for use with syscalls.
func (d *Device) Fd() uintptr {
	return d.file.Fd()
}

// validateAlignment checks that buffer and offset meet O_DIRECT requirements.
func validateAlignment(buf []byte, offset int64) error {
	if offset%SectorSize != 0 {
		return fmt.Errorf("offset %d is not aligned to sector size %d", offset, SectorSize)
	}
	if len(buf)%SectorSize != 0 {
		return fmt.Errorf("buffer length %d is not aligned to sector size %d", len(buf), SectorSize)
	}
	// Check buffer memory alignment (only if buffer is non-empty)
	if len(buf) > 0 && uintptr(unsafe.Pointer(&buf[0]))%SectorSize != 0 {
		return fmt.Errorf("buffer memory address is not aligned to sector size %d", SectorSize)
	}
	return nil
}
