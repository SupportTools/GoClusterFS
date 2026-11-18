// Package storage provides O_DIRECT block device I/O for GoClusterFS.
//
// O_DIRECT bypasses the OS page cache, which is essential for a clustered
// filesystem where multiple nodes access the same block device. Without
// O_DIRECT, each node would have its own cached view of the data, leading
// to corruption when nodes make concurrent modifications.
package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"
)

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

// stopBeforeWrite names a trigger file for tests: the first data or journal write that starts with the file's
// contents, after every gate passed, removes it and stops this process with SIGSTOP. That stalls a node in the window
// only storage fencing closes. Other writers wait before their gate until the stopped write is issued, so after
// SIGCONT it reaches the device first, the worst case after a stall.
var stopBeforeWrite = os.Getenv("GOCLUSTERFS_TEST_STOP_BEFORE_PWRITE")

// stoppedWrite is closed once the write stopBeforeWrite stopped was issued; nil while none is stopped.
var stoppedWrite atomic.Pointer[chan struct{}]

// stopIfTriggered stops the process before buf is written, if buf matches the trigger file; call resume once the
// write was issued.
func stopIfTriggered(buf []byte) (resume func()) {
	want, err := os.ReadFile(stopBeforeWrite)
	if err != nil || !bytes.HasPrefix(buf, want) || os.Remove(stopBeforeWrite) != nil {
		return func() {}
	}
	issued := make(chan struct{})
	stoppedWrite.Store(&issued)
	_ = syscall.Kill(os.Getpid(), syscall.SIGSTOP)
	return func() {
		stoppedWrite.Store(nil)
		close(issued)
	}
}

// ErrOutOfRange is the error for a read or write that reaches outside the device.
var ErrOutOfRange = errors.New("I/O outside the device")

// Device represents a block device opened with O_DIRECT.
// I/O operations are protected by sharded locks to allow parallel access
// to different regions of the device. Operations within the same 1MB region
// are serialized, while operations to different regions can proceed in parallel.
type Device struct {
	ioShards [NumIOShards]sync.Mutex // Sharded locks for parallel I/O
	closeMu  sync.RWMutex            // Protects close; held shared during I/O
	// writeGate, when set, runs before every write and can refuse it.
	writeGate atomic.Pointer[func(offset int64) error]
	// writeFresh, when set, says without I/O whether a write that passed the
	// gate may still land once it holds its locks.
	writeFresh atomic.Pointer[func(offset int64) bool]
	// readFault, when a test sets it, runs before every read and can fail it.
	readFault atomic.Pointer[func(offset int64) error]
	// writeObserver and syncObserver let tests model a volatile write cache.
	writeObserver atomic.Pointer[func(offset int64, n int)]
	syncObserver  atomic.Pointer[func()]
	file          *os.File
	path          string
	size          int64
	lastErr       atomic.Pointer[error]
	fenced        atomic.Pointer[func(error)]
	fencedOnce    sync.Once
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
	return d.readAt(buf, offset, true)
}

// ReadAtUnsharded is ReadAt without the shard lock, so other I/O to the same
// 1MB region cannot delay it. Callers must serialize their own I/O to the range.
func (d *Device) ReadAtUnsharded(buf []byte, offset int64) (int, error) {
	return d.readAt(buf, offset, false)
}

func (d *Device) readAt(buf []byte, offset int64, sharded bool) (int, error) {
	if err := validateAlignment(buf, offset); err != nil {
		return 0, err
	}
	if len(buf) == 0 {
		return 0, nil
	}
	if err := d.checkRange(offset, len(buf)); err != nil {
		return 0, err
	}
	if fault := d.readFault.Load(); fault != nil {
		if err := (*fault)(offset); err != nil {
			return 0, err
		}
	}

	// Acquire shared close lock to prevent device close during I/O
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if sharded {
		startShard, endShard := d.lockIORange(offset, len(buf))
		defer d.unlockIORange(startShard, endShard)
	}

	n, err := d.file.ReadAt(buf, offset)
	d.record(err)
	return n, err
}

// WriteAt writes data to the device at the specified offset.
// Both offset and len(buf) MUST be aligned to SectorSize.
// Concurrent writes to different 1MB regions proceed in parallel.
func (d *Device) WriteAt(buf []byte, offset int64) (int, error) {
	return d.writeAt(buf, offset, true)
}

// WriteAtUnsharded is WriteAt without the shard lock, so other I/O to the same
// 1MB region cannot delay it. Callers must serialize their own I/O to the range.
func (d *Device) WriteAtUnsharded(buf []byte, offset int64) (int, error) {
	return d.writeAt(buf, offset, false)
}

func (d *Device) writeAt(buf []byte, offset int64, sharded bool) (int, error) {
	if err := validateAlignment(buf, offset); err != nil {
		return 0, err
	}
	if len(buf) == 0 {
		return 0, nil
	}
	if err := d.checkRange(offset, len(buf)); err != nil {
		return 0, err
	}
	if err := d.CheckWrite(offset); err != nil {
		return 0, err
	}
	if observe := d.writeObserver.Load(); observe != nil {
		(*observe)(offset, len(buf))
	}
	for {
		if landed, n, err := d.writeIfFresh(buf, offset, sharded); landed {
			return n, err
		}
		if err := d.CheckWrite(offset); err != nil {
			return 0, err
		}
	}
}

// writeIfFresh writes buf unless the gate went stale while it waited for its
// locks. The gate itself may do I/O, which cannot take closeMu again here.
func (d *Device) writeIfFresh(buf []byte, offset int64, sharded bool) (landed bool, n int, err error) {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()
	if sharded {
		startShard, endShard := d.lockIORange(offset, len(buf))
		defer d.unlockIORange(startShard, endShard)
	}
	if fresh := d.writeFresh.Load(); fresh != nil && !(*fresh)(offset) {
		return false, 0, nil
	}
	resume := func() {}
	if sharded && stopBeforeWrite != "" {
		resume = stopIfTriggered(buf)
	}
	n, err = d.file.WriteAt(buf, offset)
	d.record(err)
	resume()
	return true, n, err
}

// SetWriteGate makes every write call gate with its offset first, and fail
// with gate's error instead of writing.
func (d *Device) SetWriteGate(gate func(offset int64) error) {
	d.writeGate.Store(&gate)
}

// SetWriteFresh makes every write that passed the gate ask fresh again once it
// holds its locks, right before it lands. When fresh says no, the write lets go
// of its locks and runs the gate again, so a write that waited past what its
// gate proved cannot land on the strength of the old check.
func (d *Device) SetWriteFresh(fresh func(offset int64) bool) {
	d.writeFresh.Store(&fresh)
}

// SetReadFault makes fault run before every read; a read it returns an error for fails with that error.
func (d *Device) SetReadFault(fault func(offset int64) error) {
	d.readFault.Store(&fault)
}

// SetWriteObserver makes every write call observe with its range before it writes; nil removes it.
func (d *Device) SetWriteObserver(observe func(offset int64, n int)) {
	if observe == nil {
		d.writeObserver.Store(nil)
		return
	}
	d.writeObserver.Store(&observe)
}

// SetSyncObserver makes every Sync that succeeds call synced afterwards; nil removes it.
func (d *Device) SetSyncObserver(synced func()) {
	if synced == nil {
		d.syncObserver.Store(nil)
		return
	}
	d.syncObserver.Store(&synced)
}

// CheckWrite runs the write gate for a write at offset. Callers that change the
// device without WriteAt, such as a discard, call it first.
func (d *Device) CheckWrite(offset int64) error {
	if issued := stoppedWrite.Load(); issued != nil {
		<-*issued
	}
	if gate := d.writeGate.Load(); gate != nil {
		return (*gate)(offset)
	}
	return nil
}

// Sync ensures all written data is persisted to the device.
// This is a global operation that does not block concurrent I/O.
func (d *Device) Sync() error {
	// Acquire shared close lock to prevent device close during sync
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	err := d.file.Sync()
	d.record(err)
	if synced := d.syncObserver.Load(); err == nil && synced != nil {
		(*synced)()
	}
	return err
}

// LastError returns the error of the last read, write or sync that failed,
// or nil once a later one succeeds.
func (d *Device) LastError() error {
	if err := d.lastErr.Load(); err != nil {
		return *err
	}
	return nil
}

func (d *Device) record(err error) {
	switch {
	case err == nil:
		if d.lastErr.Load() != nil {
			d.lastErr.Store(nil)
		}
	case !errors.Is(err, io.EOF):
		d.lastErr.Store(&err)
		if errors.Is(err, syscall.EBADE) {
			if fenced := d.fenced.Load(); fenced != nil {
				d.fencedOnce.Do(func() { (*fenced)(err) })
			}
		}
	}
}

// SetFencedHandler makes the first I/O that fails with a reservation conflict (EBADE) call fenced: another node
// revoked this node's access to the device, so none of its writes can land any more.
func (d *Device) SetFencedHandler(fenced func(error)) {
	d.fenced.Store(&fenced)
}

// Fd returns the file descriptor for use with syscalls.
func (d *Device) Fd() uintptr {
	return d.file.Fd()
}

// checkRange refuses n bytes at offset unless they fit inside the device; offsets come from on-disk numbers, which
// damage can make negative or past the end.
func (d *Device) checkRange(offset int64, n int) error {
	if offset < 0 || int64(n) > d.size || offset > d.size-int64(n) {
		return fmt.Errorf("%w: %d bytes at offset %d, device has %d", ErrOutOfRange, n, offset, d.size)
	}
	return nil
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
