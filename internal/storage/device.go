// Package storage provides O_DIRECT block device I/O for GoClusterFS.
//
// O_DIRECT bypasses the OS page cache, which is essential for a clustered
// filesystem where multiple nodes access the same block device. Without
// O_DIRECT, each node would have its own cached view of the data, leading
// to corruption when nodes make concurrent modifications.
package storage

import (
	"fmt"
	"os"
	"sync"
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
)

// Device represents a block device opened with O_DIRECT.
type Device struct {
	mu   sync.Mutex
	file *os.File
	path string
	size int64
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
	size, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to get device size: %w", err)
	}

	// Rewind to start
	if _, err := file.Seek(0, os.SEEK_SET); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to seek to start: %w", err)
	}

	return &Device{
		file: file,
		path: path,
		size: size,
	}, nil
}

// Close closes the device.
func (d *Device) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

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
func (d *Device) ReadAt(buf []byte, offset int64) (int, error) {
	if err := validateAlignment(buf, offset); err != nil {
		return 0, err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	return d.file.ReadAt(buf, offset)
}

// WriteAt writes data to the device at the specified offset.
// Both offset and len(buf) MUST be aligned to SectorSize.
func (d *Device) WriteAt(buf []byte, offset int64) (int, error) {
	if err := validateAlignment(buf, offset); err != nil {
		return 0, err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	return d.file.WriteAt(buf, offset)
}

// Sync ensures all written data is persisted to the device.
func (d *Device) Sync() error {
	d.mu.Lock()
	defer d.mu.Unlock()

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
