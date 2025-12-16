package lock

import (
	"context"
)

// AcquireInode acquires a lock on an inode.
func (m *Manager) AcquireInode(ctx context.Context, inodeNum uint64, mode LockMode) error {
	return m.Acquire(ctx, LockTypeInode, inodeNum, mode)
}

// ReleaseInode releases an inode lock.
func (m *Manager) ReleaseInode(inodeNum uint64) error {
	return m.Release(LockTypeInode, inodeNum)
}

// AcquireExtent acquires a lock on an extent/block.
func (m *Manager) AcquireExtent(ctx context.Context, blockNum uint64, mode LockMode) error {
	return m.Acquire(ctx, LockTypeExtent, blockNum, mode)
}

// ReleaseExtent releases an extent lock.
func (m *Manager) ReleaseExtent(blockNum uint64) error {
	return m.Release(LockTypeExtent, blockNum)
}

// AcquireBlockBitmap acquires the block bitmap lock.
func (m *Manager) AcquireBlockBitmap(ctx context.Context, mode LockMode) error {
	return m.Acquire(ctx, LockTypeBlockBitmap, 0, mode)
}

// ReleaseBlockBitmap releases the block bitmap lock.
func (m *Manager) ReleaseBlockBitmap() error {
	return m.Release(LockTypeBlockBitmap, 0)
}

// AcquireVolume acquires the volume-level lock.
func (m *Manager) AcquireVolume(ctx context.Context, mode LockMode) error {
	return m.Acquire(ctx, LockTypeVolume, 0, mode)
}

// ReleaseVolume releases the volume lock.
func (m *Manager) ReleaseVolume() error {
	return m.Release(LockTypeVolume, 0)
}

// WithInodeLock executes a function while holding an inode lock.
func (m *Manager) WithInodeLock(ctx context.Context, inodeNum uint64, mode LockMode, fn func() error) error {
	if err := m.AcquireInode(ctx, inodeNum, mode); err != nil {
		return err
	}
	defer m.ReleaseInode(inodeNum)
	return fn()
}

// WithExclusiveInode executes a function with an exclusive inode lock.
func (m *Manager) WithExclusiveInode(ctx context.Context, inodeNum uint64, fn func() error) error {
	return m.WithInodeLock(ctx, inodeNum, LockModeExclusive, fn)
}

// WithSharedInode executes a function with a shared inode lock.
func (m *Manager) WithSharedInode(ctx context.Context, inodeNum uint64, fn func() error) error {
	return m.WithInodeLock(ctx, inodeNum, LockModeShared, fn)
}

// HeldLocks returns information about currently held locks (for debugging).
func (m *Manager) HeldLocks() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var result []string
	for key, held := range m.heldLocks {
		result = append(result, formatLockInfo(key, held))
	}
	return result
}

func formatLockInfo(key lockKey, held *heldLock) string {
	typeNames := []string{
		"Volume", "SnapshotReg", "Snapshot", "Journal",
		"RefCount", "LockRegion", "BlockBitmap", "SFBContainer",
		"Inode", "Extent",
	}
	modeNames := []string{"None", "Shared", "Exclusive"}

	typeName := "Unknown"
	if int(key.Type) < len(typeNames) {
		typeName = typeNames[key.Type]
	}

	modeName := "Unknown"
	if int(held.Mode) < len(modeNames) {
		modeName = modeNames[held.Mode]
	}

	return typeName + ":" + modeName
}
