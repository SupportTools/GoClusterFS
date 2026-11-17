package dir

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"slices"
	"time"

	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/lock"
)

var (
	// ErrRaced means a name the caller looked up changed before the rename took its locks; look up again and retry.
	ErrRaced     = errors.New("a name changed since it was looked up")
	ErrIsDir     = errors.New("is a directory")
	ErrIntoSelf  = errors.New("cannot move a directory into itself")
	errNeedsRoom = errors.New("the new parent has no room for the name")
)

// RenameOp is a rename the caller looked up without locks.
type RenameOp struct {
	OldParent, NewParent uint32
	OldName, NewName     string
	Child                uint64 // what OldName named at the lookup
	ChildIsDir           bool
	Target               uint64 // what NewName named at the lookup, or 0
	TargetIsDir          bool
	NoReplace            bool
	Commit               func(*Renamed) error // runs under every lock before anything changes; an error changes nothing
	// Stuck runs under every lock when writes failed after Commit and finishing them failed too. It should stop the
	// node, so replay finishes the rename before anyone else can change its directories.
	Stuck func(error)
}

// Renamed is what a rename changes, read under its locks before the change.
type Renamed struct {
	OldParent, NewParent *inode.Inode
	Child                *inode.Inode
	Target               *inode.Inode // nil when NewName was free
	ChildType            uint8
	TargetType           uint8
	MovesDir             bool // a directory changes parent: its ".." and both link counts change
}

// Rename points NewName at Child and removes OldName. It replaces Target, which the caller frees or drops a link from.
func (m *Manager) Rename(ctx context.Context, op *RenameOp) (*Renamed, error) {
	if op.OldName == "" || op.NewName == "" {
		return nil, ErrNameEmpty
	}
	if len(op.NewName) > MaxNameLen {
		return nil, ErrNameTooLong
	}
	for attempt := 0; ; attempt++ {
		r, err := m.renameLocked(ctx, op)
		if !errors.Is(err, errNeedsRoom) {
			return r, err
		}
		if attempt >= MaxAddRetries {
			return nil, fmt.Errorf("directory %d kept filling while a rename waited for room", op.NewParent)
		}
		if err := m.grow(ctx, op.NewParent, entrySize(&DirEntry{NameLen: uint8(len(op.NewName))})); err != nil {
			return nil, err
		}
	}
}

func (m *Manager) renameLocked(ctx context.Context, op *RenameOp) (*Renamed, error) {
	dirs := []uint32{op.OldParent, op.NewParent}
	if op.ChildIsDir {
		dirs = append(dirs, uint32(op.Child))
	}
	if op.Target != 0 && op.TargetIsDir {
		dirs = append(dirs, uint32(op.Target))
	}
	defer m.lockStripes(dirs)()

	movesDir := op.ChildIsDir && op.OldParent != op.NewParent
	if m.lockMgr != nil {
		if movesDir {
			// Two moves that each pass the ancestor check could together form a cycle; the volume lock runs them one at a time.
			if err := m.lockMgr.AcquireVolume(ctx, lock.LockModeExclusive); err != nil {
				return nil, fmt.Errorf("failed to acquire the volume lock: %w", err)
			}
			defer func() { _ = m.lockMgr.ReleaseVolume() }()
		}
		inodes := []uint32{op.OldParent, op.NewParent, uint32(op.Child)}
		if op.Target != 0 {
			inodes = append(inodes, uint32(op.Target))
		}
		unlock, err := m.lockInodes(ctx, inodes)
		if err != nil {
			return nil, err
		}
		defer unlock()
	}

	p, err := m.checkRename(op, movesDir)
	if err != nil || p == nil {
		return nil, err
	}
	if err := op.Commit(p.r); err != nil {
		return nil, err
	}
	planned := map[uint32]uint64{p.r.OldParent.InodeNum: p.r.OldParent.MetaGeneration, p.r.NewParent.InodeNum: p.r.NewParent.MetaGeneration}
	failed := m.applyRename(op, p)
	if failed == nil {
		return p.r, nil
	}
	for attempt := 0; attempt < finishAttempts; attempt++ {
		err := m.finishLocked(op, p, planned)
		if err == nil {
			log.Printf("[WARN] rename %q to %q: a write failed (%v); finished the rename", op.OldName, op.NewName, failed)
			return p.r, nil
		}
		log.Printf("[WARN] rename %q to %q: a write failed (%v), and finishing it failed: %v", op.OldName, op.NewName, failed, err)
		failed = err
		time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
	}
	if op.Stuck != nil {
		replacing := ""
		if op.Target != 0 {
			replacing = fmt.Sprintf(", replacing inode %d", op.Target)
		}
		op.Stuck(fmt.Errorf("rename of %q (inode %d) in directory %d to %q in directory %d%s is half done: %w",
			op.OldName, op.Child, op.OldParent, op.NewName, op.NewParent, replacing, failed))
	}
	return nil, failed
}

// finishAttempts is how often a rename whose writes failed partway tries to finish before its node stops.
const finishAttempts = 3

// finishLocked completes a rename whose writes failed partway, under the locks it still holds, so no one else sees
// or changes the half-done names. It reads each directory again and redoes what did not land. Each parent's inode
// write landed if its generation moved past planned, the generation the rename read under its locks.
func (m *Manager) finishLocked(op *RenameOp, p *renamePlan, planned map[uint32]uint64) error {
	r := p.r
	links := map[uint32]int{}
	if r.MovesDir {
		links[r.OldParent.InodeNum]--
		links[r.NewParent.InodeNum]++
	}
	if r.Target != nil && op.TargetIsDir {
		links[r.NewParent.InodeNum]--
	}
	now := time.Now().UnixNano()
	fresh := map[uint32]*inode.Inode{}
	for num, gen := range planned {
		d, err := m.liveDirLocked(num)
		if err != nil {
			return err
		}
		if d.MetaGeneration == gen {
			d.LinkCount = uint32(int(d.LinkCount) + links[num])
			d.Mtime, d.Ctime = now, now
			if err := m.inodeTbl.WriteWithLockHeld(d); err != nil {
				m.indexes.drop(num)
				return fmt.Errorf("failed to write directory inode %d: %w", num, err)
			}
		}
		fresh[num] = d
	}
	r.OldParent, r.NewParent = fresh[r.OldParent.InodeNum], fresh[r.NewParent.InodeNum]

	if r.MovesDir {
		child, err := m.liveDirLocked(uint32(op.Child))
		if err != nil {
			return err
		}
		x, err := m.indexOf(child)
		if err != nil {
			return err
		}
		dots, found, _ := x.find(child.MetaGeneration, "..")
		if !found {
			return fmt.Errorf("%w: directory %d has no \"..\"", ErrCorrupted, child.InodeNum)
		}
		if dots.child != uint64(op.NewParent) {
			if _, err := m.editChunk(child, x, dots.chunk,
				func(chunk []byte) bool { return pointName(chunk, "..", uint64(op.NewParent), DT_DIR) },
				func(names map[string]slot) {
					names[".."] = slot{child: uint64(op.NewParent), chunk: dots.chunk, ftype: DT_DIR}
				}, nil); err != nil {
				return err
			}
		}
		r.Child = child
	}

	newX, err := m.indexOf(r.NewParent)
	if err != nil {
		return err
	}
	if s, found, _ := newX.find(r.NewParent.MetaGeneration, op.NewName); !found || s.child != op.Child {
		named := slot{child: op.Child, chunk: p.at, ftype: r.ChildType}
		place := func(chunk []byte) bool {
			entry := &DirEntry{InodeNum: op.Child, NameLen: uint8(len(op.NewName)), FileType: r.ChildType, Name: op.NewName}
			entry.RecLen = entrySize(entry)
			return addToChunk(chunk, entry)
		}
		if found {
			named.chunk = s.chunk
			place = func(chunk []byte) bool { return pointName(chunk, op.NewName, op.Child, r.ChildType) }
		}
		if placed, err := m.rewriteChunk(r.NewParent, newX, named.chunk, place, func(names map[string]slot) { names[op.NewName] = named }); err != nil || !placed {
			return cmp.Or(err, fmt.Errorf("%w: chunk %d of directory %d no longer takes %q", ErrCorrupted, named.chunk, r.NewParent.InodeNum, op.NewName))
		}
	}

	oldX, err := m.indexOf(r.OldParent)
	if err != nil {
		return err
	}
	if s, found, _ := oldX.find(r.OldParent.MetaGeneration, op.OldName); found && s.child == op.Child {
		if removed, err := m.rewriteChunk(r.OldParent, oldX, s.chunk,
			func(chunk []byte) bool { return removeFromChunk(chunk, op.OldName) },
			func(names map[string]slot) { delete(names, op.OldName) }); err != nil || !removed {
			return cmp.Or(err, fmt.Errorf("%w: %q is in the index of directory %d but not in its chunk", ErrCorrupted, op.OldName, r.OldParent.InodeNum))
		}
	}
	return nil
}

// renamePlan is what checkRename found under the locks: where each name is, and the chunk with room for NewName.
type renamePlan struct {
	r            *Renamed
	oldX, newX   *index
	child, named slot
	at           uint32
}

// checkRename rereads what op's lookups found; a nil plan means both names already name the child.
func (m *Manager) checkRename(op *RenameOp, movesDir bool) (*renamePlan, error) {
	oldDir, err := m.liveDirLocked(op.OldParent)
	if errors.Is(err, ErrRemoved) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	newDir := oldDir
	if op.NewParent != op.OldParent {
		if newDir, err = m.liveDirLocked(op.NewParent); errors.Is(err, ErrRemoved) {
			return nil, ErrNotFound
		} else if err != nil {
			return nil, err
		}
	}
	oldX, err := m.indexOf(oldDir)
	if err != nil {
		return nil, err
	}
	newX, err := m.indexOf(newDir)
	if err != nil {
		return nil, err
	}

	cs, found, _ := oldX.find(oldDir.MetaGeneration, op.OldName)
	switch {
	case !found:
		return nil, ErrNotFound
	case cs.child != op.Child || (cs.ftype == DT_DIR) != op.ChildIsDir:
		return nil, ErrRaced
	}
	ts, replacing, _ := newX.find(newDir.MetaGeneration, op.NewName)
	switch {
	case replacing && ts.child == cs.child:
		return nil, nil
	case replacing && op.NoReplace:
		return nil, ErrExists
	case replacing != (op.Target != 0), replacing && (ts.child != op.Target || (ts.ftype == DT_DIR) != op.TargetIsDir):
		return nil, ErrRaced
	case replacing && op.ChildIsDir && !op.TargetIsDir:
		return nil, ErrNotDirectory
	case replacing && !op.ChildIsDir && op.TargetIsDir:
		return nil, ErrIsDir
	}

	r := &Renamed{OldParent: oldDir, NewParent: newDir, ChildType: cs.ftype, MovesDir: movesDir}
	p := &renamePlan{r: r, oldX: oldX, newX: newX, child: cs, named: ts}
	if r.Child, err = m.liveInode(op.Child, op.ChildIsDir); err != nil {
		return nil, err
	}
	if replacing {
		r.TargetType = ts.ftype
		if r.Target, err = m.liveInode(op.Target, op.TargetIsDir); err != nil {
			return nil, err
		}
		if op.TargetIsDir {
			if empty, err := m.emptyLocked(r.Target); err != nil || !empty {
				return nil, cmp.Or(err, ErrNotEmpty)
			}
		}
	}
	if movesDir {
		if inside, err := m.isAncestor(uint32(op.Child), op.NewParent); err != nil || inside {
			return nil, cmp.Or(err, ErrIntoSelf)
		}
	}
	if !replacing {
		var ok bool
		if p.at, ok = newX.chunkWithRoom(entrySize(&DirEntry{NameLen: uint8(len(op.NewName))})); !ok {
			return nil, errNeedsRoom
		}
	}
	return p, nil
}

// applyRename writes both parents before any chunk, so a reader that sees a changed entry sees a changed generation.
func (m *Manager) applyRename(op *RenameOp, p *renamePlan) error {
	r, oldDir, newDir := p.r, p.r.OldParent, p.r.NewParent
	if r.MovesDir {
		oldDir.LinkCount--
		newDir.LinkCount++
	}
	if r.Target != nil && op.TargetIsDir {
		newDir.LinkCount--
	}
	now := time.Now().UnixNano()
	for _, d := range slices.Compact([]*inode.Inode{oldDir, newDir}) {
		d.Mtime, d.Ctime = now, now
		if err := m.inodeTbl.WriteWithLockHeld(d); err != nil {
			m.indexes.drop(oldDir.InodeNum)
			m.indexes.drop(newDir.InodeNum)
			return fmt.Errorf("failed to write directory inode %d: %w", d.InodeNum, err)
		}
	}
	if r.MovesDir {
		r.Child.Ctime = now
		childX, err := m.indexOf(r.Child)
		if err != nil {
			return err
		}
		dots, found, _ := childX.find(r.Child.MetaGeneration, "..")
		if !found {
			return fmt.Errorf("%w: directory %d has no \"..\"", ErrCorrupted, r.Child.InodeNum)
		}
		if _, err := m.editChunk(r.Child, childX, dots.chunk,
			func(chunk []byte) bool { return pointName(chunk, "..", uint64(newDir.InodeNum), DT_DIR) },
			func(names map[string]slot) {
				names[".."] = slot{child: uint64(newDir.InodeNum), chunk: dots.chunk, ftype: DT_DIR}
			}, nil); err != nil {
			return err
		}
	}

	entry := &DirEntry{InodeNum: op.Child, NameLen: uint8(len(op.NewName)), FileType: r.ChildType, Name: op.NewName}
	entry.RecLen = entrySize(entry)
	at, place := p.at, func(chunk []byte) bool { return addToChunk(chunk, entry) }
	if r.Target != nil {
		at, place = p.named.chunk, func(chunk []byte) bool { return pointName(chunk, op.NewName, op.Child, r.ChildType) }
	}
	named := slot{child: op.Child, chunk: at, ftype: r.ChildType}
	oldX, newX, cs := p.oldX, p.newX, p.child
	if oldDir == newDir && cs.chunk == at {
		_, err := m.rewriteChunk(newDir, newX, at,
			func(chunk []byte) bool { return place(chunk) && removeFromChunk(chunk, op.OldName) },
			func(names map[string]slot) { names[op.NewName] = named; delete(names, op.OldName) })
		return err
	}
	if _, err := m.rewriteChunk(newDir, newX, at, place, func(names map[string]slot) { names[op.NewName] = named }); err != nil {
		return err
	}
	_, err := m.rewriteChunk(oldDir, oldX, cs.chunk,
		func(chunk []byte) bool { return removeFromChunk(chunk, op.OldName) },
		func(names map[string]slot) { delete(names, op.OldName) })
	return err
}

// lockStripes takes each local stripe of dirs once, in stripe order, since a stripe is not reentrant.
func (m *Manager) lockStripes(dirs []uint32) func() {
	stripes := make([]uint32, 0, len(dirs))
	for _, d := range dirs {
		stripes = append(stripes, d%dirStripes)
	}
	slices.Sort(stripes)
	stripes = slices.Compact(stripes)
	for _, s := range stripes {
		m.dirMu[s].Lock()
	}
	return func() {
		for _, s := range slices.Backward(stripes) {
			m.dirMu[s].Unlock()
		}
	}
}

// lockInodes takes exclusive inode locks on nums in lock-record order, so nodes never wait on each other in a cycle.
func (m *Manager) lockInodes(ctx context.Context, nums []uint32) (func(), error) {
	slices.SortFunc(nums, func(a, b uint32) int {
		return cmp.Or(cmp.Compare(m.lockMgr.Record(lock.LockTypeInode, uint64(a)), m.lockMgr.Record(lock.LockTypeInode, uint64(b))), cmp.Compare(a, b))
	})
	nums = slices.Compact(nums)
	held := 0
	unlock := func() {
		for _, n := range slices.Backward(nums[:held]) {
			m.unlock(n)
		}
	}
	for _, n := range nums {
		if err := m.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(n), lock.LockModeExclusive); err != nil {
			unlock()
			return nil, fmt.Errorf("failed to acquire the lock of inode %d: %w", n, err)
		}
		held++
	}
	return unlock, nil
}

// liveInode rereads num under its lock; a different type or a freed slot means the name changed since the lookup.
func (m *Manager) liveInode(num uint64, isDir bool) (*inode.Inode, error) {
	ino, err := m.inodeTbl.Read(uint32(num))
	if err != nil {
		return nil, fmt.Errorf("failed to read inode %d: %w", num, err)
	}
	if ino.Magic != inode.InodeMagic || ino.IsDir() != isDir {
		return nil, ErrRaced
	}
	return ino, nil
}

func (m *Manager) emptyLocked(dirIno *inode.Inode) (bool, error) {
	x, err := m.indexOf(dirIno)
	if err != nil {
		return false, err
	}
	x.mu.RLock()
	defer x.mu.RUnlock()
	for name := range x.names {
		if name != "." && name != ".." {
			return false, nil
		}
	}
	return true, nil
}

// isAncestor reports whether directory dir is of or above it; the volume lock keeps every ".." on the way still.
func (m *Manager) isAncestor(dir, of uint32) (bool, error) {
	cur := of
	for range 1 << 16 {
		if cur == dir {
			return true, nil
		}
		if cur == inode.InodeRoot {
			return false, nil
		}
		parent, err := m.parentOf(cur)
		if err != nil {
			return false, err
		}
		cur = parent
	}
	return false, fmt.Errorf("%w: no path to the root from directory %d", ErrCorrupted, of)
}

// parentOf reads the ".." of directory num, which InitDirectory puts in its first chunk.
func (m *Manager) parentOf(num uint32) (uint32, error) {
	ino, err := m.inodeTbl.Read(num)
	if err != nil {
		return 0, fmt.Errorf("failed to read directory inode %d: %w", num, err)
	}
	if ino.Magic != inode.InodeMagic || !ino.IsDir() {
		return 0, fmt.Errorf("%w: inode %d on the way to the root is not a directory", ErrCorrupted, num)
	}
	physBlock, err := m.physicalBlock(ino, 0)
	if err != nil {
		return 0, err
	}
	chunk, err := m.readChunk(physBlock, 0)
	if err != nil {
		return 0, err
	}
	var parent uint64
	walkChunk(chunk, func(_ int, e *DirEntry) bool {
		if e.InodeNum != 0 && e.Name == ".." {
			parent = e.InodeNum
			return true
		}
		return false
	})
	if parent == 0 {
		return 0, fmt.Errorf("%w: directory %d has no \"..\"", ErrCorrupted, num)
	}
	return uint32(parent), nil
}

// pointName makes the live entry name in chunk name child, of type ftype.
func pointName(chunk []byte, name string, child uint64, ftype uint8) bool {
	pointed := false
	walkChunk(chunk, func(offset int, e *DirEntry) bool {
		if e.InodeNum == 0 || e.Name != name {
			return false
		}
		binary.LittleEndian.PutUint64(chunk[offset:], child)
		chunk[offset+11] = ftype
		pointed = true
		return true
	})
	return pointed
}

// grow gives directory num an empty block unless it already has a chunk with room for need.
func (m *Manager) grow(ctx context.Context, num uint32, need uint16) error {
	defer m.LockDir(num)()
	for attempt := 0; attempt < MaxAddRetries; attempt++ {
		fresh, err := m.liveDirLocked(num)
		if err != nil {
			return err
		}
		err = m.appendBlock(ctx, fresh,
			func(next uint64) (*inode.Inode, bool, error) {
				cur, err := m.liveDirLocked(num)
				if err != nil || extent.BlocksForSize(cur.Size) <= next {
					return cur, false, err
				}
				x, err := m.indexOf(cur)
				if err != nil {
					return nil, false, err
				}
				_, ok := x.chunkWithRoom(need)
				return cur, ok, nil
			},
			func([]byte) {}, nil)
		if !errors.Is(err, errRetryAdd) {
			return err
		}
	}
	return fmt.Errorf("directory %d kept changing while it grew", num)
}

// PointIf makes name in dirIno name child, of type ftype, if ok accepts the inode it names now; ok runs under the directory lock.
func (m *Manager) PointIf(ctx context.Context, dirIno *inode.Inode, name string, child uint64, ftype uint8, ok func(cur uint64) bool) (bool, error) {
	if dirIno.Magic != inode.InodeMagic {
		return false, ErrRemoved
	}
	defer m.LockDir(dirIno.InodeNum)()
	if m.lockMgr != nil {
		if err := m.lockMgr.Acquire(ctx, lock.LockTypeInode, uint64(dirIno.InodeNum), lock.LockModeExclusive); err != nil {
			return false, fmt.Errorf("failed to acquire directory lock: %w", err)
		}
		defer m.unlock(dirIno.InodeNum)
	}
	fresh, err := m.liveDirLocked(dirIno.InodeNum)
	if err != nil {
		return false, err
	}
	x, err := m.indexOf(fresh)
	if err != nil {
		return false, err
	}
	s, found, _ := x.find(fresh.MetaGeneration, name)
	if !found {
		return false, ErrNotFound
	}
	if !ok(s.child) {
		return false, nil
	}
	return m.editChunk(fresh, x, s.chunk,
		func(chunk []byte) bool { return pointName(chunk, name, child, ftype) },
		func(names map[string]slot) { names[name] = slot{child: child, chunk: s.chunk, ftype: ftype} }, nil)
}
