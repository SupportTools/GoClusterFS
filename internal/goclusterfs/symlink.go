package goclusterfs

import (
	"bytes"
	"context"
	"errors"
	"log"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/logging"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

// Link is a symlink's node; it has no xattr ops, so no snapshot or clone starts from one.
type Link struct {
	fs.Inode

	fs       *FS
	inodeNum uint32
	birth    uint64
}

var (
	_ = (fs.NodeReadlinker)((*Link)(nil))
	_ = (fs.NodeGetattrer)((*Link)(nil))
	_ = (fs.NodeSetattrer)((*Link)(nil))
	_ = (fs.NodeSymlinker)((*Dir)(nil))
	_ = (fs.NodeSymlinker)((*FS)(nil))
)

var errNotSymlink = errors.New("inode no longer holds this symlink")

// SetSymlinks lets the FS create symlinks; only volumes formatted with RequiredSymlinks allow them.
func (f *FS) SetSymlinks(enabled bool) {
	f.symlinks = enabled
}

// Symlink makes name in the root directory point at target.
func (f *FS) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return f.symlink(detach(ctx), &f.Inode, inode.InodeRoot, target, name, out)
}

// Symlink makes name in this directory point at target.
func (d *Dir) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return d.fs.symlink(detach(ctx), &d.Inode, d.inodeNum, target, name, out)
}

func (f *FS) symlink(ctx context.Context, parent *fs.Inode, parentNum uint32, target, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	switch {
	case !f.symlinks:
		f.noSymlinks.Do(func() {
			log.Printf("[WARN] this volume was formatted before symlinks existed; format it again to create them")
		})
		return nil, syscall.EOPNOTSUPP
	case len(target) == 0:
		return nil, syscall.ENOENT
	case len(target) > inode.MaxSymlinkTarget:
		return nil, syscall.ENAMETOOLONG
	}
	var node *fs.Inode
	errno := retryRace("Symlink", name, func() (errno syscall.Errno) {
		node, errno = f.symlinkOnce(ctx, parent, parentNum, []byte(target), name, out)
		return errno
	})
	return node, errno
}

// symlinkOnce returns EAGAIN when another node claims the free inode slot it picked first.
func (f *FS) symlinkOnce(ctx context.Context, parent *fs.Inode, parentNum uint32, target []byte, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	var block uint64
	reserved := false
	// giveBack undoes the target block if the symlink never gets its name.
	var giveBack func()
	defer func() {
		if giveBack != nil {
			giveBack()
		}
	}()
	freeBlock := func() { f.freeUnused(ctx, 0, block) }
	if len(target) > inode.MaxInlineTarget {
		var errno syscall.Errno
		if block, reserved, errno = f.symlinkBlock(ctx, target); errno != 0 {
			return nil, errno
		}
		giveBack = freeBlock
		if reserved {
			giveBack = func() { f.reserve.Untake(block) }
		}
	}

	inodeNum, err := f.inodeTbl.FindFreeSlot()
	if err != nil {
		logging.Debugf("Symlink %q: FindFreeSlot failed: %v", name, err)
		return nil, syscall.ENOSPC
	}
	defer f.inodeTbl.Release(inodeNum)
	dirIno, err := f.inodeTbl.Read(parentNum)
	if err != nil {
		logging.Debugf("Symlink %q: Read parent inode %d failed: %v", name, parentNum, err)
		return nil, syscall.EIO
	}
	uid, gid, _ := f.newOwner(ctx, dirIno)
	project := childProject(dirIno)
	if errno := f.checkQuota(ctx, project, createBlocks(journal.DirEntrySymlink, &journal.SymlinkTargetPayload{DiskBlock: block}), 1, true); errno != 0 {
		return nil, errno
	}

	// takeBack gives back the charge of a create that failed after its commit. It runs after the deferred closeIntent,
	// so replay can no longer finish the create it undoes.
	var takeBack func()
	defer func() {
		if takeBack != nil {
			takeBack()
		}
	}()
	if f.journal != nil {
		seq, err := f.logSymlink(inodeNum, parentNum, name, uid, gid, project, target, block, reserved)
		if err != nil {
			logging.Debugf("Symlink %q: journal commit failed: %v", name, err)
			// A full journal wrote nothing and Logged put a reserved block back; other failures may leave the record on disk.
			if !errors.Is(err, journal.ErrJournalFull) || reserved {
				giveBack = nil
			}
			return nil, syscall.EIO
		}
		// Replay must not map a block this node already freed, so a failure ends the intent durably before freeing it.
		defer func() {
			if giveBack == nil || block == 0 {
				f.closeIntent(seq)
				return
			}
			if err := f.journal.CompleteIntentDurable(seq); err != nil {
				log.Printf("[WARN] symlink %q: leaving block %d allocated, since ending intent %d failed: %v", name, block, seq, err)
				giveBack = nil
			}
		}()
		if block != 0 {
			giveBack = freeBlock
		}
		if afterCommit != nil {
			afterCommit("symlink")
		}
	}

	var blocks int64
	if block != 0 {
		blocks = 1
	}
	uncharge := func() { takeBack = func() { f.commitCharge(project, -blocks, -1) } }
	childIno, err := f.inodeTbl.ClaimSlot(ctx, inodeNum)
	if err != nil {
		logging.Debugf("Symlink %q: ClaimSlot %d failed: %v", name, inodeNum, err)
		uncharge()
		return nil, claimErrno(err)
	}
	now := time.Now().UnixNano()
	childIno.Mode = inode.S_IFLNK | 0o777
	childIno.UID = uid
	childIno.GID = gid
	joinProject(childIno, project)
	childIno.LinkCount = 1
	childIno.Atime = now
	childIno.Mtime = now
	childIno.Ctime = now
	if block == 0 {
		childIno.SetInlineTarget(target)
	} else {
		ext := &extent.Extent{PhysicalBlock: block, Length: 1, Flags: extent.FlagAllocated}
		if err := f.extentMgr.Insert(ctx, childIno, ext); err != nil {
			logging.Debugf("Symlink %q: mapping block %d failed: %v", name, block, err)
			f.freeNewInode(ctx, childIno.InodeNum)
			uncharge()
			return nil, syscall.EIO
		}
		childIno.Size = uint64(len(target))
		childIno.AllocatedBlocks, childIno.WrittenBlocks = 1, 1
	}
	if err := f.inodeTbl.Write(ctx, childIno); err != nil {
		logging.Debugf("Symlink %q: Write inode %d failed: %v", name, childIno.InodeNum, err)
		f.freeNewInode(ctx, childIno.InodeNum)
		uncharge()
		return nil, syscall.EIO
	}
	if afterCommit != nil {
		afterCommit("symlink: inode claimed")
	}

	if err := f.dirMgr.AddAndUpdate(ctx, dirIno, name, uint64(inodeNum), dir.DT_LNK, func(p *inode.Inode) { p.Mtime = now }); err != nil {
		logging.Debugf("Symlink %q: dirMgr.Add failed: %v", name, err)
		f.freeNewInode(ctx, childIno.InodeNum)
		uncharge()
		return nil, nameErrno(err)
	}
	giveBack = nil
	f.cacheInode(childIno)
	f.announceDir(parentNum)
	fillEntryOut(out, childIno, f.blockSize)
	return f.linkNode(ctx, parent, childIno, out), 0
}

// symlinkBlock writes a long target into a new data block, zeroing the rest of the block.
func (f *FS) symlinkBlock(ctx context.Context, target []byte) (uint64, bool, syscall.Errno) {
	var b uint64
	reserved := false
	if f.reserve != nil {
		b, reserved = f.reserve.Take(ctx)
	}
	if !reserved {
		blocks, err := f.bitmapMgr.Alloc(ctx, 1)
		if err != nil {
			return 0, false, allocErrno(err)
		}
		b = blocks[0]
	}
	buf := storage.GetBuffer(int(f.blockSize))
	defer storage.PutBuffer(buf)
	block := buf.Bytes()[:f.blockSize]
	clear(block)
	copy(block, target)
	if _, err := f.dev.WriteAt(block, int64(f.dataOffset+b*f.blockSize)); err != nil {
		logging.Debugf("Symlink: writing the target to block %d failed: %v", b, err)
		if reserved {
			f.reserve.Untake(b)
		} else {
			f.freeUnused(ctx, 0, b)
		}
		return 0, false, syscall.EIO
	}
	return b, reserved, 0
}

func (f *FS) logSymlink(num, parentNum uint32, name string, uid, gid, project uint32, target []byte, block uint64, reserved bool) (uint64, error) {
	link := &journal.SymlinkTargetPayload{InodeNum: uint64(num), DiskBlock: block, Target: target}
	txn, err := f.createTxn(num, parentNum, name, uint32(inode.S_IFLNK|0o777), uid, gid, project, journal.DirEntrySymlink, link)
	if err != nil {
		return 0, err
	}
	commit := func() (uint64, error) {
		seq, err := f.journal.Commit(txn)
		if err != nil {
			f.journal.Abort(txn)
		}
		return seq, err
	}
	if !reserved {
		return commit()
	}
	if err := txn.LogBlockUse([]uint64{block}); err != nil {
		f.journal.Abort(txn)
		return 0, err
	}
	var seq uint64
	err = f.reserve.Logged([]uint64{block}, func() error {
		var err error
		seq, err = commit()
		return err
	})
	return seq, err
}

func (f *FS) linkNode(ctx context.Context, parent *fs.Inode, ino *inode.Inode, out *fuse.EntryOut) *fs.Inode {
	fillLinkAttr(&out.Attr, ino, f.blockSize)
	return parent.NewPersistentInode(ctx, &Link{fs: f, inodeNum: ino.InodeNum, birth: ino.BirthGeneration}, fs.StableAttr{
		Mode: fuse.S_IFLNK,
		Ino:  uint64(ino.InodeNum),
		Gen:  ino.BirthGeneration,
	})
}

// current reports whether the slot still holds this node's symlink; birth 0, from older versions, matches any.
func (l *Link) current(ino *inode.Inode) bool {
	return ino.Magic == inode.InodeMagic && ino.IsSymlink() && (l.birth == 0 || ino.BirthGeneration == l.birth)
}

func fillLinkAttr(out *fuse.Attr, ino *inode.Inode, blockSize uint64) {
	out.Ino = uint64(ino.InodeNum)
	out.Mode = fuse.S_IFLNK | 0o777
	out.Nlink = ino.LinkCount
	out.Uid = ino.UID
	out.Gid = ino.GID
	out.Size = ino.Size
	out.Atime, out.Atimensec = uint64(ino.Atime/1e9), uint32(ino.Atime%1e9)
	out.Mtime, out.Mtimensec = uint64(ino.Mtime/1e9), uint32(ino.Mtime%1e9)
	out.Ctime, out.Ctimensec = uint64(ino.Ctime/1e9), uint32(ino.Ctime%1e9)
	out.Blocks = ino.AllocatedBlocks * (blockSize / 512)
	out.Blksize = uint32(blockSize)
}

// readLink reads a symlink's inode; ESTALE means its slot now holds something else.
func (l *Link) readLink() (*inode.Inode, syscall.Errno) {
	ino, err := l.fs.inodeTbl.Read(l.inodeNum)
	if err != nil {
		logging.Debugf("Link inode %d: read failed: %v", l.inodeNum, err)
		return nil, syscall.EIO
	}
	if !l.current(ino) {
		return nil, syscall.ESTALE
	}
	return ino, 0
}

func (l *Link) Readlink(ctx context.Context) ([]byte, syscall.Errno) {
	ino, errno := l.readLink()
	if errno != 0 {
		return nil, errno
	}
	return l.fs.symlinkTarget(ino)
}

func (f *FS) symlinkTarget(ino *inode.Inode) ([]byte, syscall.Errno) {
	if ino.HasInlineTarget() {
		return bytes.Clone(ino.InlineTarget()), 0
	}
	if ino.Size == 0 || ino.Size > inode.MaxSymlinkTarget {
		log.Printf("[WARN] symlink inode %d: target length %d is out of range", ino.InodeNum, ino.Size)
		return nil, syscall.EIO
	}
	phys, err := f.lookupPhysical(ino, 0)
	if err != nil || phys == extent.Hole {
		log.Printf("[WARN] symlink inode %d: target block not found: %v", ino.InodeNum, err)
		return nil, syscall.EIO
	}
	buf := storage.GetBuffer(storage.SectorSize)
	defer storage.PutBuffer(buf)
	if _, err := f.dev.ReadAt(buf.Bytes(), int64(f.dataOffset+phys*f.blockSize)); err != nil {
		return nil, syscall.EIO
	}
	return bytes.Clone(buf.Bytes()[:ino.Size]), 0
}

func (l *Link) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	ino, errno := l.readLink()
	if errno != 0 {
		return errno
	}
	out.AttrValid = 0
	fillLinkAttr(&out.Attr, ino, l.fs.blockSize)
	return 0
}

// Setattr applies lchown and lutimes. Linux has no lchmod, and a symlink cannot be truncated.
func (l *Link) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	ctx = detach(ctx)
	if _, ok := in.GetSize(); ok {
		return syscall.EINVAL
	}
	if _, ok := in.GetMode(); ok {
		return syscall.EOPNOTSUPP
	}
	now := time.Now().UnixNano()
	updated, err := l.fs.updateInodeLocked(ctx, l.inodeNum, func(ino *inode.Inode) error {
		if !l.current(ino) {
			return errNotSymlink
		}
		if uid, ok := in.GetUID(); ok {
			ino.UID = uid
			ino.Ctime = now
		}
		if gid, ok := in.GetGID(); ok {
			ino.GID = gid
			ino.Ctime = now
		}
		if atime, ok := in.GetATime(); ok {
			ino.Atime = atime.UnixNano()
		}
		if mtime, ok := in.GetMTime(); ok {
			ino.Mtime = mtime.UnixNano()
		}
		return nil
	})
	switch {
	case errors.Is(err, errNotSymlink):
		return syscall.ESTALE
	case err != nil:
		logging.Debugf("Setattr symlink inode %d failed: %v", l.inodeNum, err)
		return syscall.EIO
	}
	if l.fs.cacheMgr != nil {
		l.fs.cacheMgr.BroadcastInvalidateInode(l.inodeNum, updated.DataGeneration, updated.MetaGeneration)
	}
	l.fs.cacheInode(updated)
	out.AttrValid = 0
	fillLinkAttr(&out.Attr, updated, l.fs.blockSize)
	return 0
}
