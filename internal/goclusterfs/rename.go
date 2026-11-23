package goclusterfs

import (
	"context"
	"errors"
	"fmt"
	"log"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"

	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/node"
)

const renameNoReplace = 1 // RENAME_NOREPLACE; RENAME_EXCHANGE and RENAME_WHITEOUT are refused

var (
	_ fs.NodeRenamer = (*Dir)(nil)
	_ fs.NodeRenamer = (*FS)(nil)
)

// Rename moves name to newName in newParent; go-fuse moves its own node after a success.
func (d *Dir) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	return d.fs.rename(detach(ctx), d.inodeNum, name, newParent, newName, flags)
}

// Rename moves name in the root to newName in newParent.
func (f *FS) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	return f.rename(detach(ctx), inode.InodeRoot, name, newParent, newName, flags)
}

func (f *FS) rename(ctx context.Context, oldParent uint32, oldName string, newParentNode fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if flags&^renameNoReplace != 0 {
		return syscall.EINVAL
	}
	var newParent uint32
	switch p := newParentNode.(type) {
	case *Dir:
		newParent = p.inodeNum
	case *FS:
		newParent = inode.InodeRoot
	default:
		return syscall.EXDEV
	}
	if oldParent == newParent && oldName == newName {
		return 0
	}
	return retryRace("Rename", oldName, func() syscall.Errno {
		return f.renameOnce(ctx, oldParent, oldName, newParent, newName, flags)
	})
}

func (f *FS) renameOnce(ctx context.Context, oldParent uint32, oldName string, newParent uint32, newName string, flags uint32) syscall.Errno {
	oldDir, err := f.inodeTbl.Read(oldParent)
	if err != nil {
		return syscall.EIO
	}
	newDir, err := f.inodeTbl.Read(newParent)
	if err != nil {
		return syscall.EIO
	}
	child, ctype, err := f.dirMgr.Lookup(ctx, oldDir, oldName)
	if err != nil {
		return nameErrno(err)
	}
	if errno := f.childInRange(oldParent, oldName, child); errno != 0 {
		return errno
	}
	if oldParent != newParent {
		childIno, err := f.inodeTbl.Read(uint32(child))
		if err != nil {
			return syscall.EIO
		}
		if f.crossesProject(childIno, newDir) {
			return syscall.EXDEV
		}
	}
	op := &dir.RenameOp{OldParent: oldParent, NewParent: newParent, OldName: oldName, NewName: newName,
		Child: child, ChildIsDir: ctype == dir.DT_DIR, NoReplace: flags&renameNoReplace != 0}
	target, ttype, err := f.dirMgr.Lookup(ctx, newDir, newName)
	switch {
	case err == nil && target == child:
		return 0
	case err == nil && f.childInRange(newParent, newName, target) != 0:
		return syscall.EIO
	case err == nil:
		op.Target, op.TargetIsDir = target, ttype == dir.DT_DIR
	case !errors.Is(err, dir.ErrNotFound):
		return nameErrno(err)
	}

	// A file losing its last link is listed as an orphan before anything changes, as unlink does. No node holds a
	// symlink open, so a replaced symlink is freed at once.
	var live uint64
	var targetIno *inode.Inode
	if op.Target != 0 && !op.TargetIsDir && !op.NoReplace && ttype != dir.DT_LNK {
		if targetIno, err = f.inodeTbl.Read(uint32(op.Target)); err != nil {
			return syscall.EIO
		}
		if targetIno.LinkCount == 1 {
			if live, err = f.reserveOrphan(targetIno); errors.Is(err, errOrphansFull) {
				log.Printf("[WARN] rename over %q: %v", newName, err)
				return syscall.EBUSY
			} else if err != nil {
				return syscall.EIO
			}
		}
	}
	reserved := live != 0
	defer func() {
		if reserved && f.orphanReg != nil {
			o := orphanOf(targetIno)
			_ = f.orphanReg.DropOrphans(func(x node.Orphan) bool { return x == o })
		}
	}()

	var intentSeq uint64
	op.Commit = func(r *dir.Renamed) error {
		if f.journal == nil {
			return nil
		}
		seq, err := f.logRename(op, r, flags)
		intentSeq = seq
		if err == nil && afterCommit != nil {
			afterCommit("rename")
		}
		return err
	}
	op.Stuck = f.failStop
	var r *dir.Renamed
	err = f.untilReplayed(ctx, func() error {
		var err error
		r, err = f.dirMgr.Rename(ctx, op)
		if err != nil && intentSeq != 0 {
			// Only a rename that has not committed starts over.
			return fmt.Errorf("after its commit: %v", err)
		}
		return err
	})
	if err != nil && intentSeq != 0 {
		log.Printf("[WARN] rename %q to %q failed after its commit; leaving intent %d for replay: %v", oldName, newName, intentSeq, err)
		return syscall.EIO
	}
	if intentSeq != 0 {
		defer f.closeIntent(intentSeq)
	}
	if err != nil {
		return renameErrno(err)
	}
	if r == nil {
		return 0
	}
	if afterCommit != nil {
		afterCommit("rename: names changed")
	}
	for _, ino := range []*inode.Inode{r.OldParent, r.NewParent, r.Child} {
		f.invalidateInode(ino.InodeNum)
		if f.cacheMgr != nil {
			f.cacheMgr.BroadcastInvalidateInode(ino.InodeNum, ino.DataGeneration, ino.MetaGeneration)
		}
	}

	switch {
	case r.Target == nil:
	case op.TargetIsDir:
		f.freeReplacedDir(ctx, r.Target)
	default:
		if err := f.dropLink(ctx, r.Target.InodeNum, live); err != nil {
			log.Printf("[WARN] rename over %q: link of inode %d not dropped: %v", newName, r.Target.InodeNum, err)
		} else {
			reserved = false
		}
	}
	return 0
}

// logRename commits the rename and what it does to the target, before any of it reaches the device.
func (f *FS) logRename(op *dir.RenameOp, r *dir.Renamed, flags uint32) (uint64, error) {
	txn := f.journal.Begin()
	payload := &journal.DirRenamePayload{
		OldParentInodeNum: uint64(op.OldParent),
		NewParentInodeNum: uint64(op.NewParent),
		InodeNum:          op.Child,
		EntryType:         entryTypeOf(r.ChildType),
		Flags:             flags,
		OldParentGen:      r.OldParent.MetaGeneration,
		NewParentGen:      r.NewParent.MetaGeneration,
		MetaGeneration:    r.Child.MetaGeneration,
		OldName:           op.OldName,
		NewName:           op.NewName,
	}
	if r.Target != nil {
		payload.TargetInodeNum, payload.TargetGen, payload.TargetType = uint64(r.Target.InodeNum), r.Target.MetaGeneration, entryTypeOf(r.TargetType)
	}
	err := txn.LogDirRename(payload)
	switch {
	case err != nil || r.Target == nil:
	case op.TargetIsDir:
		if err = logExtentFree(txn, r.Target, 0); err == nil {
			err = txn.LogInodeDelete(&journal.InodeDeletePayload{InodeNum: uint64(r.Target.InodeNum), MetaGeneration: r.Target.MetaGeneration})
		}
	default:
		err = txn.LogLinkDrop(&journal.LinkDropPayload{InodeNum: uint64(r.Target.InodeNum), MetaGeneration: r.Target.MetaGeneration})
	}
	if err != nil {
		f.journal.Abort(txn)
		return 0, err
	}
	return f.journal.Commit(txn)
}

// freeReplacedDir frees an empty directory a rename replaced, unless it changed since the rename checked it.
func (f *FS) freeReplacedDir(ctx context.Context, target *inode.Inode) {
	var freed *inode.Inode
	ok, err := f.inodeTbl.FreeIf(ctx, target.InodeNum, func(ino *inode.Inode) bool {
		freed = ino
		return ino.MetaGeneration == target.MetaGeneration
	})
	switch {
	case err != nil:
		log.Printf("[WARN] rename: replaced directory inode %d not freed, keeping its blocks: %v", target.InodeNum, err)
	case !ok:
		log.Printf("[WARN] rename: replaced directory inode %d changed after it lost its name, keeping it", target.InodeNum)
	default:
		f.invalidateInode(target.InodeNum)
		if err := f.extentMgr.Delete(ctx, freed); err != nil {
			log.Printf("[WARN] rename: freeing the blocks of replaced directory %d failed, some may leak: %v", target.InodeNum, err)
		}
		if afterCommit != nil {
			afterCommit("rename: target freed")
		}
	}
}

func entryTypeOf(ftype uint8) journal.DirEntryType {
	switch ftype {
	case dir.DT_DIR:
		return journal.DirEntryDirectory
	case dir.DT_LNK:
		return journal.DirEntrySymlink
	}
	return journal.DirEntryRegular
}

func renameErrno(err error) syscall.Errno {
	switch {
	case errors.Is(err, dir.ErrRaced):
		return syscall.EAGAIN
	case errors.Is(err, dir.ErrNotEmpty):
		return syscall.ENOTEMPTY
	case errors.Is(err, dir.ErrNotDirectory):
		return syscall.ENOTDIR
	case errors.Is(err, dir.ErrIsDir):
		return syscall.EISDIR
	case errors.Is(err, dir.ErrIntoSelf):
		return syscall.EINVAL
	case errors.Is(err, dir.ErrNameTooLong):
		return syscall.ENAMETOOLONG
	}
	return nameErrno(err)
}
