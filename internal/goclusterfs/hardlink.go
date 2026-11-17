package goclusterfs

import (
	"context"
	"errors"
	"log"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/supporttools/GoClusterFS/internal/dir"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/journal"
)

var (
	_ fs.NodeLinker = (*Dir)(nil)
	_ fs.NodeLinker = (*FS)(nil)
)

// maxLinks caps a file's names, as ext4 does.
const maxLinks = 65000

var (
	errLinkGone     = errors.New("the file has no link left")
	errTooManyLinks = errors.New("the file has too many links")
)

// Link gives target another name in this directory.
func (d *Dir) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return d.fs.link(detach(ctx), &d.Inode, d.inodeNum, target, name, out)
}

// Link gives target another name in the root directory.
func (f *FS) Link(ctx context.Context, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	return f.link(detach(ctx), &f.Inode, inode.InodeRoot, target, name, out)
}

// link names target's file in directory parentNum too; only regular files take more links.
func (f *FS) link(ctx context.Context, parent *fs.Inode, parentNum uint32, target fs.InodeEmbedder, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	file, ok := target.(*File)
	if !ok {
		return nil, syscall.EPERM
	}
	ino, errno := f.addLink(ctx, parentNum, file, name)
	if errno != 0 {
		return nil, errno
	}
	ch := parent.NewPersistentInode(ctx, newFile(f, ino), fs.StableAttr{
		Mode: fuse.S_IFREG,
		Ino:  uint64(file.inodeNum),
		Gen:  ino.BirthGeneration,
	})
	fillEntryOut(out, ino)
	return ch, 0
}

// addLink gives file one more link and the name in directory parentNum, and returns its inode as written. A slot that
// another file took since the kernel looked the source up is ENOENT, so a link never names that file.
func (f *FS) addLink(ctx context.Context, parentNum uint32, file *File, name string) (*inode.Inode, syscall.Errno) {
	num := file.inodeNum
	dirIno, err := f.inodeTbl.Read(parentNum)
	if err != nil {
		return nil, syscall.EIO
	}
	if _, _, err := f.dirMgr.Lookup(ctx, dirIno, name); err == nil {
		return nil, syscall.EEXIST
	} else if !errors.Is(err, dir.ErrNotFound) {
		return nil, nameErrno(err)
	}
	ino, err := f.inodeTbl.Read(num)
	switch {
	case err != nil:
		return nil, syscall.EIO
	case !file.current(ino) || ino.LinkCount == 0:
		return nil, syscall.ENOENT
	case ino.LinkCount >= maxLinks:
		return nil, syscall.EMLINK
	}

	if f.journal != nil {
		seq, err := f.logLink(num, ino.MetaGeneration, parentNum, name)
		if err != nil {
			return nil, syscall.EIO
		}
		defer f.closeIntent(seq)
		if afterCommit != nil {
			afterCommit("link")
		}
	}

	// The count goes up before the name appears: a crash in between leaves an extra link, never a name for a freed file.
	now := time.Now().UnixNano()
	ino, err = f.updateInodeLocked(ctx, num, func(cur *inode.Inode) error {
		switch {
		case !file.current(cur) || cur.LinkCount == 0:
			return errLinkGone
		case cur.LinkCount >= maxLinks:
			return errTooManyLinks
		}
		cur.LinkCount++
		cur.Ctime = now
		return nil
	})
	switch {
	case errors.Is(err, errLinkGone):
		return nil, syscall.ENOENT
	case errors.Is(err, errTooManyLinks):
		return nil, syscall.EMLINK
	case err != nil:
		return nil, syscall.EIO
	}
	if afterCommit != nil {
		afterCommit("link: counted")
	}

	if err := f.dirMgr.AddAndUpdate(ctx, dirIno, name, uint64(num), dir.DT_REG, func(p *inode.Inode) { p.Mtime = now }); err != nil {
		f.dropUnnamedLink(ctx, num)
		return nil, nameErrno(err)
	}
	if afterCommit != nil {
		afterCommit("link: named")
	}
	f.cacheInode(ino)
	f.announceDir(parentNum)
	if f.cacheMgr != nil {
		f.cacheMgr.BroadcastInvalidateInode(num, ino.DataGeneration, ino.MetaGeneration)
	}
	return ino, 0
}

// logLink commits the intent to give inode num, at MetaGeneration gen, one more link and the name in parentNum.
func (f *FS) logLink(num uint32, gen uint64, parentNum uint32, name string) (uint64, error) {
	txn := f.journal.Begin()
	err := txn.LogLinkAdd(&journal.LinkAddPayload{InodeNum: uint64(num), MetaGeneration: gen})
	if err == nil {
		err = txn.LogDirAddEntry(&journal.DirAddEntryPayload{
			ParentInodeNum: uint64(parentNum),
			ChildInodeNum:  uint64(num),
			EntryType:      journal.DirEntryRegular,
			Name:           name,
		})
	}
	if err != nil {
		f.journal.Abort(txn)
		return 0, err
	}
	return f.journal.Commit(txn)
}

// dropUnnamedLink takes back the link of a name that never appeared; other nodes may have unlinked every name since.
func (f *FS) dropUnnamedLink(ctx context.Context, num uint32) {
	ino, err := f.inodeTbl.Read(num)
	var live uint64
	if err == nil && ino.LinkCount == 1 {
		live, err = f.reserveOrphan(ino)
	}
	if err == nil {
		err = f.dropLink(ctx, num, live)
	}
	if err != nil {
		log.Printf("[WARN] inode %d: keeps a link without a name, which fsck -repair takes back: %v", num, err)
	}
}
