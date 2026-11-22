package goclusterfs

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"

	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/quota"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

// quotaXattr sets and reads a directory's project quota. The kernel lets only CAP_SYS_ADMIN change trusted.* names.
const quotaXattr = "trusted.goclusterfs.quota"

var (
	_ fs.NodeGetxattrer    = (*Dir)(nil)
	_ fs.NodeSetxattrer    = (*Dir)(nil)
	_ fs.NodeRemovexattrer = (*Dir)(nil)
	_ fs.NodeListxattrer   = (*Dir)(nil)
)

// SetQuotaTable names the inode that holds the volume's quota table and the volume's block size, in which usage and
// limits count; num 0, the default, leaves quotas off.
func (f *FS) SetQuotaTable(num uint32, blockSize uint64) {
	f.quotaInode, f.quotaBlockSize = num, blockSize
}

func (f *FS) dataBlockSize() uint64 { return f.quotaBlockSize }

// childProject is the project a new inode in directory dir takes.
func childProject(dir *inode.Inode) uint32 {
	if dir.Flags&inode.FlagProjInherit != 0 {
		return dir.ProjectID
	}
	return 0
}

// joinProject gives a new inode project p, and a new directory PROJINHERIT too.
func joinProject(ino *inode.Inode, p uint32) {
	ino.ProjectID = p
	if p != 0 && ino.IsDir() {
		ino.Flags |= inode.FlagProjInherit
	}
}

// quotaTable maps the quota inode's blocks for table I/O.
func (f *FS) quotaTable() (quota.Table, error) {
	qi, err := f.inodeTbl.Read(f.quotaInode)
	if err != nil {
		return quota.Table{}, err
	}
	if qi.HasExtentTree() {
		return quota.Table{}, fmt.Errorf("quota inode %d maps its table through an extent tree, which this build does not read", f.quotaInode)
	}
	blockSize := f.dataBlockSize()
	perBlock := blockSize / storage.SectorSize
	physical := map[uint64]uint64{}
	for _, x := range qi.InlineExtents {
		for i := uint64(0); i < x.Length; i++ {
			physical[x.LogicalBlock+i] = x.PhysicalBlock + i
		}
	}
	return quota.Table{
		Dev:     f.dev,
		Sectors: uint64(len(physical)) * perBlock,
		At: func(sector uint64) (int64, error) {
			block, ok := physical[sector/perBlock]
			if !ok {
				return 0, fmt.Errorf("quota table sector %d is past the table's blocks", sector)
			}
			return int64(f.dataOffset + block*blockSize + sector%perBlock*storage.SectorSize), nil
		},
	}, nil
}

// quotaSetting is the value of quotaXattr: limits in bytes and inodes, 0 for none.
type quotaSetting struct {
	bytes, inodes uint64
}

func parseQuotaSetting(value string) (quotaSetting, error) {
	var q quotaSetting
	seen := map[string]bool{}
	for _, field := range strings.Fields(value) {
		key, num, ok := strings.Cut(field, "=")
		n, err := strconv.ParseUint(num, 10, 64)
		if !ok || err != nil || seen[key] {
			return q, fmt.Errorf("%q: want bytes=N inodes=N", field)
		}
		seen[key] = true
		switch key {
		case "bytes":
			q.bytes = n
		case "inodes":
			q.inodes = n
		default:
			return q, fmt.Errorf("%q: want bytes=N inodes=N", field)
		}
	}
	if len(seen) == 0 {
		return q, errors.New("empty: want bytes=N inodes=N")
	}
	return q, nil
}

// projectRoot reads the live directory num and, when it has a project, that project's record. It returns EPERM for
// a directory inside a project that is not the project's root.
func (f *FS) projectRoot(num uint32, tbl quota.Table) (*inode.Inode, *quota.Record, syscall.Errno) {
	ino, err := f.inodeTbl.Read(num)
	switch {
	case err != nil:
		return nil, nil, syscall.EIO
	case ino.Magic != inode.InodeMagic || !ino.IsDir():
		return nil, nil, syscall.ENOENT
	case ino.ProjectID == 0:
		return ino, nil, 0
	}
	rec, err := tbl.Record(ino.ProjectID)
	if err != nil {
		log.Printf("[ERROR] quota: project %d of directory inode %d: %v", ino.ProjectID, num, err)
		return nil, nil, syscall.EIO
	}
	if rec.Flags&quota.RecordInUse == 0 || rec.RootInode != num || rec.RootBirth != ino.BirthGeneration {
		return ino, nil, syscall.EPERM
	}
	return ino, rec, 0
}

// setQuota gives empty directory num a new project with the limits in value, or changes the limits of the project
// whose root it is. The record goes in before the directory names it, so a crash leaves at worst an unused record.
func (f *FS) setQuota(ctx context.Context, num uint32, value string) syscall.Errno {
	switch {
	case f.quotaInode == 0:
		return syscall.ENOTSUP
	case num == inode.InodeRoot:
		return syscall.EPERM
	}
	setting, err := parseQuotaSetting(value)
	if err != nil {
		log.Printf("[WARN] %s on directory inode %d: %v", quotaXattr, num, err)
		return syscall.EINVAL
	}
	blockSize := f.dataBlockSize()
	limitBlocks := setting.bytes / blockSize
	if setting.bytes%blockSize != 0 {
		limitBlocks++
	}
	unlock, err := f.lockFileIO(ctx, f.quotaInode, lock.LockModeExclusive)
	if err != nil {
		return errToErrno(err)
	}
	defer unlock()
	tbl, err := f.quotaTable()
	if err != nil {
		log.Printf("[ERROR] quota: %v", err)
		return syscall.EIO
	}
	ino, rec, errno := f.projectRoot(num, tbl)
	if errno != 0 {
		return errno
	}
	if rec != nil {
		rec.LimitBlocks, rec.LimitInodes = limitBlocks, setting.inodes
		if err := tbl.PutRecord(ino.ProjectID, rec); err != nil {
			log.Printf("[ERROR] quota: project %d: %v", ino.ProjectID, err)
			return syscall.EIO
		}
		return 0
	}

	if empty, err := f.dirMgr.IsEmpty(ctx, ino); err != nil {
		return syscall.EIO
	} else if !empty {
		return syscall.ENOTEMPTY
	}
	h, err := tbl.Header()
	if err != nil {
		log.Printf("[ERROR] quota: %v", err)
		return syscall.EIO
	}
	id, err := tbl.FreeID(h.NextProjectID)
	if errors.Is(err, quota.ErrTableFull) {
		log.Printf("[WARN] quota: no project for directory inode %d: %v", num, err)
		return syscall.ENOSPC
	} else if err != nil {
		log.Printf("[ERROR] quota: %v", err)
		return syscall.EIO
	}
	h.NextProjectID = id + 1
	if err := tbl.PutHeader(h); err != nil {
		log.Printf("[ERROR] quota: %v", err)
		return syscall.EIO
	}
	rec = &quota.Record{LimitBlocks: limitBlocks, LimitInodes: setting.inodes, RootInode: num, RootBirth: ino.BirthGeneration, Flags: quota.RecordInUse}
	if err := tbl.PutRecord(id, rec); err != nil {
		log.Printf("[ERROR] quota: project %d: %v", id, err)
		return syscall.EIO
	}
	birth := ino.BirthGeneration
	ino, err = f.updateInode(ctx, num, func(cur *inode.Inode) error {
		if cur.Magic != inode.InodeMagic || cur.BirthGeneration != birth || cur.ProjectID != 0 {
			return errProjectRace
		}
		joinProject(cur, id)
		return nil
	})
	switch {
	case errors.Is(err, errProjectRace):
		return syscall.EAGAIN
	case err != nil:
		return syscall.EIO
	}
	f.cacheInode(ino)
	log.Printf("Directory inode %d is the root of quota project %d", num, id)
	return 0
}

var errProjectRace = errors.New("the directory changed while it got a project")

// clearQuota lifts the limits of the project whose root is directory num; the project stays, so usage stays counted.
func (f *FS) clearQuota(ctx context.Context, num uint32) syscall.Errno {
	if f.quotaInode == 0 {
		return syscall.ENOTSUP
	}
	unlock, err := f.lockFileIO(ctx, f.quotaInode, lock.LockModeExclusive)
	if err != nil {
		return errToErrno(err)
	}
	defer unlock()
	tbl, err := f.quotaTable()
	if err != nil {
		return syscall.EIO
	}
	ino, rec, errno := f.projectRoot(num, tbl)
	switch {
	case errno != 0:
		return errno
	case rec == nil:
		return syscall.ENODATA
	}
	rec.LimitBlocks, rec.LimitInodes = 0, 0
	if err := tbl.PutRecord(ino.ProjectID, rec); err != nil {
		return syscall.EIO
	}
	return 0
}

// quotaValue is what reading quotaXattr on directory num returns.
func (f *FS) quotaValue(num uint32) (string, syscall.Errno) {
	if f.quotaInode == 0 {
		return "", syscall.ENODATA
	}
	tbl, err := f.quotaTable()
	if err != nil {
		return "", syscall.EIO
	}
	ino, err := f.inodeTbl.Read(num)
	if err != nil {
		return "", syscall.EIO
	}
	if ino.ProjectID == 0 {
		return "", syscall.ENODATA
	}
	rec, err := tbl.Record(ino.ProjectID)
	if err != nil {
		return "", syscall.EIO
	}
	blockSize := f.dataBlockSize()
	return fmt.Sprintf("project=%d bytes=%d inodes=%d used_bytes=%d used_inodes=%d",
		ino.ProjectID, rec.LimitBlocks*blockSize, rec.LimitInodes, rec.UsedBlocks*blockSize, rec.UsedInodes), 0
}

func (d *Dir) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	if attr != quotaXattr {
		return 0, syscall.ENODATA
	}
	value, errno := d.fs.quotaValue(d.inodeNum)
	if errno != 0 {
		return 0, errno
	}
	if len(dest) < len(value) {
		return uint32(len(value)), syscall.ERANGE
	}
	return uint32(copy(dest, value)), 0
}

func (d *Dir) Setxattr(ctx context.Context, attr string, data []byte, flags uint32) syscall.Errno {
	if attr != quotaXattr {
		return syscall.ENOTSUP
	}
	return d.fs.setQuota(detach(ctx), d.inodeNum, string(data))
}

func (d *Dir) Removexattr(ctx context.Context, attr string) syscall.Errno {
	if attr != quotaXattr {
		return syscall.ENODATA
	}
	return d.fs.clearQuota(detach(ctx), d.inodeNum)
}

func (d *Dir) Listxattr(ctx context.Context, dest []byte) (uint32, syscall.Errno) {
	if _, errno := d.fs.quotaValue(d.inodeNum); errno != 0 {
		return 0, 0
	}
	name := quotaXattr + "\x00"
	if len(dest) < len(name) {
		return uint32(len(name)), syscall.ERANGE
	}
	return uint32(copy(dest, name)), 0
}

// crossesProject reports whether moving or linking inode child into directory dir would take it out of its project
// or into another one. Only a project's root may move, and only between directories outside every project.
func (f *FS) crossesProject(child, dir *inode.Inode) bool {
	to := childProject(dir)
	if child.ProjectID == to {
		return false
	}
	if to != 0 || f.quotaInode == 0 {
		return true
	}
	tbl, err := f.quotaTable()
	if err != nil {
		return true
	}
	rec, err := tbl.Record(child.ProjectID)
	return err != nil || rec.RootInode != child.InodeNum || rec.RootBirth != child.BirthGeneration
}
