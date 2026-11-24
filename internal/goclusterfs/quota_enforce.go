package goclusterfs

import (
	"context"
	"log"
	"math/bits"
	"syscall"
	"time"

	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/quota"
)

// quotaFresh is how long a project's record, and the count of live nodes, read for enforcement stay current.
const quotaFresh = time.Second

type seenRecord struct {
	rec quota.Record
	at  time.Time
}

// projectRecord returns project's record as this node last read it, and reads it again when fresh is set or the read
// is older than quotaFresh.
func (f *FS) projectRecord(project uint32, fresh bool) (*quota.Record, error) {
	f.quotaSeenMu.Lock()
	seen, ok := f.quotaSeen[project]
	f.quotaSeenMu.Unlock()
	if ok && !fresh && time.Since(seen.at) < quotaFresh {
		return &seen.rec, nil
	}
	tbl, err := f.quotaTable()
	if err != nil {
		return nil, err
	}
	rec, err := tbl.Record(project)
	if err != nil {
		return nil, err
	}
	f.noteRecord(project, rec)
	return rec, nil
}

// noteRecord keeps project's record as this node just read or wrote it.
func (f *FS) noteRecord(project uint32, rec *quota.Record) {
	f.quotaSeenMu.Lock()
	defer f.quotaSeenMu.Unlock()
	if f.quotaSeen == nil {
		f.quotaSeen = map[uint32]seenRecord{}
	}
	f.quotaSeen[project] = seenRecord{rec: *rec, at: time.Now()}
}

// liveNodes is how many nodes have the volume mounted, as the registry said at most quotaFresh ago.
func (f *FS) liveNodes() int64 {
	if f.liveCount != nil {
		return f.liveCount()
	}
	if f.orphanReg == nil {
		return 1
	}
	f.quotaSeenMu.Lock()
	n, at := f.live, f.liveAt
	f.quotaSeenMu.Unlock()
	if time.Since(at) < quotaFresh && n > 0 {
		return n
	}
	if slots, err := f.orphanReg.LiveSlots(); err == nil {
		n = max(int64(bits.OnesCount64(slots)), 1)
	}
	n = max(n, 1)
	f.quotaSeenMu.Lock()
	f.live, f.liveAt = n, time.Now()
	f.quotaSeenMu.Unlock()
	return n
}

// checkQuota returns EDQUOT when adding blocks and inodes to project would pass a limit: the usage this node last
// read, plus its own unflushed changes, plus the addition. Close to a limit, where the unflushed changes of every live
// node could pass it together, it first flushes this node's changes and reads the usage again, when mayFlush allows:
// the flush takes the quota lock, so the caller may hold no lock but a file I/O lock.
func (f *FS) checkQuota(ctx context.Context, project uint32, blocks, inodes int64, mayFlush bool) syscall.Errno {
	if project == 0 || f.quotaInode == 0 || f.journal == nil || blocks <= 0 && inodes <= 0 {
		return 0
	}
	rec, err := f.projectRecord(project, false)
	if err != nil {
		log.Printf("[ERROR] quota: reading project %d: %v", project, err)
		return syscall.EIO
	}
	pending := f.journal.QuotaPending()[project]
	if mayFlush && passes(rec, pending, blocks, inodes, f.liveNodes()) {
		if err := f.flushQuota(ctx); err != nil {
			log.Printf("[WARN] quota: flushing before an allocation in project %d failed: %v", project, err)
		}
		if rec, err = f.projectRecord(project, true); err != nil {
			log.Printf("[ERROR] quota: reading project %d: %v", project, err)
			return syscall.EIO
		}
		pending = f.journal.QuotaPending()[project]
	}
	if passes(rec, pending, blocks, inodes, 1) {
		return syscall.EDQUOT
	}
	return 0
}

// passes reports whether the usage in rec, plus scale times the unflushed changes and the addition, passes a limit.
func passes(rec *quota.Record, pending journal.QuotaDeltaPayload, blocks, inodes, scale int64) bool {
	over := func(used uint64, pending, add int64, limit uint64) bool {
		return limit != 0 && add > 0 && int64(used)+scale*(pending+add) > int64(limit)
	}
	return over(rec.UsedBlocks, pending.Blocks, blocks, rec.LimitBlocks) || over(rec.UsedInodes, pending.Inodes, inodes, rec.LimitInodes)
}

// checkMapQuota returns EDQUOT when mapping the bytes from start to end of the file would pass its project's block
// limit. Only blocks the file does not map yet count, so overwriting a file in a full project works.
func (f *File) checkMapQuota(ctx context.Context, start, end uint64) syscall.Errno {
	if f.fs.quotaInode == 0 || end <= start {
		return 0
	}
	ino, err := f.fs.getInode(f.inodeNum)
	if err != nil {
		return syscall.EIO
	}
	if ino.ProjectID == 0 {
		return 0
	}
	rec, err := f.fs.projectRecord(ino.ProjectID, false)
	if err != nil {
		return syscall.EIO
	}
	if rec.LimitBlocks == 0 {
		return 0
	}
	bs := f.fs.blockSize
	holes, err := f.fs.extentMgr.Unmapped(ino, start/bs, (end+bs-1)/bs)
	if err != nil {
		return syscall.EIO
	}
	return f.fs.checkQuota(ctx, ino.ProjectID, int64(holes), 0, true)
}
