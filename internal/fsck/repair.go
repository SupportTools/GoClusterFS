package fsck

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/supporttools/GoClusterFS/internal/bitmap"
	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/node"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

// lastProblem returns the index of the most recently added problem.
func (c *Checker) lastProblem() int {
	c.result.mu.Lock()
	defer c.result.mu.Unlock()
	return len(c.result.Problems) - 1
}

// write writes a repaired sector and marks problems repaired. A dry run only
// marks them as ones repair would fix.
func (c *Checker) write(buf []byte, offset uint64, problems ...int) error {
	if !c.opts.DryRun {
		if _, err := c.dev.WriteAt(buf, int64(offset)); err != nil {
			return err
		}
	}
	c.markRepaired(problems...)
	return nil
}

func (c *Checker) markRepaired(problems ...int) {
	c.result.mu.Lock()
	defer c.result.mu.Unlock()
	for _, i := range problems {
		p := &c.result.Problems[i]
		if c.opts.DryRun {
			p.WouldRepair = true
		} else if !p.Repaired {
			p.Repaired = true
			c.result.RepairedCount++
		}
	}
}

// reportUncounted reports inodes whose blocks an older version shared without
// counting the owners. Snapshots and clones of them fail with ENOTSUP until
// repair marks the sharing counted.
func (c *Checker) reportUncounted() {
	nums := make([]uint32, 0, len(c.uncounted))
	for num := range c.uncounted {
		nums = append(nums, num)
	}
	slices.Sort(nums)
	for _, num := range nums {
		c.addProblem(PhaseRefcounts, Warning, fmt.Sprintf("inode %d", num),
			"shares blocks the way older versions did, without counting the owners; snapshots and clones of it fail until repaired",
			true)
		c.uncounted[num] = c.lastProblem()
	}
}

// markSharingCounted marks the shared extents of the inodes reportUncounted
// found as counted. Run it only after the refcounts match the references, so
// that the counts the runtime then trusts are right.
func (c *Checker) markSharingCounted() error {
	if len(c.uncounted) == 0 {
		return nil
	}
	sb := c.sbData
	var uuid [16]byte
	bm := bitmap.NewRegion(c.dev, sb.BlockBitmapOffset, sb.TotalBlocks, nil, uuid)
	tbl := inode.NewTable(c.dev, sb.InodeTableOffset, sb.InodeTableSize, nil, uuid)
	mgr := extent.NewManager(c.dev, bm, nil, sb.DataRegionOffset)
	tbl.SetTreeFlusher(mgr)

	for num, problem := range c.uncounted {
		if c.opts.DryRun {
			c.markRepaired(problem)
			continue
		}
		ino, err := tbl.Read(num)
		if err != nil {
			return fmt.Errorf("inode %d: %w", num, err)
		}
		changed, err := mgr.MarkSharingCounted(ino)
		if err != nil {
			return fmt.Errorf("inode %d: %w", num, err)
		}
		if changed {
			if err := tbl.Write(context.Background(), ino); err != nil {
				return fmt.Errorf("inode %d: %w", num, err)
			}
		}
		c.markRepaired(problem)
	}
	return nil
}

// startRepair runs after phase 1, before repair writes anything. A check
// only warns that a mounted filesystem may change while it reads.
func (c *Checker) startRepair() error {
	live, unclean := c.nodes()
	if c.opts.Mode != Repair {
		if len(live) > 0 {
			c.addProblem(PhaseSuperblock, Warning, "node registry",
				fmt.Sprintf("filesystem is mounted by %s; results may include changes in flight", strings.Join(live, ", ")),
				false)
		}
		return nil
	}
	if !c.opts.DryRun {
		if len(live) > 0 {
			return fmt.Errorf("repair refused: filesystem is mounted by %s; unmount it on every node first", strings.Join(live, ", "))
		}
		if len(unclean) > 0 && !c.opts.AllowUncleanNodes {
			return fmt.Errorf("repair refused: %s, so the journal may hold changes fsck cannot see; "+
				"mount the filesystem so the journal is replayed and unmount it, or pass -force", strings.Join(unclean, "; "))
		}
	}
	if c.superblockFix != nil {
		if err := c.superblockFix(); err != nil {
			return fmt.Errorf("repair superblock checksum: %w", err)
		}
	}
	return nil
}

// How long fsck watches a node's heartbeat counter before it counts the node
// as stopped: briefly when the heartbeat is older than the runtime's failure
// timeout, in case the node's clock lags, and otherwise for that timeout.
var (
	staleHeartbeatWatch  = 3 * node.HeartbeatInterval
	recentHeartbeatWatch = node.HeartbeatTimeout + node.HeartbeatInterval
)

// nodes describes the nodes that have the filesystem mounted, and those that
// stopped without replaying their journal. An unreadable slot counts as the
// second kind.
func (c *Checker) nodes() (live, unclean []string) {
	reg := node.NewRegistry(c.dev, c.sbData.NodeRegistryOffset)
	slots, err := reg.Slots()
	if err != nil {
		unclean = append(unclean, fmt.Sprintf("a node slot is unreadable (%v)", err))
	}
	beating := c.watchHeartbeats(reg, slots)
	for i, slot := range slots {
		if slot == nil {
			continue
		}
		host := strings.TrimRight(string(slot.Hostname[:]), "\x00")
		if beating[i] {
			live = append(live, fmt.Sprintf("node %d (%s)", i, host))
			continue
		}
		age := time.Since(time.Unix(slot.HeartbeatTime, 0)).Round(time.Second)
		if why := c.journalNeedsReplay(i); why != "" {
			unclean = append(unclean, fmt.Sprintf("node %d (%s, last heartbeat %s ago) %s", i, host, age, why))
		}
	}
	return live, unclean
}

// watchHeartbeats reports which active nodes still heartbeat. The mount
// binary leaves its slot active when it stops, so only a heartbeat counter
// that moves shows a running node.
func (c *Checker) watchHeartbeats(reg *node.Registry, slots []*node.Slot) map[int]bool {
	beating := make(map[int]bool)
	deadline := make(map[int]time.Time)
	start := time.Now()
	for i, slot := range slots {
		if slot == nil || slot.State != node.NodeStateActive {
			continue
		}
		watch := recentHeartbeatWatch
		if time.Since(time.Unix(slot.HeartbeatTime, 0)) > node.HeartbeatTimeout {
			watch = staleHeartbeatWatch
		}
		deadline[i] = start.Add(watch)
	}
	if len(deadline) > 0 {
		c.logf("  Watching %d node heartbeats...\n", len(deadline))
	}
	for {
		for i, d := range deadline {
			if !time.Now().Before(d) {
				delete(deadline, i)
			}
		}
		if len(deadline) == 0 {
			return beating
		}
		time.Sleep(node.HeartbeatInterval / 4)
		now, _ := reg.Slots()
		for i := range deadline {
			if now[i] == nil || now[i].UUID != slots[i].UUID || now[i].HeartbeatSeq != slots[i].HeartbeatSeq {
				beating[i] = now[i] != nil && now[i].State == node.NodeStateActive
				delete(deadline, i)
			}
		}
	}
}

// journalNeedsReplay says why node i's journal may hold changes fsck cannot
// see, or returns "" when it holds none. The runtime marks a journal dirty on
// its first write and clean when it stops in order.
func (c *Checker) journalNeedsReplay(i int) string {
	sb := c.sbData
	if sb.MaxNodes == 0 || uint32(i) >= sb.MaxNodes {
		return "has no journal slot"
	}
	off := sb.JournalRegionOffset + uint64(i)*(sb.JournalRegionSize/uint64(sb.MaxNodes))
	buf := storage.NewAlignedBuffer(storage.SectorSize).Bytes()
	if err := c.readSector(buf, off); err != nil {
		return "has an unreadable journal: " + err.Error()
	}
	jsb, err := journal.DeserializeJournalSuperblock(buf)
	switch {
	case err != nil && isZero(buf[:journal.JournalSuperblockSize]):
		return ""
	case err != nil:
		return "has an unreadable journal: " + err.Error()
	case jsb.NeedsRecovery() || jsb.State == journal.JournalStateCorrupt:
		return fmt.Sprintf("stopped with a %s journal", strings.ToLower(jsb.State.String()))
	}
	return ""
}
