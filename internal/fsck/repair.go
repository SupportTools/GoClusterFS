package fsck

import (
	"context"
	"errors"
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

	if !c.opts.DryRun {
		if err := c.registryUnchanged(); err != nil {
			return err
		}
	}
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
	if c.opts.DryRun {
		return nil
	}
	return c.dev.Sync()
}

// errRepairStopped stops the check whatever StopOnFatal says, since the
// filesystem may be in use.
var errRepairStopped = errors.New("repair stopped")

// startRepair runs after phase 1, before repair writes anything. A dry run
// only warns about what would make repair refuse.
func (c *Checker) startRepair() error {
	if c.opts.Mode != Repair {
		return nil
	}
	live, unclean := c.nodes()
	if c.opts.DryRun {
		for _, why := range append(live, unclean...) {
			c.addProblem(PhaseSuperblock, Warning, "node registry", "repair would refuse: "+why, false)
		}
		return nil
	}
	if len(live) > 0 {
		return fmt.Errorf("repair refused: %s; unmount it on every node first", strings.Join(live, ", "))
	}
	if len(unclean) > 0 && !c.opts.AllowUncleanNodes {
		return fmt.Errorf("repair refused: %s, so the journal may hold changes fsck cannot see; "+
			"mount the filesystem so the journal is replayed and unmount it, or pass -force", strings.Join(unclean, "; "))
	}
	if c.superblockFix != nil {
		if err := c.superblockFix(); err != nil {
			return fmt.Errorf("repair superblock checksum: %w", err)
		}
		if err := c.dev.Sync(); err != nil {
			return fmt.Errorf("sync: %w", err)
		}
	}
	return nil
}

// How long fsck watches the node registry before it decides no node is
// running: briefly when every heartbeat is older than the runtime's failure
// timeout, in case a node's clock lags, and otherwise for that timeout.
var (
	staleHeartbeatWatch  = 3 * node.HeartbeatInterval
	recentHeartbeatWatch = node.HeartbeatTimeout + node.HeartbeatInterval
)

// nodes describes the nodes that have the filesystem mounted, and those that
// stopped without replaying their journal. An unreadable slot counts as the
// second kind. It keeps the registry it saw for registryUnchanged.
func (c *Checker) nodes() (live, unclean []string) {
	reg := node.NewRegistry(c.dev, c.sbData.NodeRegistryOffset)
	slots, err := reg.Slots()
	if err != nil {
		unclean = append(unclean, fmt.Sprintf("a node slot is unreadable (%v)", err))
	}
	running := c.watchRegistry(reg, slots)
	for i, slot := range c.registry {
		switch {
		case running[i]:
			live = append(live, "filesystem is mounted by "+slotName(i, slot))
		case slot == nil:
		default:
			if why := c.journalNeedsReplay(i); why != "" {
				age := time.Since(time.Unix(slot.HeartbeatTime, 0)).Round(time.Second)
				unclean = append(unclean, fmt.Sprintf("%s, last heartbeat %s ago, %s", slotName(i, slot), age, why))
			}
		}
	}
	return live, unclean
}

func slotName(i int, slot *node.Slot) string {
	return fmt.Sprintf("node %d (%s)", i, strings.TrimRight(string(slot.Hostname[:]), "\x00"))
}

// slotChanged reports whether a node wrote its slot between two reads.
func slotChanged(a, b *node.Slot) bool {
	if a == nil || b == nil {
		return a != b
	}
	return a.UUID != b.UUID || a.State != b.State || a.HeartbeatSeq != b.HeartbeatSeq
}

// watchRegistry reports which slots belong to running nodes. The mount binary
// leaves its slot active when it stops, so only a slot that changes shows a
// running node: a heartbeat, or a node mounting during the watch.
func (c *Checker) watchRegistry(reg *node.Registry, slots []*node.Slot) map[int]bool {
	watch := time.Duration(0)
	for _, slot := range slots {
		if slot == nil || slot.State != node.NodeStateActive {
			continue
		}
		if time.Since(time.Unix(slot.HeartbeatTime, 0)) > node.HeartbeatTimeout {
			watch = max(watch, staleHeartbeatWatch)
		} else {
			watch = max(watch, recentHeartbeatWatch)
		}
	}
	if watch > 0 {
		c.logf("  Watching the node registry for up to %s...\n", watch)
	}
	running := make(map[int]bool)
	c.registry = slots
	for deadline := time.Now().Add(watch); len(running) == 0 && time.Now().Before(deadline); {
		time.Sleep(node.HeartbeatInterval / 4)
		now, _ := reg.Slots()
		for i := range now {
			if slotChanged(slots[i], now[i]) && now[i] != nil && now[i].State == node.NodeStateActive {
				running[i] = true
			}
		}
		c.registry = now
	}
	return running
}

// beforeRepairWrites lets tests change the registry after the watch.
var beforeRepairWrites func()

// registryUnchanged fails when a node wrote its slot since startRepair
// watched the registry: a node may have mounted the filesystem.
func (c *Checker) registryUnchanged() error {
	now, _ := node.NewRegistry(c.dev, c.sbData.NodeRegistryOffset).Slots()
	for i := range now {
		if slotChanged(c.registry[i], now[i]) {
			slot := now[i]
			if slot == nil {
				slot = c.registry[i]
			}
			return fmt.Errorf("%w: %s changed its node registry slot during repair; unmount it and run fsck again",
				errRepairStopped, slotName(i, slot))
		}
	}
	return nil
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
