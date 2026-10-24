package fsck

import (
	"bytes"
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
		// Marked counted, blocks past the table would be freed on their first release.
		if c.uncountedPast[num] {
			c.addProblem(PhaseRefcounts, Error, fmt.Sprintf("inode %d", num),
				"shares blocks past the refcount table the way older versions did, without counting the owners; repair cannot count them, "+
					"so writes to it change the other files that map those blocks. Copy the file and delete the original to separate it",
				false)
			delete(c.uncounted, num)
			continue
		}
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
	running, err := c.watchRegistry(slots)
	if err != nil {
		live = append(live, fmt.Sprintf("the node registry is unreadable (%v)", err))
	}
	latest, _ := reg.Slots()
	for i, slot := range slots {
		switch {
		case running[i]:
			live = append(live, "filesystem is mounted by "+slotName(i, latest[i], slot))
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

// slotName names node i after the first readable copy of its slot.
func slotName(i int, slots ...*node.Slot) string {
	for _, slot := range slots {
		if slot != nil {
			return fmt.Sprintf("node %d (%s)", i, strings.TrimRight(string(slot.Hostname[:]), "\x00"))
		}
	}
	return fmt.Sprintf("node %d", i)
}

// readRegistry reads the node slots as raw bytes, so that a write to an
// unreadable slot shows as a change too.
func (c *Checker) readRegistry() ([]byte, error) {
	size := min(c.sbData.NodeRegistrySize, node.MaxNodes*node.SlotSize) / node.SlotSize * node.SlotSize
	buf := storage.NewAlignedBuffer(int(size)).Bytes()
	if err := c.readSector(buf, c.sbData.NodeRegistryOffset); err != nil {
		return nil, err
	}
	return buf, nil
}

// changedSlots lists the slots whose bytes differ between two registry reads.
func changedSlots(a, b []byte) []int {
	var changed []int
	for off := 0; off < len(a); off += node.SlotSize {
		if !bytes.Equal(a[off:off+node.SlotSize], b[off:off+node.SlotSize]) {
			changed = append(changed, off/node.SlotSize)
		}
	}
	return changed
}

// watchRegistry reports which slots belong to running nodes. The mount binary
// leaves its slot active when it stops, so only a slot that changes shows a
// running node: a heartbeat, or a node mounting during the watch.
func (c *Checker) watchRegistry(slots []*node.Slot) (map[int]bool, error) {
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
	before, err := c.readRegistry()
	if err != nil {
		return running, err
	}
	c.registry = before
	for deadline := time.Now().Add(watch); len(running) == 0 && time.Now().Before(deadline); {
		time.Sleep(node.HeartbeatInterval / 4)
		now, err := c.readRegistry()
		if err != nil {
			return running, err
		}
		for _, i := range changedSlots(before, now) {
			running[i] = true
		}
	}
	return running, nil
}

// beforeRepairWrites lets tests change the registry after the watch.
var beforeRepairWrites func()

// registryUnchanged fails when a node wrote its slot since startRepair
// watched the registry: a node may have mounted the filesystem.
func (c *Checker) registryUnchanged() error {
	now, err := c.readRegistry()
	if err != nil {
		return fmt.Errorf("%w: cannot read the node registry: %v", errRepairStopped, err)
	}
	if changed := changedSlots(c.registry, now); len(changed) > 0 {
		latest, _ := node.NewRegistry(c.dev, c.sbData.NodeRegistryOffset).Slots()
		return fmt.Errorf("%w: %s changed its node registry slot during repair; unmount it and run fsck again",
			errRepairStopped, slotName(changed[0], latest[changed[0]]))
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
	off, _ := c.journalArea(i)
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
