package fsck

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/supporttools/GoClusterFS/internal/quota"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// projectUsage is what a project's inodes use: the runtime charges a project for each of its inodes and the blocks
// they allocate.
type projectUsage struct {
	blocks, inodes uint64
	first          uint32
}

// strayEntry is a name whose inode's project differs from the one its directory gives new entries.
type strayEntry struct {
	dir, inode uint32
	name       string
	want, got  uint32
}

func (c *Checker) quotasEnabled() bool {
	return c.sbData != nil && c.sbData.RequiredFlags&superblock.RequiredProjectQuotas != 0
}

// isQuotaInode reports whether num holds the quota table, which no directory names.
func (c *Checker) isQuotaInode(num uint32) bool {
	return c.quotasEnabled() && num == c.sbData.QuotaInode
}

func (c *Checker) countProjectUsage(num uint32, ino *InodeData) {
	if ino.ProjectID == 0 {
		return
	}
	c.projectOf[num] = ino.ProjectID
	u := c.projectUse[ino.ProjectID]
	if u == nil {
		u = &projectUsage{first: num}
		c.projectUse[ino.ProjectID] = u
	}
	u.blocks += ino.AllocatedBlocks
	u.inodes++
}

// uncountProjectUsage takes an inode repair freed out of its project's usage.
func (c *Checker) uncountProjectUsage(ino *InodeData) {
	if u := c.projectUse[ino.ProjectID]; u != nil {
		u.blocks -= min(u.blocks, ino.AllocatedBlocks)
		u.inodes -= min(u.inodes, 1)
	}
	delete(c.projectOf, ino.InodeNum)
}

func (c *Checker) noteEntryProject(dir uint32, dirIno *InodeData, name string, child uint32) {
	if !c.quotasEnabled() {
		return
	}
	var want uint32
	if dirIno.Flags&FlagProjInherit != 0 {
		want = dirIno.ProjectID
	}
	if got := c.projectOf[child]; got != want {
		c.strayEntries = append(c.strayEntries, strayEntry{dir: dir, inode: child, name: name, want: want, got: got})
	}
}

// checkQuotaTable checks that the quota inode is in use, maps every table block, and that the header and each
// record sector pass their checksums. It keeps where each table sector lies for checkQuotaUsage.
func (c *Checker) checkQuotaTable() {
	if !c.quotasEnabled() {
		return
	}
	sb := c.sbData
	loc := fmt.Sprintf("quota inode %d", sb.QuotaInode)
	if sb.QuotaInode == 0 || !c.inodeUsed[sb.QuotaInode] {
		c.addProblem(PhaseCrossValidation, Error, loc, "the superblock requires project quotas, but its quota inode is not in use", false)
		return
	}
	ino, err := c.readInode(sb.QuotaInode)
	if err != nil {
		c.addProblem(PhaseCrossValidation, Error, loc, "cannot be read: "+err.Error(), false)
		return
	}
	physical := map[uint64]uint64{}
	for _, x := range c.inodeExtents(sb.QuotaInode, ino) {
		for i := uint64(0); i < x.Length; i++ {
			physical[x.LogicalBlock+i] = x.PhysicalBlock + i
		}
	}
	blockSize := uint64(sb.BlockSize)
	sector := storage.NewAlignedBuffer(storage.SectorSize).Bytes()
	var sectors []uint64
	bad := 0
	for logical := uint64(0); logical < uint64(sb.QuotaTableBlocks); logical++ {
		block, ok := physical[logical]
		if !ok {
			c.addProblem(PhaseCrossValidation, Error, loc, fmt.Sprintf("maps no block %d of the %d-block quota table", logical, sb.QuotaTableBlocks), false)
			return
		}
		for off := uint64(0); off < blockSize; off += storage.SectorSize {
			at := sb.DataRegionOffset + block*blockSize + off
			sectors = append(sectors, at)
			if err := c.readSector(sector, at); err != nil {
				c.addProblem(PhaseCrossValidation, Error, loc, fmt.Sprintf("table sector at %d: %v", at, err), false)
				return
			}
			if logical == 0 && off == 0 {
				_, err = quota.DecodeHeader(sector)
			} else {
				_, err = quota.GetRecord(sector, 0)
			}
			if err != nil {
				if bad++; bad <= reportedLeaks {
					c.addProblem(PhaseCrossValidation, Error, loc, fmt.Sprintf("table sector at %d: %v", at, err), false)
				}
			}
		}
	}
	if bad > reportedLeaks {
		c.addProblem(PhaseCrossValidation, Error, loc, fmt.Sprintf("%d more table sectors fail their checks", bad-reportedLeaks), false)
	}
	c.quotaSectors = sectors
}

// quotaRecords reads the table's records in use. A record sector that fails its checksum, which checkQuotaTable
// reported, goes in unknown.
func (c *Checker) quotaRecords() (records map[uint32]*quota.Record, unknown map[uint64]bool) {
	records, unknown = map[uint32]*quota.Record{}, map[uint64]bool{}
	sector := storage.NewAlignedBuffer(storage.SectorSize).Bytes()
	for s := uint64(1); s < uint64(len(c.quotaSectors)); s++ {
		if c.readSector(sector, c.quotaSectors[s]) != nil {
			unknown[s] = true
			continue
		}
		for k := range quota.RecordsPerSector {
			id := uint32((s-1)*quota.RecordsPerSector + uint64(k))
			rec, err := quota.GetRecord(sector, k*quota.RecordSize)
			if err != nil {
				unknown[s] = true
				break
			}
			if id != 0 && rec.Flags&quota.RecordInUse != 0 {
				records[id] = rec
			}
		}
	}
	return records, unknown
}

// checkQuotaUsage reports inodes in projects the table has no record for and names whose project their directory
// does not give, and compares each project's usage in the table with what its inodes use. Repair writes the recount.
func (c *Checker) checkQuotaUsage() error {
	if !c.quotasEnabled() || c.quotaSectors == nil {
		return nil
	}
	records, unknown := c.quotaRecords()
	if len(records) == 0 && len(c.projectUse) == 0 {
		return nil
	}
	c.reportUnrecordedProjects(records, func(id uint32) bool {
		s, _ := quota.RecordLocation(id)
		return s < uint64(len(c.quotaSectors)) && unknown[s]
	})
	c.reportStrayEntries(records)

	if why := c.usageUnverifiable(); why != "" {
		c.addProblem(PhaseCrossValidation, Info, "quota table", "project usage not checked: "+why, false)
		return nil
	}
	var problems []int
	sectors := map[uint64]bool{}
	for _, id := range slices.Sorted(maps.Keys(records)) {
		rec, use := records[id], c.recount(id)
		if rec.UsedBlocks == use.blocks && rec.UsedInodes == use.inodes {
			continue
		}
		s, _ := quota.RecordLocation(id)
		if sectors[s] = true; len(problems) < reportedLeaks {
			c.addProblem(PhaseCrossValidation, Error, fmt.Sprintf("quota project %d", id),
				fmt.Sprintf("the quota table counts %d blocks and %d inodes, but the project's inodes use %d blocks and %d inodes",
					rec.UsedBlocks, rec.UsedInodes, use.blocks, use.inodes), true)
		} else if len(problems) == reportedLeaks {
			c.addProblem(PhaseCrossValidation, Error, "quota table", "more projects count other usage than their inodes use", true)
		} else {
			continue
		}
		problems = append(problems, c.lastProblem())
	}
	if len(problems) == 0 || c.opts.Mode != Repair {
		return nil
	}
	if err := c.beforeWrites(); err != nil {
		return err
	}
	if err := c.writeRecount(records, slices.Sorted(maps.Keys(sectors))); err != nil {
		return err
	}
	c.markRepaired(problems...)
	return nil
}

func (c *Checker) recount(id uint32) projectUsage {
	if u := c.projectUse[id]; u != nil {
		return *u
	}
	return projectUsage{}
}

// writeRecount sets the usage of every record in use in the given table sectors to the recount.
func (c *Checker) writeRecount(records map[uint32]*quota.Record, sectors []uint64) error {
	if c.opts.DryRun {
		return nil
	}
	sector := storage.NewAlignedBuffer(storage.SectorSize).Bytes()
	for _, s := range sectors {
		at := c.quotaSectors[s]
		if err := c.readSector(sector, at); err != nil {
			return fmt.Errorf("quota table sector %d: %w", s, err)
		}
		for k := range quota.RecordsPerSector {
			id := uint32((s-1)*quota.RecordsPerSector + uint64(k))
			if rec := records[id]; rec != nil {
				use := c.recount(id)
				rec.UsedBlocks, rec.UsedInodes = use.blocks, use.inodes
				quota.PutRecord(sector, k*quota.RecordSize, rec)
			}
		}
		if _, err := c.dev.WriteAt(sector, int64(at)); err != nil {
			return fmt.Errorf("quota table sector %d: %w", s, err)
		}
	}
	return c.dev.Sync()
}

func (c *Checker) reportUnrecordedProjects(records map[uint32]*quota.Record, unknown func(uint32) bool) {
	for _, id := range slices.Sorted(maps.Keys(c.projectUse)) {
		use := c.projectUse[id]
		if records[id] != nil || use.inodes == 0 || unknown(id) {
			continue
		}
		c.addProblem(PhaseCrossValidation, Error, fmt.Sprintf("quota project %d", id),
			fmt.Sprintf("%d inodes count against it, inode %d first, but the quota table has no record in use for it", use.inodes, use.first), false)
	}
}

// reportStrayEntries reports names whose inode is in another project than its directory gives new entries. A
// project's root is the one inode outside every project's directories that may have a project.
func (c *Checker) reportStrayEntries(records map[uint32]*quota.Record) {
	n := 0
	for _, e := range c.strayEntries {
		if rec := records[e.got]; e.want == 0 && rec != nil && rec.RootInode == e.inode && rec.RootBirth == c.inodeBirth[e.inode] {
			continue
		}
		if n++; n <= reportedLeaks {
			c.addProblem(PhaseCrossValidation, Error, fmt.Sprintf("inode %d", e.dir),
				fmt.Sprintf("entry %q names inode %d, which counts against project %d, but the directory gives its entries project %d",
					e.name, e.inode, e.got, e.want), false)
		}
	}
	if n > reportedLeaks {
		c.addProblem(PhaseCrossValidation, Error, "directories",
			fmt.Sprintf("%d more entries name inodes in another project than their directory gives", n-reportedLeaks), false)
	}
}

// usageUnverifiable says why the table's usage may rightly differ from the recount, or returns "".
func (c *Checker) usageUnverifiable() string {
	if !c.finished[PhaseInodes] {
		return PhaseInodes.String() + " did not finish"
	}
	if len(c.usageIncomplete) > 0 {
		return "fsck could not read every inode: " + strings.Join(c.usageIncomplete[:min(len(c.usageIncomplete), 3)], "; ")
	}
	var dirty []string
	for i := 0; i < int(c.sbData.MaxNodes); i++ {
		if c.journalNeedsReplay(i) != "" {
			dirty = append(dirty, fmt.Sprint(i))
		}
	}
	if len(dirty) > 0 {
		return fmt.Sprintf("a journal is not clean (node slot %s), so it may hold usage changes the table does not show yet; "+
			"unmount the filesystem on every node, mounting it once first if a node crashed, and run fsck again", strings.Join(dirty, ", "))
	}
	return ""
}
