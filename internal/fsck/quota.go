package fsck

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/supporttools/GoClusterFS/internal/inode"
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

// quotaRecords reads the table's records in use, by the project that has each. A record sector that fails its
// checksum, which checkQuotaTable reported, goes in unknown.
func (c *Checker) quotaRecords() (records map[uint32]*quota.Record, unknown map[uint64]bool) {
	records, unknown = map[uint32]*quota.Record{}, map[uint64]bool{}
	sector := storage.NewAlignedBuffer(storage.SectorSize).Bytes()
	for s := uint64(1); s < uint64(len(c.quotaSectors)); s++ {
		if c.readSector(sector, c.quotaSectors[s]) != nil {
			unknown[s] = true
			continue
		}
		for k := range quota.RecordsPerSector {
			index := uint32((s-1)*quota.RecordsPerSector + uint64(k))
			rec, err := quota.GetRecord(sector, k*quota.RecordSize)
			if err != nil {
				unknown[s] = true
				break
			}
			if index != 0 && rec.Flags&quota.RecordInUse != 0 {
				records[rec.Owner(index)] = rec
			}
		}
	}
	return records, unknown
}

// checkQuotaUsage reports inodes in projects that have no record, which repair takes out of every project, and names
// whose project their directory does not give. It compares each project's usage in the table with what its inodes
// use, and repair writes the recount. A record no inode carries and whose root directory is gone, repair frees.
func (c *Checker) checkQuotaUsage() error {
	if !c.quotasEnabled() || c.quotaSectors == nil {
		return nil
	}
	records, unknown := c.quotaRecords()
	if len(records) == 0 && len(c.projectUse) == 0 {
		return nil
	}
	unknownAt := func(id uint32) bool {
		s, _ := quota.RecordLocation(id)
		return s < uint64(len(c.quotaSectors)) && unknown[s]
	}
	stale := c.reportStaleProjects(records, unknownAt)
	c.reportStrayEntries(records)
	if err := c.clearStaleProjects(stale); err != nil {
		return err
	}

	if why := c.usageUnverifiable(); why != "" {
		c.addProblem(PhaseCrossValidation, Info, "quota table", "project usage not checked: "+why, false)
		return nil
	}
	var problems []int
	sectors, free := map[uint64]bool{}, map[uint32]bool{}
	for _, id := range slices.Sorted(maps.Keys(records)) {
		rec, use := records[id], c.recount(id)
		s, _ := quota.RecordLocation(id)
		switch {
		case use.inodes == 0 && use.blocks == 0 && !c.rootCarries(rec, id):
			c.addProblem(PhaseCrossValidation, Warning, fmt.Sprintf("quota project %d", id),
				"its record is in use, but no inode is in the project and its root directory is gone; repair frees the record", true)
			free[id], sectors[s] = true, true
		case rec.UsedBlocks == use.blocks && rec.UsedInodes == use.inodes:
			continue
		case len(problems) < reportedLeaks:
			c.addProblem(PhaseCrossValidation, Error, fmt.Sprintf("quota project %d", id),
				fmt.Sprintf("the quota table counts %d blocks and %d inodes, but the project's inodes use %d blocks and %d inodes",
					rec.UsedBlocks, rec.UsedInodes, use.blocks, use.inodes), true)
			sectors[s] = true
		case len(problems) == reportedLeaks:
			c.addProblem(PhaseCrossValidation, Error, "quota table", "more projects count other usage than their inodes use", true)
			sectors[s] = true
		default:
			sectors[s] = true
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
	if err := c.writeRecount(records, slices.Sorted(maps.Keys(sectors)), free); err != nil {
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

// rootCarries reports whether the root directory rec names is still in project id.
func (c *Checker) rootCarries(rec *quota.Record, id uint32) bool {
	return c.inodeUsed[rec.RootInode] && c.inodeBirth[rec.RootInode] == rec.RootBirth && c.projectOf[rec.RootInode] == id
}

// writeRecount sets the usage of every record in use in the given table sectors to the recount, and frees those in
// free.
func (c *Checker) writeRecount(records map[uint32]*quota.Record, sectors []uint64, free map[uint32]bool) error {
	if c.opts.DryRun {
		return nil
	}
	owners := map[uint32]uint32{}
	for id := range records {
		owners[quota.Index(id)] = id
	}
	sector := storage.NewAlignedBuffer(storage.SectorSize).Bytes()
	for _, s := range sectors {
		at := c.quotaSectors[s]
		if err := c.readSector(sector, at); err != nil {
			return fmt.Errorf("quota table sector %d: %w", s, err)
		}
		for k := range quota.RecordsPerSector {
			id, ok := owners[uint32((s-1)*quota.RecordsPerSector+uint64(k))]
			if !ok {
				continue
			}
			rec := records[id]
			if free[id] {
				rec = &quota.Record{ID: id}
			} else {
				use := c.recount(id)
				rec.UsedBlocks, rec.UsedInodes = use.blocks, use.inodes
			}
			quota.PutRecord(sector, k*quota.RecordSize, rec)
		}
		if _, err := c.dev.WriteAt(sector, int64(at)); err != nil {
			return fmt.Errorf("quota table sector %d: %w", s, err)
		}
	}
	return c.dev.Sync()
}

// reportStaleProjects reports, per project, the inodes that carry a project ID no record has: the project's record
// was freed, or holds a later project. It returns each such project's problem.
func (c *Checker) reportStaleProjects(records map[uint32]*quota.Record, unknown func(uint32) bool) map[uint32]int {
	stale := map[uint32]int{}
	for _, id := range slices.Sorted(maps.Keys(c.projectUse)) {
		use := c.projectUse[id]
		if records[id] != nil || use.inodes == 0 || unknown(id) {
			continue
		}
		c.addProblem(PhaseCrossValidation, Error, fmt.Sprintf("quota project %d", id),
			fmt.Sprintf("%d inodes carry it, inode %d first, but no record in the quota table belongs to it; repair takes them out of every project",
				use.inodes, use.first), true)
		stale[id] = c.lastProblem()
	}
	return stale
}

// clearStaleProjects takes the inodes of the projects in stale out of every project, as repair.
func (c *Checker) clearStaleProjects(stale map[uint32]int) error {
	if len(stale) == 0 || c.opts.Mode != Repair {
		return nil
	}
	if err := c.beforeWrites(); err != nil {
		return err
	}
	var nums []uint32
	for num, id := range c.projectOf {
		if _, ok := stale[id]; ok {
			nums = append(nums, num)
		}
	}
	slices.Sort(nums)
	if !c.opts.DryRun {
		sb := c.sbData
		tbl := inode.NewTable(c.dev, sb.InodeTableOffset, sb.InodeTableSize, nil, [16]byte{})
		for _, num := range nums {
			ino, err := tbl.Read(num)
			if err != nil {
				return fmt.Errorf("inode %d: %w", num, err)
			}
			ino.ProjectID = 0
			ino.Flags &^= inode.FlagProjInherit
			if err := tbl.Write(context.Background(), ino); err != nil {
				return fmt.Errorf("inode %d: %w", num, err)
			}
		}
		if err := c.dev.Sync(); err != nil {
			return err
		}
	}
	for _, num := range nums {
		delete(c.projectOf, num)
	}
	for id, problem := range stale {
		delete(c.projectUse, id)
		c.markRepaired(problem)
	}
	return nil
}

// reportStrayEntries reports names whose inode is in another project than its directory gives new entries. A
// project's root is the one inode outside every project's directories that may have a project. An inode in a project
// no record belongs to is left to reportStaleProjects.
func (c *Checker) reportStrayEntries(records map[uint32]*quota.Record) {
	n := 0
	for _, e := range c.strayEntries {
		rec := records[e.got]
		if e.want == 0 && rec != nil && rec.RootInode == e.inode && rec.RootBirth == c.inodeBirth[e.inode] || e.got != 0 && rec == nil {
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
