package fsck

import (
	"fmt"

	"github.com/supporttools/GoClusterFS/internal/quota"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

func (c *Checker) quotasEnabled() bool {
	return c.sbData != nil && c.sbData.RequiredFlags&superblock.RequiredProjectQuotas != 0
}

// isQuotaInode reports whether num holds the quota table, which no directory names.
func (c *Checker) isQuotaInode(num uint32) bool {
	return c.quotasEnabled() && num == c.sbData.QuotaInode
}

// checkQuotaTable checks that the quota inode is in use, maps every table block, and that the header and each
// record sector pass their checksums.
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
	bad := 0
	for logical := uint64(0); logical < uint64(sb.QuotaTableBlocks); logical++ {
		block, ok := physical[logical]
		if !ok {
			c.addProblem(PhaseCrossValidation, Error, loc, fmt.Sprintf("maps no block %d of the %d-block quota table", logical, sb.QuotaTableBlocks), false)
			return
		}
		for off := uint64(0); off < blockSize; off += storage.SectorSize {
			at := sb.DataRegionOffset + block*blockSize + off
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
}
