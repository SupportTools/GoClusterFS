package fsck

import "github.com/supporttools/GoClusterFS/internal/storage"

// inodeChunkSectors is how many inode table sectors one read fetches. Over a
// SAN each read costs a round trip, which outweighs the bytes it moves.
const inodeChunkSectors = 256

// inodeSectors reads the inode table a chunk at a time for one pass in sector
// order. A pass that writes the inode table must not use it: it caches.
type inodeSectors struct {
	c     *Checker
	chunk []byte
	first uint64
	n     uint64 // sectors in chunk; 0 when the chunk could not be read in one piece
	one   []byte
}

func (c *Checker) inodeSectors() *inodeSectors {
	return &inodeSectors{
		c:     c,
		chunk: storage.NewAlignedBuffer(inodeChunkSectors * InodeSectorSize).Bytes(),
		first: ^uint64(0),
		one:   storage.NewAlignedBuffer(InodeSectorSize).Bytes(),
	}
}

// at returns inode table sector idx. When its chunk cannot be read in one
// piece, the sector is read alone, so only an unreadable sector fails.
func (s *inodeSectors) at(idx uint64) ([]byte, error) {
	sb := s.c.sbData
	if first := idx - idx%inodeChunkSectors; first != s.first {
		s.first = first
		s.n = min(inodeChunkSectors, sb.InodeTableSize/InodeSectorSize-first)
		if s.c.readSector(s.chunk[:s.n*InodeSectorSize], sb.InodeTableOffset+first*InodeSectorSize) != nil {
			s.n = 0
		}
	}
	if i := idx - s.first; i < s.n {
		return s.chunk[i*InodeSectorSize : (i+1)*InodeSectorSize], nil
	}
	return s.one, s.c.readSector(s.one, sb.InodeTableOffset+idx*InodeSectorSize)
}

// inode reads and parses inode num, as readInode does.
func (s *inodeSectors) inode(num uint32) (*InodeData, error) {
	sector, err := s.at(uint64(num / InodesPerSector))
	if err != nil {
		return nil, err
	}
	i := num % InodesPerSector
	return s.c.parseInode(sector[i*InodeSize : (i+1)*InodeSize]), nil
}
