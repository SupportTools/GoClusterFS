package fsck

import (
	"encoding/binary"
	"fmt"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

// sectorChecksumOffset is where bitmap, refcount and snapshot registry sectors
// keep the CRC32C of the bytes before it, as the runtime writes them.
const sectorChecksumOffset = storage.SectorSize - 4

// sectorChecksumValid reports whether a sector carries a valid checksum.
// Sectors the runtime never wrote are all zero, which is valid too.
func sectorChecksumValid(buf []byte) bool {
	return binary.LittleEndian.Uint32(buf[sectorChecksumOffset:]) == storage.CRC32C(buf[:sectorChecksumOffset]) || isZero(buf)
}

func isZero(buf []byte) bool {
	for _, b := range buf {
		if b != 0 {
			return false
		}
	}
	return true
}

func setSectorChecksum(buf []byte) {
	binary.LittleEndian.PutUint32(buf[sectorChecksumOffset:], storage.CRC32C(buf[:sectorChecksumOffset]))
}

// readSector reads the sector at offset into buf, which must be aligned.
func (c *Checker) readSector(buf []byte, offset uint64) error {
	n, err := c.dev.ReadAt(buf, int64(offset))
	if err != nil {
		return fmt.Errorf("read error: %w", err)
	}
	if n != len(buf) {
		return fmt.Errorf("short read: %d bytes", n)
	}
	return nil
}
