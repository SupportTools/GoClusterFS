package storage

import (
	"hash/crc32"
)

// CRC32C uses the Castagnoli polynomial, which is hardware-accelerated
// on modern x86 processors (SSE4.2). This is the same polynomial used
// by iSCSI, SCTP, and many filesystems including Btrfs and ext4.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// CRC32C computes the CRC32C checksum of the given data.
// CRC32C is preferred over CRC32 (IEEE) because:
// 1. Hardware acceleration via SSE4.2/ARMv8-A CRC instructions
// 2. Better error detection properties
// 3. Industry standard for storage systems
func CRC32C(data []byte) uint32 {
	return crc32.Checksum(data, crc32cTable)
}

// CRC32CUpdate updates an existing CRC32C with additional data.
// This is useful for computing checksums incrementally.
func CRC32CUpdate(crc uint32, data []byte) uint32 {
	return crc32.Update(crc, crc32cTable, data)
}

// VerifyCRC32C verifies that data matches the expected checksum.
func VerifyCRC32C(data []byte, expected uint32) bool {
	return CRC32C(data) == expected
}

// CRC32CWithSeed computes CRC32C with an initial seed value.
// Useful for chained checksums or when you want a non-zero starting point.
func CRC32CWithSeed(seed uint32, data []byte) uint32 {
	return crc32.Update(seed, crc32cTable, data)
}
