package main

import (
	"fmt"
	"hash/crc32"
)

func main() {
	table := crc32.MakeTable(crc32.Castagnoli)
	fmt.Printf("hello: 0x%08x\n", crc32.Checksum([]byte("hello"), table))
	fmt.Printf("single 0x00: 0x%08x\n", crc32.Checksum([]byte{0x00}, table))
}
