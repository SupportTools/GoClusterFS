package journal

import (
	"bytes"
	"encoding/binary"
	"runtime/metrics"
	"strings"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

const (
	maxDecodeAlloc = 64 << 20
	scanDataArea   = 8 * storage.SectorSize
	scanRegionSize = JournalDataOffset + scanDataArea
	scanTimeout    = 10 * time.Second
)

var fuzzNode = [16]byte{0xf0, 0x22}

func FuzzJournalSuperblock(f *testing.F) {
	clean := NewJournalSuperblock(fuzzNode, 1, DefaultJournalSlotSize-JournalDataOffset)
	f.Add(clean.Serialize())

	dirty := *clean
	dirty.State = JournalStateDirty
	dirty.HeadOffset = 3 * storage.SectorSize
	dirty.TailOffset = storage.SectorSize
	dirty.WrapCount = 1
	f.Add(dirty.Serialize())

	maxed := *clean
	maxed.State = JournalState(^uint32(0))
	maxed.HeadOffset = ^uint32(0)
	maxed.TailOffset = ^uint32(0)
	maxed.DataAreaOffset = ^uint32(0)
	maxed.DataAreaSize = 0
	f.Add(maxed.Serialize())

	valid := clean.Serialize()
	f.Add(valid[:JournalSuperblockSize-1])
	f.Add(make([]byte, JournalSuperblockSize))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		checkJournalSuperblockDecode(t, data)
		if len(data) >= JournalSuperblockSize {
			fixed := bytes.Clone(data[:JournalSuperblockSize])
			binary.LittleEndian.PutUint32(fixed[JournalSuperblockSize-4:], storage.CRC32C(fixed[:JournalSuperblockSize-4]))
			checkJournalSuperblockDecode(t, fixed)
		}
	})
}

func checkJournalSuperblockDecode(t *testing.T, data []byte) {
	sb, err := DeserializeJournalSuperblock(data)
	if err != nil {
		return
	}
	_ = sb.AvailableSpace()

	again, err := DeserializeJournalSuperblock(sb.Serialize())
	if err != nil {
		t.Fatalf("decoding a re-encoded journal superblock: %v", err)
	}
	if *again != *sb {
		t.Fatalf("journal superblock changed after an encode/decode round trip")
	}
}

func FuzzTxnRecord(f *testing.F) {
	txn := seedTransaction(f, 5, 1)
	f.Add(txn)
	f.Add(txn[:len(txn)-1])
	f.Add(txn[:TxnHeaderSize])
	f.Add(txn[:TxnHeaderSize-1])
	empty, err := NewTransaction(6, fuzzNode, 1).Serialize()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(empty)
	f.Add(NewIntentCompletion(5).Serialize())
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		before := heapAllocBytes()

		_, _ = DeserializeIntentCompletion(data)
		_ = ValidateTxnChecksum(data)
		_ = IterateEntries(data, func(_ *TxnEntry, payload []byte) error {
			decodeAllPayloads(payload)
			return nil
		})
		if header, err := DeserializeTxnHeader(data); err == nil {
			if txn, err := parseTxnEntries(header, data); err == nil {
				for _, e := range txn.Entries {
					decodeAllPayloads(e.Payload)
				}
			}
		}

		if grown := heapAllocBytes() - before; grown > maxDecodeAlloc {
			t.Fatalf("decoding a %d-byte record allocated %d bytes", len(data), grown)
		}
	})
}

func FuzzJournalScan(f *testing.F) {
	dev, cleanup := createTestDevice(f, scanRegionSize)
	f.Cleanup(cleanup)
	img := storage.NewAlignedBuffer(scanRegionSize)

	sector := func(n int) uint32 { return uint32(n * storage.SectorSize) }

	multiSector := seedTransaction(f, 2, 40)
	end := sector(2 + (len(multiSector)+storage.SectorSize-1)/storage.SectorSize)
	linear := make([]byte, scanDataArea)
	copy(linear[sector(0):], seedTransaction(f, 1, 1))
	copy(linear[sector(1):], NewIntentCompletion(1).Serialize())
	copy(linear[sector(2):], multiSector)
	f.Add(uint32(0), end, uint32(scanDataArea), uint32(JournalDataOffset), linear)
	f.Add(uint32(0), end-storage.SectorSize, uint32(scanDataArea), uint32(JournalDataOffset), linear)
	f.Add(sector(2), sector(2), uint32(scanDataArea), uint32(JournalDataOffset), linear)

	wrapped := make([]byte, scanDataArea)
	copy(wrapped[sector(6):], seedTransaction(f, 3, 1))
	copy(wrapped[sector(7):], NewIntentCompletion(3).Serialize())
	copy(wrapped[sector(0):], seedTransaction(f, 4, 1))
	f.Add(sector(6), sector(1), uint32(scanDataArea), uint32(JournalDataOffset), wrapped)

	f.Add(uint32(0), sector(1), uint32(0), uint32(JournalDataOffset), linear)
	f.Add(uint32(0), uint32(100), sector(1), uint32(JournalDataOffset), linear[sector(1):])
	hugeTxn := bytes.Clone(linear)
	binary.LittleEndian.PutUint32(hugeTxn[16:20], 1<<30)
	f.Add(uint32(0), ^uint32(0)-storage.SectorSize+1, ^uint32(0)-storage.SectorSize+1, uint32(JournalDataOffset), hugeTxn)

	f.Fuzz(func(t *testing.T, head, tail, size, dataOffset uint32, area []byte) {
		sb := NewJournalSuperblock(fuzzNode, 1, size)
		sb.State = JournalStateDirty
		sb.HeadOffset, sb.TailOffset, sb.DataAreaOffset = head, tail, dataOffset

		buf := img.Bytes()
		clear(buf)
		copy(buf, sb.Serialize())
		copy(buf[JournalDataOffset:], area)
		fixRecordChecksums(buf[JournalDataOffset:])
		if _, err := dev.WriteAt(buf, 0); err != nil {
			t.Fatal(err)
		}

		region, err := OpenRegionForRecovery(dev, 0, scanRegionSize)
		if err != nil {
			return
		}

		before := heapAllocBytes()
		done := make(chan struct{})
		go func() {
			defer close(done)
			txns, _, _ := DryRunRecover(region)
			for _, txn := range txns {
				for _, e := range txn.Entries {
					decodeAllPayloads(e.Payload)
				}
			}
		}()
		select {
		case <-done:
		case <-time.After(scanTimeout):
			t.Fatalf("recovery scan did not finish within %v", scanTimeout)
		}

		if grown := heapAllocBytes() - before; grown > maxDecodeAlloc {
			t.Fatalf("recovery scan of a %d-byte region allocated %d bytes", scanRegionSize, grown)
		}
	})
}

func seedTransaction(tb testing.TB, seq uint64, dirEntries int) []byte {
	tb.Helper()
	txn := NewTransaction(seq, fuzzNode, 1)
	for i := range dirEntries {
		name := strings.Repeat(string(rune('a'+i%26)), 1+i*20%255)
		if err := txn.LogDirAddEntry(&DirAddEntryPayload{ParentInodeNum: 2, ChildInodeNum: uint64(100 + i), EntryType: DirEntryRegular, Name: name}); err != nil {
			tb.Fatal(err)
		}
	}
	for _, err := range []error{
		txn.LogInodeCreate(&InodeCreatePayload{InodeNum: 42, Mode: 0o100644, UID: 1000, GID: 1000, ParentInodeNum: 2, NameHash: 0xfeed}),
		txn.LogDirRename(&DirRenamePayload{OldParentInodeNum: 2, NewParentInodeNum: 3, InodeNum: 42, EntryType: DirEntryRegular, OldName: "old.txt", NewName: "new-name.txt"}),
		txn.LogExtentAdd(&ExtentAddPayload{InodeNum: 42, DiskBlock: 100, Length: 2, Flags: ExtentFlagWritten}),
		txn.LogInodeDelete(&InodeDeletePayload{InodeNum: 42}),
	} {
		if err != nil {
			tb.Fatal(err)
		}
	}
	buf, err := txn.Serialize()
	if err != nil {
		tb.Fatal(err)
	}
	return buf
}

func fixRecordChecksums(area []byte) {
	for off := 0; off+TxnHeaderSize <= len(area); off += storage.SectorSize {
		rec := area[off:]
		switch binary.LittleEndian.Uint32(rec) {
		case TxnMagic:
			n := int(binary.LittleEndian.Uint32(rec[16:20]))
			if n >= TxnHeaderSize && n <= len(rec) {
				binary.LittleEndian.PutUint32(rec[56:60], computeTxnChecksum(rec[:n]))
			}
		case IntentCompletionMagic:
			crc := storage.CRC32CWithSeed(storage.CRC32C(rec[:20]), rec[24:32])
			binary.LittleEndian.PutUint32(rec[20:24], crc)
		}
	}
}

func decodeAllPayloads(p []byte) {
	_, _ = DeserializeInodeCreate(p)
	_, _ = DeserializeInodeUpdate(p)
	_, _ = DeserializeInodeDelete(p)
	_, _ = DeserializeDirAddEntry(p)
	_, _ = DeserializeDirRemoveEntry(p)
	_, _ = DeserializeDirRename(p)
	_, _ = DeserializeBlockAlloc(p)
	_, _ = DeserializeBlockFree(p)
	_, _ = DeserializeSFBAlloc(p)
	_, _ = DeserializeSFBFree(p)
	_, _ = DeserializeSFBMigrate(p)
	_, _ = DeserializeExtentAdd(p)
	_, _ = DeserializeExtentRemove(p)
	_, _ = DeserializeSnapshotCreate(p)
	_, _ = DeserializeSnapshotDelete(p)
	_, _ = DeserializeRefCountUpdate(p)
}

func heapAllocBytes() uint64 {
	s := []metrics.Sample{{Name: "/gc/heap/allocs:bytes"}}
	metrics.Read(s)
	return s[0].Value.Uint64()
}
