package node

import (
	"bytes"
	"encoding/binary"
	"os"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

// FuzzRegistrySlot decodes a node registry slot another node wrote, or tore,
// and reads a registry that holds it.
func FuzzRegistrySlot(f *testing.F) {
	now := time.Now()
	active := &Slot{Magic: SlotMagic, State: NodeStateActive, UUID: [16]byte{1}, HeartbeatSeq: 9,
		HeartbeatTime: now.Unix(), MountTime: now.Add(-time.Hour).Unix(), JournalOffset: 5 << 20, JournalSize: 4 << 20,
		PID: 4242, ProcStart: now.UnixNano(), PIDNamespace: 4026531836}
	copy(active.Hostname[:], "node-a")
	dead := *active
	dead.State, dead.RecoveryOwner, dead.RecoveryStart = NodeStateDead, [16]byte{2}, now.Unix()
	stale := *active
	stale.HeartbeatTime = now.Add(-time.Hour).Unix()
	for _, s := range []*Slot{active, &dead, &stale, {Magic: SlotMagic, State: NodeStateEmpty}} {
		f.Add(uint8(0), serializeSlot(s), false)
	}
	f.Add(uint8(63), serializeSlot(active)[:SlotSize/2], true)
	f.Add(uint8(1), []byte{}, false)

	dir := f.TempDir()
	f.Fuzz(func(t *testing.T, slotIdx uint8, data []byte, fixChecksum bool) {
		buf := make([]byte, SlotSize)
		copy(buf, data)
		if fixChecksum {
			binary.LittleEndian.PutUint32(buf[SlotSize-4:], storage.CRC32C(buf[:SlotSize-4]))
		}

		if slot, err := deserializeSlot(buf); err == nil {
			checkSlot(t, buf, slot)
		}
		checkRegistryReads(t, dir, int(slotIdx)%MaxNodes, buf)
	})
}

func checkSlot(t *testing.T, buf []byte, slot *Slot) {
	if binary.LittleEndian.Uint32(buf[SlotSize-4:]) != storage.CRC32C(buf[:SlotSize-4]) && !isZero(buf) {
		t.Fatalf("accepted a slot that fails its checksum: magic %#x, state %s", slot.Magic, slot.State)
	}
	again, err := deserializeSlot(serializeSlot(slot))
	if err != nil {
		t.Fatalf("decoding a re-encoded slot: %v", err)
	}
	again.Checksum = slot.Checksum
	if *again != *slot {
		t.Fatalf("slot changed after an encode/decode round trip: %+v != %+v", again, slot)
	}

	r := NewRegistry(nil, 0)
	_, _ = slot.Host(), slot.State.String()
	r.observe(0, slot)
	if r.stale(0, slot, HeartbeatTimeout) && !r.deadLocalOwner(slot) {
		t.Fatalf("state %s: a slot seen for the first time counted as stale", slot.State)
	}
	expiry := slot.LeaseExpiry()
	switch slot.State {
	case NodeStateActive, NodeStateLeaving:
		old := heartbeatOlderThan(slot, HeartbeatTimeout)
		if gap := time.Until(expiry); (gap > time.Second && old) || (gap < -time.Second && !old) {
			t.Fatalf("state %s: heartbeat old is %v but the lease ends %s from now (heartbeat %d)",
				slot.State, old, gap, slot.HeartbeatTime)
		}
	case NodeStateDead:
		if want := time.Unix(slot.RecoveryStart, 0).Add(recoveryLease); !expiry.Equal(want) {
			t.Fatalf("dead slot: lease ends %v, want recovery start %d plus the lease", expiry, slot.RecoveryStart)
		}
	default:
		if !expiry.IsZero() {
			t.Fatalf("state %s: lease ends %v", slot.State, expiry)
		}
	}
}

func isZero(buf []byte) bool {
	return bytes.Count(buf, []byte{0}) == len(buf)
}

// checkRegistryReads puts buf in slot slotIdx of an otherwise empty registry
// and reads it back the ways mount and fsck do.
func checkRegistryReads(t *testing.T, dir string, slotIdx int, buf []byte) {
	f, err := os.CreateTemp(dir, "registry-*.img")
	if err != nil {
		t.Fatal(err)
	}
	path := f.Name()
	t.Cleanup(func() { _ = os.Remove(path) })
	if err := f.Truncate(MaxNodes * SlotSize); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	dev, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dev.Close() })
	sector := storage.NewAlignedBuffer(SlotSize)
	copy(sector.Bytes(), buf)
	if _, err := dev.WriteAt(sector.Bytes(), int64(slotIdx)*SlotSize); err != nil {
		t.Fatal(err)
	}

	r := NewRegistry(dev, 0)
	slots, _ := r.Slots()
	if len(slots) != MaxNodes {
		t.Fatalf("Slots returned %d slots, want %d", len(slots), MaxNodes)
	}
	for i, s := range slots {
		if i != slotIdx && s != nil {
			t.Fatalf("empty slot %d reads as %+v", i, s)
		}
	}
	if _, err := r.GetActiveNodes(); err != nil {
		t.Fatalf("GetActiveNodes: %v", err)
	}
	failed, err := r.GetFailedNodes()
	if err != nil {
		t.Fatalf("GetFailedNodes: %v", err)
	}
	for _, info := range failed {
		if info.SlotIndex != slotIdx {
			t.Fatalf("empty slot %d reported as a failed node", info.SlotIndex)
		}
	}
}
