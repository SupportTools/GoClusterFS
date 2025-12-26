package sfb

import (
	"testing"
)

func TestTierInfo(t *testing.T) {
	tests := []struct {
		tier          Tier
		wantSlotSize  uint32
		wantSlotCount uint16
	}{
		{Tier4KB, 4096, 256},
		{Tier8KB, 8192, 128},
		{Tier16KB, 16384, 64},
		{Tier64KB, 65536, 16},
	}

	for _, tt := range tests {
		t.Run(tt.tier.String(), func(t *testing.T) {
			slotSize, slotCount := tt.tier.Info()
			if slotSize != tt.wantSlotSize {
				t.Errorf("SlotSize = %d, want %d", slotSize, tt.wantSlotSize)
			}
			if slotCount != tt.wantSlotCount {
				t.Errorf("SlotCount = %d, want %d", slotCount, tt.wantSlotCount)
			}
		})
	}
}

func TestTierForSize(t *testing.T) {
	tests := []struct {
		size     uint64
		wantTier Tier
		wantOK   bool
	}{
		{0, Tier4KB, true},
		{1, Tier4KB, true},
		{4096, Tier4KB, true},
		{4097, Tier8KB, true},
		{8192, Tier8KB, true},
		{8193, Tier16KB, true},
		{16384, Tier16KB, true},
		{16385, Tier64KB, true},
		{65536, Tier64KB, true},
		{65537, 0, false}, // Too large for SFB
		{100000, 0, false},
	}

	for _, tt := range tests {
		t.Run(string(rune(tt.size)), func(t *testing.T) {
			tier, ok := TierForSize(tt.size)
			if ok != tt.wantOK {
				t.Errorf("TierForSize(%d) ok = %v, want %v", tt.size, ok, tt.wantOK)
			}
			if ok && tier != tt.wantTier {
				t.Errorf("TierForSize(%d) = %v, want %v", tt.size, tier, tt.wantTier)
			}
		})
	}
}

func TestPackUnpackPhysicalBlock(t *testing.T) {
	tests := []struct {
		containerBlock uint64
		slotIndex      uint16
	}{
		{0, 0},
		{1, 0},
		{0, 1},
		{100, 50},
		{0xFFFFFFFFFFFF, 0xFFFF}, // Max values
		{12345678, 255},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			packed := PackPhysicalBlock(tt.containerBlock, tt.slotIndex)
			gotContainer, gotSlot := UnpackPhysicalBlock(packed)

			if gotContainer != tt.containerBlock {
				t.Errorf("ContainerBlock: got %d, want %d", gotContainer, tt.containerBlock)
			}
			if gotSlot != tt.slotIndex {
				t.Errorf("SlotIndex: got %d, want %d", gotSlot, tt.slotIndex)
			}
		})
	}
}

func TestHeaderConstants(t *testing.T) {
	// Verify header magic constant
	if HeaderMagic != 0x53464243 {
		t.Errorf("HeaderMagic = 0x%X, want 0x53464243 ('SFBC')", HeaderMagic)
	}

	// Verify header version
	if HeaderVersion != 1 {
		t.Errorf("HeaderVersion = %d, want 1", HeaderVersion)
	}

	// Verify header size is 64 bytes
	if HeaderSize != 64 {
		t.Errorf("HeaderSize = %d, want 64", HeaderSize)
	}
}

func TestContainerBitmapOperations(t *testing.T) {
	// Create a header with known state
	h := &Header{
		Magic:     HeaderMagic,
		Version:   HeaderVersion,
		Tier:      Tier4KB,
		SlotCount: 256,
		FreeSlots: 256,
	}

	// Initially all slots should be free
	for i := uint16(0); i < h.SlotCount; i++ {
		byteIdx := i / 8
		bitIdx := i % 8
		if h.Bitmap[byteIdx]&(1<<bitIdx) != 0 {
			t.Errorf("Slot %d should be free initially", i)
		}
	}

	// Allocate slot 0
	slot0 := uint16(0)
	h.Bitmap[slot0/8] |= 1 << (slot0 % 8)
	h.FreeSlots--

	if h.FreeSlots != 255 {
		t.Errorf("FreeSlots after alloc = %d, want 255", h.FreeSlots)
	}

	// Verify slot 0 is now allocated
	if h.Bitmap[0]&1 == 0 {
		t.Error("Slot 0 should be allocated")
	}

	// Allocate slot 10
	slot10 := uint16(10)
	h.Bitmap[slot10/8] |= 1 << (slot10 % 8)
	h.FreeSlots--

	if h.FreeSlots != 254 {
		t.Errorf("FreeSlots after second alloc = %d, want 254", h.FreeSlots)
	}

	// Free slot 0
	h.Bitmap[slot0/8] &^= 1 << (slot0 % 8)
	h.FreeSlots++

	if h.FreeSlots != 255 {
		t.Errorf("FreeSlots after free = %d, want 255", h.FreeSlots)
	}

	// Verify slot 0 is now free
	if h.Bitmap[0]&1 != 0 {
		t.Error("Slot 0 should be free after free")
	}

	// Slot 10 should still be allocated
	if h.Bitmap[slot10/8]&(1<<(slot10%8)) == 0 {
		t.Error("Slot 10 should still be allocated")
	}
}

func TestTierString(t *testing.T) {
	tests := []struct {
		tier Tier
		want string
	}{
		{Tier4KB, "4KB"},
		{Tier8KB, "8KB"},
		{Tier16KB, "16KB"},
		{Tier64KB, "64KB"},
		{Tier(99), "Unknown"},
	}

	for _, tt := range tests {
		if got := tt.tier.String(); got != tt.want {
			t.Errorf("Tier(%d).String() = %q, want %q", tt.tier, got, tt.want)
		}
	}
}

func TestSlotOffset(t *testing.T) {
	tests := []struct {
		tier      Tier
		slotIndex uint16
		wantOff   uint64
	}{
		// Tier4KB: slots start at offset 64 (header), each 4KB
		{Tier4KB, 0, HeaderSize},
		{Tier4KB, 1, HeaderSize + 4096},
		{Tier4KB, 10, HeaderSize + 10*4096},
		// Tier8KB: each slot is 8KB
		{Tier8KB, 0, HeaderSize},
		{Tier8KB, 1, HeaderSize + 8192},
		// Tier16KB: each slot is 16KB
		{Tier16KB, 0, HeaderSize},
		{Tier16KB, 1, HeaderSize + 16384},
		// Tier64KB: each slot is 64KB
		{Tier64KB, 0, HeaderSize},
		{Tier64KB, 1, HeaderSize + 65536},
	}

	for _, tt := range tests {
		slotSize, _ := tt.tier.Info()
		gotOff := HeaderSize + uint64(tt.slotIndex)*uint64(slotSize)
		if gotOff != tt.wantOff {
			t.Errorf("SlotOffset(tier=%v, slot=%d) = %d, want %d",
				tt.tier, tt.slotIndex, gotOff, tt.wantOff)
		}
	}
}

func TestMaxFileSize(t *testing.T) {
	// MaxSFBFileSize should be 64KB (the largest SFB slot)
	if MaxFileSize != 65536 {
		t.Errorf("MaxFileSize = %d, want 65536", MaxFileSize)
	}
}

func TestHeaderSerializeDeserialize(t *testing.T) {
	h := &Header{
		Magic:     HeaderMagic,
		Version:   HeaderVersion,
		Tier:      Tier8KB,
		SlotCount: 128,
		FreeSlots: 100,
	}
	// Set some bits in bitmap
	h.Bitmap[0] = 0xFF // First 8 slots allocated
	h.Bitmap[1] = 0x0F // Next 4 slots allocated
	h.Bitmap[15] = 0x80

	// Serialize
	buf := h.Serialize()
	if len(buf) != HeaderSize {
		t.Errorf("Serialized size = %d, want %d", len(buf), HeaderSize)
	}

	// Deserialize
	h2, err := DeserializeHeader(buf)
	if err != nil {
		t.Fatalf("DeserializeHeader failed: %v", err)
	}

	// Compare fields
	if h2.Magic != h.Magic {
		t.Errorf("Magic = 0x%X, want 0x%X", h2.Magic, h.Magic)
	}
	if h2.Version != h.Version {
		t.Errorf("Version = %d, want %d", h2.Version, h.Version)
	}
	if h2.Tier != h.Tier {
		t.Errorf("Tier = %d, want %d", h2.Tier, h.Tier)
	}
	if h2.SlotCount != h.SlotCount {
		t.Errorf("SlotCount = %d, want %d", h2.SlotCount, h.SlotCount)
	}
	if h2.FreeSlots != h.FreeSlots {
		t.Errorf("FreeSlots = %d, want %d", h2.FreeSlots, h.FreeSlots)
	}
	if h2.Bitmap != h.Bitmap {
		t.Errorf("Bitmap mismatch")
	}
}

func TestDeserializeHeaderErrors(t *testing.T) {
	// Too short
	_, err := DeserializeHeader(make([]byte, 10))
	if err == nil {
		t.Error("Expected error for short buffer")
	}

	// Wrong magic
	buf := make([]byte, HeaderSize)
	buf[0] = 0xFF // Wrong magic
	_, err = DeserializeHeader(buf)
	if err == nil {
		t.Error("Expected error for wrong magic")
	}
}
