package fsck_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

func formattedDevice(t *testing.T, mutate func(*superblock.Superblock)) *storage.Device {
	t.Helper()
	dev, cleanup := testDevice(t, superblock.MinDeviceSize>>20+64)
	t.Cleanup(cleanup)

	sb, err := superblock.Format(dev)
	if err != nil {
		t.Fatalf("Format: %v", err)
	}
	mutate(sb)
	writeSector(t, dev, 0, sb.Serialize())
	return dev
}

func writeSector(t *testing.T, dev *storage.Device, offset int64, data []byte) {
	t.Helper()
	buf := storage.NewAlignedBuffer(storage.SectorSize)
	copy(buf.Bytes(), data)
	if _, err := dev.WriteAt(buf.Bytes(), offset); err != nil {
		t.Fatal(err)
	}
}

func phaseProblems(dev *storage.Device, phase fsck.Phase) []fsck.Problem {
	opts := fsck.DefaultOptions()
	opts.Output = &bytes.Buffer{}
	for p := phase + 1; p <= fsck.PhaseCrossValidation; p++ {
		opts.SkipPhases[p] = true
	}
	var problems []fsck.Problem
	for _, p := range fsck.New(dev, opts).Check().Problems {
		if p.Phase == phase {
			problems = append(problems, p)
		}
	}
	return problems
}

func phase1Problems(t *testing.T, mutate func(*superblock.Superblock)) []fsck.Problem {
	t.Helper()
	return phaseProblems(formattedDevice(t, mutate), fsck.PhaseSuperblock)
}

func TestFsckAcceptsFormattedLayout(t *testing.T) {
	if problems := phase1Problems(t, func(*superblock.Superblock) {}); len(problems) > 0 {
		t.Errorf("phase 1 problems on a freshly formatted device: %v", problems)
	}
}

func TestFsckRejectsUnevenJournalSlots(t *testing.T) {
	problems := phase1Problems(t, func(sb *superblock.Superblock) { sb.MaxNodes = 48 })
	if len(problems) != 1 || !strings.Contains(problems[0].Message, "do not split into 48 node journals") {
		t.Errorf("phase 1 problems = %v, want one uneven journal split", problems)
	}
}

func TestFsckRejectsMoreJournalsThanRegistrySlots(t *testing.T) {
	problems := phase1Problems(t, func(sb *superblock.Superblock) { sb.MaxNodes = 512 })
	if len(problems) != 1 || !strings.Contains(problems[0].Message, "registry holds 256 node slots") {
		t.Errorf("phase 1 problems = %v, want registry too small for 512 journals", problems)
	}
}

func TestFsckBitmapAcceptsUnwrittenSectors(t *testing.T) {
	dev := formattedDevice(t, func(*superblock.Superblock) {})
	if problems := phaseProblems(dev, fsck.PhaseBitmap); len(problems) > 0 {
		t.Errorf("phase 2 problems on a freshly formatted device: %v", problems)
	}

	writeSector(t, dev, 1<<20, []byte{0xff})
	problems := phaseProblems(dev, fsck.PhaseBitmap)
	if len(problems) == 0 || !strings.Contains(problems[0].Message, "checksum mismatch") {
		t.Errorf("phase 2 problems = %v, want a checksum mismatch in sector 0", problems)
	}
}
