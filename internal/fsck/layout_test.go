package fsck_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"strings"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/node"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

func formattedDevice(t *testing.T, mutate func(*superblock.Superblock)) *storage.Device {
	t.Helper()
	dev, cleanup := testDevice(t, superblock.MinDeviceSize>>20+64)
	t.Cleanup(cleanup)

	sb, err := superblock.Format(dev, 0)
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

	sb, err := superblock.Read(dev)
	if err != nil {
		t.Fatal(err)
	}
	writeSector(t, dev, int64(sb.BlockBitmapOffset), []byte{0xff})
	problems := phaseProblems(dev, fsck.PhaseBitmap)
	if len(problems) == 0 || !strings.Contains(problems[0].Message, "checksum mismatch") {
		t.Errorf("phase 2 problems = %v, want a checksum mismatch in sector 0", problems)
	}
	// A new checksum would make the runtime trust bits that may be wrong.
	if len(problems) > 0 && problems[0].Repairable {
		t.Errorf("bitmap checksum mismatch offered as repairable: %v", problems[0])
	}
}

func TestFsckStopsOnBadLayout(t *testing.T) {
	for name, mutate := range map[string]func(*superblock.Superblock){
		"inode size 0":         func(sb *superblock.Superblock) { sb.InodeSize = 0 },
		"no data blocks":       func(sb *superblock.Superblock) { sb.TotalBlocks = 0 },
		"data past the device": func(sb *superblock.Superblock) { sb.TotalBlocks = 1 << 50 },
		"offset wraps around":  func(sb *superblock.Superblock) { sb.InodeTableOffset = ^uint64(0) &^ 4095 },
		"overlapping regions":  func(sb *superblock.Superblock) { sb.RefCountTableOffset = sb.InodeTableOffset },
	} {
		t.Run(name, func(t *testing.T) {
			opts := fsck.DefaultOptions()
			opts.Output = &bytes.Buffer{}
			opts.StopOnFatal = false
			r := fsck.New(formattedDevice(t, mutate), opts).Check()
			if !r.Aborted || r.ExitCode() != fsck.ExitOperational || r.CompletedPhase > fsck.PhaseSuperblock {
				t.Errorf("aborted=%v exit=%d completed=%s, want an abort in phase 1 with exit %d",
					r.Aborted, r.ExitCode(), r.CompletedPhase, fsck.ExitOperational)
			}
		})
	}
}

func TestFsckRepairRefusesInUseFilesystem(t *testing.T) {
	var sb *superblock.Superblock
	dev := formattedDevice(t, func(s *superblock.Superblock) { sb = s })
	run := func(mode fsck.Mode, dryRun, force bool) *fsck.Result {
		opts := fsck.DefaultOptions()
		opts.Output = &bytes.Buffer{}
		opts.Mode, opts.DryRun, opts.AllowUncleanNodes = mode, dryRun, force
		for p := fsck.PhaseBitmap; p <= fsck.PhaseCrossValidation; p++ {
			opts.SkipPhases[p] = true
		}
		return fsck.New(dev, opts).Check()
	}
	expect := func(r *fsck.Result, code int, reason string) {
		t.Helper()
		var text strings.Builder
		text.WriteString(r.AbortReason)
		for _, p := range r.Problems {
			text.WriteString("\n" + p.String())
		}
		if r.ExitCode() != code || !strings.Contains(text.String(), reason) {
			t.Errorf("exit %d, want %d with %q in:\n%s", r.ExitCode(), code, reason, text.String())
		}
	}

	t.Cleanup(fsck.SetHeartbeatWatch(2*node.HeartbeatInterval, 2*node.HeartbeatInterval))
	ctx := context.Background()
	reg := node.NewRegistry(dev, sb.NodeRegistryOffset)
	slot, err := reg.Register(ctx, "node-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.StartHeartbeat(); err != nil {
		t.Fatal(err)
	}
	expect(run(fsck.Repair, false, true), fsck.ExitOperational, "mounted by node")
	expect(run(fsck.Repair, true, false), fsck.ExitClean, "repair would refuse: filesystem is mounted by node")

	// A stopped mount leaves its slot active with a recent heartbeat.
	reg.Stop()
	expect(run(fsck.Repair, false, false), fsck.ExitClean, "")

	off := int64(sb.NodeRegistryOffset) + int64(slot)*node.SlotSize
	buf := storage.NewAlignedBuffer(node.SlotSize).Bytes()
	if _, err := dev.ReadAt(buf, off); err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint64(buf[32:40], uint64(time.Now().Add(-time.Hour).Unix()))
	binary.LittleEndian.PutUint32(buf[node.SlotSize-4:], storage.CRC32C(buf[:node.SlotSize-4]))
	writeSector(t, dev, off, buf)
	// A node that never wrote its journal left nothing to replay.
	expect(run(fsck.Repair, false, false), fsck.ExitClean, "")

	journalAt := int64(sb.JournalRegionOffset + uint64(slot)*sb.JournalSlotSize())
	setJournal := func(state journal.JournalState) {
		jsb := journal.NewJournalSuperblock([16]byte{1}, 1, 1<<20)
		jsb.State = state
		writeSector(t, dev, journalAt, jsb.Serialize())
	}
	setJournal(journal.JournalStateDirty)
	expect(run(fsck.Repair, false, false), fsck.ExitOperational, "stopped with a dirty journal")
	expect(run(fsck.Repair, false, true), fsck.ExitClean, "")
	setJournal(journal.JournalStateClean)
	expect(run(fsck.Repair, false, false), fsck.ExitClean, "")
	setJournal(journal.JournalStateDirty)

	if err := reg.Unregister(ctx); err != nil {
		t.Fatal(err)
	}
	expect(run(fsck.Repair, false, false), fsck.ExitClean, "")
}

// A node that mounts while repair watches the registry, or after the watch
// but before repair writes, stops the repair.
func TestFsckRepairNoticesNodeMounting(t *testing.T) {
	t.Cleanup(fsck.SetHeartbeatWatch(3*node.HeartbeatInterval, 3*node.HeartbeatInterval))
	var sb *superblock.Superblock
	dev := formattedDevice(t, func(s *superblock.Superblock) { sb = s })
	ctx := context.Background()
	repair := func(lastPhase fsck.Phase) *fsck.Result {
		opts := fsck.DefaultOptions()
		opts.Output = &bytes.Buffer{}
		opts.Mode = fsck.Repair
		for p := lastPhase + 1; p <= fsck.PhaseCrossValidation; p++ {
			opts.SkipPhases[p] = true
		}
		return fsck.New(dev, opts).Check()
	}

	stopped := node.NewRegistry(dev, sb.NodeRegistryOffset)
	if _, err := stopped.Register(ctx, "stopped"); err != nil {
		t.Fatal(err)
	}
	done := make(chan *fsck.Result)
	go func() { done <- repair(fsck.PhaseSuperblock) }()
	time.Sleep(node.HeartbeatInterval)
	late := node.NewRegistry(dev, sb.NodeRegistryOffset)
	if _, err := late.Register(ctx, "late"); err != nil {
		t.Fatal(err)
	}
	if r := <-done; r.ExitCode() != fsck.ExitOperational || !strings.Contains(r.AbortReason, "mounted by node 1 (late)") {
		t.Errorf("node mounting during the watch: exit %d, reason %q", r.ExitCode(), r.AbortReason)
	}
	for _, reg := range []*node.Registry{stopped, late} {
		if err := reg.Unregister(ctx); err != nil {
			t.Fatal(err)
		}
	}

	// A free block with a count gives repair something to write.
	sector := storage.NewAlignedBuffer(storage.SectorSize).Bytes()
	binary.LittleEndian.PutUint32(sector[0:], 3)
	binary.LittleEndian.PutUint32(sector[storage.SectorSize-4:], storage.CRC32C(sector[:storage.SectorSize-4]))
	writeSector(t, dev, int64(sb.RefCountTableOffset), sector)
	var afterWatch *node.Registry
	t.Cleanup(fsck.SetBeforeRepairWrites(func() {
		afterWatch = node.NewRegistry(dev, sb.NodeRegistryOffset)
		if _, err := afterWatch.Register(ctx, "after-watch"); err != nil {
			t.Error(err)
		}
	}))
	r := repair(fsck.PhaseRefcounts)
	if r.ExitCode()&fsck.ExitOperational == 0 || !strings.Contains(r.AbortReason, "changed its node registry slot") {
		t.Errorf("node mounting before the writes: exit %d, reason %q", r.ExitCode(), r.AbortReason)
	}
	if _, err := dev.ReadAt(sector, int64(sb.RefCountTableOffset)); err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint32(sector) != 3 {
		t.Error("repair wrote refcounts after a node mounted")
	}
}

func TestFsckExitCodes(t *testing.T) {
	problem := func(sev fsck.Severity, repaired, wouldRepair bool) fsck.Problem {
		return fsck.Problem{Severity: sev, Repairable: repaired || wouldRepair, Repaired: repaired, WouldRepair: wouldRepair}
	}
	for _, tc := range []struct {
		name     string
		aborted  bool
		problems []fsck.Problem
		want     int
	}{
		{"clean", false, nil, fsck.ExitClean},
		{"warnings only", false, []fsck.Problem{problem(fsck.Warning, false, false)}, fsck.ExitClean},
		{"error", false, []fsck.Problem{problem(fsck.Error, false, false)}, fsck.ExitUncorrected},
		{"error a dry run would fix", false, []fsck.Problem{problem(fsck.Error, false, true)}, fsck.ExitUncorrected},
		{"all errors repaired", false, []fsck.Problem{problem(fsck.Critical, true, false), problem(fsck.Warning, false, false)}, fsck.ExitCorrected},
		{"some errors repaired", false, []fsck.Problem{problem(fsck.Error, true, false), problem(fsck.Error, false, false)}, fsck.ExitCorrected | fsck.ExitUncorrected},
		{"aborted", true, []fsck.Problem{problem(fsck.Fatal, false, false)}, fsck.ExitOperational},
		{"aborted after repairs", true, []fsck.Problem{problem(fsck.Error, true, false), problem(fsck.Fatal, false, false)}, fsck.ExitCorrected | fsck.ExitOperational},
	} {
		r := &fsck.Result{Aborted: tc.aborted}
		for _, p := range tc.problems {
			r.AddProblem(p)
		}
		if got := r.ExitCode(); got != tc.want {
			t.Errorf("%s: exit %d, want %d", tc.name, got, tc.want)
		}
	}
}
