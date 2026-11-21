package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/supporttools/GoClusterFS/internal/fence"
	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

const (
	victimWait  = 10 * time.Minute
	preemptHold = 30 * time.Second
)

// reservationTest is one run of -test-reservations: alone on one host, or one part of the two-host or persistence test.
type reservationTest struct {
	victim    bool
	preempt   uint64
	keep      bool
	checkKept uint64
	force     bool
}

// testReservations qualifies devicePath for storage fencing. It refuses a GoClusterFS volume, and a volume a node runs
// on, unless force. failed reports a step the device failed.
func testReservations(out io.Writer, dev *storage.Device, devicePath string, t reservationTest) (failed bool, err error) {
	if err := mayTestReservations(out, dev, devicePath, t.force); err != nil {
		return false, err
	}
	f, err := os.OpenFile(devicePath, os.O_RDWR, 0)
	if err != nil {
		return false, fmt.Errorf("opening %s for its reservations failed: %w", devicePath, err)
	}
	defer func() { _ = f.Close() }()
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return false, fmt.Errorf("refused: another process on this host holds %s for fencing, maybe a goclusterfs-mount (%v)", devicePath, err)
	}
	write, closeWrite, err := rewriteFirstBlock(devicePath)
	if err != nil {
		return false, err
	}
	defer closeWrite()

	counts := map[fence.Outcome]int{}
	s := fence.NewSelfTest(f, write, func(c fence.Check) {
		counts[c.Outcome]++
		_, _ = fmt.Fprintf(out, "%s  %s: %s\n", c.Outcome, c.Step, c.Answer)
	})
	switch {
	case t.victim:
		s.Victim(victimWait, func(key uint64) {
			_, _ = fmt.Fprintf(out, "\nHost A holds key %#016x. Within %v, run on host B, with its path to the same device:\n"+
				"  goclusterfs-fsck -test-reservations -preempt %#016x DEVICE\n\n", key, victimWait, key)
		})
	case t.preempt != 0:
		s.Preempt(t.preempt, preemptHold)
	case t.keep:
		if key, ok := s.Keep(); ok {
			_, _ = fmt.Fprintf(out, "\nLeft key %#016x and the reservation on %s. Detach the device and attach it again, or restart "+
				"the storage, then run on this host:\n  goclusterfs-fsck -test-reservations -check-kept %#016x %s\n",
				key, devicePath, key, devicePath)
		}
	case t.checkKept != 0:
		s.CheckKept(t.checkKept)
	default:
		s.Run()
	}
	_, err = fmt.Fprintf(out, "\n%d passed, %d failed, %d warnings, %d skipped\n", counts[fence.Pass], counts[fence.Fail], counts[fence.Warn], counts[fence.Skip])
	return s.Failed(), err
}

// mayTestReservations refuses a device that holds a GoClusterFS volume, unless force.
func mayTestReservations(out io.Writer, dev *storage.Device, devicePath string, force bool) error {
	buf := storage.NewAlignedBuffer(storage.SectorSize)
	if _, err := dev.ReadAt(buf.Bytes(), 0); err != nil {
		return fmt.Errorf("reading the first 4 KiB of %s failed: %w", devicePath, err)
	}
	if !strings.HasPrefix(string(buf.Bytes()), superblock.Magic) {
		return nil
	}
	var running []string
	if slots, err := fsck.New(dev, fsck.Options{Output: io.Discard}).Slots(); err == nil {
		for _, s := range slots {
			if s.Running {
				running = append(running, s.Name())
			}
		}
	}
	what := "a GoClusterFS volume"
	if len(running) > 0 {
		what += ", mounted by " + strings.Join(running, ", ")
	}
	if !force {
		return fmt.Errorf("refused: %s holds %s; the test registers keys on it and rewrites its first 4 KiB, so run it before you format, or pass -force", devicePath, what)
	}
	_, err := fmt.Fprintf(out, "Warning: %s holds %s; with -force the test registers keys on it and rewrites its first 4 KiB with the bytes it reads there.\n\n", devicePath, what)
	return err
}

// rewriteFirstBlock returns a write that reads the device's first 4 KiB and writes the same bytes back with O_DIRECT.
// The read works for a fenced host too, since the reservation only blocks writes.
func rewriteFirstBlock(devicePath string) (write func() error, closeWrite func(), err error) {
	fd, err := syscall.Open(devicePath, syscall.O_RDWR|syscall.O_DIRECT, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("opening %s with O_DIRECT failed: %w", devicePath, err)
	}
	buf := storage.NewAlignedBuffer(storage.SectorSize)
	write = func() error {
		if _, err := syscall.Pread(fd, buf.Bytes(), 0); err != nil {
			return fmt.Errorf("reading the first 4 KiB: %w", err)
		}
		_, err := syscall.Pwrite(fd, buf.Bytes(), 0)
		return err
	}
	return write, func() { _ = syscall.Close(fd) }, nil
}

// parseToolKey reads a key a test printed: format's and fsck's keys start with 0x4746, and preempting any other could
// fence a node.
func parseToolKey(flagName, s string) (uint64, error) {
	key, err := strconv.ParseUint(s, 0, 64)
	if err != nil || !fence.IsToolKey(key) {
		return 0, fmt.Errorf("-%s takes a key a reservation test printed, such as 0x4746..., not %q", flagName, s)
	}
	return key, nil
}

func countTrue(flags ...bool) int {
	n := 0
	for _, on := range flags {
		if on {
			n++
		}
	}
	return n
}
