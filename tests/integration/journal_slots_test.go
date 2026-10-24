package integration

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

var (
	registeredRe   = regexp.MustCompile(`Registered as node \d+ \(UUID: ([0-9a-f]+)\)`)
	slotRe         = regexp.MustCompile(`Registered as node (\d+) `)
	journalInitRe  = regexp.MustCompile(`Journal: initialized at offset (\d+)`)
	selfRecoveryRe = regexp.MustCompile(`Journal recovery: replayed \d+ committed transactions .* from crashed node ([0-9a-f]+)`)
)

// TestMultiNodeRestartKeepsPeerJournal restarts node A while node B keeps
// creating files. A must not replay or reinitialize B's live journal, and
// after B crashes, A must find and replay B's journal through B's registry slot.
func TestMultiNodeRestartKeepsPeerJournal(t *testing.T) {
	cluster := NewMultiNodeCluster(t, 2, 300)
	nodeA, nodeB := cluster.Node(0), cluster.Node(1)
	logB := &syncBuffer{}
	nodeB.Log = logB

	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("format and start node A: %v", err)
	}
	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("start node B: %v", err)
	}
	uuidB := mustMatch(t, registeredRe, logB, "node B registration")
	offsetB := mustMatch(t, journalInitRe, logB, "node B journal init")

	writer := startFileWriter(nodeB, "peer")
	writer.waitFor(t, 10)

	logA := &syncBuffer{}
	nodeA.Log = logA
	if err := cluster.KillNode(0); err != nil {
		t.Fatalf("kill node A: %v", err)
	}
	if err := cluster.StartNode(0, false); err != nil {
		t.Fatalf("restart node A: %v", err)
	}
	writer.waitFor(t, writer.count()+10)
	acked, err := writer.stop()
	if err != nil {
		t.Fatalf("node B stopped writing while node A restarted: %v", err)
	}

	for _, m := range selfRecoveryRe.FindAllStringSubmatch(logA.String(), -1) {
		if m[1] == uuidB {
			t.Errorf("node A replayed live node B's journal (%s) during its restart", m[1])
		}
	}
	if offsetA := mustMatch(t, journalInitRe, logA, "node A journal init"); offsetA == offsetB {
		t.Errorf("node A and node B both journal at offset %s", offsetA)
	}
	verifyFiles(t, nodeA, acked)

	if testing.Short() {
		return
	}

	writer = startFileWriter(nodeB, "crash")
	writer.waitFor(t, 10)
	if err := cluster.KillNode(1); err != nil {
		t.Fatalf("kill node B: %v", err)
	}
	crashAcked, _ := writer.stop()

	assertJournalOwner(t, cluster.ImagePath, offsetB, uuidB)

	recoveredRe := regexp.MustCompile(`\[Recovery\] Journal region: offset=` + offsetB + `\b`)
	deadline := time.Now().Add(45 * time.Second)
	for !recoveredRe.MatchString(logA.String()) {
		if time.Now().After(deadline) {
			t.Fatalf("node A did not replay crashed node B's journal at offset %s within 45s", offsetB)
		}
		time.Sleep(500 * time.Millisecond)
	}
	verifyFiles(t, nodeA, append(acked, crashAcked...))
}

func assertJournalOwner(t *testing.T, imagePath, offset, uuidPrefix string) {
	t.Helper()
	dev, err := storage.Open(imagePath)
	if err != nil {
		t.Fatalf("open image: %v", err)
	}
	defer dev.Close()

	var off uint64
	fmt.Sscan(offset, &off)
	region, err := journal.OpenRegionForRecovery(dev, off, journal.DefaultJournalSlotSize)
	if err != nil {
		t.Fatalf("read node B's journal at offset %d: %v", off, err)
	}
	sb := region.Superblock()
	if owner := fmt.Sprintf("%x", sb.NodeUUID[:8]); owner != uuidPrefix {
		t.Errorf("journal at offset %d belongs to %s, want crashed node B %s", off, owner, uuidPrefix)
	}
}

// TestNodeRestartReplaysOwnJournalAtMount crashes a node and restarts it well
// inside the heartbeat timeout. It must take back its slot and replay its own
// journal before serving, not leave the replay to failure detection later.
func TestNodeRestartReplaysOwnJournalAtMount(t *testing.T) {
	cluster := NewMultiNodeCluster(t, 1, 50)
	node := cluster.Node(0)
	before := &syncBuffer{}
	node.Log = before
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("format and start: %v", err)
	}
	uuid := mustMatch(t, registeredRe, before, "registration")
	slot := mustMatch(t, slotRe, before, "registration")

	var names []string
	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("before-crash-%d", i)
		if err := node.WriteFile(name, []byte(name)); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}

	after := &syncBuffer{}
	node.Log = after
	if err := cluster.KillNode(0); err != nil {
		t.Fatalf("kill: %v", err)
	}
	start := time.Now()
	if err := cluster.StartNode(0, false); err != nil {
		t.Fatalf("restart: %v", err)
	}
	t.Logf("restarted %v after the crash", time.Since(start))

	if got := mustMatch(t, slotRe, after, "registration after restart"); got != slot {
		t.Errorf("restarted node took slot %s, want its old slot %s", got, slot)
	}
	if got := mustMatch(t, selfRecoveryRe, after, "journal replay at mount"); got != uuid {
		t.Errorf("mount replayed the journal of node %s, want its own crashed journal %s", got, uuid)
	}
	verifyFiles(t, node, names)
}

// TestMountRefusesDamagedJournal damages the header of a node's journal slot.
// Mount must fail and name the slot instead of starting a new journal over
// it, and -skip-recovery must mount and start a new journal.
func TestMountRefusesDamagedJournal(t *testing.T) {
	cluster := NewMultiNodeCluster(t, 1, 50)
	node := cluster.Node(0)
	formatted := &syncBuffer{}
	node.Log = formatted
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("format and start: %v", err)
	}
	slot := mustMatch(t, slotRe, formatted, "registration")
	offset := mustMatch(t, journalInitRe, formatted, "journal init")
	if err := node.WriteFile("kept", []byte("kept")); err != nil {
		t.Fatal(err)
	}
	if err := cluster.StopNode(0); err != nil {
		t.Fatalf("stop: %v", err)
	}

	damageJournalHeader(t, cluster.ImagePath, offset)

	refused := &syncBuffer{}
	node.Log = refused
	if err := cluster.StartNode(0, false); err == nil {
		t.Fatal("mount succeeded over a damaged journal header")
	}
	if out := refused.String(); !strings.Contains(out, "Journal recovery for slot "+slot+" failed") ||
		!strings.Contains(out, journal.ErrJournalChecksumMismatch.Error()) {
		t.Fatalf("mount output does not name slot %s and the checksum mismatch:\n%s", slot, out)
	}

	skipped := &syncBuffer{}
	node.Log = skipped
	node.ExtraArgs = []string{"-skip-recovery"}
	if err := cluster.StartNode(0, false); err != nil {
		t.Fatalf("mount with -skip-recovery: %v", err)
	}
	if got := mustMatch(t, journalInitRe, skipped, "journal init"); got != offset {
		t.Errorf("-skip-recovery started a journal at offset %s, want slot %s's offset %s", got, slot, offset)
	}
	verifyFiles(t, node, []string{"kept"})

	node.ExtraArgs = nil
	if err := cluster.StopNode(0); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if err := cluster.StartNode(0, false); err != nil {
		t.Fatalf("mount after -skip-recovery started a new journal: %v", err)
	}
}

func damageJournalHeader(t *testing.T, imagePath, offset string) {
	t.Helper()
	dev, err := storage.Open(imagePath)
	if err != nil {
		t.Fatalf("open image: %v", err)
	}
	defer dev.Close()

	var off int64
	fmt.Sscan(offset, &off)
	header := storage.NewAlignedBuffer(storage.SectorSize)
	if _, err := dev.ReadAt(header.Bytes(), off); err != nil {
		t.Fatalf("read journal header: %v", err)
	}
	header.Bytes()[100] ^= 0xFF
	if _, err := dev.WriteAt(header.Bytes(), off); err != nil {
		t.Fatalf("write journal header: %v", err)
	}
}

func verifyFiles(t *testing.T, node *MountedFS, names []string) {
	t.Helper()
	for _, name := range names {
		got, err := node.ReadFile(name)
		if err != nil {
			t.Errorf("read %s: %v", name, err)
			continue
		}
		if !bytes.Equal(got, []byte(name)) {
			t.Errorf("%s: got %q", name, got)
		}
	}
}

func mustMatch(t *testing.T, re *regexp.Regexp, log *syncBuffer, what string) string {
	t.Helper()
	m := re.FindStringSubmatch(log.String())
	if m == nil {
		t.Fatalf("no %s line in mount output", what)
	}
	return m[1]
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type fileWriter struct {
	mu    sync.Mutex
	acked []string
	err   error
	quit  chan struct{}
	done  chan struct{}
}

func startFileWriter(node *MountedFS, prefix string) *fileWriter {
	w := &fileWriter{quit: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		for i := 0; ; i++ {
			select {
			case <-w.quit:
				return
			default:
			}
			name := fmt.Sprintf("%s-%04d", prefix, i)
			err := node.WriteFile(name, []byte(name))
			// A create can fail with EIO when the directory lock times out under another node's readdir.
			for retry := 0; err != nil && errors.Is(err, syscall.EIO) && retry < 5; retry++ {
				time.Sleep(100 * time.Millisecond)
				err = node.WriteFile(name, []byte(name))
			}
			w.mu.Lock()
			if err != nil {
				w.err = err
				w.mu.Unlock()
				return
			}
			w.acked = append(w.acked, name)
			w.mu.Unlock()
			time.Sleep(20 * time.Millisecond)
		}
	}()
	return w
}

func (w *fileWriter) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.acked)
}

func (w *fileWriter) waitFor(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for w.count() < n {
		select {
		case <-w.done:
			t.Fatalf("writer stopped after %d files: %v", w.count(), w.err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("writer created %d of %d files in 30s", w.count(), n)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (w *fileWriter) stop() ([]string, error) {
	close(w.quit)
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.acked, w.err
}

func TestMountRefusesLegacyLayout(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	img, err := os.OpenFile(mfs.ImagePath, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	rootInode := make([]byte, 4)
	binary.LittleEndian.PutUint32(rootInode, inode.InodeMagic)
	_, err = img.WriteAt(rootInode, 2<<20+2*inode.InodeSize)
	img.Close()
	if err != nil {
		t.Fatal(err)
	}

	out, err := exec.Command(mfs.BinaryPath, "-device", mfs.ImagePath, "-mount", mfs.MountPoint).CombinedOutput()
	if err == nil {
		t.Fatalf("mount accepted a pre-v2 image:\n%s", out)
	}
	if !strings.Contains(string(out), "pre-v2 layout") {
		t.Errorf("mount output does not explain the layout change:\n%s", out)
	}
}
