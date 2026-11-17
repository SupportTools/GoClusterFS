package integration

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/supporttools/GoClusterFS/internal/extent"
	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/journal"
	"github.com/supporttools/GoClusterFS/internal/lock"
	"github.com/supporttools/GoClusterFS/internal/node"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// findFsckBinary locates goclusterfs-fsck next to goclusterfs-mount.
func findFsckBinary(t *testing.T) string {
	t.Helper()
	mount, err := findBinary()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(filepath.Dir(mount), "goclusterfs-fsck")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("goclusterfs-fsck not found next to %s; run 'make build' first", mount)
	}
	return path
}

// runFsck runs goclusterfs-fsck and returns its exit code and output.
func runFsck(t *testing.T, args ...string) (int, string) {
	t.Helper()
	out, err := exec.Command(findFsckBinary(t), args...).CombinedOutput()
	var exit *exec.ExitError
	switch {
	case err == nil:
		return 0, string(out)
	case errors.As(err, &exit):
		return exit.ExitCode(), string(out)
	}
	t.Fatalf("run goclusterfs-fsck %v: %v", args, err)
	return 0, ""
}

// problemLines returns the problems fsck printed.
func problemLines(out string) []string {
	var lines []string
	for _, line := range strings.Split(out, "\n") {
		for _, sev := range []string{"[INFO]", "[WARN]", "[ERROR]", "[CRIT]", "[FATAL]"} {
			if strings.HasPrefix(strings.TrimSpace(line), sev) {
				lines = append(lines, line)
			}
		}
	}
	return lines
}

func expectFsck(t *testing.T, wantCode int, wantText string, args ...string) string {
	t.Helper()
	code, out := runFsck(t, args...)
	if code != wantCode || !strings.Contains(out, wantText) {
		t.Fatalf("goclusterfs-fsck %v: exit %d, want %d with %q:\n%s", args, code, wantCode, wantText, out)
	}
	return out
}

func inodeNum(t *testing.T, path string) uint32 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return uint32(fi.Sys().(*syscall.Stat_t).Ino)
}

// A filesystem the runtime wrote, with snapshots, clones, copies on write
// and extent trees, passes fsck without a single problem. fsck refuses to
// repair it while it is mounted, and repairs nothing after a clean unmount.
func TestFsckCleanAfterWorkload(t *testing.T) {
	mfs := NewMountedFS(t, 64)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount: %v", err)
	}
	write := func(name string, data []byte, off int64) {
		t.Helper()
		if err := mfs.WriteFileAt(name, data, off); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	setxattr := func(name, attr, value string) {
		t.Helper()
		if err := unix.Setxattr(mfs.Path(name), attr, []byte(value), 0); err != nil {
			t.Fatalf("setxattr %s=%s on %s: %v", attr, value, name, err)
		}
	}

	write("small.txt", bytes.Repeat([]byte("s"), 100), 0)
	write("big.bin", bytes.Repeat([]byte("b"), 3<<20), 0)
	for i := int64(0); i < 5; i++ {
		write("sparse.bin", bytes.Repeat([]byte{byte('0' + i)}, 4096), i*2<<20)
	}
	setxattr("big.bin", "user.snapshot", "s1")
	write("big.bin", []byte("copy on write"), 10)
	setxattr("big.bin", "user.clone", "/big-clone.bin")
	write("big-clone.bin", []byte("clone write"), 1<<20)
	setxattr("big.bin", "user.snapshot.clone", "s1:/from-s1.bin")
	setxattr("sparse.bin", "user.snapshot", "s2")
	write("sparse.bin", []byte("tree copy on write"), 4<<20)
	if err := unix.Removexattr(mfs.Path("sparse.bin"), "user.snapshot.s2"); err != nil {
		t.Fatalf("delete snapshot: %v", err)
	}
	waitForSnapshotState(t, mfs.Path("sparse.bin"), "", "s2")
	if err := mfs.Remove("small.txt"); err != nil {
		t.Fatal(err)
	}

	expectFsck(t, 8, "mounted by", "-repair", mfs.ImagePath)
	if err := mfs.Unmount(); err != nil {
		t.Fatal(err)
	}

	out := expectFsck(t, 0, "Status: CLEAN", mfs.ImagePath)
	if lines := problemLines(out); len(lines) > 0 {
		t.Fatalf("fsck found problems on a runtime image:\n%s", strings.Join(lines, "\n"))
	}
	// The mount binary leaves its slot behind with a clean journal.
	expectFsck(t, 0, "Status: CLEAN", "-repair", mfs.ImagePath)
	expectFsck(t, 16, "-dry-run needs -repair", "-dry-run", mfs.ImagePath)
	expectFsck(t, 8, "Failed to open device", filepath.Join(t.TempDir(), "missing.img"))
}

// shareLikeOlderVersions edits an unmounted image the way versions before
// counted sharing left it: src marked shared without FlagCounted, and clone
// mapping src's blocks with nobody counting either owner.
func shareLikeOlderVersions(t *testing.T, image string, src, clone uint32) {
	t.Helper()
	ctx := context.Background()
	dev, err := storage.Open(image)
	if err != nil {
		t.Fatal(err)
	}
	defer mustClose(t, dev)
	sb, err := superblock.Read(dev)
	if err != nil {
		t.Fatal(err)
	}
	tbl := inode.NewTable(dev, sb.InodeTableOffset, sb.InodeCount*inode.InodeSize, nil, [16]byte{})
	srcIno, err := tbl.Read(src)
	if err != nil {
		t.Fatal(err)
	}
	if srcIno.HasExtentTree() {
		t.Fatalf("inode %d: want inline extents", src)
	}
	for i := range srcIno.InlineExtents {
		if srcIno.InlineExtents[i].Length > 0 {
			srcIno.InlineExtents[i].Flags |= extent.FlagShared
		}
	}
	if err := tbl.Write(ctx, srcIno); err != nil {
		t.Fatal(err)
	}
	cloneIno, err := tbl.Read(clone)
	if err != nil {
		t.Fatal(err)
	}
	cloneIno.InlineExtents, cloneIno.ExtentCount = srcIno.InlineExtents, srcIno.ExtentCount
	cloneIno.Size, cloneIno.AllocatedBlocks, cloneIno.WrittenBlocks = srcIno.Size, srcIno.AllocatedBlocks, srcIno.WrittenBlocks
	if err := tbl.Write(ctx, cloneIno); err != nil {
		t.Fatal(err)
	}
}

// An image from before counted sharing refuses snapshots of shared files
// with ENOTSUP. goclusterfs-fsck -repair rebuilds the refcounts, after which
// snapshots work and writes to either owner leave the other intact.
func TestFsckRepairsOlderSharing(t *testing.T) {
	mfs := NewMountedFS(t, 64)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount: %v", err)
	}
	srcData := bytes.Repeat([]byte("src!"), 2<<20/4)
	if err := mfs.WriteFile("src.bin", srcData); err != nil {
		t.Fatal(err)
	}
	if err := mfs.WriteFile("old-clone.bin", nil); err != nil {
		t.Fatal(err)
	}
	src, clone := inodeNum(t, mfs.Path("src.bin")), inodeNum(t, mfs.Path("old-clone.bin"))
	if err := mfs.Unmount(); err != nil {
		t.Fatal(err)
	}
	shareLikeOlderVersions(t, mfs.ImagePath, src, clone)

	if err := mfs.Mount(false); err != nil {
		t.Fatal(err)
	}
	if err := unix.Setxattr(mfs.Path("src.bin"), "user.snapshot", []byte("refused"), 0); !errors.Is(err, unix.ENOTSUP) {
		t.Fatalf("snapshot before repair: %v, want ENOTSUP", err)
	}
	waitForSnapshotState(t, mfs.Path("src.bin"), "", "refused")
	if err := mfs.Unmount(); err != nil {
		t.Fatal(err)
	}

	before := expectFsck(t, 4, "older versions", mfs.ImagePath)
	expectFsck(t, 4, "nothing was written", "-repair", "-dry-run", mfs.ImagePath)
	if after := expectFsck(t, 4, "older versions", mfs.ImagePath); strings.Join(problemLines(after), "\n") != strings.Join(problemLines(before), "\n") {
		t.Fatalf("dry run changed the image:\nbefore:\n%s\nafter:\n%s", before, after)
	}
	expectFsck(t, 1, "Status: REPAIRED", "-repair", mfs.ImagePath)
	if out := expectFsck(t, 0, "Status: CLEAN", mfs.ImagePath); len(problemLines(out)) > 0 {
		t.Fatalf("problems left after repair:\n%s", out)
	}

	if err := mfs.Mount(false); err != nil {
		t.Fatal(err)
	}
	if err := unix.Setxattr(mfs.Path("src.bin"), "user.snapshot", []byte("after"), 0); err != nil {
		t.Fatalf("snapshot after repair: %v", err)
	}
	if err := mfs.WriteFileAt("old-clone.bin", []byte("clone"), 10); err != nil {
		t.Fatal(err)
	}
	if err := mfs.WriteFileAt("src.bin", []byte("source"), 1<<20+10); err != nil {
		t.Fatal(err)
	}
	if err := unix.Setxattr(mfs.Path("src.bin"), "user.snapshot.clone", []byte("after:/from-after.bin"), 0); err != nil {
		t.Fatalf("clone snapshot: %v", err)
	}
	for name, want := range map[string][]byte{
		"src.bin":        overwrite(srcData, 1<<20+10, []byte("source")),
		"old-clone.bin":  overwrite(srcData, 10, []byte("clone")),
		"from-after.bin": srcData,
	} {
		if got, err := mfs.ReadFile(name); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("%s: wrong data after repair (err=%v)", name, err)
		}
	}
	if err := mfs.Unmount(); err != nil {
		t.Fatal(err)
	}
	if out := expectFsck(t, 0, "Status: CLEAN", mfs.ImagePath); len(problemLines(out)) > 0 {
		t.Fatalf("problems after using the repaired image:\n%s", out)
	}
}

// TestFsckReleasesDeadSlot frees the slot of a node that will not return and
// whose journal header is damaged, so a mount can use the slot again.
func TestFsckReleasesDeadSlot(t *testing.T) {
	cluster := NewMultiNodeCluster(t, 1, 50)
	mfs := cluster.Node(0)
	formatted := &syncBuffer{}
	mfs.Log = formatted
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("format and start: %v", err)
	}
	slot := mustMatch(t, slotRe, formatted, "registration")
	offset := mustMatch(t, journalInitRe, formatted, "journal init")
	image := cluster.ImagePath

	expectFsck(t, 8, "mounted by node "+slot, "-release-slot", slot, image)
	expectFsck(t, 0, "Nothing was written", "-inspect-slot", slot, image)

	if err := cluster.KillNode(0); err != nil {
		t.Fatalf("kill: %v", err)
	}
	backdateHeartbeat(t, image, slot, time.Hour)
	damageJournalHeader(t, image, offset)

	expectFsck(t, 0, "damaged: journal superblock checksum mismatch", "-list-slots", image)
	expectFsck(t, 0, "header damaged: journal superblock checksum mismatch. A replay fails here", "-inspect-slot", slot, image)
	expectFsck(t, 16, "-inspect-slot needs a slot number", "-inspect-slot", "-2", image)
	expectFsck(t, 16, "no such node registry slot", "-inspect-slot", "64", image)
	expectFsck(t, 16, "need -release-slot", "-discard-journal", image)
	expectFsck(t, 16, "pick one of -salvage and -discard-journal", "-release-slot", slot, "-salvage", "-discard-journal", image)
	expectFsck(t, 16, "no such node registry slot", "-release-slot", "64", image)
	expectFsck(t, 0, "Nothing was written", "-release-slot", slot, image)
	report := filepath.Join(t.TempDir(), "release.txt")
	expectFsck(t, 1, "Released slot "+slot, "-release-slot", slot, "-discard-journal", "-report", report, image)
	if text, err := os.ReadFile(report); err != nil || !strings.Contains(string(text), "-release-slot "+slot+" -discard-journal at ") ||
		!strings.Contains(string(text), "Released slot "+slot) {
		t.Errorf("report file: %v\n%s", err, text)
	}

	restarted := &syncBuffer{}
	mfs.Log = restarted
	if err := cluster.StartNode(0, false); err != nil {
		t.Fatalf("mount after releasing the slot: %v", err)
	}
	if got := mustMatch(t, journalInitRe, restarted, "journal init"); got != offset {
		t.Errorf("mount started its journal at offset %s, want the released slot's offset %s", got, offset)
	}
}

// TestFsckOnlineRelease frees a paused node's slot with goclusterfs-fsck while
// another node runs; the paused node, resumed, stops before its write lands. A dead
// node's committed transaction refuses an online release and an online
// -skip-recovery.
func TestFsckOnlineRelease(t *testing.T) {
	env := []string{"GOCLUSTERFS_TEST_HEARTBEAT_HOLD=30s", "GOCLUSTERFS_TEST_NO_PEER_REPLAY=1"}
	cluster := NewMultiNodeCluster(t, 2, 50)
	a, b := cluster.Node(0), cluster.Node(1)
	a.Env, b.Env = env, env
	logA := &syncBuffer{}
	a.Log = logA
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("format and start node A: %v", err)
	}
	if err := cluster.StartNode(1, false); err != nil {
		t.Fatalf("start node B: %v", err)
	}
	slot := mustMatch(t, slotRe, logA, "node A registration")
	image := cluster.ImagePath
	if err := b.WriteFile("fenced", bytes.Repeat([]byte{'0'}, 4096)); err != nil {
		t.Fatal(err)
	}
	if err := a.WriteFileAt("fenced", bytes.Repeat([]byte{'A'}, 4096), 0); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(time.Second) {
		code, out := runFsck(t, "-inspect-slot", slot, image)
		if code == 0 && strings.Contains(out, " 0 committed transactions to replay") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("node A's journal still holds committed transactions after 30s:\n%s", out)
		}
	}

	pid := a.PID()
	if err := syscall.Kill(pid, syscall.SIGSTOP); err != nil {
		t.Fatalf("stop node A: %v", err)
	}
	defer func() { _ = cluster.KillNode(0) }()
	out := expectFsck(t, 1, "Released slot "+slot, "-release-slot", slot, "-discard-journal", image)
	if !strings.Contains(out, "has the filesystem mounted") || !strings.Contains(out, "Online: ") {
		t.Errorf("online release did not say it ran online:\n%s", out)
	}
	if err := b.WriteFileAt("fenced", bytes.Repeat([]byte{'B'}, 4096), 0); err != nil {
		t.Fatalf("node B writes after the release: %v", err)
	}

	if err := syscall.Kill(pid, syscall.SIGCONT); err != nil {
		t.Fatalf("resume node A: %v", err)
	}
	go func() { _ = a.WriteFileAt("fenced", bytes.Repeat([]byte{'A'}, 4096), 0) }()
	lost := "Node registry slot " + slot + " is no longer this node's"
	for deadline := time.Now().Add(45 * time.Second); !strings.Contains(logA.String(), lost) && time.Now().Before(deadline); {
		time.Sleep(100 * time.Millisecond)
	}
	if !strings.Contains(logA.String(), lost) {
		t.Fatal("node A did not stop after resuming from the release")
	}
	if got, err := b.ReadFile("fenced"); err != nil || !bytes.Equal(got, bytes.Repeat([]byte{'B'}, 4096)) {
		t.Fatalf("file after node A resumed: %v; want node B's data", err)
	}
	_ = cluster.KillNode(0)
	_ = a.Unmount()

	gone, journalAt, journalSize := deadNodeWithCommittedTxn(t, image)
	for _, args := range [][]string{{"-salvage"}, {"-discard-journal", "-force"}} {
		out := expectFsck(t, 8, "holds 1 committed transaction that releasing would drop", append(append([]string{"-release-slot", gone}, args...), image)...)
		if !strings.Contains(out, "Stop the cluster") || !strings.Contains(out, "goclusterfs-fsck -repair") {
			t.Errorf("online release refusal with %v does not say how to proceed:\n%s", args, out)
		}
	}
	refused := &syncBuffer{}
	a.Log = refused
	a.ExtraArgs = []string{"-skip-recovery=" + gone}
	if err := cluster.StartNode(0, false); err == nil {
		t.Fatal("a mount with -skip-recovery dropped a committed transaction while node B runs")
	}
	if out := refused.String(); !strings.Contains(out, "-skip-recovery="+gone+" refused") || !strings.Contains(out, "has a running node") {
		t.Errorf("mount with -skip-recovery while node B runs:\n%s", out)
	}
	dev, err := storage.Open(image)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dev.Close() }()
	if pending, err := journal.PendingCommitted(dev, journalAt, journalSize); pending != 1 || err != nil {
		t.Errorf("the dead node's journal after the refusals: %d committed transactions, %v; want its 1 untouched", pending, err)
	}
}

// deadNodeWithCommittedTxn registers a node in the image that never heartbeats and
// commits one transaction to its journal, as a node that crashed right after a write.
func deadNodeWithCommittedTxn(t *testing.T, imagePath string) (slot string, journalAt, journalSize uint64) {
	t.Helper()
	dev, err := storage.Open(imagePath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dev.Close() }()
	sb, err := superblock.Read(dev)
	if err != nil {
		t.Fatal(err)
	}
	reg := node.NewRegistry(dev, sb.NodeRegistryOffset)
	reg.SetLockLayout(lock.LayoutID())
	i, err := reg.Register(context.Background(), "gone")
	if err != nil {
		t.Fatal(err)
	}
	journalAt, journalSize = sb.JournalRegionOffset+uint64(i)*sb.JournalSlotSize(), sb.JournalSlotSize()
	j, err := journal.Init(dev, journalAt, journalSize, [16]byte{7}, 1, journal.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	txn := j.Begin()
	if err := txn.LogInodeDelete(&journal.InodeDeletePayload{InodeNum: 1 << 20}); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Commit(txn); err != nil {
		t.Fatal(err)
	}
	return strconv.Itoa(i), journalAt, journalSize
}

func backdateHeartbeat(t *testing.T, imagePath, slot string, by time.Duration) {
	t.Helper()
	dev, err := storage.Open(imagePath)
	if err != nil {
		t.Fatalf("open image: %v", err)
	}
	defer func() { _ = dev.Close() }()
	sb, err := superblock.Read(dev)
	if err != nil {
		t.Fatalf("read superblock: %v", err)
	}
	i, err := strconv.Atoi(slot)
	if err != nil {
		t.Fatalf("parse slot %q: %v", slot, err)
	}

	off := int64(sb.NodeRegistryOffset) + int64(i)*node.SlotSize
	buf := storage.NewAlignedBuffer(node.SlotSize).Bytes()
	if _, err := dev.ReadAt(buf, off); err != nil {
		t.Fatalf("read slot %d: %v", i, err)
	}
	binary.LittleEndian.PutUint64(buf[32:40], uint64(time.Now().Add(-by).Unix()))
	binary.LittleEndian.PutUint32(buf[node.SlotSize-4:], storage.CRC32C(buf[:node.SlotSize-4]))
	if _, err := dev.WriteAt(buf, off); err != nil {
		t.Fatalf("write slot %d: %v", i, err)
	}
}
