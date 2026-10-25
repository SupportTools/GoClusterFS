package integration

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/supporttools/GoClusterFS/internal/inode"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// v2RefcountBlocks is the number of blocks layout v2's 1 MB refcount table covered.
const v2RefcountBlocks = 130816

// createFiles makes the directories on node 0, then every node fills its share
// of them at once. A create costs about 130 ms, so one node would take 9 minutes.
func createFiles(t *testing.T, cluster *MultiNodeCluster, dirs, perDir int) {
	t.Helper()
	start := time.Now()
	for d := 0; d < dirs; d++ {
		if err := cluster.Node(0).Mkdir(fmt.Sprintf("d%02d", d)); err != nil {
			t.Fatalf("mkdir d%02d: %v", d, err)
		}
	}
	nodes := make([]int, cluster.NodeCount())
	for i := range nodes {
		nodes[i] = i
	}
	errs := cluster.RunConcurrent(nodes, func(idx int, node *MountedFS) error {
		for d := idx; d < dirs; d += len(nodes) {
			for f := 0; f < perDir; f++ {
				if err := node.WriteFile(fmt.Sprintf("d%02d/f%03d", d, f), nil); err != nil {
					return fmt.Errorf("create file %d in d%02d: %w", f, d, err)
				}
			}
		}
		return nil
	})
	for idx, err := range errs {
		if err != nil {
			t.Fatalf("node %d: %v", idx, err)
		}
	}
	t.Logf("%d nodes created %d files in %d directories in %v", len(nodes), dirs*perDir, dirs, time.Since(start))
}

func checkFiles(t *testing.T, mfs *MountedFS, dirs, perDir int) {
	t.Helper()
	for d := 0; d < dirs; d++ {
		entries, err := mfs.ReadDir(fmt.Sprintf("d%02d", d))
		if err != nil || len(entries) != perDir {
			t.Fatalf("d%02d has %d entries (err %v), want %d", d, len(entries), err, perDir)
		}
	}
}

func statfsFiles(t *testing.T, mfs *MountedFS) uint64 {
	t.Helper()
	var st unix.Statfs_t
	if err := unix.Statfs(mfs.MountPoint, &st); err != nil {
		t.Fatal(err)
	}
	return st.Files
}

func expectCleanFsck(t *testing.T, image string) {
	t.Helper()
	out := expectFsck(t, 0, "Status: CLEAN", image)
	if lines := problemLines(out); len(lines) > 0 {
		t.Fatalf("fsck found problems:\n%s", strings.Join(lines, "\n"))
	}
}

func TestMountRefusesLayoutV2(t *testing.T) {
	mfs := NewMountedFS(t, 20)
	if err := mfs.FormatAndMount(false); err != nil {
		t.Fatalf("FormatAndMount: %v", err)
	}
	if err := mfs.Unmount(); err != nil {
		t.Fatal(err)
	}
	dev, err := storage.Open(mfs.ImagePath)
	if err != nil {
		t.Fatal(err)
	}
	sb, err := superblock.Read(dev)
	if err == nil {
		sb.Version = 2
		err = superblock.Write(dev, sb)
	}
	dev.Close()
	if err != nil {
		t.Fatal(err)
	}

	out, err := exec.Command(mfs.BinaryPath, "-device", mfs.ImagePath, "-mount", mfs.MountPoint).CombinedOutput()
	if err == nil {
		t.Fatalf("mount accepted a v2 image:\n%s", out)
	}
	if !strings.Contains(string(out), "Layout v2") || !strings.Contains(string(out), "reformat with -format") {
		t.Errorf("mount output does not explain the layout change:\n%s", out)
	}
	if code, out := runFsck(t, mfs.ImagePath); code == 0 || !strings.Contains(out, "reformat with -format") {
		t.Errorf("fsck on a v2 image: exit %d, want a failure that says to reformat:\n%s", code, out)
	}
}

// A 200 GB volume was out of reach for layout v2: it had room for 4096 inodes,
// and its refcount table ended at block 130816, so later entries overwrote the
// snapshot registry. Here 4200 files exist next to data past that block, and a
// snapshot, a copy on write and a clone of that data survive a remount.
func TestLargeVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("creates 4200 files and fallocates 128 GB on a 200 GB sparse image")
	}
	const dirs, perDir = 42, 100
	cluster := NewMultiNodeCluster(t, 4, 200<<10)
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("format and start node 0: %v", err)
	}
	if err := cluster.StartAllNodes(false); err != nil {
		t.Fatal(err)
	}
	mfs := cluster.Node(0)
	createFiles(t, cluster, dirs, perDir)

	filler, err := os.Create(mfs.Path("filler.bin"))
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err = unix.Fallocate(int(filler.Fd()), 0, 0, v2RefcountBlocks<<20)
	filler.Close()
	if err != nil {
		t.Fatalf("fallocate %d blocks: %v", v2RefcountBlocks, err)
	}
	t.Logf("fallocated %d blocks in %v", v2RefcountBlocks, time.Since(start))

	original := bytes.Repeat([]byte("high"), 3<<20/4)
	if err := mfs.WriteFile("high.bin", original); err != nil {
		t.Fatal(err)
	}
	if err := unix.Setxattr(mfs.Path("high.bin"), "user.snapshot", []byte("s1"), 0); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	waitForSnapshotState(t, mfs.Path("high.bin"), "active", "s1")
	patch := []byte("copy on write")
	if err := mfs.WriteFileAt("high.bin", patch, 10); err != nil {
		t.Fatal(err)
	}
	if err := unix.Setxattr(mfs.Path("high.bin"), "user.snapshot.clone", []byte("s1:/high-s1.bin"), 0); err != nil {
		t.Fatalf("clone from snapshot: %v", err)
	}
	modified := bytes.Clone(original)
	copy(modified[10:], patch)
	highIno := inodeNum(t, mfs.Path("high.bin"))

	check := func() {
		t.Helper()
		checkFiles(t, mfs, dirs, perDir)
		for name, want := range map[string][]byte{"high.bin": modified, "high-s1.bin": original} {
			if got, err := mfs.ReadFile(name); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("%s: read %d bytes (err %v), want %d bytes of its own data", name, len(got), err, len(want))
			}
		}
		if states := snapshotStates(t, mfs.Path("high.bin")); states["s1"] != "active" {
			t.Fatalf("snapshots of high.bin: %v, want s1 active", states)
		}
		if files := statfsFiles(t, mfs); files < 200<<10 {
			t.Fatalf("statfs reports %d inodes, want one per MB", files)
		}
	}
	time.Sleep(1500 * time.Millisecond)
	check()
	cluster.StopAllNodes()

	dev, err := storage.Open(mfs.ImagePath)
	if err != nil {
		t.Fatal(err)
	}
	sb, err := superblock.Read(dev)
	if err != nil {
		t.Fatal(err)
	}
	ino, err := inode.NewTable(dev, sb.InodeTableOffset, sb.InodeCount*inode.InodeSize, nil, [16]byte{}).Read(highIno)
	dev.Close()
	if err != nil {
		t.Fatal(err)
	}
	if block := ino.InlineExtents[0].PhysicalBlock; block < v2RefcountBlocks {
		t.Fatalf("high.bin starts at block %d, want past block %d", block, v2RefcountBlocks)
	}

	if err := mfs.Mount(false); err != nil {
		t.Fatalf("remount: %v", err)
	}
	check()
	if err := mfs.Unmount(); err != nil {
		t.Fatal(err)
	}
	expectCleanFsck(t, mfs.ImagePath)
}
