package goclusterfs

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/superblock"
)

// cloneChain is the expected content of each file in a chain of clones.
type cloneChain struct {
	mu    sync.Mutex
	files map[string][]byte
}

func (c *cloneChain) write(t *testing.T, f *File, name string, block int, seed byte) {
	t.Helper()
	off := int64(block)*BlockSize + int64(seed)*512
	data := pattern(seed, 4096)
	if n, errno := f.Write(context.Background(), nil, data, off); errno != 0 || int(n) != len(data) {
		t.Errorf("write %s block %d: n=%d errno=%v", name, block, n, errno)
		return
	}
	c.mu.Lock()
	c.files[name] = overwritten(c.files[name], int(off), data)
	c.mu.Unlock()
}

func (c *cloneChain) check(t *testing.T, f *File, name string) {
	t.Helper()
	c.mu.Lock()
	want := c.files[name]
	c.mu.Unlock()
	got, errno := f.Read(context.Background(), nil, make([]byte, len(want)), 0)
	if errno != 0 {
		t.Errorf("read %s: %v", name, errno)
		return
	}
	buf, _ := got.Bytes(make([]byte, len(want)))
	if string(buf) != string(want) {
		t.Errorf("%s does not hold what was written to it and the files it was cloned from", name)
	}
}

func (c *cloneChain) remove(t *testing.T, fsys *FS, name string) {
	t.Helper()
	if errno := fsys.Unlink(context.Background(), name); errno != 0 {
		t.Errorf("unlink %s: %v", name, errno)
	}
	c.mu.Lock()
	delete(c.files, name)
	c.mu.Unlock()
}

// A chain of clones, each cloned from the one before and written at every
// level, is deleted in random order from two nodes: first one at a time with a
// write to a survivor after each delete, then the rest at once, each node
// writing and deleting its own share. Every block must be freed exactly once,
// no refcount may stay above 0, and fsck must find nothing.
func TestDeepCloneChainDeletedFromTwoNodes(t *testing.T) {
	const depth = 8
	const blocks = depth + 1
	for seed := int64(1); seed <= 3; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			path := newFormattedImageOfSize(t, superblock.MinDeviceSize+256<<20)
			nodes := []*FS{openFormattedFS(t, path), openFormattedFS(t, path)}
			free := nodes[0].bitmapMgr.FreeCount()

			chain := &cloneChain{files: map[string][]byte{}}
			names := make([]string, depth+1)
			for i := range names {
				names[i] = fmt.Sprintf("c%d", i)
			}
			writeAt(t, createFile(t, nodes[0], names[0]), pattern(0, blocks*BlockSize), 0)
			chain.files[names[0]] = pattern(0, blocks*BlockSize)
			on := func(n int, name string) *File { return lookupFile(t, nodes[n], name) }

			for i := 1; i <= depth; i++ {
				fsys := nodes[i%2]
				setxattr(t, lookupFile(t, fsys, names[i-1]), "user.clone", "/"+names[i])
				chain.mu.Lock()
				chain.files[names[i]] = append([]byte(nil), chain.files[names[i-1]]...)
				chain.mu.Unlock()
				chain.write(t, on(i%2, names[i]), names[i], i, byte(10+i))
				chain.write(t, on((i+1)%2, names[i-1]), names[i-1], blocks-1-i, byte(30+i))
			}
			for _, name := range names {
				chain.check(t, on(rng.Intn(2), name), name)
			}
			if ino, err := nodes[0].inodeTbl.Read(on(0, names[depth]).inodeNum); err != nil || !ino.HasExtentTree() {
				t.Fatalf("%s should use an extent tree after the copies on write (err=%v)", names[depth], err)
			}
			if t.Failed() {
				return
			}

			order := rng.Perm(len(names))
			half := len(order) / 2
			for k, idx := range order[:half] {
				chain.remove(t, nodes[rng.Intn(2)], names[idx])
				left := order[k+1:]
				survivor := names[left[rng.Intn(len(left))]]
				chain.write(t, on(rng.Intn(2), survivor), survivor, rng.Intn(blocks), byte(50+k))
				for _, idx := range left {
					chain.check(t, on(rng.Intn(2), names[idx]), names[idx])
				}
			}

			var wg sync.WaitGroup
			for n, fsys := range nodes {
				mine := map[string]*File{}
				for k, idx := range order[half:] {
					if k%2 == n {
						mine[names[idx]] = on(n, names[idx])
					}
				}
				wg.Go(func() {
					k := 0
					for name, f := range mine {
						chain.write(t, f, name, (k*3+n)%blocks, byte(80+10*n+k))
						chain.check(t, f, name)
						chain.remove(t, fsys, name)
						k++
					}
				})
			}
			wg.Wait()

			after := openFormattedFS(t, path)
			if got := after.bitmapMgr.FreeCount(); got != free {
				t.Errorf("%d free blocks after deleting every file, want %d", got, free)
			}
			stats, err := after.refcountReg.Scan(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if stats.ExclusiveBlocks != 0 || stats.SharedBlocks != 0 {
				t.Errorf("refcounts left after deleting every file: %d blocks at 1, %d shared", stats.ExclusiveBlocks, stats.SharedBlocks)
			}
			r := runFsck(t, after, fsck.DefaultOptions())
			if len(r.Problems) > 0 || r.ExitCode() != fsck.ExitClean {
				t.Errorf("fsck exit %d:%s", r.ExitCode(), problemList(r))
			}
		})
	}
}
