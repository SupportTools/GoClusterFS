package integration

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/supporttools/GoClusterFS/tests/integration/history"
)

const historyWriteSize = 3 * 4096

var historyNames = []string{"n0", "n1", "n2", "n3", "n4"}

// TestCoherenceHistory runs clients on several nodes against a few shared names,
// records every call with its start and end, and checks the history against the
// documented cross-node guarantees. It runs only with GCFS_HISTORY=1; -short
// runs 2 nodes for 20s. For a long run set GCFS_HISTORY_MINUTES,
// GCFS_HISTORY_NODES (2 to 4) and GCFS_HISTORY_OUT, a directory for each round's
// history. Set GCFS_IMAGE_DIR to keep the image off tmpfs, which tears writes inside a sector.
func TestCoherenceHistory(t *testing.T) {
	if os.Getenv("GCFS_HISTORY") != "1" {
		t.Skip("set GCFS_HISTORY=1 to run the coherence history checker")
	}
	nodes, clients, total := 3, 3, 2*time.Minute
	if testing.Short() {
		nodes, clients, total = 2, 2, 20*time.Second
	}
	if v, err := strconv.Atoi(os.Getenv("GCFS_HISTORY_MINUTES")); err == nil && v > 0 {
		total = time.Duration(v) * time.Minute
	}
	if v, err := strconv.Atoi(os.Getenv("GCFS_HISTORY_NODES")); err == nil && v >= 2 && v <= 4 {
		nodes = v
	}
	out := os.Getenv("GCFS_HISTORY_OUT")

	cluster := NewMultiNodeCluster(t, nodes, 1024)
	if err := cluster.FormatAndStartFirst(false); err != nil {
		t.Fatalf("format and start node 0: %v", err)
	}
	for i := 1; i < nodes; i++ {
		if err := cluster.StartNode(i, false); err != nil {
			t.Fatalf("start node %d: %v", i, err)
		}
	}
	rules := history.Rules{Window: cluster.Node(0).attrTimeout() + 500*time.Millisecond, WriteSize: historyWriteSize}

	start := time.Now()
	for round := 0; time.Since(start) < total; round++ {
		dir := fmt.Sprintf("h%d", round)
		if err := cluster.Node(0).Mkdir(dir); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
		time.Sleep(rules.Window)
		ops := runHistoryRound(cluster, dir, clients, min(10*time.Minute, total-time.Since(start)))
		violations := history.Check(ops, rules)
		t.Logf("round %d: %d calls on %d nodes, %s; %d violations", round, len(ops), nodes, summarize(ops), len(violations))
		if out != "" {
			saveHistory(t, filepath.Join(out, fmt.Sprintf("round-%03d.txt", round)), ops, violations)
		}
		for i, v := range violations {
			if i == 20 {
				t.Errorf("round %d: %d more violations", round, len(violations)-20)
				break
			}
			t.Errorf("round %d: %s", round, v)
		}
		if err := cluster.Node(0).RemoveAll(dir); err != nil {
			t.Logf("remove %s: %v", dir, err)
		}
	}
}

// runHistoryRound runs clients goroutines per node on the names in dir until d has passed.
func runHistoryRound(cluster *MultiNodeCluster, dir string, clients int, d time.Duration) []history.Op {
	start := time.Now()
	deadline := start.Add(d)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var ops []history.Op
	for n := range cluster.NodeCount() {
		node := cluster.Node(n)
		for c := range clients {
			id := n*clients + c
			wg.Add(1)
			go func() {
				defer wg.Done()
				rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
				var mine []history.Op
				for seq := uint64(1); time.Now().Before(deadline); seq++ {
					o := historyCall(node, dir, rng, uint64(id+1)<<32|seq, start)
					o.Client, o.Node = id, n
					mine = append(mine, o)
				}
				mu.Lock()
				ops = append(ops, mine...)
				mu.Unlock()
			}()
		}
	}
	wg.Wait()
	for i := range ops {
		ops[i].ID = i
	}
	return ops
}

// historyCall makes one random call and records it; value is unique to this call.
func historyCall(node *MountedFS, dir string, rng *rand.Rand, value uint64, start time.Time) history.Op {
	name := historyNames[rng.Intn(len(historyNames))]
	path := node.Path(dir + "/" + name)
	o := history.Op{Name: name}
	begin := time.Now()
	var err error
	switch p := rng.Intn(100); {
	case p < 25:
		o.Kind, o.Value = history.Write, value
		err = writeValue(path, value)
	case p < 50:
		o.Kind = history.Read
		o.Content, o.Value, o.Pieces, o.Size, err = readValue(path)
	case p < 65:
		o.Kind = history.Stat
		var fi os.FileInfo
		if fi, err = os.Stat(path); err == nil {
			o.Size = fi.Size()
		}
	case p < 73:
		o.Kind = history.Readdir
		var entries []os.DirEntry
		entries, err = os.ReadDir(node.Path(dir))
		for _, e := range entries {
			o.Names = append(o.Names, e.Name())
		}
	case p < 81:
		o.Kind = history.Create
		var f *os.File
		if f, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644); err == nil {
			err = f.Close()
		}
	case p < 95:
		o.Kind = history.Unlink
		err = os.Remove(path)
	default:
		o.Kind, o.To = history.Rename, historyNames[(slices.Index(historyNames, name)+1+rng.Intn(len(historyNames)-1))%len(historyNames)]
		err = os.Rename(path, node.Path(dir+"/"+o.To))
	}
	o.Start, o.End = begin.Sub(start), time.Since(start)
	o.Result = classify(err)
	if o.Result != history.OK {
		o.Err = err.Error()
	}
	return o
}

func writeValue(path string, value uint64) error {
	buf := make([]byte, historyWriteSize)
	for i := 0; i < len(buf); i += 8 {
		binary.LittleEndian.PutUint64(buf[i:], value)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	_, err = f.WriteAt(buf, 0)
	return errors.Join(err, f.Close())
}

// readValue reads path whole: Whole when it holds one nonzero value over the
// write size, else Torn with each run of one value.
func readValue(path string) (history.Content, uint64, []history.Piece, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, nil, 0, err
	}
	buf := make([]byte, historyWriteSize+1)
	n, err := f.ReadAt(buf, 0)
	if errors.Is(err, io.EOF) {
		err = nil
	}
	if err = errors.Join(err, f.Close()); err != nil {
		return 0, 0, nil, int64(n), err
	}
	if n == 0 {
		return history.Empty, 0, nil, 0, nil
	}
	var pieces []history.Piece
	for i := 0; i+8 <= n; i += 8 {
		if w := binary.LittleEndian.Uint64(buf[i:]); len(pieces) == 0 || pieces[len(pieces)-1].Value != w {
			pieces = append(pieces, history.Piece{At: int64(i), Value: w})
		}
	}
	if len(pieces) == 1 && n == historyWriteSize && pieces[0].Value != 0 {
		return history.Whole, pieces[0].Value, nil, int64(n), nil
	}
	return history.Torn, 0, pieces, int64(n), nil
}

func classify(err error) history.Result {
	switch {
	case err == nil:
		return history.OK
	case errors.Is(err, fs.ErrNotExist):
		return history.NotFound
	case errors.Is(err, fs.ErrExist):
		return history.Exists
	case errors.Is(err, syscall.ENOSYS), errors.Is(err, syscall.ENOTSUP), errors.Is(err, syscall.EXDEV):
		return history.Unsupported
	}
	var path *fs.PathError
	if errors.Is(err, syscall.ESTALE) && errors.As(err, &path) && path.Op == "open" {
		return history.Stale
	}
	return history.Failed
}

func summarize(ops []history.Op) string {
	counts := map[string]int{}
	for _, o := range ops {
		counts[o.Kind.String()+" "+o.Result.String()]++
	}
	var parts []string
	for _, k := range []history.Kind{history.Write, history.Read, history.Stat, history.Readdir, history.Create, history.Unlink, history.Rename} {
		for _, r := range []history.Result{history.OK, history.NotFound, history.Exists, history.Unsupported, history.Stale, history.Failed} {
			if n := counts[k.String()+" "+r.String()]; n > 0 {
				parts = append(parts, fmt.Sprintf("%s %s %d", k, r, n))
			}
		}
	}
	return strings.Join(parts, ", ")
}

func saveHistory(t *testing.T, path string, ops []history.Op, violations []history.Violation) {
	t.Helper()
	var b strings.Builder
	for _, v := range violations {
		b.WriteString(v.String())
	}
	b.WriteString("--- full history\n")
	for _, o := range ops {
		b.WriteString(o.String() + "\n")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Logf("save history: %v", err)
		return
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Logf("save history: %v", err)
	}
}
