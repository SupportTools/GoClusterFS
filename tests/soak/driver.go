//go:build linux

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const mountPoint = "/mnt/goclusterfs"

type config struct {
	checkout     string
	out          string
	duration     time.Duration
	lanes        int
	seed         uint64
	faultEvery   time.Duration
	fsckEvery    time.Duration
	barrierEvery time.Duration
	window       time.Duration
	coherence    time.Duration
	stopFor      time.Duration
	opTimeout    time.Duration
	sshBasePort  int
	diskMB       int
	budget       int64
}

type node struct {
	id    int
	port  int
	mu    sync.RWMutex // an operation holds it shared; a fault holds it exclusively
	up    atomic.Bool
	agent *agentConn
}

// record is one line of the operation log.
type record struct {
	Type string `json:"type"`
	Op
	Fault  string `json:"fault,omitempty"`
	Victim int    `json:"victim,omitempty"`
	Detail string `json:"detail,omitempty"`
}

type driver struct {
	cfg     config
	ctl     *controller
	key     string
	nodes   []*node
	model   *Model
	pause   sync.RWMutex // lanes hold it shared per operation; barriers and fsck hold it exclusively
	faultMu sync.Mutex
	seq     atomic.Uint64
	logMu   sync.Mutex
	logw    io.Writer
	stderr  io.Writer
	ctx     context.Context
	cancel  context.CancelFunc
	failMu  sync.Mutex
	// stopping is set when the run's time is up, so the operations it cuts short are not failures.
	stopping atomic.Bool
	failure  string

	ops, unexpected, faults, fscks, barriers, slow atomic.Int64
	slowestMs                                      atomic.Int64
	opsByKind                                      sync.Map
}

func (d *driver) logRecord(r record) {
	d.logMu.Lock()
	defer d.logMu.Unlock()
	b, err := json.Marshal(r)
	if err == nil {
		_, err = d.logw.Write(append(b, '\n'))
	}
	if err != nil {
		log.Printf("op log: %v", err)
	}
}

// fail stops the soak at the first failure and keeps its description.
func (d *driver) fail(format string, args ...any) {
	d.failMu.Lock()
	defer d.failMu.Unlock()
	if d.failure != "" || d.stopping.Load() {
		return
	}
	d.failure = fmt.Sprintf(format, args...)
	log.Printf("FAILURE: %s", d.failure)
	d.logRecord(record{Type: "failure", Detail: d.failure})
	d.cancel()
}

func (d *driver) bringUp() error {
	reply, _, err := d.ctl.run("up")
	if err != nil {
		return err
	}
	fields := strings.Fields(reply)
	if len(fields) < 2 {
		return fmt.Errorf("controller answered up with %q", reply)
	}
	d.key = fields[0]
	for i, p := range fields[1:] {
		n := &node{id: i + 1}
		if _, err := fmt.Sscan(p, &n.port); err != nil {
			return err
		}
		d.nodes = append(d.nodes, n)
	}
	for _, n := range d.nodes {
		if err := d.connect(n); err != nil {
			return err
		}
	}
	for _, dir := range append([]string{"/soak"}, sharedDirs...) {
		if resp, err := d.nodes[0].agent.call(d.ctx, request{Op: "mkdir", Path: dir}); err != nil || resp.Errno != 0 {
			return fmt.Errorf("mkdir %s: %v %s", dir, err, resp.Err)
		}
	}
	return nil
}

func (d *driver) connect(n *node) error {
	if err := pushAgent(d.key, n.port); err != nil {
		return err
	}
	a, err := dialAgent(d.key, n.port, mountPoint, d.stderr)
	if err != nil {
		return err
	}
	n.agent = a
	n.up.Store(true)
	return nil
}

func (d *driver) upNodes() []*node {
	var up []*node
	for _, n := range d.nodes {
		if n.up.Load() {
			up = append(up, n)
		}
	}
	return up
}

// converge retries check until it passes or the window ends. The caller holds the node. A check that passes only
// after the documented coherence window is logged for review: during a fault, reads can wait out the failure detection.
func (d *driver) converge(what string, check func() error) error {
	start := time.Now()
	stale, longestCall := 0, time.Duration(0)
	for {
		callStart := time.Now()
		err := check()
		longestCall = max(longestCall, time.Since(callStart))
		if err == nil {
			took := time.Since(start)
			if ms := took.Milliseconds(); ms > d.slowestMs.Load() {
				d.slowestMs.Store(ms)
			}
			if took > d.cfg.coherence {
				d.slow.Add(1)
				d.logRecord(record{Type: "slow", Detail: fmt.Sprintf("%s took %s: %d stale answers, longest call %s",
					what, took.Round(time.Millisecond), stale, longestCall.Round(time.Millisecond))})
			}
			return nil
		}
		stale++
		if errors.Is(err, errAgentGone) || time.Since(start) > d.cfg.window || d.ctx.Err() != nil {
			return fmt.Errorf("%v (still after %s)", err, time.Since(start).Round(time.Millisecond))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (d *driver) call(n *node, req request) (response, error) {
	ctx, cancel := context.WithTimeout(d.ctx, d.cfg.opTimeout)
	defer cancel()
	return n.agent.call(ctx, req)
}

// comparePath checks what node n shows at p against the model.
func (d *driver) comparePath(n *node, p string) error {
	d.model.mu.Lock()
	f, isDir := d.model.Files[p], d.model.Dirs[p]
	var size int64
	var want string
	if f != nil {
		size, want = int64(len(f.Data)), sum(f.Data)
	}
	d.model.mu.Unlock()
	switch {
	case f != nil:
		resp, err := d.call(n, request{Op: "read", Path: p})
		if err != nil {
			return err
		}
		if resp.Errno != 0 || resp.Size != size || resp.Sum != want {
			return fmt.Errorf("%s: read errno %d size %d sum %s, want size %d sum %s", p, resp.Errno, resp.Size, resp.Sum, size, want)
		}
	case isDir:
		resp, err := d.call(n, request{Op: "list", Path: p})
		if err != nil {
			return err
		}
		if resp.Errno != 0 {
			return fmt.Errorf("%s: list errno %d (%s), want a directory", p, resp.Errno, resp.Err)
		}
	default:
		resp, err := d.call(n, request{Op: "read", Path: p})
		if err != nil {
			return err
		}
		if resp.Errno != int(syscall.ENOENT) {
			return fmt.Errorf("%s: read errno %d size %d, want ENOENT", p, resp.Errno, resp.Size)
		}
	}
	return nil
}

func (d *driver) compareSnaps(n *node, p string) error {
	d.model.mu.Lock()
	want, exists := d.model.snapsOf(p), d.model.Files[p] != nil
	d.model.mu.Unlock()
	if !exists {
		return nil
	}
	resp, err := d.call(n, request{Op: "getxattr", Path: p, Attr: "user.snapshots"})
	if err != nil {
		return err
	}
	var got []string
	for _, line := range strings.FieldsFunc(resp.Text, func(r rune) bool { return r == '\n' || r == ',' }) {
		if name, state, ok := strings.Cut(line, ":"); ok && state == "active" {
			got = append(got, name)
		}
	}
	sort.Strings(got)
	if resp.Errno != 0 || !slices.Equal(got, want) {
		return fmt.Errorf("%s: snapshots %v (errno %d), want %v", p, got, resp.Errno, want)
	}
	return nil
}

// compareDir checks node n lists dir as the model does. A lane may have removed dir since the caller listed it.
func (d *driver) compareDir(n *node, dir string) error {
	d.model.mu.Lock()
	exists, want := d.model.Dirs[dir], d.model.listing(dir)
	d.model.mu.Unlock()
	resp, err := d.call(n, request{Op: "list", Path: dir})
	if err != nil {
		return err
	}
	if !exists {
		if resp.Errno != int(syscall.ENOENT) {
			return fmt.Errorf("%s: list errno %d, want ENOENT", dir, resp.Errno)
		}
		return nil
	}
	got := slices.Clone(resp.Names)
	sort.Strings(got)
	if resp.Errno != 0 || !slices.Equal(got, want) {
		return fmt.Errorf("%s: lists %v (errno %d), want %v", dir, got, resp.Errno, want)
	}
	return nil
}

// verifyAll checks every directory listing, and with content every file and its snapshots, on node n.
func (d *driver) verifyAll(n *node, content bool) error {
	d.model.mu.Lock()
	var dirs, files []string
	for p := range d.model.Dirs {
		dirs = append(dirs, p)
	}
	for p := range d.model.Files {
		files = append(files, p)
	}
	d.model.mu.Unlock()
	sort.Strings(dirs)
	sort.Strings(files)
	for _, dir := range dirs {
		if err := d.converge(fmt.Sprintf("node %d dir %s", n.id, dir), func() error { return d.compareDir(n, dir) }); err != nil {
			return err
		}
	}
	if !content {
		return nil
	}
	for _, p := range files {
		if err := d.converge(fmt.Sprintf("node %d %s", n.id, p), func() error { return d.comparePath(n, p) }); err != nil {
			return err
		}
		if err := d.converge(fmt.Sprintf("node %d snapshots of %s", n.id, p), func() error { return d.compareSnaps(n, p) }); err != nil {
			return err
		}
	}
	return nil
}

func (d *driver) runLane(l *lane) {
	for d.ctx.Err() == nil {
		d.pause.RLock()
		d.model.mu.Lock()
		op := l.generate(d.model)
		op.Seq = d.seq.Add(1)
		op.Want = d.model.expect(op)
		d.model.mu.Unlock()
		for d.ctx.Err() == nil {
			up := d.upNodes()
			if len(up) == 0 {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			n := pick(l.rng, up)
			n.mu.RLock()
			if !n.up.Load() {
				n.mu.RUnlock()
				continue
			}
			d.runOpLocked(op, n)
			break
		}
		d.pause.RUnlock()
	}
}

// runOpLocked runs op on n, which the caller holds shared, and releases n before checking the other nodes.
func (d *driver) runOpLocked(op *Op, n *node) {
	op.Node = n.id
	op.At = time.Now().UnixMilli()
	ctx, cancel := context.WithTimeout(d.ctx, d.cfg.opTimeout)
	resp, err := n.agent.call(ctx, op.request())
	cancel()
	n.mu.RUnlock()
	d.finishOp(op, resp, err)
}

func (d *driver) finishOp(op *Op, resp response, err error) {
	op.Ms = time.Now().UnixMilli() - op.At
	if err != nil {
		if d.ctx.Err() == nil {
			d.fail("op %d %s %s on node %d: %v", op.Seq, op.Kind, op.Path, op.Node, err)
		}
		return
	}
	op.Got, op.Err = resp.Errno, resp.Err
	d.check(op)
}

// matches reports whether an operation that returned got did what the model expected.
func matches(want, got int) bool {
	if want == anyErrno {
		return got != 0
	}
	return want == got
}

// check logs a finished operation, compares its errno with the model, applies it and checks every node shows it.
func (d *driver) check(op *Op) {
	d.logRecord(record{Type: "op", Op: *op})
	d.ops.Add(1)
	count, _ := d.opsByKind.LoadOrStore(op.Kind, new(atomic.Int64))
	count.(*atomic.Int64).Add(1)
	if !matches(op.Want, op.Got) {
		d.unexpected.Add(1)
		d.fail("op %d %s %s on node %d: errno %d (%s), want %d", op.Seq, op.Kind, op.Path, op.Node, op.Got, op.Err, op.Want)
		return
	}
	if op.Got == 0 {
		d.model.mu.Lock()
		d.model.apply(op)
		d.model.mu.Unlock()
	}
	paths := []string{op.Path}
	if op.Path2 != "" {
		paths = append(paths, op.Path2)
	}
	for _, m := range d.upNodes() {
		if err := d.onNode(m, func() error { return d.checkOp(m, op, paths) }); err != nil {
			d.fail("after op %d %s %s on node %d: node %d: %v", op.Seq, op.Kind, op.Path, op.Node, m.id, err)
			return
		}
	}
}

func (d *driver) checkOp(m *node, op *Op, paths []string) error {
	for _, p := range paths {
		if err := d.converge(fmt.Sprintf("node %d %s after op %d", m.id, p, op.Seq), func() error { return d.comparePath(m, p) }); err != nil {
			return err
		}
	}
	if slices.Contains([]string{"snapshot", "snapdelete", "snapclone"}, op.Kind) {
		return d.converge(fmt.Sprintf("node %d snapshots of %s after op %d", m.id, op.Path, op.Seq), func() error { return d.compareSnaps(m, op.Path) })
	}
	return nil
}

// onNode runs f while holding node n shared, unless a fault has taken n down.
func (d *driver) onNode(n *node, f func() error) error {
	if !n.up.Load() {
		return nil
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	if !n.up.Load() {
		return nil
	}
	return f()
}

// injectFault takes one node down, brings it back and checks it shows the model. Operations go on elsewhere.
func (d *driver) injectFault(kind string, n *node) {
	d.faultMu.Lock()
	defer d.faultMu.Unlock()
	start := time.Now()
	n.up.Store(false)
	n.mu.Lock()
	defer n.mu.Unlock()
	n.agent.close()
	command := fmt.Sprintf("%s %d", kind, n.id)
	if kind == "stop" {
		command = fmt.Sprintf("stop %d %d", n.id, int(d.cfg.stopFor.Seconds()))
	}
	log.Printf("fault: %s", command)
	if _, _, err := d.ctl.run(command); err != nil {
		d.fail("fault %s: %v", command, err)
		return
	}
	if err := d.connect(n); err != nil {
		d.fail("fault %s: reconnect: %v", command, err)
		return
	}
	n.up.Store(false)
	d.faults.Add(1)
	d.logRecord(record{Type: "fault", Fault: kind, Victim: n.id, Op: Op{At: start.UnixMilli(), Ms: time.Since(start).Milliseconds()}})
	if err := d.verifyAll(n, true); err != nil {
		d.fail("after fault %s: node %d: %v", command, n.id, err)
		return
	}
	n.up.Store(true)
}

// quiescedFsck stops all operations, unmounts every node, runs fsck, and mounts again.
func (d *driver) quiescedFsck() {
	d.pause.Lock()
	defer d.pause.Unlock()
	d.faultMu.Lock()
	defer d.faultMu.Unlock()
	start := time.Now()
	for _, n := range d.nodes {
		n.up.Store(false)
		n.agent.close()
	}
	if _, _, err := d.ctl.run("unmount-all"); err != nil {
		d.fail("fsck: unmount: %v", err)
		return
	}
	rc, lines, err := d.ctl.run("fsck")
	if err != nil {
		d.fail("fsck: %v", err)
		return
	}
	var leaks, other []string
	for _, line := range lines {
		switch {
		case strings.Contains(line, "is an orphan"):
		case isLeak(line):
			leaks = append(leaks, line)
		default:
			other = append(other, line)
		}
	}
	d.fscks.Add(1)
	d.logRecord(record{Type: "fsck", Detail: fmt.Sprintf("%s, %d leaks, %d other", rc, len(leaks), len(other)), Op: Op{At: start.UnixMilli()}})
	if len(other) > 0 {
		d.fail("fsck found more than leaks:\n%s", strings.Join(other, "\n"))
		return
	}
	if len(leaks) > 0 {
		log.Printf("fsck: repairing %d leaks", len(leaks))
		if _, _, err := d.ctl.run("fsck -repair"); err != nil {
			d.fail("fsck -repair: %v", err)
			return
		}
		if _, lines, err = d.ctl.run("fsck"); err != nil || slices.ContainsFunc(lines, func(l string) bool { return !strings.Contains(l, "is an orphan") }) {
			d.fail("fsck after repair: %v %v", err, lines)
			return
		}
	}
	if _, _, err := d.ctl.run("mount-all"); err != nil {
		d.fail("fsck: mount: %v", err)
		return
	}
	for _, n := range d.nodes {
		if err := d.connect(n); err != nil {
			d.fail("fsck: reconnect node %d: %v", n.id, err)
			return
		}
	}
	for _, n := range d.nodes {
		if err := d.verifyAll(n, true); err != nil {
			d.fail("after fsck: node %d: %v", n.id, err)
			return
		}
	}
	log.Printf("fsck: %d leaks repaired, every node shows the model (%s)", len(leaks), time.Since(start).Round(time.Second))
}

func isLeak(line string) bool {
	for _, s := range []string{"leaked", "will never be freed", "unnamed: no directory names it", "belongs to no snapshot"} {
		if strings.Contains(line, s) {
			return true
		}
	}
	return false
}

// barrier pauses every lane and checks every node lists every directory as the model does.
func (d *driver) barrier() {
	d.pause.Lock()
	defer d.pause.Unlock()
	for _, n := range d.upNodes() {
		if err := d.onNode(n, func() error { return d.verifyAll(n, false) }); err != nil {
			d.fail("barrier: node %d: %v", n.id, err)
			return
		}
	}
	d.barriers.Add(1)
}

func (d *driver) report() string {
	d.model.mu.Lock()
	files, snaps, blocks := len(d.model.Files), len(d.model.Snaps), d.model.blocks()
	d.model.mu.Unlock()
	var kinds []string
	d.opsByKind.Range(func(k, v any) bool {
		kinds = append(kinds, fmt.Sprintf("%s=%d", k, v.(*atomic.Int64).Load()))
		return true
	})
	sort.Strings(kinds)
	return fmt.Sprintf("ops=%d unexpected=%d faults=%d fscks=%d barriers=%d slow_checks=%d slowest_converge_ms=%d files=%d snapshots=%d blocks=%d [%s]",
		d.ops.Load(), d.unexpected.Load(), d.faults.Load(), d.fscks.Load(), d.barriers.Load(), d.slow.Load(), d.slowestMs.Load(),
		files, snaps, blocks, strings.Join(kinds, " "))
}

func newDriver(cfg config) (*driver, func(), error) {
	if err := os.MkdirAll(cfg.out, 0o755); err != nil {
		return nil, nil, err
	}
	logf, err := os.Create(filepath.Join(cfg.out, "ops.jsonl"))
	if err != nil {
		return nil, nil, err
	}
	clusterLog, err := os.Create(filepath.Join(cfg.out, "cluster.log"))
	if err != nil {
		return nil, nil, err
	}
	env := []string{"NODE_COUNT=3", fmt.Sprintf("VM_SSH_BASE_PORT=%d", cfg.sshBasePort), fmt.Sprintf("VM_CLUSTER_SIZE_MB=%d", cfg.diskMB)}
	ctl, err := startController(filepath.Join(cfg.checkout, "tests/soak/cluster.sh"), env, clusterLog)
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	d := &driver{cfg: cfg, ctl: ctl, model: newModel(), logw: logf, stderr: clusterLog, ctx: ctx, cancel: cancel}
	cleanup := func() {
		cancel()
		for _, n := range d.nodes {
			if n.agent != nil {
				n.agent.close()
			}
		}
		ctl.stop()
		_ = logf.Close()
		_ = clusterLog.Close()
	}
	return d, cleanup, nil
}

func every(ctx context.Context, interval time.Duration, f func()) {
	if interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			f()
		}
	}
}

func run(cfg config) error {
	d, cleanup, err := newDriver(cfg)
	if err != nil {
		return err
	}
	defer cleanup()
	log.Printf("soak: seed %d, %d lanes, %s, faults every %s, fsck every %s, output in %s",
		cfg.seed, cfg.lanes, cfg.duration, cfg.faultEvery, cfg.fsckEvery, cfg.out)
	if err := d.bringUp(); err != nil {
		return fmt.Errorf("bring up the cluster: %w", err)
	}
	ctx, stop := context.WithTimeout(d.ctx, cfg.duration)
	defer stop()
	faultRng := rand.New(rand.NewPCG(cfg.seed, 0xfa))
	var wg sync.WaitGroup
	for i := range cfg.lanes {
		l := &lane{id: i, rng: rand.New(rand.NewPCG(cfg.seed, uint64(i))), budget: cfg.budget}
		wg.Go(func() { d.runLane(l) })
	}
	wg.Go(func() {
		every(ctx, cfg.faultEvery, func() {
			d.injectFault(pick(faultRng, []string{"power", "sigkill", "stop"}), pick(faultRng, d.nodes))
		})
	})
	wg.Go(func() { every(ctx, cfg.fsckEvery, d.quiescedFsck) })
	wg.Go(func() { every(ctx, cfg.barrierEvery, d.barrier) })
	wg.Go(func() { every(ctx, time.Minute, func() { log.Print(d.report()) }) })
	<-ctx.Done()
	d.stopping.Store(true)
	d.cancel()
	wg.Wait()
	log.Print(d.report())
	return d.finish()
}

// finish writes the outcome and, after a failure, keeps the shared disk for analysis.
func (d *driver) finish() error {
	if d.failure == "" {
		return os.WriteFile(filepath.Join(d.cfg.out, "result.txt"), []byte("PASS "+d.report()+"\n"), 0o644)
	}
	text := "FAIL " + d.failure + "\n" + d.report() + "\n"
	if err := os.WriteFile(filepath.Join(d.cfg.out, "result.txt"), []byte(text), 0o644); err != nil {
		return err
	}
	if _, _, err := d.ctl.run("save-image " + filepath.Join(d.cfg.out, "cluster.img")); err != nil {
		log.Printf("save the shared disk: %v", err)
	}
	return errors.New(d.failure)
}
