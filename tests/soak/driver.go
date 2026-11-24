//go:build linux

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"os"
	"path"
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
	inflight     bool
	coherence    time.Duration
	stopFor      time.Duration
	opTimeout    time.Duration
	sshBasePort  int
	diskMB       int
	blockSize    uint64
	budget       int64
	stallAfter   time.Duration
	relogin      bool
}

type node struct {
	id   int
	port int
	mu   sync.RWMutex // an operation holds it shared; a fault holds it exclusively
	up   atomic.Bool
	// hit is set while an -inflight fault takes the node down; an operation that fails then was cut short.
	hit   atomic.Bool
	agent *agentConn
}

// record is one line of the operation log.
type record struct {
	Type string `json:"type"`
	Op
	Fault  string `json:"fault,omitempty"`
	Victim int    `json:"victim,omitempty"`
	Detail string `json:"detail,omitempty"`
	// DuringFault marks a slow check of a node that a fault hit while it ran.
	DuringFault bool `json:"during_fault,omitempty"`
}

type driver struct {
	cfg     config
	ctl     *controller
	key     string
	nodes   []*node
	model   *Model
	pause   sync.RWMutex // lanes hold it shared per operation; barriers and fsck hold it exclusively
	faultMu sync.Mutex
	// victims hands a fault, with -inflight, the node a lane is about to run an operation on.
	victims chan *node
	seq     atomic.Uint64
	logMu   sync.Mutex
	logw    io.Writer
	stderr  io.Writer
	ctx     context.Context
	cancel  context.CancelFunc
	failMu  sync.Mutex
	// pending holds operations a fault cut short on each node, until the fault reads what they left.
	pendingMu sync.Mutex
	pending   map[int][]*uncertain
	// running holds the writes and truncates running now, with the file each changes. model.mu guards it.
	running map[*Op]*File
	// stopping is set when the run's time is up, so the operations it cuts short are not failures.
	stopping atomic.Bool
	failure  string

	ops, unexpected, faults, fscks, barriers, slow, stalls, resolved atomic.Int64
	reloginSurvived, reloginStopped                                  atomic.Int64
	slowDuringFault, slowStale                                       atomic.Int64
	edquot, quotaDrift                                               atomic.Int64
	// faultsAtFsck is the fault count when the last fsck ran: usage may drift only across a fault.
	faultsAtFsck atomic.Int64
	// waitsOf reads a node's wait totals for slow-check records; nil leaves them out.
	waitsOf   func(*node) waits
	slowestMs atomic.Int64
	opsByKind sync.Map
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
	if err := d.setQuotas(d.nodes[0]); err != nil {
		return err
	}
	return d.checkStatfs(d.nodes[0])
}

func (d *driver) connect(n *node) error {
	if err := pushAgent(d.key, n.port); err != nil {
		return err
	}
	a, err := dialAgent(d.key, n.port, mountPoint, d.stderr)
	if err != nil {
		return err
	}
	a.stallAfter, a.stalled = d.cfg.stallAfter, func(req request) { d.captureStall(n, req) }
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
func (d *driver) converge(n *node, what string, check func() error) error {
	start := time.Now()
	stale, longestCall := 0, time.Duration(0)
	duringFault := n.hit.Load()
	before := make(chan waits, 1)
	if d.waitsOf != nil {
		timer := time.AfterFunc(d.cfg.coherence, func() { before <- d.waitsOf(n) })
		defer timer.Stop()
	}
	var last error
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
				d.logSlow(n, what, took, stale, longestCall, last, duringFault || n.hit.Load(), before)
			}
			return nil
		}
		stale++
		last = err
		if errors.Is(err, errAgentGone) || time.Since(start) > d.cfg.window || d.ctx.Err() != nil {
			return fmt.Errorf("%v (still after %s)", err, time.Since(start).Round(time.Millisecond))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// logSlow records a check that passed only after -coherence. Outside a fault on n it adds what n waited on from
// -coherence into the check until it passed; a check during a fault is marked, and its stale answers, which a node
// that is down gives, are not counted.
func (d *driver) logSlow(n *node, what string, took time.Duration, stale int, longestCall time.Duration, last error, duringFault bool, before <-chan waits) {
	d.slow.Add(1)
	detail := fmt.Sprintf("%s took %s: %d stale answers, longest call %s", what, took.Round(time.Millisecond), stale, longestCall.Round(time.Millisecond))
	if duringFault {
		d.slowDuringFault.Add(1)
		detail = fmt.Sprintf("during a fault on node %d: %s", n.id, detail)
	} else {
		if stale > 0 {
			d.slowStale.Add(1)
		}
		if d.waitsOf != nil {
			select {
			case b := <-before:
				detail += "; waited on " + waitsBetween(b, d.waitsOf(n))
			case <-time.After(10 * time.Second):
			}
		}
	}
	if last != nil {
		detail += fmt.Sprintf("; last stale answer: %v", last)
	}
	d.logRecord(record{Type: "slow", Detail: detail, DuringFault: duringFault})
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
	target, isLink := d.model.Links[p]
	readable := d.readable(p)
	d.model.mu.Unlock()
	switch {
	case f != nil:
		resp, err := d.call(n, request{Op: "read", Path: p})
		if err != nil {
			return err
		}
		d.model.mu.Lock()
		readable = append(readable, d.readable(p)...)
		d.model.mu.Unlock()
		if got := content(resp.Size, resp.Sum); resp.Errno != 0 || !slices.Contains(readable, got) {
			return fmt.Errorf("%s: read errno %d %s, want %s", p, resp.Errno, got, strings.Join(readable, " or "))
		}
	case isDir:
		resp, err := d.call(n, request{Op: "list", Path: p})
		if err != nil {
			return err
		}
		if resp.Errno != 0 {
			return fmt.Errorf("%s: list errno %d (%s), want a directory", p, resp.Errno, resp.Err)
		}
	case isLink:
		// Reading would follow the link, so look for it in its directory's listing.
		resp, err := d.call(n, request{Op: "list", Path: path.Dir(p)})
		if err != nil {
			return err
		}
		if want := "l:" + path.Base(p) + ":" + target; resp.Errno != 0 || !slices.Contains(resp.Names, want) {
			return fmt.Errorf("%s: parent lists %v (errno %d), want %s", p, resp.Names, resp.Errno, want)
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

// readable lists what a read of p may show: p's file, and that file after each write or truncate running on it under
// any name. The caller holds model.mu.
func (d *driver) readable(p string) []string {
	f := d.model.Files[p]
	if f == nil {
		return nil
	}
	out := []string{content(int64(len(f.Data)), sum(f.Data))}
	for op, g := range d.running {
		if g == f {
			data := bytes.Clone(f.Data)
			applyData(op, &data)
			out = append(out, content(int64(len(data)), sum(data)))
		}
	}
	return out
}

func content(size int64, sum string) string { return fmt.Sprintf("size %d sum %s", size, sum) }

// startWrite marks a write or truncate as running on its file until the returned func is called.
func (d *driver) startWrite(op *Op) (done func()) {
	d.model.mu.Lock()
	defer d.model.mu.Unlock()
	f := d.model.Files[op.Path]
	if f == nil || op.Kind != "write" && op.Kind != "truncate" {
		return func() {}
	}
	d.running[op] = f
	return func() {
		d.model.mu.Lock()
		delete(d.running, op)
		d.model.mu.Unlock()
	}
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
		if err := d.converge(n, fmt.Sprintf("node %d dir %s", n.id, dir), func() error { return d.compareDir(n, dir) }); err != nil {
			return err
		}
	}
	if !content {
		return nil
	}
	for _, p := range files {
		if err := d.converge(n, fmt.Sprintf("node %d %s", n.id, p), func() error { return d.comparePath(n, p) }); err != nil {
			return err
		}
		if err := d.converge(n, fmt.Sprintf("node %d snapshots of %s", n.id, p), func() error { return d.compareSnaps(n, p) }); err != nil {
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
	defer d.startWrite(op)()
	op.At = time.Now().UnixMilli()
	ctx, cancel := context.WithTimeout(d.ctx, d.cfg.opTimeout)
	if d.cfg.inflight {
		select {
		case d.victims <- n:
		default:
		}
	}
	resp, err := n.agent.call(ctx, op.request())
	cancel()
	// Registered before the node is released, so the fault that cut the operation short finds it.
	var u *uncertain
	if (err != nil || !matches(op.Want, resp.Errno)) && n.hit.Load() && d.ctx.Err() == nil {
		u = &uncertain{op: op, done: make(chan struct{})}
		d.pendingMu.Lock()
		d.pending[n.id] = append(d.pending[n.id], u)
		d.pendingMu.Unlock()
	}
	n.mu.RUnlock()
	if u != nil {
		op.Ms = time.Now().UnixMilli() - op.At
		op.Got, op.Err = resp.Errno, fmt.Sprint(err, resp.Err)
		d.logRecord(record{Type: "uncertain", Op: *op})
		d.awaitResolved(u)
		return
	}
	d.finishOp(op, resp, err)
}

// awaitResolved waits until the fault that cut u's operation short resolves it, or the run fails, as when that fault's
// own recovery fails and nothing will resolve it.
func (d *driver) awaitResolved(u *uncertain) {
	select {
	case <-u.done:
	case <-d.ctx.Done():
	}
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
	if op.Kind == "write" && op.Got == int(syscall.EDQUOT) {
		op.Wrote = resp.Size
	}
	d.check(op)
}

// matches reports whether an operation that returned got did what the model expected.
func matches(want, got int) bool {
	switch want {
	case anyErrno:
		return got != 0
	case okOrEDQUOT:
		return got == 0 || got == int(syscall.EDQUOT)
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
	if op.Got == 0 || op.Kind == "write" && op.Got == int(syscall.EDQUOT) {
		d.model.mu.Lock()
		d.model.apply(op)
		d.model.mu.Unlock()
	}
	if op.Got == int(syscall.EDQUOT) {
		d.edquot.Add(1)
		n := d.nodes[op.Node-1]
		if err := d.onNode(n, func() error { return d.checkRefusal(n, op) }); err != nil {
			d.fail("after op %d %s %s on node %d: %v", op.Seq, op.Kind, op.Path, op.Node, err)
			return
		}
	}
	var paths []string
	if op.Path != "" {
		paths = append(paths, op.Path)
	}
	if op.Path2 != "" && op.Kind != "symlink" {
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
		if err := d.converge(m, fmt.Sprintf("node %d %s after op %d", m.id, p, op.Seq), func() error { return d.comparePath(m, p) }); err != nil {
			return err
		}
	}
	if slices.Contains([]string{"snapshot", "snapdelete", "snapclone"}, op.Kind) {
		return d.converge(m, fmt.Sprintf("node %d snapshots of %s after op %d", m.id, op.Path, op.Seq), func() error { return d.compareSnaps(m, op.Path) })
	}
	return nil
}

// onNode runs f while holding node n shared, unless a fault has taken n down. With -inflight a fault can start while f
// runs; its errors are then dropped, and the fault checks n once it is back.
func (d *driver) onNode(n *node, f func() error) error {
	if !n.up.Load() {
		return nil
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	if !n.up.Load() {
		return nil
	}
	if err := f(); err != nil && !n.hit.Load() {
		return err
	}
	return nil
}

// victimMidOp returns the node of the next operation a lane starts within a minute, so the fault lands while it runs.
func (d *driver) victimMidOp(fallback *node) *node {
	select {
	case n := <-d.victims:
		return n
	case <-time.After(time.Minute):
	case <-d.ctx.Done():
	}
	return fallback
}

// injectFault takes one node down, brings it back and checks it shows the model. Operations go on elsewhere.
func (d *driver) injectFault(kind string, n *node) {
	d.faultMu.Lock()
	defer d.faultMu.Unlock()
	start := time.Now()
	command := fmt.Sprintf("%s %d", kind, n.id)
	if kind == "stop" {
		command = fmt.Sprintf("stop %d %d", n.id, int(d.cfg.stopFor.Seconds()))
	}
	if strings.HasPrefix(kind, "relogin") {
		d.relogin(kind, n, start)
		return
	}
	log.Printf("fault: %s", command)
	if d.cfg.inflight {
		// Operations keep starting on the node until the fault takes it down, so it cuts some of them short.
		n.hit.Store(true)
		if _, _, err := d.ctl.run("cut " + command); err != nil {
			d.fail("fault %s: %v", command, err)
			return
		}
		n.up.Store(false)
		n.mu.Lock()
		n.agent.close()
		if _, _, err := d.ctl.run(fmt.Sprintf("restore %s %d", kind, n.id)); err != nil {
			n.mu.Unlock()
			d.fail("fault %s: restore: %v", command, err)
			return
		}
	} else {
		n.up.Store(false)
		n.mu.Lock()
		n.agent.close()
		if _, _, err := d.ctl.run(command); err != nil {
			n.mu.Unlock()
			d.fail("fault %s: %v", command, err)
			return
		}
	}
	defer n.mu.Unlock()
	if err := d.connect(n); err != nil {
		d.fail("fault %s: reconnect: %v", command, err)
		return
	}
	n.up.Store(false)
	d.faults.Add(1)
	d.logRecord(record{Type: "fault", Fault: kind, Victim: n.id, Op: Op{At: start.UnixMilli(), Ms: time.Since(start).Milliseconds()}})
	if err := d.resolvePending(n); err != nil {
		d.fail("after fault %s: %v", command, err)
		return
	}
	if err := d.verifyAll(n, true); err != nil {
		d.fail("after fault %s: node %d: %v", command, n.id, err)
		return
	}
	n.hit.Store(false)
	n.up.Store(true)
}

// relogin logs n's iSCSI session out and in while lanes keep running operations on it; "relogin-paused" stops its
// mount for -stop-for around that, past the time peers take to fence it. The node either runs on, and then no
// operation on it may have failed, or it stops and the controller mounts it again.
func (d *driver) relogin(kind string, n *node, start time.Time) {
	command := fmt.Sprintf("relogin %d", n.id)
	if kind == "relogin-paused" {
		command = fmt.Sprintf("relogin %d %d", n.id, int(d.cfg.stopFor.Seconds()))
	}
	log.Printf("fault: %s", command)
	n.hit.Store(true)
	outcome, _, err := d.ctl.run(command)
	if err != nil {
		d.fail("fault %s: %v", command, err)
		return
	}
	n.up.Store(false)
	n.mu.Lock()
	defer n.mu.Unlock()
	switch outcome {
	case "survived":
		d.reloginSurvived.Add(1)
		d.pendingMu.Lock()
		cut := d.pending[n.id]
		delete(d.pending, n.id)
		d.pendingMu.Unlock()
		for _, u := range cut {
			close(u.done)
		}
		if len(cut) > 0 {
			op := cut[0].op
			d.fail("op %d %s %s on node %d failed during a relogin the node ran through; ops.jsonl has its answer", op.Seq, op.Kind, op.Path, n.id)
			return
		}
	case "stopped":
		d.reloginStopped.Add(1)
		n.agent.close()
		if err := d.connect(n); err != nil {
			d.fail("fault %s: reconnect: %v", command, err)
			return
		}
		n.up.Store(false)
	default:
		d.fail("fault %s: controller answered %q", command, outcome)
		return
	}
	d.faults.Add(1)
	d.logRecord(record{Type: "fault", Fault: kind, Victim: n.id, Detail: outcome, Op: Op{At: start.UnixMilli(), Ms: time.Since(start).Milliseconds()}})
	if err := d.resolvePending(n); err != nil {
		d.fail("after fault %s: %v", command, err)
		return
	}
	if err := d.verifyAll(n, true); err != nil {
		d.fail("after fault %s: node %d: %v", command, n.id, err)
		return
	}
	n.hit.Store(false)
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
	var leaks, drift, other []string
	for _, line := range lines {
		switch {
		case strings.Contains(line, "is an orphan"):
		case isLeak(line):
			leaks = append(leaks, line)
		case isQuotaDrift(line):
			drift = append(drift, line)
		default:
			other = append(other, line)
		}
	}
	d.fscks.Add(1)
	faults := d.faults.Load()
	quiet := faults == d.faultsAtFsck.Swap(faults)
	d.logRecord(record{Type: "fsck", Detail: fmt.Sprintf("%s, %d leaks, %d quota drift, %d other", rc, len(leaks), len(drift), len(other)), Op: Op{At: start.UnixMilli()}})
	if len(other) > 0 {
		d.fail("fsck found more than leaks:\n%s", strings.Join(other, "\n"))
		return
	}
	if len(drift) > 0 && quiet {
		d.fail("fsck found quota usage that drifted with no fault since the last fsck:\n%s", strings.Join(drift, "\n"))
		return
	}
	if len(drift) > 0 {
		d.quotaDrift.Add(int64(len(drift)))
		log.Printf("fsck: quota usage drifted across a fault:\n%s", strings.Join(drift, "\n"))
	}
	if len(leaks) > 0 || len(drift) > 0 {
		log.Printf("fsck: repairing %d leaks and %d projects' usage", len(leaks), len(drift))
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
		if err := d.checkQuotas(n); err != nil {
			d.fail("after fsck: node %d: %v", n.id, err)
			return
		}
		if err := d.checkStatfs(n); err != nil {
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
		if err := d.onNode(n, func() error { return d.checkQuotas(n) }); err != nil {
			d.fail("barrier: node %d: %v", n.id, err)
			return
		}
	}
	// Skipped while a fault runs: a stopped node would hold the check up, and a recovering one is behind.
	if d.faultMu.TryLock() {
		err := d.checkAgreement(d.upNodes())
		d.faultMu.Unlock()
		if err != nil {
			d.fail("barrier: %v", err)
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
	return fmt.Sprintf("ops=%d unexpected=%d faults=%d cut_short=%d fscks=%d barriers=%d slow_checks=%d slow_during_faults=%d slow_with_stale=%d slowest_converge_ms=%d stalls=%d relogin_survived=%d relogin_stopped=%d edquot=%d quota_drift=%d files=%d snapshots=%d blocks=%d [%s]",
		d.ops.Load(), d.unexpected.Load(), d.faults.Load(), d.resolved.Load(), d.fscks.Load(), d.barriers.Load(), d.slow.Load(), d.slowDuringFault.Load(), d.slowStale.Load(), d.slowestMs.Load(), d.stalls.Load(),
		d.reloginSurvived.Load(), d.reloginStopped.Load(), d.edquot.Load(), d.quotaDrift.Load(), files, snaps, blocks, strings.Join(kinds, " "))
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
	env := []string{"NODE_COUNT=3", fmt.Sprintf("VM_SSH_BASE_PORT=%d", cfg.sshBasePort), fmt.Sprintf("VM_CLUSTER_SIZE_MB=%d", cfg.diskMB),
		fmt.Sprintf("MULTIPATH=%t", cfg.relogin), fmt.Sprintf("VM_BLOCK_SIZE=%d", cfg.blockSize)}
	ctl, err := startController(filepath.Join(cfg.checkout, "tests/soak/cluster.sh"), env, clusterLog)
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	d := &driver{cfg: cfg, ctl: ctl, model: newModel(), logw: logf, stderr: clusterLog, ctx: ctx, cancel: cancel, pending: map[int][]*uncertain{}, running: map[*Op]*File{}, victims: make(chan *node)}
	d.waitsOf = d.nodeWaits
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
	if cfg.relogin && (os.Getenv("VM_STORAGE") != "iscsi" || !cfg.inflight) {
		return errors.New("-relogin needs VM_STORAGE=iscsi and -inflight")
	}
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
	faultKinds := []string{"power", "sigkill", "stop"}
	if cfg.relogin {
		faultKinds = append(faultKinds, "relogin", "relogin-paused")
	}
	var wg sync.WaitGroup
	for i := range cfg.lanes {
		l := &lane{id: i, rng: rand.New(rand.NewPCG(cfg.seed, uint64(i))), budget: cfg.budget}
		wg.Go(func() { d.runLane(l) })
	}
	wg.Go(func() {
		every(ctx, cfg.faultEvery, func() {
			kind, n := pick(faultRng, faultKinds), pick(faultRng, d.nodes)
			if cfg.inflight {
				n = d.victimMidOp(n)
			}
			d.injectFault(kind, n)
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
