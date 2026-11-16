//go:build linux

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// stallScript prints what goclusterfs-mount is waiting on: each thread's kernel stack, the lock and reserve wait
// totals, the FUSE requests queued, and, if the binary handles SIGUSR1, every goroutine's stack.
const stallScript = `pid=$(pgrep -f 'goclusterfs-moun[t] -mount' | head -n 1)
echo "pid ${pid:-none}"
[ -n "$pid" ] || exit 0
echo "== kernel stacks"
for t in /proc/$pid/task/*; do
  echo "-- thread ${t##*/} $(cat $t/comm 2>/dev/null) wchan=$(cat $t/wchan 2>/dev/null)"
  cat $t/stack 2>/dev/null
done
echo "== fuse requests waiting"
cat /sys/fs/fuse/connections/*/waiting 2>/dev/null
if grep -q 'goroutine dump begins' /usr/local/bin/goclusterfs-mount; then
  before=$(wc -c < /var/log/goclusterfs.log)
  kill -USR1 $pid
  sleep 1
  echo "== goroutines"
  tail -c +$((before + 1)) /var/log/goclusterfs.log
fi
echo "== metrics"
wget -T 3 -qO- http://127.0.0.1:8080/metrics 2>&1 | grep -E '^goclusterfs_(lock|reserve)_|wget'
`

const minCaptureGap = time.Minute

var captureMu sync.Mutex
var lastCapture time.Time

// captureStall saves every node's stacks when a call on n waits past -stall-capture, at most once a minute and not
// while a fault, which stalls calls on purpose, is running.
func (d *driver) captureStall(n *node, req request) {
	if d.ctx.Err() != nil || !d.faultMu.TryLock() {
		return
	}
	d.faultMu.Unlock()
	if !captureMu.TryLock() {
		return
	}
	defer captureMu.Unlock()
	if time.Since(lastCapture) < minCaptureGap {
		return
	}
	lastCapture = time.Now()
	seq := d.stalls.Add(1)
	dir := filepath.Join(d.cfg.out, "stalls")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf("stall capture: %v", err)
		return
	}
	detail := fmt.Sprintf("node %d %s %s waited over %s; stacks in stalls/%d-node*.txt", n.id, req.Op, req.Path, d.cfg.stallAfter, seq)
	log.Printf("stall: %s", detail)
	d.logRecord(record{Type: "stall", Detail: detail, Op: Op{At: time.Now().UnixMilli(), Node: n.id}})
	var wg sync.WaitGroup
	for _, m := range d.nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out := d.runOnNode(m.port, stallScript)
			_ = os.WriteFile(filepath.Join(dir, fmt.Sprintf("%d-node%d.txt", seq, m.id)), out, 0o644)
		}()
	}
	wg.Wait()
}

func (d *driver) runOnNode(port int, script string) []byte {
	ctx, cancel := context.WithTimeout(d.ctx, 30*time.Second)
	defer cancel()
	args := append(sshArgs(d.key), "-p", fmt.Sprint(port), "root@127.0.0.1", "sh", "-s")
	cmd := exec.CommandContext(ctx, "ssh", args...)
	cmd.Stdin = strings.NewReader(script)
	out, err := cmd.CombinedOutput()
	if err != nil {
		out = append(out, fmt.Sprintf("\nerror: %v\n", err)...)
	}
	return out
}
