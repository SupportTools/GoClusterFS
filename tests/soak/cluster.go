//go:build linux

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// controller drives tests/soak/cluster.sh, which owns the VMs.
type controller struct {
	mu  sync.Mutex
	cmd *exec.Cmd
	in  io.WriteCloser
	out *bufio.Scanner
}

func startController(script string, env []string, logw io.Writer) (*controller, error) {
	cmd := exec.Command("bash", script)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stderr = logw
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	sc := bufio.NewScanner(out)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	return &controller{cmd: cmd, in: in, out: sc}, nil
}

// run sends one command and returns the text after "ok" and any "out" lines before it.
func (c *controller) run(command string) (string, []string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := fmt.Fprintln(c.in, command); err != nil {
		return "", nil, err
	}
	var lines []string
	for c.out.Scan() {
		line := c.out.Text()
		switch {
		case strings.HasPrefix(line, "out "):
			lines = append(lines, strings.TrimPrefix(line, "out "))
		case line == "ok" || strings.HasPrefix(line, "ok "):
			return strings.TrimSpace(strings.TrimPrefix(line, "ok")), lines, nil
		case strings.HasPrefix(line, "err"):
			return "", lines, fmt.Errorf("controller: %s", line)
		}
	}
	return "", lines, fmt.Errorf("controller exited during %q: %v", command, c.out.Err())
}

func (c *controller) stop() {
	_, _, _ = c.run("down")
	_ = c.in.Close()
	_ = c.cmd.Wait()
}

func sshArgs(key string) []string {
	return []string{
		"-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null", "-o", "LogLevel=ERROR",
		"-o", "BatchMode=yes", "-o", "IdentitiesOnly=yes", "-o", "ServerAliveInterval=5", "-o", "ServerAliveCountMax=6",
		"-i", key,
	}
}

// pushAgent copies this binary to a VM, whose root file system does not survive a reboot.
func pushAgent(key string, port int) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	args := append(sshArgs(key), "-P", fmt.Sprint(port), exe, "root@127.0.0.1:/usr/local/bin/goclusterfs-soak")
	if out, err := exec.Command("scp", args...).CombinedOutput(); err != nil {
		return fmt.Errorf("scp to port %d: %v: %s", port, err, out)
	}
	return nil
}

// agentConn is an ssh session to the agent on one VM. Calls may overlap.
type agentConn struct {
	mu      sync.Mutex
	enc     *json.Encoder
	pending map[uint64]chan response
	nextID  atomic.Uint64
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	done    chan struct{}
	err     error
	// stalled runs once for a call still waiting after stallAfter.
	stallAfter time.Duration
	stalled    func(req request)
}

var errAgentGone = errors.New("agent connection closed")

func dialAgent(key string, port int, mount string, logw io.Writer) (*agentConn, error) {
	args := append(sshArgs(key), "-p", fmt.Sprint(port), "root@127.0.0.1",
		"/usr/local/bin/goclusterfs-soak", "agent", "-root", mount)
	cmd := exec.Command("ssh", args...)
	cmd.Stderr = logw
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	a := &agentConn{enc: json.NewEncoder(stdin), pending: map[uint64]chan response{}, cmd: cmd, stdin: stdin, done: make(chan struct{})}
	go a.read(stdout)
	if _, err := a.call(context.Background(), request{Op: "ping"}); err != nil {
		a.close()
		return nil, fmt.Errorf("agent on port %d: %w", port, err)
	}
	return a, nil
}

func (a *agentConn) read(r io.Reader) {
	dec := json.NewDecoder(r)
	for {
		var resp response
		if err := dec.Decode(&resp); err != nil {
			a.mu.Lock()
			a.err = fmt.Errorf("%w: %v", errAgentGone, err)
			for id, ch := range a.pending {
				close(ch)
				delete(a.pending, id)
			}
			a.mu.Unlock()
			close(a.done)
			return
		}
		a.mu.Lock()
		ch := a.pending[resp.ID]
		delete(a.pending, resp.ID)
		a.mu.Unlock()
		if ch != nil {
			ch <- resp
		}
	}
}

func (a *agentConn) call(ctx context.Context, req request) (response, error) {
	req.ID = a.nextID.Add(1)
	ch := make(chan response, 1)
	a.mu.Lock()
	if a.err != nil {
		a.mu.Unlock()
		return response{}, a.err
	}
	a.pending[req.ID] = ch
	err := a.enc.Encode(req)
	a.mu.Unlock()
	if err != nil {
		return response{}, fmt.Errorf("%w: %v", errAgentGone, err)
	}
	var stall <-chan time.Time
	if a.stalled != nil && a.stallAfter > 0 {
		timer := time.NewTimer(a.stallAfter)
		defer timer.Stop()
		stall = timer.C
	}
	for {
		select {
		case resp, ok := <-ch:
			if !ok {
				return response{}, a.err
			}
			return resp, nil
		case <-ctx.Done():
			return response{}, ctx.Err()
		case <-stall:
			stall = nil
			go a.stalled(req)
		}
	}
}

func (a *agentConn) close() {
	_ = a.stdin.Close()
	_ = a.cmd.Process.Kill()
	_ = a.cmd.Wait()
}
