//go:build linux

// Command goclusterfs-soak runs a long random workload on a 3-VM cluster, checks every node against a model of
// the expected state, and cuts power, kills or stops nodes on the way. See README.md.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: goclusterfs-soak run|replay|agent [flags]")
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "agent":
		fs := flag.NewFlagSet("agent", flag.ExitOnError)
		root := fs.String("root", mountPoint, "mount point to run operations under")
		_ = fs.Parse(os.Args[2:])
		err = runAgent(*root, os.Stdin, os.Stdout)
	case "run", "replay":
		var cfg config
		var oplog string
		var upto uint64
		fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
		fs.StringVar(&cfg.checkout, "checkout", ".", "repository checkout with built goclusterfs-mount and goclusterfs-fsck")
		fs.StringVar(&cfg.out, "out", filepath.Join("soak-out", time.Now().Format("20060102-150405")), "directory for the op log, cluster log and result")
		fs.DurationVar(&cfg.duration, "duration", 48*time.Hour, "how long to run")
		fs.IntVar(&cfg.lanes, "lanes", 3, "concurrent operation streams, each on its own files in the shared directories")
		fs.Uint64Var(&cfg.seed, "seed", uint64(time.Now().UnixNano()), "random seed")
		fs.DurationVar(&cfg.faultEvery, "fault-every", 10*time.Minute, "interval between faults; 0 for none")
		fs.DurationVar(&cfg.fsckEvery, "fsck-every", time.Hour, "interval between quiesced fsck runs; 0 for none")
		fs.DurationVar(&cfg.barrierEvery, "barrier-every", 5*time.Minute, "interval between full directory checks; 0 for none")
		fs.DurationVar(&cfg.window, "window", 30*time.Second, "how long a node may take to show an acknowledged change, including waiting out a failed node")
		fs.DurationVar(&cfg.coherence, "coherence", 6*time.Second, "the documented coherence window; slower checks are logged")
		fs.DurationVar(&cfg.stopFor, "stop-for", 25*time.Second, "how long a stop fault keeps the mount stopped; past the 10s heartbeat timeout")
		fs.DurationVar(&cfg.opTimeout, "op-timeout", 5*time.Minute, "an operation taking longer is a hang")
		fs.IntVar(&cfg.sshBasePort, "ssh-base-port", 3122, "host SSH port of VM 1; keep it apart from other VM runs")
		fs.IntVar(&cfg.diskMB, "disk-mb", 4096, "shared disk size")
		fs.Int64Var(&cfg.budget, "budget", 1200, "data blocks the model may hold, counting each snapshot as a full copy")
		fs.DurationVar(&cfg.stallAfter, "stall-capture", 20*time.Second, "when a call waits this long, save every node's stacks and lock wait totals; 0 turns it off")
		fs.StringVar(&oplog, "log", "", "replay: op log to run again")
		fs.Uint64Var(&upto, "upto", 0, "replay: stop after this op sequence number")
		_ = fs.Parse(os.Args[2:])
		if os.Args[1] == "run" {
			err = run(cfg)
		} else {
			err = replay(cfg, oplog, upto)
		}
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		log.Fatal(err)
	}
}

// replay runs a logged soak again in log order, with the same operations, nodes, data and faults.
func replay(cfg config, oplog string, upto uint64) error {
	if oplog == "" {
		return errors.New("replay needs -log")
	}
	f, err := os.Open(oplog)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	var records []record
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		var r record
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			return fmt.Errorf("%s: %w", oplog, err)
		}
		records = append(records, r)
	}
	if err := sc.Err(); err != nil {
		return err
	}

	d, cleanup, err := newDriver(cfg)
	if err != nil {
		return err
	}
	defer cleanup()
	if err := d.bringUp(); err != nil {
		return fmt.Errorf("bring up the cluster: %w", err)
	}
	for _, r := range records {
		if d.ctx.Err() != nil || upto > 0 && r.Type == "op" && r.Seq > upto {
			break
		}
		switch r.Type {
		case "op":
			op := r.Op
			logged := op.Want
			op.Got, op.Err = 0, ""
			d.model.mu.Lock()
			op.Want = d.model.expect(&op)
			d.model.mu.Unlock()
			if op.Want != logged {
				d.fail("op %d: the model expects errno %d where the run expected %d", op.Seq, op.Want, logged)
				break
			}
			n := d.nodes[op.Node-1]
			n.mu.RLock()
			d.runOpLocked(&op, n)
		case "fault":
			d.injectFault(r.Fault, d.nodes[r.Victim-1])
		case "fsck":
			d.quiescedFsck()
		}
	}
	log.Print(d.report())
	return d.finish()
}
