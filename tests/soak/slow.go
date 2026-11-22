//go:build linux

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// waitsScript prints a node's lock and reserve wait totals and, summed over its SCSI and virtio disks, the reads and
// writes the kernel finished and the milliseconds they took.
const waitsScript = `wget -T 3 -qO- --header 'Authorization: Bearer soak-api-token' http://127.0.0.1:8080/metrics 2>/dev/null | grep -E '^goclusterfs_(lock_wait_seconds_total|reserve_peer_wait_seconds_total)'
awk '$3 ~ /^(sd|vd)[a-z]+$/ { r += $4; rms += $7; w += $8; wms += $11 } END { print "device", r + 0, rms + 0, w + 0, wms + 0 }' /proc/diskstats
`

// waits holds one node's wait totals at one moment.
type waits struct {
	seconds                    map[string]float64
	reads, readMs, writes, wMs float64
	device                     bool
}

func parseWaits(out []byte) waits {
	w := waits{seconds: map[string]float64{}}
	sc := bufio.NewScanner(bytes.NewReader(out))
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		switch {
		case len(f) == 5 && f[0] == "device":
			v := make([]float64, 4)
			for i := range v {
				v[i], _ = strconv.ParseFloat(f[i+1], 64)
			}
			w.reads, w.readMs, w.writes, w.wMs, w.device = v[0], v[1], v[2], v[3], true
		case len(f) == 2:
			n, err := strconv.ParseFloat(f[1], 64)
			if err != nil {
				continue
			}
			name := strings.TrimPrefix(f[0], "goclusterfs_")
			if i := strings.Index(name, `type="`); i >= 0 {
				name = "lock " + strings.TrimSuffix(name[i+len(`type="`):], `"}`)
			} else {
				name = "reserve"
			}
			w.seconds[name] += n
		}
	}
	return w
}

// waitsBetween describes what a node waited on between two snapshots, largest first.
func waitsBetween(a, b waits) string {
	var parts []string
	type wait struct {
		name string
		s    float64
	}
	var ws []wait
	for name, s := range b.seconds {
		if d := s - a.seconds[name]; d >= 0.05 {
			ws = append(ws, wait{name, d})
		}
	}
	sort.Slice(ws, func(i, j int) bool { return ws[i].s > ws[j].s })
	for _, w := range ws {
		parts = append(parts, fmt.Sprintf("%s %.1fs", w.name, w.s))
	}
	if a.device && b.device {
		parts = append(parts, fmt.Sprintf("device %s reads, %s writes", ioSummary(b.reads-a.reads, b.readMs-a.readMs), ioSummary(b.writes-a.writes, b.wMs-a.wMs)))
	}
	if len(parts) == 0 {
		return "no waits recorded"
	}
	return strings.Join(parts, ", ")
}

func ioSummary(n, ms float64) string {
	if n <= 0 {
		return "0"
	}
	return fmt.Sprintf("%.0f at %.1fms", n, ms/n)
}

func (d *driver) nodeWaits(n *node) waits {
	return parseWaits(d.runOnNode(n.port, waitsScript))
}
