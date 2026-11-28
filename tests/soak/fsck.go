//go:build linux

package main

import (
	"fmt"
	"log"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// fsckOperational is goclusterfs-fsck's exit code when it could not finish the check.
const fsckOperational = 8

// fsckPass runs one fsck pass with flags, saves its whole output as fsck/<seq>-<pass>.txt in the run's directory, and
// returns its exit status and problem lines.
func (d *driver) fsckPass(seq int64, pass, flags string) (string, []string, error) {
	rc, lines, err := d.ctl.run(strings.TrimSpace("fsck " + flags))
	if err != nil {
		return "", nil, err
	}
	name := filepath.Join(d.cfg.out, "fsck", fmt.Sprintf("%d-%s.txt", seq, pass))
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		log.Printf("fsck: saving its output: %v", err)
	} else if err := os.WriteFile(name, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		log.Printf("fsck: saving its output: %v", err)
	}
	if code, err := strconv.Atoi(strings.TrimPrefix(rc, "rc=")); err != nil || code >= fsckOperational {
		return rc, nil, fmt.Errorf("fsck %s ended with %s; its output is in fsck/%d-%s.txt", flags, rc, seq, pass)
	}
	return rc, problemLines(lines), nil
}

// problemLines returns the problems in fsck's output, unindented, without INFO lines or the summary.
func problemLines(lines []string) []string {
	var problems []string
	for _, line := range lines {
		line = strings.TrimLeft(line, " ")
		if slices.ContainsFunc([]string{"[WARN]", "[ERROR]", "[CRIT]", "[FATAL]"}, func(tag string) bool { return strings.HasPrefix(line, tag) }) {
			problems = append(problems, line)
		}
	}
	return problems
}

var leakedBlocks = regexp.MustCompile(`(\d+) (?:more )?leaked blocks`)

// leakKind names what one leak line of fsck reports and how many: "8 more leaked blocks" stands for 8. A line that
// only says why repair keeps blocks other lines already listed counts none.
func leakKind(line string) (string, int) {
	n := 1
	if m := leakedBlocks.FindStringSubmatch(line); m != nil {
		n, _ = strconv.Atoi(m[1])
	}
	switch {
	case strings.Contains(line, "unnamed: no directory names it"):
		return "unnamed inodes", n
	case strings.Contains(line, "belongs to no snapshot"):
		return "snapshot inodes", n
	case strings.Contains(line, "links but"):
		return "extra links", n
	case strings.Contains(line, "mapped by"):
		return "high refcounts", n
	case strings.Contains(line, "have a refcount"), strings.Contains(line, "no inode maps it but refcount"):
		return "counted blocks", n
	case strings.Contains(line, "are kept, because"):
		return "", 0
	case strings.Contains(line, "are kept"):
		return "kept blocks", n
	}
	return "bitmap blocks", n
}

// describeLeaks counts leak lines by kind, as "13 leaks (bitmap blocks=12, unnamed inodes=1)".
func describeLeaks(leaks []string) string {
	kinds := map[string]int{}
	total := 0
	for _, line := range leaks {
		if kind, n := leakKind(line); n > 0 {
			kinds[kind] += n
			total += n
		}
	}
	if total == 0 {
		return "0 leaks"
	}
	var parts []string
	for _, kind := range slices.Sorted(maps.Keys(kinds)) {
		parts = append(parts, fmt.Sprintf("%s=%d", kind, kinds[kind]))
	}
	return fmt.Sprintf("%d leaks (%s)", total, strings.Join(parts, ", "))
}
