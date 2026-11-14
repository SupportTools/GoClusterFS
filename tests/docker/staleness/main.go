// Command gcfs-staleness measures how late one node sees another node's
// writes. The writer stamps each version into the file's size and its last
// bytes, which every other version moves into blocks it maps again. The reader watches the file three ways and logs every change it
// sees, and analyze turns both logs into per-version lag.
//
//	gcfs-staleness write -file F -versions N
//	gcfs-staleness read -file F -duration D -modes open,stat,reopen
//	gcfs-staleness flood -dir D -duration D
//	gcfs-staleness analyze -writer W -reader R -bound-open 6s -bound-stat 1.5s -bound-reopen 500ms
package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	headerLen = 11
	bigBase   = 2 << 20
	smallBase = 8192
	sizeStep  = 16
)

// sizeOf alternates between a 2 MB file and an 8 KB one, so every other
// version truncates away blocks and the next one maps new ones.
func sizeOf(v int) int64 {
	if v%2 == 0 {
		return bigBase + int64(v)*sizeStep
	}
	return smallBase + int64(v)*sizeStep
}

func versionOfSize(size int64) int {
	base := int64(smallBase)
	if size >= bigBase {
		base = bigBase
	}
	if size < base || (size-base)%sizeStep != 0 {
		return -1
	}
	return int((size - base) / sizeStep)
}

func header(v int) []byte { return []byte(fmt.Sprintf("%010d\n", v)) }

func versionOfHeader(b []byte) int {
	if len(b) < headerLen || b[headerLen-1] != '\n' {
		return -1
	}
	v, err := strconv.Atoi(string(b[:headerLen-1]))
	if err != nil {
		return -1
	}
	return v
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(2)
}

func write(args []string) {
	fs := flag.NewFlagSet("write", flag.ExitOnError)
	file := fs.String("file", "", "file to write")
	versions := fs.Int("versions", 100, "number of versions")
	minGap := fs.Duration("min", 100*time.Millisecond, "shortest pause between versions")
	maxGap := fs.Duration("max", 600*time.Millisecond, "longest pause between versions")
	_ = fs.Parse(args)

	for v := 1; v <= *versions; v++ {
		f, err := os.OpenFile(*file, os.O_RDWR|os.O_CREATE, 0o644)
		if err != nil {
			fail("open: %v", err)
		}
		if err := f.Truncate(sizeOf(v)); err != nil {
			fail("truncate v%d: %v", v, err)
		}
		if _, err := f.WriteAt(header(v), sizeOf(v)-headerLen); err != nil {
			fail("write v%d: %v", v, err)
		}
		if err := f.Close(); err != nil {
			fail("close v%d: %v", v, err)
		}
		fmt.Printf("W %d %d\n", time.Now().UnixNano(), v)
		gap := *minGap + time.Duration(rand.Int64N(int64(*maxGap-*minGap)+1))
		time.Sleep(gap)
	}
}

// observation is what one way of looking at the file saw: the version its
// size says and the version its last bytes say. -1 means unreadable.
type observation struct{ size, data int }

func read(args []string) {
	fs := flag.NewFlagSet("read", flag.ExitOnError)
	file := fs.String("file", "", "file to watch")
	duration := fs.Duration("duration", time.Minute, "how long to watch")
	poll := fs.Duration("poll", 10*time.Millisecond, "pause between looks")
	modes := fs.String("modes", "open,stat,reopen", "ways to look; one node's watchers share its kernel caches, so run one per node to measure each alone")
	_ = fs.Parse(args)

	var mu sync.Mutex
	logChange := func(mode string, o observation) {
		mu.Lock()
		fmt.Printf("R %d %s %d %d\n", time.Now().UnixNano(), mode, o.size, o.data)
		mu.Unlock()
	}
	watch := func(mode string, look func() observation) {
		last := observation{-2, -2}
		for end := time.Now().Add(*duration); time.Now().Before(end); time.Sleep(*poll) {
			if o := look(); o != last {
				logChange(mode, o)
				last = o
			}
		}
	}
	readTail := func(f *os.File, size int64) int {
		buf := make([]byte, headerLen)
		if n, err := f.ReadAt(buf, size-headerLen); n < headerLen && err != nil {
			return -1
		}
		return versionOfHeader(buf)
	}

	var held *os.File
	for deadline := time.Now().Add(30 * time.Second); held == nil; time.Sleep(*poll) {
		f, err := os.Open(*file)
		if err == nil {
			held = f
		} else if time.Now().After(deadline) {
			fail("open %s: %v", *file, err)
		}
	}
	defer func() { _ = held.Close() }()

	looks := map[string]func() observation{
		"open": func() observation {
			fi, err := held.Stat()
			if err != nil {
				return observation{-1, -1}
			}
			return observation{versionOfSize(fi.Size()), readTail(held, fi.Size())}
		},
		"stat": func() observation {
			fi, err := os.Stat(*file)
			if err != nil {
				return observation{-1, -1}
			}
			return observation{versionOfSize(fi.Size()), 0}
		},
		"reopen": func() observation {
			f, err := os.Open(*file)
			if err != nil {
				return observation{-1, -1}
			}
			defer func() { _ = f.Close() }()
			fi, err := f.Stat()
			if err != nil {
				return observation{-1, -1}
			}
			return observation{versionOfSize(fi.Size()), readTail(f, fi.Size())}
		},
	}
	var wg sync.WaitGroup
	for _, mode := range strings.Split(*modes, ",") {
		look, ok := looks[mode]
		if !ok {
			fail("unknown mode %q", mode)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			watch(mode, look)
		}()
	}
	wg.Wait()
}

func flood(args []string) {
	fs := flag.NewFlagSet("flood", flag.ExitOnError)
	dir := fs.String("dir", "", "directory for the flood files")
	files := fs.Int("files", 200, "number of files to rewrite in turn")
	duration := fs.Duration("duration", time.Minute, "how long to flood")
	_ = fs.Parse(args)

	if err := os.MkdirAll(*dir, 0o755); err != nil {
		fail("mkdir: %v", err)
	}
	handles := make([]*os.File, *files)
	for i := range handles {
		f, err := os.OpenFile(filepath.Join(*dir, fmt.Sprintf("f%04d", i)), os.O_RDWR|os.O_CREATE, 0o644)
		if err != nil {
			fail("open: %v", err)
		}
		defer func() { _ = f.Close() }()
		handles[i] = f
	}
	buf := make([]byte, 512)
	writes := 0
	for end := time.Now().Add(*duration); time.Now().Before(end); writes++ {
		if _, err := handles[writes%len(handles)].WriteAt(buf, int64(writes/len(handles))%64*512); err != nil {
			fail("write: %v", err)
		}
	}
	fmt.Printf("flood: %d writes in %v\n", writes, *duration)
}

type change struct {
	at  int64
	obs observation
}

func analyze(args []string) {
	fs := flag.NewFlagSet("analyze", flag.ExitOnError)
	writerLog := fs.String("writer", "", "writer log")
	readerLog := fs.String("reader", "", "reader logs, comma separated")
	bounds := map[string]*time.Duration{
		"open":   fs.Duration("bound-open", 6*time.Second, "most lag allowed through a file held open"),
		"stat":   fs.Duration("bound-stat", 1500*time.Millisecond, "most lag allowed for stat on the path"),
		"reopen": fs.Duration("bound-reopen", 500*time.Millisecond, "most lag allowed for a fresh open"),
	}
	_ = fs.Parse(args)

	done := map[int]int64{}
	maxVersion := 0
	scan(*writerLog, func(f []string) {
		if f[0] == "W" {
			at, _ := strconv.ParseInt(f[1], 10, 64)
			v, _ := strconv.Atoi(f[2])
			done[v] = at
			maxVersion = max(maxVersion, v)
		}
	})
	if maxVersion == 0 {
		fail("writer log has no versions")
	}
	failed := false
	changes := map[string][]change{}
	for _, readerLog := range strings.Split(*readerLog, ",") {
		looks := 0
		scan(readerLog, func(f []string) {
			if f[0] == "R" && len(f) == 5 {
				at, _ := strconv.ParseInt(f[1], 10, 64)
				size, _ := strconv.Atoi(f[3])
				data, _ := strconv.Atoi(f[4])
				changes[f[2]] = append(changes[f[2]], change{at, observation{size, data}})
				looks++
			}
		})
		if looks == 0 {
			fmt.Printf("%s has no looks: the reader never opened the file or failed\n", readerLog)
			failed = true
		}
	}

	for _, mode := range []string{"reopen", "stat", "open"} {
		cs, ok := changes[mode]
		if !ok {
			continue
		}
		var lags []time.Duration
		var worst time.Duration
		worstVersion, bad := 0, 0
		for v := 1; v <= maxVersion; v++ {
			seen := int64(-1)
			for _, c := range cs {
				if c.obs.size >= v && (mode == "stat" || c.obs.data >= v) {
					seen = c.at
					break
				}
			}
			if seen < 0 {
				continue
			}
			lag := time.Duration(max(0, seen-done[v]))
			lags = append(lags, lag)
			if lag > worst {
				worst, worstVersion = lag, v
			}
		}
		regressions, highest := 0, observation{}
		for _, c := range cs {
			if c.obs.size == -1 || (mode != "stat" && c.obs.data == -1) {
				bad++
				continue
			}
			if c.obs.size < highest.size || c.obs.data < highest.data {
				regressions++
			}
			highest = observation{max(highest.size, c.obs.size), max(highest.data, c.obs.data)}
		}
		if regressions > 0 {
			failed = true
		}
		sort.Slice(lags, func(i, j int) bool { return lags[i] < lags[j] })
		median := time.Duration(0)
		if len(lags) > 0 {
			median = lags[len(lags)/2]
		}
		verdict := "ok"
		if worst > *bounds[mode] {
			verdict = "OUT OF BOUND"
			failed = true
		}
		if regressions > 0 {
			verdict = "WENT BACKWARDS"
		}
		// Readers watch until well after the writer's last version, so each
		// must reach it.
		if len(lags) < maxVersion {
			verdict = fmt.Sprintf("NEVER SAW %d VERSIONS", maxVersion-len(lags))
			failed = true
		}
		fmt.Printf("%-6s versions seen %d/%d  median %v  worst %v (v%d)  bound %v  unreadable looks %d  regressions %d  %s\n",
			mode, len(lags), maxVersion, median.Round(time.Millisecond), worst.Round(time.Millisecond), worstVersion, *bounds[mode], bad, regressions, verdict)
	}
	if failed {
		os.Exit(1)
	}
}

func scan(path string, each func([]string)) {
	f, err := os.Open(path)
	if err != nil {
		fail("%v", err)
	}
	defer func() { _ = f.Close() }()
	s := bufio.NewScanner(f)
	for s.Scan() {
		if fields := strings.Fields(s.Text()); len(fields) > 0 {
			each(fields)
		}
	}
}

func main() {
	if len(os.Args) < 2 {
		fail("usage: gcfs-staleness write|read|flood|analyze [flags]")
	}
	switch os.Args[1] {
	case "write":
		write(os.Args[2:])
	case "read":
		read(os.Args[2:])
	case "flood":
		flood(os.Args[2:])
	case "analyze":
		analyze(os.Args[2:])
	default:
		fail("unknown command %q", os.Args[1])
	}
}
