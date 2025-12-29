// Command fsck checks and optionally repairs a GoClusterFS filesystem.
//
// Usage:
//
//	goclusterfs-fsck [flags] <device>
//
// The fsck tool validates all on-disk structures and reports any consistency
// issues found. By default it runs in check-only mode.
//
// Modes:
//
//	-check (default): Report problems without making changes
//	-repair: Fix problems automatically where possible
//	-force: Continue despite fatal errors (data recovery mode)
//
// Exit Codes:
//
//	0: Filesystem is clean
//	1: Errors were found (and possibly repaired)
//	2: Usage error or unrecoverable failure
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/storage"
)

func main() {
	var (
		repair       = flag.Bool("repair", false, "Repair problems automatically where possible")
		force        = flag.Bool("force", false, "Continue despite fatal errors (data recovery)")
		verbose      = flag.Bool("verbose", false, "Enable verbose progress output")
		quiet        = flag.Bool("quiet", false, "Only print errors and summary")
		stopOnFatal  = flag.Bool("stop-on-fatal", true, "Stop checking when a fatal error is found")
		skipPhase    = flag.Int("skip-phase", 0, "Skip specific phase (1-8, for debugging)")
		listPhases   = flag.Bool("list-phases", false, "List fsck phases and exit")
		summaryOnly  = flag.Bool("summary", false, "Only print final summary")
		showProblems = flag.Bool("show-problems", true, "Show individual problems found")
	)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags] <device>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "GoClusterFS filesystem consistency checker\n\n")
		fmt.Fprintf(os.Stderr, "Flags:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s /dev/sdb                     # Check /dev/sdb\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -repair /path/to/disk.img    # Check and repair\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -verbose -repair /dev/sdc    # Verbose repair\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nExit codes:\n")
		fmt.Fprintf(os.Stderr, "  0: Filesystem is clean\n")
		fmt.Fprintf(os.Stderr, "  1: Errors were found\n")
		fmt.Fprintf(os.Stderr, "  2: Usage error or fatal failure\n")
	}
	flag.Parse()

	// Handle list-phases request
	if *listPhases {
		printPhases()
		os.Exit(0)
	}

	// Validate arguments
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(2)
	}
	devicePath := flag.Arg(0)

	// Determine mode
	mode := fsck.CheckOnly
	if *repair {
		mode = fsck.Repair
	}
	if *force {
		mode = fsck.Force
	}

	// Quiet suppresses verbose
	if *quiet {
		*verbose = false
	}

	// Open device
	dev, err := storage.Open(devicePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to open device: %v\n", err)
		os.Exit(2)
	}
	defer dev.Close()

	// Configure fsck options
	opts := fsck.Options{
		Mode:        mode,
		Verbose:     *verbose,
		Output:      os.Stdout,
		StopOnFatal: *stopOnFatal,
		SkipPhases:  make(map[fsck.Phase]bool),
	}

	// Handle skip-phase
	if *skipPhase > 0 && *skipPhase <= 8 {
		opts.SkipPhases[fsck.Phase(*skipPhase)] = true
		if !*quiet {
			fmt.Printf("Note: Skipping %s\n", fsck.Phase(*skipPhase))
		}
	}

	// Print header
	if !*quiet {
		devSize := dev.Size()
		fmt.Printf("GoClusterFS fsck - %s (%d MB)\n", devicePath, devSize/(1024*1024))
		fmt.Printf("Mode: %s\n\n", mode)
	}

	// Create checker and run
	checker := fsck.New(dev, opts)
	result := checker.Check()

	// Print problems if requested
	if *showProblems && !*summaryOnly {
		for _, p := range result.Problems {
			// In quiet mode, only show errors and worse
			if *quiet && p.Severity < fsck.Error {
				continue
			}
			fmt.Println(p.String())
		}
		if len(result.Problems) > 0 && !*quiet {
			fmt.Println()
		}
	}

	// Print summary
	if !*quiet || result.HasErrors() {
		fmt.Println(result.Summary())
	}

	// Determine exit code
	if result.Aborted {
		fmt.Fprintf(os.Stderr, "fsck aborted: %s\n", result.AbortReason)
		os.Exit(2)
	}
	if result.HasErrors() {
		if mode == fsck.Repair && result.RepairedCount > 0 {
			fmt.Printf("Repaired %d of %d problems\n",
				result.RepairedCount, result.ErrorCount+result.CriticalCount)
		}
		os.Exit(1)
	}
	os.Exit(0)
}

// printPhases lists all fsck phases.
func printPhases() {
	fmt.Println("GoClusterFS fsck phases:")
	fmt.Println()
	phases := []struct {
		num   int
		name  string
		desc  string
	}{
		{1, "Superblock", "Validates magic, version, region bounds, checksum"},
		{2, "Bitmap", "Checks sector checksums, counts allocated blocks"},
		{3, "Inodes", "Validates per-inode checksums, mode, link count"},
		{4, "Extents", "Verifies block bounds, allocation, overlap detection"},
		{5, "Refcounts", "Checks reference counts match extent usage"},
		{6, "Directories", "Validates entries, detects orphaned inodes"},
		{7, "Snapshots", "Verifies snapshot registry and descriptors"},
		{8, "Cross-Validation", "Compares counters across all structures"},
	}
	for _, p := range phases {
		fmt.Printf("  Phase %d (%s):\n", p.num, p.name)
		fmt.Printf("    %s\n\n", p.desc)
	}
}
