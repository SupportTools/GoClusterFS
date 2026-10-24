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
//	(default): Report problems without making changes
//	-repair: Fix the problems marked [FIXABLE]
//	-repair -dry-run: Report what -repair would change, write nothing
//	-force: Keep checking after a fatal error; with -repair, repair even if
//	        a node did not unmount cleanly
//
// Exit codes:
//
//	0: No errors (warnings may remain)
//	1: Errors remain on disk
//	2: Every error found was repaired
//	4: Usage error, I/O failure, or the check could not finish or was refused
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/supporttools/GoClusterFS/internal/fsck"
	"github.com/supporttools/GoClusterFS/internal/storage"
	"github.com/supporttools/GoClusterFS/internal/version"
)

func main() {
	var (
		repair       = flag.Bool("repair", false, "Repair problems automatically where possible (see Repair below)")
		dryRun       = flag.Bool("dry-run", false, "With -repair, report what would change without writing")
		force        = flag.Bool("force", false, "Keep checking after a fatal error; with -repair, repair even if a node did not unmount cleanly")
		verbose      = flag.Bool("verbose", false, "Enable verbose progress output")
		quiet        = flag.Bool("quiet", false, "Only print errors and summary")
		stopOnFatal  = flag.Bool("stop-on-fatal", true, "Stop checking when a fatal error is found")
		skipPhase    = flag.Int("skip-phase", 0, "Skip specific phase (1-8, for debugging)")
		listPhases   = flag.Bool("list-phases", false, "List fsck phases and exit")
		summaryOnly  = flag.Bool("summary", false, "Only print final summary")
		showProblems = flag.Bool("show-problems", true, "Show individual problems found")
		showVersion  = flag.Bool("version", false, "Print version and exit")
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
		fmt.Fprintf(os.Stderr, "  %s -repair -dry-run /dev/sdc     # Show what repair would change\n", os.Args[0])
		fmt.Fprint(os.Stderr, repairHelp)
		fmt.Fprintf(os.Stderr, "\nExit codes:\n")
		fmt.Fprintf(os.Stderr, "  0: No errors (warnings may remain)\n")
		fmt.Fprintf(os.Stderr, "  1: Errors remain on disk\n")
		fmt.Fprintf(os.Stderr, "  2: Every error found was repaired\n")
		fmt.Fprintf(os.Stderr, "  4: Usage error, I/O failure, or the check could not finish or was refused\n")
	}
	flag.Parse()

	if *showVersion {
		fmt.Println("goclusterfs-fsck", version.String())
		os.Exit(0)
	}

	// Handle list-phases request
	if *listPhases {
		printPhases()
		os.Exit(0)
	}

	// Validate arguments
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(fsck.ExitFailed)
	}
	if *dryRun && !*repair {
		fmt.Fprintln(os.Stderr, "Error: -dry-run needs -repair")
		os.Exit(fsck.ExitFailed)
	}
	devicePath := flag.Arg(0)

	// Determine mode
	mode := fsck.CheckOnly
	if *repair {
		mode = fsck.Repair
	} else if *force {
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
		os.Exit(fsck.ExitFailed)
	}
	defer dev.Close()

	// Configure fsck options
	opts := fsck.Options{
		Mode:              mode,
		Verbose:           *verbose,
		Output:            os.Stdout,
		StopOnFatal:       *stopOnFatal && !*force,
		SkipPhases:        make(map[fsck.Phase]bool),
		DryRun:            *dryRun,
		AllowUncleanNodes: *force,
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
		if *dryRun {
			fmt.Printf("Mode: %s (dry run, nothing is written)\n\n", mode)
		} else {
			fmt.Printf("Mode: %s\n\n", mode)
		}
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

	if result.Aborted {
		fmt.Fprintf(os.Stderr, "fsck aborted: %s\n", result.AbortReason)
	}
	if mode == fsck.Repair {
		wouldRepair := 0
		for _, p := range result.Problems {
			if p.WouldRepair {
				wouldRepair++
			}
		}
		if *dryRun {
			fmt.Printf("Dry run: repair would fix %d problems; nothing was written\n", wouldRepair)
		} else if result.RepairedCount > 0 {
			fmt.Printf("Repaired %d problems; %d errors remain\n", result.RepairedCount, result.UnrepairedErrors())
		}
	}
	os.Exit(result.ExitCode())
}

const repairHelp = `
Repair:
  Unmount the filesystem on every node first. -repair refuses to run while a
  node has it mounted, or while a node that did not unmount cleanly may still
  have changes in its journal (-force overrides only the second check).

  -repair rewrites block refcounts from the extents of every inode, snapshot
  and clone, marks blocks that older versions shared as counted, and fixes the
  superblock checksum when every other superblock field checks out. It never
  frees blocks, deletes inodes or edits directories, and it leaves problems
  not marked [FIXABLE] as they are. It skips the refcount rebuild when some
  inode could not be read, since the rebuilt counts could then be too low.

  Back up the device first (for example with dd) and run -repair -dry-run to
  see what would change.
`

// printPhases lists all fsck phases.
func printPhases() {
	fmt.Println("GoClusterFS fsck phases:")
	fmt.Println()
	phases := []struct {
		num  int
		name string
		desc string
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
