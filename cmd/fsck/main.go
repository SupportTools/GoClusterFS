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
//	        a stopped node's journal still needs replay
//	-list-slots: List node registry slots with their lease and journal
//	-release-slot N: Free the slot of a node that will not return; with
//	        -discard-journal, reset its journal and free it
//
// Exit codes, bits as with fsck(8):
//
//	0: No errors (warnings may remain)
//	1: Errors were repaired
//	4: Errors remain on disk
//	8: I/O failure, or the check could not finish or was refused
//	16: Usage error
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
		force        = flag.Bool("force", false, "Keep checking after a fatal error; with -repair, repair even if a stopped node's journal still needs replay")
		verbose      = flag.Bool("verbose", false, "Enable verbose progress output")
		quiet        = flag.Bool("quiet", false, "Only print errors and summary")
		stopOnFatal  = flag.Bool("stop-on-fatal", true, "Stop checking when a fatal error is found")
		skipPhase    = flag.Int("skip-phase", 0, "Skip specific phase (1-8, for debugging)")
		listPhases   = flag.Bool("list-phases", false, "List fsck phases and exit")
		summaryOnly  = flag.Bool("summary", false, "Only print final summary")
		showProblems = flag.Bool("show-problems", true, "Show individual problems found")
		showVersion  = flag.Bool("version", false, "Print version and exit")

		listSlots      = flag.Bool("list-slots", false, "List node registry slots with their lease and journal, then exit")
		releaseSlotN   = flag.Int("release-slot", -1, "Check that registry slot N can be released and print what releasing it discards")
		discardJournal = flag.Bool("discard-journal", false, "With -release-slot, reset the slot's journal and free the slot")
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
		fmt.Fprintf(os.Stderr, "  %s -list-slots /dev/sdb          # Show node registry slots\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -release-slot 3 /dev/sdb      # Show what releasing slot 3 discards\n", os.Args[0])
		fmt.Fprint(os.Stderr, repairHelp)
		fmt.Fprint(os.Stderr, slotsHelp)
		fmt.Fprintf(os.Stderr, "\nExit codes (bits, as with fsck(8); 5 means some errors were repaired and some remain):\n")
		fmt.Fprintf(os.Stderr, "  0: No errors (warnings may remain)\n")
		fmt.Fprintf(os.Stderr, "  1: Errors were repaired\n")
		fmt.Fprintf(os.Stderr, "  4: Errors remain on disk\n")
		fmt.Fprintf(os.Stderr, "  8: I/O failure, or the check could not finish or was refused\n")
		fmt.Fprintf(os.Stderr, "  16: Usage error\n")
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
		os.Exit(fsck.ExitUsage)
	}
	if *dryRun && !*repair {
		fmt.Fprintln(os.Stderr, "Error: -dry-run needs -repair")
		os.Exit(fsck.ExitUsage)
	}
	releasing := *releaseSlotN != -1
	switch {
	case *discardJournal && !releasing:
		fmt.Fprintln(os.Stderr, "Error: -discard-journal needs -release-slot")
		os.Exit(fsck.ExitUsage)
	case releasing && *releaseSlotN < 0:
		fmt.Fprintln(os.Stderr, "Error: -release-slot needs a slot number")
		os.Exit(fsck.ExitUsage)
	case *listSlots && releasing, (*listSlots || releasing) && *repair:
		fmt.Fprintln(os.Stderr, "Error: -list-slots, -release-slot and -repair run separately")
		os.Exit(fsck.ExitUsage)
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
		os.Exit(fsck.ExitOperational)
	}
	defer dev.Close()

	if *listSlots || releasing {
		checker := fsck.New(dev, fsck.Options{Output: os.Stdout, Verbose: *verbose})
		if *listSlots {
			err = printSlots(os.Stdout, checker)
		} else {
			err = releaseSlot(os.Stdout, checker, *releaseSlotN, *discardJournal)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(fsck.ExitOperational)
		}
		os.Exit(fsck.ExitClean)
	}

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
  node's heartbeat still moves, which it can take up to 11 seconds to rule out
  right after an unmount, or while a stopped node's journal still needs replay
  (-force overrides only the second check).

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
		{1, "Superblock", "Validates magic, version, sizes, the format v2 region layout, checksum"},
		{2, "Bitmap", "Checks sector checksums, counts allocated blocks"},
		{3, "Inodes", "Validates per-inode checksums, mode, link count, extent trees"},
		{4, "Extents", "Verifies block bounds, allocation, overlap detection"},
		{5, "Refcounts", "Checks each block's refcount covers the inodes that map it"},
		{6, "Directories", "Validates entries, detects orphaned inodes"},
		{7, "Snapshots", "Verifies snapshot registry, descriptors and data inodes"},
		{8, "Cross-Validation", "Finds leaked blocks, blocks mapped but free, unlinked inodes"},
	}
	for _, p := range phases {
		fmt.Printf("  Phase %d (%s):\n", p.num, p.name)
		fmt.Printf("    %s\n\n", p.desc)
	}
}
