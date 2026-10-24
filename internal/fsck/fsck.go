// Package fsck provides filesystem consistency checking and repair for GoClusterFS.
//
// The fsck tool performs an 8-phase validation of the on-disk structures:
//
//  1. Superblock - Magic, version, region bounds, checksum
//  2. Block Bitmap - Sector checksums, bit consistency
//  3. Inode Table - Per-inode checksums, extent validity
//  4. Extents - Logical/physical block sanity, no overlaps
//  5. Refcounts - Sector checksums, consistency with bitmap/extents
//  6. Directories - Entry validity, parent linkage, no cycles
//  7. Snapshots - Descriptor validity, tree consistency
//  8. Cross-Validation - Counter consistency across all structures
//
// fsck can operate in three modes:
//   - CheckOnly: Report problems without making changes
//   - Repair: Fix problems automatically where possible
//   - Force: Proceed despite unfixable errors
package fsck

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/supporttools/GoClusterFS/internal/storage"
)

// Mode controls fsck behavior when problems are found.
type Mode int

const (
	// CheckOnly reports problems without making changes.
	CheckOnly Mode = iota

	// Repair fixes problems automatically where possible.
	Repair

	// Force continues despite errors (for data recovery attempts).
	Force
)

func (m Mode) String() string {
	switch m {
	case CheckOnly:
		return "check-only"
	case Repair:
		return "repair"
	case Force:
		return "force"
	default:
		return "unknown"
	}
}

// Severity indicates how serious a problem is.
type Severity int

const (
	// Info is informational, not an error.
	Info Severity = iota

	// Warning is a minor issue that doesn't affect consistency.
	Warning

	// Error is a consistency issue that should be fixed.
	Error

	// Critical is a severe issue that may cause data loss.
	Critical

	// Fatal is an issue that prevents further checking.
	Fatal
)

func (s Severity) String() string {
	switch s {
	case Info:
		return "INFO"
	case Warning:
		return "WARN"
	case Error:
		return "ERROR"
	case Critical:
		return "CRIT"
	case Fatal:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

// Problem represents a single consistency issue found during checking.
type Problem struct {
	Phase       Phase    // Which phase found the problem
	Severity    Severity // How serious the problem is
	Location    string   // Where the problem was found (e.g., "inode 42")
	Message     string   // Description of the problem
	Repairable  bool     // Whether the problem can be automatically repaired
	Repaired    bool     // Whether the problem was repaired (if Mode == Repair)
	WouldRepair bool     // Whether a dry run found that repair would fix it
}

func (p Problem) String() string {
	status := ""
	switch {
	case p.Repaired:
		status = " [FIXED]"
	case p.WouldRepair:
		status = " [WOULD FIX]"
	case p.Repairable:
		status = " [FIXABLE]"
	}
	return fmt.Sprintf("[%s] %s: %s: %s%s",
		p.Severity, p.Phase, p.Location, p.Message, status)
}

// Phase represents a validation phase in fsck.
type Phase int

const (
	PhaseSuperblock Phase = iota + 1
	PhaseBitmap
	PhaseInodes
	PhaseExtents
	PhaseRefcounts
	PhaseDirectories
	PhaseSnapshots
	PhaseCrossValidation
)

func (p Phase) String() string {
	switch p {
	case PhaseSuperblock:
		return "Phase 1 (Superblock)"
	case PhaseBitmap:
		return "Phase 2 (Bitmap)"
	case PhaseInodes:
		return "Phase 3 (Inodes)"
	case PhaseExtents:
		return "Phase 4 (Extents)"
	case PhaseRefcounts:
		return "Phase 5 (Refcounts)"
	case PhaseDirectories:
		return "Phase 6 (Directories)"
	case PhaseSnapshots:
		return "Phase 7 (Snapshots)"
	case PhaseCrossValidation:
		return "Phase 8 (Cross-Validation)"
	default:
		return fmt.Sprintf("Phase %d (Unknown)", int(p))
	}
}

// Result contains the complete results of an fsck run.
type Result struct {
	mu sync.Mutex

	// Timing
	StartTime time.Time
	EndTime   time.Time
	Duration  time.Duration

	// Mode and completion
	Mode           Mode
	CompletedPhase Phase
	Aborted        bool
	AbortReason    string

	// Problems found
	Problems []Problem

	// Summary counts
	InfoCount     int
	WarningCount  int
	ErrorCount    int
	CriticalCount int
	FatalCount    int
	RepairedCount int

	// Structure counts (discovered during check)
	TotalBlocks     uint64
	AllocatedBlocks uint64
	FreeBlocks      uint64
	TotalInodes     uint64
	UsedInodes      uint64
	FreeInodes      uint64
	DirectoryCount  uint64
	FileCount       uint64
	SnapshotCount   uint32
	OrphanedInodes  uint64
	OrphanedBlocks  uint64
}

// AddProblem adds a new problem to the result.
func (r *Result) AddProblem(p Problem) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.Problems = append(r.Problems, p)

	switch p.Severity {
	case Info:
		r.InfoCount++
	case Warning:
		r.WarningCount++
	case Error:
		r.ErrorCount++
	case Critical:
		r.CriticalCount++
	case Fatal:
		r.FatalCount++
	}

	if p.Repaired {
		r.RepairedCount++
	}
}

// HasErrors returns true if any errors (Error or worse) were found.
func (r *Result) HasErrors() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ErrorCount > 0 || r.CriticalCount > 0 || r.FatalCount > 0
}

// UnrepairedErrors counts the problems of severity Error or worse that are
// still on disk.
func (r *Result) UnrepairedErrors() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.unrepairedErrors()
}

func (r *Result) unrepairedErrors() int {
	n := 0
	for _, p := range r.Problems {
		if p.Severity >= Error && !p.Repaired {
			n++
		}
	}
	return n
}

// Exit codes of goclusterfs-fsck.
const (
	ExitClean    = 0 // no errors (warnings may remain)
	ExitErrors   = 1 // errors remain on disk
	ExitRepaired = 2 // every error found was repaired
	ExitFailed   = 4 // usage error, I/O failure, or the check could not finish or was refused
)

// ExitCode returns the exit code for the result.
func (r *Result) ExitCode() int {
	switch {
	case r.Aborted:
		return ExitFailed
	case r.UnrepairedErrors() > 0:
		return ExitErrors
	case r.RepairedCount > 0:
		return ExitRepaired
	}
	return ExitClean
}

// IsClean returns true if no problems of any kind were found.
func (r *Result) IsClean() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.Problems) == 0
}

// Summary returns a human-readable summary of the results.
func (r *Result) Summary() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	status := "CLEAN"
	if r.Aborted {
		status = "ABORTED"
	} else if r.RepairedCount > 0 && r.unrepairedErrors() == 0 {
		status = "REPAIRED"
	} else if r.FatalCount > 0 || r.CriticalCount > 0 {
		status = "ERRORS FOUND"
	} else if r.ErrorCount > 0 {
		status = "ISSUES FOUND"
	} else if r.WarningCount > 0 {
		status = "WARNINGS"
	}

	return fmt.Sprintf(`GoClusterFS fsck Results
========================
Status: %s
Mode: %s
Duration: %v
Completed Phase: %s

Filesystem Statistics:
  Total Blocks: %d
  Allocated: %d / Free: %d
  Total Inodes: %d
  Used: %d / Free: %d
  Directories: %d / Files: %d
  Snapshots: %d
  Orphaned Inodes: %d
  Orphaned Blocks: %d

Problems Found:
  Info: %d
  Warnings: %d
  Errors: %d
  Critical: %d
  Fatal: %d
  Repaired: %d
`,
		status,
		r.Mode,
		r.Duration,
		r.CompletedPhase,
		r.TotalBlocks,
		r.AllocatedBlocks, r.FreeBlocks,
		r.TotalInodes,
		r.UsedInodes, r.FreeInodes,
		r.DirectoryCount, r.FileCount,
		r.SnapshotCount,
		r.OrphanedInodes,
		r.OrphanedBlocks,
		r.InfoCount,
		r.WarningCount,
		r.ErrorCount,
		r.CriticalCount,
		r.FatalCount,
		r.RepairedCount,
	)
}

// Options configures fsck behavior.
type Options struct {
	// Mode controls whether to repair or just check.
	Mode Mode

	// Verbose enables detailed progress output.
	Verbose bool

	// Output is where to write progress/results (default: io.Discard).
	Output io.Writer

	// StopOnFatal stops checking when a fatal error is encountered.
	StopOnFatal bool

	// SkipPhases allows skipping specific phases (for testing).
	SkipPhases map[Phase]bool

	// DryRun makes Repair report what it would change without writing.
	DryRun bool

	// AllowUncleanNodes lets Repair run although a node left the filesystem
	// without unmounting. Repair never runs while a node has it mounted.
	AllowUncleanNodes bool
}

// DefaultOptions returns the default fsck options.
func DefaultOptions() Options {
	return Options{
		Mode:        CheckOnly,
		Verbose:     false,
		Output:      io.Discard,
		StopOnFatal: true,
		SkipPhases:  make(map[Phase]bool),
	}
}

// Checker performs filesystem consistency checks.
type Checker struct {
	dev    *storage.Device
	opts   Options
	result *Result

	// Parsed superblock (set after Phase 1)
	sbData *SuperblockData

	// Collected data for cross-validation
	bitmapAllocated map[uint64]bool // blocks marked allocated in bitmap
	inodeBlocks     map[uint64]int  // block -> reference count from inodes
	inodeUsed       map[uint32]bool // inode numbers that are in use
	dirEntryRefs    map[uint32]int  // inode -> reference count from dir entries
	snapshotInodes  map[uint32]bool // inodes referenced by snapshots
	snapshotData    map[uint32]bool // inodes holding a snapshot's captured extents
	sfbContainers   map[uint64]bool // blocks holding small files of older versions
	extentTrees     map[uint32]*extentTree
	uncounted       map[uint32]int // inodes an older version shared -> problem index
	refsIncomplete  []string       // why inodeBlocks may miss references
	superblockFix   func() error   // runs once repair knows no node uses the filesystem
}

// SuperblockData holds parsed superblock information for use by other phases.
type SuperblockData struct {
	Magic         [12]byte
	Version       uint32
	UUID          [16]byte
	FeatureFlags  uint64
	RequiredFlags uint64
	BlockSize     uint32
	TotalBlocks   uint64
	MetaBlockSize uint32
	MaxNodes      uint32
	InodeSize     uint32

	// Region offsets
	NodeRegistryOffset  uint64
	JournalRegionOffset uint64
	LockBitmapOffset    uint64
	InodeTableOffset    uint64
	BlockBitmapOffset   uint64
	RefCountTableOffset uint64
	SnapshotRegOffset   uint64
	DataRegionOffset    uint64

	// Region sizes
	NodeRegistrySize  uint64
	JournalRegionSize uint64
	LockBitmapSize    uint64
	InodeTableSize    uint64
	BlockBitmapSize   uint64
	RefCountTableSize uint64
	SnapshotRegSize   uint64

	// Counters
	InodeCount    uint64
	FreeInodes    uint64
	FreeBlocks    uint64
	SnapshotCount uint32
	MountCount    uint32

	// Timestamps
	CreateTime    int64
	LastMountTime int64
	LastWriteTime int64
}

// New creates a new Checker for the given device.
func New(dev *storage.Device, opts Options) *Checker {
	if opts.Output == nil {
		opts.Output = io.Discard
	}
	if opts.SkipPhases == nil {
		opts.SkipPhases = make(map[Phase]bool)
	}

	return &Checker{
		dev:             dev,
		opts:            opts,
		result:          &Result{Mode: opts.Mode},
		bitmapAllocated: make(map[uint64]bool),
		inodeBlocks:     make(map[uint64]int),
		inodeUsed:       make(map[uint32]bool),
		dirEntryRefs:    make(map[uint32]int),
		snapshotInodes:  make(map[uint32]bool),
		snapshotData:    make(map[uint32]bool),
		sfbContainers:   make(map[uint64]bool),
		extentTrees:     make(map[uint32]*extentTree),
		uncounted:       make(map[uint32]int),
	}
}

// Check runs all fsck phases and returns the result.
func (c *Checker) Check() *Result {
	c.result.StartTime = time.Now()

	phases := []struct {
		phase Phase
		fn    func() error
	}{
		{PhaseSuperblock, c.checkSuperblock},
		{PhaseBitmap, c.checkBitmap},
		{PhaseInodes, c.checkInodes},
		{PhaseExtents, c.checkExtents},
		{PhaseRefcounts, c.checkRefcounts},
		{PhaseDirectories, c.checkDirectories},
		{PhaseSnapshots, c.checkSnapshots},
		{PhaseCrossValidation, c.crossValidate},
	}

	for _, p := range phases {
		if c.opts.SkipPhases[p.phase] {
			c.logf("%s: SKIPPED\n", p.phase)
			continue
		}

		c.logf("%s: Starting...\n", p.phase)

		if err := p.fn(); err != nil {
			c.result.AddProblem(Problem{
				Phase:    p.phase,
				Severity: Fatal,
				Location: "phase execution",
				Message:  err.Error(),
			})

			// Later phases read regions the superblock places.
			if c.opts.StopOnFatal || p.phase == PhaseSuperblock {
				c.result.Aborted = true
				c.result.AbortReason = fmt.Sprintf("%s failed: %v", p.phase, err)
				break
			}
		}

		c.result.CompletedPhase = p.phase
		c.logf("%s: Complete\n", p.phase)

		if p.phase == PhaseSuperblock {
			if err := c.startRepair(); err != nil {
				c.result.Aborted = true
				c.result.AbortReason = err.Error()
				break
			}
		}
	}

	c.result.EndTime = time.Now()
	c.result.Duration = c.result.EndTime.Sub(c.result.StartTime)

	return c.result
}

// logf writes to output if verbose mode is enabled.
func (c *Checker) logf(format string, args ...interface{}) {
	if c.opts.Verbose {
		fmt.Fprintf(c.opts.Output, format, args...)
	}
}

// addProblem is a convenience method to add a problem.
func (c *Checker) addProblem(phase Phase, severity Severity, location, message string, repairable bool) {
	c.result.AddProblem(Problem{
		Phase:      phase,
		Severity:   severity,
		Location:   location,
		Message:    message,
		Repairable: repairable,
	})
}
