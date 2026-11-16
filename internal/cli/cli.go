// Package cli holds what goclusterfs-mount, goclusterfs-fsck and gfsctl
// share: exit codes, flag parsing and hints for common errors.
package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
)

// Exit codes are bits, as with fsck(8), so goclusterfs-fsck can add its own
// results (1 repaired, 4 errors remain) to them.
const (
	ExitOK      = 0
	ExitFailure = 8  // the command could not do its job, or refused to
	ExitUsage   = 16 // bad flags or arguments; nothing was changed
)

// ParseFlags parses args. When it returns false the caller exits with the
// code: ExitOK after -h, ExitUsage after a bad flag. The flag package has
// already printed the error and the usage.
func ParseFlags(fs *flag.FlagSet, args []string) (int, bool) {
	fs.Init(fs.Name(), flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return ExitOK, false
		}
		return ExitUsage, false
	}
	return ExitOK, true
}

// OpenHint says what to do about an error from opening a device or image.
func OpenHint(err error) string {
	switch {
	case errors.Is(err, os.ErrNotExist):
		return "check the path"
	case errors.Is(err, os.ErrPermission):
		return "run as root or give this user read and write access to it"
	case errors.Is(err, syscall.EINVAL):
		return "its filesystem does not support O_DIRECT (tmpfs before Linux 6.6); put the image on ext4 or xfs"
	case errors.Is(err, syscall.EBUSY):
		return "another program has it open exclusively"
	}
	return "check that the device exists and is readable and writable"
}

// PrintFlags prints fs's flags like flag.PrintDefaults, but names every
// default, including false, 0 and the empty string.
func PrintFlags(w io.Writer, fs *flag.FlagSet) {
	fs.VisitAll(func(f *flag.Flag) {
		typ, usage := flag.UnquoteUsage(f)
		head := "  -" + f.Name
		if typ != "" {
			head += " " + typ
		}
		def := f.DefValue
		switch {
		case typ == "string" && def == "":
			def = "empty"
		case typ == "string":
			def = fmt.Sprintf("%q", def)
		}
		_, _ = fmt.Fprintf(w, "%s\n    \t%s (default %s)\n", head, strings.ReplaceAll(usage, "\n", "\n    \t"), def)
	})
}
