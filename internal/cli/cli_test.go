package cli

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
	"testing"
)

func TestParseFlagsExitCodes(t *testing.T) {
	tests := []struct {
		args []string
		code int
		ok   bool
	}{
		{nil, ExitOK, true},
		{[]string{"-debug"}, ExitOK, true},
		{[]string{"--debug"}, ExitOK, true},
		{[]string{"-h"}, ExitOK, false},
		{[]string{"-no-such-flag"}, ExitUsage, false},
		{[]string{"-count", "many"}, ExitUsage, false},
	}
	for _, tt := range tests {
		fs := flag.NewFlagSet("cmd", flag.ExitOnError)
		fs.SetOutput(io.Discard)
		fs.Bool("debug", false, "")
		fs.Int("count", 0, "")
		if code, ok := ParseFlags(fs, tt.args); code != tt.code || ok != tt.ok {
			t.Errorf("ParseFlags(%q) = %d, %v, want %d, %v", tt.args, code, ok, tt.code, tt.ok)
		}
	}
}

func TestOpenHintNamesTheFix(t *testing.T) {
	tests := map[error]string{
		&os.PathError{Op: "open", Path: "/x", Err: syscall.ENOENT}: "check the path",
		&os.PathError{Op: "open", Path: "/x", Err: syscall.EACCES}: "run as root",
		fmt.Errorf("open: %w", syscall.EINVAL):                     "O_DIRECT",
	}
	for err, want := range tests {
		if got := OpenHint(err); !strings.Contains(got, want) {
			t.Errorf("OpenHint(%v) = %q, want it to mention %q", err, got, want)
		}
	}
}
