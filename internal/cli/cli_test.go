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

func TestPrintFlagsNamesEveryDefault(t *testing.T) {
	fs := flag.NewFlagSet("x", flag.ContinueOnError)
	fs.Bool("off", false, "Off flag")
	fs.Bool("on", true, "On flag")
	fs.String("empty", "", "Empty string")
	fs.String("addr", "127.0.0.1:8080", "Address")
	fs.Int("count", 0, "Count")
	fs.Duration("ttl", 0, "TTL")
	var out strings.Builder
	PrintFlags(&out, fs)
	for _, want := range []string{
		"  -off\n    \tOff flag (default false)\n",
		"  -on\n    \tOn flag (default true)\n",
		"  -empty string\n    \tEmpty string (default empty)\n",
		"  -addr string\n    \tAddress (default \"127.0.0.1:8080\")\n",
		"  -count int\n    \tCount (default 0)\n",
		"  -ttl duration\n    \tTTL (default 0s)\n",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q in:\n%s", want, out.String())
		}
	}
}
