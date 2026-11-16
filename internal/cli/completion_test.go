package cli

import (
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func completionFlags() *flag.FlagSet {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	fs.String("device", "", "Block device to mount; required")
	fs.String("mount", "", "Directory to mount on")
	fs.Int("port", 31337, "UDP port [see docs]; the same on every node")
	fs.Bool("format", false, "Erase the device. Other nodes' data is lost")
	return fs
}

var demoSpec = CompletionSpec{Values: map[string]string{"device": CompleteFile, "mount": CompleteDir}}

func script(t *testing.T, shell string, spec CompletionSpec) string {
	t.Helper()
	var out strings.Builder
	if err := WriteCompletion(&out, shell, "demo-cmd", completionFlags(), spec); err != nil {
		t.Fatal(err)
	}
	return out.String()
}

// complete runs the bash completion function for words, with the last word being completed, in dir.
func complete(t *testing.T, spec CompletionSpec, dir string, words ...string) []string {
	t.Helper()
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash is not installed")
	}
	quoted := make([]string, len(words))
	for i, w := range words {
		quoted[i] = "'" + w + "'"
	}
	cmd := exec.Command("bash", "-c", script(t, "bash", spec)+
		"COMP_WORDS=("+strings.Join(quoted, " ")+"); COMP_CWORD="+string(rune('0'+len(words)-1))+
		"; _demo_cmd; printf '%s\\n' \"${COMPREPLY[@]}\"")
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("bash: %v\n%s", err, out)
	}
	return strings.Fields(string(out))
}

func TestBashCompletion(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "mnt"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "disk.img"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	positional := CompletionSpec{Positional: CompleteFile}
	tests := []struct {
		name  string
		spec  CompletionSpec
		words []string
		want  string
	}{
		{"flag names", demoSpec, []string{"demo-cmd", "-de"}, "-device"},
		{"file value", demoSpec, []string{"demo-cmd", "-device", "di"}, "disk.img"},
		{"directory value", demoSpec, []string{"demo-cmd", "-mount", ""}, "mnt"},
		{"value without completion", demoSpec, []string{"demo-cmd", "-port", ""}, ""},
		{"no positional", demoSpec, []string{"demo-cmd", "-format", "di"}, ""},
		{"positional device", positional, []string{"demo-cmd", "-format", "di"}, "disk.img"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := strings.Join(complete(t, tt.spec, dir, tt.words...), " ")
			if got != tt.want {
				t.Errorf("completing %q = %q, want %q", tt.words, got, tt.want)
			}
		})
	}
}

func TestZshCompletion(t *testing.T) {
	out := script(t, "zsh", CompletionSpec{Values: demoSpec.Values, Positional: CompleteFile})
	for _, want := range []string{
		"#compdef demo-cmd\n",
		`'-device[Block device to mount]:file:_files'`,
		`'-mount[Directory to mount on]:directory:_files -/'`,
		`'-port[UDP port \[see docs\]]:value: '`,
		`'-format[Erase the device]'`,
		"'1:device:_files'\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("zsh script lacks %q:\n%s", want, out)
		}
	}
	if _, err := exec.LookPath("zsh"); err != nil {
		t.Skip("zsh is not installed")
	}
	if b, err := exec.Command("zsh", "-n", "-c", out).CombinedOutput(); err != nil {
		t.Errorf("zsh -n: %v\n%s", err, b)
	}
}

func TestCompletionQuotesApostrophes(t *testing.T) {
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	fs.Bool("x", false, "other nodes' caches")
	var out strings.Builder
	if err := WriteCompletion(&out, "zsh", "demo-cmd", fs, CompletionSpec{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `'-x[other nodes'\''`) {
		t.Errorf("apostrophe not quoted:\n%s", out.String())
	}
}

func TestCompletionRejectsUnknownShell(t *testing.T) {
	if err := WriteCompletion(&strings.Builder{}, "fish", "demo-cmd", completionFlags(), demoSpec); err == nil {
		t.Error("fish was accepted")
	}
}
