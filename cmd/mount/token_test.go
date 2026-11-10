package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadAPIToken(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string, mode os.FileMode) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(p, mode); err != nil {
			t.Fatal(err)
		}
		return p
	}
	good := write("good", "  s3cret\n", 0o600)
	t.Setenv(apiTokenEnv, "")

	if tok, src, err := loadAPIToken(good, ""); err != nil || tok != "s3cret" || src != tokenFromFile {
		t.Errorf("0600 file: %q %v %v", tok, src, err)
	}
	for _, tc := range []struct {
		name, content string
		mode          os.FileMode
		want          string
	}{
		{"group", "x", 0o640, "chmod 600"},
		{"world", "x", 0o604, "chmod 600"},
		{"empty", " \n", 0o600, "empty"},
	} {
		p := write(tc.name, tc.content, tc.mode)
		if _, _, err := loadAPIToken(p, ""); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s file: %v, want an error with %q", tc.name, err, tc.want)
		}
	}
	if _, _, err := loadAPIToken(filepath.Join(dir, "missing"), ""); err == nil {
		t.Error("missing file accepted")
	}
	if _, _, err := loadAPIToken(dir, ""); err == nil || !strings.Contains(err.Error(), "not a regular file") {
		t.Errorf("directory: %v", err)
	}

	if tok, src, err := loadAPIToken("", "from-flag"); err != nil || tok != "from-flag" || src != tokenFromFlag {
		t.Errorf("flag: %q %v %v", tok, src, err)
	}
	t.Setenv(apiTokenEnv, "from-env")
	if tok, src, err := loadAPIToken("", ""); err != nil || tok != "from-env" || src != tokenFromEnv {
		t.Errorf("env: %q %v %v", tok, src, err)
	}
	if _, _, err := loadAPIToken("", "from-flag"); err == nil || !strings.Contains(err.Error(), "only one") {
		t.Errorf("env and flag: %v, want a refusal", err)
	}
	if _, _, err := loadAPIToken(good, ""); err == nil {
		t.Error("env and file accepted together")
	}
}
