package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
)

const apiTokenEnv = "GOCLUSTERFS_API_TOKEN"

// tokenSource says where the API token came from.
type tokenSource int

const (
	tokenNone tokenSource = iota
	tokenFromFile
	tokenFromEnv
	tokenFromFlag
)

// loadAPIToken returns the API token from at most one of -api-token-file,
// GOCLUSTERFS_API_TOKEN and -api-token. A token file must not be readable by
// group or others: the token lets its holder change files as the mount's user.
func loadAPIToken(file, flagValue string) (string, tokenSource, error) {
	raw := os.Getenv(apiTokenEnv)
	env := strings.TrimSpace(raw)
	if raw != "" && env == "" {
		return "", tokenNone, errors.New(apiTokenEnv + " is only whitespace")
	}
	set := 0
	for _, v := range []string{file, env, flagValue} {
		if v != "" {
			set++
		}
	}
	if set > 1 {
		return "", tokenNone, errors.New("set only one of -api-token-file, " + apiTokenEnv + " and -api-token")
	}

	switch {
	case file != "":
		token, err := readTokenFile(file, os.Geteuid())
		return token, tokenFromFile, err
	case env != "":
		return env, tokenFromEnv, nil
	case flagValue != "":
		return flagValue, tokenFromFlag, nil
	default:
		return "", tokenNone, nil
	}
}

const maxTokenFile = 4096

// readTokenFile reads a token file that only uid (or root) owns and can read.
// O_NONBLOCK keeps a FIFO at that path from blocking startup.
func readTokenFile(path string, uid int) (string, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return "", fmt.Errorf("-api-token-file: %w", err)
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return "", fmt.Errorf("-api-token-file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("-api-token-file %s is not a regular file", path)
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		return "", fmt.Errorf("-api-token-file %s has mode %04o, readable by group or others; run chmod 600 %s", path, perm, path)
	}
	if st, ok := info.Sys().(*syscall.Stat_t); ok && int(st.Uid) != uid && st.Uid != 0 {
		return "", fmt.Errorf("-api-token-file %s is owned by uid %d; it must belong to uid %d or root", path, st.Uid, uid)
	}
	data, err := io.ReadAll(io.LimitReader(f, maxTokenFile+1))
	if err != nil {
		return "", fmt.Errorf("-api-token-file: %w", err)
	}
	if len(data) > maxTokenFile {
		return "", fmt.Errorf("-api-token-file %s is longer than %d bytes", path, maxTokenFile)
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return "", fmt.Errorf("-api-token-file %s is empty", path)
	}
	return token, nil
}
