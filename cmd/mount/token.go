package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
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
	env := os.Getenv(apiTokenEnv)
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
		token, err := readTokenFile(file)
		return token, tokenFromFile, err
	case env != "":
		return env, tokenFromEnv, nil
	case flagValue != "":
		return flagValue, tokenFromFlag, nil
	default:
		return "", tokenNone, nil
	}
}

func readTokenFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("-api-token-file: %w", err)
	}
	defer f.Close()
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
	data, err := io.ReadAll(io.LimitReader(f, 4096))
	if err != nil {
		return "", fmt.Errorf("-api-token-file: %w", err)
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return "", fmt.Errorf("-api-token-file %s is empty", path)
	}
	return token, nil
}
