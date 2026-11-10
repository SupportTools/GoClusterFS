package commands

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v2"
)

// AppConfig represents the configuration file structure.
type AppConfig struct {
	// Host is the default API server host.
	Host string `yaml:"host,omitempty"`

	// Port is the default API server port.
	Port int `yaml:"port,omitempty"`

	// Token is the default Bearer token for authentication.
	Token string `yaml:"token,omitempty"`

	// Output is the default output format (text, json).
	Output string `yaml:"output,omitempty"`

	// Timeout is the default request timeout.
	Timeout time.Duration `yaml:"timeout,omitempty"`
}

// configDir returns the configuration directory path.
// Uses XDG_CONFIG_HOME if set, otherwise ~/.config/gfsctl
func configDir() string {
	if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" {
		return filepath.Join(xdg, "gfsctl")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".config", "gfsctl")
}

// configPath returns the full path to the config file.
func configPath() string {
	dir := configDir()
	if dir == "" {
		return ""
	}
	return filepath.Join(dir, "config.yaml")
}

// loadConfig loads configuration from the config file.
// Returns an empty config if the file doesn't exist.
func loadConfig() (*AppConfig, error) {
	path := configPath()
	if path == "" {
		return &AppConfig{}, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &AppConfig{}, nil
		}
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg AppConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	return &cfg, nil
}

// GenerateSampleConfig returns a sample configuration file content.
func GenerateSampleConfig() string {
	return `# gfsctl configuration file
# Place this file at ~/.config/gfsctl/config.yaml
# or set XDG_CONFIG_HOME and use $XDG_CONFIG_HOME/gfsctl/config.yaml

# API server host (default: localhost)
# host: localhost

# API server port (default: 8080)
# port: 8080

# Bearer token for authentication (default: none)
# token: your-secret-token

# Output format: text or json (default: text)
# output: text

# Request timeout (default: 30s)
# timeout: 30s
`
}
