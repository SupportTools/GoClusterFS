package api

import (
	"errors"
	"testing"
)

func TestCheckExposure(t *testing.T) {
	tests := []struct {
		addr    string
		token   string
		refused bool
	}{
		{"127.0.0.1:8080", "", false},
		{"127.1.2.3:8080", "", false},
		{"localhost:8080", "", false},
		{"[::1]:8080", "", false},
		{":8080", "", true},
		{"0.0.0.0:8080", "", true},
		{"[::]:8080", "", true},
		{"10.0.0.5:8080", "", true},
		{"node1.example.com:8080", "", true},
		{":8080", "secret", false},
		{"10.0.0.5:8080", "secret", false},
	}
	for _, tt := range tests {
		cfg := DefaultConfig().WithListenAddr(tt.addr).WithAuthToken(tt.token)
		err := cfg.CheckExposure()
		if tt.refused != errors.Is(err, ErrUnauthenticatedRemote) {
			t.Errorf("CheckExposure(%q, token=%q) = %v, want refused=%v", tt.addr, tt.token, err, tt.refused)
		}
	}
}

func TestDefaultConfigPassesExposureCheck(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.CheckExposure(); err != nil {
		t.Fatalf("default config refused: %v", err)
	}
}
