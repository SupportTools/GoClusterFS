package api

import (
	"encoding/json"
	"testing"
)

func TestHealthResponse_JSONMarshaling(t *testing.T) {
	tests := []struct {
		name     string
		resp     HealthResponse
		wantJSON string
	}{
		{
			name: "simple ok",
			resp: HealthResponse{
				Status: HealthStatusOK,
			},
			wantJSON: `{"status":"ok"}`,
		},
		{
			name: "with checks",
			resp: HealthResponse{
				Status: HealthStatusDegraded,
				Checks: map[string]ComponentHealth{
					"cache": {Status: HealthStatusOK},
					"journal": {Status: HealthStatusDegraded, Message: "low disk space"},
				},
			},
			wantJSON: "", // Complex; just verify round-trip
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.resp)
			if err != nil {
				t.Fatalf("Marshal error: %v", err)
			}

			if tt.wantJSON != "" {
				if string(data) != tt.wantJSON {
					t.Errorf("got %s, want %s", string(data), tt.wantJSON)
				}
			}

			// Verify round-trip
			var got HealthResponse
			if err := json.Unmarshal(data, &got); err != nil {
				t.Fatalf("Unmarshal error: %v", err)
			}
			if got.Status != tt.resp.Status {
				t.Errorf("Status: got %s, want %s", got.Status, tt.resp.Status)
			}
		})
	}
}

func TestStatusResponse_JSONMarshaling(t *testing.T) {
	resp := StatusResponse{
		Status:        "ok",
		Version:       "1.0.0",
		UptimeSeconds: 3600,
		MountPath:     "/mnt/test",
		Capacity: &CapacityInfo{
			TotalBytes:  1073741824, // 1GB
			UsedBytes:   536870912,  // 512MB
			FreeBytes:   536870912,
			UsedPercent: 50.0,
		},
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var got StatusResponse
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if got.Status != resp.Status {
		t.Errorf("Status: got %s, want %s", got.Status, resp.Status)
	}
	if got.UptimeSeconds != resp.UptimeSeconds {
		t.Errorf("UptimeSeconds: got %d, want %d", got.UptimeSeconds, resp.UptimeSeconds)
	}
	if got.Capacity == nil {
		t.Error("Capacity should not be nil")
	} else if got.Capacity.UsedPercent != resp.Capacity.UsedPercent {
		t.Errorf("UsedPercent: got %f, want %f", got.Capacity.UsedPercent, resp.Capacity.UsedPercent)
	}
}

func TestCacheStatsResponse_JSONMarshaling(t *testing.T) {
	resp := CacheStatsResponse{
		InodeCache: CacheHitRateInfo{
			Size:    1000,
			Hits:    9500,
			Misses:  500,
			HitRate: 0.95,
		},
		ExtentCache: CacheHitRateInfo{
			Size:    500,
			Hits:    4800,
			Misses:  200,
			HitRate: 0.96,
		},
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var got CacheStatsResponse
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if got.InodeCache.HitRate != resp.InodeCache.HitRate {
		t.Errorf("InodeCache.HitRate: got %f, want %f", got.InodeCache.HitRate, resp.InodeCache.HitRate)
	}
}

func TestJournalStatsResponse_JSONMarshaling(t *testing.T) {
	resp := JournalStatsResponse{
		State:          "active",
		HeadOffset:     1024,
		TailOffset:     512,
		AvailableSpace: 65536,
		CommitCount:    12345,
		BytesWritten:   1048576,
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var got JournalStatsResponse
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}

	if got.CommitCount != resp.CommitCount {
		t.Errorf("CommitCount: got %d, want %d", got.CommitCount, resp.CommitCount)
	}
}

func TestErrorResponse_JSONMarshaling(t *testing.T) {
	resp := ErrorResponse{
		Error:   "not found",
		Code:    404,
		Details: "resource does not exist",
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	expected := `{"error":"not found","code":404,"details":"resource does not exist"}`
	if string(data) != expected {
		t.Errorf("got %s, want %s", string(data), expected)
	}
}

func TestOmitEmpty(t *testing.T) {
	// Verify omitempty works correctly
	resp := StatusResponse{
		Status:        "ok",
		Version:       "dev",
		UptimeSeconds: 0,
		// MountPath intentionally empty
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	// Should not contain mount_path since it's empty and has omitempty
	str := string(data)
	if contains(str, "mount_path") {
		t.Errorf("empty mount_path should be omitted: %s", str)
	}
	if contains(str, "capacity") {
		t.Errorf("nil capacity should be omitted: %s", str)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsAt(s, substr))
}

func containsAt(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
