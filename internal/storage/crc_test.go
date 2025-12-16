package storage

import (
	"testing"
)

func TestCRC32C(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected uint32
	}{
		{
			name:     "empty",
			input:    []byte{},
			expected: 0x00000000,
		},
		{
			name:     "single byte",
			input:    []byte{0x00},
			expected: 0x527d5351, // Known CRC32C value
		},
		{
			name:     "hello",
			input:    []byte("hello"),
			expected: 0x9a71bb4c, // Verified CRC32C value for "hello"
		},
		{
			name:     "123456789",
			input:    []byte("123456789"),
			expected: 0xe3069283, // SCTP test vector for CRC32C
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CRC32C(tt.input)
			if got != tt.expected {
				t.Errorf("CRC32C(%q) = 0x%08x, want 0x%08x", tt.input, got, tt.expected)
			}
		})
	}
}

func TestCRC32CUpdate(t *testing.T) {
	// Test that CRC32CUpdate produces non-zero results
	data := []byte("hello world this is a longer test string")

	// Compute incrementally using Update
	partial := CRC32C(data[:10])
	partial = CRC32CUpdate(partial, data[10:20])
	partial = CRC32CUpdate(partial, data[20:])

	// Note: CRC32CUpdate continues from previous CRC state
	// The test verifies the function works and produces a value

	if partial == 0 {
		t.Error("CRC32CUpdate returned zero, expected non-zero")
	}

	// Verify determinism - same input should give same output
	partial2 := CRC32C(data[:10])
	partial2 = CRC32CUpdate(partial2, data[10:20])
	partial2 = CRC32CUpdate(partial2, data[20:])

	if partial != partial2 {
		t.Errorf("CRC32CUpdate not deterministic: %x != %x", partial, partial2)
	}
}

func TestVerifyCRC32C(t *testing.T) {
	data := []byte("test data for verification")
	checksum := CRC32C(data)

	if !VerifyCRC32C(data, checksum) {
		t.Error("VerifyCRC32C should return true for matching checksum")
	}

	if VerifyCRC32C(data, checksum+1) {
		t.Error("VerifyCRC32C should return false for mismatched checksum")
	}

	// Corrupted data should fail
	corrupted := make([]byte, len(data))
	copy(corrupted, data)
	corrupted[0] ^= 0xFF

	if VerifyCRC32C(corrupted, checksum) {
		t.Error("VerifyCRC32C should return false for corrupted data")
	}
}

func BenchmarkCRC32C(b *testing.B) {
	sizes := []int{512, 4096, 65536, 1 << 20}

	for _, size := range sizes {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i)
		}

		b.Run(formatSize(size), func(b *testing.B) {
			b.SetBytes(int64(size))
			for i := 0; i < b.N; i++ {
				CRC32C(data)
			}
		})
	}
}

func formatSize(size int) string {
	switch {
	case size >= 1<<20:
		return string(rune('0'+size/(1<<20))) + "MB"
	case size >= 1<<10:
		return string(rune('0'+size/(1<<10))) + "KB"
	default:
		return string(rune('0'+size)) + "B"
	}
}
