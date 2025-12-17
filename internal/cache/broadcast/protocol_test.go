package broadcast

import (
	"bytes"
	"testing"
	"time"
)

func TestMessageMarshalUnmarshal(t *testing.T) {
	tests := []struct {
		name    string
		msg     *Message
		secret  []byte
		wantErr bool
	}{
		{
			name: "basic message without HMAC",
			msg: &Message{
				Version:    ProtocolVersion,
				Type:       TypeInvalidateInode,
				Flags:      0,
				SenderUUID: [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
				Sequence:   12345,
				Timestamp:  time.Now().UnixNano(),
				Payload:    []byte{0, 0, 0, 42, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2},
			},
			secret:  nil,
			wantErr: false,
		},
		{
			name: "message with HMAC",
			msg: &Message{
				Version:    ProtocolVersion,
				Type:       TypeInvalidateExtent,
				Flags:      0,
				SenderUUID: [16]byte{0xAA, 0xBB, 0xCC, 0xDD},
				Sequence:   99999,
				Timestamp:  time.Now().UnixNano(),
				Payload:    []byte{0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 5},
			},
			secret:  []byte("test-secret-key"),
			wantErr: false,
		},
		{
			name: "heartbeat without payload",
			msg: &Message{
				Version:    ProtocolVersion,
				Type:       TypeHeartbeat,
				Flags:      0,
				SenderUUID: [16]byte{0xFF},
				Sequence:   1,
				Timestamp:  time.Now().UnixNano(),
				Payload:    nil,
			},
			secret:  nil,
			wantErr: false,
		},
		{
			name: "urgent flag",
			msg: &Message{
				Version:    ProtocolVersion,
				Type:       TypeInvalidateAll,
				Flags:      FlagUrgent,
				SenderUUID: [16]byte{},
				Sequence:   555,
				Timestamp:  time.Now().UnixNano(),
			},
			secret:  nil,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal
			data, err := tt.msg.Marshal(tt.secret)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Marshal() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			// Unmarshal
			got, err := Unmarshal(data, tt.secret)
			if err != nil {
				t.Fatalf("Unmarshal() error = %v", err)
			}

			// Compare
			if got.Version != tt.msg.Version {
				t.Errorf("Version = %v, want %v", got.Version, tt.msg.Version)
			}
			if got.Type != tt.msg.Type {
				t.Errorf("Type = %v, want %v", got.Type, tt.msg.Type)
			}
			if got.SenderUUID != tt.msg.SenderUUID {
				t.Errorf("SenderUUID = %v, want %v", got.SenderUUID, tt.msg.SenderUUID)
			}
			if got.Sequence != tt.msg.Sequence {
				t.Errorf("Sequence = %v, want %v", got.Sequence, tt.msg.Sequence)
			}
			if got.Timestamp != tt.msg.Timestamp {
				t.Errorf("Timestamp = %v, want %v", got.Timestamp, tt.msg.Timestamp)
			}
			if !bytes.Equal(got.Payload, tt.msg.Payload) {
				t.Errorf("Payload = %v, want %v", got.Payload, tt.msg.Payload)
			}
		})
	}
}

func TestMessageMarshalErrors(t *testing.T) {
	// Test payload too large
	msg := &Message{
		Version: ProtocolVersion,
		Type:    TypeHeartbeat,
		Payload: make([]byte, MaxPayloadSize+1),
	}
	_, err := msg.Marshal(nil)
	if err != ErrPayloadTooLarge {
		t.Errorf("Expected ErrPayloadTooLarge, got %v", err)
	}
}

func TestUnmarshalErrors(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		secret  []byte
		wantErr error
	}{
		{
			name:    "message too short",
			data:    make([]byte, 10),
			wantErr: ErrMessageTooShort,
		},
		{
			name: "invalid magic",
			data: func() []byte {
				data := make([]byte, headerSize)
				// Wrong magic
				data[0], data[1], data[2], data[3] = 0, 0, 0, 0
				return data
			}(),
			wantErr: ErrInvalidMagic,
		},
		{
			name: "invalid version",
			data: func() []byte {
				data := make([]byte, headerSize)
				// Correct magic
				data[0], data[1], data[2], data[3] = 0x47, 0x43, 0x46, 0x53 // GCFS
				// Wrong version
				data[4] = 99
				return data
			}(),
			wantErr: ErrInvalidVersion,
		},
		{
			name: "HMAC required but missing",
			data: func() []byte {
				msg := &Message{
					Version:    ProtocolVersion,
					Type:       TypeHeartbeat,
					SenderUUID: [16]byte{1},
					Sequence:   1,
					Timestamp:  time.Now().UnixNano(),
				}
				data, _ := msg.Marshal(nil) // No HMAC
				return data
			}(),
			secret:  []byte("secret"), // We require HMAC
			wantErr: ErrInvalidHMAC,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Unmarshal(tt.data, tt.secret)
			if err != tt.wantErr {
				t.Errorf("Unmarshal() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHMACValidation(t *testing.T) {
	msg := &Message{
		Version:    ProtocolVersion,
		Type:       TypeInvalidateInode,
		SenderUUID: [16]byte{0xAB, 0xCD},
		Sequence:   12345,
		Timestamp:  time.Now().UnixNano(),
		Payload:    []byte{1, 2, 3, 4},
	}

	secret := []byte("correct-secret")
	data, err := msg.Marshal(secret)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// Verify with correct secret
	_, err = Unmarshal(data, secret)
	if err != nil {
		t.Errorf("Unmarshal with correct secret failed: %v", err)
	}

	// Verify with wrong secret
	_, err = Unmarshal(data, []byte("wrong-secret"))
	if err != ErrInvalidHMAC {
		t.Errorf("Expected ErrInvalidHMAC with wrong secret, got %v", err)
	}

	// Tamper with data
	tamperedData := make([]byte, len(data))
	copy(tamperedData, data)
	tamperedData[10] ^= 0xFF // Flip some bits
	_, err = Unmarshal(tamperedData, secret)
	if err != ErrInvalidHMAC {
		t.Errorf("Expected ErrInvalidHMAC with tampered data, got %v", err)
	}
}

func TestInvalidateInodePayload(t *testing.T) {
	original := &InvalidateInodePayload{
		InodeNum:       42,
		DataGeneration: 100,
		MetaGeneration: 200,
	}

	data := original.Marshal()
	if len(data) != 20 {
		t.Errorf("Expected payload size 20, got %d", len(data))
	}

	restored, err := UnmarshalInvalidateInodePayload(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if restored.InodeNum != original.InodeNum {
		t.Errorf("InodeNum = %d, want %d", restored.InodeNum, original.InodeNum)
	}
	if restored.DataGeneration != original.DataGeneration {
		t.Errorf("DataGeneration = %d, want %d", restored.DataGeneration, original.DataGeneration)
	}
	if restored.MetaGeneration != original.MetaGeneration {
		t.Errorf("MetaGeneration = %d, want %d", restored.MetaGeneration, original.MetaGeneration)
	}
}

func TestInvalidateExtentPayload(t *testing.T) {
	original := &InvalidateExtentPayload{
		InodeNum:       999,
		DataGeneration: 555,
	}

	data := original.Marshal()
	if len(data) != 12 {
		t.Errorf("Expected payload size 12, got %d", len(data))
	}

	restored, err := UnmarshalInvalidateExtentPayload(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if restored.InodeNum != original.InodeNum {
		t.Errorf("InodeNum = %d, want %d", restored.InodeNum, original.InodeNum)
	}
	if restored.DataGeneration != original.DataGeneration {
		t.Errorf("DataGeneration = %d, want %d", restored.DataGeneration, original.DataGeneration)
	}
}

func TestPeerAnnouncePayload(t *testing.T) {
	original := &PeerAnnouncePayload{
		NodeSlot: 3,
		Port:     31337,
		IPAddr:   [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF, 192, 168, 1, 100},
	}

	data := original.Marshal()
	if len(data) != 22 {
		t.Errorf("Expected payload size 22, got %d", len(data))
	}

	restored, err := UnmarshalPeerAnnouncePayload(data)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if restored.NodeSlot != original.NodeSlot {
		t.Errorf("NodeSlot = %d, want %d", restored.NodeSlot, original.NodeSlot)
	}
	if restored.Port != original.Port {
		t.Errorf("Port = %d, want %d", restored.Port, original.Port)
	}
	if restored.IPAddr != original.IPAddr {
		t.Errorf("IPAddr = %v, want %v", restored.IPAddr, original.IPAddr)
	}
}

func TestSequenceGenerator(t *testing.T) {
	gen := NewSequenceGenerator()

	// Sequences should be unique and increasing
	seq1 := gen.Next()
	seq2 := gen.Next()
	seq3 := gen.Next()

	if seq2 <= seq1 {
		t.Errorf("seq2 (%d) should be > seq1 (%d)", seq2, seq1)
	}
	if seq3 <= seq2 {
		t.Errorf("seq3 (%d) should be > seq2 (%d)", seq3, seq2)
	}
}

func TestMessageBuilder(t *testing.T) {
	uuid := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	builder := NewMessageBuilder(uuid)

	t.Run("InvalidateInode", func(t *testing.T) {
		msg := builder.InvalidateInode(42, 100, 200)

		if msg.Version != ProtocolVersion {
			t.Errorf("Version = %d, want %d", msg.Version, ProtocolVersion)
		}
		if msg.Type != TypeInvalidateInode {
			t.Errorf("Type = %v, want %v", msg.Type, TypeInvalidateInode)
		}
		if msg.SenderUUID != uuid {
			t.Errorf("SenderUUID mismatch")
		}
		if msg.Flags&FlagUrgent == 0 {
			t.Error("Expected FlagUrgent to be set")
		}

		payload, err := UnmarshalInvalidateInodePayload(msg.Payload)
		if err != nil {
			t.Fatalf("Failed to unmarshal payload: %v", err)
		}
		if payload.InodeNum != 42 {
			t.Errorf("InodeNum = %d, want 42", payload.InodeNum)
		}
	})

	t.Run("InvalidateExtent", func(t *testing.T) {
		msg := builder.InvalidateExtent(100, 50)

		if msg.Type != TypeInvalidateExtent {
			t.Errorf("Type = %v, want %v", msg.Type, TypeInvalidateExtent)
		}

		payload, err := UnmarshalInvalidateExtentPayload(msg.Payload)
		if err != nil {
			t.Fatalf("Failed to unmarshal payload: %v", err)
		}
		if payload.InodeNum != 100 {
			t.Errorf("InodeNum = %d, want 100", payload.InodeNum)
		}
		if payload.DataGeneration != 50 {
			t.Errorf("DataGeneration = %d, want 50", payload.DataGeneration)
		}
	})

	t.Run("InvalidateAll", func(t *testing.T) {
		msg := builder.InvalidateAll()

		if msg.Type != TypeInvalidateAll {
			t.Errorf("Type = %v, want %v", msg.Type, TypeInvalidateAll)
		}
		if len(msg.Payload) != 0 {
			t.Errorf("Expected empty payload, got %d bytes", len(msg.Payload))
		}
	})

	t.Run("Heartbeat", func(t *testing.T) {
		msg := builder.Heartbeat()

		if msg.Type != TypeHeartbeat {
			t.Errorf("Type = %v, want %v", msg.Type, TypeHeartbeat)
		}
		if msg.Flags&FlagUrgent != 0 {
			t.Error("Heartbeat should not have FlagUrgent")
		}
	})

	t.Run("PeerAnnounce", func(t *testing.T) {
		ipAddr := [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF, 10, 0, 0, 1}
		msg := builder.PeerAnnounce(5, 31337, ipAddr)

		if msg.Type != TypePeerAnnounce {
			t.Errorf("Type = %v, want %v", msg.Type, TypePeerAnnounce)
		}

		payload, err := UnmarshalPeerAnnouncePayload(msg.Payload)
		if err != nil {
			t.Fatalf("Failed to unmarshal payload: %v", err)
		}
		if payload.NodeSlot != 5 {
			t.Errorf("NodeSlot = %d, want 5", payload.NodeSlot)
		}
		if payload.Port != 31337 {
			t.Errorf("Port = %d, want 31337", payload.Port)
		}
	})

	t.Run("SequencesAreUnique", func(t *testing.T) {
		msg1 := builder.Heartbeat()
		msg2 := builder.Heartbeat()
		msg3 := builder.InvalidateInode(1, 1, 1)

		if msg1.Sequence == msg2.Sequence || msg2.Sequence == msg3.Sequence {
			t.Error("Message sequences should be unique")
		}
	})
}

func TestMessageTypeString(t *testing.T) {
	tests := []struct {
		typ  MessageType
		want string
	}{
		{TypeInvalidateInode, "InvalidateInode"},
		{TypeInvalidateExtent, "InvalidateExtent"},
		{TypeInvalidateAll, "InvalidateAll"},
		{TypeHeartbeat, "Heartbeat"},
		{TypePeerAnnounce, "PeerAnnounce"},
		{MessageType(99), "Unknown(99)"},
	}

	for _, tt := range tests {
		got := tt.typ.String()
		if got != tt.want {
			t.Errorf("MessageType(%d).String() = %q, want %q", tt.typ, got, tt.want)
		}
	}
}

func BenchmarkMessageMarshal(b *testing.B) {
	msg := &Message{
		Version:    ProtocolVersion,
		Type:       TypeInvalidateInode,
		SenderUUID: [16]byte{1, 2, 3, 4, 5, 6, 7, 8},
		Sequence:   12345,
		Timestamp:  time.Now().UnixNano(),
		Payload:    make([]byte, 20),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = msg.Marshal(nil)
	}
}

func BenchmarkMessageUnmarshal(b *testing.B) {
	msg := &Message{
		Version:    ProtocolVersion,
		Type:       TypeInvalidateInode,
		SenderUUID: [16]byte{1, 2, 3, 4, 5, 6, 7, 8},
		Sequence:   12345,
		Timestamp:  time.Now().UnixNano(),
		Payload:    make([]byte, 20),
	}
	data, _ := msg.Marshal(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Unmarshal(data, nil)
	}
}

func BenchmarkMessageMarshalWithHMAC(b *testing.B) {
	msg := &Message{
		Version:    ProtocolVersion,
		Type:       TypeInvalidateInode,
		SenderUUID: [16]byte{1, 2, 3, 4, 5, 6, 7, 8},
		Sequence:   12345,
		Timestamp:  time.Now().UnixNano(),
		Payload:    make([]byte, 20),
	}
	secret := []byte("benchmark-secret-key")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = msg.Marshal(secret)
	}
}
