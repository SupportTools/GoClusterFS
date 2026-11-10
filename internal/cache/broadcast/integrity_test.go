package broadcast

import (
	"bytes"
	"testing"
)

func signedInvalidation(t *testing.T, secret []byte) []byte {
	t.Helper()
	msg := NewMessageBuilder([16]byte{0xAB}).InvalidateInode(42, 7, 9)
	data, err := msg.Marshal(secret)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestHMACCoversEveryByte(t *testing.T) {
	secret := []byte("cluster-secret")
	data := signedInvalidation(t, secret)

	for i := range data {
		for _, bit := range []byte{0x01, 0x80} {
			tampered := bytes.Clone(data)
			tampered[i] ^= bit
			if _, err := Unmarshal(tampered, secret); err == nil {
				t.Errorf("flipping bit %#x of byte %d was accepted", bit, i)
			}
		}
	}

	if _, err := Unmarshal(append(bytes.Clone(data), 0), secret); err == nil {
		t.Error("a trailing byte outside the HMAC was accepted")
	}

	stripped := bytes.Clone(data[:len(data)-HMACSize])
	stripped[6] &^= FlagHMAC
	if _, err := Unmarshal(stripped, secret); err == nil {
		t.Error("a message with its HMAC stripped was accepted")
	}
}

func TestTruncatedMessageRejected(t *testing.T) {
	for _, secret := range [][]byte{nil, []byte("cluster-secret")} {
		data := signedInvalidation(t, secret)
		for n := 0; n < len(data); n++ {
			if msg, err := Unmarshal(data[:n], secret); err == nil {
				t.Errorf("secret=%t: %d of %d bytes parsed as %+v", secret != nil, n, len(data), msg)
			}
		}
	}
}

// TestLargestMessageFitsOneDatagram checks that no message needs IP
// fragmentation on a 1500-byte MTU, where one lost fragment drops the message.
func TestLargestMessageFitsOneDatagram(t *testing.T) {
	const ipv6UDPPayload = 1500 - 40 - 8
	if MaxMessageSize > ipv6UDPPayload {
		t.Fatalf("MaxMessageSize %d exceeds the %d-byte UDP payload of a 1500-byte IPv6 MTU", MaxMessageSize, ipv6UDPPayload)
	}

	largest := &Message{Version: ProtocolVersion, Type: TypeInvalidateInode, Payload: make([]byte, MaxPayloadSize)}
	data, err := largest.Marshal([]byte("cluster-secret"))
	if err != nil {
		t.Fatalf("largest allowed message failed to marshal: %v", err)
	}
	if len(data) > MaxMessageSize {
		t.Errorf("largest message is %d bytes, over MaxMessageSize %d", len(data), MaxMessageSize)
	}

	tooBig := &Message{Version: ProtocolVersion, Type: TypeInvalidateInode, Payload: make([]byte, MaxPayloadSize+1)}
	if _, err := tooBig.Marshal(nil); err == nil {
		t.Error("payload over MaxPayloadSize was accepted")
	}
}

func FuzzUnmarshal(f *testing.F) {
	secret := []byte("cluster-secret")
	builder := NewMessageBuilder([16]byte{1})
	for _, msg := range []*Message{
		builder.InvalidateInode(1, 2, 3),
		builder.InvalidateExtent(4, 5),
		builder.InvalidateAll(),
		builder.Heartbeat(),
		builder.PeerAnnounce(1, 31337, [16]byte{}),
	} {
		for _, s := range [][]byte{nil, secret} {
			data, err := msg.Marshal(s)
			if err != nil {
				f.Fatal(err)
			}
			f.Add(data)
		}
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		for _, s := range [][]byte{nil, secret} {
			msg, err := Unmarshal(data, s)
			if err != nil {
				continue
			}
			again, err := msg.Marshal(s)
			if err != nil {
				t.Fatalf("accepted message does not marshal: %v", err)
			}
			if s != nil && !bytes.Equal(again, data) {
				t.Fatalf("accepted bytes differ from the authenticated message:\n got %x\nwant %x", data, again)
			}
		}
	})
}
