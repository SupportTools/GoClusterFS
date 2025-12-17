// Package broadcast provides UDP-based cache invalidation messaging
// for near-instant cross-node cache coherency in GoClusterFS.
//
// The broadcast system is an optimization layer on top of the storage-based
// generation counter validation. It reduces the staleness window from TTL
// (typically 5s) to network latency (typically <100ms).
//
// Design principles:
//   - Fire-and-forget: Messages are best-effort; correctness comes from generation counters
//   - Multicast first: Efficient for clusters, with unicast fallback
//   - Deduplication: Sequence numbers prevent duplicate processing
//   - Optional security: HMAC authentication for untrusted networks
package broadcast

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// Protocol constants
const (
	// Magic identifies GoClusterFS broadcast messages ("GCFS")
	Magic uint32 = 0x47434653

	// ProtocolVersion is the current protocol version
	ProtocolVersion uint8 = 1

	// MaxPayloadSize limits message payload to prevent amplification attacks
	MaxPayloadSize = 1024

	// MaxMessageSize is the maximum total message size
	MaxMessageSize = 1200

	// HMACSize is the size of the optional HMAC authentication tag
	HMACSize = 32
)

// MessageType identifies the type of broadcast message.
type MessageType uint8

const (
	// TypeInvalidateInode requests peers to invalidate a cached inode.
	TypeInvalidateInode MessageType = 1

	// TypeInvalidateExtent requests peers to invalidate cached extents for an inode.
	TypeInvalidateExtent MessageType = 2

	// TypeInvalidateAll requests peers to clear all caches (used on network recovery).
	TypeInvalidateAll MessageType = 3

	// TypeHeartbeat is a periodic message to detect network partitions.
	TypeHeartbeat MessageType = 4

	// TypePeerAnnounce announces a node's presence and IP address.
	TypePeerAnnounce MessageType = 5
)

// String returns a human-readable message type name.
func (t MessageType) String() string {
	switch t {
	case TypeInvalidateInode:
		return "InvalidateInode"
	case TypeInvalidateExtent:
		return "InvalidateExtent"
	case TypeInvalidateAll:
		return "InvalidateAll"
	case TypeHeartbeat:
		return "Heartbeat"
	case TypePeerAnnounce:
		return "PeerAnnounce"
	default:
		return fmt.Sprintf("Unknown(%d)", t)
	}
}

// Message represents a broadcast message.
//
// Wire format (big-endian):
//
//	Offset  Size  Field
//	0       4     Magic (0x47434653 "GCFS")
//	4       1     Version
//	5       1     Type
//	6       1     Flags
//	7       1     Reserved
//	8       16    SenderUUID
//	24      8     Sequence
//	32      8     Timestamp (Unix nanoseconds)
//	40      2     PayloadLen
//	42      N     Payload (variable, max 1024 bytes)
//	42+N    32    HMAC (optional, if Flags & FlagHMAC)
type Message struct {
	Version    uint8
	Type       MessageType
	Flags      uint8
	SenderUUID [16]byte
	Sequence   uint64
	Timestamp  int64 // Unix nanoseconds
	Payload    []byte
}

// Message flags
const (
	// FlagHMAC indicates the message includes an HMAC authentication tag.
	FlagHMAC uint8 = 1 << 0

	// FlagUrgent indicates high-priority invalidation (e.g., after write).
	FlagUrgent uint8 = 1 << 1
)

// Header size without payload or HMAC
const headerSize = 42

// Errors
var (
	ErrInvalidMagic    = errors.New("invalid message magic")
	ErrInvalidVersion  = errors.New("unsupported protocol version")
	ErrMessageTooShort = errors.New("message too short")
	ErrMessageTooLong  = errors.New("message too long")
	ErrPayloadTooLarge = errors.New("payload too large")
	ErrInvalidHMAC     = errors.New("HMAC verification failed")
)

// Marshal serializes a message to wire format.
// If secret is non-empty, HMAC authentication is added.
func (m *Message) Marshal(secret []byte) ([]byte, error) {
	if len(m.Payload) > MaxPayloadSize {
		return nil, ErrPayloadTooLarge
	}

	hasHMAC := len(secret) > 0
	flags := m.Flags
	if hasHMAC {
		flags |= FlagHMAC
	}

	size := headerSize + len(m.Payload)
	if hasHMAC {
		size += HMACSize
	}

	if size > MaxMessageSize {
		return nil, ErrMessageTooLong
	}

	buf := make([]byte, size)

	// Header
	binary.BigEndian.PutUint32(buf[0:4], Magic)
	buf[4] = m.Version
	buf[5] = uint8(m.Type)
	buf[6] = flags
	buf[7] = 0 // Reserved
	copy(buf[8:24], m.SenderUUID[:])
	binary.BigEndian.PutUint64(buf[24:32], m.Sequence)
	binary.BigEndian.PutUint64(buf[32:40], uint64(m.Timestamp))
	binary.BigEndian.PutUint16(buf[40:42], uint16(len(m.Payload)))

	// Payload
	copy(buf[headerSize:], m.Payload)

	// HMAC (computed over everything before the HMAC field)
	if hasHMAC {
		mac := hmac.New(sha256.New, secret)
		mac.Write(buf[:headerSize+len(m.Payload)])
		copy(buf[headerSize+len(m.Payload):], mac.Sum(nil))
	}

	return buf, nil
}

// Unmarshal deserializes a message from wire format.
// If secret is non-empty, HMAC is verified.
func Unmarshal(data []byte, secret []byte) (*Message, error) {
	if len(data) < headerSize {
		return nil, ErrMessageTooShort
	}

	// Verify magic
	magic := binary.BigEndian.Uint32(data[0:4])
	if magic != Magic {
		return nil, ErrInvalidMagic
	}

	// Check version
	version := data[4]
	if version != ProtocolVersion {
		return nil, ErrInvalidVersion
	}

	flags := data[6]
	hasHMAC := flags&FlagHMAC != 0

	payloadLen := int(binary.BigEndian.Uint16(data[40:42]))
	if payloadLen > MaxPayloadSize {
		return nil, ErrPayloadTooLarge
	}

	expectedSize := headerSize + payloadLen
	if hasHMAC {
		expectedSize += HMACSize
	}

	if len(data) < expectedSize {
		return nil, ErrMessageTooShort
	}

	// Verify HMAC if present and secret provided
	if hasHMAC {
		if len(secret) == 0 {
			// Message has HMAC but we don't have a secret - reject
			return nil, ErrInvalidHMAC
		}

		mac := hmac.New(sha256.New, secret)
		mac.Write(data[:headerSize+payloadLen])
		expectedMAC := mac.Sum(nil)
		actualMAC := data[headerSize+payloadLen : headerSize+payloadLen+HMACSize]

		if !hmac.Equal(expectedMAC, actualMAC) {
			return nil, ErrInvalidHMAC
		}
	} else if len(secret) > 0 {
		// We require HMAC but message doesn't have it
		return nil, ErrInvalidHMAC
	}

	m := &Message{
		Version:   version,
		Type:      MessageType(data[5]),
		Flags:     flags,
		Sequence:  binary.BigEndian.Uint64(data[24:32]),
		Timestamp: int64(binary.BigEndian.Uint64(data[32:40])),
	}
	copy(m.SenderUUID[:], data[8:24])

	if payloadLen > 0 {
		m.Payload = make([]byte, payloadLen)
		copy(m.Payload, data[headerSize:headerSize+payloadLen])
	}

	return m, nil
}

// InvalidateInodePayload is the payload for TypeInvalidateInode messages.
type InvalidateInodePayload struct {
	InodeNum       uint32
	DataGeneration uint64
	MetaGeneration uint64
}

// Marshal serializes the payload.
func (p *InvalidateInodePayload) Marshal() []byte {
	buf := make([]byte, 20)
	binary.BigEndian.PutUint32(buf[0:4], p.InodeNum)
	binary.BigEndian.PutUint64(buf[4:12], p.DataGeneration)
	binary.BigEndian.PutUint64(buf[12:20], p.MetaGeneration)
	return buf
}

// UnmarshalInvalidateInodePayload deserializes the payload.
func UnmarshalInvalidateInodePayload(data []byte) (*InvalidateInodePayload, error) {
	if len(data) < 20 {
		return nil, ErrMessageTooShort
	}
	return &InvalidateInodePayload{
		InodeNum:       binary.BigEndian.Uint32(data[0:4]),
		DataGeneration: binary.BigEndian.Uint64(data[4:12]),
		MetaGeneration: binary.BigEndian.Uint64(data[12:20]),
	}, nil
}

// InvalidateExtentPayload is the payload for TypeInvalidateExtent messages.
type InvalidateExtentPayload struct {
	InodeNum       uint32
	DataGeneration uint64
}

// Marshal serializes the payload.
func (p *InvalidateExtentPayload) Marshal() []byte {
	buf := make([]byte, 12)
	binary.BigEndian.PutUint32(buf[0:4], p.InodeNum)
	binary.BigEndian.PutUint64(buf[4:12], p.DataGeneration)
	return buf
}

// UnmarshalInvalidateExtentPayload deserializes the payload.
func UnmarshalInvalidateExtentPayload(data []byte) (*InvalidateExtentPayload, error) {
	if len(data) < 12 {
		return nil, ErrMessageTooShort
	}
	return &InvalidateExtentPayload{
		InodeNum:       binary.BigEndian.Uint32(data[0:4]),
		DataGeneration: binary.BigEndian.Uint64(data[4:12]),
	}, nil
}

// PeerAnnouncePayload is the payload for TypePeerAnnounce messages.
type PeerAnnouncePayload struct {
	NodeSlot uint32
	Port     uint16
	IPAddr   [16]byte // IPv6 or IPv4-mapped-IPv6
}

// Marshal serializes the payload.
func (p *PeerAnnouncePayload) Marshal() []byte {
	buf := make([]byte, 22)
	binary.BigEndian.PutUint32(buf[0:4], p.NodeSlot)
	binary.BigEndian.PutUint16(buf[4:6], p.Port)
	copy(buf[6:22], p.IPAddr[:])
	return buf
}

// UnmarshalPeerAnnouncePayload deserializes the payload.
func UnmarshalPeerAnnouncePayload(data []byte) (*PeerAnnouncePayload, error) {
	if len(data) < 22 {
		return nil, ErrMessageTooShort
	}
	p := &PeerAnnouncePayload{
		NodeSlot: binary.BigEndian.Uint32(data[0:4]),
		Port:     binary.BigEndian.Uint16(data[4:6]),
	}
	copy(p.IPAddr[:], data[6:22])
	return p, nil
}

// SequenceGenerator generates unique sequence numbers for messages.
type SequenceGenerator struct {
	counter uint64
}

// NewSequenceGenerator creates a new sequence generator.
func NewSequenceGenerator() *SequenceGenerator {
	return &SequenceGenerator{
		// Start with current time to avoid collisions after restart
		counter: uint64(time.Now().UnixNano()),
	}
}

// Next returns the next sequence number.
func (g *SequenceGenerator) Next() uint64 {
	return atomic.AddUint64(&g.counter, 1)
}

// MessageBuilder helps construct broadcast messages.
type MessageBuilder struct {
	senderUUID [16]byte
	seqGen     *SequenceGenerator
}

// NewMessageBuilder creates a new message builder.
func NewMessageBuilder(senderUUID [16]byte) *MessageBuilder {
	return &MessageBuilder{
		senderUUID: senderUUID,
		seqGen:     NewSequenceGenerator(),
	}
}

// InvalidateInode creates an inode invalidation message.
func (b *MessageBuilder) InvalidateInode(inodeNum uint32, dataGen, metaGen uint64) *Message {
	payload := &InvalidateInodePayload{
		InodeNum:       inodeNum,
		DataGeneration: dataGen,
		MetaGeneration: metaGen,
	}
	return &Message{
		Version:    ProtocolVersion,
		Type:       TypeInvalidateInode,
		Flags:      FlagUrgent,
		SenderUUID: b.senderUUID,
		Sequence:   b.seqGen.Next(),
		Timestamp:  time.Now().UnixNano(),
		Payload:    payload.Marshal(),
	}
}

// InvalidateExtent creates an extent invalidation message.
func (b *MessageBuilder) InvalidateExtent(inodeNum uint32, dataGen uint64) *Message {
	payload := &InvalidateExtentPayload{
		InodeNum:       inodeNum,
		DataGeneration: dataGen,
	}
	return &Message{
		Version:    ProtocolVersion,
		Type:       TypeInvalidateExtent,
		Flags:      FlagUrgent,
		SenderUUID: b.senderUUID,
		Sequence:   b.seqGen.Next(),
		Timestamp:  time.Now().UnixNano(),
		Payload:    payload.Marshal(),
	}
}

// InvalidateAll creates a full cache invalidation message.
func (b *MessageBuilder) InvalidateAll() *Message {
	return &Message{
		Version:    ProtocolVersion,
		Type:       TypeInvalidateAll,
		Flags:      FlagUrgent,
		SenderUUID: b.senderUUID,
		Sequence:   b.seqGen.Next(),
		Timestamp:  time.Now().UnixNano(),
	}
}

// Heartbeat creates a heartbeat message.
func (b *MessageBuilder) Heartbeat() *Message {
	return &Message{
		Version:    ProtocolVersion,
		Type:       TypeHeartbeat,
		SenderUUID: b.senderUUID,
		Sequence:   b.seqGen.Next(),
		Timestamp:  time.Now().UnixNano(),
	}
}

// PeerAnnounce creates a peer announcement message.
func (b *MessageBuilder) PeerAnnounce(nodeSlot uint32, port uint16, ipAddr [16]byte) *Message {
	payload := &PeerAnnouncePayload{
		NodeSlot: nodeSlot,
		Port:     port,
		IPAddr:   ipAddr,
	}
	return &Message{
		Version:    ProtocolVersion,
		Type:       TypePeerAnnounce,
		SenderUUID: b.senderUUID,
		Sequence:   b.seqGen.Next(),
		Timestamp:  time.Now().UnixNano(),
		Payload:    payload.Marshal(),
	}
}
