package connect

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	mathrandv2 "math/rand/v2"
	"sync"
	"time"
)

// comparable
type Id [16]byte

// id generation state for NewId. An Id has the same shape as a ULID: a 48-bit
// big-endian millisecond timestamp followed by 80 bits of entropy. Ids generated
// within the same millisecond are monotonically increasing — sequence ordering
// relies on this (ReceiveSequence drops/upgrades sequences by SequenceId.LessThan
// for same-source close/reopen). We generate ids directly instead of via the
// external ulid package because ulid.New passes a slice of its return array
// through the MonotonicReader interface, forcing a heap allocation on every id,
// and NewId is on the per-packet hot path. Randomness is non-crypto (math/rand),
// matching ulid's default entropy.
var idGenState struct {
	stateLock sync.Mutex
	ms        uint64
	// 80-bit monotonic entropy for the current millisecond, as hi:lo (16:64 bits)
	entropyHi uint16
	entropyLo uint64
}

// NewId returns a new monotonic, time-ordered 16-byte id. Allocation-free.
//
// The stored timestamp is clamped to be non-decreasing: if the wall clock
// stalls or steps backward (UnixMilli is not guaranteed monotonic), the id
// keeps the last timestamp and only the entropy advances. This makes ids
// strictly increasing across the whole process regardless of clock jitter —
// stronger than the external ulid, which would emit a smaller id on a backward
// clock tick and break SequenceId ordering.
func NewId() Id {
	wallMs := uint64(time.Now().UnixMilli())

	idGenState.stateLock.Lock()
	if idGenState.ms < wallMs {
		// wall clock advanced into a new millisecond: adopt it, fresh entropy.
		idGenState.ms = wallMs
		idGenState.entropyHi = uint16(mathrandv2.Uint64())
		idGenState.entropyLo = mathrandv2.Uint64()
	} else {
		// same millisecond (or the wall clock went backward): keep the timestamp
		// and bump the entropy by a random increment so the id stays strictly
		// greater than the previous id.
		inc := 1 + (mathrandv2.Uint64() & 0xffffffff)
		lo := idGenState.entropyLo + inc
		if lo < idGenState.entropyLo {
			idGenState.entropyHi += 1 // carry into the high 16 bits
		}
		idGenState.entropyLo = lo
	}
	ms := idGenState.ms
	hi := idGenState.entropyHi
	lo := idGenState.entropyLo
	idGenState.stateLock.Unlock()

	var id Id
	id[0] = byte(ms >> 40)
	id[1] = byte(ms >> 32)
	id[2] = byte(ms >> 24)
	id[3] = byte(ms >> 16)
	id[4] = byte(ms >> 8)
	id[5] = byte(ms)
	binary.BigEndian.PutUint16(id[6:8], hi)
	binary.BigEndian.PutUint64(id[8:16], lo)
	return id
}

func IdFromBytes(idBytes []byte) (Id, error) {
	if len(idBytes) != 16 {
		return Id{}, errors.New("Id must be 16 bytes")
	}
	return Id(idBytes), nil
}

func RequireIdFromBytes(idBytes []byte) Id {
	id, err := IdFromBytes(idBytes)
	if err != nil {
		panic(err)
	}
	return id
}

func ParseId(idStr string) (Id, error) {
	return parseUuid(idStr)
}

func (self Id) Bytes() []byte {
	return self[0:16]
}

func (self Id) LessThan(b Id) bool {
	return self.Cmp(b) < 0
}

func (self Id) Cmp(b Id) int {
	for i, v := range self {
		if v < b[i] {
			return -1
		}
		if b[i] < v {
			return 1
		}
	}
	return 0
}

func (self Id) String() string {
	return encodeUuid(self)
}

// MarshalJSON has a value receiver so Id fields have one canonical encoding
// whether their containing struct is passed to json.Marshal by value or by
// pointer. A pointer receiver makes a non-addressable Id silently fall back to
// encoding/json's [16]byte array representation.
func (self Id) MarshalJSON() ([]byte, error) {
	var buf [16]byte
	copy(buf[0:16], self[0:16])
	var buff bytes.Buffer
	buff.WriteByte('"')
	buff.WriteString(encodeUuid(buf))
	buff.WriteByte('"')
	b := buff.Bytes()
	return b, nil
}

func (self *Id) UnmarshalJSON(src []byte) error {
	if len(src) != 38 {
		return fmt.Errorf("invalid length for UUID: %v", len(src))
	}
	buf, err := parseUuid(string(src[1 : len(src)-1]))
	if err != nil {
		return err
	}
	*self = buf
	return nil
}

func parseUuid(src string) (dst [16]byte, err error) {
	switch len(src) {
	case 36:
		src = src[0:8] + src[9:13] + src[14:18] + src[19:23] + src[24:]
	case 32:
		// dashes already stripped, assume valid
	default:
		// assume invalid.
		return dst, fmt.Errorf("cannot parse UUID %v", src)
	}

	buf, err := hex.DecodeString(src)
	if err != nil {
		return dst, err
	}

	copy(dst[:], buf)
	return dst, err
}

func encodeUuid(src [16]byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", src[0:4], src[4:6], src[6:8], src[8:10], src[10:16])
}
