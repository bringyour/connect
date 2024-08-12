package connect

import (
	"errors"
	// "log"
	"fmt"
	"encoding/hex"
	"bytes"

	"github.com/oklog/ulid/v2"
)


const MaxMultihopLength = 8


// use this type when counting bytes
type ByteCount = int64

func kib(c ByteCount) ByteCount {
	return c * ByteCount(1024)
}

func mib(c ByteCount) ByteCount {
	return c * ByteCount(1024 * 1024)
}

func gib(c ByteCount) ByteCount {
	return c * ByteCount(1024 * 1024 * 1024)
}


type Id [16]byte

func NewId() Id {
	return Id(ulid.Make())
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

func (self Id) String() string {
	return encodeUuid(self)
}


func (self *Id) MarshalJSON() ([]byte, error) {
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


// comparable
type MultihopId struct {
	ids [MaxMultihopLength]Id
	len int
}

func NewMultihopId(ids ... Id) (MultihopId, error) {
	if MaxMultihopLength < len(ids) {
		return MultihopId{}, fmt.Errorf("Multihop length exceeds maximum: %d < %d", MaxMultihopLength, len(ids))
	}
	multihopId := MultihopId{
		len: len(ids),
	}
	for i, id := range ids {
		multihopId.ids[i] = id
	}
	return multihopId, nil
}

func (self MultihopId) Len() int {
	return self.len
}

func (self MultihopId) Ids() []Id {
	return self.ids[0:self.len]
}

