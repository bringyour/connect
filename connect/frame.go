package connect

import (
	"errors"

	"github.com/oklog/ulid/v2"

	"bringyour.com/protocol"
)



func UlidFromProto(ulidBytes []byte) (ulid.ULID, error) {
	if len(ulidBytes) != 16 {
		return ulid.ULID{}, errors.New("ULID must be 16 bytes")
	}
	return ulid.ULID(ulidBytes), nil
}

func ToFrame(message any) *protocol.Frame {
	// FIXME
	return nil
}
