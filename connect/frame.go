package connect

import (
	"errors"

	"github.com/oklog/ulid/v2"
)



func UlidFromProto(ulidBytes []byte) (ulid.ULID, error) {
	if len(ulidBytes) != 16 {
		return ulid.ULID{}, errors.New("ULID must be 16 bytes")
	}
	return ulid.ULID(ulidBytes), nil
}
