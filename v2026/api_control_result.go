// Processed control replies retain the server's application error and require
// an explicit protobuf pack, including the legitimate empty response pack.
package connect

import (
	"encoding/json"
	"errors"
)

// A missing/null pack is not a processed response. Keep the public string
// field and existing wire format; empty protobuf bytes remain valid.
func (self *ConnectControlResult) UnmarshalJSON(data []byte) error {
	var wire struct {
		Pack  *string              `json:"pack"`
		Error *ConnectControlError `json:"error"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	if wire.Pack == nil {
		return errors.New("connect control response lacks an explicit pack")
	}
	self.Pack, self.Error = *wire.Pack, wire.Error
	return nil
}
