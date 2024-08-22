package connect

import (
	"context"
	"errors"

	"encoding/base64"

	"google.golang.org/protobuf/proto"

	"github.com/bringyour/connect/protocol"
)

// control messages for a client out of band with the client sequence
// some control messages require blocking response, but there is a potential deadlock
// when a send blocks to wait for a control receive, or vice versa, since
// all clients messages are multiplexed in the same client sequence
// and the receive/send may be blocked on the send/receive
// for example think of a remote provider setup forwarding traffic as fast as possible
// to an "echo" server with a finite buffer
type OutOfBandControl interface {
	SendControl(frames []*protocol.Frame, callback func(resultFrames []*protocol.Frame, err error))
}

type ApiOutOfBandControl struct {
	api *BringYourApi
}

func NewApiOutOfBandControl(
	ctx context.Context,
	byJwt string,
	apiUrl string,
) *ApiOutOfBandControl {
	api := NewBringYourApiWithContext(ctx, apiUrl)
	api.SetByJwt(byJwt)
	return &ApiOutOfBandControl{
		api: api,
	}
}

func NewApiOutOfBandControlWithApi(api *BringYourApi) *ApiOutOfBandControl {
	return &ApiOutOfBandControl{
		api: api,
	}
}

func (self *ApiOutOfBandControl) SendControl(
	frames []*protocol.Frame,
	callback func(resultFrames []*protocol.Frame, err error),
) {
	pack := &protocol.Pack{
		Frames: frames,
	}
	packBytes, err := proto.Marshal(pack)
	if err != nil {
		callback(nil, err)
		return
	}

	packByteStr := base64.StdEncoding.EncodeToString(packBytes)

	self.api.ConnectControl(
		&ConnectControlArgs{
			Pack: packByteStr,
		},
		NewApiCallback(func(result *ConnectControlResult, err error) {
			if err != nil {
				callback(nil, err)
				return
			}

			packBytes, err := base64.StdEncoding.DecodeString(result.Pack)
			if err != nil {
				callback(nil, err)
				return
			}

			responsePack := &protocol.Pack{}
			err = proto.Unmarshal(packBytes, responsePack)
			if err != nil {
				callback(nil, err)
				return
			}

			callback(responsePack.Frames, nil)
		}),
	)
}

type noContractClientOob struct {
}

func NewNoContractClientOob() *noContractClientOob {
	return &noContractClientOob{}
}

func (self *noContractClientOob) SendControl(frames []*protocol.Frame, callback func(resultFrames []*protocol.Frame, err error)) {
	callback(nil, errors.New("Not supported."))
}
