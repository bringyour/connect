package connect

import (
	"context"
	"errors"

	"encoding/base64"

	// "google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/v2026/protocol"
)

// control messages for a client out of band with the client sequence
// some control messages require blocking response, but there is a potential deadlock
// when a send blocks to wait for a control receive, or vice versa, since
// all clients messages are multiplexed in the same client sequence
// and the receive/send may be blocked on the send/receive
// for example think of a remote provider setup forwarding traffic as fast as possible
// to an "echo" server with a finite buffer

type OobResultFunction = func(resultFrames []*protocol.Frame, err error)

type OutOfBandControl interface {
	SendControl(frames []*protocol.Frame, callback OobResultFunction)
}

// OutOfBandControlWithCtx is an optional upgrade interface for one-shot
// control with a caller-chosen context — e.g. shutdown cleanup that must
// outlive the closed client lifecycle (closing pending contracts). Normal
// control should use `SendControl`, which stays bound to the lifecycle
// context.
type OutOfBandControlWithCtx interface {
	OutOfBandControl
	SendControlWithCtx(ctx context.Context, frames []*protocol.Frame, callback OobResultFunction)
}

type ApiOutOfBandControl struct {
	api      *BringYourApi
	ownsApi  bool
	requests *lifecycleAdmission

	// Nil test barrier exposes the exact join boundary after admission closes.
	beforeCloseWaitForTest func()
}

func NewApiOutOfBandControl(
	ctx context.Context,
	clientStrategy *ClientStrategy,
	byJwt string,
	apiUrl string,
) *ApiOutOfBandControl {
	api := NewBringYourApi(ctx, clientStrategy, apiUrl)
	api.SetByJwt(byJwt)
	return &ApiOutOfBandControl{
		api:      api,
		ownsApi:  true,
		requests: newLifecycleAdmission(),
	}
}

func NewApiOutOfBandControlWithApi(api *BringYourApi) *ApiOutOfBandControl {
	return &ApiOutOfBandControl{
		api:      api,
		requests: newLifecycleAdmission(),
	}
}

// SetByJwt updates the bearer token used by future out-of-band control
// requests. Long-lived clients call this when their renewable client JWT is
// rotated; BringYourApi provides the synchronization for concurrent sends.
func (self *ApiOutOfBandControl) SetByJwt(byJwt string) {
	self.api.SetByJwt(byJwt)
}

func (self *ApiOutOfBandControl) SendControl(
	frames []*protocol.Frame,
	callback OobResultFunction,
) {
	// bound to the api lifecycle context: keep trying as long as the
	// lifecycle is active
	self.sendControl(self.api.ConnectControl, frames, callback)
}

// SendControlWithCtx is a one-shot send on a caller-chosen context, for
// cleanup that must not be bound to the (possibly closed) lifecycle context.
// The request stays bounded by the client strategy's `RequestTimeout`.
func (self *ApiOutOfBandControl) SendControlWithCtx(
	ctx context.Context,
	frames []*protocol.Frame,
	callback OobResultFunction,
) {
	connectControl := func(connectControlArgs *ConnectControlArgs, apiCallback ConnectControlCallback) {
		self.api.ConnectControlWithCtx(ctx, connectControlArgs, apiCallback)
	}
	self.sendControl(connectControl, frames, callback)
}

func (self *ApiOutOfBandControl) sendControl(
	connectControl func(*ConnectControlArgs, ConnectControlCallback),
	frames []*protocol.Frame,
	callback OobResultFunction,
) {
	safeCallback := func(resultFrames []*protocol.Frame, err error) {
		if callback != nil {
			HandleError(func() {
				callback(resultFrames, err)
			})
		}
	}
	returnFrames := func() {
		for _, frame := range frames {
			MessagePoolReturn(frame.MessageBytes)
		}
	}
	if !self.requests.start() {
		returnFrames()
		safeCallback(nil, context.Canceled)
		return
	}

	pack := &protocol.Pack{
		Frames: frames,
	}
	packBytes, err := ProtoMarshal(pack)
	if err != nil {
		defer self.requests.finish()
		returnFrames()
		safeCallback(nil, err)
		return
	}
	encodedPack := EncodeBase64(base64.StdEncoding, packBytes)
	MessagePoolReturn(packBytes)
	returnFrames()

	connectControl(
		&ConnectControlArgs{
			Pack: encodedPack,
		},
		NewApiCallback(func(result *ConnectControlResult, err error) {
			// Request completion is published after every callback-local pooled
			// buffer has returned. CloseAndWait may therefore use completion as
			// an exact ownership barrier.
			defer self.requests.finish()
			if err != nil {
				safeCallback(nil, err)
				return
			}
			if result == nil {
				safeCallback(nil, errors.New("connect control response is absent"))
				return
			}
			if result.Error != nil {
				safeCallback(nil, errors.New("connect control rejected: "+result.Error.Message))
				return
			}

			packBytes, err := DecodeBase64(base64.StdEncoding, result.Pack)
			if err != nil {
				safeCallback(nil, err)
				return
			}
			defer MessagePoolReturn(packBytes)

			responsePack := &protocol.Pack{}
			err = ProtoUnmarshal(packBytes, responsePack)
			if err != nil {
				safeCallback(nil, err)
				return
			}

			safeCallback(responsePack.Frames, nil)
		}),
	)
}

// Close prevents later request admission. A control constructed with its own
// API also cancels API-bound requests; a wrapper around a caller-owned API
// leaves that shared API open. Caller-context cleanup requests remain bounded
// by their client-strategy timeout and are joined by CloseAndWait.
func (self *ApiOutOfBandControl) Close() {
	self.requests.close()
	if self.ownsApi {
		self.api.Close()
	}
}

// CloseAndWait closes request admission and joins every request and callback
// admitted before that boundary. An OOB callback must not call CloseAndWait
// because it would wait for its own return. It may call Close and ask an
// external owner goroutine to perform the wait.
func (self *ApiOutOfBandControl) CloseAndWait(ctx context.Context) error {
	self.Close()
	if self.beforeCloseWaitForTest != nil {
		self.beforeCloseWaitForTest()
	}
	return waitForLifecycleDone(ctx, self.requests.Done(), "api out-of-band requests")
}

type NoContractClientOob struct {
}

func NewNoContractClientOob() *NoContractClientOob {
	return &NoContractClientOob{}
}

func (self *NoContractClientOob) SendControl(frames []*protocol.Frame, callback func(resultFrames []*protocol.Frame, err error)) {
	safeCallback := func(resultFrames []*protocol.Frame, err error) {
		if callback != nil {
			HandleError(func() {
				callback(resultFrames, err)
			})
		}
	}

	// SendControl takes ownership of the frames; this oob cannot deliver them but
	// must still release the pooled bytes (mirrors ApiOutOfBandControl.sendControl)
	for _, frame := range frames {
		MessagePoolReturn(frame.MessageBytes)
	}

	safeCallback(nil, errors.New("Not supported."))
}
