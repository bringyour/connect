package connect

import (
	"context"
)


type WebRtcSettings struct {
	SendBufferSize ByteCount
}

func DefaultWebRtcSettings() *WebRtcSettings {
	return &WebRtcSettings{
		// FIXME
		SendBufferSize: mib(1),
	}
}


type WebRtcMananager struct {
	ctx context.Context

	// FIXME api

	webRtcSettings *WebRtcSettings
}


// this should return an active, tested connection
func (self *WebRtcMananager) NewP2pConnActive(ctx context.Context, peerId Id, streamId Id) (net.Conn, error) {
	// FIXME
	return nil, fmt.Errorf("Not implemented.")
}


// this should return an active, tested connection
func (self *WebRtcMananager) NewP2pConnPassive(ctx context.Context, peerId Id, streamId Id) (net.Conn, error) {
	// FIXME
	return nil, fmt.Errorf("Not implemented.")
}

