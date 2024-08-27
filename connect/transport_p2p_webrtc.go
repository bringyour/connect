package connect

import (
	"context"
	"fmt"
	"net"
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

type WebRtcManager struct {
	ctx context.Context

	// FIXME api

	webRtcSettings *WebRtcSettings
}

func NewWebRtcManager(ctx context.Context, webRtcSettings *WebRtcSettings) *WebRtcManager {
	return &WebRtcManager{
		ctx:            ctx,
		webRtcSettings: webRtcSettings,
	}
}

// this should return an active, tested connection
func (self *WebRtcManager) NewP2pConnActive(ctx context.Context, peerId Id, streamId Id) (net.Conn, error) {
	// FIXME
	return nil, fmt.Errorf("Not implemented.")
}

// this should return an active, tested connection
func (self *WebRtcManager) NewP2pConnPassive(ctx context.Context, peerId Id, streamId Id) (net.Conn, error) {
	// FIXME
	return nil, fmt.Errorf("Not implemented.")
}
