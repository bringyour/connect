package connect

import (
	"context"
	"net"
)

type WebRtcSettings struct {
	SendBufferSize ByteCount
	sTUNServers    []string
}

func DefaultWebRtcSettings() *WebRtcSettings {
	return &WebRtcSettings{
		// FIXME
		SendBufferSize: mib(1),
		sTUNServers:    []string{"stun.l.google.com:19302"},
	}
}

type WebRtcManager struct {
	ctx            context.Context
	client         *Client
	webRtcSettings *WebRtcSettings
}

func NewWebRtcManager(
	ctx context.Context,
	client *Client,
	webRtcSettings *WebRtcSettings,
) *WebRtcManager {
	return &WebRtcManager{
		ctx:            ctx,
		webRtcSettings: webRtcSettings,
	}
}

// this should return an active, tested connection
func (self *WebRtcManager) NewP2pConnActive(ctx context.Context, _, streamID Id) (net.Conn, error) {
	return self.client.webrtcConnProvider.WebRTCOffer(ctx, streamID)
}

// this should return an active, tested connection
func (self *WebRtcManager) NewP2pConnPassive(ctx context.Context, peerId, streamID Id) (net.Conn, error) {
	return self.client.webrtcConnProvider.WebRTCAnswer(ctx, peerId, streamID)
}
