package connect

import (
	"context"
	"fmt"
	"net"
	"time"

	webrtcconn "github.com/bringyour/webrtc-conn"
	"github.com/pion/webrtc/v3"
)

type WebRTCConnProvider interface {
	// WebRTCOffer creates a new WebRTC connection to the peer with the given session ID.
	// The connection is identified by the streamID+clientID.
	WebRTCOffer(ctx context.Context, streamID Id) (net.Conn, error)

	// WebRTCAnswer creates a new WebRTC connection to the peer with the given session ID.
	// The connection is identified by the streamID+peerID.
	WebRTCAnswer(ctx context.Context, peerID, streamID Id) (net.Conn, error)
}

type ApiWebRTCConnProvider struct {
	api      *BringYourApi
	clientID Id
}

func NewApiWebRTCConnProvider(
	ctx context.Context,
	clientStrategy *ClientStrategy,
	byJwt string,
	apiUrl string,
	clientID Id,
) *ApiWebRTCConnProvider {
	api := NewBringYourApiWithContext(ctx, clientStrategy, apiUrl)
	api.SetByJwt(byJwt)
	return &ApiWebRTCConnProvider{
		api: api,
	}
}

func NewApiWebRTCConnProviderWithApi(api *BringYourApi) *ApiWebRTCConnProvider {
	return &ApiWebRTCConnProvider{
		api: api,
	}
}

// WebRTC connection timeout, increase this if it leads to frequent timeouts.
const webrtcConnTimeout = 5 * time.Second

func (cp *ApiWebRTCConnProvider) WebRTCOffer(ctx context.Context, streamID Id) (net.Conn, error) {
	return webrtcconn.Offer(
		ctx,
		webrtc.Configuration{
			ICEServers: []webrtc.ICEServer{
				{
					// FIXME: use a more reliable STUN servers
					URLs: []string{"stun:stun.l.google.com:19302"},
				},
			},
		},
		NewWebRTCOfferHandshake(cp.api, cp.clientID.String()+"-"+streamID.String()),
		true,
		4,
		webrtcConnTimeout,
	)
}

func (cp *ApiWebRTCConnProvider) WebRTCAnswer(ctx context.Context, peerID, streamID Id) (net.Conn, error) {
	return webrtcconn.Answer(
		ctx,
		webrtc.Configuration{
			ICEServers: []webrtc.ICEServer{
				{
					// FIXME: use a more reliable STUN servers
					URLs: []string{"stun:stun.l.google.com:19302"},
				},
			},
		},
		NewWebRTCAnswerHandshake(cp.api, peerID.String()+"-"+streamID.String()),
		webrtcConnTimeout,
	)
}

type noOpWebRTCConnProvider struct {
}

func NewNoOpWebRTCConnProvider() *noOpWebRTCConnProvider {
	return &noOpWebRTCConnProvider{}
}

func (cp *noOpWebRTCConnProvider) WebRTCOffer(ctx context.Context, streamID Id) (net.Conn, error) {
	return nil, fmt.Errorf("WebRTCOffer not supported")
}

func (cp *noOpWebRTCConnProvider) WebRTCAnswer(ctx context.Context, peerID, streamID Id) (net.Conn, error) {
	return nil, fmt.Errorf("WebRTCAnswer not supported")
}
