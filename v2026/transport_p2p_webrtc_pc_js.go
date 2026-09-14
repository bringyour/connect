//go:build js

package connect

import (
	"context"
	"fmt"

	"github.com/pion/datachannel"
	"github.com/pion/webrtc/v4"
)

func newWebRtcPeerConnectionFactory(
	settings *WebRtcSettings,
	certificate *webrtc.Certificate,
) (*webRtcPeerConnectionFactory, *webrtc.Certificate, error) {
	// The js/wasm SettingEngine wraps the browser's native WebRTC and exposes
	// none of the tuning the non-js build uses (LoggerFactory, SCTP/MTU/ICE
	// timeouts). Setting any of those here does not compile for GOOS=js.
	s := webrtc.SettingEngine{}

	api := webrtc.NewAPI(webrtc.WithSettingEngine(s))
	configuration := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{
				URLs: settings.IceServerUrls,
			},
		},
	}
	return &webRtcPeerConnectionFactory{
		// The js/wasm SettingEngine cannot size the SCTP receive window, so the
		// network-peer window (Fix 1) is a no-op here; accept and ignore the flag
		// to satisfy the shared factory signature.
		newPeerConnection: func(
			networkPeer bool,
		) (*webrtc.PeerConnection, context.CancelFunc, error) {
			pc, err := api.NewPeerConnection(configuration)
			return pc, func() {}, err
		},
	}, certificate, nil
}

// Browsers do not expose aggregate association in-flight bytes. The JS
// transport is callback-based and currently cannot detach a data channel into
// this net.Conn implementation, so disable the native association watchdog
// rather than pretending browser DataChannel.bufferedAmount has equivalent
// semantics.
func webRtcSctpBufferedAmount(*webrtc.PeerConnection) (bufferedAmount int, ok bool) {
	return 0, false
}

func webRtcSctpReceiverWindow(
	*webrtc.PeerConnection,
) (receiverWindow uint32, ok bool) {
	return 0, false
}

// Browser WebRTC owns ICE beneath RTCPeerConnection and exposes no equivalent
// to native Pion's ICETransport.Stop. PeerConnection.Close is the only
// available teardown boundary, so the shared close helper skips the
// native-only pre-stop on JS.
func webRtcPeerConnectionTransportStop(
	*webrtc.PeerConnection,
) func() error {
	return nil
}

func detachWithDeadline(dc *webrtc.DataChannel) (datachannel.ReadWriteCloserDeadliner, error) {
	// FIXME translate from callbacks to a net.Conn
	return nil, fmt.Errorf("Not yet supported")
}
