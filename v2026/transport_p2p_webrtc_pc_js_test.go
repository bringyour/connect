//go:build js

package connect

import "testing"

// TestWebRtcJsPeerTransportStopUsesPeerCloseOnly pins the browser teardown
// boundary. Native Pion exposes ICETransport.Stop; pion/webrtc's JS wrapper
// does not, so shared teardown must fall through to RTCPeerConnection.Close.
func TestWebRtcJsPeerTransportStopUsesPeerCloseOnly(t *testing.T) {
	if stopTransport := webRtcPeerConnectionTransportStop(nil); stopTransport != nil {
		t.Fatal("JS transport unexpectedly exposed a native ICE stop operation")
	}
}
