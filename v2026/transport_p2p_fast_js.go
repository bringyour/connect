//go:build js

// This file keeps the native datagram carrier out of browser builds. Browser
// peers continue to negotiate and use the legacy WebRTC data channel.
package connect

// webRtcFastPath is an empty browser-build placeholder for peerConn's shared
// ownership field.
type webRtcFastPath struct{}

// configureFastPath leaves browser PeerConnections on their data channel.
func (self *peerConn) configureFastPath() error {
	return nil
}

// Browser peers never publish a native carrier.
func (self *peerConn) closeFastPath() {
	self.fastPath.Store(nil)
}

// startFastPathWarmup is a no-op in browser builds.
func (self *peerConn) startFastPathWarmup() {
}
