//go:build !js

package connect

// This file verifies that the native WebRTC factory uses, but does not own, a
// caller-supplied Pion socket network.

import (
	"strings"
	"testing"
	"time"

	"github.com/pion/logging"
	"github.com/pion/transport/v4/vnet"
	"github.com/pion/webrtc/v4"
)

// testWebRtcSeamNetwork is the injected virtual network for the family under
// test: a private CIDR carrying one static address, so a candidate that does
// not come from the injected network is recognizable in the SDP.
func testWebRtcSeamNetwork(ipVersion int) (cidr string, staticIp string, listenNetwork string, listenAddr string) {
	if ipVersion == 6 {
		return "fd77::/64", "fd77::2", "udp6", "[::]:0"
	}
	return "10.77.0.0/24", "10.77.0.2", "udp4", "0.0.0.0:0"
}

// Injected candidate enumeration takes precedence over host and loopback
// selection, while peer teardown leaves the shared network usable by its owner.
// Both families run: the seam is family-agnostic, and a v6-only virtual
// network proves the factory does not quietly fall back to host v4 selection.
func TestWebRtcPeerConnectionFactoryUsesCallerOwnedNetwork(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		testWebRtcPeerConnectionFactoryUsesCallerOwnedNetwork(t, ipVersion)
	})
}

func testWebRtcPeerConnectionFactoryUsesCallerOwnedNetwork(t *testing.T, ipVersion int) {
	cidr, staticIp, listenNetwork, listenAddr := testWebRtcSeamNetwork(ipVersion)
	router, err := vnet.NewRouter(&vnet.RouterConfig{
		CIDR:          cidr,
		MinDelay:      time.Millisecond,
		LoggerFactory: logging.NewDefaultLoggerFactory(),
	})
	if err != nil {
		t.Fatal(err)
	}
	network, err := vnet.NewNet(&vnet.NetConfig{StaticIPs: []string{staticIp}})
	if err != nil {
		t.Fatal(err)
	}
	if err := router.AddNet(network); err != nil {
		t.Fatal(err)
	}
	if err := router.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := router.Stop(); err != nil {
			t.Errorf("stop router: %v", err)
		}
	}()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.Network = network
	// If the injected network were ignored, this would restrict gathering to
	// the host loopback interfaces instead of the virtual static address.
	settings.UseLoopbackOnlyIceInterfaces = true
	settings.EnableDatagramFastPath = false
	factory, _, err := newWebRtcPeerConnectionFactory(settings, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer factory.Close()
	peerConnection, cancelResolve, err := factory.NewPeerConnection(false)
	if err != nil {
		t.Fatal(err)
	}
	defer cancelResolve()
	if _, err := peerConnection.CreateDataChannel("network-seam", nil); err != nil {
		peerConnection.Close()
		t.Fatal(err)
	}
	gathered := webrtc.GatheringCompletePromise(peerConnection)
	offer, err := peerConnection.CreateOffer(nil)
	if err != nil {
		peerConnection.Close()
		t.Fatal(err)
	}
	if err := peerConnection.SetLocalDescription(offer); err != nil {
		peerConnection.Close()
		t.Fatal(err)
	}
	select {
	case <-gathered:
	case <-time.After(5 * time.Second):
		peerConnection.Close()
		t.Fatal("virtual-network candidate gathering timed out")
	}
	localDescription := peerConnection.LocalDescription()
	if localDescription == nil || !strings.Contains(localDescription.SDP, staticIp) {
		peerConnection.Close()
		t.Fatalf("local candidates do not contain the injected address %s: %v", staticIp, localDescription)
	}
	if err := peerConnection.Close(); err != nil {
		t.Fatal(err)
	}

	packetConn, err := network.ListenPacket(listenNetwork, listenAddr)
	if err != nil {
		t.Fatalf("peer teardown closed the caller-owned network: %v", err)
	}
	if err := packetConn.Close(); err != nil {
		t.Fatal(err)
	}
}

// Production defaults do not inject a socket network.
func TestWebRtcSettingsNetworkDefaultsToHostSelection(t *testing.T) {
	if DefaultWebRtcSettings().Network != nil {
		t.Fatal("default WebRTC settings unexpectedly inject a socket network")
	}
}
