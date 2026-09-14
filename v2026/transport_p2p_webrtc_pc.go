//go:build !js

package connect

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/pion/datachannel"
	"github.com/pion/ice/v4"
	"github.com/pion/transport/v4/stdnet"
	"github.com/pion/webrtc/v4"
)

var iceInterfacesLogOnce sync.Once

// logIceInterfaces dumps what the Go runtime's net.Interfaces() sees for
// ICE candidate gathering. On mobile this is the ground truth for the "0 host
// candidates" failure: if the physical egress (e.g. wlan0) is missing or has no
// usable unicast address, pion gathers nothing. It is process-once as well as
// V(1)-gated: logging every interface for every churned peer connection was
// itself a setup/CPU cost and rapidly displaced useful ICE evidence.
func logIceInterfaces(log Logger) {
	if !log.V(1).Enabled() {
		return
	}
	iceInterfacesLogOnce.Do(func() {
		ifaces, err := net.Interfaces()
		if err != nil {
			log.Infof("[ice-if]net.Interfaces err = %s\n", err)
			return
		}
		for _, ifc := range ifaces {
			addrs, _ := ifc.Addrs()
			strs := make([]string, 0, len(addrs))
			for _, a := range addrs {
				strs = append(strs, a.String())
			}
			log.Infof("[ice-if]%s flags=%s addrs=[%s]\n", ifc.Name, ifc.Flags.String(), strings.Join(strs, ","))
		}
	})
}

// newWebRtcPeerConnectionFactory builds immutable manager-scoped Pion state
// once. This transport is data-channel-only, so it needs neither the default
// media codecs nor RTP interceptors. A caller-provided certificate is also
// intentionally shared across the manager's peer connections, avoiding a new
// P-256 key and X.509 certificate for every retry.
func newWebRtcPeerConnectionFactory(
	settings *WebRtcSettings,
	certificate *webrtc.Certificate,
) (*webRtcPeerConnectionFactory, *webrtc.Certificate, error) {
	s := webrtc.SettingEngine{}
	log := loggerOrDefault(settings.Log)
	s.LoggerFactory = &pionLoggerFactory{log: log}
	logIceInterfaces(log)
	selectedNet := settings.Network
	callerOwnedNet := selectedNet != nil
	if selectedNet != nil {
		// An injected network owns candidate enumeration and socket routing.
	} else if settings.UseLoopbackOnlyIceInterfaces {
		// hermetic same-host mode (tests): gather only loopback candidates so
		// the local connect cost is a couple of pairs, independent of the
		// host's interface population (see WebRtcSettings)
		s.SetIncludeLoopbackCandidate(true)
		s.SetInterfaceFilter(func(interfaceName string) bool {
			ifc, err := net.InterfaceByName(interfaceName)
			return err == nil && ifc.Flags&net.FlagLoopback != 0
		})
	} else if index4, index6 := EgressInterfaceIndex(); index4 != 0 || index6 != 0 {
		// bind ICE sockets to the physical egress interface so p2p does not
		// loop into the tunnel this process provides (R1); a no-op off
		// Windows and when no egress index is set.
		if egressNet, err := newEgressNet(log); err == nil {
			selectedNet = egressNet
		} else {
			// this branch is taken, so the iceNet fallback below is NOT: pion
			// keeps its default net, which gathers from net.Interfaces() --
			// every interface including the tun this process provides, whose
			// address it will happily offer as a host candidate. say so rather
			// than silently gathering a path that loops back to us.
			log.Infof("[egress]no egress-bound net for ice, using pion's default interface gathering (p2p may offer the tunnel's own address): %s\n", err)
		}
	} else if iceNet, ok := newIceInterfaceNet(
		log,
		settings.UseEgressOnlyIceInterfaces,
	); ok {
		// Android (API 30+) denies netlink, so pion's default net.Interfaces()
		// gathering yields zero host candidates and p2p never leaves the WAN
		// relay. Device clients also opt into the same egress-only view on
		// platforms that can enumerate: a macOS host may expose many utun,
		// bridge, AWDL, and VM addresses, and ICE checks their local×remote
		// cross-product for every peer connection. One current IPv4/IPv6 pair
		// is both the usable path and a bounded setup cost.
		// See OPTIMIZENETWORKPEER1.md §5.1.
		selectedNet = iceNet
	}
	if settings.EnableDatagramFastPath &&
		0 < settings.DatagramFastPathWriteQueueSize &&
		0 < settings.DatagramFastPathWriteBatchSize {
		if selectedNet == nil {
			standardNet, err := stdnet.NewNet()
			if err != nil {
				return nil, nil, fmt.Errorf("create WebRTC socket network: %w", err)
			}
			selectedNet = standardNet
		}
		selectedNet = newP2pUdpBatchNet(
			selectedNet,
			settings.DatagramFastPathWriteQueueSize,
			settings.DatagramFastPathWriteBatchSize,
			settings.UdpSocketBufferByteCount,
		)
	}
	s.DetachDataChannels()
	if settings.EnableSctpSnap {
		// SNAP is advertised as an optional SDP attribute. Two enabled peers
		// start from the exchanged INIT state and save the SCTP cookie
		// handshake; a peer that does not advertise it uses normal SCTP.
		s.EnableSctpSnap(true)
	}
	if settings.EnableSctpZeroChecksum {
		// RFC 9653 permits a zero SCTP checksum only after the remote endpoint
		// advertises an acceptable alternate integrity method. WebRTC's
		// authenticated DTLS encapsulation is that method, so CRC32c is
		// redundant. Older/mixed peers do not advertise the parameter and
		// retain normal checksums.
		s.EnableSCTPZeroChecksum(true)
	}
	// Native peers advertise literal host candidates (Pion's default is
	// query-only, not address-obscuring gather). They therefore do not need a
	// multicast-DNS listener per peer connection. Disabling mDNS removes that
	// socket/goroutine set and avoids repeated initialization failures on
	// Android. The JS transport is separate and retains browser defaults.
	s.SetICEMulticastDNSMode(ice.MulticastDNSModeDisabled)
	// Detached data-channel writes otherwise only append to Pion's SCTP
	// pending queue. Blocking mode is required for transfer backpressure and
	// is what makes SetWriteDeadline effective.
	s.EnableDataChannelBlockWrite(true)
	// The SCTP receive buffer is set per-API below so a trusted network peer can
	// use a larger window than public peers within one manager (Fix 1).
	if 0 < settings.MaxMessageSize {
		s.SetSCTPMaxMessageSize(uint32(settings.MaxMessageSize))
	}
	if 0 < settings.SctpMinCwnd {
		s.SetSCTPMinCwnd(settings.SctpMinCwnd)
	}
	if 0 < settings.SctpFastRtxWnd {
		s.SetSCTPFastRtxWnd(settings.SctpFastRtxWnd)
	}
	if 0 < settings.SctpCwndCAStep {
		s.SetSCTPCwndCAStep(settings.SctpCwndCAStep)
	}
	if 0 < settings.ReceiveMtu {
		s.SetReceiveMTU(uint(settings.ReceiveMtu))
	}
	s.SetICETimeouts(
		settings.DisconnectedTimeout,
		settings.FailedTimeout,
		settings.KeepAliveTimeout,
	)
	if 0 < settings.StunGatherTimeout {
		s.SetSTUNGatherTimeout(settings.StunGatherTimeout)
	}
	if certificate == nil {
		privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return nil, nil, err
		}
		certificate, err = webrtc.GenerateCertificate(privateKey)
		if err != nil {
			return nil, nil, err
		}
	}
	configuration := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{
				URLs: settings.IceServerUrls,
			},
		},
		Certificates: []webrtc.Certificate{*certificate},
	}
	mediaEngine, err := newWebRtcMediaEngine(settings)
	if err != nil {
		return nil, nil, fmt.Errorf("create WebRTC media engine: %w", err)
	}
	// The certificate, codec registry, and socket network remain manager scoped,
	// but each PeerConnection needs its own small API/SettingEngine snapshot so
	// its DNS resolver can be canceled independently. Pion's transport.Net
	// Resolve methods have no context; sharing one resolver lifetime made a
	// retired peer wait indefinitely for an OS lookup started while the tunnel's
	// DNS route was changing.
	return &webRtcPeerConnectionFactory{
		newPeerConnection: func(
			networkPeer bool,
		) (*webrtc.PeerConnection, context.CancelFunc, error) {
			peerNet := selectedNet
			if peerNet == nil {
				standardNet, createErr := stdnet.NewNet()
				if createErr != nil {
					return nil, nil, fmt.Errorf(
						"create WebRTC peer socket network: %w",
						createErr,
					)
				}
				peerNet = standardNet
			}
			cancelResolve := context.CancelFunc(func() {})
			peerSettingEngine := s
			if !callerOwnedNet || settings.iceResolverForTest != nil {
				var resolveNet *peerConnectionResolveNet
				resolveNet, cancelResolve = newPeerConnectionResolveNet(
					peerNet,
					egressAwareResolver(settings.iceResolverForTest),
					settings.StunGatherTimeout,
				)
				peerNet = resolveNet
			}
			peerSettingEngine.SetNet(peerNet)
			receiveBufferByteCount := settings.ReceiveBufferSize
			if networkPeer && 0 < settings.NetworkPeerReceiveBufferSize {
				receiveBufferByteCount = settings.NetworkPeerReceiveBufferSize
			}
			peerSettingEngine.SetSCTPMaxReceiveBufferSize(
				uint32(receiveBufferByteCount),
			)
			api := webrtc.NewAPI(
				webrtc.WithSettingEngine(peerSettingEngine),
				webrtc.WithMediaEngine(mediaEngine),
				webrtc.WithInterceptorRegistry(nil),
			)
			pc, createErr := api.NewPeerConnection(configuration)
			if createErr != nil {
				cancelResolve()
				return nil, nil, createErr
			}
			return pc, cancelResolve, nil
		},
	}, certificate, nil
}

// webRtcSctpBufferedAmount returns pending plus in-flight SCTP user data for
// the lazy no-progress watchdog. Combined with the peerConn's monotonic
// accepted-write byte count, this yields acknowledged forward bytes without
// treating unrelated reverse packets as progress.
func webRtcSctpBufferedAmount(pc *webrtc.PeerConnection) (bufferedAmount int, ok bool) {
	if pc == nil {
		return
	}
	sctp := pc.SCTP()
	if sctp == nil {
		return
	}
	return sctp.BufferedAmount(), true
}

// webRtcSctpReceiverWindow is the slower ambiguity check used only when the
// no-progress deadline expires. SCTPTransport.Stats also snapshots metadata,
// so calling it on every 250 ms progress sample would add allocations and lock
// traffic to the hot send path. A zero window means the peer is deliberately
// applying receiver backpressure and the association must remain intact.
func webRtcSctpReceiverWindow(
	pc *webrtc.PeerConnection,
) (receiverWindow uint32, ok bool) {
	if pc == nil {
		return
	}
	sctp := pc.SCTP()
	if sctp == nil || sctp.State() != webrtc.SCTPTransportStateConnected {
		return
	}
	return sctp.Stats().ReceiverWindow, true
}

// webRtcPeerConnectionTransportStop returns the native DTLS stop operation so
// teardown can interrupt SCTP reads before PeerConnection.Close joins the rest
// of Pion. ICE Stop is itself a joining operation: it closes the packet mux and
// waits for the ICE agent loop, so using it as the interrupt can strand
// teardown before PeerConnection.Close gets a chance to release SCTP/DTLS.
// The browser implementation has no public Stop API and supplies a build-tagged
// no-op in transport_p2p_webrtc_pc_js.go.
func webRtcPeerConnectionTransportStop(
	pc *webrtc.PeerConnection,
) func() error {
	if pc == nil {
		return nil
	}
	if sctpTransport := pc.SCTP(); sctpTransport != nil {
		if dtlsTransport := sctpTransport.Transport(); dtlsTransport != nil {
			return dtlsTransport.Stop
		}
	}
	return nil
}

func detachWithDeadline(dc *webrtc.DataChannel) (datachannel.ReadWriteCloserDeadliner, error) {
	return dc.DetachWithDeadline()
}
