//go:build !js

package connect

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/datachannel"
	"github.com/pion/ice/v4"
	"github.com/pion/logging"
	"github.com/pion/transport/v4/vnet"
	"github.com/pion/webrtc/v4"
)

// TestWebRtcFastPathFitsIpv6MinimumMtuOnActualWire applies the minimum IPv6
// MTU to Pion's actual encrypted UDP payload. The filter is enabled only after
// ICE, DTLS, and carrier warmup finish, so an oversized application fragment
// is the deterministic alternative to complete-message delivery.
func TestWebRtcFastPathFitsIpv6MinimumMtuOnActualWire(t *testing.T) {
	const ipv6MinimumMtuByteCount = 1280
	const ipv6UdpHeaderByteCount = 40 + 8

	router, err := vnet.NewRouter(&vnet.RouterConfig{
		CIDR:          "10.3.0.0/24",
		MinDelay:      time.Millisecond,
		LoggerFactory: logging.NewDefaultLoggerFactory(),
	})
	if err != nil {
		t.Fatal(err)
	}
	netA, err := vnet.NewNet(&vnet.NetConfig{StaticIPs: []string{"10.3.0.1"}})
	if err != nil {
		t.Fatal(err)
	}
	netB, err := vnet.NewNet(&vnet.NetConfig{StaticIPs: []string{"10.3.0.2"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := router.AddNet(netA); err != nil {
		t.Fatal(err)
	}
	if err := router.AddNet(netB); err != nil {
		t.Fatal(err)
	}
	var enforceMtu atomic.Bool
	var maximumUdpPayloadByteCount atomic.Uint64
	oversizedPacket := make(chan int, 1)
	router.AddChunkFilter(func(chunk vnet.Chunk) bool {
		source, sourceOk := chunk.SourceAddr().(*net.UDPAddr)
		destination, destinationOk := chunk.DestinationAddr().(*net.UDPAddr)
		if !enforceMtu.Load() || !sourceOk || !destinationOk ||
			!source.IP.Equal(net.ParseIP("10.3.0.1")) ||
			!destination.IP.Equal(net.ParseIP("10.3.0.2")) {
			return true
		}
		udpPayloadByteCount := len(chunk.UserData())
		for {
			maximumByteCount := maximumUdpPayloadByteCount.Load()
			if uint64(udpPayloadByteCount) <= maximumByteCount ||
				maximumUdpPayloadByteCount.CompareAndSwap(
					maximumByteCount,
					uint64(udpPayloadByteCount),
				) {
				break
			}
		}
		outerPacketByteCount := ipv6UdpHeaderByteCount + udpPayloadByteCount
		if outerPacketByteCount <= ipv6MinimumMtuByteCount {
			return true
		}
		select {
		case oversizedPacket <- outerPacketByteCount:
		default:
		}
		return false
	})
	if err := router.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := router.Stop(); err != nil {
			t.Errorf("stop router: %v", err)
		}
	})

	pair := newP2pFastPathTestPairForStreamWithSettings(
		t,
		true,
		true,
		NewId(),
		nil,
		nil,
		func(active *WebRtcSettings, passive *WebRtcSettings) {
			active.Network = netA
			passive.Network = netB
		},
	)
	activeFast := pair.active.(webRtcFastPathConn)
	passiveFast := pair.passive.(webRtcFastPathConn)
	// Bounded by this test's own deadline rather than by a fixed ten seconds.
	//
	// Establishment here is ICE, DTLS and SCTP, none of which this row tests:
	// it tests that a fast-path write fits the minimum IPv6 MTU. A fixed wait
	// for a step the row does not test is a second deadline competing with the
	// one the runner set, and under load it expired first, so the row failed in
	// setup and reported a carrier that did not become ready rather than
	// anything about MTU. The pair's own context is no help — it is built with
	// a 45 second timeout, far longer than the wait — so the bound has to come
	// from the test.
	//
	// Derived rather than counted, and named as such: the margin below is a
	// judgement, not a measurement. It leaves room for the assertion after this
	// point to run and fail on its own terms rather than being killed by the
	// runner mid-write.
	readyTimeout := 10 * time.Second
	if deadline, ok := t.Deadline(); ok {
		if remaining := time.Until(deadline) - 5*time.Second; remaining < readyTimeout {
			readyTimeout = max(remaining, time.Second)
		}
	}
	if !activeFast.WaitFastPathReady(pair.ctx, readyTimeout) ||
		!passiveFast.WaitFastPathReady(pair.ctx, readyTimeout) {
		t.Fatalf(
			"fast carrier did not become ready within %s before MTU enforcement; this row tests the MTU of a fast-path write, so a failure here is establishment rather than the property under test",
			readyTimeout,
		)
	}
	enforceMtu.Store(true)

	message := bytes.Repeat(
		[]byte{0xb4},
		2*p2pFastPathFragmentPayloadByteCount,
	)
	fragmentCount, err := activeFast.WriteFastPathMessage(message)
	if err != nil {
		t.Fatal(err)
	}
	if fragmentCount != 2 {
		t.Fatalf("fragment count=%d want=2", fragmentCount)
	}
	// the same bound as the readiness wait, for the same reason
	timer := time.NewTimer(readyTimeout)
	defer timer.Stop()
	select {
	case outerPacketByteCount := <-oversizedPacket:
		t.Fatalf(
			"fast carrier submitted %d bytes to a %d-byte IPv6 path",
			outerPacketByteCount,
			ipv6MinimumMtuByteCount,
		)
	case received := <-passiveFast.FastPathMessages():
		if !bytes.Equal(received.message, message) {
			MessagePoolReturn(received.message)
			t.Fatal("MTU-safe fast carrier changed the message")
		}
		MessagePoolReturn(received.message)
	case <-timer.C:
		t.Fatal("MTU-safe fast carrier message did not arrive")
	}
	maximumOuterPacketByteCount := ipv6UdpHeaderByteCount +
		int(maximumUdpPayloadByteCount.Load())
	if maximumOuterPacketByteCount != ipv6MinimumMtuByteCount {
		t.Fatalf(
			"maximum fast carrier packet=%d want=%d",
			maximumOuterPacketByteCount,
			ipv6MinimumMtuByteCount,
		)
	}
}

type sctpPathMeasureConfig struct {
	oneWayDelay             time.Duration
	receiveBufferByteCount  uint32
	minCwnd                 uint32
	fastRtxWnd              uint32
	cwndCAStep              uint32
	rtoMax                  time.Duration
	dropEveryDataPacket     uint64
	outageDuration          time.Duration
	maxRetransmits          *uint16
	bottleneckBitsPerSecond int
	bottleneckBurstBytes    int
	bottleneckQueueBytes    int64
	warmupByteCount         int
	measuredByteCount       int
}

type sctpPathMeasureResult struct {
	byteCount       int
	elapsed         time.Duration
	midBulkLatency  time.Duration
	bulkLatencyP95  time.Duration
	bulkLatencyMax  time.Duration
	postBulkLatency time.Duration
	droppedPackets  uint64
	minObservedCwnd uint32
	maxObservedCwnd uint32
	finalCwnd       uint32
	finalSrtt       time.Duration
}

func newVnetWebRtcPeerConnectionFactory(
	t testing.TB,
	network *vnet.Net,
	settings *WebRtcSettings,
) *webRtcPeerConnectionFactory {
	t.Helper()
	mediaEngine, err := newWebRtcMediaEngine(settings)
	if err != nil {
		t.Fatal(err)
	}
	settingEngine := webrtc.SettingEngine{}
	settingEngine.SetNet(network)
	settingEngine.SetICEMulticastDNSMode(ice.MulticastDNSModeDisabled)
	settingEngine.SetICETimeouts(
		settings.DisconnectedTimeout,
		settings.FailedTimeout,
		settings.KeepAliveTimeout,
	)
	settingEngine.DetachDataChannels()
	settingEngine.EnableDataChannelBlockWrite(true)
	settingEngine.SetSCTPMaxReceiveBufferSize(uint32(settings.ReceiveBufferSize))
	settingEngine.SetSCTPMaxMessageSize(uint32(settings.MaxMessageSize))
	if 0 < settings.SctpMinCwnd {
		settingEngine.SetSCTPMinCwnd(settings.SctpMinCwnd)
	}
	if 0 < settings.SctpFastRtxWnd {
		settingEngine.SetSCTPFastRtxWnd(settings.SctpFastRtxWnd)
	}
	if 0 < settings.SctpCwndCAStep {
		settingEngine.SetSCTPCwndCAStep(settings.SctpCwndCAStep)
	}
	api := webrtc.NewAPI(
		webrtc.WithSettingEngine(settingEngine),
		webrtc.WithMediaEngine(mediaEngine),
		webrtc.WithInterceptorRegistry(nil),
	)
	return &webRtcPeerConnectionFactory{
		newPeerConnection: func(
			networkPeer bool,
		) (*webrtc.PeerConnection, context.CancelFunc, error) {
			pc, err := api.NewPeerConnection(webrtc.Configuration{})
			return pc, func() {}, err
		},
	}
}

// ICE consent and SCTP progress are different liveness signals. This test
// leaves STUN consent working while blackholing only encrypted DTLS/SCTP
// records after an idle period. Pion continues to report ICE connected, but
// the application data plane cannot ACK the resumed write. The lazy watchdog
// must request an immediate fresh association instead of retaining it forever.
func TestWebRtcIdleResumeSctpBlackholeReconnects(t *testing.T) {
	router, err := vnet.NewRouter(&vnet.RouterConfig{
		CIDR:          "10.2.0.0/24",
		MinDelay:      time.Millisecond,
		LoggerFactory: logging.NewDefaultLoggerFactory(),
	})
	if err != nil {
		t.Fatal(err)
	}
	netA, err := vnet.NewNet(&vnet.NetConfig{StaticIPs: []string{"10.2.0.1"}})
	if err != nil {
		t.Fatal(err)
	}
	netB, err := vnet.NewNet(&vnet.NetConfig{StaticIPs: []string{"10.2.0.2"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := router.AddNet(netA); err != nil {
		t.Fatal(err)
	}
	if err := router.AddNet(netB); err != nil {
		t.Fatal(err)
	}
	var sctpBlackhole atomic.Bool
	router.AddChunkFilter(func(chunk vnet.Chunk) bool {
		payload := chunk.UserData()
		if !sctpBlackhole.Load() || len(payload) == 0 {
			return true
		}
		source, ok := chunk.SourceAddr().(*net.UDPAddr)
		if !ok || !source.IP.Equal(net.ParseIP("10.2.0.1")) {
			// Keep reverse SCTP traffic flowing. Arbitrary reverse packets
			// must not mask the active side's unacknowledged outbound queue.
			return true
		}
		// Drop the active-to-passive DTLS record layer, including CloseNotify
		// alerts, but keep STUN consent packets. This models an asymmetric
		// stale UDP/NAT data path: the passive endpoint must not learn about
		// the active endpoint's local close through an alert that the
		// blackhole should have lost.
		return payload[0] < 20 || 23 < payload[0]
	})
	if err := router.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := router.Stop(); err != nil {
			t.Errorf("stop router: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	settingsA := DefaultWebRtcSettings()
	settingsB := DefaultWebRtcSettings()
	for _, settings := range []*WebRtcSettings{settingsA, settingsB} {
		settings.Log = NewNoopLogger()
		settings.IceServerUrls = nil
		settings.DisconnectedTimeout = 2 * time.Second
		settings.FailedTimeout = 2 * time.Second
		settings.KeepAliveTimeout = 20 * time.Millisecond
		settings.SctpNoProgressTimeout = 200 * time.Millisecond
	}
	// Reverse writes are deliberately unable to receive their SACKs through
	// the one-way blackhole. Keep that side alive for the duration of this
	// test so it can prove reverse DATA does not refresh the active deadline.
	settingsB.SctpNoProgressTimeout = 0

	signalPipeA := newSignalPipe(nil)
	signalPipeB := newSignalPipe(nil)
	managerA := newTestWebRtcManager(t, ctx, signalPipeA, settingsA)
	managerB := newTestWebRtcManager(t, ctx, signalPipeB, settingsB)
	managerA.newPeerConnectionFactory = func(
		*WebRtcSettings,
		*webrtc.Certificate,
	) (*webRtcPeerConnectionFactory, *webrtc.Certificate, error) {
		return newVnetWebRtcPeerConnectionFactory(t, netA, settingsA), nil, nil
	}
	managerB.newPeerConnectionFactory = func(
		*WebRtcSettings,
		*webrtc.Certificate,
	) (*webRtcPeerConnectionFactory, *webrtc.Certificate, error) {
		return newVnetWebRtcPeerConnectionFactory(t, netB, settingsB), nil, nil
	}
	signalPipeA.SetSignalReceiver(managerB)
	signalPipeB.SetSignalReceiver(managerA)

	peerIdA := NewId()
	peerIdB := NewId()
	streamId := NewId()
	passiveValue, err := managerB.NewP2pConnPassive(
		ctx,
		NewTransferPath(peerIdB, peerIdA, streamId),
	)
	if err != nil {
		t.Fatal(err)
	}
	activeValue, err := managerA.NewP2pConnActive(
		ctx,
		NewTransferPath(peerIdA, peerIdB, streamId),
	)
	if err != nil {
		t.Fatal(err)
	}
	active := activeValue.(*peerConn)
	passive := passiveValue.(*peerConn)
	defer active.Close()
	defer passive.Close()

	// Establish the association before applying the tight data-plane
	// deadlines. A one-second deadline beginning at constructor return also
	// measured goroutine scheduling and ICE/DTLS startup, which made this
	// blackhole test intermittently fail before the blackhole was enabled.
	connectedDeadline := time.Now().Add(5 * time.Second)
	for !active.Connected() || !passive.Connected() {
		if connectedDeadline.Before(time.Now()) {
			t.Fatal("initial association did not connect")
		}
		time.Sleep(time.Millisecond)
	}
	if err := active.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := passive.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	readDone := make(chan error, 1)
	go func() {
		var payload [1]byte
		n, readErr := passive.Read(payload[:])
		if readErr == nil && (n != 1 || payload[0] != 1) {
			readErr = fmt.Errorf("healthy read = %d/%v", n, payload)
		}
		readDone <- readErr
	}()
	if n, writeErr := active.Write([]byte{1}); writeErr != nil || n != 1 {
		t.Fatalf("healthy write = %d, %v", n, writeErr)
	}
	if readErr := <-readDone; readErr != nil {
		t.Fatal(readErr)
	}

	// A fully acknowledged write leaves no active watchdog timer; ordinary
	// idle time must not churn a healthy association.
	select {
	case <-active.ctx.Done():
		t.Fatal("healthy idle association was canceled")
	case <-time.After(2 * settingsA.SctpNoProgressTimeout):
	}

	sctpBlackhole.Store(true)
	if err := active.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if n, writeErr := active.Write([]byte{2}); writeErr != nil || n != 1 {
		t.Fatalf("blackholed write = %d, %v", n, writeErr)
	}
	if buffered := active.pc.SCTP().BufferedAmount(); buffered == 0 {
		t.Fatal("blackholed write did not leave SCTP data outstanding")
	}
	reverseWriteDone := make(chan struct{})
	go func() {
		defer close(reverseWriteDone)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-active.ctx.Done():
				return
			case <-ticker.C:
				if _, reverseErr := passive.Write([]byte{3}); reverseErr != nil {
					return
				}
			}
		}
	}()
	reverseReadDone := make(chan struct{})
	go func() {
		defer close(reverseReadDone)
		var payload [1]byte
		for {
			if _, reverseErr := active.Read(payload[:]); reverseErr != nil {
				return
			}
		}
	}()
	// ICE consent packets are deliberately still flowing. Establish that
	// Pion remains connected midway through the SCTP no-progress interval,
	// so this is not merely exercising its ordinary ICE failure path.
	select {
	case <-active.ctx.Done():
		t.Fatal("association was canceled before the no-progress bound")
	case <-time.After(settingsA.SctpNoProgressTimeout / 2):
	}
	if state := active.pc.ICEConnectionState(); state != webrtc.ICEConnectionStateConnected &&
		state != webrtc.ICEConnectionStateCompleted {
		t.Fatalf("ICE consent did not remain connected during SCTP blackhole: %s", state)
	}
	select {
	case <-active.ctx.Done():
	case <-time.After(2 * settingsA.SctpNoProgressTimeout):
		t.Fatal("SCTP blackhole did not cancel the peer connection")
	}
	if cause := context.Cause(active.ctx); cause == nil ||
		!strings.Contains(cause.Error(), "SCTP no progress") {
		t.Fatalf("SCTP blackhole cancellation cause = %v", cause)
	}
	select {
	case <-reverseWriteDone:
	case <-time.After(time.Second):
		t.Fatal("reverse SCTP writer did not stop with the retired association")
	}
	select {
	case <-reverseReadDone:
	case <-time.After(time.Second):
		t.Fatal("reverse SCTP reader did not stop with the retired association")
	}
	select {
	case <-active.ImmediateReconnect():
	default:
		t.Fatal("SCTP blackhole did not request immediate reconnect")
	}

	// Only the sending/active endpoint had evidence of the one-way failure.
	// Its fresh generation must retire the passive endpoint that still
	// answers ICE consent; otherwise that endpoint mistakes every new offer
	// for a duplicate and the pair can never resume.
	replacementActiveValue, err := managerA.NewP2pConnActive(
		ctx,
		NewTransferPath(peerIdA, peerIdB, streamId),
	)
	if err != nil {
		t.Fatal(err)
	}
	replacementActive := replacementActiveValue.(*peerConn)
	defer replacementActive.Close()
	select {
	case <-passive.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("fresh active generation did not retire the stale passive association")
	}
	if cause := context.Cause(passive.ctx); cause == nil ||
		!strings.Contains(cause.Error(), "fresh remote offer replaced") {
		t.Fatalf("passive replacement cancellation cause = %v", cause)
	}
	select {
	case <-passive.ImmediateReconnect():
	default:
		t.Fatal("stale passive association did not request immediate replacement")
	}

	sctpBlackhole.Store(false)
	replacementPassiveValue, err := managerB.NewP2pConnPassive(
		ctx,
		NewTransferPath(peerIdB, peerIdA, streamId),
	)
	if err != nil {
		t.Fatal(err)
	}
	replacementPassive := replacementPassiveValue.(*peerConn)
	defer replacementPassive.Close()
	if err := replacementActive.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := replacementPassive.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	resumedRead := make(chan error, 1)
	go func() {
		var payload [1]byte
		n, readErr := replacementPassive.Read(payload[:])
		if readErr == nil && (n != 1 || payload[0] != 3) {
			readErr = fmt.Errorf("resumed read = %d/%v", n, payload)
		}
		resumedRead <- readErr
	}()
	if n, writeErr := replacementActive.Write([]byte{3}); writeErr != nil || n != 1 {
		t.Fatalf("resumed write = %d, %v", n, writeErr)
	}
	select {
	case readErr := <-resumedRead:
		if readErr != nil {
			t.Fatal(readErr)
		}
	case <-ctx.Done():
		t.Fatal("replacement association did not resume data")
	}
}

// CONNECT_WEBRTC_WINDOW_MEASURE=1 enables a controlled receive-window
// comparison over a 50 ms RTT. The mobile settings historically assumed an
// 8 ms LAN RTT, but physical Android SCTP SACKs were observed at 50-74 ms.
// Because Pion advertises MaxReceiveBufferSize as SCTP a_rwnd, this measurement
// catches a receive-window throughput ceiling before changing the device's
// bounded peer-connection memory policy.
func TestWebRtcSctpReceiveWindowThroughputMeasurement(t *testing.T) {
	if os.Getenv("CONNECT_WEBRTC_WINDOW_MEASURE") == "" {
		t.Skip("set CONNECT_WEBRTC_WINDOW_MEASURE=1")
	}

	for _, receiveBufferByteCount := range []uint32{
		128 * 1024,
		256 * 1024,
		512 * 1024,
		1024 * 1024,
		2 * 1024 * 1024,
		4 * 1024 * 1024,
	} {
		byteCount, elapsed := measureDataChannelThroughputWithDelay(
			t,
			25*time.Millisecond,
			receiveBufferByteCount,
		)
		t.Logf(
			"window=%d KiB rtt=50ms: %d bytes in %s = %.2f MiB/s",
			receiveBufferByteCount/1024,
			byteCount,
			elapsed,
			float64(byteCount)/(1024*1024)/elapsed.Seconds(),
		)
	}
}

// CONNECT_WEBRTC_CONGESTION_MEASURE=1 enables the repeatable counterpart to
// the physical-device [p2p-stats] capture. Loss is applied only to sender to
// receiver DTLS application datagrams after warmup, so ICE/DTLS setup and
// reverse SACK traffic are stable. The post-bulk one-byte delivery records
// whether a throughput win leaves a recovery tail.
func TestWebRtcSctpCongestionTuningMeasurement(t *testing.T) {
	if os.Getenv("CONNECT_WEBRTC_CONGESTION_MEASURE") == "" {
		t.Skip("set CONNECT_WEBRTC_CONGESTION_MEASURE=1")
	}

	type tuning struct {
		name       string
		minCwnd    uint32
		fastRtxWnd uint32
		cwndCAStep uint32
		rtoMax     time.Duration
	}
	tunings := []tuning{
		{name: "default"},
		{name: "ca-2mtu", cwndCAStep: 2 * 1200},
		{name: "ca-4mtu", cwndCAStep: 4 * 1200},
		{name: "ca-4mtu-rto-max-2s", cwndCAStep: 4 * 1200, rtoMax: 2 * time.Second},
		{name: "ca-6mtu", cwndCAStep: 6 * 1200},
		{name: "ca-8mtu", cwndCAStep: 8 * 1200},
		{name: "fast-4mtu", fastRtxWnd: 4 * 1200},
		{name: "fast-16k", fastRtxWnd: 16 * 1024},
		{name: "floor-16k", minCwnd: 16 * 1024},
		{name: "floor-32k", minCwnd: 32 * 1024},
		{name: "floor-64k", minCwnd: 64 * 1024},
		{name: "floor-16k-ca-4mtu", minCwnd: 16 * 1024, cwndCAStep: 4 * 1200},
		{name: "floor-32k-ca-4mtu", minCwnd: 32 * 1024, cwndCAStep: 4 * 1200},
		{name: "ca-4mtu-fast-4mtu", cwndCAStep: 4 * 1200, fastRtxWnd: 4 * 1200},
	}
	for _, dropEvery := range []uint64{0, 500, 200, 100} {
		for _, tuning := range tunings {
			result := measureSctpPath(t, sctpPathMeasureConfig{
				oneWayDelay:            25 * time.Millisecond,
				receiveBufferByteCount: 2 * 1024 * 1024,
				minCwnd:                tuning.minCwnd,
				fastRtxWnd:             tuning.fastRtxWnd,
				cwndCAStep:             tuning.cwndCAStep,
				rtoMax:                 tuning.rtoMax,
				dropEveryDataPacket:    dropEvery,
				warmupByteCount:        1024 * 1024,
				measuredByteCount:      8 * 1024 * 1024,
			})
			t.Logf(
				"drop=%d/%s bytes=%d elapsed=%s throughput=%.2f MiB/s post=%s drops=%d cwnd=%d..%d final=%d srtt=%s",
				dropEvery,
				tuning.name,
				result.byteCount,
				result.elapsed,
				float64(result.byteCount)/(1024*1024)/result.elapsed.Seconds(),
				result.postBulkLatency,
				result.droppedPackets,
				result.minObservedCwnd,
				result.maxObservedCwnd,
				result.finalCwnd,
				result.finalSrtt,
			)
		}
	}
}

// CONNECT_WEBRTC_QUEUE_MEASURE=1 checks the congestion candidates against an
// actual bottleneck rather than independent wireless-style loss. An aggressive
// floor can look excellent when drops are exogenous but create persistent
// queue loss and latency when the link is genuinely slower than the floor.
func TestWebRtcSctpCongestionQueueMeasurement(t *testing.T) {
	if os.Getenv("CONNECT_WEBRTC_QUEUE_MEASURE") == "" {
		t.Skip("set CONNECT_WEBRTC_QUEUE_MEASURE=1")
	}

	type tuning struct {
		name       string
		minCwnd    uint32
		cwndCAStep uint32
		rtoMax     time.Duration
	}
	tunings := []tuning{
		{name: "default"},
		{name: "ca-4mtu", cwndCAStep: 4 * 1200},
		{name: "ca-4mtu-rto-max-2s", cwndCAStep: 4 * 1200, rtoMax: 2 * time.Second},
		{name: "ca-8mtu", cwndCAStep: 8 * 1200},
		{name: "floor-32k", minCwnd: 32 * 1024},
		{name: "floor-64k", minCwnd: 64 * 1024},
	}
	for _, rateMbps := range []int{5, 20, 50} {
		for _, tuning := range tunings {
			result := measureSctpPath(t, sctpPathMeasureConfig{
				oneWayDelay:             25 * time.Millisecond,
				receiveBufferByteCount:  2 * 1024 * 1024,
				minCwnd:                 tuning.minCwnd,
				cwndCAStep:              tuning.cwndCAStep,
				rtoMax:                  tuning.rtoMax,
				bottleneckBitsPerSecond: rateMbps * 1000 * 1000,
				bottleneckBurstBytes:    4 * 1500,
				bottleneckQueueBytes:    64 * 1024,
				warmupByteCount:         1024 * 1024,
				measuredByteCount:       8 * 1024 * 1024,
			})
			t.Logf(
				"rate=%dMbps/%s bytes=%d elapsed=%s throughput=%.2f MiB/s bulk-p50=%s p95=%s max=%s post=%s cwnd=%d..%d final=%d srtt=%s",
				rateMbps,
				tuning.name,
				result.byteCount,
				result.elapsed,
				float64(result.byteCount)/(1024*1024)/result.elapsed.Seconds(),
				result.midBulkLatency,
				result.bulkLatencyP95,
				result.bulkLatencyMax,
				result.postBulkLatency,
				result.minObservedCwnd,
				result.maxObservedCwnd,
				result.finalCwnd,
				result.finalSrtt,
			)
		}
	}
}

// CONNECT_WEBRTC_OUTAGE_MEASURE=1 measures pause length when a short
// data-plane outage begins after a warm association. Pion's T3 timer is
// deliberately infinite and exponentially backs off to a 60-second RTO;
// lowering only RTO.Max can reduce a transient outage tail, but may also add
// retransmit work. Keep this as an opt-in rejection/selection harness.
func TestWebRtcSctpOutageRecoveryMeasurement(t *testing.T) {
	if os.Getenv("CONNECT_WEBRTC_OUTAGE_MEASURE") == "" {
		t.Skip("set CONNECT_WEBRTC_OUTAGE_MEASURE=1")
	}

	for _, outage := range []time.Duration{
		1500 * time.Millisecond,
		3500 * time.Millisecond,
		5500 * time.Millisecond,
	} {
		for _, tuning := range []struct {
			name   string
			rtoMax time.Duration
		}{
			{name: "default"},
			{name: "rto-max-2s", rtoMax: 2 * time.Second},
			{name: "rto-max-4s", rtoMax: 4 * time.Second},
		} {
			result := measureSctpPath(t, sctpPathMeasureConfig{
				oneWayDelay:            25 * time.Millisecond,
				receiveBufferByteCount: 2 * 1024 * 1024,
				cwndCAStep:             4 * 1200,
				rtoMax:                 tuning.rtoMax,
				outageDuration:         outage,
				warmupByteCount:        1024 * 1024,
				measuredByteCount:      sendPackBatchMaxMessageByteCount,
			})
			t.Logf(
				"outage=%s/%s recovery=%s post=%s drops=%d final_cwnd=%d srtt=%s",
				outage,
				tuning.name,
				result.elapsed,
				result.postBulkLatency,
				result.droppedPackets,
				result.finalCwnd,
				result.finalSrtt,
			)
		}
	}
}

func measureDataChannelThroughputWithDelay(
	t *testing.T,
	oneWayDelay time.Duration,
	receiveBufferByteCount uint32,
) (int, time.Duration) {
	t.Helper()
	result := measureSctpPath(t, sctpPathMeasureConfig{
		oneWayDelay:            oneWayDelay,
		receiveBufferByteCount: receiveBufferByteCount,
		warmupByteCount:        1024 * 1024,
		measuredByteCount:      8 * 1024 * 1024,
	})
	return result.byteCount, result.elapsed
}

func TestMeasureSctpPathCollectsBulkLatencyDistribution(t *testing.T) {
	result := measureSctpPath(t, sctpPathMeasureConfig{
		oneWayDelay:            time.Millisecond,
		receiveBufferByteCount: 2 * 1024 * 1024,
		warmupByteCount:        128 * 1024,
		measuredByteCount:      512 * 1024,
	})
	if result.byteCount <= 0 || result.elapsed <= 0 {
		t.Fatalf("invalid throughput sample: bytes=%d elapsed=%s", result.byteCount, result.elapsed)
	}
	if result.midBulkLatency <= 0 ||
		result.bulkLatencyP95 < result.midBulkLatency ||
		result.bulkLatencyMax < result.bulkLatencyP95 {
		t.Fatalf(
			"invalid bulk latency distribution: p50=%s p95=%s max=%s",
			result.midBulkLatency,
			result.bulkLatencyP95,
			result.bulkLatencyMax,
		)
	}
	if result.postBulkLatency <= 0 {
		t.Fatalf("invalid post-bulk latency: %s", result.postBulkLatency)
	}
}

func measureSctpPath(t *testing.T, config sctpPathMeasureConfig) sctpPathMeasureResult {
	t.Helper()

	router, err := vnet.NewRouter(&vnet.RouterConfig{
		CIDR:          "10.1.0.0/24",
		MinDelay:      config.oneWayDelay,
		LoggerFactory: logging.NewDefaultLoggerFactory(),
	})
	if err != nil {
		t.Fatal(err)
	}
	netA, err := vnet.NewNet(&vnet.NetConfig{StaticIPs: []string{"10.1.0.1"}})
	if err != nil {
		t.Fatal(err)
	}
	netB, err := vnet.NewNet(&vnet.NetConfig{StaticIPs: []string{"10.1.0.2"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := router.AddNet(netA); err != nil {
		t.Fatal(err)
	}
	var bottleneckQueue *vnet.Queue
	var netBRouterInterface vnet.NIC = netB
	if 0 < config.bottleneckBitsPerSecond {
		burstBytes := config.bottleneckBurstBytes
		if burstBytes <= 0 {
			burstBytes = 4 * 1500
		}
		queueBytes := config.bottleneckQueueBytes
		if queueBytes <= 0 {
			queueBytes = 64 * 1024
		}
		bottleneckQueue, err = vnet.NewQueue(
			netB,
			vnet.NewTBFQueue(config.bottleneckBitsPerSecond, burstBytes, queueBytes),
		)
		if err != nil {
			t.Fatal(err)
		}
		netBRouterInterface = bottleneckQueue
	}
	if err := router.AddNet(netBRouterInterface); err != nil {
		t.Fatal(err)
	}
	var dropEnabled atomic.Bool
	var outboundDataPacketCount atomic.Uint64
	var droppedPacketCount atomic.Uint64
	var outageUntil atomic.Int64
	if 0 < config.dropEveryDataPacket || 0 < config.outageDuration {
		router.AddChunkFilter(func(chunk vnet.Chunk) bool {
			source, sourceOk := chunk.SourceAddr().(*net.UDPAddr)
			destination, destinationOk := chunk.DestinationAddr().(*net.UDPAddr)
			if !dropEnabled.Load() || !sourceOk || !destinationOk ||
				!source.IP.Equal(net.ParseIP("10.1.0.1")) ||
				!destination.IP.Equal(net.ParseIP("10.1.0.2")) {
				return true
			}
			payload := chunk.UserData()
			// STUN packets begin with 00/01, while encrypted DTLS application
			// records use content type 23. Preserve ICE consent traffic so this
			// benchmark isolates SCTP recovery rather than path teardown.
			if len(payload) == 0 || payload[0] != 23 {
				return true
			}
			if until := outageUntil.Load(); until != 0 && time.Now().UnixNano() < until {
				droppedPacketCount.Add(1)
				return false
			}
			if config.dropEveryDataPacket == 0 {
				return true
			}
			packetNumber := outboundDataPacketCount.Add(1)
			if packetNumber%config.dropEveryDataPacket != 0 {
				return true
			}
			droppedPacketCount.Add(1)
			return false
		})
	}
	if err := router.Start(); err != nil {
		t.Fatal(err)
	}

	newPeerConnection := func(network *vnet.Net) *webrtc.PeerConnection {
		settings := webrtc.SettingEngine{}
		settings.SetNet(network)
		settings.SetICEMulticastDNSMode(ice.MulticastDNSModeDisabled)
		settings.SetICETimeouts(5*time.Second, 5*time.Second, 5*time.Second)
		settings.DetachDataChannels()
		settings.EnableDataChannelBlockWrite(true)
		settings.SetSCTPMaxReceiveBufferSize(config.receiveBufferByteCount)
		settings.SetSCTPMaxMessageSize(64 * 1024)
		if 0 < config.minCwnd {
			settings.SetSCTPMinCwnd(config.minCwnd)
		}
		if 0 < config.fastRtxWnd {
			settings.SetSCTPFastRtxWnd(config.fastRtxWnd)
		}
		if 0 < config.cwndCAStep {
			settings.SetSCTPCwndCAStep(config.cwndCAStep)
		}
		if 0 < config.rtoMax {
			settings.SetSCTPRTOMax(config.rtoMax)
		}
		api := webrtc.NewAPI(
			webrtc.WithSettingEngine(settings),
			webrtc.WithMediaEngine(&webrtc.MediaEngine{}),
			webrtc.WithInterceptorRegistry(nil),
		)
		pc, createErr := api.NewPeerConnection(webrtc.Configuration{})
		if createErr != nil {
			t.Fatal(createErr)
		}
		return pc
	}
	pcA := newPeerConnection(netA)
	pcB := newPeerConnection(netB)
	defer func() {
		if err := pcA.Close(); err != nil {
			t.Errorf("close A: %v", err)
		}
		if err := pcB.Close(); err != nil {
			t.Errorf("close B: %v", err)
		}
		if err := router.Stop(); err != nil {
			t.Errorf("stop router: %v", err)
		}
		if bottleneckQueue != nil {
			if err := bottleneckQueue.Close(); err != nil {
				t.Errorf("close bottleneck queue: %v", err)
			}
		}
	}()

	openedA := make(chan datachannel.ReadWriteCloser, 1)
	openedB := make(chan datachannel.ReadWriteCloser, 1)
	pcB.OnDataChannel(func(dc *webrtc.DataChannel) {
		dc.OnOpen(func() {
			raw, detachErr := dc.Detach()
			if detachErr != nil {
				t.Errorf("detach B: %v", detachErr)
				return
			}
			openedB <- raw
		})
	})
	unordered := false
	dcA, err := pcA.CreateDataChannel(
		"window-measure",
		&webrtc.DataChannelInit{
			Ordered:        &unordered,
			MaxRetransmits: config.maxRetransmits,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	dcA.OnOpen(func() {
		raw, detachErr := dcA.Detach()
		if detachErr != nil {
			t.Errorf("detach A: %v", detachErr)
			return
		}
		openedA <- raw
	})

	offerGathered := webrtc.GatheringCompletePromise(pcA)
	offer, err := pcA.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := pcA.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	select {
	case <-offerGathered:
	case <-time.After(10 * time.Second):
		t.Fatal("offer gathering timeout")
	}
	if err := pcB.SetRemoteDescription(*pcA.LocalDescription()); err != nil {
		t.Fatal(err)
	}

	answerGathered := webrtc.GatheringCompletePromise(pcB)
	answer, err := pcB.CreateAnswer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := pcB.SetLocalDescription(answer); err != nil {
		t.Fatal(err)
	}
	select {
	case <-answerGathered:
	case <-time.After(10 * time.Second):
		t.Fatal("answer gathering timeout")
	}
	if err := pcA.SetRemoteDescription(*pcB.LocalDescription()); err != nil {
		t.Fatal(err)
	}

	var rawA datachannel.ReadWriteCloser
	var rawB datachannel.ReadWriteCloser
	select {
	case rawA = <-openedA:
	case <-time.After(10 * time.Second):
		t.Fatal("data channel A did not open")
	}
	select {
	case rawB = <-openedB:
	case <-time.After(10 * time.Second):
		t.Fatal("data channel B did not open")
	}
	defer rawA.Close()
	defer rawB.Close()

	if config.warmupByteCount <= 0 {
		config.warmupByteCount = 1024 * 1024
	}
	if config.measuredByteCount <= 0 {
		config.measuredByteCount = 8 * 1024 * 1024
	}
	const messageByteCount = sendPackBatchMaxMessageByteCount
	warmupMessageCount := config.warmupByteCount / messageByteCount
	measuredMessageCount := config.measuredByteCount / messageByteCount
	totalMessageCount := warmupMessageCount + measuredMessageCount
	actualMeasuredByteCount := measuredMessageCount * messageByteCount
	payload := make([]byte, messageByteCount)
	receiveDone := make(chan error, 1)
	warmupDone := make(chan struct{})
	bulkDone := make(chan struct{})
	// 31 samples keep probe overhead negligible while making p95 distinct
	// from max (with 15 samples the nearest-rank p95 is necessarily sample 15).
	bulkProbeCount := min(31, max(1, measuredMessageCount))
	type probeArrival struct {
		index int
		at    time.Time
	}
	probeArrivals := make(chan probeArrival, bulkProbeCount)
	probeDone := make(chan error, 1)
	go func() {
		receiveBuffer := make([]byte, 64*1024)
		regularMessageCount := 0
		receivedProbeCount := 0
		for regularMessageCount < totalMessageCount || receivedProbeCount < bulkProbeCount {
			n, readErr := rawB.Read(receiveBuffer)
			if readErr != nil {
				receiveDone <- readErr
				return
			}
			if n == 3 && receiveBuffer[0] == 0xa5 && receiveBuffer[2] == 0x5a {
				probeIndex := int(receiveBuffer[1])
				if probeIndex < 0 || bulkProbeCount <= probeIndex {
					receiveDone <- fmt.Errorf("invalid in-bulk probe index %d", probeIndex)
					return
				}
				probeArrivals <- probeArrival{index: probeIndex, at: time.Now()}
				receivedProbeCount++
				continue
			}
			if n != messageByteCount {
				receiveDone <- fmt.Errorf("read %d bytes, expected %d", n, messageByteCount)
				return
			}
			regularMessageCount++
			if regularMessageCount == warmupMessageCount {
				close(warmupDone)
			}
			if regularMessageCount == totalMessageCount {
				close(bulkDone)
			}
		}
		n, readErr := rawB.Read(receiveBuffer)
		if readErr == nil && (n != 4 || receiveBuffer[0] != 0x5a) {
			readErr = fmt.Errorf("read post-bulk probe %d bytes, expected marker/4", n)
		}
		probeDone <- readErr
	}()

	writeMessages := func(messageCount int) {
		t.Helper()
		for range messageCount {
			n, writeErr := rawA.Write(payload)
			if writeErr != nil {
				t.Fatal(writeErr)
			}
			if n != len(payload) {
				t.Fatalf("wrote %d bytes, expected %d", n, len(payload))
			}
		}
	}
	writeMessages(warmupMessageCount)
	select {
	case <-warmupDone:
	case readErr := <-receiveDone:
		t.Fatalf("warmup receive failed: %v", readErr)
	case <-time.After(30 * time.Second):
		t.Fatal("warmup timed out")
	}

	dropEnabled.Store(true)
	if 0 < config.outageDuration {
		outageUntil.Store(time.Now().Add(config.outageDuration).UnixNano())
	}
	minObservedCwnd := ^uint32(0)
	var maxObservedCwnd uint32
	statsStop := make(chan struct{})
	statsDone := make(chan struct{})
	go func() {
		defer close(statsDone)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-statsStop:
				return
			case <-ticker.C:
				cwnd := pcA.SCTP().Stats().CongestionWindow
				if 0 < cwnd && cwnd < minObservedCwnd {
					minObservedCwnd = cwnd
				}
				if maxObservedCwnd < cwnd {
					maxObservedCwnd = cwnd
				}
			}
		}
	}()

	start := time.Now()
	probeStarts := make([]time.Time, bulkProbeCount)
	nextProbe := 0
	for messageIndex := range measuredMessageCount {
		writeMessages(1)
		if nextProbe < bulkProbeCount &&
			(messageIndex+1)*(bulkProbeCount+1) >= (nextProbe+1)*measuredMessageCount {
			probe := []byte{0xa5, byte(nextProbe), 0x5a}
			probeStarts[nextProbe] = time.Now()
			n, writeErr := rawA.Write(probe)
			if writeErr != nil {
				t.Fatal(writeErr)
			}
			if n != len(probe) {
				t.Fatalf("wrote in-bulk probe %d bytes, expected %d", n, len(probe))
			}
			nextProbe++
		}
	}
	select {
	case <-bulkDone:
	case readErr := <-receiveDone:
		t.Fatal(readErr)
	case <-time.After(60 * time.Second):
		t.Fatal("measurement timed out")
	}
	elapsed := time.Since(start)
	bulkLatencies := make([]time.Duration, 0, bulkProbeCount)
	for range bulkProbeCount {
		select {
		case arrival := <-probeArrivals:
			bulkLatencies = append(bulkLatencies, arrival.at.Sub(probeStarts[arrival.index]))
		case readErr := <-receiveDone:
			t.Fatal(readErr)
		case <-time.After(10 * time.Second):
			t.Fatal("in-bulk probe timed out")
		}
	}
	sort.Slice(bulkLatencies, func(i, j int) bool {
		return bulkLatencies[i] < bulkLatencies[j]
	})
	midBulkLatency := bulkLatencies[len(bulkLatencies)/2]
	p95Index := (95*len(bulkLatencies)+99)/100 - 1
	bulkLatencyP95 := bulkLatencies[p95Index]
	bulkLatencyMax := bulkLatencies[len(bulkLatencies)-1]

	probeStart := time.Now()
	n, err := rawA.Write([]byte{0x5a, 2, 2, 0xa5})
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("wrote post-bulk probe %d bytes, expected 4", n)
	}
	select {
	case probeErr := <-probeDone:
		if probeErr != nil {
			t.Fatal(probeErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("post-bulk probe timed out")
	}
	postBulkLatency := time.Since(probeStart)
	close(statsStop)
	<-statsDone
	finalStats := pcA.SCTP().Stats()
	if minObservedCwnd == ^uint32(0) {
		minObservedCwnd = finalStats.CongestionWindow
	}
	return sctpPathMeasureResult{
		byteCount:       actualMeasuredByteCount,
		elapsed:         elapsed,
		midBulkLatency:  midBulkLatency,
		bulkLatencyP95:  bulkLatencyP95,
		bulkLatencyMax:  bulkLatencyMax,
		postBulkLatency: postBulkLatency,
		droppedPackets:  droppedPacketCount.Load(),
		minObservedCwnd: minObservedCwnd,
		maxObservedCwnd: maxObservedCwnd,
		finalCwnd:       finalStats.CongestionWindow,
		finalSrtt:       time.Duration(finalStats.SmoothedRoundTripTime * float64(time.Second)),
	}
}

// CONNECT_WEBRTC_LOSS_MEASURE=1 enables a deterministic comparison of
// DataChannel delivery behind one dropped DTLS application datagram. The
// transfer protocol has its own sequence/reorder/retransmit layer, so this
// measures whether SCTP's default ordered delivery adds avoidable
// head-of-line latency before changing production channel semantics.
func TestWebRtcDataChannelLossHeadOfLineMeasurement(t *testing.T) {
	if os.Getenv("CONNECT_WEBRTC_LOSS_MEASURE") == "" {
		t.Skip("set CONNECT_WEBRTC_LOSS_MEASURE=1")
	}

	zeroRetransmits := uint16(0)
	oneRetransmit := uint16(1)
	ordered := true
	unordered := false
	variants := []struct {
		name           string
		ordered        *bool
		maxRetransmits *uint16
	}{
		{name: "ordered-reliable", ordered: &ordered},
		{name: "unordered-reliable", ordered: &unordered},
		{name: "unordered-max-retransmits-1", ordered: &unordered, maxRetransmits: &oneRetransmit},
		{name: "unordered-max-retransmits-0", ordered: &unordered, maxRetransmits: &zeroRetransmits},
	}

	const trials = 10
	for _, variant := range variants {
		latencies := make([]time.Duration, 0, trials)
		for trial := range trials {
			latency := measureDataChannelSecondMessageAfterOneDrop(
				t,
				trial,
				&webrtc.DataChannelInit{
					Ordered:        variant.ordered,
					MaxRetransmits: variant.maxRetransmits,
				},
			)
			latencies = append(latencies, latency)
		}
		sort.Slice(latencies, func(i, j int) bool {
			return latencies[i] < latencies[j]
		})
		t.Logf(
			"%s second-message latency after one dropped DTLS datagram: median=%s p95=%s min=%s max=%s",
			variant.name,
			latencies[len(latencies)/2],
			latencies[(len(latencies)*95-1)/100],
			latencies[0],
			latencies[len(latencies)-1],
		)
	}
}

func measureDataChannelSecondMessageAfterOneDrop(
	t *testing.T,
	trial int,
	init *webrtc.DataChannelInit,
) time.Duration {
	t.Helper()

	router, err := vnet.NewRouter(&vnet.RouterConfig{
		CIDR:          "10.0.0.0/24",
		LoggerFactory: logging.NewDefaultLoggerFactory(),
	})
	if err != nil {
		t.Fatal(err)
	}
	netA, err := vnet.NewNet(&vnet.NetConfig{StaticIPs: []string{"10.0.0.1"}})
	if err != nil {
		t.Fatal(err)
	}
	netB, err := vnet.NewNet(&vnet.NetConfig{StaticIPs: []string{"10.0.0.2"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := router.AddNet(netA); err != nil {
		t.Fatal(err)
	}
	if err := router.AddNet(netB); err != nil {
		t.Fatal(err)
	}
	if err := router.Start(); err != nil {
		t.Fatal(err)
	}

	newPeerConnection := func(network *vnet.Net) *webrtc.PeerConnection {
		settings := webrtc.SettingEngine{}
		settings.SetNet(network)
		settings.SetICEMulticastDNSMode(ice.MulticastDNSModeDisabled)
		settings.SetICETimeouts(5*time.Second, 5*time.Second, 5*time.Second)
		api := webrtc.NewAPI(
			webrtc.WithSettingEngine(settings),
			webrtc.WithMediaEngine(&webrtc.MediaEngine{}),
			webrtc.WithInterceptorRegistry(nil),
		)
		pc, createErr := api.NewPeerConnection(webrtc.Configuration{})
		if createErr != nil {
			t.Fatal(createErr)
		}
		return pc
	}
	pcA := newPeerConnection(netA)
	pcB := newPeerConnection(netB)
	defer func() {
		if err := pcA.Close(); err != nil {
			t.Errorf("close A: %v", err)
		}
		if err := pcB.Close(); err != nil {
			t.Errorf("close B: %v", err)
		}
		if err := router.Stop(); err != nil {
			t.Errorf("stop router: %v", err)
		}
	}()

	openedA := make(chan struct{}, 1)
	openedB := make(chan struct{}, 1)
	messages := make(chan string, 8)
	pcB.OnDataChannel(func(dc *webrtc.DataChannel) {
		dc.OnOpen(func() {
			select {
			case openedB <- struct{}{}:
			default:
			}
		})
		dc.OnMessage(func(message webrtc.DataChannelMessage) {
			messages <- string(message.Data)
		})
	})
	dcA, err := pcA.CreateDataChannel("loss-measure", init)
	if err != nil {
		t.Fatal(err)
	}
	dcA.OnOpen(func() {
		select {
		case openedA <- struct{}{}:
		default:
		}
	})

	offerGathered := webrtc.GatheringCompletePromise(pcA)
	offer, err := pcA.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := pcA.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	select {
	case <-offerGathered:
	case <-time.After(5 * time.Second):
		t.Fatal("offer gathering timeout")
	}
	if err := pcB.SetRemoteDescription(*pcA.LocalDescription()); err != nil {
		t.Fatal(err)
	}

	answerGathered := webrtc.GatheringCompletePromise(pcB)
	answer, err := pcB.CreateAnswer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := pcB.SetLocalDescription(answer); err != nil {
		t.Fatal(err)
	}
	select {
	case <-answerGathered:
	case <-time.After(5 * time.Second):
		t.Fatal("answer gathering timeout")
	}
	if err := pcA.SetRemoteDescription(*pcB.LocalDescription()); err != nil {
		t.Fatal(err)
	}

	for side, opened := range []<-chan struct{}{openedA, openedB} {
		select {
		case <-opened:
		case <-time.After(5 * time.Second):
			t.Fatalf("data channel side %d did not open", side)
		}
	}

	// Arm only after DCEP and DTLS setup are quiet. Restrict the drop to the
	// first DTLS application-data record sent by A, so ICE checks and the
	// reverse-direction SACK path remain intact.
	var dropArmed atomic.Bool
	dropped := make(chan struct{}, 1)
	router.AddChunkFilter(func(chunk vnet.Chunk) bool {
		source, ok := chunk.SourceAddr().(*net.UDPAddr)
		payload := chunk.UserData()
		if ok &&
			source.IP.Equal(net.ParseIP("10.0.0.1")) &&
			500 < len(payload) &&
			payload[0] == 23 &&
			dropArmed.CompareAndSwap(true, false) {
			select {
			case dropped <- struct{}{}:
			default:
			}
			return false
		}
		return true
	})
	time.Sleep(50 * time.Millisecond)
	dropArmed.Store(true)
	start := time.Now()
	// Production transfer batches are about 3 KiB. This spans several SCTP
	// chunks, guaranteeing the receiver observes a TSN gap after the dropped
	// datagram instead of waiting for an otherwise idle retransmission timer.
	firstPayload := make([]byte, sendPackBatchMaxMessageByteCount)
	firstPayload[0] = byte(trial)
	if err := dcA.Send(firstPayload); err != nil {
		t.Fatal(err)
	}
	select {
	case <-dropped:
	case <-time.After(2 * time.Second):
		t.Fatal("did not drop the armed data datagram")
	}
	if err := dcA.Send([]byte("second")); err != nil {
		t.Fatal(err)
	}

	for {
		select {
		case message := <-messages:
			if message == "second" {
				return time.Since(start)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("second message remained head-of-line blocked")
		}
	}
}
