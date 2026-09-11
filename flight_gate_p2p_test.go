package connect

// Deterministic reproductions of the fast-path mechanisms in
// FLIGHTGATEFIX.md §5 (M5 fragment loss, M6 liveness) on a pion vnet with a
// seeded loss filter below the real ICE/DTLS/SRTP stack.

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/logging"
	"github.com/pion/transport/v4/vnet"
	"github.com/pion/webrtc/v4"
)

// rtpUdpPayload reports whether a UDP payload is an RTP or RTCP packet by the
// RFC 7983 first-byte demultiplexing rule (STUN 0-3, DTLS 20-63, RTP 128-191).
// SRTP leaves the RTP header in the clear, so this holds for the encrypted
// media the fast path sends.
func rtpUdpPayload(payload []byte) bool {
	return 0 < len(payload) && 128 <= payload[0] && payload[0] <= 191
}

// flightGateVnetPair is one active/passive native fast-path association on a
// vnet whose chunk filter the test controls.
type flightGateVnetPair struct {
	ctx          context.Context
	active       *peerConn
	passive      *peerConn
	activeFast   webRtcFastPathConn
	passiveFast  webRtcFastPathConn
	activeStats  *P2pDataPlaneStats
	passiveStats *P2pDataPlaneStats
	activeIp     net.IP
}

// newFlightGateVnetPair connects two native fast-path peers through a vnet
// router; filter decides per chunk whether it is delivered.
func newFlightGateVnetPair(
	t testing.TB,
	filter func(chunk vnet.Chunk) bool,
	configure func(active *WebRtcSettings, passive *WebRtcSettings),
) *flightGateVnetPair {
	t.Helper()
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
	if filter != nil {
		router.AddChunkFilter(filter)
	}
	if err := router.Start(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	pair := &flightGateVnetPair{
		ctx:          ctx,
		activeStats:  &P2pDataPlaneStats{},
		passiveStats: &P2pDataPlaneStats{},
		activeIp:     net.ParseIP("10.3.0.1"),
	}
	settingsA := DefaultWebRtcSettings()
	settingsB := DefaultWebRtcSettings()
	for _, settings := range []*WebRtcSettings{settingsA, settingsB} {
		settings.Log = NewNoopLogger()
		settings.IceServerUrls = nil
		settings.EnableDatagramFastPath = true
		settings.DisconnectedTimeout = 5 * time.Second
		settings.FailedTimeout = 5 * time.Second
		settings.KeepAliveTimeout = 20 * time.Millisecond
	}
	settingsA.DataPlaneStats = pair.activeStats
	settingsB.DataPlaneStats = pair.passiveStats
	if configure != nil {
		configure(settingsA, settingsB)
	}
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
	passiveValue, err := managerB.NewP2pConnPassive(ctx, NewTransferPath(peerIdB, peerIdA, streamId))
	if err != nil {
		t.Fatal(err)
	}
	activeValue, err := managerA.NewP2pConnActive(ctx, NewTransferPath(peerIdA, peerIdB, streamId))
	if err != nil {
		t.Fatal(err)
	}
	pair.active = activeValue.(*peerConn)
	pair.passive = passiveValue.(*peerConn)
	t.Cleanup(func() {
		pair.active.Close()
		pair.passive.Close()
		managerA.Close()
		managerB.Close()
		cancel()
		if err := router.Stop(); err != nil {
			t.Errorf("stop router: %v", err)
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	for !pair.active.Connected() || !pair.passive.Connected() {
		if deadline.Before(time.Now()) {
			t.Fatal("vnet association did not connect")
		}
		time.Sleep(time.Millisecond)
	}
	pair.activeFast = pair.active
	pair.passiveFast = pair.passive
	for !pair.activeFast.FastPathReady() || !pair.passiveFast.FastPathReady() {
		if deadline.Before(time.Now()) {
			t.Fatalf("fast path did not bind: active=%t passive=%t",
				pair.activeFast.FastPathReady(), pair.passiveFast.FastPathReady())
		}
		time.Sleep(time.Millisecond)
	}
	return pair
}

// rtpLossFilter drops active-to-passive RTP packets with probability p from
// a seeded source, so a run is repeatable; every other chunk passes.
func rtpLossFilter(activeIp net.IP, p float64, seed int64) (func(vnet.Chunk) bool, *atomic.Bool) {
	var enabled atomic.Bool
	random := rand.New(rand.NewSource(seed))
	return func(chunk vnet.Chunk) bool {
		if !enabled.Load() || !rtpUdpPayload(chunk.UserData()) {
			return true
		}
		source, ok := chunk.SourceAddr().(*net.UDPAddr)
		if !ok || !source.IP.Equal(activeIp) {
			return true
		}
		return p <= random.Float64()
	}, &enabled
}

// measureFastPathMessageLoss sends count messages of one size and returns the
// fraction that never reassembled on the passive side.
func measureFastPathMessageLoss(
	t testing.TB,
	pair *flightGateVnetPair,
	size int,
	count int,
) float64 {
	t.Helper()
	message := bytes.Repeat([]byte{0x5a}, size)
	received := make(chan struct{}, count)
	drainDone := make(chan struct{})
	drainCtx, drainCancel := context.WithCancel(pair.ctx)
	go func() {
		defer close(drainDone)
		for {
			select {
			case <-drainCtx.Done():
				return
			case incoming := <-pair.passiveFast.FastPathMessages():
				MessagePoolReturn(incoming.message)
				received <- struct{}{}
			}
		}
	}()
	for index := 0; index < count; index += 1 {
		if _, err := pair.activeFast.WriteFastPathMessage(message); err != nil {
			t.Fatal(err)
		}
		// pace below the vnet's per-socket queue so queue drops do not
		// masquerade as link loss
		time.Sleep(200 * time.Microsecond)
	}
	// wait past the reassembly timeout for stragglers
	time.Sleep(p2pFastPathReassemblyTimeout / 2)
	drainCancel()
	<-drainDone
	return 1 - float64(len(received))/float64(count)
}

// M5. One lost RTP fragment loses the whole message, so message loss on the
// fast path is 1-(1-p)^n for n fragments: the lane's loss grows with the
// Packs the flight controller lets it carry. Characterisation; passes today
// and records reassembly evictions and the fragment histogram.
func TestFastPathMessageLossFollowsFragmentCount(t *testing.T) {
	if testing.Short() {
		t.Skip("vnet fast path loss sweep")
	}
	sizes := []int{1000, 2000, 9000, 16000}
	for _, p := range []float64{0.01, 0.03} {
		filter, enabled := rtpLossFilter(net.ParseIP("10.3.0.1"), p, 20260910)
		pair := newFlightGateVnetPair(t, filter, nil)
		enabled.Store(true)
		for _, size := range sizes {
			const count = 300
			fragments := p2pFastPathFragmentCount(size)
			expected := 1 - math.Pow(1-p, float64(fragments))
			measured := measureFastPathMessageLoss(t, pair, size, count)
			// binomial noise on 300 samples plus the vnet's own jitter
			tolerance := 2.5*math.Sqrt(expected*(1-expected)/count) + 0.02
			t.Logf("p=%.2f fragments=%d expected=%.3f measured=%.3f evictions=%d histogram=%v",
				p, fragments, expected, measured,
				pair.passiveStats.Snapshot().FastReassemblyEvictionCount,
				pair.activeStats.Snapshot().FastSendFragmentHistogram)
			if math.Abs(measured-expected) > tolerance {
				t.Fatalf("p=%.2f fragments=%d message loss %.3f, want %.3f±%.3f",
					p, fragments, measured, expected, tolerance)
			}
		}
		enabled.Store(false)
	}
}

// M6. After the fast path is ready, its RTP packets are blackholed in the
// active-to-passive direction while STUN consent and DTLS keep flowing. The
// association must be retired within the configured no-progress bound so the
// route generation changes and the sender's flight resets. Expected red on
// the tree this was written against: nothing observes fast-path delivery.
func TestFastPathBlackholeRetiresRouteAndResetsFlight(t *testing.T) {
	if testing.Short() {
		t.Skip("vnet fast path blackhole")
	}
	const noProgressTimeout = 300 * time.Millisecond
	var blackhole atomic.Bool
	activeIp := net.ParseIP("10.3.0.1")
	filter := func(chunk vnet.Chunk) bool {
		if !blackhole.Load() || !rtpUdpPayload(chunk.UserData()) {
			return true
		}
		source, ok := chunk.SourceAddr().(*net.UDPAddr)
		return !ok || !source.IP.Equal(activeIp)
	}
	pair := newFlightGateVnetPair(t, filter, func(active, passive *WebRtcSettings) {
		active.FastPathNoProgressTimeout = noProgressTimeout
	})
	// a healthy lane delivers and is never retired by the bound
	if loss := measureFastPathMessageLoss(t, pair, 1000, 20); loss != 0 {
		t.Fatalf("healthy fast path lost %.2f of its messages", loss)
	}
	select {
	case <-pair.active.ctx.Done():
		t.Fatalf("healthy association was retired: %v", context.Cause(pair.active.ctx))
	case <-time.After(2 * noProgressTimeout):
	}

	blackhole.Store(true)
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		message := bytes.Repeat([]byte{0x3c}, 1000)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-pair.active.ctx.Done():
				return
			case <-ticker.C:
				if _, err := pair.activeFast.WriteFastPathMessage(message); err != nil {
					return
				}
			}
		}
	}()
	// ICE consent is still flowing: this is not the ordinary ICE failure path
	time.Sleep(noProgressTimeout / 2)
	if state := pair.active.pc.ICEConnectionState(); state != webrtc.ICEConnectionStateConnected &&
		state != webrtc.ICEConnectionStateCompleted {
		t.Fatalf("ICE did not stay connected through the RTP blackhole: %s", state)
	}
	select {
	case <-pair.active.ctx.Done():
	case <-time.After(3 * noProgressTimeout):
		t.Fatalf("fast path blackhole did not retire the association within %s", 3*noProgressTimeout)
	}
	<-writeDone
	cause := context.Cause(pair.active.ctx)
	if cause == nil || !strings.Contains(cause.Error(), "fast path no progress") {
		t.Fatalf("retirement cause = %v, want fast path no progress", cause)
	}
}

// BenchmarkStreamFastWebRtcRouteLossSweep reports message loss and
// reassembly evictions per 10^4 messages for the fast path under seeded vnet
// loss, across fragment counts (FLIGHTGATEFIX §6.1).
func BenchmarkStreamFastWebRtcRouteLossSweep(b *testing.B) {
	for _, p := range []float64{0, 0.01, 0.03} {
		for _, size := range []int{1000, 9000, 16000} {
			b.Run(fmt.Sprintf("loss=%.2f/fragments=%d", p, p2pFastPathFragmentCount(size)), func(b *testing.B) {
				filter, enabled := rtpLossFilter(net.ParseIP("10.3.0.1"), p, 20260910)
				pair := newFlightGateVnetPair(b, filter, nil)
				enabled.Store(true)
				b.ResetTimer()
				loss := measureFastPathMessageLoss(b, pair, size, max(b.N, 100))
				b.StopTimer()
				snapshot := pair.passiveStats.Snapshot()
				b.ReportMetric(loss*1e4, "msg-loss/10k")
				b.ReportMetric(float64(snapshot.FastReassemblyEvictionCount)*1e4/float64(max(b.N, 100)), "evictions/10k")
			})
		}
	}
}

// Compatibility for 13.3: a peer from before progress reports drops each
// report as one malformed fragment (fastDropCount) and never reports back,
// so a new sender facing it must not retire a healthy lane.
func TestFastPathProgressReportIsHarmlessToOldReceiver(t *testing.T) {
	if testing.Short() {
		t.Skip("vnet fast path compatibility")
	}
	const noProgressTimeout = 300 * time.Millisecond
	pair := newFlightGateVnetPair(t, nil, func(active, passive *WebRtcSettings) {
		active.FastPathNoProgressTimeout = noProgressTimeout
		passive.FastPathNoProgressTimeout = noProgressTimeout
		passive.oldStyleFastPathReceiverForTest = true
	})
	// the old receiver still gets every message
	if loss := measureFastPathMessageLoss(t, pair, 1000, 20); loss != 0 {
		t.Fatalf("old-style receiver lost %.2f of the messages", loss)
	}
	// the new sender keeps writing for longer than its bound and is not retired
	message := bytes.Repeat([]byte{0x3c}, 1000)
	deadline := time.Now().Add(3 * noProgressTimeout)
	for time.Now().Before(deadline) {
		if _, err := pair.activeFast.WriteFastPathMessage(message); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	select {
	case <-pair.active.ctx.Done():
		t.Fatalf("new sender retired a healthy lane to an old receiver: %v", context.Cause(pair.active.ctx))
	default:
	}
	activeFast := pair.active.fastPath.Load()
	passiveFast := pair.passive.fastPath.Load()
	// the active side received messages from nobody, so it sent no reports;
	// the passive side received and would have reported: with the old-style
	// parse it sent none, and every report the active side does send to it
	// counts as one drop, no more
	if activeFast.remoteReportSeen.Load() {
		t.Fatal("an old-style receiver produced a progress report")
	}
	if sent := passiveFast.progressReportsSent.Load(); sent != 0 {
		t.Fatalf("old-style receiver sent %d reports", sent)
	}
	// a report from the old side is impossible, so drops on the old side come
	// only from reports the new side sent for messages it received: none here
	if drops := pair.passiveStats.Snapshot().FastDropCount; drops != activeFast.progressReportsSent.Load() {
		t.Fatalf("old-style receiver drops = %d, reports sent to it = %d", drops, activeFast.progressReportsSent.Load())
	}
	// now the new side receives one message, reports, and the old side must
	// drop exactly that report without any other effect
	if loss := measureFastPathMessageLossReverse(t, pair, 1000, 1); loss != 0 {
		t.Fatal("the new side did not receive the reverse message")
	}
	time.Sleep(4 * p2pFastPathProgressReportInterval)
	sent := activeFast.progressReportsSent.Load()
	if sent == 0 {
		t.Fatal("the new side received a message and reported nothing")
	}
	deadline = time.Now().Add(time.Second)
	for pair.passiveStats.Snapshot().FastDropCount < sent {
		if deadline.Before(time.Now()) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if drops := pair.passiveStats.Snapshot().FastDropCount; drops != sent {
		t.Fatalf("old-style receiver dropped %d packets for %d reports", drops, sent)
	}
	select {
	case <-pair.passive.ctx.Done():
		t.Fatalf("old-style receiver was retired: %v", context.Cause(pair.passive.ctx))
	default:
	}
}

// measureFastPathMessageLossReverse sends count messages from the passive
// side and returns the fraction the active side never reassembled.
func measureFastPathMessageLossReverse(
	t testing.TB,
	pair *flightGateVnetPair,
	size int,
	count int,
) float64 {
	t.Helper()
	message := bytes.Repeat([]byte{0x5a}, size)
	received := 0
	for index := 0; index < count; index += 1 {
		if _, err := pair.passiveFast.WriteFastPathMessage(message); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.After(2 * time.Second)
	for received < count {
		select {
		case incoming := <-pair.activeFast.FastPathMessages():
			MessagePoolReturn(incoming.message)
			received += 1
		case <-deadline:
			return 1 - float64(received)/float64(count)
		}
	}
	return 0
}

// §13.6: with size-aware admission on, the native carrier refuses frames
// over FastPathMaximumFragmentCount fragments, and once the Transfer flight
// sits at its loss floor the sequence writes frames over
// FastPathLossyFragmentCount fragments reliable-only. Off by default.
func TestP2pSizeAwareAdmissionBoundsFastPathFrames(t *testing.T) {
	settings := DefaultP2pTransportSettings()
	if settings.FastPathSizeAwareAdmission {
		t.Fatal("size-aware admission must be off by default until the benchmark sweep")
	}
	if settings.FastPathMaximumFragmentCount != 8 || settings.FastPathLossyFragmentCount != 2 {
		t.Fatalf("defaults = %d/%d fragments, want 8/2", settings.FastPathMaximumFragmentCount, settings.FastPathLossyFragmentCount)
	}
	settings.FastPathSizeAwareAdmission = true
	settings.DataPlaneMode = P2pDataPlaneModeFastOnly
	send := &P2pSendTransport{settings: settings}
	properties := p2pTransferCarrierProperties(send)
	if !properties.Unreliable {
		t.Fatal("fast-only carrier is not unreliable")
	}
	small := 2 * p2pFastPathFragmentPayloadByteCount
	large := 9 * p2pFastPathFragmentPayloadByteCount
	if !properties.unreliableForMessageByteCount(small) {
		t.Fatal("a small frame was refused by the fast path")
	}
	if properties.unreliableForMessageByteCount(large) {
		t.Fatal("a frame over the fragment cap was admitted to the fast path")
	}
	if properties.unreliableLossyMaxMessageByteCount != ByteCount(2*p2pFastPathFragmentPayloadByteCount) {
		t.Fatalf("lossy cap = %d bytes", properties.unreliableLossyMaxMessageByteCount)
	}

	sendSettings := DefaultSendBufferSettings()
	sendSettings.UnreliableInitialFlightByteCount = 8192
	sendSettings.UnreliableMinimumFlightByteCount = 8192
	sendSettings.UnreliableMaximumFlightByteCount = 65536
	controller := newSendFlightController(sendSettings)
	policy := transferFlightPolicySnapshot{
		generation:             1,
		limited:                true,
		reliableRouteAvailable: true,
		lossyMaxByteCount:      properties.unreliableLossyMaxMessageByteCount,
	}
	controller.applyPolicy(policy)
	sequence := &SendSequence{client: &Client{}, flightController: controller, sendBufferSettings: sendSettings}
	if sequence.reliableOnlyWrite(policy, ByteCount(3*p2pFastPathFragmentPayloadByteCount)) {
		t.Fatal("a growing flight wrote a mid-size frame reliable-only")
	}
	controller.reduceForLoss()
	if !controller.atFloor() {
		t.Fatal("one loss from the initial limit did not pin the flight to its floor")
	}
	if !sequence.reliableOnlyWrite(policy, ByteCount(3*p2pFastPathFragmentPayloadByteCount)) {
		t.Fatal("at the floor a frame over the lossy cap still rode the fast path")
	}
	if sequence.reliableOnlyWrite(policy, ByteCount(p2pFastPathFragmentPayloadByteCount)) {
		t.Fatal("at the floor a one-fragment frame was pushed off the fast path")
	}
}
