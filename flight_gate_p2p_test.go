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
