package connect

// Performance tests of FLIGHTGATEFIX.md §6.1 for the transfer layer.

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

// flightGatePeerModel acknowledges Packs read from two routes the way two
// real lanes would: the unreliable lane after unreliableDelay with a seeded
// drop fraction, the reliable lane after reliableDelay, every ack selective
// so a dropped Pack is recovered by the sender's own RTO.
type flightGatePeerModel struct {
	client          *Client
	peerId          Id
	fromPeer        Route
	unreliableDelay time.Duration
	reliableDelay   time.Duration
	unreliableDrop  float64
	random          *rand.Rand
	randomLock      sync.Mutex
	dropped         atomic.Uint64
}

func (self *flightGatePeerModel) run(
	b *testing.B,
	route Route,
	delay time.Duration,
	drop float64,
	stop <-chan struct{},
) {
	for {
		select {
		case <-stop:
			return
		case transferFrameBytes := <-route:
			pack := decodeFlightGatePack(b, transferFrameBytes)
			if pack == nil {
				continue
			}
			if 0 < drop {
				self.randomLock.Lock()
				dropIt := self.random.Float64() < drop
				self.randomLock.Unlock()
				if dropIt {
					self.dropped.Add(1)
					continue
				}
			}
			time.AfterFunc(delay, func() {
				ackBytes, err := ProtoMarshal(&protocol.TransferFrame{
					TransferPath: TransferPath{
						SourceId:      self.peerId,
						DestinationId: self.client.ClientId(),
					}.ToProtobuf(),
					Ack: &protocol.Ack{
						MessageId:  pack.MessageId,
						SequenceId: pack.SequenceId,
						Selective:  true,
						Tag:        pack.Tag,
					},
				})
				if err != nil {
					return
				}
				select {
				case self.fromPeer <- ackBytes:
				case <-stop:
					MessagePoolReturn(ackBytes)
				}
			})
		}
	}
}

// BenchmarkSendSequenceMixedCarriers drives one sequence over an unreliable
// lane (fast, lossy) and a reliable lane (slow, clean) at once and reports
// Packs/s plus the iterations the sequence spent flight-blocked while the
// reliable route had capacity, the M1 share of any stall.
func BenchmarkSendSequenceMixedCarriers(b *testing.B) {
	for _, scenario := range []struct {
		name string
		drop float64
	}{
		{"clean", 0},
		{"loss=0.03", 0.03},
		{"loss=0.25", 0.25},
	} {
		b.Run(scenario.name, func(b *testing.B) {
			settings := flightGateSettings(kib(8))
			settings.SendBufferSettings.UnreliableMaximumFlightByteCount = kib(256)
			settings.SendBufferSettings.UnreliableFlightIncreaseByteCount = 1150
			client, peerId, fromPeer, _ := newFlightGateSender(b, settings)
			_, unreliable := addFlightGateRoute(b, client, TransportTypeP2p, 64, true)
			_, reliable := addFlightGateRoute(b, client, TransportTypeH1, 64, false)
			model := &flightGatePeerModel{
				client:   client,
				peerId:   peerId,
				fromPeer: fromPeer,
				random:   rand.New(rand.NewSource(20260910)),
			}
			stop := make(chan struct{})
			go model.run(b, unreliable, 20*time.Millisecond, scenario.drop, stop)
			go model.run(b, reliable, 150*time.Millisecond, 0, stop)
			defer close(stop)

			var acked atomic.Int64
			done := make(chan struct{}, 1)
			ack := func(err error) {
				if acked.Add(1) == int64(b.N) {
					done <- struct{}{}
				}
			}
			b.ResetTimer()
			start := time.Now()
			for index := 0; index < b.N; index += 1 {
				frame, err := ToFrame(&protocol.SimpleMessage{Content: "bench"}, DefaultProtocolVersion)
				if err != nil {
					b.Fatal(err)
				}
				if !client.SendWithTimeout(frame, peerId, ack, 30*time.Second) {
					MessagePoolReturn(frame.MessageBytes)
					b.Fatal("message not admitted")
				}
			}
			select {
			case <-done:
			case <-time.After(120 * time.Second):
				b.Fatalf("only %d of %d Packs acknowledged", acked.Load(), b.N)
			}
			elapsed := time.Since(start)
			b.StopTimer()
			recovery := client.SendRecoveryStats()
			b.ReportMetric(float64(b.N)/elapsed.Seconds(), "packs/s")
			b.ReportMetric(float64(recovery.UnreliableFlightWaitCount)/float64(b.N), "flight-waits/pack")
			b.ReportMetric(float64(recovery.UnreliableFlightBlockedWithReliableCapacity)/float64(b.N), "gated-with-reliable-capacity/pack")
			b.ReportMetric(float64(recovery.TimeoutResendWriteCount)/float64(b.N), "rto-resends/pack")
			b.ReportMetric(float64(recovery.SelectiveGapWriteCount)/float64(b.N), "gap-resends/pack")
			b.ReportMetric(float64(model.dropped.Load())/float64(b.N), "dropped/pack")
		})
	}
}

// BenchmarkReceiveSequenceAckWorkerUnderCarrierBlock measures the latency of
// an ACK for a Pack received over h1 while the receiver's p2p route, to
// which a previous Pack's ACK is pinned, is full (M2).
func BenchmarkReceiveSequenceAckWorkerUnderCarrierBlock(b *testing.B) {
	pair := newFlightGatePeerPair(b, 200*time.Millisecond)
	inP2p := pair.receiveRoute(b, TransportTypeP2p)
	inH1 := pair.receiveRoute(b, TransportTypeH1)
	outP2p := pair.ackRoute(b, TransportTypeP2p, 1, TransferCarrierProperties{Unreliable: true})
	outH1 := pair.ackRoute(b, TransportTypeH1, 64, TransferCarrierProperties{})
	fillFlightGateRoute(outP2p)
	index := 0
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		pair.deliver(b, index, inP2p)
		index += 1
		time.Sleep(50 * time.Millisecond)
		b.StartTimer()
		pack := pair.deliver(b, index, inH1)
		index += 1
		if !awaitFlightGateAck(b, outH1, pack, 10*time.Second) {
			b.Fatal("ACK for the h1-received Pack never arrived")
		}
	}
	b.StopTimer()
	stats := pair.receiver.ReceiveStats()
	b.ReportMetric(float64(stats.AckRouteWriteTimeoutByTransport[TransportTypeP2p])/float64(b.N), "p2p-ack-timeouts/op")
}
