package connect

// FLIGHTGATEFIX §20.3. The fast path's reverse lane carries the peer's own
// messages and nothing else. §13.3's liveness reporter put an 11-byte
// control packet on it every 50 ms while the receiver's count changed,
// which on a 64 kbit/s cell-edge uplink is three times the queue's drain
// rate and tail-dropped the data behind it. Any future per-interval
// control packet must be measured on that uplink before it ships, so this
// guard fails the moment one returns.

import (
	"bytes"
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/transport/v4/vnet"
)

func TestFastPathWiresOnlyFragmentsAndWarmup(t *testing.T) {
	if testing.Short() {
		t.Skip("vnet fast path wire census")
	}
	const (
		activeToPassive  = 24
		passiveToActive  = 3
		messageByteCount = 600
	)
	passiveIp := net.ParseIP("10.3.0.2")
	var counting atomic.Bool
	var passiveEmitted atomic.Int64
	filter := func(chunk vnet.Chunk) bool {
		if !rtpUdpPayload(chunk.UserData()) {
			return true
		}
		source, ok := chunk.SourceAddr().(*net.UDPAddr)
		if ok && source.IP.Equal(passiveIp) && counting.Load() {
			passiveEmitted.Add(1)
		}
		return true
	}
	pair := newFlightGateVnetPair(t, filter, nil)

	drain := func(conn webRtcFastPathConn, into chan<- int) func() {
		done := make(chan struct{})
		ctx, cancel := context.WithCancel(pair.ctx)
		go func() {
			defer close(done)
			for {
				select {
				case <-ctx.Done():
					return
				case incoming := <-conn.FastPathMessages():
					byteCount := len(incoming.message)
					MessagePoolReturn(incoming.message)
					select {
					case into <- byteCount:
					default:
					}
				}
			}
		}()
		return func() {
			cancel()
			<-done
		}
	}
	atReceiver := make(chan int, 4096)
	atSender := make(chan int, 4096)
	stopReceiver := drain(pair.passiveFast, atReceiver)
	defer stopReceiver()
	stopSender := drain(pair.activeFast, atSender)
	defer stopSender()

	// settle the readiness exchange first: the warmup marker repeats only
	// until both receive workers are live, and it is not what this counts
	message := bytes.Repeat([]byte{0x5a}, messageByteCount)
	if _, err := pair.activeFast.WriteFastPathMessage(message); err != nil {
		t.Fatal(err)
	}
	select {
	case <-atReceiver:
	case <-time.After(10 * time.Second):
		t.Fatal("the fast path did not deliver its first message")
	}
	if _, err := pair.passiveFast.WriteFastPathMessage(message); err != nil {
		t.Fatal(err)
	}
	select {
	case <-atSender:
	case <-time.After(10 * time.Second):
		t.Fatal("the fast path did not deliver its first reverse message")
	}
	time.Sleep(2 * p2pFastPathWarmupTimeout)

	// from here the receiver's complete-message count changes steadily,
	// which is exactly the condition §13.3's reporter reported under
	counting.Store(true)
	for range activeToPassive {
		if _, err := pair.activeFast.WriteFastPathMessage(message); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	for range passiveToActive {
		if _, err := pair.passiveFast.WriteFastPathMessage(message); err != nil {
			t.Fatal(err)
		}
	}
	delivered := 0
	deadline := time.After(10 * time.Second)
	for delivered < passiveToActive {
		select {
		case <-atSender:
			delivered += 1
		case <-deadline:
			t.Fatalf("only %d of %d reverse messages arrived", delivered, passiveToActive)
		}
	}
	time.Sleep(500 * time.Millisecond)
	counting.Store(false)

	// one fragment per message at this size, and nothing else on the wire
	if emitted := passiveEmitted.Load(); emitted != passiveToActive {
		t.Fatalf(
			"the receiver put %d packets on its uplink while receiving %d messages and sending %d; "+
				"want exactly %d, its own fragments. A per-interval control packet on this lane is "+
				"three times a 64 kbit/s cell-edge queue's drain rate and tail-drops the data behind it",
			emitted, activeToPassive, passiveToActive, passiveToActive,
		)
	}
}
