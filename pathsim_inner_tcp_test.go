package connect

import (
	"context"
	"net"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/urnetwork/connect/protocol"
)

// S6. A segment lost after the transfer layer has delivered it is never
// retransmitted by the provider's TCP.
//
// THROUGHPUT-RIG-REVIEW §6, the open single-flow wedge: the provider
// terminates TCP and relies on Transfer for delivery, so its TCP does not
// retransmit data segments (ip.go, "Packet flow from the user-NAT to the
// source is assumed to never require user-NAT retransmission"). Any inner
// segment lost between the client's transfer receive and its kernel TCP —
// the client's backlog, its pruning under bursts, a tun write refused — is a
// permanent hole: the kernel holds megabytes out of order, its window
// collapses, and the download stops for the rest of the run.
//
// What is in process here is the provider's half exactly: a `TcpSequence`
// against an in-memory origin, with this test as the client's stack. The
// origin offers 256 KiB; the client acknowledges every segment except the
// third, which it drops, and answers everything after it with the duplicate
// acknowledgement a kernel would send. The provider then sends until the
// client's 64 KiB window closes and stops. Over sixty virtual seconds the
// dropped segment's sequence number is emitted exactly once: no
// retransmission timer fires on this side and duplicate acknowledgements
// trigger nothing, which is the mechanism by which the rig's flow wedges.
//
// What is not in process, and where the wedge is still being hunted: the
// client's kernel TCP behind the tun, and the exact site that loses the
// segment after delivery. That needs a client stack — the gVisor tun of
// tun.go — driven inside a virtual-time bubble against a transfer receive
// that can be told to drop one delivered packet, and neither seam exists:
// the tun has no packet-drop hook between its receive callback and its
// stack, and the gVisor stack's own goroutines and clocks are not known to
// run under synctest. Until they do, this cell pins the provider's half and
// the rig pins the whole.
//
// Produced: 65 segments of 1040 bytes emitted, all at the origin's first
// instant since nothing in this cell has a delay, up to the hole plus
// 65535 bytes where the window closed; the third segment emitted once;
// nothing emitted in the sixty seconds after the window closed.
func TestPathsimS6InnerSegmentLossIsNotRetransmittedByTheProvider(t *testing.T) {
	// the pool's lazy initialiser, born outside the bubble
	MessagePoolReturn(MessagePoolGet(64))

	synctest.Test(t, func(t *testing.T) {
		assertMessagePoolOwnership(t)

		const initialSynSeq = uint32(1000)
		const originByteCount = 256 * 1024
		const lostSegment = 3
		const clientWindow = uint16(65535)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sequenceSocket, originSocket := net.Pipe()
		settings := DefaultTcpBufferSettingsWithBufferSize(64)
		settings.ReadTimeout = 120 * time.Second
		settings.WriteTimeout = 120 * time.Second
		settings.IdleTimeout = 120 * time.Second
		settings.DialContextSettings = &DialContextSettings{
			DialContext: func(dialCtx context.Context, network string, addr string) (net.Conn, error) {
				return sequenceSocket, nil
			},
		}

		type segment struct {
			ordinal   int
			seq       uint32
			byteCount int
			at        time.Time
		}
		var stateLock sync.Mutex
		segments := []segment{}
		synAck := make(chan uint32, 1)
		arrivals := make(chan segment, 1024)
		source := SourceId(NewId())
		sourceIp := net.IPv4(192, 0, 2, 1).To4()
		destinationIp := net.IPv4(203, 0, 113, 7).To4()

		receive := func(_ TransferPath, _ protocol.ProvideMode, _ *IpPath, packet []byte) {
			_, packetSourceIp, packetDestinationIp, transport, ok := parseIpv4(packet)
			if !ok {
				return
			}
			tcp := &parsedTcp{}
			if !parseTcpPacket(packetSourceIp, packetDestinationIp, transport, tcp) {
				return
			}
			if tcp.syn {
				select {
				case synAck <- tcp.seq:
				default:
				}
				return
			}
			if len(tcp.payload) == 0 {
				return
			}
			stateLock.Lock()
			s := segment{ordinal: len(segments) + 1, seq: tcp.seq, byteCount: len(tcp.payload), at: time.Now()}
			segments = append(segments, s)
			stateLock.Unlock()
			select {
			case arrivals <- s:
			default:
				t.Errorf("S6: the arrival queue overflowed")
			}
		}
		sequence := NewTcpSequence(
			ctx,
			receive,
			source,
			protocol.ProvideMode_Network,
			4,
			sourceIp,
			40001,
			destinationIp,
			443,
			initialSynSeq,
			settings,
		)
		runDone := make(chan struct{})
		go func() {
			defer close(runDone)
			sequence.Run()
		}()
		defer func() {
			sequence.Cancel()
			cancel()
			originSocket.Close()
			<-runDone
		}()

		// the client's packets: a pool-owned IPv4/TCP header with no payload
		clientSeq := initialSynSeq + 1
		send := func(syn bool, ackNumber uint32) {
			headerByteCount := Ipv4HeaderSizeWithoutExtensions + TcpHeaderSizeWithoutExtensions
			packet := MessagePoolGet(headerByteCount)
			clear(packet)
			packet[0] = 0x45
			item := &TcpSendItem{
				source:      source,
				provideMode: protocol.ProvideMode_Network,
				tcp: parsedTcp{
					seq:        clientSeq,
					syn:        syn,
					ack:        !syn,
					ackNumber:  ackNumber,
					windowSize: clientWindow,
					payload:    packet[headerByteCount:],
				},
				ipPacket: packet,
			}
			if syn {
				item.tcp.seq = initialSynSeq
			}
			ok, err := sequence.send(item, -1)
			if err != nil || !ok {
				MessagePoolReturn(packet)
				t.Fatalf("S6: the client's packet was not accepted: %v", err)
			}
		}

		send(true, 0)
		var providerIsn uint32
		select {
		case providerIsn = <-synAck:
		case <-time.After(5 * time.Second):
			t.Fatal("S6: no SYN-ACK from the provider")
		}
		expected := providerIsn + 1
		send(false, expected)

		// the origin: a fast server writing everything it has
		origin := make([]byte, originByteCount)
		for i := range origin {
			origin[i] = byte(i)
		}
		writeDone := make(chan error, 1)
		go func() {
			_, err := originSocket.Write(origin)
			writeDone <- err
		}()

		// The client's stack: acknowledge in order, drop one segment, and
		// answer everything past it with the duplicate acknowledgement.
		hole := uint32(0)
		dropped := false
		windowClosedAt := time.Time{}
		deadline := time.After(60 * time.Second)
		receiving := true
		for receiving {
			select {
			case s := <-arrivals:
				if !dropped && s.ordinal == lostSegment {
					hole = s.seq
					dropped = true
					// dropped: no acknowledgement advances past it
					continue
				}
				if !dropped && s.seq == expected {
					expected += uint32(s.byteCount)
					send(false, expected)
					continue
				}
				// past the hole: a duplicate acknowledgement of the hole
				send(false, hole)
				if uint32(int64(hole)+int64(clientWindow)) <= s.seq+uint32(s.byteCount) {
					windowClosedAt = time.Now()
				}
			case <-deadline:
				receiving = false
			}
		}

		stateLock.Lock()
		defer stateLock.Unlock()
		if !dropped {
			t.Fatalf("S6: only %d segments arrived, so no segment was dropped", len(segments))
		}
		emissions := 0
		lastAt := time.Time{}
		for _, s := range segments {
			if s.seq == hole {
				emissions++
			}
			if lastAt.Before(s.at) {
				lastAt = s.at
			}
		}
		if emissions != 1 {
			t.Errorf("S6: the dropped segment at %d was emitted %d times over sixty seconds; the provider's TCP is not supposed to retransmit, and if it now does the wedge of the rig review's §6 has a different shape", hole, emissions)
		}
		if windowClosedAt.IsZero() {
			t.Errorf("S6: the client's window never closed behind the hole; %d segments arrived", len(segments))
		} else if windowClosedAt.Add(time.Second).Before(lastAt) {
			t.Errorf("S6: the provider emitted a segment %s after the client's window closed", lastAt.Sub(windowClosedAt))
		}
		if len(segments) == 0 || segments[len(segments)-1].seq+uint32(segments[len(segments)-1].byteCount) == providerIsn+1+originByteCount {
			t.Errorf("S6: the whole origin was delivered, so the window did not bind; %d segments", len(segments))
		}
		t.Logf("S6: %d segments emitted, the hole at %d emitted %d time(s), window closed after %s of virtual time, last emission at %s",
			len(segments), hole-providerIsn-1, emissions, windowClosedAt.Sub(segments[0].at), lastAt.Sub(segments[0].at))
	})
}
