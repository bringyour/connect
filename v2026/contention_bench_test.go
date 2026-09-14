package connect

// contention micro-benchmarks for the per-packet locking paths.
// these isolate the per-packet route-selector and multi-client send dispatch
// so the lock + allocation costs are visible (run with -benchmem). they guard
// against regressions in the per-packet lock/allocation budget.

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// isolates the route-selector write hot path (one active route, the common
// case). before the snapshot change this took the selector mutex + allocated a
// new []Route in GetActiveRoutes and took the monitor lock in NotifyChannel on
// every call; after, it reads an immutable snapshot lock-free with no
// allocation.
func BenchmarkRouteSelectorWrite(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sel := NewMultiRouteSelector(ctx, "bench", nil, SourceId(NewId()), true)
	route := make(chan []byte, 8192)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-route:
			}
		}
	}()
	sel.updateTransport(NewSendGatewayTransport(), []Route{route})

	frame := make([]byte, 1400)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i += 1 {
		if err := sel.Write(ctx, frame, -1); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// Compares the atomic writer-lifecycle admission with the previous immutable
// snapshot load and direct route send. Both cases retain identical channel
// transfer work, so their delta isolates the teardown-safety cost.
func BenchmarkRouteSnapshotWriterAdmission(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	selector := NewMultiRouteSelector(ctx, "bench", nil, SourceId(NewId()), true)
	defer selector.Close()
	route := make(chan []byte, 1)
	selector.updateTransport(NewSendGatewayTransport(), []Route{route})
	frame := make([]byte, 1400)

	b.Run("immutable_snapshot_control", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			snapshot := selector.activeRoutesSnapshot.Load()
			snapshot.routes[0] <- frame
			<-route
		}
	})
	b.Run("atomic_writer_admission", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			snapshot := selector.acquireWriterSnapshot()
			snapshot.routes[0] <- frame
			snapshot.releaseWriter()
			<-route
		}
	})
}

// Isolates the extra ACK-only probe. Default/server transports keep the
// process-wide registration count at zero, so their cost is one atomic load
// and no route-snapshot admission or map lookup.
func BenchmarkRouteSelectorH1AckPriorityProbe(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	selector := NewMultiRouteSelector(ctx, "bench", nil, SourceId(NewId()), true)
	defer selector.Close()
	route := make(Route, 1)
	selector.updateTransport(
		NewSendGatewayTransportWithType(TransportTypeH1),
		[]Route{route},
	)
	frame := make([]byte, 128)

	b.Run("server_disabled", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if success, _ := selector.tryWriteH1AckPriorityWithCarrierPreference(
				frame,
				TransportTypeH1,
			); success {
				b.Fatal("disabled priority probe accepted a frame")
			}
		}
	})

	priorityRoute := make(Route, 1)
	registerH1AckPriorityRoute(route, priorityRoute)
	defer unregisterH1AckPriorityRoute(route)
	b.Run("mobile_enabled", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if success, _ := selector.tryWriteH1AckPriorityWithCarrierPreference(
				frame,
				TransportTypeH1,
			); !success {
				b.Fatal("enabled priority probe rejected a ready frame")
			}
			<-priorityRoute
		}
	})
}

// isolates the route-selector read hot path (one active route).
func BenchmarkRouteSelectorRead(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sel := NewMultiRouteSelector(ctx, "bench", nil, SourceId(NewId()), false)
	route := make(chan []byte, 8192)
	frame := make([]byte, 1400)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case route <- frame:
			}
		}
	}()
	sel.updateTransport(NewReceiveGatewayTransport(), []Route{route})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i += 1 {
		if _, err := sel.Read(ctx, -1); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// Exercises the finite-deadline path with a permanently full route. The first
// iteration lazily creates the selector timer; steady state must reuse it so
// intentional route backpressure does not create timer garbage and GC pauses.
func BenchmarkRouteSelectorWriteTimeout(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sel := NewMultiRouteSelector(ctx, "bench", nil, SourceId(NewId()), true)
	defer sel.Close()
	route := make(chan []byte, 1)
	route <- []byte{0}
	sel.updateTransport(NewSendGatewayTransport(), []Route{route})

	frame := make([]byte, 1400)
	if success, err := sel.WriteDetailed(ctx, frame, time.Microsecond); success || err != nil {
		b.Fatalf("warm timeout: success=%t err=%v", success, err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if success, err := sel.WriteDetailed(ctx, frame, time.Microsecond); success || err != nil {
			b.Fatalf("timeout: success=%t err=%v", success, err)
		}
	}
	b.StopTimer()
}

// drives parallel egress flows through the RemoteUserNatMultiClient send
// dispatch with the provider draining (no echo), to exercise the per-packet
// send dispatch path under parallel flows.
func BenchmarkMultiClientEgressParallel(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	providerClientId := NewId()
	settings := DefaultClientSettingsWithBufferSize(256)
	providerClient := NewClient(ctx, providerClientId, NewNoContractClientOob(), settings)
	defer providerClient.Cancel()

	providerClient.AddReceiveCallback(func(source TransferPath, frames []*protocol.Frame, peer Peer) {})

	natClient, err := testingNewMultiClient(
		ctx,
		providerClient,
		func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {},
	)
	if err != nil {
		b.Fatal(err)
	}
	defer natClient.Close()

	clientId := NewId()
	source := SourceId(clientId)

	send := func(s int) {
		template, _ := tcp4Packet(s, 0, 0, 0)
		packet := MessagePoolCopy(template)
		if !natClient.SendPacket(source, protocol.ProvideMode_Network, packet, -1) {
			MessagePoolReturn(packet)
		}
	}

	g := runtime.GOMAXPROCS(0)
	for s := 1; s <= g; s += 1 {
		for i := 0; i < 32; i += 1 {
			send(s)
		}
	}

	var flowCounter atomic.Int32

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		s := int(flowCounter.Add(1))
		template, _ := tcp4Packet(s, 0, 0, 0)
		for pb.Next() {
			packet := MessagePoolCopy(template)
			if !natClient.SendPacket(source, protocol.ProvideMode_Network, packet, -1) {
				MessagePoolReturn(packet)
			}
		}
	})
	b.StopTimer()
}

// drives bidirectional traffic through the RemoteUserNatMultiClient: parallel
// egress senders plus a provider that echoes every packet back, so the parent
// stateLock, per-channel stats, transfer send/receive buffers, and route
// selector are all exercised on both the send and receive paths. this is the
// measurement vehicle for the de-contention work; profile with -mutexprofile.
func BenchmarkMultiClientBidirectional(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	providerClientId := NewId()
	settings := DefaultClientSettingsWithBufferSize(256)
	providerClient := NewClient(ctx, providerClientId, NewNoContractClientOob(), settings)
	defer providerClient.Cancel()

	type providerEcho struct {
		packet      []byte
		destination Id
		transferKey TransferKey
	}
	providerEchoes := make(chan providerEcho, 1024)
	var providerEchoWaitGroup sync.WaitGroup
	workerCount := min(runtime.GOMAXPROCS(0), 4)
	for range workerCount {
		providerEchoWaitGroup.Add(1)
		go func() {
			defer providerEchoWaitGroup.Done()
			for {
				select {
				case <-ctx.Done():
					for {
						select {
						case echo := <-providerEchoes:
							MessagePoolReturn(echo.packet)
						default:
							return
						}
					}
				case echo := <-providerEchoes:
					success, _ := providerClient.sendRawWithTimeoutDetailed(
						protocol.MessageType_IpIpPacketFromProvider,
						echo.packet,
						echo.destination,
						nil,
						0,
						time.Second,
						echo.transferKey,
					)
					if !success {
						MessagePoolReturn(echo.packet)
					}
				}
			}
		}()
	}
	// the provider echoes each received packet back with the path reversed, so
	// the echo lands on the originating flow's update (the steady-state ingress
	// path). The shared receive callback only performs a bounded zero-wait
	// handoff; fixed workers own the blocking transfer sends.
	providerReceiveUnsub := providerClient.AddReceiveCallback(func(source TransferPath, frames []*protocol.Frame, peer Peer) {
		for _, frame := range frames {
			packet, err := ipPacketToProviderBytes(frame)
			if err != nil {
				continue
			}
			var ipPath IpPath
			payload, err := parseIpPathWithPayloadBorrowed(packet, &ipPath)
			if err != nil {
				continue
			}
			reversed := ipPath.ReverseValue()
			echo := ipOosPacket(&reversed, payload)
			select {
			case providerEchoes <- providerEcho{
				packet:      echo,
				destination: source.SourceId,
				transferKey: peer.TransferKey,
			}:
			default:
				MessagePoolReturn(echo)
			}
		}
	})
	defer func() {
		providerReceiveUnsub()
		cancel()
		providerClient.Cancel()
		providerEchoWaitGroup.Wait()
	}()

	var receiveCount atomic.Int64
	natClient, err := testingNewMultiClient(
		ctx,
		providerClient,
		func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
			receiveCount.Add(1)
		},
	)
	if err != nil {
		b.Fatal(err)
	}
	defer natClient.Close()

	clientId := NewId()
	source := SourceId(clientId)

	send := func(s int) {
		template, _ := tcp4Packet(s, 0, 0, 0)
		packet := MessagePoolCopy(template)
		if !natClient.SendPacket(source, protocol.ProvideMode_Network, packet, -1) {
			MessagePoolReturn(packet)
		}
	}

	g := runtime.GOMAXPROCS(0)
	for s := 1; s <= g; s += 1 {
		for i := 0; i < 32; i += 1 {
			send(s)
		}
	}

	var flowCounter atomic.Int32
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		s := int(flowCounter.Add(1))
		template, _ := tcp4Packet(s, 0, 0, 0)
		for pb.Next() {
			packet := MessagePoolCopy(template)
			if !natClient.SendPacket(source, protocol.ProvideMode_Network, packet, -1) {
				MessagePoolReturn(packet)
			}
		}
	})
	b.StopTimer()
}
