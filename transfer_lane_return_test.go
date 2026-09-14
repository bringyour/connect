package connect

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

// THROUGHPUTFIX §30.2, the row that decides whether lanes can work at all on
// the direction they exist for. A provider's return carries the client's whole
// transfer key with only the companion contract bit changed, and the return
// send passes that key as a send option, which makes the lane explicit. Lane
// selection gives an explicit key precedence over the hashed count, so if the
// reading is right a client at lane zero pins every return to lane zero
// whatever the provider's own count is.
//
// Three outcomes are distinguished rather than one asserted, because which one
// holds decides whether a lane rollout needs a reply-key change beside the
// lock fix and the floor. The gate's own view is read directly, so the row
// says which gate binds rather than inferring it.
func TestProviderReturnsRideTheClientsLane(t *testing.T) {
	assertMessagePoolOwnership(t)

	// a loopback origin, so the NAT has something real to return from
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	accepted := make(chan net.Conn, 8)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepted <- conn
			// server-first, so the provider has a return to make
			conn.Write([]byte("origin data"))
		}
	}()
	t.Cleanup(func() {
		for {
			select {
			case conn := <-accepted:
				conn.Close()
			default:
				return
			}
		}
	})
	originPort := listener.Addr().(*net.TCPAddr).Port

	observeLane := func(clientLane uint32) (map[uint32]int, []logicalLaneGateObservation) {
		provider, _, client := newProviderSourceLifecycleTestFixture(t, nil)
		// the provider's own count, which is the setting a rollout turns on
		client.sendBuffer.sendBufferSettings.LogicalDataLaneCount = 8
		client.sendBuffer.sendBufferSettings.LaneFloorByteCount = ByteCount(256 * 1024)

		var observationLock sync.Mutex
		observations := []logicalLaneGateObservation{}
		client.sendBuffer.logicalLaneGateObserverForTest = func(observation logicalLaneGateObservation) {
			observationLock.Lock()
			defer observationLock.Unlock()
			observations = append(observations, observation)
		}
		t.Cleanup(func() {
			client.sendBuffer.logicalLaneGateObserverForTest = nil
		})

		peerId := NewId()
		route := make(chan []byte, 256)
		client.ContractManager().AddNoContractPeer(peerId)
		client.RouteManager().UpdateTransport(
			NewSendClientTransport(DestinationId(peerId)),
			[]Route{route},
		)
		drained := make(chan struct{})
		drainDone := make(chan struct{})
		go func() {
			defer close(drainDone)
			for {
				select {
				case transferFrameBytes := <-route:
					MessagePoolReturn(transferFrameBytes)
				case <-drained:
					return
				}
			}
		}()
		t.Cleanup(func() {
			close(drained)
			<-drainDone
			for {
				select {
				case transferFrameBytes := <-route:
					MessagePoolReturn(transferFrameBytes)
				default:
					return
				}
			}
		})

		// the client's pack, carrying the lane it is itself on
		clientKey := TransferKey{
			LogicalLane:    clientLane,
			EncryptionRole: protocol.SequenceRole_SequenceRoleServer,
		}
		syn := MessagePoolCopy(craftSecurityPacket(
			IpProtocolTcp,
			net.ParseIP("10.11.12.13"),
			54321,
			net.ParseIP("127.0.0.1"),
			originPort,
			true,
			nil,
		))
		ipPath, err := ParseIpPath(syn)
		if err != nil {
			MessagePoolReturn(syn)
			t.Fatalf("parse the client SYN: %v", err)
		}
		withBorrowedMessage(syn, func(syn []byte) {
			provider.receiveTransferWithRecovery(
				SourceId(peerId),
				clientKey,
				protocol.ProvideMode_Public,
				receiveRecoveryModeTcpSocket,
				ipPath,
				syn,
			)
		})
		// the origin's data comes back through the NAT and out as returns
		time.Sleep(time.Second)

		lanes := map[uint32]int{}
		func() {
			client.sendBuffer.mutex.Lock()
			defer client.sendBuffer.mutex.Unlock()
			for id := range client.sendBuffer.sendSequences {
				if id.Destination == peerId {
					lanes[id.LogicalLane] += 1
				}
			}
		}()
		observationLock.Lock()
		defer observationLock.Unlock()
		return lanes, append([]logicalLaneGateObservation{}, observations...)
	}

	zeroLanes, zeroObservations := observeLane(0)
	dataLanes, dataObservations := observeLane(3)

	summarise := func(observations []logicalLaneGateObservation) map[string]int {
		gates := map[string]int{}
		for _, observation := range observations {
			gates[observation.bindingGate] += 1
		}
		return gates
	}
	t.Logf(
		"a client on lane 0: provider return sequences by lane %v, gates %v",
		zeroLanes,
		summarise(zeroObservations),
	)
	t.Logf(
		"a client on lane 3: provider return sequences by lane %v, gates %v",
		dataLanes,
		summarise(dataObservations),
	)
	for _, observation := range zeroObservations {
		t.Logf(
			"  gate: explicit=%t explicitLane=%d schedulingValid=%t version=%d binding=%q lane=%d",
			observation.explicit,
			observation.explicitLane,
			observation.schedulingValid,
			observation.version,
			observation.bindingGate,
			observation.lane,
		)
		break
	}

	// The finding, asserted as the property a reply-key change would create.
	// Not "the return did not ride lane 3": the provider's own hash could
	// legitimately land there. What must change is which gate decides. Today
	// the client's explicit reply key short-circuits before the provider's
	// count, its advertised version and its scheduling key are consulted at
	// all, so the provider's setting is inert on the download path whatever
	// its value.
	explicitReturnCount := 0
	for _, observation := range append(zeroObservations, dataObservations...) {
		if observation.bindingGate == "explicit reply key" {
			explicitReturnCount += 1
		}
	}
	if 0 < explicitReturnCount {
		t.Errorf(
			"%d provider returns had their lane decided by the client's reply key rather than by the provider's own gate; a client on lane 0 drew returns on %v and a client on lane 3 drew returns on %v, so the provider's count is inert on the download path and a lane rollout needs a reply-key change beside the floor and the lock fix",
			explicitReturnCount,
			zeroLanes,
			dataLanes,
		)
	}

	// and the scheduling key is not what binds: the provider's return path
	// sets it from the flow, so it is valid on every return
	for _, observation := range zeroObservations {
		if observation.bindingGate == "no scheduling key" && observation.schedulingValid {
			t.Errorf("a return was refused a lane for want of a scheduling key it had")
		}
	}
}
