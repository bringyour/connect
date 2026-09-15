package connect

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
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

	// the two gates, read directly rather than inferred: the advertised
	// version recorded for the destination's base class, and whether the
	// returns carried a valid scheduling key
	type laneGateReading struct {
		lanes           map[uint32]int
		observations    []logicalLaneGateObservation
		recordedVersion uint32
		versionRecorded bool
	}
	observeLane := func(clientLane uint32, advertised bool) laneGateReading {
		// the provider's own count, which is the setting a rollout turns on,
		// set before the client starts because the send loop reads its
		// settings from its own goroutine
		provider, _, client := newProviderSourceLifecycleTestFixtureWithClientSettings(
			t,
			NewNoContractClientOob(),
			func(settings *ClientSettings) {
				settings.SendBufferSettings.LogicalDataLaneCount = 8
				settings.SendBufferSettings.LaneFloorByteCount = ByteCount(256 * 1024)
			},
			nil,
		)

		var observationLock sync.Mutex
		observations := []logicalLaneGateObservation{}
		client.sendBuffer.logicalLaneGateObserverForTest.Store(
			&logicalLaneGateObserver{
				observe: func(observation logicalLaneGateObservation) {
					observationLock.Lock()
					defer observationLock.Unlock()
					observations = append(observations, observation)
				},
			},
		)
		t.Cleanup(func() {
			client.sendBuffer.logicalLaneGateObserverForTest.Store(nil)
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
		// The origin's data comes back through the NAT and out as returns. Wait
		// for the gate to have decided at least once rather than for a fixed
		// second: the advertised arm reads the base class out of that first
		// decision, and with no decision it would record the advertisement
		// against the zero base and the second flow would meet an
		// unadvertised destination. That was a one-in-ten flake.
		waitForObservations := func(atLeast int) int {
			deadline := time.Now().Add(10 * time.Second)
			for {
				observationLock.Lock()
				count := len(observations)
				observationLock.Unlock()
				if atLeast <= count || !time.Now().Before(deadline) {
					return count
				}
				time.Sleep(20 * time.Millisecond)
			}
		}
		if count := waitForObservations(1); count == 0 {
			t.Fatalf("the provider's returns never reached the lane gate, so this cell has nothing to read")
		}
		// and then let the flow settle, so nothing is still in flight when the
		// pool ownership check runs
		time.Sleep(time.Second)

		if advertised {
			// The destination's lane-zero class advertised support. In the
			// field this is recorded from an acknowledgement that matched an
			// outstanding item; here it is set directly for the exact base the
			// gate reported consulting, so the row isolates the gate under
			// test rather than the negotiation in front of it, and a second
			// flow then meets an advertised destination.
			observationLock.Lock()
			base := sendSequenceId{}
			if 0 < len(observations) {
				base = observations[len(observations)-1].base
			}
			observations = nil
			observationLock.Unlock()
			client.sendBuffer.mutex.Lock()
			client.sendBuffer.logicalLaneVersions[base] = transferLogicalLaneVersion
			client.sendBuffer.publishLogicalLaneVersionsWithLock()
			client.sendBuffer.mutex.Unlock()

			secondSyn := MessagePoolCopy(craftSecurityPacket(
				IpProtocolTcp,
				net.ParseIP("10.11.12.13"),
				54322,
				net.ParseIP("127.0.0.1"),
				originPort,
				true,
				nil,
			))
			secondIpPath, err := ParseIpPath(secondSyn)
			if err != nil {
				MessagePoolReturn(secondSyn)
				t.Fatalf("parse the second client SYN: %v", err)
			}
			withBorrowedMessage(secondSyn, func(secondSyn []byte) {
				provider.receiveTransferWithRecovery(
					SourceId(peerId),
					clientKey,
					protocol.ProvideMode_Public,
					receiveRecoveryModeTcpSocket,
					secondIpPath,
					secondSyn,
				)
			})
			if count := waitForObservations(1); count == 0 {
				t.Fatalf("the second flow's returns never reached the lane gate")
			}
			time.Sleep(time.Second)
		}

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
		base := sendSequenceId{
			Destination:    peerId,
			EncryptionRole: sequenceTlsRoleServer,
		}
		recordedVersion, versionRecorded := func() (uint32, bool) {
			client.sendBuffer.mutex.Lock()
			defer client.sendBuffer.mutex.Unlock()
			for id, version := range client.sendBuffer.logicalLaneVersions {
				if id.Destination == peerId {
					return version, true
				}
			}
			_ = base
			return 0, false
		}()

		observationLock.Lock()
		defer observationLock.Unlock()
		return laneGateReading{
			lanes:           lanes,
			observations:    append([]logicalLaneGateObservation{}, observations...),
			recordedVersion: recordedVersion,
			versionRecorded: versionRecorded,
		}
	}

	zeroReading := observeLane(0, false)
	dataReading := observeLane(3, false)
	advertisedReading := observeLane(0, true)
	zeroLanes, zeroObservations := zeroReading.lanes, zeroReading.observations
	dataLanes, dataObservations := dataReading.lanes, dataReading.observations

	summarise := func(observations []logicalLaneGateObservation) map[string]int {
		gates := map[string]int{}
		for _, observation := range observations {
			gates[observation.bindingGate] += 1
		}
		return gates
	}
	t.Logf(
		"a client on lane 0: provider return sequences by lane %v, gates %v, advertised version recorded=%t value=%d",
		zeroLanes,
		summarise(zeroObservations),
		zeroReading.versionRecorded,
		zeroReading.recordedVersion,
	)
	t.Logf(
		"a client on lane 3: provider return sequences by lane %v, gates %v, advertised version recorded=%t value=%d",
		dataLanes,
		summarise(dataObservations),
		dataReading.versionRecorded,
		dataReading.recordedVersion,
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

	// The purpose of dropping the lane from the reply key, asserted rather
	// than inferred from the pin's absence: with the destination's support
	// advertised, the provider's own gate reaches its hash. Which lane it
	// picks is the hash's business, so the gate is what this asserts.
	hashedCount := 0
	for _, observation := range advertisedReading.observations {
		if observation.bindingGate == "hashed" {
			hashedCount += 1
		}
	}
	t.Logf(
		"an advertised destination: provider return sequences by lane %v, gates %v",
		advertisedReading.lanes,
		summarise(advertisedReading.observations),
	)
	if hashedCount <= 0 {
		t.Errorf(
			"no return reached the provider's hash with the destination's support advertised; gates were %v, so dropping the lane from the reply key has not made the provider's own count reachable on the download direction",
			summarise(advertisedReading.observations),
		)
	}
}

// THROUGHPUTFIX §30.3. Enabling a nonzero lane count used to add an
// acquisition of the send buffer's mutex to every Pack, a lock every sequence
// of the client shares, because the gate read the advertised version out of a
// map the buffer guards. A count of zero returned before it, which is why the
// cost appeared only when a count was set: a harness arm with the count at
// eight and no lane ever engaging ran 13 to 17 per cent below the same fixture
// at zero over six repetitions with overlapping distributions, and the
// mechanism rather than the statistics is what makes that credible.
//
// The observable is the acquisition and not the throughput, because a
// throughput row at that magnitude would be a timing test and would flake. The
// buffer mutex is held here while the gate runs: a gate that takes it cannot
// finish, and a lock-free one is unaffected.
func TestLaneCountGateDoesNotTakeTheBufferLockPerPack(t *testing.T) {
	assertMessagePoolOwnership(t)

	// set before the client starts: the send loop reads its settings from its
	// own goroutine, so a write after NewClient is a data race
	_, _, client := newProviderSourceLifecycleTestFixtureWithClientSettings(
		t,
		NewNoContractClientOob(),
		func(settings *ClientSettings) {
			settings.SendBufferSettings.LogicalDataLaneCount = 8
			settings.SendBufferSettings.LaneFloorByteCount = ByteCount(256 * 1024)
		},
		nil,
	)

	// a Pack shaped like a provider's return: a valid scheduling key and no
	// explicit lane, so the gate runs its whole path
	sendPack := &SendPack{
		Destination:   NewId(),
		schedulingKey: ipSendSchedulingKey(udpTestPath(4)),
	}
	if !sendPack.schedulingKey.valid {
		t.Fatal("the scheduling key is not valid, so the gate would return before the version is read")
	}

	gated := make(chan uint32, 1)
	var unlockOnce sync.Once
	client.sendBuffer.mutex.Lock()
	unlock := func() { unlockOnce.Do(client.sendBuffer.mutex.Unlock) }
	defer unlock()
	go func() {
		gated <- client.sendBuffer.selectLogicalLane(sendPack)
	}()
	select {
	case <-gated:
	case <-time.After(2 * time.Second):
		t.Error("the lane gate did not complete while the send buffer mutex was held; enabling a count puts a client-wide lock acquisition on every Pack, which is the 13 to 17 per cent the harness measured with no lane ever engaging")
	}
	unlock()
}
