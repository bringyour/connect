package connect

// This file pins carrier-policy publication and the unreliable Transfer flight
// controller at both its state-machine and real SendSequence boundaries.

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// Reliable carriers never consult the configured unreliable byte limit.
func TestSendFlightControllerLeavesReliableCarrierUnlimited(t *testing.T) {
	settings := DefaultSendBufferSettings()
	settings.UnreliableInitialFlightByteCount = 100
	settings.UnreliableMinimumFlightByteCount = 100
	settings.UnreliableMaximumFlightByteCount = 400
	controller := newSendFlightController(settings)
	controller.applyPolicy(transferFlightPolicySnapshot{
		generation: 1,
		limited:    false,
	})

	controller.send(10 * 1024)
	if !controller.canSend() {
		t.Fatal("reliable carrier was constrained by unreliable flight settings")
	}
}

// Cold progress grows by newly acknowledged bytes, while a proven gap exits
// slow start and changes growth to roughly one configured quantum per window.
func TestSendFlightControllerGrowsAndReducesFromReceiverEvidence(t *testing.T) {
	settings := DefaultSendBufferSettings()
	settings.UnreliableInitialFlightByteCount = 100
	settings.UnreliableMinimumFlightByteCount = 100
	settings.UnreliableMaximumFlightByteCount = 400
	settings.UnreliableFlightIncreaseByteCount = 25
	settings.UnreliableSlowStartGrowthDivisor = 1
	controller := newSendFlightController(settings)
	controller.applyPolicy(transferFlightPolicySnapshot{
		generation: 1,
		limited:    true,
	})

	controller.send(60)
	controller.send(60)
	if controller.canSend() || controller.byteCount != 120 || controller.byteLimit != 100 {
		t.Fatalf("cold flight = %d/%d, want blocked 120/100", controller.byteCount, controller.byteLimit)
	}
	controller.acknowledge(60)
	if !controller.canSend() || controller.byteCount != 60 || controller.byteLimit != 160 {
		t.Fatalf("slow-start flight = %d/%d, want open 60/160", controller.byteCount, controller.byteLimit)
	}

	if !controller.reduceForLoss() || controller.byteLimit != 100 || controller.slowStart {
		t.Fatalf("gap reduction = limit %d slow_start=%t, want 100/false", controller.byteLimit, controller.slowStart)
	}
	controller.send(40)
	controller.acknowledge(100)
	if controller.byteCount != 0 || controller.byteLimit != 125 {
		t.Fatalf("additive flight = %d/%d, want 0/125", controller.byteCount, controller.byteLimit)
	}
}

// The default upper-layer startup window grows by one quarter per fully
// acknowledged flight because QUIC already runs its own slow start below it.
func TestSendFlightControllerUsesCautiousStartupGrowth(t *testing.T) {
	settings := DefaultSendBufferSettings()
	settings.UnreliableInitialFlightByteCount = 100
	settings.UnreliableMinimumFlightByteCount = 100
	settings.UnreliableMaximumFlightByteCount = 400
	controller := newSendFlightController(settings)
	controller.applyPolicy(transferFlightPolicySnapshot{generation: 1, limited: true})
	controller.send(100)
	controller.acknowledge(100)
	if controller.byteLimit != 125 {
		t.Fatalf("cautious startup limit = %d, want 125", controller.byteLimit)
	}
}

// Small routed messages consume one QUIC DATAGRAM each even when their byte
// total is tiny. The message flight therefore closes before a burst can exceed
// the measured one-bar packet queue, independently of the byte limit.
func TestSendFlightControllerBoundsSmallMessageCount(t *testing.T) {
	settings := DefaultSendBufferSettings()
	settings.UnreliableInitialFlightByteCount = 1024
	settings.UnreliableMinimumFlightByteCount = 1024
	settings.UnreliableMaximumFlightByteCount = 4096
	settings.UnreliableInitialFlightMessageCount = 4
	settings.UnreliableMinimumFlightMessageCount = 2
	settings.UnreliableMaximumFlightMessageCount = 16
	settings.UnreliableSlowStartGrowthDivisor = 1
	controller := newSendFlightController(settings)
	controller.applyPolicy(transferFlightPolicySnapshot{generation: 1, limited: true})

	for range 4 {
		controller.send(10)
	}
	if controller.canSend() || controller.byteCount != 40 ||
		controller.messageCount != 4 || controller.messageLimit != 4 {
		t.Fatalf(
			"small-message flight = %dB/%d messages/%d limit, want blocked 40/4/4",
			controller.byteCount,
			controller.messageCount,
			controller.messageLimit,
		)
	}
	controller.acknowledge(10)
	if !controller.canSend() || controller.messageCount != 3 || controller.messageLimit != 5 {
		t.Fatalf(
			"acknowledged small-message flight = %d/%d, want open 3/5",
			controller.messageCount,
			controller.messageLimit,
		)
	}
	if !controller.reduceForLoss() || controller.messageLimit != 2 {
		t.Fatalf("small-message gap limit = %d, want 2", controller.messageLimit)
	}
}

// A carrier-specific queue-depth cap tightens only that route generation and
// cannot slow H3 or another unreliable carrier after the selector changes.
func TestSendFlightControllerAppliesCarrierMessageLimit(t *testing.T) {
	settings := DefaultSendBufferSettings()
	settings.UnreliableInitialFlightMessageCount = 8
	settings.UnreliableMinimumFlightMessageCount = 4
	settings.UnreliableMaximumFlightMessageCount = 32
	controller := newSendFlightController(settings)
	controller.applyPolicy(transferFlightPolicySnapshot{
		generation:   1,
		limited:      true,
		messageLimit: 4,
	})
	if controller.messageLimit != 4 || controller.activeMaximumMessageCount != 4 {
		t.Fatalf(
			"carrier-capped flight = %d/%d, want 4/4",
			controller.messageLimit,
			controller.activeMaximumMessageCount,
		)
	}
	for range 4 {
		controller.send(100)
	}
	if controller.canSend() {
		t.Fatal("carrier-capped flight admitted a fifth message")
	}
	for range 4 {
		controller.acknowledge(100)
	}
	if controller.messageLimit != 4 {
		t.Fatalf("carrier-capped flight grew to %d, want 4", controller.messageLimit)
	}

	controller.applyPolicy(transferFlightPolicySnapshot{
		generation: 2,
		limited:    true,
	})
	if controller.messageLimit != 8 || controller.activeMaximumMessageCount != 32 {
		t.Fatalf(
			"uncapped next carrier = %d/%d, want configured 8/32",
			controller.messageLimit,
			controller.activeMaximumMessageCount,
		)
	}
}

// A carrier whose final handoff has a smaller byte ceiling can reserve part of
// that queue for untracked control without reducing the global H3 policy.
func TestSendFlightControllerAppliesCarrierByteLimit(t *testing.T) {
	settings := DefaultSendBufferSettings()
	settings.UnreliableInitialFlightByteCount = 100
	settings.UnreliableMinimumFlightByteCount = 50
	settings.UnreliableMaximumFlightByteCount = 400
	settings.UnreliableSlowStartGrowthDivisor = 1
	controller := newSendFlightController(settings)
	controller.applyPolicy(transferFlightPolicySnapshot{
		generation: 1,
		limited:    true,
		byteLimit:  180,
	})
	if controller.byteLimit != 100 || controller.activeMaximumByteCount != 180 {
		t.Fatalf(
			"carrier-capped byte flight = %d/%d, want 100/180",
			controller.byteLimit,
			controller.activeMaximumByteCount,
		)
	}
	controller.send(100)
	controller.acknowledge(100)
	if controller.byteLimit != 180 {
		t.Fatalf("carrier-capped byte flight grew to %d, want 180", controller.byteLimit)
	}

	controller.applyPolicy(transferFlightPolicySnapshot{
		generation: 2,
		limited:    true,
	})
	if controller.byteLimit != 100 || controller.activeMaximumByteCount != 400 {
		t.Fatalf(
			"uncapped next carrier byte flight = %d/%d, want 100/400",
			controller.byteLimit,
			controller.activeMaximumByteCount,
		)
	}
}

// Tail loss has no later receiver delivery with which to form a selective-Ack
// gap. Its acknowledgement timeout must still exit slow start and reduce both
// independent bounds before the retry enters the same carrier queue.
func TestSendFlightControllerReducesFromTimeoutLoss(t *testing.T) {
	settings := DefaultSendBufferSettings()
	settings.UnreliableInitialFlightByteCount = 100
	settings.UnreliableMinimumFlightByteCount = 50
	settings.UnreliableMaximumFlightByteCount = 400
	settings.UnreliableInitialFlightMessageCount = 8
	settings.UnreliableMinimumFlightMessageCount = 4
	settings.UnreliableMaximumFlightMessageCount = 32
	settings.UnreliableSlowStartGrowthDivisor = 1
	controller := newSendFlightController(settings)
	controller.applyPolicy(transferFlightPolicySnapshot{generation: 1, limited: true})

	controller.send(100)
	controller.acknowledge(100)
	if controller.byteLimit != 200 || controller.messageLimit != 9 {
		t.Fatalf(
			"grown flight = %dB/%d messages, want 200/9",
			controller.byteLimit,
			controller.messageLimit,
		)
	}
	if !controller.reduceForLoss() || controller.byteLimit != 100 ||
		controller.messageLimit != 4 || controller.slowStart {
		t.Fatalf(
			"timeout loss flight = %dB/%d messages slow_start=%t, want 100/4/false",
			controller.byteLimit,
			controller.messageLimit,
			controller.slowStart,
		)
	}
}

// A new unreliable route generation is cold even if the prior path had grown;
// changing to a reliable generation immediately lifts the admission limit.
func TestSendFlightControllerResetsOnRouteGeneration(t *testing.T) {
	settings := DefaultSendBufferSettings()
	settings.UnreliableInitialFlightByteCount = 100
	settings.UnreliableMinimumFlightByteCount = 100
	settings.UnreliableMaximumFlightByteCount = 400
	settings.UnreliableSlowStartGrowthDivisor = 1
	controller := newSendFlightController(settings)
	controller.applyPolicy(transferFlightPolicySnapshot{generation: 1, limited: true})
	controller.send(100)
	controller.acknowledge(100)
	if controller.byteLimit != 200 {
		t.Fatalf("grown limit = %d, want 200", controller.byteLimit)
	}

	controller.send(150)
	controller.applyPolicy(transferFlightPolicySnapshot{generation: 2, limited: true})
	if controller.byteLimit != 100 || controller.canSend() {
		t.Fatalf("new path flight = %d/%d, want blocked at cold limit", controller.byteCount, controller.byteLimit)
	}
	controller.applyPolicy(transferFlightPolicySnapshot{generation: 3, limited: false})
	if !controller.canSend() {
		t.Fatal("reliable replacement did not lift the unreliable flight limit")
	}
}

// Route publication carries the property only while an unreliable transport
// is active and closes the prior generation notification at each transition.
func TestMultiRouteWriterPublishesUnreliableTransferPolicy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	routeManager := NewRouteManager(ctx, "flight-policy")
	destination := DestinationId(NewId())
	writer := routeManager.OpenMultiRouteWriter(destination)
	defer routeManager.CloseMultiRouteWriter(writer)
	provider := writer.(transferFlightPolicyProvider)
	initialPolicy := provider.transferFlightPolicy()

	transport := NewSendClientTransport(destination)
	route := make(chan []byte, 1)
	routeManager.UpdateTransportWithProperties(
		transport,
		[]Route{route},
		TransferCarrierProperties{
			Unreliable:                true,
			unreliableFlightByteLimit: 123,
		},
	)
	select {
	case <-initialPolicy.notify:
	default:
		t.Fatal("route publication did not retire the prior policy generation")
	}
	unreliablePolicy := provider.transferFlightPolicy()
	if !unreliablePolicy.limited || unreliablePolicy.byteLimit != 123 ||
		unreliablePolicy.generation == initialPolicy.generation {
		t.Fatalf("unreliable policy = %+v after generation %+v", unreliablePolicy, initialPolicy)
	}

	routeManager.UpdateTransport(transport, []Route{route})
	select {
	case <-unreliablePolicy.notify:
	default:
		t.Fatal("reliable replacement did not retire the unreliable generation")
	}
	reliablePolicy := provider.transferFlightPolicy()
	if reliablePolicy.limited || reliablePolicy.generation == unreliablePolicy.generation {
		t.Fatalf("reliable policy = %+v after generation %+v", reliablePolicy, unreliablePolicy)
	}
	routeManager.RemoveTransport(transport)
}

func TestMultiRouteWriterPublishesFlowReserveConservatively(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	selector := NewMultiRouteSelector(ctx, "flow-reserve-policy", nil, TransferPath{}, false)
	defer selector.Close()

	h3Transport := NewSendGatewayTransportWithType(TransportTypeH3)
	h3Route := make(Route, 1)
	selector.updateTransportWithProperties(
		h3Transport,
		[]Route{h3Route},
		TransferCarrierProperties{
			Unreliable:              true,
			UnreliableFlowIsolation: true,
			UnreliableFlowReserve:   true,
		},
	)
	if policy := selector.transferFlightPolicy(); !policy.limited ||
		!policy.flowIsolation || !policy.flowReserve {
		t.Fatalf("H3 flow-reserve policy = %+v, want limited reserve", policy)
	}

	selector.updateTransportWithProperties(
		h3Transport,
		[]Route{h3Route},
		TransferCarrierProperties{
			Unreliable:              true,
			UnreliableFlowIsolation: true,
		},
	)
	if policy := selector.transferFlightPolicy(); !policy.limited ||
		!policy.flowIsolation || policy.flowReserve {
		t.Fatalf("H3 isolation-only policy = %+v, want isolation without reserve", policy)
	}

	p2pTransport := NewSendGatewayTransportWithType(TransportTypeP2p)
	p2pRoute := make(Route, 1)
	selector.updateTransportWithProperties(
		p2pTransport,
		[]Route{p2pRoute},
		TransferCarrierProperties{Unreliable: true},
	)
	if policy := selector.transferFlightPolicy(); !policy.limited ||
		policy.flowIsolation || policy.flowReserve {
		t.Fatalf("mixed H3/P2P flow-reserve policy = %+v, want conservative disable", policy)
	}
}

func TestMultiRouteWriterPublishesH1OnlyPolicy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	selector := NewMultiRouteSelector(ctx, "h1-group-policy", nil, TransferPath{}, false)
	defer selector.Close()

	h1Transport := NewSendGatewayTransportWithType(TransportTypeH1)
	h1Route := make(Route, 1)
	selector.updateTransportWithProperties(h1Transport, []Route{h1Route}, TransferCarrierProperties{})
	if policy := selector.transferFlightPolicy(); !policy.h1Only {
		t.Fatalf("H1 route policy = %+v, want H1-only", policy)
	}

	h3Transport := NewSendGatewayTransportWithType(TransportTypeH3)
	h3Route := make(Route, 1)
	selector.updateTransportWithProperties(h3Transport, []Route{h3Route}, TransferCarrierProperties{})
	if policy := selector.transferFlightPolicy(); policy.h1Only {
		t.Fatalf("mixed H1/H3 policy = %+v, must keep MTU-compatible groups", policy)
	}

	selector.updateTransport(h3Transport, nil)
	if policy := selector.transferFlightPolicy(); !policy.h1Only {
		t.Fatalf("restored H1 policy = %+v, want H1-only", policy)
	}

	selector.updateTransport(h1Transport, nil)
	if policy := selector.transferFlightPolicy(); policy.h1Only {
		t.Fatalf("empty policy = %+v, must not assume H1", policy)
	}
}

// The route-wide policy still tells Transfer that a hybrid carrier exists,
// while the exact accepted message disposition distinguishes its reliable and
// unreliable lanes. The public transport-only result remains unchanged.
func TestMultiRouteWriterClassifiesSelectedHybridMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	selector := NewMultiRouteSelector(ctx, "hybrid-disposition", nil, TransferPath{}, false)
	defer selector.Close()

	h3Transport := NewSendGatewayTransportWithType(TransportTypeH3)
	h3Route := make(Route, 2)
	selector.updateTransportWithProperties(
		h3Transport,
		[]Route{h3Route},
		TransferCarrierProperties{
			Unreliable: true,
			unreliableForMessageByteCount: func(messageByteCount int) bool {
				return messageByteCount <= 4
			},
		},
	)
	policy := selector.transferFlightPolicy()
	if !policy.limited {
		t.Fatal("hybrid route did not publish its unreliable capability")
	}

	assertWrite := func(
		message string,
		wantUnreliable bool,
		wantReliable bool,
		wantHybridReliable bool,
	) {
		t.Helper()
		success, disposition, err := selector.writeDetailedWithCarrier(
			ctx,
			[]byte(message),
			time.Second,
		)
		if err != nil || !success ||
			disposition.transportType != TransportTypeH3 ||
			disposition.unreliable != wantUnreliable ||
			disposition.reliable != wantReliable ||
			disposition.hybridReliable != wantHybridReliable {
			t.Fatalf(
				"write %q = (%t, %+v, %v), want H3 unreliable=%t reliable=%t hybrid_reliable=%t",
				message,
				success,
				disposition,
				err,
				wantUnreliable,
				wantReliable,
				wantHybridReliable,
			)
		}
		<-h3Route
	}
	assertWrite("udp", true, false, false)
	assertWrite("stream", false, true, true)
}

// A ready non-tied route wins over an unavailable hybrid H3 route and reports
// reliable disposition even though the selector's aggregate policy remains
// limited. Only the equal-priority H1/H3 Auto pair receives direct affinity;
// generic route sets must still account the carrier that actually accepted
// bytes.
func TestMultiRouteWriterClassifiesActuallySelectedCarrier(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	selector := NewMultiRouteSelector(ctx, "selected-disposition", nil, TransferPath{}, false)
	defer selector.Close()

	h3Route := make(Route)
	selector.updateTransportWithProperties(
		NewSendGatewayTransportWithType(TransportTypeH3),
		[]Route{h3Route},
		TransferCarrierProperties{Unreliable: true},
	)
	dnsRoute := make(Route, 1)
	selector.updateTransport(
		NewSendGatewayTransportWithType(TransportTypeH3Dns),
		[]Route{dnsRoute},
	)
	if !selector.transferFlightPolicy().limited {
		t.Fatal("aggregate policy lost the active H3 route")
	}

	success, disposition, err := selector.writeDetailedWithCarrier(
		ctx,
		[]byte("dns-ready"),
		time.Second,
	)
	if err != nil || !success || disposition.transportType != TransportTypeH3Dns ||
		disposition.unreliable || !disposition.reliable || disposition.hybridReliable {
		t.Fatalf("selected write = (%t, %+v, %v), want reliable H3 DNS", success, disposition, err)
	}
	<-dnsRoute
	stats := selector.directAffinity.snapshot()
	if stats != (DirectCarrierAffinityStats{}) {
		t.Fatalf("generic route selection changed direct affinity stats: %+v", stats)
	}
}

// Builds one no-contract sender with a deterministic admission-wait barrier.
func newTransferFlightTestClient(
	t *testing.T,
) (*Client, Id, Transport, Route, Route, <-chan sendSequenceId) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	settings.SendBufferSettings.UnreliableInitialFlightByteCount = 1
	settings.SendBufferSettings.UnreliableMinimumFlightByteCount = 1
	settings.SendBufferSettings.UnreliableMaximumFlightByteCount = 1
	settings.SendBufferSettings.UnreliableFlightIncreaseByteCount = 1
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	peerId := NewId()
	client.ContractManager().AddNoContractPeer(peerId)
	transport := NewSendClientTransport(DestinationId(peerId))
	toPeer := make(chan []byte, 16)
	fromPeer := make(chan []byte, 16)
	waits := make(chan sendSequenceId, 4)
	client.sendBuffer.beforeResendCapacityWaitForTest = func(sequenceId sendSequenceId) {
		select {
		case waits <- sequenceId:
		default:
		}
	}
	client.RouteManager().UpdateTransport(NewReceiveGatewayTransport(), []Route{fromPeer})
	t.Cleanup(func() {
		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		if err := client.CloseAndWait(closeCtx); err != nil {
			t.Errorf("close flight-test client: %v", err)
		}
		drainRoute := func(route Route) {
			for {
				select {
				case message := <-route:
					MessagePoolReturn(message)
				default:
					return
				}
			}
		}
		drainRoute(toPeer)
		drainRoute(fromPeer)
	})
	return client, peerId, transport, toPeer, fromPeer, waits
}

// Sends one application frame whose ownership transfers on success.
func sendTransferFlightTestMessage(t *testing.T, client *Client, peerId Id, index int) {
	sendTransferFlightTestMessageWithOptions(t, client, peerId, index)
}

func sendTransferFlightTestMessageWithOptions(
	t *testing.T,
	client *Client,
	peerId Id,
	index int,
	opts ...any,
) {
	t.Helper()
	frame, err := ToFrame(
		&protocol.SimpleMessage{Content: fmt.Sprintf("flight-%d", index)},
		DefaultProtocolVersion,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !client.SendWithTimeout(frame, peerId, nil, 5*time.Second, opts...) {
		MessagePoolReturn(frame.MessageBytes)
		t.Fatalf("send flight message %d was not admitted", index)
	}
}

// Takes one routed Pack and returns its decoded copy after releasing the
// pooled carrier bytes.
func takeTransferFlightTestPack(t *testing.T, route Route) *protocol.Pack {
	t.Helper()
	select {
	case transferFrameBytes := <-route:
		defer MessagePoolReturn(transferFrameBytes)
		var transferFrame protocol.TransferFrame
		if err := ProtoUnmarshal(transferFrameBytes, &transferFrame); err != nil {
			t.Fatalf("decode flight TransferFrame: %v", err)
		}
		if transferFrame.Pack != nil {
			return transferFrame.Pack
		}
		frame := transferFrame.GetFrame()
		if frame == nil || frame.GetMessageType() != protocol.MessageType_TransferPack {
			t.Fatalf("flight route carried %v, want Transfer Pack", frame)
		}
		pack := &protocol.Pack{}
		if err := ProtoUnmarshal(frame.MessageBytes, pack); err != nil {
			t.Fatalf("decode flight Pack: %v", err)
		}
		return pack
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for flight Pack")
		return nil
	}
}

// Delivers one cumulative ACK through the real Client receive pump.
func acknowledgeTransferFlightTestPack(
	t *testing.T,
	client *Client,
	peerId Id,
	fromPeer Route,
	pack *protocol.Pack,
) {
	t.Helper()
	ackBytes, err := ProtoMarshal(&protocol.TransferFrame{
		TransferPath: TransferPath{
			SourceId:      peerId,
			DestinationId: client.ClientId(),
		}.ToProtobuf(),
		Ack: &protocol.Ack{
			MessageId:  pack.MessageId,
			SequenceId: pack.SequenceId,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case fromPeer <- ackBytes:
	case <-time.After(5 * time.Second):
		MessagePoolReturn(ackBytes)
		t.Fatal("could not deliver flight ACK")
	}
}

// The second original Pack waits behind an unreliable byte flight and an ACK
// deterministically opens capacity without waiting for its resend timer.
func TestSendSequenceUnreliableFlightWaitsForAcknowledgement(t *testing.T) {
	client, peerId, transport, toPeer, fromPeer, waits := newTransferFlightTestClient(t)
	client.RouteManager().UpdateTransportWithProperties(
		transport,
		[]Route{toPeer},
		TransferCarrierProperties{Unreliable: true},
	)

	sendTransferFlightTestMessage(t, client, peerId, 0)
	firstPack := takeTransferFlightTestPack(t, toPeer)
	sendTransferFlightTestMessage(t, client, peerId, 1)
	select {
	case <-waits:
	case <-time.After(10 * time.Second):
		t.Fatal("sender did not reach the unreliable flight barrier")
	}
	select {
	case unexpected := <-toPeer:
		MessagePoolReturn(unexpected)
		t.Fatal("unreliable carrier admitted a second Pack before ACK progress")
	default:
	}

	acknowledgeTransferFlightTestPack(t, client, peerId, fromPeer, firstPack)
	secondPack := takeTransferFlightTestPack(t, toPeer)
	if secondPack.GetSequenceNumber() != firstPack.GetSequenceNumber()+1 {
		t.Fatalf("second sequence number = %d, want %d", secondPack.GetSequenceNumber(), firstPack.GetSequenceNumber()+1)
	}
	recovery := client.SendRecoveryStats()
	if recovery.UnreliableFlightWaitCount == 0 ||
		recovery.UnreliableFlightWaitDuration <= 0 ||
		recovery.UnreliableFlightMaximumWaitDuration <= 0 ||
		recovery.UnreliableFlightWaitDuration < recovery.UnreliableFlightMaximumWaitDuration {
		t.Fatalf("unreliable flight wait stats = %+v", recovery)
	}
}

// An established NoAck flow has no resend item and consumes no unreliable
// Transfer flight. H3's bounded flow scheduler must therefore let it pass a
// different ACK-required flow that filled both gates; otherwise UDP/DNS and
// interactive probes inherit bulk TCP's recovery delay for no safety benefit.
func TestSendSequenceNoAckBypassesFullRecoveryAdmission(t *testing.T) {
	client, peerId, transport, toPeer, fromPeer, waits := newTransferFlightTestClient(t)
	client.RouteManager().UpdateTransportWithProperties(
		transport,
		[]Route{toPeer},
		TransferCarrierProperties{
			Unreliable:              true,
			UnreliableFlowIsolation: true,
		},
	)
	bulkKey := testSendSchedulingKey(1000)
	interactiveKey := testSendSchedulingKey(2000)

	sendTransferFlightTestMessageWithOptions(
		t,
		client,
		peerId,
		0,
		sendSchedulingKeyOption{key: bulkKey},
	)
	firstBulkPack := takeTransferFlightTestPack(t, toPeer)
	sendTransferFlightTestMessageWithOptions(
		t,
		client,
		peerId,
		1,
		sendSchedulingKeyOption{key: bulkKey},
	)
	select {
	case <-waits:
	case <-time.After(10 * time.Second):
		t.Fatal("bulk sender did not reach the recovery-admission barrier")
	}

	interactiveFrame, err := ToFrame(
		&protocol.SimpleMessage{Content: "flight-2"},
		DefaultProtocolVersion,
	)
	if err != nil {
		t.Fatal(err)
	}
	interactivePackInput := &SendPack{
		TransferOptions: TransferOptions{Ack: false},
		Frames:          []*protocol.Frame{interactiveFrame},
		logicalGroup:    true,
		Destination:     peerId,
		MessageByteCount: ByteCount(
			len(interactiveFrame.MessageBytes),
		),
		Ctx:           client.ctx,
		schedulingKey: interactiveKey,
	}
	if success, sendErr := client.enqueueSendPack(interactivePackInput, 5*time.Second); sendErr != nil || !success {
		MessagePoolReturn(interactiveFrame.MessageBytes)
		t.Fatalf("send logical-group flight message = %t, %v", success, sendErr)
	}
	interactivePack := takeTransferFlightTestPack(t, toPeer)
	if !interactivePack.GetNack() || interactivePack.GetSequenceNumber() != 0 {
		t.Fatalf(
			"interactive Pack = nack %t sequence %d, want true/0",
			interactivePack.GetNack(),
			interactivePack.GetSequenceNumber(),
		)
	}
	select {
	case unexpected := <-toPeer:
		MessagePoolReturn(unexpected)
		t.Fatal("ACK-required bulk Pack crossed the full recovery admission")
	default:
	}
	if recovery := client.SendRecoveryStats(); recovery.UnreliableNoAckAdmissionBypassCount != 1 ||
		recovery.UnreliableFlowReserveSelectionCount != 0 ||
		recovery.UnreliableFlowReserveUseCount != 0 {
		t.Fatalf("NoAck recovery-admission stats = %+v", recovery)
	}

	acknowledgeTransferFlightTestPack(t, client, peerId, fromPeer, firstBulkPack)
	secondBulkPack := takeTransferFlightTestPack(t, toPeer)
	if secondBulkPack.GetNack() ||
		secondBulkPack.GetSequenceNumber() != firstBulkPack.GetSequenceNumber()+1 {
		t.Fatalf(
			"second bulk Pack = nack %t sequence %d, want false/%d",
			secondBulkPack.GetNack(),
			secondBulkPack.GetSequenceNumber(),
			firstBulkPack.GetSequenceNumber()+1,
		)
	}
	acknowledgeTransferFlightTestPack(t, client, peerId, fromPeer, secondBulkPack)
}

func TestNoAckRecoveryAdmissionBypassRequiresStableAckedContract(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, sequence, _, contract := newSendNoContractHarness(t, ctx)
	sendPack := &SendPack{
		Frame: &protocol.Frame{MessageBytes: []byte("interactive")},
		TransferOptions: TransferOptions{
			Ack: false,
		},
	}

	if !sequence.noAckPackCanBypassRecoveryAdmission(sendPack) {
		t.Fatal("established acknowledged contract did not admit NoAck bypass")
	}
	sequence.sendContractAcked = false
	if sequence.noAckPackCanBypassRecoveryAdmission(sendPack) {
		t.Fatal("unacknowledged opening contract admitted NoAck bypass")
	}
	sequence.sendContractAcked = true
	sendPack.Frame = nil
	sendPack.Frames = []*protocol.Frame{
		{MessageBytes: []byte("first")},
		{MessageBytes: []byte("second")},
		{MessageBytes: []byte("later")},
	}
	sendPack.logicalGroup = true
	contract.effectiveTransferByteCount = ByteCount(len("first") + len("second"))
	if !sequence.noAckPackCanBypassRecoveryAdmission(sendPack) {
		t.Fatal("next bounded logical-group chunk did not admit NoAck bypass")
	}
	contract.unackedByteCount = contract.effectiveTransferByteCount
	if sequence.noAckPackCanBypassRecoveryAdmission(sendPack) {
		t.Fatal("exhausted contract admitted NoAck logical-group bypass")
	}
}

func TestUpdateContractWithoutAckPromotionDefersOnContractTransition(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, sequence, _, contract := newSendNoContractHarness(t, ctx)

	updated, deferred := sequence.updateContractWithoutAckPromotion(123)
	if !updated || deferred {
		t.Fatalf("established contract update = %t, %t, want true, false", updated, deferred)
	}
	if contract.unackedByteCount != 123 {
		t.Fatalf("established contract debit = %d, want 123", contract.unackedByteCount)
	}

	sequence.sendContractAcked = false
	updated, deferred = sequence.updateContractWithoutAckPromotion(456)
	if updated || !deferred {
		t.Fatalf("opening contract update = %t, %t, want false, true", updated, deferred)
	}
	if contract.unackedByteCount != 123 {
		t.Fatalf("deferred contract debit = %d, want unchanged 123", contract.unackedByteCount)
	}

	sequence.sendContractAcked = true
	contract.effectiveTransferByteCount = contract.unackedByteCount
	updated, deferred = sequence.updateContractWithoutAckPromotion(1)
	if updated || !deferred {
		t.Fatalf("exhausted contract update = %t, %t, want false, true", updated, deferred)
	}
	if contract.unackedByteCount != 123 {
		t.Fatalf("exhausted contract debit = %d, want unchanged 123", contract.unackedByteCount)
	}
}

// Route-generation notification releases a sender immediately when a reliable
// replacement takes ownership; it need not wait for an ACK from the old path.
func TestSendSequenceReliableRouteReplacementLiftsFlightLimit(t *testing.T) {
	client, peerId, transport, toPeer, _, waits := newTransferFlightTestClient(t)
	client.RouteManager().UpdateTransportWithProperties(
		transport,
		[]Route{toPeer},
		TransferCarrierProperties{Unreliable: true},
	)

	sendTransferFlightTestMessage(t, client, peerId, 0)
	firstPack := takeTransferFlightTestPack(t, toPeer)
	sendTransferFlightTestMessage(t, client, peerId, 1)
	select {
	case <-waits:
	case <-time.After(10 * time.Second):
		t.Fatal("sender did not reach the unreliable flight barrier")
	}

	client.RouteManager().UpdateTransport(transport, []Route{toPeer})
	secondPack := takeTransferFlightTestPack(t, toPeer)
	if secondPack.GetSequenceNumber() != firstPack.GetSequenceNumber()+1 {
		t.Fatalf("replacement sequence number = %d, want %d", secondPack.GetSequenceNumber(), firstPack.GetSequenceNumber()+1)
	}
}

// A reliable route preserves the historical behavior even when the configured
// unreliable limit is smaller than one encoded Pack.
func TestSendSequenceReliableCarrierDoesNotUseFlightLimit(t *testing.T) {
	client, peerId, transport, toPeer, _, _ := newTransferFlightTestClient(t)
	client.RouteManager().UpdateTransport(transport, []Route{toPeer})

	sendTransferFlightTestMessage(t, client, peerId, 0)
	firstPack := takeTransferFlightTestPack(t, toPeer)
	sendTransferFlightTestMessage(t, client, peerId, 1)
	secondPack := takeTransferFlightTestPack(t, toPeer)
	if secondPack.GetSequenceNumber() != firstPack.GetSequenceNumber()+1 {
		t.Fatalf("reliable sequence number = %d, want %d", secondPack.GetSequenceNumber(), firstPack.GetSequenceNumber()+1)
	}
}

// Hybrid H3 retains TCP's end-to-end Transfer ACK but a message selected for
// its reliable stream must not consume the DATAGRAM acknowledgement flight.
func TestSendSequenceHybridReliableLaneDoesNotUseUnreliableFlight(t *testing.T) {
	client, peerId, transport, toPeer, _, _ := newTransferFlightTestClient(t)
	client.RouteManager().UpdateTransportWithProperties(
		transport,
		[]Route{toPeer},
		TransferCarrierProperties{
			Unreliable: true,
			unreliableForMessageByteCount: func(int) bool {
				return false
			},
		},
	)

	sendTransferFlightTestMessage(t, client, peerId, 0)
	firstPack := takeTransferFlightTestPack(t, toPeer)
	sendTransferFlightTestMessage(t, client, peerId, 1)
	secondPack := takeTransferFlightTestPack(t, toPeer)
	if secondPack.GetSequenceNumber() != firstPack.GetSequenceNumber()+1 {
		t.Fatalf(
			"hybrid stream sequence number = %d, want %d",
			secondPack.GetSequenceNumber(),
			firstPack.GetSequenceNumber()+1,
		)
	}
	recovery := client.SendRecoveryStats()
	if recovery.UnreliableFlightWaitCount != 0 ||
		recovery.UnreliableFlightMaximumByteCount != 0 ||
		recovery.UnreliableFlightMaximumMessageCount != 0 {
		t.Fatalf("hybrid reliable lane entered unreliable flight: %+v", recovery)
	}
}

// A reliable carrier owns ordinary byte retransmission after accepting a
// Transfer item, but it cannot recover bytes from an exact route that has
// retired. Both H1 and H3-stream route retirement must therefore bypass the
// conservative nested-recovery interval and retry through the replacement.
func TestSendSequenceReliableRouteRemovalRetriesImmediately(t *testing.T) {
	tests := []struct {
		name       string
		properties TransferCarrierProperties
	}{
		{name: "h1"},
		{
			name: "h3-stream",
			properties: TransferCarrierProperties{
				Unreliable: true,
				unreliableForMessageByteCount: func(int) bool {
					return false
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, peerId, transport, oldRoute, fromPeer, _ := newTransferFlightTestClient(t)
			client.RouteManager().UpdateTransportWithProperties(
				transport,
				[]Route{oldRoute},
				test.properties,
			)

			sendTransferFlightTestMessage(t, client, peerId, 0)
			firstPack := takeTransferFlightTestPack(t, oldRoute)
			replacementRoute := make(chan []byte, 16)
			client.RouteManager().UpdateTransportWithProperties(
				transport,
				[]Route{replacementRoute},
				test.properties,
			)

			replacementPack := takeTransferFlightTestPack(t, replacementRoute)
			if !bytes.Equal(replacementPack.GetMessageId(), firstPack.GetMessageId()) ||
				replacementPack.GetSequenceNumber() != firstPack.GetSequenceNumber() {
				t.Fatalf(
					"replacement Pack = (%s, %d), want original (%s, %d)",
					replacementPack.GetMessageId(),
					replacementPack.GetSequenceNumber(),
					firstPack.GetMessageId(),
					firstPack.GetSequenceNumber(),
				)
			}
			acknowledgeTransferFlightTestPack(t, client, peerId, fromPeer, replacementPack)
			if !waitForCondition(time.Second, func() bool {
				return client.SendRecoveryStats().CarrierChangeWriteCount == 1
			}) {
				t.Fatalf("carrier-change recovery stats = %+v", client.SendRecoveryStats())
			}
			recovery := client.SendRecoveryStats()
			if recovery.TimeoutResendWriteCount != 0 {
				t.Fatalf("route removal used timeout recovery: %+v", recovery)
			}
		})
	}
}

// Publishing an equal-priority sibling does not imply that the original
// reliable connection lost its accepted stream bytes. Keep its carrier in
// sole recovery control while that exact route remains active.
func TestSendSequenceReliableSiblingRouteAdditionDoesNotRetry(t *testing.T) {
	client, peerId, transport, originalRoute, fromPeer, _ := newTransferFlightTestClient(t)
	client.RouteManager().UpdateTransport(transport, []Route{originalRoute})

	sendTransferFlightTestMessage(t, client, peerId, 0)
	firstPack := takeTransferFlightTestPack(t, originalRoute)
	siblingTransport := NewSendClientTransport(DestinationId(peerId))
	siblingRoute := make(chan []byte, 16)
	client.RouteManager().UpdateTransport(siblingTransport, []Route{siblingRoute})

	select {
	case unexpected := <-originalRoute:
		MessagePoolReturn(unexpected)
		t.Fatal("adding a sibling route retried over the original reliable route")
	case unexpected := <-siblingRoute:
		MessagePoolReturn(unexpected)
		t.Fatal("adding a sibling route retried over the new reliable route")
	case <-time.After(350 * time.Millisecond):
	}
	if recovery := client.SendRecoveryStats(); recovery.CarrierChangeWriteCount != 0 || recovery.TimeoutResendWriteCount != 0 {
		t.Fatalf("sibling route triggered recovery: %+v", recovery)
	}
	acknowledgeTransferFlightTestPack(t, client, peerId, fromPeer, firstPack)
}
