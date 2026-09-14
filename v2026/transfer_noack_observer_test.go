// This file verifies the optional first-route-write observer used by
// deterministic integration boundaries without changing normal send behavior.
package connect

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// One observed sender uses a contract-free destination and no encryption so
// tests isolate enqueue, coalescing, route-write, and close dispositions.
func newNoAckObserverTestClient(
	t *testing.T,
	ctx context.Context,
	observer func(NoAckSendObservation),
	configure ...func(*ClientSettings, Id),
) (*Client, Id) {
	t.Helper()
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	settings.SendBufferSettings.NoAckSendObserver = observer
	destinationId := NewId()
	for _, configureSettings := range configure {
		configureSettings(settings, destinationId)
	}
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	client.ContractManager().AddNoContractPeer(destinationId)
	return client, destinationId
}

// Observer events are copied into a bounded channel and never block Connect.
func noAckObserverTestEvents() (func(NoAckSendObservation), <-chan NoAckSendObservation) {
	events := make(chan NoAckSendObservation, 16)
	return func(observation NoAckSendObservation) {
		select {
		case events <- observation:
		default:
			panic("NoAck observer test event overflow")
		}
	}, events
}

// Every positive wait has only a liveness deadline; barriers establish order.
func waitNoAckObservation(
	t *testing.T,
	ctx context.Context,
	events <-chan NoAckSendObservation,
) NoAckSendObservation {
	t.Helper()
	select {
	case <-ctx.Done():
		t.Fatalf("wait for NoAck observation: %v", ctx.Err())
		return NoAckSendObservation{}
	case observation := <-events:
		return observation
	}
}

// Holding sequence startup queues two packs deterministically; their one wire
// write must still produce one uniquely paired completion per original pack.
func TestNoAckSendObserverPairsCoalescedRouteWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	observer, events := noAckObserverTestEvents()
	sequenceEntered := make(chan struct{})
	releaseSequence := make(chan struct{})
	var enterOnce sync.Once
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSequence) }) }
	defer release()
	client, destinationId := newNoAckObserverTestClient(
		t,
		ctx,
		observer,
		func(settings *ClientSettings, destinationId Id) {
			settings.SendBufferSettings.beforeRunSendSequenceForTest = func(id sendSequenceId) {
				if id.Destination != destinationId {
					return
				}
				enterOnce.Do(func() { close(sequenceEntered) })
				<-releaseSequence
			}
		},
	)
	defer client.Cancel()
	route := make(chan []byte, 1)
	client.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(destinationId)),
		[]Route{route},
	)

	contents := []string{
		"first", "second",
	}
	for _, content := range contents {
		frame := RequireToFrameWithDefaultProtocolVersion(&protocol.SimpleMessage{Content: content})
		if !client.SendWithTimeout(frame, destinationId, nil, time.Second, NoAck()) {
			MessagePoolReturn(frame.MessageBytes)
			t.Fatalf("NoAck send %q was rejected", content)
		}
	}
	select {
	case <-ctx.Done():
		t.Fatalf("wait for held sequence: %v", ctx.Err())
	case <-sequenceEntered:
	}
	started := make([]NoAckSendObservation, 0, len(contents))
	startedTokens := map[uint64]bool{}
	for range contents {
		observation := waitNoAckObservation(t, ctx, events)
		if observation.Phase != NoAckSendPhaseStarted || startedTokens[observation.Token] {
			t.Fatalf("started observation=%+v prior=%+v", observation, started)
		}
		started = append(started, observation)
		startedTokens[observation.Token] = true
	}
	release()
	var transferFrameBytes []byte
	select {
	case <-ctx.Done():
		t.Fatalf("wait for coalesced route write: %v", ctx.Err())
	case transferFrameBytes = <-route:
	}
	var transferFrame protocol.TransferFrame
	if err := ProtoUnmarshal(transferFrameBytes, &transferFrame); err != nil {
		t.Fatalf("decode coalesced transfer: %v", err)
	}
	if transferFrame.Pack == nil || len(transferFrame.Pack.Frames) != len(contents) {
		t.Fatalf("coalesced transfer=%+v", transferFrame.Pack)
	}
	MessagePoolReturn(transferFrameBytes)
	completions := map[uint64]NoAckSendObservation{}
	for len(completions) < len(contents) {
		observation := waitNoAckObservation(t, ctx, events)
		if observation.Phase != NoAckSendPhaseCompleted || observation.Err != nil {
			t.Fatalf("completion=%+v", observation)
		}
		completions[observation.Token] = observation
	}
	for _, observation := range started {
		if _, ok := completions[observation.Token]; !ok {
			t.Fatalf("started token %d had no completion", observation.Token)
		}
	}
}

// A route timeout completes the exact admitted token with the write error.
func TestNoAckSendObserverReportsRouteWriteError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	observer, events := noAckObserverTestEvents()
	client, destinationId := newNoAckObserverTestClient(t, ctx, observer)
	defer client.Cancel()
	client.settings.SendBufferSettings.WriteTimeout = 10 * time.Millisecond
	client.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(destinationId)),
		[]Route{make(chan []byte)},
	)
	frame := RequireToFrameWithDefaultProtocolVersion(&protocol.SimpleMessage{Content: "blocked"})
	if !client.SendWithTimeout(frame, destinationId, nil, time.Second, NoAck()) {
		MessagePoolReturn(frame.MessageBytes)
		t.Fatal("NoAck route-error send was rejected before route write")
	}
	started := waitNoAckObservation(t, ctx, events)
	completion := waitNoAckObservation(t, ctx, events)
	if started.Phase != NoAckSendPhaseStarted ||
		completion.Phase != NoAckSendPhaseCompleted ||
		started.Token != completion.Token || completion.Err == nil {
		t.Fatalf("route-error observations=%+v %+v", started, completion)
	}
}

// Cancellation after admission drains the queued pack through one completion.
func TestNoAckSendObserverReportsClientClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	observer, events := noAckObserverTestEvents()
	sequenceEntered := make(chan struct{})
	releaseSequence := make(chan struct{})
	var enterOnce sync.Once
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSequence) }) }
	defer release()
	client, destinationId := newNoAckObserverTestClient(
		t,
		ctx,
		observer,
		func(settings *ClientSettings, destinationId Id) {
			settings.SendBufferSettings.beforeRunSendSequenceForTest = func(id sendSequenceId) {
				if id.Destination != destinationId {
					return
				}
				enterOnce.Do(func() { close(sequenceEntered) })
				<-releaseSequence
			}
		},
	)
	frame := RequireToFrameWithDefaultProtocolVersion(&protocol.SimpleMessage{Content: "close"})
	if !client.SendWithTimeout(frame, destinationId, nil, time.Second, NoAck()) {
		MessagePoolReturn(frame.MessageBytes)
		t.Fatal("NoAck close send was rejected before admission")
	}
	select {
	case <-ctx.Done():
		t.Fatalf("wait for held close sequence: %v", ctx.Err())
	case <-sequenceEntered:
	}
	started := waitNoAckObservation(t, ctx, events)
	client.Cancel()
	release()
	completion := waitNoAckObservation(t, ctx, events)
	if started.Phase != NoAckSendPhaseStarted ||
		completion.Phase != NoAckSendPhaseCompleted ||
		started.Token != completion.Token || completion.Err == nil {
		t.Fatalf("close observations=%+v %+v", started, completion)
	}
}

// Immediate rejection still produces a complete pair for the observer while
// preserving the public contract that the caller owns rejected frame bytes.
func TestNoAckSendObserverReportsPackRejection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	observer, events := noAckObserverTestEvents()
	client, destinationId := newNoAckObserverTestClient(t, ctx, observer)
	sendCtx, sendCancel := context.WithCancel(ctx)
	sendCancel()
	frame := RequireToFrameWithDefaultProtocolVersion(&protocol.SimpleMessage{Content: "rejected"})
	success, err := client.SendWithTimeoutDetailed(frame, destinationId, nil, 0, NoAck(), Ctx(sendCtx))
	if success || err == nil {
		MessagePoolReturn(frame.MessageBytes)
		t.Fatalf("canceled send success=%t err=%v", success, err)
	}
	MessagePoolReturn(frame.MessageBytes)
	started := waitNoAckObservation(t, ctx, events)
	completion := waitNoAckObservation(t, ctx, events)
	if started.Phase != NoAckSendPhaseStarted ||
		completion.Phase != NoAckSendPhaseCompleted ||
		started.Token != completion.Token || !errors.Is(completion.Err, err) {
		t.Fatalf("rejection observations=%+v %+v sendErr=%v", started, completion, err)
	}
}

// Ack-required sends are outside this seam because their callback completion
// is a remote receipt, not an initial route-write disposition.
func TestNoAckSendObserverExcludesAckSend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	observer, events := noAckObserverTestEvents()
	client, destinationId := newNoAckObserverTestClient(t, ctx, observer)
	defer client.Cancel()
	route := make(chan []byte, 1)
	client.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(destinationId)),
		[]Route{route},
	)
	frame := RequireToFrameWithDefaultProtocolVersion(&protocol.SimpleMessage{Content: "ack"})
	if !client.SendWithTimeout(frame, destinationId, nil, time.Second) {
		MessagePoolReturn(frame.MessageBytes)
		t.Fatal("Ack send was rejected")
	}
	select {
	case <-ctx.Done():
		t.Fatalf("wait for Ack route write: %v", ctx.Err())
	case transferFrameBytes := <-route:
		MessagePoolReturn(transferFrameBytes)
	}
	select {
	case observation := <-events:
		t.Fatalf("Ack send produced NoAck observation: %+v", observation)
	default:
	}
}

// A raw-send acknowledgement target records its independent disposition.
type recordingNoAckSendTarget struct {
	results chan error
}

// Completion records the raw-send acknowledgement disposition independently
// from the no-ack lifecycle observation.
func (self *recordingNoAckSendTarget) sendAckResult(value ByteCount, err error) {
	_ = value
	self.results <- err
}

// Raw provider sends use ackTarget instead of AckCallback. Observation remains
// independent and completes for success, route error, and queued close.
func testNoAckSendObserverRawAckTargetDisposition(
	t *testing.T,
	routeError bool,
	queuedClose bool,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	observer, events := noAckObserverTestEvents()
	client, destinationId := newNoAckObserverTestClient(t, ctx, observer)
	defer client.Cancel()
	if queuedClose {
		target := &recordingNoAckSendTarget{results: make(chan error, 1)}
		sequence := NewSendSequence(
			ctx,
			client,
			client.sendBuffer,
			destinationId,
			MultiHopId{},
			false,
			false,
			false,
			sequenceTlsRoleClient,
			false,
			DefaultSendBufferSettings(),
		)
		messageBytes := MessagePoolCopy([]byte("raw-provider-close"))
		sendPack := &SendPack{
			TransferOptions: DefaultTransferOpts(),
			Destination:     destinationId,
			ackTarget:       target,
			ackValue:        1,
			Ctx:             ctx,
			noAckObserver:   observer,
			noAckClientId:   client.ClientId(),
			noAckToken:      1,
		}
		sendPack.TransferOptions.Ack = false
		sendPack.singleFrameValue = protocol.Frame{
			MessageType:  protocol.MessageType_IpIpPacketFromProvider,
			MessageBytes: messageBytes,
			Raw:          true,
		}
		sendPack.Frame = &sendPack.singleFrameValue
		observer(NoAckSendObservation{
			Phase:         NoAckSendPhaseStarted,
			ClientId:      client.ClientId(),
			DestinationId: destinationId,
			Token:         1,
		})
		success, err := sequence.Pack(sendPack, 0)
		if !success || err != nil {
			MessagePoolReturn(messageBytes)
			t.Fatalf("queue raw close pack success=%t err=%v", success, err)
		}
		sequence.Close()
		started := waitNoAckObservation(t, ctx, events)
		completed := waitNoAckObservation(t, ctx, events)
		if started.Phase != NoAckSendPhaseStarted ||
			completed.Phase != NoAckSendPhaseCompleted ||
			started.Token != completed.Token || completed.Err == nil {
			t.Fatalf("raw queued-close observations=%+v %+v", started, completed)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for raw queued-close target: %v", ctx.Err())
		case targetErr := <-target.results:
			if targetErr == nil {
				t.Fatal("raw queued-close target had no error")
			}
		}
		return
	}
	var route chan []byte
	if routeError {
		client.settings.SendBufferSettings.WriteTimeout = 10 * time.Millisecond
		route = make(chan []byte)
	} else {
		route = make(chan []byte, 1)
	}
	client.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(destinationId)),
		[]Route{route},
	)
	target := &recordingNoAckSendTarget{results: make(chan error, 1)}
	messageBytes := MessagePoolCopy([]byte("raw-provider"))
	success, err := client.sendRawWithTimeoutDetailed(
		protocol.MessageType_IpIpPacketFromProvider,
		messageBytes,
		destinationId,
		target,
		1,
		time.Second,
		NoAck(),
	)
	if !success || err != nil {
		MessagePoolReturn(messageBytes)
		t.Fatalf("raw send success=%t err=%v", success, err)
	}
	started := waitNoAckObservation(t, ctx, events)
	completed := waitNoAckObservation(t, ctx, events)
	if started.Phase != NoAckSendPhaseStarted ||
		completed.Phase != NoAckSendPhaseCompleted ||
		started.ClientId != client.ClientId() ||
		started.DestinationId != destinationId ||
		started.Token != completed.Token {
		t.Fatalf("raw observations=%+v %+v", started, completed)
	}
	if routeError || queuedClose {
		if completed.Err == nil {
			t.Fatalf("raw failed completion had no error")
		}
	} else {
		if completed.Err != nil {
			t.Fatalf("raw success completion: %v", completed.Err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for raw route bytes: %v", ctx.Err())
		case transferFrameBytes := <-route:
			MessagePoolReturn(transferFrameBytes)
		}
	}
	select {
	case <-ctx.Done():
		t.Fatalf("wait for raw ack target: %v", ctx.Err())
	case targetErr := <-target.results:
		if (targetErr != nil) != (routeError || queuedClose) {
			t.Fatalf("raw target err=%v completion=%v", targetErr, completed.Err)
		}
	}
}

// Raw sends report both a successful first route write and target disposition.
func TestNoAckSendObserverCoversRawAckTargetSuccess(t *testing.T) {
	testNoAckSendObserverRawAckTargetDisposition(t, false, false)
}

// Raw sends preserve both observer and target errors from an unavailable route.
func TestNoAckSendObserverCoversRawAckTargetRouteError(t *testing.T) {
	testNoAckSendObserverRawAckTargetDisposition(t, true, false)
}

// A queued raw send receives one observer and target error when its sequence closes.
func TestNoAckSendObserverCoversRawAckTargetQueuedClose(t *testing.T) {
	testNoAckSendObserverRawAckTargetDisposition(t, false, true)
}

// A requested NoAck pack can be forced onto the wire Ack lane until its
// opening contract is acknowledged. Serialization completion must still fire
// immediately and never wait for the remote Ack.
func TestNoAckSendObserverCompletesWhenOpeningContractForcesWireAck(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, sequence, destinationId, _ := newSendNoContractHarness(t, ctx)
	route := make(chan []byte, 1)
	client.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(destinationId)),
		[]Route{route},
	)
	sequence.sendBuffer = client.sendBuffer
	sequence.sendContractAcked = false
	observer, events := noAckObserverTestEvents()
	const token = 77
	observer(NoAckSendObservation{
		Phase:         NoAckSendPhaseStarted,
		ClientId:      client.ClientId(),
		DestinationId: destinationId,
		Token:         token,
	})
	frame := RequireToFrameWithDefaultProtocolVersion(&protocol.SimpleMessage{Content: "forced-ack"})
	sequence.sendRecord(
		[]*protocol.Frame{frame},
		sendAckRecord{},
		noAckSendRecord{
			observer:      observer,
			clientId:      client.ClientId(),
			destinationId: destinationId,
			token:         token,
		},
		false,
		false,
	)
	started := waitNoAckObservation(t, ctx, events)
	completed := waitNoAckObservation(t, ctx, events)
	if started.Phase != NoAckSendPhaseStarted || completed.Phase != NoAckSendPhaseCompleted ||
		started.Token != completed.Token || completed.Err != nil {
		t.Fatalf("forced-Ack observations=%+v %+v", started, completed)
	}
	var transferFrameBytes []byte
	select {
	case <-ctx.Done():
		t.Fatalf("wait for forced-Ack wire pack: %v", ctx.Err())
	case transferFrameBytes = <-route:
	}
	defer MessagePoolReturn(transferFrameBytes)
	var transferFrame protocol.TransferFrame
	if err := ProtoUnmarshal(transferFrameBytes, &transferFrame); err != nil {
		t.Fatalf("decode forced-Ack transfer: %v", err)
	}
	if transferFrame.Pack == nil || transferFrame.Pack.Nack {
		t.Fatalf("requested NoAck pack was not forced onto wire Ack: %+v", transferFrame.Pack)
	}
}

// One generated settings observer can receive token 1 from many clients.
// ClientId keeps the independently completed pairs from colliding.
func TestNoAckSendObserverNamespacesTokensByClient(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	observer, events := noAckObserverTestEvents()
	type observedClient struct {
		client      *Client
		destination Id
		route       chan []byte
	}
	clients := make([]observedClient, 0, 2)
	for range 2 {
		client, destinationId := newNoAckObserverTestClient(t, ctx, observer)
		defer client.Cancel()
		route := make(chan []byte, 1)
		client.RouteManager().UpdateTransport(
			NewSendClientTransport(DestinationId(destinationId)),
			[]Route{route},
		)
		clients = append(clients, observedClient{
			client:      client,
			destination: destinationId,
			route:       route,
		})
		frame := RequireToFrameWithDefaultProtocolVersion(&protocol.SimpleMessage{Content: "shared"})
		if !client.SendWithTimeout(frame, destinationId, nil, time.Second, NoAck()) {
			MessagePoolReturn(frame.MessageBytes)
			t.Fatal("shared-observer send was rejected")
		}
	}
	type observationKey struct {
		clientId Id
		token    uint64
	}
	phases := map[observationKey]map[NoAckSendPhase]int{}
	for observationCount := 0; observationCount < 4; observationCount += 1 {
		observation := waitNoAckObservation(t, ctx, events)
		key := observationKey{clientId: observation.ClientId, token: observation.Token}
		if phases[key] == nil {
			phases[key] = map[NoAckSendPhase]int{}
		}
		phases[key][observation.Phase] += 1
	}
	if len(phases) != 2 {
		t.Fatalf("shared observer keys=%v", phases)
	}
	for key, counts := range phases {
		if key.token != 1 || counts[NoAckSendPhaseStarted] != 1 ||
			counts[NoAckSendPhaseCompleted] != 1 {
			t.Fatalf("shared observer key=%+v phases=%v", key, counts)
		}
	}
	for _, observed := range clients {
		select {
		case <-ctx.Done():
			t.Fatalf("wait for shared-observer wire bytes: %v", ctx.Err())
		case transferFrameBytes := <-observed.route:
			MessagePoolReturn(transferFrameBytes)
		}
	}
}
