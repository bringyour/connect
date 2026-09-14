package connect

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"runtime"
	"slices"
	"testing"
	"time"
)

type typedPriorityRouteTestTransport struct {
	*prioritySendGatewayTransport
	transportType TransportType
}

func TestMultiRouteReaderReportsExactHybridReceiveLane(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	selector := NewMultiRouteSelector(
		ctx,
		"hybrid-receive-lane",
		nil,
		DestinationId(NewId()),
		true,
	)
	defer selector.Close()
	base := NewReceiveGatewayTransportWithType(TransportTypeH3)
	reliableRoute := make(Route, 1)
	unreliableRoute := make(Route, 1)
	selector.updateTransportWithProperties(
		base,
		[]Route{reliableRoute},
		TransferCarrierProperties{ReceiveReliability: CarrierReliabilityReliable},
	)
	selector.updateTransportWithProperties(
		newReceiveLaneTransport(base),
		[]Route{unreliableRoute},
		TransferCarrierProperties{ReceiveReliability: CarrierReliabilityUnreliable},
	)

	for _, testCase := range []struct {
		name        string
		route       Route
		reliability CarrierReliability
	}{
		{name: "QUIC stream", route: reliableRoute, reliability: CarrierReliabilityReliable},
		{name: "DATAGRAM", route: unreliableRoute, reliability: CarrierReliabilityUnreliable},
	} {
		message := MessagePoolGet(23)
		testCase.route <- message
		got, disposition, err := selector.readWithCarrier(ctx, time.Second)
		if err != nil {
			t.Fatalf("%s: %v", testCase.name, err)
		}
		if disposition.transportType != TransportTypeH3 ||
			disposition.reliability != testCase.reliability {
			MessagePoolReturn(got)
			t.Fatalf("%s receive disposition=%+v", testCase.name, disposition)
		}
		MessagePoolReturn(got)
	}
}

func TestTransferCarrierHybridSendCutoffClassifiesOnlyDatagramFrames(t *testing.T) {
	properties := TransferCarrierProperties{
		Unreliable:                    true,
		UnreliableMaxMessageByteCount: 31,
		UnreliableFlowIsolation:       true,
		UnreliableFlowReserve:         true,
	}
	if !properties.messageUnreliable(make([]byte, 31)) {
		t.Fatal("message at DATAGRAM cutoff was classified reliable")
	}
	if properties.messageUnreliable(make([]byte, 32)) {
		t.Fatal("message above DATAGRAM cutoff was classified unreliable")
	}
}

func newTypedPriorityRouteTestTransport(
	transportType TransportType,
	priority int,
) *typedPriorityRouteTestTransport {
	return &typedPriorityRouteTestTransport{
		prioritySendGatewayTransport: NewPrioritySendGatewayTransport(priority, 1),
		transportType:                transportType,
	}
}

func (self *typedPriorityRouteTestTransport) TransportType() TransportType {
	return self.transportType
}

func TestMultiRoute(t *testing.T) {
	// create route manager
	// add multiple transports and routes
	// multi route write, write a message
	// multi route reader, read a message

	WriteTimeout := 1 * time.Second
	ReadTimeout := 1 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientId := NewId()
	// client := NewClientWithDefaults(ctx, clientId)

	routeManager := NewRouteManager(ctx, "test")

	sendTransports := map[Transport][]Route{}
	receiveTransports := map[Transport][]Route{}

	transportCount := 20
	burstSize := 2048

	multiRouteWriter := routeManager.OpenMultiRouteWriter(DestinationId(clientId))

	multiRouteReader := routeManager.OpenMultiRouteReader(DestinationId(clientId))

	for i := 0; i < transportCount; i += 1 {
		r := make(chan []byte)
		sendRoutes := []Route{r}
		sendTransport := NewSendGatewayTransport()
		receiveRoutes := []Route{r}
		receiveTransport := NewReceiveGatewayTransport()

		sendTransports[sendTransport] = sendRoutes
		receiveTransports[receiveTransport] = receiveRoutes
	}

	go func() {
		for sendTransport, sendRoutes := range sendTransports {
			routeManager.UpdateTransport(sendTransport, sendRoutes)
		}
		for receiveTransport, receiveRoutes := range receiveTransports {
			routeManager.UpdateTransport(receiveTransport, receiveRoutes)
		}
	}()

	messageBytes := func(i int) []byte {
		b := new(bytes.Buffer)
		err := binary.Write(b, binary.LittleEndian, int64(i))
		if err != nil {
			panic(err)
		}
		return b.Bytes()
	}

	go func() {
		for i := 0; i < burstSize; i += 1 {
			multiRouteWriter.Write(ctx, messageBytes(i), WriteTimeout)
		}
	}()

	messages := [][]byte{}

	for i := 0; i < burstSize; i += 1 {
		b, err := multiRouteReader.Read(ctx, ReadTimeout)
		AssertEqual(t, err, nil)
		// AssertEqual(t, messageBytes(i), b)
		messages = append(messages, b)
	}

	AssertEqual(t, burstSize, len(messages))

	littleEndianCmp := func(a []byte, b []byte) int {
		if len(a) < len(b) {
			return -1
		} else if len(b) < len(a) {
			return 1
		}

		for i := len(a) - 1; 0 <= i; i -= 1 {
			aValue := a[i]
			bValue := b[i]
			if aValue < bValue {
				return -1
			} else if bValue < aValue {
				return 1
			}
		}

		return 0
	}
	slices.SortStableFunc(messages, littleEndianCmp)
	for i := 0; i < burstSize; i += 1 {
		AssertEqual(t, messageBytes(i), messages[i])
	}

	for sendTransport, _ := range sendTransports {
		routeManager.RemoveTransport(sendTransport)
	}
	for receiveTransport, _ := range receiveTransports {
		routeManager.RemoveTransport(receiveTransport)
	}
}

// A custom Auto policy may keep equal-priority direct carriers healthy, but one
// ordered destination sequence must not alternate messages between independent
// congestion controllers sharing the same physical uplink. The first healthy
// route is therefore tried first for every write while both remain writable.
func TestMultiRouteWriterKeepsEqualPriorityH1H3Affinity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	selector := NewMultiRouteSelector(
		ctx,
		"direct-affinity",
		nil,
		DestinationId(NewId()),
		true,
	)
	defer selector.Close()
	h3Transport := NewSendGatewayTransportWithType(TransportTypeH3)
	h3Route := make(Route, 64)
	selector.updateTransport(h3Transport, []Route{h3Route})
	h1Transport := NewSendGatewayTransportWithType(TransportTypeH1)
	h1Route := make(Route, 64)
	selector.updateTransport(h1Transport, []Route{h1Route})

	for i := 0; i < 32; i++ {
		message := MessagePoolGet(64)
		success, disposition, err := selector.writeDetailedWithCarrier(
			ctx,
			message,
			time.Second,
		)
		if err != nil || !success || disposition.transportType != TransportTypeH3 {
			if !success || err != nil {
				MessagePoolReturn(message)
			}
			t.Fatalf(
				"write %d = (%t, %+v, %v), want first-healthy H3",
				i,
				success,
				disposition,
				err,
			)
		}
	}
	if got := len(h1Route); got != 0 {
		t.Fatalf("non-preferred H1 accepted %d messages while H3 remained writable", got)
	}
	if got := len(h3Route); got != 32 {
		t.Fatalf("preferred H3 accepted %d messages, want 32", got)
	}
	stats := selector.directAffinity.snapshot()
	if stats.PreferredH3WriteCount != 32 ||
		stats.PreferredH1WriteCount != 0 ||
		stats.FallbackH1WriteCount != 0 ||
		stats.FallbackH3WriteCount != 0 ||
		stats.PreferredBlockedCount != 0 ||
		stats.ActivationCount != 1 ||
		stats.RouteChangeCount != 0 {
		t.Fatalf("direct affinity stats = %+v, want 32 preferred H3 writes only", stats)
	}
	for len(h3Route) != 0 {
		MessagePoolReturn(<-h3Route)
	}
}

// Route affinity is not a connection pin. Withdrawing the preferred route
// publishes a new immutable generation whose remaining carrier accepts the
// very next write; reconnecting the old carrier does not oscillate the stream
// back again.
func TestMultiRouteWriterDirectAffinityFailsOverWithoutOscillation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	selector := NewMultiRouteSelector(
		ctx,
		"direct-affinity-failover",
		nil,
		DestinationId(NewId()),
		true,
	)
	defer selector.Close()
	h3Transport := NewSendGatewayTransportWithType(TransportTypeH3)
	h3Route := make(Route, 4)
	selector.updateTransport(h3Transport, []Route{h3Route})
	h1Transport := NewSendGatewayTransportWithType(TransportTypeH1)
	h1Route := make(Route, 4)
	selector.updateTransport(h1Transport, []Route{h1Route})

	writeAndRequire := func(want TransportType) {
		t.Helper()
		message := MessagePoolGet(64)
		success, disposition, err := selector.writeDetailedWithCarrier(
			ctx,
			message,
			time.Second,
		)
		if err != nil || !success || disposition.transportType != want {
			if !success || err != nil {
				MessagePoolReturn(message)
			}
			t.Fatalf(
				"write = (%t, %+v, %v), want %s",
				success,
				disposition,
				err,
				want,
			)
		}
		switch want {
		case TransportTypeH3:
			MessagePoolReturn(<-h3Route)
		case TransportTypeH1:
			MessagePoolReturn(<-h1Route)
		}
	}

	writeAndRequire(TransportTypeH3)
	selector.updateTransport(h3Transport, nil)
	writeAndRequire(TransportTypeH1)
	selector.updateTransport(h3Transport, []Route{h3Route})
	writeAndRequire(TransportTypeH1)
	stats := selector.directAffinity.snapshot()
	if stats.PreferredH3WriteCount != 1 ||
		stats.PreferredH1WriteCount != 1 ||
		stats.FallbackH1WriteCount != 0 ||
		stats.FallbackH3WriteCount != 0 ||
		stats.PreferredBlockedCount != 0 ||
		stats.ActivationCount != 2 ||
		stats.RouteChangeCount != 1 {
		t.Fatalf("direct affinity failover stats = %+v", stats)
	}
}

// An ordinary Transfer timeout on bytes already accepted by H1 is stronger
// evidence than local queue pressure. It moves only this destination sequence
// to the tied H3 route while keeping H1 active for other sequences.
func TestMultiRouteWriterDirectAffinityFailsH1OverToHealthyH3OnTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	selector := NewMultiRouteSelector(
		ctx,
		"direct-affinity-h1-timeout",
		nil,
		DestinationId(NewId()),
		true,
	)
	defer selector.Close()
	h1Transport := NewSendGatewayTransportWithType(TransportTypeH1)
	h1Route := make(Route, 2)
	selector.updateTransport(h1Transport, []Route{h1Route})
	h3Transport := NewSendGatewayTransportWithType(TransportTypeH3)
	h3Route := make(Route, 2)
	selector.updateTransport(h3Transport, []Route{h3Route})

	writeAndRequire := func(want TransportType) {
		t.Helper()
		message := MessagePoolGet(64)
		success, disposition, err := selector.writeDetailedWithCarrier(
			ctx,
			message,
			time.Second,
		)
		if err != nil || !success || disposition.transportType != want {
			if !success || err != nil {
				MessagePoolReturn(message)
			}
			t.Fatalf("write=(%t, %+v, %v), want %s", success, disposition, err, want)
		}
		switch want {
		case TransportTypeH1:
			MessagePoolReturn(<-h1Route)
		case TransportTypeH3:
			MessagePoolReturn(<-h3Route)
		}
	}

	writeAndRequire(TransportTypeH1)
	if !selector.transferPreferH3AfterH1Timeout(h1Route) {
		t.Fatal("H1 timeout did not move the tied selector to healthy H3")
	}
	if selector.transferPreferH3AfterH1Timeout(h1Route) {
		t.Fatal("stale H1 timeout changed an already-H3-affine selector twice")
	}
	writeAndRequire(TransportTypeH3)
	if len(selector.GetActiveRoutes()) != 2 {
		t.Fatalf("timeout failover removed a healthy carrier: %v", selector.GetActiveRoutes())
	}
	stats := selector.directAffinity.snapshot()
	if stats.PreferredH1WriteCount != 1 ||
		stats.PreferredH3WriteCount != 1 ||
		stats.FallbackH1WriteCount != 0 ||
		stats.FallbackH3WriteCount != 0 ||
		stats.H1TimeoutFailoverCount != 1 ||
		stats.RouteChangeCount != 1 ||
		!stats.H3PreferredAfterH1Timeout {
		t.Fatalf("H1 timeout affinity stats=%+v", stats)
	}
}

func TestRouteManagerH1TimeoutPreferenceStaysOnAffectedSelector(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manager := NewRouteManager(ctx, "shared-h1-timeout")
	destinationA := DestinationId(NewId())
	destinationB := DestinationId(NewId())
	writerA := manager.OpenMultiRouteWriter(destinationA)
	defer manager.CloseMultiRouteWriter(writerA)
	writerB := manager.OpenMultiRouteWriter(destinationB)
	defer manager.CloseMultiRouteWriter(writerB)
	h1Transport := NewSendGatewayTransportWithType(TransportTypeH1)
	h1Route := make(Route, 4)
	manager.UpdateTransport(h1Transport, []Route{h1Route})
	h3Transport := NewSendGatewayTransportWithType(TransportTypeH3)
	h3Route := make(Route, 4)
	manager.UpdateTransport(h3Transport, []Route{h3Route})

	writeAndRequire := func(writer MultiRouteWriter, want TransportType) {
		t.Helper()
		selector := writer.(*MultiRouteSelector)
		message := MessagePoolGet(64)
		success, disposition, err := selector.writeDetailedWithCarrier(
			ctx,
			message,
			time.Second,
		)
		if err != nil || !success || disposition.transportType != want {
			if err != nil || !success {
				MessagePoolReturn(message)
			}
			t.Fatalf("write=(%t, %+v, %v), want %s", success, disposition, err, want)
		}
		if want == TransportTypeH1 {
			MessagePoolReturn(<-h1Route)
		} else {
			MessagePoolReturn(<-h3Route)
		}
	}

	writeAndRequire(writerA, TransportTypeH1)
	writeAndRequire(writerB, TransportTypeH1)
	if !writerA.(*MultiRouteSelector).transferPreferH3AfterH1Timeout(h1Route) {
		t.Fatal("H1 timeout did not move the affected selector to H3")
	}
	writeAndRequire(writerA, TransportTypeH3)
	writeAndRequire(writerB, TransportTypeH1)
	stats := manager.DirectCarrierAffinityStats()
	if !stats.H3PreferredAfterH1Timeout || stats.H1TimeoutFailoverCount != 1 ||
		stats.RouteChangeCount != 1 ||
		stats.FallbackH1WriteCount != 0 || stats.FallbackH3WriteCount != 0 {
		t.Fatalf("selector-local H1 timeout stats=%+v", stats)
	}
}

// A full preferred queue is shared-uplink congestion, not evidence that the
// carrier disappeared. The writer waits instead of opening a second
// congestion-controller burst, but a physical route publication wakes that
// exact blocked write and moves it to the surviving carrier immediately.
func TestMultiRouteWriterDirectAffinityWaitsUntilRouteWithdrawal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	selector := NewMultiRouteSelector(
		ctx,
		"direct-affinity-backpressure",
		nil,
		DestinationId(NewId()),
		true,
	)
	defer selector.Close()
	h3Transport := NewSendGatewayTransportWithType(TransportTypeH3)
	h3Route := make(Route)
	selector.updateTransport(h3Transport, []Route{h3Route})
	h1Transport := NewSendGatewayTransportWithType(TransportTypeH1)
	h1Route := make(Route, 1)
	selector.updateTransport(h1Transport, []Route{h1Route})

	timedOutMessage := MessagePoolGet(64)
	success, _, err := selector.writeDetailedWithCarrier(
		ctx,
		timedOutMessage,
		10*time.Millisecond,
	)
	if err != nil || success {
		if success {
			MessagePoolReturn(<-h1Route)
		} else {
			MessagePoolReturn(timedOutMessage)
		}
		t.Fatalf("backpressured preferred write = (%t, %v), want timeout", success, err)
	}
	MessagePoolReturn(timedOutMessage)
	if len(h1Route) != 0 {
		MessagePoolReturn(<-h1Route)
		t.Fatal("transient H3 backpressure spilled onto H1")
	}

	type writeResult struct {
		success bool
		err     error
	}
	blockedMessage := MessagePoolGet(64)
	writeDone := make(chan writeResult, 1)
	go func() {
		success, _, err := selector.writeDetailedWithCarrier(
			ctx,
			blockedMessage,
			time.Second,
		)
		writeDone <- writeResult{success: success, err: err}
	}()
	deadline := time.Now().Add(time.Second)
	for selector.directAffinity.snapshot().PreferredBlockedCount < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if selector.directAffinity.snapshot().PreferredBlockedCount < 2 {
		selector.updateTransport(h3Transport, nil)
		result := <-writeDone
		if !result.success || result.err != nil {
			MessagePoolReturn(blockedMessage)
		} else {
			MessagePoolReturn(<-h1Route)
		}
		t.Fatal("second write did not block on its preferred route")
	}
	if len(h1Route) != 0 {
		selector.updateTransport(h3Transport, nil)
		<-writeDone
		MessagePoolReturn(<-h1Route)
		t.Fatal("blocked preferred write reached H1 before route withdrawal")
	}

	selector.updateTransport(h3Transport, nil)
	select {
	case result := <-writeDone:
		if !result.success || result.err != nil {
			MessagePoolReturn(blockedMessage)
			t.Fatalf("withdrawal failover = (%t, %v)", result.success, result.err)
		}
	case <-time.After(time.Second):
		MessagePoolReturn(blockedMessage)
		t.Fatal("route withdrawal did not wake the blocked write")
	}
	MessagePoolReturn(<-h1Route)
	stats := selector.directAffinity.snapshot()
	if stats.FallbackH1WriteCount != 0 || stats.FallbackH3WriteCount != 0 ||
		stats.PreferredBlockedCount != 2 || stats.RouteChangeCount != 1 {
		t.Fatalf("direct affinity backpressure stats = %+v", stats)
	}
}

func TestMultiRouteWriterCarrierPreferenceWaitsAndFallsBackOnlyAfterWithdrawal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	selector := NewMultiRouteSelector(
		ctx,
		"carrier-reply-affinity",
		nil,
		DestinationId(NewId()),
		true,
	)
	defer selector.Close()
	h1Transport := NewSendGatewayTransportWithType(TransportTypeH1)
	h1Route := make(Route, 1)
	selector.updateTransport(h1Transport, []Route{h1Route})
	h3Transport := NewSendGatewayTransportWithType(TransportTypeH3)
	h3Route := make(Route, 1)
	selector.updateTransport(h3Transport, []Route{h3Route})

	write := func(want TransportType) {
		t.Helper()
		message := MessagePoolGet(64)
		success, disposition, err := selector.writeDetailedWithCarrierPreference(
			ctx,
			message,
			time.Second,
			TransportTypeH3,
		)
		if err != nil || !success || disposition.transportType != want {
			if !success {
				MessagePoolReturn(message)
			}
			t.Fatalf("preferred write=(%t, %+v, %v), want %s", success, disposition, err, want)
		}
		if want == TransportTypeH3 {
			MessagePoolReturn(<-h3Route)
		} else {
			MessagePoolReturn(<-h1Route)
		}
	}

	write(TransportTypeH3)
	blocker := MessagePoolGet(32)
	h3Route <- blocker
	blockedMessage := MessagePoolGet(64)
	success, _, err := selector.writeDetailedWithCarrierPreference(
		ctx,
		blockedMessage,
		10*time.Millisecond,
		TransportTypeH3,
	)
	if err != nil || success {
		if success {
			MessagePoolReturn(<-h3Route)
		} else {
			MessagePoolReturn(blockedMessage)
		}
		t.Fatalf("backpressured carrier-affine write=(%t, %v), want timeout", success, err)
	}
	MessagePoolReturn(blockedMessage)
	select {
	case message := <-h1Route:
		MessagePoolReturn(message)
		t.Fatal("carrier-affine write spilled to H1 under H3 queue pressure")
	default:
	}
	MessagePoolReturn(<-h3Route)

	selector.updateTransport(h3Transport, nil)
	write(TransportTypeH1)
}

func TestMultiRouteWriterH1AckPriorityBypassesFullOrdinaryRoute(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	selector := NewMultiRouteSelector(
		ctx,
		"h1-ack-priority",
		nil,
		DestinationId(NewId()),
		true,
	)
	defer selector.Close()
	h1Route := make(Route, 1)
	priorityRoute := make(Route, 1)
	selector.updateTransport(
		NewSendGatewayTransportWithType(TransportTypeH1),
		[]Route{h1Route},
	)
	registerH1AckPriorityRoute(h1Route, priorityRoute)
	defer unregisterH1AckPriorityRoute(h1Route)

	ordinaryBlocker := MessagePoolGet(32)
	h1Route <- ordinaryBlocker
	ack := MessagePoolGet(64)
	success, disposition := selector.tryWriteH1AckPriorityWithCarrierPreference(
		ack,
		TransportTypeH1,
	)
	if !success || disposition.route != h1Route ||
		disposition.transportType != TransportTypeH1 || !disposition.reliable {
		if !success {
			MessagePoolReturn(ack)
		}
		t.Fatalf("priority ACK disposition = (%t, %+v)", success, disposition)
	}
	if got := <-priorityRoute; &got[0] != &ack[0] {
		MessagePoolReturn(got)
		t.Fatal("priority route did not receive the original pooled ownership")
	} else {
		MessagePoolReturn(got)
	}
	MessagePoolReturn(<-h1Route)

	unregisterH1AckPriorityRoute(h1Route)
	ack = MessagePoolGet(64)
	if success, _ := selector.tryWriteH1AckPriorityWithCarrierPreference(
		ack,
		TransportTypeH1,
	); success {
		MessagePoolReturn(<-priorityRoute)
		t.Fatal("unregistered priority route accepted an ACK")
	}
	MessagePoolReturn(ack)
}

func TestMultiRouteWriterFullH1AckPriorityFallsThroughWithoutOwnership(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	selector := NewMultiRouteSelector(
		ctx,
		"h1-ack-priority-full",
		nil,
		DestinationId(NewId()),
		true,
	)
	defer selector.Close()
	h1Route := make(Route, 1)
	priorityRoute := make(Route, 1)
	selector.updateTransport(
		NewSendGatewayTransportWithType(TransportTypeH1),
		[]Route{h1Route},
	)
	registerH1AckPriorityRoute(h1Route, priorityRoute)
	defer unregisterH1AckPriorityRoute(h1Route)

	priorityBlocker := MessagePoolGet(32)
	priorityRoute <- priorityBlocker
	ack := MessagePoolGet(64)
	if success, _ := selector.tryWriteH1AckPriorityWithCarrierPreference(
		ack,
		TransportTypeH1,
	); success {
		MessagePoolReturn(<-priorityRoute)
		t.Fatal("full priority route unexpectedly accepted an ACK")
	}
	// A failed nonblocking attempt transfers no ownership.
	MessagePoolReturn(ack)
	MessagePoolReturn(<-priorityRoute)
}

func TestMultiRouteWriterDispositionReportsInitialQueueBlock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	selector := NewMultiRouteSelector(
		ctx,
		"blocked-write-disposition",
		nil,
		DestinationId(NewId()),
		true,
	)
	defer selector.Close()
	route := make(Route, 1)
	route <- MessagePoolGet(32)
	selector.updateTransport(
		NewSendGatewayTransportWithType(TransportTypeH1),
		[]Route{route},
	)

	type result struct {
		success     bool
		disposition transferWriteDisposition
		err         error
	}
	done := make(chan result, 1)
	message := MessagePoolGet(64)
	go func() {
		success, disposition, err := selector.writeDetailedWithCarrier(
			ctx,
			message,
			time.Second,
		)
		done <- result{success, disposition, err}
	}()

	deadline := time.Now().Add(time.Second)
	for selector.writeMutex.TryLock() {
		selector.writeMutex.Unlock()
		if deadline.Before(time.Now()) {
			MessagePoolReturn(<-route)
			t.Fatal("write did not enter the blocked selector path")
		}
		runtime.Gosched()
	}
	MessagePoolReturn(<-route)
	writeResult := <-done
	if !writeResult.success || writeResult.err != nil {
		MessagePoolReturn(message)
		t.Fatalf(
			"blocked write result=(%t, %v)",
			writeResult.success,
			writeResult.err,
		)
	}
	MessagePoolReturn(<-route)
	if !writeResult.disposition.initiallyBlocked {
		t.Fatal("blocked route write was reported as an immediate write")
	}
	if writeResult.disposition.initialWaitDuration <= 0 {
		t.Fatalf(
			"blocked route wait=%v, want positive duration",
			writeResult.disposition.initialWaitDuration,
		)
	}
}

// Affinity is intentionally narrower than generic route weighting. P2P and
// DNS routes can have different failure domains, and explicitly unequal H1/H3
// priorities are an operator policy; none may be silently converted to the
// direct Auto tie behavior.
func TestMultiRouteWriterDirectAffinityScope(t *testing.T) {
	tests := []struct {
		name       string
		transports []Transport
		wantSticky bool
	}{
		{
			name: "equal direct tie",
			transports: []Transport{
				newTypedPriorityRouteTestTransport(TransportTypeH3, 10),
				newTypedPriorityRouteTestTransport(TransportTypeH1, 10),
			},
			wantSticky: true,
		},
		{
			name: "unequal direct priorities",
			transports: []Transport{
				newTypedPriorityRouteTestTransport(TransportTypeH3, 10),
				newTypedPriorityRouteTestTransport(TransportTypeH1, 20),
			},
		},
		{
			name: "p2p route present",
			transports: []Transport{
				newTypedPriorityRouteTestTransport(TransportTypeH3, 10),
				newTypedPriorityRouteTestTransport(TransportTypeH1, 10),
				NewSendGatewayTransportWithType(TransportTypeP2p),
			},
		},
		{
			name: "dns fallback present",
			transports: []Transport{
				newTypedPriorityRouteTestTransport(TransportTypeH3, 10),
				newTypedPriorityRouteTestTransport(TransportTypeH1, 10),
				NewSendGatewayTransportWithType(TransportTypeH3Dns),
			},
		},
		{
			name: "one direct type",
			transports: []Transport{
				newTypedPriorityRouteTestTransport(TransportTypeH3, 10),
				newTypedPriorityRouteTestTransport(TransportTypeH3, 10),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			selector := NewMultiRouteSelector(
				ctx,
				"direct-affinity-scope",
				nil,
				DestinationId(NewId()),
				true,
			)
			defer selector.Close()
			for _, transport := range test.transports {
				selector.updateTransport(transport, []Route{make(Route, 1)})
			}
			snapshot := selector.activeRoutesSnapshot.Load()
			if gotSticky := snapshot.preferDirectRoute != nil; gotSticky != test.wantSticky {
				t.Fatalf(
					"preferDirectRoute present = %t, want %t for routes %v",
					gotSticky,
					test.wantSticky,
					snapshot.routeTransportTypes,
				)
			}
		})
	}
}

func TestTransportAffinityIsASetAndAllowsHigherPriorityRoutes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	selector := NewMultiRouteSelector(
		ctx,
		"priority-aware-affinity",
		nil,
		DestinationId(NewId()),
		true,
	)
	defer selector.Close()

	affinity10 := make(Route, 1)
	affinity20 := make(Route, 1)
	higher5 := make(Route, 1)
	higher15 := make(Route, 1)
	equal20 := make(Route, 1)
	lower30 := make(Route, 1)
	selector.updateTransport(
		newTypedPriorityRouteTestTransport(TransportTypeH1, 10),
		[]Route{affinity10},
	)
	selector.updateTransport(
		newTypedPriorityRouteTestTransport(TransportTypeH1, 20),
		[]Route{affinity20},
	)
	selector.updateTransport(
		newTypedPriorityRouteTestTransport(TransportTypeH3, 5),
		[]Route{higher5},
	)
	selector.updateTransport(
		newTypedPriorityRouteTestTransport(TransportTypeH3, 15),
		[]Route{higher15},
	)
	selector.updateTransport(
		newTypedPriorityRouteTestTransport(TransportTypeH3, 20),
		[]Route{equal20},
	)
	selector.updateTransport(
		newTypedPriorityRouteTestTransport(TransportTypeH3, 30),
		[]Route{lower30},
	)

	routes := selector.activeRoutesSnapshot.Load().writeRoutesForTransport(TransportTypeH1)
	got := map[Route]bool{}
	for _, route := range routes {
		got[route] = true
	}
	for _, route := range []Route{affinity10, affinity20, higher5, higher15} {
		if !got[route] {
			t.Fatalf("eligible routes omitted affinity or higher-priority route %p", route)
		}
	}
	for _, route := range []Route{equal20, lower30} {
		if got[route] {
			t.Fatalf("affinity admitted same/lower-priority non-affinity route %p", route)
		}
	}

	message := MessagePoolGet(64)
	success, disposition, err := selector.writeDetailedWithCarrierPreference(
		ctx,
		message,
		time.Second,
		TransportTypeH1,
	)
	if err != nil || !success || disposition.route != higher5 {
		if !success {
			MessagePoolReturn(message)
		}
		t.Fatalf("priority-aware affinity write=(%t, %+v, %v), want priority-5 route", success, disposition, err)
	}
	MessagePoolReturn(<-higher5)

	block5 := MessagePoolGet(8)
	block15 := MessagePoolGet(8)
	higher5 <- block5
	higher15 <- block15
	message = MessagePoolGet(64)
	success, disposition, err = selector.writeDetailedWithCarrierPreference(
		ctx,
		message,
		time.Second,
		TransportTypeH1,
	)
	if err != nil || !success || (disposition.route != affinity10 && disposition.route != affinity20) {
		if !success {
			MessagePoolReturn(message)
		}
		t.Fatalf("blocked higher-priority affinity write=(%t, %+v, %v), want an original H1 route", success, disposition, err)
	}
	MessagePoolReturn(<-disposition.route)
	MessagePoolReturn(<-higher5)
	MessagePoolReturn(<-higher15)
}

// HasActiveTransport is the transport cross-check the blackhole and stall
// verdicts consult: an empty transport set means the client has no carrier,
// so its silence proves nothing about the remote end. The set must read empty
// before any registration, non-empty while a transport holds routes, and
// empty again after removal -- a stale true would let verdicts convict a
// provider whose channel cannot even reach the network.
func TestRouteManagerHasActiveTransport(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	routeManager := NewRouteManager(ctx, "test")
	AssertEqual(t, routeManager.HasActiveTransport(), false)
	AssertEqual(t, routeManager.HasActiveUnreliableSendTransport(), false)

	transport := NewSendGatewayTransport()
	routes := []Route{make(chan []byte)}
	routeManager.UpdateTransport(transport, routes)
	AssertEqual(t, routeManager.HasActiveTransport(), true)
	AssertEqual(t, routeManager.HasActiveUnreliableSendTransport(), false)

	routeManager.UpdateTransportWithProperties(
		transport,
		routes,
		TransferCarrierProperties{Unreliable: true},
	)
	AssertEqual(t, routeManager.HasActiveUnreliableSendTransport(), true)

	// removal registers nil routes, which must empty the set
	routeManager.RemoveTransport(transport)
	AssertEqual(t, routeManager.HasActiveTransport(), false)
	AssertEqual(t, routeManager.HasActiveUnreliableSendTransport(), false)
}

// finishPausedRouteWrite releases and joins a test writer before returning its
// message from whichever side still owns it.
func finishPausedRouteWrite(
	t *testing.T,
	resumeWriter func(),
	writeDone <-chan error,
	route <-chan []byte,
	message []byte,
) {
	t.Helper()
	resumeWriter()
	select {
	case err := <-writeDone:
		if err != nil {
			if !MessagePoolReturn(message) {
				t.Error("failed writer did not retain its pooled message")
			}
			return
		}
		select {
		case deliveredMessage := <-route:
			assertReturnExactRouteMessage(t, deliveredMessage, message)
		case <-time.After(time.Second):
			t.Error("paused writer succeeded without delivering its message")
		}
	case <-time.After(time.Second):
		t.Error("paused writer did not stop during failure cleanup")
	}
}

// assertReturnExactRouteMessage proves one route received and relinquished the
// exact pooled buffer whose ownership the writer accepted.
func assertReturnExactRouteMessage(t *testing.T, deliveredMessage []byte, message []byte) {
	t.Helper()
	if len(deliveredMessage) != len(message) ||
		0 < len(message) && &deliveredMessage[0] != &message[0] {
		if !MessagePoolReturn(deliveredMessage) {
			t.Error("unexpected route message was not pool-owned")
		}
		t.Error("route did not receive the exact admitted writer buffer")
		return
	}
	if !MessagePoolReturn(deliveredMessage) {
		t.Error("route message ownership was already released")
	}
}

// RemoveTransport publishes the route-free generation before it returns, but
// an admitted writer may still hold the previous immutable snapshot. Removal
// must join that writer so the route owner can drain without a late enqueue.
func TestRouteManagerRemoveTransportJoinsAdmittedWriterSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	destination := DestinationId(NewId())
	routeManager := NewRouteManager(ctx, "test")
	writer := routeManager.OpenMultiRouteWriter(destination)
	selector := writer.(*MultiRouteSelector)
	defer routeManager.CloseMultiRouteWriter(writer)
	route := make(chan []byte, 1)
	transport := NewSendGatewayTransport()
	routeManager.UpdateTransport(transport, []Route{route})
	defer routeManager.RemoveTransport(transport)

	message := MessagePoolGet(128)
	snapshotAcquired, removalWaiting, resumeWriter := TestingPauseMultiRouteWriterSnapshot(writer)
	defer resumeWriter()
	writeDone := make(chan error, 1)
	go func() {
		success, err := writer.WriteDetailed(ctx, message, time.Second)
		if err == nil && !success {
			err = errors.New("paused writer did not send")
		}
		writeDone <- err
	}()
	select {
	case <-snapshotAcquired:
	case <-time.After(time.Second):
		finishPausedRouteWrite(t, resumeWriter, writeDone, route, message)
		t.Fatal("writer did not acquire the transport snapshot")
	}
	removeDone := make(chan struct{})
	go func() {
		routeManager.RemoveTransport(transport)
		close(removeDone)
	}()
	select {
	case <-removalWaiting:
	case <-time.After(time.Second):
		finishPausedRouteWrite(t, resumeWriter, writeDone, route, message)
		t.Fatal("transport removal did not reach the writer join")
	}
	selector.mutex.Lock()
	_, routeStatsPresent := selector.routeStats[route]
	selector.mutex.Unlock()
	if routeStatsPresent {
		finishPausedRouteWrite(t, resumeWriter, writeDone, route, message)
		t.Fatal("transport removal retained route stats before old-writer release")
	}
	select {
	case <-removeDone:
		finishPausedRouteWrite(t, resumeWriter, writeDone, route, message)
		t.Fatal("transport removal returned with an admitted old snapshot")
	default:
	}

	resumeWriter()
	select {
	case err := <-writeDone:
		if err != nil {
			if !MessagePoolReturn(message) {
				t.Error("failed writer did not retain its pooled message")
			}
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("paused writer did not resume")
	}
	select {
	case <-removeDone:
	case <-time.After(time.Second):
		t.Fatal("transport removal did not join the released snapshot")
	}
	selector.mutex.Lock()
	_, routeStatsPresent = selector.routeStats[route]
	selector.mutex.Unlock()
	if routeStatsPresent {
		t.Fatal("old writer recreated route stats after transport removal")
	}
	assertReturnExactRouteMessage(t, <-route, message)
}

// An asynchronous alias clear indexes its admitted old generation by physical
// transport. Later removal must join it before the route owner drains.
func TestAsyncAliasRetirementRemainsJoinedByTransportRemoval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	finalDestination := DestinationId(NewId())
	streamId := NewId()
	routeManager := NewRouteManager(ctx, "test")
	writer := routeManager.OpenMultiRouteWriter(finalDestination)
	defer routeManager.CloseMultiRouteWriter(writer)
	route := make(chan []byte, 1)
	transport := NewSendClientTransport(StreamId(streamId))
	routeManager.UpdateTransport(transport, []Route{route})
	defer routeManager.RemoveTransport(transport)
	generation, ok := routeManager.beginWriterStreamAliasGeneration(streamId)
	if !ok {
		t.Fatal("writer stream alias generation was rejected")
	}
	closeScope, ok := routeManager.openWriterStreamAliasScopeForGeneration(
		streamId,
		generation,
	)
	if !ok {
		t.Fatal("writer stream alias scope was rejected")
	}
	defer closeScope()
	routeManager.finishWriterStreamAliasGeneration(streamId, generation)
	if !routeManager.authenticateWriterStreamDestination(
		streamId,
		finalDestination.DestinationId,
	) {
		t.Fatal("live stream authentication did not publish its writer alias")
	}

	message := MessagePoolGet(128)
	snapshotAcquired, retirementWaiting, resumeWriter := TestingPauseMultiRouteWriterSnapshot(writer)
	defer resumeWriter()
	writeDone := make(chan error, 1)
	go func() {
		success, err := writer.WriteDetailed(ctx, message, time.Second)
		if err == nil && !success {
			err = errors.New("paused async-alias writer did not send")
		}
		writeDone <- err
	}()
	select {
	case <-snapshotAcquired:
	case <-time.After(time.Second):
		finishPausedRouteWrite(t, resumeWriter, writeDone, route, message)
		t.Fatal("writer did not acquire the async-alias snapshot")
	}
	if !routeManager.clearWriterStreamAliasScopeThroughGenerationAsync(
		streamId,
		generation,
	) {
		finishPausedRouteWrite(t, resumeWriter, writeDone, route, message)
		t.Fatal("async alias clear was not applied")
	}
	if activeRoutes := writer.GetActiveRoutes(); len(activeRoutes) != 0 {
		finishPausedRouteWrite(t, resumeWriter, writeDone, route, message)
		t.Fatalf("async alias clear left %d active routes", len(activeRoutes))
	}

	removeDone := make(chan struct{})
	go func() {
		routeManager.RemoveTransport(transport)
		close(removeDone)
	}()
	select {
	case <-retirementWaiting:
	case <-time.After(time.Second):
		finishPausedRouteWrite(t, resumeWriter, writeDone, route, message)
		t.Fatal("physical removal did not join the async alias retirement")
	}
	select {
	case <-removeDone:
		finishPausedRouteWrite(t, resumeWriter, writeDone, route, message)
		t.Fatal("physical removal bypassed the async alias retirement")
	default:
	}

	resumeWriter()
	select {
	case err := <-writeDone:
		if err != nil {
			if !MessagePoolReturn(message) {
				t.Error("failed writer did not retain its pooled message")
			}
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("async-alias writer did not resume")
	}
	select {
	case <-removeDone:
	case <-time.After(time.Second):
		t.Fatal("physical removal did not finish after async writer release")
	}
	assertReturnExactRouteMessage(t, <-route, message)
}

// A paused alias retirement for transport A must not delay independent
// physical removal of transport B. Each owner joins only snapshots that could
// still enqueue into its own routes.
func TestAsyncAliasRetirementDoesNotBlockUnrelatedTransportRemoval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	routeManager := NewRouteManager(ctx, "test")
	destinationA := DestinationId(NewId())
	destinationB := DestinationId(NewId())
	aliasA := StreamId(NewId())
	writerA := routeManager.OpenMultiRouteWriter(destinationA)
	defer routeManager.CloseMultiRouteWriter(writerA)
	writerB := routeManager.OpenMultiRouteWriter(destinationB)
	defer routeManager.CloseMultiRouteWriter(writerB)
	routeA := make(chan []byte, 1)
	routeB := make(chan []byte, 1)
	transportA := NewSendClientTransport(aliasA)
	transportB := NewSendClientTransport(destinationB)
	routeManager.UpdateTransport(transportA, []Route{routeA})
	defer routeManager.RemoveTransport(transportA)
	routeManager.UpdateTransport(transportB, []Route{routeB})
	defer routeManager.RemoveTransport(transportB)
	routeManager.AddWriterDestinationAlias(destinationA, aliasA)

	message := MessagePoolGet(128)
	snapshotAcquired, retirementWaiting, resumeWriter := TestingPauseMultiRouteWriterSnapshot(writerA)
	defer resumeWriter()
	writeDone := make(chan error, 1)
	go func() {
		success, err := writerA.WriteDetailed(ctx, message, time.Second)
		if err == nil && !success {
			err = errors.New("paused independent writer did not send")
		}
		writeDone <- err
	}()
	select {
	case <-snapshotAcquired:
	case <-time.After(time.Second):
		finishPausedRouteWrite(t, resumeWriter, writeDone, routeA, message)
		t.Fatal("writer A did not acquire its alias snapshot")
	}
	routeManager.updateWriterMatchStateAsync(func() {
		routeManager.writerMatchState.removeDestinationAliasWithLock(destinationA, aliasA)
	})

	removeBDone := make(chan struct{})
	go func() {
		routeManager.RemoveTransport(transportB)
		close(removeBDone)
	}()
	select {
	case <-removeBDone:
	case <-time.After(time.Second):
		finishPausedRouteWrite(t, resumeWriter, writeDone, routeA, message)
		t.Fatal("transport B removal waited for transport A's writer")
	}

	removeADone := make(chan struct{})
	go func() {
		routeManager.RemoveTransport(transportA)
		close(removeADone)
	}()
	select {
	case <-retirementWaiting:
	case <-time.After(time.Second):
		finishPausedRouteWrite(t, resumeWriter, writeDone, routeA, message)
		t.Fatal("transport A removal did not join its alias retirement")
	}
	select {
	case <-removeADone:
		finishPausedRouteWrite(t, resumeWriter, writeDone, routeA, message)
		t.Fatal("transport A removal bypassed its admitted writer")
	default:
	}
	resumeWriter()
	select {
	case err := <-writeDone:
		if err != nil {
			if !MessagePoolReturn(message) {
				t.Error("failed writer did not retain its pooled message")
			}
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("writer A did not resume")
	}
	select {
	case <-removeADone:
	case <-time.After(time.Second):
		t.Fatal("transport A removal did not finish")
	}
	assertReturnExactRouteMessage(t, <-routeA, message)
}

// Repeated alias publications without another admitted writer do not grow the
// pending retirement indexes behind one intentionally stalled generation.
func TestAsyncAliasRetirementStateBoundedByAdmittedWriters(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	routeManager := NewRouteManager(ctx, "test")
	destination := DestinationId(NewId())
	alias := StreamId(NewId())
	writer := routeManager.OpenMultiRouteWriter(destination)
	defer routeManager.CloseMultiRouteWriter(writer)
	route := make(chan []byte, 1)
	transport := NewSendClientTransport(alias)
	routeManager.UpdateTransport(transport, []Route{route})
	defer routeManager.RemoveTransport(transport)
	routeManager.AddWriterDestinationAlias(destination, alias)

	message := MessagePoolGet(128)
	snapshotAcquired, retirementWaiting, resumeWriter := TestingPauseMultiRouteWriterSnapshot(writer)
	defer resumeWriter()
	writeDone := make(chan error, 1)
	go func() {
		success, err := writer.WriteDetailed(ctx, message, time.Second)
		if err == nil && !success {
			err = errors.New("paused churn writer did not send")
		}
		writeDone <- err
	}()
	select {
	case <-snapshotAcquired:
	case <-time.After(time.Second):
		finishPausedRouteWrite(t, resumeWriter, writeDone, route, message)
		t.Fatal("churn writer did not acquire its alias snapshot")
	}
	routeManager.updateWriterMatchStateAsync(func() {
		routeManager.writerMatchState.removeDestinationAliasWithLock(destination, alias)
	})
	for range 4 * maxWriterStreamAliasGenerations {
		routeManager.updateWriterMatchStateAsync(func() {
			routeManager.writerMatchState.addDestinationAliasWithLock(destination, alias)
		})
		routeManager.updateWriterMatchStateAsync(func() {
			routeManager.writerMatchState.removeDestinationAliasWithLock(destination, alias)
		})
	}

	routeManager.transportUpdateLock.Lock()
	routeManager.mutex.Lock()
	pendingCount := len(routeManager.pendingWriterSnapshots)
	transportPendingCount := len(routeManager.pendingWriterSnapshotsByTransport[transport])
	selectorPendingCount := len(routeManager.pendingWriterSnapshotsBySelector[writer.(*MultiRouteSelector)])
	routeManager.mutex.Unlock()
	routeManager.transportUpdateLock.Unlock()
	if pendingCount != 1 || transportPendingCount != 1 || selectorPendingCount != 1 {
		finishPausedRouteWrite(t, resumeWriter, writeDone, route, message)
		t.Fatalf(
			"churn pending total/transport/selector=%d/%d/%d, want 1/1/1",
			pendingCount,
			transportPendingCount,
			selectorPendingCount,
		)
	}

	removeDone := make(chan struct{})
	go func() {
		routeManager.RemoveTransport(transport)
		close(removeDone)
	}()
	select {
	case <-retirementWaiting:
	case <-time.After(time.Second):
		finishPausedRouteWrite(t, resumeWriter, writeDone, route, message)
		t.Fatal("churn transport removal did not join its one pending writer")
	}
	resumeWriter()
	select {
	case err := <-writeDone:
		if err != nil {
			if !MessagePoolReturn(message) {
				t.Error("failed writer did not retain its pooled message")
			}
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("churn writer did not resume")
	}
	select {
	case <-removeDone:
	case <-time.After(time.Second):
		t.Fatal("churn transport removal did not finish")
	}
	assertReturnExactRouteMessage(t, <-route, message)
	routeManager.transportUpdateLock.Lock()
	routeManager.mutex.Lock()
	pendingCount = len(routeManager.pendingWriterSnapshots)
	routeManager.mutex.Unlock()
	routeManager.transportUpdateLock.Unlock()
	if pendingCount != 0 {
		t.Fatalf("churn cleanup retained %d pending writer snapshots", pendingCount)
	}
}

// Removing an alias is also a destructive route publication. It must join a
// writer admitted to the old matching generation before reporting that the
// stream route is withdrawn.
func TestRouteManagerAliasRemovalJoinsAdmittedWriterSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	finalDestination := DestinationId(NewId())
	streamAlias := StreamId(NewId())
	routeManager := NewRouteManager(ctx, "test")
	writer := routeManager.OpenMultiRouteWriter(finalDestination)
	defer routeManager.CloseMultiRouteWriter(writer)
	route := make(chan []byte, 1)
	transport := NewSendClientTransport(streamAlias)
	routeManager.UpdateTransport(transport, []Route{route})
	defer routeManager.RemoveTransport(transport)
	removeAlias := routeManager.AddWriterDestinationAlias(
		finalDestination,
		streamAlias,
	)

	message := MessagePoolGet(128)
	snapshotAcquired, removalWaiting, resumeWriter := TestingPauseMultiRouteWriterSnapshot(writer)
	defer resumeWriter()
	writeDone := make(chan error, 1)
	go func() {
		success, err := writer.WriteDetailed(ctx, message, time.Second)
		if err == nil && !success {
			err = errors.New("paused alias writer did not send")
		}
		writeDone <- err
	}()
	select {
	case <-snapshotAcquired:
	case <-time.After(time.Second):
		finishPausedRouteWrite(t, resumeWriter, writeDone, route, message)
		t.Fatal("writer did not acquire the alias snapshot")
	}
	removeDone := make(chan struct{})
	go func() {
		removeAlias()
		close(removeDone)
	}()
	select {
	case <-removalWaiting:
	case <-time.After(time.Second):
		finishPausedRouteWrite(t, resumeWriter, writeDone, route, message)
		t.Fatal("alias removal did not reach the writer join")
	}
	select {
	case <-removeDone:
		finishPausedRouteWrite(t, resumeWriter, writeDone, route, message)
		t.Fatal("alias removal returned with an admitted old snapshot")
	default:
	}

	resumeWriter()
	select {
	case err := <-writeDone:
		if err != nil {
			if !MessagePoolReturn(message) {
				t.Error("failed writer did not retain its pooled message")
			}
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("paused alias writer did not resume")
	}
	select {
	case <-removeDone:
	case <-time.After(time.Second):
		t.Fatal("alias removal did not join the released snapshot")
	}
	if activeRoutes := writer.GetActiveRoutes(); len(activeRoutes) != 0 {
		t.Fatalf("alias removal left %d active routes", len(activeRoutes))
	}
	assertReturnExactRouteMessage(t, <-route, message)
}

// Writer generation admission remains allocation-free on the common
// single-route fast path.
func TestRouteSelectorWriterAdmissionAllocations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	selector := NewMultiRouteSelector(ctx, "test", nil, DestinationId(NewId()), true)
	defer selector.Close()
	route := make(chan []byte, 1)
	selector.updateTransport(NewSendGatewayTransport(), []Route{route})
	frame := make([]byte, 128)
	allocations := testing.AllocsPerRun(1000, func() {
		success, err := selector.WriteDetailed(ctx, frame, -1)
		if err != nil || !success {
			panic("single-route write failed")
		}
		<-route
	})
	if allocations != 0 {
		t.Fatalf("single-route writer admission allocations=%f, want 0", allocations)
	}
}

// One hostile stream identity cannot retain more authenticated final peers
// than its fixed per-stream relationship budget.
func TestWriterStreamAliasDestinationPerStreamBound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	routeManager := NewRouteManager(ctx, "test")
	streamId := NewId()
	closeScope := routeManager.openWriterStreamAliasScope(streamId)

	for range maxWriterStreamAliasDestinationsPerStream {
		if !routeManager.authenticateWriterStreamDestination(streamId, NewId()) {
			t.Fatal("in-budget live stream destination was rejected")
		}
	}
	if routeManager.authenticateWriterStreamDestination(streamId, NewId()) {
		t.Fatal("per-stream destination overflow was retained")
	}

	routeManager.mutex.Lock()
	destinationCount := len(routeManager.writerStreamAuthenticatedDestinations[streamId])
	globalCount := routeManager.writerStreamAuthenticatedDestinationCount
	routeManager.mutex.Unlock()
	if destinationCount != maxWriterStreamAliasDestinationsPerStream {
		t.Fatalf("per-stream destinations=%d, want %d", destinationCount, maxWriterStreamAliasDestinationsPerStream)
	}
	if globalCount != maxWriterStreamAliasDestinationsPerStream {
		t.Fatalf("global destinations=%d, want %d", globalCount, maxWriterStreamAliasDestinationsPerStream)
	}

	closeScope()
	routeManager.clearWriterStreamAliasScope(streamId)
	closeReusedScope := routeManager.openWriterStreamAliasScope(streamId)
	if !routeManager.authenticateWriterStreamDestination(streamId, NewId()) {
		closeReusedScope()
		t.Fatal("cleared per-stream destination capacity was not reusable")
	}
	routeManager.mutex.Lock()
	destinationCount = len(routeManager.writerStreamAuthenticatedDestinations[streamId])
	globalCount = routeManager.writerStreamAuthenticatedDestinationCount
	routeManager.mutex.Unlock()
	if destinationCount != 1 || globalCount != 1 {
		closeReusedScope()
		t.Fatalf(
			"reused per-stream/global destinations=%d/%d, want 1/1",
			destinationCount,
			globalCount,
		)
	}
	closeReusedScope()
	routeManager.clearWriterStreamAliasScope(streamId)
	routeManager.mutex.Lock()
	globalCount = routeManager.writerStreamAuthenticatedDestinationCount
	_, streamRetained := routeManager.writerStreamAuthenticatedDestinations[streamId]
	routeManager.mutex.Unlock()
	if globalCount != 0 || streamRetained {
		t.Fatalf(
			"per-stream cleanup destinations/retained=%d/%t, want 0/false",
			globalCount,
			streamRetained,
		)
	}
}

// Distinct live streams share one RouteManager-wide relationship budget; a
// new stream cannot bypass it after every earlier stream reaches its own cap.
func TestWriterStreamAliasDestinationGlobalBound(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	routeManager := NewRouteManager(ctx, "test")
	closeScopes := make([]func(), 0, maxWriterStreamAliasDestinations/maxWriterStreamAliasDestinationsPerStream+1)
	streamIds := make([]Id, 0, maxWriterStreamAliasDestinations/maxWriterStreamAliasDestinationsPerStream)

	for range maxWriterStreamAliasDestinations / maxWriterStreamAliasDestinationsPerStream {
		streamId := NewId()
		streamIds = append(streamIds, streamId)
		closeScopes = append(closeScopes, routeManager.openWriterStreamAliasScope(streamId))
		for range maxWriterStreamAliasDestinationsPerStream {
			if !routeManager.authenticateWriterStreamDestination(streamId, NewId()) {
				t.Fatal("in-budget global stream destination was rejected")
			}
		}
	}
	overflowStreamId := NewId()
	closeScopes = append(closeScopes, routeManager.openWriterStreamAliasScope(overflowStreamId))
	if routeManager.authenticateWriterStreamDestination(overflowStreamId, NewId()) {
		t.Fatal("global destination overflow was retained")
	}

	routeManager.mutex.Lock()
	globalCount := routeManager.writerStreamAuthenticatedDestinationCount
	_, overflowRetained := routeManager.writerStreamAuthenticatedDestinations[overflowStreamId]
	routeManager.mutex.Unlock()
	if globalCount != maxWriterStreamAliasDestinations {
		t.Fatalf("global destinations=%d, want %d", globalCount, maxWriterStreamAliasDestinations)
	}
	if overflowRetained {
		t.Fatal("global overflow created an authenticated stream record")
	}

	closeScopes[0]()
	routeManager.clearWriterStreamAliasScope(streamIds[0])
	if !routeManager.authenticateWriterStreamDestination(overflowStreamId, NewId()) {
		t.Fatal("released global destination capacity was not reusable")
	}
	routeManager.mutex.Lock()
	globalCount = routeManager.writerStreamAuthenticatedDestinationCount
	overflowDestinationCount := len(
		routeManager.writerStreamAuthenticatedDestinations[overflowStreamId],
	)
	routeManager.mutex.Unlock()
	wantReusedGlobalCount := maxWriterStreamAliasDestinations -
		maxWriterStreamAliasDestinationsPerStream + 1
	if globalCount != wantReusedGlobalCount || overflowDestinationCount != 1 {
		t.Fatalf(
			"reused global/stream destinations=%d/%d, want %d/1",
			globalCount,
			overflowDestinationCount,
			wantReusedGlobalCount,
		)
	}
	for _, closeScope := range closeScopes {
		closeScope()
	}
	routeManager.clearWriterStreamAliasScopesExcept(map[Id]bool{})
	routeManager.mutex.Lock()
	globalCount = routeManager.writerStreamAuthenticatedDestinationCount
	routeManager.mutex.Unlock()
	if globalCount != 0 {
		t.Fatalf("global destination cleanup retained %d relationships", globalCount)
	}
}

// Live alias scopes share the StreamBuffer sequence bound and release their
// capacity when scopes close, so sequential churn remains available.
func TestWriterStreamAliasScopeBoundAndReuse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	routeManager := NewRouteManager(ctx, "test")
	closeScopes := make([]func(), 0, maxWriterStreamAliasScopes)
	for range maxWriterStreamAliasScopes {
		closeScopes = append(closeScopes, routeManager.openWriterStreamAliasScope(NewId()))
	}
	overflowStreamId := NewId()
	overflowGeneration, ok := routeManager.beginWriterStreamAliasGeneration(overflowStreamId)
	if !ok {
		t.Fatal("scope overflow could not allocate a transient generation")
	}
	_, opened := routeManager.openWriterStreamAliasScopeForGeneration(
		overflowStreamId,
		overflowGeneration,
	)
	routeManager.finishWriterStreamAliasGeneration(overflowStreamId, overflowGeneration)
	if opened {
		t.Fatal("live writer stream alias scope exceeded its bound")
	}
	for _, closeScope := range closeScopes {
		closeScope()
	}
	closeReusedScope := routeManager.openWriterStreamAliasScope(NewId())
	closeReusedScope()
}

// Concurrent construction tokens stop at their fixed generation bound, and
// finishing one token immediately makes that capacity reusable.
func TestWriterStreamAliasGenerationBoundAndReuse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	routeManager := NewRouteManager(ctx, "test")
	streamIds := make([]Id, maxWriterStreamAliasGenerations)
	generations := make([]uint64, maxWriterStreamAliasGenerations)
	for index := range streamIds {
		streamIds[index] = NewId()
		generation, ok := routeManager.beginWriterStreamAliasGeneration(streamIds[index])
		if !ok {
			t.Fatalf("in-budget generation %d was rejected", index)
		}
		generations[index] = generation
	}
	if _, ok := routeManager.beginWriterStreamAliasGeneration(NewId()); ok {
		t.Fatal("writer stream alias generation exceeded its bound")
	}
	routeManager.finishWriterStreamAliasGeneration(streamIds[0], generations[0])
	reusedStreamId := NewId()
	reusedGeneration, ok := routeManager.beginWriterStreamAliasGeneration(reusedStreamId)
	if !ok {
		t.Fatal("finished generation capacity was not reusable")
	}
	routeManager.finishWriterStreamAliasGeneration(reusedStreamId, reusedGeneration)
	for index := 1; index < len(streamIds); index += 1 {
		routeManager.finishWriterStreamAliasGeneration(streamIds[index], generations[index])
	}
	routeManager.mutex.Lock()
	remainingGenerationCount := len(routeManager.writerStreamAliasGenerations)
	routeManager.mutex.Unlock()
	if remainingGenerationCount != 0 {
		t.Fatalf("generation cleanup retained %d tokens", remainingGenerationCount)
	}
}

func TestP2pSendTransportMatchesPeerDestination(t *testing.T) {
	// when a stream is created, the stream send transport must carry
	// any traffic addressed to the peer,
	// not only traffic tagged with the stream id

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peerId := NewId()
	otherId := NewId()
	streamId := NewId()

	routeManager := NewRouteManager(ctx, "test")

	// writers open before the stream transport exists
	peerWriter := routeManager.OpenMultiRouteWriter(DestinationId(peerId))
	defer routeManager.CloseMultiRouteWriter(peerWriter)

	peerStreamWriter := routeManager.OpenMultiRouteWriter(TransferPath{
		DestinationId: peerId,
		StreamId:      streamId,
	})
	defer routeManager.CloseMultiRouteWriter(peerStreamWriter)

	streamWriter := routeManager.OpenMultiRouteWriter(TransferPath{
		StreamId: streamId,
	})
	defer routeManager.CloseMultiRouteWriter(streamWriter)

	otherWriter := routeManager.OpenMultiRouteWriter(DestinationId(otherId))
	defer routeManager.CloseMultiRouteWriter(otherWriter)

	localConn, remoteConn := net.Pipe()
	defer localConn.Close()
	defer remoteConn.Close()

	transportCtx, transportCancel := context.WithCancel(ctx)
	defer transportCancel()

	transport, route := NewP2pSendTransportForPeer(
		transportCtx,
		transportCancel,
		localConn,
		peerId,
		streamId,
		DefaultP2pTransportSettings(),
	)
	routeManager.UpdateTransport(transport, []Route{route})
	defer routeManager.RemoveTransport(transport)

	// the stream transport matches the peer, the peer with stream, and the stream
	AssertEqual(t, 1, len(peerWriter.GetActiveRoutes()))
	AssertEqual(t, 1, len(peerStreamWriter.GetActiveRoutes()))
	AssertEqual(t, 1, len(streamWriter.GetActiveRoutes()))
	// the stream transport does not match other destinations
	AssertEqual(t, 0, len(otherWriter.GetActiveRoutes()))

	// a writer to the peer opened after the stream transport also matches
	latePeerWriter := routeManager.OpenMultiRouteWriter(DestinationId(peerId))
	defer routeManager.CloseMultiRouteWriter(latePeerWriter)
	AssertEqual(t, 1, len(latePeerWriter.GetActiveRoutes()))

	// traffic addressed to the peer flows over the stream transport conn
	message := []byte("traffic to the peer")
	err := peerWriter.Write(ctx, MessagePoolCopy(message), 1*time.Second)
	AssertEqual(t, err, nil)

	remoteConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	b := make([]byte, 64)
	n, err := remoteConn.Read(b)
	AssertEqual(t, err, nil)
	AssertEqual(t, message, b[:n])
}

// Keeps a logical final-destination writer on one adjacent stream transport
// only while its ref-counted stream alias and the transport routes are live.
func TestRouteManagerWriterDestinationAliasLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	finalDestinationId := NewId()
	adjacentPeerId := NewId()
	streamId := NewId()
	routeManager := NewRouteManager(ctx, "test")
	writer := routeManager.OpenMultiRouteWriter(DestinationId(finalDestinationId))
	defer routeManager.CloseMultiRouteWriter(writer)

	localConn, remoteConn := net.Pipe()
	defer localConn.Close()
	defer remoteConn.Close()
	transportCtx, transportCancel := context.WithCancel(ctx)
	defer transportCancel()
	transport, route := NewP2pSendTransportForPeer(
		transportCtx,
		transportCancel,
		localConn,
		adjacentPeerId,
		streamId,
		DefaultP2pTransportSettings(),
	)
	routeManager.UpdateTransport(transport, []Route{route})
	defer routeManager.RemoveTransport(transport)

	AssertEqual(t, 0, len(writer.GetActiveRoutes()))
	removeFirstAlias := routeManager.AddWriterDestinationAlias(
		DestinationId(finalDestinationId),
		StreamId(streamId),
	)
	AssertEqual(t, 1, len(writer.GetActiveRoutes()))
	removeSecondAlias := routeManager.AddWriterDestinationAlias(
		DestinationId(finalDestinationId),
		StreamId(streamId),
	)

	removeFirstAlias()
	removeFirstAlias()
	AssertEqual(t, 1, len(writer.GetActiveRoutes()))
	removeSecondAlias()
	AssertEqual(t, 0, len(writer.GetActiveRoutes()))

	removeAlias := routeManager.AddWriterDestinationAlias(
		DestinationId(finalDestinationId),
		StreamId(streamId),
	)
	defer removeAlias()
	AssertEqual(t, 1, len(writer.GetActiveRoutes()))
	routeManager.RemoveTransport(transport)
	AssertEqual(t, 0, len(writer.GetActiveRoutes()))
	routeManager.UpdateTransport(transport, []Route{route})
	AssertEqual(t, 1, len(writer.GetActiveRoutes()))
}

// Removing a redundant stream alias must retain a transport that independently
// matches the writer's final destination.
func TestRouteManagerWriterAliasRemovalKeepsDirectMatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	finalDestinationId := NewId()
	streamId := NewId()
	routeManager := NewRouteManager(ctx, "test")
	writer := routeManager.OpenMultiRouteWriter(DestinationId(finalDestinationId))
	defer routeManager.CloseMultiRouteWriter(writer)

	localConn, remoteConn := net.Pipe()
	defer localConn.Close()
	defer remoteConn.Close()
	transportCtx, transportCancel := context.WithCancel(ctx)
	defer transportCancel()
	transport, route := NewP2pSendTransportForPeer(
		transportCtx,
		transportCancel,
		localConn,
		finalDestinationId,
		streamId,
		DefaultP2pTransportSettings(),
	)
	routeManager.UpdateTransport(transport, []Route{route})
	defer routeManager.RemoveTransport(transport)
	AssertEqual(t, 1, len(writer.GetActiveRoutes()))

	removeAlias := routeManager.AddWriterDestinationAlias(
		DestinationId(finalDestinationId),
		StreamId(streamId),
	)
	AssertEqual(t, 1, len(writer.GetActiveRoutes()))
	removeAlias()
	AssertEqual(t, 1, len(writer.GetActiveRoutes()))
}

// Two live stream generations and the exchange gateway are all eligible for
// one destination-only writer. The same logical payload succeeds when each
// physical path is made the sole route in turn.
func TestRouteManagerSharedAliasesArePathIndependent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	finalDestinationId := NewId()
	streamId1 := NewId()
	streamId2 := NewId()
	routeManager := NewRouteManager(ctx, "test")

	newTransport := func(streamId Id) (Transport, Route, net.Conn) {
		localConn, remoteConn := net.Pipe()
		t.Cleanup(func() {
			localConn.Close()
			remoteConn.Close()
		})
		transportCtx, transportCancel := context.WithCancel(ctx)
		t.Cleanup(transportCancel)
		transport, route := NewP2pSendTransportForPeer(
			transportCtx,
			transportCancel,
			localConn,
			NewId(),
			streamId,
			DefaultP2pTransportSettings(),
		)
		routeManager.UpdateTransport(transport, []Route{route})
		t.Cleanup(func() {
			routeManager.RemoveTransport(transport)
		})
		return transport, route, remoteConn
	}
	transport1, route1, remoteConn1 := newTransport(streamId1)
	transport2, route2, remoteConn2 := newTransport(streamId2)
	gatewayRoute := make(chan []byte, 1)
	gatewayTransport := NewSendGatewayTransport()
	routeManager.UpdateTransport(gatewayTransport, []Route{gatewayRoute})
	defer routeManager.RemoveTransport(gatewayTransport)

	removeAlias1 := routeManager.AddWriterDestinationAlias(
		DestinationId(finalDestinationId),
		StreamId(streamId1),
	)
	defer removeAlias1()
	removeAlias2 := routeManager.AddWriterDestinationAlias(
		DestinationId(finalDestinationId),
		StreamId(streamId2),
	)
	defer removeAlias2()

	writer := routeManager.OpenMultiRouteWriter(DestinationId(finalDestinationId))
	defer routeManager.CloseMultiRouteWriter(writer)
	AssertEqual(t, 3, len(writer.GetActiveRoutes()))

	readMessage := func(conn net.Conn) []byte {
		AssertEqual(t, nil, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
		buffer := make([]byte, 64)
		readByteCount, err := conn.Read(buffer)
		AssertEqual(t, nil, err)
		return buffer[:readByteCount]
	}
	writeMessage := func(message []byte) {
		AssertEqual(t, nil, writer.Write(ctx, MessagePoolCopy(message), time.Second))
	}

	routeManager.RemoveTransport(transport2)
	routeManager.RemoveTransport(gatewayTransport)
	AssertEqual(t, 1, len(writer.GetActiveRoutes()))
	message1 := []byte("stream one")
	writeMessage(message1)
	AssertEqual(t, message1, readMessage(remoteConn1))

	routeManager.UpdateTransport(transport2, []Route{route2})
	routeManager.RemoveTransport(transport1)
	AssertEqual(t, 1, len(writer.GetActiveRoutes()))
	message2 := []byte("stream two")
	writeMessage(message2)
	AssertEqual(t, message2, readMessage(remoteConn2))

	routeManager.UpdateTransport(gatewayTransport, []Route{gatewayRoute})
	routeManager.RemoveTransport(transport2)
	AssertEqual(t, 1, len(writer.GetActiveRoutes()))
	gatewayMessage := []byte("exchange gateway")
	writeMessage(gatewayMessage)
	receivedGatewayMessage := <-gatewayRoute
	AssertEqual(t, gatewayMessage, receivedGatewayMessage)
	MessagePoolReturn(receivedGatewayMessage)

	routeManager.UpdateTransport(transport1, []Route{route1})
	routeManager.UpdateTransport(transport2, []Route{route2})
	AssertEqual(t, 3, len(writer.GetActiveRoutes()))
}

func TestP2pSendTransportTwoStreamsToSamePeer(t *testing.T) {
	// two streams to the same peer both carry traffic addressed to the peer:
	// a peer destination matches both stream transports,
	// a peer destination tagged with one stream matches both
	// (one by stream id, one by peer),
	// and a pure stream mask has no destination id
	// so it matches only its own stream transport

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peerId := NewId()
	streamId1 := NewId()
	streamId2 := NewId()

	routeManager := NewRouteManager(ctx, "test")

	localConn1, remoteConn1 := net.Pipe()
	defer localConn1.Close()
	defer remoteConn1.Close()
	localConn2, remoteConn2 := net.Pipe()
	defer localConn2.Close()
	defer remoteConn2.Close()

	transportCtx, transportCancel := context.WithCancel(ctx)
	defer transportCancel()

	transport1, route1 := NewP2pSendTransportForPeer(
		transportCtx,
		transportCancel,
		localConn1,
		peerId,
		streamId1,
		DefaultP2pTransportSettings(),
	)
	transport2, route2 := NewP2pSendTransportForPeer(
		transportCtx,
		transportCancel,
		localConn2,
		peerId,
		streamId2,
		DefaultP2pTransportSettings(),
	)
	routeManager.UpdateTransport(transport1, []Route{route1})
	routeManager.UpdateTransport(transport2, []Route{route2})
	defer routeManager.RemoveTransport(transport1)
	defer routeManager.RemoveTransport(transport2)

	peerWriter := routeManager.OpenMultiRouteWriter(DestinationId(peerId))
	defer routeManager.CloseMultiRouteWriter(peerWriter)
	AssertEqual(t, 2, len(peerWriter.GetActiveRoutes()))

	peerStream1Writer := routeManager.OpenMultiRouteWriter(TransferPath{
		DestinationId: peerId,
		StreamId:      streamId1,
	})
	defer routeManager.CloseMultiRouteWriter(peerStream1Writer)
	AssertEqual(t, 2, len(peerStream1Writer.GetActiveRoutes()))

	stream1Writer := routeManager.OpenMultiRouteWriter(TransferPath{
		StreamId: streamId1,
	})
	defer routeManager.CloseMultiRouteWriter(stream1Writer)
	AssertEqual(t, 1, len(stream1Writer.GetActiveRoutes()))
}

func TestP2pSendTransportZeroPeerIdMatchesOnlyStream(t *testing.T) {
	// a send transport with no peer id must match only its stream.
	// the control mask and pure stream masks have a zero destination id
	// and must never match a stream transport by peer

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamId := NewId()
	otherStreamId := NewId()

	routeManager := NewRouteManager(ctx, "test")

	localConn, remoteConn := net.Pipe()
	defer localConn.Close()
	defer remoteConn.Close()

	transportCtx, transportCancel := context.WithCancel(ctx)
	defer transportCancel()

	transport, route := NewP2pSendTransport(
		transportCtx,
		transportCancel,
		localConn,
		streamId,
		DefaultP2pTransportSettings(),
	)
	routeManager.UpdateTransport(transport, []Route{route})
	defer routeManager.RemoveTransport(transport)

	streamWriter := routeManager.OpenMultiRouteWriter(TransferPath{
		StreamId: streamId,
	})
	defer routeManager.CloseMultiRouteWriter(streamWriter)
	AssertEqual(t, 1, len(streamWriter.GetActiveRoutes()))

	controlWriter := routeManager.OpenMultiRouteWriter(DestinationId(ControlId))
	defer routeManager.CloseMultiRouteWriter(controlWriter)
	AssertEqual(t, 0, len(controlWriter.GetActiveRoutes()))

	otherStreamWriter := routeManager.OpenMultiRouteWriter(TransferPath{
		StreamId: otherStreamId,
	})
	defer routeManager.CloseMultiRouteWriter(otherStreamWriter)
	AssertEqual(t, 0, len(otherStreamWriter.GetActiveRoutes()))
}

func TestP2pSendTransportDowngradeMatchesPeer(t *testing.T) {
	// `Downgrade` mirrors `MatchesSend`: an audit/degrade signal addressed to
	// the peer must shed the stream transport carrying direct-peer traffic,
	// not only a signal tagged with the stream id. A zero peer id must never
	// match a path without a destination id (control / pure stream masks).

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peerId := NewId()
	streamId := NewId()

	newTransport := func(withPeer bool) (Transport, context.Context, net.Conn) {
		localConn, remoteConn := net.Pipe()
		t.Cleanup(func() {
			localConn.Close()
			remoteConn.Close()
		})
		transportCtx, transportCancel := context.WithCancel(ctx)
		transportPeerId := Id{}
		if withPeer {
			transportPeerId = peerId
		}
		transport, _ := NewP2pSendTransportForPeer(
			transportCtx,
			transportCancel,
			localConn,
			transportPeerId,
			streamId,
			DefaultP2pTransportSettings(),
		)
		return transport, transportCtx, remoteConn
	}

	// an unrelated destination does not shed the transport
	transport, transportCtx, _ := newTransport(true)
	transport.Downgrade(DestinationId(NewId()))
	select {
	case <-transportCtx.Done():
		t.Fatal("downgrade for an unrelated destination must not cancel the transport")
	default:
	}

	// a signal carrying the peer destination (no stream id) sheds the transport
	transport.Downgrade(DestinationId(peerId))
	select {
	case <-transportCtx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("downgrade for the peer destination must cancel the transport")
	}

	// a signal tagged with the stream id still sheds the transport
	transport, transportCtx, _ = newTransport(true)
	transport.Downgrade(TransferPath{StreamId: streamId})
	select {
	case <-transportCtx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("downgrade for the stream must cancel the transport")
	}

	// a zero peer id never matches a path without a destination id
	transport, transportCtx, _ = newTransport(false)
	transport.Downgrade(TransferPath{StreamId: NewId()})
	transport.Downgrade(DestinationId(NewId()))
	select {
	case <-transportCtx.Done():
		t.Fatal("a zero-peer transport must only downgrade on its own stream")
	default:
	}
}

func TestP2pSendTransportReconnectPeerDestination(t *testing.T) {
	// the p2p transport is added to and removed from the route manager
	// as the conn connects, disconnects, and reconnects.
	// a writer to the peer must track the transport across the flaps

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peerId := NewId()
	streamId := NewId()

	routeManager := NewRouteManager(ctx, "test")

	peerWriter := routeManager.OpenMultiRouteWriter(DestinationId(peerId))
	defer routeManager.CloseMultiRouteWriter(peerWriter)

	localConn, remoteConn := net.Pipe()
	defer localConn.Close()
	defer remoteConn.Close()

	transportCtx, transportCancel := context.WithCancel(ctx)
	defer transportCancel()

	transport, route := NewP2pSendTransportForPeer(
		transportCtx,
		transportCancel,
		localConn,
		peerId,
		streamId,
		DefaultP2pTransportSettings(),
	)

	routeManager.UpdateTransport(transport, []Route{route})
	AssertEqual(t, 1, len(peerWriter.GetActiveRoutes()))

	routeManager.RemoveTransport(transport)
	AssertEqual(t, 0, len(peerWriter.GetActiveRoutes()))

	routeManager.UpdateTransport(transport, []Route{route})
	defer routeManager.RemoveTransport(transport)
	AssertEqual(t, 1, len(peerWriter.GetActiveRoutes()))

	// a write after reconnect flows over the stream transport conn
	message := []byte("after reconnect")
	err := peerWriter.Write(ctx, MessagePoolCopy(message), 1*time.Second)
	AssertEqual(t, err, nil)

	remoteConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	b := make([]byte, 64)
	n, err := remoteConn.Read(b)
	AssertEqual(t, err, nil)
	AssertEqual(t, message, b[:n])
}

func TestP2pSendTransportPreferredOverGateway(t *testing.T) {
	// with both a gateway transport and a stream transport matching the peer,
	// the stream transport has priority 0 and route weight 1.0
	// which leaves the gateway route weight 0,
	// so all traffic to the peer flows over the stream.
	// the gateway route has no reader,
	// so a message routed to it would never reach the conn
	// and the read below would time out

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peerId := NewId()
	streamId := NewId()

	routeManager := NewRouteManager(ctx, "test")

	gatewayRoute := make(chan []byte, 32)
	gatewayTransport := NewSendGatewayTransport()
	routeManager.UpdateTransport(gatewayTransport, []Route{gatewayRoute})
	defer routeManager.RemoveTransport(gatewayTransport)

	localConn, remoteConn := net.Pipe()
	defer localConn.Close()
	defer remoteConn.Close()

	transportCtx, transportCancel := context.WithCancel(ctx)
	defer transportCancel()

	transport, route := NewP2pSendTransportForPeer(
		transportCtx,
		transportCancel,
		localConn,
		peerId,
		streamId,
		DefaultP2pTransportSettings(),
	)
	routeManager.UpdateTransport(transport, []Route{route})
	defer routeManager.RemoveTransport(transport)

	peerWriter := routeManager.OpenMultiRouteWriter(DestinationId(peerId))
	defer routeManager.CloseMultiRouteWriter(peerWriter)
	AssertEqual(t, 2, len(peerWriter.GetActiveRoutes()))

	b := make([]byte, 64)
	burstSize := 16
	for i := 0; i < burstSize; i += 1 {
		message := []byte{byte(i)}
		err := peerWriter.Write(ctx, MessagePoolCopy(message), 1*time.Second)
		AssertEqual(t, err, nil)

		remoteConn.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, err := remoteConn.Read(b)
		AssertEqual(t, err, nil)
		AssertEqual(t, message, b[:n])
	}

	// nothing was routed to the gateway
	AssertEqual(t, 0, len(gatewayRoute))
}
