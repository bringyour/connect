package connect

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
	"google.golang.org/protobuf/proto"
)

func testLogicalLaneSchedulingKey(sourcePort int) sendSchedulingKey {
	return ipSendSchedulingKey(&IpPath{
		Version:         4,
		Protocol:        IpProtocolTcp,
		SourceIp:        net.ParseIP("10.0.0.2"),
		SourcePort:      sourcePort,
		DestinationIp:   net.ParseIP("93.184.216.34"),
		DestinationPort: 443,
	})
}

func TestLogicalLaneHashStableAndBounded(t *testing.T) {
	key := testLogicalLaneSchedulingKey(54321)
	if !key.valid {
		t.Fatal("test five-tuple did not produce a scheduling key")
	}
	// Pin one vector so a refactor cannot silently move a live flow between
	// sequence lanes across process versions.
	for count, want := range map[int]uint32{1: 1, 4: 3, 8: 3} {
		for range 10 {
			if got := key.logicalLaneForCount(count); got != want {
				t.Fatalf("logicalLaneForCount(%d) = %d, want %d", count, got, want)
			}
		}
	}
	if got := key.logicalLaneForCount(100); got < 1 || maxLogicalDataLaneCount < got {
		t.Fatalf("oversized lane count produced out-of-range lane %d", got)
	}
	if got := (sendSchedulingKey{}).logicalLaneForCount(8); got != 0 {
		t.Fatalf("invalid scheduling key selected lane %d", got)
	}

	seen := map[uint32]bool{}
	for sourcePort := 54000; sourcePort < 54128; sourcePort++ {
		lane := testLogicalLaneSchedulingKey(sourcePort).logicalLaneForCount(8)
		if lane < 1 || 8 < lane {
			t.Fatalf("source port %d produced lane %d", sourcePort, lane)
		}
		seen[lane] = true
	}
	if len(seen) != 8 {
		t.Fatalf("128 adjacent flows reached %d/8 lanes: %v", len(seen), seen)
	}
}

func TestLogicalLaneCapabilityRequiresLiveLaneZero(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultClientSettings()
	settings.Log = NewNoopLogger()
	settings.SendBufferSettings.LogicalDataLaneCount = 8
	applyTestEncryptionSettings(settings, encryptionModeOff)
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()

	destination := NewId()
	pack := &SendPack{
		TransferOptions: settings.DefaultTransferOpts,
		Destination:     destination,
		schedulingKey:   testLogicalLaneSchedulingKey(54321),
	}
	if got := client.sendBuffer.selectLogicalLane(pack); got != 0 {
		t.Fatalf("unnegotiated peer selected lane %d", got)
	}

	laneZero := NewSendSequence(
		ctx,
		client,
		client.sendBuffer,
		destination,
		MultiHopId{},
		false,
		false,
		false,
		sequenceTlsRoleClient,
		false,
		settings.SendBufferSettings,
	)
	base := laneZero.id()
	client.sendBuffer.observeLogicalLaneVersion(laneZero, transferLogicalLaneVersion)
	if got := client.sendBuffer.selectLogicalLane(pack); got != 0 {
		t.Fatalf("unindexed sequence supplied capability for lane %d", got)
	}

	client.sendBuffer.mutex.Lock()
	client.sendBuffer.sendSequences[base] = laneZero
	client.sendBuffer.mutex.Unlock()
	client.sendBuffer.observeLogicalLaneVersion(laneZero, transferLogicalLaneVersion)
	wantLane := pack.schedulingKey.logicalLaneForCount(8)
	if got := client.sendBuffer.selectLogicalLane(pack); got != wantLane {
		t.Fatalf("negotiated peer selected lane %d, want %d", got, wantLane)
	}

	dataLane := newSendSequenceWithLogicalLane(
		ctx,
		client,
		client.sendBuffer,
		destination,
		MultiHopId{},
		false,
		false,
		false,
		sequenceTlsRoleClient,
		false,
		wantLane,
		settings.SendBufferSettings,
		NewTransferMemoryBudget(settings.SendBufferSettings.ResendQueueMaxByteCount),
	)
	client.sendBuffer.mutex.Lock()
	client.sendBuffer.sendSequences[dataLane.id()] = dataLane
	client.sendBuffer.mutex.Unlock()

	// A later matching legacy ACK is explicit downgrade evidence. It removes
	// capability and retires dependent lanes promptly.
	client.sendBuffer.observeLogicalLaneVersion(laneZero, 0)
	if got := client.sendBuffer.selectLogicalLane(pack); got != 0 {
		t.Fatalf("withdrawn capability retained lane %d", got)
	}
	select {
	case <-dataLane.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("capability withdrawal did not cancel the data lane")
	}

	client.sendBuffer.mutex.Lock()
	delete(client.sendBuffer.sendSequences, base)
	delete(client.sendBuffer.sendSequences, dataLane.id())
	client.sendBuffer.mutex.Unlock()
	laneZero.Cancel()
}

func TestLogicalLanePinsCapabilitySequenceUntilLaneExit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runGate := make(chan struct{})
	settings := DefaultClientSettings()
	settings.Log = NewNoopLogger()
	settings.SendBufferSettings.LogicalDataLaneCount = 8
	settings.SendBufferSettings.beforeRunSendSequenceForTest = func(id sendSequenceId) {
		if id.LogicalLane != 0 {
			<-runGate
		}
	}
	applyTestEncryptionSettings(settings, encryptionModeOff)
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()

	destination := NewId()
	laneZero := NewSendSequence(
		ctx,
		client,
		client.sendBuffer,
		destination,
		MultiHopId{},
		false,
		false,
		false,
		sequenceTlsRoleClient,
		false,
		settings.SendBufferSettings,
	)
	base := laneZero.id()
	client.sendBuffer.mutex.Lock()
	client.sendBuffer.sendSequences[base] = laneZero
	client.sendBuffer.logicalLaneVersions[base] = transferLogicalLaneVersion
	client.sendBuffer.mutex.Unlock()

	key := testLogicalLaneSchedulingKey(54321)
	lane := key.logicalLaneForCount(8)
	id := base
	id.LogicalLane = lane
	dataLane := client.sendBuffer.createSendSequence(id, &SendPack{
		TransferOptions: settings.DefaultTransferOpts,
		Destination:     destination,
		schedulingKey:   key,
	})
	if dataLane == nil {
		close(runGate)
		t.Fatal("negotiated data lane was not created")
	}
	if dataLane.logicalLaneBaseSequence != laneZero || !dataLane.logicalLaneBasePinned {
		close(runGate)
		t.Fatal("data lane did not pin its advertising lane-zero generation")
	}
	checkpoint := laneZero.idleCondition.Checkpoint()
	if laneZero.idleCondition.Close(checkpoint) {
		close(runGate)
		t.Fatal("lane-zero generation idled while a data lane depended on it")
	}

	dataLane.Cancel()
	close(runGate)
	select {
	case <-dataLane.done:
	case <-time.After(5 * time.Second):
		t.Fatal("data lane did not terminate")
	}
	if dataLane.logicalLaneBasePinned {
		t.Fatal("data lane retained its lane-zero idle pin after exit")
	}
	checkpoint = laneZero.idleCondition.Checkpoint()
	if !laneZero.idleCondition.Close(checkpoint) {
		t.Fatal("lane-zero idle pin was not released after data-lane exit")
	}

	client.sendBuffer.mutex.Lock()
	delete(client.sendBuffer.sendSequences, base)
	delete(client.sendBuffer.logicalLaneVersions, base)
	client.sendBuffer.mutex.Unlock()
	laneZero.Cancel()
}

func TestLogicalLaneAckRoutesOnlyExactSequence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	destination := NewId()
	unrelatedId := NewId()
	targetId := NewId()
	unrelatedAck := receiveAckMessage{sequenceId: unrelatedId}
	unrelated := &SendSequence{
		ctx:         ctx,
		destination: destination,
		sequenceId:  unrelatedId,
		acks:        make(chan receiveAckMessage, 1),
	}
	target := &SendSequence{
		ctx:         ctx,
		destination: destination,
		sequenceId:  targetId,
		acks:        make(chan receiveAckMessage, 1),
	}
	unrelated.acks <- unrelatedAck // a broadcast implementation stalls here
	buffer := &SendBuffer{
		ctx: ctx,
		log: NewNoopLogger(),
		sendSequencesBySequenceId: map[Id]*SendSequence{
			unrelatedId: unrelated,
			targetId:    target,
		},
	}
	targetMessageId := NewId()
	targetAck := &protocol.Ack{
		MessageId:  targetMessageId.Bytes(),
		SequenceId: targetId.Bytes(),
	}
	if !buffer.Ack(destination, targetAck, 0) {
		t.Fatal("exact target ACK was rejected by an unrelated full ACK queue")
	}
	select {
	case got := <-target.acks:
		if got.messageId != targetMessageId || got.sequenceId != targetId {
			t.Fatal("target sequence received a different ACK")
		}
	default:
		t.Fatal("target sequence did not receive its ACK")
	}
	select {
	case got := <-unrelated.acks:
		if got != unrelatedAck {
			t.Fatal("unrelated sequence ACK queue was modified")
		}
	default:
		t.Fatal("unrelated sequence ACK was consumed")
	}
	if buffer.Ack(NewId(), targetAck, 0) {
		t.Fatal("ACK with mismatched destination was accepted")
	}
}

func TestLogicalLaneReplyTransferKeyIsExplicitAndBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultClientSettings()
	settings.Log = NewNoopLogger()
	applyTestEncryptionSettings(settings, encryptionModeOff)
	if settings.SendBufferSettings.LogicalDataLaneCount != 0 {
		t.Fatalf(
			"default logical data lane count=%d, want disabled",
			settings.SendBufferSettings.LogicalDataLaneCount,
		)
	}
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()

	for _, testCase := range []struct {
		name string
		key  TransferKey
		want uint32
	}{
		{name: "legacy", key: TransferKey{}, want: 0},
		{name: "negotiated lane", key: TransferKey{LogicalLane: 4}, want: 4},
		{name: "out of range", key: TransferKey{LogicalLane: maxLogicalDataLaneCount + 1}, want: 0},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			resolved := client.resolveSendOptions([]any{testCase.key})
			if !resolved.logicalLaneExplicit {
				t.Fatal("received TransferKey did not select an explicit reply lane")
			}
			if resolved.logicalLane != testCase.want {
				t.Fatalf("reply lane=%d, want %d", resolved.logicalLane, testCase.want)
			}
		})
	}
}

func TestLogicalLaneBuffersShareFixedLazyBudgets(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultClientSettings()
	settings.Log = NewNoopLogger()
	applyTestEncryptionSettings(settings, encryptionModeOff)
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()
	if client.sendBuffer.logicalLaneResendBudget != nil {
		t.Fatal("disabled logical lanes eagerly allocated a resend budget")
	}
	if client.receiveBuffer.logicalLaneReceiveBudget != nil {
		t.Fatal("unused logical lanes eagerly allocated a receive budget")
	}

	resendBudget := NewTransferMemoryBudget(
		settings.SendBufferSettings.ResendQueueMaxByteCount,
	)
	var sendSequences []*SendSequence
	var sendChannelCapacity int
	var ackChannelCapacity int
	for lane := uint32(1); lane <= maxLogicalDataLaneCount; lane++ {
		sequence := newSendSequenceWithLogicalLane(
			ctx,
			client,
			client.sendBuffer,
			NewId(),
			MultiHopId{},
			false,
			false,
			false,
			sequenceTlsRoleClient,
			false,
			lane,
			settings.SendBufferSettings,
			resendBudget,
		)
		sendSequences = append(sendSequences, sequence)
		sendChannelCapacity += cap(sequence.packs)
		ackChannelCapacity += cap(sequence.acks)
		if sequence.resendQueue.budget != resendBudget || sequence.resendQueue.minByteCount != 0 {
			t.Fatalf("send lane %d did not share the zero-floor resend pool", lane)
		}
		sequence.resendQueue.Add(&sendItem{
			transferItem: transferItem{
				messageId:      NewId(),
				sequenceNumber: uint64(lane),
			},
			transferFrameBytes: make([]byte, 1024),
		})
	}
	maxSendCapacity := logicalLaneSequenceBufferSize(
		settings.SendBufferSettings.SequenceBufferSize,
		1,
	) * maxLogicalDataLaneCount
	maxAckCapacity := logicalLaneSequenceBufferSize(
		settings.SendBufferSettings.AckBufferSize,
		1,
	) * maxLogicalDataLaneCount
	if sendChannelCapacity != maxSendCapacity || ackChannelCapacity != maxAckCapacity {
		t.Fatalf(
			"data-lane channels = (%d,%d), want bounded (%d,%d)",
			sendChannelCapacity,
			ackChannelCapacity,
			maxSendCapacity,
			maxAckCapacity,
		)
	}
	if got := resendBudget.UsedByteCount(); got != 8*1024 {
		t.Fatalf("shared resend budget used %d, want %d", got, 8*1024)
	}
	for _, sequence := range sendSequences {
		sequence.resendQueue.Clear()
		sequence.Cancel()
	}
	if got := resendBudget.UsedByteCount(); got != 0 {
		t.Fatalf("shared resend budget retained %d bytes", got)
	}
	reserved, released := resendBudget.Counts()
	if reserved != released {
		t.Fatalf("resend budget reserve/release = %d/%d", reserved, released)
	}

	receiveBudget := NewTransferMemoryBudget(
		settings.ReceiveBufferSettings.ReceiveQueueMaxByteCount,
	)
	var receiveSequences []*ReceiveSequence
	var receiveChannelCapacity int
	for lane := uint32(1); lane <= maxLogicalDataLaneCount; lane++ {
		sequence := newReceiveSequenceWithLogicalLaneBudget(
			ctx,
			client,
			SourceId(NewId()),
			NewId(),
			TransferKey{LogicalLane: lane},
			settings.ReceiveBufferSettings,
			receiveBudget,
		)
		receiveSequences = append(receiveSequences, sequence)
		receiveChannelCapacity += cap(sequence.packs)
		if sequence.receiveQueue.budget != receiveBudget || sequence.receiveQueue.minByteCount != 0 {
			t.Fatalf("receive lane %d did not share the zero-floor reorder pool", lane)
		}
		sequence.receiveQueue.Add(&receiveItem{transferItem: transferItem{
			messageId:        NewId(),
			messageByteCount: 2048,
			sequenceNumber:   uint64(lane),
		}})
	}
	maxReceiveCapacity := logicalLaneSequenceBufferSize(
		settings.ReceiveBufferSettings.SequenceBufferSize,
		1,
	) * maxLogicalDataLaneCount
	if receiveChannelCapacity != maxReceiveCapacity {
		t.Fatalf(
			"data-lane receive channels = %d, want bounded %d",
			receiveChannelCapacity,
			maxReceiveCapacity,
		)
	}
	if got := receiveBudget.UsedByteCount(); got != 8*2048 {
		t.Fatalf("shared receive budget used %d, want %d", got, 8*2048)
	}
	for _, sequence := range receiveSequences {
		sequence.receiveQueue.Clear()
		sequence.Cancel()
	}
	if got := receiveBudget.UsedByteCount(); got != 0 {
		t.Fatalf("shared receive budget retained %d bytes", got)
	}
	reserved, released = receiveBudget.Counts()
	if reserved != released {
		t.Fatalf("receive budget reserve/release = %d/%d", reserved, released)
	}
}

func TestLogicalLaneAdaptiveH1DepthDividesCountAndKeepsBytesFixed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultReceiveBufferSettingsWithBufferSize(16)
	settings.H1SequenceBufferSize = 64
	settings.H1SequenceBufferAdaptiveMaxSize = 512
	settings.H1SequenceBufferAdaptiveStepSize = 64
	settings.H1SequenceBufferAdaptiveSaturationThreshold = 2
	settings.H1SequenceBufferAdaptiveSaturationWindow = 100 * time.Millisecond
	settings.SequenceBufferByteCount = 128 * 1024
	settings.H1SequenceBufferByteCount = 128 * 1024
	// Count-only iterative depth must never manufacture a second byte window.
	settings.H1SequenceBufferAdaptiveMaxByteCount = 0
	settings.H1SequenceBufferAdaptiveStepByteCount = 0

	sequence := newReceiveSequenceWithLogicalLaneBudget(
		ctx,
		&Client{},
		SourceId(NewId()),
		NewId(),
		TransferKey{LogicalLane: 1},
		settings,
		NewTransferMemoryBudget(2*1024*1024),
	)
	defer sequence.Close()
	if got := cap(sequence.packs); got != 64 {
		t.Fatalf("adaptive lane channel capacity = %d, want 64", got)
	}
	if sequence.packQueueH1Limit != 8 ||
		sequence.packQueueH1AdaptiveMaxLimit != 64 ||
		sequence.packQueueH1AdaptiveStep != 8 {
		t.Fatalf(
			"adaptive lane depths = %d/%d step %d, want 8/64 step 8",
			sequence.packQueueH1Limit,
			sequence.packQueueH1AdaptiveMaxLimit,
			sequence.packQueueH1AdaptiveStep,
		)
	}
	if sequence.packQueueH1ByteLimit != 128*1024 ||
		sequence.packQueueH1AdaptiveMaxByteLimit != 128*1024 ||
		sequence.packQueueH1AdaptiveByteStep != 0 {
		t.Fatalf(
			"adaptive lane bytes = %d/%d step %d, want fixed 128 KiB",
			sequence.packQueueH1ByteLimit,
			sequence.packQueueH1AdaptiveMaxByteLimit,
			sequence.packQueueH1AdaptiveByteStep,
		)
	}
}

func TestReceiveLogicalLanesCoexistAndRejectOutOfRange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runGate := make(chan struct{})
	settings := DefaultClientSettings()
	settings.Log = NewNoopLogger()
	settings.ReceiveBufferSettings.beforeRunReceiveSequenceForTest = func(id receiveSequenceId) {
		if id.LogicalLane != 0 {
			<-runGate
		}
	}
	applyTestEncryptionSettings(settings, encryptionModeOff)
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	source := SourceId(NewId())
	client.ContractManager().AddNoContractPeer(source.SourceId)

	for lane := uint32(1); lane <= 2; lane++ {
		sequenceId := NewId()
		receivePack := &ReceivePack{
			Source:     source,
			SequenceId: sequenceId,
			Pack: &protocol.Pack{
				MessageId:      NewId().Bytes(),
				SequenceId:     sequenceId.Bytes(),
				SequenceNumber: 0,
				LogicalLane:    lane,
			},
			MessageByteCount: 1,
			Ctx:              ctx,
		}
		success, err := client.receiveBuffer.Pack(receivePack, 0)
		if err != nil || !success {
			close(runGate)
			client.Cancel()
			t.Fatalf("receive lane %d admission = %t, %v", lane, success, err)
		}
	}

	client.receiveBuffer.mutex.Lock()
	if got := len(client.receiveBuffer.receiveSequences); got != 2 {
		client.receiveBuffer.mutex.Unlock()
		close(runGate)
		client.Cancel()
		t.Fatalf("parallel logical lanes created %d receive sequences, want 2", got)
	}
	if got := len(client.receiveBuffer.headReceiveSequenceIds); got != 2 {
		client.receiveBuffer.mutex.Unlock()
		close(runGate)
		client.Cancel()
		t.Fatalf("parallel logical lanes created %d heads, want 2", got)
	}
	var laneBudget *TransferMemoryBudget
	for id, sequence := range client.receiveBuffer.receiveSequences {
		if id.LogicalLane < 1 || 2 < id.LogicalLane {
			client.receiveBuffer.mutex.Unlock()
			close(runGate)
			client.Cancel()
			t.Fatalf("unexpected receive lane %d", id.LogicalLane)
		}
		if laneBudget == nil {
			laneBudget = sequence.receiveQueue.budget
		} else if sequence.receiveQueue.budget != laneBudget {
			client.receiveBuffer.mutex.Unlock()
			close(runGate)
			client.Cancel()
			t.Fatal("receive lanes did not use one lazy byte pool")
		}
	}
	client.receiveBuffer.mutex.Unlock()
	if laneBudget == nil || laneBudget != client.receiveBuffer.logicalLaneReceiveBudget {
		close(runGate)
		client.Cancel()
		t.Fatal("receive buffer did not publish its lazy lane pool")
	}

	invalidSequenceId := NewId()
	invalid := &ReceivePack{
		Source:     source,
		SequenceId: invalidSequenceId,
		Pack: &protocol.Pack{
			MessageId:   NewId().Bytes(),
			SequenceId:  invalidSequenceId.Bytes(),
			LogicalLane: maxLogicalDataLaneCount + 1,
		},
		Ctx: ctx,
	}
	success, err := client.receiveBuffer.Pack(invalid, 0)
	if err == nil || success {
		close(runGate)
		client.Cancel()
		t.Fatalf("out-of-range logical lane admission = %t, %v", success, err)
	}
	client.receiveBuffer.mutex.Lock()
	sequenceCount := len(client.receiveBuffer.receiveSequences)
	client.receiveBuffer.mutex.Unlock()
	if sequenceCount != 2 {
		close(runGate)
		client.Cancel()
		t.Fatalf("invalid lane changed receive sequence count to %d", sequenceCount)
	}

	client.Cancel()
	close(runGate)
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()
	if err := client.CloseAndWait(closeCtx); err != nil {
		t.Fatalf("close receive lane test client: %v", err)
	}
}

func TestNegotiatedLogicalLanePreventsCrossFlowHeadOfLineBlocking(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping client-pair loss integration in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	senderId := NewId()
	receiverId := NewId()
	senderOut := make(chan []byte, 32)
	receiverIn := make(chan []byte, 32)
	receiverOut := make(chan []byte, 32)
	senderIn := make(chan []byte, 32)
	laneOneDropped := make(chan struct{}, 1)
	releaseLaneOne := make(chan struct{})
	linkErrors := make(chan error, 1)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case wire := <-senderOut:
				var frame protocol.TransferFrame
				if err := proto.Unmarshal(wire, &frame); err != nil {
					select {
					case linkErrors <- err:
					default:
					}
					continue
				}
				if frame.Pack.GetLogicalLane() == 1 {
					select {
					case <-releaseLaneOne:
						// The test has observed the independent healthy lane;
						// allow a later Transfer retransmit to recover lane one.
					default:
						select {
						case laneOneDropped <- struct{}{}:
						default:
						}
						continue
					}
				}
				select {
				case <-ctx.Done():
					return
				case receiverIn <- wire:
				}
			}
		}
	}()

	newSettings := func() *ClientSettings {
		settings := DefaultClientSettingsWithBufferSize(64)
		settings.Log = NewNoopLogger()
		settings.SendBufferSettings.LogicalDataLaneCount = 8
		settings.SendBufferSettings.MinResendInterval = 250 * time.Millisecond
		settings.SendBufferSettings.RttMinResendInterval = 250 * time.Millisecond
		settings.SendBufferSettings.MaxResendInterval = 500 * time.Millisecond
		settings.SendBufferSettings.UnreliableMaxResendInterval = 500 * time.Millisecond
		settings.SendBufferSettings.AckTimeout = 10 * time.Second
		settings.ReceiveBufferSettings.GapTimeout = 10 * time.Second
		applyTestEncryptionSettings(settings, encryptionModeOff)
		return settings
	}
	sender := NewClient(ctx, senderId, NewNoContractClientOob(), newSettings())
	defer sender.Cancel()
	receiver := NewClient(ctx, receiverId, NewNoContractClientOob(), newSettings())
	defer receiver.Cancel()

	sender.RouteManager().UpdateTransport(
		NewSendGatewayTransportWithType(TransportTypeH3),
		[]Route{senderOut},
	)
	receiver.RouteManager().UpdateTransport(
		NewReceiveGatewayTransportWithType(TransportTypeH3),
		[]Route{receiverIn},
	)
	receiver.RouteManager().UpdateTransport(
		NewSendGatewayTransportWithType(TransportTypeH3),
		[]Route{receiverOut},
	)
	sender.RouteManager().UpdateTransport(
		NewReceiveGatewayTransportWithType(TransportTypeH3),
		[]Route{senderIn},
	)
	// ACKs do not need a conditioner; this forwarder preserves channel
	// ownership and keeps the two physical directions explicit.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case wire := <-receiverOut:
				select {
				case <-ctx.Done():
					return
				case senderIn <- wire:
				}
			}
		}
	}()
	sender.ContractManager().AddNoContractPeer(receiverId)
	receiver.ContractManager().AddNoContractPeer(senderId)

	type receivedMessage struct {
		content string
		lane    uint32
	}
	received := make(chan receivedMessage, 8)
	receiveErrors := make(chan error, 1)
	receiver.AddReceiveCallback(func(_ TransferPath, frames []*protocol.Frame, peer Peer) {
		for _, frame := range frames {
			message, err := FromFrame(frame)
			if err != nil {
				select {
				case receiveErrors <- err:
				default:
				}
				return
			}
			simple, ok := message.(*protocol.SimpleMessage)
			if !ok {
				continue
			}
			select {
			case received <- receivedMessage{
				content: simple.Content,
				lane:    peer.TransferKey.LogicalLane,
			}:
			default:
				select {
				case receiveErrors <- ErrSendPackNotAdmitted:
				default:
				}
			}
		}
	})

	type ackResult struct {
		content string
		err     error
	}
	acks := make(chan ackResult, 8)
	send := func(content string, key sendSchedulingKey) {
		frame := RequireToFrameWithDefaultProtocolVersion(
			&protocol.SimpleMessage{Content: content},
		)
		if !sender.SendWithTimeout(
			frame,
			receiverId,
			func(err error) { acks <- ackResult{content: content, err: err} },
			2*time.Second,
			sendSchedulingKeyOption{key: key},
		) {
			MessagePoolReturn(frame.MessageBytes)
			t.Fatalf("send %q was not admitted", content)
		}
	}

	bootstrapKey := testLogicalLaneSchedulingKey(54321)
	send("bootstrap", bootstrapKey)
	waitForReceive := func(content string, lane uint32, timeout time.Duration) {
		deadline := time.NewTimer(timeout)
		defer deadline.Stop()
		for {
			select {
			case got := <-received:
				if got.content != content || got.lane != lane {
					t.Fatalf(
						"receive = (%q,lane %d), want (%q,lane %d)",
						got.content,
						got.lane,
						content,
						lane,
					)
				}
				return
			case err := <-receiveErrors:
				t.Fatalf("receive callback: %v", err)
			case err := <-linkErrors:
				t.Fatalf("conditioned link decode: %v", err)
			case <-deadline.C:
				t.Fatalf("timed out waiting for %q on lane %d", content, lane)
			}
		}
	}
	waitForAck := func(content string) {
		select {
		case ack := <-acks:
			if ack.content != content || ack.err != nil {
				t.Fatalf("ack = (%q,%v), want (%q,nil)", ack.content, ack.err, content)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %q ACK", content)
		}
	}
	waitForReceive("bootstrap", 0, 5*time.Second)
	waitForAck("bootstrap")

	findKeyForLane := func(want uint32) sendSchedulingKey {
		for port := 54000; port < 65000; port++ {
			key := testLogicalLaneSchedulingKey(port)
			if key.logicalLaneForCount(8) == want {
				return key
			}
		}
		t.Fatalf("could not find a flow key for lane %d", want)
		return sendSchedulingKey{}
	}
	send("held", findKeyForLane(1))
	select {
	case <-laneOneDropped:
	case err := <-linkErrors:
		t.Fatalf("conditioned link decode: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("negotiated flow did not use logical lane one")
	}

	// Every lane-one transmission remains blackholed here. Lane two therefore
	// cannot arrive if it shares receive ordering with the missing lane-one
	// sequence number.
	send("healthy", findKeyForLane(2))
	waitForReceive("healthy", 2, 3*time.Second)
	waitForAck("healthy")
	close(releaseLaneOne)
	waitForReceive("held", 1, 5*time.Second)
	waitForAck("held")
}
