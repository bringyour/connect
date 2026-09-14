// Provider regressions preserve receiver-visible routing and encryption keys
// across the user-space nat without exposing transport-local stream ids.
package connect

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// Constructs a provider around a deliberately paused NAT input queue. Tests
// can inspect the exact ingress metadata before any protocol worker consumes it.
func newProviderTransferKeyTestFixture(
	t *testing.T,
) (*RemoteUserNatProvider, *Client, *LocalUserNat) {
	return newProviderTransferKeyTestFixtureWithSettings(
		t,
		DefaultRemoteUserNatProviderSettings(),
	)
}

// Constructs the paused provider fixture with explicit sender bounds or
// protocol settings.
func newProviderTransferKeyTestFixtureWithSettings(
	t *testing.T,
	settings *RemoteUserNatProviderSettings,
) (*RemoteUserNatProvider, *Client, *LocalUserNat) {
	return newProviderTransferKeyTestFixtureWithClientSettings(
		t,
		settings,
		DefaultClientSettings(),
	)
}

// Constructs the provider fixture with transfer timing fixed before the
// client's workers start.
func newProviderTransferKeyTestFixtureWithClientSettings(
	t *testing.T,
	providerSettings *RemoteUserNatProviderSettings,
	clientSettings *ClientSettings,
) (*RemoteUserNatProvider, *Client, *LocalUserNat) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), clientSettings)
	localUserNat := &LocalUserNat{
		ctx:         ctx,
		sendPackets: make(chan *SendPacket, 1),
	}
	provider := &RemoteUserNatProvider{
		ctx:                      ctx,
		client:                   client,
		cancel:                   cancel,
		localUserNat:             localUserNat,
		securityPolicy:           providerSettings.SecurityPolicyGenerator(ctx, DefaultSecurityPolicyStatsCollector()),
		settings:                 providerSettings,
		packetStatsCounters:      &packetStatsCounters{},
		packetStatsCallbacks:     NewCallbackList[PacketStatsFunction](),
		sourceProvideMode:        map[Id]protocol.ProvideMode{},
		sourceP2pPriorityRefresh: map[Id]time.Time{},
	}
	provider.startReturnSenders()
	t.Cleanup(func() {
		provider.Close()
		client.Cancel()
	})
	return provider, client, localUserNat
}

// providerReturnRetryRecord is the stable wire identity and lane of one
// provider-return Pack.
type providerReturnRetryRecord struct {
	path                    TransferPath
	messageId               Id
	sequenceId              Id
	sequenceNumber          uint64
	head                    bool
	nack                    bool
	forceStream             bool
	companionContract       bool
	sessionRole             protocol.SequenceRole
	sessionCompanion        bool
	sessionCompanionPresent bool
	wireBytes               []byte
}

// providerReturnRetryGate drops one exact return record, holds its retry until
// released, and leaves unrelated traffic untouched.
type providerReturnRetryGate struct {
	ctx            context.Context
	targetPacket   []byte
	firstDropped   chan providerReturnRetryRecord
	retryHeld      chan providerReturnRetryRecord
	releaseRetry   chan struct{}
	inspectionErrs chan error
}

// Construction clones the target so later caller mutation cannot change the
// exact packet identity selected by the gate.
func newProviderReturnRetryGate(
	ctx context.Context,
	targetPacket []byte,
) *providerReturnRetryGate {
	return &providerReturnRetryGate{
		ctx:            ctx,
		targetPacket:   bytes.Clone(targetPacket),
		firstDropped:   make(chan providerReturnRetryRecord, 1),
		retryHeld:      make(chan providerReturnRetryRecord, 1),
		releaseRetry:   make(chan struct{}),
		inspectionErrs: make(chan error, 1),
	}
}

// run owns messages taken from input until they are dropped or handed to
// output.
func (self *providerReturnRetryGate) run(input Route, output Route) {
	var first providerReturnRetryRecord
	firstSeen := false
	retrySeen := false
	for {
		select {
		case <-self.ctx.Done():
			return
		case transferFrameBytes, ok := <-input:
			if !ok {
				return
			}
			record, target, err := providerReturnRetryRecordFromWire(
				transferFrameBytes,
				self.targetPacket,
			)
			if err != nil {
				select {
				case self.inspectionErrs <- err:
				default:
				}
			}
			if target && !firstSeen {
				first = record
				firstSeen = true
				MessagePoolReturn(transferFrameBytes)
				self.firstDropped <- record
				continue
			}
			if target && !retrySeen {
				retrySeen = true
				if !providerReturnRetryIdentityEqual(first, record) {
					select {
					case self.inspectionErrs <- fmt.Errorf("provider return retry identity changed"):
					default:
					}
				}
				self.retryHeld <- record
				select {
				case <-self.ctx.Done():
					MessagePoolReturn(transferFrameBytes)
					return
				case <-self.releaseRetry:
				}
			}
			select {
			case <-self.ctx.Done():
				MessagePoolReturn(transferFrameBytes)
				return
			case output <- transferFrameBytes:
			}
		}
	}
}

// Decoding selects only a Pack containing the exact provider-return packet and
// retains every lane and session field needed to compare its retry identity.
func providerReturnRetryRecordFromWire(
	transferFrameBytes []byte,
	targetPacket []byte,
) (providerReturnRetryRecord, bool, error) {
	var transferFrame protocol.TransferFrame
	if err := ProtoUnmarshal(transferFrameBytes, &transferFrame); err != nil {
		return providerReturnRetryRecord{}, false, fmt.Errorf("decode transfer frame: %w", err)
	}
	pack := transferFrame.Pack
	if pack == nil {
		frame := transferFrame.GetFrame()
		if frame == nil || frame.GetMessageType() != protocol.MessageType_TransferPack {
			return providerReturnRetryRecord{}, false, nil
		}
		pack = &protocol.Pack{}
		if err := ProtoUnmarshal(frame.GetMessageBytes(), pack); err != nil {
			return providerReturnRetryRecord{}, false, fmt.Errorf("decode transfer pack: %w", err)
		}
	}
	target := false
	for _, frame := range pack.Frames {
		if frame.GetMessageType() != protocol.MessageType_IpIpPacketFromProvider {
			continue
		}
		packet, err := ipPacketFromProviderBytes(frame)
		if err != nil {
			return providerReturnRetryRecord{}, false, fmt.Errorf("decode provider return packet: %w", err)
		}
		if bytes.Equal(packet, targetPacket) {
			target = true
			break
		}
	}
	if !target {
		return providerReturnRetryRecord{}, false, nil
	}
	path, err := TransferPathFromProtobuf(transferFrame.TransferPath)
	if err != nil {
		return providerReturnRetryRecord{}, false, fmt.Errorf("decode provider return path: %w", err)
	}
	messageId, err := IdFromBytes(pack.MessageId)
	if err != nil {
		return providerReturnRetryRecord{}, false, fmt.Errorf("decode provider return message id: %w", err)
	}
	sequenceId, err := IdFromBytes(pack.SequenceId)
	if err != nil {
		return providerReturnRetryRecord{}, false, fmt.Errorf("decode provider return sequence id: %w", err)
	}
	return providerReturnRetryRecord{
		path:                    path,
		messageId:               messageId,
		sequenceId:              sequenceId,
		sequenceNumber:          pack.SequenceNumber,
		head:                    pack.Head,
		nack:                    pack.Nack,
		forceStream:             pack.ForceStream,
		companionContract:       pack.CompanionContract,
		sessionRole:             transferFrame.GetSessionRole(),
		sessionCompanion:        transferFrame.GetSessionCompanion(),
		sessionCompanionPresent: transferFrame.SessionCompanion != nil,
		wireBytes:               bytes.Clone(transferFrameBytes),
	}, true, nil
}

// Equality requires a retry to preserve both destination and logical Transfer
// identity while allowing its serialized framing bytes to be reconstructed.
func providerReturnRetryIdentityEqual(
	first providerReturnRetryRecord,
	retry providerReturnRetryRecord,
) bool {
	return first.path == retry.path &&
		first.messageId == retry.messageId &&
		first.sequenceId == retry.sequenceId &&
		first.sequenceNumber == retry.sequenceNumber &&
		first.head == retry.head &&
		first.nack == retry.nack &&
		first.forceStream == retry.forceStream &&
		first.companionContract == retry.companionContract &&
		first.sessionRole == retry.sessionRole &&
		first.sessionCompanion == retry.sessionCompanion &&
		first.sessionCompanionPresent == retry.sessionCompanionPresent
}

// providerReturnAckRecord is the exact Transfer acknowledgement returned for
// the retried Pack.
type providerReturnAckRecord struct {
	path       TransferPath
	messageId  Id
	sequenceId Id
	selective  bool
}

// providerReturnTestAckTarget observes the exact inline completion edge after
// SendSequence removes the acknowledged item.
type providerReturnTestAckTarget struct {
	completed chan error
}

// Completion publishes the inline acknowledgement disposition to the test.
func (self *providerReturnTestAckTarget) sendAckResult(_ ByteCount, err error) {
	self.completed <- err
}

// holdProviderReturnAck forwards non-ack traffic and holds the first Transfer
// ack behind an explicit test barrier.
func holdProviderReturnAck(
	ctx context.Context,
	input Route,
	output Route,
	held chan<- providerReturnAckRecord,
	release <-chan struct{},
	inspectionErrs chan<- error,
) {
	heldAck := false
	for {
		select {
		case <-ctx.Done():
			return
		case transferFrameBytes, ok := <-input:
			if !ok {
				return
			}
			record, ack, err := providerReturnAckRecordFromWire(transferFrameBytes)
			if err != nil {
				select {
				case inspectionErrs <- err:
				default:
				}
			}
			if ack && !heldAck {
				heldAck = true
				held <- record
				select {
				case <-ctx.Done():
					MessagePoolReturn(transferFrameBytes)
					return
				case <-release:
				}
			}
			select {
			case <-ctx.Done():
				MessagePoolReturn(transferFrameBytes)
				return
			case output <- transferFrameBytes:
			}
		}
	}
}

// Decoding selects the exact Transfer acknowledgement while forwarding every
// unrelated frame through the surrounding gate.
func providerReturnAckRecordFromWire(
	transferFrameBytes []byte,
) (providerReturnAckRecord, bool, error) {
	var transferFrame protocol.TransferFrame
	if err := ProtoUnmarshal(transferFrameBytes, &transferFrame); err != nil {
		return providerReturnAckRecord{}, false, fmt.Errorf("decode ack transfer frame: %w", err)
	}
	ack := transferFrame.Ack
	if ack == nil {
		frame := transferFrame.GetFrame()
		if frame == nil || frame.GetMessageType() != protocol.MessageType_TransferAck {
			return providerReturnAckRecord{}, false, nil
		}
		ack = &protocol.Ack{}
		if err := ProtoUnmarshal(frame.GetMessageBytes(), ack); err != nil {
			return providerReturnAckRecord{}, false, fmt.Errorf("decode transfer ack: %w", err)
		}
	}
	path, err := TransferPathFromProtobuf(transferFrame.TransferPath)
	if err != nil {
		return providerReturnAckRecord{}, false, fmt.Errorf("decode transfer ack path: %w", err)
	}
	messageId, err := IdFromBytes(ack.MessageId)
	if err != nil {
		return providerReturnAckRecord{}, false, fmt.Errorf("decode transfer ack message id: %w", err)
	}
	sequenceId, err := IdFromBytes(ack.SequenceId)
	if err != nil {
		return providerReturnAckRecord{}, false, fmt.Errorf("decode transfer ack sequence id: %w", err)
	}
	return providerReturnAckRecord{
		path:       path,
		messageId:  messageId,
		sequenceId: sequenceId,
		selective:  ack.Selective,
	}, true, nil
}

// Installs a deliberately paused send sequence so a test can inspect the
// exact provider return pack before a sequence worker serializes it.
func installProviderReturnTestSequence(
	t *testing.T,
	provider *RemoteUserNatProvider,
	client *Client,
	id sendSequenceId,
) *SendSequence {
	t.Helper()
	sequence := &SendSequence{
		ctx:           provider.ctx,
		cancel:        func() {},
		packs:         make(chan *SendPack, 1),
		idleCondition: NewIdleCondition(),
	}
	client.sendBuffer.mutex.Lock()
	client.sendBuffer.sendSequences[id] = sequence
	client.sendBuffer.wireSendSequences[id.wireId()] = sequence
	client.sendBuffer.mutex.Unlock()
	t.Cleanup(func() {
		client.sendBuffer.mutex.Lock()
		if client.sendBuffer.sendSequences[id] == sequence {
			delete(client.sendBuffer.sendSequences, id)
		}
		if client.sendBuffer.wireSendSequences[id.wireId()] == sequence {
			delete(client.sendBuffer.wireSendSequences, id.wireId())
		}
		client.sendBuffer.mutex.Unlock()
		for {
			select {
			case queued := <-sequence.packs:
				queued.returnFrames()
			default:
				return
			}
		}
	})
	return sequence
}

// Waits for the asynchronous provider sender without using a scheduling race
// as the assertion.
func waitProviderReturnTestPack(t *testing.T, sequence *SendSequence) *SendPack {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case queued := <-sequence.packs:
		return queued
	case <-timer.C:
		t.Fatal("provider did not enqueue the NAT return")
		return nil
	}
}

// Verifies the destination-only public stream lane shared by legacy packet
// and batch retries.
func requireProviderV1RetryLane(t *testing.T, queued *SendPack, destinationId Id) {
	t.Helper()
	if queued.Destination != destinationId {
		t.Fatalf("provider v1 retry destination = %s, want direct id %s", queued.Destination, destinationId)
	}
	if !queued.ForceStream || !queued.CompanionContract || queued.NetworkPeer || !queued.Ack {
		t.Fatalf("provider v1 retry options = %#v, want public acknowledged stream companion", queued.TransferOptions)
	}
	if queued.EncryptionRole != sequenceTlsRoleServer || queued.EncryptionCompanion {
		t.Fatalf(
			"provider v1 retry encryption lane = (%v,%t), want server/non-companion",
			queued.EncryptionRole,
			queued.EncryptionCompanion,
		)
	}
}

// Builds one ordinary public TCP packet accepted by the provider's reversed
// egress policy.
func providerTransferKeyTestPacket() []byte {
	return craftSecurityPacket(
		IpProtocolTcp,
		net.ParseIP("10.0.0.9"),
		42001,
		net.ParseIP("203.0.113.7"),
		8080,
		false,
		[]byte("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"),
	)
}

// The provider must carry the immutable receive lane into NAT conntrack. If it
// keeps only TransferPath, a return packet silently falls back to a different
// force-stream, contract, or encryption lane.
func TestRemoteUserNatProviderIngressPreservesTransferKey(t *testing.T) {
	provider, _, localUserNat := newProviderTransferKeyTestFixture(t)
	source := SourceId(NewId())
	transferKey := TransferKey{
		ForceStream:         true,
		CompanionContract:   true,
		EncryptionRole:      protocol.SequenceRole_SequenceRoleServer,
		EncryptionCompanion: true,
	}
	packet := providerTransferKeyTestPacket()
	frame, err := ipPacketToProviderFrame(packet, DefaultProtocolVersion)
	if err != nil {
		t.Fatalf("build provider ingress frame: %v", err)
	}
	defer MessagePoolReturn(frame.MessageBytes)

	provider.ClientReceive(
		source,
		[]*protocol.Frame{frame},
		Peer{
			ProvideMode: protocol.ProvideMode_Public,
			TransferKey: transferKey,
		},
	)

	select {
	case queued := <-localUserNat.sendPackets:
		defer queued.finish()
		if queued.source != source.LocalMask() {
			t.Fatalf("NAT source = %s, want %s", queued.source, source.LocalMask())
		}
		if queued.transferKey != transferKey {
			t.Fatalf("NAT transfer key = %#v, want %#v", queued.transferKey, transferKey)
		}
		if queued.provideMode != protocol.ProvideMode_Public {
			t.Fatalf("NAT provide mode = %v, want Public", queued.provideMode)
		}
		if len(queued.packets) != 1 {
			t.Fatalf("NAT packet count = %d, want 1", len(queued.packets))
		}
		for _, queuedPacket := range queued.packets {
			MessagePoolReturn(queuedPacket)
		}
	default:
		t.Fatal("provider did not enqueue accepted ingress into NAT")
	}
}

// Two peers using the same IP flow tuple retain their own authenticated source
// and reply lane; neither value is inferred from or stored inside the other.
func TestRemoteUserNatProviderIngressKeepsSourceAndTransferKeyPaired(t *testing.T) {
	provider, _, localUserNat := newProviderTransferKeyTestFixture(t)
	sources := []TransferPath{SourceId(NewId()), SourceId(NewId())}
	transferKeys := []TransferKey{
		{
			ForceStream:         true,
			EncryptionRole:      protocol.SequenceRole_SequenceRoleClient,
			EncryptionCompanion: false,
		},
		{
			ForceStream:         false,
			EncryptionRole:      protocol.SequenceRole_SequenceRoleServer,
			EncryptionCompanion: true,
		},
	}
	packet := providerTransferKeyTestPacket()
	frame, err := ipPacketToProviderFrame(packet, DefaultProtocolVersion)
	if err != nil {
		t.Fatalf("build provider ingress frame: %v", err)
	}
	defer MessagePoolReturn(frame.MessageBytes)

	for pairIndex := range sources {
		provider.ClientReceive(
			sources[pairIndex],
			[]*protocol.Frame{frame},
			Peer{
				ProvideMode: protocol.ProvideMode_Public,
				TransferKey: transferKeys[pairIndex],
			},
		)
		var queued *SendPacket
		select {
		case queued = <-localUserNat.sendPackets:
		default:
			t.Fatalf("provider did not enqueue source/key pair %d", pairIndex)
		}
		defer queued.finish()
		if queued.source != sources[pairIndex].LocalMask() {
			t.Fatalf(
				"NAT pair %d source = %s, want %s",
				pairIndex,
				queued.source,
				sources[pairIndex].LocalMask(),
			)
		}
		if queued.transferKey != transferKeys[pairIndex] {
			t.Fatalf(
				"NAT pair %d key = %#v, want %#v",
				pairIndex,
				queued.transferKey,
				transferKeys[pairIndex],
			)
		}
		for _, queuedPacket := range queued.packets {
			MessagePoolReturn(queuedPacket)
		}
	}
	if stats := provider.PacketStats(); stats.RemoteIngressPacketCount != 2 {
		t.Fatalf("paired ingress stats = %+v, want two packets", stats)
	}
}

// A NAT return uses the authenticated source id as its direct destination
// while preserving the receiver-visible force-stream and encryption lane on
// the queued SendPack. Public policy derives a companion contract separately,
// while TCP remains acknowledged because the upstream socket already consumed
// the bytes and is the only component able to reproduce them.
func TestRemoteUserNatProviderReturnUsesDestinationOnlyTransferKey(t *testing.T) {
	provider, client, _ := newProviderTransferKeyTestFixture(t)
	peerId := NewId()
	source := SourceId(peerId)
	transferKey := TransferKey{
		ForceStream:         true,
		CompanionContract:   false,
		EncryptionRole:      protocol.SequenceRole_SequenceRoleServer,
		EncryptionCompanion: false,
	}
	destinationId := peerId
	id := sendSequenceId{
		Destination:         destinationId,
		CompanionContract:   true,
		ForceStream:         true,
		EncryptionRole:      sequenceTlsRoleServer,
		EncryptionCompanion: false,
	}
	sequence := installProviderReturnTestSequence(t, provider, client, id)

	response := craftSecurityPacket(
		IpProtocolTcp,
		net.ParseIP("203.0.113.7"),
		8080,
		net.ParseIP("10.0.0.9"),
		42001,
		false,
		[]byte("HTTP/1.1 200 OK\r\n\r\n"),
	)
	responsePath, err := ParseIpPath(response)
	if err != nil {
		t.Fatalf("parse provider return packet: %v", err)
	}
	provider.receiveTransfer(source, transferKey, protocol.ProvideMode_Public, responsePath, response)

	queued := waitProviderReturnTestPack(t, sequence)
	defer queued.returnFrames()
	if queued.Frame == nil || queued.Frames != nil || !queued.Frame.Raw {
		t.Fatalf("provider v2 return frame = %#v, want one raw frame", queued)
	}
	packetBytes, err := ipPacketFromProviderBytes(queued.Frame)
	if err != nil {
		t.Fatalf("decode provider v2 return: %v", err)
	}
	if !bytes.Equal(packetBytes, response) {
		t.Fatal("provider v2 return changed the packet bytes")
	}
	if queued.Destination != destinationId {
		t.Fatalf("provider return destination = %s, want direct id %s", queued.Destination, destinationId)
	}
	if !queued.ForceStream || !queued.CompanionContract || queued.NetworkPeer || !queued.Ack {
		t.Fatalf("provider return options = %#v, want public acknowledged stream companion", queued.TransferOptions)
	}
	if queued.EncryptionRole != sequenceTlsRoleServer || queued.EncryptionCompanion {
		t.Fatalf(
			"provider return encryption lane = (%v,%t), want server/non-companion",
			queued.EncryptionRole,
			queued.EncryptionCompanion,
		)
	}
}

// Legacy wrapping must reproduce the same destination-only reply lane as the
// v2 raw path, including the receiver-selected encryption session and TCP
// recovery ownership.
func TestRemoteUserNatProviderReturnV1PreservesTransferKey(t *testing.T) {
	provider, client, _ := newProviderTransferKeyTestFixture(t)
	provider.settings.ProtocolVersion = 1
	peerId := NewId()
	source := SourceId(peerId)
	transferKey := TransferKey{
		ForceStream:         true,
		CompanionContract:   false,
		EncryptionRole:      protocol.SequenceRole_SequenceRoleServer,
		EncryptionCompanion: false,
	}
	destinationId := peerId
	sequence := installProviderReturnTestSequence(t, provider, client, sendSequenceId{
		Destination:         destinationId,
		CompanionContract:   true,
		ForceStream:         true,
		EncryptionRole:      sequenceTlsRoleServer,
		EncryptionCompanion: false,
	})
	response := craftSecurityPacket(
		IpProtocolTcp,
		net.ParseIP("203.0.113.7"),
		8080,
		net.ParseIP("10.0.0.9"),
		42001,
		false,
		[]byte("HTTP/1.1 200 OK\r\nX-Legacy: 1\r\n\r\n"),
	)
	responsePath, err := ParseIpPath(response)
	if err != nil {
		t.Fatalf("parse legacy provider return packet: %v", err)
	}

	provider.receiveTransfer(source, transferKey, protocol.ProvideMode_Public, responsePath, response)
	queued := waitProviderReturnTestPack(t, sequence)
	defer queued.returnFrames()
	if queued.Frame == nil || queued.Frames != nil || queued.Frame.Raw {
		t.Fatalf("provider v1 return frame = %#v, want one wrapped frame", queued)
	}
	packetBytes, err := ipPacketFromProviderBytes(queued.Frame)
	if err != nil {
		t.Fatalf("decode provider v1 return: %v", err)
	}
	if !bytes.Equal(packetBytes, response) {
		t.Fatal("provider v1 return changed the packet bytes")
	}
	if queued.Destination != destinationId {
		t.Fatalf("provider v1 destination = %s, want direct id %s", queued.Destination, destinationId)
	}
	if !queued.ForceStream || !queued.CompanionContract || queued.NetworkPeer || !queued.Ack {
		t.Fatalf("provider v1 options = %#v, want public acknowledged stream companion", queued.TransferOptions)
	}
	if queued.EncryptionRole != sequenceTlsRoleServer || queued.EncryptionCompanion {
		t.Fatalf(
			"provider v1 encryption lane = (%v,%t), want server/non-companion",
			queued.EncryptionRole,
			queued.EncryptionCompanion,
		)
	}
}

// A rejected legacy wrapped packet stays owned by its socket-return callback
// and is retried unchanged after Transfer capacity appears.
func TestRemoteUserNatProviderReturnV1RetriesRejectedSocketPacket(t *testing.T) {
	settings := DefaultRemoteUserNatProviderSettings()
	settings.ProtocolVersion = 1
	settings.WriteTimeout = 0
	settings.ReturnSendRetryTimeout = time.Millisecond
	provider, client, _ := newProviderTransferKeyTestFixtureWithSettings(t, settings)
	peerId := NewId()
	transferKey := TransferKey{
		ForceStream:         true,
		CompanionContract:   false,
		EncryptionRole:      protocol.SequenceRole_SequenceRoleServer,
		EncryptionCompanion: false,
	}
	sequence := installProviderReturnTestSequence(t, provider, client, sendSequenceId{
		Destination:         peerId,
		CompanionContract:   true,
		ForceStream:         true,
		EncryptionRole:      sequenceTlsRoleServer,
		EncryptionCompanion: false,
	})
	sequence.packs = make(chan *SendPack)
	attemptResults := make(chan providerReturnSendResult, 3)
	provider.afterReturnSendAttemptForTest = func(result providerReturnSendResult) {
		attemptResults <- result
	}
	finalResults := make(chan providerReturnSendResult, 2)
	provider.afterReturnSendForTest = func(result providerReturnSendResult) {
		finalResults <- result
	}
	retryEntered := make(chan struct{}, 1)
	releaseRetry := make(chan struct{}, 1)
	t.Cleanup(func() {
		select {
		case releaseRetry <- struct{}{}:
		default:
		}
	})
	provider.beforeTcpReturnSendRetryForTest = func() {
		select {
		case retryEntered <- struct{}{}:
		default:
		}
		<-releaseRetry
	}

	response := MessagePoolCopy(craftSecurityPacket(
		IpProtocolTcp,
		net.ParseIP("203.0.113.7"),
		8080,
		net.ParseIP("10.0.0.9"),
		42001,
		false,
		[]byte("HTTP/1.1 200 OK\r\nX-Legacy-Retry: packet\r\n\r\n"),
	))
	responseOwned := true
	defer func() {
		if responseOwned {
			MessagePoolReturn(response)
		}
	}()
	expectedResponse := bytes.Clone(response)
	responsePath, err := ParseIpPath(response)
	if err != nil {
		t.Fatalf("parse retried legacy provider packet: %v", err)
	}
	callbackReturned := make(chan struct{})
	go func() {
		provider.receiveTransferWithRecovery(
			SourceId(peerId),
			transferKey,
			protocol.ProvideMode_Public,
			receiveRecoveryModeTcpSocket,
			responsePath,
			response,
		)
		close(callbackReturned)
	}()
	waitProviderReturnSendResult(t, attemptResults, false, 1, ByteCount(len(response)))
	waitProviderReturnBarrier(t, retryEntered, "legacy packet retry")
	if stats := provider.PacketStats(); stats.RemoteEgressPacketCount != 0 || stats.RemoteEgressByteCount != 0 {
		t.Fatalf("rejected legacy packet changed delivery stats: %+v", stats)
	}
	if drops := provider.CongestionDropStats(); drops != (ProviderCongestionDrops{}) {
		t.Fatalf("transient legacy packet rejection changed drop stats: %+v", drops)
	}

	sequence.packs = make(chan *SendPack, 1)
	releaseRetry <- struct{}{}
	waitProviderReturnSendResult(t, attemptResults, true, 1, ByteCount(len(response)))
	queued := waitProviderReturnTestPack(t, sequence)
	queuedOwned := true
	defer func() {
		if queuedOwned {
			queued.returnFrames()
		}
	}()
	if queued.Frame == nil || queued.Frames != nil || queued.Frame.Raw {
		t.Fatalf("retried provider v1 packet = %#v, want one wrapped frame", queued)
	}
	packetBytes, err := ipPacketFromProviderBytes(queued.Frame)
	if err != nil {
		t.Fatalf("decode retried provider v1 packet: %v", err)
	}
	if !bytes.Equal(packetBytes, expectedResponse) {
		t.Fatal("legacy packet retry changed the wrapped packet bytes")
	}
	requireProviderV1RetryLane(t, queued, peerId)
	waitProviderReturnSendResult(t, finalResults, true, 1, ByteCount(len(response)))
	waitProviderReturnBarrier(t, callbackReturned, "legacy packet callback completion")
	select {
	case extra := <-attemptResults:
		t.Fatalf("legacy packet had an extra send attempt: %+v", extra)
	default:
	}
	select {
	case extra := <-finalResults:
		t.Fatalf("legacy packet had an extra final disposition: %+v", extra)
	default:
	}

	frameWitness := MessagePoolShareReadOnly(queued.Frame.MessageBytes)
	queued.returnFrames()
	queuedOwned = false
	if !MessagePoolReturn(frameWitness) {
		t.Fatal("legacy packet retry retained its wrapped frame buffer")
	}
	responseReturned := MessagePoolReturn(response)
	responseOwned = false
	if !responseReturned {
		t.Fatal("legacy packet retry retained its borrowed socket packet")
	}
	stats := provider.PacketStats()
	if stats.RemoteEgressPacketCount != 1 || stats.RemoteEgressByteCount != ByteCount(len(expectedResponse)) {
		t.Fatalf("legacy packet retry stats=%+v, want one exact delivery", stats)
	}
	if drops := provider.CongestionDropStats(); drops != (ProviderCongestionDrops{}) {
		t.Fatalf("successful legacy packet retry recorded congestion loss: %+v", drops)
	}
}

// A keyed no-contract return batch must remain one logical group while
// preserving the received force-stream, role, and session independently of
// contract policy, and retaining TCP recovery ownership for every frame.
func TestRemoteUserNatProviderReturnBatchPreservesTransferKey(t *testing.T) {
	provider, client, _ := newProviderTransferKeyTestFixture(t)
	peerId := NewId()
	client.ContractManager().AddNoContractPeer(peerId)
	source := SourceId(peerId)
	transferKey := TransferKey{
		ForceStream:         true,
		CompanionContract:   false,
		EncryptionRole:      protocol.SequenceRole_SequenceRoleServer,
		EncryptionCompanion: false,
	}
	destinationId := peerId
	sequence := installProviderReturnTestSequence(t, provider, client, sendSequenceId{
		Destination:         destinationId,
		CompanionContract:   true,
		ForceStream:         true,
		EncryptionRole:      sequenceTlsRoleServer,
		EncryptionCompanion: false,
	})
	responses := [][]byte{
		craftSecurityPacket(
			IpProtocolTcp,
			net.ParseIP("203.0.113.7"),
			8080,
			net.ParseIP("10.0.0.9"),
			42001,
			false,
			[]byte("HTTP/1.1 200 OK\r\nX-Part: 1\r\n\r\n"),
		),
		craftSecurityPacket(
			IpProtocolTcp,
			net.ParseIP("203.0.113.7"),
			8080,
			net.ParseIP("10.0.0.9"),
			42001,
			false,
			[]byte("HTTP/1.1 200 OK\r\nX-Part: 2\r\n\r\n"),
		),
	}
	responsePath, err := ParseIpPath(responses[0])
	if err != nil {
		t.Fatalf("parse provider return batch: %v", err)
	}

	provider.receiveTransferBatch(
		source,
		transferKey,
		protocol.ProvideMode_Public,
		responsePath,
		responses,
	)
	queued := waitProviderReturnTestPack(t, sequence)
	defer queued.returnFrames()
	if queued.Frame != nil || !queued.logicalGroup || len(queued.Frames) != len(responses) {
		t.Fatalf(
			"provider batch shape = (%p,logical=%t,%d), want (nil,true,%d)",
			queued.Frame,
			queued.logicalGroup,
			len(queued.Frames),
			len(responses),
		)
	}
	for frameIndex, frame := range queued.Frames {
		if !frame.Raw {
			t.Fatalf("provider batch frame %d is wrapped, want raw", frameIndex)
		}
		packetBytes, err := ipPacketFromProviderBytes(frame)
		if err != nil {
			t.Fatalf("decode provider batch frame %d: %v", frameIndex, err)
		}
		if !bytes.Equal(packetBytes, responses[frameIndex]) {
			t.Fatalf("provider batch frame %d changed the packet bytes", frameIndex)
		}
	}
	if queued.Destination != destinationId {
		t.Fatalf("provider batch destination = %s, want direct id %s", queued.Destination, destinationId)
	}
	if !queued.ForceStream || !queued.CompanionContract || queued.NetworkPeer || !queued.Ack {
		t.Fatalf("provider batch options = %#v, want public acknowledged stream companion", queued.TransferOptions)
	}
	if queued.EncryptionRole != sequenceTlsRoleServer || queued.EncryptionCompanion {
		t.Fatalf(
			"provider batch encryption lane = (%v,%t), want server/non-companion",
			queued.EncryptionRole,
			queued.EncryptionCompanion,
		)
	}
}

// Contract-bearing socket drains must enter the same logical group path. The
// selected SendSequence, not this callback, owns carrier-safe chunking and
// contract-envelope boundaries; falling back to one Pack per packet would
// silently restore H1's download-direction framing and ACK amplification.
func TestRemoteUserNatProviderReturnBatchGroupsContractBearingDrain(t *testing.T) {
	provider, client, _ := newProviderTransferKeyTestFixture(t)
	peerId := NewId()
	source := SourceId(peerId)
	transferKey := TransferKey{
		ForceStream:         true,
		EncryptionRole:      protocol.SequenceRole_SequenceRoleServer,
		EncryptionCompanion: false,
	}
	sequence := installProviderReturnTestSequence(t, provider, client, sendSequenceId{
		Destination:       peerId,
		CompanionContract: true,
		ForceStream:       true,
		EncryptionRole:    sequenceTlsRoleServer,
	})
	responses := [][]byte{
		craftSecurityPacket(
			IpProtocolTcp,
			net.ParseIP("203.0.113.11"),
			443,
			net.ParseIP("10.0.0.12"),
			43001,
			false,
			[]byte{1},
		),
		craftSecurityPacket(
			IpProtocolTcp,
			net.ParseIP("203.0.113.11"),
			443,
			net.ParseIP("10.0.0.12"),
			43001,
			false,
			[]byte{2},
		),
	}
	responsePath, err := ParseIpPath(responses[0])
	if err != nil {
		t.Fatalf("parse contract-bearing provider return batch: %v", err)
	}

	provider.receiveTransferBatch(
		source,
		transferKey,
		protocol.ProvideMode_Public,
		responsePath,
		responses,
	)
	queued := waitProviderReturnTestPack(t, sequence)
	defer queued.returnFrames()
	if queued.Frame != nil || !queued.logicalGroup || len(queued.Frames) != len(responses) {
		t.Fatalf(
			"contract-bearing provider batch shape=(%p,logical=%t,%d), want (nil,true,%d)",
			queued.Frame,
			queued.logicalGroup,
			len(queued.Frames),
			len(responses),
		)
	}
	for frameIndex, frame := range queued.Frames {
		packetBytes, decodeErr := ipPacketFromProviderBytes(frame)
		if decodeErr != nil {
			t.Fatalf("decode contract-bearing provider frame %d: %v", frameIndex, decodeErr)
		}
		if !bytes.Equal(packetBytes, responses[frameIndex]) {
			t.Fatalf("contract-bearing provider frame %d changed or reordered", frameIndex)
		}
	}
}

func TestRemoteUserNatProviderReturnBatchUsesProductionLogicalBound(t *testing.T) {
	provider, client, _ := newProviderTransferKeyTestFixture(t)
	peerId := NewId()
	client.ContractManager().AddNoContractPeer(peerId)
	sequence := installProviderReturnTestSequence(t, provider, client, sendSequenceId{
		Destination:       peerId,
		CompanionContract: true,
		ForceStream:       true,
		EncryptionRole:    sequenceTlsRoleServer,
	})
	sequence.packs = make(chan *SendPack, 2)
	template := craftSecurityPacket(
		IpProtocolTcp,
		net.ParseIP("203.0.113.11"),
		443,
		net.ParseIP("10.0.0.12"),
		43001,
		false,
		make([]byte, 1400),
	)
	path, err := ParseIpPath(template)
	if err != nil {
		t.Fatalf("parse provider production-bound packet: %v", err)
	}
	packets := make([][]byte, providerReturnBatchMaxFrames+1)
	for packetIndex := range packets {
		packets[packetIndex] = MessagePoolCopy(template)
	}
	item := providerReturnItem{
		source: SourceId(peerId),
		transferKey: TransferKey{
			ForceStream:       true,
			CompanionContract: true,
			EncryptionRole:    protocol.SequenceRole_SequenceRoleServer,
		},
		provideMode:     protocol.ProvideMode_Public,
		recoveryMode:    receiveRecoveryModeTcpSocket,
		ipProtocol:      IpProtocolTcp,
		packets:         packets,
		packetByteCount: ByteCount(len(packets) * len(template)),
		batch:           true,
		schedulingKey:   ipSendSchedulingKey(path),
	}
	if !provider.sendReturnBatch(&item) {
		t.Fatal("provider production-bound batch was not admitted")
	}
	if got := len(sequence.packs); got != 2 {
		t.Fatalf("provider production-bound logical groups=%d, want 2", got)
	}
	first := <-sequence.packs
	second := <-sequence.packs
	defer first.returnFrames()
	defer second.returnFrames()
	if len(first.Frames) != providerReturnBatchMaxFrames || len(second.Frames) != 1 {
		t.Fatalf(
			"provider production-bound group frames=%d/%d, want %d/1",
			len(first.Frames),
			len(second.Frames),
			providerReturnBatchMaxFrames,
		)
	}
}

// BenchmarkRemoteUserNatProviderReturnBatchLimits measures the provider-side
// policy/routing admission boundary that a larger socket drain can amortize.
// Carrier-safe H1 chunks remain independently bounded by SendSequence; this
// benchmark changes only the logical group presented to that scheduler.
func BenchmarkRemoteUserNatProviderReturnBatchLimits(b *testing.B) {
	const packetCount = 64
	variants := []struct {
		name      string
		maxFrames int
		maxBytes  int64
	}{
		{name: "16_frames_24_KiB", maxFrames: 16, maxBytes: 24 * 1024},
		{name: "32_frames_48_KiB", maxFrames: 32, maxBytes: 48 * 1024},
	}
	for _, variant := range variants {
		b.Run(variant.name, func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			client := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
			b.Cleanup(func() {
				cancel()
				client.Cancel()
			})
			provider := &RemoteUserNatProvider{
				ctx:                 ctx,
				client:              client,
				settings:            DefaultRemoteUserNatProviderSettings(),
				packetStatsCounters: &packetStatsCounters{},
			}
			peerId := NewId()
			client.ContractManager().AddNoContractPeer(peerId)
			sequenceId := sendSequenceId{
				Destination:       peerId,
				CompanionContract: true,
				ForceStream:       true,
				EncryptionRole:    sequenceTlsRoleServer,
			}
			sequence := &SendSequence{
				ctx:           ctx,
				cancel:        func() {},
				packs:         make(chan *SendPack, packetCount),
				idleCondition: NewIdleCondition(),
			}
			client.sendBuffer.mutex.Lock()
			client.sendBuffer.sendSequences[sequenceId] = sequence
			client.sendBuffer.wireSendSequences[sequenceId.wireId()] = sequence
			client.sendBuffer.mutex.Unlock()
			b.Cleanup(func() {
				client.sendBuffer.mutex.Lock()
				delete(client.sendBuffer.sendSequences, sequenceId)
				delete(client.sendBuffer.wireSendSequences, sequenceId.wireId())
				client.sendBuffer.mutex.Unlock()
				for 0 < len(sequence.packs) {
					(<-sequence.packs).returnFrames()
				}
			})

			template := craftSecurityPacket(
				IpProtocolTcp,
				net.ParseIP("203.0.113.11"),
				443,
				net.ParseIP("10.0.0.12"),
				43001,
				false,
				make([]byte, 1400),
			)
			path, err := ParseIpPath(template)
			if err != nil {
				b.Fatalf("parse provider benchmark packet: %v", err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(packetCount * len(template)))
			var logicalGroupCount int64
			b.ResetTimer()
			for range b.N {
				var packetValues [packetCount][]byte
				packets := packetValues[:]
				for packetIndex := range packets {
					packets[packetIndex] = MessagePoolCopy(template)
				}
				item := providerReturnItem{
					source: SourceId(peerId),
					transferKey: TransferKey{
						ForceStream:       true,
						CompanionContract: true,
						EncryptionRole:    protocol.SequenceRole_SequenceRoleServer,
					},
					provideMode:     protocol.ProvideMode_Public,
					recoveryMode:    receiveRecoveryModeTcpSocket,
					ipProtocol:      IpProtocolTcp,
					packets:         packets,
					packetByteCount: ByteCount(packetCount * len(template)),
					batch:           true,
					schedulingKey:   ipSendSchedulingKey(path),
				}
				if !provider.sendReturnBatchWithLimits(
					&item,
					variant.maxFrames,
					variant.maxBytes,
				) {
					b.Fatal("provider benchmark batch was not admitted")
				}
				groupCount := len(sequence.packs)
				if groupCount == 0 {
					b.Fatal("provider benchmark emitted no logical group")
				}
				logicalGroupCount += int64(groupCount)
				for range groupCount {
					(<-sequence.packs).returnFrames()
				}
			}
			b.ReportMetric(float64(logicalGroupCount)/float64(b.N), "logical-groups/op")
		})
	}
}

// A rejected legacy no-contract batch retains every wrapped frame as one
// ordered socket-owned disposition until Transfer accepts it.
func TestRemoteUserNatProviderReturnV1RetriesRejectedSocketBatch(t *testing.T) {
	settings := DefaultRemoteUserNatProviderSettings()
	settings.ProtocolVersion = 1
	settings.WriteTimeout = 0
	settings.ReturnSendRetryTimeout = time.Millisecond
	provider, client, _ := newProviderTransferKeyTestFixtureWithSettings(t, settings)
	peerId := NewId()
	client.ContractManager().AddNoContractPeer(peerId)
	transferKey := TransferKey{
		ForceStream:         true,
		CompanionContract:   false,
		EncryptionRole:      protocol.SequenceRole_SequenceRoleServer,
		EncryptionCompanion: false,
	}
	sequence := installProviderReturnTestSequence(t, provider, client, sendSequenceId{
		Destination:         peerId,
		CompanionContract:   true,
		ForceStream:         true,
		EncryptionRole:      sequenceTlsRoleServer,
		EncryptionCompanion: false,
	})
	sequence.packs = make(chan *SendPack)
	attemptResults := make(chan providerReturnSendResult, 3)
	provider.afterReturnSendAttemptForTest = func(result providerReturnSendResult) {
		attemptResults <- result
	}
	finalResults := make(chan providerReturnSendResult, 2)
	provider.afterReturnSendForTest = func(result providerReturnSendResult) {
		finalResults <- result
	}
	retryEntered := make(chan struct{}, 1)
	releaseRetry := make(chan struct{}, 1)
	t.Cleanup(func() {
		select {
		case releaseRetry <- struct{}{}:
		default:
		}
	})
	provider.beforeTcpReturnSendRetryForTest = func() {
		select {
		case retryEntered <- struct{}{}:
		default:
		}
		<-releaseRetry
	}

	responses := make([][]byte, 2)
	expectedResponses := make([][]byte, len(responses))
	for responseIndex := range responses {
		responses[responseIndex] = MessagePoolCopy(craftSecurityPacket(
			IpProtocolTcp,
			net.ParseIP("203.0.113.7"),
			8080,
			net.ParseIP("10.0.0.9"),
			42001,
			false,
			[]byte{byte(responseIndex + 1)},
		))
		expectedResponses[responseIndex] = bytes.Clone(responses[responseIndex])
	}
	defer func() {
		for _, response := range responses {
			if response != nil {
				MessagePoolReturn(response)
			}
		}
	}()
	responsePath, err := ParseIpPath(responses[0])
	if err != nil {
		t.Fatalf("parse retried legacy provider batch: %v", err)
	}
	packetByteCount := ByteCount(len(responses[0]) + len(responses[1]))
	callbackReturned := make(chan struct{})
	go func() {
		provider.receiveTransferBatchWithRecovery(
			SourceId(peerId),
			transferKey,
			protocol.ProvideMode_Public,
			receiveRecoveryModeTcpSocket,
			responsePath,
			responses,
		)
		close(callbackReturned)
	}()
	waitProviderReturnSendResult(t, attemptResults, false, len(responses), packetByteCount)
	waitProviderReturnBarrier(t, retryEntered, "legacy batch retry")
	if stats := provider.PacketStats(); stats.RemoteEgressPacketCount != 0 || stats.RemoteEgressByteCount != 0 {
		t.Fatalf("rejected legacy batch changed delivery stats: %+v", stats)
	}
	if drops := provider.CongestionDropStats(); drops != (ProviderCongestionDrops{}) {
		t.Fatalf("transient legacy batch rejection changed drop stats: %+v", drops)
	}

	sequence.packs = make(chan *SendPack, 1)
	releaseRetry <- struct{}{}
	waitProviderReturnSendResult(t, attemptResults, true, len(responses), packetByteCount)
	queued := waitProviderReturnTestPack(t, sequence)
	queuedOwned := true
	defer func() {
		if queuedOwned {
			queued.returnFrames()
		}
	}()
	if queued.Frame != nil || !queued.logicalGroup || len(queued.Frames) != len(expectedResponses) {
		t.Fatalf(
			"retried provider v1 batch shape=(%p,logical=%t,%d), want (nil,true,%d)",
			queued.Frame,
			queued.logicalGroup,
			len(queued.Frames),
			len(expectedResponses),
		)
	}
	for frameIndex, frame := range queued.Frames {
		if frame.Raw {
			t.Fatalf("retried provider v1 batch frame %d is raw, want wrapped", frameIndex)
		}
		packetBytes, decodeErr := ipPacketFromProviderBytes(frame)
		if decodeErr != nil {
			t.Fatalf("decode retried provider v1 batch frame %d: %v", frameIndex, decodeErr)
		}
		if !bytes.Equal(packetBytes, expectedResponses[frameIndex]) {
			t.Fatalf("legacy batch retry changed or reordered frame %d", frameIndex)
		}
	}
	requireProviderV1RetryLane(t, queued, peerId)
	waitProviderReturnSendResult(t, finalResults, true, len(responses), packetByteCount)
	waitProviderReturnBarrier(t, callbackReturned, "legacy batch callback completion")
	select {
	case extra := <-attemptResults:
		t.Fatalf("legacy batch had an extra send attempt: %+v", extra)
	default:
	}
	select {
	case extra := <-finalResults:
		t.Fatalf("legacy batch had an extra final disposition: %+v", extra)
	default:
	}

	frameWitnesses := make([][]byte, len(queued.Frames))
	for frameIndex, frame := range queued.Frames {
		frameWitnesses[frameIndex] = MessagePoolShareReadOnly(frame.MessageBytes)
	}
	queued.returnFrames()
	queuedOwned = false
	for frameIndex, frameWitness := range frameWitnesses {
		if !MessagePoolReturn(frameWitness) {
			t.Fatalf("legacy batch retry retained wrapped frame buffer %d", frameIndex)
		}
	}
	for responseIndex, response := range responses {
		responseReturned := MessagePoolReturn(response)
		responses[responseIndex] = nil
		if !responseReturned {
			t.Fatalf("legacy batch retry retained borrowed socket packet %d", responseIndex)
		}
	}
	stats := provider.PacketStats()
	if stats.RemoteEgressPacketCount != int64(len(expectedResponses)) || stats.RemoteEgressByteCount != packetByteCount {
		t.Fatalf("legacy batch retry stats=%+v, want %d exact deliveries", stats, len(expectedResponses))
	}
	if drops := provider.CongestionDropStats(); drops != (ProviderCongestionDrops{}) {
		t.Fatalf("successful legacy batch retry recorded congestion loss: %+v", drops)
	}
}

// A provider has already consumed TCP bytes before it builds the return IP
// packet, so Transfer owns recovery even when the carrier is a direct stream.
// Drop the first exact Pack before delivery, hold its same-identity retry, then
// hold the receiver's exact ack before it reaches the provider. This fails at
// the retry barrier if providerReturnIpTransferOptions returns Ack=false.
func TestRemoteUserNatProviderForceStreamTcpReturnRetriesFirstDrop(t *testing.T) {
	clientSettings := DefaultClientSettings()
	clientSettings.SendBufferSettings.MinResendInterval = 10 * time.Millisecond
	clientSettings.SendBufferSettings.RttMinResendInterval = 10 * time.Millisecond
	clientSettings.SendBufferSettings.MaxResendInterval = 20 * time.Millisecond
	clientSettings.SendBufferSettings.AckTimeout = 5 * time.Second
	clientSettings.SendBufferSettings.IdleTimeout = 5 * time.Second
	clientSettings.SendBufferSettings.WriteTimeout = time.Second
	providerSettings := DefaultRemoteUserNatProviderSettings()
	providerSettings.ReturnSendWorkerCount = 1
	providerSettings.ReturnSendQueueSize = 1
	providerSettings.WriteTimeout = time.Second
	provider, providerClient, _ := newProviderTransferKeyTestFixtureWithClientSettings(
		t,
		providerSettings,
		clientSettings,
	)
	ackCompletion := &providerReturnTestAckTarget{
		completed: make(chan error, 1),
	}
	provider.returnAckTargetForTest = ackCompletion

	peerId := NewId()
	receiverSettings := DefaultClientSettings()
	receiverSettings.EncryptionSettings.Mode = EncryptionModeOff
	receiver := NewClient(provider.ctx, peerId, NewNoContractClientOob(), receiverSettings)
	defer receiver.Cancel()

	response := craftSecurityPacket(
		IpProtocolTcp,
		net.ParseIP("203.0.113.7"),
		8080,
		net.ParseIP("10.0.0.9"),
		42001,
		false,
		[]byte("HTTP/1.1 200 OK\r\nX-Retry-Identity: exact\r\n\r\n"),
	)
	responsePath, err := ParseIpPath(response)
	if err != nil {
		t.Fatalf("parse provider return packet: %v", err)
	}

	returnGate := newProviderReturnRetryGate(provider.ctx, response)
	providerToGate := make(Route)
	gateToReceiver := make(Route)
	go returnGate.run(providerToGate, gateToReceiver)
	providerClient.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(peerId)),
		[]Route{providerToGate},
	)
	receiver.RouteManager().UpdateTransport(
		NewReceiveGatewayTransport(),
		[]Route{gateToReceiver},
	)

	ackFromReceiver := make(Route)
	ackToProvider := make(Route)
	heldAck := make(chan providerReturnAckRecord, 1)
	releaseAck := make(chan struct{})
	ackInspectionErrs := make(chan error, 1)
	go holdProviderReturnAck(
		provider.ctx,
		ackFromReceiver,
		ackToProvider,
		heldAck,
		releaseAck,
		ackInspectionErrs,
	)
	receiver.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(providerClient.ClientId())),
		[]Route{ackFromReceiver},
	)
	providerClient.RouteManager().UpdateTransport(
		NewReceiveGatewayTransport(),
		[]Route{ackToProvider},
	)
	providerClient.ContractManager().AddNoContractPeer(peerId)
	receiver.ContractManager().AddNoContractPeer(providerClient.ClientId())

	type delivery struct {
		source TransferPath
		peer   Peer
		packet []byte
		raw    bool
	}
	deliveries := make(chan delivery, 2)
	deliveryErrs := make(chan error, 1)
	unsub := receiver.AddReceiveCallback(func(
		source TransferPath,
		frames []*protocol.Frame,
		peer Peer,
	) {
		for _, frame := range frames {
			if frame.GetMessageType() != protocol.MessageType_IpIpPacketFromProvider {
				continue
			}
			packet, decodeErr := ipPacketFromProviderBytes(frame)
			if decodeErr != nil {
				select {
				case deliveryErrs <- decodeErr:
				default:
				}
				continue
			}
			if !bytes.Equal(packet, response) {
				continue
			}
			observation := delivery{
				source: source,
				peer:   peer,
				packet: bytes.Clone(packet),
				raw:    frame.Raw,
			}
			select {
			case deliveries <- observation:
			default:
				select {
				case deliveryErrs <- fmt.Errorf("provider return callback queue full"):
				default:
				}
			}
		}
	})
	defer unsub()

	receivedTransferKey := TransferKey{
		ForceStream:         true,
		CompanionContract:   false,
		EncryptionRole:      protocol.SequenceRole_SequenceRoleServer,
		EncryptionCompanion: false,
	}
	provider.receiveTransferWithRecovery(
		SourceId(peerId),
		receivedTransferKey,
		protocol.ProvideMode_Public,
		receiveRecoveryModeTcpSocket,
		responsePath,
		response,
	)

	var first providerReturnRetryRecord
	select {
	case first = <-returnGate.firstDropped:
	case inspectionErr := <-returnGate.inspectionErrs:
		t.Fatalf("inspect first provider return: %v", inspectionErr)
	case <-time.After(5 * time.Second):
		t.Fatal("provider return did not reach the exact first-drop barrier")
	}
	expectedDataPath := NewTransferPath(providerClient.ClientId(), peerId, Id{})
	if first.path != expectedDataPath {
		t.Fatalf("provider return path = %s, want destination-only local path %s", first.path, expectedDataPath)
	}
	if first.messageId == (Id{}) || first.sequenceId == (Id{}) {
		t.Fatalf("provider return identity = (%s,%s), want nonzero message and sequence ids", first.messageId, first.sequenceId)
	}
	if first.sequenceNumber != 0 || !first.head {
		t.Fatalf("provider return sequence = (%d,head=%t), want initial head", first.sequenceNumber, first.head)
	}
	if first.nack {
		t.Fatal("provider ForceStream TCP return is unacknowledged; Transfer cannot retry the consumed TCP bytes")
	}
	if !first.forceStream || !first.companionContract {
		t.Fatalf("provider return lane = (stream=%t,companion=%t), want public stream companion", first.forceStream, first.companionContract)
	}
	if first.sessionRole != protocol.SequenceRole_SequenceRoleServer ||
		first.sessionCompanionPresent || first.sessionCompanion {
		t.Fatalf(
			"provider return session = (%v,present=%t,companion=%t), want server/non-companion",
			first.sessionRole,
			first.sessionCompanionPresent,
			first.sessionCompanion,
		)
	}

	var retry providerReturnRetryRecord
	select {
	case retry = <-returnGate.retryHeld:
	case inspectionErr := <-returnGate.inspectionErrs:
		t.Fatalf("inspect provider return retry: %v", inspectionErr)
	case <-time.After(5 * time.Second):
		t.Fatal("provider return was not retried after the forced first drop")
	}
	if !providerReturnRetryIdentityEqual(first, retry) {
		t.Fatalf("provider return retry identity changed: first=%+v retry=%+v", first, retry)
	}
	if !bytes.Equal(first.wireBytes, retry.wireBytes) {
		t.Fatal("provider return retry changed its serialized Transfer record")
	}
	close(returnGate.releaseRetry)

	var delivered delivery
	select {
	case delivered = <-deliveries:
	case deliveryErr := <-deliveryErrs:
		t.Fatalf("receive provider return retry: %v", deliveryErr)
	case inspectionErr := <-returnGate.inspectionErrs:
		t.Fatalf("forward provider return retry: %v", inspectionErr)
	case <-time.After(5 * time.Second):
		t.Fatal("retried provider return was not delivered")
	}
	if delivered.source != SourceId(providerClient.ClientId()) {
		t.Fatalf("provider return callback source = %s, want %s", delivered.source, SourceId(providerClient.ClientId()))
	}
	if !delivered.raw || !bytes.Equal(delivered.packet, response) {
		t.Fatal("provider return retry changed the raw IP packet")
	}
	expectedPeerTransferKey := TransferKey{
		ForceStream:         true,
		CompanionContract:   true,
		EncryptionRole:      protocol.SequenceRole_SequenceRoleClient,
		EncryptionCompanion: false,
	}
	if delivered.peer.TransferKey != expectedPeerTransferKey {
		t.Fatalf("provider return callback key = %#v, want %#v", delivered.peer.TransferKey, expectedPeerTransferKey)
	}

	var ack providerReturnAckRecord
	select {
	case ack = <-heldAck:
	case inspectionErr := <-ackInspectionErrs:
		t.Fatalf("inspect provider return ack: %v", inspectionErr)
	case <-time.After(5 * time.Second):
		t.Fatal("receiver did not acknowledge the retried provider return")
	}
	expectedAckPath := NewTransferPath(peerId, providerClient.ClientId(), Id{})
	if ack.path != expectedAckPath || ack.messageId != first.messageId ||
		ack.sequenceId != first.sequenceId || ack.selective {
		t.Fatalf(
			"provider return ack = (%s,%s,%s,selective=%t), want (%s,%s,%s,selective=false)",
			ack.path,
			ack.messageId,
			ack.sequenceId,
			ack.selective,
			expectedAckPath,
			first.messageId,
			first.sequenceId,
		)
	}
	select {
	case completionErr := <-ackCompletion.completed:
		t.Fatalf("provider return completed before its held ack was released: %v", completionErr)
	default:
	}
	close(releaseAck)
	select {
	case completionErr := <-ackCompletion.completed:
		if completionErr != nil {
			t.Fatalf("provider return ack completion: %v", completionErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("provider return send did not complete after its exact ack was released")
	}

	select {
	case duplicate := <-deliveries:
		t.Fatalf("provider return callback delivered a duplicate: %+v", duplicate)
	default:
	}
	select {
	case inspectionErr := <-returnGate.inspectionErrs:
		t.Fatalf("provider return gate: %v", inspectionErr)
	default:
	}
	select {
	case inspectionErr := <-ackInspectionErrs:
		t.Fatalf("provider return ack gate: %v", inspectionErr)
	default:
	}
}

// A provider has already consumed TCP bytes from the origin socket before it
// hands the reconstructed packet to Transfer. Admission therefore moves the
// only recoverable copy into SendSequence: an end-to-end Ack timeout must keep
// retrying that same item, not close the sequence and report a terminal error.
//
// This is the exact main-proxy failure from 2026-08-30. The origin LB returned
// 200, the hosted DeviceLocal stayed ready, and remote ingress stopped until
// the 30-second request deadline because the provider-return Transfer item hit
// its finite AckTimeout while the H1 carrier was reforming.
func TestRemoteUserNatProviderTcpReturnSurvivesAckTimeout(t *testing.T) {
	var forceAckTimeout atomic.Bool
	clientSettings := DefaultClientSettings()
	clientSettings.EncryptionSettings.Mode = EncryptionModeOff
	clientSettings.SendBufferSettings.MinResendInterval = 5 * time.Millisecond
	clientSettings.SendBufferSettings.RttMinResendInterval = 5 * time.Millisecond
	clientSettings.SendBufferSettings.MaxResendInterval = 10 * time.Millisecond
	clientSettings.SendBufferSettings.AckTimeout = time.Hour
	clientSettings.SendBufferSettings.IdleTimeout = time.Hour
	clientSettings.SendBufferSettings.forceAckTimeoutForTest = func(sendSequenceId) bool {
		return forceAckTimeout.Load()
	}
	provider, providerClient, _ := newProviderTransferKeyTestFixtureWithClientSettings(
		t,
		DefaultRemoteUserNatProviderSettings(),
		clientSettings,
	)

	peerId := NewId()
	providerClient.ContractManager().AddNoContractPeer(peerId)
	providerToDrop := make(Route)
	providerClient.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(peerId)),
		[]Route{providerToDrop},
	)

	response := craftSecurityPacket(
		IpProtocolTcp,
		net.ParseIP("203.0.113.7"),
		8080,
		net.ParseIP("10.0.0.9"),
		42001,
		false,
		[]byte("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"),
	)
	responsePath, err := ParseIpPath(response)
	if err != nil {
		t.Fatalf("parse provider TCP return: %v", err)
	}
	transferKey := TransferKey{
		ForceStream:         true,
		EncryptionRole:      protocol.SequenceRole_SequenceRoleServer,
		EncryptionCompanion: false,
	}
	completion := &providerReturnTestAckTarget{completed: make(chan error, 1)}
	provider.returnAckTargetForTest = completion

	provider.receiveTransferWithRecovery(
		SourceId(peerId),
		transferKey,
		protocol.ProvideMode_Public,
		receiveRecoveryModeTcpSocket,
		responsePath,
		response,
	)

	waitWrite := func(name string) providerReturnRetryRecord {
		t.Helper()
		select {
		case transferFrameBytes := <-providerToDrop:
			defer MessagePoolReturn(transferFrameBytes)
			record, target, decodeErr := providerReturnRetryRecordFromWire(
				transferFrameBytes,
				response,
			)
			if decodeErr != nil {
				t.Fatalf("decode %s provider return: %v", name, decodeErr)
			}
			if !target {
				t.Fatalf("%s write did not contain the provider TCP return", name)
			}
			return record
		case completionErr := <-completion.completed:
			t.Fatalf("provider return completed before %s write: %v", name, completionErr)
			return providerReturnRetryRecord{}
		case <-time.After(time.Second):
			t.Fatalf("wait for %s provider return write", name)
			return providerReturnRetryRecord{}
		}
	}

	first := waitWrite("initial")
	forceAckTimeout.Store(true)
	for retryIndex := 1; retryIndex <= 3; retryIndex++ {
		retry := waitWrite(fmt.Sprintf("post-timeout retry %d", retryIndex))
		if !providerReturnRetryIdentityEqual(first, retry) ||
			!bytes.Equal(first.wireBytes, retry.wireBytes) {
			t.Fatalf(
				"post-timeout retry %d changed provider return identity: first=%+v retry=%+v",
				retryIndex,
				first,
				retry,
			)
		}
		select {
		case completionErr := <-completion.completed:
			t.Fatalf(
				"provider return became terminal after post-timeout retry %d: %v",
				retryIndex,
				completionErr,
			)
		default:
		}
	}
	provider.Close()
	select {
	case completionErr := <-completion.completed:
		if completionErr == nil {
			t.Fatal("provider shutdown acknowledged an undelivered retained return")
		}
	case <-time.After(time.Second):
		t.Fatal("provider shutdown did not release its retained return")
	}
}
