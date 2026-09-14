// Provider callback regressions pin nonblocking datagram handoff, lossless
// per-flow TCP return, and receiver-visible reply metadata.
package connect

import (
	"bytes"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// waitProviderCallbackReturn requires a callback to finish without inheriting
// a downstream queue's configured blocking timeout.
func waitProviderCallbackReturn(t *testing.T, callback func()) {
	t.Helper()
	returned := make(chan struct{})
	go func() {
		callback()
		close(returned)
	}()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-returned:
	case <-timer.C:
		t.Fatal("provider receive callback blocked on downstream congestion")
	}
}

// Waits for one exact provider queue admission decision.
func waitProviderReturnEnqueueResult(t *testing.T, results <-chan bool, want bool) {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case got := <-results:
		if got != want {
			t.Fatalf("provider return queue admission=%t, want %t", got, want)
		}
	case <-timer.C:
		t.Fatal("provider return queue admission was not observed")
	}
}

// Waits for one exact provider sender completion.
func waitProviderReturnSendCompletion(
	t *testing.T,
	results <-chan providerReturnSendResult,
) providerReturnSendResult {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case result := <-results:
		return result
	case <-timer.C:
		t.Fatal("provider return sender completion was not observed")
		return providerReturnSendResult{}
	}
}

// Waits for one completion and verifies its exact accounting outcome.
func waitProviderReturnSendResult(
	t *testing.T,
	results <-chan providerReturnSendResult,
	wantSent bool,
	wantPacketCount int,
	wantPacketByteCount ByteCount,
) providerReturnSendResult {
	t.Helper()
	result := waitProviderReturnSendCompletion(t, results)
	if result.sent != wantSent ||
		result.packetCount != wantPacketCount ||
		result.packetByteCount != wantPacketByteCount {
		t.Fatalf(
			"provider return result=%+v, want sent/count/bytes=%t/%d/%d",
			result,
			wantSent,
			wantPacketCount,
			wantPacketByteCount,
		)
	}
	return result
}

// Waits for one explicit provider lifecycle barrier. The timer is only a
// liveness bound; the named state transition is the correctness witness.
func waitProviderReturnBarrier(t *testing.T, barrier <-chan struct{}, name string) {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-barrier:
	case <-timer.C:
		t.Fatalf("provider return did not reach %s", name)
	}
}

// Requires one raw provider packet and returns its bytes for order checks.
func providerCallbackQueuePacketBytes(t *testing.T, queued *SendPack) []byte {
	t.Helper()
	if queued.Frame == nil || queued.Frames != nil || !queued.Frame.Raw {
		t.Fatalf("provider return pack = %#v, want one raw frame", queued)
	}
	packetBytes, err := ipPacketFromProviderBytes(queued.Frame)
	if err != nil {
		t.Fatalf("decode provider return: %v", err)
	}
	return packetBytes
}

// The exported provider snapshot reports every congestion boundary without
// folding capacity loss into security-policy block accounting.
func TestRemoteUserNatProviderCongestionDropStatsSnapshot(t *testing.T) {
	provider := &RemoteUserNatProvider{}
	provider.congestionDrops.addIngressNat(2, 200)
	provider.congestionDrops.addReturnQueue(3, 300)
	provider.congestionDrops.addReturnSend(4, 400)

	first := provider.CongestionDropStats()
	if first != (ProviderCongestionDrops{
		IngressNatPacketCount:  2,
		IngressNatByteCount:    200,
		ReturnQueuePacketCount: 3,
		ReturnQueueByteCount:   300,
		ReturnSendPacketCount:  4,
		ReturnSendByteCount:    400,
	}) {
		t.Fatalf("first provider congestion snapshot=%+v", first)
	}

	provider.congestionDrops.addIngressNat(1, 10)
	second := provider.CongestionDropStats()
	if first.IngressNatPacketCount != 2 || first.IngressNatByteCount != 200 {
		t.Fatalf("first provider congestion snapshot changed after a later update: %+v", first)
	}
	if second.IngressNatPacketCount != 3 || second.IngressNatByteCount != 210 {
		t.Fatalf("second provider congestion snapshot=%+v", second)
	}
}

// TestRemoteUserNatProviderClientReceiveUsesNonblockingNatHandoff verifies
// that a full NAT input queue cannot delay the transfer receive sequence.
func TestRemoteUserNatProviderClientReceiveUsesNonblockingNatHandoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	defer client.Cancel()
	localUserNat := &LocalUserNat{
		ctx:         ctx,
		sendPackets: make(chan *SendPacket),
	}
	settings := DefaultRemoteUserNatProviderSettings()
	settings.IngressDispatchTimeout = time.Hour
	provider := &RemoteUserNatProvider{
		ctx:                      ctx,
		client:                   client,
		localUserNat:             localUserNat,
		securityPolicy:           settings.SecurityPolicyGenerator(ctx, DefaultSecurityPolicyStatsCollector()),
		settings:                 settings,
		packetStatsCounters:      &packetStatsCounters{},
		sourceProvideMode:        map[Id]protocol.ProvideMode{},
		sourceP2pPriorityRefresh: map[Id]time.Time{},
	}
	source := SourceId(NewId())
	packet := providerTransferKeyTestPacket()
	frame, err := ipPacketToProviderFrame(packet, DefaultProtocolVersion)
	if err != nil {
		t.Fatalf("build provider ingress frame: %v", err)
	}
	defer MessagePoolReturn(frame.MessageBytes)

	waitProviderCallbackReturn(t, func() {
		provider.ClientReceive(
			source,
			[]*protocol.Frame{frame},
			Peer{ProvideMode: protocol.ProvideMode_Public},
		)
	})
	select {
	case queued := <-localUserNat.sendPackets:
		queued.finish()
		for _, queuedPacket := range queued.packets {
			MessagePoolReturn(queuedPacket)
		}
		t.Fatal("full NAT input queue unexpectedly accepted the packet")
	default:
	}
	drops := provider.CongestionDropStats()
	if drops.IngressNatPacketCount != 1 || drops.IngressNatByteCount != ByteCount(len(packet)) {
		t.Fatalf("provider NAT admission drops = %+v, want one packet", drops)
	}
	if drops.ReturnQueuePacketCount != 0 || drops.ReturnSendPacketCount != 0 {
		t.Fatalf("provider NAT rejection changed return drops: %+v", drops)
	}
	stats := provider.PacketStats()
	if stats.BlockIngressPacketCount != 0 || stats.BlockEgressPacketCount != 0 {
		t.Fatalf("provider congestion changed security block stats: %+v", stats)
	}
}

// TestRemoteUserNatProviderUdpReturnCallbackDoesNotWaitForClientSend verifies
// that a datagram callback never inherits the provider's long send timeout.
func TestRemoteUserNatProviderUdpReturnCallbackDoesNotWaitForClientSend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	defer client.Cancel()
	localUserNat := NewLocalUserNatWithDefaults(ctx, "provider-callback")
	settings := DefaultRemoteUserNatProviderSettings()
	settings.WriteTimeout = time.Hour
	provider := NewRemoteUserNatProvider(client, localUserNat, settings)
	defer provider.Close()

	peerId := NewId()
	source := SourceId(peerId)
	transferKey := TransferKey{}
	sequenceId := sendSequenceId{
		Destination:       peerId,
		CompanionContract: true,
	}
	sequence := installProviderReturnTestSequence(t, provider, client, sequenceId)
	sequence.packs = make(chan *SendPack)

	packet := craftSecurityPacket(
		IpProtocolUdp,
		net.ParseIP("203.0.113.7"),
		8080,
		net.ParseIP("10.0.0.9"),
		42001,
		false,
		[]byte("HTTP/1.1 200 OK\r\n\r\n"),
	)
	ipPath, err := ParseIpPath(packet)
	if err != nil {
		t.Fatalf("parse provider return packet: %v", err)
	}
	waitProviderCallbackReturn(t, func() {
		provider.receiveTransfer(
			source,
			transferKey,
			protocol.ProvideMode_Public,
			ipPath,
			packet,
		)
	})
}

// A datagram sender worker is still shared across unrelated flows after the
// receive callback hands ownership to its bounded queue. It must therefore use
// a zero-wait Transfer admission even when the provider's reliable-socket
// timeout is long.
func TestRemoteUserNatProviderUdpReturnWorkerDoesNotWaitForClientSend(t *testing.T) {
	settings := DefaultRemoteUserNatProviderSettings()
	settings.WriteTimeout = time.Hour
	settings.ReturnSendWorkerCount = 1
	settings.ReturnSendQueueSize = 1
	provider, client, _ := newProviderTransferKeyTestFixtureWithSettings(t, settings)
	peerId := NewId()
	sequence := installProviderReturnTestSequence(t, provider, client, sendSequenceId{
		Destination:       peerId,
		CompanionContract: true,
	})
	sequence.packs = make(chan *SendPack)
	sendResults := make(chan providerReturnSendResult, 1)
	provider.afterReturnSendForTest = func(result providerReturnSendResult) {
		sendResults <- result
	}
	packet := MessagePoolCopy(craftSecurityPacket(
		IpProtocolUdp,
		net.ParseIP("203.0.113.7"),
		8080,
		net.ParseIP("10.0.0.9"),
		42001,
		false,
		[]byte("nonblocking worker"),
	))
	packetByteCount := ByteCount(len(packet))
	ipPath, err := ParseIpPath(packet)
	if err != nil {
		MessagePoolReturn(packet)
		t.Fatalf("parse provider return packet: %v", err)
	}
	provider.Receive(
		SourceId(peerId),
		protocol.ProvideMode_Public,
		ipPath,
		packet,
	)
	waitProviderReturnSendResult(t, sendResults, false, 1, packetByteCount)
	if !MessagePoolReturn(packet) {
		t.Fatal("rejected datagram return retained its borrowed packet share")
	}
	drops := provider.CongestionDropStats()
	if drops.ReturnSendPacketCount != 1 || drops.ReturnSendByteCount != packetByteCount {
		t.Fatalf("datagram send drops=%+v, want one rejected packet", drops)
	}
}

// A dedicated socket reader retains its consumed bytes across failed attempts,
// and synthesized control can be regenerated after a shared sender's zero-wait
// refusal. An arbitrary public callback makes neither ownership promise.
func TestRemoteUserNatProviderReturnRecoveryOptionClassifiesOwnedRecovery(t *testing.T) {
	provider := &RemoteUserNatProvider{}
	for _, testCase := range []struct {
		name            string
		recoveryMode    receiveRecoveryMode
		wantRecoverable bool
	}{
		{
			name:         "public callback",
			recoveryMode: receiveRecoveryModeNonblocking,
		},
		{
			name:            "dedicated socket",
			recoveryMode:    receiveRecoveryModeTcpSocket,
			wantRecoverable: true,
		},
		{
			name:            "regenerable control",
			recoveryMode:    receiveRecoveryModeRegenerableControl,
			wantRecoverable: true,
		},
	} {
		option := provider.returnSendRecoveryOption(&providerReturnItem{
			recoveryMode: testCase.recoveryMode,
		})
		if option.upstreamRecoverable != testCase.wantRecoverable {
			t.Fatalf(
				"%s recovery option=%t, want %t",
				testCase.name,
				option.upstreamRecoverable,
				testCase.wantRecoverable,
			)
		}
	}
}

// Exported/public TCP callbacks have no dedicated socket reader that can
// recover consumed bytes. They therefore return after one nonblocking queue
// admission and record a rejected downstream send without retrying.
func TestRemoteUserNatProviderPublicTcpReturnCallbackDoesNotRetry(t *testing.T) {
	settings := DefaultRemoteUserNatProviderSettings()
	settings.WriteTimeout = 0
	settings.ReturnSendWorkerCount = 1
	settings.ReturnSendQueueSize = 1
	provider, client, _ := newProviderTransferKeyTestFixtureWithSettings(t, settings)
	peerId := NewId()
	sequence := installProviderReturnTestSequence(t, provider, client, sendSequenceId{
		Destination:       peerId,
		CompanionContract: true,
	})
	sequence.packs = make(chan *SendPack)
	enqueueResults := make(chan bool, 1)
	provider.afterReturnEnqueueForTest = func(queued bool) {
		enqueueResults <- queued
	}
	sendResults := make(chan providerReturnSendResult, 1)
	provider.afterReturnSendForTest = func(result providerReturnSendResult) {
		sendResults <- result
	}
	returnReleased := make(chan struct{}, 1)
	provider.afterReturnReleaseForTest = func() {
		returnReleased <- struct{}{}
	}
	unexpectedRetry := make(chan struct{}, 1)
	provider.beforeTcpReturnSendRetryForTest = func() {
		select {
		case unexpectedRetry <- struct{}{}:
		default:
		}
	}
	packet := MessagePoolCopy(craftSecurityPacket(
		IpProtocolTcp,
		net.ParseIP("203.0.113.7"),
		8080,
		net.ParseIP("10.0.0.9"),
		42001,
		false,
		[]byte("public callback"),
	))
	packetByteCount := ByteCount(len(packet))
	ipPath, err := ParseIpPath(packet)
	if err != nil {
		MessagePoolReturn(packet)
		t.Fatalf("parse public TCP return packet: %v", err)
	}
	waitProviderCallbackReturn(t, func() {
		provider.Receive(
			SourceId(peerId),
			protocol.ProvideMode_Public,
			ipPath,
			packet,
		)
	})
	waitProviderReturnEnqueueResult(t, enqueueResults, true)
	waitProviderReturnSendResult(t, sendResults, false, 1, packetByteCount)
	waitProviderReturnBarrier(t, returnReleased, "public TCP return ownership release")
	select {
	case <-unexpectedRetry:
		t.Fatal("public TCP callback entered socket-owned recovery")
	default:
	}
	if !MessagePoolReturn(packet) {
		t.Fatal("public TCP callback retained its borrowed packet share")
	}
	drops := provider.CongestionDropStats()
	if drops.ReturnSendPacketCount != 1 || drops.ReturnSendByteCount != packetByteCount {
		t.Fatalf("public TCP send drops=%+v, want one rejected packet", drops)
	}
	if drops.ReturnQueuePacketCount != 0 || drops.IngressNatPacketCount != 0 {
		t.Fatalf("public TCP send rejection changed unrelated drops: %+v", drops)
	}
}

// TestRemoteUserNatProviderUdpReturnQueueIsBoundedAndOrdered deterministically
// fills the sole sender shard, verifies datagram overflow drops without
// blocking, and pins ownership across successful and rejected handoffs.
func TestRemoteUserNatProviderUdpReturnQueueIsBoundedAndOrdered(t *testing.T) {
	settings := DefaultRemoteUserNatProviderSettings()
	settings.WriteTimeout = time.Hour
	settings.ReturnSendWorkerCount = 1
	settings.ReturnSendQueueSize = 1
	provider, client, _ := newProviderTransferKeyTestFixtureWithSettings(t, settings)
	peerId := NewId()
	source := SourceId(peerId)
	transferKey := TransferKey{
		ForceStream:         true,
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
	enqueueResults := make(chan bool, 3)
	provider.afterReturnEnqueueForTest = func(queued bool) {
		enqueueResults <- queued
	}
	sendResults := make(chan providerReturnSendResult, 2)
	provider.afterReturnSendForTest = func(result providerReturnSendResult) {
		sendResults <- result
	}
	started := make(chan struct{}, 3)
	releaseFirst := make(chan struct{})
	var releaseFirstOnce sync.Once
	t.Cleanup(func() {
		releaseFirstOnce.Do(func() {
			close(releaseFirst)
		})
	})
	var blockFirst sync.Once
	provider.returnSendStarted = func() {
		select {
		case started <- struct{}{}:
		default:
		}
		blockFirst.Do(func() {
			<-releaseFirst
		})
	}

	packets := make([][]byte, 3)
	expectedPackets := make([][]byte, len(packets))
	for packetIndex := range packets {
		packets[packetIndex] = MessagePoolCopy(craftSecurityPacket(
			IpProtocolUdp,
			net.ParseIP("203.0.113.7"),
			8080,
			net.ParseIP("10.0.0.9"),
			42001,
			false,
			[]byte{byte(packetIndex + 1)},
		))
		expectedPackets[packetIndex] = append([]byte(nil), packets[packetIndex]...)
	}
	ipPath, err := ParseIpPath(packets[0])
	if err != nil {
		t.Fatalf("parse provider return packet: %v", err)
	}

	waitProviderCallbackReturn(t, func() {
		provider.receiveTransfer(source, transferKey, protocol.ProvideMode_Public, ipPath, packets[0])
	})
	waitProviderReturnEnqueueResult(t, enqueueResults, true)
	if pooled, shared := MessagePoolCheck(packets[0]); !pooled || !shared {
		t.Fatalf("active return packet ownership = (%t,%t), want pooled/shared", pooled, shared)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("provider return worker did not start")
	}

	waitProviderCallbackReturn(t, func() {
		provider.receiveTransfer(source, transferKey, protocol.ProvideMode_Public, ipPath, packets[1])
	})
	waitProviderReturnEnqueueResult(t, enqueueResults, true)
	if pooled, shared := MessagePoolCheck(packets[1]); !pooled || !shared {
		t.Fatalf("queued return packet ownership = (%t,%t), want pooled/shared", pooled, shared)
	}
	if queueSize := len(provider.returnSendQueues[0]); queueSize != 1 {
		t.Fatalf("provider sender queue size = %d, want 1", queueSize)
	}

	waitProviderCallbackReturn(t, func() {
		provider.receiveTransfer(source, transferKey, protocol.ProvideMode_Public, ipPath, packets[2])
	})
	waitProviderReturnEnqueueResult(t, enqueueResults, false)
	if !MessagePoolReturn(packets[2]) {
		t.Fatal("dropped return packet retained its borrowed queue reference")
	}
	if queueSize := len(provider.returnSendQueues[0]); queueSize != 1 {
		t.Fatalf("overflow changed provider sender queue size to %d, want 1", queueSize)
	}

	sequence.packs = make(chan *SendPack, 1)
	releaseFirstOnce.Do(func() {
		close(releaseFirst)
	})
	first := waitProviderReturnTestPack(t, sequence)
	if !bytes.Equal(providerCallbackQueuePacketBytes(t, first), expectedPackets[0]) {
		t.Fatal("provider changed the first queued packet")
	}
	first.returnFrames()
	waitProviderReturnSendResult(
		t,
		sendResults,
		true,
		1,
		ByteCount(len(expectedPackets[0])),
	)
	if !MessagePoolReturn(packets[0]) {
		t.Fatal("first return packet retained its sender reference")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("provider return worker did not preserve the queued successor")
	}
	second := waitProviderReturnTestPack(t, sequence)
	if !bytes.Equal(providerCallbackQueuePacketBytes(t, second), expectedPackets[1]) {
		t.Fatal("provider return queue changed per-flow order")
	}
	second.returnFrames()
	waitProviderReturnSendResult(
		t,
		sendResults,
		true,
		1,
		ByteCount(len(expectedPackets[1])),
	)
	if !MessagePoolReturn(packets[1]) {
		t.Fatal("second return packet retained its sender reference")
	}

	stats := provider.PacketStats()
	if stats.RemoteEgressPacketCount != 2 ||
		stats.RemoteEgressByteCount != ByteCount(len(expectedPackets[0])+len(expectedPackets[1])) {
		t.Fatalf("provider return stats = %+v, want two packets", stats)
	}
	drops := provider.CongestionDropStats()
	if drops.ReturnQueuePacketCount != 1 ||
		drops.ReturnQueueByteCount != ByteCount(len(expectedPackets[2])) {
		t.Fatalf("provider return queue drops = %+v, want the overflow packet", drops)
	}
	if drops.IngressNatPacketCount != 0 || drops.ReturnSendPacketCount != 0 {
		t.Fatalf("provider return overflow changed unrelated drops: %+v", drops)
	}
	stats = provider.PacketStats()
	if stats.BlockIngressPacketCount != 0 || stats.BlockEgressPacketCount != 0 {
		t.Fatalf("provider return congestion changed security block stats: %+v", stats)
	}
}

// A synthesized orphan reset originates on the shared local-NAT dispatch
// shard, so a blocked downstream return worker must not park the next packet
// on that shard. The explicit disposition edges also pin transfer-key
// threading, packet ownership, and congestion attribution.
func TestRemoteUserNatProviderOrphanRstDoesNotBlockLocalNatShard(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	defer client.Cancel()
	localSettings := DefaultLocalUserNatSettings()
	localSettings.SendShardCount = 1
	localSettings.TcpBufferSettings.EnableOrphanRst = true
	localSettings.TcpBufferSettings.OrphanRstPerSecond = 0
	localUserNat := NewLocalUserNat(ctx, "orphan-rst-dispatch", localSettings)
	defer localUserNat.Close()
	providerSettings := DefaultRemoteUserNatProviderSettings()
	providerSettings.WriteTimeout = 0
	providerSettings.ReturnSendWorkerCount = 1
	providerSettings.ReturnSendQueueSize = 1
	providerSettings.SecurityPolicyGenerator = DisableSecurityPolicyWithStats
	provider := NewRemoteUserNatProvider(client, localUserNat, providerSettings)
	defer provider.Close()

	peerId := NewId()
	source := SourceId(peerId)
	transferKey := TransferKey{
		ForceStream:         true,
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

	returnStarted := make(chan struct{})
	releaseReturn := make(chan struct{})
	var releaseReturnOnce sync.Once
	t.Cleanup(func() {
		releaseReturnOnce.Do(func() {
			close(releaseReturn)
		})
	})
	var firstReturn sync.Once
	provider.returnSendStarted = func() {
		firstReturn.Do(func() {
			close(returnStarted)
			<-releaseReturn
		})
	}
	enqueueResults := make(chan bool, 2)
	provider.afterReturnEnqueueForTest = func(queued bool) {
		enqueueResults <- queued
	}
	sendResults := make(chan providerReturnSendResult, 2)
	provider.afterReturnSendForTest = func(result providerReturnSendResult) {
		sendResults <- result
	}
	returnReleases := make(chan struct{}, 2)
	provider.afterReturnReleaseForTest = func() {
		returnReleases <- struct{}{}
	}
	unexpectedRetry := make(chan struct{}, 1)
	provider.beforeTcpReturnSendRetryForTest = func() {
		select {
		case unexpectedRetry <- struct{}{}:
		default:
		}
	}

	type orphanRstObservation struct {
		transferKey  TransferKey
		recoveryMode receiveRecoveryMode
		packet       []byte
	}
	orphanRsts := make(chan orphanRstObservation, 2)
	unsub := localUserNat.addReceiveTransferPacketCallback(func(
		_ TransferPath,
		callbackTransferKey TransferKey,
		_ protocol.ProvideMode,
		recoveryMode receiveRecoveryMode,
		_ *IpPath,
		packet []byte,
	) {
		orphanRsts <- orphanRstObservation{
			transferKey:  callbackTransferKey,
			recoveryMode: recoveryMode,
			packet:       MessagePoolShareReadOnly(packet),
		}
	})
	defer unsub()
	packetDispositions := make(chan struct{}, 2)
	localUserNat.afterSendPacketForTest = func() {
		packetDispositions <- struct{}{}
	}

	packetWitnesses := make([][]byte, 2)
	for packetIndex := range 2 {
		packet := MessagePoolCopy(craftSecurityPacket(
			IpProtocolTcp,
			net.ParseIP("10.0.0.9"),
			42001+packetIndex,
			net.ParseIP("203.0.113.7"),
			8080,
			false,
			[]byte{byte(packetIndex + 1)},
		))
		packetWitnesses[packetIndex] = MessagePoolShareReadOnly(packet)
		if !localUserNat.sendTransferPacketsWithTimeout(
			source,
			transferKey,
			protocol.ProvideMode_Public,
			[][]byte{packet},
			-1,
		) {
			MessagePoolReturn(packet)
			t.Fatalf("local NAT rejected orphan packet %d", packetIndex)
		}
		if packetIndex == 0 {
			waitProviderReturnBarrier(t, returnStarted, "blocked orphan RST return worker")
		}
	}
	waitProviderReturnBarrier(t, packetDispositions, "first orphan packet disposition")
	waitProviderReturnBarrier(t, packetDispositions, "second same-shard packet disposition")
	waitProviderReturnEnqueueResult(t, enqueueResults, true)
	waitProviderReturnEnqueueResult(t, enqueueResults, true)
	for packetIndex, witness := range packetWitnesses {
		if !MessagePoolReturn(witness) {
			t.Fatalf("orphan input packet %d retained dispatch ownership", packetIndex)
		}
	}

	releaseReturnOnce.Do(func() {
		close(releaseReturn)
	})
	observations := make([]orphanRstObservation, 0, 2)
	var returnByteCount ByteCount
	for range 2 {
		result := waitProviderReturnSendCompletion(t, sendResults)
		if result.sent || result.packetCount != 1 || result.packetByteCount <= 0 {
			t.Fatalf("orphan RST return result=%+v, want one rejected packet", result)
		}
		returnByteCount += result.packetByteCount
		observation := <-orphanRsts
		observations = append(observations, observation)
	}
	waitProviderReturnBarrier(t, returnReleases, "first orphan RST ownership release")
	waitProviderReturnBarrier(t, returnReleases, "second orphan RST ownership release")
	select {
	case <-unexpectedRetry:
		t.Fatal("shared orphan RST entered synchronous TCP recovery")
	default:
	}
	for observationIndex, observation := range observations {
		if observation.transferKey != transferKey {
			t.Fatalf(
				"orphan RST %d transfer key=%+v, want %+v",
				observationIndex,
				observation.transferKey,
				transferKey,
			)
		}
		if observation.recoveryMode != receiveRecoveryModeRegenerableControl {
			t.Fatalf(
				"orphan RST %d recovery mode=%d, want regenerable control",
				observationIndex,
				observation.recoveryMode,
			)
		}
		if !MessagePoolReturn(observation.packet) {
			t.Fatalf("orphan RST %d retained return ownership", observationIndex)
		}
	}
	drops := provider.CongestionDropStats()
	if drops.ReturnSendPacketCount != 2 || drops.ReturnSendByteCount != returnByteCount {
		t.Fatalf("orphan RST send drops=%+v, want two rejected packets/%d bytes", drops, returnByteCount)
	}
	if drops.IngressNatPacketCount != 0 || drops.ReturnQueuePacketCount != 0 {
		t.Fatalf("orphan RST send rejection changed unrelated drops: %+v", drops)
	}
	stats := provider.PacketStats()
	if stats.RemoteEgressPacketCount != 0 ||
		stats.BlockIngressPacketCount != 0 ||
		stats.BlockEgressPacketCount != 0 {
		t.Fatalf("orphan RST congestion changed delivery/security stats: %+v", stats)
	}
}

// One local-NAT TCP flow keeps its callback and packet ownership until
// Transfer accepts the data. A three-packet producer therefore cannot outrun
// a one-item shared queue and lose its tail; after the explicit retry release,
// every packet enters Transfer in exact callback order.
func TestRemoteUserNatProviderTcpReturnCallbackPreservesFlowUntilTransferAccepts(t *testing.T) {
	settings := DefaultRemoteUserNatProviderSettings()
	settings.WriteTimeout = 0
	settings.ReturnSendRetryTimeout = time.Millisecond
	settings.ReturnSendWorkerCount = 1
	settings.ReturnSendQueueSize = 1
	provider, client, _ := newProviderTransferKeyTestFixtureWithSettings(t, settings)
	peerId := NewId()
	source := SourceId(peerId)
	transferKey := TransferKey{
		ForceStream:         true,
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
	sendResults := make(chan providerReturnSendResult, 3)
	provider.afterReturnSendForTest = func(result providerReturnSendResult) {
		sendResults <- result
	}
	attemptResults := make(chan providerReturnSendResult, 4)
	provider.afterReturnSendAttemptForTest = func(result providerReturnSendResult) {
		attemptResults <- result
	}
	retryEntered := make(chan struct{}, 1)
	releaseRetry := make(chan struct{})
	var firstRetry sync.Once
	provider.beforeTcpReturnSendRetryForTest = func() {
		firstRetry.Do(func() {
			close(retryEntered)
			<-releaseRetry
		})
	}

	packets := make([][]byte, 3)
	expectedPackets := make([][]byte, len(packets))
	for packetIndex := range packets {
		packets[packetIndex] = MessagePoolCopy(craftSecurityPacket(
			IpProtocolTcp,
			net.ParseIP("203.0.113.7"),
			8080,
			net.ParseIP("10.0.0.9"),
			42001,
			false,
			[]byte{byte(packetIndex + 1)},
		))
		expectedPackets[packetIndex] = bytes.Clone(packets[packetIndex])
	}
	ipPath, err := ParseIpPath(packets[0])
	if err != nil {
		t.Fatalf("parse provider TCP return packet: %v", err)
	}

	producerReturned := make(chan struct{})
	go func() {
		for _, packet := range packets {
			provider.receiveTransferWithRecovery(
				source,
				transferKey,
				protocol.ProvideMode_Public,
				receiveRecoveryModeTcpSocket,
				ipPath,
				packet,
			)
		}
		close(producerReturned)
	}()
	waitProviderReturnSendResult(t, attemptResults, false, 1, ByteCount(len(packets[0])))
	waitProviderReturnBarrier(t, retryEntered, "first TCP Transfer retry")
	sequence.packs = make(chan *SendPack, len(packets))
	close(releaseRetry)

	for packetIndex, expectedPacket := range expectedPackets {
		waitProviderReturnSendResult(
			t,
			attemptResults,
			true,
			1,
			ByteCount(len(expectedPacket)),
		)
		queued := waitProviderReturnTestPack(t, sequence)
		if !bytes.Equal(providerCallbackQueuePacketBytes(t, queued), expectedPacket) {
			t.Fatalf("provider changed or reordered TCP return packet %d", packetIndex)
		}
		queued.returnFrames()
		waitProviderReturnSendResult(
			t,
			sendResults,
			true,
			1,
			ByteCount(len(expectedPacket)),
		)
	}
	waitProviderReturnBarrier(t, producerReturned, "ordered TCP callback completion")
	for packetIndex, packet := range packets {
		if !MessagePoolReturn(packet) {
			t.Fatalf("TCP return packet %d retained its sender share", packetIndex)
		}
	}
	if drops := provider.CongestionDropStats(); drops.ReturnQueuePacketCount != 0 ||
		drops.ReturnQueueByteCount != 0 || drops.ReturnSendPacketCount != 0 {
		t.Fatalf("lossless TCP return recorded congestion loss: %+v", drops)
	}
	stats := provider.PacketStats()
	if stats.RemoteEgressPacketCount != 3 {
		t.Fatalf("provider TCP return stats=%+v, want three packets", stats)
	}
}

// A rejected TCP destination cannot park a shared sender shard. Both peers
// would hash to the sole configured shard in the old design, yet the second
// flow reaches Transfer while the first is held at its exact retry barrier.
func TestRemoteUserNatProviderTcpRetryDoesNotBlockUnrelatedFlow(t *testing.T) {
	settings := DefaultRemoteUserNatProviderSettings()
	settings.WriteTimeout = 0
	settings.ReturnSendRetryTimeout = time.Millisecond
	settings.ReturnSendWorkerCount = 1
	settings.ReturnSendQueueSize = 1
	provider, client, _ := newProviderTransferKeyTestFixtureWithSettings(t, settings)
	firstPeerId := NewId()
	secondPeerId := NewId()
	firstSequence := installProviderReturnTestSequence(t, provider, client, sendSequenceId{
		Destination:       firstPeerId,
		CompanionContract: true,
	})
	firstSequence.packs = make(chan *SendPack)
	secondSequence := installProviderReturnTestSequence(t, provider, client, sendSequenceId{
		Destination:       secondPeerId,
		CompanionContract: true,
	})
	secondSequence.packs = make(chan *SendPack, 1)
	retryEntered := make(chan struct{})
	releaseRetry := make(chan struct{})
	var firstRetry sync.Once
	provider.beforeTcpReturnSendRetryForTest = func() {
		firstRetry.Do(func() {
			close(retryEntered)
			<-releaseRetry
		})
	}
	firstPacket := MessagePoolCopy(craftSecurityPacket(
		IpProtocolTcp,
		net.ParseIP("203.0.113.7"),
		8080,
		net.ParseIP("10.0.0.9"),
		42001,
		false,
		[]byte("blocked destination"),
	))
	secondPacket := MessagePoolCopy(craftSecurityPacket(
		IpProtocolTcp,
		net.ParseIP("203.0.113.8"),
		8081,
		net.ParseIP("10.0.0.10"),
		42002,
		false,
		[]byte("independent destination"),
	))
	firstExpected := bytes.Clone(firstPacket)
	secondExpected := bytes.Clone(secondPacket)
	firstPath, err := ParseIpPath(firstPacket)
	if err != nil {
		t.Fatalf("parse first provider TCP return packet: %v", err)
	}
	secondPath, err := ParseIpPath(secondPacket)
	if err != nil {
		t.Fatalf("parse second provider TCP return packet: %v", err)
	}
	firstReturned := make(chan struct{})
	go func() {
		provider.receiveTransferWithRecovery(
			SourceId(firstPeerId),
			TransferKey{},
			protocol.ProvideMode_Public,
			receiveRecoveryModeTcpSocket,
			firstPath,
			firstPacket,
		)
		close(firstReturned)
	}()
	waitProviderReturnBarrier(t, retryEntered, "first destination TCP retry")

	secondReturned := make(chan struct{})
	go func() {
		provider.receiveTransferWithRecovery(
			SourceId(secondPeerId),
			TransferKey{},
			protocol.ProvideMode_Public,
			receiveRecoveryModeTcpSocket,
			secondPath,
			secondPacket,
		)
		close(secondReturned)
	}()
	secondQueued := waitProviderReturnTestPack(t, secondSequence)
	if !bytes.Equal(providerCallbackQueuePacketBytes(t, secondQueued), secondExpected) {
		t.Fatal("unrelated TCP flow changed while the first destination retried")
	}
	secondQueued.returnFrames()
	waitProviderReturnBarrier(t, secondReturned, "unrelated TCP callback completion")

	firstSequence.packs = make(chan *SendPack, 1)
	close(releaseRetry)
	firstQueued := waitProviderReturnTestPack(t, firstSequence)
	if !bytes.Equal(providerCallbackQueuePacketBytes(t, firstQueued), firstExpected) {
		t.Fatal("retried TCP flow changed after the unrelated flow completed")
	}
	firstQueued.returnFrames()
	waitProviderReturnBarrier(t, firstReturned, "first TCP callback completion")
	if !MessagePoolReturn(firstPacket) || !MessagePoolReturn(secondPacket) {
		t.Fatal("independent TCP callbacks retained a sender packet share")
	}
	if drops := provider.CongestionDropStats(); drops.ReturnQueuePacketCount != 0 ||
		drops.ReturnSendPacketCount != 0 {
		t.Fatalf("independent TCP callbacks recorded congestion loss: %+v", drops)
	}
}

// Closing a provider cancels an exact in-flight TCP Transfer retry and waits
// for its callback to release the retained packet before returning.
func TestRemoteUserNatProviderCloseInterruptsTcpReturnRetry(t *testing.T) {
	settings := DefaultRemoteUserNatProviderSettings()
	settings.WriteTimeout = 0
	settings.ReturnSendRetryTimeout = time.Hour
	settings.ReturnSendWorkerCount = 1
	settings.ReturnSendQueueSize = 1
	provider, client, _ := newProviderTransferKeyTestFixtureWithSettings(t, settings)
	peerId := NewId()
	sequence := installProviderReturnTestSequence(t, provider, client, sendSequenceId{
		Destination:       peerId,
		CompanionContract: true,
	})
	sequence.packs = make(chan *SendPack)
	retryEntered := make(chan struct{}, 1)
	provider.beforeTcpReturnSendRetryForTest = func() {
		select {
		case retryEntered <- struct{}{}:
		default:
		}
	}
	releaseEntered := make(chan struct{}, 1)
	admissionsClosed := make(chan struct{}, 1)
	provider.beforeReturnAdmissionsWaitForTest = func() {
		select {
		case admissionsClosed <- struct{}{}:
		default:
		}
	}
	released := make(chan struct{}, 1)
	provider.afterReturnReleaseForTest = func() {
		select {
		case released <- struct{}{}:
		default:
		}
	}
	releaseReturn := make(chan struct{})
	var releaseReturnOnce sync.Once
	t.Cleanup(func() {
		releaseReturnOnce.Do(func() {
			close(releaseReturn)
		})
	})
	provider.beforeTcpReturnReleaseForTest = func() {
		select {
		case releaseEntered <- struct{}{}:
		default:
		}
		<-releaseReturn
	}
	packet := MessagePoolCopy(craftSecurityPacket(
		IpProtocolTcp,
		net.ParseIP("203.0.113.7"),
		8080,
		net.ParseIP("10.0.0.9"),
		42001,
		false,
		[]byte("close retained retry"),
	))
	packetByteCount := ByteCount(len(packet))
	ipPath, err := ParseIpPath(packet)
	if err != nil {
		t.Fatalf("parse provider TCP return packet: %v", err)
	}
	callbackReturned := make(chan struct{})
	go func() {
		provider.receiveTransferWithRecovery(
			SourceId(peerId),
			TransferKey{},
			protocol.ProvideMode_Public,
			receiveRecoveryModeTcpSocket,
			ipPath,
			packet,
		)
		close(callbackReturned)
	}()
	waitProviderReturnBarrier(t, retryEntered, "in-flight TCP retry before close")
	closed := make(chan struct{})
	go func() {
		provider.Close()
		close(closed)
	}()
	waitProviderReturnBarrier(t, releaseEntered, "cancelled TCP retry before ownership release")
	waitProviderReturnBarrier(t, admissionsClosed, "closed provider return admission before ownership wait")
	select {
	case <-closed:
		t.Fatal("provider Close returned before its admitted TCP callback released ownership")
	default:
	}
	releaseReturnOnce.Do(func() {
		close(releaseReturn)
	})
	waitProviderReturnBarrier(t, released, "cancelled TCP packet ownership release")
	waitProviderReturnBarrier(t, callbackReturned, "cancelled TCP callback completion")
	waitProviderReturnBarrier(t, closed, "provider close after TCP callback completion")
	if !MessagePoolReturn(packet) {
		t.Fatal("provider close retained the TCP retry packet")
	}
	drops := provider.CongestionDropStats()
	if drops.ReturnSendPacketCount != 1 || drops.ReturnSendByteCount != packetByteCount {
		t.Fatalf("provider close send disposition=%+v, want one cancelled TCP packet", drops)
	}
	if drops.ReturnQueuePacketCount != 0 {
		t.Fatalf("provider close misclassified a TCP retry as queue loss: %+v", drops)
	}
}

// TestRemoteUserNatProviderUdpReturnSendRejectionIsCounted distinguishes a
// datagram send rejection from queue loss and security-policy blocking while
// returning the worker's packet share.
func TestRemoteUserNatProviderUdpReturnSendRejectionIsCounted(t *testing.T) {
	settings := DefaultRemoteUserNatProviderSettings()
	settings.WriteTimeout = 0
	settings.ReturnSendWorkerCount = 1
	settings.ReturnSendQueueSize = 1
	provider, client, _ := newProviderTransferKeyTestFixtureWithSettings(t, settings)
	peerId := NewId()
	source := SourceId(peerId)
	sequence := installProviderReturnTestSequence(t, provider, client, sendSequenceId{
		Destination:       peerId,
		CompanionContract: true,
	})
	sequence.packs = make(chan *SendPack)
	sendResults := make(chan providerReturnSendResult, 1)
	provider.afterReturnSendForTest = func(result providerReturnSendResult) {
		sendResults <- result
	}
	packet := MessagePoolCopy(craftSecurityPacket(
		IpProtocolUdp,
		net.ParseIP("203.0.113.7"),
		8080,
		net.ParseIP("10.0.0.9"),
		42001,
		false,
		[]byte("rejected"),
	))
	packetByteCount := ByteCount(len(packet))
	ipPath, err := ParseIpPath(packet)
	if err != nil {
		MessagePoolReturn(packet)
		t.Fatalf("parse provider return packet: %v", err)
	}

	waitProviderCallbackReturn(t, func() {
		provider.receiveTransfer(
			source,
			TransferKey{},
			protocol.ProvideMode_Public,
			ipPath,
			packet,
		)
	})
	waitProviderReturnSendResult(t, sendResults, false, 1, packetByteCount)
	drops := provider.CongestionDropStats()
	if drops.ReturnSendPacketCount != 1 || drops.ReturnSendByteCount != packetByteCount {
		t.Fatalf("provider return send drops = %+v, want one rejected packet", drops)
	}
	if drops.IngressNatPacketCount != 0 || drops.ReturnQueuePacketCount != 0 {
		t.Fatalf("provider send rejection changed unrelated drops: %+v", drops)
	}
	if !MessagePoolReturn(packet) {
		t.Fatal("rejected return packet retained its sender reference")
	}
	stats := provider.PacketStats()
	if stats.RemoteEgressPacketCount != 0 ||
		stats.BlockIngressPacketCount != 0 ||
		stats.BlockEgressPacketCount != 0 {
		t.Fatalf("provider send congestion changed delivery/security stats: %+v", stats)
	}
}

// A rejected TCP packet remains caller-owned, waits at the explicit retry
// barrier, and then enters Transfer unchanged after sender capacity appears.
func TestRemoteUserNatProviderTcpReturnSendRetriesRejectedPacket(t *testing.T) {
	settings := DefaultRemoteUserNatProviderSettings()
	settings.WriteTimeout = 0
	settings.ReturnSendRetryTimeout = time.Millisecond
	settings.ReturnSendWorkerCount = 1
	settings.ReturnSendQueueSize = 1
	provider, client, _ := newProviderTransferKeyTestFixtureWithSettings(t, settings)
	peerId := NewId()
	source := SourceId(peerId)
	transferKey := TransferKey{
		ForceStream:         true,
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
	attemptResults := make(chan providerReturnSendResult, 2)
	provider.afterReturnSendAttemptForTest = func(result providerReturnSendResult) {
		attemptResults <- result
	}
	finalResults := make(chan providerReturnSendResult, 1)
	provider.afterReturnSendForTest = func(result providerReturnSendResult) {
		finalResults <- result
	}
	retryEntered := make(chan struct{}, 1)
	releaseRetry := make(chan struct{})
	provider.beforeTcpReturnSendRetryForTest = func() {
		select {
		case retryEntered <- struct{}{}:
		default:
		}
		<-releaseRetry
	}
	packet := MessagePoolCopy(craftSecurityPacket(
		IpProtocolTcp,
		net.ParseIP("203.0.113.7"),
		8080,
		net.ParseIP("10.0.0.9"),
		42001,
		false,
		[]byte("retry exact TCP packet"),
	))
	expectedPacket := bytes.Clone(packet)
	ipPath, err := ParseIpPath(packet)
	if err != nil {
		t.Fatalf("parse provider TCP return packet: %v", err)
	}
	callbackReturned := make(chan struct{})
	go func() {
		provider.receiveTransferWithRecovery(
			source,
			transferKey,
			protocol.ProvideMode_Public,
			receiveRecoveryModeTcpSocket,
			ipPath,
			packet,
		)
		close(callbackReturned)
	}()
	waitProviderReturnSendResult(t, attemptResults, false, 1, ByteCount(len(packet)))
	waitProviderReturnBarrier(t, retryEntered, "rejected TCP packet retry")
	if drops := provider.CongestionDropStats(); drops.ReturnSendPacketCount != 0 ||
		drops.ReturnQueuePacketCount != 0 {
		t.Fatalf("transient TCP rejection was recorded as packet loss: %+v", drops)
	}

	sequence.packs = make(chan *SendPack, 1)
	close(releaseRetry)
	waitProviderReturnSendResult(t, attemptResults, true, 1, ByteCount(len(packet)))
	queued := waitProviderReturnTestPack(t, sequence)
	if !bytes.Equal(providerCallbackQueuePacketBytes(t, queued), expectedPacket) {
		t.Fatal("TCP send retry changed the caller-owned packet")
	}
	queued.returnFrames()
	waitProviderReturnSendResult(t, finalResults, true, 1, ByteCount(len(packet)))
	waitProviderReturnBarrier(t, callbackReturned, "successful TCP packet retry completion")
	if !MessagePoolReturn(packet) {
		t.Fatal("successful TCP retry retained its sender packet share")
	}
	if drops := provider.CongestionDropStats(); drops.ReturnSendPacketCount != 0 ||
		drops.ReturnQueuePacketCount != 0 {
		t.Fatalf("successful TCP retry recorded congestion loss: %+v", drops)
	}
	if stats := provider.PacketStats(); stats.RemoteEgressPacketCount != 1 ||
		stats.RemoteEgressByteCount != ByteCount(len(expectedPacket)) {
		t.Fatalf("successful TCP retry stats=%+v, want one exact packet", stats)
	}
}

// The no-contract batch path uses the same retry ownership rule: a rejected
// multi-frame TCP Pack is retained whole and admitted unchanged after release.
func TestRemoteUserNatProviderTcpReturnSendRetriesRejectedBatch(t *testing.T) {
	settings := DefaultRemoteUserNatProviderSettings()
	settings.WriteTimeout = 0
	settings.ReturnSendRetryTimeout = time.Millisecond
	settings.ReturnSendWorkerCount = 1
	settings.ReturnSendQueueSize = 1
	provider, client, _ := newProviderTransferKeyTestFixtureWithSettings(t, settings)
	peerId := NewId()
	client.ContractManager().AddNoContractPeer(peerId)
	transferKey := TransferKey{
		ForceStream:         true,
		CompanionContract:   true,
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
	attemptResults := make(chan providerReturnSendResult, 2)
	provider.afterReturnSendAttemptForTest = func(result providerReturnSendResult) {
		attemptResults <- result
	}
	finalResults := make(chan providerReturnSendResult, 1)
	provider.afterReturnSendForTest = func(result providerReturnSendResult) {
		finalResults <- result
	}
	retryEntered := make(chan struct{}, 1)
	releaseRetry := make(chan struct{})
	provider.beforeTcpReturnSendRetryForTest = func() {
		select {
		case retryEntered <- struct{}{}:
		default:
		}
		<-releaseRetry
	}

	packets := make([][]byte, 2)
	expectedPackets := make([][]byte, len(packets))
	item := &providerReturnItem{
		source:       SourceId(peerId),
		transferKey:  transferKey,
		provideMode:  protocol.ProvideMode_Public,
		recoveryMode: receiveRecoveryModeTcpSocket,
		ipProtocol:   IpProtocolTcp,
		batch:        true,
	}
	for packetIndex := range packets {
		packets[packetIndex] = MessagePoolCopy(craftSecurityPacket(
			IpProtocolTcp,
			net.ParseIP("203.0.113.7"),
			8080,
			net.ParseIP("10.0.0.9"),
			42001,
			false,
			[]byte{byte(packetIndex + 1)},
		))
		expectedPackets[packetIndex] = bytes.Clone(packets[packetIndex])
		item.packets = append(item.packets, MessagePoolShareReadOnly(packets[packetIndex]))
		item.packetByteCount += ByteCount(len(packets[packetIndex]))
	}
	sendDone := make(chan struct{})
	go func() {
		provider.sendReturnItem(item)
		close(sendDone)
	}()
	waitProviderReturnSendResult(t, attemptResults, false, 2, item.packetByteCount)
	waitProviderReturnBarrier(t, retryEntered, "rejected TCP batch retry")
	sequence.packs = make(chan *SendPack, 1)
	close(releaseRetry)
	waitProviderReturnSendResult(t, attemptResults, true, 2, item.packetByteCount)
	queued := waitProviderReturnTestPack(t, sequence)
	if queued.Frame != nil || len(queued.Frames) != len(expectedPackets) {
		t.Fatalf("provider TCP batch Pack=%#v, want %d frames", queued, len(expectedPackets))
	}
	for packetIndex, frame := range queued.Frames {
		packetBytes, decodeErr := ipPacketFromProviderBytes(frame)
		if decodeErr != nil {
			t.Fatalf("decode provider TCP batch packet %d: %v", packetIndex, decodeErr)
		}
		if !bytes.Equal(packetBytes, expectedPackets[packetIndex]) {
			t.Fatalf("TCP batch retry changed or reordered packet %d", packetIndex)
		}
	}
	queued.returnFrames()
	waitProviderReturnSendResult(t, finalResults, true, 2, item.packetByteCount)
	waitProviderReturnBarrier(t, sendDone, "successful TCP batch retry completion")
	for packetIndex, packet := range packets {
		if !MessagePoolReturn(packet) {
			t.Fatalf("successful TCP batch retry retained packet %d", packetIndex)
		}
	}
	if drops := provider.CongestionDropStats(); drops.ReturnSendPacketCount != 0 ||
		drops.ReturnQueuePacketCount != 0 {
		t.Fatalf("successful TCP batch retry recorded congestion loss: %+v", drops)
	}
	if stats := provider.PacketStats(); stats.RemoteEgressPacketCount != 2 ||
		stats.RemoteEgressByteCount != item.packetByteCount {
		t.Fatalf("successful TCP batch retry stats=%+v, want two exact packets", stats)
	}
}

// TestRemoteUserNatProviderCloseInterruptsAndDrainsDatagramReturnQueue verifies
// that shutdown cancels a blocked datagram sender, releases its raw frame, and
// drains every queued borrowed-packet share before Close returns.
func TestRemoteUserNatProviderCloseInterruptsAndDrainsDatagramReturnQueue(t *testing.T) {
	settings := DefaultRemoteUserNatProviderSettings()
	settings.WriteTimeout = time.Hour
	settings.ReturnSendWorkerCount = 1
	settings.ReturnSendQueueSize = 1
	provider, client, _ := newProviderTransferKeyTestFixtureWithSettings(t, settings)
	peerId := NewId()
	source := SourceId(peerId)
	transferKey := TransferKey{}
	sequence := installProviderReturnTestSequence(t, provider, client, sendSequenceId{
		Destination:       peerId,
		CompanionContract: true,
	})
	sequence.packs = make(chan *SendPack)
	started := make(chan struct{}, 1)
	provider.returnSendStarted = func() {
		select {
		case started <- struct{}{}:
		default:
		}
		<-provider.ctx.Done()
	}
	packets := [][]byte{
		MessagePoolCopy(craftSecurityPacket(
			IpProtocolUdp,
			net.ParseIP("203.0.113.7"),
			8080,
			net.ParseIP("10.0.0.9"),
			42001,
			false,
			[]byte("active"),
		)),
		MessagePoolCopy(craftSecurityPacket(
			IpProtocolUdp,
			net.ParseIP("203.0.113.7"),
			8080,
			net.ParseIP("10.0.0.9"),
			42001,
			false,
			[]byte("queued"),
		)),
	}
	ipPath, err := ParseIpPath(packets[0])
	if err != nil {
		t.Fatalf("parse provider return packet: %v", err)
	}

	waitProviderCallbackReturn(t, func() {
		provider.receiveTransfer(source, transferKey, protocol.ProvideMode_Public, ipPath, packets[0])
	})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("provider return worker did not reach the shutdown barrier")
	}
	waitProviderCallbackReturn(t, func() {
		provider.receiveTransfer(source, transferKey, protocol.ProvideMode_Public, ipPath, packets[1])
	})
	if queueSize := len(provider.returnSendQueues[0]); queueSize != 1 {
		t.Fatalf("provider sender queue size = %d, want 1 before shutdown", queueSize)
	}

	closed := make(chan struct{})
	go func() {
		provider.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("provider Close did not interrupt the blocked sender")
	}
	for packetIndex, packet := range packets {
		if !MessagePoolReturn(packet) {
			t.Fatalf(
				"provider shutdown retained packet %d",
				packetIndex,
			)
		}
	}
	if stats := provider.PacketStats(); stats.RemoteEgressPacketCount != 0 {
		t.Fatalf("provider shutdown counted an unsent return: %+v", stats)
	}
	drops := provider.CongestionDropStats()
	if drops.ReturnSendPacketCount != 1 ||
		drops.ReturnSendByteCount != ByteCount(len(packets[0])) {
		t.Fatalf("provider shutdown send drops = %+v, want the active packet", drops)
	}
	if drops.IngressNatPacketCount != 0 || drops.ReturnQueuePacketCount != 0 {
		t.Fatalf("provider shutdown changed unrelated congestion drops: %+v", drops)
	}
}
