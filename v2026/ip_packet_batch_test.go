// These tests pin packet grouping order and the additive batch-client
// ownership contract without relying on worker scheduling.
package connect

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// Closing either dispatch topology must return every pooled packet that was
// accepted into a queue but not yet handed to a protocol flow.
func TestLocalUserNatCloseReturnsQueuedPacketOwnership(t *testing.T) {
	for _, shardCount := range []int{1, 4} {
		t.Run(fmt.Sprintf("shards-%d", shardCount), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			settings := DefaultLocalUserNatSettingsWithBufferSize(64)
			settings.SendShardCount = shardCount
			localUserNat := NewLocalUserNat(ctx, "close-queue-ownership", settings)

			entered := make(chan struct{})
			release := make(chan struct{})
			var blockOnce sync.Once
			localUserNat.afterSendPacketForTest = func() {
				blockOnce.Do(func() {
					close(entered)
					<-release
				})
			}

			// The malformed lead packet is returned synchronously by the worker,
			// then its test edge holds that worker while the owned queue fills.
			lead := MessagePoolCopy([]byte{0xff})
			if !localUserNat.SendPacket(SourceId(NewId()), protocol.ProvideMode_Public, lead, time.Second) {
				MessagePoolReturn(lead)
				t.Fatal("lead packet was not admitted")
			}
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("local NAT worker did not reach the disposition edge")
			}

			witnesses := make([][]byte, 64)
			for packetIndex := range witnesses {
				packet := MessagePoolCopy([]byte{0xff})
				witnesses[packetIndex] = MessagePoolShareReadOnly(packet)
				if !localUserNat.SendPacket(
					SourceId(NewId()),
					protocol.ProvideMode_Public,
					packet,
					time.Second,
				) {
					MessagePoolReturn(packet)
					for _, witness := range witnesses[:packetIndex+1] {
						MessagePoolReturn(witness)
					}
					close(release)
					localUserNat.Close()
					t.Fatalf("queued packet %d was not admitted", packetIndex)
				}
			}

			localUserNat.Close()
			close(release)
			select {
			case <-localUserNat.runDone:
			case <-time.After(5 * time.Second):
				t.Fatal("local NAT shutdown did not finish")
			}
			requireIpPacketBatchWitnessesReleased(t, witnesses)
		})
	}
}

// The joining close cannot publish completion while a packet-owning worker is
// held, and every queued owner is returned before that completion is visible.
func TestLocalUserNatCloseAndWaitJoinsQueuedPacketOwnership(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultLocalUserNatSettingsWithBufferSize(8)
	localUserNat := NewLocalUserNat(ctx, "close-wait-queue-ownership", settings)

	workerEntered := make(chan struct{})
	releaseWorker := make(chan struct{})
	var blockOnce sync.Once
	localUserNat.afterSendPacketForTest = func() {
		blockOnce.Do(func() {
			close(workerEntered)
			<-releaseWorker
		})
	}
	waitEntered := make(chan struct{})
	localUserNat.beforeRunDoneWaitForTest = func() {
		close(waitEntered)
	}

	lead := MessagePoolCopy([]byte{0xff})
	if !localUserNat.SendPacket(
		SourceId(NewId()),
		protocol.ProvideMode_Public,
		lead,
		time.Second,
	) {
		MessagePoolReturn(lead)
		t.Fatal("lead packet was not admitted")
	}
	select {
	case <-workerEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("local NAT worker did not reach the disposition edge")
	}

	packet := MessagePoolCopy([]byte{0xff})
	witness := MessagePoolShareReadOnly(packet)
	if !localUserNat.SendPacket(
		SourceId(NewId()),
		protocol.ProvideMode_Public,
		packet,
		time.Second,
	) {
		MessagePoolReturn(packet)
		MessagePoolReturn(witness)
		close(releaseWorker)
		t.Fatal("queued packet was not admitted")
	}

	joinCtx, joinCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer joinCancel()
	joinResult := make(chan error, 1)
	go func() {
		joinResult <- localUserNat.CloseAndWait(joinCtx)
	}()
	select {
	case <-waitEntered:
	case <-joinCtx.Done():
		close(releaseWorker)
		t.Fatalf("joining close did not reach its ownership barrier: %v", joinCtx.Err())
	}
	select {
	case err := <-joinResult:
		close(releaseWorker)
		t.Fatalf("joining close returned while the packet worker was held: %v", err)
	default:
	}

	close(releaseWorker)
	select {
	case err := <-joinResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-joinCtx.Done():
		t.Fatalf("joining close did not finish after worker release: %v", joinCtx.Err())
	}
	if !MessagePoolReturn(witness) {
		t.Fatal("queued packet owner remained live after joining close")
	}
}

// A joining close cannot publish completion after dispatch has stopped while
// a TCP sequence still owns its first queued packet.
func TestLocalUserNatCloseAndWaitJoinsTcpSequenceOwnership(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultLocalUserNatSettingsWithBufferSize(8)
	sequenceEntered := make(chan struct{})
	releaseSequence := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(releaseSequence) })
	}
	defer release()
	settings.TcpBufferSettings.beforeSequenceRunForTest = func() {
		close(sequenceEntered)
		<-releaseSequence
	}
	flowWaitEntered := make(chan struct{})
	settings.beforeFlowWorkersWaitForTest = func() {
		close(flowWaitEntered)
	}
	localUserNat := NewLocalUserNat(ctx, "close-wait-tcp-ownership", settings)

	ipPath := &IpPath{
		Version:         4,
		Protocol:        IpProtocolTcp,
		SourceIp:        net.IPv4(10, 0, 0, 9),
		SourcePort:      40001,
		DestinationIp:   net.IPv4(203, 0, 113, 7),
		DestinationPort: 443,
	}
	packet := MessagePoolCopy(ipOosTcpPacketSequence(ipPath, tcpFlagSyn, 1000, nil))
	earlyWitness := MessagePoolShareReadOnly(packet)
	finalWitness := MessagePoolShareReadOnly(packet)
	earlyWitnessLive := true
	finalWitnessLive := true
	defer func() {
		if earlyWitnessLive {
			MessagePoolReturn(earlyWitness)
		}
		if finalWitnessLive {
			MessagePoolReturn(finalWitness)
		}
	}()
	if !localUserNat.SendPacket(
		SourceId(NewId()),
		protocol.ProvideMode_Public,
		packet,
		time.Second,
	) {
		MessagePoolReturn(packet)
		t.Fatal("TCP SYN was not admitted")
	}
	select {
	case <-sequenceEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("TCP sequence did not reach the held ownership edge")
	}

	joinCtx, joinCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer joinCancel()
	joinResult := make(chan error, 1)
	go func() {
		joinResult <- localUserNat.CloseAndWait(joinCtx)
	}()
	select {
	case <-flowWaitEntered:
	case <-joinCtx.Done():
		t.Fatalf("local NAT did not reach its flow-worker join: %v", joinCtx.Err())
	}
	select {
	case err := <-joinResult:
		t.Fatalf("joining close returned while the TCP owner was held: %v", err)
	default:
	}
	if MessagePoolReturn(earlyWitness) {
		earlyWitnessLive = false
		t.Fatal("held TCP sequence did not retain production packet ownership")
	}
	earlyWitnessLive = false

	release()
	select {
	case err := <-joinResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-joinCtx.Done():
		t.Fatalf("joining close did not finish after TCP release: %v", joinCtx.Err())
	}
	if !MessagePoolReturn(finalWitness) {
		finalWitnessLive = false
		t.Fatal("TCP sequence ownership remained live after joining close")
	}
	finalWitnessLive = false
}

// A producer admitted before cancellation may publish after a receive worker's
// cancellation drain. The terminal wait performs one final drain only after
// every producer has joined.
func TestUdpReceiveDispatcherTerminalWaitDrainsLateAdmittedPacket(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	settings := DefaultUdpBufferSettingsWithBufferSize(1)
	dispatcher := newUdpReceiveDispatcher(ctx, settings)
	shard := &dispatcher.shards[0]
	workerDone := make(chan struct{})
	dispatcher.waitGroup.Add(1)
	go func() {
		defer dispatcher.waitGroup.Done()
		defer close(workerDone)
		shard.run()
	}()
	cancel()
	select {
	case <-workerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("UDP receive dispatcher did not stop")
	}

	packet := MessagePoolCopy([]byte{0xff})
	witness := MessagePoolShareReadOnly(packet)
	shard.items <- udpReceiveDispatchItem{packet: packet}
	dispatcher.waitForLifecycle()
	if MessagePoolReturn(witness) {
		return
	}

	// Reclaim the production owner on old behavior so a regression failure
	// cannot contaminate later process-wide pool assertions.
	returnQueuedUdpDispatchItems(shard.items)
	t.Fatal("terminal UDP dispatcher wait left a late admitted packet queued")
}

// Returns pooled UDP packet bytes suitable for exact directional-flow tests.
func ipPacketBatchTestPacket(sourcePort int, destinationPort int, payload string) []byte {
	return MessagePoolCopy(craftSecurityPacket(
		IpProtocolUdp,
		net.ParseIP("10.0.0.9"),
		sourcePort,
		net.ParseIP("203.0.113.7"),
		destinationPort,
		false,
		[]byte(payload),
	))
}

// Releases retained references and requires every production owner to have
// released its reference first.
func requireIpPacketBatchWitnessesReleased(t *testing.T, witnesses [][]byte) {
	t.Helper()
	for witnessIndex, witness := range witnesses {
		if !MessagePoolReturn(witness) {
			t.Fatalf("packet witness %d was not the final owner", witnessIndex)
		}
	}
}

// Exact tuples form stable first-seen groups while malformed packets remain
// caller-owned and each group's retained path owns its address bytes.
func TestGroupIpPacketsPreservesDirectionalFlowOrder(t *testing.T) {
	flowAFirst := ipPacketBatchTestPacket(41001, 53, "a1")
	flowBFirst := ipPacketBatchTestPacket(41002, 53, "b1")
	rejectedPacket := MessagePoolCopy([]byte{0xff})
	flowASecond := ipPacketBatchTestPacket(41001, 53, "a2")
	flowBSecond := ipPacketBatchTestPacket(41002, 53, "b2")
	packets := [][]byte{
		flowAFirst,
		flowBFirst,
		rejectedPacket,
		flowASecond,
		flowBSecond,
	}
	defer func() {
		for _, packet := range packets {
			if packet != nil {
				MessagePoolReturn(packet)
			}
		}
	}()

	groups, rejected := groupIpPackets(packets)
	if len(groups) != 2 {
		t.Fatalf("group count = %d, want 2", len(groups))
	}
	if len(rejected) != 1 || &rejected[0][0] != &rejectedPacket[0] {
		t.Fatalf("rejected packets = %#v, want exact malformed packet", rejected)
	}
	if len(groups[0].packets) != 2 ||
		&groups[0].packets[0][0] != &flowAFirst[0] ||
		&groups[0].packets[1][0] != &flowASecond[0] {
		t.Fatalf("first group packet order did not preserve flow A")
	}
	if len(groups[1].packets) != 2 ||
		&groups[1].packets[0][0] != &flowBFirst[0] ||
		&groups[1].packets[1][0] != &flowBSecond[0] {
		t.Fatalf("second group packet order did not preserve flow B")
	}
	if groups[0].byteCount != ByteCount(len(flowAFirst)+len(flowASecond)) {
		t.Fatalf("first group byte count = %d, want %d", groups[0].byteCount, len(flowAFirst)+len(flowASecond))
	}
	if groups[0].ipPath.SourcePort != 41001 || groups[1].ipPath.SourcePort != 41002 {
		t.Fatalf(
			"group source ports = (%d, %d), want (41001, 41002)",
			groups[0].ipPath.SourcePort,
			groups[1].ipPath.SourcePort,
		)
	}

	wantSourceIp := append(net.IP(nil), groups[0].ipPath.SourceIp...)
	flowAFirst[12] = 192
	if !groups[0].ipPath.SourceIp.Equal(wantSourceIp) {
		t.Fatal("group path source address aliases packet storage")
	}
	packets[0] = nil
	if &groups[0].packets[0][0] != &flowAFirst[0] {
		t.Fatal("group packet slice aliases the caller's slice header")
	}
	MessagePoolReturn(flowAFirst)
}

func TestGroupIpPacketsBoundedSplitsOwnershipWithoutRejectingPackets(t *testing.T) {
	packets := [][]byte{
		ipPacketBatchTestPacket(41101, 443, "a"),
		ipPacketBatchTestPacket(41101, 443, "b"),
		ipPacketBatchTestPacket(41101, 443, "c"),
		ipPacketBatchTestPacket(41101, 443, "d"),
		ipPacketBatchTestPacket(41101, 443, "e"),
	}
	defer func() {
		for _, packet := range packets {
			MessagePoolReturn(packet)
		}
	}()

	groups, rejected := groupIpPacketsBounded(packets, 2, mib(1))
	if len(rejected) != 0 {
		t.Fatalf("bounded grouping rejected %d valid packets", len(rejected))
	}
	wantCounts := []int{2, 2, 1}
	if len(groups) != len(wantCounts) {
		t.Fatalf("group count = %d, want %d", len(groups), len(wantCounts))
	}
	packetIndex := 0
	for groupIndex, group := range groups {
		if len(group.packets) != wantCounts[groupIndex] {
			t.Fatalf("group %d packets = %d, want %d", groupIndex, len(group.packets), wantCounts[groupIndex])
		}
		for _, packet := range group.packets {
			if &packet[0] != &packets[packetIndex][0] {
				t.Fatalf("group order changed at packet %d", packetIndex)
			}
			packetIndex++
		}
	}

	// A byte bound splits before crossing it, but cannot make one otherwise
	// valid oversized packet impossible to send.
	byteBound := ByteCount(len(packets[0]) + len(packets[1]) - 1)
	groups, rejected = groupIpPacketsBounded(packets[:2], 8, byteBound)
	if len(rejected) != 0 || len(groups) != 2 ||
		len(groups[0].packets) != 1 || len(groups[1].packets) != 1 {
		t.Fatalf("byte-bounded groups = %#v rejected=%d", groups, len(rejected))
	}
	groups, rejected = groupIpPacketsBounded(packets[:1], 8, 1)
	if len(rejected) != 0 || len(groups) != 1 || len(groups[0].packets) != 1 {
		t.Fatal("oversized singleton was rejected by the ownership bound")
	}
}

// Local admission is all-or-nothing, while the additive batch API consumes
// every input on both admission outcomes and reports the exact accepted count.
func TestLocalUserNatSendPacketBatchConsumesAllPackets(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	localUserNat := &LocalUserNat{
		ctx:         ctx,
		sendPackets: make(chan *SendPacket, 1),
	}
	acceptedPackets := [][]byte{
		MessagePoolCopy([]byte("accepted-a")),
		MessagePoolCopy([]byte("accepted-b")),
	}
	acceptedWitnesses := [][]byte{
		MessagePoolShareReadOnly(acceptedPackets[0]),
		MessagePoolShareReadOnly(acceptedPackets[1]),
	}
	acceptedCount := localUserNat.SendPacketBatch(
		SourceId(NewId()),
		protocol.ProvideMode_Public,
		acceptedPackets,
		0,
	)
	if acceptedCount != len(acceptedPackets) {
		t.Fatalf("accepted count = %d, want %d", acceptedCount, len(acceptedPackets))
	}
	acceptedFirst := acceptedPackets[0]
	acceptedPackets[0] = nil
	queued := <-localUserNat.sendPackets
	defer queued.finish()
	if len(queued.packets) != len(acceptedPackets) {
		t.Fatalf("queued packet count = %d, want %d", len(queued.packets), len(acceptedPackets))
	}
	if queued.packets[0] == nil || &queued.packets[0][0] != &acceptedFirst[0] {
		t.Fatal("queued packet list aliases caller slice metadata")
	}
	for packetIndex, packet := range queued.packets {
		wantPacket := acceptedPackets[packetIndex]
		if packetIndex == 0 {
			wantPacket = acceptedFirst
		}
		if &packet[0] != &wantPacket[0] {
			t.Fatalf("queued packet %d is not the exact input", packetIndex)
		}
		MessagePoolReturn(packet)
	}
	requireIpPacketBatchWitnessesReleased(t, acceptedWitnesses)

	rejectingLocalUserNat := &LocalUserNat{
		ctx:         ctx,
		sendPackets: make(chan *SendPacket),
	}
	rejectedPackets := [][]byte{
		MessagePoolCopy([]byte("rejected-a")),
		MessagePoolCopy([]byte("rejected-b")),
	}
	rejectedWitnesses := [][]byte{
		MessagePoolShareReadOnly(rejectedPackets[0]),
		MessagePoolShareReadOnly(rejectedPackets[1]),
	}
	acceptedCount = rejectingLocalUserNat.SendPacketBatch(
		SourceId(NewId()),
		protocol.ProvideMode_Public,
		rejectedPackets,
		0,
	)
	if acceptedCount != 0 {
		t.Fatalf("rejected batch accepted count = %d, want 0", acceptedCount)
	}
	requireIpPacketBatchWitnessesReleased(t, rejectedWitnesses)
}

// Even early parse rejections satisfy the remote batch API's consumes-all
// contract rather than leaking ownership back to a caller that cannot retry.
func TestRemoteUserNatClientSendPacketBatchConsumesRejectedPackets(t *testing.T) {
	remoteUserNatClient := &RemoteUserNatClient{}
	packets := [][]byte{
		MessagePoolCopy([]byte{0xff}),
		MessagePoolCopy([]byte{0xee}),
	}
	witnesses := [][]byte{
		MessagePoolShareReadOnly(packets[0]),
		MessagePoolShareReadOnly(packets[1]),
	}
	acceptedCount := remoteUserNatClient.SendPacketBatch(
		SourceId(NewId()),
		protocol.ProvideMode_Public,
		packets,
		0,
	)
	if acceptedCount != 0 {
		t.Fatalf("invalid batch accepted count = %d, want 0", acceptedCount)
	}
	requireIpPacketBatchWitnessesReleased(t, witnesses)
}

// Provider ingress sends one LocalUserNat item per exact tuple in first-seen
// group order, retaining source/lane metadata and in-flow packet order.
func TestRemoteUserNatProviderGroupsIngressByDirectionalTuple(t *testing.T) {
	provider, _, localUserNat := newProviderTransferKeyTestFixture(t)
	localUserNat.sendPackets = make(chan *SendPacket, 3)
	source := SourceId(NewId())
	transferKey := TransferKey{ForceStream: true}
	packets := [][]byte{
		ipPacketBatchTestPacket(42001, 53, "a1"),
		ipPacketBatchTestPacket(42002, 53, "b1"),
		ipPacketBatchTestPacket(42001, 53, "a2"),
		ipPacketBatchTestPacket(42003, 53, "c1"),
		ipPacketBatchTestPacket(42002, 53, "b2"),
	}
	frames := make([]*protocol.Frame, len(packets))
	witnesses := make([][]byte, len(packets))
	for packetIndex, packet := range packets {
		frame, err := ipPacketToProviderFrame(packet, DefaultProtocolVersion)
		if err != nil {
			t.Fatalf("build ingress frame %d: %v", packetIndex, err)
		}
		frames[packetIndex] = frame
		witnesses[packetIndex] = MessagePoolShareReadOnly(packet)
	}
	defer func() {
		for _, packet := range packets {
			MessagePoolReturn(packet)
		}
	}()

	provider.ClientReceive(
		source,
		frames,
		Peer{
			ProvideMode: protocol.ProvideMode_Public,
			TransferKey: transferKey,
		},
	)

	wantGroupIndexes := [][]int{{0, 2}, {1, 4}, {3}}
	if len(localUserNat.sendPackets) != len(wantGroupIndexes) {
		t.Fatalf(
			"provider queued group count = %d, want %d",
			len(localUserNat.sendPackets),
			len(wantGroupIndexes),
		)
	}
	for groupIndex, wantPacketIndexes := range wantGroupIndexes {
		queued := <-localUserNat.sendPackets
		defer queued.finish()
		if queued.source != source.LocalMask() || queued.transferKey != transferKey {
			t.Fatalf("group %d metadata = (%s, %#v), want (%s, %#v)", groupIndex, queued.source, queued.transferKey, source.LocalMask(), transferKey)
		}
		if len(queued.packets) != len(wantPacketIndexes) {
			t.Fatalf("group %d packet count = %d, want %d", groupIndex, len(queued.packets), len(wantPacketIndexes))
		}
		for packetIndex, inputIndex := range wantPacketIndexes {
			if &queued.packets[packetIndex][0] != &packets[inputIndex][0] {
				t.Fatalf("group %d packet %d is not input %d", groupIndex, packetIndex, inputIndex)
			}
			MessagePoolReturn(queued.packets[packetIndex])
		}
	}
	if len(localUserNat.sendPackets) != 0 {
		t.Fatalf("provider queued %d unexpected ingress groups", len(localUserNat.sendPackets))
	}
	for _, packet := range packets {
		MessagePoolReturn(packet)
	}
	packets = nil
	requireIpPacketBatchWitnessesReleased(t, witnesses)
}

// Compile-time assertions keep the optional interface additive for both
// provider-local and remote clients.
var _ UserNatBatchClient = (*RemoteUserNatClient)(nil)
