package connect

// Local-only lifecycle tests carried across the reliability-checkpoint merge.
// Ported where the subject survived with a changed surface. Tests whose
// subjects the merge replaced wholesale were deliberately dropped rather than
// force-ported, because their asserted properties are no longer the design:
//   - runMultiClientPingAdmission + the MultiClientGeneratorContext
//     maintenance layer (WindowClientCreateTimeout): replaced by the inline
//     admission and the windowGeneratorCall lateResult machinery.
//   - reconcileSendClientPath: replaced by the update-race commit path.
//   - busyStale/isBlackholeAt: replaced by the verdict system
//     (blackholeReasonFromStats/verdictAction) — the surviving semantics are
//     pinned in ip_remote_multi_client_connect_evidence_test.go.
//   - update.Close clearing the committed client: the clear now CONFLICTS
//     with the donor-affinity design, which deliberately reads a retired
//     flow's last exit (guarded by IsDone) so the next flow to the same site
//     inherits it.
//   - bounded reset prioritization: replaced by the removal-budget +
//     bench-time hand-off design.

import (
	"context"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

func TestMultiClientRejectedSendDoesNotCreateBlackholeEvidence(t *testing.T) {
	settings := DefaultMultiClientSettings()
	log := NewNoopLogger()
	clientCtx, cancelClient := context.WithCancel(context.Background())
	cancelClient()
	channel := &multiClientChannel{
		ctx:                       clientCtx,
		log:                       log,
		args:                      &multiClientChannelArgs{Destination: RequireMultiHopId(NewId())},
		client:                    &Client{ctx: clientCtx},
		createTime:                time.Now(),
		settings:                  settings,
		eventBuckets:              []*multiClientEventBucket{},
		ip4DestinationSourceCount: map[Ip4Path]map[Ip4Path]int{},
		ip6DestinationSourceCount: map[Ip6Path]map[Ip6Path]int{},
		packetStats:               &clientWindowStats{log: log},
	}
	ipPath := &IpPath{
		Version:         4,
		Protocol:        IpProtocolTcp,
		SourceIp:        []byte{10, 0, 0, 1},
		SourcePort:      12345,
		DestinationIp:   []byte{192, 0, 2, 1},
		DestinationPort: 443,
	}

	success, err := channel.SendDetailedWithAck(&parsedPacket{packet: make([]byte, 512), ipPath: ipPath}, time.Second, true)
	if success || err == nil {
		t.Fatalf("canceled transfer queue accepted packet: success=%t err=%v", success, err)
	}

	var sendNackCount int
	var sendNackByteCount ByteCount
	var pendingSendTime time.Time
	func() {
		channel.stateLock.Lock()
		defer channel.stateLock.Unlock()
		sendNackCount = channel.packetStats.sendNackCount
		sendNackByteCount = channel.packetStats.sendNackByteCount
		pendingSendTime = channel.pendingSendTime
	}()
	if sendNackCount != 0 || sendNackByteCount != 0 {
		t.Fatalf(
			"rejected send retained outstanding accounting: %d %dB",
			sendNackCount,
			sendNackByteCount,
		)
	}
	if !pendingSendTime.IsZero() {
		t.Fatal("rejected send retained a stall deadline")
	}
	stats, err := channel.WindowStats()
	if err == nil {
		// canceled fixtures may not produce stats; when they do, the rejected
		// local enqueue must not read as any remote verdict
		reason, held := blackholeReasonFromStats(
			time.Now().Add(2*settings.BlackholeTimeout),
			stats,
			settings.BlackholeTimeout,
			settings.BlackholeReceiveTimeout,
			settings.BlackholeConnectTimeout,
			blackholeGates{},
		)
		if reason != blackholeNone || held != blackholeNone {
			t.Fatalf("rejected local enqueue was classified as a remote verdict: reason=%q held=%q", reason, held)
		}
	}
}

// A stats observer can remain parked after app suspension. Essential client,
// transport, and generator cleanup must happen before observer-only close
// events, otherwise every peer churn retains another transport/client record.
func TestMultiClientCleanupPrecedesBlockedObservers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	generator := &cleanupOrderTestGenerator{removeCalled: make(chan struct{})}
	settings := DefaultMultiClientSettings()
	settings.CPingRestTimeout = time.Hour
	settings.BlackholeTimeout = time.Hour
	statsGate := newStallGate()

	channel, err := newMultiClientChannel(
		ctx,
		&multiClientChannelArgs{
			MultiClientGeneratorClientArgs: MultiClientGeneratorClientArgs{ClientId: NewId()},
			Destination:                    RequireMultiHopId(NewId()),
		},
		generator,
		func(*multiClientChannel, TransferPath, protocol.ProvideMode, TransportType, *IpPath, []byte) {},
		nil,
		DefaultSecurityPolicy(ctx),
		func(*ContractStatus) {},
		func([]*ContractStatsEvent) { statsGate.Wait() },
		func() {},
		nil,
		settings,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	entry := generator.client.ContractManager().registerContractStats(
		NewId(),
		false,
		false,
		TransferPath{},
		100,
	)
	entry.updateUsedByteCount(10)
	channel.Cancel()

	select {
	case <-generator.removeCalled:
	case <-time.After(time.Second):
		t.Fatal("blocked observer retained essential generator/client resources")
	}
	waitForStallStart(t, statsGate)
	statsGate.Release()
}
