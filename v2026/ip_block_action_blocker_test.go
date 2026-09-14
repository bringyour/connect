package connect

import (
	"context"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// TestBlockActionApplyBlocker: the blocker fuses into the egress decision as
// a default block, overridable like the security decision — except that an
// un-blocked blocker match (allow-classified) egresses remotely as normal.
func TestBlockActionApplyBlocker(t *testing.T) {
	blockTrue := &blockActionMatch{blockOverride: &BlockOverride{Block: true}}
	blockFalse := &blockActionMatch{blockOverride: &BlockOverride{Block: false}}
	localTrue := &blockActionMatch{routeOverride: &RouteOverride{Local: true}}

	type applyCase struct {
		r            SecurityPolicyResult
		bypass       bool
		blockerBlock bool
		match        *blockActionMatch
		block        bool
		local        bool
	}
	cases := []applyCase{
		// a blocker match blocks by default
		{SecurityPolicyResultAllow, false, true, nil, true, false},
		{SecurityPolicyResultAllow, true, true, nil, true, false},
		// the blocker block wins over the bypass-local default route
		{SecurityPolicyResultDrop, true, true, nil, true, false},
		// an un-blocked blocker match egresses remotely (allow-classified)
		{SecurityPolicyResultAllow, false, true, blockFalse, false, false},
		// an un-blocked drop-classified match still never egresses remotely
		{SecurityPolicyResultDrop, false, true, blockFalse, false, true},
		// a block override agrees with the blocker
		{SecurityPolicyResultAllow, false, true, blockTrue, true, false},
		// a route override on a blocker match: block wins over local
		{SecurityPolicyResultAllow, false, true, localTrue, true, false},
		// incident is terminal regardless of the blocker
		{SecurityPolicyResultIncident, false, true, blockFalse, true, false},
		// no blocker match leaves the existing behavior (spot check)
		{SecurityPolicyResultAllow, false, false, nil, false, false},
		{SecurityPolicyResultDrop, true, false, nil, false, true},
	}
	for i, c := range cases {
		block, local := blockActionApply(c.r, c.bypass, c.blockerBlock, c.match)
		AssertEqual(t, c.block, block)
		AssertEqual(t, c.local, local)
		if c.block != block || c.local != local {
			t.Logf("case %d failed: %+v", i, c)
		}
	}
}

func testingBlockerMultiClient(ctx context.Context, lookup ServerNameLookup) *RemoteUserNatMultiClient {
	securityPolicy := &testingFixedSecurityPolicy{
		stats:  DefaultSecurityPolicyStatsCollector(),
		result: SecurityPolicyResultAllow,
	}
	settings := DefaultMultiClientSettings()
	settings.EventEpoch = 20 * time.Millisecond
	settings.SecurityPolicyGenerator = func(ctx context.Context, stats *SecurityPolicyStatsCollector) SecurityPolicy {
		return securityPolicy
	}
	multiClient := NewRemoteUserNatMultiClient(
		ctx,
		&testingEmptyMultiClientGenerator{},
		func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
		},
		protocol.ProvideMode_Network,
		settings,
	)
	if lookup != nil {
		multiClient.SetServerNameLookup(lookup)
	}
	return multiClient
}

// TestMultiClientBlockerIp: a destination inside the blocker's ip ranges is
// dropped with block stats and a Blocker-attributed block action; toggling
// the blocker off (and on) takes effect immediately across the decision
// cache; a clean destination is never touched.
func TestMultiClientBlockerIp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	multiClient := testingBlockerMultiClient(ctx, nil)
	defer multiClient.Close()

	blocker := blockerTestNew(
		nil,
		[][2]uint32{{blockerTestIp4("127.0.0.9"), blockerTestIp4("127.0.0.9")}},
		nil,
	)
	multiClient.SetBlocker(blocker)

	blockActionsChannel := make(chan []*BlockAction, 16)
	unsub := multiClient.AddBlockActionCallback(func(blockActions []*BlockAction) {
		blockActionsChannel <- blockActions
	})
	defer unsub()

	nextBlockActions := func() []*BlockAction {
		select {
		case blockActions := <-blockActionsChannel:
			return blockActions
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for block actions")
			return nil
		}
	}

	source := SourceId(NewId())
	blockedPacket := testingUdp4Packet("10.0.0.5", "127.0.0.9", 443, []byte("hello"))
	cleanPacket := testingUdp4Packet("10.0.0.5", "127.0.0.8", 443, []byte("hello"))

	// blocked: dropped with stats and a Blocker-attributed action
	success := multiClient.SendPacket(source, protocol.ProvideMode_Network, blockedPacket, 0)
	AssertEqual(t, false, success)

	blockActions := nextBlockActions()
	AssertEqual(t, 1, len(blockActions))
	AssertEqual(t, true, blockActions[0].Block)
	AssertEqual(t, false, blockActions[0].Local)
	AssertEqual(t, true, blockActions[0].Blocker)
	AssertEqual(t, true, blockActions[0].BlockOverrideId == nil)

	packetStats := multiClient.PacketStats()
	AssertEqual(t, int64(1), packetStats.BlockEgressPacketCount)
	AssertEqual(t, ByteCount(len(blockedPacket)), packetStats.BlockEgressByteCount)

	// a clean destination is not blocked (the remote send fails on the empty
	// generator, but nothing lands in the block counters)
	multiClient.SendPacket(source, protocol.ProvideMode_Network, cleanPacket, 0)
	packetStats = multiClient.PacketStats()
	AssertEqual(t, int64(1), packetStats.BlockEgressPacketCount)

	// disabling the blocker takes effect immediately (the cached blocked
	// decision revalidates against the enabled state)
	blocker.SetEnabled(false)
	multiClient.SendPacket(source, protocol.ProvideMode_Network, blockedPacket, 0)
	packetStats = multiClient.PacketStats()
	AssertEqual(t, int64(1), packetStats.BlockEgressPacketCount)

	// and re-enabling blocks again
	blocker.SetEnabled(true)
	success = multiClient.SendPacket(source, protocol.ProvideMode_Network, blockedPacket, 0)
	AssertEqual(t, false, success)
	packetStats = multiClient.PacketStats()
	AssertEqual(t, int64(2), packetStats.BlockEgressPacketCount)
}

// TestMultiClientBlockerServerName: a destination whose own observed server
// name is a blocked hostname is dropped (the reverse-index backstop for
// flows that skipped dns-level blocking); a destination whose names are
// clean is untouched; a user un-block override wins and the flow egresses
// remotely (not locally).
func TestMultiClientBlockerServerName(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lookup := &testingServerNameLookup{
		serverNames: map[string][]string{
			"127.0.0.9": {"cdn.example.com", "sub.ads.example.com"},
			"127.0.0.8": {"cdn.example.com"},
		},
	}
	multiClient := testingBlockerMultiClient(ctx, lookup)
	defer multiClient.Close()

	blocker := blockerTestNew([]string{"ads.example.com"}, nil, nil)
	multiClient.SetBlocker(blocker)

	blockActionsChannel := make(chan []*BlockAction, 16)
	unsub := multiClient.AddBlockActionCallback(func(blockActions []*BlockAction) {
		blockActionsChannel <- blockActions
	})
	defer unsub()

	nextBlockActions := func() []*BlockAction {
		select {
		case blockActions := <-blockActionsChannel:
			return blockActions
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for block actions")
			return nil
		}
	}

	source := SourceId(NewId())
	blockedPacket := testingUdp4Packet("10.0.0.5", "127.0.0.9", 443, []byte("hello"))
	cleanPacket := testingUdp4Packet("10.0.0.5", "127.0.0.8", 443, []byte("hello"))

	// blocked via its own server name (sub.ads.example.com suffix-matches
	// the blocked base)
	success := multiClient.SendPacket(source, protocol.ProvideMode_Network, blockedPacket, 0)
	AssertEqual(t, false, success)

	blockActions := nextBlockActions()
	AssertEqual(t, 1, len(blockActions))
	AssertEqual(t, true, blockActions[0].Block)
	AssertEqual(t, true, blockActions[0].Blocker)

	packetStats := multiClient.PacketStats()
	AssertEqual(t, int64(1), packetStats.BlockEgressPacketCount)

	// the clean destination shares a name (cdn.example.com) but none of ITS
	// names are blocked — not blocked
	multiClient.SendPacket(source, protocol.ProvideMode_Network, cleanPacket, 0)
	packetStats = multiClient.PacketStats()
	AssertEqual(t, int64(1), packetStats.BlockEgressPacketCount)

	// drain the clean destination's (allow) action from the epoch: every
	// routing decision is surfaced while a callback is registered
	blockActions = nextBlockActions()
	AssertEqual(t, 1, len(blockActions))
	AssertEqual(t, false, blockActions[0].Block)
	AssertEqual(t, false, blockActions[0].Blocker)

	// a user un-block override on the blocked host wins over the blocker,
	// and the allow-classified flow egresses REMOTELY (local stays false)
	unblockOverride := &BlockActionOverride{
		OverrideId:    NewId(),
		Hosts:         []string{"**.ads.example.com"},
		BlockOverride: &BlockOverride{Block: false},
	}
	multiClient.SetBlockActionOverrides([]*BlockActionOverride{unblockOverride})

	multiClient.SendPacket(source, protocol.ProvideMode_Network, blockedPacket, 0)

	blockActions = nextBlockActions()
	AssertEqual(t, 1, len(blockActions))
	AssertEqual(t, false, blockActions[0].Block)
	AssertEqual(t, false, blockActions[0].Local)
	AssertEqual(t, true, blockActions[0].Blocker)
	AssertEqual(t, true, blockActions[0].BlockOverrideId != nil)
	AssertEqual(t, unblockOverride.OverrideId, *blockActions[0].BlockOverrideId)

	// the un-blocked flow did not land in the block or local counters
	packetStats = multiClient.PacketStats()
	AssertEqual(t, int64(1), packetStats.BlockEgressPacketCount)
	AssertEqual(t, int64(0), packetStats.LocalEgressPacketCount)
}
