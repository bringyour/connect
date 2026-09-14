package connect

import (
	"context"
	"net"
	"testing"
	"time"
)

// The donor-affinity contract this file pins was the deciding argument in a
// merge conflict: a pre-merge fix had update.Close() clear the committed
// client ("a retired flow must not retain its selected provider"), and the
// reliability checkpoint removed that clear. The removal is deliberate and
// load-bearing — the affinity donor read deliberately looks at RETIRED flows'
// updates, IsDone-guarded, so the next flow to the same site inherits the
// exit the site was just using (providers terminate tcp, so keeping a site's
// flows on one exit is what keeps its connections working). Re-adding the
// clear would silently break site affinity for every reconnect-after-close,
// which is most of them: a browser reopening a connection to the site it just
// closed one to.
//
// The retention is BOUNDED, not indefinite: every flow spawned through
// sendUpdate has a reaper goroutine that, once the update is done, removes it
// from ip4PathUpdates and its affinity groups and nils the committed client —
// atomically under the parent stateLock, so there is no observable
// registered-but-cleared state. The donor window is therefore "from Close
// until the reaper runs". These tests construct the retired flow's
// registration directly (the suite's established fixture pattern) rather
// than through sendUpdate, so the reaper does not race the assertion — an
// earlier version used sendUpdate and lost that race under -race scheduling.

func donorAffinityPath(sourcePort int) *IpPath {
	return &IpPath{
		Version:         4,
		Protocol:        IpProtocolTcp,
		SourceIp:        net.ParseIP("10.20.30.40"),
		SourcePort:      sourcePort,
		DestinationIp:   net.ParseIP("198.51.100.7"),
		DestinationPort: 443,
	}
}

// registerDonorFlow installs a committed flow record and its affinity-group
// membership exactly as sendUpdate would, minus the reaper goroutine, so the
// tests control when (whether) the record is reaped.
func registerDonorFlow(mc *RemoteUserNatMultiClient, ipPath *IpPath, exit *multiClientChannel) *multiClientChannelUpdate {
	update := newMultiClientChannelUpdate(mc.ctx, ipPath)
	update.client.Store(exit)
	ip4Path := ipPath.ToIp4Path()
	mc.stateLock.Lock()
	defer mc.stateLock.Unlock()
	mc.ip4PathUpdates[ip4Path] = update
	for _, affinityIpPath := range mc.affinityIpPathsWithLock(ipPath) {
		affinityIp4Path := affinityIpPath.ToIp4Path()
		update.affinityIp4Paths[affinityIp4Path] = true
		paths, ok := mc.affinityIp4Paths[affinityIp4Path]
		if !ok {
			paths = map[Ip4Path]time.Time{}
			mc.affinityIp4Paths[affinityIp4Path] = paths
		}
		paths[ip4Path] = time.Now()
	}
	if updates, ok := mc.clientUpdates[exit]; ok {
		updates[update] = true
	} else {
		mc.clientUpdates[exit] = map[*multiClientChannelUpdate]bool{update: true}
	}
	return update
}

func donorAffinityClient(ctx context.Context) *RemoteUserNatMultiClient {
	mc := &RemoteUserNatMultiClient{
		ctx: ctx,
		settings: &MultiClientSettings{
			DestinationAffinity:    true,
			FreshFlowAffinity:      true,
			SequenceIdleTimeout:    10 * time.Minute,
			TcpSequenceIdleTimeout: 10 * time.Minute,
		},
		ip4PathUpdates:   map[Ip4Path]*multiClientChannelUpdate{},
		ip6PathUpdates:   map[Ip6Path]*multiClientChannelUpdate{},
		affinityIp4Paths: map[Ip4Path]map[Ip4Path]time.Time{},
		affinityIp6Paths: map[Ip6Path]map[Ip6Path]time.Time{},
		clientUpdates:    map[*multiClientChannel]map[*multiClientChannelUpdate]bool{},
	}
	mc.config.Store(&multiClientConfig{})
	return mc
}

// TestDonorAffinityRetiredFlowDonatesItsExit: a flow that commits to an exit
// and then closes must still donate that exit to the next flow to the same
// site. This is the exact property update.Close clearing the committed client
// would destroy.
func TestDonorAffinityRetiredFlowDonatesItsExit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mc := donorAffinityClient(ctx)
	exit := &multiClientChannel{ctx: ctx}

	// flow 1 commits to the exit, then retires (the connection closed). The
	// registration is built directly so the flow reaper does not race the
	// donor read below (see the file doc).
	flow1 := donorAffinityPath(40001)
	update1 := registerDonorFlow(mc, flow1, exit)
	update1.Close()

	if got := update1.client.Load(); got != exit {
		t.Fatal("a retired flow's update must retain its last exit: the donor read depends on it, and clearing it silently breaks site affinity for every reconnect-after-close")
	}

	// flow 2 to the same site inherits the retired flow's exit
	update2, _, current2 := mc.sendUpdate(donorAffinityPath(40002), flowPin{})
	if current2 != exit || update2.client.Load() != exit {
		t.Fatalf(
			"a new flow to the same site must inherit the retired flow's exit (got current=%p committed=%p want %p)",
			current2, update2.client.Load(), exit,
		)
	}
}

// The production policy keeps the same measurement key but does not build or
// consume the reverse donor index for an ordinary fresh flow. This drives the
// complete sendUpdate path, not only the donor helper.
func TestDonorAffinityDefaultFreshFlowReachesRace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mc := donorAffinityClient(ctx)
	mc.settings.FreshFlowAffinity = false
	exit := &multiClientChannel{ctx: ctx}
	flow1 := donorAffinityPath(40011)
	update1 := registerDonorFlow(mc, flow1, exit)
	defer update1.Close()

	update2, _, current2 := mc.sendUpdate(donorAffinityPath(40012), flowPin{})
	defer update2.Close()
	if current2 != nil || update2.client.Load() != nil {
		t.Fatal("ordinary fresh flow bypassed the provider race through a donor")
	}
	if len(update2.affinityIp4Paths) == 0 {
		t.Fatal("fresh flow lost its bounded affinity-performance key")
	}

	for key := range update2.affinityIp4Paths {
		if _, retained := mc.affinityIp4Paths[key][update2.ipPath.ToIp4Path()]; retained {
			t.Fatal("default-off hard affinity retained a reverse donor-index entry")
		}
	}

	pinned, _, pinnedCurrent := mc.sendUpdate(
		donorAffinityPath(40013),
		flowPin{site: true},
	)
	defer pinned.Close()
	if pinnedCurrent != exit || pinned.client.Load() != exit {
		t.Fatal("explicit host pin did not opt back into hard site affinity")
	}
}

func TestFreshFlowAffinityRuntimeToggleRebuildsOnlyNeededDonorIndexes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mc := donorAffinityClient(ctx)
	exit := &multiClientChannel{ctx: ctx}
	ordinary := registerDonorFlow(mc, donorAffinityPath(40021), exit)
	defer ordinary.Close()
	pinned := registerDonorFlow(mc, donorAffinityPath(40022), exit)
	defer pinned.Close()
	pinned.pinned = true

	override := ReliabilitySettingsFrom(mc.settings)
	override.FreshFlowAffinity = false
	mc.SetReliabilitySettings(override)
	foundOrdinary := false
	foundPinned := false
	for _, paths := range mc.affinityIp4Paths {
		if _, ok := paths[ordinary.ipPath.ToIp4Path()]; ok {
			foundOrdinary = true
		}
		if _, ok := paths[pinned.ipPath.ToIp4Path()]; !ok {
			continue
		}
		foundPinned = true
	}
	if foundOrdinary {
		t.Fatal("disabled hard affinity retained an ordinary donor index")
	}
	if !foundPinned {
		t.Fatal("disabled ordinary affinity discarded an explicit pin index")
	}

	// Clearing the override restores the constructed legacy A/B setting and
	// immediately rebuilds current ordinary donors.
	mc.SetReliabilitySettings(nil)
	foundOrdinary = false
	for _, paths := range mc.affinityIp4Paths {
		if _, ok := paths[ordinary.ipPath.ToIp4Path()]; ok {
			foundOrdinary = true
		}
	}
	if !foundOrdinary {
		t.Fatal("reenabling hard affinity did not rebuild current donor indexes")
	}
}

// TestDonorAffinityDeadExitIsNotInherited is the guard on the guard: the
// retained pointer is only usable through the IsDone check, so a dead exit is
// never donated. Retention without this check would hand new flows a corpse.
func TestDonorAffinityDeadExitIsNotInherited(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mc := donorAffinityClient(ctx)
	exitCtx, cancelExit := context.WithCancel(context.Background())
	exit := &multiClientChannel{ctx: exitCtx}

	flow1 := donorAffinityPath(40001)
	update1 := registerDonorFlow(mc, flow1, exit)
	update1.Close()

	// the exit dies after the flow retired
	cancelExit()

	update2, _, current2 := mc.sendUpdate(donorAffinityPath(40002), flowPin{})
	if current2 == exit || update2.client.Load() == exit {
		t.Fatal("a dead exit must never be donated: the retention is only safe because the donor read is IsDone-guarded")
	}
}

// TestDonorAffinityWarnedExitIsNotInherited pins the second eligibility gate:
// a warned exit (dial-starved, quarantine-scarred) is refused as a donor even
// while alive, so a site does not keep re-boarding an exit the window is
// steering away from.
func TestDonorAffinityWarnedExitIsNotInherited(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mc := donorAffinityClient(ctx)
	exit := &multiClientChannel{ctx: ctx}
	func() {
		exit.stateLock.Lock()
		defer exit.stateLock.Unlock()
		exit.warning = true
	}()

	flow1 := donorAffinityPath(40001)
	update1 := registerDonorFlow(mc, flow1, exit)
	update1.Close()

	update2, _, current2 := mc.sendUpdate(donorAffinityPath(40002), flowPin{})
	if current2 == exit || update2.client.Load() == exit {
		t.Fatal("a warned exit must be refused as a donor")
	}
}
