package connect

import (
	"context"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// cnnDashPath models the two independent HTTP/1.1 TCP connections involved in
// the field failure: one fetches the DASH manifest and one fetches a media
// range. Both names collapse to cnn.com affinity in production.
func cnnDashPath(sourcePort int, destination string) *IpPath {
	return &IpPath{
		Version:         4,
		Protocol:        IpProtocolTcp,
		SourceIp:        net.ParseIP("10.44.0.2").To4(),
		SourcePort:      sourcePort,
		DestinationIp:   net.ParseIP(destination).To4(),
		DestinationPort: 443,
		Syn:             true,
	}
}

func bindH1MediaFlow(
	parent *RemoteUserNatMultiClient,
	client *multiClientChannel,
	path *IpPath,
	affinityKey Ip4Path,
	now time.Time,
) *multiClientChannelUpdate {
	update := newMultiClientChannelUpdate(parent.ctx, path)
	update.client.Store(client)
	update.receivedInbound.Store(true)
	update.activityTime = now
	pathKey := path.ToIp4Path()
	update.affinityIp4Paths[affinityKey] = true
	parent.ip4PathUpdates[pathKey] = update
	parent.flowUpdates[update] = true
	paths := parent.affinityIp4Paths[affinityKey]
	if paths == nil {
		paths = map[Ip4Path]time.Time{}
		parent.affinityIp4Paths[affinityKey] = paths
	}
	paths[pathKey] = now
	parent.bindClientFlow(update, client)
	return update
}

// This is the root-cause regression for the Android CNN capture. A provider
// passed admission, established the browser's H1 connections, then stopped
// returning transfer traffic. Quarantine used to preserve both split-TCP
// flows and their affinity records, so the player kept its poisoned manifest
// connection and fresh media handshakes followed the same benched exit.
//
// Recovery must be one atomic policy transition from the application's point
// of view: detach every established H1 flow, reset it toward the browser, and
// forget every DNS/site/app affinity reference to the failed exit. The next
// browser connection may then bind to a healthy provider immediately.
func TestH1DashBlackholeQuarantineResetsAndRebinds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultMultiClientSettings()
	settings.QuicRebindOnExitLoss = true
	// Keep the legacy DNS inheritance path enabled in this test so the CNN
	// quarantine remediation remains covered even though production now races
	// ordinary fresh flows by default.
	settings.FreshFlowAffinity = true
	parent := flowReaperTestParent(ctx, settings)
	parent.reliabilityMetrics = newReliabilityMetrics()
	parent.removalReceiveQueue = make(chan receivePacket, 8)
	parent.dnsExitHints = map[string]dnsExitHint{}
	parent.dnsAddressExitHints = map[netip.Addr]dnsExitHint{}

	failed := &multiClientChannel{ctx: ctx, settings: settings}
	healthy := &multiClientChannel{ctx: ctx, settings: settings}
	parent.rebindCandidatesFunc = func(client *multiClientChannel) []*multiClientChannel {
		if client != failed {
			t.Fatalf("candidate gather for %p, want failed exit %p", client, failed)
		}
		return []*multiClientChannel{healthy}
	}

	affinityName := affinityNameForServerName("gcp.amer-free.prd.media.cnn.com")
	if affinityName != affinityNameForServerName("www.cnn.com") {
		t.Fatalf("CNN page and media names do not share affinity: %q", affinityName)
	}
	affinityKey := (&IpPath{ServerName: affinityName}).ToIp4Path()
	now := time.Now()
	manifest := bindH1MediaFlow(parent, failed, cnnDashPath(44001, "198.51.100.20"), affinityKey, now)
	segment := bindH1MediaFlow(parent, failed, cnnDashPath(44002, "198.51.100.21"), affinityKey, now)

	manifestAddress := netip.MustParseAddr("198.51.100.20")
	parent.dnsExitHints[affinityName] = dnsExitHint{client: failed, affinityName: affinityName, createTime: now}
	parent.dnsAddressExitHints[manifestAddress] = dnsExitHint{client: failed, affinityName: affinityName, createTime: now}
	parent.appPinClients = map[string]*multiClientChannel{"cnn-app": failed}

	stampDonorReceived(failed)
	failed.setQuarantined(blackholeNoReceiveAck)
	rebound, _, remaining := parent.migrateClientFlows(failed, "bench")
	if rebound != 0 {
		t.Fatalf("rebound %d H1 flows, want 0: split TCP cannot migrate", rebound)
	}
	if remaining != 0 {
		t.Fatalf("failed exit retained %d flows after confirmed blackhole", remaining)
	}

	for name, update := range map[string]*multiClientChannelUpdate{"manifest": manifest, "segment": segment} {
		if !update.IsDone() {
			t.Errorf("%s flow was not retired", name)
		}
		if update.client.Load() != nil {
			t.Errorf("%s flow still points at failed provider", name)
		}
		if parent.ip4PathUpdates[update.ipPath.ToIp4Path()] == update {
			t.Errorf("%s flow remains routable after reset", name)
		}
	}
	if _, ok := parent.affinityIp4Paths[affinityKey]; ok {
		t.Fatal("CNN site affinity still contains the quarantined exit")
	}
	if _, ok := parent.dnsExitHints[affinityName]; ok {
		t.Fatal("CNN DNS name hint still points at the quarantined exit")
	}
	if _, ok := parent.dnsAddressExitHints[manifestAddress]; ok {
		t.Fatal("CNN DNS address hint still points at the quarantined exit")
	}
	if _, ok := parent.appPinClients["cnn-app"]; ok {
		t.Fatal("CNN app pin still points at the quarantined exit")
	}

	for i := 0; i < 2; i++ {
		select {
		case reset := <-parent.removalReceiveQueue:
			resetPath, err := ParseIpPath(reset.Packet)
			if err != nil || !resetPath.Rst {
				t.Fatalf("reset %d is not a TCP RST: path=%#v err=%v", i, resetPath, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("browser received only %d of 2 H1 reset signals", i)
		}
	}

	metrics := parent.ReliabilityMetrics()
	if metrics.QuarantineTcpResets != 2 {
		t.Fatalf("quarantine TCP resets = %d, want 2", metrics.QuarantineTcpResets)
	}
	if metrics.QuarantineAffinityInvalidations < 4 {
		t.Fatalf("affinity invalidations = %d, want site+name+address+app", metrics.QuarantineAffinityInvalidations)
	}

	// A fresh resolver result arrives over the healthy provider. The browser's
	// retry must inherit that provider, never the quarantined donor retained by
	// the old flow generation.
	retryAddress := netip.MustParseAddr("198.51.100.22")
	parent.dnsExitHints[affinityName] = dnsExitHint{client: healthy, affinityName: affinityName, createTime: time.Now()}
	parent.dnsAddressExitHints[retryAddress] = dnsExitHint{client: healthy, affinityName: affinityName, createTime: time.Now()}
	retryPath := cnnDashPath(44003, retryAddress.String())
	retry := newMultiClientChannelUpdate(ctx, retryPath)
	defer retry.Close()
	parent.stateLock.Lock()
	bound := parent.inheritDnsExitHintWithLock(retry, retryPath, []*IpPath{{ServerName: affinityName}})
	parent.stateLock.Unlock()
	if !bound || retry.client.Load() != healthy {
		t.Fatal("CNN retry did not bind to the healthy resolver exit")
	}
}

// The Bloomberg field failure is deliberately the opposite of the CNN
// blackhole above. Chrome decrypted an HTTP denial page, but Connect received
// an ordinary TLS application-data response. The tunnel cannot see an HTTP
// status inside that ciphertext, and must not infer one from packet shape.
//
// This is the root-cause boundary regression: drive six opaque TLS response
// bursts through one already-bound flow, matching the Android Bloomberg trace
// where Chrome retried six media Fetches on the same H2 connection. They are
// counted and delivered as network progress, but none is a new transport flow
// and therefore none can invoke MultiClient placement. The same aged send
// window is a positive no-receive verdict before the responses; afterward it
// must produce no verdict, quarantine, H1 teardown, or rebind. A
// Bloomberg-specific 403/challenge classifier belongs outside the encrypted
// tunnel in an HTTP health observer, not in this path.
func TestH1OpaqueTlsResponseIsProgressNotBlackhole(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultMultiClientSettings()
	parent := flowReaperTestParent(ctx, settings)
	parent.securityPolicy = DisableSecurityPolicy()
	parent.packetStatsCounters = &packetStatsCounters{}
	parent.reliabilityMetrics = newReliabilityMetrics()

	delivered := 0
	parent.SetReceivePacketCallback(func(
		source TransferPath,
		provideMode protocol.ProvideMode,
		ipPath *IpPath,
		packet []byte,
	) {
		delivered++
	})

	now := time.Now()
	exit := &multiClientChannel{
		ctx: ctx,
		log: NewNoopLogger(),
		args: &multiClientChannelArgs{DestinationStats: DestinationStats{
			ReputationFailures: []string{"bloomberg"},
		}},
		settings:                  settings,
		packetStats:               &clientWindowStats{log: NewNoopLogger()},
		eventBuckets:              []*multiClientEventBucket{},
		ip4DestinationSourceCount: map[Ip4Path]map[Ip4Path]int{},
		ip6DestinationSourceCount: map[Ip6Path]map[Ip6Path]int{},
	}
	exit.clientReceivePacketCallback = parent.clientReceivePacket

	manifestPath := &IpPath{
		Version:         4,
		Protocol:        IpProtocolTcp,
		SourceIp:        net.ParseIP("10.44.0.2").To4(),
		SourcePort:      44201,
		DestinationIp:   net.ParseIP("198.51.100.40").To4(),
		DestinationPort: 443,
		Syn:             true,
	}
	affinityName := affinityNameForServerName("www.bloomberg.com")
	affinityKey := (&IpPath{ServerName: affinityName}).ToIp4Path()
	manifest := bindH1MediaFlow(parent, exit, manifestPath, affinityKey, now)

	// Model a mature no-receive window. The send acknowledgements prove the
	// provider is alive, but without any returned IP packet the soft receive
	// verdict is otherwise eligible.
	exit.packetStats.firstSendNackTime = now.Add(-settings.BlackholeReceiveTimeout - time.Second)
	exit.packetStats.sendAckCount = 4
	exit.packetStats.sendAckByteCount = 1200
	reason, held := blackholeReasonFromStats(
		now,
		exit.packetStats,
		settings.BlackholeTimeout,
		settings.BlackholeReceiveTimeout,
		settings.BlackholeConnectTimeout,
		blackholeGates{},
	)
	if reason != blackholeNoReceiveAck || held != blackholeNone {
		t.Fatalf("positive control: silent return path reason=%q held=%q, want %q/none", reason, held, blackholeNoReceiveAck)
	}

	// A syntactically valid TLS 1.2 application-data record with opaque fixed
	// ciphertext. There is intentionally no HTTP status or challenge text in
	// this fixture: those bytes exist only after Chrome decrypts the record.
	opaqueTlsRecord := []byte{
		0x17, 0x03, 0x03, 0x00, 0x10,
		0x8c, 0x63, 0x2d, 0xf1, 0x44, 0xa7, 0x09, 0xbe,
		0x51, 0x98, 0x26, 0xd4, 0x70, 0x3a, 0xef, 0x15,
	}
	returnPacket := ipOosTcpPacket(manifestPath.Reverse(), tcpFlagAck|tcpFlagPsh, opaqueTlsRecord)
	const sameConnectionResponseCount = 6
	for i := 0; i < sameConnectionResponseCount; i++ {
		frame := RequireToFrameWithDefaultProtocolVersion(&protocol.IpPacketFromProvider{
			IpPacket: &protocol.IpPacket{PacketBytes: returnPacket},
		})
		exit.clientReceive(
			TransferPath{},
			[]*protocol.Frame{frame},
			Peer{ProvideMode: protocol.ProvideMode_Public, TransportType: TransportTypeH1},
		)
		MessagePoolReturn(frame.MessageBytes)
	}

	if delivered != sameConnectionResponseCount {
		t.Fatalf("opaque TLS responses delivered %d times, want %d", delivered, sameConnectionResponseCount)
	}
	wantReceivedBytes := ByteCount(sameConnectionResponseCount * len(returnPacket))
	if exit.packetStats.receiveAckCount != sameConnectionResponseCount || exit.packetStats.receiveAckByteCount != wantReceivedBytes {
		t.Fatalf(
			"opaque TLS response accounting = %d/%dB, want %d/%dB",
			exit.packetStats.receiveAckCount,
			exit.packetStats.receiveAckByteCount,
			sameConnectionResponseCount,
			wantReceivedBytes,
		)
	}
	if parent.ip4PathUpdates[manifestPath.ToIp4Path()] != manifest {
		t.Fatal("same-connection TLS retries created or rebound a transport flow")
	}

	reason, held = blackholeReasonFromStats(
		now,
		exit.packetStats,
		settings.BlackholeTimeout,
		settings.BlackholeReceiveTimeout,
		settings.BlackholeConnectTimeout,
		blackholeGates{},
	)
	if reason != blackholeNone || held != blackholeNone {
		t.Fatalf("returned TLS traffic was classified as a network blackhole: reason=%q held=%q", reason, held)
	}
	if action := verdictAction(
		reason,
		settings.SoftVerdictDemote,
		1,
		time.Time{},
		now,
		settings.StatsWindowKeepUnhealthyDuration,
		false,
	); action != verdictActionNone {
		t.Fatalf("returned TLS traffic selected verdict action %d, want none", action)
	}
	if manifest.IsDone() || manifest.client.Load() != exit {
		t.Fatal("established Bloomberg H1 flow moved or closed after an opaque TLS response")
	}
	if paths := parent.affinityIp4Paths[affinityKey]; len(paths) != 1 {
		t.Fatalf("Bloomberg affinity changed after an opaque TLS response: %+v", paths)
	}
	if quarantineReason, _ := exit.quarantineState(); quarantineReason != blackholeNone {
		t.Fatalf("exit quarantined after an opaque TLS response: %q", quarantineReason)
	}
	select {
	case reset := <-parent.removalReceiveQueue:
		t.Fatalf("browser received an unexpected teardown packet after opaque TLS progress: %x", reset.Packet)
	default:
	}
}

// Even before the parent has completed its affinity cleanup, a newly-created
// handshake must never follow a quarantined donor. This closes the small
// setQuarantined -> parent callback race and deliberately supersedes the old
// false-positive group-follow behavior for fresh flows.
func TestFreshHandshakeNeverFollowsQuarantinedAffinityDonor(t *testing.T) {
	parent := bindFlowTestParent()
	parent.ip4PathUpdates = map[Ip4Path]*multiClientChannelUpdate{}
	donor := bindFlowTestChannel(parent)
	stampDonorReceived(donor)
	donor.setQuarantined(blackholeNoReceiveAck)

	path := cnnDashPath(44100, "198.51.100.30")
	pathKey := path.ToIp4Path()
	donorUpdate := newMultiClientChannelUpdate(context.Background(), path)
	defer donorUpdate.Close()
	donorUpdate.client.Store(donor)
	parent.ip4PathUpdates[pathKey] = donorUpdate

	fresh := newMultiClientChannelUpdate(context.Background(), cnnDashPath(44101, "198.51.100.31"))
	defer fresh.Close()
	parent.stateLock.Lock()
	verdict, _ := parent.inheritAffinityClient4WithLock(fresh, map[Ip4Path]time.Time{pathKey: time.Now()})
	parent.stateLock.Unlock()
	if fresh.client.Load() != nil || verdict == donorQuarantineFollowed {
		t.Fatal("a fresh H1 handshake followed the quarantined affinity donor")
	}
}
