package connect

import (
	"context"
	"net"
	"net/netip"
	"testing"
	"time"
)

func dnsAffinityTestParent() *RemoteUserNatMultiClient {
	settings := DefaultMultiClientSettings()
	// These fixtures test the legacy DNS-donor mechanism. Production leaves
	// hard fresh-flow inheritance off unless an explicit pin requests it.
	settings.FreshFlowAffinity = true
	settings.Log = NewNoopLogger()
	return &RemoteUserNatMultiClient{
		settings:                   settings,
		log:                        settings.Log,
		ip4PathUpdates:             map[Ip4Path]*multiClientChannelUpdate{},
		ip6PathUpdates:             map[Ip6Path]*multiClientChannelUpdate{},
		destinationServiceFailures: map[destinationServiceFailureKey]destinationServiceFailure{},
		dnsExitHints:               map[string]dnsExitHint{},
		dnsAddressExitHints:        map[netip.Addr]dnsExitHint{},
	}
}

func bindDnsAffinityTestFlow(
	parent *RemoteUserNatMultiClient,
	client *multiClientChannel,
	path *IpPath,
) *multiClientChannelUpdate {
	update := newMultiClientChannelUpdate(context.Background(), path)
	update.client.Store(client)
	if path.Version == 4 {
		parent.ip4PathUpdates[path.ToIp4Path()] = update
	} else {
		parent.ip6PathUpdates[path.ToIp6Path()] = update
	}
	return update
}

func TestDnsExitHintDoesNotHardBindOrdinaryFreshFlowByDefault(t *testing.T) {
	parent := dnsAffinityTestParent()
	parent.settings.FreshFlowAffinity = false
	exit := &multiClientChannel{ctx: context.Background(), settings: parent.settings}
	address := netip.MustParseAddr("203.0.113.24")
	affinityName := affinityNameForServerName("video.example.test")
	dohPath := destinationServiceTestPath(4, "1.1.1.1", 443)
	dohUpdate := bindDnsAffinityTestFlow(parent, exit, dohPath)
	defer dohUpdate.Close()
	if parent.bindDnsResultToExit(dohPath, "video.example.test", []netip.Addr{address}) {
		t.Fatal("disabled fresh-flow affinity retained a DNS-exit hint")
	}
	if len(parent.dnsExitHints) != 0 || len(parent.dnsAddressExitHints) != 0 {
		t.Fatal("disabled DNS-exit affinity retained provider channel state")
	}

	// Inject one legacy hint to prove the read path independently honors the
	// default-off policy (including state restored by an older build).
	parent.dnsAddressExitHints[address] = dnsExitHint{
		client: exit, affinityName: affinityName, createTime: time.Now(),
	}

	path := destinationServiceTestPath(4, address.String(), 443)
	update := newMultiClientChannelUpdate(context.Background(), path)
	defer update.Close()
	parent.stateLock.Lock()
	bound := parent.inheritDnsExitHintWithLock(update, path, []*IpPath{{ServerName: affinityName}})
	parent.stateLock.Unlock()
	if bound || update.client.Load() != nil {
		t.Fatal("ordinary fresh TLS flow hard-bound to its resolver exit")
	}

	update.pinned = true
	parent.stateLock.Lock()
	bound = parent.inheritDnsExitHintWithLock(update, path, []*IpPath{{ServerName: affinityName}})
	parent.stateLock.Unlock()
	if !bound || update.client.Load() != exit {
		t.Fatal("explicitly pinned flow did not retain DNS-exit affinity")
	}
}

func TestDisablingFreshFlowAffinityReleasesDnsChannelHints(t *testing.T) {
	parent := dnsAffinityTestParent()
	exit := &multiClientChannel{ctx: context.Background(), settings: parent.settings}
	parent.dnsExitHints["example.test"] = dnsExitHint{
		client: exit, affinityName: "example.test", createTime: time.Now(),
	}
	parent.dnsAddressExitHints[netip.MustParseAddr("203.0.113.29")] = dnsExitHint{
		client: exit, affinityName: "example.test", createTime: time.Now(),
	}

	override := ReliabilitySettingsFrom(parent.settings)
	override.FreshFlowAffinity = false
	parent.SetReliabilitySettings(override)
	if len(parent.dnsExitHints) != 0 || len(parent.dnsAddressExitHints) != 0 {
		t.Fatal("disabling hard fresh affinity retained DNS provider references")
	}
}

func TestDnsAnswerUsesTheExitThatResolvedItsAddress(t *testing.T) {
	parent := dnsAffinityTestParent()
	settings := parent.settings
	exitForFirst := &multiClientChannel{ctx: context.Background(), settings: settings}
	exitForSecond := &multiClientChannel{ctx: context.Background(), settings: settings}
	domain := "smtp.example.test"
	firstAddress := netip.MustParseAddr("203.0.113.25")
	secondAddress := netip.MustParseAddr("203.0.113.26")

	dohFirst := destinationServiceTestPath(4, "1.1.1.1", 443)
	dohFirst.SourceIp = net.ParseIP("169.254.0.10")
	dohFirst.SourcePort = 53001
	updateFirst := bindDnsAffinityTestFlow(parent, exitForFirst, dohFirst)
	defer updateFirst.Close()
	if !parent.bindDnsResultToExit(dohFirst, domain, []netip.Addr{firstAddress}) {
		t.Fatal("first A answer was not joined to its DoH exit")
	}

	dohSecond := destinationServiceTestPath(4, "8.8.8.8", 443)
	dohSecond.SourceIp = net.ParseIP("169.254.0.10")
	dohSecond.SourcePort = 53002
	updateSecond := bindDnsAffinityTestFlow(parent, exitForSecond, dohSecond)
	defer updateSecond.Close()
	if !parent.bindDnsResultToExit(dohSecond, domain, []netip.Addr{secondAddress}) {
		t.Fatal("second A answer was not joined to its DoH exit")
	}

	affinityPaths := []*IpPath{{ServerName: affinityNameForServerName(domain)}}
	firstMail := destinationServiceTestPath(4, firstAddress.String(), 465)
	firstMailUpdate := newMultiClientChannelUpdate(context.Background(), firstMail)
	defer firstMailUpdate.Close()
	parent.stateLock.Lock()
	bound := parent.inheritDnsExitHintWithLock(firstMailUpdate, firstMail, affinityPaths)
	parent.stateLock.Unlock()
	if !bound || firstMailUpdate.client.Load() != exitForFirst {
		t.Fatal("first SMTP address did not inherit its resolver exit")
	}

	secondMail := destinationServiceTestPath(4, secondAddress.String(), 465)
	secondMailUpdate := newMultiClientChannelUpdate(context.Background(), secondMail)
	defer secondMailUpdate.Close()
	parent.stateLock.Lock()
	bound = parent.inheritDnsExitHintWithLock(secondMailUpdate, secondMail, affinityPaths)
	parent.stateLock.Unlock()
	if !bound || secondMailUpdate.client.Load() != exitForSecond {
		t.Fatal("second SMTP address did not inherit its resolver exit")
	}
}

func TestDnsExitHintYieldsToDestinationPortFailure(t *testing.T) {
	parent := dnsAffinityTestParent()
	settings := parent.settings
	resolvedExit := &multiClientChannel{ctx: context.Background(), settings: settings}
	alternateExit := &multiClientChannel{ctx: context.Background(), settings: settings}
	domain := "smtp.example.test"
	address := netip.MustParseAddr("203.0.113.58")

	dohResolved := destinationServiceTestPath(4, "1.1.1.1", 443)
	dohResolved.SourceIp = net.ParseIP("169.254.0.11")
	dohResolved.SourcePort = 53101
	resolvedUpdate := bindDnsAffinityTestFlow(parent, resolvedExit, dohResolved)
	defer resolvedUpdate.Close()
	if !parent.bindDnsResultToExit(dohResolved, domain, []netip.Addr{address}) {
		t.Fatal("DNS answer was not bound")
	}

	// A later answer for the same name supplies a safe domain-level fallback,
	// while the exact first address remains tied to resolvedExit.
	dohAlternate := destinationServiceTestPath(4, "8.8.8.8", 443)
	dohAlternate.SourceIp = net.ParseIP("169.254.0.11")
	dohAlternate.SourcePort = 53102
	alternateUpdate := bindDnsAffinityTestFlow(parent, alternateExit, dohAlternate)
	defer alternateUpdate.Close()
	if !parent.bindDnsResultToExit(dohAlternate, domain, []netip.Addr{netip.MustParseAddr("203.0.113.87")}) {
		t.Fatal("alternate DNS answer was not bound")
	}

	mail := destinationServiceTestPath(4, address.String(), 587)
	parent.stateLock.Lock()
	parent.recordDestinationServiceFailureWithLock(resolvedExit, mail)
	parent.stateLock.Unlock()

	update := newMultiClientChannelUpdate(context.Background(), mail)
	defer update.Close()
	parent.stateLock.Lock()
	bound := parent.inheritDnsExitHintWithLock(
		update,
		mail,
		[]*IpPath{{ServerName: affinityNameForServerName(domain)}},
	)
	parent.stateLock.Unlock()
	if !bound || update.client.Load() != alternateExit {
		t.Fatal("destination-specific failure did not move the DNS-affine SMTP flow to the alternate exit")
	}
}

func TestDnsExitHintDoesNotCrossSharedAddressHostnames(t *testing.T) {
	parent := dnsAffinityTestParent()
	settings := parent.settings
	exitA := &multiClientChannel{ctx: context.Background(), settings: settings}
	exitB := &multiClientChannel{ctx: context.Background(), settings: settings}
	sharedAddress := netip.MustParseAddr("203.0.113.200")

	dohA := destinationServiceTestPath(4, "1.1.1.1", 443)
	dohA.SourceIp = net.ParseIP("169.254.0.12")
	dohA.SourcePort = 53201
	updateA := bindDnsAffinityTestFlow(parent, exitA, dohA)
	defer updateA.Close()
	if !parent.bindDnsResultToExit(dohA, "mail.alpha.test", []netip.Addr{sharedAddress}) {
		t.Fatal("first shared-address answer was not bound")
	}

	dohB := destinationServiceTestPath(4, "8.8.8.8", 443)
	dohB.SourceIp = net.ParseIP("169.254.0.12")
	dohB.SourcePort = 53202
	updateB := bindDnsAffinityTestFlow(parent, exitB, dohB)
	defer updateB.Close()
	if !parent.bindDnsResultToExit(dohB, "mail.beta.test", []netip.Addr{sharedAddress}) {
		t.Fatal("second shared-address answer was not bound")
	}

	mailA := destinationServiceTestPath(4, sharedAddress.String(), 465)
	mailUpdate := newMultiClientChannelUpdate(context.Background(), mailA)
	defer mailUpdate.Close()
	parent.stateLock.Lock()
	bound := parent.inheritDnsExitHintWithLock(
		mailUpdate,
		mailA,
		[]*IpPath{{ServerName: affinityNameForServerName("mail.alpha.test")}},
	)
	parent.stateLock.Unlock()
	if !bound || mailUpdate.client.Load() != exitA {
		t.Fatal("shared CDN address leaked the newer hostname's resolver exit into the first hostname")
	}
}
