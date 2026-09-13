package connect

import (
	"fmt"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

// The directory refusals, the remaining policy edges and the address
// normalization every entry point shares (EXTENDER.md B5, E1).
//
// The transitions themselves are pinned in net_extender_directory_test.go.
// What is here is the other half: the inputs that must be refused rather than
// applied, the bounds that keep the directory finite, and the rule that one
// address is one entry however its family is written.

// A message that is not a record or a revocation is refused rather than
// applied as an empty change, and a nil message names itself.
func TestExtenderDirectoryRefusesAMessageThatCarriesNeither(t *testing.T) {
	clock := newTestClock()
	directory, _ := newTestExtenderDirectory(t, clock, nil)

	if changed, err := directory.ApplySource(nil, ExtenderSourceFeed); err == nil || changed {
		t.Errorf("a nil message applied: changed=%v err=%v", changed, err)
	}
	empty := &protocol.ExtenderGossipMessage{}
	if changed, err := directory.ApplySource(empty, ExtenderSourceFeed); err == nil || changed {
		t.Errorf("an empty message applied: changed=%v err=%v", changed, err)
	}
}

// A record or a revocation that names no key is refused: without a key there
// is nothing to attach the evidence to, and an entry keyed by nothing would
// answer for every extender.
func TestExtenderDirectoryRefusesAMessageWithNoPublicKey(t *testing.T) {
	clock := newTestClock()
	directory, rootPrivateKey := newTestExtenderDirectory(t, clock, nil)
	now := clock.Now()

	record := signTestRecord(
		t, rootPrivateKey, nil, now, now.Add(time.Hour), testExtenderAddress("192.0.2.10"))
	if changed, err := directory.ApplyRecord(record, ExtenderSourceFeed); err == nil || changed {
		t.Errorf("a record with no key applied: changed=%v err=%v", changed, err)
	}
	revocation := signTestRevocation(t, rootPrivateKey, nil, now)
	if changed, err := directory.ApplyRevocation(revocation); err == nil || changed {
		t.Errorf("a revocation with no key applied: changed=%v err=%v", changed, err)
	}
}

// A revocation from another space, or one signed by a key the anchor does not
// accept, never retires a local extender.
func TestExtenderDirectoryRefusesAForeignRevocation(t *testing.T) {
	clock := newTestClock()
	directory, rootPrivateKey := newTestExtenderDirectory(t, clock, nil)
	now := clock.Now()
	publicKey := newTestExtenderKey(t)
	ip := netip.MustParseAddr("192.0.2.10")

	record := signTestRecord(
		t, rootPrivateKey, publicKey, now, now.Add(time.Hour), testExtenderAddress(ip.String()))
	if _, err := directory.ApplyRecord(record, ExtenderSourceFeed); err != nil {
		t.Fatal(err)
	}
	if state := testDirectoryState(t, directory, ip); state != ExtenderStateActive {
		t.Fatalf("state = %s", state)
	}

	// another space's revocation
	foreign, err := SignExtenderRevocation(rootPrivateKey, &protocol.ExtenderRevocationBody{
		PublicKey:   publicKey,
		IssueTimeMs: uint64(now.Add(time.Minute).UnixMilli()),
		NetworkHost: "other.example",
	})
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := directory.ApplyRevocation(foreign); err == nil || changed {
		t.Errorf("a foreign revocation applied: changed=%v err=%v", changed, err)
	}

	// a revocation signed by a key outside the anchor
	otherRootPrivateKey, _ := newTestRootKey(t)
	unsigned := signTestRevocation(t, otherRootPrivateKey, publicKey, now.Add(time.Minute))
	if changed, err := directory.ApplyRevocation(unsigned); err == nil || changed {
		t.Errorf("an unaccepted revocation applied: changed=%v err=%v", changed, err)
	}

	if state := testDirectoryState(t, directory, ip); state != ExtenderStateActive {
		t.Fatalf("state after the refusals = %s, expected still active", state)
	}
}

// A revocation that arrives before any record still retires the key, so a
// record replayed from an older feed cannot bring a revoked extender back.
func TestExtenderDirectoryRevocationBeforeAnyRecordHoldsTheKey(t *testing.T) {
	clock := newTestClock()
	directory, rootPrivateKey := newTestExtenderDirectory(t, clock, nil)
	now := clock.Now()
	publicKey := newTestExtenderKey(t)
	ip := netip.MustParseAddr("192.0.2.10")

	revocationTime := now.Add(time.Hour)
	revocation := signTestRevocation(t, rootPrivateKey, publicKey, revocationTime)
	if changed, err := directory.ApplyRevocation(revocation); err != nil || !changed {
		t.Fatalf("the revocation did not apply: changed=%v err=%v", changed, err)
	}

	// the replayed record is older than the revocation, so the key stays
	// retired and its address is not usable
	replayed := signTestRecord(
		t,
		rootPrivateKey,
		publicKey,
		now,
		now.Add(24*time.Hour),
		testExtenderAddress(ip.String()),
	)
	if changed, err := directory.ApplyRecord(replayed, ExtenderSourceFeed); err != nil || !changed {
		t.Fatalf("the replayed record did not apply: changed=%v err=%v", changed, err)
	}
	if state := testDirectoryState(t, directory, ip); state != ExtenderStateRevoked {
		t.Fatalf("state = %s, expected revoked", state)
	}
	if directory.AddressUsable(ip) {
		t.Fatal("a revoked address is usable")
	}

	// a re-activation newer than the revocation returns it
	reactivated := signTestRecord(
		t,
		rootPrivateKey,
		publicKey,
		revocationTime.Add(time.Millisecond),
		revocationTime.Add(24*time.Hour),
		testExtenderAddress(ip.String()),
	)
	if changed, err := directory.ApplyRecord(reactivated, ExtenderSourceFeed); err != nil || !changed {
		t.Fatalf("the re-activation did not apply: changed=%v err=%v", changed, err)
	}
	if state := testDirectoryState(t, directory, ip); state != ExtenderStateActive {
		t.Fatalf("state after re-activation = %s", state)
	}
}

// Only a revocation newer than the one held replaces it, so a replayed older
// revocation is not a change and does not reorder the key.
func TestExtenderDirectoryKeepsTheNewestRevocation(t *testing.T) {
	clock := newTestClock()
	directory, rootPrivateKey := newTestExtenderDirectory(t, clock, nil)
	now := clock.Now()
	publicKey := newTestExtenderKey(t)

	newest := signTestRevocation(t, rootPrivateKey, publicKey, now.Add(time.Hour))
	if changed, err := directory.ApplyRevocation(newest); err != nil || !changed {
		t.Fatalf("the first revocation did not apply: changed=%v err=%v", changed, err)
	}
	for _, issueTime := range []time.Time{now, now.Add(time.Hour)} {
		older := signTestRevocation(t, rootPrivateKey, publicKey, issueTime)
		changed, err := directory.ApplyRevocation(older)
		if err != nil {
			t.Fatalf("a superseded revocation errored: %v", err)
		}
		if changed {
			t.Errorf("a revocation issued at %s replaced the newest", issueTime)
		}
	}
}

// One address is one entry however the record writes it: a v4-mapped v6 form
// keys the same entry as the plain v4 form, at every entry point. Otherwise a
// record and a bootstrap for one host would age as two.
func TestExtenderDirectoryUnmapsEveryAddressEntryPoint(t *testing.T) {
	clock := newTestClock()
	directory, rootPrivateKey := newTestExtenderDirectory(t, clock, nil)
	now := clock.Now()
	publicKey := newTestExtenderKey(t)
	ip := netip.MustParseAddr("192.0.2.10")
	mapped := netip.MustParseAddr("::ffff:192.0.2.10")

	if !directory.AddBootstrap(mapped, ExtenderSourceDns) {
		t.Fatal("the mapped bootstrap was not added")
	}
	if !testDirectoryKnown(directory, ip) {
		t.Fatal("the mapped bootstrap did not key the v4 entry")
	}
	if directory.AddBootstrap(ip, ExtenderSourceDns) {
		t.Fatal("the plain form was added a second time")
	}
	if count := len(directory.Snapshot().Entries); count != 1 {
		t.Fatalf("the directory holds %d entries", count)
	}
	// the entry reports the v4 family, which is what the candidate filter uses
	if entry := testDirectoryEntry(t, directory, ip); entry.IpVersion != 4 {
		t.Fatalf("ip version = %d", entry.IpVersion)
	}

	// a record naming the mapped form upgrades the same entry
	record := signTestRecord(
		t,
		rootPrivateKey,
		publicKey,
		now,
		now.Add(time.Hour),
		testExtenderAddress(mapped.String(), ExtenderCarrierTcp),
	)
	if changed, err := directory.ApplyRecord(record, ExtenderSourceFeed); err != nil || !changed {
		t.Fatalf("the mapped record did not apply: changed=%v err=%v", changed, err)
	}
	if count := len(directory.Snapshot().Entries); count != 1 {
		t.Fatalf("the mapped record split the entry into %d", count)
	}
	entry := testDirectoryEntry(t, directory, ip)
	if len(entry.PublicKey) == 0 {
		t.Fatal("the mapped record did not verify the v4 entry")
	}

	// the evidence and the usability check follow the same key
	directory.RecordSuccess(mapped, ExtenderConnectModeTcpTls)
	if entry := testDirectoryEntry(t, directory, ip); entry.SuccessCount != 1 {
		t.Fatalf("success count = %d", entry.SuccessCount)
	}
	directory.SetInUse(mapped, 1)
	if entry := testDirectoryEntry(t, directory, ip); entry.InUse != 1 {
		t.Fatalf("in use = %d", entry.InUse)
	}
	directory.SetInUse(mapped, -1)
	if !directory.AddressUsable(mapped) {
		t.Fatal("the mapped form is not usable while the plain form is")
	}
	// a candidate exclusion written in the mapped form excludes the entry
	if candidates := directory.Candidates(0, 4, mapped); 0 < len(candidates) {
		t.Fatalf("the mapped exclusion left %d candidates", len(candidates))
	}
	directory.RecordFailure(mapped, ExtenderConnectModeTcpTls)
	if entry := testDirectoryEntry(t, directory, ip); entry.FailureCount != 1 {
		t.Fatalf("failure count = %d", entry.FailureCount)
	}
}

// An address that is not valid is refused at every entry point rather than
// stored under a zero key, and evidence for an address nobody knows is a
// no-op rather than a new entry.
func TestExtenderDirectoryRefusesAnInvalidAddress(t *testing.T) {
	clock := newTestClock()
	directory, _ := newTestExtenderDirectory(t, clock, nil)

	var invalid netip.Addr
	if directory.AddBootstrap(invalid, ExtenderSourceDns) {
		t.Error("an invalid bootstrap was added")
	}
	if directory.AddManual(invalid) {
		t.Error("an invalid manual address was added")
	}
	if directory.AddressUsable(invalid) {
		t.Error("an invalid address is usable")
	}

	// evidence for an unknown address creates nothing
	unknown := netip.MustParseAddr("192.0.2.99")
	directory.RecordSuccess(unknown, ExtenderConnectModeTcpTls)
	directory.RecordFailure(unknown, ExtenderConnectModeTcpTls)
	directory.SetInUse(unknown, 1)
	if 0 < len(directory.Snapshot().Entries) {
		t.Fatalf("the directory holds %d entries", len(directory.Snapshot().Entries))
	}
	if directory.AddressUsable(unknown) {
		t.Error("an unknown address is usable")
	}
}

// The in-use count never goes below zero, so a release that outnumbers the
// holds cannot make an address look free forever.
func TestExtenderDirectoryInUseClampsAtZero(t *testing.T) {
	clock := newTestClock()
	directory, _ := newTestExtenderDirectory(t, clock, nil)
	ip := netip.MustParseAddr("192.0.2.10")
	directory.AddBootstrap(ip, ExtenderSourceDns)

	directory.SetInUse(ip, 1)
	directory.SetInUse(ip, -4)
	if entry := testDirectoryEntry(t, directory, ip); entry.InUse != 0 {
		t.Fatalf("in use = %d, expected the clamp at 0", entry.InUse)
	}
	directory.SetInUse(ip, 1)
	directory.SetInUse(ip, 0)
	if entry := testDirectoryEntry(t, directory, ip); entry.InUse != 1 {
		t.Fatalf("in use after a zero delta = %d", entry.InUse)
	}
}

// A maximum hold below the first hold clamps the first hold too, so a
// misconfigured pair cannot hold an address longer than its own maximum.
func TestExtenderDirectoryHoldClampsToTheMaximum(t *testing.T) {
	clock := newTestClock()
	directory, _ := newTestExtenderDirectory(t, clock, func(settings *ExtenderDirectorySettings) {
		settings.HoldTimeout = time.Hour
		settings.MaxHoldTimeout = time.Minute
	})
	ip := netip.MustParseAddr("192.0.2.10")
	directory.AddBootstrap(ip, ExtenderSourceDns)

	directory.RecordFailure(ip, ExtenderConnectModeTcpTls)
	clock.advance(time.Minute)
	if !directory.AddressUsable(ip) {
		t.Fatal("the hold outlived the maximum hold")
	}

	// a long run of failures never exceeds the maximum either
	for i := 0; i < 64; i += 1 {
		directory.RecordFailure(ip, ExtenderConnectModeTcpTls)
	}
	clock.advance(time.Minute)
	if !directory.AddressUsable(ip) {
		t.Fatal("a long failure run held the address past the maximum")
	}
}

// The cap evicts the succeeded addresses oldest-success first once the
// inactive and never-succeeded ones are gone. A manual address is never
// evictable, so adding one over the cap pushes the eviction into the
// succeeded tier, which is where the last-success order decides.
func TestExtenderDirectoryCapEvictsOldestSuccessFirst(t *testing.T) {
	clock := newTestClock()
	directory, _ := newTestExtenderDirectory(t, clock, func(settings *ExtenderDirectorySettings) {
		settings.MaxAddressCount = 3
	})

	// three addresses that have all succeeded, at increasing times
	ips := []netip.Addr{
		netip.MustParseAddr("192.0.2.10"),
		netip.MustParseAddr("192.0.2.11"),
		netip.MustParseAddr("2001:db8::12"),
	}
	for _, ip := range ips {
		directory.AddBootstrap(ip, ExtenderSourceDns)
		directory.RecordSuccess(ip, ExtenderConnectModeTcpTls)
		clock.advance(time.Minute)
	}

	// the manual address cannot be evicted, so the cap takes one of the three
	manual := netip.MustParseAddr("192.0.2.13")
	if !directory.AddManual(manual) {
		t.Fatal("the manual address was not added")
	}
	if !testDirectoryKnown(directory, manual) {
		t.Fatal("the manual address was evicted")
	}
	if testDirectoryKnown(directory, ips[0]) {
		t.Error("the oldest success survived the cap")
	}
	for _, ip := range ips[1:] {
		if !testDirectoryKnown(directory, ip) {
			t.Errorf("%s was evicted before the oldest success", ip)
		}
	}
}

// Addresses that arrive together in one record share an add time, so the cap
// breaks the tie by address rather than by map order -- two runs of the same
// input evict the same entry.
func TestExtenderDirectoryCapBreaksAnAddTimeTieByAddress(t *testing.T) {
	ipTexts := []string{"192.0.2.10", "192.0.2.11", "2001:db8::12"}
	// the surviving set must be the same whichever order the record lists
	orders := [][]string{
		{ipTexts[0], ipTexts[1], ipTexts[2]},
		{ipTexts[2], ipTexts[1], ipTexts[0]},
		{ipTexts[1], ipTexts[2], ipTexts[0]},
	}
	for _, order := range orders {
		clock := newTestClock()
		directory, rootPrivateKey := newTestExtenderDirectory(
			t,
			clock,
			func(settings *ExtenderDirectorySettings) {
				settings.MaxAddressCount = 2
			},
		)
		now := clock.Now()
		addresses := []*protocol.ExtenderAddress{}
		for _, ipText := range order {
			addresses = append(addresses, testExtenderAddress(ipText, ExtenderCarrierTcp))
		}
		record := signTestRecord(
			t, rootPrivateKey, newTestExtenderKey(t), now, now.Add(time.Hour), addresses...)
		if _, err := directory.ApplyRecord(record, ExtenderSourceFeed); err != nil {
			t.Fatal(err)
		}
		known := []string{}
		for _, entry := range directory.Snapshot().Entries {
			known = append(known, entry.Ip.String())
		}
		if len(known) != 2 {
			t.Fatalf("order %v left %v", order, known)
		}
		// the lowest address string is evicted first, so the two that survive
		// are the same set whatever the record's order
		if known[0] != "192.0.2.11" || known[1] != "2001:db8::12" {
			t.Errorf("order %v left %v", order, known)
		}
	}
}

// A directory whose entries are all manual holds more than the cap rather than
// dropping a hand-configured extender, which the operator cannot rediscover.
func TestExtenderDirectoryCapNeverEvictsManualAddresses(t *testing.T) {
	clock := newTestClock()
	directory, _ := newTestExtenderDirectory(t, clock, func(settings *ExtenderDirectorySettings) {
		settings.MaxAddressCount = 1
	})
	ips := []netip.Addr{
		netip.MustParseAddr("192.0.2.10"),
		netip.MustParseAddr("192.0.2.11"),
		netip.MustParseAddr("2001:db8::12"),
	}
	for _, ip := range ips {
		directory.AddManual(ip)
	}
	directory.Expire(clock.Now())
	for _, ip := range ips {
		if !testDirectoryKnown(directory, ip) {
			t.Errorf("the manual address %s was evicted", ip)
		}
	}
}

// The event ring is bounded, so a flood of applies inside one window cannot
// grow the event list without end.
func TestExtenderDirectoryEventRingIsBounded(t *testing.T) {
	clock := newTestClock()
	directory, rootPrivateKey := newTestExtenderDirectory(t, clock, func(settings *ExtenderDirectorySettings) {
		settings.EventWindowTimeout = time.Hour
		settings.MaxAddressCount = 0
	})
	now := clock.Now()

	// each apply is a distinct key, so every one is a real change and an event
	applyCount := ExtenderDirectoryEventRingCount + 64
	for i := 0; i < applyCount; i += 1 {
		publicKey := newTestExtenderKey(t)
		record := signTestRecord(
			t,
			rootPrivateKey,
			publicKey,
			now.Add(time.Duration(i)*time.Millisecond),
			now.Add(time.Hour),
			testExtenderAddress(fmt.Sprintf("192.0.2.%d", i%256)),
		)
		if _, err := directory.ApplyRecord(record, ExtenderSourceFeed); err != nil {
			t.Fatal(err)
		}
	}
	eventCount := directory.EventCountSince(now.Add(-time.Hour))
	if eventCount != ExtenderDirectoryEventRingCount {
		t.Fatalf(
			"the ring holds %d events after %d applies, expected the cap %d",
			eventCount, applyCount, ExtenderDirectoryEventRingCount)
	}
}

// The gate state never goes back to pending once an attempt has finished, so a
// network client rebuilt after the first sample does not make startup wait
// again.
func TestExtenderDirectoryInitialSampleNeverGoesBackToPending(t *testing.T) {
	clock := newTestClock()
	directory, _ := newTestExtenderDirectory(t, clock, nil)

	state, _ := directory.InitialSampleMonitor().Get()
	if state != ExtenderInitialSampleNone {
		t.Fatalf("initial state = %v", state)
	}
	directory.SetInitialSamplePending()
	if state, _ := directory.InitialSampleMonitor().Get(); state != ExtenderInitialSamplePending {
		t.Fatalf("state = %v, expected pending", state)
	}
	directory.SetInitialSampleDone()
	directory.SetInitialSamplePending()
	if state, _ := directory.InitialSampleMonitor().Get(); state != ExtenderInitialSampleDone {
		t.Fatalf("state = %v, expected done to be terminal", state)
	}
}

// Per-family counts separate the two address families, which is what the
// low-water re-bootstrap and the startup gate read. A held address still
// counts as active but is no longer usable.
func TestExtenderDirectoryCountsPerFamily(t *testing.T) {
	clock := newTestClock()
	directory, _ := newTestExtenderDirectory(t, clock, func(settings *ExtenderDirectorySettings) {
		settings.HoldTimeout = time.Hour
		settings.MaxHoldTimeout = time.Hour
	})
	v4 := netip.MustParseAddr("192.0.2.10")
	v6 := netip.MustParseAddr("2001:db8::10")
	directory.AddBootstrap(v4, ExtenderSourceDns)
	directory.AddBootstrap(v6, ExtenderSourceDns)

	cases := []struct {
		ipVersion int
		want      int
	}{
		{ipVersion: 0, want: 2},
		{ipVersion: 4, want: 1},
		{ipVersion: 6, want: 1},
	}
	for _, c := range cases {
		if count := directory.ActiveCount(c.ipVersion); count != c.want {
			t.Errorf("ActiveCount(%d) = %d, expected %d", c.ipVersion, count, c.want)
		}
		if count := directory.UsableCount(c.ipVersion); count != c.want {
			t.Errorf("UsableCount(%d) = %d, expected %d", c.ipVersion, count, c.want)
		}
	}

	// a hold leaves the address active for the low-water count and takes it
	// out of the usable count the gate reads
	directory.RecordFailure(v4, ExtenderConnectModeTcpTls)
	if count := directory.ActiveCount(4); count != 1 {
		t.Errorf("a held address left the active count: %d", count)
	}
	if count := directory.UsableCount(4); count != 0 {
		t.Errorf("a held address is still usable: %d", count)
	}
	if count := directory.UsableCount(0); count != 1 {
		t.Errorf("usable count = %d, expected only the other family", count)
	}
}

// A record address that does not parse is skipped and the rest of the record
// still applies, so one bad entry does not lose the whole record.
func TestExtenderDirectorySkipsAnUnparseableRecordAddress(t *testing.T) {
	clock := newTestClock()
	directory, rootPrivateKey := newTestExtenderDirectory(t, clock, nil)
	now := clock.Now()
	publicKey := newTestExtenderKey(t)
	good := netip.MustParseAddr("2001:db8::10")

	record := signTestRecord(
		t,
		rootPrivateKey,
		publicKey,
		now,
		now.Add(time.Hour),
		&protocol.ExtenderAddress{
			Ip:        "not-an-ip",
			IpVersion: 4,
			Carriers:  []string{ExtenderCarrierTcp},
		},
		&protocol.ExtenderAddress{Ip: "", IpVersion: 4, Carriers: []string{ExtenderCarrierTcp}},
		testExtenderAddress(good.String(), ExtenderCarrierTcp),
	)
	if changed, err := directory.ApplyRecord(record, ExtenderSourceFeed); err != nil || !changed {
		t.Fatalf("the record did not apply: changed=%v err=%v", changed, err)
	}
	entries := directory.Snapshot().Entries
	if len(entries) != 1 {
		t.Fatalf("the directory holds %d entries", len(entries))
	}
	if entries[0].Ip != good {
		t.Fatalf("the directory holds %s", entries[0].Ip)
	}
}

// A record lists the carriers of each address, and only the three known names
// survive, so an unknown carrier from a newer operator yields no dialer rather
// than an unknown connect mode.
func TestExtenderDirectoryKeepsOnlyKnownCarriers(t *testing.T) {
	clock := newTestClock()
	directory, rootPrivateKey := newTestExtenderDirectory(t, clock, nil)
	now := clock.Now()
	publicKey := newTestExtenderKey(t)
	ip := netip.MustParseAddr("192.0.2.10")

	record := signTestRecord(
		t,
		rootPrivateKey,
		publicKey,
		now,
		now.Add(time.Hour),
		&protocol.ExtenderAddress{
			Ip:        ip.String(),
			IpVersion: 4,
			Carriers:  []string{"webtransport", ExtenderCarrierQuic, "sctp"},
		},
	)
	if _, err := directory.ApplyRecord(record, ExtenderSourceFeed); err != nil {
		t.Fatal(err)
	}
	candidates := directory.Candidates(0, 4)
	if len(candidates) != 1 {
		t.Fatalf("the directory offered %d candidates", len(candidates))
	}
	if carriers := candidates[0].Carriers; len(carriers) != 1 || carriers[0] != ExtenderCarrierQuic {
		t.Fatalf("carriers = %v, expected only the known one", carriers)
	}

	// a record naming the address with no known carrier offers no carrier at
	// all, rather than falling back to the defaults
	noneRecord := signTestRecord(
		t,
		rootPrivateKey,
		publicKey,
		now.Add(time.Minute),
		now.Add(time.Hour),
		&protocol.ExtenderAddress{
			Ip:        ip.String(),
			IpVersion: 4,
			Carriers:  []string{"webtransport"},
		},
	)
	if _, err := directory.ApplyRecord(noneRecord, ExtenderSourceFeed); err != nil {
		t.Fatal(err)
	}
	candidates = directory.Candidates(0, 4)
	if len(candidates) != 1 {
		t.Fatalf("the directory offered %d candidates", len(candidates))
	}
	if carriers := candidates[0].Carriers; 0 < len(carriers) {
		t.Fatalf("carriers = %v, expected none", carriers)
	}
}

// A rotation that retires the key a revocation was signed under drops the
// revocation too, so the directory never holds evidence it can no longer
// verify.
func TestExtenderDirectorySetRootKeysDropsUnverifiableRevocations(t *testing.T) {
	clock := newTestClock()
	directory, rootPrivateKey := newTestExtenderDirectory(t, clock, nil)
	now := clock.Now()
	publicKey := newTestExtenderKey(t)
	ip := netip.MustParseAddr("192.0.2.10")

	record := signTestRecord(
		t, rootPrivateKey, publicKey, now, now.Add(time.Hour), testExtenderAddress(ip.String()))
	if _, err := directory.ApplyRecord(record, ExtenderSourceFeed); err != nil {
		t.Fatal(err)
	}
	revocation := signTestRevocation(t, rootPrivateKey, publicKey, now.Add(time.Minute))
	if _, err := directory.ApplyRevocation(revocation); err != nil {
		t.Fatal(err)
	}
	if state := testDirectoryState(t, directory, ip); state != ExtenderStateRevoked {
		t.Fatalf("state = %s, expected revoked", state)
	}

	// the anchor rotates to a key that signed neither, so both are dropped and
	// the address falls back to the local evidence it still has
	otherRootPrivateKey, otherRootPublicKey := newTestRootKey(t)
	directory.SetRootKeys(NewExtenderRootKeySet(otherRootPublicKey))
	if state := testDirectoryState(t, directory, ip); state != ExtenderStateUnverified {
		t.Fatalf("state after the rotation = %s, expected unverified", state)
	}
	if entry := testDirectoryEntry(t, directory, ip); 0 < len(entry.PublicKey) {
		t.Fatal("the retired identity survived the rotation")
	}

	// a record signed under the new anchor verifies the same address again,
	// and the dropped revocation no longer retires it
	rotated := signTestRecord(
		t,
		otherRootPrivateKey,
		publicKey,
		now.Add(2*time.Hour),
		now.Add(3*time.Hour),
		testExtenderAddress(ip.String()),
	)
	if changed, err := directory.ApplyRecord(rotated, ExtenderSourceFeed); err != nil || !changed {
		t.Fatalf("the rotated record did not apply: changed=%v err=%v", changed, err)
	}
	if state := testDirectoryState(t, directory, ip); state != ExtenderStateActive {
		t.Fatalf("state under the new anchor = %s, expected active", state)
	}
}

// A hold makes an address unusable for exactly the hold, on both families, and
// a success inside the hold clears it at once.
func TestExtenderDirectoryHoldAppliesToBothFamilies(t *testing.T) {
	for _, ipText := range []string{"192.0.2.10", "2001:db8::10"} {
		clock := newTestClock()
		directory, _ := newTestExtenderDirectory(t, clock, func(settings *ExtenderDirectorySettings) {
			settings.HoldTimeout = 10 * time.Minute
			settings.MaxHoldTimeout = 6 * time.Hour
		})
		ip := netip.MustParseAddr(ipText)
		directory.AddBootstrap(ip, ExtenderSourceDns)

		directory.RecordFailure(ip, ExtenderConnectModeTcpTls)
		if directory.AddressUsable(ip) {
			t.Errorf("%s is usable inside its hold", ip)
		}
		if state := testDirectoryState(t, directory, ip); state != ExtenderStateHold {
			t.Errorf("%s state = %s, expected hold", ip, state)
		}
		clock.advance(10 * time.Minute)
		if !directory.AddressUsable(ip) {
			t.Errorf("%s is still held after the hold", ip)
		}
		// the second failure doubles the hold
		directory.RecordFailure(ip, ExtenderConnectModeTcpTls)
		clock.advance(10 * time.Minute)
		if directory.AddressUsable(ip) {
			t.Errorf("%s left the doubled hold early", ip)
		}
		clock.advance(10 * time.Minute)
		if !directory.AddressUsable(ip) {
			t.Errorf("%s is still held after the doubled hold", ip)
		}
		// a success clears the run, so the next hold starts over
		directory.RecordSuccess(ip, ExtenderConnectModeTcpTls)
		directory.RecordFailure(ip, ExtenderConnectModeTcpTls)
		clock.advance(10 * time.Minute)
		if !directory.AddressUsable(ip) {
			t.Errorf("%s kept its failure run across a success", ip)
		}
	}
}

// The defaults of E1, which every policy above is judged against. A silent
// change to one of them changes how long a failing extender is held or how
// many are kept, with nothing else to notice it.
func TestExtenderDirectoryDefaultSettings(t *testing.T) {
	settings := DefaultExtenderDirectorySettings()
	cases := []struct {
		name string
		got  any
		want any
	}{
		{name: "HoldTimeout", got: settings.HoldTimeout, want: 10 * time.Minute},
		{name: "MaxHoldTimeout", got: settings.MaxHoldTimeout, want: 6 * time.Hour},
		{name: "WarningConsecutiveFailureCount", got: settings.WarningConsecutiveFailureCount, want: 1},
		{name: "NeverSucceededRemoveTimeout", got: settings.NeverSucceededRemoveTimeout, want: 24 * time.Hour},
		{name: "StaleSuccessRemoveTimeout", got: settings.StaleSuccessRemoveTimeout, want: 7 * 24 * time.Hour},
		{name: "RemoveConsecutiveFailureCount", got: settings.RemoveConsecutiveFailureCount, want: 3},
		{name: "RecordExpireSkew", got: settings.RecordExpireSkew, want: 5 * time.Minute},
		{name: "MaxAddressCount", got: settings.MaxAddressCount, want: 512},
		{name: "SaveTimeout", got: settings.SaveTimeout, want: time.Second},
		{name: "EventWindowTimeout", got: settings.EventWindowTimeout, want: time.Minute},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %v, expected %v", c.name, c.got, c.want)
		}
	}
	if settings.Store != nil {
		t.Error("the default directory carries a store")
	}
	if settings.Now == nil {
		t.Error("the default directory carries no clock")
	}
	if 0 < len(settings.NetworkHosts) {
		t.Errorf("the default directory accepts %v", settings.NetworkHosts)
	}
}

// The state names the sdk and the app render (K3), pinned so a rename is a
// deliberate change rather than an invisible one.
func TestExtenderDirectoryStateNames(t *testing.T) {
	cases := []struct {
		state string
		want  string
	}{
		{state: ExtenderStateActive, want: "active"},
		{state: ExtenderStateWarning, want: "warning"},
		{state: ExtenderStateHold, want: "hold"},
		{state: ExtenderStateUnverified, want: "unverified"},
		{state: ExtenderStateRevoked, want: "revoked"},
		{state: ExtenderStateExpired, want: "expired"},
		{state: ExtenderSourceDns, want: "dns"},
		{state: ExtenderSourceFeed, want: "feed"},
		{state: ExtenderSourceGossip, want: "gossip"},
		{state: ExtenderSourceBootstrap, want: "bootstrap"},
		{state: ExtenderSourceManual, want: "manual"},
		{state: ExtenderSourceImport, want: "import"},
	}
	for _, c := range cases {
		if c.state != c.want {
			t.Errorf("%q, expected %q", c.state, c.want)
		}
	}
	if !strings.HasSuffix(DefaultExtenderDnsTld, ".") {
		t.Errorf("the default dns tld %q is not fully qualified", DefaultExtenderDnsTld)
	}
	if DefaultExtenderDnsTld != "ur.xyz." {
		t.Errorf("the default dns tld is %q", DefaultExtenderDnsTld)
	}
}
