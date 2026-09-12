package connect

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

// Directory policy tests (EXTENDER.md E1). Every transition is pinned against
// the fake clock in the settings, so nothing here waits on wall time.

const testExtenderNetworkHost = "space.example"

// testClock is the directory's only clock in these tests.
type testClock struct {
	stateLock sync.Mutex
	now       time.Time
}

func newTestClock() *testClock {
	return &testClock{
		now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func (self *testClock) Now() time.Time {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.now
}

func (self *testClock) advance(d time.Duration) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.now = self.now.Add(d)
}

// testExtenderDirectoryStore is an in-memory store that counts saves, which is
// what the coalescing test measures.
type testExtenderDirectoryStore struct {
	stateLock  sync.Mutex
	stateBytes []byte
	loadErr    error
	saveCount  int
	saved      chan struct{}
}

func newTestExtenderDirectoryStore() *testExtenderDirectoryStore {
	return &testExtenderDirectoryStore{
		saved: make(chan struct{}, 64),
	}
}

func (self *testExtenderDirectoryStore) Load() ([]byte, error) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if self.loadErr != nil {
		return nil, self.loadErr
	}
	return self.stateBytes, nil
}

func (self *testExtenderDirectoryStore) Save(stateBytes []byte) error {
	self.stateLock.Lock()
	self.stateBytes = append([]byte(nil), stateBytes...)
	self.saveCount += 1
	self.stateLock.Unlock()
	select {
	case self.saved <- struct{}{}:
	default:
	}
	return nil
}

func (self *testExtenderDirectoryStore) counts() (int, []byte) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.saveCount, self.stateBytes
}

// One directory with the fake clock and, optionally, a store.
func newTestExtenderDirectory(
	t *testing.T,
	clock *testClock,
	configure func(settings *ExtenderDirectorySettings),
) (*ExtenderDirectory, ed25519.PrivateKey) {
	t.Helper()
	rootSeed, err := NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	rootPrivateKey, err := ExtenderPrivateKeyFromSeed(rootSeed)
	if err != nil {
		t.Fatal(err)
	}
	rootPublicKey := rootPrivateKey.Public().(ed25519.PublicKey)

	settings := DefaultExtenderDirectorySettings()
	settings.Now = clock.Now
	settings.NetworkHosts = []string{testExtenderNetworkHost}
	if configure != nil {
		configure(settings)
	}
	ctx, cancel := context.WithCancel(context.Background())
	directory := NewExtenderDirectory(ctx, settings)
	directory.SetRootKeys(NewExtenderRootKeySet(rootPublicKey))
	t.Cleanup(func() {
		directory.Close()
		cancel()
	})
	return directory, rootPrivateKey
}

// Builds and signs one record.
func signTestRecord(
	t *testing.T,
	rootPrivateKey ed25519.PrivateKey,
	extenderPublicKey ed25519.PublicKey,
	issueTime time.Time,
	expireTime time.Time,
	addresses ...*protocol.ExtenderAddress,
) *protocol.ExtenderRecord {
	t.Helper()
	body := &protocol.ExtenderRecordBody{
		PublicKey:    extenderPublicKey,
		Addresses:    addresses,
		TcpPort:      443,
		UdpPort:      443,
		DnsPort:      53,
		DnsTld:       "x.example.",
		CountryCode:  "us",
		IssueTimeMs:  uint64(issueTime.UnixMilli()),
		ExpireTimeMs: uint64(expireTime.UnixMilli()),
		NetworkHost:  testExtenderNetworkHost,
	}
	record, err := SignExtenderRecord(rootPrivateKey, body)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func signTestRevocation(
	t *testing.T,
	rootPrivateKey ed25519.PrivateKey,
	extenderPublicKey ed25519.PublicKey,
	issueTime time.Time,
) *protocol.ExtenderRevocation {
	t.Helper()
	revocation, err := SignExtenderRevocation(rootPrivateKey, &protocol.ExtenderRevocationBody{
		PublicKey:   extenderPublicKey,
		IssueTimeMs: uint64(issueTime.UnixMilli()),
		NetworkHost: testExtenderNetworkHost,
	})
	if err != nil {
		t.Fatal(err)
	}
	return revocation
}

func newTestExtenderKey(t *testing.T) ed25519.PublicKey {
	t.Helper()
	seed, err := NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	publicKey, err := ExtenderPublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	return publicKey
}

func testExtenderAddress(ip string, carriers ...string) *protocol.ExtenderAddress {
	ipVersion := 4
	if addr, err := netip.ParseAddr(ip); err == nil {
		ipVersion = addressIpVersion(addr)
	}
	if len(carriers) == 0 {
		carriers = []string{ExtenderCarrierTcp, ExtenderCarrierQuic, ExtenderCarrierDns}
	}
	return &protocol.ExtenderAddress{
		Ip:        ip,
		IpVersion: uint32(ipVersion),
		Carriers:  carriers,
	}
}

// A failure holds an address, and the hold doubles per consecutive failure up
// to the cap (E1).
func TestExtenderDirectoryHoldDoublesToTheCap(t *testing.T) {
	clock := newTestClock()
	directory, _ := newTestExtenderDirectory(t, clock, func(settings *ExtenderDirectorySettings) {
		// the removal rules are pinned by their own tests; this one runs for
		// days of fake time
		settings.NeverSucceededRemoveTimeout = 1000 * 24 * time.Hour
	})
	ip := netip.MustParseAddr("192.0.2.10")
	directory.AddBootstrap(ip, ExtenderSourceDns)

	expectHoldTimeouts := []time.Duration{
		10 * time.Minute,
		20 * time.Minute,
		40 * time.Minute,
		80 * time.Minute,
		160 * time.Minute,
		320 * time.Minute,
		// the cap, not 640 minutes
		6 * time.Hour,
		6 * time.Hour,
	}
	for i, expectHoldTimeout := range expectHoldTimeouts {
		directory.RecordFailure(ip, ExtenderConnectModeTcpTls)
		clock.advance(expectHoldTimeout - time.Nanosecond)
		if directory.AddressUsable(ip) {
			t.Fatalf("failure %d: the address was usable before the %s hold ended", i+1, expectHoldTimeout)
		}
		clock.advance(time.Nanosecond)
		if !directory.AddressUsable(ip) {
			t.Fatalf("failure %d: the address was still held after %s", i+1, expectHoldTimeout)
		}
	}
}

// One consecutive failure is a warning; a success clears the run and the hold
// (E1, F2).
func TestExtenderDirectoryWarningAndSuccessClearsTheRun(t *testing.T) {
	clock := newTestClock()
	directory, _ := newTestExtenderDirectory(t, clock, nil)
	ip := netip.MustParseAddr("192.0.2.11")
	directory.AddBootstrap(ip, ExtenderSourceDns)

	if state := testDirectoryState(t, directory, ip); state != ExtenderStateUnverified {
		t.Fatalf("state = %s, expected unverified", state)
	}
	directory.RecordFailure(ip, ExtenderConnectModeTcpTls)
	if state := testDirectoryState(t, directory, ip); state != ExtenderStateHold {
		t.Fatalf("state = %s, expected hold", state)
	}
	clock.advance(10 * time.Minute)
	if state := testDirectoryState(t, directory, ip); state != ExtenderStateWarning {
		t.Fatalf("state = %s, expected warning", state)
	}
	directory.RecordSuccess(ip, ExtenderConnectModeTcpTls)
	if state := testDirectoryState(t, directory, ip); state != ExtenderStateUnverified {
		t.Fatalf("state = %s, expected the warning to clear", state)
	}
	// the next failure starts the hold at the base again
	directory.RecordFailure(ip, ExtenderConnectModeTcpTls)
	clock.advance(10 * time.Minute)
	if !directory.AddressUsable(ip) {
		t.Fatal("a success did not reset the hold doubling")
	}
}

// An address that has never succeeded is removed once its first failure is old
// enough (E1).
func TestExtenderDirectoryRemovesNeverSucceeded(t *testing.T) {
	clock := newTestClock()
	directory, _ := newTestExtenderDirectory(t, clock, nil)
	ip := netip.MustParseAddr("192.0.2.12")
	directory.AddBootstrap(ip, ExtenderSourceDns)

	directory.RecordFailure(ip, ExtenderConnectModeTcpTls)
	clock.advance(24*time.Hour - time.Nanosecond)
	directory.Expire(clock.Now())
	if testDirectoryKnown(directory, ip) != true {
		t.Fatal("the address was removed before the never-succeeded timeout")
	}
	clock.advance(time.Nanosecond)
	directory.Expire(clock.Now())
	if testDirectoryKnown(directory, ip) {
		t.Fatal("the address was not removed at the never-succeeded timeout")
	}
}

// An address that has succeeded is removed only when its last success is stale
// AND the consecutive failures reach the count (E1).
func TestExtenderDirectoryRemovesStaleSuccess(t *testing.T) {
	clock := newTestClock()
	directory, _ := newTestExtenderDirectory(t, clock, nil)
	ip := netip.MustParseAddr("192.0.2.13")
	directory.AddBootstrap(ip, ExtenderSourceDns)
	directory.RecordSuccess(ip, ExtenderConnectModeTcpTls)

	clock.advance(8 * 24 * time.Hour)
	// stale, but only two consecutive failures
	directory.RecordFailure(ip, ExtenderConnectModeTcpTls)
	directory.RecordFailure(ip, ExtenderConnectModeTcpTls)
	directory.Expire(clock.Now())
	if !testDirectoryKnown(directory, ip) {
		t.Fatal("the address was removed below the consecutive failure count")
	}
	directory.RecordFailure(ip, ExtenderConnectModeTcpTls)
	if testDirectoryKnown(directory, ip) {
		t.Fatal("the address was not removed at the third failure of a stale success")
	}
}

// A record is active until its expiry plus the skew (E1, B5).
func TestExtenderDirectoryRecordExpiryUsesTheSkew(t *testing.T) {
	clock := newTestClock()
	directory, rootPrivateKey := newTestExtenderDirectory(t, clock, nil)
	extenderPublicKey := newTestExtenderKey(t)
	ip := netip.MustParseAddr("192.0.2.14")

	expireTime := clock.Now().Add(time.Hour)
	record := signTestRecord(
		t,
		rootPrivateKey,
		extenderPublicKey,
		clock.Now(),
		expireTime,
		testExtenderAddress(ip.String()),
	)
	if changed, err := directory.ApplyRecord(record, ExtenderSourceFeed); err != nil || !changed {
		t.Fatalf("apply record changed=%v err=%v", changed, err)
	}
	if state := testDirectoryState(t, directory, ip); state != ExtenderStateActive {
		t.Fatalf("state = %s, expected active", state)
	}
	// past the expiry but inside the skew
	clock.advance(time.Hour + 4*time.Minute)
	if state := testDirectoryState(t, directory, ip); state != ExtenderStateActive {
		t.Fatalf("state = %s, expected the skew to keep it active", state)
	}
	if !directory.AddressUsable(ip) {
		t.Fatal("an address inside the expiry skew was not usable")
	}
	clock.advance(2 * time.Minute)
	if state := testDirectoryState(t, directory, ip); state != ExtenderStateExpired {
		t.Fatalf("state = %s, expected expired", state)
	}
	if directory.AddressUsable(ip) {
		t.Fatal("an expired address was still usable")
	}
}

// A revocation at or after the record's issue time retires the key, and an
// older revocation does not (B5).
func TestExtenderDirectoryRevocationOrdersByIssueTime(t *testing.T) {
	clock := newTestClock()
	directory, rootPrivateKey := newTestExtenderDirectory(t, clock, nil)
	extenderPublicKey := newTestExtenderKey(t)
	ip := netip.MustParseAddr("192.0.2.15")

	issueTime := clock.Now()
	record := signTestRecord(
		t,
		rootPrivateKey,
		extenderPublicKey,
		issueTime,
		issueTime.Add(14*24*time.Hour),
		testExtenderAddress(ip.String()),
	)
	if _, err := directory.ApplyRecord(record, ExtenderSourceFeed); err != nil {
		t.Fatal(err)
	}

	// an older revocation does not retire a newer record
	older := signTestRevocation(t, rootPrivateKey, extenderPublicKey, issueTime.Add(-time.Minute))
	if _, err := directory.ApplyRevocation(older); err != nil {
		t.Fatal(err)
	}
	if state := testDirectoryState(t, directory, ip); state != ExtenderStateActive {
		t.Fatalf("state = %s, expected an older revocation to be ignored", state)
	}

	// at the record's issue time it does
	current := signTestRevocation(t, rootPrivateKey, extenderPublicKey, issueTime)
	if changed, err := directory.ApplyRevocation(current); err != nil || !changed {
		t.Fatalf("apply revocation changed=%v err=%v", changed, err)
	}
	if state := testDirectoryState(t, directory, ip); state != ExtenderStateRevoked {
		t.Fatalf("state = %s, expected revoked", state)
	}
	if directory.AddressUsable(ip) {
		t.Fatal("a revoked address was still usable")
	}

	// a re-activation issues a record newer than the revocation
	reactivated := signTestRecord(
		t,
		rootPrivateKey,
		extenderPublicKey,
		issueTime.Add(time.Minute),
		issueTime.Add(14*24*time.Hour),
		testExtenderAddress(ip.String()),
	)
	if changed, err := directory.ApplyRecord(reactivated, ExtenderSourceFeed); err != nil || !changed {
		t.Fatalf("apply reactivation changed=%v err=%v", changed, err)
	}
	if state := testDirectoryState(t, directory, ip); state != ExtenderStateActive {
		t.Fatalf("state = %s, expected the re-activation to take", state)
	}
}

// An older record never replaces the newest one held for a key, whatever order
// the two arrive in (B5).
func TestExtenderDirectoryKeepsTheNewestRecord(t *testing.T) {
	clock := newTestClock()
	directory, rootPrivateKey := newTestExtenderDirectory(t, clock, nil)
	extenderPublicKey := newTestExtenderKey(t)

	issueTime := clock.Now()
	newer := signTestRecord(
		t,
		rootPrivateKey,
		extenderPublicKey,
		issueTime.Add(time.Hour),
		issueTime.Add(14*24*time.Hour),
		testExtenderAddress("192.0.2.16"),
	)
	older := signTestRecord(
		t,
		rootPrivateKey,
		extenderPublicKey,
		issueTime,
		issueTime.Add(14*24*time.Hour),
		testExtenderAddress("192.0.2.17"),
	)
	if _, err := directory.ApplyRecord(newer, ExtenderSourceFeed); err != nil {
		t.Fatal(err)
	}
	changed, err := directory.ApplyRecord(older, ExtenderSourceFeed)
	if err != nil {
		t.Fatal(err)
	}
	if changed {
		t.Fatal("an older record replaced the newest one")
	}
	if testDirectoryKnown(directory, netip.MustParseAddr("192.0.2.17")) {
		t.Fatal("an older record added its address")
	}
}

// A dns bootstrap address keeps its local evidence and its source when a
// record names it (E1).
func TestExtenderDirectoryBootstrapUpgradesToVerified(t *testing.T) {
	clock := newTestClock()
	directory, rootPrivateKey := newTestExtenderDirectory(t, clock, nil)
	extenderPublicKey := newTestExtenderKey(t)
	ip := netip.MustParseAddr("192.0.2.18")

	directory.AddBootstrap(ip, ExtenderSourceDns)
	directory.RecordSuccess(ip, ExtenderConnectModeTcpTls)
	if state := testDirectoryState(t, directory, ip); state != ExtenderStateUnverified {
		t.Fatalf("state = %s, expected unverified", state)
	}

	record := signTestRecord(
		t,
		rootPrivateKey,
		extenderPublicKey,
		clock.Now(),
		clock.Now().Add(14*24*time.Hour),
		testExtenderAddress(ip.String(), ExtenderCarrierTcp, ExtenderCarrierDns),
	)
	if _, err := directory.ApplyRecord(record, ExtenderSourceFeed); err != nil {
		t.Fatal(err)
	}
	entry := testDirectoryEntry(t, directory, ip)
	if entry.State != ExtenderStateActive {
		t.Fatalf("state = %s, expected active", entry.State)
	}
	if entry.Source != ExtenderSourceDns {
		t.Fatalf("source = %s, expected the origin to be kept", entry.Source)
	}
	if entry.SuccessCount != 1 {
		t.Fatalf("success count = %d, expected the local evidence to be kept", entry.SuccessCount)
	}
	candidates := directory.Candidates(4, 4)
	if len(candidates) != 1 {
		t.Fatalf("candidates = %d, expected 1", len(candidates))
	}
	if !candidates[0].Verified || len(candidates[0].PublicKey) == 0 {
		t.Fatal("the upgraded candidate carries no key")
	}
	if len(candidates[0].Carriers) != 2 {
		t.Fatalf("carriers = %v, expected the record's two", candidates[0].Carriers)
	}
	if candidates[0].DnsTld != "x.example." {
		t.Fatalf("dns tld = %q, expected the record's", candidates[0].DnsTld)
	}
}

// A manually configured address is never removed by the policy (E1).
func TestExtenderDirectoryManualIsNeverRemoved(t *testing.T) {
	clock := newTestClock()
	directory, _ := newTestExtenderDirectory(t, clock, nil)
	manualIp := netip.MustParseAddr("192.0.2.19")
	dnsIp := netip.MustParseAddr("192.0.2.20")
	directory.AddBootstrap(manualIp, ExtenderSourceManual)
	directory.AddBootstrap(dnsIp, ExtenderSourceDns)

	directory.RecordFailure(manualIp, ExtenderConnectModeTcpTls)
	directory.RecordFailure(dnsIp, ExtenderConnectModeTcpTls)
	clock.advance(30 * 24 * time.Hour)
	directory.Expire(clock.Now())

	if !testDirectoryKnown(directory, manualIp) {
		t.Fatal("a manual address was removed by policy")
	}
	if testDirectoryKnown(directory, dnsIp) {
		t.Fatal("a dns address survived the never-succeeded removal")
	}
}

// Over the cap the eviction order is expired, then never succeeded oldest
// first, then oldest last success; manual entries are never evicted (E1).
func TestExtenderDirectoryCapEvictionOrder(t *testing.T) {
	clock := newTestClock()
	directory, rootPrivateKey := newTestExtenderDirectory(t, clock, func(settings *ExtenderDirectorySettings) {
		settings.MaxAddressCount = 5
	})

	manualIp := netip.MustParseAddr("192.0.2.30")
	directory.AddBootstrap(manualIp, ExtenderSourceManual)

	// an expired verified address
	expiredIp := netip.MustParseAddr("192.0.2.31")
	expiredRecord := signTestRecord(
		t,
		rootPrivateKey,
		newTestExtenderKey(t),
		clock.Now(),
		clock.Now().Add(time.Minute),
		testExtenderAddress(expiredIp.String()),
	)
	if _, err := directory.ApplyRecord(expiredRecord, ExtenderSourceFeed); err != nil {
		t.Fatal(err)
	}

	// two never-succeeded addresses, the first one older
	oldNeverIp := netip.MustParseAddr("192.0.2.32")
	directory.AddBootstrap(oldNeverIp, ExtenderSourceDns)
	clock.advance(time.Minute)
	newNeverIp := netip.MustParseAddr("192.0.2.33")
	directory.AddBootstrap(newNeverIp, ExtenderSourceDns)

	// one that has succeeded
	succeededIp := netip.MustParseAddr("192.0.2.34")
	directory.AddBootstrap(succeededIp, ExtenderSourceDns)
	directory.RecordSuccess(succeededIp, ExtenderConnectModeTcpTls)

	// now past the expired record's expiry and skew
	clock.advance(10 * time.Minute)

	// one more address puts the directory over the cap of 5
	overflowIp := netip.MustParseAddr("192.0.2.35")
	directory.AddBootstrap(overflowIp, ExtenderSourceDns)

	if testDirectoryKnown(directory, expiredIp) {
		t.Fatal("the expired address was not evicted first")
	}
	for _, ip := range []netip.Addr{manualIp, oldNeverIp, newNeverIp, succeededIp, overflowIp} {
		if !testDirectoryKnown(directory, ip) {
			t.Fatalf("%s was evicted before the expired address ran out", ip)
		}
	}

	// one more forces a second eviction, which takes the oldest never-succeeded
	secondOverflowIp := netip.MustParseAddr("192.0.2.36")
	directory.AddBootstrap(secondOverflowIp, ExtenderSourceDns)
	if testDirectoryKnown(directory, oldNeverIp) {
		t.Fatal("the oldest never-succeeded address was not evicted next")
	}
	if !testDirectoryKnown(directory, manualIp) {
		t.Fatal("a manual address was evicted")
	}
	if !testDirectoryKnown(directory, succeededIp) {
		t.Fatal("an address that succeeded was evicted before a never-succeeded one")
	}
}

// Candidates are filtered by family and ordered verified first (E1, E2).
func TestExtenderDirectoryCandidatesFilterFamilyAndPreferVerified(t *testing.T) {
	clock := newTestClock()
	directory, rootPrivateKey := newTestExtenderDirectory(t, clock, nil)

	unverifiedIp := netip.MustParseAddr("192.0.2.40")
	verifiedIp := netip.MustParseAddr("192.0.2.41")
	v6Ip := netip.MustParseAddr("2001:db8::41")
	directory.AddBootstrap(unverifiedIp, ExtenderSourceDns)
	record := signTestRecord(
		t,
		rootPrivateKey,
		newTestExtenderKey(t),
		clock.Now(),
		clock.Now().Add(14*24*time.Hour),
		testExtenderAddress(verifiedIp.String()),
		testExtenderAddress(v6Ip.String()),
	)
	if _, err := directory.ApplyRecord(record, ExtenderSourceFeed); err != nil {
		t.Fatal(err)
	}

	v4Candidates := directory.Candidates(4, 8)
	if len(v4Candidates) != 2 {
		t.Fatalf("v4 candidates = %d, expected 2", len(v4Candidates))
	}
	if v4Candidates[0].Ip != verifiedIp {
		t.Fatalf("first v4 candidate = %s, expected the verified one", v4Candidates[0].Ip)
	}
	v6Candidates := directory.Candidates(6, 8)
	if len(v6Candidates) != 1 || v6Candidates[0].Ip != v6Ip {
		t.Fatalf("v6 candidates = %v, expected only the v6 address", v6Candidates)
	}
	// a held address is not a candidate
	directory.RecordFailure(verifiedIp, ExtenderConnectModeTcpTls)
	heldCandidates := directory.Candidates(4, 8)
	if len(heldCandidates) != 1 || heldCandidates[0].Ip != unverifiedIp {
		t.Fatalf("candidates = %v, expected the held address to be skipped", heldCandidates)
	}
	// and an excluded one is not either
	if excluded := directory.Candidates(4, 8, unverifiedIp); len(excluded) != 0 {
		t.Fatalf("candidates = %v, expected the exclusion to hold", excluded)
	}
}

// The store round-trips the records, the revocations and the local evidence.
func TestExtenderDirectoryStoreRoundTrip(t *testing.T) {
	clock := newTestClock()
	store := newTestExtenderDirectoryStore()
	directory, rootPrivateKey := newTestExtenderDirectory(t, clock, func(settings *ExtenderDirectorySettings) {
		settings.Store = store
	})
	extenderPublicKey := newTestExtenderKey(t)
	verifiedIp := netip.MustParseAddr("192.0.2.50")
	manualIp := netip.MustParseAddr("192.0.2.51")

	record := signTestRecord(
		t,
		rootPrivateKey,
		extenderPublicKey,
		clock.Now(),
		clock.Now().Add(14*24*time.Hour),
		testExtenderAddress(verifiedIp.String(), ExtenderCarrierTcp),
	)
	if _, err := directory.ApplyRecord(record, ExtenderSourceFeed); err != nil {
		t.Fatal(err)
	}
	directory.AddBootstrap(manualIp, ExtenderSourceManual)
	directory.RecordSuccess(verifiedIp, ExtenderConnectModeTcpTls)
	directory.RecordFailure(manualIp, ExtenderConnectModeQuic)
	// Close writes anything the coalescing window still held
	directory.Close()

	saveCount, stateBytes := store.counts()
	if saveCount == 0 || len(stateBytes) == 0 {
		t.Fatal("the directory was never saved")
	}

	restoredCtx, restoredCancel := context.WithCancel(context.Background())
	defer restoredCancel()
	restoredSettings := DefaultExtenderDirectorySettings()
	restoredSettings.Now = clock.Now
	restoredSettings.NetworkHosts = []string{testExtenderNetworkHost}
	restoredSettings.Store = store
	restored := NewExtenderDirectory(restoredCtx, restoredSettings)
	defer restored.Close()

	entry := testDirectoryEntry(t, restored, verifiedIp)
	if entry.State != ExtenderStateActive {
		t.Fatalf("restored state = %s, expected active", entry.State)
	}
	if entry.SuccessCount != 1 {
		t.Fatalf("restored success count = %d, expected 1", entry.SuccessCount)
	}
	candidates := restored.Candidates(4, 8)
	if len(candidates) != 1 || !candidates[0].Verified {
		t.Fatalf("restored candidates = %v, expected the verified address", candidates)
	}
	if len(candidates[0].Carriers) != 1 || candidates[0].Carriers[0] != ExtenderCarrierTcp {
		t.Fatalf("restored carriers = %v, expected the record's", candidates[0].Carriers)
	}
	manualEntry := testDirectoryEntry(t, restored, manualIp)
	if manualEntry.Source != ExtenderSourceManual || manualEntry.FailureCount != 1 {
		t.Fatalf("restored manual entry = %+v", manualEntry)
	}
}

// A store that cannot be read, holds junk, or holds another version leaves an
// empty directory rather than failing construction.
func TestExtenderDirectoryToleratesACorruptStore(t *testing.T) {
	clock := newTestClock()
	futureVersion, err := json.Marshal(&extenderDirectoryStoreState{Version: ExtenderDirectoryStoreVersion + 1})
	if err != nil {
		t.Fatal(err)
	}
	cases := [][]byte{
		[]byte("{not json"),
		[]byte(""),
		[]byte(`{"version":1,"records":[{"public_key":"zz"}],"addresses":[{"ip":"not-an-ip"}]}`),
		futureVersion,
	}
	for i, stateBytes := range cases {
		store := newTestExtenderDirectoryStore()
		store.stateBytes = stateBytes
		ctx, cancel := context.WithCancel(context.Background())
		settings := DefaultExtenderDirectorySettings()
		settings.Now = clock.Now
		settings.Store = store
		directory := NewExtenderDirectory(ctx, settings)
		if snapshot := directory.Snapshot(); snapshot.KnownCount != 0 {
			t.Errorf("case %d: known = %d, expected an empty directory", i, snapshot.KnownCount)
		}
		directory.Close()
		cancel()
	}

	// an unreadable store is the same
	store := newTestExtenderDirectoryStore()
	store.loadErr = fmt.Errorf("unreadable")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultExtenderDirectorySettings()
	settings.Now = clock.Now
	settings.Store = store
	directory := NewExtenderDirectory(ctx, settings)
	defer directory.Close()
	if snapshot := directory.Snapshot(); snapshot.KnownCount != 0 {
		t.Fatalf("known = %d, expected an empty directory", snapshot.KnownCount)
	}
}

// A burst of changes inside the save window costs one write (E1).
func TestExtenderDirectorySaveIsCoalesced(t *testing.T) {
	clock := newTestClock()
	store := newTestExtenderDirectoryStore()
	directory, _ := newTestExtenderDirectory(t, clock, func(settings *ExtenderDirectorySettings) {
		settings.Store = store
		settings.SaveTimeout = 50 * time.Millisecond
	})

	for i := range 16 {
		directory.AddBootstrap(
			netip.MustParseAddr(fmt.Sprintf("192.0.2.%d", 60+i)),
			ExtenderSourceDns,
		)
	}
	// the first save lands one window after the burst started
	select {
	case <-store.saved:
	case <-time.After(10 * time.Second):
		t.Fatal("the directory was never saved")
	}
	saveCount, stateBytes := store.counts()
	if saveCount != 1 {
		t.Fatalf("saves = %d, expected the burst to coalesce into one", saveCount)
	}
	state := &extenderDirectoryStoreState{}
	if err := json.Unmarshal(stateBytes, state); err != nil {
		t.Fatal(err)
	}
	if len(state.Addresses) != 16 {
		t.Fatalf("saved addresses = %d, expected the whole burst", len(state.Addresses))
	}
}

// A root key rotation that retires a key retires everything it signed (B4).
func TestExtenderDirectorySetRootKeysDropsUnverifiableRecords(t *testing.T) {
	clock := newTestClock()
	directory, rootPrivateKey := newTestExtenderDirectory(t, clock, nil)
	ip := netip.MustParseAddr("192.0.2.70")
	record := signTestRecord(
		t,
		rootPrivateKey,
		newTestExtenderKey(t),
		clock.Now(),
		clock.Now().Add(14*24*time.Hour),
		testExtenderAddress(ip.String()),
	)
	if _, err := directory.ApplyRecord(record, ExtenderSourceFeed); err != nil {
		t.Fatal(err)
	}

	otherSeed, err := NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	otherPublicKey, err := ExtenderPublicKeyFromSeed(otherSeed)
	if err != nil {
		t.Fatal(err)
	}
	directory.SetRootKeys(NewExtenderRootKeySet(otherPublicKey))

	entry := testDirectoryEntry(t, directory, ip)
	if entry.State != ExtenderStateUnverified {
		t.Fatalf("state = %s, expected the record to be dropped", entry.State)
	}
	// and a record signed by the retired key is no longer accepted
	if _, err := directory.ApplyRecord(record, ExtenderSourceFeed); err == nil {
		t.Fatal("a record signed by a retired root key was accepted")
	}
}

// A record for another network space is rejected (B2).
func TestExtenderDirectoryRejectsAnotherNetworkHost(t *testing.T) {
	clock := newTestClock()
	directory, rootPrivateKey := newTestExtenderDirectory(t, clock, nil)
	body := &protocol.ExtenderRecordBody{
		PublicKey:    newTestExtenderKey(t),
		Addresses:    []*protocol.ExtenderAddress{testExtenderAddress("192.0.2.80")},
		IssueTimeMs:  uint64(clock.Now().UnixMilli()),
		ExpireTimeMs: uint64(clock.Now().Add(14 * 24 * time.Hour).UnixMilli()),
		NetworkHost:  "other.example",
	}
	record, err := SignExtenderRecord(rootPrivateKey, body)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := directory.Apply(&protocol.ExtenderGossipMessage{
		Message: &protocol.ExtenderGossipMessage_Record{Record: record},
	}); err == nil {
		t.Fatal("a record for another network space was accepted")
	}
	if testDirectoryKnown(directory, netip.MustParseAddr("192.0.2.80")) {
		t.Fatal("a rejected record added its address")
	}
}

// The snapshot counts every state the sdk status reports (F2).
func TestExtenderDirectorySnapshotCounts(t *testing.T) {
	clock := newTestClock()
	directory, rootPrivateKey := newTestExtenderDirectory(t, clock, nil)
	activeIp := netip.MustParseAddr("192.0.2.90")
	record := signTestRecord(
		t,
		rootPrivateKey,
		newTestExtenderKey(t),
		clock.Now(),
		clock.Now().Add(14*24*time.Hour),
		testExtenderAddress(activeIp.String()),
	)
	if _, err := directory.ApplyRecord(record, ExtenderSourceFeed); err != nil {
		t.Fatal(err)
	}
	warningIp := netip.MustParseAddr("192.0.2.92")
	directory.AddBootstrap(warningIp, ExtenderSourceDns)
	directory.RecordFailure(warningIp, ExtenderConnectModeTcpTls)
	// the hold of that failure is over, so it reads as a warning
	clock.advance(10 * time.Minute)
	holdIp := netip.MustParseAddr("192.0.2.91")
	directory.AddBootstrap(holdIp, ExtenderSourceDns)
	directory.RecordFailure(holdIp, ExtenderConnectModeTcpTls)
	directory.SetInUse(activeIp, 1)

	snapshot := directory.Snapshot()
	if snapshot.KnownCount != 3 {
		t.Fatalf("known = %d, expected 3", snapshot.KnownCount)
	}
	if snapshot.ActiveCount != 1 {
		t.Fatalf("active = %d, expected 1", snapshot.ActiveCount)
	}
	if snapshot.WarningCount != 1 {
		t.Fatalf("warning = %d, expected 1", snapshot.WarningCount)
	}
	if snapshot.HoldCount != 1 {
		t.Fatalf("hold = %d, expected 1", snapshot.HoldCount)
	}
	for _, entry := range snapshot.Entries {
		if entry.Ip == activeIp && entry.InUse != 1 {
			t.Fatalf("in use = %d, expected 1", entry.InUse)
		}
	}
}

func testDirectoryEntry(t *testing.T, directory *ExtenderDirectory, ip netip.Addr) *ExtenderDirectoryEntry {
	t.Helper()
	for _, entry := range directory.Snapshot().Entries {
		if entry.Ip == ip {
			return entry
		}
	}
	t.Fatalf("%s is not in the directory", ip)
	return nil
}

func testDirectoryState(t *testing.T, directory *ExtenderDirectory, ip netip.Addr) string {
	t.Helper()
	return testDirectoryEntry(t, directory, ip).State
}

func testDirectoryKnown(directory *ExtenderDirectory, ip netip.Addr) bool {
	for _, entry := range directory.Snapshot().Entries {
		if entry.Ip == ip {
			return true
		}
	}
	return false
}

// SampleRecords serves only active verified identities, with the node's own
// record first (D4).
func TestExtenderDirectorySampleRecordsOwnFirst(t *testing.T) {
	clock := newTestClock()
	directory, rootPrivateKey := newTestExtenderDirectory(t, clock, nil)

	ownKey := newTestExtenderKey(t)
	otherKey := newTestExtenderKey(t)
	revokedKey := newTestExtenderKey(t)
	expiredKey := newTestExtenderKey(t)

	now := clock.Now()
	for _, c := range []struct {
		publicKey  ed25519.PublicKey
		ip         string
		expireTime time.Time
	}{
		{publicKey: ownKey, ip: "192.0.2.1", expireTime: now.Add(time.Hour)},
		{publicKey: otherKey, ip: "192.0.2.2", expireTime: now.Add(time.Hour)},
		{publicKey: revokedKey, ip: "192.0.2.3", expireTime: now.Add(time.Hour)},
		{publicKey: expiredKey, ip: "192.0.2.4", expireTime: now.Add(-time.Hour)},
	} {
		record := signTestRecord(
			t,
			rootPrivateKey,
			c.publicKey,
			now,
			c.expireTime,
			testExtenderAddress(c.ip),
		)
		if _, err := directory.ApplyRecord(record, ExtenderSourceGossip); err != nil {
			t.Fatal(err)
		}
	}
	revocation := signTestRevocation(t, rootPrivateKey, revokedKey, now)
	if _, err := directory.ApplyRevocation(revocation); err != nil {
		t.Fatal(err)
	}

	messages := directory.SampleRecords(8, ownKey)
	if len(messages) != 2 {
		t.Fatalf("sample = %d records, expected the two active ones", len(messages))
	}
	firstBody, err := directory.RootKeys().VerifyRecord(messages[0].GetRecord())
	if err != nil {
		t.Fatal(err)
	}
	if !ed25519.PublicKey(firstBody.PublicKey).Equal(ownKey) {
		t.Fatalf("the first sampled record is not the node's own")
	}

	// the cap is honored, and the node's own record is the one that survives it
	capped := directory.SampleRecords(1, ownKey)
	if len(capped) != 1 {
		t.Fatalf("sample = %d records, expected 1", len(capped))
	}
	cappedBody, err := directory.RootKeys().VerifyRecord(capped[0].GetRecord())
	if err != nil {
		t.Fatal(err)
	}
	if !ed25519.PublicKey(cappedBody.PublicKey).Equal(ownKey) {
		t.Fatalf("the capped sample dropped the node's own record")
	}

	// a node with no record of its own simply samples the others
	if messages := directory.SampleRecords(8, nil); len(messages) != 2 {
		t.Fatalf("sample = %d records, expected the two active ones", len(messages))
	}
}

// Subscribe delivers every applied message and closes when the consumer falls
// a whole buffer behind (D4).
func TestExtenderDirectorySubscribeDeliversAndCutsOff(t *testing.T) {
	clock := newTestClock()
	directory, rootPrivateKey := newTestExtenderDirectory(t, clock, nil)

	messages, unsubscribe := directory.Subscribe()
	now := clock.Now()

	extenderKey := newTestExtenderKey(t)
	record := signTestRecord(
		t,
		rootPrivateKey,
		extenderKey,
		now,
		now.Add(time.Hour),
		testExtenderAddress("192.0.2.10"),
	)
	if _, err := directory.ApplyRecord(record, ExtenderSourceGossip); err != nil {
		t.Fatal(err)
	}
	message, ok := <-messages
	if !ok || message.GetRecord() == nil {
		t.Fatalf("the applied record was not delivered")
	}
	revocation := signTestRevocation(t, rootPrivateKey, extenderKey, now.Add(time.Second))
	if _, err := directory.ApplyRevocation(revocation); err != nil {
		t.Fatal(err)
	}
	message, ok = <-messages
	if !ok || message.GetRevocation() == nil {
		t.Fatalf("the applied revocation was not delivered")
	}

	// fill the buffer without reading: one message over it cuts the
	// subscription off rather than holding the apply
	for i := 0; i <= ExtenderDirectorySubscribeBufferCount; i += 1 {
		overflowKey := newTestExtenderKey(t)
		overflowRecord := signTestRecord(
			t,
			rootPrivateKey,
			overflowKey,
			now,
			now.Add(time.Hour),
			testExtenderAddress(fmt.Sprintf("198.51.100.%d", i)),
		)
		if _, err := directory.ApplyRecord(overflowRecord, ExtenderSourceGossip); err != nil {
			t.Fatal(err)
		}
	}
	closed := false
	for range ExtenderDirectorySubscribeBufferCount + 2 {
		if _, ok := <-messages; !ok {
			closed = true
			break
		}
	}
	if !closed {
		t.Fatalf("the overflowing subscription was not cut off")
	}
	// the unsubscribe is still safe after the overflow closed the channel
	unsubscribe()
	unsubscribe()
}

// An unsubscribed consumer stops receiving and never blocks an apply (D4).
func TestExtenderDirectoryUnsubscribeStopsDelivery(t *testing.T) {
	clock := newTestClock()
	directory, rootPrivateKey := newTestExtenderDirectory(t, clock, nil)

	messages, unsubscribe := directory.Subscribe()
	unsubscribe()
	if _, ok := <-messages; ok {
		t.Fatalf("an unsubscribed channel delivered a message")
	}

	now := clock.Now()
	record := signTestRecord(
		t,
		rootPrivateKey,
		newTestExtenderKey(t),
		now,
		now.Add(time.Hour),
		testExtenderAddress("192.0.2.20"),
	)
	if _, err := directory.ApplyRecord(record, ExtenderSourceGossip); err != nil {
		t.Fatal(err)
	}
}
