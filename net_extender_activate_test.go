package connect

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/netip"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/protocol"
)

// Activation loop tests (EXTENDER.md C2, G3).
//
// The operator is two httptest servers, one per family loopback, exactly as the
// api-v4 and api-v6 hosts an activation is posted to. The caller address check
// runs against a third server, so the loop's own hello is the only traffic
// there and can be used as a pass barrier: the loop is single threaded, so a
// hello of pass n+1 proves pass n ran to completion.
//
// Nothing here sleeps. The cadence decisions all read the fake clock, and the
// loop is woken between clock steps by a directory change, which is a wake it
// already selects on.

// The addresses the fixture operator publishes for each family.
const (
	testActivateIpv4 = "198.51.100.7"
	testActivateIpv6 = "2001:db8::7"
	// another extender, returned in the bootstrap sample (C2, D6)
	testActivateBootstrapIpv4 = "198.51.100.9"
)

// One activation post as the operator saw it.
type testActivatePost struct {
	ipVersion int
	args      *ExtenderActivateArgs
	// the fake clock at the post, which is what the cadence assertions read
	postTime time.Time
}

// testActivateOperator serves `/network/extender-activate` and `/hello` for
// both families (C2, C7).
type testActivateOperator struct {
	clock          *testClock
	rootPrivateKey ed25519.PrivateKey
	bootstrapKey   ed25519.PublicKey

	v4Url    string
	v6Url    string
	helloUrl string

	posts  chan *testActivatePost
	hellos chan struct{}

	stateLock     sync.Mutex
	clientAddress string
	// non-empty refuses every activation with this message
	refusal    string
	postCounts map[int]int
	// the addresses this extender has activated, by family. One record names
	// every one of them, as the operator's does from its address rows (C1, C2).
	activatedIps map[int]string
	// keeps each signed record newer than the last, which is what the newest
	// wins rule of B5 needs when two activations land in the same instant
	issueSerial int
}

func newTestActivateOperator(
	t *testing.T,
	clock *testClock,
	rootPrivateKey ed25519.PrivateKey,
) *testActivateOperator {
	t.Helper()
	operator := &testActivateOperator{
		clock:          clock,
		rootPrivateKey: rootPrivateKey,
		bootstrapKey:   newTestExtenderKey(t),
		posts:          make(chan *testActivatePost, 64),
		hellos:         make(chan struct{}, 64),
		clientAddress:  testActivateIpv4 + ":41001",
		postCounts:     map[int]int{},
		activatedIps:   map[int]string{},
	}
	v4Server := newFamilyHttptestServer(t, 4, operator.familyHandler(4))
	v6Server := newFamilyHttptestServer(t, 6, operator.familyHandler(6))
	helloServer := newFamilyHttptestServer(t, 4, http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			select {
			case operator.hellos <- struct{}{}:
			default:
			}
			operator.writeHello(w)
		}))
	t.Cleanup(func() {
		v4Server.Close()
		v6Server.Close()
		helloServer.Close()
	})
	operator.v4Url = v4Server.URL
	operator.v6Url = v6Server.URL
	operator.helloUrl = helloServer.URL
	return operator
}

// The api host of one family. The strategy's own hello probe reaches these
// too, so `/hello` is served here as well and is deliberately not the barrier.
func (self *testActivateOperator) familyHandler(ipVersion int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hello" {
			self.writeHello(w)
			return
		}
		if r.URL.Path != ExtenderActivatePath {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		args := &ExtenderActivateArgs{}
		if err := json.NewDecoder(r.Body).Decode(args); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		now := self.clock.Now()
		self.stateLock.Lock()
		refusal := self.refusal
		self.postCounts[ipVersion] += 1
		self.stateLock.Unlock()
		select {
		case self.posts <- &testActivatePost{ipVersion: ipVersion, args: args, postTime: now}:
		default:
		}

		w.Header().Set("Content-Type", "application/json")
		if refusal != "" {
			json.NewEncoder(w).Encode(&ExtenderActivateResult{
				Activated: false,
				Error:     refusal,
			})
			return
		}
		result, err := self.activateResult(ipVersion, args, now)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(result)
	})
}

func (self *testActivateOperator) writeHello(w http.ResponseWriter) {
	self.stateLock.Lock()
	clientAddress := self.clientAddress
	self.stateLock.Unlock()
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"client_address":%q}`, clientAddress)
}

// The signed answer of one activation: this extender's own record for the
// family's address, and one other extender as the bootstrap sample (C2).
func (self *testActivateOperator) activateResult(
	ipVersion int,
	args *ExtenderActivateArgs,
	now time.Time,
) (*ExtenderActivateResult, error) {
	publicKey, err := ParseExtenderPublicKeyHex(args.PublicKeyHex)
	if err != nil {
		return nil, err
	}
	ip := testActivateIpv4
	if ipVersion == 6 {
		ip = testActivateIpv6
	}
	self.stateLock.Lock()
	self.activatedIps[ipVersion] = ip
	activatedIps := []string{}
	for _, activatedIpVersion := range []int{4, 6} {
		if activatedIp, ok := self.activatedIps[activatedIpVersion]; ok {
			activatedIps = append(activatedIps, activatedIp)
		}
	}
	self.stateLock.Unlock()

	expireTime := now.Add(14 * 24 * time.Hour)
	record, err := self.signRecord(publicKey, activatedIps, now, expireTime, args.Carriers)
	if err != nil {
		return nil, err
	}
	bootstrapRecord, err := self.signRecord(
		self.bootstrapKey,
		[]string{testActivateBootstrapIpv4},
		now,
		expireTime,
		[]string{ExtenderCarrierTcp},
	)
	if err != nil {
		return nil, err
	}
	return &ExtenderActivateResult{
		Activated:    true,
		Ip:           ip,
		IpVersion:    ipVersion,
		Carriers:     args.Carriers,
		ExpireTime:   &expireTime,
		AllowedHosts: []string{testExtenderNetworkHost, "*." + testExtenderNetworkHost},
		Record:       record,
		Bootstrap:    []string{bootstrapRecord},
	}, nil
}

func (self *testActivateOperator) signRecord(
	publicKey ed25519.PublicKey,
	ips []string,
	issueTime time.Time,
	expireTime time.Time,
	carriers []string,
) (string, error) {
	addresses := []*protocol.ExtenderAddress{}
	for _, ip := range ips {
		addresses = append(addresses, testExtenderAddress(ip, carriers...))
	}
	self.stateLock.Lock()
	self.issueSerial += 1
	issueTime = issueTime.Add(time.Duration(self.issueSerial) * time.Millisecond)
	self.stateLock.Unlock()
	record, err := SignExtenderRecord(self.rootPrivateKey, &protocol.ExtenderRecordBody{
		PublicKey:    publicKey,
		Addresses:    addresses,
		TcpPort:      443,
		UdpPort:      443,
		DnsPort:      53,
		DnsTld:       testActivateDnsTld,
		CountryCode:  "zz",
		IssueTimeMs:  uint64(issueTime.UnixMilli()),
		ExpireTimeMs: uint64(expireTime.UnixMilli()),
		NetworkHost:  testExtenderNetworkHost,
	})
	if err != nil {
		return "", err
	}
	recordBytes, err := proto.Marshal(record)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(recordBytes), nil
}

func (self *testActivateOperator) setClientAddress(clientAddress string) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.clientAddress = clientAddress
}

func (self *testActivateOperator) setRefusal(refusal string) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.refusal = refusal
}

func (self *testActivateOperator) postCount(ipVersion int) int {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.postCounts[ipVersion]
}

// Synthetic encoding tld of the dns carrier in these tests.
const testActivateDnsTld = "x.example."

// testActivatorFixture is one activator against the fixture operator, with the
// directory it fills and the seams the loop reads.
type testActivatorFixture struct {
	t         *testing.T
	clock     *testClock
	directory *ExtenderDirectory
	operator  *testActivateOperator
	activator *ExtenderActivator

	publicKey ed25519.PublicKey

	// the activations OnActivated reported
	activations chan int
	// the process network change listener the loop subscribed with
	networkChanged func()

	stateLock  sync.Mutex
	supported  map[int]bool
	carriers   []string
	wakeSerial int
}

func newTestActivatorFixture(
	t *testing.T,
	configure func(settings *ExtenderActivatorSettings),
) *testActivatorFixture {
	t.Helper()
	clock := newTestClock()
	directory, rootPrivateKey := newTestExtenderDirectory(t, clock, nil)
	operator := newTestActivateOperator(t, clock, rootPrivateKey)

	seed, err := NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	publicKey, err := ExtenderPublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}

	fixture := &testActivatorFixture{
		t:           t,
		clock:       clock,
		directory:   directory,
		operator:    operator,
		publicKey:   publicKey,
		activations: make(chan int, 64),
		supported:   map[int]bool{4: true, 6: true},
		carriers: []string{
			ExtenderCarrierTcp,
			ExtenderCarrierQuic,
			ExtenderCarrierDns,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	// one dialer, so one request reaches the operator per call: the resilient
	// variants race the same request and would make the pass barrier count a
	// pass more than once
	strategySettings := DefaultClientStrategySettings()
	strategySettings.EnableResilient = false
	clientStrategy := NewDirectClientStrategy(ctx, strategySettings, 0)

	settings := DefaultExtenderActivatorSettings()
	settings.ApiUrlV4 = operator.v4Url
	settings.ApiUrlV6 = operator.v6Url
	settings.HelloUrl = operator.helloUrl
	settings.ByJwt = func() string { return "test-jwt" }
	settings.ClientStrategy = clientStrategy
	settings.PublicKey = publicKey
	settings.DnsTld = testActivateDnsTld
	settings.Carriers = fixture.currentCarriers
	settings.Directory = directory
	settings.OnActivated = func(ipVersion int, result *ExtenderActivateResult) {
		select {
		case fixture.activations <- ipVersion:
		default:
		}
	}
	// every stepped pass advances the fake clock by at least this, so the
	// caller address check -- the pass barrier -- runs on every step
	settings.AddressCheckTimeout = 1 * time.Minute
	settings.RequestTimeout = 20 * time.Second
	settings.Now = clock.Now
	settings.IpVersionSupported = fixture.ipVersionSupported
	settings.AddNetworkChangeListener = func(listener func()) func() {
		fixture.networkChanged = listener
		return func() {}
	}
	if configure != nil {
		configure(settings)
	}
	fixture.activator = NewExtenderActivator(ctx, settings)
	t.Cleanup(func() {
		fixture.activator.Close()
		clientStrategy.Close()
		cancel()
	})
	return fixture
}

func (self *testActivatorFixture) currentCarriers() []string {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return slices.Clone(self.carriers)
}

func (self *testActivatorFixture) ipVersionSupported(ipVersion int) bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.supported[ipVersion]
}

func (self *testActivatorFixture) setSupported(ipVersion int, supported bool) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.supported[ipVersion] = supported
}

// Wakes the loop with a directory change, which is a wake it already selects
// on. Each call adds a fresh unverified address so the change is real.
func (self *testActivatorFixture) wake() {
	self.stateLock.Lock()
	self.wakeSerial += 1
	serial := self.wakeSerial
	self.stateLock.Unlock()
	self.directory.AddBootstrap(
		netip.MustParseAddr(fmt.Sprintf("203.0.113.%d", serial)), ExtenderSourceDns)
}

// Waits for the loop to begin one pass, which its caller address check marks.
// Because the loop is single threaded, this also proves every earlier pass
// finished.
func (self *testActivatorFixture) waitPass() {
	self.t.Helper()
	select {
	case <-self.operator.hellos:
	case <-time.After(30 * time.Second):
		self.t.Fatal("the activator did not run a pass")
	}
}

// Advances the fake clock, wakes the loop and waits for the pass it starts.
func (self *testActivatorFixture) step(d time.Duration) {
	self.t.Helper()
	self.clock.advance(d)
	self.wake()
	self.waitPass()
}

// Waits for one activation post.
func (self *testActivatorFixture) waitPost() *testActivatePost {
	self.t.Helper()
	select {
	case post := <-self.operator.posts:
		return post
	case <-time.After(30 * time.Second):
		self.t.Fatal("the activator did not post an activation")
		return nil
	}
}

// Waits for one activation to be reported, which is after its record has been
// applied to the directory.
func (self *testActivatorFixture) waitActivation() int {
	self.t.Helper()
	select {
	case ipVersion := <-self.activations:
		return ipVersion
	case <-time.After(30 * time.Second):
		self.t.Fatal("the activator did not report an activation")
		return 0
	}
}

// The state of one address as the directory reports it.
func (self *testActivatorFixture) directoryState(ip string) string {
	return testDirectoryState(self.t, self.directory, netip.MustParseAddr(ip))
}

// Both families activate against their own api url, the record and the
// bootstrap land in the directory, and the callback reports each family (C2,
// G3).
func TestExtenderActivatorActivatesEveryFamily(t *testing.T) {
	fixture := newTestActivatorFixture(t, nil)

	fixture.waitPass()
	posts := map[int]*testActivatePost{}
	for range 2 {
		post := fixture.waitPost()
		posts[post.ipVersion] = post
	}
	if len(posts) != 2 {
		t.Fatalf("posts = %v, expected one per family", posts)
	}

	for ipVersion, post := range posts {
		if post.args.PublicKeyHex != hex.EncodeToString(fixture.publicKey) {
			t.Fatalf("v%d public key = %q", ipVersion, post.args.PublicKeyHex)
		}
		expectedCarriers := []string{
			ExtenderCarrierTcp,
			ExtenderCarrierQuic,
			ExtenderCarrierDns,
		}
		if !slices.Equal(post.args.Carriers, expectedCarriers) {
			t.Fatalf("v%d carriers = %v, expected %v", ipVersion, post.args.Carriers, expectedCarriers)
		}
		if post.args.TcpPort != ExtenderTcpPort ||
			post.args.UdpPort != ExtenderQuicPort ||
			post.args.DnsPort != ExtenderDnsPort {
			t.Fatalf("v%d ports = %d/%d/%d", ipVersion,
				post.args.TcpPort, post.args.UdpPort, post.args.DnsPort)
		}
		if post.args.DnsTld != testActivateDnsTld {
			t.Fatalf("v%d dns tld = %q", ipVersion, post.args.DnsTld)
		}
		// with no listening ports to offer the operator reads DnsPort alone,
		// which is what an extender that predates the list sends (L2)
		if post.args.DnsPorts != nil {
			t.Fatalf("v%d dns ports = %v, expected none", ipVersion, post.args.DnsPorts)
		}
	}

	activated := map[int]bool{}
	for range 2 {
		select {
		case ipVersion := <-fixture.activations:
			activated[ipVersion] = true
		case <-time.After(30 * time.Second):
			t.Fatal("the activation callback did not report both families")
		}
	}
	if !activated[4] || !activated[6] {
		t.Fatalf("activated = %v, expected both families", activated)
	}

	// the own record and the bootstrap sample are both in the directory, and
	// the own addresses are verified and active (C2, E1)
	for _, ip := range []string{testActivateIpv4, testActivateIpv6, testActivateBootstrapIpv4} {
		if state := fixture.directoryState(ip); state != ExtenderStateActive {
			t.Fatalf("%s state = %q, expected active", ip, state)
		}
	}
	ownEntry := testDirectoryEntry(t, fixture.directory, netip.MustParseAddr(testActivateIpv4))
	if !slices.Equal(ownEntry.PublicKey, fixture.publicKey) {
		t.Fatal("the own record was not applied under the extender key")
	}
	if ownEntry.Source != ExtenderSourceBootstrap {
		t.Fatalf("own record source = %q, expected bootstrap", ownEntry.Source)
	}

	status := fixture.activator.Status()
	for _, ipVersion := range []int{4, 6} {
		family := status.Family(ipVersion)
		if family == nil || !family.Activated {
			t.Fatalf("v%d status = %+v, expected activated", ipVersion, family)
		}
		if family.LastActivationTime != fixture.clock.Now() {
			t.Fatalf("v%d activation time = %s", ipVersion, family.LastActivationTime)
		}
		if family.LastError != "" {
			t.Fatalf("v%d error = %q", ipVersion, family.LastError)
		}
		if !slices.Equal(family.AllowedHosts, []string{
			testExtenderNetworkHost,
			"*." + testExtenderNetworkHost,
		}) {
			t.Fatalf("v%d allowed hosts = %v", ipVersion, family.AllowedHosts)
		}
	}
	if ip := status.Family(4).Ip.String(); ip != testActivateIpv4 {
		t.Fatalf("v4 ip = %s", ip)
	}
	if ip := status.Family(6).Ip.String(); ip != testActivateIpv6 {
		t.Fatalf("v6 ip = %s", ip)
	}
}

// A family with no api url is not run at all, and a family this host has no
// address for is never posted (G3).
func TestExtenderActivatorSkipsEmptyAndUnsupportedFamilies(t *testing.T) {
	// v6 has an api url but no address on this host
	unsupported := newTestActivatorFixture(t, func(settings *ExtenderActivatorSettings) {
		settings.IpVersionSupported = func(ipVersion int) bool { return ipVersion == 4 }
	})
	unsupported.waitPass()
	if post := unsupported.waitPost(); post.ipVersion != 4 {
		t.Fatalf("posted family = %d, expected v4", post.ipVersion)
	}
	// a second stepped pass proves the first completed without a v6 post
	unsupported.step(2 * time.Minute)
	unsupported.step(2 * time.Minute)
	if count := unsupported.operator.postCount(6); count != 0 {
		t.Fatalf("v6 posts = %d, expected none", count)
	}
	if family := unsupported.activator.Status().Family(6); family == nil || family.Activated {
		t.Fatalf("v6 status = %+v, expected an inactive entry", family)
	}
	unsupported.activator.Close()

	// v6 has no api url at all, so it is not a family this activator runs
	noUrl := newTestActivatorFixture(t, func(settings *ExtenderActivatorSettings) {
		settings.ApiUrlV6 = ""
	})
	noUrl.waitPass()
	if post := noUrl.waitPost(); post.ipVersion != 4 {
		t.Fatalf("posted family = %d, expected v4", post.ipVersion)
	}
	noUrl.step(2 * time.Minute)
	noUrl.step(2 * time.Minute)
	if count := noUrl.operator.postCount(6); count != 0 {
		t.Fatalf("v6 posts = %d, expected none", count)
	}
	if family := noUrl.activator.Status().Family(6); family != nil {
		t.Fatalf("v6 status = %+v, expected no entry", family)
	}
}

// A refusal holds the next attempt for ten minutes and doubles from there (G3).
func TestExtenderActivatorBacksOffOnRefusal(t *testing.T) {
	fixture := newTestActivatorFixture(t, func(settings *ExtenderActivatorSettings) {
		settings.ApiUrlV6 = ""
	})
	fixture.operator.setRefusal("the tcp carrier did not answer")
	startTime := fixture.clock.Now()

	fixture.waitPass()
	first := fixture.waitPost()
	if first.postTime != startTime {
		t.Fatalf("first post at %s, expected %s", first.postTime, startTime)
	}

	// short of the ten minute hold, nothing is posted
	fixture.step(5 * time.Minute)
	fixture.step(1 * time.Minute)
	if count := fixture.operator.postCount(4); count != 1 {
		t.Fatalf("posts after 6 minutes = %d, expected the first only", count)
	}

	fixture.clock.advance(4 * time.Minute)
	fixture.wake()
	second := fixture.waitPost()
	if second.postTime != startTime.Add(10*time.Minute) {
		t.Fatalf("second post at %s, expected +10m", second.postTime.Sub(startTime))
	}

	// the hold doubled: nineteen minutes later is still too soon
	fixture.step(19 * time.Minute)
	fixture.step(1 * time.Minute)
	if count := fixture.operator.postCount(4); count != 2 {
		t.Fatalf("posts after +30m of holding = %d, expected two", count)
	}

	fixture.clock.advance(10 * time.Minute)
	fixture.wake()
	third := fixture.waitPost()
	if third.postTime != startTime.Add(40*time.Minute) {
		t.Fatalf("third post at %s, expected +40m", third.postTime.Sub(startTime))
	}

	status := fixture.activator.Status().Family(4)
	if status.Activated {
		t.Fatal("a refused activation reported activated")
	}
	if status.LastError != "the tcp carrier did not answer" {
		t.Fatalf("last error = %q", status.LastError)
	}
}

// A revocation of this extender's own key, observed in the directory,
// re-activates at once rather than waiting out the 24 hour tick (G3, B5).
func TestExtenderActivatorReactivatesOnRevocation(t *testing.T) {
	fixture := newTestActivatorFixture(t, func(settings *ExtenderActivatorSettings) {
		settings.ApiUrlV6 = ""
	})
	fixture.waitPass()
	fixture.waitPost()
	fixture.waitActivation()
	if state := fixture.directoryState(testActivateIpv4); state != ExtenderStateActive {
		t.Fatalf("state = %q, expected active", state)
	}

	// the operator revoked this key; the record it signs next is newer, so the
	// address returns to active
	fixture.clock.advance(1 * time.Hour)
	revocation := signTestRevocation(
		t, fixture.operator.rootPrivateKey, fixture.publicKey, fixture.clock.Now())
	if _, err := fixture.directory.ApplyRevocation(revocation); err != nil {
		t.Fatal(err)
	}

	second := fixture.waitPost()
	if second.postTime != fixture.clock.Now() {
		t.Fatalf("re-activation at %s, expected the revocation instant", second.postTime)
	}
	fixture.waitActivation()
	if state := fixture.directoryState(testActivateIpv4); state != ExtenderStateActive {
		t.Fatalf("state after re-activation = %q, expected active", state)
	}
}

// A caller address that no longer matches what the operator published
// re-activates on the hourly check (G3).
func TestExtenderActivatorReactivatesOnAddressChange(t *testing.T) {
	fixture := newTestActivatorFixture(t, func(settings *ExtenderActivatorSettings) {
		settings.ApiUrlV6 = ""
	})
	fixture.waitPass()
	first := fixture.waitPost()

	// a pass with the same address changes nothing, and neither does a new
	// source port: every check arrives on its own connection
	fixture.step(2 * time.Minute)
	fixture.operator.setClientAddress(testActivateIpv4 + ":41002")
	fixture.step(2 * time.Minute)
	fixture.step(2 * time.Minute)
	if count := fixture.operator.postCount(4); count != 1 {
		t.Fatalf("posts with an unchanged address = %d, expected one", count)
	}

	fixture.operator.setClientAddress("198.51.100.8:41003")
	fixture.clock.advance(2 * time.Minute)
	fixture.wake()
	second := fixture.waitPost()
	if !second.postTime.After(first.postTime) {
		t.Fatalf("re-activation at %s, expected after %s", second.postTime, first.postTime)
	}
}

// The caller address the operator reports carries the source port, which is
// not part of the address the record is published under (G3).
func TestExtenderClientAddressIp(t *testing.T) {
	cases := []struct {
		clientAddress string
		want          string
	}{
		{"198.51.100.7:41001", "198.51.100.7"},
		{"198.51.100.7", "198.51.100.7"},
		{"[2001:db8::7]:41001", "2001:db8::7"},
		{"2001:db8::7", "2001:db8::7"},
		{"::ffff:198.51.100.7", "198.51.100.7"},
		{"", ""},
	}
	for _, c := range cases {
		if got := extenderClientAddressIp(c.clientAddress); got != c.want {
			t.Errorf("extenderClientAddressIp(%q) = %q, want %q", c.clientAddress, got, c.want)
		}
	}
}

// A path change re-activates: the address the operator published may no longer
// be this host's (G3).
func TestExtenderActivatorReactivatesOnNetworkChange(t *testing.T) {
	fixture := newTestActivatorFixture(t, func(settings *ExtenderActivatorSettings) {
		settings.ApiUrlV6 = ""
	})
	fixture.waitPass()
	first := fixture.waitPost()
	if fixture.networkChanged == nil {
		t.Fatal("the activator did not subscribe to network changes")
	}

	fixture.clock.advance(2 * time.Minute)
	fixture.networkChanged()
	second := fixture.waitPost()
	if !second.postTime.After(first.postTime) {
		t.Fatalf("re-activation at %s, expected after %s", second.postTime, first.postTime)
	}
}

// A successful activation is reissued on the 24 hour tick and not before (G3).
func TestExtenderActivatorReactivatesOnTheTick(t *testing.T) {
	fixture := newTestActivatorFixture(t, func(settings *ExtenderActivatorSettings) {
		settings.ApiUrlV6 = ""
	})
	startTime := fixture.clock.Now()
	fixture.waitPass()
	fixture.waitPost()

	fixture.step(23 * time.Hour)
	fixture.step(1 * time.Minute)
	if count := fixture.operator.postCount(4); count != 1 {
		t.Fatalf("posts before the tick = %d, expected one", count)
	}

	fixture.clock.advance(59 * time.Minute)
	fixture.wake()
	second := fixture.waitPost()
	if second.postTime != startTime.Add(24*time.Hour) {
		t.Fatalf("tick post at %s, expected +24h", second.postTime.Sub(startTime))
	}
}

// An extender with no carrier listening never offers an empty carrier list: it
// holds on the backoff instead, since an activation with no carrier cannot be
// probed (C2, G2).
func TestExtenderActivatorHoldsWithNoCarrier(t *testing.T) {
	fixture := newTestActivatorFixture(t, func(settings *ExtenderActivatorSettings) {
		settings.ApiUrlV6 = ""
		settings.Carriers = func() []string { return nil }
	})
	fixture.waitPass()
	fixture.step(2 * time.Minute)
	fixture.step(2 * time.Minute)
	if count := fixture.operator.postCount(4); count != 0 {
		t.Fatalf("posts = %d, expected none", count)
	}
	status := fixture.activator.Status().Family(4)
	if status.Activated || status.LastError != "no carrier is listening" {
		t.Fatalf("status = %+v", status)
	}
}

// With neither family api url set the activator posts once per cycle to the
// plain api url and records the outcome under the family the answer reports,
// which is the family the operator derived from the caller address (C2, G3).
func TestExtenderActivatorUsesThePlainApiUrl(t *testing.T) {
	// the plain url is the v6 host of the fixture, so the family in the status
	// can only have come from the answer and never from a default
	fixture := newTestActivatorFixture(t, func(settings *ExtenderActivatorSettings) {
		settings.ApiUrl = settings.ApiUrlV6
		settings.ApiUrlV4 = ""
		settings.ApiUrlV6 = ""
	})

	fixture.waitPass()
	if post := fixture.waitPost(); post.ipVersion != 6 {
		t.Fatalf("posted to v%d, expected the plain api url", post.ipVersion)
	}
	if ipVersion := fixture.waitActivation(); ipVersion != 6 {
		t.Fatalf("activated v%d, expected the family the answer reported", ipVersion)
	}

	status := fixture.activator.Status()
	if len(status.Families) != 1 {
		t.Fatalf("families = %+v, expected only the family the answer named", status.Families)
	}
	family := status.Family(6)
	if family == nil || !family.Activated {
		t.Fatalf("v6 status = %+v, expected activated", family)
	}
	if ip := family.Ip.String(); ip != testActivateIpv6 {
		t.Fatalf("v6 ip = %s, expected %s", ip, testActivateIpv6)
	}
	if state := fixture.directoryState(testActivateIpv6); state != ExtenderStateActive {
		t.Fatalf("%s state = %q, expected active", testActivateIpv6, state)
	}

	// one post per cycle: there is no second url to reach the other family with
	fixture.clock.advance(25 * time.Hour)
	fixture.wake()
	fixture.waitPost()
	fixture.waitActivation()
	fixture.step(1 * time.Minute)
	if count := fixture.operator.postCount(6); count != 2 {
		t.Fatalf("posts = %d, expected one per cycle", count)
	}
	if count := fixture.operator.postCount(4); count != 0 {
		t.Fatalf("posts to the v4 host = %d, expected none", count)
	}
}

// A refusal through the plain api url, which names no family, is still visible
// in the status: it is held under the placeholder family until an answer names
// one (C2, F3).
func TestExtenderActivatorRecordsAPlainApiUrlRefusal(t *testing.T) {
	fixture := newTestActivatorFixture(t, func(settings *ExtenderActivatorSettings) {
		settings.ApiUrl = settings.ApiUrlV4
		settings.ApiUrlV4 = ""
		settings.ApiUrlV6 = ""
	})
	fixture.operator.setRefusal("the tcp carrier did not answer")

	fixture.waitPass()
	fixture.waitPost()
	fixture.step(1 * time.Minute)

	status := fixture.activator.Status()
	if len(status.Families) != 1 {
		t.Fatalf("families = %+v, expected the placeholder only", status.Families)
	}
	placeholder := status.Families[0]
	if placeholder.IpVersion != 0 || placeholder.Activated {
		t.Fatalf("placeholder = %+v, expected an unactivated family 0", placeholder)
	}
	if placeholder.LastError != "the tcp carrier did not answer" {
		t.Fatalf("last error = %q", placeholder.LastError)
	}

	// the first answer that names a family replaces the placeholder
	fixture.operator.setRefusal("")
	fixture.clock.advance(25 * time.Hour)
	fixture.wake()
	fixture.waitPost()
	if ipVersion := fixture.waitActivation(); ipVersion != 4 {
		t.Fatalf("activated v%d, expected v4", ipVersion)
	}
	status = fixture.activator.Status()
	if len(status.Families) != 1 || status.Families[0].IpVersion != 4 {
		t.Fatalf("families = %+v, expected v4 alone", status.Families)
	}
	if !status.Families[0].Activated || status.Families[0].LastError != "" {
		t.Fatalf("v4 status = %+v, expected activated", status.Families[0])
	}
}

// The posted args are the server's json contract field for field (C2). The
// literal document is the shape `controller.ExtenderActivateArgs` declares.
func TestExtenderActivateArgsJsonContract(t *testing.T) {
	args := &ExtenderActivateArgs{
		PublicKeyHex: "00112233",
		TcpPort:      443,
		UdpPort:      443,
		DnsPort:      53,
		DnsTld:       "x.example.",
		Carriers:     []string{"tcp", "quic", "dns"},
	}
	argsBytes, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	expected := `{"public_key_hex":"00112233","tcp_port":443,"udp_port":443,` +
		`"dns_port":53,"dns_tld":"x.example.","carriers":["tcp","quic","dns"]}`
	if string(argsBytes) != expected {
		t.Fatalf("args json = %s, expected %s", argsBytes, expected)
	}

	decoded := &ExtenderActivateArgs{}
	if err := json.Unmarshal([]byte(expected), decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, args) {
		t.Fatalf("decoded args = %+v, expected %+v", decoded, args)
	}
}

// The activation answer decodes field for field from the server's json (C2).
func TestExtenderActivateResultJsonContract(t *testing.T) {
	document := `{"activated":true,"ip":"198.51.100.7","ip_version":4,` +
		`"carriers":["tcp","quic"],"error":"","expire_time":"2026-01-15T00:00:00Z",` +
		`"allowed_hosts":["space.example","*.space.example"],` +
		`"record":"AQID","bootstrap":["BAUG","BwgJ"]}`
	result := &ExtenderActivateResult{}
	if err := json.Unmarshal([]byte(document), result); err != nil {
		t.Fatal(err)
	}
	expireTime := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	expected := &ExtenderActivateResult{
		Activated:    true,
		Ip:           "198.51.100.7",
		IpVersion:    4,
		Carriers:     []string{"tcp", "quic"},
		ExpireTime:   &expireTime,
		AllowedHosts: []string{"space.example", "*.space.example"},
		Record:       "AQID",
		Bootstrap:    []string{"BAUG", "BwgJ"},
	}
	if !reflect.DeepEqual(result, expected) {
		t.Fatalf("result = %+v, expected %+v", result, expected)
	}

	refusal := `{"activated":false,"error":"the tcp carrier did not answer"}`
	refused := &ExtenderActivateResult{}
	if err := json.Unmarshal([]byte(refusal), refused); err != nil {
		t.Fatal(err)
	}
	if refused.Activated || refused.Error != "the tcp carrier did not answer" {
		t.Fatalf("refusal = %+v", refused)
	}
	if refused.ExpireTime != nil {
		t.Fatalf("refusal expire time = %v, expected none", refused.ExpireTime)
	}
}

// The activation advertises every dns port that is listening, in dial order,
// beside the single port an operator that predates the list reads (L2).
func TestExtenderActivatorSendsDnsPorts(t *testing.T) {
	fixture := newTestActivatorFixture(t, func(settings *ExtenderActivatorSettings) {
		// the bound order is the server's; the wire order is the dial order
		settings.DnsPorts = func() []int { return []int{DefaultWhodisPort, DefaultDnsPort} }
	})

	fixture.waitPass()
	post := fixture.waitPost()
	if !slices.Equal(post.args.DnsPorts, []int{DefaultDnsPort, DefaultWhodisPort}) {
		t.Fatalf("dns ports = %v, expected 53 then 4053", post.args.DnsPorts)
	}
	if post.args.DnsPort != ExtenderDnsPort {
		t.Fatalf("dns port = %d, expected %d", post.args.DnsPort, ExtenderDnsPort)
	}
}
