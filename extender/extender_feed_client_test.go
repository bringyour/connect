// The feed client against a real extender (EXTENDER.md D4, E3).
//
// This test lives here rather than in connect root because it needs an
// extender server, and connect root must never import its own subpackage. The
// server half of the feed is written here with the exported codec, which is
// the same framing the phase 5a feed server will speak.

package extender

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect"
	"github.com/urnetwork/connect/protocol"
)

// The network space of the records in this test.
const testFeedNetworkHost = "space.example"

// feedServer is the in-process feed service the extender hands its taken-over
// streams to (A8). It serves the configured sample, then end_of_sample, then
// whatever a test pushes while the subscription is open.
type feedServer struct {
	t      *testing.T
	sample []*protocol.ExtenderFeedFrame
	pushes chan *protocol.ExtenderFeedFrame
	opened chan *protocol.ExtenderFeedRequest
	closed chan struct{}

	stateLock       sync.Mutex
	refuseRemaining int
}

func newFeedServer(t *testing.T, sample ...*protocol.ExtenderFeedFrame) *feedServer {
	server := &feedServer{
		t:      t,
		sample: sample,
		pushes: make(chan *protocol.ExtenderFeedFrame, 16),
		opened: make(chan *protocol.ExtenderFeedRequest, 16),
		closed: make(chan struct{}),
	}
	// closed first, so the handler returns before the extender's shutdown
	// joins it
	t.Cleanup(func() {
		close(server.closed)
	})
	return server
}

// The stream is owned until this returns, which is the A8 contract.
func (self *feedServer) handle(conn net.Conn) {
	request, err := connect.ReadExtenderFeedRequest(conn)
	if err != nil {
		return
	}
	select {
	case self.opened <- request:
	default:
	}
	if self.takeRefusal() {
		// the connection is dropped without a sample, which is what a failed
		// feed attempt looks like to the client
		return
	}
	for _, frame := range self.sample {
		if err := connect.WriteExtenderFeedFrame(conn, frame); err != nil {
			return
		}
	}
	if err := connect.WriteExtenderFeedFrame(conn, &protocol.ExtenderFeedFrame{
		Frame: &protocol.ExtenderFeedFrame_EndOfSample{EndOfSample: true},
	}); err != nil {
		return
	}
	if !request.Subscribe {
		return
	}
	for {
		select {
		case <-self.closed:
			return
		case frame := <-self.pushes:
			if err := connect.WriteExtenderFeedFrame(conn, frame); err != nil {
				return
			}
		case <-time.After(100 * time.Millisecond):
			// the keepalive of an idle subscription, at a test cadence
			if err := connect.WriteExtenderFeedFrame(conn, &protocol.ExtenderFeedFrame{
				Frame: &protocol.ExtenderFeedFrame_Keepalive{Keepalive: true},
			}); err != nil {
				return
			}
		}
	}
}

// Reports whether this connection is one of the refusals a test asked for.
func (self *feedServer) takeRefusal() bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if self.refuseRemaining <= 0 {
		return false
	}
	self.refuseRemaining -= 1
	return true
}

func (self *feedServer) setRefusals(count int) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.refuseRemaining = count
}

// One extender with an identity key and the feed service, plus the root key
// that signs the records in the test.
type feedFixture struct {
	extender      *extenderFixture
	feed          *feedServer
	rootPrivate   ed25519.PrivateKey
	rootPublic    ed25519.PublicKey
	extenderKey   ed25519.PublicKey
	extenderIp    netip.Addr
	extenderPorts map[string]int
}

func newFeedFixture(t *testing.T, feed *feedServer) *feedFixture {
	t.Helper()
	rootSeed, err := connect.NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	rootPrivateKey, err := connect.ExtenderPrivateKeyFromSeed(rootSeed)
	if err != nil {
		t.Fatal(err)
	}
	extenderSeed, err := connect.NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	extenderPublicKey, err := connect.ExtenderPublicKeyFromSeed(extenderSeed)
	if err != nil {
		t.Fatal(err)
	}

	// an open extender, which is what an operator activated one is (A4)
	fixture := newExtenderFixtureWithSecrets(t, "127.0.0.1", nil, func(settings *ExtenderSettings) {
		settings.IdentityKeySeed = extenderSeed
		settings.FeedConnHandler = feed.handle
	})
	return &feedFixture{
		extender:    fixture,
		feed:        feed,
		rootPrivate: rootPrivateKey,
		rootPublic:  rootPrivateKey.Public().(ed25519.PublicKey),
		extenderKey: extenderPublicKey,
		extenderIp:  fixture.ip,
		extenderPorts: map[string]int{
			connect.ExtenderCarrierTcp:  fixture.tcpPort,
			connect.ExtenderCarrierQuic: fixture.quicPort,
			connect.ExtenderCarrierDns:  fixture.dnsPort,
		},
	}
}

// One signed record for an address and carrier set.
func (self *feedFixture) signRecord(
	t *testing.T,
	extenderPublicKey ed25519.PublicKey,
	ip string,
	issueTime time.Time,
	carriers ...string,
) *protocol.ExtenderRecord {
	t.Helper()
	ipVersion := uint32(4)
	if addr, err := netip.ParseAddr(ip); err == nil && addr.Is6() {
		ipVersion = 6
	}
	body := &protocol.ExtenderRecordBody{
		PublicKey: extenderPublicKey,
		Addresses: []*protocol.ExtenderAddress{
			{
				Ip:        ip,
				IpVersion: ipVersion,
				Carriers:  carriers,
			},
		},
		TcpPort:      uint32(self.extenderPorts[connect.ExtenderCarrierTcp]),
		UdpPort:      uint32(self.extenderPorts[connect.ExtenderCarrierQuic]),
		DnsPort:      uint32(self.extenderPorts[connect.ExtenderCarrierDns]),
		DnsTld:       testDnsTld,
		CountryCode:  "us",
		IssueTimeMs:  uint64(issueTime.UnixMilli()),
		ExpireTimeMs: uint64(issueTime.Add(14 * 24 * time.Hour).UnixMilli()),
		NetworkHost:  testFeedNetworkHost,
	}
	record, err := connect.SignExtenderRecord(self.rootPrivate, body)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func (self *feedFixture) signRevocation(
	t *testing.T,
	extenderPublicKey ed25519.PublicKey,
	issueTime time.Time,
) *protocol.ExtenderRevocation {
	t.Helper()
	revocation, err := connect.SignExtenderRevocation(self.rootPrivate, &protocol.ExtenderRevocationBody{
		PublicKey:   extenderPublicKey,
		IssueTimeMs: uint64(issueTime.UnixMilli()),
		NetworkHost: testFeedNetworkHost,
	})
	if err != nil {
		t.Fatal(err)
	}
	return revocation
}

// The client dial configuration of one carrier of this extender, with the
// outer leaf pinned to the extender identity key (B3, E5).
func (self *feedFixture) extenderConfigForCarrier(carrier string) *connect.ExtenderConfig {
	connectMode, _ := connect.ExtenderConnectModeForCarrier(carrier)
	profile := connect.ExtenderProfile{
		ConnectMode: connectMode,
		ServerName:  testServerName,
		Port:        self.extenderPorts[carrier],
	}
	if carrier == connect.ExtenderCarrierDns {
		profile.DnsTld = testDnsTld
	}
	return &connect.ExtenderConfig{
		Profile:   profile,
		Ip:        self.extenderIp,
		PublicKey: self.extenderKey,
	}
}

// The feed client reaches the service over every carrier, receives the sample
// and then the subscription (D4).
func TestExtenderFeedClientReceivesTheSampleOverEveryCarrier(t *testing.T) {
	issueTime := time.Now()
	for _, carrier := range []string{
		connect.ExtenderCarrierTcp,
		connect.ExtenderCarrierQuic,
		connect.ExtenderCarrierDns,
	} {
		feed := newFeedServer(t)
		fixture := newFeedFixture(t, feed)
		feed.sample = []*protocol.ExtenderFeedFrame{
			{Frame: &protocol.ExtenderFeedFrame_Record{
				Record: fixture.signRecord(
					t,
					fixture.extenderKey,
					"198.51.100.20",
					issueTime,
					connect.ExtenderCarrierTcp,
				),
			}},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		stream, err := connect.DialExtenderFeed(
			ctx,
			fixture.extender.connectSettings(),
			fixture.extenderConfigForCarrier(carrier),
			&protocol.ExtenderFeedRequest{SampleCount: 16, Subscribe: true},
		)
		if err != nil {
			cancel()
			t.Fatalf("%s: %v", carrier, err)
		}
		if len(stream.Response().PublicKey) == 0 {
			cancel()
			stream.Close()
			t.Fatalf("%s: the extender answered with no identity key", carrier)
		}

		sampleCount := 0
		for {
			frame, err := stream.Next(ctx)
			if err != nil {
				cancel()
				stream.Close()
				t.Fatalf("%s: %v", carrier, err)
			}
			if frame.GetEndOfSample() {
				break
			}
			if frame.GetRecord() != nil {
				sampleCount += 1
			}
		}
		if sampleCount != 1 {
			cancel()
			stream.Close()
			t.Fatalf("%s: sample = %d records, expected 1", carrier, sampleCount)
		}

		// the subscription is open; a pushed revocation arrives on the same
		// stream
		feed.pushes <- &protocol.ExtenderFeedFrame{
			Frame: &protocol.ExtenderFeedFrame_Revocation{
				Revocation: fixture.signRevocation(t, fixture.extenderKey, issueTime),
			},
		}
		for {
			frame, err := stream.Next(ctx)
			if err != nil {
				cancel()
				stream.Close()
				t.Fatalf("%s: %v", carrier, err)
			}
			if frame.GetKeepalive() {
				// an idle subscription is alive; keep reading
				continue
			}
			if frame.GetRevocation() == nil {
				cancel()
				stream.Close()
				t.Fatalf("%s: frame = %+v, expected the pushed revocation", carrier, frame)
			}
			break
		}
		stream.Close()
		cancel()
	}
}

// The network client bootstraps over the injected resolver, samples the feed
// through the directory candidate, applies what it receives and keeps the
// subscription open (E3).
func TestExtenderNetworkClientSamplesAnExtender(t *testing.T) {
	issueTime := time.Now()
	feed := newFeedServer(t)
	fixture := newFeedFixture(t, feed)

	sampledPublicKey := newFeedExtenderKey(t)
	sampledIp := netip.MustParseAddr("198.51.100.30")
	feed.sample = []*protocol.ExtenderFeedFrame{
		{Frame: &protocol.ExtenderFeedFrame_Record{
			Record: fixture.signRecord(
				t,
				sampledPublicKey,
				sampledIp.String(),
				issueTime,
				connect.ExtenderCarrierTcp,
			),
		}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	directory := fixture.newDirectory(t, ctx, nil)
	// the fixture's own record makes it a verified candidate on its test port
	if _, err := directory.ApplyRecord(
		fixture.signRecord(t, fixture.extenderKey, fixture.extenderIp.String(), issueTime, connect.ExtenderCarrierTcp),
		connect.ExtenderSourceBootstrap,
	); err != nil {
		t.Fatal(err)
	}

	bootstrapIp := netip.MustParseAddr("198.51.100.40")
	networkClient := fixture.newNetworkClient(t, ctx, directory, func(settings *connect.ExtenderNetworkClientSettings) {
		settings.ResolveDns = func(ctx context.Context, name string) ([]netip.Addr, error) {
			return []netip.Addr{bootstrapIp}, nil
		}
	})

	// the sample reached the directory
	waitForDirectory(t, directory, func(snapshot *connect.ExtenderDirectorySnapshot) bool {
		for _, entry := range snapshot.Entries {
			if entry.Ip == sampledIp && entry.State == connect.ExtenderStateActive {
				return true
			}
		}
		return false
	}, "the sampled record never reached the directory")

	// and so did the dns bootstrap
	waitForDirectory(t, directory, func(snapshot *connect.ExtenderDirectorySnapshot) bool {
		for _, entry := range snapshot.Entries {
			if entry.Ip == bootstrapIp && entry.Source == connect.ExtenderSourceDns {
				return true
			}
		}
		return false
	}, "the dns bootstrap never reached the directory")

	status := networkClient.Status()
	if !status.FeedConnected {
		t.Fatal("the status does not report the open feed")
	}
	if status.FeedIp != fixture.extenderIp {
		t.Fatalf("feed ip = %s, expected %s", status.FeedIp, fixture.extenderIp)
	}
	if status.LastSampleTime.IsZero() {
		t.Fatal("the status carries no sample time")
	}
	if !status.InitialAttemptDone {
		t.Fatal("the status does not report the completed first attempt")
	}

	// a revocation pushed on the open subscription is applied
	feed.pushes <- &protocol.ExtenderFeedFrame{
		Frame: &protocol.ExtenderFeedFrame_Revocation{
			Revocation: fixture.signRevocation(t, sampledPublicKey, issueTime),
		},
	}
	waitForDirectory(t, directory, func(snapshot *connect.ExtenderDirectorySnapshot) bool {
		for _, entry := range snapshot.Entries {
			if entry.Ip == sampledIp && entry.State == connect.ExtenderStateRevoked {
				return true
			}
		}
		return false
	}, "the pushed revocation was never applied")
}

// A feed attempt that is refused is retried on the backoff until one answers
// (E3).
func TestExtenderNetworkClientReconnectsWithBackoff(t *testing.T) {
	issueTime := time.Now()
	feed := newFeedServer(t)
	fixture := newFeedFixture(t, feed)
	feed.setRefusals(2)

	sampledIp := netip.MustParseAddr("198.51.100.50")
	feed.sample = []*protocol.ExtenderFeedFrame{
		{Frame: &protocol.ExtenderFeedFrame_Record{
			Record: fixture.signRecord(
				t,
				newFeedExtenderKey(t),
				sampledIp.String(),
				issueTime,
				connect.ExtenderCarrierTcp,
			),
		}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// the hold after a failed feed attempt is what a retry has to wait out,
	// and this test has only one extender to retry against
	directory := fixture.newDirectory(t, ctx, func(settings *connect.ExtenderDirectorySettings) {
		settings.HoldTimeout = time.Millisecond
		settings.MaxHoldTimeout = 10 * time.Millisecond
	})
	if _, err := directory.ApplyRecord(
		fixture.signRecord(t, fixture.extenderKey, fixture.extenderIp.String(), issueTime, connect.ExtenderCarrierTcp),
		connect.ExtenderSourceBootstrap,
	); err != nil {
		t.Fatal(err)
	}
	fixture.newNetworkClient(t, ctx, directory, func(settings *connect.ExtenderNetworkClientSettings) {
		// no other candidate, so every retry goes back to this extender
		settings.ResolveDns = func(ctx context.Context, name string) ([]netip.Addr, error) {
			return nil, nil
		}
	})

	for i := range 3 {
		select {
		case <-feed.opened:
		case <-time.After(60 * time.Second):
			t.Fatalf("the feed was not reconnected (attempt %d)", i+1)
		}
	}
	waitForDirectory(t, directory, func(snapshot *connect.ExtenderDirectorySnapshot) bool {
		for _, entry := range snapshot.Entries {
			if entry.Ip == sampledIp {
				return true
			}
		}
		return false
	}, "the retried sample never reached the directory")
}

// A directory and a network client wired to this fixture.
func (self *feedFixture) newDirectory(
	t *testing.T,
	ctx context.Context,
	configure func(settings *connect.ExtenderDirectorySettings),
) *connect.ExtenderDirectory {
	t.Helper()
	settings := connect.DefaultExtenderDirectorySettings()
	settings.NetworkHosts = []string{testFeedNetworkHost}
	if configure != nil {
		configure(settings)
	}
	directory := connect.NewExtenderDirectory(ctx, settings)
	directory.SetRootKeys(connect.NewExtenderRootKeySet(self.rootPublic))
	t.Cleanup(directory.Close)
	return directory
}

func (self *feedFixture) newNetworkClient(
	t *testing.T,
	ctx context.Context,
	directory *connect.ExtenderDirectory,
	configure func(settings *connect.ExtenderNetworkClientSettings),
) *connect.ExtenderNetworkClient {
	t.Helper()
	strategySettings := connect.DefaultClientStrategySettings()
	strategySettings.ConnectSettings = *self.extender.connectSettings()
	strategySettings.ExtenderDirectory = directory
	clientStrategy := connect.NewClientStrategy(ctx, strategySettings)
	t.Cleanup(clientStrategy.Close)

	settings := connect.DefaultExtenderNetworkClientSettings()
	settings.ExtenderDnsName = "extender.space.example"
	settings.MinBackoff = 10 * time.Millisecond
	settings.MaxBackoff = 200 * time.Millisecond
	settings.DialTimeout = 20 * time.Second
	settings.HelloTimeout = 20 * time.Second
	settings.IpVersionSupported = func(ipVersion int) bool { return true }
	settings.Hello = func(ctx context.Context) (*connect.ExtenderHelloResult, error) {
		return &connect.ExtenderHelloResult{
			RootPublicKeyHexes: []string{hex.EncodeToString(self.rootPublic)},
		}, nil
	}
	if configure != nil {
		configure(settings)
	}
	networkClient := connect.NewExtenderNetworkClient(ctx, clientStrategy, directory, settings)
	t.Cleanup(networkClient.Close)
	return networkClient
}

// Waits for the directory to reach a state, driven by its change monitor so
// nothing here polls on a timer.
func waitForDirectory(
	t *testing.T,
	directory *connect.ExtenderDirectory,
	reached func(snapshot *connect.ExtenderDirectorySnapshot) bool,
	message string,
) {
	t.Helper()
	timeout := time.After(60 * time.Second)
	for {
		_, update := directory.ChangeMonitor().Get()
		if reached(directory.Snapshot()) {
			return
		}
		select {
		case <-update:
		case <-timeout:
			t.Fatal(message)
		}
	}
}

func newFeedExtenderKey(t *testing.T) ed25519.PublicKey {
	t.Helper()
	seed, err := connect.NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	publicKey, err := connect.ExtenderPublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	return publicKey
}
