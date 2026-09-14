package connect

import (
	"context"
	"encoding/json"
	mathrand "math/rand"
	"net"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// The extender network client (EXTENDER.md E3).
//
// One instance serves one network space. It owns everything that fills the
// directory from outside: the dns bootstrap, the root key refresh from hello,
// and the feed connection that carries the sample and, in the feed role, the
// live subscription.
//
// The shape is one `run` loop started by the constructor. It bootstraps, takes
// a sample, and then either holds the subscription open or sleeps until the
// next tick; every failure path goes back through the same backoff. Three
// things wake it early: a network path change, a drop below the low-water
// mark, and `Close`.
//
// Every clock and every side effect is a settings seam -- `Now`, `ResolveDns`,
// `Hello` -- so the whole loop is deterministic in tests.

// The hello answer this client reads. Both fields are additive (B4, C7): an
// operator that has not configured root keys answers with none, which leaves
// the stored anchor alone, and one that does not serve a gossip identity yet
// answers with an empty id, which means there is no operator to dial (D3).
type ExtenderHelloResult struct {
	// Hex ed25519 root public keys, the trust anchor of every record (B4).
	RootPublicKeyHexes []string
	// The operator gossip node's libp2p peer id, empty when it has none (C6).
	GossipPeerId string
}

// The json shape of the hello fields this client reads.
type extenderHelloResultJson struct {
	ExtenderRootPublicKeys []string `json:"extender_root_public_keys"`
	GossipPeerId           string   `json:"gossip_peer_id"`
}

type ExtenderNetworkClientSettings struct {
	Log Logger

	// The dns name whose A and AAAA answers bootstrap the directory (F1).
	// Empty disables the bootstrap, which leaves the client on the stored
	// directory alone.
	ExtenderDnsName string
	// The api url whose /hello carries the root keys (B4). Empty disables the
	// refresh.
	ApiUrl string

	// Subscribe keeps the feed stream open after the sample and applies
	// everything the server pushes (D5, the feed role).
	Subscribe bool
	// Records asked for in one sample (D4).
	SampleCount int

	// Reconnect backoff after a failed feed attempt, doubling to the max.
	MinBackoff time.Duration
	MaxBackoff time.Duration
	// The dns bootstrap repeats on this period, and so does the hello refresh.
	RebootstrapTimeout time.Duration
	// A directory with fewer usable addresses than this re-bootstraps at once.
	LowWaterCount int
	// Budget of one feed dial and of one hello.
	DialTimeout  time.Duration
	HelloTimeout time.Duration
	// Longest an open subscription may be silent. The server keepalives an
	// idle subscription every 30 s (D4), so a longer silence is a stream that
	// is no longer there. Not named in E3; without it a black-holed
	// subscription would never reconnect, because nothing else ends the read.
	SubscribeIdleTimeout time.Duration

	// ManualHosts are hostnames or ip literals configured by hand (K6). An ip
	// literal is added as a manual address at start; a hostname is resolved
	// through the resolver seam below at start and on every rebootstrap, and
	// its answers are added the same way. They supplement discovery: manual
	// addresses union with the dns bootstrap and with everything the feed and
	// the mesh deliver, and are never removed by policy.
	ManualHosts []string

	// DohSettings configures the bootstrap resolution. Nil takes the strategy
	// settings.
	DohSettings *DohSettings

	// The only clock this client reads. Tests install a fake one.
	Now func() time.Time
	// ResolveDns, when set, replaces the bootstrap resolution. Nil resolves A
	// and AAAA over DoH with the system resolver as the fallback (E3).
	ResolveDns func(ctx context.Context, name string) ([]netip.Addr, error)
	// Hello, when set, replaces the hello fetch. Nil reads /hello through the
	// client strategy.
	Hello func(ctx context.Context) (*ExtenderHelloResult, error)
	// IpVersionSupported, when set, replaces the host family probe. Nil uses
	// probeFamilySupport, which is what the strategy also dials by.
	IpVersionSupported func(ipVersion int) bool
}

func DefaultExtenderNetworkClientSettings() *ExtenderNetworkClientSettings {
	return &ExtenderNetworkClientSettings{
		Subscribe:            true,
		SampleCount:          DefaultExtenderFeedSampleCount,
		MinBackoff:           1 * time.Second,
		MaxBackoff:           5 * time.Minute,
		RebootstrapTimeout:   6 * time.Hour,
		LowWaterCount:        4,
		DialTimeout:          30 * time.Second,
		HelloTimeout:         30 * time.Second,
		SubscribeIdleTimeout: 90 * time.Second,
		Now:                  time.Now,
	}
}

// What the sdk status reports (E3, F2). Comparable, so it rides a
// MonitorValue and a consumer is woken only on an actual change.
type ExtenderNetworkClientStatus struct {
	FeedConnected bool
	// True while a sample or subscribe dial is in flight and no stream is up
	// yet, which is the app's yellow connecting state (K4).
	Connecting bool
	FeedIp     netip.Addr
	// The time of the last completed sample, zero when there has been none.
	LastSampleTime time.Time
	LastError      string
	// True once the first attempt has finished, whether or not it produced a
	// sample. The startup gate reads the same fact from the directory.
	InitialAttemptDone bool
	// The operator gossip node's peer id as hello last carried it, empty until
	// the operator serves one (C6, D3). The member role's node dials the
	// operator only once this is known.
	GossipPeerId string
}

// The state of the gossip network as the app's status dot shows it (K4, K5).
// The two roles read different evidence -- a feed app has a stream, a member
// has a mesh -- so the derivation lives here, once, rather than in each app.
const (
	ExtenderGossipStateConnected    = "connected"
	ExtenderGossipStateConnecting   = "connecting"
	ExtenderGossipStateDisconnected = "disconnected"
)

// The feed role's state: green while the stream is up, yellow while a dial is
// in flight, red otherwise -- backoff, no candidate, or disabled.
func ExtenderGossipStateForFeed(status ExtenderNetworkClientStatus) string {
	switch {
	case status.FeedConnected:
		return ExtenderGossipStateConnected
	case status.Connecting:
		return ExtenderGossipStateConnecting
	default:
		return ExtenderGossipStateDisconnected
	}
}

// The member role's state, from a gossip node status: green with at least one
// mesh peer, yellow while a peering round has dials in flight. The node status
// is passed as its two fields rather than as the value, because connect root
// cannot import its own gossip subpackage.
func ExtenderGossipStateForMember(meshPeerCount int, connecting bool) string {
	switch {
	case 0 < meshPeerCount:
		return ExtenderGossipStateConnected
	case connecting:
		return ExtenderGossipStateConnecting
	default:
		return ExtenderGossipStateDisconnected
	}
}

type ExtenderNetworkClient struct {
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	done      chan struct{}
	log       Logger

	clientStrategy *ClientStrategy
	directory      *ExtenderDirectory
	settings       *ExtenderNetworkClientSettings

	statusMonitor *MonitorValue[ExtenderNetworkClientStatus]
	// closed and replaced on a wake request; a network change and a low-water
	// drop both take this path
	wakeMonitor        *Monitor
	unsubNetworkChange func()

	stateLock sync.Mutex
	// the open subscription, so a network change can end it at once rather
	// than leaving the loop parked on a stream bound to the old path
	feedStream *ExtenderFeedStream
	// the manually configured hosts and the version that changes with them,
	// which is what makes `SetManualHosts` re-resolve at once rather than at
	// the next tick (K6)
	manualHosts        []string
	manualHostsVersion uint64
}

// The client is running when this returns: the directory has been told a first
// attempt is in flight, and the loop is up.
func NewExtenderNetworkClient(
	ctx context.Context,
	clientStrategy *ClientStrategy,
	directory *ExtenderDirectory,
	settings *ExtenderNetworkClientSettings,
) *ExtenderNetworkClient {
	if settings == nil {
		settings = DefaultExtenderNetworkClientSettings()
	}
	if settings.Now == nil {
		copied := *settings
		copied.Now = time.Now
		settings = &copied
	}
	cancelCtx, cancel := context.WithCancel(ctx)
	self := &ExtenderNetworkClient{
		ctx:            cancelCtx,
		cancel:         cancel,
		done:           make(chan struct{}),
		log:            loggerOrDefault(settings.Log),
		clientStrategy: clientStrategy,
		directory:      directory,
		settings:       settings,
		statusMonitor:  NewMonitorValue[ExtenderNetworkClientStatus](ExtenderNetworkClientStatus{}),
		wakeMonitor:    NewMonitor(),
		manualHosts:    slices.Clone(settings.ManualHosts),
	}
	directory.SetInitialSamplePending()
	// a path change invalidates the feed connection and the addresses that
	// were reachable on the old path
	self.unsubNetworkChange = AddNetworkChangeListener(self.networkChanged)
	go HandleError(func() {
		defer close(self.done)
		self.run()
	}, cancel)
	return self
}

func (self *ExtenderNetworkClient) Status() ExtenderNetworkClientStatus {
	return self.statusMonitor.Value()
}

// The status value and a channel armed at the same instant, for a consumer
// that renders it.
func (self *ExtenderNetworkClient) StatusMonitor() *MonitorValue[ExtenderNetworkClientStatus] {
	return self.statusMonitor
}

// A path change invalidates the open subscription: it is bound to the old
// path, and the loop would otherwise sit on it until the idle timeout. The
// stream is closed with no lock held, as any external object is.
func (self *ExtenderNetworkClient) networkChanged() {
	feedStream := func() *ExtenderFeedStream {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		return self.feedStream
	}()
	if feedStream != nil {
		feedStream.Close()
	}
	self.wakeMonitor.NotifyAll()
}

// Publishes the open subscription so a network change can end it.
func (self *ExtenderNetworkClient) setFeedStream(feedStream *ExtenderFeedStream) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.feedStream = feedStream
}

// Ends the loop and joins it.
func (self *ExtenderNetworkClient) Close() {
	self.closeOnce.Do(func() {
		if self.unsubNetworkChange != nil {
			self.unsubNetworkChange()
		}
		self.cancel()
		<-self.done
	})
}

// Releases the startup gate (E4). It is called as soon as the first sample
// completes -- not when the pass ends -- because in the feed role the pass
// lasts as long as the subscription, and a cold start must not wait out the
// gate timeout after the sample it was waiting for has already landed.
func (self *ExtenderNetworkClient) markInitialAttemptDone() {
	self.directory.SetInitialSampleDone()
	self.updateStatus(func(status *ExtenderNetworkClientStatus) {
		status.InitialAttemptDone = true
	})
}

func (self *ExtenderNetworkClient) updateStatus(update func(*ExtenderNetworkClientStatus)) {
	self.statusMonitor.Update(func(status ExtenderNetworkClientStatus) ExtenderNetworkClientStatus {
		update(&status)
		return status
	})
}

// The refresh loop. One pass bootstraps when it is due, refreshes the root
// keys when they are due, and then takes a sample, holding the subscription
// for the feed role. Every exit from a pass goes through the backoff, which a
// success resets.
func (self *ExtenderNetworkClient) run() {
	backoff := self.settings.MinBackoff
	var lastBootstrapTime time.Time
	var lastHelloTime time.Time
	var lastManualTime time.Time
	// the manual host list this loop has already applied; a reconfiguration
	// changes the version and re-resolves at once (K6)
	var manualHostsVersion uint64

	for {
		select {
		case <-self.ctx.Done():
			return
		default:
		}

		now := self.settings.Now()
		// subscribe before the reads below, so a wake that lands while this
		// pass runs is carried into the next wait instead of being lost
		wake := self.wakeMonitor.NotifyChannel()

		if lastHelloTime.IsZero() || self.settings.RebootstrapTimeout <= now.Sub(lastHelloTime) {
			if self.refreshRootKeys() {
				lastHelloTime = now
			}
		}
		if lastBootstrapTime.IsZero() ||
			self.settings.RebootstrapTimeout <= now.Sub(lastBootstrapTime) ||
			self.directory.ActiveCount(0) < self.settings.LowWaterCount {
			self.bootstrap()
			lastBootstrapTime = now
		}
		if version := self.manualHostsVersionValue(); lastManualTime.IsZero() ||
			version != manualHostsVersion ||
			self.settings.RebootstrapTimeout <= now.Sub(lastManualTime) {
			manualHostsVersion = self.applyManualHosts()
			lastManualTime = now
		}
		self.directory.Expire(self.settings.Now())

		// a subscription holds inside this call for as long as it lives; the
		// first attempt is marked done from inside, as soon as the sample
		// completes
		passStartTime := self.settings.Now()
		sampled := self.sample()
		// the first attempt is complete either way; the startup gate must not
		// wait on an attempt that has already failed
		self.markInitialAttemptDone()

		var wait time.Duration
		switch {
		case sampled && !self.settings.Subscribe:
			// a one-shot sample; nothing to do until the next refresh
			backoff = self.settings.MinBackoff
			wait = self.settings.RebootstrapTimeout
		case sampled:
			// the subscription ended, whatever ended it, so the next pass
			// reconnects through another candidate on the backoff. The backoff
			// resets only when the stream stayed up: an extender that accepts,
			// serves a sample and drops at once would otherwise be redialed
			// once a second forever, since a served sample is never a failure
			// the directory would hold it for.
			if self.settings.MaxBackoff <= self.settings.Now().Sub(passStartTime) {
				backoff = self.settings.MinBackoff
			}
			wait = backoff
			backoff = min(2*backoff, self.settings.MaxBackoff)
		default:
			wait = backoff
			backoff = min(2*backoff, self.settings.MaxBackoff)
		}
		if wait <= 0 {
			wait = self.settings.MinBackoff
		}

		select {
		case <-self.ctx.Done():
			return
		case <-wake:
		case <-time.After(wait):
		}
	}
}

// Reads hello and applies what it carries (B4, C7). An empty root key list
// leaves the stored anchor alone, which is what an operator that has not
// configured keys yet answers; the gossip peer id is published on the status
// either way, so the member role's node learns the operator as soon as the
// operator serves one.
func (self *ExtenderNetworkClient) refreshRootKeys() bool {
	hello := self.settings.Hello
	if hello == nil {
		if self.settings.ApiUrl == "" || self.clientStrategy == nil {
			return false
		}
		hello = self.hello
	}
	ctx, cancel := context.WithTimeout(self.ctx, self.settings.HelloTimeout)
	defer cancel()
	helloResult, err := hello(ctx)
	if err != nil {
		self.log.Infof("[extender]hello err = %s\n", err)
		return false
	}
	if helloResult == nil {
		return true
	}
	self.updateStatus(func(status *ExtenderNetworkClientStatus) {
		status.GossipPeerId = helloResult.GossipPeerId
	})
	if len(helloResult.RootPublicKeyHexes) == 0 {
		return true
	}
	keySet, err := NewExtenderRootKeySetFromHex(helloResult.RootPublicKeyHexes...)
	if err != nil {
		self.log.Infof("[extender]hello root keys err = %s\n", err)
		return false
	}
	self.directory.SetRootKeys(keySet)
	return true
}

func (self *ExtenderNetworkClient) hello(ctx context.Context) (*ExtenderHelloResult, error) {
	request, err := HelloRequestFromUrl(ctx, self.settings.ApiUrl, "")
	if err != nil {
		return nil, err
	}
	bodyBytes, err := HttpGetWithStrategyRaw(ctx, self.clientStrategy, request.URL.String(), "")
	if err != nil {
		return nil, err
	}
	helloResultJson := &extenderHelloResultJson{}
	if err := json.Unmarshal(bodyBytes, helloResultJson); err != nil {
		return nil, err
	}
	return &ExtenderHelloResult{
		RootPublicKeyHexes: helloResultJson.ExtenderRootPublicKeys,
		GossipPeerId:       helloResultJson.GossipPeerId,
	}, nil
}

// Replaces the manually configured hosts and re-resolves them at once (K6).
// The addresses the previous list produced stay in the directory: a manual
// address is only removed by the directory being rebuilt, which is what
// saving the setting does.
func (self *ExtenderNetworkClient) SetManualHosts(hosts []string) {
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		self.manualHosts = slices.Clone(hosts)
		self.manualHostsVersion += 1
	}()
	// the resolution runs here rather than only at the next pass: in the feed
	// role the loop is parked on a live subscription for as long as it lasts,
	// and a reconfiguration must not wait that out. It is bounded by the
	// client context, and the loop's own apply of the same version is
	// idempotent, so the overlap costs at most one resolution.
	go HandleError(func() {
		self.applyManualHosts()
	})
	self.wakeMonitor.NotifyAll()
}

// The configured hosts and the version they are at, read together so the loop
// records exactly the version it applied.
func (self *ExtenderNetworkClient) manualHostsValue() ([]string, uint64) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return slices.Clone(self.manualHosts), self.manualHostsVersion
}

func (self *ExtenderNetworkClient) manualHostsVersionValue() uint64 {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.manualHostsVersion
}

// Adds the manually configured hosts (K6). An ip literal is added as it
// stands; a name is resolved through the same seam the dns bootstrap uses, so
// a host that configured DoH resolves manual hosts over DoH too. Every answer
// becomes a manual address, which the removal policy never takes away, and
// unions with the dns bootstrap and with everything the feed and the mesh
// deliver. Returns the version applied.
func (self *ExtenderNetworkClient) applyManualHosts() uint64 {
	hosts, version := self.manualHostsValue()
	if len(hosts) == 0 {
		return version
	}
	resolve := self.settings.ResolveDns
	if resolve == nil {
		resolve = self.resolveDns
	}
	ctx, cancel := context.WithTimeout(self.ctx, self.settings.HelloTimeout)
	defer cancel()
	for _, host := range hosts {
		host = strings.TrimSpace(host)
		if host == "" {
			continue
		}
		if ip, err := netip.ParseAddr(host); err == nil {
			self.directory.AddManual(ip)
			continue
		}
		ips, err := resolve(ctx, host)
		if err != nil {
			self.log.Infof("[extender]manual host %s err = %s\n", host, err)
			continue
		}
		for _, ip := range ips {
			self.directory.AddManual(ip)
		}
	}
	return version
}

// Resolves the extender dns name and adds every answer as an unverified
// address with source dns (E3). A record naming one of these upgrades it.
func (self *ExtenderNetworkClient) bootstrap() {
	if self.settings.ExtenderDnsName == "" {
		return
	}
	resolve := self.settings.ResolveDns
	if resolve == nil {
		resolve = self.resolveDns
	}
	// the resolution shares the hello budget: both are one short name-service
	// round trip before anything can be dialed
	ctx, cancel := context.WithTimeout(self.ctx, self.settings.HelloTimeout)
	defer cancel()
	ips, err := resolve(ctx, self.settings.ExtenderDnsName)
	if err != nil {
		self.log.Infof("[extender]bootstrap err = %s\n", err)
		return
	}
	for _, ip := range ips {
		self.directory.AddBootstrap(ip, ExtenderSourceDns)
	}
}

// The default bootstrap resolution: A and AAAA over the strategy's DoH
// settings, with the system resolver as the fallback when DoH yields nothing.
func (self *ExtenderNetworkClient) resolveDns(
	ctx context.Context,
	name string,
) ([]netip.Addr, error) {
	dohSettings := self.settings.DohSettings
	if dohSettings == nil && self.clientStrategy != nil {
		dohSettings = self.clientStrategy.settings.DohSettings
	}
	ips := []netip.Addr{}
	if dohSettings != nil {
		for _, query := range []struct {
			ipVersion  int
			recordType string
		}{
			{ipVersion: 4, recordType: "A"},
			{ipVersion: 6, recordType: "AAAA"},
		} {
			if !self.ipVersionSupported(query.ipVersion) {
				continue
			}
			for ip := range DohQuery(ctx, query.ipVersion, query.recordType, dohSettings, name) {
				ips = append(ips, ip.Unmap())
			}
		}
	}
	if 0 < len(ips) {
		return ips, nil
	}
	// the fallback is the ordinary resolver, which is all a host with a
	// hostile or blocked DoH path has left. It goes through dialResolver so a
	// configured resolver wins and, on a host steering its own sockets around
	// the tunnel it provides, the egress-bound resolver is used instead of the
	// OS one (egress_dial.go).
	var customResolver *net.Resolver
	if self.clientStrategy != nil {
		customResolver = self.clientStrategy.settings.ConnectSettings.Resolver
	}
	netIps, err := dialResolver(customResolver).LookupNetIP(ctx, "ip", name)
	if err != nil {
		return nil, err
	}
	for _, ip := range netIps {
		ips = append(ips, ip.Unmap())
	}
	return ips, nil
}

func (self *ExtenderNetworkClient) ipVersionSupported(ipVersion int) bool {
	if self.settings.IpVersionSupported != nil {
		return self.settings.IpVersionSupported(ipVersion)
	}
	return probeFamilySupport(ipVersion)
}

// Takes one sample from the best candidate that answers, applying every frame.
// It reports whether a sample completed. In the feed role the same stream is
// then read until it ends, which is what makes a pass long lived.
func (self *ExtenderNetworkClient) sample() bool {
	candidates := self.candidates()
	if len(candidates) == 0 {
		// nothing to dial is not connecting, it is disconnected (K4)
		self.updateStatus(func(status *ExtenderNetworkClientStatus) {
			status.FeedConnected = false
			status.Connecting = false
			status.FeedIp = netip.Addr{}
			status.LastError = "no extender candidate"
		})
		return false
	}

	// the whole pass is the connecting state, from the first dial to the last
	// candidate; a dial that connects clears it inside `runFeed`, so a live
	// subscription never reads as connecting (K4)
	self.updateStatus(func(status *ExtenderNetworkClientStatus) {
		status.Connecting = true
	})
	defer self.updateStatus(func(status *ExtenderNetworkClientStatus) {
		status.Connecting = false
	})

	for _, candidate := range candidates {
		select {
		case <-self.ctx.Done():
			return false
		default:
		}
		sampled, err := self.sampleCandidate(candidate)
		if sampled {
			return true
		}
		if err != nil {
			self.log.Infof("[extender]feed %s err = %s\n", candidate.Ip, err)
			self.updateStatus(func(status *ExtenderNetworkClientStatus) {
				status.FeedConnected = false
				status.FeedIp = netip.Addr{}
				status.LastError = err.Error()
			})
		}
	}
	return false
}

// Every candidate of a family this host has, verified first (E3).
func (self *ExtenderNetworkClient) candidates() []*ExtenderCandidate {
	candidates := []*ExtenderCandidate{}
	for _, ipVersion := range []int{4, 6} {
		if !self.ipVersionSupported(ipVersion) {
			continue
		}
		candidates = append(
			candidates,
			self.directory.Candidates(ipVersion, self.settings.LowWaterCount+1)...,
		)
	}
	return candidates
}

// Tries the carriers of one candidate in order -- tcp, then quic, then dns --
// and runs the feed on the first that answers.
func (self *ExtenderNetworkClient) sampleCandidate(
	candidate *ExtenderCandidate,
) (sampled bool, resultErr error) {
	connectSettings := DefaultConnectSettings()
	if self.clientStrategy != nil {
		connectSettings = &self.clientStrategy.settings.ConnectSettings
	}
	for _, carrier := range orderedExtenderCarriers(candidate.Carriers) {
		select {
		case <-self.ctx.Done():
			return false, nil
		default:
		}
		connectMode, ok := ExtenderConnectModeForCarrier(carrier)
		if !ok {
			continue
		}
		extenderConfig := extenderFeedConfig(candidate, connectMode)
		if extenderConfig == nil {
			continue
		}
		sampled, err := self.runFeed(connectSettings, extenderConfig)
		if sampled {
			return true, nil
		}
		resultErr = err
		self.directory.RecordFailure(candidate.Ip, connectMode)
	}
	return false, resultErr
}

// Opens the feed, applies the sample, and in the feed role keeps applying
// until the stream ends. It reports whether the sample completed.
func (self *ExtenderNetworkClient) runFeed(
	connectSettings *ConnectSettings,
	extenderConfig *ExtenderConfig,
) (sampled bool, resultErr error) {
	dialCtx, dialCancel := context.WithTimeout(self.ctx, self.settings.DialTimeout)
	defer dialCancel()
	stream, err := DialExtenderFeed(
		dialCtx,
		connectSettings,
		extenderConfig,
		&protocol.ExtenderFeedRequest{
			SampleCount: uint32(self.settings.SampleCount),
			Subscribe:   self.settings.Subscribe,
		},
	)
	if err != nil {
		return false, err
	}
	defer stream.Close()
	self.setFeedStream(stream)
	defer self.setFeedStream(nil)

	self.directory.RecordSuccess(extenderConfig.Ip, extenderConfig.Profile.ConnectMode)
	self.directory.SetInUse(extenderConfig.Ip, 1)
	defer self.directory.SetInUse(extenderConfig.Ip, -1)
	self.updateStatus(func(status *ExtenderNetworkClientStatus) {
		status.FeedConnected = true
		status.Connecting = false
		status.FeedIp = extenderConfig.Ip
		status.LastError = ""
	})
	defer self.updateStatus(func(status *ExtenderNetworkClientStatus) {
		status.FeedConnected = false
		status.FeedIp = netip.Addr{}
	})

	// the sample is bounded by the dial budget; the subscription that follows
	// is bounded only by the client lifetime and the server's keepalive
	sampleCtx, sampleCancel := context.WithTimeout(self.ctx, self.settings.DialTimeout)
	defer sampleCancel()
	for {
		readCtx := sampleCtx
		var readCancel context.CancelFunc
		if sampled {
			// each subscribed read is bounded by the idle timeout, so a
			// subscription that went silent ends instead of parking the loop.
			// A zero timeout disables the bound rather than expiring at once.
			readCtx = self.ctx
			if 0 < self.settings.SubscribeIdleTimeout {
				readCtx, readCancel = context.WithTimeout(self.ctx, self.settings.SubscribeIdleTimeout)
			}
		}
		frame, err := stream.Next(readCtx)
		if readCancel != nil {
			readCancel()
		}
		if err != nil {
			if sampled {
				return true, nil
			}
			return false, err
		}
		switch {
		case frame.GetRecord() != nil:
			if _, err := self.directory.ApplyRecord(frame.GetRecord(), ExtenderSourceFeed); err != nil {
				self.log.Infof("[extender]feed record err = %s\n", err)
			}
		case frame.GetRevocation() != nil:
			if _, err := self.directory.ApplyRevocation(frame.GetRevocation()); err != nil {
				self.log.Infof("[extender]feed revocation err = %s\n", err)
			}
		case frame.GetEndOfSample():
			sampled = true
			sampleTime := self.settings.Now()
			self.updateStatus(func(status *ExtenderNetworkClientStatus) {
				status.LastSampleTime = sampleTime
				status.LastError = ""
			})
			self.markInitialAttemptDone()
			if !self.settings.Subscribe {
				return true, nil
			}
		case frame.GetKeepalive():
			// an idle subscription is still alive; nothing to apply
		}
	}
}

// The carrier order of E3: tcp, then quic, then dns. A carrier the record does
// not list is not tried.
func orderedExtenderCarriers(carriers []string) []string {
	ordered := []string{}
	for _, carrier := range []string{ExtenderCarrierTcp, ExtenderCarrierQuic, ExtenderCarrierDns} {
		for _, candidateCarrier := range carriers {
			if candidateCarrier == carrier {
				ordered = append(ordered, carrier)
				break
			}
		}
	}
	return ordered
}

// The dial configuration of one candidate carrier. The outer name is one
// random spoof domain (A10); with no bundled list the extender ip is presented,
// which sends no sni at all rather than naming the destination -- a feed dial
// has no destination host.
func extenderFeedConfig(
	candidate *ExtenderCandidate,
	connectMode ExtenderConnectMode,
) *ExtenderConfig {
	profile := ExtenderProfile{
		ConnectMode: connectMode,
		ServerName:  candidate.Ip.String(),
	}
	if spoofDomains := SpoofDomains(); 0 < len(spoofDomains) {
		profile.ServerName = spoofDomains[mathrand.Intn(len(spoofDomains))]
	}
	switch connectMode {
	case ExtenderConnectModeQuic:
		profile.Port = candidate.UdpPort
	case ExtenderConnectModeDns:
		profile.Port = candidate.DnsPort
		profile.DnsTld = candidate.DnsTld
	default:
		profile.Port = candidate.TcpPort
	}
	if profile.Port <= 0 {
		return nil
	}
	return &ExtenderConfig{
		Profile:   profile,
		Ip:        candidate.Ip,
		PublicKey: candidate.PublicKey,
	}
}
