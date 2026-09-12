package connect

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/protocol"
)

// The extender activation loop (EXTENDER.md C2, G3).
//
// An extender proves itself to the operator rather than announcing itself: it
// posts the carriers it is listening on, the operator probes back on the caller
// address of that request, and what comes back is a signed record naming the
// address the operator actually saw. That is why the activation is posted once
// per family, to the family api url, and why it must never cross an extender --
// the operator would see the extender's address and publish a record for the
// wrong host.
//
// The loop runs on five triggers (G3): start, the 24 hour tick, the own key
// observed revoked in the directory, a changed caller address from hello, and a
// network change. Everything else is backoff: a refusal holds the next attempt
// for ten minutes, doubling to six hours, and a success resets it.
//
// Every clock and every side effect is a settings seam -- `Now`,
// `IpVersionSupported`, `AddNetworkChangeListener` -- so the whole loop is
// deterministic in tests.
//
// The activator is safe for concurrent use.

// The activation endpoint, appended to a family api url (C2).
const ExtenderActivatePath = "/network/extender-activate"

// The activation request (C2). The json is the server's contract; the field
// names are the wire and are not ours to rename.
type ExtenderActivateArgs struct {
	PublicKeyHex string   `json:"public_key_hex"`
	TcpPort      int      `json:"tcp_port"`
	UdpPort      int      `json:"udp_port"`
	DnsPort      int      `json:"dns_port"`
	DnsTld       string   `json:"dns_tld"`
	Carriers     []string `json:"carriers"`
}

// The activation answer (C2). A refusal is a normal answer with `Activated`
// false and `Error` naming the carrier that failed its probe, not a request
// error, so the caller can act on it rather than on an http status.
type ExtenderActivateResult struct {
	Activated bool   `json:"activated"`
	Ip        string `json:"ip,omitempty"`
	IpVersion int    `json:"ip_version,omitempty"`
	// the carriers that passed their probe, which is what the signed record
	// lists
	Carriers   []string   `json:"carriers,omitempty"`
	Error      string     `json:"error,omitempty"`
	ExpireTime *time.Time `json:"expire_time,omitempty"`
	// the operator patterns this extender may forward to (A5)
	AllowedHosts []string `json:"allowed_hosts,omitempty"`
	// base64 of a serialized protocol.ExtenderRecord, this extender's own
	Record string `json:"record,omitempty"`
	// base64 of serialized protocol.ExtenderRecord messages of other active
	// extenders, the directory this extender starts from (D6)
	Bootstrap []string `json:"bootstrap,omitempty"`
}

// PostExtenderActivate posts one activation to one family api url (C2). The
// strategy must be direct-only: an activation that crossed an extender would
// publish that extender's address as this host's.
func PostExtenderActivate(
	ctx context.Context,
	clientStrategy *ClientStrategy,
	apiUrl string,
	byJwt string,
	args *ExtenderActivateArgs,
) (*ExtenderActivateResult, error) {
	if clientStrategy == nil {
		return nil, fmt.Errorf("the extender activation needs a client strategy")
	}
	if args == nil {
		return nil, fmt.Errorf("the extender activation needs args")
	}
	apiUrl = strings.TrimRight(strings.TrimSpace(apiUrl), "/")
	if apiUrl == "" {
		return nil, fmt.Errorf("the extender activation needs an api url")
	}
	return HttpPostWithStrategy(
		ctx,
		clientStrategy,
		apiUrl+ExtenderActivatePath,
		args,
		byJwt,
		&ExtenderActivateResult{},
		NewNoopApiCallback[*ExtenderActivateResult](),
	)
}

type ExtenderActivatorSettings struct {
	Log Logger

	// The family api urls (C2). An activation is posted to the url of the
	// family it activates, so the caller address the operator probes back has
	// a known family. Either may be empty, which skips that family.
	ApiUrlV4 string
	ApiUrlV6 string
	// The api url whose `/hello` reports the caller address this extender is
	// published under (G3). Empty disables the address check.
	HelloUrl string

	// The client jwt. A getter rather than a value, since the jwt refreshes
	// and an activation may be 24 hours after the last one.
	ByJwt func() string

	// The direct-only strategy every activation and hello is posted through
	// (A4, C2). An activation that crossed an extender would tell the operator
	// the extender's address.
	ClientStrategy *ClientStrategy

	// This extender's identity key (B1), which the operator probes back
	// against and signs the record for.
	PublicKey []byte

	// The carrier ports and the encoding tld this extender serves (C1, C2).
	// Zero ports and an empty tld take the operator's defaults.
	TcpPort int
	UdpPort int
	DnsPort int
	DnsTld  string

	// The carriers that are listening right now (G2). Only these are offered,
	// so a carrier whose bind failed is never probed. Nil offers none, which
	// holds every activation.
	Carriers func() []string

	// The directory the signed record and the bootstrap records are applied
	// into (C2, E1). It is also where a revocation of this extender's own key
	// is observed, which is one of the re-activation triggers (G3).
	Directory *ExtenderDirectory

	// OnActivated runs after each successful activation of one family, with
	// the result that produced it. The extender's mesh listen addresses are
	// updated from it (D2, G4). It runs on the loop goroutine and must not
	// block.
	OnActivated func(ipVersion int, result *ExtenderActivateResult)

	// The re-activation period (G3).
	ActivateTimeout time.Duration
	// The caller address check period (G3).
	AddressCheckTimeout time.Duration
	// Backoff after a failed pass, doubling to the max (G3).
	MinBackoff time.Duration
	MaxBackoff time.Duration
	// Budget of one activation post and of one hello.
	RequestTimeout time.Duration

	// The only clock this loop reads. Tests install a fake one.
	Now func() time.Time
	// IpVersionSupported, when set, replaces the host family probe. Nil uses
	// FamilySupported, which is what a family without a global address answers
	// no for (G3).
	IpVersionSupported func(ipVersion int) bool
	// AddNetworkChangeListener, when set, replaces the process network change
	// subscription. It returns the unsubscribe. Nil takes the process one.
	AddNetworkChangeListener func(listener func()) func()
}

func DefaultExtenderActivatorSettings() *ExtenderActivatorSettings {
	return &ExtenderActivatorSettings{
		TcpPort:             ExtenderTcpPort,
		UdpPort:             ExtenderQuicPort,
		DnsPort:             ExtenderDnsPort,
		DnsTld:              DefaultExtenderDnsTld,
		ActivateTimeout:     24 * time.Hour,
		AddressCheckTimeout: 1 * time.Hour,
		MinBackoff:          10 * time.Minute,
		MaxBackoff:          6 * time.Hour,
		RequestTimeout:      60 * time.Second,
		Now:                 time.Now,
	}
}

// The activation state of one family, which the sdk provider status renders
// (F3). `AllowedHosts` is what the operator last said this extender may
// forward to (A5); the extender's own whitelist is configured, not taken from
// here, so a difference is visible rather than silently applied.
type ExtenderFamilyActivationStatus struct {
	IpVersion int
	Activated bool
	Ip        netip.Addr
	// The last completed activation attempt, successful or not.
	LastActivationTime time.Time
	LastError          string
	// When a revocation of this extender's key was last observed in the
	// directory (G3).
	RevokedTime  time.Time
	ExpireTime   time.Time
	AllowedHosts []string
}

// The whole activation state, one entry per family in ip version order.
type ExtenderActivatorStatus struct {
	Families []*ExtenderFamilyActivationStatus
}

// The entry of one family, nil when the activator does not run that family.
func (self *ExtenderActivatorStatus) Family(ipVersion int) *ExtenderFamilyActivationStatus {
	for _, family := range self.Families {
		if family.IpVersion == ipVersion {
			return family
		}
	}
	return nil
}

type ExtenderActivator struct {
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	done      chan struct{}
	log       Logger

	settings *ExtenderActivatorSettings

	// increments on every status change; a consumer that renders the status
	// watches it, exactly as it watches the directory's
	changeMonitor *MonitorValue[uint64]
	// closed and replaced on a wake request; a network change takes this path
	wakeMonitor        *Monitor
	unsubNetworkChange func()

	stateLock sync.Mutex
	version   uint64
	families  map[int]*ExtenderFamilyActivationStatus
	// true while a revocation of this key is standing in the directory, so the
	// re-activation triggers on the edge and the backoff governs the retry
	revokedObserved bool
	// set by a network change, taken by the next pass
	forcedActivate bool
}

// The activator is running when this returns: the loop is up and its first
// pass has been started.
func NewExtenderActivator(
	ctx context.Context,
	settings *ExtenderActivatorSettings,
) *ExtenderActivator {
	if settings == nil {
		settings = DefaultExtenderActivatorSettings()
	}
	copied := *settings
	if copied.Now == nil {
		copied.Now = time.Now
	}
	if copied.ActivateTimeout <= 0 {
		copied.ActivateTimeout = 24 * time.Hour
	}
	if copied.AddressCheckTimeout <= 0 {
		copied.AddressCheckTimeout = 1 * time.Hour
	}
	if copied.MinBackoff <= 0 {
		copied.MinBackoff = 10 * time.Minute
	}
	if copied.MaxBackoff < copied.MinBackoff {
		copied.MaxBackoff = copied.MinBackoff
	}
	if copied.RequestTimeout <= 0 {
		copied.RequestTimeout = 60 * time.Second
	}
	settings = &copied

	cancelCtx, cancel := context.WithCancel(ctx)
	self := &ExtenderActivator{
		ctx:           cancelCtx,
		cancel:        cancel,
		done:          make(chan struct{}),
		log:           loggerOrDefault(settings.Log),
		settings:      settings,
		changeMonitor: NewMonitorValue[uint64](0),
		wakeMonitor:   NewMonitor(),
		families:      map[int]*ExtenderFamilyActivationStatus{},
	}
	for _, ipVersion := range self.ipVersions() {
		self.families[ipVersion] = &ExtenderFamilyActivationStatus{IpVersion: ipVersion}
	}

	addNetworkChangeListener := settings.AddNetworkChangeListener
	if addNetworkChangeListener == nil {
		addNetworkChangeListener = AddNetworkChangeListener
	}
	// a path change can move this host's public address, which is the address
	// the operator published for it
	self.unsubNetworkChange = addNetworkChangeListener(self.networkChanged)

	go HandleError(func() {
		defer close(self.done)
		self.run()
	}, cancel)
	return self
}

// The families this activator runs, in ip version order. A family with no api
// url is not run at all, so it never appears in the status.
func (self *ExtenderActivator) ipVersions() []int {
	ipVersions := []int{}
	if strings.TrimSpace(self.settings.ApiUrlV4) != "" {
		ipVersions = append(ipVersions, 4)
	}
	if strings.TrimSpace(self.settings.ApiUrlV6) != "" {
		ipVersions = append(ipVersions, 6)
	}
	return ipVersions
}

func (self *ExtenderActivator) apiUrl(ipVersion int) string {
	if ipVersion == 6 {
		return strings.TrimSpace(self.settings.ApiUrlV6)
	}
	return strings.TrimSpace(self.settings.ApiUrlV4)
}

// The status as a consumer reads it. The entries are copies, so a renderer
// cannot alias the loop's state.
func (self *ExtenderActivator) Status() *ExtenderActivatorStatus {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	status := &ExtenderActivatorStatus{Families: []*ExtenderFamilyActivationStatus{}}
	for _, ipVersion := range []int{4, 6} {
		family, ok := self.families[ipVersion]
		if !ok {
			continue
		}
		copied := *family
		copied.AllowedHosts = slices.Clone(family.AllowedHosts)
		status.Families = append(status.Families, &copied)
	}
	return status
}

// The change counter. A consumer reads the status and waits on the channel the
// same `Get` returned, which is what keeps the read and the subscribe from
// separating.
func (self *ExtenderActivator) ChangeMonitor() *MonitorValue[uint64] {
	return self.changeMonitor
}

// Wakes the loop, which re-activates every family (G3). A path change can move
// this host's public address.
func (self *ExtenderActivator) networkChanged() {
	self.stateLock.Lock()
	// the next pass activates whatever the cadence says, because the address
	// the operator published may no longer be this host's
	self.forcedActivate = true
	self.stateLock.Unlock()
	self.wakeMonitor.NotifyAll()
}

// Ends the loop and joins it.
func (self *ExtenderActivator) Close() {
	self.closeOnce.Do(func() {
		if self.unsubNetworkChange != nil {
			self.unsubNetworkChange()
		}
		self.cancel()
		<-self.done
	})
}

// Publishes a status change. Called with the state lock held, so the change and
// its notification cannot be separated.
func (self *ExtenderActivator) changedWithLock() {
	self.version += 1
	self.changeMonitor.Set(self.version)
}

// The activation loop. One pass observes the directory for a revocation of this
// key, checks the caller address when it is due, and activates when the cadence
// or a trigger says so. Everything the pass needs is read after the subscribe
// above it, so a trigger that lands while the pass runs is carried into the
// next wait rather than lost.
func (self *ExtenderActivator) run() {
	backoff := self.settings.MinBackoff
	var nextActivateTime time.Time
	var nextAddressCheckTime time.Time
	var clientAddress string

	for {
		select {
		case <-self.ctx.Done():
			return
		default:
		}

		now := self.settings.Now()
		wake := self.wakeMonitor.NotifyChannel()
		// the directory is read below, so the subscribe belongs here: a
		// revocation applied while this pass runs must wake the next wait.
		// With no directory the channel stays nil, which never fires.
		var directoryChange chan struct{}
		if self.settings.Directory != nil {
			_, directoryChange = self.settings.Directory.ChangeMonitor().Get()
		}

		if self.observeRevocation(now) {
			nextActivateTime = now
		}
		if self.takeForcedActivate() {
			nextActivateTime = now
		}

		if self.settings.HelloUrl != "" && !now.Before(nextAddressCheckTime) {
			address, err := self.hello()
			if err != nil {
				// a failed check retries on the backoff floor rather than on
				// every wake, which a directory change would otherwise be
				self.log.Infof("[extender]activate hello err = %s\n", err)
				nextAddressCheckTime = now.Add(self.settings.MinBackoff)
			} else {
				nextAddressCheckTime = now.Add(self.settings.AddressCheckTimeout)
				if clientAddress != "" && address != clientAddress {
					// the operator published an address this host no longer
					// has; the record must be reissued for the new one
					self.log.Infof(
						"[extender]activate address %s -> %s\n", clientAddress, address)
					nextActivateTime = now
				}
				clientAddress = address
			}
		}

		if !now.Before(nextActivateTime) {
			if self.activate(now) {
				backoff = self.settings.MinBackoff
				nextActivateTime = now.Add(self.settings.ActivateTimeout)
			} else {
				nextActivateTime = now.Add(backoff)
				backoff = min(2*backoff, self.settings.MaxBackoff)
			}
		}

		wait := nextActivateTime.Sub(now)
		if self.settings.HelloUrl != "" {
			wait = min(wait, nextAddressCheckTime.Sub(now))
		}
		if wait <= 0 {
			// nothing above left a deadline in the past; a zero here would be
			// a busy loop
			wait = self.settings.MinBackoff
		}

		select {
		case <-self.ctx.Done():
			return
		case <-wake:
		case <-directoryChange:
		case <-time.After(wait):
		}
	}
}

// Reports whether a revocation of this extender's key has just appeared in the
// directory (G3, B5). It is an edge, not a level: a re-activation that the
// operator refuses leaves the revocation standing, and retrying it on every
// directory change instead of on the backoff would spin.
func (self *ExtenderActivator) observeRevocation(now time.Time) (observed bool) {
	if self.settings.Directory == nil || len(self.settings.PublicKey) == 0 {
		return false
	}
	revoked := false
	for _, entry := range self.settings.Directory.Snapshot().Entries {
		if entry.State == ExtenderStateRevoked && slices.Equal(entry.PublicKey, self.settings.PublicKey) {
			revoked = true
			break
		}
	}

	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if revoked == self.revokedObserved {
		return false
	}
	self.revokedObserved = revoked
	if !revoked {
		return false
	}
	for _, family := range self.families {
		family.Activated = false
		family.RevokedTime = now
	}
	self.changedWithLock()
	return true
}

// Takes and clears the network change trigger.
func (self *ExtenderActivator) takeForcedActivate() bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	forced := self.forcedActivate
	self.forcedActivate = false
	return forced
}

// Reads the caller address this host is seen at (G3). Only the address is
// read; the root keys of the same answer are the network client's business
// (B4, E3).
func (self *ExtenderActivator) hello() (string, error) {
	if self.settings.ClientStrategy == nil {
		return "", fmt.Errorf("the extender activation needs a client strategy")
	}
	ctx, cancel := context.WithTimeout(self.ctx, self.settings.RequestTimeout)
	defer cancel()
	request, err := HelloRequestFromUrl(ctx, self.settings.HelloUrl, self.byJwt())
	if err != nil {
		return "", err
	}
	bodyBytes, err := HttpGetWithStrategyRaw(ctx, self.settings.ClientStrategy, request.URL.String(), self.byJwt())
	if err != nil {
		return "", err
	}
	helloResult := &extenderHelloResult{}
	if err := json.Unmarshal(bodyBytes, helloResult); err != nil {
		return "", err
	}
	if helloResult.ClientAddress == "" {
		return "", fmt.Errorf("hello carried no client address")
	}
	return extenderClientAddressIp(helloResult.ClientAddress), nil
}

// The address half of a caller address. The operator reports the source port
// with it, and that changes on every connection: comparing the whole string
// would re-activate on every check (G3).
func extenderClientAddressIp(clientAddress string) string {
	clientAddress = strings.TrimSpace(clientAddress)
	if addrPort, err := netip.ParseAddrPort(clientAddress); err == nil {
		return addrPort.Addr().Unmap().String()
	}
	if addr, err := netip.ParseAddr(clientAddress); err == nil {
		return addr.Unmap().String()
	}
	if host, _, err := net.SplitHostPort(clientAddress); err == nil {
		return host
	}
	return clientAddress
}

func (self *ExtenderActivator) byJwt() string {
	if self.settings.ByJwt == nil {
		return ""
	}
	return self.settings.ByJwt()
}

// Activates every family that has an api url and an address on this host.
// Reports whether the pass is a success, which is what resets the backoff: a
// family that was attempted and refused holds the whole pass, since the next
// attempt reissues every record anyway, and a pass that attempted nothing holds
// too, so a host that gains an address is retried on the backoff rather than on
// the 24 hour tick.
func (self *ExtenderActivator) activate(now time.Time) bool {
	carriers := []string{}
	if self.settings.Carriers != nil {
		carriers = self.settings.Carriers()
	}

	attempted := false
	succeeded := true
	for _, ipVersion := range self.ipVersions() {
		if !self.ipVersionSupported(ipVersion) {
			// a family with no global address on this host cannot be probed
			// back, so it is skipped rather than refused (G3)
			continue
		}
		attempted = true
		if len(carriers) == 0 {
			self.recordFailure(ipVersion, now, "no carrier is listening")
			succeeded = false
			continue
		}
		if !self.activateFamily(ipVersion, carriers, now) {
			succeeded = false
		}
	}
	return attempted && succeeded
}

func (self *ExtenderActivator) ipVersionSupported(ipVersion int) bool {
	if self.settings.IpVersionSupported != nil {
		return self.settings.IpVersionSupported(ipVersion)
	}
	return FamilySupported(ipVersion)
}

// Posts one family's activation and records what came back (C2).
func (self *ExtenderActivator) activateFamily(
	ipVersion int,
	carriers []string,
	now time.Time,
) bool {
	args := &ExtenderActivateArgs{
		PublicKeyHex: hex.EncodeToString(self.settings.PublicKey),
		TcpPort:      self.settings.TcpPort,
		UdpPort:      self.settings.UdpPort,
		DnsPort:      self.settings.DnsPort,
		DnsTld:       self.settings.DnsTld,
		Carriers:     carriers,
	}
	ctx, cancel := context.WithTimeout(self.ctx, self.settings.RequestTimeout)
	defer cancel()
	result, err := PostExtenderActivate(
		ctx,
		self.settings.ClientStrategy,
		self.apiUrl(ipVersion),
		self.byJwt(),
		args,
	)
	if err != nil {
		self.log.Infof("[extender]activate v%d err = %s\n", ipVersion, err)
		self.recordFailure(ipVersion, now, err.Error())
		return false
	}
	if result == nil {
		self.recordFailure(ipVersion, now, "the operator sent no activation result")
		return false
	}
	if !result.Activated {
		message := result.Error
		if message == "" {
			message = "the operator refused the activation"
		}
		self.log.Infof("[extender]activate v%d refused = %s\n", ipVersion, message)
		self.recordFailure(ipVersion, now, message)
		return false
	}

	self.applyRecords(result)
	self.recordSuccess(ipVersion, now, result)
	if self.settings.OnActivated != nil {
		self.settings.OnActivated(ipVersion, result)
	}
	return true
}

// Applies this extender's own record and the bootstrap sample into the
// directory (C2, D6). Both carry the bootstrap source: they came from the
// activation, not from the feed or the mesh. A record that does not verify is
// dropped with a log -- the activation itself still stands, because the
// operator proved the carriers.
func (self *ExtenderActivator) applyRecords(result *ExtenderActivateResult) {
	if self.settings.Directory == nil {
		return
	}
	apply := func(recordBase64 string) {
		recordBytes, err := base64.StdEncoding.DecodeString(recordBase64)
		if err != nil {
			self.log.Infof("[extender]activate record err = %s\n", err)
			return
		}
		record := &protocol.ExtenderRecord{}
		if err := proto.Unmarshal(recordBytes, record); err != nil {
			self.log.Infof("[extender]activate record err = %s\n", err)
			return
		}
		if _, err := self.settings.Directory.ApplyRecord(record, ExtenderSourceBootstrap); err != nil {
			self.log.Infof("[extender]activate record err = %s\n", err)
		}
	}
	if result.Record != "" {
		apply(result.Record)
	}
	for _, recordBase64 := range result.Bootstrap {
		apply(recordBase64)
	}
}

func (self *ExtenderActivator) recordSuccess(
	ipVersion int,
	now time.Time,
	result *ExtenderActivateResult,
) {
	ip, _ := netip.ParseAddr(result.Ip)

	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	family := self.families[ipVersion]
	if family == nil {
		return
	}
	family.Activated = true
	family.Ip = ip.Unmap()
	family.LastActivationTime = now
	family.LastError = ""
	family.RevokedTime = time.Time{}
	family.AllowedHosts = slices.Clone(result.AllowedHosts)
	family.ExpireTime = time.Time{}
	if result.ExpireTime != nil {
		family.ExpireTime = *result.ExpireTime
	}
	self.changedWithLock()
}

func (self *ExtenderActivator) recordFailure(ipVersion int, now time.Time, message string) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	family := self.families[ipVersion]
	if family == nil {
		return
	}
	family.Activated = false
	family.LastActivationTime = now
	family.LastError = message
	self.changedWithLock()
}
