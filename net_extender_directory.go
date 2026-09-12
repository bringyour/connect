package connect

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/protocol"
)

// The client extender directory (EXTENDER.md E1).
//
// Two kinds of entry live here. A verified identity is keyed by the extender
// public key and holds the newest signed record and the newest revocation by
// issue time (B5). An address is keyed by its ip and carries the local
// evidence -- successes, failures, hold, use -- that decides whether the
// strategy may dial it. An address learned from dns bootstrap or from manual
// configuration has no key; it upgrades to verified when a record listing that
// ip arrives, keeping the local evidence it has already collected.
//
// The policy numbers are all settings so a test can pin every transition, and
// `Now` is the only clock the policy reads, so a fake clock makes the whole
// policy deterministic.
//
// Methods are safe for concurrent use. The state lock is never held across a
// call to the store or to a monitor consumer; the save runs on the internal
// `run` goroutine started by the constructor, coalesced one save timeout after
// a change, so a burst of applied records costs one write.

// Where an address was learned (F2). The source is descriptive: it never
// changes once an address is known, so an upgrade to verified keeps the origin
// that first produced it.
const (
	ExtenderSourceDns       = "dns"
	ExtenderSourceFeed      = "feed"
	ExtenderSourceGossip    = "gossip"
	ExtenderSourceBootstrap = "bootstrap"
	ExtenderSourceManual    = "manual"
)

// The address states reported by the status (F2), in precedence order: trust
// first, because a revoked or expired record is not dialable whatever the
// local evidence says, then the local failure evidence.
const (
	ExtenderStateActive     = "active"
	ExtenderStateWarning    = "warning"
	ExtenderStateHold       = "hold"
	ExtenderStateUnverified = "unverified"
	ExtenderStateRevoked    = "revoked"
	ExtenderStateExpired    = "expired"
)

// Version of the persisted envelope. A stored document of another version is
// discarded, exactly like an unreadable one.
const ExtenderDirectoryStoreVersion = 1

// The progress of the network client's first feed sample, which is what the
// startup gate waits on (E4). `None` means no network client is running, so
// nothing will ever complete and the gate must not wait at all.
type ExtenderInitialSampleState int

const (
	ExtenderInitialSampleNone ExtenderInitialSampleState = iota
	ExtenderInitialSamplePending
	ExtenderInitialSampleDone
)

// Persistence of one directory. The bytes are the JSON envelope below. A
// storage-less host installs no store, which keeps the directory in memory.
type ExtenderDirectoryStore interface {
	Load() ([]byte, error)
	Save([]byte) error
}

type ExtenderDirectorySettings struct {
	Log Logger

	// Store, when set, loads at construction and receives a coalesced save
	// after every change. Nil keeps the directory in memory.
	Store ExtenderDirectoryStore

	// NetworkHosts are the hosts whose records this directory accepts (B2):
	// the space host and its migration host. A record for any other host is
	// rejected, which is what separates two network spaces sharing one root
	// key.
	NetworkHosts []string

	// Hold after a failure, doubling per consecutive failure up to the max.
	HoldTimeout    time.Duration
	MaxHoldTimeout time.Duration
	// Consecutive failures that make an address a warning.
	WarningConsecutiveFailureCount int
	// An address that has never succeeded is removed this long after its
	// first failure.
	NeverSucceededRemoveTimeout time.Duration
	// An address that has succeeded is removed when its last success is older
	// than this and it has reached the consecutive failure count.
	StaleSuccessRemoveTimeout     time.Duration
	RemoveConsecutiveFailureCount int
	// Clock skew allowed against a record expiry.
	RecordExpireSkew time.Duration
	// Cap on known addresses. Over the cap the eviction order is expired,
	// then never succeeded oldest first, then oldest last success. Manual
	// addresses are never evicted.
	MaxAddressCount int
	// A change is saved this long after it lands, so a burst costs one write.
	SaveTimeout time.Duration

	// The only clock the policy reads. Tests install a fake one.
	Now func() time.Time
}

func DefaultExtenderDirectorySettings() *ExtenderDirectorySettings {
	return &ExtenderDirectorySettings{
		HoldTimeout:                    10 * time.Minute,
		MaxHoldTimeout:                 6 * time.Hour,
		WarningConsecutiveFailureCount: 1,
		NeverSucceededRemoveTimeout:    24 * time.Hour,
		StaleSuccessRemoveTimeout:      7 * 24 * time.Hour,
		RemoveConsecutiveFailureCount:  3,
		RecordExpireSkew:               5 * time.Minute,
		MaxAddressCount:                512,
		SaveTimeout:                    1 * time.Second,
		Now:                            time.Now,
	}
}

// One verified identity: the newest record and the newest revocation by issue
// time (B5). The revocation is kept even with no record, so a replayed older
// record cannot reactivate a revoked key.
type extenderDirectoryRecord struct {
	publicKey []byte

	record     *protocol.ExtenderRecord
	recordBody *protocol.ExtenderRecordBody

	revocation     *protocol.ExtenderRevocation
	revocationBody *protocol.ExtenderRevocationBody
}

// The local evidence about one address. `publicKeyHex` is empty while the
// address is unverified.
type extenderDirectoryAddress struct {
	ip           netip.Addr
	source       string
	publicKeyHex string

	addTime                 time.Time
	successCount            int
	failureCount            int
	lastSuccessTime         time.Time
	lastFailureTime         time.Time
	firstFailureTime        time.Time
	consecutiveFailureCount int
	holdUntilTime           time.Time
	lastUseTime             time.Time
	inUseCount              int
}

// One dialable endpoint handed to the strategy and to the network client. The
// carriers, ports and tld come from the record; an unverified address carries
// the fixed carrier defaults, which is what a dns bootstrap address is dialed
// with before any record names it.
type ExtenderCandidate struct {
	Ip        netip.Addr
	IpVersion int
	// The identity key of a verified record, empty when unverified. The outer
	// leaf is checked against it (B3, E5).
	PublicKey   []byte
	Carriers    []string
	TcpPort     int
	UdpPort     int
	DnsPort     int
	DnsTld      string
	CountryCode string
	Source      string
	Verified    bool
}

// One address as the status reports it (F2).
type ExtenderDirectoryEntry struct {
	Ip              netip.Addr
	IpVersion       int
	PublicKey       []byte
	Carriers        []string
	CountryCode     string
	State           string
	Source          string
	LastSuccessTime time.Time
	LastFailureTime time.Time
	SuccessCount    int
	FailureCount    int
	InUse           int
	ExpireTime      time.Time
}

// The whole directory as the status reads it, with the counts the sdk exposes
// so a caller does not recount the entries.
type ExtenderDirectorySnapshot struct {
	Entries      []*ExtenderDirectoryEntry
	KnownCount   int
	ActiveCount  int
	WarningCount int
	HoldCount    int
}

type ExtenderDirectory struct {
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	done      chan struct{}
	log       Logger

	settings *ExtenderDirectorySettings

	// increments on every change; the save loop watches it, and a consumer
	// that renders the directory watches it too
	changeMonitor *MonitorValue[uint64]
	// the startup gate's rendezvous with the network client (E4)
	initialSampleMonitor *MonitorValue[ExtenderInitialSampleState]

	stateLock  sync.Mutex
	rootKeySet *ExtenderRootKeySet
	// verified identities by hex public key
	keyHexRecords map[string]*extenderDirectoryRecord
	// every known address
	ipAddresses  map[netip.Addr]*extenderDirectoryAddress
	version      uint64
	savedVersion uint64
}

func NewExtenderDirectoryWithDefaults(ctx context.Context) *ExtenderDirectory {
	return NewExtenderDirectory(ctx, DefaultExtenderDirectorySettings())
}

// The directory is running when this returns: the store has been loaded and
// the coalesced save loop is up.
func NewExtenderDirectory(
	ctx context.Context,
	settings *ExtenderDirectorySettings,
) *ExtenderDirectory {
	if settings == nil {
		settings = DefaultExtenderDirectorySettings()
	}
	if settings.Now == nil {
		copied := *settings
		copied.Now = time.Now
		settings = &copied
	}
	cancelCtx, cancel := context.WithCancel(ctx)
	self := &ExtenderDirectory{
		ctx:                  cancelCtx,
		cancel:               cancel,
		done:                 make(chan struct{}),
		log:                  loggerOrDefault(settings.Log),
		settings:             settings,
		changeMonitor:        NewMonitorValue[uint64](0),
		initialSampleMonitor: NewMonitorValue[ExtenderInitialSampleState](ExtenderInitialSampleNone),
		rootKeySet:           NewExtenderRootKeySet(),
		keyHexRecords:        map[string]*extenderDirectoryRecord{},
		ipAddresses:          map[netip.Addr]*extenderDirectoryAddress{},
	}
	self.load()
	// arm the save loop's subscription here, not inside the goroutine: a
	// change that lands between the constructor returning and the goroutine
	// being scheduled would otherwise close a channel nobody held, and the
	// first save would wait for a second change
	_, notify := self.changeMonitor.Get()
	go HandleError(func() {
		defer close(self.done)
		self.run(notify)
	}, cancel)
	return self
}

// The change counter. A consumer reads the snapshot and waits on the channel
// the same `Get` returned, which is what keeps the read and the subscribe from
// separating.
func (self *ExtenderDirectory) ChangeMonitor() *MonitorValue[uint64] {
	return self.changeMonitor
}

// The startup gate's view of the network client's first sample (E4).
func (self *ExtenderDirectory) InitialSampleMonitor() *MonitorValue[ExtenderInitialSampleState] {
	return self.initialSampleMonitor
}

// Announces that a network client is running and has not finished its first
// attempt. Only the network client calls this, from its constructor.
func (self *ExtenderDirectory) SetInitialSamplePending() {
	self.initialSampleMonitor.Update(func(state ExtenderInitialSampleState) ExtenderInitialSampleState {
		if state == ExtenderInitialSampleDone {
			return state
		}
		return ExtenderInitialSamplePending
	})
}

// Announces that the first attempt finished, whether or not it produced a
// sample. The gate never waits again after this.
func (self *ExtenderDirectory) SetInitialSampleDone() {
	self.initialSampleMonitor.Set(ExtenderInitialSampleDone)
}

// Replaces the accepted root keys (B4). Stored records are re-verified under
// the new set and dropped when they no longer verify, so a rotation that
// retires a key also retires everything it signed. An empty set is the
// unconfigured state and judges nothing: `Apply` will reject every message
// until keys arrive, and what is already stored is left alone.
func (self *ExtenderDirectory) SetRootKeys(keySet *ExtenderRootKeySet) {
	if keySet == nil {
		keySet = NewExtenderRootKeySet()
	}
	changed := false
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()

		self.rootKeySet = keySet
		if keySet.Len() == 0 {
			return
		}
		for keyHex, keyRecord := range self.keyHexRecords {
			if keyRecord.record != nil {
				if _, err := keySet.VerifyRecord(keyRecord.record); err != nil {
					keyRecord.record = nil
					keyRecord.recordBody = nil
					changed = true
				}
			}
			if keyRecord.revocation != nil {
				if _, err := keySet.VerifyRevocation(keyRecord.revocation); err != nil {
					keyRecord.revocation = nil
					keyRecord.revocationBody = nil
					changed = true
				}
			}
			if keyRecord.record == nil && keyRecord.revocation == nil {
				delete(self.keyHexRecords, keyHex)
				// the addresses that record produced become unverified rather
				// than disappearing: the local evidence about them is still
				// evidence, and a later record can claim them again
				for _, address := range self.ipAddresses {
					if address.publicKeyHex == keyHex {
						address.publicKeyHex = ""
					}
				}
			}
		}
		if changed {
			self.changedWithLock()
		}
	}()
}

// The accepted root keys.
func (self *ExtenderDirectory) RootKeys() *ExtenderRootKeySet {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.rootKeySet
}

// Applies one signed gossip message, verifying it under the current root keys
// and the allowed network hosts. Records applied in this phase arrive over the
// feed; the gossip node of phase 5a applies with its own source.
func (self *ExtenderDirectory) Apply(message *protocol.ExtenderGossipMessage) (changed bool, err error) {
	return self.ApplySource(message, ExtenderSourceFeed)
}

func (self *ExtenderDirectory) ApplySource(
	message *protocol.ExtenderGossipMessage,
	source string,
) (changed bool, err error) {
	if message == nil {
		return false, fmt.Errorf("extender gossip message is missing")
	}
	switch {
	case message.GetRecord() != nil:
		return self.ApplyRecord(message.GetRecord(), source)
	case message.GetRevocation() != nil:
		return self.ApplyRevocation(message.GetRevocation())
	default:
		return false, fmt.Errorf("extender gossip message carries neither a record nor a revocation")
	}
}

// Applies one signed record. A record older than the one already held for the
// key changes nothing, which is what makes the newest-wins rule of B5 order
// independent.
func (self *ExtenderDirectory) ApplyRecord(
	record *protocol.ExtenderRecord,
	source string,
) (changed bool, err error) {
	keySet := self.RootKeys()
	body, err := keySet.VerifyRecord(record)
	if err != nil {
		return false, err
	}
	if !ExtenderNetworkHostAllowed(body.NetworkHost, self.settings.NetworkHosts...) {
		return false, fmt.Errorf("extender record is for network host %q", body.NetworkHost)
	}
	if len(body.PublicKey) == 0 {
		return false, fmt.Errorf("extender record carries no public key")
	}
	if source == "" {
		source = ExtenderSourceFeed
	}
	keyHex := hex.EncodeToString(body.PublicKey)
	now := self.settings.Now()

	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	keyRecord := self.keyHexRecords[keyHex]
	if keyRecord == nil {
		keyRecord = &extenderDirectoryRecord{
			publicKey: slices.Clone(body.PublicKey),
		}
		self.keyHexRecords[keyHex] = keyRecord
	} else if keyRecord.recordBody != nil && body.IssueTimeMs <= keyRecord.recordBody.IssueTimeMs {
		// an older or identical record; the newest one already held wins
		return false, nil
	}
	keyRecord.record = record
	keyRecord.recordBody = body

	for _, recordAddress := range body.Addresses {
		ip, parseErr := netip.ParseAddr(recordAddress.Ip)
		if parseErr != nil || !ip.IsValid() {
			continue
		}
		ip = ip.Unmap()
		address := self.ipAddresses[ip]
		if address == nil {
			address = &extenderDirectoryAddress{
				ip:      ip,
				source:  source,
				addTime: now,
			}
			self.ipAddresses[ip] = address
		}
		// an unverified bootstrap address upgrades here, keeping the local
		// evidence it collected before any record named it
		address.publicKeyHex = keyHex
	}
	self.enforceAddressCapWithLock(now)
	self.changedWithLock()
	return true, nil
}

// Applies one signed revocation. A revocation with an issue time at or after
// the held record's issue time makes the key inactive at once (B5).
func (self *ExtenderDirectory) ApplyRevocation(
	revocation *protocol.ExtenderRevocation,
) (changed bool, err error) {
	keySet := self.RootKeys()
	body, err := keySet.VerifyRevocation(revocation)
	if err != nil {
		return false, err
	}
	if !ExtenderNetworkHostAllowed(body.NetworkHost, self.settings.NetworkHosts...) {
		return false, fmt.Errorf("extender revocation is for network host %q", body.NetworkHost)
	}
	if len(body.PublicKey) == 0 {
		return false, fmt.Errorf("extender revocation carries no public key")
	}
	keyHex := hex.EncodeToString(body.PublicKey)

	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	keyRecord := self.keyHexRecords[keyHex]
	if keyRecord == nil {
		// keep the revocation even with no record, so a replayed older record
		// cannot reactivate the key
		keyRecord = &extenderDirectoryRecord{
			publicKey: slices.Clone(body.PublicKey),
		}
		self.keyHexRecords[keyHex] = keyRecord
	} else if keyRecord.revocationBody != nil && body.IssueTimeMs <= keyRecord.revocationBody.IssueTimeMs {
		return false, nil
	}
	keyRecord.revocation = revocation
	keyRecord.revocationBody = body
	self.changedWithLock()
	return true, nil
}

// Adds an address learned outside the signed path: a dns bootstrap answer or a
// manually configured extender. It is unverified until a record lists it. An
// address that is already known keeps everything it has.
func (self *ExtenderDirectory) AddBootstrap(ip netip.Addr, source string) (changed bool) {
	if !ip.IsValid() {
		return false
	}
	ip = ip.Unmap()
	if source == "" {
		source = ExtenderSourceBootstrap
	}
	now := self.settings.Now()

	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	if _, ok := self.ipAddresses[ip]; ok {
		return false
	}
	self.ipAddresses[ip] = &extenderDirectoryAddress{
		ip:      ip,
		source:  source,
		addTime: now,
	}
	self.enforceAddressCapWithLock(now)
	self.changedWithLock()
	return true
}

// Records a completed dial over `connectMode`. A success clears the hold and
// the consecutive failure run.
func (self *ExtenderDirectory) RecordSuccess(ip netip.Addr, connectMode ExtenderConnectMode) {
	if !ip.IsValid() {
		return
	}
	ip = ip.Unmap()
	now := self.settings.Now()

	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	address := self.ipAddresses[ip]
	if address == nil {
		return
	}
	address.successCount += 1
	address.lastSuccessTime = now
	address.lastUseTime = now
	address.consecutiveFailureCount = 0
	address.firstFailureTime = time.Time{}
	address.holdUntilTime = time.Time{}
	if self.log.V(2).Enabled() {
		self.log.Infof("[extender]success %s %s\n", ip, connectMode)
	}
	self.changedWithLock()
}

// Records a failed dial over `connectMode`: the address is held for the
// doubling hold timeout and removed when the removal policy is met.
func (self *ExtenderDirectory) RecordFailure(ip netip.Addr, connectMode ExtenderConnectMode) {
	if !ip.IsValid() {
		return
	}
	ip = ip.Unmap()
	now := self.settings.Now()

	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	address := self.ipAddresses[ip]
	if address == nil {
		return
	}
	address.failureCount += 1
	address.consecutiveFailureCount += 1
	address.lastFailureTime = now
	address.lastUseTime = now
	if address.firstFailureTime.IsZero() {
		address.firstFailureTime = now
	}
	address.holdUntilTime = now.Add(self.holdTimeout(address.consecutiveFailureCount))
	if self.log.V(2).Enabled() {
		self.log.Infof("[extender]failure %s %s consecutive=%d\n", ip, connectMode, address.consecutiveFailureCount)
	}
	if self.shouldRemoveWithLock(address, now) {
		delete(self.ipAddresses, ip)
		self.pruneKeyRecordsWithLock()
	}
	self.changedWithLock()
}

// Adjusts the in-use count of one address, which the status reports. A
// positive delta also stamps the last use.
func (self *ExtenderDirectory) SetInUse(ip netip.Addr, delta int) {
	if !ip.IsValid() || delta == 0 {
		return
	}
	ip = ip.Unmap()
	now := self.settings.Now()

	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	address := self.ipAddresses[ip]
	if address == nil {
		return
	}
	address.inUseCount = max(0, address.inUseCount+delta)
	if 0 < delta {
		address.lastUseTime = now
	}
	self.changedWithLock()
}

// The hold after `consecutiveFailureCount` failures: the base doubling per
// failure, capped. The shift is bounded before it is taken, so a long run
// cannot overflow into a negative duration.
func (self *ExtenderDirectory) holdTimeout(consecutiveFailureCount int) time.Duration {
	if consecutiveFailureCount <= 1 {
		return min(self.settings.HoldTimeout, self.settings.MaxHoldTimeout)
	}
	holdTimeout := self.settings.HoldTimeout
	for i := 1; i < consecutiveFailureCount; i += 1 {
		if self.settings.MaxHoldTimeout <= holdTimeout {
			return self.settings.MaxHoldTimeout
		}
		holdTimeout *= 2
	}
	return min(holdTimeout, self.settings.MaxHoldTimeout)
}

// Applies the removal policy and the address cap. The network client calls it
// on its refresh tick, so a directory that is only read still ages.
func (self *ExtenderDirectory) Expire(now time.Time) (changed bool) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	for ip, address := range self.ipAddresses {
		if self.shouldRemoveWithLock(address, now) {
			delete(self.ipAddresses, ip)
			changed = true
		}
	}
	if self.enforceAddressCapWithLock(now) {
		changed = true
	}
	if self.pruneKeyRecordsWithLock() {
		changed = true
	}
	if changed {
		self.changedWithLock()
	}
	return changed
}

// Up to `count` dialable endpoints of `ipVersion` (0 for any family), active
// and not held, verified first. The order is deterministic -- verified, then
// fewest consecutive failures, then the most recent success, then the address
// -- because the strategy does its own weighting on top and a stable order
// makes the policy testable.
func (self *ExtenderDirectory) Candidates(
	ipVersion int,
	count int,
	exclude ...netip.Addr,
) []*ExtenderCandidate {
	if count <= 0 {
		return []*ExtenderCandidate{}
	}
	excludeIps := map[netip.Addr]bool{}
	for _, ip := range exclude {
		excludeIps[ip.Unmap()] = true
	}
	now := self.settings.Now()

	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	addresses := []*extenderDirectoryAddress{}
	for ip, address := range self.ipAddresses {
		if excludeIps[ip] {
			continue
		}
		if ipVersion != 0 && addressIpVersion(ip) != ipVersion {
			continue
		}
		if now.Before(address.holdUntilTime) {
			continue
		}
		if !self.addressActiveWithLock(address, now) {
			continue
		}
		addresses = append(addresses, address)
	}
	slices.SortFunc(addresses, func(a *extenderDirectoryAddress, b *extenderDirectoryAddress) int {
		aVerified := a.publicKeyHex != ""
		bVerified := b.publicKeyHex != ""
		if aVerified != bVerified {
			if aVerified {
				return -1
			}
			return 1
		}
		if a.consecutiveFailureCount != b.consecutiveFailureCount {
			return a.consecutiveFailureCount - b.consecutiveFailureCount
		}
		if !a.lastSuccessTime.Equal(b.lastSuccessTime) {
			// the most recent success first
			if a.lastSuccessTime.After(b.lastSuccessTime) {
				return -1
			}
			return 1
		}
		return strings.Compare(a.ip.String(), b.ip.String())
	})

	candidates := []*ExtenderCandidate{}
	for _, address := range addresses {
		if count <= len(candidates) {
			break
		}
		candidates = append(candidates, self.candidateWithLock(address))
	}
	return candidates
}

// The dialable form of one address, filled from its record when it has one.
func (self *ExtenderDirectory) candidateWithLock(address *extenderDirectoryAddress) *ExtenderCandidate {
	candidate := &ExtenderCandidate{
		Ip:        address.ip,
		IpVersion: addressIpVersion(address.ip),
		Carriers:  []string{ExtenderCarrierTcp, ExtenderCarrierQuic, ExtenderCarrierDns},
		TcpPort:   ExtenderTcpPort,
		UdpPort:   ExtenderQuicPort,
		DnsPort:   ExtenderDnsPort,
		DnsTld:    DefaultExtenderDnsTld,
		Source:    address.source,
	}
	keyRecord := self.keyHexRecords[address.publicKeyHex]
	if keyRecord == nil || keyRecord.recordBody == nil {
		return candidate
	}
	body := keyRecord.recordBody
	candidate.Verified = true
	candidate.PublicKey = slices.Clone(keyRecord.publicKey)
	candidate.CountryCode = body.CountryCode
	if 0 < body.TcpPort {
		candidate.TcpPort = int(body.TcpPort)
	}
	if 0 < body.UdpPort {
		candidate.UdpPort = int(body.UdpPort)
	}
	if 0 < body.DnsPort {
		candidate.DnsPort = int(body.DnsPort)
	}
	if body.DnsTld != "" {
		candidate.DnsTld = body.DnsTld
	}
	if carriers := recordAddressCarriers(body, address.ip); carriers != nil {
		candidate.Carriers = carriers
	}
	return candidate
}

// The carriers the record lists for one address, or nil when the record does
// not name the address, which leaves the caller on the carrier defaults.
func recordAddressCarriers(body *protocol.ExtenderRecordBody, ip netip.Addr) []string {
	for _, recordAddress := range body.Addresses {
		recordIp, err := netip.ParseAddr(recordAddress.Ip)
		if err != nil || recordIp.Unmap() != ip {
			continue
		}
		carriers := []string{}
		for _, carrier := range recordAddress.Carriers {
			switch carrier {
			case ExtenderCarrierTcp, ExtenderCarrierQuic, ExtenderCarrierDns:
				carriers = append(carriers, carrier)
			}
		}
		return carriers
	}
	return nil
}

// Reports whether the key backing an address is active (B5) -- an unverified
// address is dialable on the carrier defaults, so it counts as active here.
func (self *ExtenderDirectory) addressActiveWithLock(
	address *extenderDirectoryAddress,
	now time.Time,
) bool {
	if address.publicKeyHex == "" {
		return true
	}
	keyRecord := self.keyHexRecords[address.publicKeyHex]
	if keyRecord == nil || keyRecord.recordBody == nil {
		return false
	}
	if self.keyRecordRevokedWithLock(keyRecord) {
		return false
	}
	return !self.keyRecordExpiredWithLock(keyRecord, now)
}

// B5: a revocation at or after the record's issue time.
func (self *ExtenderDirectory) keyRecordRevokedWithLock(keyRecord *extenderDirectoryRecord) bool {
	if keyRecord.revocationBody == nil {
		return false
	}
	if keyRecord.recordBody == nil {
		return true
	}
	return keyRecord.recordBody.IssueTimeMs <= keyRecord.revocationBody.IssueTimeMs
}

// B5: the record expiry with the configured skew.
func (self *ExtenderDirectory) keyRecordExpiredWithLock(
	keyRecord *extenderDirectoryRecord,
	now time.Time,
) bool {
	if keyRecord.recordBody == nil {
		return true
	}
	if keyRecord.recordBody.ExpireTimeMs == 0 {
		return false
	}
	expireTime := time.UnixMilli(int64(keyRecord.recordBody.ExpireTimeMs)).Add(self.settings.RecordExpireSkew)
	return expireTime.Before(now)
}

// The removal policy of E1. A manual address is never removed by policy: it
// was configured by hand and only a reconfiguration takes it away.
func (self *ExtenderDirectory) shouldRemoveWithLock(
	address *extenderDirectoryAddress,
	now time.Time,
) bool {
	if address.source == ExtenderSourceManual {
		return false
	}
	if address.successCount == 0 {
		if address.firstFailureTime.IsZero() {
			return false
		}
		return self.settings.NeverSucceededRemoveTimeout <= now.Sub(address.firstFailureTime)
	}
	return self.settings.StaleSuccessRemoveTimeout <= now.Sub(address.lastSuccessTime) &&
		self.settings.RemoveConsecutiveFailureCount <= address.consecutiveFailureCount
}

// Evicts down to the address cap: expired first, then never succeeded oldest
// first, then oldest last success. Manual addresses are never evicted, so a
// cap smaller than the manual set simply holds more than the cap.
func (self *ExtenderDirectory) enforceAddressCapWithLock(now time.Time) (changed bool) {
	if self.settings.MaxAddressCount <= 0 || len(self.ipAddresses) <= self.settings.MaxAddressCount {
		return false
	}
	evictable := []*extenderDirectoryAddress{}
	for _, address := range self.ipAddresses {
		if address.source == ExtenderSourceManual {
			continue
		}
		evictable = append(evictable, address)
	}
	tier := func(address *extenderDirectoryAddress) int {
		if address.publicKeyHex != "" && !self.addressActiveWithLock(address, now) {
			return 0
		}
		if address.successCount == 0 {
			return 1
		}
		return 2
	}
	slices.SortFunc(evictable, func(a *extenderDirectoryAddress, b *extenderDirectoryAddress) int {
		aTier, bTier := tier(a), tier(b)
		if aTier != bTier {
			return aTier - bTier
		}
		switch aTier {
		case 1:
			// the oldest known first
			if !a.addTime.Equal(b.addTime) {
				if a.addTime.Before(b.addTime) {
					return -1
				}
				return 1
			}
		case 2:
			// the oldest last success first
			if !a.lastSuccessTime.Equal(b.lastSuccessTime) {
				if a.lastSuccessTime.Before(b.lastSuccessTime) {
					return -1
				}
				return 1
			}
		}
		return strings.Compare(a.ip.String(), b.ip.String())
	})
	for _, address := range evictable {
		if len(self.ipAddresses) <= self.settings.MaxAddressCount {
			break
		}
		delete(self.ipAddresses, address.ip)
		changed = true
	}
	if changed {
		self.pruneKeyRecordsWithLock()
	}
	return changed
}

// Drops identities that no longer describe anything: no address and no
// revocation to enforce.
func (self *ExtenderDirectory) pruneKeyRecordsWithLock() (changed bool) {
	referencedKeyHexes := map[string]bool{}
	for _, address := range self.ipAddresses {
		if address.publicKeyHex != "" {
			referencedKeyHexes[address.publicKeyHex] = true
		}
	}
	for keyHex, keyRecord := range self.keyHexRecords {
		if referencedKeyHexes[keyHex] || keyRecord.revocation != nil {
			continue
		}
		delete(self.keyHexRecords, keyHex)
		changed = true
	}
	return changed
}

// The status view (F2).
func (self *ExtenderDirectory) Snapshot() *ExtenderDirectorySnapshot {
	now := self.settings.Now()

	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	snapshot := &ExtenderDirectorySnapshot{
		Entries: []*ExtenderDirectoryEntry{},
	}
	for _, address := range self.ipAddresses {
		candidate := self.candidateWithLock(address)
		state := self.addressStateWithLock(address, now)
		entry := &ExtenderDirectoryEntry{
			Ip:              address.ip,
			IpVersion:       addressIpVersion(address.ip),
			PublicKey:       candidate.PublicKey,
			Carriers:        candidate.Carriers,
			CountryCode:     candidate.CountryCode,
			State:           state,
			Source:          address.source,
			LastSuccessTime: address.lastSuccessTime,
			LastFailureTime: address.lastFailureTime,
			SuccessCount:    address.successCount,
			FailureCount:    address.failureCount,
			InUse:           address.inUseCount,
		}
		if keyRecord := self.keyHexRecords[address.publicKeyHex]; keyRecord != nil && keyRecord.recordBody != nil {
			if 0 < keyRecord.recordBody.ExpireTimeMs {
				entry.ExpireTime = time.UnixMilli(int64(keyRecord.recordBody.ExpireTimeMs))
			}
		}
		snapshot.Entries = append(snapshot.Entries, entry)
		snapshot.KnownCount += 1
		switch state {
		case ExtenderStateActive, ExtenderStateUnverified:
			snapshot.ActiveCount += 1
		case ExtenderStateWarning:
			snapshot.WarningCount += 1
		case ExtenderStateHold:
			snapshot.HoldCount += 1
		}
	}
	slices.SortFunc(snapshot.Entries, func(a *ExtenderDirectoryEntry, b *ExtenderDirectoryEntry) int {
		return strings.Compare(a.Ip.String(), b.Ip.String())
	})
	return snapshot
}

// The count of addresses whose key is active (B5), hold included. This is what
// the low-water re-bootstrap reads: a held address is still a known extender,
// and re-resolving dns would not produce a better one.
func (self *ExtenderDirectory) ActiveCount(ipVersion int) int {
	now := self.settings.Now()

	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	count := 0
	for ip, address := range self.ipAddresses {
		if ipVersion != 0 && addressIpVersion(ip) != ipVersion {
			continue
		}
		if self.addressActiveWithLock(address, now) {
			count += 1
		}
	}
	return count
}

// The count of addresses that are dialable right now, hold excluded, which is
// what the startup gate reads.
func (self *ExtenderDirectory) UsableCount(ipVersion int) int {
	now := self.settings.Now()

	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	count := 0
	for ip, address := range self.ipAddresses {
		if ipVersion != 0 && addressIpVersion(ip) != ipVersion {
			continue
		}
		if now.Before(address.holdUntilTime) {
			continue
		}
		if self.addressActiveWithLock(address, now) {
			count += 1
		}
	}
	return count
}

// Reports whether this address may still be dialed: it is known, not held and
// its key is active. The strategy drops the dialers of everything else.
func (self *ExtenderDirectory) AddressUsable(ip netip.Addr) bool {
	if !ip.IsValid() {
		return false
	}
	ip = ip.Unmap()
	now := self.settings.Now()

	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	address := self.ipAddresses[ip]
	if address == nil {
		return false
	}
	if now.Before(address.holdUntilTime) {
		return false
	}
	return self.addressActiveWithLock(address, now)
}

func (self *ExtenderDirectory) addressStateWithLock(
	address *extenderDirectoryAddress,
	now time.Time,
) string {
	if address.publicKeyHex != "" {
		keyRecord := self.keyHexRecords[address.publicKeyHex]
		if keyRecord == nil {
			return ExtenderStateUnverified
		}
		if self.keyRecordRevokedWithLock(keyRecord) {
			return ExtenderStateRevoked
		}
		if self.keyRecordExpiredWithLock(keyRecord, now) {
			return ExtenderStateExpired
		}
	}
	if now.Before(address.holdUntilTime) {
		return ExtenderStateHold
	}
	if 0 < self.settings.WarningConsecutiveFailureCount &&
		self.settings.WarningConsecutiveFailureCount <= address.consecutiveFailureCount {
		return ExtenderStateWarning
	}
	if address.publicKeyHex == "" {
		return ExtenderStateUnverified
	}
	return ExtenderStateActive
}

func (self *ExtenderDirectory) changedWithLock() {
	self.version += 1
	self.changeMonitor.Set(self.version)
}

// Ends the save loop after one last save of anything the loop has not written
// yet, so a directory closed inside the coalescing window is still durable.
func (self *ExtenderDirectory) Close() {
	self.closeOnce.Do(func() {
		self.cancel()
		<-self.done
		self.save()
	})
}

// The coalescing save loop (E1). It waits for a change, sleeps the save
// timeout so a burst collapses, and only then re-subscribes and writes the
// state it reads -- subscribing before the sleep would leave the next round
// armed by changes the write already carried.
func (self *ExtenderDirectory) run(notify chan struct{}) {
	if self.settings.Store == nil {
		<-self.ctx.Done()
		return
	}
	for {
		select {
		case <-self.ctx.Done():
			return
		case <-notify:
		}
		select {
		case <-self.ctx.Done():
			return
		case <-time.After(self.settings.SaveTimeout):
		}
		_, notify = self.changeMonitor.Get()
		self.save()
	}
}

// The persisted envelope (E1).
type extenderDirectoryStoreState struct {
	Version   int                              `json:"version"`
	Records   []*extenderDirectoryStoreRecord  `json:"records"`
	Addresses []*extenderDirectoryStoreAddress `json:"addresses"`
}

// A stored identity carries the signed messages verbatim, so a later root key
// rotation can re-judge exactly what was received.
type extenderDirectoryStoreRecord struct {
	PublicKey  string `json:"public_key"`
	Record     []byte `json:"record,omitempty"`
	Revocation []byte `json:"revocation,omitempty"`
}

type extenderDirectoryStoreAddress struct {
	Ip                      string `json:"ip"`
	Source                  string `json:"source,omitempty"`
	PublicKey               string `json:"public_key,omitempty"`
	AddTimeMs               int64  `json:"add_time_ms,omitempty"`
	SuccessCount            int    `json:"success_count,omitempty"`
	FailureCount            int    `json:"failure_count,omitempty"`
	LastSuccessTimeMs       int64  `json:"last_success_time_ms,omitempty"`
	LastFailureTimeMs       int64  `json:"last_failure_time_ms,omitempty"`
	FirstFailureTimeMs      int64  `json:"first_failure_time_ms,omitempty"`
	ConsecutiveFailureCount int    `json:"consecutive_failure_count,omitempty"`
	HoldUntilTimeMs         int64  `json:"hold_until_time_ms,omitempty"`
	LastUseTimeMs           int64  `json:"last_use_time_ms,omitempty"`
}

// Writes the current state through the store. A store failure is logged and
// dropped: the directory is a cache, and losing a write costs a rediscovery.
func (self *ExtenderDirectory) save() {
	if self.settings.Store == nil {
		return
	}
	var stateBytes []byte
	var version uint64
	err := func() error {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()

		if self.version == self.savedVersion {
			return nil
		}
		version = self.version
		state := &extenderDirectoryStoreState{
			Version:   ExtenderDirectoryStoreVersion,
			Records:   []*extenderDirectoryStoreRecord{},
			Addresses: []*extenderDirectoryStoreAddress{},
		}
		for keyHex, keyRecord := range self.keyHexRecords {
			storeRecord := &extenderDirectoryStoreRecord{
				PublicKey: keyHex,
			}
			if keyRecord.record != nil {
				recordBytes, err := proto.Marshal(keyRecord.record)
				if err != nil {
					return err
				}
				storeRecord.Record = recordBytes
			}
			if keyRecord.revocation != nil {
				revocationBytes, err := proto.Marshal(keyRecord.revocation)
				if err != nil {
					return err
				}
				storeRecord.Revocation = revocationBytes
			}
			state.Records = append(state.Records, storeRecord)
		}
		slices.SortFunc(state.Records, func(a *extenderDirectoryStoreRecord, b *extenderDirectoryStoreRecord) int {
			return strings.Compare(a.PublicKey, b.PublicKey)
		})
		for _, address := range self.ipAddresses {
			state.Addresses = append(state.Addresses, &extenderDirectoryStoreAddress{
				Ip:                      address.ip.String(),
				Source:                  address.source,
				PublicKey:               address.publicKeyHex,
				AddTimeMs:               extenderTimeMs(address.addTime),
				SuccessCount:            address.successCount,
				FailureCount:            address.failureCount,
				LastSuccessTimeMs:       extenderTimeMs(address.lastSuccessTime),
				LastFailureTimeMs:       extenderTimeMs(address.lastFailureTime),
				FirstFailureTimeMs:      extenderTimeMs(address.firstFailureTime),
				ConsecutiveFailureCount: address.consecutiveFailureCount,
				HoldUntilTimeMs:         extenderTimeMs(address.holdUntilTime),
				LastUseTimeMs:           extenderTimeMs(address.lastUseTime),
			})
		}
		slices.SortFunc(state.Addresses, func(a *extenderDirectoryStoreAddress, b *extenderDirectoryStoreAddress) int {
			return strings.Compare(a.Ip, b.Ip)
		})
		var err error
		stateBytes, err = json.Marshal(state)
		return err
	}()
	if err != nil {
		self.log.Infof("[extender]directory save err = %s\n", err)
		return
	}
	if stateBytes == nil {
		return
	}
	// the store is an external object, so it is called with no state lock
	if err := self.settings.Store.Save(stateBytes); err != nil {
		self.log.Infof("[extender]directory save err = %s\n", err)
		return
	}
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.savedVersion = version
}

// Reads the store at construction. An unreadable, truncated or foreign-version
// document is treated as an empty directory: the cost is a rediscovery, and a
// corrupt file must never keep a client from starting. The stored records are
// decoded without re-verification -- the local store is as trusted as the
// process -- and re-judged as soon as `SetRootKeys` installs an anchor.
func (self *ExtenderDirectory) load() {
	if self.settings.Store == nil {
		return
	}
	stateBytes, err := self.settings.Store.Load()
	if err != nil || len(stateBytes) == 0 {
		return
	}
	state := &extenderDirectoryStoreState{}
	if err := json.Unmarshal(stateBytes, state); err != nil {
		self.log.Infof("[extender]directory load err = %s\n", err)
		return
	}
	if state.Version != ExtenderDirectoryStoreVersion {
		return
	}

	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	for _, storeRecord := range state.Records {
		publicKey, err := hex.DecodeString(storeRecord.PublicKey)
		if err != nil || len(publicKey) == 0 {
			continue
		}
		keyRecord := &extenderDirectoryRecord{
			publicKey: publicKey,
		}
		if 0 < len(storeRecord.Record) {
			record := &protocol.ExtenderRecord{}
			body := &protocol.ExtenderRecordBody{}
			if proto.Unmarshal(storeRecord.Record, record) == nil &&
				proto.Unmarshal(record.Body, body) == nil {
				keyRecord.record = record
				keyRecord.recordBody = body
			}
		}
		if 0 < len(storeRecord.Revocation) {
			revocation := &protocol.ExtenderRevocation{}
			body := &protocol.ExtenderRevocationBody{}
			if proto.Unmarshal(storeRecord.Revocation, revocation) == nil &&
				proto.Unmarshal(revocation.Body, body) == nil {
				keyRecord.revocation = revocation
				keyRecord.revocationBody = body
			}
		}
		if keyRecord.record == nil && keyRecord.revocation == nil {
			continue
		}
		self.keyHexRecords[strings.ToLower(storeRecord.PublicKey)] = keyRecord
	}
	for _, storeAddress := range state.Addresses {
		ip, err := netip.ParseAddr(storeAddress.Ip)
		if err != nil || !ip.IsValid() {
			continue
		}
		ip = ip.Unmap()
		source := storeAddress.Source
		if source == "" {
			source = ExtenderSourceBootstrap
		}
		publicKeyHex := strings.ToLower(storeAddress.PublicKey)
		if publicKeyHex != "" && self.keyHexRecords[publicKeyHex] == nil {
			// the identity did not survive the load; keep the local evidence
			// and let a later record claim the address again
			publicKeyHex = ""
		}
		self.ipAddresses[ip] = &extenderDirectoryAddress{
			ip:                      ip,
			source:                  source,
			publicKeyHex:            publicKeyHex,
			addTime:                 extenderTimeFromMs(storeAddress.AddTimeMs),
			successCount:            storeAddress.SuccessCount,
			failureCount:            storeAddress.FailureCount,
			lastSuccessTime:         extenderTimeFromMs(storeAddress.LastSuccessTimeMs),
			lastFailureTime:         extenderTimeFromMs(storeAddress.LastFailureTimeMs),
			firstFailureTime:        extenderTimeFromMs(storeAddress.FirstFailureTimeMs),
			consecutiveFailureCount: storeAddress.ConsecutiveFailureCount,
			holdUntilTime:           extenderTimeFromMs(storeAddress.HoldUntilTimeMs),
			lastUseTime:             extenderTimeFromMs(storeAddress.LastUseTimeMs),
		}
	}
	self.pruneKeyRecordsWithLock()
	// a load is the state the store already holds, so it is not a change to
	// save back
	self.savedVersion = self.version
}

// 4 or 6 for an address, 0 for an invalid one.
func addressIpVersion(ip netip.Addr) int {
	switch {
	case ip.Is4() || ip.Is4In6():
		return 4
	case ip.Is6():
		return 6
	default:
		return 0
	}
}

func extenderTimeMs(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func extenderTimeFromMs(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}
