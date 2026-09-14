package connect

import (
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	// "errors"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"fmt"

	// "slices"
	// "runtime/debug"
	mathrand "math/rand"

	"maps"

	// "google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/v2026/protocol"
)

// manage contracts which are embedded into each transfer sequence

// oobErrThrottle rate-limits `[contract]oob err`. During a control-API outage
// every sequence's contract request fails, so this line is emitted once per
// sequence per retry across the whole client. See logThrottle in
// log_throttle.go.
var oobErrThrottle = newLogThrottle(time.Minute)

// shouldLogOobErr reports whether an out-of-band error should be logged and the number of errors suppressed since the previous allowed log.
func shouldLogOobErr() (bool, int64) { return oobErrThrottle.Allow(time.Now()) }

type ContractKey struct {
	Destination       TransferPath
	IntermediaryIds   MultiHopId
	CompanionContract bool
	ForceStream       bool
	// LogicalLane is local queue-generation identity only. It is intentionally
	// absent from CreateContract: every lane requests the same backend contract
	// class, but must not flush a sibling lane's pending queue on idle exit.
	LogicalLane uint32
	// NetworkPeer is contract sizing/retention policy, not routing identity:
	// it is true only when the sender has an authenticated same-network
	// relationship with Destination. Unlike ForceStream, it never classifies a
	// public direct stream as trusted/no-escrow.
	NetworkPeer bool
	// EncryptionRole separates the contract queues of the two per-peer
	// encryption send sequences to the same destination: the client-role
	// sequence (normal application data) and the server-role sequence
	// (EncryptedControl carrier + server replies). Without this, both would
	// share one queue, and one sequence's exit-flush (`FlushContractQueue`
	// on idle) would discard the other's pending contracts — starving the
	// handshake carrier. Zero value is client, so non-encrypted traffic and
	// legacy/pushed contracts key the same as before.
	EncryptionRole sequenceTlsRole
	// EncryptionCompanion separates the contract queues of two same-role send
	// sequences differing only by session identity companion — the two
	// server-role reply carriers that echo a companion vs non-companion
	// initiator both ride the same EncryptionControlUseCompanion contract, so
	// `CompanionContract` alone doesn't separate them. Without this they share a
	// queue and starve each other on exit-flush, as `EncryptionRole` guards for
	// the client/server split. Zero value false, so non-encrypted and
	// legacy/pushed contracts key as before.
	EncryptionCompanion bool
}

func (self ContractKey) Legacy() ContractKey {
	return ContractKey{
		Destination: self.Destination,
	}
}

type ContractStatus struct {
	Key     ContractKey
	Error   *protocol.ContractError
	Premium bool
}

type ContractStatusFunction = func(ContractStatus *ContractStatus)

type contractStatusCallbackWorker struct {
	ctx      context.Context
	cancel   context.CancelFunc
	callback ContractStatusFunction

	// Status delivery is observability/control feedback, not the Client
	// send/receive/forward backpressure contract. A suspended observer must not
	// eventually fill a channel and park HandleControlFrame. Keep the latest
	// status per contract key in a bounded ordered set instead.
	stateLock  sync.Mutex
	pending    map[ContractKey]*ContractStatus
	order      []ContractKey
	orderHead  int
	orderCount int
	maxCount   int
	closed     bool
	notify     chan struct{}
	done       chan struct{}
}

func newContractStatusCallbackWorker(
	ctx context.Context,
	callback ContractStatusFunction,
	bufferSize int,
	finished ...func(),
) *contractStatusCallbackWorker {
	callbackCtx, cancel := context.WithCancel(ctx)
	worker := &contractStatusCallbackWorker{
		ctx:      callbackCtx,
		cancel:   cancel,
		callback: callback,
		pending:  map[ContractKey]*ContractStatus{},
		order:    make([]ContractKey, max(1, bufferSize)),
		maxCount: max(1, bufferSize),
		notify:   make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	go func() {
		defer func() {
			close(worker.done)
			if 0 < len(finished) && finished[0] != nil {
				finished[0]()
			}
		}()
		HandleError(worker.run, cancel)
	}()
	return worker
}

func (self *contractStatusCallbackWorker) run() {
	for {
		select {
		case <-self.ctx.Done():
			return
		case <-self.notify:
		}
		for {
			self.stateLock.Lock()
			if self.closed || self.orderCount == 0 {
				self.stateLock.Unlock()
				break
			}
			key := self.order[self.orderHead]
			self.order[self.orderHead] = ContractKey{}
			self.orderHead = (self.orderHead + 1) % self.maxCount
			self.orderCount -= 1
			contractStatus := self.pending[key]
			delete(self.pending, key)
			self.stateLock.Unlock()

			HandleError(func() {
				self.callback(contractStatus)
			})
		}
	}
}

func (self *contractStatusCallbackWorker) Dispatch(contractStatus *ContractStatus) {
	if contractStatus == nil {
		return
	}
	cloned := *contractStatus

	self.stateLock.Lock()
	if self.closed {
		self.stateLock.Unlock()
		return
	}
	key := cloned.Key
	if _, ok := self.pending[key]; !ok {
		if self.maxCount <= self.orderCount {
			evictedKey := self.order[self.orderHead]
			delete(self.pending, evictedKey)
			// Full ring: overwrite the oldest slot with the new tail, then
			// advance head so the following oldest stays at the front.
			self.order[self.orderHead] = key
			self.orderHead = (self.orderHead + 1) % self.maxCount
		} else {
			tail := (self.orderHead + self.orderCount) % self.maxCount
			self.order[tail] = key
			self.orderCount += 1
		}
	}
	self.pending[key] = &cloned
	self.stateLock.Unlock()

	select {
	case <-self.ctx.Done():
	case self.notify <- struct{}{}:
	default:
	}
}

func (self *contractStatusCallbackWorker) Close() {
	self.stateLock.Lock()
	if self.closed {
		self.stateLock.Unlock()
		return
	}
	self.closed = true
	clear(self.pending)
	self.order = nil
	self.stateLock.Unlock()
	self.cancel()
}

// closeAndWait cancels delivery and joins the callback invocation worker.
func (self *contractStatusCallbackWorker) closeAndWait(ctx context.Context) error {
	self.Close()
	return waitForLifecycleDone(ctx, self.done, "contract status callback")
}

type ContractManagerStats struct {
	ContractOpenCount  int64
	ContractCloseCount int64
	// contract id -> byte count
	ContractOpenByteCounts map[Id]ByteCount
	// contract id -> contract key
	ContractOpenKeys              map[Id]ContractKey
	ContractCloseByteCount        ByteCount
	ReceiveContractCloseByteCount ByteCount
}

func NewContractManagerStats() *ContractManagerStats {
	return &ContractManagerStats{
		ContractOpenCount:             0,
		ContractCloseCount:            0,
		ContractOpenByteCounts:        map[Id]ByteCount{},
		ContractOpenKeys:              map[Id]ContractKey{},
		ContractCloseByteCount:        0,
		ReceiveContractCloseByteCount: 0,
	}
}

func (self *ContractManagerStats) ContractOpenByteCount() ByteCount {
	netContractOpenByteCount := ByteCount(0)
	for _, contractOpenByteCount := range self.ContractOpenByteCounts {
		netContractOpenByteCount += contractOpenByteCount
	}
	return netContractOpenByteCount
}

// SignStoredContract returns the HMAC signature for a stored contract using the
// format appropriate for the current time relative to
// settings.NetworkEventTimeChangeHmac. Before that time, signers emit the
// legacy form (`mac.Sum(storedContractBytes)`, which appends a key-only HMAC
// to the contract bytes). At or after that time, signers emit the standard
// form (`mac.Write(storedContractBytes); mac.Sum(nil)`).
//
// Both connect and server/connect must use this helper so the cutover is
// consistent across client and server.
func SignStoredContract(settings *ContractManagerSettings, provideSecretKey []byte, storedContractBytes []byte) []byte {
	mac := hmac.New(sha256.New, provideSecretKey)
	if time.Now().Before(settings.NetworkEventTimeChangeHmac) {
		// legacy: this leaves HMAC(key, "") in the trailing 32 bytes of the
		// returned slice. preserved for backward compatibility.
		return mac.Sum(storedContractBytes)
	}
	mac.Write(storedContractBytes)
	return mac.Sum(nil)
}

// VerifyStoredContract validates a stored-contract HMAC against the provide
// secret key, accepting both the legacy and standard HMAC formats so that
// signers may cross over at settings.NetworkEventTimeChangeHmac without
// breaking compatibility with peers that have not yet cut over.
func VerifyStoredContract(settings *ContractManagerSettings, provideSecretKey []byte, storedContractBytes []byte, storedContractHmac []byte) bool {
	legacyMac := hmac.New(sha256.New, provideSecretKey)
	if hmac.Equal(storedContractHmac, legacyMac.Sum(storedContractBytes)) {
		return true
	}
	standardMac := hmac.New(sha256.New, provideSecretKey)
	standardMac.Write(storedContractBytes)
	return hmac.Equal(storedContractHmac, standardMac.Sum(nil))
}

func DefaultContractManagerSettings() *ContractManagerSettings {
	return DefaultContractManagerSettingsWithBufferSize(defaultTransferBufferSize)
}

func DefaultContractManagerSettingsWithBufferSize(bufferSize int) *ContractManagerSettings {
	// NETWORK EVENT: at the enable contracts date, all clients will require contracts
	// up to that time, contracts are optional for the sender and match for the receiver
	networkEventTimeEnableContracts, err := time.Parse(time.RFC3339, "2024-05-01T00:00:00Z")
	if err != nil {
		panic(err)
	}
	// NETWORK EVENT: at the change-hmac date, signers cut over from the legacy
	// HMAC format to the standard form. verifiers accept both forms at all
	// times so the cutover can be deployed asymmetrically.
	// Pushed out from the original 2026-07-01: clients on connect < v2026.5.14
	// (2026-05-13) have legacy-only verifiers and cannot verify the standard
	// form, so the original date broke them the moment it passed. Hold the
	// cutover until that older fleet has drained.
	networkEventTimeChangeHmac, err := time.Parse(time.RFC3339, "2026-09-01T00:00:00Z")
	if err != nil {
		panic(err)
	}
	return &ContractManagerSettings{
		SequenceBufferSize: bufferSize,
		// The first contract of every sequence used to be kib(16), of which
		// ~80% is usable -- about nine packets. Acquiring a contract blocks
		// the send sequence, so a new destination paid two blocking
		// negotiations before it reached the second contract: one to open,
		// one nine packets later. Web traffic is many short flows to many
		// destinations, and every new destination restarts at sequence 0, so
		// it never outgrows that; a provider log showed ~40 opening
		// acquisitions in 13 minutes across ten destinations at 80ms-2.4s
		// each, which is what a several-second stall on a new domain looks
		// like. mib(1) covers essentially any single web response in the
		// opening contract. It is a larger pre-settlement exposure to an
		// unproven peer -- a deliberate tradeoff of escrow risk for the
		// first-connection stall, bounded by the unchanged mib(128) ceiling.
		InitialContractTransferByteCount:            mib(1),
		InitialNetworkPeerContractTransferByteCount: mib(1),
		StandardContractTransferByteCount:           mib(128),
		ContractTransferByteSeqScale:                4,

		NetworkEventTimeEnableContracts: networkEventTimeEnableContracts,
		NetworkEventTimeChangeHmac:      networkEventTimeChangeHmac,

		ProvidePingTimeout: 0,

		OriginContractLinger: 300 * time.Second,

		ContractQueueExpireTimeout: 120 * time.Second,

		ContractStatsEpoch: 1 * time.Second,

		ProtocolVersion: DefaultProtocolVersion,

		// TODO remove
		LegacyCreateContract: false,
		// TODO remove
		TrackUsedContracts: false,
	}
}

func DefaultContractManagerSettingsNoNetworkEvents() *ContractManagerSettings {
	settings := DefaultContractManagerSettings()
	settings.NetworkEventTimeEnableContracts = time.Time{}
	settings.NetworkEventTimeChangeHmac = time.Time{}
	return settings
}

type ContractManagerSettings struct {
	SequenceBufferSize int

	// this should be enough to do a single ping
	InitialContractTransferByteCount ByteCount
	// InitialNetworkPeerContractTransferByteCount covers a bounded interactive
	// burst to a stable same-network peer. Network contracts are no-escrow and
	// otherwise exhaust 16 KiB during the first page, blocking the ordered
	// sequence on a control-plane round trip. Public/friends streams retain the
	// small initial contract because ForceStream is a routing choice, not a
	// trusted relationship, and their contracts may reserve escrow.
	InitialNetworkPeerContractTransferByteCount ByteCount
	StandardContractTransferByteCount           ByteCount
	// scale up the contract size over this many contracts
	ContractTransferByteSeqScale uint64

	// enable contracts on the network
	// this can be removed after wide adoption
	NetworkEventTimeEnableContracts time.Time

	// cut over the stored-contract HMAC signing format. before this time,
	// SignStoredContract emits the legacy form (mac.Sum(bytes)); at or after,
	// it emits the standard form (mac.Write(bytes); mac.Sum(nil)). verifiers
	// accept both forms at all times.
	NetworkEventTimeChangeHmac time.Time

	// an active ping to the control fast-tracks any timeouts
	ProvidePingTimeout time.Duration

	// server-side companion policy: allow a return (companion) contract to be
	// created for up to this long after the origin contract in the opposite
	// direction was closed, so reply traffic can resume after the request side
	// goes idle.
	OriginContractLinger time.Duration

	// expire queued contracts that no sequence has taken within this window.
	// Bounds `destinationContracts` growth from orphans (e.g. a
	// `CreateContractResult` that lands after the owning sequence exit-flushed
	// its queue, for a destination that is never used again), and prevents
	// handing out a stale contract the platform may have already force-closed
	// server-side — keep this below the platform's unused-contract force-close
	// window (5 minutes). <= 0 disables expiry.
	ContractQueueExpireTimeout time.Duration

	// the epoch for emitting open contract usage events to
	// `AddContractStatsCallback` listeners
	ContractStatsEpoch time.Duration

	ProtocolVersion int

	// TODO remove
	LegacyCreateContract bool
	// TODO remove
	TrackUsedContracts bool
}

func (self *ContractManagerSettings) ContractsEnabled() bool {
	return self.NetworkEventTimeEnableContracts.Before(time.Now())
}

type ContractManager struct {
	ctx    context.Context
	cancel context.CancelFunc
	client *Client

	settings *ContractManagerSettings

	mutex             sync.Mutex
	closed            bool
	workers           *lifecycleAdmission
	closeControlSyncs map[*ControlSync]bool

	// `provideSecretKeys` retains all keys until app restart (typically system restart)
	// this makes it faster for clients to reconnect with existing contracts
	// otherwise the client will have to time out the send sequence and flush its pending contracts
	provideSecretKeys map[protocol.ProvideMode][]byte
	provideModes      map[protocol.ProvideMode]bool
	// provide paused overrides the set provide modes
	providePaused  bool
	provideMonitor *Monitor

	destinationContracts map[ContractKey]*contractQueue

	receiveNoContractClientIds map[Id]bool
	sendNoContractClientIds    map[Id]bool

	contractStatusCallbacks *CallbackList[*contractStatusCallbackWorker]
	// Multi-client windows install one nonblocking dispatcher per client and
	// coalesce all of those clients into the window's single callback worker.
	// Keeping this internal prevents a general caller from putting blocking work
	// back on HandleControlFrame.
	contractStatusDispatchCallbacks *CallbackList[ContractStatusFunction]

	localStats *ContractManagerStats

	// open contract usage, updated by the owning sequences and
	// emitted per epoch (see transfer_contract_stats.go)
	contractStatsLock      sync.Mutex
	contractStatsEntries   map[contractStatsKey]*contractStatsEntry
	contractStatsCallbacks *CallbackList[ContractStatsFunction]
	contractStatsStarted   bool
	// per-contract event sequence numbers, assigned under `contractStatsLock`
	// at snapshot time (see `emitContractStats`)
	contractStatsSequences map[Id]uint64

	controlSyncProvide    *ControlSync
	controlSyncProvideOob *ControlSyncOob

	// Tests pause the drain/detach boundary and observe a competing opener.
	// Nil keeps production queue lifecycle unchanged.
	testingBeforeOpenContractQueueLock func(ContractKey)
	// Nil test barrier pauses an owned background worker after its cleanup and
	// before joined completion.
	beforeWorkerDoneForTest func(string)
	// Nil test barrier pauses a provide ping after frame ownership is created.
	beforeProvidePingSendForTest func(*protocol.Frame)
	// Nil test barriers expose callback admission and manager join entry.
	beforeCallbackAdmissionLockForTest func()
	beforeCloseWaitForTest             func()
}

func NewContractManagerWithDefaults(ctx context.Context, client *Client) *ContractManager {
	return NewContractManager(ctx, client, DefaultContractManagerSettings())
}

func NewContractManager(
	ctx context.Context,
	client *Client,
	settings *ContractManagerSettings,
) *ContractManager {
	managerCtx, cancel := context.WithCancel(ctx)
	// at a minimum
	// - messages to/from the platform (ControlId) do not need a contract
	//   this is because the platform is needed to create contracts
	// - messages to self do not need a contract
	receiveNoContractClientIds := map[Id]bool{
		ControlId:         true,
		client.ClientId(): true,
	}
	sendNoContractClientIds := map[Id]bool{
		ControlId:         true,
		client.ClientId(): true,
	}

	contractManager := &ContractManager{
		ctx:                             managerCtx,
		cancel:                          cancel,
		client:                          client,
		settings:                        settings,
		provideSecretKeys:               map[protocol.ProvideMode][]byte{},
		provideModes:                    map[protocol.ProvideMode]bool{},
		providePaused:                   false,
		provideMonitor:                  NewMonitor(),
		destinationContracts:            map[ContractKey]*contractQueue{},
		receiveNoContractClientIds:      receiveNoContractClientIds,
		sendNoContractClientIds:         sendNoContractClientIds,
		contractStatusCallbacks:         NewCallbackList[*contractStatusCallbackWorker](),
		contractStatusDispatchCallbacks: NewCallbackList[ContractStatusFunction](),
		localStats:                      NewContractManagerStats(),
		contractStatsEntries:            map[contractStatsKey]*contractStatsEntry{},
		contractStatsCallbacks:          NewCallbackList[ContractStatsFunction](),
		contractStatsSequences:          map[Id]uint64{},
		controlSyncProvide:              NewControlSync(managerCtx, client, "provide"),
		controlSyncProvideOob:           NewControlSyncOob(managerCtx, client, "provide-oob"),
		workers:                         newLifecycleAdmission(),
		closeControlSyncs:               map[*ControlSync]bool{},
	}

	if client.ClientId() != ControlId {
		contractManager.startWorker("provide ping", contractManager.providePing)
	}

	contractManager.startWorker("contract expiry", contractManager.expireQueuedContracts)

	return contractManager
}

// startWorker admits one manager-owned background loop before launch.
func (self *ContractManager) startWorker(name string, run func()) bool {
	if !self.workers.start() {
		return false
	}
	go func() {
		defer self.workers.finish()
		HandleError(run, self.client.Cancel)
		if self.beforeWorkerDoneForTest != nil {
			self.beforeWorkerDoneForTest(name)
		}
	}()
	return true
}

// Close prevents later manager work, cancels control retries, and requests
// callback and background-worker teardown without waiting.
func (self *ContractManager) Close() {
	self.mutex.Lock()
	if self.closed {
		self.mutex.Unlock()
		return
	}
	self.closed = true
	closeControlSyncs := make(
		[]*ControlSync,
		0,
		len(self.closeControlSyncs),
	)
	for controlSync := range self.closeControlSyncs {
		closeControlSyncs = append(closeControlSyncs, controlSync)
	}
	self.mutex.Unlock()

	self.cancel()
	self.workers.close()
	self.controlSyncProvide.Close()
	self.controlSyncProvideOob.Close()
	for _, controlSync := range closeControlSyncs {
		controlSync.Close()
	}
	for _, worker := range self.contractStatusCallbacks.Get() {
		worker.Close()
	}
}

// closeAndWait joins manager background/callback work and every registered
// control retry generation, or returns when ctx expires.
func (self *ContractManager) closeAndWait(ctx context.Context) error {
	self.Close()
	if self.beforeCloseWaitForTest != nil {
		self.beforeCloseWaitForTest()
	}
	var result error
	if err := waitForLifecycleDone(
		ctx,
		self.workers.Done(),
		"contract manager workers",
	); err != nil {
		result = errors.Join(result, err)
	}
	if err := self.controlSyncProvide.closeAndWait(ctx); err != nil {
		result = errors.Join(result, err)
	}
	if err := self.controlSyncProvideOob.closeAndWait(ctx); err != nil {
		result = errors.Join(result, err)
	}

	self.mutex.Lock()
	closeControlSyncs := make(
		[]*ControlSync,
		0,
		len(self.closeControlSyncs),
	)
	for controlSync := range self.closeControlSyncs {
		closeControlSyncs = append(closeControlSyncs, controlSync)
	}
	self.mutex.Unlock()
	for _, controlSync := range closeControlSyncs {
		if err := controlSync.closeAndWait(ctx); err != nil {
			result = errors.Join(result, err)
		}
	}
	return result
}

// expireQueuedContracts periodically closes queued contracts that no sequence
// took within `ContractQueueExpireTimeout` and removes the emptied queues.
// This bounds `destinationContracts` against orphans — e.g. a
// `CreateContractResult` that lands after the owning sequence exit-flushed its
// queue (`FlushContractQueue` force-remove) re-creates the queue entry, and if
// that destination is never used again (provider rotation) the entry would
// otherwise be retained forever.
func (self *ContractManager) expireQueuedContracts() {
	timeout := self.settings.ContractQueueExpireTimeout

	// the contract manager is closing: close all still-queued (pending)
	// contracts so their escrow is released promptly. `closeContracts`
	// routes shutdown closes over the out-of-band api on a Background
	// context, since the client context (and the in-band transport) is
	// already closed.
	finalFlush := func() {
		pending := []*protocol.Contract{}
		func() {
			self.mutex.Lock()
			defer self.mutex.Unlock()

			for contractKey, contractQueue := range self.destinationContracts {
				pending = append(pending, contractQueue.Flush(false)...)
				if contractQueue.IsDone() {
					delete(self.destinationContracts, contractKey)
				}
			}
		}()
		if 0 < len(pending) {
			if self.client.log.V(1).Enabled() {
				self.client.log.Infof("[contract]closing %d pending contracts on close\n", len(pending))
			}
			self.closeContracts(pending)
		}
	}

	for {
		// when expiry is disabled the nil tick channel blocks forever and the
		// loop only waits for shutdown
		var tick <-chan time.Time
		if 0 < timeout {
			tick = time.After(timeout / 2)
		}

		select {
		case <-self.ctx.Done():
			finalFlush()
			return
		case <-self.client.Done():
			// the manager ctx is the client's parent ctx; the client closing
			// is the shutdown signal
			finalFlush()
			return
		case <-tick:
		}

		minEnqueueTime := time.Now().Add(-timeout)
		expired := self.expireQueuedContractsBefore(minEnqueueTime)
		if 0 < len(expired) {
			if self.client.log.V(1).Enabled() {
				self.client.log.Infof("[contract]expired %d queued contracts\n", len(expired))
			}
			// close outside the manager mutex: CloseContract re-takes it
			self.closeContracts(expired)
		}
	}
}

// expireQueuedContractsBefore removes orphaned pending contracts while
// retaining the prefetched successor of every live send contract. A send
// sequence can use one small contract slowly for longer than the orphan expiry
// window; expiring its already-created successor turns the eventual boundary
// into a synchronous control-plane pause. The open contract is the ownership
// lease: sequence teardown removes it synchronously and force-flushes the
// successor queue, so skipping that queue does not make orphan retention
// unbounded.
func (self *ContractManager) expireQueuedContractsBefore(
	minEnqueueTime time.Time,
) []*protocol.Contract {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	expired := []*protocol.Contract{}
	for contractKey, contractQueue := range self.destinationContracts {
		if self.isNetworkPeerContract(contractKey) &&
			self.hasOpenContractForKeyWithLock(contractKey) {
			// Keep the newest successor even when it outlives the orphan
			// timeout, but do not retain every delayed/retried create result.
			// One stale successor is sufficient to make rollover immediate;
			// retaining all of them would make a slow live sequence an
			// unbounded memory/escrow sink.
			expired = append(
				expired,
				contractQueue.ExpireBeforeKeepingNewest(minEnqueueTime)...,
			)
			continue
		}
		expired = append(expired, contractQueue.Expire(minEnqueueTime)...)
		if contractQueue.IsDone() {
			delete(self.destinationContracts, contractKey)
		}
	}
	return expired
}

func (self *ContractManager) providePing() {
	if self.settings.ProvidePingTimeout == 0 {
		return
	}

	// Wait for the client to finish wiring before our first send. This
	// goroutine is started from `NewContractManager`, which runs inside
	// `NewClientWithTag` before `initBuffers` constructs `sendBuffer`.
	// Without this gate the ping path can race the buffer wiring.
	select {
	case <-self.client.ReadyNotify():
	case <-self.ctx.Done():
		return
	}

	// used for logging states only
	logWait := false

	waitForProvide := func() bool {
		for {
			notify := self.provideMonitor.NotifyChannel()
			var provide bool
			func() {
				self.mutex.Lock()
				defer self.mutex.Unlock()

				if self.providePaused {
					provide = false
				} else {
					provide = self.provideModes[protocol.ProvideMode_Public] || self.provideModes[protocol.ProvideMode_PublicStream]
				}
			}()
			if provide {
				if logWait {
					logWait = false
					self.client.log.Infof("[contract]provide ping continue\n")
				}
				return true
			}
			if !logWait {
				logWait = true
				self.client.log.Infof("[contract]provide ping wait\n")
			}
			select {
			case <-self.ctx.Done():
				return false
			case <-notify:
			}
		}
	}

	lastPingTime := time.Time{}
	for {
		if !waitForProvide() {
			return
		}

		// uniform timeout with mean `ProvidePingTimeout`
		timeout := time.Duration(mathrand.Int63n(int64(2*self.settings.ProvidePingTimeout))) - time.Now().Sub(lastPingTime)
		if 0 < timeout {
			select {
			case <-self.ctx.Done():
				return
			case <-WakeupAfter(timeout, self.settings.ProvidePingTimeout):
			}
		} else {
			select {
			case <-self.ctx.Done():
				return
			default:
			}
		}

		ack := make(chan error)
		providePing := &protocol.ProvidePing{}
		frame, err := ToFrame(providePing, self.settings.ProtocolVersion)
		if err != nil {
			self.client.log.Infof("[contract]could not create provide ping frame = %s", err)
			return
		}
		if self.beforeProvidePingSendForTest != nil {
			self.beforeProvidePingSendForTest(frame)
		}
		if !self.client.SendControl(frame, func(err error) {
			select {
			case ack <- err:
			case <-self.ctx.Done():
			}
		}) {
			// SendControl transfers MessageBytes only on success. Shutdown keeps
			// ownership with this worker, so release it before the joined exit.
			MessagePoolReturn(frame.MessageBytes)
			return
		}
		// wait for the ack before sending another ping
		select {
		case err := <-ack:
			if err != nil {
				self.client.log.Infof("[contract]provide ping err = %s\n", err)
			}
		case <-self.ctx.Done():
			return
		}
		lastPingTime = time.Now()
	}
}

func (self *ContractManager) StandardContractTransferByteCount() ByteCount {
	return self.settings.StandardContractTransferByteCount
}

func (self *ContractManager) AddContractStatusCallback(contractStatusCallback ContractStatusFunction) func() {
	if self.beforeCallbackAdmissionLockForTest != nil {
		self.beforeCallbackAdmissionLockForTest()
	}
	self.mutex.Lock()
	if self.closed || !self.workers.start() {
		self.mutex.Unlock()
		return func() {}
	}
	worker := newContractStatusCallbackWorker(
		self.ctx,
		contractStatusCallback,
		self.settings.SequenceBufferSize,
		self.workers.finish,
	)
	callbackId := self.contractStatusCallbacks.Add(worker)
	self.mutex.Unlock()
	return func() {
		self.contractStatusCallbacks.Remove(callbackId)
		worker.Close()
	}
}

// addContractStatusDispatchCallback registers an internal callback whose only
// permitted work is a bounded, nonblocking Dispatch into a parent-owned
// worker. RemoteUserNatMultiClient uses it to avoid allocating a goroutine and
// SequenceBufferSize-sized ring for every exit when one coalescer per window is
// sufficient. Public callbacks continue through AddContractStatusCallback and
// retain independent failure containment.
func (self *ContractManager) addContractStatusDispatchCallback(
	contractStatusCallback ContractStatusFunction,
) func() {
	if contractStatusCallback == nil {
		return func() {}
	}
	self.mutex.Lock()
	if self.closed {
		self.mutex.Unlock()
		return func() {}
	}
	callbackId := self.contractStatusDispatchCallbacks.Add(contractStatusCallback)
	self.mutex.Unlock()
	return func() {
		self.contractStatusDispatchCallbacks.Remove(callbackId)
	}
}

// ContractStatusFunction
func (self *ContractManager) contractStatus(contractStatus *ContractStatus) {
	for _, dispatch := range self.contractStatusDispatchCallbacks.Get() {
		dispatch(contractStatus)
	}
	for _, contractStatusCallback := range self.contractStatusCallbacks.Get() {
		contractStatusCallback.Dispatch(contractStatus)
	}
}

/*
// ReceiveFunction
func (self *ContractManager) Receive(source TransferPath, frames []*protocol.Frame, peer Peer) {
	if source.IsControlSource() {
		for _, frame := range frames {
			self.handleControlFrame(nil, frame)
		}
	}
}
*/

func (self *ContractManager) HandleControlFrame(contractKey ContractKey, frame *protocol.Frame) error {
	return self.handleControlFrameForQueue(contractKey, nil, frame)
}

// Handles one contract result for either the current key or the exact queue
// generation that issued the request. A captured generation prevents a late
// response from reopening a route hint that its send sequence already retired.
func (self *ContractManager) handleControlFrameForQueue(
	contractKey ContractKey,
	ownedQueue *contractQueue,
	frame *protocol.Frame,
) error {
	switch frame.MessageType {
	case protocol.MessageType_TransferCreateContractResult:
		contracts, contractErrors := self.parseControlFrame(frame)
		for _, contract := range contracts {
			c := func() error {
				var contractStatus *ContractStatus
				defer func() {
					if contractStatus != nil {
						self.contractStatus(contractStatus)
					}
				}()
				var err error
				if ownedQueue == nil {
					err = self.addContract(contractKey, contract)
				} else {
					err = self.addContractToQueue(contractKey, ownedQueue, contract)
				}
				if errors.Is(err, errContractQueueDrained) {
					// The owning send sequence promoted or closed while this OOB
					// request was in flight. Retire the returned platform contract
					// without publishing it under a replacement queue generation.
					self.closeContracts([]*protocol.Contract{contract})
					return nil
				}
				if err != nil {
					// contract rejected
					contractError := protocol.ContractError_Trust
					contractStatus = &ContractStatus{
						Key:   contractKey,
						Error: &contractError,
					}
					return err
				}
				storedContract := &protocol.StoredContract{}
				err = ProtoUnmarshal(contract.StoredContractBytes, storedContract)
				if err != nil {
					contractError := protocol.ContractError_Invalid
					contractStatus = &ContractStatus{
						Key:   contractKey,
						Error: &contractError,
					}
					return err
				}
				premium := false
				if storedContract.Priority != nil {
					premium = 0 < *storedContract.Priority
				}
				contractStatus = &ContractStatus{
					Key:     contractKey,
					Premium: premium,
				}
				return nil
			}
			if self.client.log.V(2).Enabled() {
				TraceWithReturn(
					"[contract]add",
					c,
				)
			} else {
				c()
			}
		}
		for _, contractError := range contractErrors {
			if self.client.log.V(1).Enabled() {
				self.client.log.Infof("[contract]error = %s\n", contractError)
			}
			c := func() {
				contractStatus := &ContractStatus{
					Key:   contractKey,
					Error: &contractError,
				}

				self.contractStatus(contractStatus)
			}
			if self.client.log.V(2).Enabled() {
				Trace(
					fmt.Sprintf("[contract]error = %s", contractError),
					c,
				)
			} else {
				c()
			}
		}
	}
	return nil
}

// frames are verified before calling to be from source ControlId
func (self *ContractManager) parseControlFrame(frame *protocol.Frame) (
	contracts []*protocol.Contract,
	contractErrors []protocol.ContractError,
) {
	addResult := func(v *protocol.CreateContractResult) {
		if contractError := v.Error; contractError != nil {
			contractErrors = append(contractErrors, *contractError)
		} else if contract := v.Contract; contract != nil {
			storedContract := &protocol.StoredContract{}
			err := ProtoUnmarshal(contract.StoredContractBytes, storedContract)
			if err != nil {
				return
			}

			contracts = append(contracts, contract)
		}
	}

	switch frame.MessageType {
	case protocol.MessageType_TransferCreateContractResult:
		b := make([]byte, len(frame.MessageBytes))
		copy(b, frame.MessageBytes)
		r := &protocol.CreateContractResult{}
		err := ProtoUnmarshal(b, r)
		if err == nil {
			addResult(r)
		}
	}
	return
}

func (self *ContractManager) GetProvideSecretKeys() map[protocol.ProvideMode][]byte {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	return maps.Clone(self.provideSecretKeys)
}

func (self *ContractManager) LoadProvideSecretKeys(provideSecretKeys map[protocol.ProvideMode][]byte) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	for provideMode, provideSecretKey := range provideSecretKeys {
		self.provideSecretKeys[provideMode] = provideSecretKey
	}
}

func (self *ContractManager) InitProvideSecretKeys() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	for i, _ := range protocol.ProvideMode_name {
		provideMode := protocol.ProvideMode(i)
		provideSecretKey, ok := self.provideSecretKeys[provideMode]
		if !ok {
			// generate a new key
			provideSecretKey = make([]byte, 32)
			_, err := rand.Read(provideSecretKey)
			if err != nil {
				panic(err)
			}
			self.provideSecretKeys[provideMode] = provideSecretKey
		}
	}
}

func (self *ContractManager) SetProvidePaused(providePaused bool) bool {
	changed := false
	func() {
		self.mutex.Lock()
		defer self.mutex.Unlock()

		if self.providePaused != providePaused {
			self.providePaused = providePaused
			self.provideMonitor.NotifyAll()
			changed = true
		}
	}()
	if changed {
		self.reconcileInboundProviderStreams()
		if provideFrame, err := self.provideFrame(); err == nil && provideFrame != nil {
			self.controlSyncProvide.Send(
				provideFrame,
				nil,
				nil,
			)
		}
		return true
	}
	return false
}

// inboundProviderStreamPolicy converts provide registration into the policy
// applicable to endpoint StreamOpen state. Stream is return-traffic
// registration, not permission to originate a provider stream. Pause keeps
// only same-network provider streams, matching provideFrame and Verify.
func inboundProviderStreamPolicy(
	provideModes map[protocol.ProvideMode]bool,
	providePaused bool,
) (allowAny bool, allowNetwork bool) {
	allowNetwork = provideModes[protocol.ProvideMode_Network]
	if !providePaused {
		allowAny =
			provideModes[protocol.ProvideMode_FriendsAndFamily] ||
				provideModes[protocol.ProvideMode_Public] ||
				provideModes[protocol.ProvideMode_PublicStream]
	}
	return
}

func (self *ContractManager) inboundProviderStreamPolicy() (allowAny bool, allowNetwork bool) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return inboundProviderStreamPolicy(self.provideModes, self.providePaused)
}

func (self *ContractManager) reconcileInboundProviderStreams() {
	if self.client.streamManager == nil || self.client.peerManager == nil {
		return
	}
	allowAny, allowNetwork := self.inboundProviderStreamPolicy()
	self.client.streamManager.reconcileInboundProviderStreams(allowAny, allowNetwork)
}

func (self *ContractManager) IsProvidePaused() bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	return self.providePaused
}

func (self *ContractManager) provideFrame() (*protocol.Frame, error) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	var provide *protocol.Provide
	if self.providePaused {
		// pause stops providing to public/ff only.
		// keep ProvideMode_Stream to allow return traffic and
		// ProvideMode_Network so network peers never fall back to stream, if set
		provideKeys := []*protocol.ProvideKey{}
		for provideMode, allow := range self.provideModes {
			if allow && (provideMode == protocol.ProvideMode_Stream || provideMode == protocol.ProvideMode_Network) {
				provideSecretKey, ok := self.provideSecretKeys[provideMode]
				if ok {
					provideKeys = append(provideKeys, &protocol.ProvideKey{
						Mode:             provideMode,
						ProvideSecretKey: provideSecretKey,
					})
				} else {
					self.client.log.Infof("[contract]missing provide key for %d. Will omit.\n", provideMode)
				}
			}
		}

		provide = &protocol.Provide{
			Keys: provideKeys,
		}
	} else {
		provideKeys := []*protocol.ProvideKey{}
		for provideMode, allow := range self.provideModes {
			if allow {
				provideSecretKey, ok := self.provideSecretKeys[provideMode]
				if ok {
					provideKeys = append(provideKeys, &protocol.ProvideKey{
						Mode:             provideMode,
						ProvideSecretKey: provideSecretKey,
					})
				} else {
					self.client.log.Infof("[contract]missing provide key for %d. Will omit.\n", provideMode)
				}
			}
		}

		provide = &protocol.Provide{
			Keys: provideKeys,
		}
	}
	provideFrame, err := ToFrame(provide, self.settings.ProtocolVersion)
	if err != nil {
		self.client.log.Infof("[contract]could not create provide frame = %s", err)
		return nil, err
	}
	return provideFrame, nil
}

func (self *ContractManager) SetProvideModesWithReturnTraffic(provideModes map[protocol.ProvideMode]bool) {
	self.SetProvideModesWithReturnTrafficWithAckCallback(provideModes, func(err error) {})
}

// clients must enable `ProvideMode_Stream` to allow return traffic
func (self *ContractManager) SetProvideModesWithReturnTrafficWithAckCallback(provideModes map[protocol.ProvideMode]bool, ackCallback func(err error)) {
	updatedProvideModes := map[protocol.ProvideMode]bool{}
	maps.Copy(updatedProvideModes, provideModes)
	updatedProvideModes[protocol.ProvideMode_Stream] = true
	self.SetProvideModesWithAckCallback(updatedProvideModes, ackCallback)
}

func (self *ContractManager) SetProvideModes(provideModes map[protocol.ProvideMode]bool) {
	self.SetProvideModesWithAckCallback(provideModes, func(err error) {})
}

// applyProvideModes generates any missing provide secret keys and updates the
// active provide modes. The provide frame must be (re)sent afterward to register
// the change with the platform.
func (self *ContractManager) applyProvideModes(provideModes map[protocol.ProvideMode]bool) {
	self.mutex.Lock()

	// keep all keys (see note on `provideSecretKeys`)
	for provideMode, allow := range provideModes {
		if allow {
			provideSecretKey, ok := self.provideSecretKeys[provideMode]
			if !ok {
				// generate a new key
				provideSecretKey = make([]byte, 32)
				_, err := rand.Read(provideSecretKey)
				if err != nil {
					panic(err)
				}
				self.provideSecretKeys[provideMode] = provideSecretKey
			}
		}
	}

	self.provideModes = maps.Clone(provideModes)
	self.provideMonitor.NotifyAll()
	self.mutex.Unlock()

	// Reconcile after publishing the new local policy and outside the manager
	// lock: stream cancellation can fan into route/transport teardown.
	self.reconcileInboundProviderStreams()
}

func (self *ContractManager) SetProvideModesWithAckCallback(provideModes map[protocol.ProvideMode]bool, ackCallback func(err error)) {
	self.applyProvideModes(provideModes)
	if provideFrame, err := self.provideFrame(); err != nil {
		ackCallback(err)
	} else if provideFrame != nil {
		self.controlSyncProvide.Send(
			provideFrame,
			nil,
			ackCallback,
		)
	} else {
		ackCallback(nil)
	}
}

// SetProvideModesWithReturnTrafficWithOobAckCallback is like
// SetProvideModesWithReturnTrafficWithAckCallback, but registers the provide via
// the out-of-band control, so the ack means the platform has committed the
// provide secret (the in-band control ack only means the message was delivered).
// Use this when a caller must wait for the secret to be registered before using
// the client — e.g. the return path of a multi-client client, whose companion
// (Stream) contracts are verified against this secret.
func (self *ContractManager) SetProvideModesWithReturnTrafficWithOobAckCallback(provideModes map[protocol.ProvideMode]bool, ackCallback func(err error)) {
	updatedProvideModes := map[protocol.ProvideMode]bool{}
	maps.Copy(updatedProvideModes, provideModes)
	updatedProvideModes[protocol.ProvideMode_Stream] = true
	self.SetProvideModesWithOobAckCallback(updatedProvideModes, ackCallback)
}

func (self *ContractManager) SetProvideModesWithOobAckCallback(provideModes map[protocol.ProvideMode]bool, ackCallback func(err error)) {
	self.applyProvideModes(provideModes)
	if provideFrame, err := self.provideFrame(); err != nil {
		ackCallback(err)
	} else if provideFrame != nil {
		self.controlSyncProvideOob.Send(
			provideFrame,
			ackCallback,
		)
	} else {
		ackCallback(nil)
	}
}

func (self *ContractManager) GetProvideModes() map[protocol.ProvideMode]bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	return maps.Clone(self.provideModes)
}

func (self *ContractManager) Verify(storedContractHmac []byte, storedContractBytes []byte, provideMode protocol.ProvideMode) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	// when paused, only allow ProvideMode_Stream for return traffic and
	// ProvideMode_Network for network peers (pause stops public/ff only)
	if self.providePaused && provideMode != protocol.ProvideMode_Stream && provideMode != protocol.ProvideMode_Network {
		return false
	}

	if !self.provideModes[provideMode] {
		return false
	}

	provideSecretKey, ok := self.provideSecretKeys[provideMode]
	if !ok {
		// provide mode is not enabled
		return false
	}

	return VerifyStoredContract(self.settings, provideSecretKey, storedContractBytes, storedContractHmac)
}

func (self *ContractManager) GetProvideSecretKey(provideMode protocol.ProvideMode) ([]byte, bool) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if !self.provideModes[provideMode] {
		return nil, false
	}

	provideSecretKey, ok := self.provideSecretKeys[provideMode]
	return provideSecretKey, ok
}

func (self *ContractManager) RequireProvideSecretKey(provideMode protocol.ProvideMode) []byte {
	secretKey, ok := self.GetProvideSecretKey(provideMode)
	if !ok {
		panic(fmt.Errorf("Missing provide secret for %s", provideMode))
	}
	return secretKey
}

func (self *ContractManager) AddNoContractPeer(clientId Id) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.sendNoContractClientIds[clientId] = true
	self.receiveNoContractClientIds[clientId] = true
}

func (self *ContractManager) SendNoContract(destinationId Id) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if allow, ok := self.sendNoContractClientIds[destinationId]; ok {
		return allow
	}

	if !self.settings.ContractsEnabled() {
		return true
	}

	return false
}

func (self *ContractManager) ReceiveNoContract(sourceId Id) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if allow, ok := self.receiveNoContractClientIds[sourceId]; ok {
		return allow
	}

	if !self.settings.ContractsEnabled() {
		return true
	}

	return false
}

func (self *ContractManager) TakeContract(
	ctx context.Context,
	contractKey ContractKey,
	timeout time.Duration,
) *protocol.Contract {
	contractQueue := self.openContractQueue(contractKey)
	defer self.closeContractQueue(contractKey, contractQueue)

	enterTime := time.Now()
	for {
		notify := contractQueue.updateMonitor.NotifyChannel()
		var minEnqueueTime time.Time
		if 0 < self.settings.ContractQueueExpireTimeout &&
			!(self.isNetworkPeerContract(contractKey) &&
				self.hasOpenContractForKey(contractKey)) {
			minEnqueueTime = time.Now().Add(-self.settings.ContractQueueExpireTimeout)
		}
		contract, expired := contractQueue.Poll(minEnqueueTime)
		if 0 < len(expired) {
			// stale queued contracts may already be force-closed server-side;
			// close them rather than handing them to a sequence
			self.closeContracts(expired)
		}

		if contract == nil && contractQueue.Drained() {
			// the queue was force-removed (e.g., the owning send sequence closed).
			// notifications now go to a fresh queue at this key; bail out instead
			// of waiting forever on this orphan's monitor.
			return nil
		}

		if contract != nil {
			storedContract := &protocol.StoredContract{}
			if err := ProtoUnmarshal(contract.StoredContractBytes, storedContract); err == nil {
				if contractId, err := IdFromBytes(storedContract.ContractId); err == nil {
					func() {
						self.mutex.Lock()
						defer self.mutex.Unlock()

						self.localStats.ContractOpenCount += 1
						self.localStats.ContractOpenByteCounts[contractId] = ByteCount(storedContract.TransferByteCount)
						self.localStats.ContractOpenKeys[contractId] = contractKey
					}()
				}
			}

			return contract
		}

		if timeout < 0 {
			select {
			case <-self.ctx.Done():
				return nil
			case <-ctx.Done():
				return nil
			case <-notify:
			}
		} else if timeout == 0 {
			return nil
		} else {
			remainingTimeout := enterTime.Add(timeout).Sub(time.Now())
			if remainingTimeout <= 0 {
				return nil
			}
			select {
			case <-self.ctx.Done():
				return nil
			case <-ctx.Done():
				return nil
			case <-notify:
			case <-time.After(remainingTimeout):
				return nil
			}
		}
	}
}

func (self *ContractManager) hasOpenContractForKey(contractKey ContractKey) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return self.hasOpenContractForKeyWithLock(contractKey)
}

func (self *ContractManager) hasOpenContractForKeyWithLock(contractKey ContractKey) bool {
	if self.settings.LegacyCreateContract {
		contractKey = contractKey.Legacy()
	}
	for _, openContractKey := range self.localStats.ContractOpenKeys {
		if self.settings.LegacyCreateContract {
			openContractKey = openContractKey.Legacy()
		}
		if openContractKey == contractKey {
			return true
		}
	}
	return false
}

func (self *ContractManager) addContract(contractKey ContractKey, contract *protocol.Contract) error {
	contractQueue := self.openContractQueue(contractKey)
	defer self.closeContractQueue(contractKey, contractQueue)
	return self.addContractToQueue(contractKey, contractQueue, contract)
}

// Validates and adds a result only to the queue generation that owns the
// corresponding request. The queue itself linearizes add against drain.
func (self *ContractManager) addContractToQueue(
	contractKey ContractKey,
	contractQueue *contractQueue,
	contract *protocol.Contract,
) error {
	storedContract := &protocol.StoredContract{}
	err := ProtoUnmarshal(contract.StoredContractBytes, storedContract)
	if err != nil {
		return err
	}

	sourceId, err := IdFromBytes(storedContract.SourceId)
	if err != nil {
		return fmt.Errorf("Contract source id malformed: %w", err)
	}
	if sourceId != self.client.ClientId() {
		return fmt.Errorf("Contract source must be this client: %s<>%s", sourceId, self.client.ClientId())
	}

	if self.client.log.V(1).Enabled() {
		self.client.log.Infof("[contract]add %s %s\n", self.client.ClientId(), contractKey.Destination)
	}

	return contractQueue.Add(contract, storedContract)
}

func (self *ContractManager) CreateContract(contractKey ContractKey, contractSeqIndex uint64, minByteCount ByteCount) {
	// Retain ownership through the asynchronous callback. A route promotion can
	// force-remove and drain this exact generation while the request is in
	// flight; the callback then rejects and closes its stale result instead of
	// reopening the old key.
	contractQueue := self.openContractQueue(contractKey)

	streamVersion := uint32(DefaultStreamVersion)
	senderRole := contractKey.EncryptionRole.toProtobuf()

	createContract := &protocol.CreateContract{
		DestinationId:     contractKey.Destination.DestinationId.Bytes(),
		IntermediaryIds:   contractKey.IntermediaryIds.Bytes(),
		TransferByteCount: uint64(self.contractByteCount(contractKey, contractSeqIndex, minByteCount)),
		Companion:         contractKey.CompanionContract,
		ForceStream:       &contractKey.ForceStream,
		StreamVersion:     &streamVersion,
		SenderRole:        &senderRole,
	}
	if self.settings.TrackUsedContracts {
		createContract.UsedContractIds = contractQueue.UsedContractIdBytes()
	}
	frame, err := ToFrame(createContract, self.settings.ProtocolVersion)
	if err != nil {
		self.closeContractQueue(contractKey, contractQueue)
		self.client.log.Infof("[contract]could not create contract frame = %s", err)
		return
	}

	if self.client.log.V(1).Enabled() {
		self.client.log.Infof("[contract]create %s %s\n", self.client.ClientId(), contractKey.Destination)
	}

	self.client.ClientOob().SendControl(
		[]*protocol.Frame{frame},
		func(resultFrames []*protocol.Frame, err error) {
			defer self.closeContractQueue(contractKey, contractQueue)
			if err == nil {
				// the OOB round-trip completed: the backend is reachable
				noteBackendSuccess()
				for _, resultFrame := range resultFrames {
					self.handleControlFrameForQueue(contractKey, contractQueue, resultFrame)
				}
			} else {
				select {
				case <-self.client.Done():
					// no need to log warnings when the client closes
				default:
					noteBackendFailure()
					if ok, suppressed := shouldLogOobErr(); ok {
						if suppressed > 0 {
							self.client.log.Infof("[contract]oob err = %s (%d suppressed)\n", err, suppressed)
						} else {
							self.client.log.Infof("[contract]oob err = %s\n", err)
						}
					} else if v := self.client.log.V(1); v.Enabled() {
						v.Infof("[contract]oob err = %s\n", err)
					}
				}
			}
		},
	)
}

func (self *ContractManager) contractByteCount(
	contractKey ContractKey,
	contractSeqIndex uint64,
	minByteCount ByteCount,
) ByteCount {
	initialContractTransferByteCount := max(
		ByteCount(0),
		self.settings.InitialContractTransferByteCount,
	)
	if self.isNetworkPeerContract(contractKey) {
		initialContractTransferByteCount = max(
			initialContractTransferByteCount,
			self.settings.InitialNetworkPeerContractTransferByteCount,
		)
	}
	standardContractTransferByteCount := self.settings.StandardContractTransferByteCount
	if standardContractTransferByteCount <= 0 {
		standardContractTransferByteCount = initialContractTransferByteCount
	} else {
		initialContractTransferByteCount = min(
			initialContractTransferByteCount,
			standardContractTransferByteCount,
		)
	}
	targetByteCount := func() ByteCount {
		if self.settings.ContractTransferByteSeqScale <= contractSeqIndex {
			return standardContractTransferByteCount
		} else {
			// lerp between initial and standard
			return initialContractTransferByteCount + ByteCount(
				(contractSeqIndex*uint64(standardContractTransferByteCount-initialContractTransferByteCount))/self.settings.ContractTransferByteSeqScale,
			)
		}
	}()
	return max(targetByteCount, minByteCount, ByteCount(0))
}

func (self *ContractManager) isNetworkPeerContract(contractKey ContractKey) bool {
	if contractKey.NetworkPeer {
		return true
	}
	destinationId := contractKey.Destination.DestinationId
	if destinationId == (Id{}) {
		return false
	}
	return self.client != nil &&
		self.client.peerManager != nil &&
		self.client.peerManager.isConnectedNetworkPeer(destinationId)
}

func (self *ContractManager) CheckpointContract(
	contractId Id,
	ackedByteCount ByteCount,
	unackedByteCount ByteCount,
) {
	self.CloseContractWithCheckpoint(contractId, ackedByteCount, unackedByteCount, true)
}

func (self *ContractManager) CloseContract(
	contractId Id,
	ackedByteCount ByteCount,
	unackedByteCount ByteCount,
) {
	self.CloseContractWithCheckpoint(contractId, ackedByteCount, unackedByteCount, false)
}

func (self *ContractManager) CloseContractWithCheckpoint(
	contractId Id,
	ackedByteCount ByteCount,
	unackedByteCount ByteCount,
	checkpoint bool,
) {
	// the sequence stops updating the contract at close/checkpoint,
	// so this is final for the stats entry either way
	self.closeContractStats(contractId)

	opened := false
	var contractKey ContractKey

	func() {
		self.mutex.Lock()
		defer self.mutex.Unlock()

		if _, ok := self.localStats.ContractOpenByteCounts[contractId]; ok {
			// opened via the contract manager
			opened = true
			contractKey = self.localStats.ContractOpenKeys[contractId]
			self.localStats.ContractCloseCount += 1
			delete(self.localStats.ContractOpenByteCounts, contractId)
			delete(self.localStats.ContractOpenKeys, contractId)
			self.localStats.ContractCloseByteCount += ackedByteCount
		} else {
			self.localStats.ReceiveContractCloseByteCount += ackedByteCount
		}
	}()

	// Reliable delivery via a per-contract `ControlSync`. The
	// previous implementation called `ClientOob().SendControl(...)`
	// once and dropped the result on the floor — a single transient
	// transport failure would leave the contract `open=true` on the
	// server, its escrow permanently deducted from the network
	// balance with no way for the client to ever signal completion.
	//
	// `ControlSync` retries the send until the platform acks (or
	// the client's context is canceled). One `ControlSync` per
	// close: each contract's close is independent and must not be
	// superseded by another close's `Send` (which is what would
	// happen on a shared `ControlSync` — its `syncCount` would
	// abandon the older close as "replaced"). The per-call instance
	// holds little state — a mutex, a monitor, and a derived context
	// — and its supervisor goroutine exits on success or when the
	// parent context closes, so there's no long-lived leak.
	frame, err := ToFrame(&protocol.CloseContract{
		ContractId:       contractId.Bytes(),
		AckedByteCount:   uint64(ackedByteCount),
		UnackedByteCount: uint64(unackedByteCount),
		Checkpoint:       checkpoint,
	}, self.settings.ProtocolVersion)
	if err != nil {
		self.client.log.Infof("[contract]could not create close contract frame = %s\n", err)
		return
	}

	if self.ctx.Err() != nil || self.client.IsDone() {
		// the client context is closed (the contract manager is closing).
		// note the manager ctx is the client's parent ctx, so check both.
		// `ControlSync` rides the in-band client transport, which is gone —
		// it would drop the close without a single attempt. Send a one-shot
		// cleanup over the out-of-band api on a Background context instead,
		// since the lifecycle context is closed. One shot, never retried, so
		// cleanup cannot run away; the server's expired-contract force-close
		// remains the backstop if the single attempt fails.
		sendCallback := func(resultFrames []*protocol.Frame, sendErr error) {
			if sendErr == nil {
				if self.client.log.V(1).Enabled() {
					self.client.log.Infof("[contract]closed %s after client close\n", contractId)
				}
			} else {
				self.client.log.Infof("[contract]could not close %s after client close = %s\n", contractId, sendErr)
			}
		}
		frames := []*protocol.Frame{frame}
		if clientOob, ok := self.client.ClientOob().(OutOfBandControlWithCtx); ok {
			clientOob.SendControlWithCtx(context.Background(), frames, sendCallback)
		} else {
			self.client.ClientOob().SendControl(frames, sendCallback)
		}
		return
	}

	closeControlSync := NewControlSync(self.ctx, self.client, fmt.Sprintf("close-contract-%s", contractId))
	self.mutex.Lock()
	if self.closed {
		self.mutex.Unlock()
		closeControlSync.Close()
		MessagePoolReturn(frame.MessageBytes)
		return
	}
	self.closeControlSyncs[closeControlSync] = true
	if !self.startWorker("contract close sync", func() {
		<-closeControlSync.workers.Done()
		self.mutex.Lock()
		delete(self.closeControlSyncs, closeControlSync)
		self.mutex.Unlock()
	}) {
		delete(self.closeControlSyncs, closeControlSync)
		self.mutex.Unlock()
		closeControlSync.Close()
		MessagePoolReturn(frame.MessageBytes)
		return
	}
	self.mutex.Unlock()
	closeControlSync.Send(frame, nil, func(sendErr error) {
		defer closeControlSync.Close()
		if sendErr == nil && opened {
			contractQueue := self.openContractQueue(contractKey)
			contractQueue.RemoveUsedContract(contractId)
			self.closeContractQueue(contractKey, contractQueue)
		}
	})
}

func (self *ContractManager) LocalStats() *ContractManagerStats {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	return &ContractManagerStats{
		ContractOpenCount:      self.localStats.ContractOpenCount,
		ContractCloseCount:     self.localStats.ContractCloseCount,
		ContractOpenByteCounts: maps.Clone(self.localStats.ContractOpenByteCounts),
		ContractOpenKeys:       maps.Clone(self.localStats.ContractOpenKeys),
		// ContractOpenDestinationIds: maps.Clone(self.localStats.ContractOpenDestinationIds),
		ContractCloseByteCount:        self.localStats.ContractCloseByteCount,
		ReceiveContractCloseByteCount: self.localStats.ReceiveContractCloseByteCount,
	}
}

func (self *ContractManager) ResetLocalStats() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	// Open-contract maps are operational ownership state as well as stats:
	// they let CloseContract associate the close with its queue and keep a
	// live sequence's prefetched successor out of orphan expiry. Reset the
	// counters while preserving those live entries.
	openByteCounts := self.localStats.ContractOpenByteCounts
	openKeys := self.localStats.ContractOpenKeys
	self.localStats = NewContractManagerStats()
	self.localStats.ContractOpenByteCounts = openByteCounts
	self.localStats.ContractOpenKeys = openKeys
}

func (self *ContractManager) Flush(resetUsedContractIds bool) []Id {
	// close queued contracts
	contracts := func() []*protocol.Contract {
		self.mutex.Lock()
		defer self.mutex.Unlock()

		if self.client.log.V(1).Enabled() {
			self.client.log.Infof("[contract]flush %s %v\n", self.client.ClientId(), slices.Collect(maps.Keys(self.destinationContracts)))
		}

		contracts := []*protocol.Contract{}
		for contractKey, contractQueue := range self.destinationContracts {
			for _, contract := range contractQueue.Flush(resetUsedContractIds) {
				contracts = append(contracts, contract)
			}
			if contractQueue.IsDone() {
				delete(self.destinationContracts, contractKey)
			}
		}
		return contracts
	}()

	return self.closeContracts(contracts)
}

func (self *ContractManager) FlushContractQueue(contractKey ContractKey, resetUsedContractIds bool) []Id {
	if self.settings.LegacyCreateContract {
		contractKey = contractKey.Legacy()
	}

	var contractQueue *contractQueue
	var contracts []*protocol.Contract
	func() {
		// Keep the manager-to-queue lock order used by every queue lifecycle
		// operation. Holding the manager lock through drain and map deletion
		// makes retirement one boundary: a concurrent opener either owns this
		// generation before the drain or creates a fresh one after deletion.
		self.mutex.Lock()
		defer self.mutex.Unlock()

		contractQueue = self.destinationContracts[contractKey]
		if contractQueue == nil {
			return
		}
		contractQueue.mutex.Lock()
		contracts = contractQueue.flushAndDrainWithLock(resetUsedContractIds)
		contractQueue.mutex.Unlock()
		if self.destinationContracts[contractKey] == contractQueue {
			delete(self.destinationContracts, contractKey)
		}
	}()
	if contractQueue != nil {
		contractQueue.updateMonitor.NotifyAll()
	}

	return self.closeContracts(contracts)
}

func (self *ContractManager) closeContracts(contracts []*protocol.Contract) []Id {
	contractIds := []Id{}
	for _, contract := range contracts {
		storedContract := &protocol.StoredContract{}
		if err := ProtoUnmarshal(contract.StoredContractBytes, storedContract); err == nil {
			if contractId, err := IdFromBytes(storedContract.ContractId); err == nil {
				contractIds = append(contractIds, contractId)
				self.CloseContract(contractId, ByteCount(0), ByteCount(0))
			}
		}
	}
	return contractIds
}

func (self *ContractManager) openContractQueue(contractKey ContractKey) *contractQueue {
	if self.settings.LegacyCreateContract {
		contractKey = contractKey.Legacy()
	}
	if self.testingBeforeOpenContractQueueLock != nil {
		self.testingBeforeOpenContractQueueLock(contractKey)
	}

	self.mutex.Lock()
	defer self.mutex.Unlock()

	contractQueue, ok := self.destinationContracts[contractKey]
	if !ok {
		contractQueue = newContractQueue(self.client.log, self.settings.TrackUsedContracts)
		self.destinationContracts[contractKey] = contractQueue
	}
	contractQueue.Open()

	return contractQueue
}

func (self *ContractManager) closeContractQueue(contractKey ContractKey, contractQueue *contractQueue) {
	self.closeContractQueueWithForceRemove(contractKey, contractQueue, false)
}

func (self *ContractManager) closeContractQueueWithForceRemove(
	contractKey ContractKey,
	ownedQueue *contractQueue,
	forceRemove bool,
) {
	if self.settings.LegacyCreateContract {
		contractKey = contractKey.Legacy()
	}
	if ownedQueue == nil {
		return
	}

	var toDrain *contractQueue
	func() {
		self.mutex.Lock()
		defer self.mutex.Unlock()

		// Close exactly the queue generation returned by openContractQueue.
		// A force-flush may have removed it and installed a replacement at the
		// same key while this caller was blocked in OOB/control work. Looking
		// up by key here would decrement and potentially delete that replacement.
		ownedQueue.Close()
		if self.destinationContracts[contractKey] != ownedQueue {
			if forceRemove {
				toDrain = ownedQueue
			}
			return
		}
		if forceRemove {
			// remove from the map so future addContract creates a fresh queue.
			// existing waiters on the old monitor would never wake up
			// otherwise; drain wakes them so they can bail.
			delete(self.destinationContracts, contractKey)
			toDrain = ownedQueue
		} else if ownedQueue.IsDone() {
			delete(self.destinationContracts, contractKey)
		}
	}()
	if toDrain != nil {
		toDrain.Drain()
	}
}

// a contract waiting in the queue, stamped so unconsumed contracts can be
// expired (see `ContractQueueExpireTimeout`)
type queuedContract struct {
	contract    *protocol.Contract
	enqueueTime time.Time
}

type contractQueue struct {
	updateMonitor *Monitor
	log           Logger

	mutex     sync.Mutex
	openCount int
	contracts map[Id]*queuedContract
	drained   bool

	// remember all added contract ids
	trackUsedContracts bool
	usedContractIds    map[Id]bool

	// Tests pause after this queue lock is owned so manager-lock atomicity can
	// be proved at the drain boundary. Nil keeps production flushes unchanged.
	testingBeforeFlushWithLock func()
}

var errContractQueueDrained = errors.New("contract queue drained")

func newContractQueue(log Logger, trackUsedContracts bool) *contractQueue {
	return &contractQueue{
		updateMonitor:      NewMonitor(),
		log:                loggerOrDefault(log),
		openCount:          0,
		contracts:          map[Id]*queuedContract{},
		trackUsedContracts: trackUsedContracts,
		usedContractIds:    map[Id]bool{},
	}
}

func (self *contractQueue) Open() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.openCount += 1
}

func (self *contractQueue) Close() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.openCount -= 1
}

// Poll returns one queued contract, never one enqueued before
// `minEnqueueTime` — stale entries are removed and returned as `expired` for
// the caller to close (the platform force-closes unused contracts, so a stale
// queued contract may already be settled server-side). A zero `minEnqueueTime`
// expires nothing.
func (self *contractQueue) Poll(minEnqueueTime time.Time) (*protocol.Contract, []*protocol.Contract) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	expired := self.expireWithLock(minEnqueueTime)

	// choose arbitrarily
	for contractId, queuedContract := range self.contracts {
		delete(self.contracts, contractId)
		return queuedContract.contract, expired
	}
	return nil, expired
}

// Expire removes and returns all contracts enqueued before `minEnqueueTime`.
func (self *contractQueue) Expire(minEnqueueTime time.Time) []*protocol.Contract {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	return self.expireWithLock(minEnqueueTime)
}

// ExpireBeforeKeepingNewest expires stale entries while retaining at most one
// stale successor. If a non-stale successor exists, every stale entry can be
// removed; otherwise the newest stale entry is the bounded rollover reserve.
func (self *contractQueue) ExpireBeforeKeepingNewest(
	minEnqueueTime time.Time,
) []*protocol.Contract {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if minEnqueueTime.IsZero() || len(self.contracts) == 0 {
		return nil
	}
	var newestId Id
	var newestTime time.Time
	for contractId, queued := range self.contracts {
		if newestTime.IsZero() || newestTime.Before(queued.enqueueTime) {
			newestId = contractId
			newestTime = queued.enqueueTime
		}
	}

	expired := []*protocol.Contract{}
	for contractId, queued := range self.contracts {
		if queued.enqueueTime.Before(minEnqueueTime) &&
			(contractId != newestId || !newestTime.Before(minEnqueueTime)) {
			expired = append(expired, queued.contract)
			delete(self.contracts, contractId)
		}
	}
	return expired
}

func (self *contractQueue) expireWithLock(minEnqueueTime time.Time) []*protocol.Contract {
	var expired []*protocol.Contract
	for contractId, queuedContract := range self.contracts {
		if queuedContract.enqueueTime.Before(minEnqueueTime) {
			expired = append(expired, queuedContract.contract)
			delete(self.contracts, contractId)
		}
	}
	return expired
}

func (self *contractQueue) Add(contract *protocol.Contract, storedContract *protocol.StoredContract) error {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if self.drained {
		return errContractQueueDrained
	}

	contractId, err := IdFromBytes(storedContract.ContractId)
	if err != nil {
		return err
	}

	// update contract if present
	if _, ok := self.contracts[contractId]; ok {
		if self.log.V(2).Enabled() {
			self.log.Infof("[contract]add update existing %s\n", contractId)
		}
		self.contracts[contractId] = &queuedContract{
			contract:    contract,
			enqueueTime: time.Now(),
		}
		self.updateMonitor.NotifyAll()
	} else if !self.trackUsedContracts || !self.usedContractIds[contractId] {
		if self.log.V(2).Enabled() {
			self.log.Infof("[contract]add %s\n", contractId)
		}
		if self.trackUsedContracts {
			self.usedContractIds[contractId] = true
		}
		self.contracts[contractId] = &queuedContract{
			contract:    contract,
			enqueueTime: time.Now(),
		}
		self.updateMonitor.NotifyAll()
	} else {
		if self.log.V(2).Enabled() {
			self.log.Infof("[contract]add already used %s\n", contractId)
		}
		// drop this contract. it has already been used locally
	}
	return nil
}

func (self *contractQueue) RemoveUsedContract(contractId Id) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	delete(self.usedContractIds, contractId)
}

func (self *contractQueue) Flush(removeUsedContractIds bool) []*protocol.Contract {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if self.testingBeforeFlushWithLock != nil {
		self.testingBeforeFlushWithLock()
	}

	contracts := []*protocol.Contract{}
	for _, queuedContract := range self.contracts {
		contracts = append(contracts, queuedContract.contract)
	}
	self.contracts = map[Id]*queuedContract{}
	if removeUsedContractIds {
		self.usedContractIds = map[Id]bool{}
	}

	return contracts
}

// flushAndDrainWithLock retires a generation and returns every result that won
// its race with retirement. The manager holds its own lock and this queue's
// lock so no opener can observe the drained generation in the map.
func (self *contractQueue) flushAndDrainWithLock(removeUsedContractIds bool) []*protocol.Contract {
	if self.testingBeforeFlushWithLock != nil {
		self.testingBeforeFlushWithLock()
	}
	contracts := []*protocol.Contract{}
	for _, queuedContract := range self.contracts {
		contracts = append(contracts, queuedContract.contract)
	}
	self.contracts = map[Id]*queuedContract{}
	if removeUsedContractIds {
		self.usedContractIds = map[Id]bool{}
	}
	self.drained = true
	return contracts
}

// Drain marks the queue as no longer accepting new contracts and wakes any
// waiters so they can exit cleanly. Used when the queue is being force-removed
// from the manager while waiters still hold references.
func (self *contractQueue) Drain() {
	func() {
		self.mutex.Lock()
		defer self.mutex.Unlock()
		self.drained = true
	}()
	self.updateMonitor.NotifyAll()
}

func (self *contractQueue) Drained() bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return self.drained
}

func (self *contractQueue) IsDone() bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if 0 < self.openCount {
		return false
	}

	return 0 == len(self.contracts) && 0 == len(self.usedContractIds)
}

func (self *contractQueue) UsedContractIdBytes() [][]byte {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	usedContractIdBytes := [][]byte{}
	for contractId, _ := range self.usedContractIds {
		usedContractIdBytes = append(usedContractIdBytes, contractId.Bytes())
	}
	return usedContractIdBytes
}
