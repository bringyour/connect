package connect

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// icmp echo support for the user-space nat (see ICMP.md). the icmp path is
// echo-only by decision: an echo request creates a per-flow egress transaction
// through an unprivileged platform backend, and the echo reply returns along
// the flow's reverse path with the flow's inner identifier restored. every
// other icmp type fails the parse and drops. the flow is keyed like udp with
// the echo identifier standing in for the client-side port.

const IcmpHeaderSize = 8

// icmp echo types (rfc 792 / rfc 4443)
const (
	icmp4TypeEchoReply   = byte(0)
	icmp4TypeEchoRequest = byte(8)
	icmp6TypeEchoRequest = byte(128)
	icmp6TypeEchoReply   = byte(129)
)

// icmpv6 link-local control types (rfc 4443, rfc 4861, rfc 3810): multicast
// listener query/report/done (130-132), router solicitation/advertisement
// (133-134), neighbor solicitation/advertisement (135-136) and redirect
// (137). None has meaning across a point-to-point tunnel; they are dropped
// silently wherever they appear (see isIcmpv6LinkControlType).
const (
	icmp6TypeMulticastListenerQuery = byte(130)
	icmp6TypeRedirect               = byte(137)
)

// errIcmpv6LinkControl is the parse error for an icmpv6 link-local control
// message, distinct from a malformed packet so a caller can drop it quietly.
var errIcmpv6LinkControl = errors.New("icmpv6 link-local control message is not carried")

// isIcmpv6LinkControlType reports whether an icmpv6 type is link-local control
// chatter (mld, router and neighbor discovery, redirect).
func isIcmpv6LinkControlType(icmpType byte) bool {
	return icmp6TypeMulticastListenerQuery <= icmpType && icmpType <= icmp6TypeRedirect
}

// minimal parsed view of an icmp echo packet on the send path, matching the
// tcp/udp views. all slices alias the backing ip packet.
type parsedIcmp struct {
	sourceIp       net.IP
	destinationIp  net.IP
	echoRequest    bool
	identifier     uint16
	sequenceNumber uint16
	// ip header ttl (v4) or hop limit (v6), forwarded to the egress backend
	ttl     uint8
	payload []byte
}

// parses an icmp transport into `icmp`. echo request and reply with code 0
// are the only supported shapes (see ICMP.md); anything else fails.
func parseIcmpPacket(ipVersion int, sourceIp net.IP, destinationIp net.IP, transport []byte, icmp *parsedIcmp) bool {
	if len(transport) < IcmpHeaderSize {
		return false
	}
	if transport[1] != 0 {
		// echo code is always 0
		return false
	}
	echoRequest := false
	switch {
	case ipVersion == 4 && transport[0] == icmp4TypeEchoRequest,
		ipVersion == 6 && transport[0] == icmp6TypeEchoRequest:
		echoRequest = true
	case ipVersion == 4 && transport[0] == icmp4TypeEchoReply,
		ipVersion == 6 && transport[0] == icmp6TypeEchoReply:
	default:
		return false
	}
	icmp.sourceIp = sourceIp
	icmp.destinationIp = destinationIp
	icmp.echoRequest = echoRequest
	icmp.identifier = binary.BigEndian.Uint16(transport[4:6])
	icmp.sequenceNumber = binary.BigEndian.Uint16(transport[6:8])
	icmp.payload = transport[IcmpHeaderSize:]
	return true
}

// the platform backend for one flow's echo transactions. WriteEcho sends one
// echo request toward the flow destination; ReadEcho blocks until a matching
// echo reply, the deadline, or an error. Implementations own reply matching
// (socket identifier and peer address) and deliver only this flow's replies.
// The returned payload aliases an internal buffer and is valid only until the
// next ReadEcho call. WriteEcho and ReadEcho may be called from one writer and
// one reader goroutine concurrently.
type icmpEgress interface {
	WriteEcho(deadline time.Time, ttl int, sequenceNumber uint16, payload []byte) error
	ReadEcho(deadline time.Time) (sequenceNumber uint16, payload []byte, err error)
	Close()
}

type IcmpBufferSettings struct {
	// nil resolves to the local user nat `Log`
	Log          Logger
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	// an echo flow is chatty or dead, so the idle reap is short
	IdleTimeout         time.Duration
	Mtu                 int
	ReadBufferByteCount int
	SequenceBufferSize  int
	// the number of open flows per source
	// uses an lru cleanup where new flows over the limit close old flows
	UserLimit int
	// the number of open flows across all sources (per address family
	// buffer), an aggregate flow and fd ceiling. 0 is no limit.
	GlobalLimit int
	// transaction backends (windows): the maximum in-flight echo
	// transactions per flow; requests over the cap drop, which ping
	// tolerates
	OutstandingLimit int
}

const defaultIcmpFlowBufferSize = 64

func DefaultIcmpBufferSettings() *IcmpBufferSettings {
	return DefaultIcmpBufferSettingsWithBufferSize(MemoryScaledCount(defaultIcmpFlowBufferSize, 16))
}

func DefaultIcmpBufferSettingsWithBufferSize(bufferSize int) *IcmpBufferSettings {
	globalLimit := 0
	if 0 < MemoryBudget() {
		globalLimit = MemoryScaledCount(512, 64)
	}
	return &IcmpBufferSettings{
		ReadTimeout:  300 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
		Mtu:          DefaultMtu,
		// a reply mirrors a request that fit the device mtu, plus slack for
		// platform framing (the darwin v4 read includes the ip header). the
		// backends allocate one read and one write buffer of this size per
		// flow, so it is the dominant per-flow heap item in the budget model
		// (see providerIcmpFlowByteCount)
		ReadBufferByteCount: DefaultTunnelMtu + 64,
		SequenceBufferSize:  bufferSize,
		UserLimit:           0,
		GlobalLimit:         globalLimit,
		OutstandingLimit:    4,
	}
}

type Icmp4Buffer struct {
	IcmpBuffer[BufferId4]
}

func NewIcmp4Buffer(ctx context.Context, receiveCallback ReceivePacketFunction,
	icmpBufferSettings *IcmpBufferSettings) *Icmp4Buffer {
	return newIcmp4BufferWithTransferKey(
		ctx,
		receiveTransferPacketAdapter(receiveCallback),
		icmpBufferSettings,
	)
}

// Builds the provider form without dropping the transfer lane.
func newIcmp4BufferWithTransferKey(
	ctx context.Context,
	receiveCallback receiveTransferPacketFunction,
	icmpBufferSettings *IcmpBufferSettings,
) *Icmp4Buffer {
	return &Icmp4Buffer{
		IcmpBuffer: *newIcmpBuffer[BufferId4](ctx, receiveCallback, icmpBufferSettings),
	}
}

func (self *Icmp4Buffer) send(source TransferPath, provideMode protocol.ProvideMode,
	icmp *parsedIcmp, timeout time.Duration, ipPacket []byte) (bool, error) {
	return self.sendTransferKey(
		source,
		TransferKey{},
		provideMode,
		icmp,
		timeout,
		ipPacket,
	)
}

// Retains the reply lane separately from the ICMP flow identity.
func (self *Icmp4Buffer) sendTransferKey(
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	icmp *parsedIcmp,
	timeout time.Duration,
	ipPacket []byte,
) (bool, error) {
	source = source.LocalMask()
	bufferId := NewBufferId4(
		source,
		icmp.sourceIp, int(icmp.identifier),
		icmp.destinationIp, 0,
	)

	return self.icmpSend(
		bufferId,
		source,
		transferKey,
		provideMode,
		4,
		icmp,
		timeout,
		ipPacket,
	)
}

type Icmp6Buffer struct {
	IcmpBuffer[BufferId6]
}

func NewIcmp6Buffer(ctx context.Context, receiveCallback ReceivePacketFunction,
	icmpBufferSettings *IcmpBufferSettings) *Icmp6Buffer {
	return newIcmp6BufferWithTransferKey(
		ctx,
		receiveTransferPacketAdapter(receiveCallback),
		icmpBufferSettings,
	)
}

// Builds the IPv6 provider form without dropping the transfer lane.
func newIcmp6BufferWithTransferKey(
	ctx context.Context,
	receiveCallback receiveTransferPacketFunction,
	icmpBufferSettings *IcmpBufferSettings,
) *Icmp6Buffer {
	return &Icmp6Buffer{
		IcmpBuffer: *newIcmpBuffer[BufferId6](ctx, receiveCallback, icmpBufferSettings),
	}
}

func (self *Icmp6Buffer) send(source TransferPath, provideMode protocol.ProvideMode,
	icmp *parsedIcmp, timeout time.Duration, ipPacket []byte) (bool, error) {
	return self.sendTransferKey(
		source,
		TransferKey{},
		provideMode,
		icmp,
		timeout,
		ipPacket,
	)
}

// Retains the reply lane separately from the IPv6 flow identity.
func (self *Icmp6Buffer) sendTransferKey(
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	icmp *parsedIcmp,
	timeout time.Duration,
	ipPacket []byte,
) (bool, error) {
	source = source.LocalMask()
	bufferId := NewBufferId6(
		source,
		icmp.sourceIp, int(icmp.identifier),
		icmp.destinationIp, 0,
	)

	return self.icmpSend(
		bufferId,
		source,
		transferKey,
		provideMode,
		6,
		icmp,
		timeout,
		ipPacket,
	)
}

// the flow table for one address family, mirroring `UdpBuffer`: per-source and
// aggregate lru caps, and an identity-checked cleanup when a sequence exits.
type IcmpBuffer[BufferId comparable] struct {
	ctx                            context.Context
	log                            Logger
	receiveCallback                receiveTransferPacketFunction
	receiveTransferPacketsCallback receiveTransferPacketsBatchFunction
	icmpBufferSettings             *IcmpBufferSettings

	mutex sync.Mutex
	// The local NAT closes send admission before waiting, so no Add can race
	// the terminal Wait.
	sequenceWaitGroup sync.WaitGroup

	sequences        map[BufferId]*IcmpSequence
	sourceSequences  map[TransferPath]map[BufferId]*IcmpSequence
	retiredSourceIds map[Id]bool
}

func newIcmpBuffer[BufferId comparable](
	ctx context.Context,
	receiveCallback receiveTransferPacketFunction,
	icmpBufferSettings *IcmpBufferSettings,
) *IcmpBuffer[BufferId] {
	return &IcmpBuffer[BufferId]{
		ctx:                ctx,
		log:                loggerOrDefault(icmpBufferSettings.Log),
		receiveCallback:    receiveCallback,
		icmpBufferSettings: icmpBufferSettings,
		sequences:          map[BufferId]*IcmpSequence{},
		sourceSequences:    map[TransferPath]map[BufferId]*IcmpSequence{},
		retiredSourceIds:   map[Id]bool{},
	}
}

func (self *IcmpBuffer[BufferId]) icmpSend(
	bufferId BufferId,
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	ipVersion int,
	icmp *parsedIcmp,
	timeout time.Duration,
	ipPacket []byte,
) (bool, error) {
	source = source.LocalMask()
	initSequence := func(skip *IcmpSequence) *IcmpSequence {
		self.mutex.Lock()
		defer self.mutex.Unlock()
		if self.retiredSourceIds[source.SourceId] {
			return nil
		}

		sequence, ok := self.sequences[bufferId]
		if ok {
			if skip == nil || skip != sequence {
				return sequence
			} else {
				sequence.Cancel()
				delete(self.sequences, bufferId)
				sourceSequences := self.sourceSequences[sequence.source]
				delete(sourceSequences, bufferId)
				if 0 == len(sourceSequences) {
					delete(self.sourceSequences, sequence.source)
				}
			}
		}

		if 0 < self.icmpBufferSettings.UserLimit {
			// limit the total flows per source to avoid blowing up the ulimit
			if sourceSequences := self.sourceSequences[source]; self.icmpBufferSettings.UserLimit <= len(sourceSequences) {
				applyLruMapLimit(sourceSequences, self.icmpBufferSettings.UserLimit-1, func(bufferId BufferId, sequence *IcmpSequence) bool {
					if self.log.V(1).Enabled() {
						self.log.Infof(
							"[lnr]icmp limit source %s->%s\n",
							source,
							sequence.destinationIp,
						)
					}
					self.removeSequenceWithLock(bufferId, sequence)
					return true
				})
			}
		}
		if 0 < self.icmpBufferSettings.GlobalLimit {
			// limit the total flows across all sources, an aggregate flow
			// and fd ceiling
			if self.icmpBufferSettings.GlobalLimit <= len(self.sequences) {
				applyLruMapLimit(self.sequences, self.icmpBufferSettings.GlobalLimit-1, func(bufferId BufferId, sequence *IcmpSequence) bool {
					if self.log.V(1).Enabled() {
						self.log.Infof(
							"[lnr]icmp limit global %s->%s\n",
							sequence.source,
							sequence.destinationIp,
						)
					}
					self.removeSequenceWithLock(bufferId, sequence)
					return true
				})
			}
		}

		sourceIpCopy := make(net.IP, len(icmp.sourceIp))
		copy(sourceIpCopy, icmp.sourceIp)

		destinationIpCopy := make(net.IP, len(icmp.destinationIp))
		copy(destinationIpCopy, icmp.destinationIp)

		sequence = newIcmpSequenceWithTransferKey(
			self.ctx,
			self.receiveCallback,
			source,
			transferKey,
			provideMode,
			ipVersion,
			sourceIpCopy,
			icmp.identifier,
			destinationIpCopy,
			self.icmpBufferSettings,
		)
		sequence.receiveTransferPacketsCallback = self.receiveTransferPacketsCallback
		self.sequences[bufferId] = sequence
		sourceSequences := self.sourceSequences[source]
		if sourceSequences == nil {
			sourceSequences = map[BufferId]*IcmpSequence{}
			self.sourceSequences[source] = sourceSequences
		}
		sourceSequences[bufferId] = sequence
		self.sequenceWaitGroup.Add(1)
		go HandleError(func() {
			defer self.sequenceWaitGroup.Done()
			defer close(sequence.retirementDone)
			defer func() {
				self.mutex.Lock()
				defer self.mutex.Unlock()
				sequence.Close()
				// clean up
				if sequence == self.sequences[bufferId] {
					delete(self.sequences, bufferId)
					sourceSequences := self.sourceSequences[sequence.source]
					delete(sourceSequences, bufferId)
					if 0 == len(sourceSequences) {
						delete(self.sourceSequences, sequence.source)
					}
				}
			}()
			sequence.Run()
		})
		return sequence
	}

	sendItem := &IcmpSendItem{
		source:      source,
		transferKey: transferKey,
		provideMode: provideMode,
		icmp:        *icmp,
		ipPacket:    ipPacket,
	}
	sequence := initSequence(nil)
	if sequence == nil {
		return false, nil
	}
	if success, err := sequence.send(sendItem, timeout); err == nil {
		return success, nil
	} else {
		// sequence closed
		sequence = initSequence(sequence)
		if sequence == nil {
			return false, nil
		}
		return sequence.send(sendItem, timeout)
	}
}

// Completion means every admitted echo sequence and its socket reader has
// released all packet ownership. The caller has already stopped dispatch.
func (self *IcmpBuffer[BufferId]) waitForLifecycle() {
	self.sequenceWaitGroup.Wait()
}

// removeSequenceWithLock removes a sequence from both indexes before canceling
// it (see the udp counterpart). The caller holds mutex.
func (self *IcmpBuffer[BufferId]) removeSequenceWithLock(bufferId BufferId, sequence *IcmpSequence) {
	if self.sequences[bufferId] != sequence {
		return
	}
	delete(self.sequences, bufferId)
	sourceSequences := self.sourceSequences[sequence.source]
	delete(sourceSequences, bufferId)
	if len(sourceSequences) == 0 {
		delete(self.sourceSequences, sequence.source)
	}
	sequence.Cancel()
}

// Applies one exact provider-source tombstone and cancels matching echo-flow
// ownership without disturbing sibling sources.
func (self *IcmpBuffer[BufferId]) setSourceRetired(
	sourceId Id,
	retired bool,
) (doneChannels []<-chan struct{}) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if !retired {
		delete(self.retiredSourceIds, sourceId)
		return nil
	}
	self.retiredSourceIds[sourceId] = true
	for source, sourceSequences := range self.sourceSequences {
		if source.SourceId != sourceId {
			continue
		}
		for bufferId, sequence := range sourceSequences {
			doneChannels = append(doneChannels, sequence.retirementDone)
			self.removeSequenceWithLock(bufferId, sequence)
		}
	}
	return doneChannels
}

type IcmpSendItem struct {
	source      TransferPath
	transferKey TransferKey
	provideMode protocol.ProvideMode
	icmp        parsedIcmp
	ipPacket    []byte
}

// one echo flow: a bounded send queue feeding the platform backend, and a
// read goroutine returning matched replies toward the source with the inner
// identifier restored. transfer to this sequence is lossless and in order;
// the backend is datagram best-effort like the network itself.
type IcmpSequence struct {
	ctx                            context.Context
	cancel                         context.CancelFunc
	log                            Logger
	receiveCallback                receiveTransferPacketFunction
	receiveTransferPacketsCallback receiveTransferPacketsBatchFunction
	icmpBufferSettings             *IcmpBufferSettings
	// Closed by the owning buffer after Run joins its echo backend and drains
	// queued packet ownership.
	retirementDone chan struct{}

	sendMutex sync.Mutex
	sendItems chan *IcmpSendItem
	// Lazily allocated only when the bounded send queue actually blocks.
	// sendMutex serializes Reset/Stop.
	sendTimer *time.Timer

	idleCondition *IdleCondition

	source        TransferPath
	transferState transferState
	provideMode   protocol.ProvideMode
	ipVersion     int
	sourceIp      net.IP
	identifier    uint16
	destinationIp net.IP

	// cached immutable ip path for this flow, written once before the
	// per-packet goroutines start
	ipPath *IpPath
	// reusable backing for single-packet receive delivery
	singlePacket [1][]byte

	userLimited
}

func NewIcmpSequence(ctx context.Context, receiveCallback ReceivePacketFunction,
	source TransferPath,
	provideMode protocol.ProvideMode,
	ipVersion int,
	sourceIp net.IP,
	identifier uint16,
	destinationIp net.IP,
	icmpBufferSettings *IcmpBufferSettings) *IcmpSequence {
	return newIcmpSequenceWithTransferKey(
		ctx,
		receiveTransferPacketAdapter(receiveCallback),
		source,
		TransferKey{},
		provideMode,
		ipVersion,
		sourceIp,
		identifier,
		destinationIp,
		icmpBufferSettings,
	)
}

// Builds the provider form without dropping the transfer lane.
func newIcmpSequenceWithTransferKey(
	ctx context.Context,
	receiveCallback receiveTransferPacketFunction,
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	ipVersion int,
	sourceIp net.IP,
	identifier uint16,
	destinationIp net.IP,
	icmpBufferSettings *IcmpBufferSettings,
) *IcmpSequence {
	source = source.LocalMask()
	cancelCtx, cancel := context.WithCancel(ctx)
	return &IcmpSequence{
		ctx:                cancelCtx,
		cancel:             cancel,
		log:                loggerOrDefault(icmpBufferSettings.Log),
		receiveCallback:    receiveCallback,
		icmpBufferSettings: icmpBufferSettings,
		retirementDone:     make(chan struct{}),
		sendItems:          make(chan *IcmpSendItem, icmpBufferSettings.SequenceBufferSize),
		idleCondition:      NewIdleCondition(),
		source:             source,
		transferState:      newTransferState(source, transferKey),
		provideMode:        provideMode,
		ipVersion:          ipVersion,
		sourceIp:           sourceIp,
		identifier:         identifier,
		destinationIp:      destinationIp,
		userLimited: userLimited{
			lastActivityTime: time.Now(),
		},
	}
}

// the immutable ip path for this flow. the identifier stands in for the
// client-side (source) port; the destination side has no port.
func (self *IcmpSequence) IpPath() *IpPath {
	if self.ipPath == nil {
		self.ipPath = &IpPath{
			Version:       self.ipVersion,
			Protocol:      IpProtocolIcmp,
			SourceIp:      self.sourceIp,
			SourcePort:    int(self.identifier),
			DestinationIp: self.destinationIp,
		}
	}
	return self.ipPath
}

func (self *IcmpSequence) send(sendItem *IcmpSendItem, timeout time.Duration) (bool, error) {
	self.sendMutex.Lock()
	defer self.sendMutex.Unlock()

	select {
	case <-self.ctx.Done():
		return false, errors.New("Done.")
	default:
	}

	if !self.idleCondition.UpdateOpen() {
		return false, nil
	}
	defer self.idleCondition.UpdateClose()
	self.transferState.update(sendItem.source, sendItem.transferKey)

	select {
	case <-self.ctx.Done():
		return false, errors.New("Done.")
	default:
	}

	// fast path without arming a timer
	select {
	case self.sendItems <- sendItem:
		return true, nil
	default:
	}

	if timeout < 0 {
		select {
		case <-self.ctx.Done():
			return false, errors.New("Done.")
		case self.sendItems <- sendItem:
			return true, nil
		}
	} else if timeout == 0 {
		select {
		case <-self.ctx.Done():
			return false, errors.New("Done.")
		case self.sendItems <- sendItem:
			return true, nil
		default:
			return false, nil
		}
	} else {
		timeoutChan := resetOrCreateTimer(&self.sendTimer, timeout)
		select {
		case <-self.ctx.Done():
			self.sendTimer.Stop()
			return false, errors.New("Done.")
		case self.sendItems <- sendItem:
			self.sendTimer.Stop()
			return true, nil
		case <-timeoutChan:
			return false, nil
		}
	}
}

// delivers one reply packet, coalescing through the batch consumer when one
// is registered (the provider return path), else per packet
func (self *IcmpSequence) receivePacket(packet []byte) {
	self.singlePacket[0] = packet
	source, transferKey := self.transferState.get()
	if self.receiveTransferPacketsCallback != nil &&
		self.receiveTransferPacketsCallback(
			source,
			transferKey,
			self.provideMode,
			receiveRecoveryModeNonblocking,
			self.IpPath(),
			self.singlePacket[:],
		) {
		MessagePoolReturn(packet)
		return
	}
	self.receiveCallback(
		source,
		transferKey,
		self.provideMode,
		receiveRecoveryModeNonblocking,
		self.IpPath(),
		packet,
	)
	MessagePoolReturn(packet)
}

func (self *IcmpSequence) Run() {
	var childWorkers sync.WaitGroup
	defer childWorkers.Wait()
	defer func() {
		self.cancel()

		func() {
			self.sendMutex.Lock()
			defer self.sendMutex.Unlock()
			close(self.sendItems)
		}()

		// drain the channel
		func() {
			for {
				select {
				case sendItem, ok := <-self.sendItems:
					if !ok {
						return
					}
					MessagePoolReturn(sendItem.ipPacket)
				default:
					return
				}
			}
		}()
	}()

	self.log.V(2).Infof("[init]icmp connect\n")
	egress, err := newIcmpEgress(self.ctx, self.ipVersion, self.destinationIp, self.icmpBufferSettings)
	if err != nil {
		// fail soft: the platform cannot open an unprivileged echo socket,
		// so the flow drops exactly as before icmp support
		if self.log.V(1).Enabled() {
			self.log.Infof("[init]icmp connect error = %s\n", err)
		}
		return
	}
	defer egress.Close()
	self.UpdateLastActivityTime()
	self.log.V(2).Infof("[init]icmp connect success\n")

	childWorkers.Add(1)
	go HandleError(func() {
		defer childWorkers.Done()
		defer self.cancel()

		for replyIter := uint64(0); ; replyIter += 1 {
			select {
			case <-self.ctx.Done():
				return
			default:
			}

			sequenceNumber, payload, err := egress.ReadEcho(time.Now().Add(self.icmpBufferSettings.ReadTimeout))
			if err != nil {
				if self.log.V(1).Enabled() {
					self.log.Infof("[r%d]icmp receive err = %s\n", replyIter, err)
				}
				return
			}
			self.UpdateLastActivityTime()

			packet := self.echoReplyPacket(sequenceNumber, payload)
			if packet == nil {
				// oversized for the device mtu, drop
				continue
			}
			if self.log.V(1).Enabled() {
				self.log.Infof("[r%d]icmp receive %d\n", replyIter, len(packet))
			}
			self.receivePacket(packet)
		}
	}, self.cancel)

	// reusable idle timer: a per-iteration time.After would allocate a timer
	// per packet and retain it for the whole idle timeout (hot-path timer
	// reuse per CODESTYLE)
	idleTimer := time.NewTimer(0)
	defer idleTimer.Stop()

	for sendIter := uint64(0); ; {
		checkpointId := self.idleCondition.Checkpoint()
		idleTimer.Reset(self.icmpBufferSettings.IdleTimeout)
		select {
		case <-self.ctx.Done():
			return
		case sendItem, ok := <-self.sendItems:
			if !ok {
				return
			}
			err := egress.WriteEcho(
				time.Now().Add(self.icmpBufferSettings.WriteTimeout),
				int(sendItem.icmp.ttl),
				sendItem.icmp.sequenceNumber,
				sendItem.icmp.payload,
			)
			if err == nil {
				self.UpdateLastActivityTime()
				if self.log.V(2).Enabled() {
					self.log.Infof("[f%d]icmp forward %d\n", sendIter, len(sendItem.icmp.payload))
				}
			} else if self.log.V(1).Enabled() {
				self.log.Infof("[f%d]icmp forward error = %s\n", sendIter, err)
			}
			MessagePoolReturn(sendItem.ipPacket)
			sendIter += 1
			if err != nil {
				return
			}
		case <-idleTimer.C:
			done := false
			func() {
				self.sendMutex.Lock()
				defer self.sendMutex.Unlock()
				if self.idleCondition.Close(checkpointId) {
					// close the sequence
					done = true
				}
			}()
			if done {
				// close the sequence
				return
			}
			// else there are pending updates
		}
	}
}

// builds the echo reply toward the flow source with the inner identifier
// restored, in a single pool buffer. returns nil when the reply would exceed
// the device mtu.
func (self *IcmpSequence) echoReplyPacket(sequenceNumber uint16, payload []byte) []byte {
	var ipHeaderByteCount int
	var icmpType byte
	var ipProtocol ipProtocolNumber
	switch self.ipVersion {
	case 4:
		ipHeaderByteCount = Ipv4HeaderSizeWithoutExtensions
		icmpType = icmp4TypeEchoReply
		ipProtocol = ipProtocolNumberIcmp4
	default:
		ipHeaderByteCount = Ipv6HeaderSize
		icmpType = icmp6TypeEchoReply
		ipProtocol = ipProtocolNumberIcmp6
	}
	packetByteCount := ipHeaderByteCount + IcmpHeaderSize + len(payload)
	if self.icmpBufferSettings.Mtu < packetByteCount {
		return nil
	}

	packet := MessagePoolGet(packetByteCount)
	switch self.ipVersion {
	case 4:
		writeIpv4Header(packet, ipProtocol, self.destinationIp, self.sourceIp)
	default:
		writeIpv6Header(packet, ipProtocol, self.destinationIp, self.sourceIp)
	}

	icmp := packet[ipHeaderByteCount:]
	icmp[0] = icmpType
	icmp[1] = 0
	// checksum, set below
	icmp[2] = 0
	icmp[3] = 0
	binary.BigEndian.PutUint16(icmp[4:6], self.identifier)
	binary.BigEndian.PutUint16(icmp[6:8], sequenceNumber)
	copy(icmp[IcmpHeaderSize:], payload)
	var checksum uint16
	switch self.ipVersion {
	case 4:
		// icmpv4 has no pseudo header (rfc 792)
		checksum = checksumFinish(checksumAdd(0, icmp))
	default:
		// icmpv6 uses the pseudo header (rfc 4443)
		checksum = transportChecksum(ipProtocol, self.destinationIp, self.sourceIp, icmp)
	}
	binary.BigEndian.PutUint16(icmp[2:4], checksum)
	return packet
}

func (self *IcmpSequence) Cancel() {
	self.cancel()
}

func (self *IcmpSequence) Close() {
	self.cancel()
}

// icmpEgressUnsupportedError is the shared fail-soft error for platforms with
// no echo backend (see the build-tagged newIcmpEgress implementations)
func icmpEgressUnsupportedError(ipVersion int) error {
	return fmt.Errorf("No icmp egress support for ip version %d on this platform.", ipVersion)
}
