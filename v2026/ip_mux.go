package connect

// IpMux is a composable packet stage between a downstream packet source and an
// upstream UserNat. It conforms to the UserNat send/receive shapes so muxes chain
// transparently:
//
//   - SendPacket is called by the downstream. A concrete mux's onSend hook may
//     claim (and locally terminate) a packet; anything not claimed is forwarded to
//     the wrapped upstream send.
//   - Receive is installed as the upstream's receive callback. Packets addressed to
//     the mux's internal Tun address are delivered into the Tun (they are replies to
//     the Tun's own upstream connections); everything else is dispatched to the
//     registered receivers (toward the downstream / OS TUN).
//   - The internal Tun "sends out of the upstream": packets the userspace stack
//     emits are forwarded to the upstream send.
//
// The internal Tun reserves one address from the shared local-address pool, so a
// mux address never collides with the tunnel address or another mux.

import (
	"context"
	"net/netip"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// IpMuxSend matches the UserNat send signature.
type IpMuxSend = func(source TransferPath, provideMode protocol.ProvideMode, packet []byte, timeout time.Duration) bool

// ipPacketGroupSend conditionally transfers one exact directional flow group.
// Success transfers every packet; failure leaves every packet with the caller.
type ipPacketGroupSend = func(
	source TransferPath,
	provideMode protocol.ProvideMode,
	group *ipPacketGroup,
	timeout time.Duration,
) bool

// ipMuxOnSend lets a concrete mux claim and terminate a send-path packet. It
// returns true if it handled the packet, or false to forward it upstream.
type ipMuxOnSend = func(source TransferPath, provideMode protocol.ProvideMode, packet []byte, timeout time.Duration) bool

// ipMuxOnSendGroup classifies one exact directional flow as a unit. It may
// inspect every member for content state, but returns one disposition for the
// complete group.
type ipMuxOnSendGroup = func(
	source TransferPath,
	provideMode protocol.ProvideMode,
	group *ipPacketGroup,
	timeout time.Duration,
) bool

// ipMuxOnPump lets a concrete mux intercept a packet the internal stack emits
// before it is forwarded upstream — e.g. to un-NAT a locally-terminated connection's
// reply and deliver it downstream. It returns true if it handled the packet, or false
// to forward it upstream as usual.
type ipMuxOnPump = func(packet []byte) bool

type IpMux struct {
	ctx    context.Context
	cancel context.CancelFunc
	log    Logger

	tun            *Tun
	localAddresses []netip.Addr

	onSend      ipMuxOnSend
	onSendGroup ipMuxOnSendGroup
	onPump      ipMuxOnPump

	// source/provideMode/sendTimeout stamp the Tun's upstream-originated packets
	// (the local servers' own upstream connections).
	source      TransferPath
	provideMode protocol.ProvideMode
	sendTimeout time.Duration

	stateLock         sync.Mutex
	upstream          IpMuxSend
	upstreamGroupSend ipPacketGroupSend

	receivers      *CallbackList[ReceivePacketFunction]
	batchReceivers *CallbackList[ReceivePacketsFunction]

	// rejectedPumpPacketCount rate-limits diagnostics for intentional upstream
	// backpressure. The internal stack owns retransmission; the pump owns and
	// returns each rejected pooled packet.
	rejectedPumpPacketCount atomic.Uint64
}

func NewIpMux(
	ctx context.Context,
	tun *Tun,
	source TransferPath,
	provideMode protocol.ProvideMode,
	sendTimeout time.Duration,
	onSend ipMuxOnSend,
	onPump ipMuxOnPump,
	initialReceiver ReceivePacketFunction,
	log Logger,
) *IpMux {
	cancelCtx, cancel := context.WithCancel(ctx)
	receivers := NewCallbackList[ReceivePacketFunction]()
	batchReceivers := NewCallbackList[ReceivePacketsFunction]()
	if initialReceiver != nil {
		receivers.Add(initialReceiver)
	}
	self := &IpMux{
		ctx:            cancelCtx,
		cancel:         cancel,
		log:            loggerOrDefault(log),
		tun:            tun,
		localAddresses: tun.LocalAddresses(),
		onSend:         onSend,
		onPump:         onPump,
		source:         source,
		provideMode:    provideMode,
		sendTimeout:    sendTimeout,
		receivers:      receivers,
		batchReceivers: batchReceivers,
	}
	go HandleError(self.pump)
	return self
}

// SetUpstream wires the wrapped upstream send. It is settable after construction
// because the upstream (e.g. a RemoteUserNatMultiClient) is created with the mux's
// Receive as its callback, so the two have a circular dependency.
func (self *IpMux) SetUpstream(upstream IpMuxSend) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.upstream = upstream
}

// Installs the exact-flow classifier used by the batch path. Construction is
// complete before native traffic can call this method, so the callback itself
// remains immutable while packets are flowing.
func (self *IpMux) setOnSendGroup(onSendGroup ipMuxOnSendGroup) {
	self.onSendGroup = onSendGroup
}

// Wires the homogeneous batch path beside the singular compatibility path.
func (self *IpMux) setUpstreamGroupSend(upstreamGroupSend ipPacketGroupSend) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.upstreamGroupSend = upstreamGroupSend
}

func (self *IpMux) getUpstream() IpMuxSend {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.upstream
}

// Returns the current homogeneous batch path.
func (self *IpMux) getUpstreamGroupSend() ipPacketGroupSend {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.upstreamGroupSend
}

// Returns one internally consistent view of both upstream entry points.
func (self *IpMux) getUpstreams() (IpMuxSend, ipPacketGroupSend) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.upstream, self.upstreamGroupSend
}

// AddReceiver registers a downstream receiver; returns an unregister closure.
func (self *IpMux) AddReceiver(receiver ReceivePacketFunction) func() {
	callbackId := self.receivers.Add(receiver)
	return func() {
		self.receivers.Remove(callbackId)
	}
}

// Batch receivers take the common all-downstream batch exclusively. Singular
// receivers remain installed for synthesized and mixed local/downstream paths.
func (self *IpMux) AddPacketsReceiver(receiver ReceivePacketsFunction) func() {
	callbackId := self.batchReceivers.Add(receiver)
	return func() {
		self.batchReceivers.Remove(callbackId)
	}
}

// Tun exposes the internal userspace stack so a concrete mux can run local
// servers (ListenTCP/ListenUDP), dial upstream (DialContext), and resolve (DohCache).
func (self *IpMux) Tun() *Tun {
	return self.tun
}

// SendPacket is the downstream entry point: a claimed packet is terminated locally
// by the concrete mux; otherwise it is forwarded to the upstream.
func (self *IpMux) SendPacket(source TransferPath, provideMode protocol.ProvideMode, packet []byte, timeout time.Duration) bool {
	if self.onSend != nil && self.onSend(source, provideMode, packet, timeout) {
		MessagePoolReturn(packet)
		return true
	}
	upstream := self.getUpstream()
	if upstream == nil {
		return false
	}
	return upstream(source, provideMode, packet, timeout)
}

// Consumes a packet burst after grouping it by exact directional flow. Each
// group is classified once at the routing boundary while content-aware
// observers still inspect its packets in order.
func (self *IpMux) SendPacketBatch(
	source TransferPath,
	provideMode protocol.ProvideMode,
	packets [][]byte,
	timeout time.Duration,
) int {
	groups, rejectedPackets := groupIpPackets(packets)
	for _, packet := range rejectedPackets {
		MessagePoolReturn(packet)
	}
	sentPacketCount := 0
	upstream, upstreamGroupSend := self.getUpstreams()
	for _, group := range groups {
		claimed := false
		if self.onSendGroup != nil {
			claimed = self.onSendGroup(source, provideMode, group, timeout)
		} else if self.onSend != nil {
			for _, packet := range group.packets {
				if self.onSend(source, provideMode, packet, timeout) {
					claimed = true
				}
			}
		}
		if claimed {
			for _, packet := range group.packets {
				MessagePoolReturn(packet)
			}
			sentPacketCount += len(group.packets)
			continue
		}
		if smtpNeedsOrderedSend(group.ipPath) {
			// SMTP's route/encryption decision advances per TCP segment and can
			// accept an early negotiation segment while rejecting a later one.
			// The exact-flow batch fast path enters RemoteUserNatMultiClient at
			// sendPacketGroup, below its public SMTP gate. Preserve both the
			// state machine and TCP/25's forced-local route by using the public
			// singular upstream for these small, exceptional flows.
			if upstream == nil {
				for _, packet := range group.packets {
					MessagePoolReturn(packet)
				}
				continue
			}
			for _, packet := range group.packets {
				if upstream(source, provideMode, packet, timeout) {
					sentPacketCount += 1
				} else {
					MessagePoolReturn(packet)
				}
			}
			continue
		}
		if upstreamGroupSend != nil {
			if upstreamGroupSend(source, provideMode, group, timeout) {
				sentPacketCount += len(group.packets)
			} else {
				for _, packet := range group.packets {
					MessagePoolReturn(packet)
				}
			}
			continue
		}
		if upstream == nil {
			for _, packet := range group.packets {
				MessagePoolReturn(packet)
			}
			continue
		}
		for _, packet := range group.packets {
			if upstream(source, provideMode, packet, timeout) {
				sentPacketCount += 1
			} else {
				MessagePoolReturn(packet)
			}
		}
	}
	return sentPacketCount
}

// Receive is installed as the upstream's receive callback. The callback IpPath
// is the canonical outbound flow identity even for return packets, so its
// DestinationIp is the remote server—not this mux. Classify the authoritative
// packet bytes instead: packets actually addressed to the mux's Tun enter the
// internal stack; the rest go downstream.
func (self *IpMux) Receive(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
	if self.isLocalPacketDestination(packet) {
		// The mux's userspace Tun is a final local-stack injection boundary,
		// covered by the device-TUN exception in CODESTYLE.md. Returning from
		// this callback is what permits Transfer to acknowledge the packet.
		self.tun.Write(packet)
		return
	}
	self.deliverDownstream(source, provideMode, ipPath, packet)
}

// A homogeneous batch crosses the mux boundary once. Mixed batches retain the
// singular routing behavior because mux-local and downstream packets have
// different consumers and are uncommon outside control traffic.
func (self *IpMux) ReceivePackets(
	source TransferPath,
	provideMode protocol.ProvideMode,
	ipPath *IpPath,
	packets [][]byte,
) {
	if len(packets) == 0 {
		return
	}
	localPacketCount := 0
	for _, packet := range packets {
		if self.isLocalPacketDestination(packet) {
			localPacketCount += 1
		}
	}
	if localPacketCount == len(packets) {
		// Batched form of the synchronous local-stack boundary above.
		_, _ = self.tun.WriteBatch(packets)
		return
	}
	if localPacketCount == 0 && self.deliverDownstreamPackets(
		source,
		provideMode,
		ipPath,
		packets,
	) {
		return
	}
	for _, packet := range packets {
		self.Receive(source, provideMode, ipPath, packet)
	}
}

// deliverDownstream dispatches a packet to the registered receivers. A concrete mux
// also uses this to inject locally-generated replies (e.g. DNS responses) toward
// the downstream.
func (self *IpMux) deliverDownstream(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
	if packet == nil {
		// a synthesizer with no builder for the path (see ipOosPacket)
		return
	}
	for _, receiver := range self.receivers.Get() {
		safeReceive(receiver, source, provideMode, ipPath, packet)
	}
}

// A batch callback borrows every packet and replaces singular delivery. Nil
// packets use the singular path so its existing synthesizer filtering holds.
func (self *IpMux) deliverDownstreamPackets(
	source TransferPath,
	provideMode protocol.ProvideMode,
	ipPath *IpPath,
	packets [][]byte,
) bool {
	for _, packet := range packets {
		if packet == nil {
			return false
		}
	}
	batchReceivers := self.batchReceivers.Get()
	if len(batchReceivers) == 0 {
		return false
	}
	for _, receiver := range batchReceivers {
		safeReceivePackets(receiver, source, provideMode, ipPath, packets)
	}
	return true
}

// safeReceive calls one receiver with the panic isolation that wrapping it in
// HandleError would give, but without allocating a closure. This runs once per
// received packet per receiver (the whole inbound path), so it must not allocate:
// the deferred recover closure captures no variables, so it is a static func value
// (see safeAck).
func safeReceive(receiver ReceivePacketFunction, source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
	defer func() {
		if r := recover(); r != nil {
			if IsDoneError(r) {
				// the context was canceled and raised; standard pattern, do not log
			} else {
				DefaultLogger().Warningf("Unexpected error: %s\n", ErrorJson(r, debug.Stack()))
			}
		}
	}()
	receiver(source, provideMode, ipPath, packet)
}

// Batch callback panics are isolated without adding an allocation to the
// ordinary receive path.
func safeReceivePackets(
	receiver ReceivePacketsFunction,
	source TransferPath,
	provideMode protocol.ProvideMode,
	ipPath *IpPath,
	packets [][]byte,
) {
	defer func() {
		if r := recover(); r != nil {
			if !IsDoneError(r) {
				DefaultLogger().Warningf(
					"Unexpected error: %s\n",
					ErrorJson(r, debug.Stack()),
				)
			}
		}
	}()
	receiver(source, provideMode, ipPath, packets)
}

// isLocalPacketDestination reads only fixed IP-header fields and allocates no
// flow object. Receive is on the entire inbound packet path, so a full parse
// here would tax all ordinary traffic to classify the rare mux-local return.
func (self *IpMux) isLocalPacketDestination(packet []byte) bool {
	_, addr, ok := ipPacketSourceDestinationAddrs(packet)
	if !ok {
		return false
	}
	for _, local := range self.localAddresses {
		if local == addr {
			return true
		}
	}
	return false
}

// ipPacketSourceDestinationAddrs reads only fixed IP-header fields. It is
// shared by return-path consumers that cannot infer wire direction from the
// canonical outbound IpPath passed to ReceivePacketFunction.
func ipPacketSourceDestinationAddrs(packet []byte) (netip.Addr, netip.Addr, bool) {
	if len(packet) == 0 {
		return netip.Addr{}, netip.Addr{}, false
	}
	switch packet[0] >> 4 {
	case 4:
		headerByteCount := int(packet[0]&0x0f) * 4
		if headerByteCount < 20 || len(packet) < headerByteCount {
			return netip.Addr{}, netip.Addr{}, false
		}
		return netip.AddrFrom4([4]byte(packet[12:16])),
			netip.AddrFrom4([4]byte(packet[16:20])),
			true
	case 6:
		if len(packet) < 40 {
			return netip.Addr{}, netip.Addr{}, false
		}
		return netip.AddrFrom16([16]byte(packet[8:24])),
			netip.AddrFrom16([16]byte(packet[24:40])),
			true
	default:
		return netip.Addr{}, netip.Addr{}, false
	}
}

// pump forwards packets the internal stack emits (the local servers' upstream
// connections) out via the upstream send.
//
// NOTE: this is correct while the only stack-originated traffic is upstream-bound
// (DNS resolution in step 3 via the Tun's DohCache/DialContext). Step 4 (HTTP), which
// terminates client TCP on the stack, will also emit client-bound replies here and
// will need to route stack output by destination.
func (self *IpMux) pump() {
	for {
		packet, err := self.tun.Read()
		if err != nil {
			// tun.Read only fails once the tun ctx is canceled (Close); the pump is done
			return
		}
		// isolate per-packet processing: a panic handling one packet (a parse/rewrite edge
		// case, or deep in the upstream send) must not kill the pump. Otherwise the tun's
		// stack-originated traffic (DoH resolution, HTTP upgrade) would stop being forwarded
		// while the tun stays open, and DNS would break until the mux is recreated (restart).
		HandleError(func() {
			if self.onPump != nil && self.onPump(packet) {
				return
			}
			upstream := self.getUpstream()
			if upstream != nil && upstream(self.source, self.provideMode, packet, self.sendTimeout) {
				// A successful send transfers packet ownership upstream.
				return
			}
			// A failed send retains ownership here. This path is normal
			// backpressure—the gVisor endpoint retransmits—but losing the pool
			// return leaked one buffer per rejected SYN/data packet.
			MessagePoolReturn(packet)
			rejectedCount := self.rejectedPumpPacketCount.Add(1)
			if rejectedCount == 1 || rejectedCount&(rejectedCount-1) == 0 {
				self.log.Infof("[mux]internal upstream backpressure rejected %d packets\n", rejectedCount)
			}
		})
	}
}

func (self *IpMux) Close() {
	self.cancel()
	self.tun.Close()
}
