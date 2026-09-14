package connect

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	mathrand "math/rand"
	"net"
	// "runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	// "google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/v2026/protocol"
)

// implements user-space NAT (UNAT) and packet inspection
// The UNAT emulates a raw socket using user-space sockets.

// use 0 for deadlock testing
const defaultIpBufferSize = 1024

// packets are written directly into the receiver tap/tun interface,
// so the packet size must not exceed the device interface MTU.
// this is a contract with the devices and must not be raised.
// DefaultMtu is the largest packet the provider-side packetizer builds, for
// both families. At 1,100 bytes, a full steady-state encrypted IP Transfer
// frame uses H3's reliable stream at QUIC's safe 1,200-byte packet floor and
// fits one DATAGRAM on the optimistic discovered path; smaller complete
// messages that fit one DATAGRAM retain the unordered packet lane. IPv6 does
// not require packets to be 1,280 bytes, only that the link carry them, so
// the packetizer keeps this size for v6 as well and the link requirement
// lives in DefaultTunnelMtu.
const DefaultMtu = 1100

// DefaultTunnelMtu is the interface mtu every native tunnel and the gVisor tun
// configure. It is 1,280 because Linux, Darwin and gVisor refuse IPv6 on a
// link below RFC 8200's minimum, and it stays a separate constant from
// DefaultMtu because raising the packet size would take a full return packet
// off H3's single-DATAGRAM lane on every real path (see the H3 mtu sizing
// test). Packets the client originates may be as large as this; the transfer
// batch bound admits an oversized first frame on its own, and the provider
// side accepts any size the tun can carry. Must never be below DefaultMtu.
const DefaultTunnelMtu = 1280
const Ipv4HeaderSizeWithoutExtensions = 20
const Ipv6HeaderSize = 40
const UdpHeaderSize = 8
const TcpHeaderSizeWithoutExtensions = 20

const debugVerifyHeaders = false

// send from a raw socket
// note `ipProtocol` is not supplied. The implementation must do a packet inspection to determine protocol
// `provideMode` is the relationship between the source and this device
type SendPacketFunction func(provideMode protocol.ProvideMode, packet []byte, timeout time.Duration) bool

// receive into a raw socket. ipPath is the canonical outbound flow identity
// (client→server), while packet retains its actual wire direction
// (server→client on a normal return). A callee that needs the packet's source
// or destination must inspect packet rather than infer its direction from
// ipPath. Both are read-only for the duration of the callback. A callee that
// retains packet must call MessagePoolShareReadOnly; a callee that needs a
// mutable path must clone it. The callback runs inline and must not block,
// except when it is the documented final device-TUN injection boundary.
type ReceivePacketFunction func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte)

// receive a batch of packets from one flow (same source, provideMode, ipPath)
// in a single call, so a consumer can coalesce them (see the provider return
// path, which packs the batch into one wire Pack). The packet buffers are
// read-only and valid only for the duration of the callback. A callee that
// retains one must call MessagePoolShareReadOnly before returning. It has the
// same inline/nonblocking contract and final device-TUN exception as
// ReceivePacketFunction.
type ReceivePacketsFunction func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packets [][]byte)

// Identifies the provider return's recovery promise after failed admission. It
// is deliberately independent of the wire protocol: socket data stays retained,
// internal controls can be regenerated, and arbitrary public callbacks make no
// recovery promise.
type receiveRecoveryMode int

const (
	receiveRecoveryModeNonblocking receiveRecoveryMode = iota
	receiveRecoveryModeTcpSocket
	// A synthesized ACK, reset, or unreachable response is safe to refuse
	// without blocking its producer: the peer's retry elicits another response.
	// Keep that promise distinct from arbitrary public callbacks.
	receiveRecoveryModeRegenerableControl
)

// Internal provider delivery retains the receiver-visible transfer identity
// and recovery ownership while the public callback stays source-compatible.
type receiveTransferPacketFunction func(
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	recoveryMode receiveRecoveryMode,
	ipPath *IpPath,
	packet []byte,
)

// Internal provider batch callbacks receive one flow's borrowed packet list
// and the recovery ownership shared by every packet in it.
type receiveTransferPacketsFunction func(
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	recoveryMode receiveRecoveryMode,
	ipPath *IpPath,
	packets [][]byte,
)

// Internal batch delivery reports whether a keyed consumer borrowed the list.
type receiveTransferPacketsBatchFunction func(
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	recoveryMode receiveRecoveryMode,
	ipPath *IpPath,
	packets [][]byte,
) (batched bool)

// Internal TCP lifecycle delivery carries the authenticated source and the
// canonical outbound tuple. Both are borrowed for the call.
type tcpFlowCloseFunction func(source TransferPath, ipPath *IpPath)

// sourceRetirementFunction applies one owner-refcounted provider rejection to
// every protocol buffer in a local NAT shard.
type sourceRetirementFunction func(sourceId Id, retired bool) []<-chan struct{}

// A flow's authenticated source is immutable while its receiver-visible reply
// lane may change. Keeping both under one lock prevents a callback from
// observing a source with another update's lane.
type transferState struct {
	stateLock   sync.RWMutex
	source      TransferPath
	transferKey TransferKey
}

// Initializes one flow's authenticated source and current reply lane.
func newTransferState(source TransferPath, transferKey TransferKey) transferState {
	return transferState{
		source:      source.LocalMask(),
		transferKey: transferKey,
	}
}

// Updates a reply lane only when it belongs to this flow's source.
func (self *transferState) update(source TransferPath, transferKey TransferKey) {
	source = source.LocalMask()
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if self.source != (TransferPath{}) && source != self.source {
		return
	}
	self.source = source
	self.transferKey = transferKey
}

// Returns one stable source/lane pair for a callback invocation.
func (self *transferState) get() (TransferPath, TransferKey) {
	self.stateLock.RLock()
	defer self.stateLock.RUnlock()
	return self.source, self.transferKey
}

type UserNatClient interface {
	// `SendPacketFunction`
	SendPacket(source TransferPath, provideMode protocol.ProvideMode, packet []byte, timeout time.Duration) bool
	Close()
	Shuffle()

	SecurityPolicyStats(reset bool) SecurityPolicyStats

	// allow traffic that fails the security policy of the peers to stay local
	SetLocalSecurityBypass(localSecurityBypass bool)
}

// UserNatBatchClient optionally admits an owned packet list. The method
// consumes every packet and returns the number accepted for delivery. The
// outer slice is borrowed only for the duration of the call; implementations
// that queue a batch asynchronously must copy that slice metadata.
type UserNatBatchClient interface {
	UserNatClient
	SendPacketBatch(
		source TransferPath,
		provideMode protocol.ProvideMode,
		packets [][]byte,
		timeout time.Duration,
	) int
}

// ipPacketFlowKey is one exact directional IP five-tuple. IpVersion keeps an
// IPv4 address distinct from the same bytes represented in IPv6.
type ipPacketFlowKey struct {
	sourceIp        [16]byte
	destinationIp   [16]byte
	protocol        IpProtocol
	sourcePort      uint16
	destinationPort uint16
	ipVersion       uint8
}

// ipPacketFlowKeyFromPath returns the exact directional tuple without
// retaining any of the packet-backed IP slices in ipPath.
func ipPacketFlowKeyFromPath(ipPath *IpPath) (ipPacketFlowKey, bool) {
	if ipPath == nil ||
		ipPath.SourcePort < 0 || 65535 < ipPath.SourcePort ||
		ipPath.DestinationPort < 0 || 65535 < ipPath.DestinationPort {
		return ipPacketFlowKey{}, false
	}

	var sourceIp net.IP
	var destinationIp net.IP
	switch ipPath.Version {
	case 4:
		sourceIp = ipPath.SourceIp.To4()
		destinationIp = ipPath.DestinationIp.To4()
	case 6:
		sourceIp = ipPath.SourceIp.To16()
		destinationIp = ipPath.DestinationIp.To16()
	default:
		return ipPacketFlowKey{}, false
	}
	if sourceIp == nil || destinationIp == nil {
		return ipPacketFlowKey{}, false
	}

	key := ipPacketFlowKey{
		protocol:        ipPath.Protocol,
		sourcePort:      uint16(ipPath.SourcePort),
		destinationPort: uint16(ipPath.DestinationPort),
		ipVersion:       uint8(ipPath.Version),
	}
	copy(key.sourceIp[:], sourceIp)
	copy(key.destinationIp[:], destinationIp)
	return key, true
}

// ipPacketGroup owns its packet-slice header and a canonical path whose
// addresses do not alias packet storage. The packet buffers remain with the
// caller until that caller transfers or returns them.
type ipPacketGroup struct {
	ipPath    *IpPath
	packets   [][]byte
	ipPaths   []IpPath
	payloads  [][]byte
	byteCount ByteCount
}

// Returns a comparable flow key and a path with owned canonical addresses.
func ownIpPacketFlow(ipPath *IpPath) (ipPacketFlowKey, *IpPath, bool) {
	key, ok := ipPacketFlowKeyFromPath(ipPath)
	if !ok {
		return ipPacketFlowKey{}, nil, false
	}

	var ipByteCount int
	switch ipPath.Version {
	case 4:
		ipByteCount = net.IPv4len
	case 6:
		ipByteCount = net.IPv6len
	}
	sourceIp := ipPath.SourceIp[len(ipPath.SourceIp)-ipByteCount:]
	destinationIp := ipPath.DestinationIp[len(ipPath.DestinationIp)-ipByteCount:]

	ownedIpPath := *ipPath
	ipBacking := make(net.IP, 2*ipByteCount)
	copy(ipBacking[:ipByteCount], sourceIp)
	copy(ipBacking[ipByteCount:], destinationIp)
	ownedIpPath.SourceIp = ipBacking[:ipByteCount:ipByteCount]
	ownedIpPath.DestinationIp = ipBacking[ipByteCount:]
	return key, &ownedIpPath, true
}

// Adds a parsed packet while retaining the first-seen group and in-flow
// order. The packet remains owned by the caller.
func appendIpPacketGroup(
	groups *[]*ipPacketGroup,
	groupsByKey map[ipPacketFlowKey]*ipPacketGroup,
	ipPath *IpPath,
	payload []byte,
	packet []byte,
) bool {
	return appendIpPacketGroupBounded(
		groups,
		groupsByKey,
		ipPath,
		payload,
		packet,
		0,
		0,
	)
}

func appendIpPacketGroupBounded(
	groups *[]*ipPacketGroup,
	groupsByKey map[ipPacketFlowKey]*ipPacketGroup,
	ipPath *IpPath,
	payload []byte,
	packet []byte,
	maxPacketCount int,
	maxByteCount ByteCount,
) bool {
	key, ownedIpPath, ok := ownIpPacketFlow(ipPath)
	if !ok {
		return false
	}
	group := groupsByKey[key]
	if group != nil &&
		((0 < maxPacketCount && maxPacketCount <= len(group.packets)) ||
			(0 < maxByteCount && 0 < group.byteCount &&
				maxByteCount < group.byteCount+ByteCount(len(packet)))) {
		group = nil
	}
	if group == nil {
		group = &ipPacketGroup{
			ipPath:   ownedIpPath,
			packets:  [][]byte{packet},
			ipPaths:  []IpPath{*ipPath},
			payloads: [][]byte{payload},
		}
		groupsByKey[key] = group
		*groups = append(*groups, group)
	} else {
		group.packets = append(group.packets, packet)
		group.ipPaths = append(group.ipPaths, *ipPath)
		group.payloads = append(group.payloads, payload)
	}
	group.byteCount += ByteCount(len(packet))
	return true
}

// Groups parseable packets by exact directional five-tuple. Groups retain
// first-seen order, packets retain in-flow order, and rejected packets remain
// caller-owned.
func groupIpPackets(packets [][]byte) (groups []*ipPacketGroup, rejected [][]byte) {
	return groupIpPacketsBounded(packets, 0, 0)
}

func groupIpPacketsBounded(
	packets [][]byte,
	maxPacketCount int,
	maxByteCount ByteCount,
) (groups []*ipPacketGroup, rejected [][]byte) {
	groupsByKey := map[ipPacketFlowKey]*ipPacketGroup{}
	for _, packet := range packets {
		var ipPath IpPath
		payload, err := parseIpPathWithPayloadBorrowed(packet, &ipPath)
		if err != nil ||
			!appendIpPacketGroupBounded(
				&groups,
				groupsByKey,
				&ipPath,
				payload,
				packet,
				maxPacketCount,
				maxByteCount,
			) {
			rejected = append(rejected, packet)
		}
	}
	return
}

// The per-flow channel depths dominate the nat's preallocated flow memory
// (three channels per flow at ~8-88 bytes per slot of backing array), so the
// defaults are scaled by the memory budget. UDP tolerates drops, so its queues
// are short. TCP keeps a bounded streaming queue independently of its maximum
// advertised window: making a 16 MiB high-BDP ceiling preallocate one entire
// window in every live flow would consume hundreds of MiB under ordinary
// provider fanout. A full queue applies backpressure and, at the nonblocking
// provider ingress boundary, ordinary inner TCP loss recovery.
const defaultUdpFlowBufferSize = 256
const defaultTcpFlowBufferSize = 1024

func DefaultUdpBufferSettings() *UdpBufferSettings {
	return DefaultUdpBufferSettingsWithBufferSize(MemoryScaledCount(defaultUdpFlowBufferSize, 32))
}

func DefaultUdpBufferSettingsWithBufferSize(bufferSize int) *UdpBufferSettings {
	globalLimit := 0
	if 0 < MemoryBudget() {
		globalLimit = MemoryScaledCount(2048, 256)
	}
	return &UdpBufferSettings{
		ReadTimeout:  300 * time.Second,
		WriteTimeout: 15 * time.Second,
		// short idle reap, standard for udp nats: single round trip flows
		// (dns, quic probes) dominate udp flow counts, and each idle flow
		// pins its channels, read buffer, and goroutines until reaped
		IdleTimeout: 60 * time.Second,
		Mtu:         DefaultMtu,
		// QUIC Initial datagrams are at least 1,200 bytes. The IP packet path
		// fragments them at DefaultMtu, but the provider-facing UDP socket must
		// first receive the complete datagram. 2 KiB covers ordinary QUIC and
		// EDNS responses without a 64 KiB buffer in every UDP flow.
		ReadBufferByteCount: defaultUdpReadBufferByteCount,
		SequenceBufferSize:  bufferSize,
		WriteBatchSize:      64,
		// Flows are assigned round-robin to a bounded number of ordered
		// receive workers. Workers start lazily, so one active flow still uses
		// one receive worker; five or five thousand flows use at most four.
		ReceiveShardCount: 4,
		UserLimit:         0,
		// A process with an explicit memory budget is a constrained device and
		// gets a scaled hard cap. Unbudgeted server/provider callers are not
		// silently assigned the phone cap; provider entry points select their
		// explicit profile below.
		GlobalLimit:     globalLimit,
		MaxWindowSize:   uint32(MemoryScaledByteCount(mib(1), kib(256))),
		ConnectSettings: *DefaultConnectSettings(),
	}
}

func DefaultTcpBufferSettings() *TcpBufferSettings {
	return DefaultTcpBufferSettingsWithBufferSize(MemoryScaledCount(defaultTcpFlowBufferSize, 192))
}

func DefaultTcpBufferSettingsWithBufferSize(bufferSize int) *TcpBufferSettings {
	minWindowSize := uint32(kib(64))
	initialWindowSize := scaledPow2WindowSize(
		uint32(mib(1)),
		minWindowSize,
		uint32(kib(128)),
	)
	globalLimit := 0
	if 0 < MemoryBudget() {
		globalLimit = MemoryScaledCount(512, 64)
	}
	tcpBufferSettings := &TcpBufferSettings{
		// ConnectTimeout:     60 * time.Second,
		ReadTimeout: 300 * time.Second,
		// WriteTimeout bounds ZERO-progress time on the upstream socket (the
		// write pipeline re-arms it on partial progress). A closed window on
		// the tunnel side legitimately parks the whole pipeline — the device's
		// acks can starve for tens of seconds behind a saturated tunnel — and
		// a kill here tears down an alive flow with an RST. A dead upstream
		// peer normally surfaces as RST/EPIPE rather than eternal silence, and
		// the idle reaper (IdleTimeout/ReadTimeout) still bounds a truly
		// wedged flow, so patience is cheap.
		WriteTimeout:       60 * time.Second,
		AckCompressTimeout: 50 * time.Millisecond,
		IdleTimeout:        300 * time.Second,
		SequenceBufferSize: bufferSize,
		Mtu:                DefaultMtu,
		// large socket reads are split into mtu-sized data packets by `DataPackets`
		ReadBufferByteCount: int(MemoryScaledByteCount(kib(64), kib(16))),
		WriteBatchSize:      64,
		MinWindowSize:       minWindowSize,
		InitialWindowSize:   initialWindowSize,
		MaxWindowSize: scaledPow2WindowSize(
			uint32(mib(16)),
			minWindowSize,
			uint32(kib(256)),
		),
		UserLimit: 0,
		// See the UDP profile above. In particular, an unbudgeted provider
		// must not inherit a 512-flow phone cap and reset established users.
		GlobalLimit:        globalLimit,
		EnableOrphanRst:    true,
		OrphanRstPerSecond: 256,
		// on by default: the benchmark range is reserved (RFC 2544) and the
		// synthetic server only ever answers flows explicitly addressed to it
		EnableSyntheticSpeed: true,
		ConnectSettings:      *DefaultConnectSettings(),
	}
	return tcpBufferSettings
}

// scaledPow2WindowSize scales `maxWindowSize` by the memory budget with a
// floor of `floorWindowSize`, rounded down to a power of 2 multiple of
// `minWindowSize` to preserve the `MaxWindowSize` contract (the window
// doubling ladder must land exactly on the max)
// configureUpstreamTcpConn prepares a connected upstream socket for proxying.
//
// The receive buffer is deliberately left to the kernel. The socket is already
// connected here, so its window clamp was fixed at SYN time from the default
// buffer (about 64 KB). On Linux an explicit SO_RCVBUF locks receive
// autotuning, which is the only thing that raises that clamp, so the window
// advertised to the origin stays at 64 KB for the life of the flow and caps a
// single download near window/RTT. Autotuning grows it to tcp_rmem's maximum.
func configureUpstreamTcpConn(tcpConn *net.TCPConn, tcpBufferSettings *TcpBufferSettings) {
	tcpConn.SetKeepAlive(true)
	tcpConn.SetNoDelay(true)
	// the os may silently cap this at system limits.
	tcpConn.SetWriteBuffer(int(tcpBufferSettings.MaxWindowSize))
}

func scaledPow2WindowSize(maxWindowSize uint32, minWindowSize uint32, floorWindowSize uint32) uint32 {
	scaledWindowSize := uint32(MemoryScaledByteCount(ByteCount(maxWindowSize), ByteCount(floorWindowSize)))
	windowSize := minWindowSize
	for windowSize*2 <= scaledWindowSize {
		windowSize *= 2
	}
	return windowSize
}

func DefaultLocalUserNatSettings() *LocalUserNatSettings {
	return &LocalUserNatSettings{
		// scaled by the memory budget: slots on the dispatch channel pin
		// in-flight pool buffers under backpressure
		SequenceBufferSize: MemoryScaledCount(defaultIpBufferSize, 256),
		SendShardCount:     1,
		// BufferTimeout:      15 * time.Second,
		UdpBufferSettings:  DefaultUdpBufferSettings(),
		TcpBufferSettings:  DefaultTcpBufferSettings(),
		IcmpBufferSettings: DefaultIcmpBufferSettings(),
	}
}

// providerUdpIdleTimeout is the udp idle reap for an unbudgeted
// provider/egress. The general 60s udp idle (tuned for constrained devices,
// where single round trip flows dominate and each idle flow pins memory) is
// too aggressive for an egress provider: long-lived plain-udp sessions
// (wireguard, voip, games) legitimately go quiet for minutes, and reaping
// their NAT bindings breaks the flow for the user. 300s is the historical
// provider default from before the idle reap was shortened.
const providerUdpIdleTimeout = 300 * time.Second

// per-flow byte cost model for deriving the provider flow caps from the
// provider memory target: the expected in-process bytes one flow pins
// (channel backing arrays and queued packets, read buffer occupancy, window
// bookkeeping). Kernel socket buffers are excluded — they live outside the
// process footprint. These are the calibration levers for the provider
// target; validate changes against the provider load measurement
// (sdk TestDeviceLocalProviderMemoryUnderLoad).
const providerUdpFlowByteCount = 2 * 1024
const providerTcpFlowByteCount = 8 * 1024

// the icmp item of the same model: the backend read and write buffers (one
// mtu each on unix; the windows transactor's bounded outstanding reply
// buffers are comparable), the send queue backing array, flow bookkeeping,
// and the packet-class pool buffer a live flow keeps in circulation.
// Calibrated to the marginal heap of a live flow (~7 KiB measured, see
// TestIcmpFlowMemoryFootprint), rounded up. Like its udp and tcp siblings
// this figure is heap attributable: goroutine stacks (~10 KiB for a flow's
// two goroutines) and kernel socket buffers live outside it.
const providerIcmpFlowByteCount = 8 * 1024

// the icmp share of the nat target. icmp is an additive item above the
// udp/tcp split (see the provider profile below), so the divisor bounds the
// overcommit rather than carving the other tables.
const providerIcmpTargetDivisor = 16

// A cold public page can fan out across more than 100 origins while the
// browser keeps prior http/2 and quic connections alive. The target-derived
// 4 MiB mobile provider profile used to cap one source at 51 tcp and 153 udp
// flows, so an ordinary page evicted live handshakes and fell onto the
// browser's 17/33/65 second syn retry ladder. These are bounded functional
// floors, not unlimited exceptions: the aggregate caps still contain the
// complete table, and larger provider targets continue to scale from the
// byte-cost model above.
const providerMinUdpUserLimit = 256
const providerMinUdpGlobalLimit = 512
const providerMinTcpUserLimit = 256
const providerMinTcpGlobalLimit = 512

// icmp functional floors, in the style of the udp/tcp floors above: echo
// flows are one per ping process, so even a tiny target keeps a working
// ping table (see ICMP.md).
const providerMinIcmpUserLimit = 64
const providerMinIcmpGlobalLimit = 128

// DefaultProviderLocalUserNatSettings is the explicit provider/egress profile.
// A process that installed a memory budget is a constrained device and gets
// the scaled per-source/aggregate caps. An unbudgeted desktop/server provider
// preserves the historical unlimited flow counts: silently assigning it the
// phone's 512-TCP cap resets established provider traffic under ordinary
// server-scale load. Keeping this choice here makes provider policy explicit
// without changing every generic LocalUserNat caller.
func DefaultProviderLocalUserNatSettings() *LocalUserNatSettings {
	return DefaultProviderLocalUserNatSettingsWithMemoryTarget(0)
}

// DefaultProviderLocalUserNatSettingsWithMemoryTarget sizes the provider
// profile from the owner's provider memory target (the per-device share, see
// the sdk device wiring). 0 keeps the legacy behavior: process-budget-scaled
// caps, or unlimited flow counts for an unbudgeted server/desktop provider.
func DefaultProviderLocalUserNatSettingsWithMemoryTarget(targetByteCount ByteCount) *LocalUserNatSettings {
	settings := DefaultLocalUserNatSettings()
	if 0 < targetByteCount {
		// the provider target has its own gate, independent of the process
		// budget. Half the target sizes the egress nat flow tables (60% udp /
		// 40% tcp by bytes, converted to flow counts by the per-flow cost
		// model above, keeping the historical user:global ratios and floors);
		// the other half sizes the provider client's transfer budgets (see
		// the sdk device wiring). Flows over a cap evict the
		// least-recently-active flow, so the tables remain predictably
		// bounded under load. The functional fanout floors above may exceed a
		// very small target's byte-derived count.
		natTarget := targetByteCount / 2
		udpGlobalLimit := max(providerMinUdpGlobalLimit, int(natTarget*3/5/providerUdpFlowByteCount))
		tcpGlobalLimit := max(providerMinTcpGlobalLimit, int(natTarget*2/5/providerTcpFlowByteCount))
		settings.UdpBufferSettings.UserLimit = max(providerMinUdpUserLimit, udpGlobalLimit/4)
		settings.UdpBufferSettings.GlobalLimit = udpGlobalLimit
		// One readiness shard per receive-dispatch shard removes the socket-read
		// goroutine from every provider UDP flow while preserving a bounded
		// callback failure domain. Unsupported platforms transparently retain the
		// portable per-flow reader.
		settings.UdpBufferSettings.SocketReadShardCount =
			settings.UdpBufferSettings.ReceiveShardCount
		settings.UdpBufferSettings.SharedSocketLifecycle = true
		settings.TcpBufferSettings.UserLimit = max(providerMinTcpUserLimit, tcpGlobalLimit/2)
		settings.TcpBufferSettings.GlobalLimit = tcpGlobalLimit
		// icmp is its own budget item, additive above the udp/tcp split: an
		// idle icmp path must not shrink the udp/tcp tables, so its bound is
		// not carved from their shares. The caps are ceilings — an unused
		// table holds zero bytes — and while icmp flows exist the worst-case
		// overcommit of the nat target is bounded by the divisor (like the
		// functional floors above at small targets).
		icmpGlobalLimit := max(
			providerMinIcmpGlobalLimit,
			int(natTarget/providerIcmpTargetDivisor/providerIcmpFlowByteCount),
		)
		settings.IcmpBufferSettings.UserLimit = max(providerMinIcmpUserLimit, icmpGlobalLimit/2)
		settings.IcmpBufferSettings.GlobalLimit = icmpGlobalLimit

		// A provider can hold hundreds of simultaneous page flows. Never grow a
		// per-flow channel to the high-BDP maximum window; the window is a
		// demand-driven streaming ceiling, while channel backing is allocated at
		// flow construction. When a constrained profile's window is smaller than
		// the generic queue, trim the queue to one window. Otherwise retain the
		// memory-scaled queue so a larger window does not multiply cold-flow
		// memory.
		tcpPayloadByteCount := max(
			1,
			settings.TcpBufferSettings.Mtu-Ipv6HeaderSize-TcpHeaderSizeWithoutExtensions,
		)
		windowPacketCount := int(
			(settings.TcpBufferSettings.MaxWindowSize +
				uint32(tcpPayloadByteCount) - 1) /
				uint32(tcpPayloadByteCount),
		)
		settings.TcpBufferSettings.SequenceBufferSize = min(
			settings.TcpBufferSettings.SequenceBufferSize,
			max(1, windowPacketCount),
		)
		return settings
	}
	if MemoryBudget() <= 0 {
		// unbudgeted: keep the unlimited flow counts and give plain-udp NAT
		// bindings the provider-tuned idle instead of the general short reap
		settings.UdpBufferSettings.IdleTimeout = providerUdpIdleTimeout
		return settings
	}
	settings.UdpBufferSettings.UserLimit = MemoryScaledCount(512, 64)
	settings.UdpBufferSettings.GlobalLimit = MemoryScaledCount(2048, 256)
	settings.TcpBufferSettings.UserLimit = MemoryScaledCount(256, 32)
	settings.TcpBufferSettings.GlobalLimit = MemoryScaledCount(512, 64)
	settings.IcmpBufferSettings.UserLimit = MemoryScaledCount(128, 16)
	settings.IcmpBufferSettings.GlobalLimit = MemoryScaledCount(256, 32)
	return settings
}

// DefaultLocalUserNatSettingsWithBufferSize applies `bufferSize` verbatim to
// the nat dispatch channel and every per flow channel (no memory budget
// scaling of the depths; callers that pass an explicit size own the depth
// choice). The window and read buffer defaults still scale.
func DefaultLocalUserNatSettingsWithBufferSize(bufferSize int) *LocalUserNatSettings {
	return &LocalUserNatSettings{
		SequenceBufferSize: bufferSize,
		SendShardCount:     1,
		UdpBufferSettings:  DefaultUdpBufferSettingsWithBufferSize(bufferSize),
		TcpBufferSettings:  DefaultTcpBufferSettingsWithBufferSize(bufferSize),
		IcmpBufferSettings: DefaultIcmpBufferSettingsWithBufferSize(bufferSize),
	}
}

type LocalUserNatSettings struct {
	SequenceBufferSize int
	// the number of send dispatch shards.
	// flows are pinned to a shard by their address tuple, which preserves
	// the per flow lossless in-order assumption.
	// the default is 1, since the per flow sequences already parallelize the
	// heavy work and sharding measured neutral at eight parallel flows.
	// note the tcp/udp user limits apply per shard.
	SendShardCount int
	// BufferTimeout      time.Duration
	UdpBufferSettings *UdpBufferSettings
	TcpBufferSettings *TcpBufferSettings
	// nil resolves to the defaults, so settings constructed before icmp
	// support keep working
	IcmpBufferSettings *IcmpBufferSettings

	// Log, when set, is used by the local user nat and its udp/tcp/icmp
	// buffers and sequences (used for a buffer whose settings `Log` is nil,
	// via a private copy — the caller's settings are never mutated).
	// nil resolves to `DefaultLogger()`.
	Log Logger
	// Tests observe the terminal dispatch-to-flow join edge. Nil is a
	// production no-op.
	beforeFlowWorkersWaitForTest func()
}

// Forwards packets using user-space sockets. Transfer is expected to remain
// lossless. Route promotion can briefly reorder source-side TCP packets, which
// each TCP sequence retains within a bounded crossover window.
type LocalUserNat struct {
	ctx       context.Context
	cancel    context.CancelFunc
	clientTag string
	log       Logger

	sendPackets chan *SendPacket
	sendLock    sync.Mutex
	sendClosed  bool
	sendWg      sync.WaitGroup
	runDone     chan struct{}

	settings *LocalUserNatSettings

	// receive callback
	receiveCallbacks *CallbackList[ReceivePacketFunction]
	// provider callback that retains the transfer lane
	receiveTransferCallbacks *CallbackList[receiveTransferPacketFunction]
	// batch receive callback (see receiveTransferPackets)
	receivePacketsCallbacks *CallbackList[ReceivePacketsFunction]
	// provider batch callback that retains the transfer lane
	receiveTransferPacketsCallbacks *CallbackList[receiveTransferPacketsFunction]
	// TCP completion includes teardown, socket failure, capacity retirement,
	// and idle timeout.
	tcpFlowCloseCallbacks *CallbackList[tcpFlowCloseFunction]

	// Provider generations share a LocalUserNat during ordinary ownership
	// handoffs. A rejected source remains tombstoned until every generation
	// that claimed it releases its owner; no generation may clear a sibling's
	// rejection. The lock order is this state then a protocol buffer mutex.
	sourceRetirementLock      sync.Mutex
	nextSourceRetirementOwner uint64
	nextSourceRetirementCb    uint64
	sourceRetirementOwnerIds  map[uint64]map[Id]bool
	sourceRetirementSourceIds map[Id]map[uint64]bool
	sourceRetirementDoneIds   map[Id][]<-chan struct{}
	sourceRetirementCallbacks map[uint64]sourceRetirementFunction

	// Test-only completed-disposition edge. Nil is a production no-op.
	afterSendPacketForTest   func()
	beforeRunDoneWaitForTest func()
}

func NewLocalUserNatWithDefaults(ctx context.Context, clientTag string) *LocalUserNat {
	return NewLocalUserNat(ctx, clientTag, DefaultLocalUserNatSettings())
}

func NewLocalUserNat(ctx context.Context, clientTag string, settings *LocalUserNatSettings) *LocalUserNat {
	cancelCtx, cancel := context.WithCancel(ctx)

	log := loggerOrDefault(settings.Log)
	// propagate so a nat-level logger covers the udp/tcp/icmp buffers and
	// sequences. Copy instead of writing through the caller's settings: the
	// caller may share them with other components or concurrent
	// constructions (see the platform transport framer settings for the
	// same rule).
	{
		copied := *settings
		if copied.UdpBufferSettings != nil && copied.UdpBufferSettings.Log == nil {
			udpCopied := *copied.UdpBufferSettings
			udpCopied.Log = log
			copied.UdpBufferSettings = &udpCopied
		}
		if copied.TcpBufferSettings != nil && copied.TcpBufferSettings.Log == nil {
			tcpCopied := *copied.TcpBufferSettings
			tcpCopied.Log = log
			copied.TcpBufferSettings = &tcpCopied
		}
		if copied.IcmpBufferSettings == nil {
			copied.IcmpBufferSettings = DefaultIcmpBufferSettings()
		}
		if copied.IcmpBufferSettings.Log == nil {
			icmpCopied := *copied.IcmpBufferSettings
			icmpCopied.Log = log
			copied.IcmpBufferSettings = &icmpCopied
		}
		settings = &copied
	}
	localUserNat := &LocalUserNat{
		ctx:                             cancelCtx,
		cancel:                          cancel,
		clientTag:                       clientTag,
		log:                             log,
		sendPackets:                     make(chan *SendPacket, settings.SequenceBufferSize),
		runDone:                         make(chan struct{}),
		settings:                        settings,
		receiveCallbacks:                NewCallbackList[ReceivePacketFunction](),
		receiveTransferCallbacks:        NewCallbackList[receiveTransferPacketFunction](),
		receivePacketsCallbacks:         NewCallbackList[ReceivePacketsFunction](),
		receiveTransferPacketsCallbacks: NewCallbackList[receiveTransferPacketsFunction](),
		tcpFlowCloseCallbacks:           NewCallbackList[tcpFlowCloseFunction](),
		sourceRetirementOwnerIds:        map[uint64]map[Id]bool{},
		sourceRetirementSourceIds:       map[Id]map[uint64]bool{},
		sourceRetirementDoneIds:         map[Id][]<-chan struct{}{},
		sourceRetirementCallbacks:       map[uint64]sourceRetirementFunction{},
	}
	go func() {
		defer close(localUserNat.runDone)
		HandleError(localUserNat.Run)
	}()

	return localUserNat
}

// Allocates one provider-generation owner. The owner must be released after
// that provider has joined all source work.
func (self *LocalUserNat) newSourceRetirementOwner() uint64 {
	self.sourceRetirementLock.Lock()
	defer self.sourceRetirementLock.Unlock()
	for {
		self.nextSourceRetirementOwner += 1
		if self.nextSourceRetirementOwner != 0 {
			break
		}
	}
	ownerId := self.nextSourceRetirementOwner
	self.sourceRetirementOwnerIds[ownerId] = map[Id]bool{}
	return ownerId
}

// Publishes one exact-source tombstone for an owner. Protocol callbacks run
// under the retirement lock to serialize registration, retirement, and final
// release; callbacks only take their leaf buffer mutexes and never reenter the
// LocalUserNat.
func (self *LocalUserNat) retireSourceForOwner(
	ownerId uint64,
	sourceId Id,
) []<-chan struct{} {
	if ownerId == 0 || sourceId == (Id{}) {
		return nil
	}
	self.sourceRetirementLock.Lock()
	defer self.sourceRetirementLock.Unlock()
	ownerSourceIds, ok := self.sourceRetirementOwnerIds[ownerId]
	if !ok {
		return nil
	}
	if ownerSourceIds[sourceId] {
		return slices.Clone(self.sourceRetirementDoneIds[sourceId])
	}
	ownerSourceIds[sourceId] = true
	sourceOwnerIds := self.sourceRetirementSourceIds[sourceId]
	if sourceOwnerIds == nil {
		sourceOwnerIds = map[uint64]bool{}
		self.sourceRetirementSourceIds[sourceId] = sourceOwnerIds
	}
	firstOwner := len(sourceOwnerIds) == 0
	sourceOwnerIds[ownerId] = true
	if firstOwner {
		for _, callback := range self.sourceRetirementCallbacks {
			self.sourceRetirementDoneIds[sourceId] = append(
				self.sourceRetirementDoneIds[sourceId],
				callback(sourceId, true)...,
			)
		}
	}
	return slices.Clone(self.sourceRetirementDoneIds[sourceId])
}

// Releases only one generation's claims. A source becomes admissible again
// only after its final owner has gone away.
func (self *LocalUserNat) releaseSourceRetirementOwner(ownerId uint64) {
	if ownerId == 0 {
		return
	}
	self.sourceRetirementLock.Lock()
	defer self.sourceRetirementLock.Unlock()
	ownerSourceIds, ok := self.sourceRetirementOwnerIds[ownerId]
	if !ok {
		return
	}
	delete(self.sourceRetirementOwnerIds, ownerId)
	for sourceId := range ownerSourceIds {
		sourceOwnerIds := self.sourceRetirementSourceIds[sourceId]
		delete(sourceOwnerIds, ownerId)
		if len(sourceOwnerIds) != 0 {
			continue
		}
		delete(self.sourceRetirementSourceIds, sourceId)
		delete(self.sourceRetirementDoneIds, sourceId)
		for _, callback := range self.sourceRetirementCallbacks {
			callback(sourceId, false)
		}
	}
}

// Registers one shard and replays current tombstones before it can process a
// packet. The returned unsubscribe is idempotent.
func (self *LocalUserNat) addSourceRetirementCallback(callback sourceRetirementFunction) func() {
	self.sourceRetirementLock.Lock()
	for {
		self.nextSourceRetirementCb += 1
		if self.nextSourceRetirementCb != 0 {
			break
		}
	}
	callbackId := self.nextSourceRetirementCb
	self.sourceRetirementCallbacks[callbackId] = callback
	for sourceId := range self.sourceRetirementSourceIds {
		callback(sourceId, true)
	}
	self.sourceRetirementLock.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			self.sourceRetirementLock.Lock()
			delete(self.sourceRetirementCallbacks, callbackId)
			self.sourceRetirementLock.Unlock()
		})
	}
}

func (self *LocalUserNat) SecurityPolicyStats(reset bool) SecurityPolicyStats {
	return SecurityPolicyStats{}
}

func (self *LocalUserNat) SendPacketWithTimeout(source TransferPath, provideMode protocol.ProvideMode,
	packet []byte, timeout time.Duration) bool {
	return self.SendPacketsWithTimeout(source, provideMode, [][]byte{packet}, timeout)
}

// `SendPacketWithTimeout` for a batch of packets from one source.
// queueing a batch is one channel operation, which reduces wakeups when the
// caller already has multiple packets in hand.
// on success the local user nat owns `packets` and each packet in it.
// on failure the caller keeps ownership of all of the packets.
func (self *LocalUserNat) SendPacketsWithTimeout(source TransferPath, provideMode protocol.ProvideMode,
	packets [][]byte, timeout time.Duration) bool {
	return self.sendTransferPacketsWithTimeout(
		source,
		TransferKey{},
		provideMode,
		packets,
		timeout,
	)
}

// SendPacketBatch transfers every packet on successful all-or-nothing local
// admission. A rejection returns every packet to the pool.
func (self *LocalUserNat) SendPacketBatch(
	source TransferPath,
	provideMode protocol.ProvideMode,
	packets [][]byte,
	timeout time.Duration,
) int {
	if len(packets) == 0 {
		return 0
	}
	// SendPacketsWithTimeout queues the list itself. Preserve the batch-client
	// contract by owning the slice metadata while retaining packet ownership.
	ownedPackets := slices.Clone(packets)
	if self.SendPacketsWithTimeout(source, provideMode, ownedPackets, timeout) {
		return len(packets)
	}
	for _, packet := range packets {
		MessagePoolReturn(packet)
	}
	return 0
}

// Queues provider ingress without discarding its receiver-visible reply lane.
func (self *LocalUserNat) sendTransferPacketsWithTimeout(
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	packets [][]byte,
	timeout time.Duration,
	completed ...func(),
) bool {
	var disposition *localUserNatSendDisposition
	if 0 < len(completed) && completed[0] != nil {
		disposition = newLocalUserNatSendDisposition(completed[0])
	}
	if !self.beginSend() {
		disposition.finish()
		return false
	}
	defer self.sendWg.Done()

	sendPacket := &SendPacket{
		source:      source.LocalMask(),
		transferKey: transferKey,
		provideMode: provideMode,
		packets:     packets,
		disposition: disposition,
	}
	// fast path without arming a timer
	select {
	case self.sendPackets <- sendPacket:
		return true
	default:
	}
	if timeout < 0 {
		select {
		case <-self.ctx.Done():
			sendPacket.finish()
			return false
		case self.sendPackets <- sendPacket:
			return true
		}
	} else if 0 == timeout {
		select {
		case <-self.ctx.Done():
			sendPacket.finish()
			return false
		case self.sendPackets <- sendPacket:
			return true
		default:
			// full
			sendPacket.finish()
			return false
		}
	} else {
		select {
		case <-self.ctx.Done():
			sendPacket.finish()
			return false
		case self.sendPackets <- sendPacket:
			return true
		case <-time.After(timeout):
			// full
			sendPacket.finish()
			return false
		}
	}
}

// beginSend registers one sender before shutdown can start waiting for all
// possible channel publications. The lock makes WaitGroup.Add and Wait
// mutually exclusive, so a canceled NAT cannot acquire a late queued batch.
func (self *LocalUserNat) beginSend() bool {
	self.sendLock.Lock()
	defer self.sendLock.Unlock()
	if self.sendClosed || self.ctx.Err() != nil {
		return false
	}
	self.sendWg.Add(1)
	return true
}

// stopSending closes admission and releases senders blocked on a full queue.
func (self *LocalUserNat) stopSending() {
	self.sendLock.Lock()
	self.sendClosed = true
	self.sendLock.Unlock()
	self.cancel()
}

// `SendPacketFunction`
func (self *LocalUserNat) SendPacket(source TransferPath, provideMode protocol.ProvideMode, packet []byte, timeout time.Duration) bool {
	return self.SendPacketWithTimeout(source, provideMode, packet, timeout)
}

// `SendPackets` for a batch of packets from one source. see `SendPacketsWithTimeout`.
func (self *LocalUserNat) SendPackets(source TransferPath, provideMode protocol.ProvideMode, packets [][]byte, timeout time.Duration) bool {
	return self.SendPacketsWithTimeout(source, provideMode, packets, timeout)
}

// func (self *LocalUserNat) ReceiveN(source TransferPath, provideMode protocol.ProvideMode, packet []byte, n int) {
//     self.Receive(source, provideMode, packet[0:n])
// }

func (self *LocalUserNat) AddReceivePacketCallback(receiveCallback ReceivePacketFunction) func() {
	callbackId := self.receiveCallbacks.Add(receiveCallback)
	return func() {
		self.receiveCallbacks.Remove(callbackId)
	}
}

// Registers an internal provider return callback that retains the transfer
// lane. Public packet consumers continue to receive only the source path.
func (self *LocalUserNat) addReceiveTransferPacketCallback(
	receiveCallback receiveTransferPacketFunction,
) func() {
	callbackId := self.receiveTransferCallbacks.Add(receiveCallback)
	return func() {
		self.receiveTransferCallbacks.Remove(callbackId)
	}
}

// func (self *LocalUserNat) RemoveReceivePacketCallback(receiveCallback ReceivePacketFunction) {
//     self.receiveCallbacks.Remove(receiveCallback)
// }

// Delivers one borrowed packet while retaining its key and recovery owner.
func (self *LocalUserNat) receiveTransfer(
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	recoveryMode receiveRecoveryMode,
	ipPath *IpPath,
	packet []byte,
) {
	for _, receiveCallback := range self.receiveTransferCallbacks.Get() {
		HandleError(func() {
			receiveCallback(source, transferKey, provideMode, recoveryMode, ipPath, packet)
		})
	}
	for _, receiveCallback := range self.receiveCallbacks.Get() {
		HandleError(func() {
			receiveCallback(source, provideMode, ipPath, packet)
		})
	}
}

// Adapts legacy direct buffer construction to the keyed internal callback.
func receiveTransferPacketAdapter(receiveCallback ReceivePacketFunction) receiveTransferPacketFunction {
	return func(
		source TransferPath,
		_ TransferKey,
		provideMode protocol.ProvideMode,
		_ receiveRecoveryMode,
		ipPath *IpPath,
		packet []byte,
	) {
		receiveCallback(source, provideMode, ipPath, packet)
	}
}

// receiveTransferPackets fans a flow's packet batch to the registered batch
// callbacks (the provider return path coalesces it into one wire Pack).
// Batch consumers take the batch exclusively: when any is registered, the
// per-packet callbacks do not see these packets (the flow read-loop only
// falls back to per-packet delivery when receivePackets reports
// not-batched). A nat therefore has either batch consumers (the provider
// exit nat) or per-packet consumers (the device local nats), not both.
// The batch callback shares read-only what it retains; the read-loop keeps
// buffer ownership.
func (self *LocalUserNat) receiveTransferPackets(
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	recoveryMode receiveRecoveryMode,
	ipPath *IpPath,
	packets [][]byte,
) (batched bool) {
	transferBatchCallbacks := self.receiveTransferPacketsCallbacks.Get()
	for _, receiveCallback := range transferBatchCallbacks {
		HandleError(func() {
			receiveCallback(source, transferKey, provideMode, recoveryMode, ipPath, packets)
		})
		batched = true
	}
	batchCallbacks := self.receivePacketsCallbacks.Get()
	for _, batchCallback := range batchCallbacks {
		HandleError(func() {
			batchCallback(source, provideMode, ipPath, packets)
		})
		batched = true
	}
	return batched
}

// AddReceivePacketsCallback registers a batch receive callback (see
// receiveTransferPackets). A batch callback borrows the flow's drained packet buffers
// for the duration of the call; the read loop retains ownership.
func (self *LocalUserNat) AddReceivePacketsCallback(receiveCallback ReceivePacketsFunction) func() {
	callbackId := self.receivePacketsCallbacks.Add(receiveCallback)
	return func() {
		self.receivePacketsCallbacks.Remove(callbackId)
	}
}

// Registers the keyed batch counterpart for the provider return path.
func (self *LocalUserNat) addReceiveTransferPacketsCallback(
	receiveCallback receiveTransferPacketsFunction,
) func() {
	callbackId := self.receiveTransferPacketsCallbacks.Add(receiveCallback)
	return func() {
		self.receiveTransferPacketsCallbacks.Remove(callbackId)
	}
}

// addTcpFlowCloseCallback registers an internal provider lifecycle consumer.
func (self *LocalUserNat) addTcpFlowCloseCallback(callback tcpFlowCloseFunction) func() {
	callbackId := self.tcpFlowCloseCallbacks.Add(callback)
	return func() {
		self.tcpFlowCloseCallbacks.Remove(callbackId)
	}
}

// closeTcpFlow delivers one synchronous, nonblocking lifecycle edge.
func (self *LocalUserNat) closeTcpFlow(source TransferPath, ipPath *IpPath) {
	for _, callback := range self.tcpFlowCloseCallbacks.Get() {
		HandleError(func() {
			callback(source, ipPath)
		})
	}
}

func (self *LocalUserNat) Run() {
	shardCount := max(1, self.settings.SendShardCount)
	var shardSendPackets []chan *SendPacket
	var shardWg sync.WaitGroup
	defer func() {
		self.stopSending()
		self.sendWg.Wait()
		shardWg.Wait()
		returnQueuedSendPackets(self.sendPackets)
		for _, sendPackets := range shardSendPackets {
			returnQueuedSendPackets(sendPackets)
		}
	}()

	if shardCount == 1 {
		self.runSendShard(self.sendPackets)
		return
	}

	// shard the send dispatch. flows are pinned to a shard by their address
	// tuple, which preserves the per flow lossless in-order assumption.
	shardSendPackets = make([]chan *SendPacket, shardCount)
	for i := 0; i < shardCount; i += 1 {
		sendPackets := make(chan *SendPacket, self.settings.SequenceBufferSize)
		shardSendPackets[i] = sendPackets
		shardWg.Add(1)
		go func() {
			defer shardWg.Done()
			HandleError(func() {
				self.runSendShard(sendPackets)
			}, self.cancel)
		}()
	}

	forward := func(shard int, sendPacket *SendPacket) bool {
		select {
		case <-self.ctx.Done():
			for _, packet := range sendPacket.packets {
				MessagePoolReturn(packet)
			}
			sendPacket.finish()
			return false
		case shardSendPackets[shard] <- sendPacket:
			return true
		}
	}

	route := func(sendPacket *SendPacket) bool {
		if len(sendPacket.packets) == 0 {
			sendPacket.finish()
			return true
		}
		// common case: all packets in the batch are for one shard
		shard := sendShard(sendPacket.packets[0], shardCount)
		split := false
		for _, packet := range sendPacket.packets[1:] {
			if sendShard(packet, shardCount) != shard {
				split = true
				break
			}
		}
		if !split {
			return forward(shard, sendPacket)
		}

		shardPackets := make([][][]byte, shardCount)
		for _, packet := range sendPacket.packets {
			packetShard := sendShard(packet, shardCount)
			shardPackets[packetShard] = append(shardPackets[packetShard], packet)
		}
		childCount := 0
		for _, packets := range shardPackets {
			if 0 < len(packets) {
				childCount += 1
			}
		}
		sendPacket.disposition.add(childCount - 1)
		for packetShard, packets := range shardPackets {
			if len(packets) == 0 {
				continue
			}
			shardSendPacket := &SendPacket{
				source:      sendPacket.source,
				transferKey: sendPacket.transferKey,
				provideMode: sendPacket.provideMode,
				packets:     packets,
				disposition: sendPacket.disposition,
			}
			if !forward(packetShard, shardSendPacket) {
				for _, returnPackets := range shardPackets[packetShard+1:] {
					if len(returnPackets) == 0 {
						continue
					}
					for _, packet := range returnPackets {
						MessagePoolReturn(packet)
					}
					sendPacket.disposition.finish()
				}
				return false
			}
		}
		return true
	}

send:
	for {
		select {
		case <-self.ctx.Done():
			return
		case sendPacket := <-self.sendPackets:
			if !route(sendPacket) {
				return
			}
			// opportunistically drain queued packets to reduce wakeups
			for {
				select {
				case <-self.ctx.Done():
					return
				case sendPacket := <-self.sendPackets:
					if !route(sendPacket) {
						return
					}
				default:
					continue send
				}
			}
		}
	}
}

// returnQueuedSendPackets releases packet ownership left in a retired dispatch
// queue. Callers first stop and join every possible producer of the queue.
func returnQueuedSendPackets(sendPackets chan *SendPacket) {
	for {
		select {
		case sendPacket := <-sendPackets:
			func() {
				defer sendPacket.finish()
				for _, packet := range sendPacket.packets {
					MessagePoolReturn(packet)
				}
			}()
		default:
			return
		}
	}
}

// pins a packet to a send shard by its address tuple.
// all packets of a flow map to the same shard.
func sendShard(ipPacket []byte, shardCount int) int {
	// fnv-1a over the address tuple
	hash := uint32(2166136261)
	hashBytes := func(b []byte) {
		for _, c := range b {
			hash = (hash ^ uint32(c)) * 16777619
		}
	}
	if 0 < len(ipPacket) {
		switch uint8(ipPacket[0]) >> 4 {
		case 4:
			if Ipv4HeaderSizeWithoutExtensions <= len(ipPacket) {
				hashBytes(ipPacket[12:20])
				headerByteCount := int(ipPacket[0]&0xf) * 4
				flagsAndOffset := binary.BigEndian.Uint16(ipPacket[6:8])
				if flagsAndOffset&0x3fff != 0 {
					// Non-first fragments do not contain transport ports. Pin every
					// fragment of one datagram by its shared IPv4 identity instead.
					hashBytes(ipPacket[9:10])
					hashBytes(ipPacket[4:6])
				} else if ipProtocolNumber(ipPacket[9]) == ipProtocolNumberIcmp4 {
					// the first transport bytes are type/code/checksum, which
					// vary per packet; the echo identifier is the flow identity
					if headerByteCount+6 <= len(ipPacket) {
						hashBytes(ipPacket[headerByteCount+4 : headerByteCount+6])
					}
				} else if headerByteCount+4 <= len(ipPacket) {
					hashBytes(ipPacket[headerByteCount : headerByteCount+4])
				}
			}
		case 6:
			if Ipv6HeaderSize <= len(ipPacket) {
				hashBytes(ipPacket[8:40])
				nextHeader := ipProtocolNumber(ipPacket[6])
				transportOffset := Ipv6HeaderSize
				if isIpv6ExtensionHeader(ipPacket[6]) {
					// walk the chain so a flow with extension headers pins
					// to the same shard as its plain packets. Every fragment
					// of one datagram, including the first, pins by the
					// shared identification: non-first fragments carry no
					// transport ports.
					walk, ok := walkIpv6ExtensionHeaders(ipPacket)
					if !ok {
						break
					}
					if walk.fragmented {
						var identification [4]byte
						binary.BigEndian.PutUint32(identification[:], walk.fragment.identification)
						hashBytes(identification[:])
						break
					}
					nextHeader = walk.nextHeader
					transportOffset = walk.transportOffset
				}
				if nextHeader == ipProtocolNumberIcmp6 {
					// the echo identifier is the flow identity
					if transportOffset+6 <= len(ipPacket) {
						hashBytes(ipPacket[transportOffset+4 : transportOffset+6])
					}
				} else if transportOffset+4 <= len(ipPacket) {
					hashBytes(ipPacket[transportOffset : transportOffset+4])
				}
			}
		}
	}
	return int(hash % uint32(shardCount))
}

func (self *LocalUserNat) runSendShard(sendPackets chan *SendPacket) {
	defer self.cancel()
	ipv4Fragments := newIpv4FragmentReassembler()
	defer ipv4Fragments.close()
	ipv6Fragments := newIpv6FragmentReassembler()
	defer ipv6Fragments.close()

	udp4Buffer := newUdp4BufferWithTransferKey(self.ctx, self.receiveTransfer, self.settings.UdpBufferSettings)
	udp6Buffer := newUdp6BufferWithTransferKey(self.ctx, self.receiveTransfer, self.settings.UdpBufferSettings)
	tcp4Buffer := newTcp4BufferWithTransferKey(self.ctx, self.receiveTransfer, self.settings.TcpBufferSettings)
	tcp6Buffer := newTcp6BufferWithTransferKey(self.ctx, self.receiveTransfer, self.settings.TcpBufferSettings)
	tcp4Buffer.flowCloseCallback = self.closeTcpFlow
	tcp6Buffer.flowCloseCallback = self.closeTcpFlow
	icmp4Buffer := newIcmp4BufferWithTransferKey(self.ctx, self.receiveTransfer, self.settings.IcmpBufferSettings)
	icmp6Buffer := newIcmp6BufferWithTransferKey(self.ctx, self.receiveTransfer, self.settings.IcmpBufferSettings)
	sourceRetirementUnsub := self.addSourceRetirementCallback(func(sourceId Id, retired bool) []<-chan struct{} {
		var doneChannels []<-chan struct{}
		doneChannels = append(doneChannels, udp4Buffer.setSourceRetired(sourceId, retired)...)
		doneChannels = append(doneChannels, udp6Buffer.setSourceRetired(sourceId, retired)...)
		doneChannels = append(doneChannels, tcp4Buffer.setSourceRetired(sourceId, retired)...)
		doneChannels = append(doneChannels, tcp6Buffer.setSourceRetired(sourceId, retired)...)
		doneChannels = append(doneChannels, icmp4Buffer.setSourceRetired(sourceId, retired)...)
		doneChannels = append(doneChannels, icmp6Buffer.setSourceRetired(sourceId, retired)...)
		return doneChannels
	})
	defer sourceRetirementUnsub()
	defer func() {
		// Dispatch has stopped and its context is terminal. Join every flow
		// owner before this shard can publish completion to LocalUserNat.Run.
		self.cancel()
		if self.settings.beforeFlowWorkersWaitForTest != nil {
			self.settings.beforeFlowWorkersWaitForTest()
		}
		udp4Buffer.waitForLifecycle()
		udp6Buffer.waitForLifecycle()
		tcp4Buffer.waitForLifecycle()
		tcp6Buffer.waitForLifecycle()
		icmp4Buffer.waitForLifecycle()
		icmp6Buffer.waitForLifecycle()
	}()
	// the per-flow read-loops route their drained batch through
	// receiveTransferPackets, which coalesces it into one wire Pack when a batch
	// consumer is registered (the provider return path) and otherwise
	// reports not-batched so the loop falls back to per-packet delivery.
	// The check is live (per batch), since the provider registers its batch
	// callback after this nat's Run starts.
	udp4Buffer.receiveTransferPacketsCallback = self.receiveTransferPackets
	udp6Buffer.receiveTransferPacketsCallback = self.receiveTransferPackets
	tcp4Buffer.receiveTransferPacketsCallback = self.receiveTransferPackets
	tcp6Buffer.receiveTransferPacketsCallback = self.receiveTransferPackets
	icmp4Buffer.receiveTransferPacketsCallback = self.receiveTransferPackets
	icmp6Buffer.receiveTransferPacketsCallback = self.receiveTransferPackets

	// parsed per-packet views. these are copied by value into the send items,
	// so the locals can be reused across packets.
	var udpPacket parsedUdp
	var tcpPacket parsedTcp
	var icmpPacket parsedIcmp

	handleIpPacket := func(source TransferPath, transferKey TransferKey, provideMode protocol.ProvideMode, ipPacket []byte) {
		if len(ipPacket) == 0 {
			MessagePoolReturn(ipPacket)
			return
		}
		ipVersion := uint8(ipPacket[0]) >> 4
		switch ipVersion {
		case 4:
			ipPacket = ipv4Fragments.process(source, transferKey, provideMode, ipPacket)
			if ipPacket == nil {
				return
			}
			ipProtocol, sourceIp, destinationIp, transport, ok := parseIpv4(ipPacket)
			if !ok {
				// malformed, drop
				MessagePoolReturn(ipPacket)
				return
			}
			switch ipProtocol {
			case ipProtocolNumberUdp:
				if !parseUdpPacket(sourceIp, destinationIp, transport, &udpPacket) {
					// malformed, drop
					MessagePoolReturn(ipPacket)
					return
				}
				c := func() bool {
					success, err := udp4Buffer.sendTransferKey(
						source,
						transferKey,
						provideMode,
						&udpPacket,
						self.settings.UdpBufferSettings.WriteTimeout,
						ipPacket,
					)
					return success && err == nil
				}
				delivered := false
				if self.log.V(2).Enabled() {
					delivered = TraceWithReturn(
						fmt.Sprintf("[lnr]send udp4 %s<-%s", self.clientTag, source.SourceId),
						c,
					)
				} else {
					delivered = c()
				}
				if !delivered {
					// full/timeout drop: the sequence never took ownership
					MessagePoolReturn(ipPacket)
				}
			case ipProtocolNumberTcp:
				if !parseTcpPacket(sourceIp, destinationIp, transport, &tcpPacket) {
					// malformed, drop
					MessagePoolReturn(ipPacket)
					return
				}
				c := func() bool {
					success, err := tcp4Buffer.sendTransferKey(
						source,
						transferKey,
						provideMode,
						&tcpPacket,
						self.settings.TcpBufferSettings.WriteTimeout,
						ipPacket,
					)
					return success && err == nil
				}
				delivered := false
				if self.log.V(2).Enabled() {
					delivered = TraceWithReturn(
						fmt.Sprintf("[lnr]send tcp4 %s<-%s", self.clientTag, source.SourceId),
						c,
					)
				} else {
					delivered = c()
				}
				if !delivered {
					// full/timeout drop: the sequence never took ownership
					MessagePoolReturn(ipPacket)
				}
			case ipProtocolNumberIcmp4:
				if mtu, embedded, ok := ipParseIcmpv4FragmentationNeeded(transport); ok {
					// the source is telling us a packet we built toward it
					// was too big for its path: shrink that flow, then drop
					// the message itself (nothing to forward)
					applyPathMtu(source, 4, embedded, mtu, tcp4Buffer, udp4Buffer)
					MessagePoolReturn(ipPacket)
					return
				}
				if !parseIcmpPacket(4, sourceIp, destinationIp, transport, &icmpPacket) {
					// unsupported type or malformed, drop
					MessagePoolReturn(ipPacket)
					return
				}
				if !icmpPacket.echoRequest {
					// an outbound echo reply is always an orphan: unsolicited
					// inbound pings cannot reach a source, and the egress
					// backends cannot send one anyway (see ICMP.md)
					MessagePoolReturn(ipPacket)
					return
				}
				icmpPacket.ttl = ipPacket[8]
				c := func() bool {
					success, err := icmp4Buffer.sendTransferKey(
						source,
						transferKey,
						provideMode,
						&icmpPacket,
						self.settings.IcmpBufferSettings.WriteTimeout,
						ipPacket,
					)
					return success && err == nil
				}
				delivered := false
				if self.log.V(2).Enabled() {
					delivered = TraceWithReturn(
						fmt.Sprintf("[lnr]send icmp4 %s<-%s", self.clientTag, source.SourceId),
						c,
					)
				} else {
					delivered = c()
				}
				if !delivered {
					// full/timeout drop: the sequence never took ownership
					MessagePoolReturn(ipPacket)
				}
			default:
				// no support for this protocol, drop
				MessagePoolReturn(ipPacket)
			}
		case 6:
			ipPacket = ipv6Fragments.process(source, transferKey, provideMode, ipPacket)
			if ipPacket == nil {
				return
			}
			ipProtocol, sourceIp, destinationIp, transport, ok := parseIpv6(ipPacket)
			if !ok {
				// malformed, drop
				MessagePoolReturn(ipPacket)
				return
			}
			switch ipProtocol {
			case ipProtocolNumberUdp:
				if !parseUdpPacket(sourceIp, destinationIp, transport, &udpPacket) {
					// malformed, drop
					MessagePoolReturn(ipPacket)
					return
				}
				c := func() bool {
					success, err := udp6Buffer.sendTransferKey(
						source,
						transferKey,
						provideMode,
						&udpPacket,
						self.settings.UdpBufferSettings.WriteTimeout,
						ipPacket,
					)
					return success && err == nil
				}
				delivered := false
				if self.log.V(2).Enabled() {
					delivered = TraceWithReturn(
						fmt.Sprintf("[lnr]send udp6 %s<-%s", self.clientTag, source.SourceId),
						c,
					)
				} else {
					delivered = c()
				}
				if !delivered {
					// full/timeout drop: the sequence never took ownership
					MessagePoolReturn(ipPacket)
				}
			case ipProtocolNumberTcp:
				if !parseTcpPacket(sourceIp, destinationIp, transport, &tcpPacket) {
					// malformed, drop
					MessagePoolReturn(ipPacket)
					return
				}
				c := func() bool {
					success, err := tcp6Buffer.sendTransferKey(
						source,
						transferKey,
						provideMode,
						&tcpPacket,
						self.settings.TcpBufferSettings.WriteTimeout,
						ipPacket,
					)
					return success && err == nil
				}
				delivered := false
				if self.log.V(2).Enabled() {
					delivered = TraceWithReturn(
						fmt.Sprintf("[lnr]send tcp6 %s<-%s", self.clientTag, source.SourceId),
						c,
					)
				} else {
					delivered = c()
				}
				if !delivered {
					// full/timeout drop: the sequence never took ownership
					MessagePoolReturn(ipPacket)
				}
			case ipProtocolNumberIcmp6:
				if 0 < len(transport) && isIcmpv6LinkControlType(transport[0]) {
					// neighbor discovery, router discovery, mld and redirect
					// are link-local chatter with no meaning across a
					// point-to-point tunnel: drop silently, never log
					MessagePoolReturn(ipPacket)
					return
				}
				if mtu, embedded, ok := ipParseIcmpv6PacketTooBig(transport); ok {
					// see the v4 fragmentation-needed case
					applyPathMtu(source, 6, embedded, mtu, tcp6Buffer, udp6Buffer)
					MessagePoolReturn(ipPacket)
					return
				}
				if !parseIcmpPacket(6, sourceIp, destinationIp, transport, &icmpPacket) {
					// unsupported type or malformed, drop
					MessagePoolReturn(ipPacket)
					return
				}
				if !icmpPacket.echoRequest {
					// an outbound echo reply is always an orphan (see the v4
					// case)
					MessagePoolReturn(ipPacket)
					return
				}
				// hop limit
				icmpPacket.ttl = ipPacket[7]
				c := func() bool {
					success, err := icmp6Buffer.sendTransferKey(
						source,
						transferKey,
						provideMode,
						&icmpPacket,
						self.settings.IcmpBufferSettings.WriteTimeout,
						ipPacket,
					)
					return success && err == nil
				}
				delivered := false
				if self.log.V(2).Enabled() {
					delivered = TraceWithReturn(
						fmt.Sprintf("[lnr]send icmp6 %s<-%s", self.clientTag, source.SourceId),
						c,
					)
				} else {
					delivered = c()
				}
				if !delivered {
					// full/timeout drop: the sequence never took ownership
					MessagePoolReturn(ipPacket)
				}
			default:
				// no support for this protocol, drop
				MessagePoolReturn(ipPacket)
			}
		default:
			// unknown IP version, drop
			MessagePoolReturn(ipPacket)
		}
	}

	handleSendPacket := func(sendPacket *SendPacket) {
		defer sendPacket.finish()
		for _, ipPacket := range sendPacket.packets {
			handleIpPacket(sendPacket.source, sendPacket.transferKey, sendPacket.provideMode, ipPacket)
			if self.afterSendPacketForTest != nil {
				self.afterSendPacketForTest()
			}
		}
	}

send:
	for {
		select {
		case <-self.ctx.Done():
			return
		case sendPacket := <-sendPackets:
			handleSendPacket(sendPacket)
			// opportunistically drain queued packets to reduce wakeups
			for {
				select {
				case <-self.ctx.Done():
					return
				case sendPacket := <-sendPackets:
					handleSendPacket(sendPacket)
				default:
					continue send
				}
			}
		}
	}
}

func (self *LocalUserNat) Close() {
	self.stopSending()
}

// A successful join is the ownership barrier for queued packets and every
// protocol-flow worker admitted before Close.
func (self *LocalUserNat) CloseAndWait(ctx context.Context) error {
	self.Close()
	if self.beforeRunDoneWaitForTest != nil {
		self.beforeRunDoneWaitForTest()
	}
	return waitForLifecycleDone(ctx, self.runDone, "local user NAT")
}

// a batch of packets from one source
type SendPacket struct {
	source      TransferPath
	transferKey TransferKey
	provideMode protocol.ProvideMode
	packets     [][]byte
	disposition *localUserNatSendDisposition
}

// One provider admission can fan into several LocalUserNat send shards. The
// shared disposition releases it only after every child has either handed all
// packets to protocol flow ownership or returned them to the message pool.
type localUserNatSendDisposition struct {
	remaining atomic.Int64
	completed func()
}

func newLocalUserNatSendDisposition(completed func()) *localUserNatSendDisposition {
	disposition := &localUserNatSendDisposition{completed: completed}
	disposition.remaining.Store(1)
	return disposition
}

func (self *localUserNatSendDisposition) add(count int) {
	if self != nil && 0 < count {
		self.remaining.Add(int64(count))
	}
}

func (self *localUserNatSendDisposition) finish() {
	if self != nil && self.remaining.Add(-1) == 0 {
		HandleError(self.completed)
	}
}

func (self *SendPacket) finish() {
	if self != nil {
		self.disposition.finish()
	}
}

// comparable
type BufferId4 struct {
	source          TransferPath
	sourceIp        [4]byte
	sourcePort      int
	destinationIp   [4]byte
	destinationPort int
}

func NewBufferId4(source TransferPath, sourceIp net.IP, sourcePort int, destinationIp net.IP, destinationPort int) BufferId4 {
	return BufferId4{
		source:          source,
		sourceIp:        [4]byte(sourceIp),
		sourcePort:      sourcePort,
		destinationIp:   [4]byte(destinationIp),
		destinationPort: destinationPort,
	}
}

// comparable
type BufferId6 struct {
	source          TransferPath
	sourceIp        [16]byte
	sourcePort      int
	destinationIp   [16]byte
	destinationPort int
}

func NewBufferId6(source TransferPath, sourceIp net.IP, sourcePort int, destinationIp net.IP, destinationPort int) BufferId6 {
	return BufferId6{
		source:          source,
		sourceIp:        [16]byte(sourceIp),
		sourcePort:      sourcePort,
		destinationIp:   [16]byte(destinationIp),
		destinationPort: destinationPort,
	}
}

// the smallest mtu a path-mtu signal may shrink a flow to: the link minimum
// of each family (rfc 791 §3.2 requires 68 but 576 is the practical floor
// every stack assumes; rfc 8200 §5 requires 1280)
const (
	ipv4MinimumPathMtu = 576
	ipv6MinimumPathMtu = 1280
)

func ipMinimumPathMtu(ipVersion int) int {
	if ipVersion == 6 {
		return ipv6MinimumPathMtu
	}
	return ipv4MinimumPathMtu
}

// applyPathMtu routes an icmp path-mtu signal from a source to the flow it
// describes. `embedded` is the packet the source could not carry -- one this
// NAT built toward the source, so it runs destination->source -- and the flow
// it belongs to is the reverse tuple. A signal for a flow this NAT does not
// hold is ignored: a source cannot shrink a flow it does not own.
func applyPathMtu(
	source TransferPath,
	ipVersion int,
	embedded *IpPath,
	mtu int,
	tcpBuffer interface {
		applyPathMtuFor(TransferPath, *IpPath, int) bool
	},
	udpBuffer interface {
		applyPathMtuFor(TransferPath, *IpPath, int) bool
	},
) bool {
	if embedded == nil || embedded.Version != ipVersion {
		return false
	}
	switch embedded.Protocol {
	case IpProtocolTcp:
		return tcpBuffer.applyPathMtuFor(source, embedded, mtu)
	case IpProtocolUdp:
		return udpBuffer.applyPathMtuFor(source, embedded, mtu)
	default:
		return false
	}
}

type UdpBufferSettings struct {
	// nil resolves to the local user nat `Log`
	Log                 Logger
	ReadTimeout         time.Duration
	WriteTimeout        time.Duration
	IdleTimeout         time.Duration
	Mtu                 int
	ReadBufferByteCount int
	SequenceBufferSize  int
	// the maximum number of payloads to process under a single write deadline.
	// udp datagrams cannot be coalesced, so each payload is one socket write.
	WriteBatchSize int
	// Number of ordered receive-dispatch shards shared by every UDP flow in
	// this address-family buffer. Zero retains the default of four for custom
	// settings built before this field existed. Each shard queue has
	// SequenceBufferSize slots, making aggregate user-space receive
	// backpressure independent of flow count.
	ReceiveShardCount int
	// SocketReadShardCount replaces one blocking socket-reader goroutine per
	// UDP flow with this many OS-readiness workers per address family. Zero
	// retains the portable per-flow reader. Constrained provider profiles turn
	// this on; generic/custom callers keep the established implementation until
	// their platform and latency cohorts are measured.
	SocketReadShardCount int
	// SharedSocketLifecycle writes each provider UDP datagram directly under
	// its flow lock and uses one buffer-level idle timer instead of retaining a
	// send goroutine, channel, and timer per flow. It requires the readiness
	// poller; unsupported platforms fall back to the established Run loop.
	SharedSocketLifecycle bool
	// the number of open sockets per user
	// uses an lru cleanup where new sockets over the limit close old sockets
	UserLimit int
	// the number of open sockets across all users (per address family
	// buffer), an aggregate flow state and fd ceiling for hosts with a hard
	// process memory budget. uses the same lru cleanup as `UserLimit`.
	// 0 (the default) is no limit.
	GlobalLimit   int
	MaxWindowSize uint32

	ConnectSettings
}

// iana ip protocol numbers, as carried in the ipv4 protocol and ipv6 next
// header fields. tcp, udp, and icmp echo are handled; other protocols are
// dropped. the icmp numbers also label the flow-teardown unreachable
// messages built by `ipOosUnreachable`, which are injected toward the
// source.
type ipProtocolNumber uint8

const (
	ipProtocolNumberIcmp4 = ipProtocolNumber(1)
	ipProtocolNumberTcp   = ipProtocolNumber(6)
	ipProtocolNumberUdp   = ipProtocolNumber(17)
	ipProtocolNumberIcmp6 = ipProtocolNumber(58)
)

// minimal parsed views of packets on the send path.
// these avoid per-packet decode allocations in the hot dispatch.
// all slices alias the backing ip packet, which stays valid while the
// owning send item holds the packet.

type parsedUdp struct {
	sourceIp        net.IP
	destinationIp   net.IP
	sourcePort      uint16
	destinationPort uint16
	payload         []byte
}

type parsedTcp struct {
	sourceIp          net.IP
	destinationIp     net.IP
	sourcePort        uint16
	destinationPort   uint16
	fin               bool
	syn               bool
	rst               bool
	psh               bool
	ack               bool
	seq               uint32
	ackNumber         uint32
	windowSize        uint16
	options           []byte
	enableMss         bool
	mss               uint32
	enableWindowScale bool
	windowScale       uint32
	enableTimestamp   bool
	timestampValue    uint32
	timestampEcho     uint32
	payload           []byte
}

func (self *parsedTcp) flagsString() string {
	flags := []string{}
	if self.fin {
		flags = append(flags, "FIN")
	}
	if self.syn {
		flags = append(flags, "SYN")
	}
	if self.rst {
		flags = append(flags, "RST")
	}
	if self.psh {
		flags = append(flags, "PSH")
	}
	if self.ack {
		flags = append(flags, "ACK")
	}
	return strings.Join(flags, ", ")
}

// parses the ipv4 header. the returned slices alias `ipPacket`.
func parseIpv4(ipPacket []byte) (ipProtocol ipProtocolNumber, sourceIp net.IP, destinationIp net.IP, transport []byte, ok bool) {
	if len(ipPacket) < Ipv4HeaderSizeWithoutExtensions {
		return
	}
	headerByteCount := int(ipPacket[0]&0xf) * 4
	totalByteCount := int(binary.BigEndian.Uint16(ipPacket[2:4]))
	if headerByteCount < Ipv4HeaderSizeWithoutExtensions || totalByteCount < headerByteCount || len(ipPacket) < totalByteCount {
		return
	}
	// fragments are not reassembled: a non-first fragment has no transport
	// header and a first fragment has a truncated payload, so either would
	// misparse payload bytes as transport fields. one 16 bit load covers mf
	// plus the whole offset field (0x3fff); df and the reserved bit pass.
	if binary.BigEndian.Uint16(ipPacket[6:8])&0x3fff != 0 {
		return
	}
	ipProtocol = ipProtocolNumber(ipPacket[9])
	sourceIp = net.IP(ipPacket[12:16])
	destinationIp = net.IP(ipPacket[16:20])
	transport = ipPacket[headerByteCount:totalByteCount]
	ok = true
	return
}

// parses the ipv6 header. the returned slices alias `ipPacket`.
// the extension chain is walked (see ip_ipv6_ext.go), so `transport` starts
// after any hop-by-hop, routing, destination-options or atomic fragment
// headers and `ipProtocol` is the real upper-layer protocol. a non-atomic
// fragment fails the parse, mirroring parseIpv4: fragments are reassembled
// before they reach a parser, and a lone fragment would misparse payload
// bytes as transport fields.
func parseIpv6(ipPacket []byte) (ipProtocol ipProtocolNumber, sourceIp net.IP, destinationIp net.IP, transport []byte, ok bool) {
	if len(ipPacket) < Ipv6HeaderSize {
		return
	}
	var transportOffset int
	var payloadEnd int
	if !isIpv6ExtensionHeader(ipPacket[6]) {
		// the common case, answered without the walk
		payloadEnd = Ipv6HeaderSize + int(binary.BigEndian.Uint16(ipPacket[4:6]))
		if len(ipPacket) < payloadEnd {
			return
		}
		ipProtocol = ipProtocolNumber(ipPacket[6])
		transportOffset = Ipv6HeaderSize
	} else {
		walk, walkOk := walkIpv6ExtensionHeaders(ipPacket)
		if !walkOk || walk.fragmented {
			return
		}
		ipProtocol = walk.nextHeader
		transportOffset = walk.transportOffset
		payloadEnd = walk.payloadEnd
	}
	sourceIp = net.IP(ipPacket[8:24])
	destinationIp = net.IP(ipPacket[24:40])
	transport = ipPacket[transportOffset:payloadEnd]
	ok = true
	return
}

// parses a udp packet into `udp`. the slices alias the backing packet.
func parseUdpPacket(sourceIp net.IP, destinationIp net.IP, transport []byte, udp *parsedUdp) bool {
	if len(transport) < UdpHeaderSize {
		return false
	}
	udpByteCount := int(binary.BigEndian.Uint16(transport[4:6]))
	if udpByteCount < UdpHeaderSize || len(transport) < udpByteCount {
		return false
	}
	udp.sourceIp = sourceIp
	udp.destinationIp = destinationIp
	udp.sourcePort = binary.BigEndian.Uint16(transport[0:2])
	udp.destinationPort = binary.BigEndian.Uint16(transport[2:4])
	udp.payload = transport[UdpHeaderSize:udpByteCount]
	return true
}

// parses a tcp packet into `tcp`. the slices alias the backing packet.
func parseTcpPacket(sourceIp net.IP, destinationIp net.IP, transport []byte, tcp *parsedTcp) bool {
	if len(transport) < TcpHeaderSizeWithoutExtensions {
		return false
	}
	headerByteCount := int(transport[12]>>4) * 4
	if headerByteCount < TcpHeaderSizeWithoutExtensions || len(transport) < headerByteCount {
		return false
	}
	flags := transport[13]
	tcp.sourceIp = sourceIp
	tcp.destinationIp = destinationIp
	tcp.sourcePort = binary.BigEndian.Uint16(transport[0:2])
	tcp.destinationPort = binary.BigEndian.Uint16(transport[2:4])
	tcp.seq = binary.BigEndian.Uint32(transport[4:8])
	tcp.ackNumber = binary.BigEndian.Uint32(transport[8:12])
	tcp.fin = (flags & 0x01) != 0
	tcp.syn = (flags & 0x02) != 0
	tcp.rst = (flags & 0x04) != 0
	tcp.psh = (flags & 0x08) != 0
	tcp.ack = (flags & 0x10) != 0
	tcp.windowSize = binary.BigEndian.Uint16(transport[14:16])
	tcp.options = transport[TcpHeaderSizeWithoutExtensions:headerByteCount]
	parseTcpOptions(tcp)
	tcp.payload = transport[headerByteCount:]
	return true
}

// Extracts the options needed by the synthetic provider endpoint. Unknown
// options remain opaque, and malformed tails stop parsing without turning an
// otherwise valid TCP segment into a malformed IP packet.
func parseTcpOptions(tcp *parsedTcp) {
	tcp.enableMss = false
	tcp.mss = 0
	tcp.enableWindowScale = false
	tcp.windowScale = 0
	tcp.enableTimestamp = false
	tcp.timestampValue = 0
	tcp.timestampEcho = 0
	for optionIndex := 0; optionIndex < len(tcp.options); {
		switch tcp.options[optionIndex] {
		case 0:
			return
		case 1:
			optionIndex += 1
		default:
			if len(tcp.options) < optionIndex+2 {
				return
			}
			optionByteCount := int(tcp.options[optionIndex+1])
			if optionByteCount < 2 || len(tcp.options) < optionIndex+optionByteCount {
				return
			}
			switch tcp.options[optionIndex] {
			case 2:
				if optionByteCount == 4 {
					mss := binary.BigEndian.Uint16(tcp.options[optionIndex+2 : optionIndex+4])
					if mss != 0 {
						tcp.enableMss = true
						tcp.mss = uint32(mss)
					}
				}
			case 3:
				if optionByteCount == 3 {
					tcp.enableWindowScale = true
					tcp.windowScale = min(uint32(tcp.options[optionIndex+2]), 14)
				}
			case 8:
				if optionByteCount == 10 {
					tcp.enableTimestamp = true
					tcp.timestampValue = binary.BigEndian.Uint32(tcp.options[optionIndex+2 : optionIndex+6])
					tcp.timestampEcho = binary.BigEndian.Uint32(tcp.options[optionIndex+6 : optionIndex+10])
				}
			}
			optionIndex += optionByteCount
		}
	}
}

// tcp flag bits
const (
	tcpFlagFin = byte(0x01)
	tcpFlagSyn = byte(0x02)
	tcpFlagRst = byte(0x04)
	tcpFlagPsh = byte(0x08)
	tcpFlagAck = byte(0x10)
)

const tcpTimestampOptionByteCount = 12

var tcpTimestampEpoch = time.Now()

// ones' complement sum in the style of rfc 1071.
// an odd final byte is padded high.
func checksumAdd(sum uint32, b []byte) uint32 {
	i := 0
	for ; i+1 < len(b); i += 2 {
		sum += uint32(binary.BigEndian.Uint16(b[i : i+2]))
	}
	if i < len(b) {
		sum += uint32(b[i]) << 8
	}
	return sum
}

func checksumFinish(sum uint32) uint16 {
	for 0xffff < sum {
		sum = (sum & 0xffff) + (sum >> 16)
	}
	return ^uint16(sum)
}

// computes the transport checksum with the ipv4 or ipv6 pseudo header.
// the two pseudo headers sum identically for transport lengths that fit
// in 16 bits.
func transportChecksum(ipProtocol ipProtocolNumber, packetSourceIp net.IP, packetDestinationIp net.IP, transport []byte) uint16 {
	sum := checksumAdd(0, packetSourceIp)
	sum = checksumAdd(sum, packetDestinationIp)
	sum += uint32(ipProtocol)
	sum += uint32(len(transport))
	return checksumFinish(checksumAdd(sum, transport))
}

// writes an ipv4 header with no options.
// `packet` must be sized to the full packet.
func writeIpv4Header(packet []byte, ipProtocol ipProtocolNumber, packetSourceIp net.IP, packetDestinationIp net.IP) {
	// version 4, header length 5 words
	packet[0] = 0x45
	// tos
	packet[1] = 0
	binary.BigEndian.PutUint16(packet[2:4], uint16(len(packet)))
	// id, flags, fragment offset
	packet[4] = 0
	packet[5] = 0
	packet[6] = 0
	packet[7] = 0
	// ttl
	packet[8] = 64
	packet[9] = byte(ipProtocol)
	// checksum, set below
	packet[10] = 0
	packet[11] = 0
	copy(packet[12:16], packetSourceIp)
	copy(packet[16:20], packetDestinationIp)
	binary.BigEndian.PutUint16(packet[10:12], checksumFinish(checksumAdd(0, packet[0:Ipv4HeaderSizeWithoutExtensions])))
}

// writes an ipv6 header with no extensions.
// `packet` must be sized to the full packet.
func writeIpv6Header(packet []byte, ipProtocol ipProtocolNumber, packetSourceIp net.IP, packetDestinationIp net.IP) {
	// version 6, traffic class and flow label zero
	packet[0] = 0x60
	packet[1] = 0
	packet[2] = 0
	packet[3] = 0
	binary.BigEndian.PutUint16(packet[4:6], uint16(len(packet)-Ipv6HeaderSize))
	packet[6] = byte(ipProtocol)
	// hop limit
	packet[7] = 64
	copy(packet[8:24], packetSourceIp)
	copy(packet[24:40], packetDestinationIp)
}

// udpReceiveDispatcher replaces one receive channel and one receive goroutine
// per UDP flow with a small set of shared FIFO shards. A flow is permanently
// assigned to one shard, and its socket has exactly one read producer, so its
// callback order is unchanged. The aggregate user-space queue is now
// shardCount*SequenceBufferSize rather than flowCount*SequenceBufferSize.
//
// A callback can still block unrelated flows in the same shard. Four shards
// bound that failure domain without reintroducing per-flow stacks/queues; the
// socket kernel buffers provide the next bounded absorption layer.
const defaultUdpReceiveShardCount = 4

type udpReceiveDispatchItem struct {
	sequence *UdpSequence
	packet   []byte
}

type udpReceiveDispatchShard struct {
	ctx       context.Context
	startOnce sync.Once
	items     chan udpReceiveDispatchItem
	batchSize int
}

type udpReceiveDispatcher struct {
	ctx       context.Context
	shards    []udpReceiveDispatchShard
	nextShard int
	waitGroup sync.WaitGroup
}

func newUdpReceiveDispatcher(ctx context.Context, settings *UdpBufferSettings) *udpReceiveDispatcher {
	shardCount := settings.ReceiveShardCount
	if shardCount <= 0 {
		shardCount = defaultUdpReceiveShardCount
	}
	dispatcher := &udpReceiveDispatcher{
		ctx:    ctx,
		shards: make([]udpReceiveDispatchShard, shardCount),
	}
	for i := range dispatcher.shards {
		dispatcher.shards[i] = udpReceiveDispatchShard{
			ctx:       ctx,
			items:     make(chan udpReceiveDispatchItem, max(0, settings.SequenceBufferSize)),
			batchSize: max(1, settings.WriteBatchSize),
		}
	}
	return dispatcher
}

// assignShard is called under UdpBuffer.mutex when a flow is created.
func (dispatcher *udpReceiveDispatcher) assignShard() int {
	shard := dispatcher.nextShard
	dispatcher.nextShard = (dispatcher.nextShard + 1) % len(dispatcher.shards)
	return shard
}

func (dispatcher *udpReceiveDispatcher) enqueue(sequence *UdpSequence, packet []byte) bool {
	if dispatcher == nil || sequence == nil || len(dispatcher.shards) == 0 {
		return false
	}
	if !sequence.startRetirementOperation() {
		return false
	}
	shard := &dispatcher.shards[sequence.receiveShard]
	shard.startOnce.Do(func() {
		dispatcher.waitGroup.Add(1)
		go HandleError(func() {
			defer dispatcher.waitGroup.Done()
			shard.run()
		})
	})
	select {
	case <-dispatcher.ctx.Done():
		sequence.finishRetirementOperation()
		return false
	case <-sequence.ctx.Done():
		sequence.finishRetirementOperation()
		return false
	case shard.items <- udpReceiveDispatchItem{sequence: sequence, packet: packet}:
		return true
	}
}

// Completion means every receive shard returned its queued packet owners.
// The parent first joins every producer, so no shard can start during Wait.
func (dispatcher *udpReceiveDispatcher) waitForLifecycle() {
	if dispatcher == nil {
		return
	}
	dispatcher.waitGroup.Wait()
	// Cancellation and a buffered send can become ready together. The worker's
	// first drain may therefore finish just before that already-admitted send
	// publishes. Every producer is joined before this method, so one terminal
	// drain closes that select race without polling.
	for i := range dispatcher.shards {
		returnQueuedUdpDispatchItems(dispatcher.shards[i].items)
	}
}

// Releases packet ownership left in a retired receive-dispatch queue.
func returnQueuedUdpDispatchItems(items chan udpReceiveDispatchItem) {
	for {
		select {
		case item := <-items:
			MessagePoolReturn(item.packet)
			item.sequence.finishRetirementOperation()
		default:
			return
		}
	}
}

func (shard *udpReceiveDispatchShard) run() {
	batch := make([][]byte, 0, shard.batchSize)
	var pending udpReceiveDispatchItem
	hasPending := false

	releaseQueued := func() {
		if hasPending {
			MessagePoolReturn(pending.packet)
			pending.sequence.finishRetirementOperation()
			pending = udpReceiveDispatchItem{}
			hasPending = false
		}
		returnQueuedUdpDispatchItems(shard.items)
	}

	for {
		var item udpReceiveDispatchItem
		if hasPending {
			item = pending
			pending = udpReceiveDispatchItem{}
			hasPending = false
		} else {
			select {
			case <-shard.ctx.Done():
				releaseQueued()
				return
			case item = <-shard.items:
			}
		}

		sequence := item.sequence
		batch = append(batch[:0], item.packet)

		// Coalesce only adjacent packets from the same flow. Holding one
		// different-flow item as pending preserves the shard's total FIFO
		// order while retaining the existing two-frame wire batching win.
	fill:
		for len(batch) < cap(batch) {
			select {
			case next := <-shard.items:
				if next.sequence != sequence {
					pending = next
					hasPending = true
					break fill
				}
				batch = append(batch, next.packet)
			default:
				break fill
			}
		}
		func() {
			defer func() {
				for range batch {
					sequence.finishRetirementOperation()
				}
			}()
			sequence.receiveBatch(batch)
		}()
	}
}

type Udp4Buffer struct {
	UdpBuffer[BufferId4]
}

func NewUdp4Buffer(ctx context.Context, receiveCallback ReceivePacketFunction,
	udpBufferSettings *UdpBufferSettings) *Udp4Buffer {
	return newUdp4BufferWithTransferKey(
		ctx,
		receiveTransferPacketAdapter(receiveCallback),
		udpBufferSettings,
	)
}

// Builds the provider form without dropping the transfer lane.
func newUdp4BufferWithTransferKey(
	ctx context.Context,
	receiveCallback receiveTransferPacketFunction,
	udpBufferSettings *UdpBufferSettings,
) *Udp4Buffer {
	return &Udp4Buffer{
		UdpBuffer: *newUdpBuffer[BufferId4](ctx, receiveCallback, udpBufferSettings),
	}
}

func (self *Udp4Buffer) send(source TransferPath, provideMode protocol.ProvideMode,
	udp *parsedUdp, timeout time.Duration, ipPacket []byte) (bool, error) {
	return self.sendTransferKey(
		source,
		TransferKey{},
		provideMode,
		udp,
		timeout,
		ipPacket,
	)
}

// Retains the reply lane separately from the UDP flow identity.
// applyPathMtuFor keys the reverse of `embedded` (a packet this NAT built
// toward the source) the way sendTransferKey keys the flow.
func (self *Udp4Buffer) applyPathMtuFor(source TransferPath, embedded *IpPath, mtu int) bool {
	return self.applyPathMtu(NewBufferId4(
		source.LocalMask(),
		embedded.DestinationIp.To4(), embedded.DestinationPort,
		embedded.SourceIp.To4(), embedded.SourcePort,
	), mtu)
}

func (self *Udp4Buffer) sendTransferKey(
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	udp *parsedUdp,
	timeout time.Duration,
	ipPacket []byte,
) (bool, error) {
	source = source.LocalMask()
	bufferId := NewBufferId4(
		source,
		udp.sourceIp, int(udp.sourcePort),
		udp.destinationIp, int(udp.destinationPort),
	)

	return self.udpSend(
		bufferId,
		source,
		transferKey,
		provideMode,
		4,
		udp,
		timeout,
		ipPacket,
	)
}

type Udp6Buffer struct {
	UdpBuffer[BufferId6]
}

func NewUdp6Buffer(ctx context.Context, receiveCallback ReceivePacketFunction,
	udpBufferSettings *UdpBufferSettings) *Udp6Buffer {
	return newUdp6BufferWithTransferKey(
		ctx,
		receiveTransferPacketAdapter(receiveCallback),
		udpBufferSettings,
	)
}

// Builds the IPv6 provider form without dropping the transfer lane.
func newUdp6BufferWithTransferKey(
	ctx context.Context,
	receiveCallback receiveTransferPacketFunction,
	udpBufferSettings *UdpBufferSettings,
) *Udp6Buffer {
	return &Udp6Buffer{
		UdpBuffer: *newUdpBuffer[BufferId6](ctx, receiveCallback, udpBufferSettings),
	}
}

func (self *Udp6Buffer) send(source TransferPath, provideMode protocol.ProvideMode,
	udp *parsedUdp, timeout time.Duration, ipPacket []byte) (bool, error) {
	return self.sendTransferKey(
		source,
		TransferKey{},
		provideMode,
		udp,
		timeout,
		ipPacket,
	)
}

// Retains the reply lane separately from the IPv6 flow identity.
// applyPathMtuFor keys the reverse of `embedded` (a packet this NAT built
// toward the source) the way sendTransferKey keys the flow.
func (self *Udp6Buffer) applyPathMtuFor(source TransferPath, embedded *IpPath, mtu int) bool {
	return self.applyPathMtu(NewBufferId6(
		source.LocalMask(),
		embedded.DestinationIp.To16(), embedded.DestinationPort,
		embedded.SourceIp.To16(), embedded.SourcePort,
	), mtu)
}

func (self *Udp6Buffer) sendTransferKey(
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	udp *parsedUdp,
	timeout time.Duration,
	ipPacket []byte,
) (bool, error) {
	source = source.LocalMask()
	bufferId := NewBufferId6(
		source,
		udp.sourceIp, int(udp.sourcePort),
		udp.destinationIp, int(udp.destinationPort),
	)

	return self.udpSend(
		bufferId,
		source,
		transferKey,
		provideMode,
		6,
		udp,
		timeout,
		ipPacket,
	)
}

type UdpBuffer[BufferId comparable] struct {
	ctx                            context.Context
	log                            Logger
	receiveCallback                receiveTransferPacketFunction
	receiveTransferPacketsCallback receiveTransferPacketsBatchFunction
	udpBufferSettings              *UdpBufferSettings
	receiveDispatcher              *udpReceiveDispatcher
	socketReadPoller               *udpSocketReadPoller
	sharedLifecycleOnce            sync.Once
	sharedLifecycleWake            chan struct{}
	sharedLifecycleWaitGroup       sync.WaitGroup
	sequenceWaitGroup              sync.WaitGroup

	mutex sync.Mutex

	sequences        map[BufferId]*UdpSequence
	sourceSequences  map[TransferPath]map[BufferId]*UdpSequence
	retiredSourceIds map[Id]bool
}

func newUdpBuffer[BufferId comparable](
	ctx context.Context,
	receiveCallback receiveTransferPacketFunction,
	udpBufferSettings *UdpBufferSettings,
) *UdpBuffer[BufferId] {
	return &UdpBuffer[BufferId]{
		ctx:                 ctx,
		log:                 loggerOrDefault(udpBufferSettings.Log),
		receiveCallback:     receiveCallback,
		udpBufferSettings:   udpBufferSettings,
		receiveDispatcher:   newUdpReceiveDispatcher(ctx, udpBufferSettings),
		sharedLifecycleWake: make(chan struct{}, 1),
		sequences:           map[BufferId]*UdpSequence{},
		sourceSequences:     map[TransferPath]map[BufferId]*UdpSequence{},
		retiredSourceIds:    map[Id]bool{},
	}
}

// applyPathMtu shrinks a live flow's path mtu (see applyPathMtu). false
// when no sequence holds the key.
func (self *UdpBuffer[BufferId]) applyPathMtu(bufferId BufferId, mtu int) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	sequence, ok := self.sequences[bufferId]
	if !ok {
		return false
	}
	sequence.applyPathMtu(mtu)
	return true
}

func (self *UdpBuffer[BufferId]) udpSend(
	bufferId BufferId,
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	ipVersion int,
	udp *parsedUdp,
	timeout time.Duration,
	ipPacket []byte,
) (bool, error) {
	source = source.LocalMask()
	initSequence := func(skip *UdpSequence) *UdpSequence {
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
				self.removeSequenceWithLock(bufferId, sequence)
			}
		}

		if 0 < self.udpBufferSettings.UserLimit {
			// limit the total connections per source to avoid blowing up the ulimit
			if sourceSequences := self.sourceSequences[source]; self.udpBufferSettings.UserLimit <= len(sourceSequences) {
				applyLruMapLimit(sourceSequences, self.udpBufferSettings.UserLimit-1, func(bufferId BufferId, sequence *UdpSequence) bool {
					if self.log.V(1).Enabled() {
						self.log.Infof(
							"[lnr]udp limit source %s->%s\n",
							source,
							net.JoinHostPort(
								sequence.destinationIp.String(),
								strconv.Itoa(int(sequence.destinationPort)),
							),
						)
					}
					self.removeSequenceWithLock(bufferId, sequence)
					return true
				})
			}
		}
		if 0 < self.udpBufferSettings.GlobalLimit {
			// limit the total connections across all sources, an aggregate
			// flow state and fd ceiling
			if self.udpBufferSettings.GlobalLimit <= len(self.sequences) {
				applyLruMapLimit(self.sequences, self.udpBufferSettings.GlobalLimit-1, func(bufferId BufferId, sequence *UdpSequence) bool {
					if self.log.V(1).Enabled() {
						self.log.Infof(
							"[lnr]udp limit global %s->%s\n",
							sequence.source,
							net.JoinHostPort(
								sequence.destinationIp.String(),
								strconv.Itoa(int(sequence.destinationPort)),
							),
						)
					}
					self.removeSequenceWithLock(bufferId, sequence)
					return true
				})
			}
		}

		// TODO
		// limit the number of new connections per second per source
		// self.sourceLimiter[source].Limit()

		sourceIpCopy := make(net.IP, len(udp.sourceIp))
		copy(sourceIpCopy, udp.sourceIp)

		destinationIpCopy := make(net.IP, len(udp.destinationIp))
		copy(destinationIpCopy, udp.destinationIp)

		sequence = newUdpSequenceWithTransferKey(
			self.ctx,
			self.receiveCallback,
			source,
			transferKey,
			provideMode,
			ipVersion,
			sourceIpCopy,
			udp.sourcePort,
			destinationIpCopy,
			udp.destinationPort,
			self.udpBufferSettings,
		)
		sequence.receiveTransferPacketsCallback = self.receiveTransferPacketsCallback
		sequence.receiveDispatcher = self.receiveDispatcher
		sequence.receiveShard = self.receiveDispatcher.assignShard()
		if self.socketReadPoller == nil {
			// The buffer mutex serializes this lazy construction. A TCP-only
			// provider and an unused address family retain no poll descriptors,
			// worker stacks, or scheduler wakeups.
			self.socketReadPoller = newUdpSocketReadPoller(self.ctx, self.udpBufferSettings)
		}
		sequence.socketReadPoller = self.socketReadPoller
		sequence.sharedSocketLifecycle =
			self.socketReadPoller != nil && self.udpBufferSettings.SharedSocketLifecycle
		sequence.sharedLifecycleWake = self.sharedLifecycleWake
		self.sequences[bufferId] = sequence
		sourceSequences := self.sourceSequences[source]
		if sourceSequences == nil {
			sourceSequences = map[BufferId]*UdpSequence{}
			self.sourceSequences[source] = sourceSequences
		}
		sourceSequences[bufferId] = sequence
		if sequence.sharedSocketLifecycle {
			sequence.sharedSocketLifecycle = sequence.startSharedSocket()
		}
		if sequence.sharedSocketLifecycle {
			self.sharedLifecycleOnce.Do(func() {
				self.sharedLifecycleWaitGroup.Add(1)
				go HandleError(func() {
					defer self.sharedLifecycleWaitGroup.Done()
					self.runSharedSocketLifecycle()
				})
			})
			self.wakeSharedSocketLifecycle()
		} else {
			sequence.ensureSendItems()
			self.sequenceWaitGroup.Add(1)
			go HandleError(func() {
				defer self.sequenceWaitGroup.Done()
				defer func() {
					sequence.Close()
					<-sequence.retirementOperations.Done()
					close(sequence.retirementDone)
				}()
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
		}
		return sequence
	}

	sendItem := &UdpSendItem{
		source:      source,
		transferKey: transferKey,
		provideMode: provideMode,
		udp:         *udp,
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

// Completion means every sequence, readiness reader, lifecycle worker, and
// receive-dispatch shard has released its queued packet owners. The local NAT
// joins its send dispatcher before calling this method, so sequence admission
// is already terminal.
func (self *UdpBuffer[BufferId]) waitForLifecycle() {
	self.sequenceWaitGroup.Wait()
	self.sharedLifecycleWaitGroup.Wait()
	if self.socketReadPoller != nil {
		self.socketReadPoller.waitForLifecycle()
	}
	self.receiveDispatcher.waitForLifecycle()
}

// removeSequenceWithLock removes a UDP sequence from both indexes before
// canceling it. The sequence goroutine's deferred cleanup is identity-checked,
// so eager removal is safe and makes the configured cap exact under bursts.
// The caller holds mutex.
func (self *UdpBuffer[BufferId]) removeSequenceWithLock(bufferId BufferId, sequence *UdpSequence) {
	if self.sequences[bufferId] != sequence {
		return
	}
	delete(self.sequences, bufferId)
	sourceSequences := self.sourceSequences[sequence.source]
	delete(sourceSequences, bufferId)
	if len(sourceSequences) == 0 {
		delete(self.sourceSequences, sequence.source)
	}
	sequence.Close()
}

// Applies one exact provider-source tombstone and cancels every matching UDP
// socket generation before allowing the retirement worker to join it.
func (self *UdpBuffer[BufferId]) setSourceRetired(
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
			doneChannels = append(doneChannels, sequence.sourceRetirementDone())
			self.removeSequenceWithLock(bufferId, sequence)
		}
	}
	return doneChannels
}

func (self *UdpBuffer[BufferId]) wakeSharedSocketLifecycle() {
	select {
	case self.sharedLifecycleWake <- struct{}{}:
	default:
	}
}

func udpSequenceDone(sequence *UdpSequence) bool {
	if sequence == nil {
		return true
	}
	select {
	case <-sequence.ctx.Done():
		return true
	default:
		return false
	}
}

// runSharedSocketLifecycle replaces one idle timer and one parked send loop per
// readiness-managed UDP flow. Activity can only move a deadline later, so the
// worker needs a wakeup for construction/close but not for every packet; an
// early timer simply rescans and rearms to the new minimum.
func (self *UdpBuffer[BufferId]) runSharedSocketLifecycle() {
	var idleTimer *time.Timer
	var idleTimerChannel <-chan time.Time
	stopTimer := func() {
		if idleTimer == nil || !idleTimer.Stop() {
			return
		}
	}
	defer stopTimer()

	for {
		select {
		case <-self.ctx.Done():
			self.mutex.Lock()
			for bufferId, sequence := range self.sequences {
				self.removeSequenceWithLock(bufferId, sequence)
			}
			self.mutex.Unlock()
			return
		case <-self.sharedLifecycleWake:
		case <-idleTimerChannel:
		}

		now := time.Now()
		var nextDeadline time.Time
		self.mutex.Lock()
		for bufferId, sequence := range self.sequences {
			if !sequence.sharedSocketLifecycle {
				continue
			}
			deadline := sequence.LastActivityTime().Add(self.udpBufferSettings.IdleTimeout)
			if udpSequenceDone(sequence) || !now.Before(deadline) {
				self.removeSequenceWithLock(bufferId, sequence)
				continue
			}
			if nextDeadline.IsZero() || deadline.Before(nextDeadline) {
				nextDeadline = deadline
			}
		}
		self.mutex.Unlock()

		if nextDeadline.IsZero() || self.udpBufferSettings.IdleTimeout <= 0 {
			stopTimer()
			idleTimerChannel = nil
			continue
		}
		delay := max(time.Until(nextDeadline), time.Millisecond)
		if idleTimer == nil {
			idleTimer = time.NewTimer(delay)
		} else {
			stopTimer()
			idleTimer.Reset(delay)
		}
		idleTimerChannel = idleTimer.C
	}
}

type UdpSequence struct {
	ctx                            context.Context
	cancel                         context.CancelFunc
	log                            Logger
	receiveCallback                receiveTransferPacketFunction
	receiveTransferPacketsCallback receiveTransferPacketsBatchFunction
	receiveDispatcher              *udpReceiveDispatcher
	receiveShard                   int
	socketReadPoller               *udpSocketReadPoller
	socketReadPollShard            int
	socketReadPollFd               int
	sharedSocketLifecycle          bool
	sharedLifecycleWake            chan struct{}
	udpBufferSettings              *UdpBufferSettings
	sharedSocket                   net.Conn
	sharedSocketErr                error
	closeOnce                      sync.Once
	retirementOperations           *lifecycleAdmission
	retirementDone                 chan struct{}

	sendMutex sync.Mutex
	sendItems chan *UdpSendItem
	// Lazily allocated only when the bounded send queue actually blocks.
	// sendMutex serializes Reset/Stop.
	sendTimer *time.Timer

	idleCondition *IdleCondition
	transferState transferState

	StreamState
}

func NewUdpSequence(ctx context.Context, receiveCallback ReceivePacketFunction,
	source TransferPath,
	provideMode protocol.ProvideMode,
	ipVersion int,
	sourceIp net.IP, sourcePort uint16,
	destinationIp net.IP, destinationPort uint16,
	udpBufferSettings *UdpBufferSettings) *UdpSequence {
	return newUdpSequenceWithTransferKey(
		ctx,
		receiveTransferPacketAdapter(receiveCallback),
		source,
		TransferKey{},
		provideMode,
		ipVersion,
		sourceIp,
		sourcePort,
		destinationIp,
		destinationPort,
		udpBufferSettings,
	)
}

// Builds one flow while separating its reply lane from its socket identity.
func newUdpSequenceWithTransferKey(
	ctx context.Context,
	receiveCallback receiveTransferPacketFunction,
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	ipVersion int,
	sourceIp net.IP,
	sourcePort uint16,
	destinationIp net.IP,
	destinationPort uint16,
	udpBufferSettings *UdpBufferSettings,
) *UdpSequence {
	source = source.LocalMask()
	cancelCtx, cancel := context.WithCancel(ctx)
	var sendItems chan *UdpSendItem
	if !udpBufferSettings.SharedSocketLifecycle {
		sendItems = make(chan *UdpSendItem, udpBufferSettings.SequenceBufferSize)
	}
	return &UdpSequence{
		ctx:                  cancelCtx,
		cancel:               cancel,
		retirementOperations: newLifecycleAdmission(),
		retirementDone:       make(chan struct{}),
		log:                  loggerOrDefault(udpBufferSettings.Log),
		receiveCallback:      receiveCallback,
		sendItems:            sendItems,
		udpBufferSettings:    udpBufferSettings,
		idleCondition:        NewIdleCondition(),
		socketReadPollFd:     -1,
		transferState:        newTransferState(source, transferKey),
		StreamState: StreamState{
			source:          source,
			provideMode:     provideMode,
			ipVersion:       ipVersion,
			sourceIp:        sourceIp,
			sourcePort:      sourcePort,
			destinationIp:   destinationIp,
			destinationPort: destinationPort,
			userLimited: userLimited{
				lastActivityTime: time.Now(),
			},
		},
	}
}

func (self *UdpSequence) ensureSendItems() {
	if self.sendItems == nil {
		self.sendItems = make(chan *UdpSendItem, self.udpBufferSettings.SequenceBufferSize)
	}
}

// Direct low-level fixtures may omit retirement tracking. Constructor-owned
// sequences always have it; the nil form preserves their historical behavior.
func (self *UdpSequence) startRetirementOperation() bool {
	return self != nil &&
		(self.retirementOperations == nil || self.retirementOperations.start())
}

func (self *UdpSequence) finishRetirementOperation() {
	if self != nil && self.retirementOperations != nil {
		self.retirementOperations.finish()
	}
}

func (self *UdpSequence) send(sendItem *UdpSendItem, timeout time.Duration) (bool, error) {
	if !self.startRetirementOperation() {
		return false, errors.New("Done.")
	}
	defer self.finishRetirementOperation()
	if self.sharedSocketLifecycle {
		return self.sendSharedSocket(sendItem)
	}
	self.sendMutex.Lock()
	defer self.sendMutex.Unlock()

	select {
	case <-self.ctx.Done():
		return false, errors.New("Done.")
	default:
	}
	self.transferState.update(sendItem.source, sendItem.transferKey)

	if !self.idleCondition.UpdateOpen() {
		return false, nil
	}
	defer self.idleCondition.UpdateClose()

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

func (self *UdpSequence) sendSharedSocket(sendItem *UdpSendItem) (bool, error) {
	self.sendMutex.Lock()
	defer self.sendMutex.Unlock()
	select {
	case <-self.ctx.Done():
		if self.sharedSocketErr != nil {
			return false, self.sharedSocketErr
		}
		return false, errors.New("Done.")
	default:
	}
	if self.sharedSocket == nil {
		return false, errors.New("udp shared socket unavailable")
	}

	self.transferState.update(sendItem.source, sendItem.transferKey)
	payload := sendItem.udp.payload
	if len(payload) != 0 {
		if err := self.sharedSocket.SetWriteDeadline(
			time.Now().Add(self.udpBufferSettings.WriteTimeout),
		); err != nil {
			self.sharedSocketErr = err
			self.Close()
			return false, err
		}
		n, err := self.sharedSocket.Write(payload)
		if err != nil {
			self.sharedSocketErr = err
			self.Close()
			return false, err
		}
		if n != len(payload) {
			err = io.ErrShortWrite
			self.sharedSocketErr = err
			self.Close()
			return false, err
		}
		self.UpdateLastActivityTime()
	}
	MessagePoolReturn(sendItem.ipPacket)
	return true, nil
}

func (self *UdpSequence) receivePacket(packet []byte) {
	source, transferKey := self.transferState.get()
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

// receiveBatch delivers a flow's drained batch in one call when a batch
// consumer is registered (coalesced into one wire Pack), else per packet. The
// dispatcher owns the buffers; the consumer shares read-only what it retains.
func (self *UdpSequence) receiveBatch(packets [][]byte) {
	source, transferKey := self.transferState.get()
	if self.receiveTransferPacketsCallback != nil &&
		self.receiveTransferPacketsCallback(
			source,
			transferKey,
			self.provideMode,
			receiveRecoveryModeNonblocking,
			self.IpPath(),
			packets,
		) {
		for _, packet := range packets {
			MessagePoolReturn(packet)
		}
		return
	}
	for _, packet := range packets {
		self.receivePacket(packet)
	}
}

func (self *UdpSequence) openSocket() (net.Conn, error) {
	self.log.V(2).Infof("[init]udp connect\n")
	socket, err := self.udpBufferSettings.DialContext(
		self.ctx,
		"udp",
		self.IpPath().DestinationHostPort(),
	)
	if err != nil {
		if self.log.V(1).Enabled() {
			self.log.Infof("[init]udp connect error = %s\n", err)
		}
		// A UDP dial cannot yield a meaningful refusal to the inner source.
		if _, signal := classifyDialFailure(self.IpPath(), err); signal != nil {
			self.receivePacket(MessagePoolCopy(signal))
		}
		return nil, err
	}
	self.UpdateLastActivityTime()
	self.log.V(2).Infof("[init]connect success\n")
	if udpConn, ok := socket.(*net.UDPConn); ok {
		// The OS may cap these requests at its configured limits.
		udpConn.SetReadBuffer(int(self.udpBufferSettings.MaxWindowSize))
		udpConn.SetWriteBuffer(int(self.udpBufferSettings.MaxWindowSize))
	}
	return socket, nil
}

func (self *UdpSequence) startSharedSocket() bool {
	socket, err := self.openSocket()
	if err != nil {
		self.sharedSocketErr = err
		self.cancel()
		return true
	}
	self.sharedSocket = socket
	if self.socketReadPoller == nil || !self.socketReadPoller.register(self, socket) {
		_ = socket.Close()
		self.sharedSocket = nil
		return false
	}
	return true
}

func (self *UdpSequence) Run() {
	var childWorkers sync.WaitGroup
	defer childWorkers.Wait()
	self.ensureSendItems()
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

	socket, err := self.openSocket()
	if err != nil {
		return
	}
	defer socket.Close()
	// f, _ := udpConn.File()
	// fd := SocketHandle(f.Fd())
	// syscall.SetsockoptInt(fd, syscall.IPPROTO_IP, syscall.IP_MTU, self.udpBufferSettings.Mtu)

	readSocket := func() {
		// The dispatcher owns every successfully enqueued packet. Canceling
		// the sequence closes the socket/main send loop, but already queued
		// packets remain valid and drain in shard FIFO order.
		defer self.cancel()

		buffer := make([]byte, self.udpBufferSettings.ReadBufferByteCount)

		for forwardIter := uint64(0); ; forwardIter += 1 {
			select {
			case <-self.ctx.Done():
				return
			default:
			}

			readTimeout := time.Now().Add(self.udpBufferSettings.ReadTimeout)
			socket.SetReadDeadline(readTimeout)
			n, err := socket.Read(buffer)

			if err != nil {
				if self.log.V(1).Enabled() {
					self.log.Infof("[f%d]udp receive err = %s\n", forwardIter, err)
				}
			}

			if 0 < n {
				self.UpdateLastActivityTime()

				packets, packetsErr := self.DataPackets(buffer, n, self.udpBufferSettings.Mtu)
				if packetsErr != nil {
					self.log.Infof("[f%d]udp receive packets error = %s\n", forwardIter, packetsErr)
					return
				}
				if 1 < len(packets) {
					if self.log.V(2).Enabled() {
						self.log.Infof("[f%d]udp receive segemented packets = %d\n", forwardIter, len(packets))
					}
				}
				for _, packet := range packets {
					if self.log.V(1).Enabled() {
						self.log.Infof("[f%d]udp receive %d\n", forwardIter, len(packet))
					}
					if self.receiveDispatcher == nil {
						// Directly constructed sequences (primarily focused
						// tests) retain synchronous ordered delivery.
						self.singleDataPacket[0] = packet
						self.receiveBatch(self.singleDataPacket[:])
					} else if !self.receiveDispatcher.enqueue(self, packet) {
						MessagePoolReturn(packet)
					}
				}
			}

			if err != nil {
				if err == io.EOF {
					return
				} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					if self.log.V(1).Enabled() {
						self.log.Infof("[f%d]timeout\n", forwardIter)
					}
					return
				} else {
					// some other error
					return
				}
			}
		}
	}
	if self.socketReadPoller != nil && self.socketReadPoller.register(self, socket) {
		defer self.socketReadPoller.unregister(self)
	} else {
		childWorkers.Add(1)
		go HandleError(func() {
			defer childWorkers.Done()
			readSocket()
		}, self.cancel)
	}

	sendIter := uint64(0)
	// The sequence goroutine owns the outbound socket writes directly. The
	// former write worker plus writePayloads channel duplicated both one
	// goroutine and one SequenceBufferSize queue per UDP flow. sendItems is
	// already the bounded producer/consumer queue, and net.Conn permits one
	// concurrent reader plus one writer, so the extra stage added memory,
	// scheduling, and a handoff without adding parallelism.
	writeBatch := make([]*UdpSendItem, 0, self.udpBufferSettings.WriteBatchSize)
	writeSendItems := func(first *UdpSendItem) bool {
		writeBatch = append(writeBatch[:0], first)
	drain:
		for len(writeBatch) < cap(writeBatch) {
			select {
			case sendItem, ok := <-self.sendItems:
				if !ok {
					break drain
				}
				writeBatch = append(writeBatch, sendItem)
			default:
				break drain
			}
		}

		socket.SetWriteDeadline(time.Now().Add(self.udpBufferSettings.WriteTimeout))
		var writeErr error
		for _, sendItem := range writeBatch {
			payload := sendItem.udp.payload
			if writeErr == nil && 0 < len(payload) {
				// Each payload is one datagram; writes cannot be coalesced, but
				// the drained batch shares one deadline and one scheduler wake.
				n, err := socket.Write(payload)
				if err == nil {
					if self.log.V(2).Enabled() {
						self.log.Infof("[f%d]udp forward %d\n", sendIter, n)
					}
				} else if self.log.V(1).Enabled() {
					self.log.Infof("[f%d]udp forward %d error = %s", sendIter, n, err)
				}
				if 0 < n {
					self.UpdateLastActivityTime()
				}
				writeErr = err
			}
			MessagePoolReturn(sendItem.ipPacket)
			sendIter += 1
		}
		return writeErr == nil
	}

	// reusable idle timer: this send loop wakes per datagram, so a per-iteration
	// time.After allocated a timer per packet (the dominant alloc in the udp
	// egress profile). hot-path timer reuse per CODESTYLE.
	idleTimer := time.NewTimer(0)
	defer idleTimer.Stop()

	for {
		checkpointId := self.idleCondition.Checkpoint()
		idleTimer.Reset(self.udpBufferSettings.IdleTimeout)
		select {
		case <-self.ctx.Done():
			return
		case sendItem, ok := <-self.sendItems:
			if !ok {
				return
			}
			if !writeSendItems(sendItem) {
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
			// else there pending updates
		}
	}
}

func (self *UdpSequence) Cancel() {
	self.Close()
}

func (self *UdpSequence) Close() {
	self.closeOnce.Do(func() {
		self.retirementOperations.close()
		self.cancel()
		if self.sharedSocketLifecycle {
			if self.socketReadPoller != nil {
				self.socketReadPoller.unregister(self)
			}
			if self.sharedSocket != nil {
				_ = self.sharedSocket.Close()
			}
			select {
			case self.sharedLifecycleWake <- struct{}{}:
			default:
			}
		}
	})
}

// Completion includes every send and queued receive callback. Nonshared
// sequences also wait for their socket worker and terminal queue drain.
func (self *UdpSequence) sourceRetirementDone() <-chan struct{} {
	if self.sharedSocketLifecycle {
		return self.retirementOperations.Done()
	}
	return self.retirementDone
}

type UdpSendItem struct {
	source      TransferPath
	transferKey TransferKey
	provideMode protocol.ProvideMode
	udp         parsedUdp
	ipPacket    []byte
}

type StreamState struct {
	source          TransferPath
	provideMode     protocol.ProvideMode
	ipVersion       int
	sourceIp        net.IP
	sourcePort      uint16
	destinationIp   net.IP
	destinationPort uint16
	userLimited

	// cached immutable ip path for this stream (see IpPath). primed by the
	// first call, which happens at sequence setup (DialContext) before the
	// per-packet goroutines start, so it is written once and then read-only.
	ipPath *IpPath

	// reusable backing for the common single-datagram DataPackets result.
	// DataPackets is called from one goroutine and its result is consumed
	// before the next call, so the backing can be reused; fragmented payloads
	// allocate a fresh slice.
	singleDataPacket [1][]byte

	// pathMtu is the largest packet the source has told us its path toward
	// it can carry (icmp fragmentation-needed / packet-too-big), or zero when
	// nothing has been learned. Read by DataPackets, written from the NAT
	// dispatch goroutine, hence atomic.
	pathMtu atomic.Int32
}

// clampPathMtu lowers the configured mtu to a learned path mtu.
func (self *StreamState) clampPathMtu(mtu int) int {
	if pathMtu := int(self.pathMtu.Load()); 0 < pathMtu && pathMtu < mtu {
		return pathMtu
	}
	return mtu
}

// applyPathMtu records a smaller path mtu learned from the source. Never
// grows an existing value, and never goes below the family minimum.
func (self *StreamState) applyPathMtu(mtu int) {
	mtu = max(mtu, ipMinimumPathMtu(self.ipVersion))
	for {
		current := self.pathMtu.Load()
		if current != 0 && int(current) <= mtu {
			return
		}
		if self.pathMtu.CompareAndSwap(current, int32(mtu)) {
			return
		}
	}
}

// IpPath returns the immutable ip path for this stream. The path is built once
// and cached; the stream identity (version, ips, ports) never changes.
func (self *StreamState) IpPath() *IpPath {
	if self.ipPath == nil {
		self.ipPath = &IpPath{
			Version:         self.ipVersion,
			Protocol:        IpProtocolUdp,
			SourceIp:        self.sourceIp,
			SourcePort:      int(self.sourcePort),
			DestinationIp:   self.destinationIp,
			DestinationPort: int(self.destinationPort),
		}
	}
	return self.ipPath
}

// this must only be called from one goroutine
// this is called from the writer only and does not need to syncrhronize with the reader state
func (self *StreamState) DataPackets(payload []byte, n int, mtu int) ([][]byte, error) {
	if n < 0 || len(payload) < n {
		return nil, errors.New("invalid UDP payload length")
	}
	var headerByteCount int
	switch self.ipVersion {
	case 4:
		headerByteCount = Ipv4HeaderSizeWithoutExtensions + UdpHeaderSize
	case 6:
		headerByteCount = Ipv6HeaderSize + UdpHeaderSize
	default:
		return nil, errors.New("unsupported IP version")
	}
	if 0xffff-UdpHeaderSize < n {
		return nil, errors.New("UDP payload exceeds the datagram limit")
	}
	if 0xffff-Ipv4HeaderSizeWithoutExtensions-UdpHeaderSize < n && self.ipVersion == 4 {
		return nil, errors.New("UDP payload exceeds the IPv4 packet limit")
	}
	mtu = self.clampPathMtu(mtu)
	if n <= mtu-headerByteCount {
		// reuse the single-packet backing for the common unfragmented case
		// (see singleDataPacket); the result is consumed before the next call.
		self.singleDataPacket[0] = self.udpPacket(payload[0:n])
		return self.singleDataPacket[:], nil
	}
	// splitting one UDP payload into several independent datagrams would
	// corrupt application framing, so an oversized datagram is fragmented at
	// the ip layer in both families and reassembled by the source stack
	if self.ipVersion == 6 {
		return fragmentIpv6Packet(self.udpPacket(payload[:n]), mtu)
	}
	return fragmentIpv4Packet(self.udpPacket(payload[:n]), mtu)
}

// builds a udp packet from the stream destination to the stream source
// into a single pool buffer
func (self *StreamState) udpPacket(payload []byte) []byte {
	var ipHeaderByteCount int
	switch self.ipVersion {
	case 4:
		ipHeaderByteCount = Ipv4HeaderSizeWithoutExtensions
	case 6:
		ipHeaderByteCount = Ipv6HeaderSize
	}

	packet := MessagePoolGet(ipHeaderByteCount + UdpHeaderSize + len(payload))
	switch self.ipVersion {
	case 4:
		writeIpv4Header(packet, ipProtocolNumberUdp, self.destinationIp, self.sourceIp)
	case 6:
		writeIpv6Header(packet, ipProtocolNumberUdp, self.destinationIp, self.sourceIp)
	}

	udp := packet[ipHeaderByteCount:]
	binary.BigEndian.PutUint16(udp[0:2], uint16(self.destinationPort))
	binary.BigEndian.PutUint16(udp[2:4], uint16(self.sourcePort))
	binary.BigEndian.PutUint16(udp[4:6], uint16(UdpHeaderSize+len(payload)))
	// checksum, set below
	udp[6] = 0
	udp[7] = 0
	copy(udp[UdpHeaderSize:], payload)
	checksum := transportChecksum(ipProtocolNumberUdp, self.destinationIp, self.sourceIp, udp)
	if checksum == 0 {
		// zero means no checksum
		checksum = 0xffff
	}
	binary.BigEndian.PutUint16(udp[6:8], checksum)
	return packet
}

type TcpBufferSettings struct {
	// nil resolves to the local user nat `Log`
	Log Logger
	// ConnectTimeout     time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	// coalesce pure acks for up to this duration.
	// an ack is sent sooner when the unacked byte count reaches half the window.
	// zero sends a pure ack on every send seq advance.
	AckCompressTimeout time.Duration
	// ReadPollTimeout time.Duration
	// WritePollTimeout time.Duration
	IdleTimeout         time.Duration
	ReadBufferByteCount int
	// the maximum number of payloads to coalesce into a single socket write
	WriteBatchSize     int
	SequenceBufferSize int
	Mtu                int
	// the window size is the max amount of packet data in memory for each sequence
	// `WindowSize / 2^WindowScale` must fit in uint16
	// see https://datatracker.ietf.org/doc/html/rfc1323#page-8
	WindowScale uint32
	// the minimum window size after backpressure-driven contraction
	MinWindowSize uint32
	// the initial window after the literal, unscaled SYN handshake window
	InitialWindowSize uint32
	// `MaxWindowSize` should be a power of 2 multiple of `MinWindowSize`
	MaxWindowSize uint32
	// the number of open sockets per user
	// uses an lru cleanup where new sockets over the limit close old sockets
	UserLimit int
	// the number of open sockets across all users (per address family
	// buffer), an aggregate flow state and fd ceiling for hosts with a hard
	// process memory budget. uses the same lru cleanup as `UserLimit`.
	// 0 (the default) is no limit.
	GlobalLimit int
	// EnableOrphanRst replies with a RST to a non-SYN packet that matches no
	// sequence (PROXYDRAIN1.md §3.5). Without it a source whose flow state
	// was lost here (e.g. the source client restarted with a fresh identity,
	// orphaning its provider-side flows) retransmits into silence and the
	// application hangs to its own timeout; the RST makes it reconnect
	// immediately. Rate limited by `OrphanRstPerSecond`.
	EnableOrphanRst bool
	// OrphanRstPerSecond bounds orphan RST generation per buffer (an abuse
	// valve: orphan packets are attacker-influenceable). <= 0 is unlimited.
	OrphanRstPerSecond int

	// EnableSyntheticSpeed terminates TCP flows to the RFC 2544 benchmark
	// range 198.18.0.0/15 at an in-memory synthetic speed server instead of
	// dialing upstream (see ip_synthetic_speed.go). The range is never
	// publicly routable, so no real destination is shadowed. Serves
	// measurement of the tunnel path itself, isolated from origin and
	// upstream network variability.
	EnableSyntheticSpeed bool
	// Tests may hold a newly admitted sequence before it can consume its first
	// pooled packet. Nil is a production no-op.
	beforeSequenceRunForTest func()

	ConnectSettings
}

type Tcp4Buffer struct {
	TcpBuffer[BufferId4]
}

func NewTcp4Buffer(ctx context.Context, receiveCallback ReceivePacketFunction,
	tcpBufferSettings *TcpBufferSettings) *Tcp4Buffer {
	return newTcp4BufferWithTransferKey(
		ctx,
		receiveTransferPacketAdapter(receiveCallback),
		tcpBufferSettings,
	)
}

// Builds the provider form without dropping the transfer lane.
func newTcp4BufferWithTransferKey(
	ctx context.Context,
	receiveCallback receiveTransferPacketFunction,
	tcpBufferSettings *TcpBufferSettings,
) *Tcp4Buffer {
	return &Tcp4Buffer{
		TcpBuffer: *newTcpBuffer[BufferId4](ctx, receiveCallback, tcpBufferSettings),
	}
}

func (self *Tcp4Buffer) send(source TransferPath, provideMode protocol.ProvideMode,
	tcp *parsedTcp, timeout time.Duration, ipPacket []byte) (bool, error) {
	return self.sendTransferKey(
		source,
		TransferKey{},
		provideMode,
		tcp,
		timeout,
		ipPacket,
	)
}

// Retains the reply lane separately from the TCP flow identity.
// applyPathMtuFor keys the reverse of `embedded` (a packet this NAT built
// toward the source) the way sendTransferKey keys the flow.
func (self *Tcp4Buffer) applyPathMtuFor(source TransferPath, embedded *IpPath, mtu int) bool {
	return self.applyPathMtu(NewBufferId4(
		source.LocalMask(),
		embedded.DestinationIp.To4(), embedded.DestinationPort,
		embedded.SourceIp.To4(), embedded.SourcePort,
	), mtu)
}

func (self *Tcp4Buffer) sendTransferKey(
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	tcp *parsedTcp,
	timeout time.Duration,
	ipPacket []byte,
) (bool, error) {
	source = source.LocalMask()
	bufferId := NewBufferId4(
		source,
		tcp.sourceIp, int(tcp.sourcePort),
		tcp.destinationIp, int(tcp.destinationPort),
	)

	return self.tcpSend(
		bufferId,
		source,
		transferKey,
		provideMode,
		4,
		tcp,
		timeout,
		ipPacket,
	)
}

type Tcp6Buffer struct {
	TcpBuffer[BufferId6]
}

func NewTcp6Buffer(ctx context.Context, receiveCallback ReceivePacketFunction,
	tcpBufferSettings *TcpBufferSettings) *Tcp6Buffer {
	return newTcp6BufferWithTransferKey(
		ctx,
		receiveTransferPacketAdapter(receiveCallback),
		tcpBufferSettings,
	)
}

// Builds the IPv6 provider form without dropping the transfer lane.
func newTcp6BufferWithTransferKey(
	ctx context.Context,
	receiveCallback receiveTransferPacketFunction,
	tcpBufferSettings *TcpBufferSettings,
) *Tcp6Buffer {
	return &Tcp6Buffer{
		TcpBuffer: *newTcpBuffer[BufferId6](ctx, receiveCallback, tcpBufferSettings),
	}
}

func (self *Tcp6Buffer) send(source TransferPath, provideMode protocol.ProvideMode,
	tcp *parsedTcp, timeout time.Duration, ipPacket []byte) (bool, error) {
	return self.sendTransferKey(
		source,
		TransferKey{},
		provideMode,
		tcp,
		timeout,
		ipPacket,
	)
}

// Retains the reply lane separately from the IPv6 flow identity.
// applyPathMtuFor keys the reverse of `embedded` (a packet this NAT built
// toward the source) the way sendTransferKey keys the flow.
func (self *Tcp6Buffer) applyPathMtuFor(source TransferPath, embedded *IpPath, mtu int) bool {
	return self.applyPathMtu(NewBufferId6(
		source.LocalMask(),
		embedded.DestinationIp.To16(), embedded.DestinationPort,
		embedded.SourceIp.To16(), embedded.SourcePort,
	), mtu)
}

func (self *Tcp6Buffer) sendTransferKey(
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	tcp *parsedTcp,
	timeout time.Duration,
	ipPacket []byte,
) (bool, error) {
	source = source.LocalMask()
	bufferId := NewBufferId6(
		source,
		tcp.sourceIp, int(tcp.sourcePort),
		tcp.destinationIp, int(tcp.destinationPort),
	)

	return self.tcpSend(
		bufferId,
		source,
		transferKey,
		provideMode,
		6,
		tcp,
		timeout,
		ipPacket,
	)
}

type TcpBuffer[BufferId comparable] struct {
	log                            Logger
	ctx                            context.Context
	receiveCallback                receiveTransferPacketFunction
	receiveTransferPacketsCallback receiveTransferPacketsBatchFunction
	tcpBufferSettings              *TcpBufferSettings
	flowCloseCallback              tcpFlowCloseFunction

	mutex sync.Mutex
	// The local NAT closes send admission before waiting, so no Add can race
	// the terminal Wait.
	sequenceWaitGroup sync.WaitGroup

	sequences        map[BufferId]*TcpSequence
	sourceSequences  map[TransferPath]map[BufferId]*TcpSequence
	retiredSourceIds map[Id]bool

	// orphan rst rate limiting (guarded by mutex; see EnableOrphanRst)
	orphanRstWindowStart time.Time
	orphanRstWindowCount int
}

func newTcpBuffer[BufferId comparable](
	ctx context.Context,
	receiveCallback receiveTransferPacketFunction,
	tcpBufferSettings *TcpBufferSettings,
) *TcpBuffer[BufferId] {
	return &TcpBuffer[BufferId]{
		log:               loggerOrDefault(tcpBufferSettings.Log),
		ctx:               ctx,
		receiveCallback:   receiveCallback,
		tcpBufferSettings: tcpBufferSettings,
		sequences:         map[BufferId]*TcpSequence{},
		sourceSequences:   map[TransferPath]map[BufferId]*TcpSequence{},
		retiredSourceIds:  map[Id]bool{},
	}
}

// applyPathMtu shrinks a live flow's path mtu (see applyPathMtu). false
// when no sequence holds the key.
func (self *TcpBuffer[BufferId]) applyPathMtu(bufferId BufferId, mtu int) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	sequence, ok := self.sequences[bufferId]
	if !ok {
		return false
	}
	sequence.applyPathMtu(mtu)
	return true
}

// allowOrphanRstWithLock applies the orphan rst rate limit. The caller must
// hold `mutex`.
func (self *TcpBuffer[BufferId]) allowOrphanRstWithLock() bool {
	limit := self.tcpBufferSettings.OrphanRstPerSecond
	if limit <= 0 {
		return true
	}
	now := time.Now()
	if 1*time.Second <= now.Sub(self.orphanRstWindowStart) {
		self.orphanRstWindowStart = now
		self.orphanRstWindowCount = 0
	}
	if limit <= self.orphanRstWindowCount {
		return false
	}
	self.orphanRstWindowCount += 1
	return true
}

func (self *TcpBuffer[BufferId]) tcpSend(
	bufferId BufferId,
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	ipVersion int,
	tcp *parsedTcp,
	timeout time.Duration,
	ipPacket []byte,
) (bool, error) {
	source = source.LocalMask()
	var orphanRst []byte
	var orphanSourceIp net.IP
	var orphanDestinationIp net.IP
	initSequence := func() *TcpSequence {
		self.mutex.Lock()
		defer self.mutex.Unlock()
		if self.retiredSourceIds[source.SourceId] {
			return nil
		}

		if sequence, ok := self.sequences[bufferId]; ok {
			if tcp.rst {
				// drop the packet
				sequence.Cancel()
				delete(self.sequences, bufferId)
				sourceSequences := self.sourceSequences[sequence.source]
				delete(sourceSequences, bufferId)
				if 0 == len(sourceSequences) {
					delete(self.sourceSequences, sequence.source)
				}
				// Return false without consuming ipPacket; the LocalUserNat
				// dispatcher owns and returns every packet a sequence does not
				// accept.
				return nil
			}
			if !tcp.syn || sequence.ctx.Err() == nil && tcp.seq == sequence.initialSynSeq {
				return sequence
			}

			// A source may reuse a four-tuple before the previous sequence's
			// goroutine reaches deferred map cleanup. A fresh SYN sent to that
			// old sequence is rejected by its established sequence-number
			// check, leaving the replacement connection silent until timeout.
			// Keep an exact live SYN retransmission on the current generation;
			// a different initial sequence number, or a canceled generation,
			// atomically replaces it.
			self.removeSequenceWithLock(bufferId, sequence)
		}

		if !tcp.syn {
			// drop the packet; only create a new sequence on SYN.
			// Reply with a RST so the source fails fast instead of
			// retransmitting into silence (PROXYDRAIN1.md §3.5) — sent
			// outside the lock, below. Never reset a reset.
			if self.tcpBufferSettings.EnableOrphanRst && !tcp.rst && self.allowOrphanRstWithLock() {
				orphanRst = tcpRstForOrphan(ipVersion, tcp)
				// parseIpv4/parseIpv6 return views into ipPacket, which is
				// returned below before the callback runs. Preserve the path
				// independently of that pooled backing. Use one allocation for
				// both address slices.
				ipBacking := make(net.IP, len(tcp.sourceIp)+len(tcp.destinationIp))
				sourceIpByteCount := copy(ipBacking, tcp.sourceIp)
				copy(ipBacking[sourceIpByteCount:], tcp.destinationIp)
				orphanSourceIp = ipBacking[:sourceIpByteCount:sourceIpByteCount]
				orphanDestinationIp = ipBacking[sourceIpByteCount:]
			}
			if self.log.V(2).Enabled() {
				self.log.Infof("[lnr]tcp drop no syn (%s)\n", tcp.flagsString())
			}
			// As above, false means ownership stays with the dispatcher.
			return nil
		}

		// else new sequence
		// if sequence, ok := self.sequences[bufferId]; ok {
		// 	sequence.Cancel()
		// 	delete(self.sequences, bufferId)
		// 	sourceSequences := self.sourceSequences[sequence.source]
		// 	delete(sourceSequences, bufferId)
		// 	if 0 == len(sourceSequences) {
		// 		delete(self.sourceSequences, sequence.source)
		// 	}
		// }
		if 0 < self.tcpBufferSettings.UserLimit {
			// limit the total connections per source to avoid blowing up the ulimit
			if sourceSequences := self.sourceSequences[source]; self.tcpBufferSettings.UserLimit <= len(sourceSequences) {
				applyLruMapLimit(sourceSequences, self.tcpBufferSettings.UserLimit-1, func(bufferId BufferId, sequence *TcpSequence) bool {
					if self.log.V(1).Enabled() {
						self.log.Infof(
							"[lnr]tcp limit source %s->%s\n",
							source,
							net.JoinHostPort(
								sequence.destinationIp.String(),
								strconv.Itoa(int(sequence.destinationPort)),
							),
						)
					}
					self.removeSequenceWithLock(bufferId, sequence)
					return true
				})
			}
		}
		if 0 < self.tcpBufferSettings.GlobalLimit {
			// limit the total connections across all sources, an aggregate
			// flow state and fd ceiling
			if self.tcpBufferSettings.GlobalLimit <= len(self.sequences) {
				applyLruMapLimit(self.sequences, self.tcpBufferSettings.GlobalLimit-1, func(bufferId BufferId, sequence *TcpSequence) bool {
					if self.log.V(1).Enabled() {
						self.log.Infof(
							"[lnr]tcp limit global %s->%s\n",
							sequence.source,
							net.JoinHostPort(
								sequence.destinationIp.String(),
								strconv.Itoa(int(sequence.destinationPort)),
							),
						)
					}
					self.removeSequenceWithLock(bufferId, sequence)
					return true
				})
			}
		}

		// TODO
		// limit the number of new connections per second per source
		// self.sourceLimiter[source].Limit()

		sourceIpCopy := make(net.IP, len(tcp.sourceIp))
		copy(sourceIpCopy, tcp.sourceIp)

		destinationIpCopy := make(net.IP, len(tcp.destinationIp))
		copy(destinationIpCopy, tcp.destinationIp)

		sequence := newTcpSequenceWithTransferKey(
			self.ctx,
			self.receiveCallback,
			source,
			transferKey,
			provideMode,
			ipVersion,
			sourceIpCopy,
			tcp.sourcePort,
			destinationIpCopy,
			tcp.destinationPort,
			tcp.seq,
			self.tcpBufferSettings,
		)
		sequence.receiveTransferPacketsCallback = self.receiveTransferPacketsCallback
		sequence.flowCloseCallback = self.flowCloseCallback
		self.sequences[bufferId] = sequence
		sourceSequences := self.sourceSequences[source]
		if sourceSequences == nil {
			sourceSequences = map[BufferId]*TcpSequence{}
			self.sourceSequences[source] = sourceSequences
		}
		sourceSequences[bufferId] = sequence
		self.sequenceWaitGroup.Add(1)
		go HandleError(func() {
			defer self.sequenceWaitGroup.Done()
			defer close(sequence.retirementDone)
			defer func() {
				sequence.Close()
				self.mutex.Lock()
				defer self.mutex.Unlock()
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
	sequence := initSequence()
	if sequence == nil {
		if orphanRst != nil {
			// The shared local-NAT shard owns this synthesized control, so its
			// callback must retain a nonblocking recovery contract.
			self.receiveCallback(
				source,
				transferKey,
				provideMode,
				receiveRecoveryModeRegenerableControl,
				&IpPath{
					Version:         ipVersion,
					Protocol:        IpProtocolTcp,
					SourceIp:        orphanSourceIp,
					SourcePort:      int(tcp.sourcePort),
					DestinationIp:   orphanDestinationIp,
					DestinationPort: int(tcp.destinationPort),
				},
				orphanRst,
			)
			MessagePoolReturn(orphanRst)
		}
		// sequence does not exist and not a syn packet, drop
		return false, nil
	}

	// An established pure ACK has no sequence-space ordering dependency. Apply
	// its monotonic acknowledgement/window update directly so a download does
	// not allocate and schedule one TcpSendItem for every browser ACK. The
	// sequence method returns false until the SYN has initialized the flow, in
	// which case the ordinary queue preserves handshake order.
	if tcp.ack && !tcp.syn && !tcp.fin && !tcp.rst && len(tcp.payload) == 0 &&
		sequence.applyEstablishedPureAck(source, transferKey, tcp, ipPacket) {
		return true, nil
	}

	sendItem := &TcpSendItem{
		source:      source,
		transferKey: transferKey,
		provideMode: provideMode,
		tcp:         *tcp,
		ipPacket:    ipPacket,
	}
	return sequence.send(sendItem, timeout)
}

// Completion means every admitted TCP sequence and its child workers has
// released all packet ownership. The caller has already stopped dispatch.
func (self *TcpBuffer[BufferId]) waitForLifecycle() {
	self.sequenceWaitGroup.Wait()
}

// removeSequenceWithLock is the TCP counterpart of the UDP helper above.
// Eager index removal keeps the cap exact even when canceled sequence
// goroutines have not reached their deferred cleanup yet. The caller holds
// mutex.
func (self *TcpBuffer[BufferId]) removeSequenceWithLock(bufferId BufferId, sequence *TcpSequence) {
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

// Applies one exact provider-source tombstone and cancels matching TCP flow
// ownership without disturbing sibling sources.
func (self *TcpBuffer[BufferId]) setSourceRetired(
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

// tcpRstForOrphan builds the RFC 793 reset for a segment that matched no
// sequence, addressed back to the segment's source: with ACK set the reset
// carries seq = segment.ack and no ack flag; otherwise seq 0 and
// ack = segment.seq + payload length (+1 each for syn/fin), with RST|ACK.
func tcpRstForOrphan(ipVersion int, tcp *parsedTcp) []byte {
	var ipHeaderByteCount int
	switch ipVersion {
	case 4:
		ipHeaderByteCount = Ipv4HeaderSizeWithoutExtensions
	case 6:
		ipHeaderByteCount = Ipv6HeaderSize
	default:
		return nil
	}

	packet := MessagePoolGet(ipHeaderByteCount + TcpHeaderSizeWithoutExtensions)
	switch ipVersion {
	case 4:
		writeIpv4Header(packet, ipProtocolNumberTcp, tcp.destinationIp, tcp.sourceIp)
	case 6:
		writeIpv6Header(packet, ipProtocolNumberTcp, tcp.destinationIp, tcp.sourceIp)
	}

	t := packet[ipHeaderByteCount:]
	binary.BigEndian.PutUint16(t[0:2], tcp.destinationPort)
	binary.BigEndian.PutUint16(t[2:4], tcp.sourcePort)
	var seq uint32
	var ackNumber uint32
	flags := byte(tcpFlagRst)
	if tcp.ack {
		seq = tcp.ackNumber
	} else {
		ackNumber = tcp.seq + uint32(len(tcp.payload))
		if tcp.syn {
			ackNumber += 1
		}
		if tcp.fin {
			ackNumber += 1
		}
		flags |= tcpFlagAck
	}
	binary.BigEndian.PutUint32(t[4:8], seq)
	binary.BigEndian.PutUint32(t[8:12], ackNumber)
	// data offset, no options
	t[12] = byte(TcpHeaderSizeWithoutExtensions/4) << 4
	t[13] = flags
	// window
	t[14] = 0
	t[15] = 0
	// checksum, set below
	t[16] = 0
	t[17] = 0
	// urgent
	t[18] = 0
	t[19] = 0
	binary.BigEndian.PutUint16(t[16:18], transportChecksum(ipProtocolNumberTcp, tcp.destinationIp, tcp.sourceIp, t))
	return packet
}

/*
** Important implementation note **
Packet flow from the user-NAT to the source is assumed to never require
user-NAT retransmission; transfer must remain lossless. The source TCP stack
can reorder packets that cross during a route promotion.
*/
// writeWithProgressDeadline writes to an upstream socket, bounding
// ZERO-PROGRESS time rather than total time: the deadline re-arms whenever a
// write advances, so a peer applying ordinary flow control (accepting data
// steadily but slowly) is not failed, while a peer that accepts nothing for a
// full timeout still is.
//
// A single deadline for the whole batch killed alive flows under sustained
// bulk transfer: the return path's acks stall behind saturated tunnel queues
// long enough that the upstream socket drains slower than one batch, and the
// partial write at the deadline tore the sequence down (observed as mid-stream
// resets in the full-stack tcp test).
func writeWithProgressDeadline(
	socket net.Conn,
	buffers net.Buffers,
	timeout time.Duration,
) (int64, error) {
	n := int64(0)
	for {
		socket.SetWriteDeadline(time.Now().Add(timeout))
		wn, err := buffers.WriteTo(socket)
		n += wn
		if err == nil {
			return n, nil
		}
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() && 0 < wn {
			// progress inside this deadline window: the peer is alive and
			// draining, so give the remainder a fresh window
			continue
		}
		return n, err
	}
}

// Describes the exact non-forwarding decision for one crossover segment.
type tcpReorderDisposition int

const (
	tcpReorderDispositionRetained tcpReorderDisposition = iota
	tcpReorderDispositionRejected
	tcpReorderDispositionStale
)

// TcpSequence owns one user-NAT TCP flow and its bounded crossover reorder state.
type TcpSequence struct {
	ctx    context.Context
	cancel context.CancelFunc
	log    Logger
	// Closed by the owning buffer only after Run has joined child workers and
	// drained every queued packet.
	retirementDone chan struct{}

	receiveCallback                receiveTransferPacketFunction
	receiveTransferPacketsCallback receiveTransferPacketsBatchFunction

	tcpBufferSettings *TcpBufferSettings

	sendMutex sync.Mutex
	sendItems chan *TcpSendItem
	// Lazily allocated only when the bounded send queue actually blocks.
	// sendMutex serializes Reset/Stop.
	sendTimer *time.Timer
	// Pure return-path ACKs can update an established flow without occupying
	// the sequence-space data queue. The condition remains shared with the
	// socket reader so a direct window update wakes a blocked download. Some
	// focused tests build TcpSequence values directly, hence lazy initialization.
	receiveAckCondOnce sync.Once
	receiveAckCond     *sync.Cond
	established        bool // guarded by ConnectionState.mutex

	idleCondition *IdleCondition
	transferState transferState
	// immutable generation identity used to distinguish a retransmitted SYN
	// from four-tuple reuse while the previous flow is still being reaped
	initialSynSeq uint32
	// handshakeAcked records that the source acknowledged the synthesized
	// SYN-ACK. Until then an identical SYN is a handshake retransmission and
	// must receive SYN-ACK again, not the pure ACK used for a stale SYN on an
	// established connection. Guarded by ConnectionState.mutex.
	handshakeAcked bool
	flowCloseOnce  sync.Once
	// Called once after Run and its child workers release the socket lifecycle.
	flowCloseCallback tcpFlowCloseFunction

	// Tests observe exact reorder decisions without using negative socket-read
	// timeouts. The callback must not block; nil is a production no-op.
	afterReorderDispositionForTest func(tcpReorderDisposition)
	// Tests may hold a pool-owned pure acknowledgement after construction to force
	// cancellation at its ownership boundary. Nil is a production no-op.
	afterPureAckBuildForTest func([]byte)
	// Tests join the pure-acknowledgement worker before inspecting ownership. Nil is a
	// production no-op.
	afterPureAckWorkerStopForTest func()
	// Tests observe the exact Run boundary immediately before all child workers
	// are joined. Nil is a production no-op.
	beforeChildWorkersWaitForTest func()
	// Tests may hold Run before its first queued owner is consumed. Nil is a
	// production no-op.
	beforeRunForTest func()
	ConnectionState
}

func NewTcpSequence(ctx context.Context, receiveCallback ReceivePacketFunction,
	source TransferPath,
	provideMode protocol.ProvideMode,
	ipVersion int,
	sourceIp net.IP, sourcePort uint16,
	destinationIp net.IP, destinationPort uint16,
	initialSynSeq uint32,
	tcpBufferSettings *TcpBufferSettings) *TcpSequence {
	return newTcpSequenceWithTransferKey(
		ctx,
		receiveTransferPacketAdapter(receiveCallback),
		source,
		TransferKey{},
		provideMode,
		ipVersion,
		sourceIp,
		sourcePort,
		destinationIp,
		destinationPort,
		initialSynSeq,
		tcpBufferSettings,
	)
}

// Builds one connection while separating its reply lane from its socket
// identity.
func newTcpSequenceWithTransferKey(
	ctx context.Context,
	receiveCallback receiveTransferPacketFunction,
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	ipVersion int,
	sourceIp net.IP,
	sourcePort uint16,
	destinationIp net.IP,
	destinationPort uint16,
	initialSynSeq uint32,
	tcpBufferSettings *TcpBufferSettings,
) *TcpSequence {
	source = source.LocalMask()
	cancelCtx, cancel := context.WithCancel(ctx)

	initialWindowSize := tcpBufferSettings.InitialWindowSize
	if initialWindowSize == 0 {
		initialWindowSize = tcpBufferSettings.MinWindowSize
	}
	initialWindowSize = min(
		max(initialWindowSize, tcpBufferSettings.MinWindowSize),
		tcpBufferSettings.MaxWindowSize,
	)
	timestampOffset := mathrand.Uint32()

	return &TcpSequence{
		ctx:               cancelCtx,
		cancel:            cancel,
		log:               loggerOrDefault(tcpBufferSettings.Log),
		retirementDone:    make(chan struct{}),
		receiveCallback:   receiveCallback,
		tcpBufferSettings: tcpBufferSettings,
		sendItems:         make(chan *TcpSendItem, tcpBufferSettings.SequenceBufferSize),
		idleCondition:     NewIdleCondition(),
		transferState:     newTransferState(source, transferKey),
		initialSynSeq:     initialSynSeq,
		beforeRunForTest:  tcpBufferSettings.beforeSequenceRunForTest,
		ConnectionState: ConnectionState{
			source:          source,
			provideMode:     provideMode,
			ipVersion:       ipVersion,
			sourceIp:        sourceIp,
			sourcePort:      sourcePort,
			destinationIp:   destinationIp,
			destinationPort: destinationPort,
			// prime the cached ip path before the sequence goroutines start
			// (see IpPath)
			ipPath: &IpPath{
				Version:         ipVersion,
				Protocol:        IpProtocolTcp,
				SourceIp:        sourceIp,
				SourcePort:      int(sourcePort),
				DestinationIp:   destinationIp,
				DestinationPort: int(destinationPort),
			},
			// The SYN advertises the protocol's literal 16-bit ceiling. The first
			// post-handshake ACK can immediately advertise this larger scaled
			// warmup window instead of spending several RTTs doubling from 64 KiB.
			enableWindowScale: false,
			windowSize:        initialWindowSize,
			windowScale:       0,
			timestampOffset:   timestampOffset,
			userLimited: userLimited{
				lastActivityTime: time.Now(),
			},
		},
	}
}

func (self *TcpSequence) send(sendItem *TcpSendItem, timeout time.Duration) (bool, error) {
	self.sendMutex.Lock()
	defer self.sendMutex.Unlock()

	select {
	case <-self.ctx.Done():
		return false, errors.New("Done.")
	default:
	}
	self.transferState.update(sendItem.source, sendItem.transferKey)

	if !self.idleCondition.UpdateOpen() {
		return false, nil
	}
	defer self.idleCondition.UpdateClose()

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

func (self *TcpSequence) receiveAckCondition() *sync.Cond {
	self.receiveAckCondOnce.Do(func() {
		self.receiveAckCond = sync.NewCond(&self.mutex)
	})
	return self.receiveAckCond
}

// applyEstablishedPureAck applies the independent ACK/window half of a TCP
// packet directly after the flow handshake. Pure ACKs consume no TCP sequence
// space, and applySendAckWithLock already preserves the greatest advertised
// window edge, so making them wait behind upload data only adds allocation and
// scheduler latency. Packets that arrive before initializeSynWithLock, carry
// payload, or change connection state retain the ordered sendItems path.
//
// A true result transfers and returns ipPacket ownership. False leaves it with
// the caller, which must use the ordinary sequence queue or reject it.
func (self *TcpSequence) applyEstablishedPureAck(
	source TransferPath,
	transferKey TransferKey,
	tcp *parsedTcp,
	ipPacket []byte,
) bool {
	self.sendMutex.Lock()
	defer self.sendMutex.Unlock()

	select {
	case <-self.ctx.Done():
		return false
	default:
	}

	self.mutex.Lock()
	if !self.established {
		self.mutex.Unlock()
		return false
	}
	self.mutex.Unlock()

	self.transferState.update(source, transferKey)
	if !self.idleCondition.UpdateOpen() {
		return false
	}
	defer self.idleCondition.UpdateClose()

	self.mutex.Lock()
	self.updateTimestampRecentWithLock(tcp)
	if self.applySendAckWithLock(tcp) {
		self.receiveAckCondition().Broadcast()
	}
	self.mutex.Unlock()

	MessagePoolReturn(ipPacket)
	return true
}

// initializeSynWithLock establishes sequence and window state from the first
// SYN. The sequence mutex must be held.
func (self *TcpSequence) initializeSynWithLock(tcp *parsedTcp) {
	// SYN and FIN consume one sequence number.
	self.sendSeq = tcp.seq + 1
	// The synthetic return sequence may start at the sender's sequence because
	// there is no transport-security boundary between the two sides.
	self.receiveSeq = tcp.seq
	self.receiveSeqAck = tcp.seq

	parseTcpOptions(tcp)
	self.enableWindowScale = tcp.enableWindowScale
	self.receiveWindowScale = tcp.windowScale
	self.enableTimestamp = tcp.enableTimestamp
	self.timestampRecent = tcp.timestampValue
	if tcp.enableMss {
		self.peerMss = tcp.mss
	}
	// RFC 7323 applies the negotiated scale only after the handshake. The
	// Window field in a SYN is always the literal, unscaled value.
	self.receiveWindowSize = uint32(tcp.windowSize)
	self.receiveWindowEnd = self.receiveSeqAck + self.receiveWindowSize
	self.receiveWindowEndSet = true
	self.established = true
	if self.enableWindowScale {
		bits := math.Log2(float64(self.tcpBufferSettings.MaxWindowSize) / float64(math.MaxUint16))
		if 0 <= bits {
			self.windowScale = uint32(math.Ceil(bits))
		} else {
			self.windowScale = 0
		}
	} else {
		self.windowScale = 0
	}
}

// Delivers one packet with its explicit recovery owner and current reply key.
func (self *TcpSequence) receivePacket(packet []byte, recoveryMode receiveRecoveryMode) {
	source, transferKey := self.transferState.get()
	self.receiveCallback(
		source,
		transferKey,
		self.provideMode,
		recoveryMode,
		self.IpPath(),
		packet,
	)
	MessagePoolReturn(packet)
}

// Delivers a drained batch with one stable reply-key and recovery snapshot.
func (self *TcpSequence) receiveBatch(packets [][]byte, recoveryMode receiveRecoveryMode) {
	source, transferKey := self.transferState.get()
	if self.receiveTransferPacketsCallback != nil &&
		self.receiveTransferPacketsCallback(
			source,
			transferKey,
			self.provideMode,
			recoveryMode,
			self.IpPath(),
			packets,
		) {
		for _, packet := range packets {
			MessagePoolReturn(packet)
		}
		return
	}
	for _, packet := range packets {
		self.receivePacket(packet, recoveryMode)
	}
}

func (self *TcpSequence) Run() {
	var childWorkers sync.WaitGroup
	defer func() {
		if self.beforeChildWorkersWaitForTest != nil {
			self.beforeChildWorkersWaitForTest()
		}
		childWorkers.Wait()
	}()
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
	if self.beforeRunForTest != nil {
		self.beforeRunForTest()
	}
	runChildWorker := func(run func()) {
		childWorkers.Add(1)
		go HandleError(func() {
			defer childWorkers.Done()
			run()
		}, self.cancel)
	}

	// One timer serves the initial SYN wait and the steady-state idle wait.
	// The send loop wakes per segment, so time.After here previously allocated
	// a timer per packet even when the send channel was immediately ready.
	idleTimer := time.NewTimer(0)
	defer idleTimer.Stop()

	// f, _ := tcpConn.File()
	// fd := SocketHandle(f.Fd())
	// syscall.SetsockoptInt(fd, syscall.IPPROTO_IP, syscall.IP_MTU, self.tcpBufferSettings.Mtu)

	var packet []byte
	var packetErr error
	for syn := false; !syn; {
		checkpointId := self.idleCondition.Checkpoint()
		idleTimer.Reset(self.tcpBufferSettings.ConnectTimeout)
		select {
		case <-self.ctx.Done():
			idleTimer.Stop()
			return
		case sendItem := <-self.sendItems:
			idleTimer.Stop()
			if self.log.V(2).Enabled() {
				self.log.Infof("[init]send(%d)\n", len(sendItem.tcp.payload))
			}
			// the first packet must be a syn
			if sendItem.tcp.syn {
				self.log.V(2).Infof("[init]SYN\n")

				func() {
					self.mutex.Lock()
					defer self.mutex.Unlock()

					self.initializeSynWithLock(&sendItem.tcp)
					if self.log.V(2).Enabled() {
						self.log.Infof("[init]window=%d/%d, receive=%d/%d\n", self.windowSize, self.windowScale, self.receiveWindowSize, self.receiveWindowScale)
					}

					packet, packetErr = self.SynAck(self.tcpBufferSettings.Mtu)
					self.receiveSeq += 1
				}()

				syn = true
			} else {
				// an ACK here could be for a previous FIN
				if self.log.V(2).Enabled() {
					self.log.Infof("[init]waiting for SYN (%s)\n", sendItem.tcp.flagsString())
				}
			}
			MessagePoolReturn(sendItem.ipPacket)
		case <-idleTimer.C:
			if self.idleCondition.Close(checkpointId) {
				// close the sequence
				self.log.V(2).Infof("[init]connect timeout\n")
				return
			}
			// else there pending updates
		}
	}

	if packetErr != nil {
		return
	}

	// connect to upstream before sending the syn+ack
	self.log.V(2).Infof("[init]tcp connect\n")
	var socket net.Conn
	var err error
	if self.tcpBufferSettings.EnableSyntheticSpeed && isSyntheticSpeedIp(self.IpPath().DestinationIp) {
		// benchmark-range destination: terminate at the in-memory synthetic
		// speed server (see ip_synthetic_speed.go)
		if self.log.V(1).Enabled() {
			self.log.Infof("[init]tcp connect synthetic %s\n", self.IpPath().DestinationHostPort())
		}
		socket = newSyntheticSpeedConn()
	} else {
		socket, err = self.tcpBufferSettings.DialContext(
			self.ctx,
			"tcp",
			self.IpPath().DestinationHostPort(),
		)
	}
	if err != nil {
		// The source has not seen our synthetic syn-ack yet. Abandoning the
		// sequence silently makes it retransmit the syn on an exponential
		// schedule (17/33/65 seconds in Chromium under repeated provider dial
		// failure). Release the unsent syn-ack and reject the flow so the
		// source can fail or retry immediately. A parent cancellation is
		// teardown, not an upstream refusal, and must not generate new output.
		MessagePoolReturn(packet)
		if self.log.V(1).Enabled() {
			self.log.Infof("[init]tcp connect error = %s\n", err)
		}
		// answer the source instead of going silent: a refused destination gets
		// an honest RST+ACK, everything else a capacity-class unreachable. both
		// ride the same receive callback the SynAck uses (from-destination
		// orientation); then return as before -- no socket exists. the ctx
		// guard implements the comment above: classifyDialFailure cannot tell
		// a canceled parent from a capacity failure, and teardown must not
		// generate new output.
		if self.ctx.Err() == nil {
			switch action, signal := classifyDialFailure(self.IpPath(), err); action {
			case dialFailureRst:
				var rstPacket []byte
				var rstErr error
				func() {
					self.mutex.Lock()
					defer self.mutex.Unlock()
					rstPacket, rstErr = self.RstAck()
				}()
				if rstErr == nil {
					self.receivePacket(rstPacket, receiveRecoveryModeRegenerableControl)
				}
			case dialFailureUnreachable:
				self.receivePacket(
					MessagePoolCopy(signal),
					receiveRecoveryModeRegenerableControl,
				)
			}
		}
		return
	}
	self.UpdateLastActivityTime()
	self.log.V(2).Infof("[init]connect success\n")

	defer socket.Close()
	if tcpConn, ok := socket.(*net.TCPConn); ok {
		configureUpstreamTcpConn(tcpConn, self.tcpBufferSettings)
	}

	self.log.V(2).Infof("[init]receive SYN+ACK\n")
	// The upstream may be server-first (SMTP/587, SSH, IMAP): socket bytes can
	// be readable immediately after connect. Keep the SYN+ACK on the same
	// synchronous recovery lane as those bytes so the socket reader cannot
	// overtake a queued handshake packet. The provider has already consumed the
	// successful dial and the following stream is lossless, so this control is
	// part of that ordered ownership boundary too.
	self.receivePacket(packet, receiveRecoveryModeTcpSocket)

	/*
		if v, ok := socket.(*net.TCPConn); ok {
			if err := v.SetWriteBuffer(int(self.windowSize)); err != nil {
				self.log.Infof("[init]could not set write buffer = %d\n", self.windowSize)
			}
			// if err := v.SetReadBuffer(int(self.receiveWindowSize)); err != nil {
			// 	self.log.Infof("[init]could not set read buffer = %d\n", self.receiveWindowSize)
			// }
		}
	*/

	receiveAckCond := self.receiveAckCondition()
	ackCond := sync.NewCond(&self.mutex)
	defer func() {
		self.mutex.Lock()
		defer self.mutex.Unlock()

		receiveAckCond.Broadcast()
		ackCond.Broadcast()
	}()

	// signals the ack pipeline to send a coalesced ack now
	ackSignal := make(chan struct{}, 1)

	var ackedSendSeq uint32
	func() {
		self.mutex.Lock()
		defer self.mutex.Unlock()

		ackedSendSeq = self.sendSeq
	}()

	// pipelines

	type writePayload struct {
		sendIter         uint64
		ipPacket         []byte
		payloadOffset    uint16
		payloadByteCount uint16
	}

	writePayloads := make(chan writePayload, self.tcpBufferSettings.SequenceBufferSize)
	runChildWorker(func() {
		// best effort return of queued payloads after cancel
		defer func() {
			for {
				select {
				case writePayload, ok := <-writePayloads:
					if !ok {
						return
					}
					MessagePoolReturn(writePayload.ipPacket)
				default:
					return
				}
			}
		}()
		defer self.cancel()

		batch := make([]writePayload, 0, self.tcpBufferSettings.WriteBatchSize)
		bufferStorage := make([][]byte, 0, self.tcpBufferSettings.WriteBatchSize)

		for {
			batch = batch[:0]
			closed := false
			select {
			case <-self.ctx.Done():
				return
			case writePayload, ok := <-writePayloads:
				if !ok {
					return
				}
				batch = append(batch, writePayload)
			}
			// opportunistically coalesce queued payloads into a single socket write
		drain:
			for len(batch) < self.tcpBufferSettings.WriteBatchSize {
				select {
				case writePayload, ok := <-writePayloads:
					if !ok {
						closed = true
						break drain
					}
					batch = append(batch, writePayload)
				default:
					break drain
				}
			}

			bufferStorage = bufferStorage[:0]
			byteCount := 0
			for _, writePayload := range batch {
				payloadStart := int(writePayload.payloadOffset)
				payloadEnd := payloadStart + int(writePayload.payloadByteCount)
				payload := writePayload.ipPacket[payloadStart:payloadEnd]
				bufferStorage = append(bufferStorage, payload)
				byteCount += len(payload)
			}
			// `net.Buffers` uses a single vectored write when the socket supports it.
			// `WriteTo` retries partial writes until fully written, a timeout, or an error.
			buffers := net.Buffers(bufferStorage)

			n, err := writeWithProgressDeadline(
				socket,
				buffers,
				self.tcpBufferSettings.WriteTimeout,
			)

			if err == nil {
				if self.log.V(2).Enabled() {
					self.log.Infof("[f%d]tcp forward %d/%d\n", batch[0].sendIter, n, byteCount)
				}
			} else {
				if self.log.V(1).Enabled() {
					self.log.Infof("[f%d]tcp forward %d/%d error = %s\n", batch[0].sendIter, n, byteCount, err)
				}
			}
			if 0 < n {
				self.UpdateLastActivityTime()
			}
			for _, writePayload := range batch {
				MessagePoolReturn(writePayload.ipPacket)
			}
			if err != nil {
				// timeout or socket error
				return
			}
			if closed {
				return
			}
		}
	})

	// Keep at most one callback batch of read-ahead per flow. The former
	// SequenceBufferSize queue reserved a full send window again even though
	// the socket reader already enforces that window. One batch preserves the
	// measured socket-read/delivery overlap while a stalled callback still
	// applies bounded backpressure to its own flow.
	readQueueSize := min(
		self.tcpBufferSettings.SequenceBufferSize,
		max(1, self.tcpBufferSettings.WriteBatchSize),
	)
	readPackets := make(chan []byte, readQueueSize)
	runChildWorker(func() {
		defer self.cancel()

		defer func() {
			// drain to the close so that ordered data and any final
			// fin/rst reach the source on teardown. the socket read side
			// always closes `readPackets` on exit, which is unblocked by
			// the deferred socket close in `Run`
			for packet := range readPackets {
				self.receivePacket(packet, receiveRecoveryModeTcpSocket)
			}
		}()

		// reused across drains; receiveBatch consumes it before the next
		// drain reuses it
		batch := make([][]byte, 0, self.tcpBufferSettings.WriteBatchSize)

	read:
		for {
			select {
			case <-self.ctx.Done():
				return
			case packet, ok := <-readPackets:
				if !ok {
					return
				}
				batch = append(batch[:0], packet)
				// opportunistically drain queued packets into one batch to
				// reduce wakeups and coalesce the return-path wire Pack
				for len(batch) < cap(batch) {
					select {
					case packet, ok := <-readPackets:
						if !ok {
							self.receiveBatch(batch, receiveRecoveryModeTcpSocket)
							return
						}
						batch = append(batch, packet)
					default:
						self.receiveBatch(batch, receiveRecoveryModeTcpSocket)
						continue read
					}
				}
				self.receiveBatch(batch, receiveRecoveryModeTcpSocket)
			}
		}
	})

	runChildWorker(func() {
		fin := false
		defer func() {
			// close without cancel so that the receive pipeline drains all
			// queued packets before the sequence cancels.
			// the receive pipeline cancels after the drain.
			if !fin {
				var packet []byte
				var err error
				func() {
					self.mutex.Lock()
					defer self.mutex.Unlock()

					packet, err = self.RstAck()
				}()
				if err == nil {
					select {
					case readPackets <- packet:
						fin = true
					}
				}
			}

			close(readPackets)
		}()

		buffer := make([]byte, self.tcpBufferSettings.ReadBufferByteCount)

		for forwardIter := uint64(0); ; forwardIter += 1 {
			select {
			case <-self.ctx.Done():
				return
			default:
			}

			readTimeout := time.Now().Add(self.tcpBufferSettings.ReadTimeout)
			socket.SetReadDeadline(readTimeout)

			n, err := socket.Read(buffer)

			if err != nil {
				if self.log.V(1).Enabled() {
					self.log.Infof("[f%d]tcp receive error = %s\n", forwardIter, err)
				}
			}

			if 0 < n {
				self.UpdateLastActivityTime()

				// Transfer must not lose these emitted segments. The source TCP stack
				// can reorder a route crossover, but the user-NAT does not retain data
				// for retransmission.
				// packetize and emit one window-sized chunk at a time, so that a
				// read larger than the receive window cannot stall. each chunk
				// must be emitted before waiting for window room for the next
				// chunk, since the window only opens as the source acks
				// emitted data.
				stop := false
				packetCount := 0
				for i := 0; i < n && !stop; {
					var chunkPackets [][]byte
					func() {
						self.mutex.Lock()
						defer self.mutex.Unlock()

						for {
							select {
							case <-self.ctx.Done():
								stop = true
								return
							default:
							}

							windowByteCount := int(int64(self.receiveWindowSize) - int64(self.receiveSeq-self.receiveSeqAck))
							if 0 < windowByteCount {
								j := min(i+windowByteCount, n)
								var err error
								chunkPackets, err = self.DataPackets(buffer[i:j], j-i, self.tcpBufferSettings.Mtu)
								if err != nil {
									self.log.Infof("[f%d]tcp receive packets error = %s\n", forwardIter, err)
									stop = true
									return
								}
								self.receiveSeq += uint32(j - i)
								ackedSendSeq = self.sendSeq
								i = j
								return
							}

							if self.log.V(2).Enabled() {
								self.log.Infof("[f%d]tcp receive window wait\n", forwardIter)
							}
							receiveAckCond.Wait()
						}
					}()
					for _, packet := range chunkPackets {
						if stop {
							MessagePoolReturn(packet)
						} else {
							select {
							case <-self.ctx.Done():
								MessagePoolReturn(packet)
								stop = true
							case readPackets <- packet:
								packetCount += 1
							}
						}
					}
				}
				if stop {
					return
				}

				if 1 < packetCount {
					if self.log.V(2).Enabled() {
						self.log.Infof("[f%d]tcp receive segmented packets %d\n", forwardIter, packetCount)
					}
				}
				if self.log.V(2).Enabled() {
					self.log.Infof("[f%d]tcp receive %d %d\n", forwardIter, n, packetCount)
				}
			}

			if err != nil {
				if err == io.EOF {
					// closed (FIN)
					// propagate the FIN and close the sequence
					self.log.V(2).Infof("[final]FIN\n")
					var finPacket []byte
					var finErr error
					func() {
						self.mutex.Lock()
						defer self.mutex.Unlock()

						finPacket, finErr = self.FinAck()
						self.receiveSeq += 1
					}()
					if finErr == nil {
						select {
						case <-self.ctx.Done():
							MessagePoolReturn(finPacket)
						case readPackets <- finPacket:
							fin = true
						}
					}
					return
				} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					if self.log.V(2).Enabled() {
						self.log.Infof("[f%d]timeout\n", forwardIter)
					}
					return
				} else {
					// some other error
					return
				}
			}
		}
	})

	runChildWorker(func() {
		defer self.cancel()
		defer func() {
			if self.afterPureAckWorkerStopForTest != nil {
				self.afterPureAckWorkerStopForTest()
			}
		}()

		// reusable ack-compress timer (avoids a per-iteration time.After alloc
		// on the hot ack coalescing path)
		ackCompressTimer := time.NewTimer(0)
		defer ackCompressTimer.Stop()

		for {
			select {
			case <-self.ctx.Done():
				return
			default:
			}

			var packet []byte
			func() {
				self.mutex.Lock()
				defer self.mutex.Unlock()

				select {
				case <-self.ctx.Done():
					return
				default:
				}

				for self.sendSeq == ackedSendSeq {
					ackCond.Wait()
					select {
					case <-self.ctx.Done():
						return
					default:
					}
				}

				var err error
				packet, err = self.PureAck()
				if err != nil {
					self.log.Infof("[r]ack err = %s\n", err)
				}
				ackedSendSeq = self.sendSeq
			}()
			if packet == nil {
				return
			}
			if self.afterPureAckBuildForTest != nil {
				self.afterPureAckBuildForTest(packet)
			}

			select {
			case <-self.ctx.Done():
				MessagePoolReturn(packet)
				return
			default:
			}

			self.receivePacket(packet, receiveRecoveryModeRegenerableControl)

			if 0 < self.tcpBufferSettings.AckCompressTimeout {
				// coalesce acks up to the timeout.
				// the send loop signals to ack sooner when the unacked byte
				// count reaches half the window, so the source never stalls
				// on a full window waiting for the timeout.
				ackCompressTimer.Reset(self.tcpBufferSettings.AckCompressTimeout)
				select {
				case <-ackCompressTimer.C:
				case <-ackSignal:
				case <-self.ctx.Done():
					return
				}
			}
		}
	})

	// A route promotion can make a packet sent on the new path overtake one
	// already in flight on the old path. Retain that bounded crossover window
	// until its gap arrives. The item count shares the configured per-flow
	// packet bound, while owned packet bytes cannot exceed the configured
	// maximum TCP window.
	maxReorderItemCount := max(1, self.tcpBufferSettings.SequenceBufferSize)
	maxReorderPacketByteCount := max(
		1,
		max(int(self.tcpBufferSettings.MaxWindowSize), self.tcpBufferSettings.Mtu),
	)
	reorderItems := []*TcpSendItem{}
	reorderPacketByteCount := 0
	returnSendItem := func(sendItem *TcpSendItem) {
		if sendItem != nil && sendItem.ipPacket != nil {
			MessagePoolReturn(sendItem.ipPacket)
			sendItem.ipPacket = nil
		}
	}
	defer func() {
		for _, sendItem := range reorderItems {
			returnSendItem(sendItem)
		}
	}()

	sequenceByteCount := func(tcp *parsedTcp) int64 {
		byteCount := int64(len(tcp.payload))
		if tcp.fin {
			byteCount += 1
		}
		return byteCount
	}
	sequenceBounds := func(sendItem *TcpSendItem, nextSeq uint32) (start int64, end int64) {
		start = int64(int32(sendItem.tcp.seq - nextSeq))
		end = start + sequenceByteCount(&sendItem.tcp)
		return
	}
	removeReorderItem := func(itemIndex int) *TcpSendItem {
		sendItem := reorderItems[itemIndex]
		reorderPacketByteCount -= len(sendItem.ipPacket)
		copy(reorderItems[itemIndex:], reorderItems[itemIndex+1:])
		reorderItems[len(reorderItems)-1] = nil
		reorderItems = reorderItems[:len(reorderItems)-1]
		return sendItem
	}
	bufferReorderItem := func(sendItem *TcpSendItem, nextSeq uint32) bool {
		start, end := sequenceBounds(sendItem, nextSeq)
		if start <= 0 || int64(maxReorderPacketByteCount) < end {
			return false
		}

		coveredItemCount := 0
		coveredPacketByteCount := 0
		for _, reorderItem := range reorderItems {
			reorderStart, reorderEnd := sequenceBounds(reorderItem, nextSeq)
			if reorderStart <= start && end <= reorderEnd {
				// The retained item already covers every sequence byte in this
				// retransmission, including FIN when present.
				return false
			}
			if start <= reorderStart && reorderEnd <= end {
				coveredItemCount += 1
				coveredPacketByteCount += len(reorderItem.ipPacket)
			}
		}

		retainedItemCount := len(reorderItems) - coveredItemCount
		retainedPacketByteCount := reorderPacketByteCount - coveredPacketByteCount
		if maxReorderItemCount < retainedItemCount+1 ||
			maxReorderPacketByteCount-retainedPacketByteCount < len(sendItem.ipPacket) {
			return false
		}

		// Prefer the widest retransmission and release items it fully covers.
		// Partial overlaps remain safe: the drain trims their accepted prefix.
		for itemIndex := 0; itemIndex < len(reorderItems); {
			reorderStart, reorderEnd := sequenceBounds(reorderItems[itemIndex], nextSeq)
			if start <= reorderStart && reorderEnd <= end {
				returnSendItem(removeReorderItem(itemIndex))
				continue
			}
			itemIndex += 1
		}
		reorderItems = append(reorderItems, sendItem)
		reorderPacketByteCount += len(sendItem.ipPacket)
		return true
	}
	takeReadyReorderItem := func(nextSeq uint32) *TcpSendItem {
		// A wider overlap can make retained retransmissions wholly stale. Purge
		// them before selecting the ready item that advances farthest.
		for itemIndex := 0; itemIndex < len(reorderItems); {
			_, end := sequenceBounds(reorderItems[itemIndex], nextSeq)
			if end <= 0 {
				returnSendItem(removeReorderItem(itemIndex))
				continue
			}
			itemIndex += 1
		}

		readyItemIndex := -1
		readyEnd := int64(0)
		for itemIndex, reorderItem := range reorderItems {
			start, end := sequenceBounds(reorderItem, nextSeq)
			if start <= 0 && (readyItemIndex < 0 || readyEnd < end) {
				readyItemIndex = itemIndex
				readyEnd = end
			}
		}
		if readyItemIndex < 0 {
			return nil
		}
		return removeReorderItem(readyItemIndex)
	}
	sendCurrentAck := func(retransmittedSyn bool) {
		var packet []byte
		recoveryMode := receiveRecoveryModeRegenerableControl
		func() {
			self.mutex.Lock()
			defer self.mutex.Unlock()

			var err error
			if retransmittedSyn && !self.handshakeAcked {
				packet, err = self.synAckWithSequence(
					self.tcpBufferSettings.Mtu,
					self.initialSynSeq,
				)
				recoveryMode = receiveRecoveryModeTcpSocket
			} else {
				packet, err = self.PureAck()
			}
			if err != nil {
				self.log.Infof("[r]duplicate ack err = %s\n", err)
			}
		}()
		if packet == nil {
			return
		}
		select {
		case <-self.ctx.Done():
			MessagePoolReturn(packet)
		default:
			self.receivePacket(packet, recoveryMode)
		}
	}

	// window scaling depends on `nonBlockingByteCount` and `blockingByteCount` per `self.windowSize`
	nonBlockingByteCount := uint32(0)
	blockingByteCount := uint32(0)
	fin := false
	sendIter := uint64(0)
	// returns false when the send loop must stop:
	// rst or cancel with `fin` false, or fin flush with `fin` true
	handleSendItem := func(sendItem *TcpSendItem) bool {
		for sendItem != nil {
			if self.log.V(2).Enabled() {
				if "ACK" != sendItem.tcp.flagsString() {
					self.log.Infof("[r%d]receive(%d %s)\n", sendIter, len(sendItem.tcp.payload), sendItem.tcp.flagsString())
				}
			}

			if sendItem.tcp.rst {
				// A RST typically appears for a bad TCP segment.
				if self.log.V(2).Enabled() {
					self.log.Infof("[r%d]RST\n", sendIter)
				}
				returnSendItem(sendItem)
				return false
			}

			var nextSeq uint32
			func() {
				self.mutex.Lock()
				defer self.mutex.Unlock()

				self.updateTimestampRecentWithLock(&sendItem.tcp)
				if !sendItem.ackApplied {
					sendItem.ackApplied = true
					if self.applySendAckWithLock(&sendItem.tcp) {
						receiveAckCond.Broadcast()
					}
				}
				nextSeq = self.sendSeq
			}()

			if sendItem.tcp.syn {
				// Until the source ACKs the synthesized handshake, an identical
				// SYN means its SYN-ACK was lost and must be retransmitted. Once
				// acknowledged, the current pure ACK is the established-flow
				// response to a stale SYN.
				retransmittedSyn := sendItem.tcp.seq == self.initialSynSeq
				returnSendItem(sendItem)
				sendCurrentAck(retransmittedSyn)
				return true
			}

			itemSequenceByteCount := sequenceByteCount(&sendItem.tcp)
			if itemSequenceByteCount == 0 {
				// Pure ACKs consume no sequence space. Their independent return-path
				// window update was applied above regardless of callback ordering.
				returnSendItem(sendItem)
				return true
			}

			start, end := sequenceBounds(sendItem, nextSeq)
			if 0 < start {
				retained := bufferReorderItem(sendItem, nextSeq)
				if !retained {
					returnSendItem(sendItem)
				}
				if self.afterReorderDispositionForTest != nil {
					disposition := tcpReorderDispositionRejected
					if retained {
						disposition = tcpReorderDispositionRetained
					}
					self.afterReorderDispositionForTest(disposition)
				}
				// Both retained gaps and bounded-buffer rejection need an immediate
				// duplicate ACK so ordinary TCP retransmission can recover.
				sendCurrentAck(false)
				return true
			}
			if end <= 0 {
				// A retransmission wholly covered by bytes already accepted upstream
				// owns no new sequence space.
				returnSendItem(sendItem)
				if self.afterReorderDispositionForTest != nil {
					self.afterReorderDispositionForTest(tcpReorderDispositionStale)
				}
				sendCurrentAck(false)
				return true
			}

			if start < 0 {
				// Forward only the suffix beyond the accepted prefix. FIN follows the
				// payload in sequence space, so it remains when all payload bytes but
				// not the FIN have already been accepted.
				payloadTrimByteCount := min(int64(len(sendItem.tcp.payload)), -start)
				sendItem.payloadTrimByteCount += int(payloadTrimByteCount)
				sendItem.tcp.payload = sendItem.tcp.payload[payloadTrimByteCount:]
				sendItem.tcp.seq += uint32(payloadTrimByteCount)
			}

			payload := sendItem.tcp.payload
			if 0 < len(payload) {
				ipHeaderByteCount := Ipv6HeaderSize
				if sendItem.ipPacket[0]>>4 == 4 {
					ipHeaderByteCount = int(sendItem.ipPacket[0]&0xf) * 4
				}
				writePayload := writePayload{
					sendIter: sendIter,
					ipPacket: sendItem.ipPacket,
					payloadOffset: uint16(
						ipHeaderByteCount +
							TcpHeaderSizeWithoutExtensions +
							len(sendItem.tcp.options) +
							sendItem.payloadTrimByteCount,
					),
					payloadByteCount: uint16(len(payload)),
				}
				// FIXME count the number of non-blocking versus blocking channel adds
				// FIXME every window size, check the count:
				// FIXME - if 0 blocking, double window size
				// FIXME - if >half blocking, half the window size
				// FIXME else leave the window size unchanged
				select {
				case writePayloads <- writePayload:
					nonBlockingByteCount += uint32(len(payload))
				default:
					select {
					case writePayloads <- writePayload:
						blockingByteCount += uint32(len(payload))
					case <-self.ctx.Done():
						returnSendItem(sendItem)
						return false
					}
				}
				// The socket writer now owns this packet.
				sendItem.ipPacket = nil
			} else {
				returnSendItem(sendItem)
			}

			advanceByteCount := uint32(len(payload))
			if sendItem.tcp.fin {
				advanceByteCount += 1
				if self.log.V(2).Enabled() {
					self.log.Infof("[r%d]FIN\n", sendIter)
				}
			}
			func() {
				self.mutex.Lock()
				defer self.mutex.Unlock()

				// self.log.Infof("[r%d]eval window size (%d, %d, %d)\n", sendIter, self.windowSize, nonBlockingByteCount, blockingByteCount)
				if 0 < len(payload) && self.windowSize <= blockingByteCount+nonBlockingByteCount {
					if self.windowSize <= nonBlockingByteCount {
						nextWindowSize := min(self.windowSize*2, self.tcpBufferSettings.MaxWindowSize)
						if self.windowSize != nextWindowSize {
							if self.log.V(1).Enabled() {
								self.log.Infof("[r%d]increase window size %d -> %d\n", sendIter, self.windowSize, nextWindowSize)
							}
							self.windowSize = nextWindowSize
						}
					} else if self.windowSize/2 <= blockingByteCount {
						nextWindowSize := max(self.windowSize/2, self.tcpBufferSettings.MinWindowSize)
						if self.windowSize != nextWindowSize {
							if self.log.V(1).Enabled() {
								self.log.Infof("[r%d]decrease window size %d -> %d\n", sendIter, self.windowSize, nextWindowSize)
							}
							self.windowSize = nextWindowSize
						}
					}
					// Else no change to the window. Reset the stats.
					nonBlockingByteCount = uint32(0)
					blockingByteCount = uint32(0)
				}

				self.sendSeq += advanceByteCount
				nextSeq = self.sendSeq
				ackCond.Broadcast()
				if 0 < len(payload) && self.windowSize/2 <= self.sendSeq-ackedSendSeq {
					select {
					case ackSignal <- struct{}{}:
					default:
					}
				}
			}()

			if sendItem.tcp.fin {
				// Flush queued payload before propagating the FIN to the upstream
				// socket and closing the sequence.
				close(writePayloads)
				fin = true
				return false
			}

			sendItem = takeReadyReorderItem(nextSeq)
		}
		return true
	}

send:
	for {
		checkpointId := self.idleCondition.Checkpoint()
		idleTimer.Reset(self.tcpBufferSettings.IdleTimeout)
		select {
		case <-self.ctx.Done():
			idleTimer.Stop()
			return
		case sendItem := <-self.sendItems:
			idleTimer.Stop()
			if !handleSendItem(sendItem) {
				if !fin {
					return
				}
				break send
			}
			sendIter += 1
			// opportunistically drain queued send items to reduce wakeups
			for {
				select {
				case sendItem := <-self.sendItems:
					if !handleSendItem(sendItem) {
						if !fin {
							return
						}
						break send
					}
					sendIter += 1
				default:
					continue send
				}
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
				if self.log.V(2).Enabled() {
					self.log.Infof("[r%d]timeout\n", sendIter)
				}
				return
			}
			// else there pending updates
		}
	}

	// wait for `writePayloads` to finish
	select {
	case <-self.ctx.Done():
	}
}

// Updates the return-path acknowledgment/window. The caller holds mutex.
func (self *TcpSequence) applySendAckWithLock(tcp *parsedTcp) (receiveAckUpdated bool) {
	if tcp.ack &&
		0 <= int32(tcp.ackNumber-self.receiveSeqAck) &&
		0 <= int32(self.receiveSeq-tcp.ackNumber) {
		if !self.handshakeAcked &&
			0 <= int32(tcp.ackNumber-(self.initialSynSeq+1)) {
			self.handshakeAcked = true
		}
		// ACK generation and upload packetization may run concurrently in the
		// source TCP stack. A pure ACK can therefore arrive with a sequence
		// just ahead of or behind upload payload already delivered here. Its
		// ACK field is independent and must still reopen the return window.
		// Bound it by receiveSeq so a corrupt/future ACK cannot acknowledge
		// bytes this sequence has not emitted.
		if !self.receiveWindowEndSet {
			self.receiveWindowEnd = self.receiveSeqAck + self.receiveWindowSize
			self.receiveWindowEndSet = true
		}

		// A TCP receiver never shrinks the right edge it has already
		// advertised. ACK generation and application reads can produce
		// duplicate window updates concurrently, so their callback order is
		// not a reliable chronology. Preserve the greatest advertised edge:
		// this accepts zero-window closure (the ACK advances to the existing
		// edge), accepts a later reopen (the edge grows), and ignores a stale
		// smaller-window duplicate that arrives after the reopen.
		receiveWindowEnd := tcp.ackNumber + (uint32(tcp.windowSize) << self.receiveWindowScale)
		if 0 < int32(receiveWindowEnd-self.receiveWindowEnd) {
			self.receiveWindowEnd = receiveWindowEnd
		}

		receiveSeqAck := tcp.ackNumber
		receiveWindowSize := uint32(0)
		if 0 < int32(self.receiveWindowEnd-receiveSeqAck) {
			receiveWindowSize = self.receiveWindowEnd - receiveSeqAck
		}
		if self.receiveSeqAck != receiveSeqAck || self.receiveWindowSize != receiveWindowSize {
			self.receiveSeqAck = receiveSeqAck
			self.receiveWindowSize = receiveWindowSize
			receiveAckUpdated = true
		}
	}

	return
}

func (self *TcpSequence) Cancel() {
	self.cancel()
}

func (self *TcpSequence) Close() {
	self.flowCloseOnce.Do(func() {
		self.cancel()
		if self.flowCloseCallback != nil {
			self.flowCloseCallback(self.source, self.ipPath)
		}
	})
}

type TcpSendItem struct {
	source      TransferPath
	transferKey TransferKey
	provideMode protocol.ProvideMode
	tcp         parsedTcp
	ipPacket    []byte
	// Number of already-accepted payload bytes at the front of ipPacket. A
	// crossover segment can overlap the current receive sequence; retaining the
	// offset lets the socket writer forward only its new suffix without copying.
	payloadTrimByteCount int
	// An out-of-order item applies its independent return-path ACK when it first
	// arrives. Draining it after the gap closes must not apply that update twice.
	ackApplied bool
}

type ConnectionState struct {
	source          TransferPath
	provideMode     protocol.ProvideMode
	ipVersion       int
	sourceIp        net.IP
	sourcePort      uint16
	destinationIp   net.IP
	destinationPort uint16

	mutex sync.Mutex

	sendSeq             uint32
	receiveSeq          uint32
	receiveSeqAck       uint32
	receiveWindowSize   uint32
	receiveWindowEnd    uint32
	receiveWindowEndSet bool
	receiveWindowScale  uint32
	enableWindowScale   bool
	windowSize          uint32
	windowScale         uint32
	enableTimestamp     bool
	timestampRecent     uint32
	timestampOffset     uint32
	peerMss             uint32
	// Tests provide an exact timestamp without sleeping. Nil uses elapsed
	// monotonic process time and is a production no-op.
	timestampValueForTest func() uint32
	// encodedWindowSize  uint16

	// cached immutable ip path for this connection (see IpPath). primed at
	// construction, before the sequence goroutines start, so it is written
	// once and then read-only
	ipPath *IpPath

	// reusable backing for the common single-packet DataPackets result.
	// DataPackets is called from one goroutine and its result is consumed
	// before the next call, so the backing can be reused; segmented payloads
	// allocate a fresh slice.
	singleDataPacket [1][]byte

	// pathMtu is the largest packet the source has told us its path toward
	// it can carry (icmp fragmentation-needed / packet-too-big), or zero when
	// nothing has been learned. Read by DataPackets, written from the NAT
	// dispatch goroutine, hence atomic.
	pathMtu atomic.Int32

	userLimited
}

// clampPathMtu lowers the configured mtu to a learned path mtu.
func (self *ConnectionState) clampPathMtu(mtu int) int {
	if pathMtu := int(self.pathMtu.Load()); 0 < pathMtu && pathMtu < mtu {
		return pathMtu
	}
	return mtu
}

// applyPathMtu records a smaller path mtu learned from the source. Never
// grows an existing value, and never goes below the family minimum.
func (self *ConnectionState) applyPathMtu(mtu int) {
	mtu = max(mtu, ipMinimumPathMtu(self.ipVersion))
	for {
		current := self.pathMtu.Load()
		if current != 0 && int(current) <= mtu {
			return
		}
		if self.pathMtu.CompareAndSwap(current, int32(mtu)) {
			return
		}
	}
}

// IpPath returns the immutable ip path for this connection. The path is
// primed at construction and cached; the connection identity (version, ips,
// ports) never changes. A zero-value state (tests) builds a fresh uncached
// path, since the sequence goroutines may race a lazy write.
func (self *ConnectionState) IpPath() *IpPath {
	if self.ipPath != nil {
		return self.ipPath
	}
	return &IpPath{
		Version:         self.ipVersion,
		Protocol:        IpProtocolTcp,
		SourceIp:        self.sourceIp,
		SourcePort:      int(self.sourcePort),
		DestinationIp:   self.destinationIp,
		DestinationPort: int(self.destinationPort),
	}
}

func (self *ConnectionState) encodedWindowSize() uint16 {
	return uint16(min(
		uint32(self.windowSize>>self.windowScale),
		uint32(math.MaxUint16),
	))
}

// Returns a nonzero millisecond timestamp for RFC 7323 packets. TCP compares
// these values modulo 32 bits, so process-relative monotonic time is enough.
func (self *ConnectionState) timestampValue() uint32 {
	if self.timestampValueForTest != nil {
		return self.timestampValueForTest()
	}
	return self.timestampOffset + uint32(time.Since(tcpTimestampEpoch)/time.Millisecond) + 1
}

// Tracks the newest timestamp observed from the source. The sequence mutex
// must be held. Reordered older packets do not move the echoed value backward.
func (self *ConnectionState) updateTimestampRecentWithLock(tcp *parsedTcp) {
	if !self.enableTimestamp || !tcp.enableTimestamp {
		return
	}
	// RFC 7323 updates the recent timestamp only when the segment begins at or
	// before the greatest cumulative acknowledgement already sent. A future
	// out-of-order segment is reconsidered when its gap closes; accepting its
	// timestamp here would move the echo clock ahead of the receive frontier.
	if 0 < int32(tcp.seq-self.sendSeq) {
		return
	}
	if self.timestampRecent == 0 || 0 <= int32(tcp.timestampValue-self.timestampRecent) {
		self.timestampRecent = tcp.timestampValue
	}
}

// SynAck builds the syn-ack for the connect handshake into a single pool
// buffer, advertising the mss and negotiated window/timestamp extensions.
func (self *ConnectionState) SynAck(mtu int) ([]byte, error) {
	return self.synAckWithSequence(mtu, self.receiveSeq)
}

// synAckWithSequence builds a SYN-ACK at the original handshake sequence.
// The ordinary first-send path passes receiveSeq; a retransmission passes the
// preserved initial value after receiveSeq has advanced past SYN's sequence
// byte.
func (self *ConnectionState) synAckWithSequence(mtu int, sequence uint32) ([]byte, error) {
	var ipHeaderByteCount int
	switch self.ipVersion {
	case 4:
		ipHeaderByteCount = Ipv4HeaderSizeWithoutExtensions
	case 6:
		ipHeaderByteCount = Ipv6HeaderSize
	}

	// MSS (kind 2, length 4), optional window scale (kind 3, length 3), and
	// timestamp (kind 8, length 10), zero padded to a header word.
	optionsByteCount := 4
	if self.enableWindowScale {
		optionsByteCount += 3
	}
	if self.enableTimestamp {
		optionsByteCount += 10
	}
	paddedOptionsByteCount := (optionsByteCount + 3) &^ 3

	tcpHeaderByteCount := TcpHeaderSizeWithoutExtensions + paddedOptionsByteCount
	packet := MessagePoolGet(ipHeaderByteCount + tcpHeaderByteCount)
	switch self.ipVersion {
	case 4:
		writeIpv4Header(packet, ipProtocolNumberTcp, self.destinationIp, self.sourceIp)
	case 6:
		writeIpv6Header(packet, ipProtocolNumberTcp, self.destinationIp, self.sourceIp)
	}

	tcp := packet[ipHeaderByteCount:]
	binary.BigEndian.PutUint16(tcp[0:2], uint16(self.destinationPort))
	binary.BigEndian.PutUint16(tcp[2:4], uint16(self.sourcePort))
	binary.BigEndian.PutUint32(tcp[4:8], sequence)
	binary.BigEndian.PutUint32(tcp[8:12], self.sendSeq)
	tcp[12] = byte(tcpHeaderByteCount/4) << 4
	tcp[13] = tcpFlagSyn | tcpFlagAck
	// RFC 7323 applies the negotiated scale only after the handshake. A SYN's
	// Window field is always the literal, unscaled 16-bit value.
	binary.BigEndian.PutUint16(tcp[14:16], uint16(min(self.windowSize, uint32(math.MaxUint16))))
	// checksum, set below
	tcp[16] = 0
	tcp[17] = 0
	// urgent
	tcp[18] = 0
	tcp[19] = 0
	options := tcp[TcpHeaderSizeWithoutExtensions:]
	clear(options)
	// advertise the mss so the source does not segment to a conservative default
	options[0] = 2
	options[1] = 4
	binary.BigEndian.PutUint16(options[2:4], uint16(mtu-ipHeaderByteCount-TcpHeaderSizeWithoutExtensions))
	optionIndex := 4
	if self.enableTimestamp {
		options[optionIndex] = 8
		options[optionIndex+1] = 10
		binary.BigEndian.PutUint32(options[optionIndex+2:optionIndex+6], self.timestampValue())
		binary.BigEndian.PutUint32(options[optionIndex+6:optionIndex+10], self.timestampRecent)
		optionIndex += 10
	}
	if self.enableWindowScale {
		if self.enableTimestamp {
			options[optionIndex] = 1
			optionIndex += 1
		}
		options[optionIndex] = 3
		options[optionIndex+1] = 3
		options[optionIndex+2] = byte(self.windowScale)
	}
	binary.BigEndian.PutUint16(tcp[16:18], transportChecksum(ipProtocolNumberTcp, self.destinationIp, self.sourceIp, tcp))
	return packet, nil
}

func (self *ConnectionState) PureAck() ([]byte, error) {
	return self.tcpPacket(tcpFlagAck, self.receiveSeq, nil), nil
}

func (self *ConnectionState) FinAck() ([]byte, error) {
	return self.tcpPacket(tcpFlagAck|tcpFlagFin, self.receiveSeq, nil), nil
}

func (self *ConnectionState) RstAck() ([]byte, error) {
	return self.tcpPacket(tcpFlagAck|tcpFlagRst, self.receiveSeq, nil), nil
}

func (self *ConnectionState) DataPackets(payload []byte, n int, mtu int) ([][]byte, error) {
	var ipHeaderByteCount int
	switch self.ipVersion {
	case 4:
		ipHeaderByteCount = Ipv4HeaderSizeWithoutExtensions
	case 6:
		ipHeaderByteCount = Ipv6HeaderSize
	}
	headerByteCount := ipHeaderByteCount + TcpHeaderSizeWithoutExtensions
	if self.enableTimestamp {
		headerByteCount += tcpTimestampOptionByteCount
	}

	mtu = self.clampPathMtu(mtu)
	packetByteCount := mtu - headerByteCount
	if self.peerMss != 0 {
		optionByteCount := headerByteCount - ipHeaderByteCount - TcpHeaderSizeWithoutExtensions
		packetByteCount = min(packetByteCount, int(self.peerMss)-optionByteCount)
	}
	packetByteCount = max(1, packetByteCount)
	if n <= packetByteCount {
		// reuse the single-packet backing for the common unsegmented case
		// (see singleDataPacket); the result is consumed before the next call
		self.singleDataPacket[0] = self.tcpPacket(tcpFlagAck, self.receiveSeq, payload[0:n])
		return self.singleDataPacket[:], nil
	}
	// segment
	packets := make([][]byte, 0, (n+packetByteCount-1)/packetByteCount)
	for i := 0; i < n; {
		j := min(i+packetByteCount, n)
		packets = append(packets, self.tcpPacket(tcpFlagAck, self.receiveSeq+uint32(i), payload[i:j]))
		i = j
	}
	return packets, nil
}

// builds a tcp packet from the stream destination to the stream source
// into a single pool buffer. the ack number is always set.
func (self *ConnectionState) tcpPacket(flags byte, seq uint32, payload []byte) []byte {
	var ipHeaderByteCount int
	switch self.ipVersion {
	case 4:
		ipHeaderByteCount = Ipv4HeaderSizeWithoutExtensions
	case 6:
		ipHeaderByteCount = Ipv6HeaderSize
	}

	tcpHeaderByteCount := TcpHeaderSizeWithoutExtensions
	if self.enableTimestamp {
		tcpHeaderByteCount += tcpTimestampOptionByteCount
	}
	packet := MessagePoolGet(ipHeaderByteCount + tcpHeaderByteCount + len(payload))
	switch self.ipVersion {
	case 4:
		writeIpv4Header(packet, ipProtocolNumberTcp, self.destinationIp, self.sourceIp)
	case 6:
		writeIpv6Header(packet, ipProtocolNumberTcp, self.destinationIp, self.sourceIp)
	}

	tcp := packet[ipHeaderByteCount:]
	binary.BigEndian.PutUint16(tcp[0:2], uint16(self.destinationPort))
	binary.BigEndian.PutUint16(tcp[2:4], uint16(self.sourcePort))
	binary.BigEndian.PutUint32(tcp[4:8], seq)
	binary.BigEndian.PutUint32(tcp[8:12], self.sendSeq)
	tcp[12] = byte(tcpHeaderByteCount/4) << 4
	tcp[13] = flags
	binary.BigEndian.PutUint16(tcp[14:16], self.encodedWindowSize())
	// checksum, set below
	tcp[16] = 0
	tcp[17] = 0
	// urgent
	tcp[18] = 0
	tcp[19] = 0
	options := tcp[TcpHeaderSizeWithoutExtensions:tcpHeaderByteCount]
	optionIndex := 0
	if self.enableTimestamp {
		options[optionIndex] = 1
		options[optionIndex+1] = 1
		options[optionIndex+2] = 8
		options[optionIndex+3] = 10
		binary.BigEndian.PutUint32(options[optionIndex+4:optionIndex+8], self.timestampValue())
		binary.BigEndian.PutUint32(options[optionIndex+8:optionIndex+12], self.timestampRecent)
		optionIndex += tcpTimestampOptionByteCount
	}
	copy(tcp[tcpHeaderByteCount:], payload)
	binary.BigEndian.PutUint16(tcp[16:18], transportChecksum(ipProtocolNumberTcp, self.destinationIp, self.sourceIp, tcp))
	return packet
}

func DefaultRemoteUserNatProviderSettings() *RemoteUserNatProviderSettings {
	return DefaultRemoteUserNatProviderSettingsWithMemoryTarget(0)
}

// DefaultRemoteUserNatProviderSettingsWithMemoryTarget sizes the provider
// wrapper from the owner's provider memory target. 0 keeps the legacy
// process-budget-scaled bounds.
func DefaultRemoteUserNatProviderSettingsWithMemoryTarget(targetByteCount ByteCount) *RemoteUserNatProviderSettings {
	// bounds the per-source return provide mode map (see
	// `recordSourceProvideMode`): derived from the provider memory target
	// when set (~1 KiB of target per tracked source), else scaled by the
	// process memory budget
	maxSourceCount := MemoryScaledCount(8192, 1024)
	if 0 < targetByteCount {
		maxSourceCount = max(1024, int(targetByteCount/kib(1)))
	}
	return &RemoteUserNatProviderSettings{
		WriteTimeout:            30 * time.Second,
		ReturnSendRetryTimeout:  10 * time.Millisecond,
		ReturnSendWorkerCount:   8,
		ReturnSendQueueSize:     MemoryScaledCount(256, 64),
		ProtocolVersion:         DefaultProtocolVersion,
		SecurityPolicyGenerator: DefaultProviderSecurityPolicyWithStats,
		EventEpoch:              1 * time.Second,
		MaxSourceCount:          maxSourceCount,
		IngressDispatchTimeout:  0,

		// twice the NAT's zero-progress bound for an upstream TCP write
		// (`TcpBufferSettings.WriteTimeout`), which already tolerates tens of
		// seconds of acknowledgement starvation on a live flow
		ReturnSendAbandonTimeout: 120 * time.Second,
	}
}

type RemoteUserNatProviderSettings struct {
	WriteTimeout time.Duration
	// ReturnSendRetryTimeout is the strict pacing floor between failed TCP
	// sender admissions. Zero retains the default.
	ReturnSendRetryTimeout time.Duration

	// ReturnSendAbandonTimeout bounds how long a socket-owned TCP return may
	// go unadmitted before its source is treated as unreachable and released.
	// A connected destination acknowledges Transfer on receipt, so admission
	// that makes no progress for this long means the destination is gone.
	// The check runs after an attempt returns, and one attempt may wait
	// WriteTimeout, so a release lands up to WriteTimeout later. Nothing is
	// released while the backend is degraded, when no destination can get a
	// contract. A non-positive value retries until the source or provider
	// closes.
	ReturnSendAbandonTimeout time.Duration

	// ReturnSendWorkerCount is the number of datagram sender shards used after
	// the local NAT's borrowed-packet callback. Zero retains the default.
	ReturnSendWorkerCount int

	// ReturnSendQueueSize is the aggregate datagram item capacity divided
	// across return sender shards. Zero retains the default.
	ReturnSendQueueSize int

	ProtocolVersion int

	SecurityPolicyGenerator func(context.Context, *SecurityPolicyStatsCollector) SecurityPolicy

	// epoch to flush packet stats events to listeners
	EventEpoch time.Duration

	// the maximum number of sources tracked for return provide modes.
	// 0 is no limit.
	MaxSourceCount int

	// IngressDispatchTimeout is retained for settings compatibility. Provider
	// ClientReceive always uses a zero-timeout NAT handoff because receive
	// callbacks may not block.
	IngressDispatchTimeout time.Duration

	// ReturnSendObserver is a nil-by-default integration seam that publishes one
	// Started/Completed pair for every provider return item. Completed follows
	// final send/drop accounting. It must not block; a panic is recovered and
	// logged without stopping return processing.
	ReturnSendObserver func(RemoteUserNatProviderReturnSendObservation)

	// SourceLifecycleSaturated reports once when permanent Reliability
	// tombstones consume the source bound. The owner must schedule replacement
	// and return promptly; provider callbacks never join their own Client tree.
	SourceLifecycleSaturated func()
}

// RemoteUserNatProviderReturnSendPhase identifies exact item admission and
// terminal send disposition boundaries.
type RemoteUserNatProviderReturnSendPhase uint8

const (
	// RemoteUserNatProviderReturnSendPhaseStarted publishes before queue
	// admission is attempted, so every item has an observable disposition.
	RemoteUserNatProviderReturnSendPhaseStarted RemoteUserNatProviderReturnSendPhase = iota + 1
	// RemoteUserNatProviderReturnSendPhaseCompleted publishes after cumulative
	// success or failure accounting for the same token.
	RemoteUserNatProviderReturnSendPhaseCompleted
)

// A comparable value identifies one authenticated peer's IP flow without
// retaining packet buffers or transport-route state. Valid is false when the
// first packet cannot be parsed; DestinationId still identifies its peer.
type RemoteUserNatProviderReturnFlowKey struct {
	DestinationId   Id
	SourceIp        [16]byte
	DestinationIp   [16]byte
	Protocol        IpProtocol
	SourcePort      uint16
	DestinationPort uint16
	IpVersion       uint8
	Valid           bool
}

// RemoteUserNatProviderReturnSendObservation identifies one return item's
// admission attempt and final disposition without cumulative-counter polling.
type RemoteUserNatProviderReturnSendObservation struct {
	Phase           RemoteUserNatProviderReturnSendPhase
	Token           uint64
	FlowKey         RemoteUserNatProviderReturnFlowKey
	PacketCount     int
	PacketByteCount ByteCount
	Sent            bool
}

// Cumulative admission and downstream-capacity failures remain separate from
// security-policy Block packet statistics.
type ProviderCongestionDrops struct {
	IngressNatPacketCount  int64
	IngressNatByteCount    ByteCount
	ReturnQueuePacketCount int64
	ReturnQueueByteCount   ByteCount
	ReturnSendPacketCount  int64
	ReturnSendByteCount    ByteCount
}

// Atomic provider congestion counters are safe at callback and sender-worker
// boundaries without adding a shared provider lock to either hot path.
type providerCongestionDropCounters struct {
	ingressNatPacketCount  atomic.Int64
	ingressNatByteCount    atomic.Int64
	returnQueuePacketCount atomic.Int64
	returnQueueByteCount   atomic.Int64
	returnSendPacketCount  atomic.Int64
	returnSendByteCount    atomic.Int64
}

// Returns one stable-enough cumulative snapshot of independent atomics.
func (self *providerCongestionDropCounters) snapshot() ProviderCongestionDrops {
	return ProviderCongestionDrops{
		IngressNatPacketCount:  self.ingressNatPacketCount.Load(),
		IngressNatByteCount:    ByteCount(self.ingressNatByteCount.Load()),
		ReturnQueuePacketCount: self.returnQueuePacketCount.Load(),
		ReturnQueueByteCount:   ByteCount(self.returnQueueByteCount.Load()),
		ReturnSendPacketCount:  self.returnSendPacketCount.Load(),
		ReturnSendByteCount:    ByteCount(self.returnSendByteCount.Load()),
	}
}

// Records packets rejected by the zero-timeout provider-to-NAT handoff.
func (self *providerCongestionDropCounters) addIngressNat(packetCount int, byteCount ByteCount) {
	self.ingressNatPacketCount.Add(int64(packetCount))
	self.ingressNatByteCount.Add(int64(byteCount))
}

// Records packet shares rejected by a full datagram queue or closed return
// admission.
func (self *providerCongestionDropCounters) addReturnQueue(packetCount int, byteCount ByteCount) {
	self.returnQueuePacketCount.Add(int64(packetCount))
	self.returnQueueByteCount.Add(int64(byteCount))
}

// Records return packets rejected by the downstream client send buffer.
func (self *providerCongestionDropCounters) addReturnSend(packetCount int, byteCount ByteCount) {
	self.returnSendPacketCount.Add(int64(packetCount))
	self.returnSendByteCount.Add(int64(byteCount))
}

// providerReturnItem owns packet shares once the local NAT callback hands them
// to provider return processing. Exactly one of packet or packets is set.
type providerReturnItem struct {
	source          TransferPath
	transferKey     TransferKey
	provideMode     protocol.ProvideMode
	recoveryMode    receiveRecoveryMode
	ipProtocol      IpProtocol
	packet          []byte
	packets         [][]byte
	packetByteCount ByteCount
	batch           bool
	schedulingKey   sendSchedulingKey
	observer        func(RemoteUserNatProviderReturnSendObservation)
	observerToken   uint64
	observerFlowKey RemoteUserNatProviderReturnFlowKey
	// afterRelease runs only after this item's packet ownership has reached a
	// terminal disposition. Rare control-plane telemetry uses it to stay
	// behind a synthesized TCP reset instead of competing with that reset for
	// the same bounded send-sequence slot.
	afterRelease func()
	// sourceLifecycle keeps the exact source behind the terminal verdict gate
	// until this queued or in-flight return reaches final disposition.
	sourceLifecycle *providerSourceLifecycle
}

// One sender-worker result exposes the exact completed accounting edge to
// deterministic tests without polling cumulative counters.
type providerReturnSendResult struct {
	packetCount     int
	packetByteCount ByteCount
	sent            bool
}

// safeReturnSendObserve keeps optional integration code from terminating a
// NAT callback or its ordered provider return worker.
func safeReturnSendObserve(
	observer func(RemoteUserNatProviderReturnSendObservation),
	observation RemoteUserNatProviderReturnSendObservation,
) {
	if observer == nil {
		return
	}
	HandleError(func() {
		observer(observation)
	})
}

// Allocates a nonzero token even across the atomic counter's wrap boundary.
func (self *RemoteUserNatProvider) nextReturnSendToken() uint64 {
	for {
		token := self.nextReturnSendObserverToken.Add(1)
		if token != 0 {
			return token
		}
	}
}

// Observer-only parsing normalizes IPv4 into the first four address bytes and
// IPv6 into all sixteen. The IP version keeps those representations distinct.
func remoteUserNatProviderReturnFlowKey(
	destinationId Id,
	packet []byte,
) RemoteUserNatProviderReturnFlowKey {
	key := RemoteUserNatProviderReturnFlowKey{DestinationId: destinationId}
	ipPath, err := ParseIpPath(packet)
	if err != nil || ipPath == nil || destinationId == (Id{}) ||
		ipPath.SourcePort < 0 || 65535 < ipPath.SourcePort ||
		ipPath.DestinationPort < 0 || 65535 < ipPath.DestinationPort {
		return key
	}
	var sourceIp net.IP
	var destinationIp net.IP
	switch ipPath.Version {
	case 4:
		sourceIp = ipPath.SourceIp.To4()
		destinationIp = ipPath.DestinationIp.To4()
	case 6:
		sourceIp = ipPath.SourceIp.To16()
		destinationIp = ipPath.DestinationIp.To16()
	default:
		return key
	}
	if sourceIp == nil || destinationIp == nil || ipPath.Protocol == IpProtocolUnknown {
		return key
	}
	copy(key.SourceIp[:], sourceIp)
	copy(key.DestinationIp[:], destinationIp)
	key.Protocol = ipPath.Protocol
	key.SourcePort = uint16(ipPath.SourcePort)
	key.DestinationPort = uint16(ipPath.DestinationPort)
	key.IpVersion = uint8(ipPath.Version)
	key.Valid = true
	return key
}

// Captures the configured observer and publishes exactly one Started phase.
func (self *RemoteUserNatProvider) beginReturnSendObservation(item *providerReturnItem) {
	observer := self.settings.ReturnSendObserver
	if observer == nil || item.observerToken != 0 {
		return
	}
	item.observer = observer
	item.observerToken = self.nextReturnSendToken()
	item.observerFlowKey = remoteUserNatProviderReturnFlowKey(
		item.source.SourceId,
		item.firstPacket(),
	)
	safeReturnSendObserve(observer, RemoteUserNatProviderReturnSendObservation{
		Phase:           RemoteUserNatProviderReturnSendPhaseStarted,
		Token:           item.observerToken,
		FlowKey:         item.observerFlowKey,
		PacketCount:     item.packetCount(),
		PacketByteCount: item.packetByteCount,
	})
}

// Completion is the final exported observer publication for one admitted item.
func (self *RemoteUserNatProvider) completeReturnSendObservation(
	item *providerReturnItem,
	result providerReturnSendResult,
) {
	observer := item.observer
	token := item.observerToken
	flowKey := item.observerFlowKey
	item.observer = nil
	item.observerToken = 0
	item.observerFlowKey = RemoteUserNatProviderReturnFlowKey{}
	if observer == nil || token == 0 {
		return
	}
	safeReturnSendObserve(observer, RemoteUserNatProviderReturnSendObservation{
		Phase:           RemoteUserNatProviderReturnSendPhaseCompleted,
		Token:           token,
		FlowKey:         flowKey,
		PacketCount:     result.packetCount,
		PacketByteCount: result.packetByteCount,
		Sent:            result.sent,
	})
}

// Actual sender attempts also retain the older private deterministic hook.
func (self *RemoteUserNatProvider) observeReturnSend(
	item *providerReturnItem,
	result providerReturnSendResult,
) {
	self.completeReturnSendObservation(item, result)
	if self.afterReturnSendForTest != nil {
		self.afterReturnSendForTest(result)
	}
}

// packetCount reports the number of packet shares owned by this queue item.
func (self *providerReturnItem) packetCount() int {
	if self.batch {
		return len(self.packets)
	}
	return 1
}

// firstPacket returns the flow tuple used to select an ordered sender shard.
func (self *providerReturnItem) firstPacket() []byte {
	if self.packet != nil {
		return self.packet
	}
	if 0 < len(self.packets) {
		return self.packets[0]
	}
	return nil
}

// returnPackets releases packet shares that have not moved into a send pack.
func (self *providerReturnItem) returnPackets() {
	if self.packet != nil {
		MessagePoolReturn(self.packet)
		self.packet = nil
	}
	for packetIndex, packet := range self.packets {
		MessagePoolReturn(packet)
		self.packets[packetIndex] = nil
	}
	self.packets = self.packets[:0]
}

// Returns the exact source cancellation boundary when this item was admitted;
// direct unit fixtures without a lifecycle retain the provider fallback.
func (self *providerReturnItem) sendContext(fallback context.Context) context.Context {
	if self.sourceLifecycle != nil {
		return self.sourceLifecycle.ctx
	}
	return fallback
}

// One source gate linearizes callback admission with a terminal Reliability
// verdict. Healthy gates are reclaimed at the final admission; terminal gates
// remain for the provider generation lifetime.
type providerSourceLifecycle struct {
	sourceId   Id
	ctx        context.Context
	cancel     context.CancelFunc
	admissions *lifecycleAdmission
	terminal   bool
	// Terminal lifecycles are their own pending cleanup nodes, so the status
	// path allocates no second capacity-sized queue.
	retirementNext *providerSourceLifecycle
}

type RemoteUserNatProvider struct {
	ctx            context.Context
	client         *Client
	cancel         context.CancelFunc
	localUserNat   *LocalUserNat
	securityPolicy SecurityPolicy
	// Immutable provider identity sent to each authenticated source on its
	// first packet. The policy digest is computed once at construction, never
	// on a packet hot path.
	buildVersion       string
	securityPolicyHash string
	// Defense in depth at the exit. Client and provider policies are
	// intentionally independent, and multiple tunnel clients can share an IP
	// tuple, so smtpEgressGuard namespaces its state by the authenticated source.
	smtpIngressGuard     smtpEgressGuard
	ingressIpv4Fragments ipv4FragmentGate
	egressIpv4Fragments  ipv4FragmentGate
	settings             *RemoteUserNatProviderSettings
	localUserNatUnsub    func()
	clientUnsub          func()
	contractStatusUnsub  func()
	// Joins an inline status callback captured by ContractManager before the
	// callback was unsubscribed. The callback only closes one source gate and
	// appends its existing lifecycle node to the fixed-worker pending list.
	contractStatusAdmissions *lifecycleAdmission

	// cumulative packet counts relayed for remote clients, in the same
	// direction convention as the contracts: ingress is traffic received from
	// the tunnel (remote clients' egress), egress is the return into the tunnel
	packetStatsCounters         *packetStatsCounters
	packetStatsCallbacks        *CallbackList[PacketStatsFunction]
	congestionDrops             providerCongestionDropCounters
	nextReturnSendObserverToken atomic.Uint64

	// the return provide mode recorded per source (see recordSourceProvideMode),
	// so the return path can echo it. A source on the same network sends under
	// ProvideMode_Network and its return traffic should also be network mode,
	// which skips the public security rules and forgoes the companion contract.
	stateLock         sync.Mutex
	sourceProvideMode map[Id]protocol.ProvideMode
	// sourceDiagnostics holds source-scoped block counters and the publication
	// generation. It is bounded and evicted with sourceProvideMode.
	sourceDiagnostics map[Id]*providerSourceDiagnostics
	// sourceP2pPriorityRefresh rate-limits Network-peer admission promotion
	// on the provider packet path. Entries are bounded with
	// sourceProvideMode and removed on the same arbitrary safe eviction.
	sourceP2pPriorityRefresh map[Id]time.Time
	// sourceLifecycles contains only active healthy sources and permanent
	// terminal tombstones. Healthy entries disappear at their final admission;
	// terminal entries never expire or evict within this provider generation.
	sourceLifecycles            map[Id]*providerSourceLifecycle
	terminalSourceCount         int
	sourceLifecycleClosed       bool
	sourceLifecycleSaturated    bool
	sourceRetirementOwnerId     uint64
	sourceRetirementNotify      chan struct{}
	sourceRetirementHead        *providerSourceLifecycle
	sourceRetirementTail        *providerSourceLifecycle
	sourceRetirementClosed      bool
	sourceRetirementWorkerCount int
	sourceRetirementWorkers     sync.WaitGroup

	// sources with an unreachable release in progress
	unreachableSourceReleases       map[Id]bool
	unreachableSourceReleaseWorkers sync.WaitGroup
	// transient retirement owners of sources that turned terminal during
	// their release, kept for the generation like the terminal tombstone
	unreachableTerminalOwnerIds []uint64
	// the packet stats epoch worker started (on the first callback)
	packetStatsStarted bool

	// Nonblocking callbacks share borrowed packets into bounded sender shards
	// and drop when their shard is full. Only actual upstream socket returns
	// bypass those queues and retry synchronously on their dedicated TCP flow
	// reader. Packet protocol alone never grants synchronous recovery ownership.
	// Identical source/flow tuples always select the same worker.
	returnStateLock  sync.RWMutex
	returnClosed     bool
	returnSendQueues []chan *providerReturnItem
	returnItemPool   chan *providerReturnItem
	returnAdmissions sync.WaitGroup
	returnWorkers    sync.WaitGroup
	closeOnce        sync.Once

	// Test-only synchronization seams. Nil keeps production callbacks and
	// sender workers unchanged.
	returnSendStarted                   func()
	beforeTcpReturnSendRetryForTest     func()
	beforeTcpReturnReleaseForTest       func()
	beforeReturnAdmissionsWaitForTest   func()
	afterReturnEnqueueForTest           func(bool)
	afterReturnSendAttemptForTest       func(providerReturnSendResult)
	afterReturnSendForTest              func(providerReturnSendResult)
	afterReturnReleaseForTest           func()
	returnAckTargetForTest              sendAckTarget
	afterContractStatusAdmissionForTest func()
	beforeSourceAdmissionForTest        func(Id)
	afterSourceAdmissionForTest         func(Id)
	beforeLocalUserNatAdmissionForTest  func(Id, [][]byte)
	beforeSourceRetirementWaitForTest   func(Id)
	afterReceiveSourceRetirementForTest func(Id)
	afterSendSourceRetirementForTest    func(Id)
	afterSourceRetirementForTest        func(Id)

	afterUnreachableSourceReleaseForTest func(Id)
	backendDegradedForTest               func() bool
}

func NewRemoteUserNatProviderWithDefaults(
	client *Client,
	localUserNat *LocalUserNat,
) *RemoteUserNatProvider {
	return NewRemoteUserNatProvider(client, localUserNat, DefaultRemoteUserNatProviderSettings())
}

func NewRemoteUserNatProvider(
	client *Client,
	localUserNat *LocalUserNat,
	settings *RemoteUserNatProviderSettings,
) *RemoteUserNatProvider {
	// the security policy runs a background scan goroutine; scope it to this provider (a child of
	// the client ctx) so Close stops it, rather than leaking it for the life of the client
	cancelCtx, cancel := context.WithCancel(client.Ctx())
	securityPolicy := settings.SecurityPolicyGenerator(cancelCtx, DefaultSecurityPolicyStatsCollector())
	userNatProvider := &RemoteUserNatProvider{
		ctx:                      cancelCtx,
		client:                   client,
		cancel:                   cancel,
		localUserNat:             localUserNat,
		securityPolicy:           securityPolicy,
		buildVersion:             BuildVersion(),
		securityPolicyHash:       SecurityPolicyHash(securityPolicy),
		settings:                 settings,
		packetStatsCounters:      &packetStatsCounters{},
		packetStatsCallbacks:     NewCallbackList[PacketStatsFunction](),
		sourceProvideMode:        map[Id]protocol.ProvideMode{},
		sourceDiagnostics:        map[Id]*providerSourceDiagnostics{},
		sourceP2pPriorityRefresh: map[Id]time.Time{},
		sourceLifecycles:         map[Id]*providerSourceLifecycle{},
		contractStatusAdmissions: newLifecycleAdmission(),
	}
	userNatProvider.sourceRetirementOwnerId = localUserNat.newSourceRetirementOwner()
	userNatProvider.startReturnSenders()

	// Register both return paths. No-contract peers can take the NAT's drained
	// batch directly; contract-bearing peers are fanned back to Receive so the
	// transfer sequence can leave contract heads unbatched and only coalesce
	// already-queued frames when the current contract has room. Synthesized
	// control packets always arrive through the per-packet callback.
	localUserNatBatchUnsub := localUserNat.addReceiveTransferPacketsCallback(
		userNatProvider.receiveTransferBatchWithRecovery,
	)
	localUserNatPacketUnsub := localUserNat.addReceiveTransferPacketCallback(
		userNatProvider.receiveTransferWithRecovery,
	)
	localUserNatFlowCloseUnsub := localUserNat.addTcpFlowCloseCallback(
		userNatProvider.tcpFlowClosed,
	)
	localUserNatUnsub := func() {
		localUserNatBatchUnsub()
		localUserNatPacketUnsub()
		localUserNatFlowCloseUnsub()
	}
	userNatProvider.localUserNatUnsub = localUserNatUnsub
	clientUnsub := client.AddReceiveCallback(userNatProvider.ClientReceive)
	userNatProvider.clientUnsub = clientUnsub
	userNatProvider.contractStatusUnsub = client.ContractManager().addContractStatusDispatchCallback(
		func(status *ContractStatus) {
			if !userNatProvider.contractStatusAdmissions.start() {
				return
			}
			defer userNatProvider.contractStatusAdmissions.finish()
			if userNatProvider.afterContractStatusAdmissionForTest != nil {
				userNatProvider.afterContractStatusAdmissionForTest()
			}
			userNatProvider.contractStatus(status)
		},
	)
	context.AfterFunc(cancelCtx, userNatProvider.Close)

	return userNatProvider
}

// Resolves a hard bound even for legacy settings that used zero to leave only
// the return-mode cache unlimited. Permanent rejection state is never allowed
// to grow without a ceiling.
func (self *RemoteUserNatProvider) sourceLifecycleMaxCount() int {
	if self.settings != nil && 0 < self.settings.MaxSourceCount {
		return self.settings.MaxSourceCount
	}
	return MemoryScaledCount(8192, 1024)
}

// Starts a fixed cleanup cohort behind an intrusive pending list. Each
// terminal lifecycle already owns one node, so even a very large configured
// source ceiling does not allocate a capacity-sized channel backing array.
func (self *RemoteUserNatProvider) startSourceRetirementWorkers() {
	if self.sourceRetirementNotify != nil {
		return
	}
	self.sourceRetirementNotify = make(chan struct{}, 1)
	workerCount := 8
	if self.settings != nil && 0 < self.settings.ReturnSendWorkerCount {
		workerCount = min(8, self.settings.ReturnSendWorkerCount)
	}
	workerCount = max(1, workerCount)
	self.sourceRetirementWorkerCount = workerCount
	for range workerCount {
		self.sourceRetirementWorkers.Add(1)
		go HandleError(func() {
			defer self.sourceRetirementWorkers.Done()
			self.runSourceRetirementWorker()
		})
	}
}

// Wakes at most one cleanup consumer; each pop wakes another while work
// remains, allowing the fixed cohort to progress concurrently.
func (self *RemoteUserNatProvider) wakeSourceRetirementWorkers() {
	select {
	case self.sourceRetirementNotify <- struct{}{}:
	default:
	}
}

func (self *RemoteUserNatProvider) runSourceRetirementWorker() {
	for {
		<-self.sourceRetirementNotify
		for {
			self.stateLock.Lock()
			sourceLifecycle := self.sourceRetirementHead
			if sourceLifecycle != nil {
				self.sourceRetirementHead = sourceLifecycle.retirementNext
				sourceLifecycle.retirementNext = nil
				if self.sourceRetirementHead == nil {
					self.sourceRetirementTail = nil
				}
			}
			more := self.sourceRetirementHead != nil
			closed := self.sourceRetirementClosed && !more && sourceLifecycle == nil
			self.stateLock.Unlock()
			if more {
				self.wakeSourceRetirementWorkers()
			}
			if sourceLifecycle == nil {
				if closed {
					self.wakeSourceRetirementWorkers()
					return
				}
				break
			}
			self.retireSourceLifecycle(sourceLifecycle.sourceId, sourceLifecycle)
		}
	}
}

// Admits one exact source operation against the terminal verdict boundary.
// The provider lock remains held through the gate admission, so a concurrent
// Reliability status either observes this producer or wins first and rejects
// it. Healthy entries at the concurrent cap recover when producers finish.
func (self *RemoteUserNatProvider) acquireSourceLifecycle(sourceId Id) *providerSourceLifecycle {
	if sourceId == (Id{}) {
		return nil
	}
	if self.beforeSourceAdmissionForTest != nil {
		self.beforeSourceAdmissionForTest(sourceId)
	}
	self.stateLock.Lock()
	if self.sourceLifecycleClosed {
		self.stateLock.Unlock()
		return nil
	}
	if self.sourceLifecycles == nil {
		self.sourceLifecycles = map[Id]*providerSourceLifecycle{}
	}
	sourceLifecycle := self.sourceLifecycles[sourceId]
	if sourceLifecycle == nil {
		if self.sourceLifecycleSaturated ||
			self.sourceLifecycleMaxCount() <= len(self.sourceLifecycles) {
			self.stateLock.Unlock()
			return nil
		}
		sourceCtx, cancel := context.WithCancel(self.ctx)
		sourceLifecycle = &providerSourceLifecycle{
			sourceId:   sourceId,
			ctx:        sourceCtx,
			cancel:     cancel,
			admissions: newLifecycleAdmission(),
		}
		self.sourceLifecycles[sourceId] = sourceLifecycle
	}
	admitted := sourceLifecycle.admissions.start()
	self.stateLock.Unlock()
	if !admitted {
		return nil
	}
	if self.afterSourceAdmissionForTest != nil {
		self.afterSourceAdmissionForTest(sourceId)
	}
	return sourceLifecycle
}

// Releases one source operation and reclaims a healthy idle gate at the same
// provider-lock boundary as later admission and terminal status handling.
func (self *RemoteUserNatProvider) releaseSourceLifecycle(
	sourceId Id,
	sourceLifecycle *providerSourceLifecycle,
) {
	if sourceLifecycle == nil {
		return
	}
	self.stateLock.Lock()
	idle := sourceLifecycle.admissions.finishAndIdle()
	if idle && !sourceLifecycle.terminal &&
		self.sourceLifecycles[sourceId] == sourceLifecycle {
		delete(self.sourceLifecycles, sourceId)
		sourceLifecycle.cancel()
	}
	self.stateLock.Unlock()
}

// Receives one status on the provider-owned inline callback. Exact Reliability closes
// only the source gate and context synchronously; all protocol cancellation
// and joins run on a tracked source worker, so a stuck source cannot stall
// later status delivery.
func (self *RemoteUserNatProvider) contractStatus(status *ContractStatus) {
	if status == nil || status.Error == nil ||
		*status.Error != protocol.ContractError_Reliability {
		return
	}
	destination := status.Key.Destination
	if destination.SourceId != (Id{}) || destination.StreamId != (Id{}) ||
		destination.DestinationId == (Id{}) ||
		destination.DestinationId == self.client.ClientId() {
		return
	}
	sourceId := destination.DestinationId
	var sourceLifecycle *providerSourceLifecycle
	var saturationCallback func()
	wakeRetirement := false
	notifySaturated := false
	self.stateLock.Lock()
	if !self.sourceLifecycleClosed {
		if self.sourceLifecycles == nil {
			self.sourceLifecycles = map[Id]*providerSourceLifecycle{}
		}
		sourceLifecycle = self.sourceLifecycles[sourceId]
		if sourceLifecycle == nil {
			if self.sourceLifecycleMaxCount() <= len(self.sourceLifecycles) {
				if !self.sourceLifecycleSaturated {
					self.sourceLifecycleSaturated = true
					notifySaturated = true
				}
			} else {
				sourceCtx, cancel := context.WithCancel(self.ctx)
				sourceLifecycle = &providerSourceLifecycle{
					sourceId:   sourceId,
					ctx:        sourceCtx,
					cancel:     cancel,
					admissions: newLifecycleAdmission(),
					terminal:   true,
				}
				self.sourceLifecycles[sourceId] = sourceLifecycle
				self.terminalSourceCount += 1
				sourceLifecycle.admissions.close()
				sourceLifecycle.cancel()
			}
		} else if !sourceLifecycle.terminal {
			sourceLifecycle.terminal = true
			self.terminalSourceCount += 1
			sourceLifecycle.admissions.close()
			sourceLifecycle.cancel()
		} else {
			sourceLifecycle = nil
		}
		if !self.sourceLifecycleSaturated &&
			self.sourceLifecycleMaxCount() <= self.terminalSourceCount {
			self.sourceLifecycleSaturated = true
			notifySaturated = true
		}
		if sourceLifecycle != nil {
			if self.sourceRetirementNotify == nil {
				self.startSourceRetirementWorkers()
			}
			if self.sourceRetirementTail == nil {
				self.sourceRetirementHead = sourceLifecycle
			} else {
				self.sourceRetirementTail.retirementNext = sourceLifecycle
			}
			self.sourceRetirementTail = sourceLifecycle
			wakeRetirement = true
		}
		if notifySaturated && self.settings != nil &&
			self.settings.SourceLifecycleSaturated != nil {
			saturationCallback = self.settings.SourceLifecycleSaturated
		}
	}
	self.stateLock.Unlock()

	if wakeRetirement {
		self.wakeSourceRetirementWorkers()
	}
	if saturationCallback != nil {
		// Ownership transfers to the external provider owner. This callback
		// may need the owner's state lock while that owner is synchronously
		// closing this provider, so Close must not join the handoff.
		go HandleError(saturationCallback)
	}
}

// Publishes the LocalUserNat tombstone before waiting for already-admitted
// callbacks and return items. After that gate drains, both Transfer directions
// are canceled and joined, then source-scoped policy state is discarded.
func (self *RemoteUserNatProvider) retireSourceLifecycle(
	sourceId Id,
	sourceLifecycle *providerSourceLifecycle,
) {
	flowDoneChannels := self.localUserNat.retireSourceForOwner(
		self.sourceRetirementOwnerId,
		sourceId,
	)
	if self.beforeSourceRetirementWaitForTest != nil {
		self.beforeSourceRetirementWaitForTest(sourceId)
	}
	<-sourceLifecycle.admissions.Done()
	for _, done := range flowDoneChannels {
		<-done
	}
	self.client.receiveBuffer.cancelSourceAndWait(sourceId)
	if self.afterReceiveSourceRetirementForTest != nil {
		self.afterReceiveSourceRetirementForTest(sourceId)
	}
	self.client.sendBuffer.cancelDestinationAndWait(sourceId)
	if self.afterSendSourceRetirementForTest != nil {
		self.afterSendSourceRetirementForTest(sourceId)
	}
	self.senderDisconnected(sourceId)
	if self.afterSourceRetirementForTest != nil {
		self.afterSourceRetirementForTest(sourceId)
	}
}

// releaseUnreachableSource ends a source whose socket-owned TCP return made no
// Transfer admission progress for ReturnSendAbandonTimeout. Nothing else ends
// it: Reliability retirement needs the platform's answer to a new contract,
// which a sequence with a full window never requests, so without this the
// source's flows retry and retransmit until the provider restarts, and they
// throttle every other source of the provider while they do.
//
// It is the non-terminal counterpart of retireSourceLifecycle. The stalled
// generation closes its admissions and is cancelled exactly as a terminal one,
// so its retries return and new packets are refused rather than re-admitted
// under a fresh context. Once its producers finish, the source's NAT flows
// and Transfer sequences are retired and joined under a transient owner.
// Releasing that owner and unmapping the generation then readmits the source,
// because a client that reconnects keeps its id. A Reliability status that
// arrives meanwhile makes the generation terminal and keeps it.
func (self *RemoteUserNatProvider) releaseUnreachableSource(
	sourceId Id,
	sourceLifecycle *providerSourceLifecycle,
) {
	if sourceId == (Id{}) || sourceLifecycle == nil {
		return
	}
	self.stateLock.Lock()
	if self.sourceLifecycleClosed ||
		sourceLifecycle.terminal ||
		self.sourceLifecycles[sourceId] != sourceLifecycle ||
		self.unreachableSourceReleases[sourceId] {
		self.stateLock.Unlock()
		return
	}
	if self.unreachableSourceReleases == nil {
		self.unreachableSourceReleases = map[Id]bool{}
	}
	self.unreachableSourceReleases[sourceId] = true
	sourceLifecycle.admissions.close()
	sourceLifecycle.cancel()
	self.unreachableSourceReleaseWorkers.Add(1)
	self.stateLock.Unlock()

	// The caller is a producer of this generation and one of the flows being
	// retired, so the join runs apart from it. Close joins this worker; a
	// provider close cancels every wait.
	go HandleError(func() {
		defer self.unreachableSourceReleaseWorkers.Done()
		ownerId := self.retireUnreachableSource(sourceId, sourceLifecycle)
		keepOwner := false
		self.stateLock.Lock()
		delete(self.unreachableSourceReleases, sourceId)
		if sourceLifecycle.terminal {
			// A Reliability status retired this generation during the release.
			// Its tombstone lasts the provider generation, so the transient
			// claim does too: releasing it here could readmit the NAT source
			// before the terminal retirement claims it.
			if ownerId != 0 {
				self.unreachableTerminalOwnerIds = append(self.unreachableTerminalOwnerIds, ownerId)
				keepOwner = true
			}
		} else if self.sourceLifecycles[sourceId] == sourceLifecycle {
			// a later Reliability status finds no generation and retires anew
			delete(self.sourceLifecycles, sourceId)
		}
		self.stateLock.Unlock()
		if ownerId != 0 && !keepOwner {
			self.localUserNat.releaseSourceRetirementOwner(ownerId)
		}
		if self.afterUnreachableSourceReleaseForTest != nil {
			self.afterUnreachableSourceReleaseForTest(sourceId)
		}
	})
}

// backendDegraded reports the process-wide backend state that gates releases.
func (self *RemoteUserNatProvider) backendDegraded() bool {
	if self.backendDegradedForTest != nil {
		return self.backendDegradedForTest()
	}
	return isBackendDegraded()
}

// Joins the released generation's producers, then retires its NAT flows and
// Transfer sequences. Returns the transient retirement owner, or zero if the
// provider closed first; releasing the owner readmits the NAT source unless
// another owner still retires it.
func (self *RemoteUserNatProvider) retireUnreachableSource(
	sourceId Id,
	sourceLifecycle *providerSourceLifecycle,
) uint64 {
	select {
	case <-sourceLifecycle.admissions.Done():
	case <-self.ctx.Done():
		return 0
	}
	ownerId := self.localUserNat.newSourceRetirementOwner()
	for _, done := range self.localUserNat.retireSourceForOwner(ownerId, sourceId) {
		select {
		case <-done:
		case <-self.ctx.Done():
			return ownerId
		}
	}
	if self.ctx.Err() != nil {
		return ownerId
	}
	self.client.receiveBuffer.cancelSourceAndWait(sourceId)
	if self.ctx.Err() != nil {
		return ownerId
	}
	self.client.sendBuffer.cancelDestinationAndWait(sourceId)
	return ownerId
}

// tcpFlowClosed retires policy state when the provider NAT releases the actual
// upstream TCP lifecycle, including failures without an observed FIN/RST.
func (self *RemoteUserNatProvider) tcpFlowClosed(source TransferPath, ipPath *IpPath) {
	senderClientId := source.LocalMask().SourceId
	self.smtpIngressGuard.retireForOwner(senderClientId, ipPath)
	retireIngressSecurityFlowForSender(self.securityPolicy, senderClientId, ipPath)
}

// Purges all state for a sender after an authenticated platform disconnect.
func (self *RemoteUserNatProvider) senderDisconnected(senderClientId Id) {
	self.smtpIngressGuard.retireOwner(senderClientId)
	retireSecuritySender(self.securityPolicy, senderClientId)
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		delete(self.sourceProvideMode, senderClientId)
		delete(self.sourceP2pPriorityRefresh, senderClientId)
		delete(self.sourceDiagnostics, senderClientId)
	}()
}

// Applies the authoritative disconnect markers delivered on the control path.
func (self *RemoteUserNatProvider) retireDisconnectedSenders(frames []*protocol.Frame) {
	for _, frame := range frames {
		if frame.MessageType != protocol.MessageType_TransferNetworkPeersUpdate {
			continue
		}
		message, err := FromFrame(frame)
		if err != nil {
			continue
		}
		update, ok := message.(*protocol.NetworkPeersUpdate)
		if !ok {
			continue
		}
		for _, networkPeer := range update.Peers {
			if networkPeer.DisconnectTime == nil {
				continue
			}
			senderClientId, err := IdFromBytes(networkPeer.ClientId)
			if err == nil {
				self.senderDisconnected(senderClientId)
			}
		}
	}
}

// startReturnSenders creates fixed-capacity datagram shards before callbacks
// can admit work. Queue capacity is aggregate, not multiplied by worker count.
func (self *RemoteUserNatProvider) startReturnSenders() {
	workerCount := self.settings.ReturnSendWorkerCount
	if workerCount <= 0 {
		workerCount = 8
	}
	queueSize := self.settings.ReturnSendQueueSize
	if queueSize <= 0 {
		queueSize = MemoryScaledCount(256, 64)
	}
	workerCount = min(workerCount, max(1, queueSize))
	self.returnSendQueues = make([]chan *providerReturnItem, workerCount)
	self.returnItemPool = make(chan *providerReturnItem, queueSize+workerCount)
	for workerIndex := range workerCount {
		workerQueueSize := queueSize / workerCount
		if workerIndex < queueSize%workerCount {
			workerQueueSize += 1
		}
		queue := make(chan *providerReturnItem, workerQueueSize)
		self.returnSendQueues[workerIndex] = queue
		self.returnWorkers.Add(1)
		go HandleError(func() {
			defer self.returnWorkers.Done()
			self.runReturnSender(queue)
		}, self.cancel)
	}
}

// takeReturnItem reuses only provider-owned envelopes; packet buffers retain
// their independent message-pool ownership.
func (self *RemoteUserNatProvider) takeReturnItem() *providerReturnItem {
	select {
	case item := <-self.returnItemPool:
		return item
	default:
		return &providerReturnItem{}
	}
}

// releaseReturnItem returns remaining packet shares and then recycles the
// bounded envelope without blocking its callback or datagram worker.
func (self *RemoteUserNatProvider) releaseReturnItem(item *providerReturnItem) {
	if item == nil {
		return
	}
	if item.observerToken != 0 {
		self.completeReturnSendObservation(item, providerReturnSendResult{
			packetCount:     item.packetCount(),
			packetByteCount: item.packetByteCount,
			sent:            false,
		})
	}
	item.returnPackets()
	afterRelease := item.afterRelease
	item.afterRelease = nil
	sourceId := item.source.SourceId
	sourceLifecycle := item.sourceLifecycle
	item.sourceLifecycle = nil
	packets := item.packets
	*item = providerReturnItem{}
	item.packets = packets
	select {
	case self.returnItemPool <- item:
	default:
	}
	self.releaseSourceLifecycle(sourceId, sourceLifecycle)
	if afterRelease != nil {
		HandleError(afterRelease)
	}
}

// providerReturnSendShard combines the authenticated source with a datagram
// flow tuple so one flow remains ordered without coupling identical private
// tuples from every source.
func providerReturnSendShard(
	source TransferPath,
	packet []byte,
	shardCount int,
) int {
	if shardCount <= 1 {
		return 0
	}
	shard := uint32(sendShard(packet, shardCount))
	sourceHash := uint32(2166136261)
	for _, value := range source.SourceId {
		sourceHash = (sourceHash ^ uint32(value)) * 16777619
	}
	return int((shard + sourceHash) % uint32(shardCount))
}

// Admits one provider return without blocking shared receive pumps. Only a
// dedicated TCP socket reader may request synchronous recovery after upstream
// bytes were consumed; synthesized controls and public callbacks use the
// bounded nonblocking shards regardless of their IP protocol. Admission under
// the read lock lets shutdown stop and join every registered decision without
// holding a state lock across a synchronous send.
func (self *RemoteUserNatProvider) enqueueReturnItem(item *providerReturnItem) bool {
	self.beginReturnSendObservation(item)
	sourceLifecycle := self.acquireSourceLifecycle(item.source.SourceId)
	if sourceLifecycle == nil {
		self.congestionDrops.addReturnQueue(item.packetCount(), item.packetByteCount)
		self.releaseReturnItem(item)
		return false
	}
	item.sourceLifecycle = sourceLifecycle
	if item.recoveryMode == receiveRecoveryModeTcpSocket {
		admitted := false
		self.returnStateLock.RLock()
		if !self.returnClosed {
			self.returnAdmissions.Add(1)
			admitted = true
		}
		self.returnStateLock.RUnlock()
		if admitted {
			self.sendReturnItem(item)
			if self.beforeTcpReturnReleaseForTest != nil {
				self.beforeTcpReturnReleaseForTest()
			}
			self.releaseReturnItem(item)
			if self.afterReturnReleaseForTest != nil {
				self.afterReturnReleaseForTest()
			}
			self.returnAdmissions.Done()
			return true
		}
		self.congestionDrops.addReturnQueue(item.packetCount(), item.packetByteCount)
		self.releaseReturnItem(item)
		return false
	}

	shardIndex := providerReturnSendShard(
		item.source,
		item.firstPacket(),
		len(self.returnSendQueues),
	)
	queued := false
	var queue chan *providerReturnItem
	self.returnStateLock.RLock()
	if !self.returnClosed && 0 < len(self.returnSendQueues) {
		queue = self.returnSendQueues[shardIndex]
		self.returnAdmissions.Add(1)
	}
	self.returnStateLock.RUnlock()
	if queue != nil {
		select {
		case queue <- item:
			queued = true
		default:
		}
		self.returnAdmissions.Done()
	}
	if !queued {
		self.congestionDrops.addReturnQueue(item.packetCount(), item.packetByteCount)
		self.releaseReturnItem(item)
	}
	if self.afterReturnEnqueueForTest != nil {
		self.afterReturnEnqueueForTest(queued)
	}
	return queued
}

// Runs one nonblocking callback shard; packet protocol does not select it.
func (self *RemoteUserNatProvider) runReturnSender(queue <-chan *providerReturnItem) {
	defer func() {
		for {
			select {
			case item := <-queue:
				self.releaseReturnItem(item)
			default:
				return
			}
		}
	}()
	for {
		select {
		case <-self.ctx.Done():
			return
		default:
		}
		select {
		case <-self.ctx.Done():
			return
		case item := <-queue:
			self.sendReturnItem(item)
			self.releaseReturnItem(item)
			if self.afterReturnReleaseForTest != nil {
				self.afterReturnReleaseForTest()
			}
		}
	}
}

// Sends one provider-owned return from either a dedicated TCP socket callback
// or the selected nonblocking worker.
func (self *RemoteUserNatProvider) sendReturnItem(item *providerReturnItem) {
	if self.returnSendStarted != nil {
		self.returnSendStarted()
	}
	packetCount := item.packetCount()
	packetByteCount := item.packetByteCount
	var sent bool
	if item.batch {
		sent = self.sendReturnBatch(item)
	} else if self.client.log.V(2).Enabled() {
		sent = TraceWithReturn(
			fmt.Sprintf(
				"[unps]%s %s->%s",
				item.ipProtocol,
				self.client.ClientTag(),
				item.source.SourceId,
			),
			func() bool {
				return self.sendReturnPacket(item)
			},
		)
	} else {
		sent = self.sendReturnPacket(item)
	}
	self.observeReturnSend(item, providerReturnSendResult{
		packetCount:     packetCount,
		packetByteCount: packetByteCount,
		sent:            sent,
	})
}

func (self *RemoteUserNatProvider) SecurityPolicyStats(reset bool) SecurityPolicyStats {
	return self.securityPolicy.Stats().Stats(reset)
}

// PacketStats returns the cumulative packet counts relayed for remote clients.
// remote ingress is traffic received from the tunnel (remote clients' egress),
// remote egress is the return traffic sent back into the tunnel, and blocked is
// the traffic dropped by the provider security policy
func (self *RemoteUserNatProvider) PacketStats() *PacketStats {
	return self.packetStatsCounters.snapshot()
}

// Returns cumulative congestion losses without changing security Block
// statistics. The independent atomic fields form a stable-enough diagnostic
// snapshot; callers that need a transaction boundary must stop packet flow.
func (self *RemoteUserNatProvider) CongestionDropStats() ProviderCongestionDrops {
	return self.congestionDrops.snapshot()
}

// AddPacketStatsCallback registers a listener fired on the event epoch when the
// stats change. the epoch worker starts on the first callback
func (self *RemoteUserNatProvider) AddPacketStatsCallback(packetStatsCallback PacketStatsFunction) func() {
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		if !self.packetStatsStarted {
			self.packetStatsStarted = true
			go HandleError(self.runPacketStats, self.cancel)
		}
	}()
	callbackId := self.packetStatsCallbacks.Add(packetStatsCallback)
	return func() {
		self.packetStatsCallbacks.Remove(callbackId)
	}
}

// flushes packet stats events to listeners on the event epoch
func (self *RemoteUserNatProvider) runPacketStats() {
	var lastPacketStats *PacketStats
	for {
		select {
		case <-self.ctx.Done():
			return
		case <-time.After(self.settings.EventEpoch):
		}

		if callbacks := self.packetStatsCallbacks.Get(); 0 < len(callbacks) {
			packetStats := self.packetStatsCounters.snapshot()
			if !packetStatsEqual(packetStats, lastPacketStats) {
				lastPacketStats = packetStats
				for _, callback := range callbacks {
					HandleError(func() {
						callback(packetStats)
					})
				}
			}
		}
	}
}

// recordSourceProvideMode remembers the provide mode to echo on a source's
// return path. ProvideMode is a set of flags, not an ordered scale, so this is a
// per-case choice, never a numeric min: prefer the same-Network relationship once
// a source has used it — its Network contract is verified, so the return traffic
// can ride the network relationship and skip the public ingress rules — otherwise
// remember the source's (non-Network) mode so the echo rides a companion contract.
func (self *RemoteUserNatProvider) recordSourceProvideMode(sourceId Id, provideMode protocol.ProvideMode) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	switch existing, ok := self.sourceProvideMode[sourceId]; {
	case !ok:
		// bound the map: evict an arbitrary entry at the cap. Eviction is
		// safe — the return path falls back to the packet's carried provide
		// mode (see `sourceReturnProvideMode`), and an active source
		// re-records on its next inbound packet.
		if maxCount := self.settings.MaxSourceCount; 0 < maxCount && maxCount <= len(self.sourceProvideMode) {
			for evictSourceId := range self.sourceProvideMode {
				delete(self.sourceProvideMode, evictSourceId)
				delete(self.sourceP2pPriorityRefresh, evictSourceId)
				delete(self.sourceDiagnostics, evictSourceId)
				break
			}
		}
		self.sourceProvideMode[sourceId] = provideMode
	case existing == protocol.ProvideMode_Network:
		// already network; keep it
	case provideMode == protocol.ProvideMode_Network:
		self.sourceProvideMode[sourceId] = protocol.ProvideMode_Network
	default:
		self.sourceProvideMode[sourceId] = provideMode
	}
	self.ensureSourceDiagnosticsWithLock(sourceId)
}

const providerP2pPriorityRefreshInterval = 5 * time.Second

// refreshP2pPriority promotes an explicitly selected same-network source out
// of the provider's bounded public-peer admission lottery. The provider sees
// the relationship on relayed data before P2P is established, so it can
// reclaim one existing reservation without requiring new StreamOpen protocol
// fields or increasing the fixed memory ceiling.
func (self *RemoteUserNatProvider) refreshP2pPriority(sourceId Id, provideMode protocol.ProvideMode) {
	if provideMode != protocol.ProvideMode_Network || sourceId == (Id{}) {
		return
	}
	now := time.Now()
	shouldRefresh := false
	self.stateLock.Lock()
	if self.sourceP2pPriorityRefresh == nil {
		self.sourceP2pPriorityRefresh = map[Id]time.Time{}
	}
	if _, ok := self.sourceP2pPriorityRefresh[sourceId]; !ok {
		if maxCount := self.settings.MaxSourceCount; 0 < maxCount && maxCount <= len(self.sourceP2pPriorityRefresh) {
			for evictSourceId := range self.sourceP2pPriorityRefresh {
				delete(self.sourceP2pPriorityRefresh, evictSourceId)
				break
			}
		}
	}
	if next := self.sourceP2pPriorityRefresh[sourceId]; !now.Before(next) {
		self.sourceP2pPriorityRefresh[sourceId] = now.Add(providerP2pPriorityRefreshInterval)
		shouldRefresh = true
	}
	self.stateLock.Unlock()
	if shouldRefresh {
		self.client.webRtcManager.PrioritizePeer(sourceId)
	}
}

// sourceReturnProvideMode returns the recorded return provide mode for a source
// (see recordSourceProvideMode), falling back to the provide mode of the current
// return packet (carried back through the local nat conntrack) if the source is
// not yet tracked
func (self *RemoteUserNatProvider) sourceReturnProvideMode(sourceId Id, fallback protocol.ProvideMode) protocol.ProvideMode {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if provideMode, ok := self.sourceProvideMode[sourceId]; ok {
		return provideMode
	}
	return fallback
}

// Derives the provider's return contract while retaining the receiver-visible
// lane and encryption session. The authenticated source remains separate.
func providerReplyTransferKey(
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
) TransferKey {
	transferKey.CompanionContract = provideMode != protocol.ProvideMode_Network
	return transferKey
}

// Applies provider contract policy to the lane carried by a reply key.
func providerReturnTransferOptions(
	defaultOptions TransferOptions,
	provideMode protocol.ProvideMode,
	transferKey TransferKey,
) TransferOptions {
	defaultOptions.CompanionContract = transferKey.CompanionContract
	defaultOptions.ForceStream = transferKey.ForceStream
	defaultOptions.NetworkPeer = provideMode == protocol.ProvideMode_Network
	return defaultOptions
}

// Provider-return TCP always stays Transfer-reliable because the proxy may
// have already consumed upstream socket bytes. Carrier reliability cannot
// commit those bytes across a disconnect or route replacement. UDP and ICMP
// retain datagram semantics on a direct carrier; otherwise they retain the
// configured default.
func providerReturnIpTransferOptions(
	defaultOptions TransferOptions,
	provideMode protocol.ProvideMode,
	transferKey TransferKey,
	ipProtocol IpProtocol,
) TransferOptions {
	options := providerReturnTransferOptions(defaultOptions, provideMode, transferKey)
	if ipProtocol == IpProtocolTcp {
		options.Ack = true
	} else if transferKey.ForceStream {
		options.Ack = false
	}
	return options
}

// `ReceivePacketFunction`
// providerReturnBatchMaxFrames / providerReturnBatchMaxBytes bound one logical
// provider return group. SendSequence applies the actual carrier wire bound:
// H3 keeps the existing two-frame/one-MTU DATAGRAM-safe chunks, while H1 can
// amortize routing, encryption, and framing over a larger reliable-stream
// group. The byte cap also prevents one busy flow from monopolizing admission.
const providerReturnBatchMaxFrames = 16
const providerReturnBatchMaxBytes = 24 * 1024

// Retries caller-owned socket-return data until Transfer accepts it or the
// provider closes. Every non-owned callback has one downstream disposition,
// even for a synthesized or public TCP packet. Failed owned attempts wait a
// strict pacing floor, including immediate no-route and full-buffer failures.
func (self *RemoteUserNatProvider) retryReturnSend(
	item *providerReturnItem,
	packetCount int,
	packetByteCount ByteCount,
	send func() bool,
) bool {
	retryTimeout := self.settings.ReturnSendRetryTimeout
	if retryTimeout <= 0 {
		retryTimeout = 10 * time.Millisecond
	}
	abandonTimeout := self.settings.ReturnSendAbandonTimeout
	startTime := time.Now()
	for {
		retry := NewPacedReconnect(retryTimeout)
		sent := send()
		if self.afterReturnSendAttemptForTest != nil {
			self.afterReturnSendAttemptForTest(providerReturnSendResult{
				packetCount:     packetCount,
				packetByteCount: packetByteCount,
				sent:            sent,
			})
		}
		if sent || item.recoveryMode != receiveRecoveryModeTcpSocket {
			return sent
		}
		sendCtx := item.sendContext(self.ctx)
		if sendCtx.Err() != nil {
			// the attempt failed because the source or provider closed
			return false
		}
		if 0 < abandonTimeout && abandonTimeout <= time.Since(startTime) && !self.backendDegraded() {
			self.releaseUnreachableSource(item.source.SourceId, item.sourceLifecycle)
			return false
		}
		if self.beforeTcpReturnSendRetryForTest != nil {
			self.beforeTcpReturnSendRetryForTest()
		}
		select {
		case <-sendCtx.Done():
			return false
		case <-retry.After():
		}
	}
}

// only one dedicated TCP socket reader can retain consumed upstream bytes
// while Transfer admission waits. Datagram and shared callback workers serve
// unrelated flows, so one full destination must receive a zero-wait refusal
// instead of parking that worker for the provider-wide write timeout.
func (self *RemoteUserNatProvider) returnWriteTimeout(item *providerReturnItem) time.Duration {
	if item.recoveryMode == receiveRecoveryModeTcpSocket {
		return self.settings.WriteTimeout
	}
	return 0
}

// A dedicated TCP socket reader retains the exact consumed bytes until
// Transfer admission. Admission moves the only recoverable copy into the
// bounded resend queue, which must keep retrying past the ordinary Ack timeout.
// Synthesized controls remain regenerable by the peer and need no such lease.
func (self *RemoteUserNatProvider) returnSendRecoveryOption(
	item *providerReturnItem,
) sendPackRecoveryOption {
	return sendPackRecoveryOption{
		upstreamRecoverable: item.recoveryMode == receiveRecoveryModeTcpSocket ||
			item.recoveryMode == receiveRecoveryModeRegenerableControl,
		retainAfterAckTimeout: item.recoveryMode == receiveRecoveryModeTcpSocket,
	}
}

// sendReturnPacket transfers one packet share to Transfer on success. A
// rejected socket-owned TCP attempt retains and retries the exact same bytes.
func (self *RemoteUserNatProvider) sendReturnPacket(item *providerReturnItem) bool {
	packet := item.packet
	item.packet = nil
	returnTransferKey := item.transferKey
	returnOption := providerReturnIpTransferOptions(
		self.client.settings.DefaultTransferOpts,
		item.provideMode,
		returnTransferKey,
		item.ipProtocol,
	)
	destinationId := item.source.SourceId
	writeTimeout := self.returnWriteTimeout(item)
	recoveryOption := self.returnSendRecoveryOption(item)
	transportAttribution := newTransportPacketAttribution(
		self.packetStatsCounters,
		1,
		item.packetByteCount,
	)
	var sent bool
	if 2 <= self.settings.ProtocolVersion {
		sent = self.retryReturnSend(item, 1, item.packetByteCount, func() bool {
			attemptSent, _ := self.client.sendRawWithTimeoutDetailed(
				protocol.MessageType_IpIpPacketFromProvider,
				packet,
				destinationId,
				self.returnAckTargetForTest,
				0,
				writeTimeout,
				returnOption,
				returnTransferKey,
				Ctx(item.sendContext(self.ctx)),
				sendSchedulingKeyOption{key: item.schedulingKey},
				observeTransportWrite(transportAttribution.observe),
				recoveryOption,
			)
			return attemptSent
		})
		if !sent {
			MessagePoolReturn(packet)
		}
	} else {
		frame, err := ipPacketFromProviderFrame(packet, self.settings.ProtocolVersion)
		MessagePoolReturn(packet)
		if err != nil {
			if self.client.log.V(2).Enabled() {
				self.client.log.Infof(
					"drop remote user nat provider s packet ->%s = %s\n",
					item.source.SourceId,
					err,
				)
			}
			panic(err)
		}
		sent = self.retryReturnSend(item, 1, item.packetByteCount, func() bool {
			return self.client.SendWithTimeout(
				frame,
				destinationId,
				func(err error) {},
				writeTimeout,
				returnOption,
				returnTransferKey,
				Ctx(item.sendContext(self.ctx)),
				sendSchedulingKeyOption{key: item.schedulingKey},
				observeTransportWrite(transportAttribution.observe),
				recoveryOption,
			)
		})
		if !sent {
			MessagePoolReturn(frame.MessageBytes)
		}
	}
	if sent {
		transportAttribution.admit()
	} else {
		self.congestionDrops.addReturnSend(1, item.packetByteCount)
	}
	return sent
}

// sendReturnBatch builds bounded logical frame groups. SendSequence splits each
// group into carrier-safe physical Packs after its route generation is known.
// Every packet share either moves into a raw frame or is returned after legacy
// wrapping before this function exits.
func (self *RemoteUserNatProvider) sendReturnBatch(item *providerReturnItem) bool {
	return self.sendReturnBatchWithLimits(
		item,
		providerReturnBatchMaxFrames,
		providerReturnBatchMaxBytes,
	)
}

// sendReturnBatchWithLimits keeps the production fairness bounds explicit and
// lets deterministic benchmarks compare candidate batch shapes without a
// mutable process-wide knob.
func (self *RemoteUserNatProvider) sendReturnBatchWithLimits(
	item *providerReturnItem,
	maxFrames int,
	maxBytes int64,
) bool {
	if maxFrames <= 0 || maxBytes <= 0 {
		panic("provider return batch limits must be positive")
	}
	packets := item.packets
	item.packets = nil
	defer func() {
		for _, packet := range packets {
			MessagePoolReturn(packet)
		}
	}()
	returnTransferKey := item.transferKey
	returnOption := providerReturnIpTransferOptions(
		self.client.settings.DefaultTransferOpts,
		item.provideMode,
		returnTransferKey,
		item.ipProtocol,
	)
	destinationId := item.source.SourceId
	writeTimeout := self.returnWriteTimeout(item)
	recoveryOption := self.returnSendRecoveryOption(item)
	frames := make([]*protocol.Frame, 0, maxFrames)
	wrappedShares := make([][]byte, 0, maxFrames)
	var chunkBytes int64
	var chunkPacketBytes int64
	allSent := true
	flush := func() {
		if len(frames) == 0 {
			return
		}
		sendFrames := frames
		frames = make([]*protocol.Frame, 0, maxFrames)
		frameCount := len(sendFrames)
		packetBytes := chunkPacketBytes
		transportAttribution := newTransportPacketAttribution(
			self.packetStatsCounters,
			frameCount,
			ByteCount(packetBytes),
		)
		sent := self.retryReturnSend(
			item,
			frameCount,
			ByteCount(packetBytes),
			func() bool {
				admitted, _ := self.client.sendGroupWithTimeoutDetailed(
					sendFrames,
					destinationId,
					func(err error) {},
					writeTimeout,
					returnOption,
					returnTransferKey,
					Ctx(item.sendContext(self.ctx)),
					sendSchedulingKeyOption{key: item.schedulingKey},
					observeTransportWrite(transportAttribution.observe),
					recoveryOption,
				)
				return admitted
			},
		)
		if sent {
			transportAttribution.admit()
		} else {
			allSent = false
			self.congestionDrops.addReturnSend(frameCount, ByteCount(packetBytes))
			for _, frame := range sendFrames {
				MessagePoolReturn(frame.MessageBytes)
			}
		}
		for _, share := range wrappedShares {
			MessagePoolReturn(share)
		}
		wrappedShares = wrappedShares[:0]
		chunkBytes = 0
		chunkPacketBytes = 0
	}
	defer func() {
		for _, frame := range frames {
			MessagePoolReturn(frame.MessageBytes)
		}
		for _, share := range wrappedShares {
			MessagePoolReturn(share)
		}
	}()
	for packetIndex, packet := range packets {
		frame, err := ipPacketFromProviderFrame(packet, self.settings.ProtocolVersion)
		if err != nil {
			panic(err)
		}
		// Keep a logical group within the provider fairness bound. The selected
		// SendSequence later applies its H1/H3 wire-size limit without rerunning
		// provider routing or changing packet order.
		if 0 < len(frames) &&
			maxBytes < chunkBytes+int64(len(frame.MessageBytes)) {
			flush()
		}
		if frame.Raw {
			packets[packetIndex] = nil
		} else {
			wrappedShares = append(wrappedShares, packet)
			packets[packetIndex] = nil
		}
		frames = append(frames, frame)
		chunkBytes += int64(len(frame.MessageBytes))
		chunkPacketBytes += int64(len(packet))
		if maxFrames <= len(frames) || maxBytes <= chunkBytes {
			flush()
		}
	}
	flush()
	return allSent
}

// `ReceivePacketsFunction`
// ReceiveBatch coalesces a flow's drained packet batch into bounded logical
// groups, collapsing per-packet policy/routing admission while leaving
// carrier-safe physical chunking to SendSequence. All packets share the flow's
// source/ipPath, so the egress policy is evaluated once. Ownership mirrors
// the per-packet Receive: each packet is shared read-only into a frame; the
// share/marshal buffers are freed on the same raw/wrapped rules.
func (self *RemoteUserNatProvider) ReceiveBatch(
	source TransferPath,
	provideMode protocol.ProvideMode,
	ipPath *IpPath,
	packets [][]byte,
) {
	self.receiveTransferBatch(source, TransferKey{}, provideMode, ipPath, packets)
}

// Returns a public/shared batch with one nonblocking disposition.
func (self *RemoteUserNatProvider) receiveTransferBatch(
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	ipPath *IpPath,
	packets [][]byte,
) {
	source = source.LocalMask()
	sourceLifecycle := self.acquireSourceLifecycle(source.SourceId)
	if sourceLifecycle == nil {
		return
	}
	defer self.releaseSourceLifecycle(source.SourceId, sourceLifecycle)
	self.receiveTransferBatchWithRecovery(
		source,
		transferKey,
		provideMode,
		receiveRecoveryModeNonblocking,
		ipPath,
		packets,
	)
}

// inspectReturnPacketsForSender evaluates actual server-to-client packet
// paths. LocalUserNat's callback path is the canonical outbound identity and
// intentionally does not contain return-direction endpoints or teardown flags.
func (self *RemoteUserNatProvider) inspectReturnPacketsForSender(
	senderClientId Id,
	provideMode protocol.ProvideMode,
	packets [][]byte,
) (SecurityPolicyResult, error) {
	var returnIpPath *IpPath
	for _, packet := range packets {
		packetIpPath, err := ParseIpPath(packet)
		if err != nil {
			return SecurityPolicyResultDrop, err
		}
		if returnIpPath == nil {
			returnIpPath = packetIpPath
		}
		self.smtpIngressGuard.retireReturnForOwner(senderClientId, packetIpPath)
	}
	if returnIpPath == nil {
		return SecurityPolicyResultDrop, errors.New("empty provider return packet group")
	}
	return inspectAndRefreshEgressForSenderBorrowed(
		self.securityPolicy,
		senderClientId,
		provideMode,
		*returnIpPath,
		nil,
	)
}

// Performs the provider-side return policy on a complete UDP datagram and
// queues its original ip fragments (either family). The fragments are owned by
// the caller and transfer to the return item only on success.
func (self *RemoteUserNatProvider) enqueueFragmentedReturn(
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	recoveryMode receiveRecoveryMode,
	canonicalIpPath *IpPath,
	reassembled []byte,
	fragments [][]byte,
) bool {
	if len(fragments) == 0 || canonicalIpPath == nil {
		return false
	}
	returnIpPath, err := ParseIpPath(reassembled)
	if err != nil || returnIpPath.Protocol != IpProtocolUdp {
		return false
	}
	self.smtpIngressGuard.retireReturnForOwner(source.SourceId, returnIpPath)
	r, err := inspectAndRefreshEgressForSenderBorrowed(
		self.securityPolicy,
		source.SourceId,
		provideMode,
		*returnIpPath,
		nil,
	)
	if err != nil {
		return false
	}
	var byteCount ByteCount
	for _, fragment := range fragments {
		byteCount += ByteCount(len(fragment))
	}
	if r != SecurityPolicyResultAllow {
		self.packetStatsCounters.blockEgressPacketCount.Add(int64(len(fragments)))
		self.packetStatsCounters.blockEgressByteCount.Add(int64(byteCount))
		self.recordProviderBlock(source.SourceId, false, len(fragments), byteCount)
		self.publishProviderDiagnostics(source, transferKey, provideMode)
		return false
	}

	returnProvideMode := self.sourceReturnProvideMode(source.SourceId, provideMode)
	returnTransferKey := providerReplyTransferKey(transferKey, returnProvideMode)
	item := self.takeReturnItem()
	item.source = source.LocalMask()
	item.transferKey = returnTransferKey
	item.provideMode = returnProvideMode
	item.recoveryMode = recoveryMode
	item.ipProtocol = canonicalIpPath.Protocol
	item.schedulingKey = ipSendSchedulingKey(canonicalIpPath)
	item.batch = true
	item.packets = append(item.packets, fragments...)
	item.packetByteCount = byteCount
	self.enqueueReturnItem(item)
	return true
}

// Returns a keyed NAT batch without dropping its explicit recovery owner.
func (self *RemoteUserNatProvider) receiveTransferBatchWithRecovery(
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	recoveryMode receiveRecoveryMode,
	ipPath *IpPath,
	packets [][]byte,
) {
	if len(packets) == 0 {
		return
	}
	source = source.LocalMask()
	if self.client.ClientId() == source.SourceId {
		if self.client.log.V(2).Enabled() {
			self.client.log.Infof("drop remote user nat provider s packet ->%s\n", source.SourceId)
		}
		return
	}
	containsFragments := false
	for _, packet := range packets {
		if isIpFragmentPacket(packet) {
			containsFragments = true
			break
		}
	}
	if containsFragments {
		for _, packet := range packets {
			if !isIpFragmentPacket(packet) {
				self.receiveTransferWithRecovery(
					source,
					transferKey,
					provideMode,
					recoveryMode,
					ipPath,
					packet,
				)
				continue
			}
			result := self.egressIpv4Fragments.processOwned(
				source,
				transferKey,
				provideMode,
				MessagePoolCopy(packet),
			)
			if result.packet != nil && self.enqueueFragmentedReturn(
				source,
				transferKey,
				provideMode,
				recoveryMode,
				ipPath,
				result.packet,
				result.fragments,
			) {
				result.fragments = nil
			}
			returnIpFragmentProcessResult(result)
		}
		return
	}
	// Preserve the sparse single-packet path, including its raw-frame fast
	// path. Multi-packet drains remain one logical group even when a contract
	// must be attached: SendSequence owns contract-envelope-safe H1/H3 wire
	// chunking and keeps a single routing/admission decision for the group.
	if len(packets) == 1 {
		self.receiveTransferWithRecovery(
			source,
			transferKey,
			provideMode,
			recoveryMode,
			ipPath,
			packets[0],
		)
		return
	}
	// Every member has the same reverse tuple. Scan all members so a trailing
	// FIN/RST retires SMTP state before the flow-level policy decision.
	r, err := self.inspectReturnPacketsForSender(
		source.SourceId,
		provideMode,
		packets,
	)
	if err != nil {
		return
	}
	if r != SecurityPolicyResultAllow {
		var blockedBytes int64
		for _, packet := range packets {
			blockedBytes += int64(len(packet))
		}
		self.packetStatsCounters.blockEgressPacketCount.Add(int64(len(packets)))
		self.packetStatsCounters.blockEgressByteCount.Add(blockedBytes)
		self.recordProviderBlock(source.SourceId, false, len(packets), ByteCount(blockedBytes))
		self.publishProviderDiagnostics(source, transferKey, provideMode)
		return
	}

	returnProvideMode := self.sourceReturnProvideMode(source.SourceId, provideMode)
	returnTransferKey := providerReplyTransferKey(transferKey, returnProvideMode)
	item := self.takeReturnItem()
	item.source = source
	item.transferKey = returnTransferKey
	item.provideMode = returnProvideMode
	item.recoveryMode = recoveryMode
	item.ipProtocol = ipPath.Protocol
	item.schedulingKey = ipSendSchedulingKey(ipPath)
	item.batch = true
	for _, packet := range packets {
		item.packets = append(item.packets, MessagePoolShareReadOnly(packet))
		item.packetByteCount += ByteCount(len(packet))
	}
	self.enqueueReturnItem(item)
}

func (self *RemoteUserNatProvider) Receive(
	source TransferPath,
	provideMode protocol.ProvideMode,
	ipPath *IpPath,
	packet []byte,
) {
	self.receiveTransfer(source, TransferKey{}, provideMode, ipPath, packet)
}

// Returns one public/shared packet with a nonblocking disposition.
func (self *RemoteUserNatProvider) receiveTransfer(
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	ipPath *IpPath,
	packet []byte,
) {
	source = source.LocalMask()
	sourceLifecycle := self.acquireSourceLifecycle(source.SourceId)
	if sourceLifecycle == nil {
		return
	}
	defer self.releaseSourceLifecycle(source.SourceId, sourceLifecycle)
	self.receiveTransferWithRecovery(
		source,
		transferKey,
		provideMode,
		receiveRecoveryModeNonblocking,
		ipPath,
		packet,
	)
}

// Returns one keyed NAT packet without dropping its explicit recovery owner.
func (self *RemoteUserNatProvider) receiveTransferWithRecovery(
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	recoveryMode receiveRecoveryMode,
	ipPath *IpPath,
	packet []byte,
) {
	self.receiveTransferWithRecoveryAndRelease(
		source,
		transferKey,
		provideMode,
		recoveryMode,
		ipPath,
		packet,
		nil,
	)
}

// receiveTransferWithRecoveryAndRelease attaches a rare lifecycle action to
// the packet's terminal return disposition. The action also runs on every
// early rejection, so callers never need a timeout or a second ownership
// channel to learn that the packet can no longer precede their control work.
func (self *RemoteUserNatProvider) receiveTransferWithRecoveryAndRelease(
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	recoveryMode receiveRecoveryMode,
	ipPath *IpPath,
	packet []byte,
	afterRelease func(),
) {
	pendingAfterRelease := afterRelease
	defer func() {
		if pendingAfterRelease != nil {
			HandleError(pendingAfterRelease)
		}
	}()
	source = source.LocalMask()
	// self.client.log.Infof("[trace]provider return packet for %s\n", source.SourceId)

	if self.client.ClientId() == source.SourceId {
		// locally generated traffic should use a separate local user nat
		if self.client.log.V(2).Enabled() {
			self.client.log.Infof("drop remote user nat provider s packet ->%s\n", source.SourceId)
		}
		return
	}
	if isIpFragmentPacket(packet) {
		result := self.egressIpv4Fragments.processOwned(
			source,
			transferKey,
			provideMode,
			MessagePoolCopy(packet),
		)
		if result.packet != nil && self.enqueueFragmentedReturn(
			source,
			transferKey,
			provideMode,
			recoveryMode,
			ipPath,
			result.packet,
			result.fragments,
		) {
			result.fragments = nil
		}
		returnIpFragmentProcessResult(result)
		return
	}
	// the provider's egress is the return into the tunnel (destination->client); the reversed
	// provider policy applies the client-ingress source check here, then refreshes the flow so an
	// active download isn't reclaimed while the outbound side is quiet
	r, err := self.inspectReturnPacketsForSender(
		source.SourceId,
		provideMode,
		[][]byte{packet},
	)
	if err != nil {
		return
	}
	if r != SecurityPolicyResultAllow {
		self.packetStatsCounters.blockEgressPacketCount.Add(1)
		self.packetStatsCounters.blockEgressByteCount.Add(int64(len(packet)))
		self.recordProviderBlock(source.SourceId, false, 1, ByteCount(len(packet)))
		self.publishProviderDiagnostics(source, transferKey, provideMode)
		return
	}

	// echo the recorded return provide mode for the source. A same-network source
	// sends under ProvideMode_Network; its return traffic is also network mode,
	// which uses the network relationship (no companion contract) so the device
	// receives it as network mode and skips the public ingress rules. Other
	// modes ride a companion contract (verified as Stream) as before.
	returnProvideMode := self.sourceReturnProvideMode(source.SourceId, provideMode)
	returnTransferKey := providerReplyTransferKey(transferKey, returnProvideMode)
	item := self.takeReturnItem()
	item.source = source
	item.transferKey = returnTransferKey
	item.provideMode = returnProvideMode
	item.recoveryMode = recoveryMode
	item.ipProtocol = ipPath.Protocol
	item.schedulingKey = ipSendSchedulingKey(ipPath)
	item.packet = MessagePoolShareReadOnly(packet)
	item.packetByteCount = ByteCount(len(packet))
	item.afterRelease = pendingAfterRelease
	pendingAfterRelease = nil
	self.enqueueReturnItem(item)
}

// `connect.ReceiveFunction`
func (self *RemoteUserNatProvider) ClientReceive(source TransferPath, frames []*protocol.Frame, peer Peer) {
	// receive functions should be non-blocking
	// clients should manage their own congestion protocols on top to avoid overflowing the sequence queues
	if source.IsControlSource() {
		self.retireDisconnectedSenders(frames)
		return
	}
	source = source.LocalMask()
	sourceLifecycle := self.acquireSourceLifecycle(source.SourceId)
	if sourceLifecycle == nil {
		return
	}
	defer self.releaseSourceLifecycle(source.SourceId, sourceLifecycle)
	transferKey := peer.TransferKey

	provideMode := peer.ProvideMode
	// One observation per delivered Pack is enough. The former call inside
	// every packet case serialized a large coalesced return batch on this
	// provider-global lock even though every frame has the same source and
	// relationship.
	self.recordSourceProvideMode(source.SourceId, provideMode)
	self.refreshP2pPriority(source.SourceId, provideMode)

	// Collect allowed packets into exact directional flows before crossing the
	// LocalUserNat queue. This keeps every queued batch homogeneous without
	// changing first-seen group or in-flow order.
	var packetGroups []*ipPacketGroup
	packetGroupsByKey := map[ipPacketFlowKey]*ipPacketGroup{}
	// A synthesized policy reset must enter the bounded return sequence before
	// diagnostics for the same rejection. The barrier is allocated only on the
	// rare SMTP-block path; its completion never blocks a return worker.
	var diagnosticsAfterReset chan struct{}
	for _, frame := range frames {
		switch frame.MessageType {
		case protocol.MessageType_IpIpPing:
			if self.client.log.V(1).Enabled() {
				self.client.log.Infof("[ip]provider ping <- %s(%d)\n", source, provideMode)
			}
			// Receive callback frames are borrowed and valid only until this
			// callback returns. The send queue is asynchronous, so forwarding
			// `frame` itself lets the decoded-frame pool reset/reuse it before
			// SendSequence serializes it. Besides corrupting the echo, that can
			// make contract accounting charge the original empty IpPing and
			// later acknowledge whatever larger frame reused the object,
			// terminating the whole send sequence with "Bad accounting".
			//
			// Keep the payload alive with a read-only share and give the send
			// its own frame object. IpPing is rare, so the one small object is
			// preferable to letting a callback-lifetime object escape.
			echoBytes := MessagePoolShareReadOnly(frame.MessageBytes)
			echoFrame := &protocol.Frame{
				MessageType:  frame.MessageType,
				MessageBytes: echoBytes,
				Raw:          frame.Raw,
			}
			// echo the recorded return provide mode for the source, like the
			// provider's other return traffic. A same-network source pings
			// under ProvideMode_Network; its echo is also network mode (no
			// companion contract). For other modes the source only provides
			// ProvideMode_Stream, so a forward contract would be rejected
			// (no permission); the echo rides a companion contract instead.
			returnProvideMode := self.sourceReturnProvideMode(source.SourceId, provideMode)
			returnTransferKey := providerReplyTransferKey(transferKey, returnProvideMode)
			returnOptions := providerReturnTransferOptions(
				self.client.settings.DefaultTransferOpts,
				returnProvideMode,
				returnTransferKey,
			)
			if !self.client.SendWithTimeout(
				echoFrame,
				source.SourceId,
				func(err error) {},
				0,
				returnOptions,
				returnTransferKey,
			) {
				self.congestionDrops.addReturnSend(1, ByteCount(len(echoBytes)))
				MessagePoolReturn(echoBytes)
			}
		case protocol.MessageType_IpIpPacketToProvider:
			packetBytes, err := ipPacketToProviderBytes(frame)
			if err != nil {
				panic(err)
			}
			if isIpFragmentPacket(packetBytes) {
				result := self.ingressIpv4Fragments.processOwned(
					source,
					transferKey,
					provideMode,
					MessagePoolCopy(packetBytes),
				)
				if result.packet != nil {
					var ipPath IpPath
					payload, parseErr := parseIpPathWithPayloadBorrowed(result.packet, &ipPath)
					if parseErr == nil && ipPath.Protocol == IpProtocolUdp {
						r, inspectErr := inspectAndRefreshIngressForSenderBorrowed(
							self.securityPolicy,
							source.SourceId,
							provideMode,
							ipPath,
							payload,
						)
						if inspectErr == nil && r == SecurityPolicyResultAllow {
							if 0 < len(result.fragments) && appendIpPacketGroup(
								&packetGroups,
								packetGroupsByKey,
								&ipPath,
								payload,
								result.fragments[0],
							) {
								for _, fragment := range result.fragments[1:] {
									if !appendIpPacketGroup(
										&packetGroups,
										packetGroupsByKey,
										&ipPath,
										payload,
										fragment,
									) {
										panic("validated IPv4 fragment group lost its flow identity")
									}
								}
								result.fragments = nil
							}
						} else if inspectErr == nil {
							var fragmentByteCount int64
							for _, fragment := range result.fragments {
								fragmentByteCount += int64(len(fragment))
							}
							self.packetStatsCounters.blockIngressPacketCount.Add(int64(len(result.fragments)))
							self.packetStatsCounters.blockIngressByteCount.Add(fragmentByteCount)
							self.recordProviderBlock(
								source.SourceId,
								true,
								len(result.fragments),
								ByteCount(fragmentByteCount),
							)
							if r == SecurityPolicyResultIncident {
								self.client.ReportAbuse(source)
							}
						}
					}
				}
				returnIpFragmentProcessResult(result)
				continue
			}

			var ipPath IpPath
			payload, err := parseIpPathWithPayloadBorrowed(packetBytes, &ipPath)
			if err == nil {
				// The provider independently enforces the same SMTP encryption
				// boundary as the client. This must precede the reversed general
				// policy: TCP/587 legitimately starts with a small plaintext
				// EHLO/STARTTLS exchange, while transaction/authentication commands
				// are forbidden until a TLS ClientHello has been observed.
				if self.smtpIngressGuard.inspectForOwner(
					source.SourceId,
					&ipPath,
					payload,
				) == smtpEgressReject {
					self.packetStatsCounters.blockIngressPacketCount.Add(1)
					self.packetStatsCounters.blockIngressByteCount.Add(int64(len(packetBytes)))
					self.recordProviderBlock(source.SourceId, true, 1, ByteCount(len(packetBytes)))
					var afterResetRelease func()
					if diagnosticsAfterReset == nil {
						diagnosticsAfterReset = make(chan struct{})
						barrier := diagnosticsAfterReset
						afterResetRelease = func() {
							go func() {
								select {
								case <-barrier:
									self.publishProviderDiagnostics(source, transferKey, provideMode)
								case <-self.ctx.Done():
								}
							}()
						}
					}
					deliverTcpPolicyReset(
						func(
							resetSource TransferPath,
							resetProvideMode protocol.ProvideMode,
							resetPath *IpPath,
							reset []byte,
						) {
							self.receiveTransferWithRecoveryAndRelease(
								resetSource,
								transferKey,
								resetProvideMode,
								receiveRecoveryModeRegenerableControl,
								resetPath,
								reset,
								afterResetRelease,
							)
						},
						source,
						provideMode,
						&ipPath,
						packetBytes,
					)
					continue
				}
				// the provider's ingress is the remote client's egress (outbound, received from the
				// tunnel); the reversed provider policy applies the client-egress DPI here
				r, err := inspectAndRefreshIngressForSenderBorrowed(
					self.securityPolicy,
					source.SourceId,
					provideMode,
					ipPath,
					payload,
				)
				if err == nil {
					switch r {
					case SecurityPolicyResultAllow:
						var packet []byte
						if frame.Raw {
							packet = MessagePoolShareReadOnly(packetBytes)
						} else {
							packet = MessagePoolCopy(packetBytes)
						}
						if !appendIpPacketGroup(
							&packetGroups,
							packetGroupsByKey,
							&ipPath,
							payload,
							packet,
						) {
							MessagePoolReturn(packet)
						}
					default:
						// drop or incident: blocked by the provider security policy
						self.packetStatsCounters.blockIngressPacketCount.Add(1)
						self.packetStatsCounters.blockIngressByteCount.Add(int64(len(packetBytes)))
						self.recordProviderBlock(source.SourceId, true, 1, ByteCount(len(packetBytes)))
						if r == SecurityPolicyResultIncident {
							self.client.ReportAbuse(source)
						}
					}
				}
			}
		}
	}
	// A block observed while processing this callback increments the source's
	// generation. Normally publish that final state after the borrowed frame
	// loop. If a reset was synthesized, release its completion instead; that
	// makes diagnostics strictly lower priority than the recovery packet.
	if diagnosticsAfterReset == nil {
		self.publishProviderDiagnostics(source, transferKey, provideMode)
	} else {
		close(diagnosticsAfterReset)
	}

	for _, packetGroup := range packetGroups {
		c := func() bool {
			dropPacketGroup := func() {
				self.congestionDrops.addIngressNat(
					len(packetGroup.packets),
					packetGroup.byteCount,
				)
				for _, packet := range packetGroup.packets {
					MessagePoolReturn(packet)
				}
			}
			// Extend the exact-source admission through LocalUserNat queue and
			// shard disposition. The outer callback admission alone ends when
			// this handoff returns, before a queued packet reaches its protocol
			// tombstone.
			if self.beforeLocalUserNatAdmissionForTest != nil {
				self.beforeLocalUserNatAdmissionForTest(
					source.SourceId,
					packetGroup.packets,
				)
			}
			if !sourceLifecycle.admissions.start() {
				dropPacketGroup()
				return false
			}
			success := self.localUserNat.sendTransferPacketsWithTimeout(
				source,
				transferKey,
				provideMode,
				packetGroup.packets,
				0,
				func() {
					self.releaseSourceLifecycle(source.SourceId, sourceLifecycle)
				},
			)
			if success {
				self.packetStatsCounters.recordRemoteIngress(
					peer.TransportType,
					int64(len(packetGroup.packets)),
					packetGroup.byteCount,
				)
			} else {
				dropPacketGroup()
			}
			return success
		}
		if self.client.log.V(2).Enabled() {
			TraceWithReturn(
				fmt.Sprintf("[unpr]%d %s<-%s", len(packetGroup.packets), self.client.ClientTag(), source.SourceId),
				c,
			)
		} else {
			c()
		}
	}
}

func (self *RemoteUserNatProvider) Close() {
	self.closeOnce.Do(func() {
		if self.contractStatusUnsub != nil {
			self.contractStatusUnsub()
		}
		if self.clientUnsub != nil {
			self.clientUnsub()
		}
		if self.localUserNatUnsub != nil {
			self.localUserNatUnsub()
		}
		if self.contractStatusAdmissions != nil {
			self.contractStatusAdmissions.close()
			<-self.contractStatusAdmissions.Done()
		}
		// Cancel before closing admission so an in-flight synchronous TCP retry
		// can return. The admission wait makes packet ownership quiescent before
		// Close returns; the final drain covers datagrams admitted just before
		// cancellation stopped their sender worker.
		if self.cancel != nil {
			self.cancel()
		}
		self.stateLock.Lock()
		self.sourceLifecycleClosed = true
		self.sourceRetirementClosed = true
		sourceLifecycles := make(
			[]*providerSourceLifecycle,
			0,
			len(self.sourceLifecycles),
		)
		for _, sourceLifecycle := range self.sourceLifecycles {
			sourceLifecycle.admissions.close()
			sourceLifecycle.cancel()
			sourceLifecycles = append(sourceLifecycles, sourceLifecycle)
		}
		wakeSourceRetirement := self.sourceRetirementNotify != nil
		self.stateLock.Unlock()
		if wakeSourceRetirement {
			self.wakeSourceRetirementWorkers()
		}
		self.ingressIpv4Fragments.close()
		self.egressIpv4Fragments.close()
		self.returnStateLock.Lock()
		self.returnClosed = true
		self.returnStateLock.Unlock()
		if self.beforeReturnAdmissionsWaitForTest != nil {
			self.beforeReturnAdmissionsWaitForTest()
		}
		self.returnAdmissions.Wait()
		self.returnWorkers.Wait()
		for _, queue := range self.returnSendQueues {
		drainQueue:
			for {
				select {
				case item := <-queue:
					self.releaseReturnItem(item)
				default:
					break drainQueue
				}
			}
		}
		for _, sourceLifecycle := range sourceLifecycles {
			<-sourceLifecycle.admissions.Done()
		}
		self.sourceRetirementWorkers.Wait()
		self.unreachableSourceReleaseWorkers.Wait()
		self.stateLock.Lock()
		unreachableTerminalOwnerIds := self.unreachableTerminalOwnerIds
		self.unreachableTerminalOwnerIds = nil
		self.stateLock.Unlock()
		for _, ownerId := range unreachableTerminalOwnerIds {
			self.localUserNat.releaseSourceRetirementOwner(ownerId)
		}
		self.localUserNat.releaseSourceRetirementOwner(self.sourceRetirementOwnerId)
	})
}

// this is a basic implementation. See `RemoteUserNatWindowedClient` for a more robust implementation
type RemoteUserNatClient struct {
	client                *Client
	cancel                context.CancelFunc
	receivePacketCallback ReceivePacketFunction
	securityPolicy        SecurityPolicy
	smtpEgressGuard       smtpEgressGuard
	egressIpv4Fragments   ipv4FragmentGate
	ingressIpv4Fragments  ipv4FragmentGate
	pathTable             *pathTable
	// the provide mode of the source packets
	// for locally generated packets this is `ProvideMode_Network`
	provideMode       protocol.ProvideMode
	localUserNat      *LocalUserNat
	closeCallback     func()
	clientUnsub       func()
	localUserNatUnsub func()

	stateLock           sync.Mutex
	allowDirect         bool
	localSecurityBypass bool
}

func NewRemoteUserNatClient(
	client *Client,
	receivePacketCallback ReceivePacketFunction,
	destinations []MultiHopId,
	provideMode protocol.ProvideMode,
) *RemoteUserNatClient {
	return NewRemoteUserNatClientWithClose(client, receivePacketCallback, destinations, provideMode, nil)
}

func NewRemoteUserNatClientWithClose(
	client *Client,
	receivePacketCallback ReceivePacketFunction,
	destinations []MultiHopId,
	provideMode protocol.ProvideMode,
	closeCallback func(),
) *RemoteUserNatClient {
	pathTable := newPathTable(destinations)

	localUserNatSettings := DefaultLocalUserNatSettings()
	// no ulimit for local traffic
	localUserNatSettings.UdpBufferSettings.UserLimit = 0
	localUserNatSettings.TcpBufferSettings.UserLimit = 0
	localUserNat := NewLocalUserNat(client.Ctx(), "remote local", localUserNatSettings)

	// the security policy runs a background scan goroutine; scope it to this client (a child of
	// the client ctx) so Close stops it rather than leaking it for the life of the client
	cancelCtx, cancel := context.WithCancel(client.Ctx())
	userNatClient := &RemoteUserNatClient{
		client:                client,
		cancel:                cancel,
		receivePacketCallback: receivePacketCallback,
		securityPolicy:        DefaultSecurityPolicy(cancelCtx),
		pathTable:             pathTable,
		provideMode:           provideMode,
		localUserNat:          localUserNat,
		closeCallback:         closeCallback,
	}

	clientUnsub := client.AddReceiveCallback(userNatClient.ClientReceive)
	userNatClient.clientUnsub = clientUnsub

	userNatClient.localUserNatUnsub = localUserNat.AddReceivePacketCallback(receivePacketCallback)

	return userNatClient
}

func (self *RemoteUserNatClient) Destinations() []MultiHopId {
	return self.pathTable.Destinations()
}

func (self *RemoteUserNatClient) DestinationIds() []Id {
	return self.pathTable.DestinationIds()
}

func (self *RemoteUserNatClient) SecurityPolicyStats(reset bool) SecurityPolicyStats {
	return self.securityPolicy.Stats().Stats(reset)
}

// `SendPacketFunction`
func (self *RemoteUserNatClient) SendPacket(source TransferPath, provideMode protocol.ProvideMode, packet []byte, timeout time.Duration) bool {
	if isIpFragmentPacket(packet) {
		result := self.egressIpv4Fragments.processOwned(
			source,
			TransferKey{},
			provideMode,
			packet,
		)
		if result.packet != nil {
			if self.sendReassembledUdpFragments(
				source,
				provideMode,
				result.packet,
				result.fragments,
				timeout,
			) {
				// The local NAT or Transfer now owns every original fragment.
				result.fragments = nil
			}
		}
		returnIpFragmentProcessResult(result)
		// Fragment admission consumes the input even when the completed
		// datagram is rejected by policy. Returning true preserves the
		// SendPacket ownership contract; block/drop accounting happens at the
		// completed-datagram boundary.
		return true
	}

	relationship := egressRelationship(provideMode, self.provideMode)

	var ipPath IpPath
	payload, err := parseIpPathWithPayloadBorrowed(packet, &ipPath)
	if err != nil {
		return false
	}
	// TCP/25 is a deliberate kill-switch exception: it is routed directly
	// before CFAA inspection so an SMTP relay attempt can never reach a
	// provider. Ports 465/587 stay remote, but only after their per-flow SMTP
	// state has accepted this segment.
	if smtpRoutesLocally(&ipPath) {
		return self.localUserNat.SendPacket(source, provideMode, packet, 0)
	}
	if self.smtpEgressGuard.inspect(&ipPath, payload) == smtpEgressReject {
		deliverTcpPolicyReset(
			self.receivePacketCallback,
			source,
			provideMode,
			&ipPath,
			packet,
		)
		return false
	}
	r, err := inspectAndRefreshEgressBorrowed(self.securityPolicy, relationship, ipPath, payload)
	if err != nil {
		return false
	}

	switch r {
	case SecurityPolicyResultAllow:
		destination, err := self.pathTable.SelectDestination(packet)
		if err != nil {
			// drop
			return false
		}

		// The default v2 path embeds the raw frame in the pooled SendPack. It
		// consumes packet only when the send accepts it and avoids both a frame
		// allocation and the extra share that previously left the caller's
		// original reference outstanding after a successful send.
		if 2 <= DefaultProtocolVersion {
			success, _ := self.client.sendRawMultiHopWithTimeoutDetailed(
				protocol.MessageType_IpIpPacketToProvider,
				packet,
				destination,
				nil,
				0,
				timeout,
			)
			return success
		}

		frame, err := ipPacketToProviderFrame(packet, DefaultProtocolVersion)
		if err != nil {
			panic(err)
		}

		// the sender will control transfer
		// note udp is sent with ack because because otherwise the delivery reliability will mulitply with the egress
		success := self.client.SendMultiHopWithTimeout(frame, destination, func(err error) {}, timeout)
		if success {
			// Legacy serialization copied the packet into frame.MessageBytes;
			// consume the caller's packet only after the queue accepts the copy.
			MessagePoolReturn(packet)
		} else {
			MessagePoolReturn(frame.MessageBytes)
		}
		return success
	case SecurityPolicyResultDrop:
		if self.LocalSecurityBypass() {
			return self.localUserNat.SendPacket(source, provideMode, packet, 0)
		} else {
			return false
		}
	default:
		return false
	}
}

// Runs device-side policy once on a complete fragmented UDP datagram, then
// forwards the original MTU-sized ip fragments (either family) as one ordered
// Transfer group. Other fragmented protocols are rejected; TCP should segment
// at MSS, and accepting a partial TCP/SMTP header would create a
// policy-evasion path.
func (self *RemoteUserNatClient) sendReassembledUdpFragments(
	source TransferPath,
	provideMode protocol.ProvideMode,
	reassembled []byte,
	fragments [][]byte,
	timeout time.Duration,
) bool {
	if len(fragments) == 0 {
		return false
	}
	ipPath, payload, err := ParseIpPathWithPayload(reassembled)
	if err != nil || ipPath.Protocol != IpProtocolUdp {
		return false
	}
	relationship := egressRelationship(provideMode, self.provideMode)
	r, err := inspectAndRefreshEgressBorrowed(self.securityPolicy, relationship, *ipPath, payload)
	if err != nil {
		return false
	}
	if r == SecurityPolicyResultDrop && self.LocalSecurityBypass() {
		return self.localUserNat != nil && self.localUserNat.SendPackets(
			source,
			provideMode,
			fragments,
			timeout,
		)
	}
	if r != SecurityPolicyResultAllow || self.client == nil || self.pathTable == nil {
		return false
	}
	destination, err := self.pathTable.SelectDestination(reassembled)
	if err != nil {
		return false
	}
	frames := make([]*protocol.Frame, len(fragments))
	for i, fragment := range fragments {
		frame, frameErr := ipPacketToProviderFrame(fragment, DefaultProtocolVersion)
		if frameErr != nil {
			for _, built := range frames[:i] {
				if !built.Raw {
					MessagePoolReturn(built.MessageBytes)
				}
			}
			return false
		}
		frames[i] = frame
	}
	success, sendErr := self.client.sendMultiHopGroupWithTimeoutDetailed(
		frames,
		destination,
		func(error) {},
		timeout,
		scheduleIpFlow(ipPath),
	)
	if sendErr != nil || !success {
		for _, frame := range frames {
			if !frame.Raw {
				MessagePoolReturn(frame.MessageBytes)
			}
		}
		return false
	}
	for i, frame := range frames {
		if !frame.Raw {
			MessagePoolReturn(fragments[i])
		}
	}
	return true
}

// SendPacketBatch consumes every packet and reports the exact number accepted
// by the basic remote client's existing per-packet multi-hop send path.
func (self *RemoteUserNatClient) SendPacketBatch(
	source TransferPath,
	provideMode protocol.ProvideMode,
	packets [][]byte,
	timeout time.Duration,
) int {
	acceptedCount := 0
	for _, packet := range packets {
		if self.SendPacket(source, provideMode, packet, timeout) {
			acceptedCount += 1
		} else {
			MessagePoolReturn(packet)
		}
	}
	return acceptedCount
}

// `connect.ReceiveFunction`
func (self *RemoteUserNatClient) ClientReceive(source TransferPath, frames []*protocol.Frame, peer Peer) {
	// only process frames from the destinations
	// if allow := self.sourceFilter[source]; !allow {
	//     return
	// }

	for _, frame := range frames {
		// self.client.log.Infof("[trace]receive frame %s\n", frame.MessageType)
		switch frame.MessageType {
		case protocol.MessageType_IpIpPacketFromProvider:
			packet, err := ipPacketFromProviderBytes(frame)
			if err != nil {
				panic(err)
			}
			if isIpFragmentPacket(packet) {
				result := self.ingressIpv4Fragments.processOwned(
					source,
					peer.TransferKey,
					peer.ProvideMode,
					MessagePoolCopy(packet),
				)
				if result.packet != nil {
					ipPath, parseErr := ParseIpPath(result.packet)
					if parseErr == nil && ipPath.Protocol == IpProtocolUdp {
						self.smtpEgressGuard.retireReturn(ipPath)
						self.securityPolicy.RefreshIngress(ipPath)
						for _, fragment := range result.fragments {
							fragment := fragment
							HandleError(func() {
								self.receivePacketCallback(
									source,
									peer.ProvideMode,
									ipPath,
									fragment,
								)
							})
						}
					}
				}
				returnIpFragmentProcessResult(result)
				continue
			}

			ipPath, err := ParseIpPath(packet)
			if err == nil {
				self.smtpEgressGuard.retireReturn(ipPath)
				self.securityPolicy.RefreshIngress(ipPath)
				// Deliberate device-TUN exception from CODESTYLE.md: Transfer
				// acknowledges only after this borrowed packet returns, so the
				// synchronous final injection is its receive-side flow control.
				// This exception does not permit another send or queued wait here.
				HandleError(func() {
					self.receivePacketCallback(
						source,
						peer.ProvideMode,
						ipPath,
						packet,
					)
				})
			}
			// else not an ip packet, drop
		}
	}
}

func (self *RemoteUserNatClient) Shuffle() {
}

func (self *RemoteUserNatClient) Close() {
	// self.client.RemoveReceiveCallback(self.clientCallbackId)
	self.cancel()
	self.egressIpv4Fragments.close()
	self.ingressIpv4Fragments.close()
	self.localUserNat.Close()
	self.localUserNatUnsub()
	self.clientUnsub()
	if self.closeCallback != nil {
		self.closeCallback()
	}
}

// A successful join makes this client's local packet dispatcher and all of
// its protocol flows ownership-terminal. The underlying transfer client
// remains owned by its caller.
func (self *RemoteUserNatClient) CloseAndWait(ctx context.Context) error {
	self.Close()
	if self.localUserNat == nil {
		return nil
	}
	return self.localUserNat.CloseAndWait(ctx)
}

func (self *RemoteUserNatClient) SetAllowDirect(allowDirect bool) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.allowDirect = allowDirect
}

func (self *RemoteUserNatClient) AllowDirect() bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.allowDirect
}

func (self *RemoteUserNatClient) SetLocalSecurityBypass(localSecurityBypass bool) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.localSecurityBypass = localSecurityBypass
}

func (self *RemoteUserNatClient) LocalSecurityBypass() bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.localSecurityBypass
}

type pathTable struct {
	destinations []MultiHopId

	// TODO clean up entries that haven't been used in some time
	paths4 map[Ip4Path]MultiHopId
	paths6 map[Ip6Path]MultiHopId
}

func newPathTable(destinations []MultiHopId) *pathTable {
	return &pathTable{
		destinations: destinations,
		paths4:       map[Ip4Path]MultiHopId{},
		paths6:       map[Ip6Path]MultiHopId{},
	}
}

func (self *pathTable) Destinations() []MultiHopId {
	return slices.Clone(self.destinations)
}

func (self *pathTable) DestinationIds() []Id {
	var clientIds []Id
	for _, destination := range self.destinations {
		clientIds = append(clientIds, destination.Tail())
	}
	return clientIds
}

func (self *pathTable) SelectDestination(packet []byte) (MultiHopId, error) {
	if len(self.destinations) == 0 {
		return MultiHopId{}, fmt.Errorf("No destinations")
	}
	if len(self.destinations) == 1 {
		return self.destinations[0], nil
	}

	ipPath, err := ParseIpPath(packet)
	if err != nil {
		return MultiHopId{}, err
	}
	switch ipPath.Version {
	case 4:
		ip4Path := ipPath.ToIp4Path()
		if destination, ok := self.paths4[ip4Path]; ok {
			return destination, nil
		}
		i := mathrand.Intn(len(self.destinations))
		destination := self.destinations[i]
		self.paths4[ip4Path] = destination
		return destination, nil
	case 6:
		ip6Path := ipPath.ToIp6Path()
		if destination, ok := self.paths6[ip6Path]; ok {
			return destination, nil
		}
		i := mathrand.Intn(len(self.destinations))
		destination := self.destinations[i]
		self.paths6[ip6Path] = destination
		return destination, nil
	default:
		// no support for this version
		return MultiHopId{}, fmt.Errorf("No support for ip version %d", ipPath.Version)
	}
}

type IpProtocol int

const (
	IpProtocolUnknown IpProtocol = 0
	IpProtocolTcp     IpProtocol = 1
	IpProtocolUdp     IpProtocol = 2
	IpProtocolIcmp    IpProtocol = 3
)

func (self IpProtocol) String() string {
	switch self {
	case IpProtocolTcp:
		return "tcp"
	case IpProtocolUdp:
		return "udp"
	case IpProtocolIcmp:
		return "icmp"
	default:
		return "unknown"
	}
}

type IpPath struct {
	Version         int
	Protocol        IpProtocol
	SourceIp        net.IP
	SourcePort      int
	DestinationIp   net.IP
	DestinationPort int

	SequenceNumber    uint32
	AckSequenceNumber uint32
	// TcpWindowSize is the raw 16-bit advertised receive window. Keeping it in
	// the parsed path lets tunnel collapse prevention distinguish a genuine
	// zero-window close/reopen from a duplicate ACK; the negotiated scale is
	// irrelevant for that change detector.
	TcpWindowSize uint16
	// TcpPayloadByteCount makes the FIN edge unambiguous: a FIN can legally
	// share a segment with application bytes, and its acknowledged sequence is
	// therefore seq + payload + 1 rather than merely seq + 1.
	TcpPayloadByteCount int
	Syn                 bool
	Fin                 bool
	Rst                 bool
	Ack                 bool

	ServerName string
}

func ParseIpPath(ipPacket []byte) (*IpPath, error) {
	ipPath, _, err := ParseIpPathWithPayload(ipPacket)
	return ipPath, err
}

func ParseIpPathWithPayload(ipPacket []byte) (*IpPath, []byte, error) {
	var ipPath IpPath
	payload, err := parseIpPathWithPayloadBorrowed(ipPacket, &ipPath)
	if err != nil {
		return nil, nil, err
	}
	// The public result owns its addresses. Keep this copy outside the borrowed
	// parser so escape analysis does not conservatively apply this allocation
	// branch to every synchronous borrowed parse.
	ipBacking := make(net.IP, len(ipPath.SourceIp)+len(ipPath.DestinationIp))
	sourceByteCount := copy(ipBacking, ipPath.SourceIp)
	copy(ipBacking[sourceByteCount:], ipPath.DestinationIp)
	ipPath.SourceIp = ipBacking[:sourceByteCount:sourceByteCount]
	ipPath.DestinationIp = ipBacking[sourceByteCount:]
	return &ipPath, payload, nil
}

// IsTcpAckOnlyPacket identifies a TCP acknowledgement with no sequence-space
// payload or connection-state edge. SACK, ECN, duplicate ACK, and advertised
// window information may still be present. Callers may use the classification
// for bounded admission priority, but must preserve both the exact packet bytes
// and ordinary Transfer recovery. Parsing is synchronous and allocation-free.
func IsTcpAckOnlyPacket(ipPacket []byte) bool {
	var ipPath IpPath
	payload, err := parseIpPathWithPayloadBorrowed(ipPacket, &ipPath)
	return err == nil &&
		ipPath.Protocol == IpProtocolTcp &&
		ipPath.Ack &&
		!ipPath.Syn &&
		!ipPath.Fin &&
		!ipPath.Rst &&
		len(payload) == 0
}

// parseIpPathWithPayloadBorrowed fills ipPath without allocating address
// copies. The address slices alias ipPacket and are valid only while ipPacket
// is valid. It is for synchronous packet-policy hot paths; anything retaining
// an IpPath must use ParseIpPathWithPayload instead.
func parseIpPathWithPayloadBorrowed(ipPacket []byte, ipPath *IpPath) ([]byte, error) {
	if len(ipPacket) == 0 {
		return nil, fmt.Errorf("Empty packet.")
	}
	ipVersion := uint8(ipPacket[0]) >> 4
	var ipProtocol ipProtocolNumber
	var sourceIp net.IP
	var destinationIp net.IP
	var transport []byte
	var ok bool
	switch ipVersion {
	case 4:
		ipProtocol, sourceIp, destinationIp, transport, ok = parseIpv4(ipPacket)
	case 6:
		ipProtocol, sourceIp, destinationIp, transport, ok = parseIpv6(ipPacket)
	default:
		// no support for this version
		return nil, fmt.Errorf("No support for ip version %d", ipVersion)
	}
	if !ok {
		return nil, fmt.Errorf("Malformed ip packet.")
	}

	switch ipProtocol {
	case ipProtocolNumberUdp:
		var udp parsedUdp
		if !parseUdpPacket(sourceIp, destinationIp, transport, &udp) {
			return nil, fmt.Errorf("Malformed udp packet.")
		}

		*ipPath = IpPath{
			Version:         int(ipVersion),
			Protocol:        IpProtocolUdp,
			SourceIp:        sourceIp,
			SourcePort:      int(udp.sourcePort),
			DestinationIp:   destinationIp,
			DestinationPort: int(udp.destinationPort),
		}
		return udp.payload, nil
	case ipProtocolNumberTcp:
		var tcp parsedTcp
		if !parseTcpPacket(sourceIp, destinationIp, transport, &tcp) {
			return nil, fmt.Errorf("Malformed tcp packet.")
		}

		*ipPath = IpPath{
			Version:             int(ipVersion),
			Protocol:            IpProtocolTcp,
			SourceIp:            sourceIp,
			SourcePort:          int(tcp.sourcePort),
			DestinationIp:       destinationIp,
			DestinationPort:     int(tcp.destinationPort),
			SequenceNumber:      tcp.seq,
			AckSequenceNumber:   tcp.ackNumber,
			TcpWindowSize:       tcp.windowSize,
			TcpPayloadByteCount: len(tcp.payload),
			Syn:                 tcp.syn,
			Fin:                 tcp.fin,
			Rst:                 tcp.rst,
			Ack:                 tcp.ack,
		}
		return tcp.payload, nil
	default:
		// icmp and the unsupported protocols are parsed out of line: keeping
		// this switch at its original two hot cases preserves the tcp and udp
		// dispatch (adding cases here measurably slowed the udp parse)
		return parseIcmpIpPathBorrowed(ipVersion, ipProtocol, sourceIp, destinationIp, transport, ipPath)
	}
}

// parseIcmpIpPathBorrowed is the cold tail of parseIpPathWithPayloadBorrowed:
// icmp echo, else the unsupported protocol error. Split out so the hot tcp/udp
// dispatch above is unchanged by icmp support.
//
//go:noinline
func parseIcmpIpPathBorrowed(
	ipVersion uint8,
	ipProtocol ipProtocolNumber,
	sourceIp net.IP,
	destinationIp net.IP,
	transport []byte,
	ipPath *IpPath,
) ([]byte, error) {
	switch ipProtocol {
	case ipProtocolNumberIcmp4, ipProtocolNumberIcmp6:
	default:
		return nil, fmt.Errorf("No support for protocol %d", ipProtocol)
	}
	if (ipProtocol == ipProtocolNumberIcmp4) != (ipVersion == 4) {
		// the icmp variant must match the ip version
		return nil, fmt.Errorf("No support for protocol %d", ipProtocol)
	}
	var icmp parsedIcmp
	if !parseIcmpPacket(int(ipVersion), sourceIp, destinationIp, transport, &icmp) {
		if ipVersion == 6 && 0 < len(transport) && isIcmpv6LinkControlType(transport[0]) {
			// a distinct error so a caller can drop link-local control
			// chatter (neighbor/router discovery, mld) without logging it
			return nil, errIcmpv6LinkControl
		}
		return nil, fmt.Errorf("Unsupported or malformed icmp packet.")
	}

	// the echo identifier stands in for the port on the client side of the
	// flow: the source side of a request, the destination side of a reply,
	// so Reverse aligns the two directions (see ICMP.md)
	*ipPath = IpPath{
		Version:       int(ipVersion),
		Protocol:      IpProtocolIcmp,
		SourceIp:      sourceIp,
		DestinationIp: destinationIp,
	}
	if icmp.echoRequest {
		ipPath.SourcePort = int(icmp.identifier)
	} else {
		ipPath.DestinationPort = int(icmp.identifier)
	}
	return icmp.payload, nil
}

func (self *IpPath) SourceHostPort() string {
	return net.JoinHostPort(
		self.SourceIp.String(),
		strconv.Itoa(self.SourcePort),
	)
}

func (self *IpPath) DestinationHostPort() string {
	return net.JoinHostPort(
		self.DestinationIp.String(),
		strconv.Itoa(self.DestinationPort),
	)
}

func (self *IpPath) ToIp4Path() Ip4Path {
	var sourceIp [4]byte
	if self.SourceIp != nil {
		if sourceIp4 := self.SourceIp.To4(); sourceIp4 != nil {
			sourceIp = [4]byte(sourceIp4)
		}
	}
	var destinationIp [4]byte
	if self.DestinationIp != nil {
		if destinationIp4 := self.DestinationIp.To4(); destinationIp4 != nil {
			destinationIp = [4]byte(destinationIp4)
		}
	}
	return Ip4Path{
		Protocol:        self.Protocol,
		SourceIp:        sourceIp,
		SourcePort:      self.SourcePort,
		DestinationIp:   destinationIp,
		DestinationPort: self.DestinationPort,
		ServerName:      self.ServerName,
	}
}

func (self *IpPath) ToIp6Path() Ip6Path {
	var sourceIp [16]byte
	if self.SourceIp != nil {
		if sourceIp6 := self.SourceIp.To16(); sourceIp6 != nil {
			sourceIp = [16]byte(sourceIp6)
		}
	}
	var destinationIp [16]byte
	if self.DestinationIp != nil {
		if destinationIp6 := self.DestinationIp.To16(); destinationIp6 != nil {
			destinationIp = [16]byte(destinationIp6)
		}
	}
	return Ip6Path{
		Protocol:        self.Protocol,
		SourceIp:        sourceIp,
		SourcePort:      self.SourcePort,
		DestinationIp:   destinationIp,
		DestinationPort: self.DestinationPort,
		ServerName:      self.ServerName,
	}
}

// dialFailureAction classifies a failed upstream DialContext into the signal
// the provider sends back to the source in place of the SynAck. Without a
// signal the source sits in syn-retransmit backoff (3s..63s) and the blackhole
// timeout eventually removes the provider along with its working flows -- the
// bug this replaces.
type dialFailureAction int

const (
	// dialFailureNone: send nothing (nil path / unsupported ip version).
	dialFailureNone dialFailureAction = iota
	// dialFailureRst: the destination itself refused the connection. The caller
	// answers with ConnectionState.RstAck() -- honest tcp semantics, forwarded
	// to the app.
	dialFailureRst
	// dialFailureUnreachable: capacity-class failure. The caller delivers the
	// icmp destination-unreachable packet returned alongside this action.
	dialFailureUnreachable
)

// classifyDialFailure maps an upstream DialContext error to the signal the
// provider should return in place of a SynAck. It is pure -- no live socket, no
// ConnectionState -- so the errno logic is unit-testable.
//
// For dialFailureUnreachable the built icmp packet is returned as well (a fresh
// non-pool buffer from ipOosUnreachable; the caller copies it into the pool
// before delivery). For dialFailureRst and dialFailureNone the packet is nil --
// a RST needs the ConnectionState's sequence state, so the caller builds it.
//
// tcp + ECONNREFUSED means the destination refused: RST+ACK. Anything else on
// tcp (timeouts, EMFILE/ENFILE, EADDRNOTAVAIL, unreachable, unrecognized) is
// capacity-class and gets an unreachable; defaulting unrecognized errors to the
// capacity class is deliberate -- new clients intercept the signal and old ones
// drop it at ParseIpPath, so a misclassification is cheap. errors.Is folds the
// Windows WSAECONNREFUSED onto syscall.ECONNREFUSED, but the providers this runs
// on are almost all linux, so linux errno semantics are what matter. udp "dial"
// cannot produce a meaningful ECONNREFUSED at connect time, so udp is always
// capacity-class.
func classifyDialFailure(ipPath *IpPath, err error) (dialFailureAction, []byte) {
	if ipPath == nil {
		return dialFailureNone, nil
	}
	switch ipPath.Protocol {
	case IpProtocolTcp:
		if errors.Is(err, syscall.ECONNREFUSED) {
			return dialFailureRst, nil
		}
		// otherwise fall through to the unreachable build below
	case IpProtocolUdp:
		// always capacity-class; fall through
	default:
		return dialFailureNone, nil
	}
	if packet, ok := ipOosUnreachable(ipPath); ok {
		return dialFailureUnreachable, packet
	}
	return dialFailureNone, nil
}

func (self *IpPath) Source() *IpPath {
	return &IpPath{
		Protocol:   self.Protocol,
		Version:    self.Version,
		SourceIp:   self.SourceIp,
		SourcePort: self.SourcePort,
	}
}

func (self *IpPath) Destination() *IpPath {
	return &IpPath{
		Protocol:        self.Protocol,
		Version:         self.Version,
		DestinationIp:   self.DestinationIp,
		DestinationPort: self.DestinationPort,
	}
}

// ReverseValue returns the flow tuple in the opposite direction without
// allocating. Sequence/flag state is intentionally omitted, matching Reverse:
// synthetic packets built from a reversed path are out of sequence.
func (self *IpPath) ReverseValue() IpPath {
	return IpPath{
		Protocol:        self.Protocol,
		Version:         self.Version,
		SourceIp:        self.DestinationIp,
		SourcePort:      self.DestinationPort,
		DestinationIp:   self.SourceIp,
		DestinationPort: self.SourcePort,
	}
}

func (self *IpPath) Reverse() *IpPath {
	reversed := self.ReverseValue()
	return &reversed
}

// comparable
type Ip4Path struct {
	Protocol        IpProtocol
	SourceIp        [4]byte
	SourcePort      int
	DestinationIp   [4]byte
	DestinationPort int
	ServerName      string
}

func (self *Ip4Path) Source() Ip4Path {
	return Ip4Path{
		Protocol:   self.Protocol,
		SourceIp:   self.SourceIp,
		SourcePort: self.SourcePort,
	}
}

func (self *Ip4Path) Destination() Ip4Path {
	return Ip4Path{
		Protocol:        self.Protocol,
		DestinationIp:   self.DestinationIp,
		DestinationPort: self.DestinationPort,
	}
}

// comparable
type Ip6Path struct {
	Protocol        IpProtocol
	SourceIp        [16]byte
	SourcePort      int
	DestinationIp   [16]byte
	DestinationPort int
	ServerName      string
}

func (self *Ip6Path) Source() Ip6Path {
	return Ip6Path{
		Protocol:   self.Protocol,
		SourceIp:   self.SourceIp,
		SourcePort: self.SourcePort,
	}
}

func (self *Ip6Path) Destination() Ip6Path {
	return Ip6Path{
		Protocol:        self.Protocol,
		DestinationIp:   self.DestinationIp,
		DestinationPort: self.DestinationPort,
	}
}

type UserLimited interface {
	LastActivityTime() time.Time
	Cancel()
}

type userLimited struct {
	mutex            sync.Mutex
	lastActivityTime time.Time
}

func newUserLimited() *userLimited {
	return &userLimited{
		lastActivityTime: time.Now(),
	}
}

func (self *userLimited) LastActivityTime() time.Time {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return self.lastActivityTime
}

func (self *userLimited) UpdateLastActivityTime() {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.lastActivityTime = time.Now()
}

const lruEvictionSampleSize = 32

// applyLruMapLimit bounds eviction work independently of table cardinality.
// Go map iteration starts at a pseudo-random bucket, so choosing the oldest of
// the first fixed-size sample is an approximate LRU without collecting and
// sorting the entire flow table under its dispatch lock. limitCallback must
// remove an accepted resource from resources; it owns any cancellation.
func applyLruMapLimit[K comparable, R UserLimited](
	resources map[K]R,
	limit int,
	limitCallback func(K, R) bool,
) {
	for limit < len(resources) {
		var oldestKey K
		var oldest R
		var oldestTime time.Time
		found := false
		sampled := 0
		for key, resource := range resources {
			activityTime := resource.LastActivityTime()
			if !found || activityTime.Before(oldestTime) {
				oldestKey = key
				oldest = resource
				oldestTime = activityTime
				found = true
			}
			sampled += 1
			if lruEvictionSampleSize <= sampled {
				break
			}
		}
		if !found || !limitCallback(oldestKey, oldest) {
			return
		}
	}
}
