package connect

// a userspace tun device backed by the gvisor network stack.
// `Tun` exposes a packet interface on one side (`Read`/`Write`) and
// socket interfaces on the other (`DialContext`, `ListenTCP`, `ListenUDP`).
// each tun instance owns a private gvisor stack with one nic, one link-local
// ipv4 address and, when the link mtu admits it, one ula ipv6 address.
//
// Dual stack (IPV6.md C1): the stack carries both families with a default
// route for each, so a tun can originate and accept v6 flows exactly like v4.
// gVisor refuses to emit IPv6 on a link narrower than the protocol's minimum
// MTU (1280, RFC 8200 §5), so a tun whose settings.Mtu is below
// tunIpv6MinimumMtu keeps IPv4 only: no v6 address, no v6 route, and v6 dials
// and writes fail with EAFNOSUPPORT as they always did. See Ipv6Enabled.

import (
	// "bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"runtime"
	// "regexp"
	mathrand "math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	// "github.com/gopacket/gopacket"
	// "github.com/gopacket/gopacket/layers"

	"gvisor.dev/gvisor/pkg/buffer"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/link/channel"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv6"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
	stackgro "gvisor.dev/gvisor/pkg/tcpip/stack/gro"
	"gvisor.dev/gvisor/pkg/tcpip/transport/icmp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
	"gvisor.dev/gvisor/pkg/tcpip/transport/udp"
	"gvisor.dev/gvisor/pkg/waiter"
)

// const DefaultChannelSize = 64

func DefaultTunSettings() *TunSettings {
	return DefaultTunSettingsWithBufferSize(1024)
}

func DefaultTunSettingsWithBufferSize(bufferSize int) *TunSettings {
	return &TunSettings{
		ChannelSize: bufferSize,
		// Far above a healthy drain interval (the reader empties a full
		// queue in milliseconds), so ordinary bulk transfer still sees
		// backpressure rather than drops.
		OutboundQueueWaitTimeout: 250 * time.Millisecond,
		// the link mtu, matching the native tunnel interfaces
		// (`DefaultTunnelMtu`). Packets written into the tun are at most
		// `DefaultMtu`, which is below this by design. IPv6 needs at least
		// tunIpv6MinimumMtu here; below that the tun is IPv4 only (see the
		// file comment).
		Mtu: DefaultTunnelMtu,

		DialRace:          2,
		DialRaceTimeout:   2 * time.Second,
		DialTimeout:       30 * time.Second,
		DohRequestTimeout: 60 * time.Second,

		// the gvisor udp endpoint buffers default to 32KiB, which is too small for fast
		// transfer; cap at 1MiB (gvisor clamps to at most 4MiB) to bound per-endpoint
		// memory on the shared stack used by the server/proxy.
		// per endpoint, so scaled by the memory budget.
		UdpReceiveBufferByteCount: int(MemoryScaledByteCount(mib(1), kib(128))),
		UdpSendBufferByteCount:    int(MemoryScaledByteCount(mib(1), kib(128))),

		// tcp buffer auto-tuning ranges for the server/proxy data plane (the shared
		// stack). Max applies per connection, so it caps per-connection memory; a
		// memory-constrained IpMux on a private stack shrinks these much further.
		// default and max are per connection, so scaled by the memory budget.
		// The tunnel path's effective ack rtt runs orders of magnitude above
		// loopback (userspace relay hops + ack coalescing), so the throughput
		// of a single stream is window/rtt-bound: the former 256KiB default
		// measured ~1.8 MiB/s upload / ~0.8 MiB/s download against a tunnel
		// with 17+ MiB/s of udp capacity. Larger defaults let auto-tuning
		// reach a window that covers the tunnel's bandwidth-delay product.
		TcpReceiveBuffer: TcpBufferRange{
			Min:     4 * 1024,
			Default: int(MemoryScaledByteCount(mib(1), kib(128))),
			Max:     int(MemoryScaledByteCount(mib(4), kib(512))),
		},
		TcpSendBuffer: TcpBufferRange{
			Min:     4 * 1024,
			Default: int(MemoryScaledByteCount(mib(1), kib(128))),
			Max:     int(MemoryScaledByteCount(mib(4), kib(512))),
		},

		// cap rto backoff well below the gvisor default (120s). The path under
		// the tun is a tunnel that does its own retransmission, so segment loss
		// is dominated by transient starvation/burst-drop windows; after such a
		// window a 120s-capped backoff strands an otherwise-recovered stream in
		// a minutes-long silent stall. Retrying every few seconds bounds the
		// stall at negligible bandwidth cost.
		TcpMaxRto: 8 * time.Second,

		// coalesce WriteBatch tcp packets into super-segments before delivery
		// (see Tun.WriteBatch). Off in a zero-value TunSettings, which keeps
		// hand-rolled settings on the per-packet behavior.
		TcpGro: true,
	}
}

type TunSettings struct {
	// Log, when set, is used by the tun. nil resolves to `DefaultLogger()`.
	Log Logger

	ChannelSize int
	Mtu         int
	// OutboundQueueWaitTimeout bounds how long netstack waits for space in
	// the outbound (tun read) queue before dropping the rest of a write. The
	// queue's consumer can be inside an inbound injection itself (SendPacket
	// -> receive callback -> Tun.Write -> gVisor reply), which would otherwise
	// deadlock the whole stack. Non-positive waits without bound.
	OutboundQueueWaitTimeout time.Duration

	DialRace        int
	DialRaceTimeout time.Duration
	DialTimeout     time.Duration

	// DohRequestTimeout bounds a single DoH request through this tun's resolver (total connect +
	// TLS + query). The IpMux sets it from DnsUpgradeSettings.ResolveTimeout so DNS resolution has
	// a single timeout knob. 0 falls back to a default.
	DohRequestTimeout time.Duration

	// DohSettings, when set, is the base settings for the tun's resolver cache — a
	// memory-constrained consumer bounds the cache and fan-out here (see the UpgradeMux).
	// buildDohCache copies it and overlays the tun dialer, log, timeouts, and resolver
	// settings, so those fields of the base are ignored. nil uses DefaultDohSettings.
	DohSettings *DohSettings

	UdpReceiveBufferByteCount int
	UdpSendBufferByteCount    int

	// TcpReceiveBuffer/TcpSendBuffer are the gVisor TCP buffer auto-tuning ranges. Max
	// applies per connection, so these dominate per-connection memory; a memory-bound
	// consumer should lower them.
	TcpReceiveBuffer TcpBufferRange
	TcpSendBuffer    TcpBufferRange

	// TcpMaxRto, when positive, caps the gVisor TCP retransmission timeout
	// (the default cap is 120s). See DefaultTunSettings for why the tun uses
	// a small cap.
	TcpMaxRto time.Duration
	// TcpMinRto, when positive, sets the floor of the gVisor TCP
	// retransmission timeout (the stack default is 200ms). The floor bounds
	// the peer's acknowledgement compression from above: an acknowledgement
	// held longer than a sender's floor is a spurious retransmission
	// (THROUGHPUTFIX §22). Zero leaves the stack default.
	TcpMinRto time.Duration

	// TcpGro enables generic receive offload for Tun.WriteBatch: the tcp
	// packets of one batch coalesce into super-segments before delivery,
	// amortizing per-segment dispatch, endpoint enqueue, and ack generation.
	// Tun.Write is unaffected.
	TcpGro bool
}

// TcpBufferRange is a gVisor TCP buffer auto-tuning range in bytes.
type TcpBufferRange struct {
	Min     int
	Default int
	Max     int
}

// tunIpv6MinimumMtu is the smallest link mtu on which gVisor will emit IPv6
// (header.IPv6MinimumMTU, 1280). A tun below it is IPv4 only.
const tunIpv6MinimumMtu = int(header.IPv6MinimumMTU)

func newTunStack(
	tcpReceive TcpBufferRange,
	tcpSend TcpBufferRange,
	tcpMaxRto time.Duration,
	tcpMinRto time.Duration,
) *stack.Stack {
	opts := stack.Options{
		NetworkProtocols: []stack.NetworkProtocolFactory{
			ipv4.NewProtocolWithOptions(ipv4.Options{AllowExternalLoopbackTraffic: true}),
			// the tun is a point-to-point link into the tunnel: there is no
			// router to solicit and no neighbor to detect a duplicate
			// address against, and every such probe would otherwise leave
			// through the tunnel as user traffic (and hold the address
			// tentative, failing dials, until it timed out)
			ipv6.NewProtocolWithOptions(ipv6.Options{
				NDPConfigs: ipv6.NDPConfigurations{
					MaxRtrSolicitations: 0,
					HandleRAs:           ipv6.HandlingRAsDisabled,
				},
				DADConfigs:                   stack.DADConfigurations{DupAddrDetectTransmits: 0},
				AllowExternalLoopbackTraffic: true,
			}),
		},
		TransportProtocols: []stack.TransportProtocolFactory{tcp.NewProtocol, udp.NewProtocol, icmp.NewProtocol4, icmp.NewProtocol6},
		HandleLocal:        true,
	}
	s := stack.New(opts)

	// size the tcp buffer ranges above the gvisor defaults.
	// inbound segments are accounted against the receive buffer size, and
	// in-window segments that exceed it are dropped. senders into the tun do
	// not retransmit, so the receive buffer needs headroom for inbound bursts
	// above the advertised window.
	{
		opt := tcpip.TCPReceiveBufferSizeRangeOption{
			Min:     tcpReceive.Min,
			Default: tcpReceive.Default,
			Max:     tcpReceive.Max,
		}
		s.SetTransportProtocolOption(tcp.ProtocolNumber, &opt)
	}
	{
		opt := tcpip.TCPSendBufferSizeRangeOption{
			Min:     tcpSend.Min,
			Default: tcpSend.Default,
			Max:     tcpSend.Max,
		}
		s.SetTransportProtocolOption(tcp.ProtocolNumber, &opt)
	}
	if 0 < tcpMaxRto {
		opt := tcpip.TCPMaxRTOOption(tcpMaxRto)
		s.SetTransportProtocolOption(tcp.ProtocolNumber, &opt)
	}
	if 0 < tcpMinRto {
		opt := tcpip.TCPMinRTOOption(tcpMinRto)
		s.SetTransportProtocolOption(tcp.ProtocolNumber, &opt)
	}

	return s
}

type NicIdAllocator struct {
	stateLock   sync.Mutex
	counter     uint32
	freeList    []tcpip.NICID
	maxFreeList int
}

func NewNicIdAllocator(maxFreeList int) *NicIdAllocator {
	return &NicIdAllocator{
		maxFreeList: maxFreeList,
	}
}

func (self *NicIdAllocator) TakeNicId() tcpip.NICID {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if n := len(self.freeList); n > 0 {
		id := self.freeList[n-1]
		self.freeList = self.freeList[:n-1]
		return id
	}
	self.counter += 1
	return tcpip.NICID(self.counter)
}

func (self *NicIdAllocator) ReturnNicId(id tcpip.NICID) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if len(self.freeList) >= self.maxFreeList {
		return
	}
	self.freeList = append(self.freeList, id)
}

var defaultNicIdAllocator = NewNicIdAllocator(128)

type LocalIpv4AddressAllocator struct {
	stateLock   sync.Mutex
	generator   *AddrGenerator
	freeList    []netip.Addr
	maxFreeList int
}

func NewLocalIpv4AddressAllocator(prefix netip.Prefix, maxFreeList int) *LocalIpv4AddressAllocator {
	return &LocalIpv4AddressAllocator{
		generator:   NewAddrGenerator(prefix),
		maxFreeList: maxFreeList,
	}
}

func (self *LocalIpv4AddressAllocator) TakeAddr() (netip.Addr, bool) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if n := len(self.freeList); n > 0 {
		addr := self.freeList[n-1]
		self.freeList = self.freeList[:n-1]
		return addr, true
	}
	return self.generator.Next()
}

func (self *LocalIpv4AddressAllocator) ReturnAddr(addr netip.Addr) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if len(self.freeList) >= self.maxFreeList {
		return
	}
	self.freeList = append(self.freeList, addr)
}

// defaultLocalIpv4AddressAllocator is created lazily on first use so that merely
// importing connect does not spin up the generator goroutine (NewAddrGenerator
// launches one) unless a local address is actually reserved.
var defaultLocalIpv4AddressAllocator = sync.OnceValue(func() *LocalIpv4AddressAllocator {
	return NewLocalIpv4AddressAllocator(
		netip.MustParsePrefix("169.254.0.0/16"),
		128,
	)
})

// LocalIpv6Prefix is the one fixed unique-local /64 (RFC 4193) every tun and
// native tunnel address in this process lives in: fd75:726e:6574::/64, the
// hex of "urnet" under fd00::/8. A fixed prefix is the v6 counterpart of the
// 169.254.0.0/16 pool: nothing on a real network routes it, and both ends of
// a tunnel can recognize it as tunnel-internal.
var LocalIpv6Prefix = netip.MustParsePrefix("fd75:726e:6574::/64")

// localIpv6AllocatorPrefix is the low /96 of LocalIpv6Prefix that the tun
// allocator hands out sequentially. A /96 keeps the address iterator's count
// inside an int (a /64 has 2^64 hosts, which overflows it to zero).
var localIpv6AllocatorPrefix = netip.MustParsePrefix("fd75:726e:6574::/96")

// LocalIpv6AddressAllocator is the v6 counterpart of LocalIpv4AddressAllocator:
// process-unique tun addresses from localIpv6AllocatorPrefix with a bounded
// free list. Safe for concurrent use.
type LocalIpv6AddressAllocator struct {
	stateLock   sync.Mutex
	generator   *AddrGenerator
	freeList    []netip.Addr
	maxFreeList int
}

func NewLocalIpv6AddressAllocator(prefix netip.Prefix, maxFreeList int) *LocalIpv6AddressAllocator {
	return &LocalIpv6AddressAllocator{
		generator:   NewAddrGenerator(prefix),
		maxFreeList: maxFreeList,
	}
}

func (self *LocalIpv6AddressAllocator) TakeAddr() (netip.Addr, bool) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if n := len(self.freeList); n > 0 {
		addr := self.freeList[n-1]
		self.freeList = self.freeList[:n-1]
		return addr, true
	}
	return self.generator.Next()
}

func (self *LocalIpv6AddressAllocator) ReturnAddr(addr netip.Addr) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if len(self.freeList) >= self.maxFreeList {
		return
	}
	self.freeList = append(self.freeList, addr)
}

// defaultLocalIpv6AddressAllocator is lazy for the same reason as the v4 one.
var defaultLocalIpv6AddressAllocator = sync.OnceValue(func() *LocalIpv6AddressAllocator {
	return NewLocalIpv6AddressAllocator(localIpv6AllocatorPrefix, 128)
})

// TakeLocalIpv6Address reserves a process-unique local IPv6 address from the
// tun pool inside LocalIpv6Prefix. Return it with ReturnLocalIpv6Address when
// the address is no longer in use.
func TakeLocalIpv6Address() (netip.Addr, bool) {
	return defaultLocalIpv6AddressAllocator().TakeAddr()
}

// ReturnLocalIpv6Address returns an address previously taken with
// TakeLocalIpv6Address to the pool's free list.
func ReturnLocalIpv6Address(addr netip.Addr) {
	defaultLocalIpv6AddressAllocator().ReturnAddr(addr)
}

// RandomLocalIpv6 returns a native tunnel address in LocalIpv6Prefix with a
// random 64-bit interface identifier outside the tun allocator's /96, the v6
// counterpart of RandomLocalIpv4. A unique-local address never overlaps a
// real network the way a 10/8 lease can, so there is nothing to avoid.
func RandomLocalIpv6() netip.Addr {
	addr := LocalIpv6Prefix.Masked().Addr().As16()
	for {
		mathrand.Read(addr[8:])
		candidate := netip.AddrFrom16(addr)
		// keep clear of the subnet anycast address and the tun pool
		if candidate != LocalIpv6Prefix.Masked().Addr() && !localIpv6AllocatorPrefix.Contains(candidate) {
			return candidate
		}
	}
}

// TakeLocalIpv4Address reserves a process-unique local IPv4 address from the default
// 169.254.0.0/16 pool shared by Tun and the SDK tunnel address. Return it with
// ReturnLocalIpv4Address when the address is no longer in use.
func TakeLocalIpv4Address() (netip.Addr, bool) {
	return defaultLocalIpv4AddressAllocator().TakeAddr()
}

// ReturnLocalIpv4Address returns an address previously taken with
// TakeLocalIpv4Address to the pool's free list.
func ReturnLocalIpv4Address(addr netip.Addr) {
	defaultLocalIpv4AddressAllocator().ReturnAddr(addr)
}

// LocalIpv4Networks returns the IPv4 networks currently assigned to the device's
// interfaces (each masked to its prefix), best-effort. Callers use it to avoid
// handing out a tunnel address that overlaps a real local subnet. On platforms
// where interface enumeration is restricted it returns nil, and the caller falls
// back to an unchecked random address.
func LocalIpv4Networks() []netip.Prefix {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}
	var networks []netip.Prefix
	for _, iface := range ifaces {
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			ipNet, ok := addr.(*net.IPNet)
			if !ok {
				continue
			}
			ip4 := ipNet.IP.To4()
			if ip4 == nil {
				continue
			}
			ones, bits := ipNet.Mask.Size()
			if bits != 32 {
				continue
			}
			prefix := netip.PrefixFrom(netip.AddrFrom4([4]byte{ip4[0], ip4[1], ip4[2], ip4[3]}), ones)
			networks = append(networks, prefix.Masked())
		}
	}
	return networks
}

// RandomLocalIpv4 returns a tunnel address in 10.0.0.0/8 whose /24 is the
// lexicographically smallest 10.a.b.0/24 that does not overlap any prefix in
// `avoid` (the device's real local subnets), with the host octet randomized in
// [2, 254] so it looks like an ordinary DHCP lease (never .0/.1/.255) instead of
// a fixed value that could fingerprint the network. Preferring the minimum free
// /24 lands on common subnets such as 10.0.0.0/24 that blend in. If every /24 is
// excluded (e.g. the device holds all of 10/8), it falls back to 10.0.0.h.
func RandomLocalIpv4(avoid []netip.Prefix) netip.Addr {
	h := byte(2 + mathrand.Intn(253)) // 2..254, never .0/.1/.255
	for a := 0; a < 256; a++ {
		for b := 0; b < 256; b++ {
			subnet := netip.PrefixFrom(netip.AddrFrom4([4]byte{10, byte(a), byte(b), 0}), 24)
			conflict := false
			for _, network := range avoid {
				if network.Overlaps(subnet) {
					conflict = true
					break
				}
			}
			if !conflict {
				return netip.AddrFrom4([4]byte{10, byte(a), byte(b), h})
			}
		}
	}
	return netip.AddrFrom4([4]byte{10, 0, 0, h})
}

type Tun struct {
	ctx    context.Context
	cancel context.CancelFunc
	log    Logger

	settings *TunSettings

	ep                        *tunLinkEndpoint
	stack                     *stack.Stack
	nicId                     tcpip.NICID
	nicIdAllocator            *NicIdAllocator
	localAddresses            []netip.Addr
	localIpv4AddressAllocator *LocalIpv4AddressAllocator
	localIpv6AddressAllocator *LocalIpv6AddressAllocator
	// ipv6Enabled is whether the stack carries IPv6: a v6 address and default
	// route exist. False when settings.Mtu is below tunIpv6MinimumMtu.
	ipv6Enabled bool
	// mtu                 int
	// registeredAddresses map[netip.Addr]bool
	dohResolver atomic.Pointer[DohCache]

	// tcpInboundShards preserve same-flow injection order while independent
	// browser connections continue in parallel.
	tcpInboundShards [tunTcpInboundShardCount]tunTcpInboundShard

	// gro coalesces the tcp packets of a WriteBatch into super-segments
	// before delivery, amortizing the per-segment dispatch, endpoint
	// enqueue, and ack generation that dominate the per-packet Write path.
	// groMu serializes batches (the gvisor GRO state is not thread-safe);
	// per-batch Flush empties the state, so no packet outlives its batch.
	groMu sync.Mutex
	gro   stackgro.GRO

	// closeOnce makes Close idempotent: a second Close must not return the nic id and
	// local address to the shared allocators again — a double return hands the same
	// address out twice, and two later tuns silently share it (breaking their routing).
	closeOnce sync.Once

	stateLock sync.Mutex
}

const (
	// Reconcile each producer burst well below gVisor's 100-segment processing
	// quantum. A processor that meets a syscall-owned endpoint relies on the
	// subsequent user unlock to requeue it; the transfer shim has no return-path
	// retransmission with which to recover from a missed handoff.
	tunTcpInboundBurstPacketCount = 16
	tunTcpInboundShardCount       = 32
)

// tunTcpInboundShard bounds one set of TCP flow handoffs without serializing
// unrelated flows. Its fixed arrays make memory independent of flow churn.
type tunTcpInboundShard struct {
	writeLock     sync.Mutex
	packetCount   uint32
	endpointIds   [tunTcpInboundBurstPacketCount]stack.TransportEndpointID
	endpointCount int
}

// tcpInboundFlow parses the endpoint identity and stable shard of a complete,
// unfragmented IPv4 or IPv6 TCP packet. A v6 packet whose next header is an
// extension header is not a flow here: the in-process NAT writes plain
// headers, so such a packet is not one whose finite-burst handoff this shard
// machinery exists to protect.
func tcpInboundFlow(packet []byte) (stack.TransportEndpointID, int, bool) {
	if len(packet) < header.IPv4MinimumSize {
		return stack.TransportEndpointID{}, 0, false
	}
	var transport []byte
	var endpointId stack.TransportEndpointID
	var flowHash uint32
	switch packet[0] >> 4 {
	case 4:
		if packet[9] != uint8(header.TCPProtocolNumber) {
			return stack.TransportEndpointID{}, 0, false
		}
		ipHeaderByteCount := int(packet[0]&0x0f) * 4
		if ipHeaderByteCount < header.IPv4MinimumSize ||
			len(packet) < ipHeaderByteCount+header.TCPMinimumSize ||
			binary.BigEndian.Uint16(packet[6:8])&0x1fff != 0 {
			return stack.TransportEndpointID{}, 0, false
		}
		transport = packet[ipHeaderByteCount:]
		endpointId.LocalAddress = tcpip.AddrFrom4Slice(packet[16:20])
		endpointId.RemoteAddress = tcpip.AddrFrom4Slice(packet[12:16])
		flowHash = binary.BigEndian.Uint32(packet[12:16]) ^ binary.BigEndian.Uint32(packet[16:20])
	case 6:
		if len(packet) < header.IPv6MinimumSize+header.TCPMinimumSize ||
			packet[6] != uint8(header.TCPProtocolNumber) {
			return stack.TransportEndpointID{}, 0, false
		}
		transport = packet[header.IPv6MinimumSize:]
		endpointId.LocalAddress = tcpip.AddrFrom16Slice(packet[24:40])
		endpointId.RemoteAddress = tcpip.AddrFrom16Slice(packet[8:24])
		for offset := 8; offset < 40; offset += 4 {
			flowHash ^= binary.BigEndian.Uint32(packet[offset : offset+4])
		}
	default:
		return stack.TransportEndpointID{}, 0, false
	}
	localPort := binary.BigEndian.Uint16(transport[2:4])
	remotePort := binary.BigEndian.Uint16(transport[0:2])
	endpointId.LocalPort = localPort
	endpointId.RemotePort = remotePort
	flowHash ^= uint32(localPort)<<16 | uint32(remotePort)
	flowHash ^= flowHash >> 16
	return endpointId, int(flowHash & (tunTcpInboundShardCount - 1)), true
}

// tcpInboundNetworkProtocol is the network protocol an inbound flow's
// endpoint was registered under, read off the endpoint id's address width.
func tcpInboundNetworkProtocol(endpointId stack.TransportEndpointID) tcpip.NetworkProtocolNumber {
	if endpointId.LocalAddress.Len() == header.IPv6AddressSize {
		return ipv6.ProtocolNumber
	}
	return ipv4.ProtocolNumber
}

// addTcpInboundEndpointWithLock records an endpoint once in the current
// bounded burst. The shard write lock must be held.
func (self *Tun) addTcpInboundEndpointWithLock(shard *tunTcpInboundShard, endpointId stack.TransportEndpointID) {
	for endpointIndex := 0; endpointIndex < shard.endpointCount; endpointIndex += 1 {
		if shard.endpointIds[endpointIndex] == endpointId {
			return
		}
	}
	shard.endpointIds[shard.endpointCount] = endpointId
	shard.endpointCount += 1
}

// advanceTcpInboundShardWithLock records one injection and reports when its
// shard needs an endpoint handoff. The shard write lock must be held.
func (self *Tun) advanceTcpInboundShardWithLock(shard *tunTcpInboundShard, endpointId stack.TransportEndpointID) bool {
	self.addTcpInboundEndpointWithLock(shard, endpointId)
	shard.packetCount += 1
	if shard.packetCount < tunTcpInboundBurstPacketCount {
		return false
	}
	shard.packetCount = 0
	return true
}

// synchronizeTcpInboundProcessorsWithLock performs gVisor's documented user
// unlock handoff for every endpoint touched in the burst. The shard write lock
// remains held so the next burst cannot overtake the handoff. Endpoint state
// is cleared independently from packetCount: individual finite callbacks must
// retain their shared cadence until one of them performs the scheduler yield.
func (self *Tun) synchronizeTcpInboundProcessorsWithLock(shard *tunTcpInboundShard) {
	for endpointIndex := 0; endpointIndex < shard.endpointCount; endpointIndex += 1 {
		endpointId := shard.endpointIds[endpointIndex]
		stackEndpoint := self.stack.FindTransportEndpoint(
			tcpInboundNetworkProtocol(endpointId),
			tcp.ProtocolNumber,
			endpointId,
			self.nicId,
		)
		if endpoint, ok := stackEndpoint.(*tcp.Endpoint); ok {
			endpoint.LockUser()
			endpoint.UnlockUser()
		}
	}
	shard.endpointCount = 0
}

// tunLinkEndpoint converts channel.Endpoint's silent bounded-queue drop into
// bounded backpressure. The user-NAT TCP bridge is intentionally lossless and
// does not retransmit its return path, so dropping one ACK here can otherwise
// strand a flow forever at its advertised receive window.
type tunLinkEndpoint struct {
	*channel.Endpoint
	ctx   context.Context
	space chan struct{}
	// waitTimeout bounds how long a netstack writer waits for outbound queue
	// space before the rest of its batch is dropped (counted in dropCount).
	// The goroutine that drains this queue can itself be inside an inbound
	// injection (SendPacket -> receive callback -> Tun.Write -> gVisor reply),
	// so an unbounded wait is a self-deadlock. Non-positive keeps the
	// unbounded backpressure.
	waitTimeout time.Duration
	dropCount   atomic.Uint64

	// dispatcher is the NIC's network dispatcher, captured at Attach so
	// WriteBatch can deliver GRO-coalesced packets through the same path
	// InjectInbound uses.
	dispatcherMu sync.RWMutex
	dispatcher   stack.NetworkDispatcher
}

func (self *tunLinkEndpoint) Attach(dispatcher stack.NetworkDispatcher) {
	self.dispatcherMu.Lock()
	self.dispatcher = dispatcher
	self.dispatcherMu.Unlock()
	self.Endpoint.Attach(dispatcher)
}

func (self *tunLinkEndpoint) networkDispatcher() stack.NetworkDispatcher {
	self.dispatcherMu.RLock()
	defer self.dispatcherMu.RUnlock()
	return self.dispatcher
}

func newTunLinkEndpoint(ctx context.Context, size int, mtu uint32, linkAddr tcpip.LinkAddress, waitTimeout time.Duration) *tunLinkEndpoint {
	endpoint := channel.New(size, mtu, linkAddr)
	// Inbound packets originate from the in-process user NAT over an
	// authenticated tunnel, so ip/tcp checksum validation here is redundant
	// cost. It is also REQUIRED for GRO delivery (see Tun.WriteBatch): gvisor's
	// GRO extends the leading packet's ip TotalLength on merge without
	// recomputing the ip header checksum — its deployments assume RX checksum
	// offload — and without this capability the ipv4 layer drops every merged
	// packet as checksum-invalid.
	endpoint.LinkEPCapabilities |= stack.CapabilityRXChecksumOffload
	return &tunLinkEndpoint{
		Endpoint:    endpoint,
		ctx:         ctx,
		space:       make(chan struct{}, 1),
		waitTimeout: waitTimeout,
	}
}

func (self *tunLinkEndpoint) notifySpace() {
	select {
	case self.space <- struct{}{}:
	default:
	}
}

func (self *tunLinkEndpoint) Read() *stack.PacketBuffer {
	packet := self.Endpoint.Read()
	if packet != nil {
		self.notifySpace()
	}
	return packet
}

func (self *tunLinkEndpoint) ReadContext(ctx context.Context) *stack.PacketBuffer {
	packet := self.Endpoint.ReadContext(ctx)
	if packet != nil {
		self.notifySpace()
	}
	return packet
}

func (self *tunLinkEndpoint) WritePackets(packets stack.PacketBufferList) (int, tcpip.Error) {
	packetSlice := packets.AsSlice()
	written := 0
	remaining := packets
	waitedForSpace := false
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		n, err := self.Endpoint.WritePackets(remaining)
		written += n
		if 0 < n && waitedForSpace {
			// Batch reads coalesce their space notifications into one token.
			// Pass it on after making progress so other parked writers can
			// use the remaining slots without waiting for another read.
			self.notifySpace()
		}
		if err != nil || written == len(packetSlice) {
			return written, err
		}
		waitedForSpace = true

		// A partial batch is unusual (gVisor normally passes one packet), so
		// construct its suffix only on queue saturation. A completely rejected
		// one-packet write reuses the original list without allocating.
		if 0 < n {
			remaining = stack.PacketBufferList{}
			for _, packet := range packetSlice[written:] {
				remaining.PushBack(packet)
			}
		}

		if self.waitTimeout <= 0 {
			select {
			case <-self.ctx.Done():
				return written, &tcpip.ErrClosedForSend{}
			case <-self.space:
			}
			continue
		}
		if timer == nil {
			timer = time.NewTimer(self.waitTimeout)
		}
		select {
		case <-self.ctx.Done():
			return written, &tcpip.ErrClosedForSend{}
		case <-self.space:
		case <-timer.C:
			// Drop the rest like a saturated NIC queue would. The caller keeps
			// its own packet references, so nothing is released here; TCP
			// recovers by retransmission once the queue drains.
			self.dropCount.Add(uint64(len(packetSlice) - written))
			return written, nil
		}
	}
}

func CreateTunWithDefaults(ctx context.Context) (*Tun, error) {
	return CreateTun(ctx, DefaultTunSettings())
}

func CreateTun(ctx context.Context, settings *TunSettings) (*Tun, error) {
	return CreateTunWithResolver(ctx, settings, nil)
}

func CreateTunWithResolver(ctx context.Context, settings *TunSettings, dnsResolverSettings *DnsResolverSettings) (*Tun, error) {
	cancelCtx, cancel := context.WithCancel(ctx)

	nicIdAllocator := defaultNicIdAllocator
	localIpv4AddressAllocator := defaultLocalIpv4AddressAllocator()
	localIpv6AddressAllocator := defaultLocalIpv6AddressAllocator()

	localIpv4Address, ok := localIpv4AddressAllocator.TakeAddr()
	if !ok {
		cancel()
		return nil, fmt.Errorf("No more local addresses")
	}

	// IPv6 rides only a link wide enough for it (see the file comment)
	ipv6Enabled := tunIpv6MinimumMtu <= settings.Mtu
	var localIpv6Address netip.Addr
	if ipv6Enabled {
		localIpv6Address, ok = localIpv6AddressAllocator.TakeAddr()
		if !ok {
			localIpv4AddressAllocator.ReturnAddr(localIpv4Address)
			cancel()
			return nil, fmt.Errorf("No more local ipv6 addresses")
		}
	}

	nicId := nicIdAllocator.TakeNicId()

	// each Tun owns a private gVisor stack, destroyed on Close() so all of its
	// endpoints are reclaimed. (There is no shared stack: it could not reclaim a
	// closed Tun's connection endpoints, leaking them under Tun churn.)
	tunStackInstance := newTunStack(
		settings.TcpReceiveBuffer,
		settings.TcpSendBuffer,
		settings.TcpMaxRto,
		settings.TcpMinRto,
	)

	// v4 first: consumers that predate dual stack read the tun's address
	// from the head of this list
	localAddresses := []netip.Addr{
		localIpv4Address,
	}
	if ipv6Enabled {
		localAddresses = append(localAddresses, localIpv6Address)
	}

	ep := newTunLinkEndpoint(
		cancelCtx,
		settings.ChannelSize,
		uint32(settings.Mtu),
		tcpip.LinkAddress(fmt.Sprintf("%x", nicId)),
		settings.OutboundQueueWaitTimeout,
	)

	releaseOnError := func() {
		ep.Close()
		nicIdAllocator.ReturnNicId(nicId)
		for _, addr := range localAddresses {
			if addr.Is4() {
				localIpv4AddressAllocator.ReturnAddr(addr)
			} else {
				localIpv6AddressAllocator.ReturnAddr(addr)
			}
		}
		cancel()
	}

	tun := &Tun{
		ctx:                       cancelCtx,
		cancel:                    cancel,
		log:                       loggerOrDefault(settings.Log),
		settings:                  settings,
		ep:                        ep,
		stack:                     tunStackInstance,
		nicId:                     nicId,
		nicIdAllocator:            nicIdAllocator,
		localAddresses:            localAddresses,
		localIpv4AddressAllocator: localIpv4AddressAllocator,
		localIpv6AddressAllocator: localIpv6AddressAllocator,
		ipv6Enabled:               ipv6Enabled,
	}

	tun.dohResolver.Store(tun.buildDohCache(dnsResolverSettings, settings.DohRequestTimeout))

	if tcpipErr := tun.stack.CreateNIC(nicId, ep); tcpipErr != nil {
		releaseOnError()
		return nil, fmt.Errorf("Could not create nic err=%s", tcpipErr)
	}

	for _, ip := range localAddresses {
		var protoNumber tcpip.NetworkProtocolNumber
		if ip.Is4() {
			protoNumber = ipv4.ProtocolNumber
		} else if ip.Is6() {
			protoNumber = ipv6.ProtocolNumber
		}
		protoAddr := tcpip.ProtocolAddress{
			Protocol:          protoNumber,
			AddressWithPrefix: tcpip.AddrFromSlice(ip.AsSlice()).WithPrefix(),
		}

		if tcpipErr := tun.stack.AddProtocolAddress(nicId, protoAddr, stack.AddressProperties{}); tcpipErr != nil {
			tun.stack.RemoveNIC(nicId)
			releaseOnError()
			return nil, fmt.Errorf("Could not create add nic address err=%s", tcpipErr)
		}
	}
	tun.stack.AddRoute(tcpip.Route{Destination: header.IPv4EmptySubnet, NIC: nicId})
	if ipv6Enabled {
		// ::/0 routes the same way as 0.0.0.0/0: everything leaves the nic
		tun.stack.AddRoute(tcpip.Route{Destination: header.IPv6EmptySubnet, NIC: nicId})
	} else if tun.log.V(1).Enabled() {
		tun.log.Infof("[tun]ipv6 disabled: mtu %d is below the ipv6 minimum %d\n", settings.Mtu, tunIpv6MinimumMtu)
	}

	tun.gro.Init(settings.TcpGro)

	return tun, nil
}

// Ipv6Enabled reports whether this tun carries IPv6: a v6 local address and
// default route exist, so v6 packets are accepted and v6 dials are made.
// False when the link mtu is below tunIpv6MinimumMtu.
func (self *Tun) Ipv6Enabled() bool {
	return self.ipv6Enabled
}

// injectNetworkProtocol maps a packet's version nibble to the network protocol
// it is injected under, or false for a version this tun does not carry.
func (self *Tun) injectNetworkProtocol(version byte) (tcpip.NetworkProtocolNumber, bool) {
	switch version {
	case 4:
		return header.IPv4ProtocolNumber, true
	case 6:
		return header.IPv6ProtocolNumber, self.ipv6Enabled
	default:
		return 0, false
	}
}

func (self *Tun) DohCache() *DohCache {
	return self.dohResolver.Load()
}

// buildDohCache constructs a DohCache resolving through this tun (remote paths dial via
// the tun; local paths use the host), with the given resolver settings (nil = default).
func (self *Tun) buildDohCache(dnsResolverSettings *DnsResolverSettings, requestTimeout time.Duration) *DohCache {
	var dohSettings *DohSettings
	if self.settings.DohSettings != nil {
		// copy so the overlays below don't mutate the caller's base
		copied := *self.settings.DohSettings
		dohSettings = &copied
	} else {
		dohSettings = DefaultDohSettings()
	}
	dohSettings.ConnectSettings.Log = self.log
	dohSettings.RequestTimeout = requestTimeout
	if dohSettings.RequestTimeout <= 0 {
		dohSettings.RequestTimeout = 60 * time.Second
	}
	dohSettings.TlsTimeout = 30 * time.Second
	dohSettings.DialContextSettings = &DialContextSettings{
		DialContext: self.DialContext,
	}
	if dnsResolverSettings != nil {
		dohSettings.DnsResolverSettings = dnsResolverSettings
	}
	return NewDohCache(dohSettings)
}

// SetDnsResolverSettings rebuilds the tun's DohCache with new resolver settings and DoH request
// timeout, taking effect for subsequent queries. Retiring the prior cache cancels its in-flight
// requests so no old-path dial or pooled connection survives the swap. Safe to call concurrently
// with DohCache()/Query.
func (self *Tun) SetDnsResolverSettings(dnsResolverSettings *DnsResolverSettings, requestTimeout time.Duration) {
	if replaced := self.dohResolver.Swap(self.buildDohCache(dnsResolverSettings, requestTimeout)); replaced != nil {
		// release the replaced cache's pooled connections (and their endpoints on this
		// tun's stack) now instead of holding both generations until the idle timeout
		replaced.Close()
	}
}

// LocalAddresses returns the addresses assigned to the internal stack's NIC
// (reserved from the shared local-address pool). Callers must not mutate it.
func (self *Tun) LocalAddresses() []netip.Addr {
	return self.localAddresses
}

func (self *Tun) Read() ([]byte, error) {
	// read directly from the gvisor endpoint's outbound queue. that queue is
	// itself a buffered FIFO (size `ChannelSize`) that drops on overflow, so it
	// is the sequence buffer. ReadContext blocks until a packet is available or
	// the ctx is canceled.
	pkt := self.ep.ReadContext(self.ctx)
	if pkt == nil {
		return nil, fmt.Errorf("Done")
	}
	packet := messagePoolCopyPacketBuffer(pkt)
	pkt.DecRef()
	return packet, nil
}

// messagePoolCopyPacketBuffer copies a packet buffer's bytes into a pooled message
// with a single copy and no intermediate allocation (ToView would deep-copy into an
// intermediate view first, on every packet the stack emits).
func messagePoolCopyPacketBuffer(pkt *stack.PacketBuffer) []byte {
	packet := MessagePoolGet(pkt.Size())
	vl, offset := pkt.AsViewList()
	i := 0
	for v := vl.Front(); v != nil; v = v.Next() {
		s := v.AsSlice()
		if 0 < offset {
			if len(s) <= offset {
				offset -= len(s)
				continue
			}
			s = s[offset:]
			offset = 0
		}
		i += copy(packet[i:], s)
	}
	return packet[:i]
}

// reads one or more packets, blocking until at least one is available.
// fills up to `len(packets)` entries and returns the count.
// a batch read wakes the reader once per burst instead of once per packet.
func (self *Tun) ReadBatch(packets [][]byte) (int, error) {
	if len(packets) == 0 {
		return 0, nil
	}
	// block for the first packet, then drain whatever else is already queued
	// without blocking. the gvisor endpoint queue is the sequence buffer (it
	// drops on overflow), and a single reader popping it preserves per-flow order.
	pkt := self.ep.ReadContext(self.ctx)
	if pkt == nil {
		return 0, fmt.Errorf("Done")
	}
	n := 0
	for pkt != nil {
		packets[n] = messagePoolCopyPacketBuffer(pkt)
		pkt.DecRef()
		n += 1
		if n >= len(packets) {
			break
		}
		pkt = self.ep.Read()
	}
	return n, nil
}

// safe to call from multiple goroutines
func (self *Tun) Write(packet []byte) (int, error) {
	return self.write(packet, nil)
}

// WriteBatch injects a batch of packets, coalescing the batch's tcp packets
// into super-segments (GRO) before delivery when TcpGro is enabled. This
// amortizes the per-segment dispatch, endpoint enqueue, processor wake, and
// ack generation that make per-packet Write the inbound throughput ceiling.
// Same-flow order is preserved: each touched tcp flow's shard write lock is
// held from the flow's first packet until the whole batch has been delivered,
// exactly like consecutive Write calls. Cross-flow order within a batch is
// not guaranteed (IP makes no such guarantee). The caller retains ownership
// of every packet slice.
func (self *Tun) WriteBatch(packets [][]byte) (int, error) {
	if len(packets) == 0 {
		return 0, nil
	}
	if len(packets) == 1 {
		return self.write(packets[0], nil)
	}

	dispatcher := self.ep.networkDispatcher()
	if dispatcher == nil {
		// not attached (only possible mid-construction): fall back per packet
		total := 0
		for _, packet := range packets {
			n, err := self.write(packet, nil)
			total += n
			if err != nil {
				return total, err
			}
		}
		return total, nil
	}

	self.groMu.Lock()
	defer self.groMu.Unlock()
	self.gro.Dispatcher = dispatcher

	// first-touch shard locks are held until the batch is fully delivered, so
	// a concurrent Write on the same flow cannot interleave mid-batch
	var lockedShards [tunTcpInboundShardCount]bool
	defer func() {
		for shardIndex := range lockedShards {
			if lockedShards[shardIndex] {
				self.tcpInboundShards[shardIndex].writeLock.Unlock()
			}
		}
	}()

	total := 0
	for _, packet := range packets {
		if len(packet) == 0 {
			continue
		}
		networkProtocol, ok := self.injectNetworkProtocol(packet[0] >> 4)
		if !ok {
			// a version this tun does not carry, matching write()
			continue
		}

		endpointId, shardIndex, tcpInbound := tcpInboundFlow(packet)
		var shard *tunTcpInboundShard
		if tcpInbound {
			shard = &self.tcpInboundShards[shardIndex]
			if !lockedShards[shardIndex] {
				shard.writeLock.Lock()
				lockedShards[shardIndex] = true
			}
		}

		pkb := stack.NewPacketBuffer(stack.PacketBufferOptions{
			Payload: buffer.MakeWithData(packet),
		})
		pkb.NetworkProtocolNumber = networkProtocol
		// the trusted in-process NAT computed these checksums; skipping GRO's
		// re-validation matches the link's CapabilityRXChecksumOffload
		pkb.RXChecksumValidated = true
		self.gro.Enqueue(pkb)
		pkb.DecRef()
		total += len(packet)

		if tcpInbound && self.advanceTcpInboundShardWithLock(shard, endpointId) {
			// the shard's burst is full: deliver everything queued so far so
			// the user-unlock handoff runs against enqueued segments
			// (write()'s inject-then-synchronize order), and so the shard's
			// bounded endpoint array cannot overflow mid-batch
			self.gro.Flush()
			self.synchronizeTcpInboundProcessorsWithLock(shard)
			// UnlockUser requeues protocol work but does not run it
			// synchronously. Yield while the shard remains gated so a new
			// producer cannot immediately overtake the awakened worker.
			runtime.Gosched()
		}
	}
	self.gro.Flush()

	// A finite response commonly ends with fewer than the 16 packets that
	// trigger the mid-batch cadence above. gVisor can have queued one of those
	// packets while a syscall owned the endpoint; without this final
	// LockUser/UnlockUser handoff there may be no later packet to wake its TCP
	// processor. The provider NAT has already consumed the upstream bytes, so
	// that missed tail is permanent rather than recoverable by retransmission.
	finalHandoff := false
	for shardIndex, locked := range lockedShards {
		if !locked {
			continue
		}
		shard := &self.tcpInboundShards[shardIndex]
		if shard.endpointCount == 0 {
			shard.packetCount = 0
			continue
		}
		self.synchronizeTcpInboundProcessorsWithLock(shard)
		shard.packetCount = 0
		finalHandoff = true
	}
	if finalHandoff {
		// UnlockUser queues processors asynchronously. Yield once for the whole
		// finite batch while every touched shard remains gated so the awakened
		// workers cannot be overtaken by the next producer callback.
		runtime.Gosched()
	}

	return total, nil
}

// write injects one packet and releases the creator's PacketBuffer reference
// after gVisor has taken any references it needs. onRelease is test
// instrumentation invoked when every gVisor reference has also been released.
func (self *Tun) write(packet []byte, onRelease func()) (int, error) {
	// defer MessagePoolReturn(packet)

	if len(packet) == 0 {
		return 0, nil
	}

	endpointId, shardIndex, tcpInbound := tcpInboundFlow(packet)
	var tcpInboundShard *tunTcpInboundShard
	yieldProcessor := false
	if tcpInbound {
		tcpInboundShard = &self.tcpInboundShards[shardIndex]
		tcpInboundShard.writeLock.Lock()
		yieldProcessor = self.advanceTcpInboundShardWithLock(tcpInboundShard, endpointId)
	}

	// copy the packet
	pkb := stack.NewPacketBuffer(stack.PacketBufferOptions{
		Payload:   buffer.MakeWithData(packet),
		OnRelease: onRelease,
	})
	// InjectInbound borrows the creator's reference. The stack takes its own
	// references for asynchronous work, exactly as gVisor's TUN and veth link
	// endpoints do. Without this release, every inbound packet and its copied
	// payload remain live for the process lifetime.

	networkProtocol, ok := self.injectNetworkProtocol(packet[0] >> 4)
	if !ok {
		pkb.DecRef()
		if tcpInbound {
			tcpInboundShard.writeLock.Unlock()
		}
		return 0, syscall.EAFNOSUPPORT
	}
	self.ep.InjectInbound(networkProtocol, pkb)
	pkb.DecRef()
	if tcpInbound {
		// A one-packet callback is itself a complete finite burst. Complete
		// gVisor's user-unlock handoff before returning: deferred execution
		// can strand a short H1/TLS response behind an unrelated shard or a
		// worker scheduling delay, and the provider NAT cannot retransmit it.
		self.synchronizeTcpInboundProcessorsWithLock(tcpInboundShard)
		if yieldProcessor {
			runtime.Gosched()
		}
		tcpInboundShard.writeLock.Unlock()
	}
	return len(packet), nil
}

func (self *Tun) convertToFullAddr(endpoint netip.AddrPort) (tcpip.FullAddress, tcpip.NetworkProtocolNumber) {
	var protoNumber tcpip.NetworkProtocolNumber
	if endpoint.Addr().Is4() {
		protoNumber = ipv4.ProtocolNumber
	} else {
		protoNumber = ipv6.ProtocolNumber
	}
	return tcpip.FullAddress{
		NIC:  self.nicId,
		Addr: tcpip.AddrFromSlice(endpoint.Addr().AsSlice()),
		Port: endpoint.Port(),
	}, protoNumber
}

// dialCtx joins one call's cancellation to the tun lifecycle without parking
// a goroutine for every unresolved dial. The returned cleanup owns both the
// callback registration and derived context and must be called by the caller.
func (self *Tun) dialCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	dialCtx, dialCancel := context.WithCancel(self.ctx)
	stopCallerCancel := context.AfterFunc(ctx, dialCancel)
	if ctx.Err() != nil {
		dialCancel()
	}
	return dialCtx, func() {
		stopCallerCancel()
		dialCancel()
	}
}

func (self *Tun) ListenTCP(addr *net.TCPAddr) (*gonet.TCPListener, error) {
	var addrPort netip.AddrPort
	if addr != nil {
		// Unmap: a 16-byte net.IP holding a v4 address must bind the v4
		// endpoint, not a v4-mapped v6 one
		ip, _ := netip.AddrFromSlice(addr.IP)
		addrPort = netip.AddrPortFrom(ip.Unmap(), uint16(addr.Port))
	}
	fa, pn := self.convertToFullAddr(addrPort)
	return gonet.ListenTCP(self.stack, fa, pn)
}

func (self *Tun) ListenUDP(laddr *net.UDPAddr) (*gonet.UDPConn, error) {
	var addrPort netip.AddrPort
	if laddr != nil {
		ip, _ := netip.AddrFromSlice(laddr.IP)
		addrPort = netip.AddrPortFrom(ip.Unmap(), uint16(laddr.Port))
	}
	lfa, pn := self.convertToFullAddr(addrPort)
	return self.dialUdp(&lfa, nil, pn)
}

// creates a udp endpoint with the tun buffer sizes applied.
// this mirrors `gonet.DialUDP` with sized endpoint buffers.
func (self *Tun) dialUdp(laddr *tcpip.FullAddress, raddr *tcpip.FullAddress, protoNumber tcpip.NetworkProtocolNumber) (*gonet.UDPConn, error) {
	wq := &waiter.Queue{}
	ep, tcpipErr := self.stack.NewEndpoint(udp.ProtocolNumber, protoNumber, wq)
	if tcpipErr != nil {
		return nil, fmt.Errorf("Could not create udp endpoint err=%s", tcpipErr)
	}

	ep.SocketOptions().SetReceiveBufferSize(int64(self.settings.UdpReceiveBufferByteCount), true)
	ep.SocketOptions().SetSendBufferSize(int64(self.settings.UdpSendBufferByteCount), true)

	if laddr != nil {
		if tcpipErr := ep.Bind(*laddr); tcpipErr != nil {
			ep.Close()
			return nil, fmt.Errorf("Could not bind udp endpoint err=%s", tcpipErr)
		}
	}

	conn := gonet.NewUDPConn(wq, ep)

	if raddr != nil {
		if tcpipErr := ep.Connect(*raddr); tcpipErr != nil {
			conn.Close()
			return nil, fmt.Errorf("Could not connect udp endpoint err=%s", tcpipErr)
		}
	}

	return conn, nil
}

// safe to call from multiple goroutines
func (self *Tun) DialContext(ctx context.Context, network string, address string) (net.Conn, error) {
	return raceTunDialContext(
		ctx,
		self.ctx,
		network,
		address,
		self.settings.DialRace,
		self.settings.DialRaceTimeout,
		self.settings.DialTimeout,
		self.dialContext,
	)
}

type tunDialResult struct {
	conn net.Conn
	err  error
}

func raceTunDialContext(
	ctx context.Context,
	tunCtx context.Context,
	network string,
	address string,
	dialRace int,
	dialRaceTimeout time.Duration,
	dialTimeout time.Duration,
	dialContext DialContextFunction,
) (net.Conn, error) {
	raceCtx, raceCancel := context.WithCancel(ctx)
	defer raceCancel()

	// One absolute deadline bounds the whole staggered race. The former shape
	// waited DialRaceTimeout after every launch and then waited
	// DialTimeout-DialRaceTimeout again; with the default two attempts a
	// nominal 30s dial could take 32s, and larger race counts grew without
	// bound.
	overallTimer := time.NewTimer(max(time.Duration(0), dialTimeout))
	defer overallTimer.Stop()

	attemptCount := max(1, dialRace)
	results := make(chan tunDialResult)
	launched := 0
	completed := 0
	var lastErr error
	launch := func() {
		launched++
		go HandleError(func() {
			conn, err := dialContext(raceCtx, network, address)
			select {
			case results <- tunDialResult{conn: conn, err: err}:
			case <-raceCtx.Done():
				if conn != nil {
					conn.Close()
				}
			}
		})
	}

	launch()
	if dialRaceTimeout <= 0 {
		for launched < attemptCount {
			launch()
		}
	}

	var staggerTimer *time.Timer
	var staggerC <-chan time.Time
	scheduleStagger := func() {
		if launched >= attemptCount || dialRaceTimeout <= 0 {
			staggerC = nil
			return
		}
		if staggerTimer == nil {
			staggerTimer = time.NewTimer(dialRaceTimeout)
		} else {
			staggerTimer.Reset(dialRaceTimeout)
		}
		staggerC = staggerTimer.C
	}
	stopStagger := func() {
		if staggerTimer != nil {
			staggerTimer.Stop()
		}
		staggerC = nil
	}
	defer stopStagger()
	scheduleStagger()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-tunCtx.Done():
			return nil, tunCtx.Err()
		case <-raceCtx.Done():
			return nil, raceCtx.Err()
		case <-overallTimer.C:
			return nil, os.ErrDeadlineExceeded
		case <-staggerC:
			launch()
			scheduleStagger()
		case result := <-results:
			if result.err == nil && result.conn != nil {
				// The result channel is deliberately unbuffered. Exactly one
				// successful connection transfers ownership to this caller;
				// after return, raceCancel makes every other successful dial
				// close instead of leaving it queued with no receiver.
				return result.conn, nil
			}
			if result.conn != nil {
				result.conn.Close()
			}
			if result.err != nil {
				lastErr = result.err
			} else {
				lastErr = errors.New("dial returned no connection")
			}
			completed++
			if completed == attemptCount {
				return nil, lastErr
			}

			// A definitive failure is a stronger signal than the stagger:
			// replace it immediately rather than adding avoidable latency.
			if launched < attemptCount {
				stopStagger()
				launch()
				scheduleStagger()
			}
		}
	}
}

// dialContext is one attempt of the stream/datagram dial through this tun's
// stack (raceTunDialContext may run several). A name resolves through the
// tun's DoH cache for the families the network permits, A and AAAA
// concurrently, and a stream dial races the addresses v6-first with the
// package fallback delay (net_dial_race.go). A datagram dial cannot be raced
// (connect always succeeds) and takes the first v4 address, else the first
// address. Family-specific networks and literals are honored: a v6 target on
// an IPv4-only tun is EAFNOSUPPORT.
//
// safe to call from multiple goroutines
func (self *Tun) dialContext(ctx context.Context, network string, address string) (net.Conn, error) {
	var stream bool
	switch network {
	case "tcp", "tcp4", "tcp6":
		stream = true
	case "udp", "udp4", "udp6":
		stream = false
	default:
		return nil, fmt.Errorf("Unsupported network %s", network)
	}
	if strings.HasSuffix(network, "6") && !self.ipv6Enabled {
		return nil, syscall.EAFNOSUPPORT
	}

	host, portStr, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, err
	}
	if port < 0 || 65535 < port {
		return nil, fmt.Errorf("invalid port %q", portStr)
	}

	dialCtx, dialCtxCancel := self.dialCtx(ctx)
	defer dialCtxCancel()

	var addrs []netip.Addr
	if literal, literalErr := netip.ParseAddr(host); literalErr == nil {
		// address is ip:port: no resolution, and the family is settled
		literal = literal.Unmap()
		if literal.Is6() && !self.ipv6Enabled {
			return nil, syscall.EAFNOSUPPORT
		}
		if strings.HasSuffix(network, "4") && !literal.Is4() || strings.HasSuffix(network, "6") && !literal.Is6() {
			return nil, syscall.EAFNOSUPPORT
		}
		addrs = []netip.Addr{literal}
	} else {
		resolveNetwork := network
		if !self.ipv6Enabled {
			// an IPv4-only tun must not resolve an address it cannot dial
			resolveNetwork = strings.TrimRight(network, "46") + "4"
		}
		addrs, err = resolveDohDialAddrs(dialCtx, self.DohCache(), resolveNetwork, host)
		if self.log.V(1).Enabled() {
			self.log.Infof("[tun]query doh (%s) found %v err=%v\n", host, addrs, err)
		}
		if err != nil {
			return nil, fmt.Errorf("Could not resolve %s: %w", address, err)
		}
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("Could not resolve %s", address)
	}

	if stream {
		return dialAddrsRace(dialCtx, addrs, DefaultDialFallbackDelay, func(ctx context.Context, addr netip.Addr) (net.Conn, error) {
			return self.dialTcpAddr(ctx, host, netip.AddrPortFrom(addr, uint16(port)))
		})
	}
	return self.dialUdpAddr(host, netip.AddrPortFrom(udpDialAddr(addrs), uint16(port)))
}

// udpDialAddr picks the one address a datagram dial uses: the first v4
// address when there is one, since nothing can prove a v6 path before the
// first reply, else the first address.
func udpDialAddr(addrs []netip.Addr) netip.Addr {
	for _, addr := range addrs {
		if addr.Is4() {
			return addr
		}
	}
	return addrs[0]
}

// A stream connection through the stack that keeps its endpoint, so a test
// or a measurement can read the stack's view of the connection (congestion
// window, slow start threshold, smoothed round trip, retransmission timeout)
// without an accessor the gonet adapter does not provide.
type TunTcpConn struct {
	*gonet.TCPConn
	endpoint tcpip.Endpoint
}

// TcpInfo reads the stack's TCP info for this connection.
func (self *TunTcpConn) TcpInfo() (tcpip.TCPInfoOption, error) {
	var info tcpip.TCPInfoOption
	if tcpipErr := self.endpoint.GetSockOpt(&info); tcpipErr != nil {
		return tcpip.TCPInfoOption{}, fmt.Errorf("Could not read tcp info err=%s", tcpipErr)
	}
	return info, nil
}

// creates a tcp endpoint and connects it. This mirrors
// `gonet.DialContextTCP`, which does not expose the endpoint it creates.
func (self *Tun) dialTcp(
	ctx context.Context,
	remoteAddr tcpip.FullAddress,
	protoNumber tcpip.NetworkProtocolNumber,
) (*TunTcpConn, error) {
	wq := &waiter.Queue{}
	ep, tcpipErr := self.stack.NewEndpoint(tcp.ProtocolNumber, protoNumber, wq)
	if tcpipErr != nil {
		return nil, fmt.Errorf("Could not create tcp endpoint err=%s", tcpipErr)
	}
	// registered before connect, which always returns before completing
	waitEntry, notify := waiter.NewChannelEntry(waiter.WritableEvents)
	wq.EventRegister(&waitEntry)
	defer wq.EventUnregister(&waitEntry)

	select {
	case <-ctx.Done():
		ep.Close()
		return nil, ctx.Err()
	default:
	}
	tcpipErr = ep.Connect(remoteAddr)
	if _, started := tcpipErr.(*tcpip.ErrConnectStarted); started {
		select {
		case <-ctx.Done():
			ep.Close()
			return nil, ctx.Err()
		case <-notify:
		}
		tcpipErr = ep.LastError()
	}
	if tcpipErr != nil {
		ep.Close()
		return nil, &net.OpError{
			Op:   "connect",
			Net:  "tcp",
			Addr: &net.TCPAddr{IP: net.IP(remoteAddr.Addr.AsSlice()), Port: int(remoteAddr.Port)},
			Err:  fmt.Errorf("%s", tcpipErr),
		}
	}
	return &TunTcpConn{
		TCPConn:  gonet.NewTCPConn(wq, ep),
		endpoint: ep,
	}, nil
}

// dialTcpAddr is one stream connect through the stack to a resolved address.
func (self *Tun) dialTcpAddr(ctx context.Context, host string, addrPort netip.AddrPort) (net.Conn, error) {
	fa, pn := self.convertToFullAddr(addrPort)
	conn, err := self.dialTcp(ctx, fa, pn)
	if err == nil {
		if self.log.V(1).Enabled() {
			self.log.Infof("[tun]tcp connect (%s)->%s success\n", host, addrPort)
		}
		return conn, nil
	}
	if self.log.V(1).Enabled() {
		self.log.Infof("[tun]tcp connect (%s)->%s err = %s\n", host, addrPort, err)
	}
	return nil, err
}

// dialUdpAddr is one datagram connect through the stack to a resolved address.
func (self *Tun) dialUdpAddr(host string, addrPort netip.AddrPort) (net.Conn, error) {
	fa, pn := self.convertToFullAddr(addrPort)
	conn, err := self.dialUdp(nil, &fa, pn)
	if err == nil {
		if self.log.V(1).Enabled() {
			self.log.Infof("[tun]udp connect (%s)->%s success\n", host, addrPort)
		}
		return conn, nil
	}
	if self.log.V(1).Enabled() {
		self.log.Infof("[tun]udp connect (%s)->%s err = %s\n", host, addrPort, err)
	}
	return nil, err
}

func (self *Tun) Dial(network, address string) (net.Conn, error) {
	return self.DialContext(context.Background(), network, address)
}

func (self *Tun) Close() error {
	self.closeOnce.Do(func() {
		self.cancel()
		// Cancel and join resolver HTTP requests/dials before destroying the
		// stack, so net/http cannot install a late h2/TLS connection after the
		// idle pool was closed.
		self.dohResolver.Load().Close()
		self.stack.RemoveNIC(self.nicId)
		// ep.Close() drains and DecRefs any packets still queued in the endpoint.
		self.ep.Close()
		self.nicIdAllocator.ReturnNicId(self.nicId)
		for _, addr := range self.localAddresses {
			if addr.Is4() {
				self.localIpv4AddressAllocator.ReturnAddr(addr)
			} else {
				self.localIpv6AddressAllocator.ReturnAddr(addr)
			}
		}
		// destroy this Tun's stack so its endpoints and background goroutines are released.
		// Do NOT stack.Wait() here: Close() can run under the device stateLock during a
		// reconfigure (SetDestination), and Wait() blocks until every stack goroutine halts —
		// one stuck goroutine would wedge the device, and DNS, until restart. The stack's
		// goroutines exit asynchronously after Close().
		self.stack.Close()
	})
	return nil
}

// Stats returns the gVisor stack statistics for this Tun's private stack.
// OutboundDropCount is the number of netstack packets dropped because the
// outbound queue stayed full past OutboundQueueWaitTimeout.
// TunLinkStatsSnapshot is the tun link's own counters for campaign records;
// the outbound drop count must read 0 wherever the bounded wait is only a
// guard (FLIGHTGATEFIX §13.4).
type TunLinkStatsSnapshot struct {
	OutboundDropCount uint64
}

// LinkStats reads the link endpoint counters without stopping the stack.
func (self *Tun) LinkStats() TunLinkStatsSnapshot {
	return TunLinkStatsSnapshot{OutboundDropCount: self.OutboundDropCount()}
}

func (self *Tun) OutboundDropCount() uint64 {
	return self.ep.dropCount.Load()
}

func (self *Tun) Stats() tcpip.Stats {
	return self.stack.Stats()
}
