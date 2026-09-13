// The libp2p transport that carries the mesh over extenders (EXTENDER.md D2).
//
// It replaces the tcp transport for `/ip4|ip6/<ip>/tcp/<port>` addresses. A
// dial is the ordinary extender dial of connect root with `Service` gossip: the
// outer tls is fronted with a spoof name and the extender's leaf is verified
// against the identity key from its signed record (B3), so the byte stream that
// comes back is already known to come from the key the directory names. The
// libp2p upgrader then runs noise and yamux over it and authenticates the peer
// id, which is derived from that same key -- two independent proofs of the same
// identity, and a mismatch between them fails the dial.
//
// A listen exists only on an extender, where it is the in-process listener the
// extender hands its gossip streams to. A member app has no listener at all and
// is outbound only (D3).
//
// The transport is safe for concurrent use.

package gossip

import (
	"bytes"
	"context"
	"fmt"
	mathrand "math/rand"
	"net/netip"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/transport"

	ma "github.com/multiformats/go-multiaddr"
	mafmt "github.com/multiformats/go-multiaddr-fmt"
	manet "github.com/multiformats/go-multiaddr/net"

	"github.com/urnetwork/connect"
)

// `/ip4|ip6/<ip>/tcp/<port>`, the same shape the tcp transport dials. The
// extender's own carrier ports come from its record, so the port is not fixed
// here.
var extenderDialMatcher = mafmt.And(mafmt.IP, mafmt.Base(ma.P_TCP))

type extenderTransportSettings struct {
	Log connect.Logger

	// The directory that names the identity key of an address (E1). A dial to
	// an address it does not hold a key for is refused: without the key there
	// is nothing to verify the outer leaf against.
	Directory *connect.ExtenderDirectory
	// Dial configuration of the carrier.
	ConnectSettings *connect.ConnectSettings
	// The extender's in-process listener, set on extenders only (D2). Nil
	// refuses every Listen, which is what a member app is.
	Listener *InProcessListener

	// Supplied by libp2p when the transport is constructed.
	Upgrader        transport.Upgrader
	ResourceManager network.ResourceManager
}

type extenderTransport struct {
	log      connect.Logger
	settings *extenderTransportSettings
}

var _ transport.Transport = (*extenderTransport)(nil)

func newExtenderTransport(settings *extenderTransportSettings) (*extenderTransport, error) {
	if settings == nil || settings.Directory == nil {
		return nil, fmt.Errorf("the extender transport needs a directory")
	}
	copied := *settings
	if copied.ConnectSettings == nil {
		copied.ConnectSettings = connect.DefaultConnectSettings()
	}
	if copied.ResourceManager == nil {
		copied.ResourceManager = &network.NullResourceManager{}
	}
	return &extenderTransport{
		log:      loggerOrDefault(copied.Log),
		settings: &copied,
	}, nil
}

func (self *extenderTransport) CanDial(addr ma.Multiaddr) bool {
	return extenderDialMatcher.Matches(addr)
}

func (self *extenderTransport) Protocols() []int {
	return []int{ma.P_TCP}
}

func (self *extenderTransport) Proxy() bool {
	return false
}

func (self *extenderTransport) String() string {
	return "Extender"
}

// Dial opens the gossip service on the extender at `raddr` and upgrades the
// stream to `peerId`. The directory decides what key the address must present,
// and the peer id must be the mesh identity of that key, so a record that names
// an address cannot be used to reach some other peer at it.
func (self *extenderTransport) Dial(
	ctx context.Context,
	raddr ma.Multiaddr,
	peerId peer.ID,
) (transport.CapableConn, error) {
	ip, port, err := extenderDialArgs(raddr)
	if err != nil {
		return nil, err
	}
	publicKey := self.directoryPublicKey(ip)
	if len(publicKey) == 0 {
		return nil, fmt.Errorf("the directory holds no extender key for %s", ip)
	}
	addressPeerId, err := PeerIdForExtenderPublicKey(publicKey)
	if err != nil {
		return nil, err
	}
	if addressPeerId != peerId {
		return nil, fmt.Errorf(
			"the extender at %s is %s in the directory, not %s",
			ip,
			addressPeerId,
			peerId,
		)
	}

	connScope, err := self.settings.ResourceManager.OpenConnection(network.DirOutbound, true, raddr)
	if err != nil {
		return nil, err
	}
	capableConn, err := self.dialWithScope(ctx, raddr, ip, port, peerId, publicKey, connScope)
	if err != nil {
		connScope.Done()
		return nil, err
	}
	return capableConn, nil
}

func (self *extenderTransport) dialWithScope(
	ctx context.Context,
	raddr ma.Multiaddr,
	ip netip.Addr,
	port int,
	peerId peer.ID,
	publicKey []byte,
	connScope network.ConnManagementScope,
) (transport.CapableConn, error) {
	if err := connScope.SetPeer(peerId); err != nil {
		return nil, err
	}
	conn, response, err := connect.DialExtender(
		ctx,
		self.settings.ConnectSettings,
		extenderGossipConfig(ip, port, publicKey),
		&connect.ExtenderDial{
			Service: connect.ExtenderServiceGossip,
		},
	)
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			conn.Close()
		}
	}()
	// the outer leaf already proved possession of the key (B3); an extender
	// that answers with another key is misconfigured or is relaying, and the
	// noise handshake below would fail anyway
	if response != nil && 0 < len(response.PublicKey) && !bytes.Equal(response.PublicKey, publicKey) {
		return nil, fmt.Errorf("the extender at %s answered with another identity key", ip)
	}

	localMultiaddr := raddr
	if localAddr := conn.LocalAddr(); localAddr != nil {
		if converted, err := manet.FromNetAddr(localAddr); err == nil {
			localMultiaddr = converted
		}
	}
	capableConn, err := self.settings.Upgrader.Upgrade(
		ctx,
		self,
		newMaConn(conn, localMultiaddr, raddr),
		network.DirOutbound,
		peerId,
		connScope,
	)
	if err != nil {
		return nil, err
	}
	success = true
	return capableConn, nil
}

// Listen hands back the extender's in-process listener for one advertised
// address (D2). A node with no listener -- every member app -- refuses, which
// is what keeps a member outbound only.
func (self *extenderTransport) Listen(laddr ma.Multiaddr) (transport.Listener, error) {
	if self.settings.Listener == nil {
		return nil, fmt.Errorf("this node has no extender listener")
	}
	if !self.CanDial(laddr) {
		return nil, fmt.Errorf("the extender transport cannot listen on %s", laddr)
	}
	maListener := newInProcessMaListener(self.settings.Listener, laddr)
	return self.settings.Upgrader.UpgradeGatedMaListener(
		self,
		self.settings.Upgrader.GateMaListener(maListener),
	), nil
}

// The identity key the directory holds for one address, empty when the address
// is unknown or still unverified.
func (self *extenderTransport) directoryPublicKey(ip netip.Addr) []byte {
	for _, entry := range self.settings.Directory.Snapshot().Entries {
		if entry.Ip == ip && 0 < len(entry.PublicKey) {
			return entry.PublicKey
		}
	}
	return nil
}

// The ip and port of a dialable extender address.
func extenderDialArgs(addr ma.Multiaddr) (netip.Addr, int, error) {
	// the swarm strips /p2p before dialing, but an address built by hand may
	// still carry it
	baseAddr, _ := peer.SplitAddr(addr)
	if baseAddr == nil {
		baseAddr = addr
	}
	network, address, err := manet.DialArgs(baseAddr)
	if err != nil {
		return netip.Addr{}, 0, err
	}
	switch network {
	case "tcp", "tcp4", "tcp6":
	default:
		return netip.Addr{}, 0, fmt.Errorf("the extender transport cannot dial %s", network)
	}
	addrPort, err := netip.ParseAddrPort(address)
	if err != nil {
		return netip.Addr{}, 0, err
	}
	return addrPort.Addr().Unmap(), int(addrPort.Port()), nil
}

// The dial configuration of one gossip carrier (A10, E5). The outer name is one
// random spoof domain; with no bundled list the extender ip is presented, which
// puts no name in the ClientHello at all rather than naming the operator.
func extenderGossipConfig(ip netip.Addr, port int, publicKey []byte) *connect.ExtenderConfig {
	profile := connect.ExtenderProfile{
		ConnectMode: connect.ExtenderConnectModeTcpTls,
		ServerName:  ip.String(),
		Port:        port,
	}
	if spoofDomains := connect.SpoofDomains(); 0 < len(spoofDomains) {
		profile.ServerName = spoofDomains[mathrand.Intn(len(spoofDomains))]
	}
	return &connect.ExtenderConfig{
		Profile:   profile,
		Ip:        ip,
		PublicKey: publicKey,
	}
}
