// The address handling of the extender transport (EXTENDER.md D2).
//
// Nothing here opens a connection: the point of every case is that the
// transport decides before it dials, from the multiaddr and from the
// directory alone.

package gossip

import (
	"context"
	"fmt"
	"net/netip"
	"strings"
	"testing"

	ma "github.com/multiformats/go-multiaddr"

	"github.com/urnetwork/connect/v2026"
)

// A known but unverified address -- a dns bootstrap or a manual one, which
// carries no identity key -- is refused before any connection, exactly like an
// address the directory has never heard of (D2, E1).
func TestGossipTransportRefusesAnAddressWithNoKey(t *testing.T) {
	rootKey := newTestKey(t)
	extenderKey := newTestKey(t)
	directory := newTestDirectory(t, rootKey)
	extenderTransport := newTestTransport(t, directory, nil)

	ip := netip.MustParseAddr("192.0.2.11")
	if changed := directory.AddBootstrap(ip, connect.ExtenderSourceBootstrap); !changed {
		t.Fatal("the bootstrap address was not added")
	}
	// the address is known, and it is known without a key
	if state := snapshotState(directory.Snapshot(), ip); state != connect.ExtenderStateUnverified {
		t.Fatalf("the bootstrap address is %q, expected %q", state, connect.ExtenderStateUnverified)
	}
	if publicKey := extenderTransport.directoryPublicKey(ip); publicKey != nil {
		t.Fatalf("an unverified address carried the key %x", publicKey)
	}

	extenderPeerId, err := PeerIdForExtenderPublicKey(extenderKey.publicKey)
	if err != nil {
		t.Fatal(err)
	}
	addr, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/8443", ip))
	if err != nil {
		t.Fatal(err)
	}
	capableConn, err := extenderTransport.Dial(context.Background(), addr, extenderPeerId)
	if err == nil {
		capableConn.Close()
		t.Fatal("a dial to a known but unverified address succeeded")
	}
	if !strings.Contains(err.Error(), "holds no extender key") {
		t.Fatalf("the dial failed with %v, expected a missing key", err)
	}
}

// The dial arguments of one advertised address: a trailing `/p2p` is not part
// of the endpoint, only the tcp carrier is dialable, and a v4-mapped address
// is a v4 dial (D2).
func TestGossipTransportDialArgs(t *testing.T) {
	extenderPeerId, err := PeerIdForExtenderPublicKey(newTestKey(t).publicKey)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		addr      string
		expectIp  string
		expectErr string
	}{
		{addr: "/ip4/192.0.2.1/tcp/8443", expectIp: "192.0.2.1"},
		{addr: "/ip6/2001:db8::1/tcp/8443", expectIp: "2001:db8::1"},
		// the swarm strips /p2p before dialing, but an address built by hand
		// still carries it
		{
			addr:     fmt.Sprintf("/ip4/192.0.2.1/tcp/8443/p2p/%s", extenderPeerId),
			expectIp: "192.0.2.1",
		},
		{
			addr:     fmt.Sprintf("/ip6/2001:db8::1/tcp/8443/p2p/%s", extenderPeerId),
			expectIp: "2001:db8::1",
		},
		// a v4-mapped address is dialed as the v4 address it is
		{addr: "/ip6/::ffff:192.0.2.1/tcp/8443", expectIp: "192.0.2.1"},
		// the mesh rides the tcp carrier only
		{addr: "/ip4/192.0.2.1/udp/8443", expectErr: "the extender transport cannot dial udp4"},
		{addr: "/ip6/2001:db8::1/udp/8443", expectErr: "the extender transport cannot dial udp6"},
		{
			addr:      "/ip4/192.0.2.1/udp/8443/quic-v1",
			expectErr: "the extender transport cannot dial udp4",
		},
		// a name is not an endpoint this transport resolves
		{addr: "/dns/gossip.space.example/tcp/443/wss", expectErr: "gossip.space.example"},
	}
	for _, c := range cases {
		addr, err := ma.NewMultiaddr(c.addr)
		if err != nil {
			t.Errorf("%s: %v", c.addr, err)
			continue
		}
		ip, port, err := extenderDialArgs(addr)
		if c.expectErr != "" {
			if err == nil {
				t.Errorf("%s: expected an error, got %s %d", c.addr, ip, port)
				continue
			}
			if !strings.Contains(err.Error(), c.expectErr) {
				t.Errorf("%s: error = %v, expected %q", c.addr, err, c.expectErr)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", c.addr, err)
			continue
		}
		if ip.String() != c.expectIp {
			t.Errorf("%s: ip = %s, expected %s", c.addr, ip, c.expectIp)
		}
		if port != 8443 {
			t.Errorf("%s: port = %d, expected 8443", c.addr, port)
		}
	}
}

// An extender listens on its own ip tcp addresses and on nothing else, so a
// carrier the mesh does not ride cannot become a listen address (D2).
func TestGossipTransportListenRefusesANonTcpAddr(t *testing.T) {
	rootKey := newTestKey(t)
	listenAddrs, err := ExtenderListenAddrs(testAddrs(t, "192.0.2.1"), 8443)
	if err != nil {
		t.Fatal(err)
	}
	listenerSettings := DefaultInProcessListenerSettings()
	listenerSettings.ListenAddrs = listenAddrs
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	listener := NewInProcessListener(ctx, listenerSettings)
	t.Cleanup(listener.Close)
	extenderTransport := newTestTransport(t, newTestDirectory(t, rootKey), listener)

	for _, addrStr := range []string{
		"/ip4/192.0.2.1/udp/8443/quic-v1",
		"/ip4/192.0.2.1/tcp/8443/ws",
		"/dns/gossip.space.example/tcp/443/wss",
	} {
		addr, err := ma.NewMultiaddr(addrStr)
		if err != nil {
			t.Errorf("%s: %v", addrStr, err)
			continue
		}
		transportListener, err := extenderTransport.Listen(addr)
		if err == nil {
			transportListener.Close()
			t.Errorf("%s: the extender listened", addrStr)
			continue
		}
		expect := fmt.Sprintf("the extender transport cannot listen on %s", addr)
		if err.Error() != expect {
			t.Errorf("%s: error = %v, expected %q", addrStr, err, expect)
		}
	}
}
