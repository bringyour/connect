// The address conversions of EXTENDER.md D1 and D2.
//
// Everything here is a pure function of its arguments, so every case is a
// table entry. The addresses are RFC 5737 and RFC 3849 documentation
// addresses and `.example` names throughout.

package gossip

import (
	"net/netip"
	"testing"
)

// The operator url carries the scheme's default port and its websocket
// protocol, an address with no host is an error, no identity is no address at
// all, and a v4-mapped literal is unmapped to `/ip4` (D1, D3, C7).
func TestGossipOperatorAddrsFromUrlSchemes(t *testing.T) {
	operatorKey := newTestKey(t)
	operatorPeerId, err := PeerIdForExtenderPublicKey(operatorKey.publicKey)
	if err != nil {
		t.Fatal(err)
	}
	peerIdStr := operatorPeerId.String()

	cases := []struct {
		gossipUrl string
		peerId    string
		expect    string
		expectErr bool
	}{
		// https and http are the same two websocket protocols at the same two
		// default ports as wss and ws
		{
			gossipUrl: "https://gossip.space.example",
			peerId:    peerIdStr,
			expect:    "/dns/gossip.space.example/tcp/443/wss/p2p/" + peerIdStr,
		},
		{
			gossipUrl: "http://gossip.space.example",
			peerId:    peerIdStr,
			expect:    "/dns/gossip.space.example/tcp/80/ws/p2p/" + peerIdStr,
		},
		{
			gossipUrl: "https://gossip.space.example:8443",
			peerId:    peerIdStr,
			expect:    "/dns/gossip.space.example/tcp/8443/wss/p2p/" + peerIdStr,
		},
		// a v4-mapped literal is one address, and it is a v4 one
		{
			gossipUrl: "ws://[::ffff:192.0.2.7]:8443",
			peerId:    peerIdStr,
			expect:    "/ip4/192.0.2.7/tcp/8443/ws/p2p/" + peerIdStr,
		},
		// a peer id of nothing but whitespace is no identity, which is no
		// address rather than an error
		{gossipUrl: "wss://gossip.space.example", peerId: "   ", expect: ""},
		{gossipUrl: "wss://", peerId: peerIdStr, expectErr: true},
		{gossipUrl: "wss:///", peerId: peerIdStr, expectErr: true},
	}
	for _, c := range cases {
		operatorAddrs, err := OperatorAddrsFromUrl(c.gossipUrl, c.peerId)
		if c.expectErr {
			if err == nil {
				t.Errorf("%s %q: expected an error, got %v", c.gossipUrl, c.peerId, operatorAddrs)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s %q: %v", c.gossipUrl, c.peerId, err)
			continue
		}
		if c.expect == "" {
			if operatorAddrs != nil {
				t.Errorf("%s %q: expected no address, got %v", c.gossipUrl, c.peerId, operatorAddrs)
			}
			continue
		}
		if len(operatorAddrs) != 1 || operatorAddrs[0].String() != c.expect {
			t.Errorf("%s: address = %v, expected %s", c.gossipUrl, operatorAddrs, c.expect)
		}
	}
}

// A wildcard operator listen is exactly one address per family, a literal is
// exactly that family, and an address that is not an ip is an error (C6, D1).
func TestGossipWebsocketListenAddrs(t *testing.T) {
	cases := []struct {
		ip        string
		secure    bool
		expects   []string
		expectErr bool
	}{
		{
			ip:      "",
			expects: []string{"/ip4/0.0.0.0/tcp/9443/ws", "/ip6/::/tcp/9443/ws"},
		},
		{
			ip:      "   ",
			secure:  true,
			expects: []string{"/ip4/0.0.0.0/tcp/9443/wss", "/ip6/::/tcp/9443/wss"},
		},
		{ip: "2001:db8::1", expects: []string{"/ip6/2001:db8::1/tcp/9443/ws"}},
		{ip: "192.0.2.1", secure: true, expects: []string{"/ip4/192.0.2.1/tcp/9443/wss"}},
		// a v4-mapped literal is a v4 listen
		{ip: "::ffff:192.0.2.1", expects: []string{"/ip4/192.0.2.1/tcp/9443/ws"}},
		{ip: "gossip.space.example", expectErr: true},
		{ip: "not-an-ip", expectErr: true},
	}
	for _, c := range cases {
		listenAddrs, err := WebsocketListenAddrs(c.ip, 9443, c.secure)
		if c.expectErr {
			if err == nil {
				t.Errorf("%q: expected an error, got %v", c.ip, listenAddrs)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: %v", c.ip, err)
			continue
		}
		if len(listenAddrs) != len(c.expects) {
			t.Errorf("%q: listen addrs = %v, expected %v", c.ip, listenAddrs, c.expects)
			continue
		}
		for i, expect := range c.expects {
			if listenAddrs[i].String() != expect {
				t.Errorf("%q: listen addr = %s, expected %s", c.ip, listenAddrs[i], expect)
			}
		}
	}
}

// An extender advertises one address per activated family, unmaps a v4-mapped
// address, refuses an address that is not valid, and activating nothing is not
// an error (D2, G3).
func TestGossipExtenderListenAddrsPerFamily(t *testing.T) {
	cases := []struct {
		name      string
		ips       []netip.Addr
		expects   []string
		expectErr bool
	}{
		{name: "no family activated", ips: []netip.Addr{}, expects: []string{}},
		{name: "nil", ips: nil, expects: []string{}},
		{
			name:    "both families",
			ips:     testAddrs(t, "192.0.2.1", "2001:db8::1"),
			expects: []string{"/ip4/192.0.2.1/tcp/8443", "/ip6/2001:db8::1/tcp/8443"},
		},
		{
			name:    "v4 mapped",
			ips:     testAddrs(t, "::ffff:192.0.2.1"),
			expects: []string{"/ip4/192.0.2.1/tcp/8443"},
		},
		{name: "the zero address", ips: []netip.Addr{{}}, expectErr: true},
	}
	for _, c := range cases {
		listenAddrs, err := ExtenderListenAddrs(c.ips, 8443)
		if c.expectErr {
			if err == nil {
				t.Errorf("%s: expected an error, got %v", c.name, listenAddrs)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if listenAddrs == nil {
			t.Errorf("%s: listen addrs are nil, expected a slice", c.name)
			continue
		}
		if len(listenAddrs) != len(c.expects) {
			t.Errorf("%s: listen addrs = %v, expected %v", c.name, listenAddrs, c.expects)
			continue
		}
		for i, expect := range c.expects {
			if listenAddrs[i].String() != expect {
				t.Errorf("%s: listen addr = %s, expected %s", c.name, listenAddrs[i], expect)
			}
		}
	}
}
