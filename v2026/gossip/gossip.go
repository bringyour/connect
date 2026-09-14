// The gossip mesh that carries signed extender records (EXTENDER.md D1 to D6).
//
// This package holds everything that needs libp2p: the node (D1), the extender
// transport and its in-process listener (D2), the peering loop (D3) and the
// feed server (D4). connect root holds the records, the directory and the feed
// client and never imports this package, so an app that only takes the feed
// carries no libp2p at all.
//
// Identity is one ed25519 key everywhere (B1). A node's libp2p peer id is
// derived from that key, so the peer id of an extender is exactly what a client
// computes from the key in its signed record -- which is what lets a dial
// demand the peer it meant to reach without any other directory.
//
// The functions in this file are safe for concurrent use.

package gossip

import (
	"crypto/ed25519"
	"fmt"
	"net/netip"
	"net/url"
	"strconv"
	"strings"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	ma "github.com/multiformats/go-multiaddr"

	"github.com/urnetwork/connect/v2026"
)

// The gossipsub topic of one network space (D1). One topic per space keeps two
// spaces that share a root key from relaying each other's records.
func Topic(networkHost string) string {
	host := strings.ToLower(strings.TrimSuffix(strings.TrimSpace(networkHost), "."))
	return fmt.Sprintf("/ur/extender/%s/1", host)
}

// The libp2p private key of one extender identity seed (B1).
func privateKeyForSeed(seed []byte) (crypto.PrivKey, error) {
	privateKey, err := connect.ExtenderPrivateKeyFromSeed(seed)
	if err != nil {
		return nil, err
	}
	return crypto.UnmarshalEd25519PrivateKey(privateKey)
}

// PeerIdForExtenderPublicKey is the mesh identity of one extender identity key
// (D1, D2). A client that holds a signed record therefore knows the peer id it
// must meet at the other end of the dial, with no lookup of its own.
func PeerIdForExtenderPublicKey(publicKey []byte) (peer.ID, error) {
	if len(publicKey) != ed25519.PublicKeySize {
		return "", fmt.Errorf("extender public key must be %d bytes, got %d", ed25519.PublicKeySize, len(publicKey))
	}
	libp2pPublicKey, err := crypto.UnmarshalEd25519PublicKey(publicKey)
	if err != nil {
		return "", err
	}
	return peer.IDFromPublicKey(libp2pPublicKey)
}

// OperatorAddrsFromUrl converts the network space's resolved gossip url and the
// operator's peer id into the addresses a node dials (D1, F1). An empty peer id
// yields no address at all: without the identity there is nothing to
// authenticate the operator against, and D3 would rather have no operator link
// than an unauthenticated one.
//
// A url with a path -- the env secret form of a non-main environment -- has no
// websocket multiaddr, because a multiaddr carries no http path. Such a url is
// an error rather than a silently truncated dial.
func OperatorAddrsFromUrl(gossipUrl string, peerIdStr string) ([]ma.Multiaddr, error) {
	if strings.TrimSpace(peerIdStr) == "" {
		return nil, nil
	}
	peerId, err := peer.Decode(strings.TrimSpace(peerIdStr))
	if err != nil {
		return nil, err
	}
	baseAddr, err := websocketAddr(gossipUrl)
	if err != nil {
		return nil, err
	}
	p2pComponent, err := ma.NewComponent("p2p", peerId.String())
	if err != nil {
		return nil, err
	}
	return []ma.Multiaddr{baseAddr.Encapsulate(p2pComponent)}, nil
}

// The `/ws` or `/wss` multiaddr of one websocket url.
func websocketAddr(gossipUrl string) (ma.Multiaddr, error) {
	parsedUrl, err := url.Parse(strings.TrimSpace(gossipUrl))
	if err != nil {
		return nil, err
	}
	var wsProtocol string
	var defaultPort int
	switch strings.ToLower(parsedUrl.Scheme) {
	case "wss", "https":
		wsProtocol = "wss"
		defaultPort = 443
	case "ws", "http":
		wsProtocol = "ws"
		defaultPort = 80
	default:
		return nil, fmt.Errorf("gossip url scheme %q is not a websocket scheme", parsedUrl.Scheme)
	}
	if path := strings.Trim(parsedUrl.Path, "/"); path != "" {
		return nil, fmt.Errorf("gossip url %q carries a path, which a websocket multiaddr cannot", gossipUrl)
	}
	host := parsedUrl.Hostname()
	if host == "" {
		return nil, fmt.Errorf("gossip url %q carries no host", gossipUrl)
	}
	port := defaultPort
	if portStr := parsedUrl.Port(); portStr != "" {
		if port, err = strconv.Atoi(portStr); err != nil {
			return nil, err
		}
	}
	// `/dns` rather than `/dns4`: a dual stack or v6-only host must be able to
	// reach the operator on whichever family it has (IPV6.md A4)
	hostProtocol := "dns"
	if ip, err := netip.ParseAddr(host); err == nil {
		hostProtocol = "ip4"
		if ip.Unmap().Is6() {
			hostProtocol = "ip6"
		}
		host = ip.Unmap().String()
	}
	return ma.NewMultiaddr(fmt.Sprintf("/%s/%s/tcp/%d/%s", hostProtocol, host, port, wsProtocol))
}

// WebsocketListenAddrs are the listen addresses of a node that serves the
// websocket transport, which is the operator's shape (C6, D1). An empty ip
// listens on every interface of both families; a literal listens on that
// address alone. `secure` selects `/wss` for a node that terminates tls itself;
// the operator sits behind nginx and listens with plain `/ws`.
func WebsocketListenAddrs(ip string, port int, secure bool) ([]ma.Multiaddr, error) {
	wsProtocol := "ws"
	if secure {
		wsProtocol = "wss"
	}
	hostAddrs := []string{"/ip4/0.0.0.0", "/ip6/::"}
	if ip = strings.TrimSpace(ip); ip != "" {
		parsedIp, err := netip.ParseAddr(ip)
		if err != nil {
			return nil, err
		}
		hostProtocol := "ip4"
		if parsedIp.Unmap().Is6() {
			hostProtocol = "ip6"
		}
		hostAddrs = []string{fmt.Sprintf("/%s/%s", hostProtocol, parsedIp.Unmap())}
	}
	listenAddrs := []ma.Multiaddr{}
	for _, hostAddr := range hostAddrs {
		listenAddr, err := ma.NewMultiaddr(fmt.Sprintf("%s/tcp/%d/%s", hostAddr, port, wsProtocol))
		if err != nil {
			return nil, err
		}
		listenAddrs = append(listenAddrs, listenAddr)
	}
	return listenAddrs, nil
}

// ExtenderListenAddrs are the addresses an extender advertises for its own
// carrier (D2): one per activated family, at the extender's tcp port. The
// extender learns its public addresses from its activation result (G3); tests
// pass loopback.
func ExtenderListenAddrs(ips []netip.Addr, tcpPort int) ([]ma.Multiaddr, error) {
	listenAddrs := []ma.Multiaddr{}
	for _, ip := range ips {
		ip = ip.Unmap()
		if !ip.IsValid() {
			return nil, fmt.Errorf("extender listen address is not valid")
		}
		hostProtocol := "ip4"
		if ip.Is6() {
			hostProtocol = "ip6"
		}
		listenAddr, err := ma.NewMultiaddr(fmt.Sprintf("/%s/%s/tcp/%d", hostProtocol, ip, tcpPort))
		if err != nil {
			return nil, err
		}
		listenAddrs = append(listenAddrs, listenAddr)
	}
	return listenAddrs, nil
}
