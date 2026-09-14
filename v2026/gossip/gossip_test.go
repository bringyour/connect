// Unit tests of the pieces that do not need a mesh (EXTENDER.md D1, D2).
//
// Everything here is synthetic: `.example` names, RFC 5737 and RFC 3849
// documentation addresses, and keys generated in the test.

package gossip

import (
	"context"
	"crypto/ed25519"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	ma "github.com/multiformats/go-multiaddr"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/v2026"
	"github.com/urnetwork/connect/v2026/protocol"
)

// The network space of every record in these tests.
const testNetworkHost = "space.example"

// One ed25519 identity, kept as the seed the whole tree passes around.
type testKey struct {
	seed       []byte
	privateKey ed25519.PrivateKey
	publicKey  ed25519.PublicKey
}

func newTestKey(t *testing.T) *testKey {
	t.Helper()
	seed, err := connect.NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	privateKey, err := connect.ExtenderPrivateKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	return &testKey{
		seed:       seed,
		privateKey: privateKey,
		publicKey:  privateKey.Public().(ed25519.PublicKey),
	}
}

// One signed record for an address and tcp port. The ip version is derived
// from the address rather than fixed, so a v6 fixture publishes a v6 record.
func signTestRecord(
	t *testing.T,
	rootKey *testKey,
	extenderKey *testKey,
	ip string,
	tcpPort int,
	issueTime time.Time,
) *protocol.ExtenderRecord {
	t.Helper()
	parsedIp, err := netip.ParseAddr(ip)
	if err != nil {
		t.Fatal(err)
	}
	ipVersion := 4
	if parsedIp.Unmap().Is6() {
		ipVersion = 6
	}
	record, err := connect.SignExtenderRecord(rootKey.privateKey, &protocol.ExtenderRecordBody{
		PublicKey: extenderKey.publicKey,
		Addresses: []*protocol.ExtenderAddress{
			{
				Ip:        parsedIp.Unmap().String(),
				IpVersion: uint32(ipVersion),
				Carriers:  []string{connect.ExtenderCarrierTcp},
			},
		},
		TcpPort:      uint32(tcpPort),
		CountryCode:  "us",
		IssueTimeMs:  uint64(issueTime.UnixMilli()),
		ExpireTimeMs: uint64(issueTime.Add(14 * 24 * time.Hour).UnixMilli()),
		NetworkHost:  testNetworkHost,
	})
	if err != nil {
		t.Fatal(err)
	}
	return record
}

// One directory anchored on the test root key.
func newTestDirectory(t *testing.T, rootKey *testKey) *connect.ExtenderDirectory {
	t.Helper()
	settings := connect.DefaultExtenderDirectorySettings()
	settings.NetworkHosts = []string{testNetworkHost}
	ctx, cancel := context.WithCancel(context.Background())
	directory := connect.NewExtenderDirectory(ctx, settings)
	directory.SetRootKeys(connect.NewExtenderRootKeySet(rootKey.publicKey))
	t.Cleanup(func() {
		directory.Close()
		cancel()
	})
	return directory
}

// The topic separates network spaces and normalizes the host (D1).
func TestGossipTopicIsPerNetworkHost(t *testing.T) {
	cases := []struct {
		networkHost string
		expect      string
	}{
		{networkHost: "space.example", expect: "/ur/extender/space.example/1"},
		{networkHost: "SPACE.example.", expect: "/ur/extender/space.example/1"},
		{networkHost: " other.example ", expect: "/ur/extender/other.example/1"},
	}
	for _, c := range cases {
		if topic := Topic(c.networkHost); topic != c.expect {
			t.Errorf("topic(%q) = %q, expected %q", c.networkHost, topic, c.expect)
		}
	}
}

// The operator address is the gossip url plus its identity, and nothing at all
// without the identity (D3, C7).
func TestGossipOperatorAddrsFromUrl(t *testing.T) {
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
		{
			gossipUrl: "wss://gossip.space.example",
			peerId:    peerIdStr,
			expect:    "/dns/gossip.space.example/tcp/443/wss/p2p/" + peerIdStr,
		},
		{
			gossipUrl: "ws://gossip.space.example",
			peerId:    peerIdStr,
			expect:    "/dns/gossip.space.example/tcp/80/ws/p2p/" + peerIdStr,
		},
		{
			gossipUrl: "ws://192.0.2.7:8443",
			peerId:    peerIdStr,
			expect:    "/ip4/192.0.2.7/tcp/8443/ws/p2p/" + peerIdStr,
		},
		{
			gossipUrl: "ws://[2001:db8::1]:8443",
			peerId:    peerIdStr,
			expect:    "/ip6/2001:db8::1/tcp/8443/ws/p2p/" + peerIdStr,
		},
		// the operator has not published an identity yet, so there is nothing
		// to dial
		{gossipUrl: "wss://gossip.space.example", peerId: "", expect: ""},
		// an env secret path has no websocket multiaddr
		{gossipUrl: "wss://gossip.space.example/sekret", peerId: peerIdStr, expectErr: true},
		{gossipUrl: "quic://gossip.space.example", peerId: peerIdStr, expectErr: true},
		{gossipUrl: "wss://gossip.space.example", peerId: "not-a-peer-id", expectErr: true},
	}
	for _, c := range cases {
		operatorAddrs, err := OperatorAddrsFromUrl(c.gossipUrl, c.peerId)
		if c.expectErr {
			if err == nil {
				t.Errorf("%s %s: expected an error, got %v", c.gossipUrl, c.peerId, operatorAddrs)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s %s: %v", c.gossipUrl, c.peerId, err)
			continue
		}
		if c.expect == "" {
			if len(operatorAddrs) != 0 {
				t.Errorf("%s: expected no address, got %v", c.gossipUrl, operatorAddrs)
			}
			continue
		}
		if len(operatorAddrs) != 1 || operatorAddrs[0].String() != c.expect {
			t.Errorf("%s: address = %v, expected %s", c.gossipUrl, operatorAddrs, c.expect)
		}
	}
}

// The mesh identity of an extender is exactly what its identity key derives,
// so a client that holds a record knows the peer it must meet (D1, D2).
func TestGossipPeerIdIsDerivedFromTheIdentityKey(t *testing.T) {
	extenderKey := newTestKey(t)
	recordPeerId, err := PeerIdForExtenderPublicKey(extenderKey.publicKey)
	if err != nil {
		t.Fatal(err)
	}
	libp2pPrivateKey, err := crypto.UnmarshalEd25519PrivateKey(extenderKey.privateKey)
	if err != nil {
		t.Fatal(err)
	}
	hostPeerId, err := peer.IDFromPrivateKey(libp2pPrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if recordPeerId != hostPeerId {
		t.Fatalf("record peer id %s is not the host peer id %s", recordPeerId, hostPeerId)
	}
	if _, err := PeerIdForExtenderPublicKey([]byte{1, 2, 3}); err == nil {
		t.Fatalf("a key that is not an ed25519 key produced a peer id")
	}
}

// Listen addresses are one per family at the extender's own tcp port (D2).
func TestGossipExtenderListenAddrs(t *testing.T) {
	listenAddrs, err := ExtenderListenAddrs(testAddrs(t, "192.0.2.1", "2001:db8::1"), 8443)
	if err != nil {
		t.Fatal(err)
	}
	expects := []string{"/ip4/192.0.2.1/tcp/8443", "/ip6/2001:db8::1/tcp/8443"}
	for i, expect := range expects {
		if listenAddrs[i].String() != expect {
			t.Errorf("listen addr = %s, expected %s", listenAddrs[i], expect)
		}
	}

	websocketAddrs, err := WebsocketListenAddrs("127.0.0.1", 9443, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(websocketAddrs) != 1 || websocketAddrs[0].String() != "/ip4/127.0.0.1/tcp/9443/ws" {
		t.Fatalf("websocket addrs = %v", websocketAddrs)
	}
	wildcardAddrs, err := WebsocketListenAddrs("", 9443, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(wildcardAddrs) != 2 {
		t.Fatalf("websocket addrs = %v, expected one per family", wildcardAddrs)
	}
}

// The transport takes the ip tcp addresses the tcp transport would, and
// nothing else (D2).
func TestGossipTransportCanDialIpTcpOnly(t *testing.T) {
	rootKey := newTestKey(t)
	extenderTransport := newTestTransport(t, newTestDirectory(t, rootKey), nil)

	cases := []struct {
		addr   string
		expect bool
	}{
		{addr: "/ip4/192.0.2.1/tcp/443", expect: true},
		{addr: "/ip6/2001:db8::1/tcp/443", expect: true},
		{addr: "/ip4/192.0.2.1/udp/443/quic-v1", expect: false},
		{addr: "/dns/gossip.space.example/tcp/443/wss", expect: false},
		{addr: "/ip4/192.0.2.1/tcp/443/ws", expect: false},
	}
	for _, c := range cases {
		addr, err := ma.NewMultiaddr(c.addr)
		if err != nil {
			t.Fatal(err)
		}
		if canDial := extenderTransport.CanDial(addr); canDial != c.expect {
			t.Errorf("can dial %s = %v, expected %v", c.addr, canDial, c.expect)
		}
	}
	if protocols := extenderTransport.Protocols(); len(protocols) != 1 || protocols[0] != ma.P_TCP {
		t.Errorf("protocols = %v, expected tcp", protocols)
	}
	if extenderTransport.Proxy() {
		t.Errorf("the extender transport is not a proxy transport")
	}
}

// A dial is refused before any connection is opened when the directory holds
// no key for the address, or holds a key that is not the peer asked for (D2).
func TestGossipTransportRefusesAnUnverifiedAddress(t *testing.T) {
	rootKey := newTestKey(t)
	extenderKey := newTestKey(t)
	directory := newTestDirectory(t, rootKey)
	extenderTransport := newTestTransport(t, directory, nil)

	extenderPeerId, err := PeerIdForExtenderPublicKey(extenderKey.publicKey)
	if err != nil {
		t.Fatal(err)
	}
	addr, err := ma.NewMultiaddr("/ip4/192.0.2.1/tcp/8443")
	if err != nil {
		t.Fatal(err)
	}

	// nothing in the directory names this address
	if _, err := extenderTransport.Dial(context.Background(), addr, extenderPeerId); err == nil {
		t.Fatalf("a dial to an address with no record succeeded")
	}

	record := signTestRecord(t, rootKey, extenderKey, "192.0.2.1", 8443, time.Now())
	if _, err := directory.ApplyRecord(record, connect.ExtenderSourceGossip); err != nil {
		t.Fatal(err)
	}

	// the address is verified now, but for another peer
	otherPeerId, err := PeerIdForExtenderPublicKey(newTestKey(t).publicKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := extenderTransport.Dial(context.Background(), addr, otherPeerId); err == nil {
		t.Fatalf("a dial to a mismatched peer id succeeded")
	}
}

// Only an extender listens (D2, D3).
func TestGossipTransportListenNeedsAListener(t *testing.T) {
	rootKey := newTestKey(t)
	addr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/8443")
	if err != nil {
		t.Fatal(err)
	}
	memberTransport := newTestTransport(t, newTestDirectory(t, rootKey), nil)
	if _, err := memberTransport.Listen(addr); err == nil {
		t.Fatalf("a member listened")
	}
}

// The validator accepts a root-signed message of this space and rejects
// everything else, which is what keeps a forgery from being relayed (D1).
func TestGossipValidatorAcceptsOnlyThisSpacesSignedMessages(t *testing.T) {
	rootKey := newTestKey(t)
	otherRootKey := newTestKey(t)
	extenderKey := newTestKey(t)
	directory := newTestDirectory(t, rootKey)
	node := &Node{
		settings: &NodeSettings{
			NetworkHost: testNetworkHost,
			Directory:   directory,
		},
	}

	now := time.Now()
	otherHostRecord, err := connect.SignExtenderRecord(rootKey.privateKey, &protocol.ExtenderRecordBody{
		PublicKey:    extenderKey.publicKey,
		Addresses:    []*protocol.ExtenderAddress{{Ip: "192.0.2.1", IpVersion: 4}},
		TcpPort:      443,
		IssueTimeMs:  uint64(now.UnixMilli()),
		ExpireTimeMs: uint64(now.Add(time.Hour).UnixMilli()),
		NetworkHost:  "other.example",
	})
	if err != nil {
		t.Fatal(err)
	}
	revocation, err := connect.SignExtenderRevocation(rootKey.privateKey, &protocol.ExtenderRevocationBody{
		PublicKey:   extenderKey.publicKey,
		IssueTimeMs: uint64(now.UnixMilli()),
		NetworkHost: testNetworkHost,
	})
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name    string
		message *protocol.ExtenderGossipMessage
		data    []byte
		expect  pubsub.ValidationResult
	}{
		{
			name: "record",
			message: &protocol.ExtenderGossipMessage{
				Message: &protocol.ExtenderGossipMessage_Record{
					Record: signTestRecord(t, rootKey, extenderKey, "192.0.2.1", 443, now),
				},
			},
			expect: pubsub.ValidationAccept,
		},
		{
			name: "revocation",
			message: &protocol.ExtenderGossipMessage{
				Message: &protocol.ExtenderGossipMessage_Revocation{Revocation: revocation},
			},
			expect: pubsub.ValidationAccept,
		},
		{
			name: "another root key",
			message: &protocol.ExtenderGossipMessage{
				Message: &protocol.ExtenderGossipMessage_Record{
					Record: signTestRecord(t, otherRootKey, extenderKey, "192.0.2.1", 443, now),
				},
			},
			expect: pubsub.ValidationReject,
		},
		{
			name: "another network host",
			message: &protocol.ExtenderGossipMessage{
				Message: &protocol.ExtenderGossipMessage_Record{Record: otherHostRecord},
			},
			expect: pubsub.ValidationReject,
		},
		{
			name:    "empty message",
			message: &protocol.ExtenderGossipMessage{},
			expect:  pubsub.ValidationReject,
		},
		{
			name:   "not a gossip message",
			data:   []byte{0xff, 0xff, 0xff, 0xff},
			expect: pubsub.ValidationReject,
		},
	}
	for _, c := range cases {
		data := c.data
		if c.message != nil {
			messageBytes, err := proto.Marshal(c.message)
			if err != nil {
				t.Fatal(err)
			}
			data = messageBytes
		}
		result := node.validate(context.Background(), "", &pubsub.Message{
			Message: &pubsubpb.Message{Data: data},
		})
		if result != c.expect {
			t.Errorf("%s: validation = %v, expected %v", c.name, result, c.expect)
		}
	}
}

// Handle owns the stream until the mesh releases it, and refuses rather than
// waiting when the accept queue is full (A8, D2).
func TestGossipInProcessListenerHoldsTheStreamUntilItIsReleased(t *testing.T) {
	settings := DefaultInProcessListenerSettings()
	settings.AcceptQueueCount = 1
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	listener := NewInProcessListener(ctx, settings)
	t.Cleanup(listener.Close)
	done := make(chan struct{})

	// an accepted connection releases its handler only when the mesh closes it
	handledConn := newTestPipe(t)
	handled := make(chan struct{})
	go func() {
		defer close(handled)
		listener.Handle(handledConn)
	}()
	accepted, err := listener.accept(done)
	if err != nil {
		t.Fatal(err)
	}
	accepted.Close()
	select {
	case <-handled:
	case <-time.After(5 * time.Second):
		t.Fatal("the extender handler was not released by the close")
	}

	// a connection that arrives with the queue full is refused at once, rather
	// than parking the extender's connection goroutine
	listener.conns <- &inProcessConn{
		Conn:     newTestPipe(t),
		released: make(chan struct{}),
	}
	refused := make(chan struct{})
	go func() {
		defer close(refused)
		listener.Handle(newTestPipe(t))
	}()
	select {
	case <-refused:
	case <-time.After(5 * time.Second):
		t.Fatal("a connection over the accept bound was not refused")
	}

	// closing the listener releases a handler that is still holding a stream
	waitingConn := newTestPipe(t)
	waiting := make(chan struct{})
	go func() {
		defer close(waiting)
		listener.Handle(waitingConn)
	}()
	// drain the filler, then take the waiting connection
	if _, err := listener.accept(done); err != nil {
		t.Fatal(err)
	}
	if _, err := listener.accept(done); err != nil {
		t.Fatal(err)
	}
	listener.Close()
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("the close did not release a waiting handler")
	}
}

// One transport with no upgrader, which is all the checks above reach.
func newTestTransport(
	t *testing.T,
	directory *connect.ExtenderDirectory,
	listener *InProcessListener,
) *extenderTransport {
	t.Helper()
	extenderTransport, err := newExtenderTransport(&extenderTransportSettings{
		Directory: directory,
		Listener:  listener,
	})
	if err != nil {
		t.Fatal(err)
	}
	return extenderTransport
}

func testAddrs(t *testing.T, ips ...string) []netip.Addr {
	t.Helper()
	addrs := []netip.Addr{}
	for _, ip := range ips {
		addr, err := netip.ParseAddr(ip)
		if err != nil {
			t.Fatal(err)
		}
		addrs = append(addrs, addr)
	}
	return addrs
}

// One end of a pipe, closed with the test.
func newTestPipe(t *testing.T) net.Conn {
	t.Helper()
	localConn, remoteConn := net.Pipe()
	t.Cleanup(func() {
		localConn.Close()
		remoteConn.Close()
	})
	return localConn
}
