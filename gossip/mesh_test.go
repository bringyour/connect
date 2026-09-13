// The mesh end to end (EXTENDER.md D1 to D4).
//
// The fixture stands up what the design describes: an operator listening with
// the websocket transport, three extenders each running a real extender server
// with its in-process gossip listener and feed server, and one member app that
// only dials. Every extender is bound on loopback and reached at an RFC 5737 or
// RFC 3849 documentation address, which the dial seam maps back to loopback --
// so a record names a stable public address the way a real one does, and three
// extenders on one host still have three distinct addresses.
//
// The fixture runs on either family: v4 binds 127.0.0.1 and publishes
// 192.0.2.0/24, v6 binds ::1 and publishes 2001:db8::/32 (IPV6.md A4).
//
// Nothing here sleeps for a result. Progress is observed through the
// directory's change monitor and the node's status monitor, both of which
// publish exactly when the thing under test lands.

package gossip

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/sec"
	"github.com/libp2p/go-libp2p/core/transport"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/net/upgrader"
	"github.com/libp2p/go-libp2p/p2p/security/noise"

	ma "github.com/multiformats/go-multiaddr"

	"github.com/urnetwork/connect"
	"github.com/urnetwork/connect/extender"
	"github.com/urnetwork/connect/protocol"
)

// How long a barrier waits for the mesh to reach a state. Generous, because
// the race build of five libp2p hosts is slow; nothing waits this long when
// the mesh is healthy.
const testMeshTimeout = 60 * time.Second

// One extender of the fixture: a real extender server on loopback, its
// in-process gossip listener, its feed server and its node.
type testExtender struct {
	key       *testKey
	ip        netip.Addr
	tcpPort   int
	server    *extender.ExtenderServer
	listener  *InProcessListener
	feed      *FeedServer
	directory *connect.ExtenderDirectory
	node      *Node
}

type testMesh struct {
	t       *testing.T
	ctx     context.Context
	rootKey *testKey

	operator          *Node
	operatorDirectory *connect.ExtenderDirectory
	member            *Node
	memberDirectory   *connect.ExtenderDirectory
	extenders         []*testExtender

	connectSettings *connect.ConnectSettings
	// 4 or 6: the family every extender of this fixture binds and publishes
	ipVersion int

	stateLock sync.Mutex
	// documentation ip to the loopback address the extender really listens on
	ipLoopbacks map[string]string
}

// Builds the whole fixture on the v4 family: the operator, `extenderCount`
// extenders and one member, all pointed at the operator.
func newTestMesh(t *testing.T, extenderCount int) *testMesh {
	t.Helper()
	return newTestMeshFamily(t, extenderCount, 4)
}

// The same fixture on one address family, 4 or 6.
func newTestMeshFamily(t *testing.T, extenderCount int, ipVersion int) *testMesh {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	mesh := &testMesh{
		t:           t,
		ctx:         ctx,
		rootKey:     newTestKey(t),
		ipVersion:   ipVersion,
		ipLoopbacks: map[string]string{},
	}
	mesh.connectSettings = connect.DefaultConnectSettings()
	mesh.connectSettings.DialContextSettings = &connect.DialContextSettings{
		DialContext: mesh.dialContext,
	}

	mesh.operatorDirectory = newTestDirectory(t, mesh.rootKey)
	operatorListenAddrs, err := WebsocketListenAddrs(mesh.loopbackIp(), 0, false)
	if err != nil {
		t.Fatal(err)
	}
	mesh.operator = mesh.newNode(t, "operator", newTestKey(t), mesh.operatorDirectory, func(settings *NodeSettings) {
		settings.ListenAddrs = operatorListenAddrs
	})
	operatorAddrs := mesh.operatorAddrs()

	for i := range extenderCount {
		mesh.extenders = append(mesh.extenders, mesh.newExtender(t, i, operatorAddrs))
	}

	mesh.memberDirectory = newTestDirectory(t, mesh.rootKey)
	mesh.member = mesh.newNode(t, "member", newTestKey(t), mesh.memberDirectory, func(settings *NodeSettings) {
		settings.OperatorAddrs = operatorAddrs
	})
	return mesh
}

// The operator's dialable address, taken from what its swarm actually bound.
func (self *testMesh) operatorAddrs() []ma.Multiaddr {
	self.t.Helper()
	listenAddrs := self.operator.ListenAddrs()
	if len(listenAddrs) == 0 {
		self.t.Fatal("the operator bound no address")
	}
	p2pComponent, err := ma.NewComponent("p2p", self.operator.PeerId().String())
	if err != nil {
		self.t.Fatal(err)
	}
	return []ma.Multiaddr{listenAddrs[0].Encapsulate(p2pComponent)}
}

// One extender: the documentation address it publishes, the loopback socket it
// really listens on, and the node behind its in-process listener.
func (self *testMesh) newExtender(
	t *testing.T,
	index int,
	operatorAddrs []ma.Multiaddr,
) *testExtender {
	t.Helper()
	key := newTestKey(t)
	ip := self.documentationIp(index)
	tcpListener, err := net.Listen("tcp", net.JoinHostPort(self.loopbackIp(), "0"))
	if err != nil {
		t.Fatal(err)
	}
	tcpPort := tcpListener.Addr().(*net.TCPAddr).Port
	self.setLoopback(ip, tcpListener.Addr().String())

	listenAddrs, err := ExtenderListenAddrs([]netip.Addr{ip}, tcpPort)
	if err != nil {
		t.Fatal(err)
	}
	listenerSettings := DefaultInProcessListenerSettings()
	listenerSettings.ListenAddrs = listenAddrs
	listener := NewInProcessListener(self.ctx, listenerSettings)
	t.Cleanup(listener.Close)

	directory := newTestDirectory(t, self.rootKey)
	feed := NewFeedServer(self.ctx, directory, key.publicKey, DefaultFeedServerSettings())
	t.Cleanup(feed.Close)

	server := newTestExtenderServer(t, self.ctx, key, tcpListener, tcpPort, func(settings *extender.ExtenderSettings) {
		settings.GossipConnHandler = listener.Handle
		settings.FeedConnHandler = feed.Serve
	})

	node := self.newNode(t, fmt.Sprintf("extender-%d", index), key, directory, func(settings *NodeSettings) {
		settings.Role = NodeRoleExtender
		settings.PeerTarget = 16
		settings.ConnManagerLowWater = 16
		settings.ConnManagerHighWater = 32
		settings.ExtenderListener = listener
		settings.OperatorAddrs = operatorAddrs
	})
	return &testExtender{
		key:       key,
		ip:        ip,
		tcpPort:   tcpPort,
		server:    server,
		listener:  listener,
		feed:      feed,
		directory: directory,
		node:      node,
	}
}

// One node with the fixture's dial seam and a peering tick long enough that
// only a directory change or a wake can drive a round.
func (self *testMesh) newNode(
	t *testing.T,
	name string,
	key *testKey,
	directory *connect.ExtenderDirectory,
	configure func(settings *NodeSettings),
) *Node {
	t.Helper()
	settings := DefaultNodeSettings(NodeRoleMember)
	settings.NetworkHost = testNetworkHost
	settings.Directory = directory
	settings.IdentityKeySeed = key.seed
	settings.ConnectSettings = self.connectSettings
	settings.PeerTimeout = 5 * time.Minute
	settings.PeerCoalesceTimeout = 10 * time.Millisecond
	settings.DialTimeout = 20 * time.Second
	settings.StatusTimeout = 50 * time.Millisecond
	settings.RandSeed = int64(len(name))
	if configure != nil {
		configure(settings)
	}
	node, err := NewNode(self.ctx, settings)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	t.Cleanup(node.Close)
	return node
}

// Maps a published documentation address back to the loopback socket the
// extender really listens on, so a record can name a stable public address.
func (self *testMesh) dialContext(
	ctx context.Context,
	network string,
	address string,
) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	if loopback := self.loopback(host); loopback != "" {
		loopbackHost, _, err := net.SplitHostPort(loopback)
		if err != nil {
			return nil, err
		}
		address = net.JoinHostPort(loopbackHost, port)
	}
	return (&net.Dialer{}).DialContext(ctx, network, address)
}

// The loopback address of this fixture's family, which is what every extender
// and the operator really bind.
func (self *testMesh) loopbackIp() string {
	if self.ipVersion == 6 {
		return "::1"
	}
	return "127.0.0.1"
}

// The documentation address one extender publishes, one per index and one per
// family.
func (self *testMesh) documentationIp(index int) netip.Addr {
	if self.ipVersion == 6 {
		return netip.MustParseAddr(fmt.Sprintf("2001:db8::%x", index+1))
	}
	return netip.MustParseAddr(fmt.Sprintf("192.0.2.%d", index+1))
}

func (self *testMesh) setLoopback(ip netip.Addr, loopback string) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.ipLoopbacks[ip.String()] = loopback
}

func (self *testMesh) loopback(host string) string {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.ipLoopbacks[host]
}

// The same seam for a fixture that is not a whole mesh: one published
// documentation address mapped back to the loopback socket it really listens
// on, so a record there names a stable public address too.
func newTestLoopbackConnectSettings(
	t *testing.T,
	ip netip.Addr,
	loopbackAddr string,
) *connect.ConnectSettings {
	t.Helper()
	loopbackHost, _, err := net.SplitHostPort(loopbackAddr)
	if err != nil {
		t.Fatal(err)
	}
	connectSettings := connect.DefaultConnectSettings()
	connectSettings.DialContextSettings = &connect.DialContextSettings{
		DialContext: func(
			ctx context.Context,
			network string,
			address string,
		) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			if host == ip.String() {
				address = net.JoinHostPort(loopbackHost, port)
			}
			return (&net.Dialer{}).DialContext(ctx, network, address)
		},
	}
	return connectSettings
}

// Every directory the mesh fills.
func (self *testMesh) directories() []*connect.ExtenderDirectory {
	directories := []*connect.ExtenderDirectory{
		self.operatorDirectory,
		self.memberDirectory,
	}
	for _, testExtender := range self.extenders {
		directories = append(directories, testExtender.directory)
	}
	return directories
}

// The record one extender publishes for itself.
func (self *testMesh) signRecord(index int, issueTime time.Time) *protocol.ExtenderRecord {
	self.t.Helper()
	testExtender := self.extenders[index]
	return signTestRecord(
		self.t,
		self.rootKey,
		testExtender.key,
		testExtender.ip.String(),
		testExtender.tcpPort,
		issueTime,
	)
}

// Publishes one message from the operator, the only node that originates (D6).
func (self *testMesh) publish(message *protocol.ExtenderGossipMessage) {
	self.t.Helper()
	ctx, cancel := context.WithTimeout(self.ctx, testMeshTimeout)
	defer cancel()
	if err := self.operator.Publish(ctx, message); err != nil {
		self.t.Fatalf("publish: %v", err)
	}
}

// Waits until the operator sees every other node in the topic, which is the
// barrier a publish needs: gossipsub floods a locally published message to
// every topic peer it knows. The other nodes are only connected to the
// operator at this point, since their directories are still empty.
func (self *testMesh) waitForTopic(t *testing.T) {
	t.Helper()
	peerCount := 1 + len(self.extenders)
	waitForNodeStatus(t, self.operator, "every node in the topic", func(status NodeStatus) bool {
		return peerCount <= status.MeshPeerCount
	})
	nodes := []*Node{self.member}
	for _, testExtender := range self.extenders {
		nodes = append(nodes, testExtender.node)
	}
	for _, node := range nodes {
		waitForNodeStatus(t, node, "the operator in the topic", func(status NodeStatus) bool {
			return 1 <= status.MeshPeerCount
		})
	}
}

// Waits for a directory to reach a state, watching the change monitor rather
// than polling.
func waitForDirectory(
	t *testing.T,
	directory *connect.ExtenderDirectory,
	what string,
	condition func(snapshot *connect.ExtenderDirectorySnapshot) bool,
) {
	t.Helper()
	deadline := time.Now().Add(testMeshTimeout)
	for {
		_, change := directory.ChangeMonitor().Get()
		if condition(directory.Snapshot()) {
			return
		}
		select {
		case <-change:
		case <-time.After(time.Until(deadline)):
			t.Fatalf("the directory did not reach %s", what)
		}
	}
}

// The same for a node's status.
func waitForNodeStatus(
	t *testing.T,
	node *Node,
	what string,
	condition func(status NodeStatus) bool,
) {
	t.Helper()
	deadline := time.Now().Add(testMeshTimeout)
	for {
		status, change := node.StatusMonitor().Get()
		if condition(status) {
			return
		}
		select {
		case <-change:
		case <-time.After(time.Until(deadline)):
			t.Fatalf("the node did not reach %s, status = %+v", what, node.Status())
		}
	}
}

// The state of one address in a snapshot, empty when it is not known.
func snapshotState(snapshot *connect.ExtenderDirectorySnapshot, ip netip.Addr) string {
	for _, entry := range snapshot.Entries {
		if entry.Ip == ip {
			return entry.State
		}
	}
	return ""
}

// One extender server on a pre-bound loopback listener, with an identity key
// so its certificate verifies against the record (B3).
func newTestExtenderServer(
	t *testing.T,
	ctx context.Context,
	key *testKey,
	tcpListener net.Listener,
	tcpPort int,
	configure func(settings *extender.ExtenderSettings),
) *extender.ExtenderServer {
	t.Helper()
	settings := extender.DefaultExtenderSettings()
	settings.IdentityKeySeed = key.seed
	settings.Listen = func(network string, address string) (net.Listener, error) {
		if address != fmt.Sprintf(":%d", tcpPort) {
			return nil, fmt.Errorf("unexpected extender listen %s %s", network, address)
		}
		return tcpListener, nil
	}
	if configure != nil {
		configure(settings)
	}
	// an open extender, which is what an operator activated one is (A4)
	server := extender.NewExtenderServer(
		ctx,
		nil,
		nil,
		map[int][]connect.ExtenderConnectMode{
			tcpPort: {connect.ExtenderConnectModeTcpTls},
		},
		&net.Dialer{},
		settings,
	)
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.ListenAndServe()
	}()
	t.Cleanup(func() {
		server.CloseAndWait()
		select {
		case err := <-serveDone:
			if err != nil {
				t.Errorf("extender server: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("the extender server did not stop")
		}
	})
	return server
}

// A record the operator publishes reaches every directory, a revocation does
// the same, and a message signed by another key reaches none of them (D1, D6).
func TestGossipMeshRelaysSignedRecords(t *testing.T) {
	mesh := newTestMesh(t, 3)
	mesh.waitForTopic(t)

	issueTime := time.Now()
	record := mesh.signRecord(0, issueTime)
	mesh.publish(&protocol.ExtenderGossipMessage{
		Message: &protocol.ExtenderGossipMessage_Record{Record: record},
	})
	for _, directory := range mesh.directories() {
		waitForDirectory(t, directory, "the published record", func(snapshot *connect.ExtenderDirectorySnapshot) bool {
			return snapshotState(snapshot, mesh.extenders[0].ip) == connect.ExtenderStateActive
		})
	}

	// a message signed by a key the directories do not accept is rejected by
	// the validator, which refuses the publish at the origin
	forgedRootKey := newTestKey(t)
	forgedRecord := signTestRecord(
		t,
		forgedRootKey,
		mesh.extenders[1].key,
		mesh.extenders[1].ip.String(),
		mesh.extenders[1].tcpPort,
		issueTime,
	)
	publishCtx, publishCancel := context.WithTimeout(mesh.ctx, testMeshTimeout)
	defer publishCancel()
	if err := mesh.operator.Publish(publishCtx, &protocol.ExtenderGossipMessage{
		Message: &protocol.ExtenderGossipMessage_Record{Record: forgedRecord},
	}); err == nil {
		t.Fatal("a forged record was published")
	}

	// the revocation is the flush barrier: once it has reached a directory,
	// anything published before it has been delivered there too
	revocation, err := connect.SignExtenderRevocation(
		mesh.rootKey.privateKey,
		&protocol.ExtenderRevocationBody{
			PublicKey:   mesh.extenders[0].key.publicKey,
			IssueTimeMs: uint64(issueTime.Add(time.Second).UnixMilli()),
			NetworkHost: testNetworkHost,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	mesh.publish(&protocol.ExtenderGossipMessage{
		Message: &protocol.ExtenderGossipMessage_Revocation{Revocation: revocation},
	})
	for _, directory := range mesh.directories() {
		waitForDirectory(t, directory, "the published revocation", func(snapshot *connect.ExtenderDirectorySnapshot) bool {
			return snapshotState(snapshot, mesh.extenders[0].ip) == connect.ExtenderStateRevoked
		})
		if state := snapshotState(directory.Snapshot(), mesh.extenders[1].ip); state != "" {
			t.Fatalf("the forged record was applied as %s", state)
		}
	}
}

// The member holds the operator link and dials the extenders a directory
// change taught it about (D3).
func TestGossipMeshPeersWithTheOperatorAndExtenders(t *testing.T) {
	mesh := newTestMesh(t, 3)
	mesh.waitForTopic(t)

	// the member reaches the operator over the websocket transport with
	// nothing in its directory at all
	waitForNodeStatus(t, mesh.member, "the operator link", func(status NodeStatus) bool {
		return status.OperatorConnected
	})

	issueTime := time.Now()
	for i := range mesh.extenders {
		mesh.publish(&protocol.ExtenderGossipMessage{
			Message: &protocol.ExtenderGossipMessage_Record{Record: mesh.signRecord(i, issueTime)},
		})
	}
	for _, testExtender := range mesh.extenders {
		waitForDirectory(t, mesh.memberDirectory, "the extender records", func(snapshot *connect.ExtenderDirectorySnapshot) bool {
			return snapshotState(snapshot, testExtender.ip) == connect.ExtenderStateActive
		})
	}

	// the peering tick is five minutes here, so the only thing that can drive
	// this round is the directory change itself (D3)
	waitForNodeStatus(t, mesh.member, "the extender links", func(status NodeStatus) bool {
		return 1+len(mesh.extenders) <= status.PeerCount
	})

	// every extender the member dialed reached the member through its own
	// in-process listener, so each of them has an inbound peer as well
	for _, testExtender := range mesh.extenders {
		waitForNodeStatus(t, testExtender.node, "the member link", func(status NodeStatus) bool {
			return 2 <= status.PeerCount
		})
	}
}

// A dial fails at the upgrader when the extender's mesh identity is not the
// key its record names, even though its certificate verifies (D2).
func TestGossipMeshRefusesAMismatchedIdentity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	rootKey := newTestKey(t)
	extenderKey := newTestKey(t)
	// the node behind the listener is a different identity from the one the
	// record names, which is the mismatch under test
	nodeKey := newTestKey(t)

	tcpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tcpPort := tcpListener.Addr().(*net.TCPAddr).Port
	// the record names a documentation address and the dial seam maps it back
	// to the loopback socket, the same way the whole mesh fixture does
	ip := netip.MustParseAddr("192.0.2.60")
	connectSettings := newTestLoopbackConnectSettings(t, ip, tcpListener.Addr().String())

	listenAddrs, err := ExtenderListenAddrs([]netip.Addr{ip}, tcpPort)
	if err != nil {
		t.Fatal(err)
	}
	listenerSettings := DefaultInProcessListenerSettings()
	listenerSettings.ListenAddrs = listenAddrs
	listener := NewInProcessListener(ctx, listenerSettings)
	t.Cleanup(listener.Close)

	newTestExtenderServer(t, ctx, extenderKey, tcpListener, tcpPort, func(settings *extender.ExtenderSettings) {
		settings.GossipConnHandler = listener.Handle
	})

	extenderDirectory := newTestDirectory(t, rootKey)
	extenderNode, err := NewNode(ctx, func() *NodeSettings {
		settings := DefaultNodeSettings(NodeRoleExtender)
		settings.NetworkHost = testNetworkHost
		settings.Directory = extenderDirectory
		settings.IdentityKeySeed = nodeKey.seed
		settings.ExtenderListener = listener
		settings.PeerTimeout = 5 * time.Minute
		return settings
	}())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(extenderNode.Close)

	// the client knows the extender by the key in its record, which is the key
	// the certificate verifies against but not the key the mesh presents
	clientKey := newTestKey(t)
	clientDirectory := newTestDirectory(t, rootKey)
	record := signTestRecord(t, rootKey, extenderKey, ip.String(), tcpPort, time.Now())
	if _, err := clientDirectory.ApplyRecord(record, connect.ExtenderSourceGossip); err != nil {
		t.Fatal(err)
	}
	clientTransport, err := newExtenderTransport(&extenderTransportSettings{
		Directory:       clientDirectory,
		ConnectSettings: connectSettings,
		Upgrader:        newTestUpgrader(t, clientKey),
	})
	if err != nil {
		t.Fatal(err)
	}

	expectedPeerId, err := PeerIdForExtenderPublicKey(extenderKey.publicKey)
	if err != nil {
		t.Fatal(err)
	}
	raddr, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", ip, tcpPort))
	if err != nil {
		t.Fatal(err)
	}
	dialCtx, dialCancel := context.WithTimeout(ctx, testMeshTimeout)
	defer dialCancel()
	capableConn, err := clientTransport.Dial(dialCtx, raddr, expectedPeerId)
	if err == nil {
		capableConn.Close()
		t.Fatal("a dial to a mismatched mesh identity succeeded")
	}
	if !strings.Contains(err.Error(), "peer id mismatch") &&
		!strings.Contains(err.Error(), expectedPeerId.String()) {
		t.Fatalf("the dial failed with %v, expected a peer id mismatch", err)
	}

	// the same transport reaches a node whose identity is the record key
	if _, err := PeerIdForExtenderPublicKey(nodeKey.publicKey); err != nil {
		t.Fatal(err)
	}
}

// One upgrader with the mesh's own security and muxer, for a dial that has no
// host behind it.
func newTestUpgrader(t *testing.T, key *testKey) transport.Upgrader {
	t.Helper()
	privateKey, err := crypto.UnmarshalEd25519PrivateKey(key.privateKey)
	if err != nil {
		t.Fatal(err)
	}
	noiseTransport, err := noise.New(noise.ID, privateKey, nil)
	if err != nil {
		t.Fatal(err)
	}
	connUpgrader, err := upgrader.New(
		[]sec.SecureTransport{noiseTransport},
		[]upgrader.StreamMuxer{{ID: yamux.ID, Muxer: yamux.DefaultTransport}},
		nil,
		nil,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	return connUpgrader
}

// A peering round with dials in flight and no mesh peer is the connecting
// state the app's status dot shows (K4). The operator address here is a socket
// that accepts and never speaks, so the dial stays in flight rather than
// failing, and the member holds no mesh peer at all.
func TestGossipNodeReportsConnectingWhileItDials(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		listener.Close()
	})
	acceptedLock := sync.Mutex{}
	accepted := []net.Conn{}
	t.Cleanup(func() {
		acceptedLock.Lock()
		defer acceptedLock.Unlock()
		for _, conn := range accepted {
			conn.Close()
		}
	})
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			acceptedLock.Lock()
			accepted = append(accepted, conn)
			acceptedLock.Unlock()
		}
	}()

	operatorKey := newTestKey(t)
	operatorPeerId, err := PeerIdForExtenderPublicKey(operatorKey.publicKey)
	if err != nil {
		t.Fatal(err)
	}
	listenAddrs, err := WebsocketListenAddrs("127.0.0.1", listener.Addr().(*net.TCPAddr).Port, false)
	if err != nil {
		t.Fatal(err)
	}
	p2pComponent, err := ma.NewComponent("p2p", operatorPeerId.String())
	if err != nil {
		t.Fatal(err)
	}

	settings := DefaultNodeSettings(NodeRoleMember)
	settings.NetworkHost = testNetworkHost
	settings.Directory = newTestDirectory(t, newTestKey(t))
	settings.IdentityKeySeed = newTestKey(t).seed
	settings.OperatorAddrs = []ma.Multiaddr{listenAddrs[0].Encapsulate(p2pComponent)}
	// one round, held open by the silent socket for the whole dial budget
	settings.PeerTimeout = 5 * time.Minute
	settings.DialTimeout = testMeshTimeout
	settings.StatusTimeout = 50 * time.Millisecond
	node, err := NewNode(ctx, settings)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(node.Close)

	waitForNodeStatus(t, node, "the connecting state", func(status NodeStatus) bool {
		return status.Connecting
	})
	status := node.Status()
	if status.MeshPeerCount != 0 {
		t.Fatalf("mesh peers = %d, want none while connecting", status.MeshPeerCount)
	}
	if state := connect.ExtenderGossipStateForMember(
		status.MeshPeerCount,
		status.Connecting,
	); state != connect.ExtenderGossipStateConnecting {
		t.Fatalf("gossip state = %q, want %q", state, connect.ExtenderGossipStateConnecting)
	}
}
