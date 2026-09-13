// The node's construction, late listen and shutdown (EXTENDER.md D1, D2, D3).
//
// The nodes here are real: a libp2p host, the four loops, and an in-process
// listener where a listen address is needed. Nothing dials anything, so a node
// costs one host and no network at all.

package gossip

import (
	"context"
	"slices"
	"testing"
	"time"

	ma "github.com/multiformats/go-multiaddr"
)

// How long a node assertion waits for something that has already happened.
const testNodeTimeout = 30 * time.Second

// The defaults of a role are what the mesh is sized by, and a role the sdk
// does not know is a member (D1, D3).
func TestGossipDefaultNodeSettingsPerRole(t *testing.T) {
	cases := []struct {
		role             string
		expectRole       string
		expectPeerTarget int
		expectLowWater   int
		expectHighWater  int
	}{
		{
			role:             NodeRoleMember,
			expectRole:       NodeRoleMember,
			expectPeerTarget: 8,
			expectLowWater:   8,
			expectHighWater:  16,
		},
		{
			role:             NodeRoleExtender,
			expectRole:       NodeRoleExtender,
			expectPeerTarget: 16,
			expectLowWater:   16,
			expectHighWater:  32,
		},
		// anything that is not the extender role is a member, including a role
		// that only differs in case
		{
			role:             "",
			expectRole:       NodeRoleMember,
			expectPeerTarget: 8,
			expectLowWater:   8,
			expectHighWater:  16,
		},
		{
			role:             "feed",
			expectRole:       NodeRoleMember,
			expectPeerTarget: 8,
			expectLowWater:   8,
			expectHighWater:  16,
		},
		{
			role:             "Extender",
			expectRole:       NodeRoleMember,
			expectPeerTarget: 8,
			expectLowWater:   8,
			expectHighWater:  16,
		},
	}
	for _, c := range cases {
		settings := DefaultNodeSettings(c.role)
		if settings.Role != c.expectRole {
			t.Errorf("%q: role = %q, expected %q", c.role, settings.Role, c.expectRole)
		}
		if settings.PeerTarget != c.expectPeerTarget {
			t.Errorf("%q: peer target = %d, expected %d", c.role, settings.PeerTarget, c.expectPeerTarget)
		}
		if settings.ConnManagerLowWater != c.expectLowWater {
			t.Errorf(
				"%q: low water = %d, expected %d",
				c.role,
				settings.ConnManagerLowWater,
				c.expectLowWater,
			)
		}
		if settings.ConnManagerHighWater != c.expectHighWater {
			t.Errorf(
				"%q: high water = %d, expected %d",
				c.role,
				settings.ConnManagerHighWater,
				c.expectHighWater,
			)
		}
		// the timeouts and the clock do not vary by role
		if settings.PeerTimeout != 60*time.Second {
			t.Errorf("%q: peer timeout = %s, expected 60s", c.role, settings.PeerTimeout)
		}
		if settings.PeerCoalesceTimeout != 1*time.Second {
			t.Errorf("%q: peer coalesce timeout = %s, expected 1s", c.role, settings.PeerCoalesceTimeout)
		}
		if settings.DialTimeout != 30*time.Second {
			t.Errorf("%q: dial timeout = %s, expected 30s", c.role, settings.DialTimeout)
		}
		if settings.StatusTimeout != 5*time.Second {
			t.Errorf("%q: status timeout = %s, expected 5s", c.role, settings.StatusTimeout)
		}
		if settings.Now == nil {
			t.Errorf("%q: the node has no clock", c.role)
		}
	}
}

// A node without the three things it cannot invent is refused at construction
// rather than half built (D1).
func TestGossipNewNodeRefusesIncompleteSettings(t *testing.T) {
	rootKey := newTestKey(t)
	directory := newTestDirectory(t, rootKey)

	cases := []struct {
		name     string
		settings *NodeSettings
		expect   string
	}{
		{name: "no settings", settings: nil, expect: "the gossip node needs settings"},
		{
			name: "no directory",
			settings: &NodeSettings{
				NetworkHost: testNetworkHost,
			},
			expect: "the gossip node needs a directory",
		},
		{
			name: "no network host",
			settings: &NodeSettings{
				Directory: directory,
			},
			expect: "the gossip node needs a network host",
		},
	}
	for _, c := range cases {
		node, err := NewNode(context.Background(), c.settings)
		if err == nil {
			node.Close()
			t.Errorf("%s: a node was built", c.name)
			continue
		}
		if err.Error() != c.expect {
			t.Errorf("%s: error = %v, expected %q", c.name, err, c.expect)
		}
	}
}

// The settings are copied before they are defaulted, so the caller's struct is
// still the caller's after the node is up (D1).
func TestGossipNewNodeDoesNotMutateTheCallerSettings(t *testing.T) {
	rootKey := newTestKey(t)
	settings := &NodeSettings{
		NetworkHost: testNetworkHost,
		Directory:   newTestDirectory(t, rootKey),
		Role:        NodeRoleMember,
	}
	node, err := NewNode(context.Background(), settings)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(node.Close)

	if settings.Now != nil {
		t.Errorf("the caller's clock was filled in")
	}
	if settings.PeerTimeout != 0 {
		t.Errorf("the caller's peer timeout was filled in with %s", settings.PeerTimeout)
	}
	if settings.DialTimeout != 0 {
		t.Errorf("the caller's dial timeout was filled in with %s", settings.DialTimeout)
	}
	if settings.StatusTimeout != 0 {
		t.Errorf("the caller's status timeout was filled in with %s", settings.StatusTimeout)
	}
	if settings.ConnectSettings != nil {
		t.Errorf("the caller's connect settings were filled in")
	}
	if settings.IdentityKeySeed != nil {
		t.Errorf("the caller's identity seed was filled in")
	}
	// and the node did take the defaults for itself
	if node.settings == settings {
		t.Fatalf("the node kept the caller's settings struct")
	}
	if node.settings.PeerTimeout != 60*time.Second {
		t.Errorf("the node's peer timeout = %s, expected 60s", node.settings.PeerTimeout)
	}
	if node.settings.DialTimeout != 30*time.Second {
		t.Errorf("the node's dial timeout = %s, expected 30s", node.settings.DialTimeout)
	}
	if node.settings.StatusTimeout != 5*time.Second {
		t.Errorf("the node's status timeout = %s, expected 5s", node.settings.StatusTimeout)
	}
	if node.settings.Now == nil {
		t.Errorf("the node has no clock")
	}
	if node.settings.ConnectSettings == nil {
		t.Errorf("the node has no connect settings")
	}
}

// A publish with nothing to publish is refused rather than marshalled into an
// empty message the mesh would relay (D1, D6).
func TestGossipNodePublishNeedsAMessage(t *testing.T) {
	node := newTestLocalNode(t, nil)
	err := node.Publish(context.Background(), nil)
	if err == nil {
		t.Fatal("a publish with no message succeeded")
	}
	if err.Error() != "extender gossip message is missing" {
		t.Fatalf("error = %v, expected the missing message error", err)
	}
}

// An extender learns its public addresses after the node is already up, so a
// family that activates late becomes a listen address then, without disturbing
// the families that are already up (D2, G3).
func TestGossipNodeListenActivatesAFamilyLate(t *testing.T) {
	v4Addr := testMultiaddr(t, "/ip4/192.0.2.31/tcp/9001")
	v6Addr := testMultiaddr(t, "/ip6/2001:db8::31/tcp/9001")
	otherAddr := testMultiaddr(t, "/ip4/198.51.100.31/tcp/9001")

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	listenerSettings := DefaultInProcessListenerSettings()
	listenerSettings.ListenAddrs = []ma.Multiaddr{v4Addr}
	listener := NewInProcessListener(ctx, listenerSettings)
	t.Cleanup(listener.Close)
	node := newTestLocalNode(t, func(settings *NodeSettings) {
		settings.Role = NodeRoleExtender
		settings.ExtenderListener = listener
	})

	if listenAddrs := testListenAddrs(node); !slices.Equal(listenAddrs, []string{v4Addr.String()}) {
		t.Fatalf("listen addrs = %v, expected only the activated family", listenAddrs)
	}

	// the second family activates, and the node should peer now rather than at
	// the end of the round
	wake := node.wakeMonitor.NotifyChannel()
	if err := node.Listen(v6Addr); err != nil {
		t.Fatal(err)
	}
	select {
	case <-wake:
	case <-time.After(testNodeTimeout):
		t.Error("a late listen did not wake the peering round")
	}
	expectBoth := []string{v4Addr.String(), v6Addr.String()}
	slices.Sort(expectBoth)
	if listenAddrs := testListenAddrs(node); !slices.Equal(listenAddrs, expectBoth) {
		t.Fatalf("listen addrs = %v, expected %v", listenAddrs, expectBoth)
	}

	// a family that is already listened is not listened twice, however it is
	// presented
	for _, listenAddrs := range [][]ma.Multiaddr{
		{v6Addr},
		{v4Addr, v6Addr},
		{v6Addr, v6Addr},
		{},
		nil,
	} {
		if err := node.Listen(listenAddrs...); err != nil {
			t.Fatalf("%v: %v", listenAddrs, err)
		}
		if nodeAddrs := testListenAddrs(node); !slices.Equal(nodeAddrs, expectBoth) {
			t.Fatalf("%v: listen addrs = %v, expected %v", listenAddrs, nodeAddrs, expectBoth)
		}
	}

	// a new address repeated within one call is collapsed to one listen
	if err := node.Listen(otherAddr, otherAddr); err != nil {
		t.Fatal(err)
	}
	expectAll := append(slices.Clone(expectBoth), otherAddr.String())
	slices.Sort(expectAll)
	if listenAddrs := testListenAddrs(node); !slices.Equal(listenAddrs, expectAll) {
		t.Fatalf("listen addrs = %v, expected %v", listenAddrs, expectAll)
	}
}

// The operator addresses are replaced whole and handed out as a clone, so the
// sdk's hello can hand the node a slice it keeps using (D3, C7).
func TestGossipNodeSetOperatorAddrsReplacesAndClones(t *testing.T) {
	firstAddr := testMultiaddr(t, "/dns/gossip.space.example/tcp/443/wss")
	secondAddr := testMultiaddr(t, "/dns/other.space.example/tcp/443/wss")
	thirdAddr := testMultiaddr(t, "/ip4/192.0.2.41/tcp/443/ws")

	constructedAddrs := []ma.Multiaddr{firstAddr}
	node := newTestLocalNode(t, func(settings *NodeSettings) {
		settings.OperatorAddrs = constructedAddrs
	})

	// the construction slice is cloned, so writing to it changes nothing
	constructedAddrs[0] = secondAddr
	if operatorAddrs := node.OperatorAddrs(); len(operatorAddrs) != 1 || !operatorAddrs[0].Equal(firstAddr) {
		t.Fatalf("operator addrs = %v, expected %v", operatorAddrs, firstAddr)
	}

	replacementAddrs := []ma.Multiaddr{secondAddr, thirdAddr}
	node.SetOperatorAddrs(replacementAddrs)
	operatorAddrs := node.OperatorAddrs()
	if len(operatorAddrs) != 2 ||
		!operatorAddrs[0].Equal(secondAddr) ||
		!operatorAddrs[1].Equal(thirdAddr) {
		t.Fatalf("operator addrs = %v, expected the replacement", operatorAddrs)
	}

	// neither the caller's slice nor the clone it was handed back reaches the
	// node
	replacementAddrs[0] = firstAddr
	operatorAddrs[1] = firstAddr
	heldAddrs := node.OperatorAddrs()
	if len(heldAddrs) != 2 || !heldAddrs[0].Equal(secondAddr) || !heldAddrs[1].Equal(thirdAddr) {
		t.Fatalf("operator addrs = %v, expected the replacement", heldAddrs)
	}

	// replacing with nothing is an operator link the node no longer has
	node.SetOperatorAddrs(nil)
	if operatorAddrs := node.OperatorAddrs(); len(operatorAddrs) != 0 {
		t.Fatalf("operator addrs = %v, expected none", operatorAddrs)
	}
}

// Close joins every loop it started and is safe to call again, so an owner
// that closes a node knows nothing of it is still running (D1).
func TestGossipNodeCloseJoinsItsLoops(t *testing.T) {
	node := newTestLocalNode(t, nil)

	closed := make(chan struct{})
	go func() {
		defer close(closed)
		node.Close()
	}()
	select {
	case <-closed:
	case <-time.After(testNodeTimeout):
		t.Fatal("close did not return")
	}

	// every loop the constructor started has ended
	for _, done := range []struct {
		name string
		done chan struct{}
	}{
		{name: "peering", done: node.peeringDone},
		{name: "receive", done: node.receiveDone},
		{name: "topic events", done: node.topicEventsDone},
		{name: "status", done: node.statusDone},
	} {
		select {
		case <-done.done:
		default:
			t.Errorf("the %s loop is still running", done.name)
		}
	}

	// a second close joins nothing and returns at once
	closedAgain := make(chan struct{})
	go func() {
		defer close(closedAgain)
		node.Close()
	}()
	select {
	case <-closedAgain:
	case <-time.After(testNodeTimeout):
		t.Fatal("a second close did not return")
	}
}

// One node with no operator and an empty directory, so its peering round has
// nothing to dial and only a wake can drive another one.
func newTestLocalNode(t *testing.T, configure func(settings *NodeSettings)) *Node {
	t.Helper()
	settings := DefaultNodeSettings(NodeRoleMember)
	settings.NetworkHost = testNetworkHost
	settings.Directory = newTestDirectory(t, newTestKey(t))
	settings.IdentityKeySeed = newTestKey(t).seed
	settings.PeerTimeout = 5 * time.Minute
	settings.StatusTimeout = 5 * time.Minute
	settings.RandSeed = 1
	if configure != nil {
		configure(settings)
	}
	node, err := NewNode(context.Background(), settings)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(node.Close)
	return node
}

// The addresses the swarm actually bound, sorted, because the swarm holds its
// listeners in a map and the order carries no meaning.
func testListenAddrs(node *Node) []string {
	listenAddrs := []string{}
	for _, listenAddr := range node.ListenAddrs() {
		listenAddrs = append(listenAddrs, listenAddr.String())
	}
	slices.Sort(listenAddrs)
	return listenAddrs
}

// One multiaddr the test names by its string form.
func testMultiaddr(t *testing.T, addr string) ma.Multiaddr {
	t.Helper()
	multiaddr, err := ma.NewMultiaddr(addr)
	if err != nil {
		t.Fatal(err)
	}
	return multiaddr
}
