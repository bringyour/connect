// The topic validator's trust boundary (EXTENDER.md D1).
//
// The validator is what stops a forgery before it is relayed: a message it
// accepts costs the whole mesh, and one it rejects goes no further. These tests
// drive it directly, with no host and no topic, because the decision is a pure
// function of the message and the directory's key set.

package gossip

import (
	"context"
	"testing"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect"
	"github.com/urnetwork/connect/protocol"
)

// A revocation is judged exactly like a record: it must be signed by a root
// key the directory accepts and it must name this network space, or it is
// rejected before it is relayed (D1).
func TestGossipValidatorAcceptsOnlyThisSpacesSignedRevocations(t *testing.T) {
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
	cases := []struct {
		name        string
		rootKey     *testKey
		networkHost string
		expect      pubsub.ValidationResult
	}{
		{
			name:        "this space, signed by the root key",
			rootKey:     rootKey,
			networkHost: testNetworkHost,
			expect:      pubsub.ValidationAccept,
		},
		// the root key of another space signs nothing this space relays, even
		// for a host name this space owns
		{
			name:        "another root key",
			rootKey:     otherRootKey,
			networkHost: testNetworkHost,
			expect:      pubsub.ValidationReject,
		},
		// a revocation of another space is not this space's to relay, even
		// under a key this space accepts
		{
			name:        "another network host",
			rootKey:     rootKey,
			networkHost: "other.example",
			expect:      pubsub.ValidationReject,
		},
		{
			name:        "no network host",
			rootKey:     rootKey,
			networkHost: "",
			expect:      pubsub.ValidationReject,
		},
	}
	for _, c := range cases {
		revocation, err := connect.SignExtenderRevocation(
			c.rootKey.privateKey,
			&protocol.ExtenderRevocationBody{
				PublicKey:   extenderKey.publicKey,
				IssueTimeMs: uint64(now.UnixMilli()),
				NetworkHost: c.networkHost,
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		result := testValidate(t, node, &protocol.ExtenderGossipMessage{
			Message: &protocol.ExtenderGossipMessage_Revocation{Revocation: revocation},
		})
		if result != c.expect {
			t.Errorf("%s: validation = %v, expected %v", c.name, result, c.expect)
		}
	}
}

// The validator reads the directory's key set on every message, so a root key
// rotation takes effect on the next message rather than at the next restart
// (B4, D1).
func TestGossipValidatorFollowsARootKeyRotation(t *testing.T) {
	rootKey := newTestKey(t)
	rotatedRootKey := newTestKey(t)
	extenderKey := newTestKey(t)
	directory := newTestDirectory(t, rootKey)
	node := &Node{
		settings: &NodeSettings{
			NetworkHost: testNetworkHost,
			Directory:   directory,
		},
	}

	now := time.Now()
	heldMessage := &protocol.ExtenderGossipMessage{
		Message: &protocol.ExtenderGossipMessage_Record{
			Record: signTestRecord(t, rootKey, extenderKey, "192.0.2.21", 8443, now),
		},
	}
	rotatedMessage := &protocol.ExtenderGossipMessage{
		Message: &protocol.ExtenderGossipMessage_Record{
			Record: signTestRecord(t, rotatedRootKey, extenderKey, "192.0.2.22", 8443, now),
		},
	}

	if result := testValidate(t, node, heldMessage); result != pubsub.ValidationAccept {
		t.Fatalf("the held key was rejected before the rotation: %v", result)
	}
	if result := testValidate(t, node, rotatedMessage); result != pubsub.ValidationReject {
		t.Fatalf("the rotated key was accepted before the rotation: %v", result)
	}

	directory.SetRootKeys(connect.NewExtenderRootKeySet(rotatedRootKey.publicKey))

	// the next message is judged under the new key set, both ways round
	if result := testValidate(t, node, rotatedMessage); result != pubsub.ValidationAccept {
		t.Fatalf("the rotated key was rejected after the rotation: %v", result)
	}
	if result := testValidate(t, node, heldMessage); result != pubsub.ValidationReject {
		t.Fatalf("the retired key was accepted after the rotation: %v", result)
	}
}

// Runs one message through the validator the way pubsub delivers it.
func testValidate(
	t *testing.T,
	node *Node,
	message *protocol.ExtenderGossipMessage,
) pubsub.ValidationResult {
	t.Helper()
	messageBytes, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return node.validate(context.Background(), "", &pubsub.Message{
		Message: &pubsubpb.Message{Data: messageBytes},
	})
}
