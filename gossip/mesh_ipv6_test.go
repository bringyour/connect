// The mesh on the v6 family (EXTENDER.md D1, D2, IPV6.md A4).
//
// The same fixture as mesh_test.go with every extender bound on the v6
// loopback and publishing an RFC 3849 documentation address, which the dial
// seam maps back to `::1`. A v6-only host reaches the mesh exactly the way a
// v4 host does, so this is the v4 relay test with the family swapped, plus the
// dial that proves the extender transport carried it.

package gossip

import (
	"testing"
	"time"

	"github.com/urnetwork/connect"
	"github.com/urnetwork/connect/protocol"
)

// A record the operator publishes for a v6 extender reaches every directory,
// and the member dials the v6 address that record names (D1, D2, D6).
func TestGossipMeshRelaysSignedRecordsOverIpv6(t *testing.T) {
	mesh := newTestMeshFamily(t, 1, 6)
	mesh.waitForTopic(t)

	extenderIp := mesh.extenders[0].ip
	if !extenderIp.Is6() {
		t.Fatalf("the extender published %s, expected a v6 address", extenderIp)
	}

	record := mesh.signRecord(0, time.Now())
	body, err := mesh.memberDirectory.RootKeys().VerifyRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if len(body.Addresses) != 1 {
		t.Fatalf("the record carries %v, expected one address", body.Addresses)
	}
	if body.Addresses[0].Ip != extenderIp.String() || body.Addresses[0].IpVersion != 6 {
		t.Fatalf("the record carries %v, expected v6 %s", body.Addresses[0], extenderIp)
	}

	mesh.publish(&protocol.ExtenderGossipMessage{
		Message: &protocol.ExtenderGossipMessage_Record{Record: record},
	})
	for _, directory := range mesh.directories() {
		waitForDirectory(t, directory, "the published v6 record", func(snapshot *connect.ExtenderDirectorySnapshot) bool {
			return snapshotState(snapshot, extenderIp) == connect.ExtenderStateActive
		})
	}

	// the directory change is the only thing that can drive this round, and
	// the only address it learned is the v6 one, so the member's second peer
	// is a v6 extender dial
	waitForNodeStatus(t, mesh.member, "the v6 extender link", func(status NodeStatus) bool {
		return 2 <= status.PeerCount
	})
	waitForNodeStatus(t, mesh.extenders[0].node, "the member link", func(status NodeStatus) bool {
		return 2 <= status.PeerCount
	})
}
