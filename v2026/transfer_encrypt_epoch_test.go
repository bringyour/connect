package connect

// Tests for epoch identity on EncryptedControl (see EncryptedControl.epoch_id).
//
// The generation on the wire exists so a control can be routed to the epoch
// that produced it. Without it, a proof bound to the peer's NEW exporter was
// judged against our OLD one, failed as a bad signature, and terminally
// tombstoned a session the peer was still encrypting into — a permanent stall
// (see repro-epochdesync). These cover the routing rules:
//   - a proof for a NEWER generation converges (re-handshake), never tombstones
//   - a proof for an OLDER generation is ignored, never tombstones
//   - handshake bytes follow the same ordering rule
//   - a legacy (unset) generation keeps the pre-epoch behavior
//   - the responder adopts the initiator's generation and echoes it

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/v2026/protocol"
)

func injectTestEpochWithId(sess *peerEncryptionSession, completed bool, handshakeErr error, epochId Id) *tlsHandshakeEpoch {
	e := injectTestEpoch(sess, completed, handshakeErr)
	sess.stateLock.Lock()
	e.epochId = epochId
	sess.stateLock.Unlock()
	return e
}

// olderId/newerId build ulid-ordered generations around a reference.
func olderAndNewerIds(t *testing.T) (older Id, newer Id) {
	t.Helper()
	older = NewId()
	// ulids are time-ordered; a later mint compares greater
	time.Sleep(2 * time.Millisecond)
	newer = NewId()
	if !older.LessThan(newer) {
		t.Fatalf("expected ulid ordering: %s < %s", older, newer)
	}
	return older, newer
}

// TestIdentityProofForNewerEpochConverges: the peer re-handshaked, so its proof
// names a newer generation. We must rebuild onto that generation — and must NOT
// mark this epoch identity-failed (the old behavior, which stalled the session
// permanently while the peer kept encrypting).
func TestIdentityProofForNewerEpochConverges(t *testing.T) {
	older, newer := olderAndNewerIds(t)

	sess, cleanup := newTestEncryptionSession(t, sequenceTlsRoleClient)
	defer cleanup()
	e1 := injectTestEpochWithId(sess, true, nil, older)

	sess.receivePeerIdentityProofForEpoch(make([]byte, 64), newer)

	if sess.currentEpoch() == e1 {
		t.Fatal("expected a newer-generation proof to converge onto a fresh epoch")
	}
	sess.stateLock.Lock()
	failed := e1.identityFailed
	sess.stateLock.Unlock()
	if failed {
		t.Fatal("a foreign-generation proof must never mark this epoch identity-failed")
	}
}

// TestIdentityProofForOlderEpochIgnored: a straggling proof from a superseded
// generation is dropped — it neither tombstones nor resets the live epoch.
func TestIdentityProofForOlderEpochIgnored(t *testing.T) {
	older, newer := olderAndNewerIds(t)

	sess, cleanup := newTestEncryptionSession(t, sequenceTlsRoleClient)
	defer cleanup()
	e1 := injectTestEpochWithId(sess, true, nil, newer)

	sess.receivePeerIdentityProofForEpoch(make([]byte, 64), older)

	if sess.currentEpoch() != e1 {
		t.Fatal("expected a stale-generation proof to leave the live epoch alone")
	}
	sess.stateLock.Lock()
	failed := e1.identityFailed
	sess.stateLock.Unlock()
	if failed {
		t.Fatal("a stale-generation proof must never mark the epoch identity-failed")
	}
}

// TestHandshakeForOlderEpochDropped: straggling handshake bytes from a
// superseded generation must not be fed to the live epoch's TLS state.
func TestHandshakeForOlderEpochDropped(t *testing.T) {
	older, newer := olderAndNewerIds(t)
	clientHello := []byte{22, 3, 3, 0, 100, 1, 0, 0, 96}

	sess, cleanup := newTestEncryptionSession(t, sequenceTlsRoleServer)
	defer cleanup()
	e1 := injectTestEpochWithId(sess, true, nil, newer)

	sess.deliverHandshake(clientHello, older)

	if sess.currentEpoch() != e1 {
		t.Fatal("expected stale-generation handshake bytes to be dropped without reset")
	}
}

// TestHandshakeForNewerEpochResets: the peer restarted; its ClientHello names a
// newer generation, so we reset onto it even though our epoch is live.
func TestHandshakeForNewerEpochResets(t *testing.T) {
	older, newer := olderAndNewerIds(t)
	clientHello := []byte{22, 3, 3, 0, 100, 1, 0, 0, 96}

	sess, cleanup := newTestEncryptionSession(t, sequenceTlsRoleServer)
	defer cleanup()
	e1 := injectTestEpochWithId(sess, false, nil, older)

	sess.deliverHandshake(clientHello, newer)

	if sess.currentEpoch() == e1 {
		t.Fatal("expected a newer-generation ClientHello to reset onto the peer's generation")
	}
}

// TestLegacyUnsetEpochKeepsPriorBehavior: a peer that predates epoch_id sends
// controls with no generation; those must behave exactly as before (the
// completed-server-handshake ClientHello reset).
func TestLegacyUnsetEpochKeepsPriorBehavior(t *testing.T) {
	clientHello := []byte{22, 3, 3, 0, 100, 1, 0, 0, 96}

	sess, cleanup := newTestEncryptionSession(t, sequenceTlsRoleServer)
	defer cleanup()
	e1 := injectTestEpochWithId(sess, true, nil, NewId())

	sess.deliverHandshake(clientHello, Id{})

	if sess.currentEpoch() == e1 {
		t.Fatal("expected legacy (unset generation) delivery to keep the prior reset behavior")
	}

	// and a legacy proof still routes to the current epoch's verification path
	e2 := injectTestEpochWithId(sess, true, errors.New("x"), NewId())
	sess.receivePeerIdentityProofForEpoch(make([]byte, 64), Id{})
	if sess.currentEpoch() != e2 {
		t.Fatal("expected a legacy proof to leave the epoch in place")
	}
}

// TestResponderAdoptsInitiatorEpoch: the TLS-server role mints no generation of
// its own — it adopts the initiator's from the inbound handshake, so both sides
// name the same epoch and the proof exchange matches.
func TestResponderAdoptsInitiatorEpoch(t *testing.T) {
	clientHello := []byte{22, 3, 3, 0, 100, 1, 0, 0, 96}
	initiator := NewId()

	sess, cleanup := newTestEncryptionSession(t, sequenceTlsRoleServer)
	defer cleanup()

	sess.deliverHandshake(clientHello, initiator)

	e := sess.currentEpoch()
	if e == nil {
		t.Fatal("expected an epoch after inbound handshake")
	}
	if got := sess.epochIdOf(e); got != initiator {
		t.Fatalf("responder epoch id = %s, want the initiator's %s", got, initiator)
	}

	// a proof for that same generation is now in-generation (no reset)
	sess.receivePeerIdentityProofForEpoch(make([]byte, 64), initiator)
	if sess.currentEpoch() != e {
		t.Fatal("expected an in-generation proof to keep the adopted epoch")
	}
}

// TestClientRoleMintsEpochId: the initiator names every generation it builds,
// so its controls always carry one.
func TestClientRoleMintsEpochId(t *testing.T) {
	sess, cleanup := newTestEncryptionSession(t, sequenceTlsRoleClient)
	defer cleanup()

	sess.reset()
	e := sess.currentEpoch()
	if e == nil {
		t.Fatal("expected an epoch after reset")
	}
	first := sess.epochIdOf(e)
	if first == (Id{}) {
		t.Fatal("client-role epoch must mint a generation id")
	}

	sess.reset()
	second := sess.epochIdOf(sess.currentEpoch())
	if second == (Id{}) || second == first {
		t.Fatalf("each generation needs a distinct id: %s then %s", first, second)
	}
	if !first.LessThan(second) {
		t.Fatalf("expected monotonic generations: %s then %s", first, second)
	}
}

// TestStaleEpochProofDoesNotConsumePendingSlot locks the invariant whose
// violation caused the new-instance stall: an epoch holds ONE pending
// identity-proof slot, and a proof from a foreign generation must never
// occupy it. If it does, the real proof is refused as "already buffered" and
// the stale one is later verified against this epoch's exporter, fails, and
// terminally tombstones a session the peer is still encrypting into.
//
// The regression arrived through the OPTIMISTIC delivery path (Client.receive
// applies EncryptedControl before the in-order drain), which delivered proofs
// without their generation. Both paths must carry it.
func TestStaleEpochProofDoesNotConsumePendingSlot(t *testing.T) {
	older, newer := olderAndNewerIds(t)

	sess, cleanup := newTestEncryptionSession(t, sequenceTlsRoleClient)
	defer cleanup()
	e := injectTestEpochWithId(sess, true, nil, newer)
	// an exporter exists, so a same-generation proof would be verifiable
	sess.stateLock.Lock()
	e.tlsExporter = make([]byte, 32)
	sess.stateLock.Unlock()

	// a proof from the superseded generation
	sess.receivePeerIdentityProofForEpoch(make([]byte, 64), older)

	sess.stateLock.Lock()
	pending := len(e.pendingPeerIdentityProof)
	failed := e.identityFailed
	sess.stateLock.Unlock()
	if pending != 0 {
		t.Fatal("a stale-generation proof must not occupy the pending slot")
	}
	if failed {
		t.Fatal("a stale-generation proof must not mark the epoch identity-failed")
	}

	// the slot stays available for this generation's proof
	sess.receivePeerIdentityProofForEpoch(make([]byte, 64), newer)
	sess.stateLock.Lock()
	pending = len(e.pendingPeerIdentityProof)
	sess.stateLock.Unlock()
	if pending == 0 {
		t.Fatal("the current generation's proof must be accepted into the pending slot")
	}
}

// TestOptimisticIdentityProofCarriesEpoch drives the OPTIMISTIC EncryptedControl
// path in `Client.receive` — the fast path that applies a control before the
// in-order ReceiveSequence drain — and asserts it honors the generation.
//
// This is the regression test for the delivery path that the epoch fix
// originally missed. The in-order path checked the generation, the optimistic
// path did not, and a stale proof arriving on the fast path took the epoch's
// single pending-proof slot: the real proof was then refused as "already
// buffered", the stale one was finally verified against this epoch's exporter,
// failed, and terminally tombstoned a session the peer was still encrypting
// into (see the WithNewInstance stall).
//
// It must fail if `Client.receive` stops passing the epoch: with no generation
// the stale proof is buffered, and `pendingPeerIdentityProof` becomes non-empty.
func TestOptimisticIdentityProofCarriesEpoch(t *testing.T) {
	older, newer := olderAndNewerIds(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOpportunistic
	settings.EncryptionSettings.TlsTimeout = 5 * time.Second

	peerId := NewId()
	receiver := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer receiver.Cancel()

	toReceiver := make(chan []byte, 8)
	receiver.RouteManager().UpdateTransport(NewReceiveGatewayTransport(), []Route{toReceiver})
	receiver.ContractManager().AddNoContractPeer(peerId)

	// the local session the peer's client-role control routes into: the peer
	// sends as client, so it drives our server session
	sess := receiver.EncryptionSessionManager().getOrCreate(peerId, sequenceTlsRoleServer, false)
	if sess == nil {
		t.Fatal("expected a per-peer session")
	}
	// hold a live generation with an exporter, so a same-generation proof
	// would be buffered for verification
	e := injectTestEpochWithId(sess, true, nil, newer)
	sess.stateLock.Lock()
	e.tlsExporter = make([]byte, 32)
	sess.stateLock.Unlock()

	// buildProofFrame renders a peer identity-proof control for `epochId` on
	// the wire exactly as the peer would send it: a plaintext pack carrying
	// one EncryptedControl.
	buildProofFrame := func(epochId Id) []byte {
		ec := &protocol.EncryptedControl{
			ControlType: protocol.EncryptedControlType_EncryptedControlIdentityProof,
			Payload:     make([]byte, 64),
			SessionRole: protocol.SequenceRole_SequenceRoleClient,
			Companion:   false,
			EpochId:     epochId.Bytes(),
		}
		ecBytes, err := proto.Marshal(ec)
		if err != nil {
			t.Fatal(err)
		}
		packBytes, err := proto.Marshal(&protocol.Pack{
			MessageId:      NewId().Bytes(),
			SequenceId:     NewId().Bytes(),
			SequenceNumber: 0,
			Head:           true,
			Nack:           true,
			Frames: []*protocol.Frame{{
				MessageType:  protocol.MessageType_TransferEncryptedControl,
				MessageBytes: ecBytes,
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
		messageType := protocol.MessageType_TransferPack
		transferFrameBytes, err := proto.Marshal(&protocol.TransferFrame{
			TransferPath: TransferPath{
				SourceId:      peerId,
				DestinationId: receiver.ClientId(),
			}.ToProtobuf(),
			MessageType: &messageType,
			Frame: &protocol.Frame{
				MessageType:  protocol.MessageType_TransferPack,
				MessageBytes: packBytes,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return transferFrameBytes
	}

	deliver := func(transferFrameBytes []byte) {
		select {
		case toReceiver <- transferFrameBytes:
		case <-time.After(5 * time.Second):
			t.Fatal("route write timed out")
		}
	}

	pendingProof := func() (int, bool) {
		sess.stateLock.Lock()
		defer sess.stateLock.Unlock()
		return len(e.pendingPeerIdentityProof), e.identityFailed
	}

	// 1. the superseded generation's proof must never reach this epoch. With
	//    the generation dropped on this path it is buffered instead, taking the
	//    slot the real proof needs.
	deliver(buildProofFrame(older))
	staleDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(staleDeadline) {
		if pending, failed := pendingProof(); 0 < pending || failed {
			t.Fatalf(
				"stale-generation proof reached the epoch through the optimistic path (pending=%d failed=%t): the path is not carrying the generation",
				pending, failed,
			)
		}
		time.Sleep(25 * time.Millisecond)
	}

	// 2. positive control: this generation's proof MUST arrive, proving the
	//    frame shape and the optimistic path are live — without this a broken
	//    delivery would make step 1 pass vacuously.
	deliver(buildProofFrame(newer))
	liveDeadline := time.Now().Add(10 * time.Second)
	for {
		if pending, _ := pendingProof(); 0 < pending {
			break
		}
		if !time.Now().Before(liveDeadline) {
			t.Fatal("current-generation proof never reached the epoch: the optimistic path is not delivering, so this test proves nothing")
		}
		time.Sleep(25 * time.Millisecond)
	}
}
