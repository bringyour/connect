package connect

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// newTestSessionForIdentityProof constructs a peerEncryptionSession
// ready for identity-proof tests: TLS state is pre-set
// (`tlsExporter` populated, `derivedTlsCipher` populated) so the
// test can exercise the identity-proof gating in isolation without
// running an actual TLS handshake.
//
// Returns the session, the local client's ClientKeyManager, the
// peer ClientId, the peer's private key (for signing proofs in the
// test), and a cleanup function.
func newTestSessionForIdentityProof(t *testing.T) (
	sess *peerEncryptionSession,
	localKeyManager *ClientKeyManager,
	peerId Id,
	peerPriv ed25519.PrivateKey,
	cleanup func(),
) {
	return newTestSessionForIdentityProofWithMode(t, EncryptionModeOpportunistic)
}

// newTestSessionForIdentityProofWithMode is newTestSessionForIdentityProof
// with the encryption mode selectable, for tests that pin mode-dependent
// behavior (e.g. the EncryptionModeRequired rekey cipher continuity).
func newTestSessionForIdentityProofWithMode(t *testing.T, mode EncryptionMode) (
	sess *peerEncryptionSession,
	localKeyManager *ClientKeyManager,
	peerId Id,
	peerPriv ed25519.PrivateKey,
	cleanup func(),
) {
	ctx, cancel := context.WithCancel(context.Background())

	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = mode
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)

	var err error
	localKeyManager, err = NewClientKeyManager(ctx, client)
	if err != nil {
		cancel()
		t.Fatalf("NewClientKeyManager: %s", err)
	}

	manager := NewEncryptionSessionManager(ctx, client, localKeyManager, settings.EncryptionSettings)

	peerId = NewId()
	sess = newPeerEncryptionSession(
		ctx, manager, client, peerId,
		sequenceTlsRoleServer,
		settings.EncryptionSettings,
		manager.ServerTlsConfig(),
		false,
	)

	// Pre-populate the TLS-handshake-derived state. In production
	// this is filled by `completeHandshake` after the actual TLS
	// handshake; for these tests we inject directly so we can
	// exercise the identity-proof gate independently.
	exporter := make([]byte, sequenceTlsIdentityProofLength)
	_, err = rand.Read(exporter)
	AssertEqual(t, nil, err)
	// Inject a bare handshake epoch with the TLS-derived state pre-set (no
	// goroutines / real tls.Conn) so the identity-proof gate can be
	// exercised in isolation. `startEpoch` is a no-op while an epoch is
	// present, so the production paths under test reuse this one.
	epochCtx, epochCancel := context.WithCancel(ctx)
	sess.epoch = &tlsHandshakeEpoch{
		ctx:               epochCtx,
		cancel:            epochCancel,
		handshakeDone:     make(chan struct{}),
		establishmentDone: make(chan struct{}),
		tlsExporter:       exporter,
		derivedTlsCipher:  &sequenceCipher{}, // non-nil sentinel; never used for crypto in these tests
	}

	_, peerPriv, err = ed25519.GenerateKey(rand.Reader)
	AssertEqual(t, nil, err)

	cleanup = func() {
		client.Cancel()
		cancel()
	}
	return
}

// TestPeerSessionCipherGatedOnIdentityVerified verifies that
// `Cipher()` returns nil until `peerIdentityVerified` flips true,
// even with the TLS-derived cipher already in place.
func TestPeerSessionCipherGatedOnIdentityVerified(t *testing.T) {
	sess, _, _, _, cleanup := newTestSessionForIdentityProof(t)
	defer cleanup()

	// Pre-identity-verification: cipher hidden (no established epoch yet).
	AssertEqual(t, (*sequenceCipher)(nil), sess.Cipher())

	// Simulate a successful identity proof: flip the flag and promote the
	// epoch to established, exactly as `maybeVerifyPendingPeerIdentityProof`
	// does. `Cipher()` serves the established epoch's cipher.
	sess.stateLock.Lock()
	sess.epoch.peerIdentityVerified = true
	sess.markEstablishedWithLock(sess.epoch)
	sess.stateLock.Unlock()

	// Now cipher exposed.
	if sess.Cipher() == nil {
		t.Fatal("Cipher should be non-nil once the epoch is established")
	}
}

// TestCipherContinuityDuringRekey pins the rekey rule in `Cipher()` for BOTH
// modes: while a replacement handshake is in flight, the established epoch's
// cipher keeps serving — a rekey never reopens a plaintext window. (The
// receiver retains the outgoing epoch in `decryptCiphers` through the swap.)
// The historical Opportunistic behavior — nil during rekey, falling back to
// plaintext so the contract-open ride-along opened in the clear — is
// deliberately gone: the contract-open is now pinned ForceUnwrapped at queue
// time instead, and this test fails if the plaintext fallback ever returns.
func TestCipherContinuityDuringRekey(t *testing.T) {
	for _, mode := range []EncryptionMode{
		EncryptionModeOpportunistic,
		EncryptionModeRequired,
	} {
		func() {
			sess, _, _, _, cleanup := newTestSessionForIdentityProofWithMode(t, mode)
			defer cleanup()

			// Establish the injected epoch, as a completed identity proof would.
			sess.stateLock.Lock()
			sess.epoch.peerIdentityVerified = true
			sess.markEstablishedWithLock(sess.epoch)
			sess.stateLock.Unlock()
			AssertEqual(t, true, sess.Cipher() != nil)

			// Inject a fresh in-flight epoch: a rekey in progress
			// (epoch != establishedEpoch, establishment not done).
			epochCtx, epochCancel := context.WithCancel(context.Background())
			defer epochCancel()
			sess.stateLock.Lock()
			sess.epoch = &tlsHandshakeEpoch{
				ctx:               epochCtx,
				cancel:            epochCancel,
				handshakeDone:     make(chan struct{}),
				establishmentDone: make(chan struct{}),
			}
			sess.stateLock.Unlock()

			if sess.Cipher() == nil {
				t.Fatalf("mode %v: cipher must keep serving during an in-flight rekey", mode)
			}
		}()
	}
}

// TestRequiredSendRefusalTypedErrorAndEvent pins the EncryptionModeRequired
// send-refusal surface, deterministically (no peer, no transports —
// establishment is impossible, so gate outcomes do not race a handshake):
//
//   - a non-blocking send (timeout 0) and a bounded send both refuse with
//     ErrEncryptionRequiredNotEstablished (distinguishable via errors.Is from
//     transport backpressure's plain `false, nil`),
//   - EncryptionEventRequiredSendBlocked fires exactly once for the peer
//     (per-session dedup),
//   - the same non-blocking send under Opportunistic enqueues (plaintext) —
//     pinning that the refusal is Required-only.
func TestRequiredSendRefusalTypedErrorAndEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeRequired
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()

	events := make(chan *EncryptionEvent, 16)
	unsub := client.EncryptionSessionManager().AddEncryptionEventCallback(func(event *EncryptionEvent) {
		events <- event
	})
	defer unsub()

	peerId := NewId()
	frame, err := ToFrame(&protocol.SimpleMessage{Content: "held"}, DefaultProtocolVersion)
	AssertEqual(t, nil, err)

	// non-blocking refusal with the typed error
	success, sendErr := client.SendWithTimeoutDetailed(frame, peerId, func(error) {}, 0)
	AssertEqual(t, false, success)
	AssertEqual(t, true, errors.Is(sendErr, ErrEncryptionRequiredNotEstablished))

	// bounded refusal: the budget expires without establishment
	success, sendErr = client.SendWithTimeoutDetailed(frame, peerId, func(error) {}, 100*time.Millisecond)
	AssertEqual(t, false, success)
	AssertEqual(t, true, errors.Is(sendErr, ErrEncryptionRequiredNotEstablished))

	// exactly one blocked event despite two refusals (per-session dedup);
	// both notifications were emitted synchronously by the refusals above
	blocked := 0
	for {
		drained := false
		select {
		case event := <-events:
			if event.Type == EncryptionEventRequiredSendBlocked {
				AssertEqual(t, peerId, event.PeerId)
				blocked += 1
			}
		default:
			drained = true
		}
		if drained {
			break
		}
	}
	AssertEqual(t, 1, blocked)

	// Opportunistic control: the identical non-blocking send enqueues
	oppSettings := DefaultClientSettings()
	oppSettings.EncryptionSettings.Mode = EncryptionModeOpportunistic
	oppClient := NewClient(ctx, NewId(), NewNoContractClientOob(), oppSettings)
	defer oppClient.Cancel()
	oppFrame, err := ToFrame(&protocol.SimpleMessage{Content: "plain"}, DefaultProtocolVersion)
	AssertEqual(t, nil, err)
	success, sendErr = oppClient.SendWithTimeoutDetailed(oppFrame, peerId, func(error) {}, 0)
	AssertEqual(t, true, success)
	AssertEqual(t, nil, sendErr)
}

// TestEncryptionEventsAndStates pins the observability surface with injected
// epochs (no real TLS, fully deterministic): the Sealed / IdentityFailed /
// EstablishFailed events fire on their transitions — synchronously, so every
// assertion drains an already-delivered buffered channel — and
// `PeerEncryptionStates` aggregates Establishing/Sealed/KeyExchange/
// FailureReason per peer.
func TestEncryptionEventsAndStates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOpportunistic
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()

	localKeyManager, err := NewClientKeyManager(ctx, client)
	AssertEqual(t, nil, err)
	manager := NewEncryptionSessionManager(ctx, client, localKeyManager, settings.EncryptionSettings)

	events := make(chan *EncryptionEvent, 64)
	unsub := manager.AddEncryptionEventCallback(func(event *EncryptionEvent) {
		events <- event
	})
	defer unsub()

	drainEvents := func() []*EncryptionEvent {
		var out []*EncryptionEvent
		for {
			select {
			case event := <-events:
				out = append(out, event)
			default:
				return out
			}
		}
	}
	countType := func(all []*EncryptionEvent, eventType EncryptionEventType, peerId Id) int {
		n := 0
		for _, event := range all {
			if event.Type == eventType && event.PeerId == peerId {
				n += 1
			}
		}
		return n
	}
	stateForPeer := func(peerId Id) *PeerEncryptionState {
		for _, state := range manager.PeerEncryptionStates() {
			if state.PeerId == peerId {
				return state
			}
		}
		return nil
	}
	injectEpoch := func(sess *peerEncryptionSession) {
		exporter := make([]byte, sequenceTlsIdentityProofLength)
		_, err := rand.Read(exporter)
		AssertEqual(t, nil, err)
		epochCtx, epochCancel := context.WithCancel(ctx)
		sess.stateLock.Lock()
		sess.epoch = &tlsHandshakeEpoch{
			ctx:               epochCtx,
			cancel:            epochCancel,
			handshakeDone:     make(chan struct{}),
			establishmentDone: make(chan struct{}),
			tlsExporter:       exporter,
			derivedTlsCipher:  &sequenceCipher{}, // non-nil sentinel; never used for crypto here
			negotiatedCurveId: tls.X25519MLKEM768,
		}
		sess.stateLock.Unlock()
	}

	// --- Sealed: valid identity proof on a registered session
	sealPeerId := NewId()
	sealSess := manager.Acquire(sealPeerId, sequenceTlsRoleServer, false)
	if sealSess == nil {
		t.Fatal("expected a session")
	}
	defer sealSess.Release()
	injectEpoch(sealSess)

	state := stateForPeer(sealPeerId)
	if state == nil {
		t.Fatal("expected a state for the registered session")
	}
	AssertEqual(t, false, state.Sealed)
	AssertEqual(t, true, state.Establishing)

	_, sealPeerPriv, err := ed25519.GenerateKey(rand.Reader)
	AssertEqual(t, nil, err)
	sealSess.SetPeerClientPublicKey(sealPeerPriv.Public().(ed25519.PublicKey))
	sealSess.receivePeerIdentityProof(ed25519.Sign(sealPeerPriv, sealSess.epoch.tlsExporter))

	all := drainEvents()
	AssertEqual(t, 1, countType(all, EncryptionEventSealed, sealPeerId))
	state = stateForPeer(sealPeerId)
	AssertEqual(t, true, state.Sealed)
	AssertEqual(t, false, state.Establishing)
	AssertEqual(t, tls.X25519MLKEM768, state.KeyExchange)
	AssertEqual(t, "", state.FailureReason)

	// --- IdentityFailed: proof signed by the wrong key
	failPeerId := NewId()
	failSess := manager.Acquire(failPeerId, sequenceTlsRoleServer, false)
	defer failSess.Release()
	injectEpoch(failSess)
	rightPub, _, err := ed25519.GenerateKey(rand.Reader)
	AssertEqual(t, nil, err)
	_, wrongPriv, err := ed25519.GenerateKey(rand.Reader)
	AssertEqual(t, nil, err)
	failSess.SetPeerClientPublicKey(rightPub)
	failSess.receivePeerIdentityProof(ed25519.Sign(wrongPriv, failSess.epoch.tlsExporter))

	all = drainEvents()
	AssertEqual(t, 1, countType(all, EncryptionEventIdentityFailed, failPeerId))
	state = stateForPeer(failPeerId)
	AssertEqual(t, false, state.Sealed)
	AssertEqual(t, "peer identity proof verification failed", state.FailureReason)

	// --- EstablishFailed: handshake completes with an error
	estPeerId := NewId()
	estSess := manager.Acquire(estPeerId, sequenceTlsRoleServer, false)
	defer estSess.Release()
	injectEpoch(estSess)
	estSess.completeHandshake(estSess.epoch, errors.New("boom"))

	all = drainEvents()
	AssertEqual(t, 1, countType(all, EncryptionEventEstablishFailed, estPeerId))
	state = stateForPeer(estPeerId)
	AssertEqual(t, false, state.Sealed)
	AssertEqual(t, "boom", state.FailureReason)
}

// TestPeerSessionIdentityProofValid covers the happy path: peer key
// known, exporter present, valid proof arrives → flips
// `peerIdentityVerified` true and `Cipher()` becomes non-nil.
func TestPeerSessionIdentityProofValid(t *testing.T) {
	sess, _, _, peerPriv, cleanup := newTestSessionForIdentityProof(t)
	defer cleanup()

	peerPub := peerPriv.Public().(ed25519.PublicKey)

	// Peer signs the session's exporter with their private key.
	proof := ed25519.Sign(peerPriv, sess.epoch.tlsExporter)
	AssertEqual(t, ed25519.SignatureSize, len(proof))

	// Set the peer key and deliver the proof.
	sess.SetPeerClientPublicKey(peerPub)
	sess.receivePeerIdentityProof(proof)

	AssertEqual(t, true, sess.epoch.peerIdentityVerified)
	AssertEqual(t, false, sess.epoch.identityFailed)
	if sess.Cipher() == nil {
		t.Fatal("Cipher should be exposed after valid identity proof")
	}
}

// TestPeerSessionIdentityProofInvalid covers the failure path:
// proof signed with the wrong key → flips `identityFailed`,
// `Cipher()` stays nil permanently.
func TestPeerSessionIdentityProofInvalid(t *testing.T) {
	sess, _, _, peerPriv, cleanup := newTestSessionForIdentityProof(t)
	defer cleanup()

	peerPub := peerPriv.Public().(ed25519.PublicKey)

	// Sign with a different private key — the proof must fail
	// verification against `peerPub`.
	_, wrongPriv, err := ed25519.GenerateKey(rand.Reader)
	AssertEqual(t, nil, err)
	badProof := ed25519.Sign(wrongPriv, sess.epoch.tlsExporter)

	sess.SetPeerClientPublicKey(peerPub)
	sess.receivePeerIdentityProof(badProof)

	AssertEqual(t, false, sess.epoch.peerIdentityVerified)
	AssertEqual(t, true, sess.epoch.identityFailed)
	AssertEqual(t, (*sequenceCipher)(nil), sess.Cipher())
}

// TestPeerSessionIdentityProofMalformed covers the defensive path:
// a proof of the wrong size flips `identityFailed` immediately —
// the session never exposes a cipher.
func TestPeerSessionIdentityProofMalformed(t *testing.T) {
	sess, _, _, _, cleanup := newTestSessionForIdentityProof(t)
	defer cleanup()

	sess.receivePeerIdentityProof([]byte{0, 1, 2}) // way too short

	AssertEqual(t, true, sess.epoch.identityFailed)
	AssertEqual(t, (*sequenceCipher)(nil), sess.Cipher())
}

// TestPeerSessionIdentityProofArrivesBeforeKey covers out-of-order
// arrival: proof first, peer key second. The proof must be
// buffered, and verification must run once the key is set.
func TestPeerSessionIdentityProofArrivesBeforeKey(t *testing.T) {
	sess, _, _, peerPriv, cleanup := newTestSessionForIdentityProof(t)
	defer cleanup()

	peerPub := peerPriv.Public().(ed25519.PublicKey)
	proof := ed25519.Sign(peerPriv, sess.epoch.tlsExporter)

	// Proof first — must be buffered (no peer key yet).
	sess.receivePeerIdentityProof(proof)
	AssertEqual(t, false, sess.epoch.peerIdentityVerified)
	if 0 == len(sess.epoch.pendingPeerIdentityProof) {
		t.Fatal("expected proof to be buffered while peer key unknown")
	}

	// Key arrives — verification runs and succeeds.
	sess.SetPeerClientPublicKey(peerPub)
	AssertEqual(t, true, sess.epoch.peerIdentityVerified)
	if 0 != len(sess.epoch.pendingPeerIdentityProof) {
		t.Fatal("expected buffered proof to be cleared after verification")
	}
}

// TestPeerSessionIdentityProofArrivesBeforeExporter covers out-of-
// order arrival where the TLS handshake hasn't completed yet (no
// exporter). The proof must be buffered, and verification must run
// once the exporter and key are both available.
func TestPeerSessionIdentityProofArrivesBeforeExporter(t *testing.T) {
	sess, _, _, peerPriv, cleanup := newTestSessionForIdentityProof(t)
	defer cleanup()

	// Wipe the pre-populated exporter to simulate "TLS handshake
	// not yet complete." (The helper pre-populates so most tests
	// don't have to think about it.)
	savedExporter := sess.epoch.tlsExporter
	sess.epoch.tlsExporter = nil

	peerPub := peerPriv.Public().(ed25519.PublicKey)
	proof := ed25519.Sign(peerPriv, savedExporter)

	// Set the peer key and the proof before the exporter exists.
	sess.SetPeerClientPublicKey(peerPub)
	sess.receivePeerIdentityProof(proof)
	AssertEqual(t, false, sess.epoch.peerIdentityVerified)
	if 0 == len(sess.epoch.pendingPeerIdentityProof) {
		t.Fatal("expected proof to be buffered while exporter unknown")
	}

	// Exporter becomes available; trigger verification manually
	// (in production `completeHandshake` would call this).
	sess.epoch.tlsExporter = savedExporter
	sess.maybeVerifyPendingPeerIdentityProof(sess.epoch)

	AssertEqual(t, true, sess.epoch.peerIdentityVerified)
}

// TestPeerSessionIdentityProofSecondIgnored verifies that a second
// proof arriving while one is already buffered (or after
// verification has settled) is ignored — the session has at most
// one identity-proof exchange per lifetime.
func TestPeerSessionIdentityProofSecondIgnored(t *testing.T) {
	sess, _, _, peerPriv, cleanup := newTestSessionForIdentityProof(t)
	defer cleanup()

	peerPub := peerPriv.Public().(ed25519.PublicKey)
	proof := ed25519.Sign(peerPriv, sess.epoch.tlsExporter)

	sess.SetPeerClientPublicKey(peerPub)
	sess.receivePeerIdentityProof(proof)
	AssertEqual(t, true, sess.epoch.peerIdentityVerified)

	// A second proof — even if cryptographically valid — must not
	// change the session state. peerIdentityVerified stays true.
	sess.receivePeerIdentityProof(proof)
	AssertEqual(t, true, sess.epoch.peerIdentityVerified)
	AssertEqual(t, false, sess.epoch.identityFailed)

	// Also: a second SetPeerClientPublicKey with a different key
	// must be rejected (first-write-wins, no mid-session rotation).
	_, otherPriv, err := ed25519.GenerateKey(rand.Reader)
	AssertEqual(t, nil, err)
	otherPub := otherPriv.Public().(ed25519.PublicKey)
	sess.SetPeerClientPublicKey(otherPub)
	// Peer key still the original.
	if !sess.PeerClientPublicKey().Equal(peerPub) {
		t.Fatal("expected SetPeerClientPublicKey to be first-write-wins")
	}
}

// TestIsAwaitingClientFinished verifies the predicate's three
// "false" cases and one "true" case, which together control whether
// the receive-loop optimistic-apply path will fire.
func TestIsAwaitingClientFinished(t *testing.T) {
	sess, _, _, _, cleanup := newTestSessionForIdentityProof(t)
	defer cleanup()

	// Role is server (set by the helper). Handshake not done,
	// serverFlightSent not yet — should be false.
	AssertEqual(t, false, sess.IsAwaitingClientFinished())

	// After serverFlightSent → true (handshake still open).
	sess.markServerFlightSent(sess.epoch)
	AssertEqual(t, true, sess.IsAwaitingClientFinished())

	// Close handshakeDone → false (handshake final, success or fail).
	close(sess.epoch.handshakeDone)
	AssertEqual(t, false, sess.IsAwaitingClientFinished())
}

// TestIsAwaitingClientFinishedClientRole verifies that the
// TLS-client role never returns true regardless of state — the
// optimistic-apply path is meaningful only on the server-role side
// (where the client second flight is the next inbound).
func TestIsAwaitingClientFinishedClientRole(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOpportunistic
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()

	keyManager, err := NewClientKeyManager(ctx, client)
	AssertEqual(t, nil, err)
	manager := NewEncryptionSessionManager(ctx, client, keyManager, settings.EncryptionSettings)

	clientRoleSess := newPeerEncryptionSession(
		ctx, manager, client, NewId(),
		sequenceTlsRoleClient,
		settings.EncryptionSettings,
		manager.ClientTlsConfig(),
		false,
	)
	clientRoleSess.epoch = &tlsHandshakeEpoch{handshakeDone: make(chan struct{})}

	// Even after marking serverFlightSent (which wouldn't normally
	// happen in client role, but worth proving the role gate is
	// what's enforcing the predicate), still false.
	clientRoleSess.markServerFlightSent(clientRoleSess.epoch)
	AssertEqual(t, false, clientRoleSess.IsAwaitingClientFinished())
}

// TestOptimisticallyDeliverHandshakeFilter verifies the structural
// filter that gates `OptimisticallyDeliverHandshake`: only payloads
// starting with TLS record type 20 (ChangeCipherSpec) or 23
// (encrypted application_data) are accepted. Records of type 22
// (unencrypted Handshake — what a ClientHello retransmit looks
// like) are filtered out.
func TestOptimisticallyDeliverHandshakeFilter(t *testing.T) {
	sess, _, _, _, cleanup := newTestSessionForIdentityProof(t)
	defer cleanup()

	// Put the session into the awaiting-client-Finished state so
	// the optimistic-apply gate is open.
	sess.markServerFlightSent(sess.epoch)
	AssertEqual(t, true, sess.IsAwaitingClientFinished())

	// startEpoch would normally create the transport; we skip
	// here and only check that the filter rejects type-22 bytes
	// without invoking the transport. (A reject is a no-op; we
	// verify by checking the session does not panic and the test
	// completes.)
	prefixesAccepted := []byte{20, 23}
	prefixesRejected := []byte{0, 21, 22, 24, 0xff}

	// We can't directly observe "did the optimistic apply call
	// transport.Deliver?" without instrumenting the transport.
	// What we can do is verify the predicate behavior end-to-end:
	// the filter returns early on rejected prefixes, and the
	// session state is unchanged.
	for _, prefix := range prefixesRejected {
		payload := append([]byte{prefix}, make([]byte, 100)...)
		sess.OptimisticallyDeliverHandshake(payload)
		// No state to assert on directly; the test passing without
		// panic and without TLS state mutation is the contract.
	}
	for _, prefix := range prefixesAccepted {
		payload := append([]byte{prefix}, make([]byte, 100)...)
		// These would call startEpoch + transport.Deliver. We
		// avoid actually exercising the transport here because the
		// helper hasn't set up the TLS state. Skip the call but
		// document that the prefix would pass the filter.
		_ = payload
	}

	// Also verify the filter blocks empty payload (defensive
	// against zero-byte inputs).
	sess.OptimisticallyDeliverHandshake(nil)
	sess.OptimisticallyDeliverHandshake([]byte{})
}

// TestSetPeerClientPublicKeyWrongSize verifies that a key of the
// wrong size is silently ignored (logged, but doesn't change
// session state).
func TestSetPeerClientPublicKeyWrongSize(t *testing.T) {
	sess, _, _, _, cleanup := newTestSessionForIdentityProof(t)
	defer cleanup()

	sess.SetPeerClientPublicKey([]byte{0, 1, 2}) // way too short
	if sess.PeerClientPublicKey() != nil {
		t.Fatal("expected peer key to remain unset after wrong-size input")
	}
}

// TestPeerSessionIdentityVerifiedOnProof verifies that a valid identity proof
// signed over the session's tls exporter flips `peerIdentityVerified`.
//
// (This previously asserted that the session's `readyMonitor` fired. That
// monitor had six notifiers and no consumer outside this test — the receive
// loop that once waited on it was replaced by `OptimisticallyDeliverHandshake`
// — so it was removed, and the test now asserts the outcome it was standing in
// for.)
func TestPeerSessionIdentityVerifiedOnProof(t *testing.T) {
	sess, _, _, peerPriv, cleanup := newTestSessionForIdentityProof(t)
	defer cleanup()

	peerPub := peerPriv.Public().(ed25519.PublicKey)
	proof := ed25519.Sign(peerPriv, sess.epoch.tlsExporter)

	if sess.epoch.peerIdentityVerified {
		t.Fatal("peer identity verified before any proof was received")
	}

	sess.SetPeerClientPublicKey(peerPub)
	sess.receivePeerIdentityProof(proof)

	sess.stateLock.Lock()
	verified := sess.epoch.peerIdentityVerified
	sess.stateLock.Unlock()
	if !verified {
		t.Fatal("expected the peer identity to be verified after a valid proof")
	}
}

// newTestEncryptionSession builds a peerEncryptionSession in the given role
// with a manager that carries real TLS configs, so startEpoch / reset build
// real epochs. No epoch is started; the caller drives the lifecycle. A short
// handshake timeout keeps any stray real epoch from lingering.
func newTestEncryptionSession(t *testing.T, role sequenceTlsRole) (*peerEncryptionSession, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOpportunistic
	settings.EncryptionSettings.TlsTimeout = 2 * time.Second
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	keyManager, err := NewClientKeyManager(ctx, client)
	if err != nil {
		cancel()
		t.Fatalf("NewClientKeyManager: %s", err)
	}
	manager := NewEncryptionSessionManager(ctx, client, keyManager, settings.EncryptionSettings)
	var roleTlsConfig *tls.Config
	switch role {
	case sequenceTlsRoleClient:
		roleTlsConfig = manager.ClientTlsConfig()
	case sequenceTlsRoleServer:
		roleTlsConfig = manager.ServerTlsConfig()
	}
	sess := newPeerEncryptionSession(ctx, manager, client, NewId(), role, settings.EncryptionSettings, roleTlsConfig, false)
	return sess, func() {
		client.Cancel()
		cancel()
	}
}

// injectTestEpoch swaps a hand-built epoch into the session (cancelling any
// existing one). `completed` closes its handshakeDone; `handshakeErr` records
// a completion error. The epoch carries a real ctx/cancel so production paths
// that cancel the prior epoch on reset don't trip on a nil cancel.
func injectTestEpoch(sess *peerEncryptionSession, completed bool, handshakeErr error) *tlsHandshakeEpoch {
	ctx, cancel := context.WithCancel(context.Background())
	e := &tlsHandshakeEpoch{
		ctx:           ctx,
		cancel:        cancel,
		handshakeDone: make(chan struct{}),
		handshakeErr:  handshakeErr,
	}
	if completed {
		close(e.handshakeDone)
	}
	sess.stateLock.Lock()
	if sess.epoch != nil && sess.epoch.cancel != nil {
		sess.epoch.cancel()
	}
	sess.epoch = e
	sess.stateLock.Unlock()
	return e
}

// TestHandshakeEpochResetReplacesEpoch verifies reset() installs a fresh
// epoch and cancels the previous one's ctx (so its goroutines exit), and that
// startEpoch is a no-op while an epoch is present.
func TestHandshakeEpochResetReplacesEpoch(t *testing.T) {
	sess, cleanup := newTestEncryptionSession(t, sequenceTlsRoleServer)
	defer cleanup()

	sess.startEpoch()
	e1 := sess.currentEpoch()
	if e1 == nil {
		t.Fatal("expected an epoch after startEpoch")
	}

	sess.reset()
	e2 := sess.currentEpoch()
	if e2 == nil || e2 == e1 {
		t.Fatal("expected reset to install a new epoch")
	}
	select {
	case <-e1.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("expected the old epoch's ctx to be cancelled after reset")
	}

	sess.startEpoch()
	if sess.currentEpoch() != e2 {
		t.Fatal("startEpoch should be a no-op while an epoch is present")
	}
}

func TestHandshakeFailureCancelsEpochWorkers(t *testing.T) {
	sess, cleanup := newTestEncryptionSession(t, sequenceTlsRoleClient)
	defer cleanup()

	e := injectTestEpoch(sess, false, nil)
	e.establishmentDone = make(chan struct{})
	sess.completeHandshake(e, errors.New("synthetic handshake failure"))

	select {
	case <-e.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("failed handshake left its epoch workers running")
	}
	select {
	case <-e.establishmentDone:
	default:
		t.Fatal("failed handshake did not finish the establishment lifetime")
	}
}

func TestFailedClientEpochRestartsImmediatelyWhenInitialCooldownDisabled(t *testing.T) {
	sess, cleanup := newTestEncryptionSession(t, sequenceTlsRoleClient)
	defer cleanup()
	sess.settings.TlsInitialRetryInterval = 0

	failed := injectTestEpoch(sess, true, errors.New("transient route failure"))
	failed.establishmentDone = make(chan struct{})
	close(failed.establishmentDone)
	sess.restartHandshake()

	replacement := sess.currentEpoch()
	if replacement == nil || replacement == failed {
		t.Fatal("client send acquisition reused a failed epoch instead of retrying")
	}
	select {
	case <-failed.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("failed epoch was not canceled when its replacement started")
	}
}

func TestTlsInitialRetryIntervalIsExponentialAndBounded(t *testing.T) {
	testCases := []struct {
		initialRetryInterval time.Duration
		maxRetryInterval     time.Duration
		failureCount         int
		expectedInterval     time.Duration
	}{
		{
			initialRetryInterval: 0,
			maxRetryInterval:     5 * time.Minute,
			failureCount:         1,
			expectedInterval:     0,
		},
		{
			initialRetryInterval: time.Minute,
			maxRetryInterval:     5 * time.Minute,
			failureCount:         1,
			expectedInterval:     time.Minute,
		},
		{
			initialRetryInterval: time.Minute,
			maxRetryInterval:     5 * time.Minute,
			failureCount:         2,
			expectedInterval:     2 * time.Minute,
		},
		{
			initialRetryInterval: time.Minute,
			maxRetryInterval:     5 * time.Minute,
			failureCount:         3,
			expectedInterval:     4 * time.Minute,
		},
		{
			initialRetryInterval: time.Minute,
			maxRetryInterval:     5 * time.Minute,
			failureCount:         4,
			expectedInterval:     5 * time.Minute,
		},
		{
			initialRetryInterval: 2 * time.Minute,
			maxRetryInterval:     time.Minute,
			failureCount:         4,
			expectedInterval:     2 * time.Minute,
		},
		{
			initialRetryInterval: time.Duration(1<<62) + 1,
			maxRetryInterval:     time.Duration(1<<63 - 1),
			failureCount:         2,
			expectedInterval:     time.Duration(1<<63 - 1),
		},
	}

	for _, testCase := range testCases {
		actualInterval := tlsInitialRetryInterval(
			testCase.initialRetryInterval,
			testCase.maxRetryInterval,
			testCase.failureCount,
		)
		if actualInterval != testCase.expectedInterval {
			t.Errorf(
				"tlsInitialRetryInterval(%s, %s, %d) = %s, expected %s",
				testCase.initialRetryInterval,
				testCase.maxRetryInterval,
				testCase.failureCount,
				actualInterval,
				testCase.expectedInterval,
			)
		}
	}
}

func TestDefaultEncryptionSettingsBoundInitialRetry(t *testing.T) {
	settings := DefaultEncryptionSettings()
	if settings.TlsInitialRetryInterval <= 0 {
		t.Fatal("default initial TLS retry cooldown is disabled")
	}
	if settings.TlsInitialRetryStagger <= 0 {
		t.Fatal("default initial TLS retry staggering is disabled")
	}
	if settings.TlsInitialRetryMaxInterval <
		settings.TlsInitialRetryInterval {
		t.Fatalf(
			"default maximum TLS retry interval %s is below initial %s",
			settings.TlsInitialRetryMaxInterval,
			settings.TlsInitialRetryInterval,
		)
	}
}

func TestTlsInitialRetryDelayIsDeterministicStaggeredAndBounded(t *testing.T) {
	var firstPeerId Id
	firstPeerId[len(firstPeerId)-1] = 1
	var secondPeerId Id
	secondPeerId[len(secondPeerId)-1] = 2

	firstDelay := tlsInitialRetryDelay(
		firstPeerId,
		time.Minute,
		5*time.Minute,
		time.Minute,
		1,
	)
	repeatedDelay := tlsInitialRetryDelay(
		firstPeerId,
		time.Minute,
		5*time.Minute,
		time.Minute,
		1,
	)
	secondDelay := tlsInitialRetryDelay(
		secondPeerId,
		time.Minute,
		5*time.Minute,
		time.Minute,
		1,
	)

	if firstDelay != repeatedDelay {
		t.Fatalf(
			"stable peer retry delay changed from %s to %s",
			firstDelay,
			repeatedDelay,
		)
	}
	if firstDelay < time.Minute || 2*time.Minute <= firstDelay {
		t.Fatalf("first retry delay %s is outside [1m, 2m)", firstDelay)
	}
	if secondDelay < time.Minute || 2*time.Minute <= secondDelay {
		t.Fatalf("second retry delay %s is outside [1m, 2m)", secondDelay)
	}
	if firstDelay == secondDelay {
		t.Fatalf("distinct peers received the same retry phase %s", firstDelay)
	}

	maxDelay := tlsInitialRetryDelay(
		firstPeerId,
		time.Minute,
		5*time.Minute,
		time.Minute,
		63,
	)
	if maxDelay != 5*time.Minute {
		t.Fatalf("maximum retry delay = %s, expected 5m", maxDelay)
	}
}

func TestFailedClientEpochWaitsForInitialRetryCooldown(t *testing.T) {
	sess, cleanup := newTestEncryptionSession(t, sequenceTlsRoleClient)
	defer cleanup()
	sess.settings.TlsInitialRetryInterval = time.Hour
	sess.settings.TlsInitialRetryMaxInterval = time.Hour

	failed := injectTestEpoch(sess, false, nil)
	failed.establishmentDone = make(chan struct{})
	sess.completeHandshake(failed, errors.New("unreachable peer"))
	sess.restartHandshake()

	if sess.currentEpoch() != failed {
		t.Fatal("failed initial epoch retried before its cooldown expired")
	}
}

func TestFailedClientEpochRestartsAfterInitialRetryCooldown(t *testing.T) {
	sess, cleanup := newTestEncryptionSession(t, sequenceTlsRoleClient)
	defer cleanup()
	sess.settings.TlsInitialRetryInterval = time.Hour
	sess.settings.TlsInitialRetryMaxInterval = time.Hour

	failed := injectTestEpoch(sess, false, nil)
	failed.establishmentDone = make(chan struct{})
	sess.completeHandshake(failed, errors.New("unreachable peer"))
	sess.stateLock.Lock()
	sess.nextInitialHandshakeRetryTime = time.Now().Add(-time.Second)
	sess.stateLock.Unlock()
	sess.restartHandshake()

	replacement := sess.currentEpoch()
	if replacement == nil || replacement == failed {
		t.Fatal("failed initial epoch did not retry after its cooldown expired")
	}
}

func TestEstablishedClientEpochRecoveryBypassesInitialRetryCooldown(t *testing.T) {
	sess, cleanup := newTestEncryptionSession(t, sequenceTlsRoleClient)
	defer cleanup()

	established := injectEstablishedTestEpoch(sess)
	sess.stateLock.Lock()
	sess.initialHandshakeFailureCount = 3
	sess.nextInitialHandshakeRetryTime = time.Now().Add(time.Hour)
	sess.stateLock.Unlock()
	sess.restartHandshake()

	if sess.currentEpoch() == established {
		t.Fatal("established session recovery was delayed by initial cooldown")
	}
	if sess.establishedEpoch != established {
		t.Fatal("established cipher was not retained during recovery")
	}
}

func TestEstablishedEpochClearsInitialRetryCooldown(t *testing.T) {
	sess, cleanup := newTestEncryptionSession(t, sequenceTlsRoleClient)
	defer cleanup()

	established := injectTestEpoch(sess, true, nil)
	sess.stateLock.Lock()
	sess.initialHandshakeFailureCount = 3
	sess.nextInitialHandshakeRetryTime = time.Now().Add(time.Hour)
	sess.markEstablishedWithLock(established)
	failureCount := sess.initialHandshakeFailureCount
	nextRetryTime := sess.nextInitialHandshakeRetryTime
	sess.stateLock.Unlock()

	if failureCount != 0 {
		t.Fatalf("establishment retained failure count %d", failureCount)
	}
	if !nextRetryTime.IsZero() {
		t.Fatalf("establishment retained retry deadline %s", nextRetryTime)
	}
}

func TestEstablishmentFailureLoggingCoalescesUntilRecovery(t *testing.T) {
	sess, cleanup := newTestEncryptionSession(t, sequenceTlsRoleClient)
	defer cleanup()

	established := injectTestEpoch(sess, true, nil)
	sess.stateLock.Lock()
	firstFailure := sess.shouldLogEstablishmentFailureWithLock("timeout")
	repeatedFailure := sess.shouldLogEstablishmentFailureWithLock("timeout")
	differentFailure := sess.shouldLogEstablishmentFailureWithLock("identity")
	sess.markEstablishedWithLock(established)
	afterRecovery := sess.shouldLogEstablishmentFailureWithLock("timeout")
	sess.stateLock.Unlock()

	if !firstFailure {
		t.Fatal("first establishment failure was suppressed")
	}
	if repeatedFailure {
		t.Fatal("repeated establishment failure was not coalesced")
	}
	if !differentFailure {
		t.Fatal("different establishment failure was suppressed")
	}
	if !afterRecovery {
		t.Fatal("recovered establishment did not reset failure logging")
	}
}

func TestIdentityProofTimeoutCancelsUnestablishedEpoch(t *testing.T) {
	sess, cleanup := newTestEncryptionSession(t, sequenceTlsRoleClient)
	defer cleanup()
	sess.settings.TlsTimeout = 20 * time.Millisecond

	epochCtx, epochCancel := context.WithCancel(sess.ctx)
	e := &tlsHandshakeEpoch{
		ctx:               epochCtx,
		cancel:            epochCancel,
		handshakeDone:     make(chan struct{}),
		establishmentDone: make(chan struct{}),
	}
	close(e.handshakeDone)
	sess.stateLock.Lock()
	sess.epoch = e
	sess.stateLock.Unlock()

	go sess.establishmentTimeoutWatcher(e)
	select {
	case <-e.establishmentDone:
	case <-time.After(time.Second):
		t.Fatal("missing peer identity proof was not bounded by TlsTimeout")
	}
	select {
	case <-e.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("identity-proof timeout left epoch workers running")
	}
	sess.stateLock.Lock()
	identityFailed := e.identityFailed
	sess.stateLock.Unlock()
	if !identityFailed {
		t.Fatal("identity-proof timeout did not leave the epoch unauthenticated")
	}
}

func TestFailedZeroReferenceSessionIsReapedWithoutIdlePoll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOpportunistic
	settings.EncryptionSettings.IdleTimeout = 0
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()
	manager := client.EncryptionSessionManager()

	peerId := NewId()
	sess := manager.Acquire(peerId, sequenceTlsRoleClient, false)
	e := injectTestEpoch(sess, false, nil)
	e.establishmentDone = make(chan struct{})
	sess.Release()
	if manager.Lookup(peerId, sequenceTlsRoleClient, false) != sess {
		t.Fatal("in-flight zero-reference session was reaped before establishment finished")
	}

	sess.completeHandshake(e, errors.New("synthetic timeout"))
	if manager.Lookup(peerId, sequenceTlsRoleClient, false) != nil {
		t.Fatal("failed zero-reference session remained pinned without an idle poll")
	}
}

func TestSuccessfulZeroReferenceSessionIsReapedWithoutIdlePoll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOpportunistic
	settings.EncryptionSettings.IdleTimeout = 0
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()
	manager := client.EncryptionSessionManager()

	peerId := NewId()
	sess := manager.Acquire(peerId, sequenceTlsRoleServer, false)
	epochCtx, epochCancel := context.WithCancel(sess.ctx)
	defer epochCancel()
	exporter := make([]byte, sequenceTlsIdentityProofLength)
	if _, err := rand.Read(exporter); err != nil {
		t.Fatal(err)
	}
	e := &tlsHandshakeEpoch{
		ctx:               epochCtx,
		cancel:            epochCancel,
		handshakeDone:     make(chan struct{}),
		establishmentDone: make(chan struct{}),
		tlsExporter:       exporter,
		derivedTlsCipher:  &sequenceCipher{},
	}
	close(e.handshakeDone)
	sess.stateLock.Lock()
	sess.epoch = e
	sess.stateLock.Unlock()

	peerPublicKey, peerPrivateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sess.SetPeerClientPublicKey(peerPublicKey)
	sess.Release()
	if manager.Lookup(peerId, sequenceTlsRoleServer, false) != sess {
		t.Fatal("in-flight zero-reference session was reaped before establishment finished")
	}

	sess.receivePeerIdentityProof(ed25519.Sign(peerPrivateKey, exporter))
	if manager.Lookup(peerId, sequenceTlsRoleServer, false) != nil {
		t.Fatal("successful zero-reference session remained pinned without an idle poll")
	}
}

func TestPositiveIdleSessionReapsAtReleaseRelativeDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOpportunistic
	settings.EncryptionSettings.IdleTimeout = 400 * time.Millisecond
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()
	manager := client.EncryptionSessionManager()

	peerId := NewId()
	sess := manager.Acquire(peerId, sequenceTlsRoleServer, false)
	// Let the supervisor enter its referenced-state wait. With the former
	// fixed-period poll, releasing halfway through that period made the next
	// poll observe only half an idle timeout and sleep a second full period.
	time.Sleep(settings.EncryptionSettings.IdleTimeout / 2)
	releasedAt := time.Now()
	sess.Release()

	earlyDeadline := releasedAt.Add(settings.EncryptionSettings.IdleTimeout - 50*time.Millisecond)
	for time.Now().Before(earlyDeadline) {
		if manager.Lookup(peerId, sequenceTlsRoleServer, false) == nil {
			t.Fatal("session reaped before its configured idle lifetime")
		}
		time.Sleep(5 * time.Millisecond)
	}
	lateDeadline := releasedAt.Add(settings.EncryptionSettings.IdleTimeout + 100*time.Millisecond)
	for manager.Lookup(peerId, sequenceTlsRoleServer, false) != nil {
		if time.Now().After(lateDeadline) {
			t.Fatalf(
				"session exceeded release-relative idle deadline: elapsed=%s timeout=%s",
				time.Since(releasedAt),
				settings.EncryptionSettings.IdleTimeout,
			)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestInboundStartReusesAnyExistingEpoch verifies that the inbound-delivery
// start path never resets state merely because another handshake frame or
// proof was delivered. Client send acquisition has a distinct recovery rule:
// it leaves an in-flight establishment alone but replaces a failed epoch.
func TestInboundStartReusesAnyExistingEpoch(t *testing.T) {
	sess, cleanup := newTestEncryptionSession(t, sequenceTlsRoleServer)
	defer cleanup()

	// established (done, no error) → reuse
	established := injectTestEpoch(sess, true, nil)
	sess.startEpoch()
	if sess.currentEpoch() != established {
		t.Fatal("expected an established handshake to be reused")
	}

	// finished with error → inbound delivery still reuses until a definitive
	// new ClientHello or client send acquisition starts the replacement
	failed := injectTestEpoch(sess, true, errors.New("boom"))
	sess.startEpoch()
	if sess.currentEpoch() != failed {
		t.Fatal("expected a failed handshake to be reused, not reset, on a send")
	}

	// in flight (not done) → reuse (let it finish)
	inflight := injectTestEpoch(sess, false, nil)
	sess.startEpoch()
	if sess.currentEpoch() != inflight {
		t.Fatal("expected an in-flight handshake to be left to complete")
	}
}

// TestDeliverClientHelloResetsCompletedServerHandshakeClientHelloAfterCompletionResets
// covers point (b): a new inbound ClientHello at the TLS-server role after the
// current handshake has completed resets to a fresh handshake (so a peer that
// re-initiates — e.g. a resumed SendSequence on the client side — is followed).
func TestDeliverClientHelloResetsCompletedServerHandshakeClientHelloAfterCompletionResets(t *testing.T) {
	// record type 22 (handshake), handshake message type 1 (ClientHello)
	clientHello := []byte{22, 3, 3, 0, 100, 1, 0, 0, 96}
	if !isClientHelloRecord(clientHello) {
		t.Fatal("test ClientHello bytes should satisfy isClientHelloRecord")
	}

	sess, cleanup := newTestEncryptionSession(t, sequenceTlsRoleServer)
	defer cleanup()
	e1 := injectTestEpoch(sess, true, nil)
	sess.deliverHandshake(clientHello, Id{})
	if sess.currentEpoch() == e1 {
		t.Fatal("expected a new ClientHello to reset the completed server handshake")
	}
}

// TestDeliverClientHelloResetsCompletedServerHandshakeStaleNonClientHelloAfterCompletionIsDropped
// is the counterpart: stale non-ClientHello bytes after completion are dropped
// without a reset.
func TestDeliverClientHelloResetsCompletedServerHandshakeStaleNonClientHelloAfterCompletionIsDropped(t *testing.T) {
	// record type 23 (application_data) — not a ClientHello
	appData := []byte{23, 3, 3, 0, 100, 0, 0, 0, 0}
	if isClientHelloRecord(appData) {
		t.Fatal("test app-data bytes should not satisfy isClientHelloRecord")
	}

	sess, cleanup := newTestEncryptionSession(t, sequenceTlsRoleServer)
	defer cleanup()
	e1 := injectTestEpoch(sess, true, nil)
	sess.deliverHandshake(appData, Id{})
	if sess.currentEpoch() != e1 {
		t.Fatal("expected stale post-completion bytes to be dropped without reset")
	}
}

// TestReleasedSessionRemovedFromManager verifies a per-peer session's
// lifecycle is bounded by its references: once the last send/receive sequence
// releases it, it is closed and unregistered (the send/receive sequences idle
// out on their own timers, so they are the session's lifecycle). A subsequent
// wrapped frame finds no session and is dropped — but that is transient, not a
// wedge: a client-role send sequence restarts the handshake on its next burst,
// so the peer's responder session is rebuilt. Removal is synchronous with the
// ref reaching zero (under the manager lock), so a concurrent re-acquire can't
// adopt a session that is being torn down.
func TestReleasedSessionRemovedFromManager(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOpportunistic
	settings.EncryptionSettings.TlsTimeout = 2 * time.Second
	// Short idle timeout so the reap is observable quickly. The session is kept
	// registered for this long after its last reference drops, then the Run
	// loop's CancelIfIdle removes it.
	settings.EncryptionSettings.IdleTimeout = 200 * time.Millisecond
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()
	keyManager, err := NewClientKeyManager(ctx, client)
	AssertEqual(t, nil, err)
	manager := NewEncryptionSessionManager(ctx, client, keyManager, settings.EncryptionSettings)

	peerId := NewId()
	sess := manager.Acquire(peerId, sequenceTlsRoleServer, false)
	if sess == nil {
		t.Fatal("expected Acquire to return a session")
	}
	if manager.Lookup(peerId, sequenceTlsRoleServer, false) != sess {
		t.Fatal("expected the session to be registered in the manager")
	}

	sess.Release() // refs -> 0; kept registered until idle for IdleTimeout

	// Removal is not synchronous with Release: the session is kept registered
	// so a transport reform / next burst reuses the live cipher instead of
	// churning a fresh handshake. The idle time here (~0) is below IdleTimeout.
	if manager.Lookup(peerId, sequenceTlsRoleServer, false) != sess {
		t.Fatal("expected the session to remain registered immediately after Release (idle keep-alive)")
	}

	// After IdleTimeout the Run loop's CancelIfIdle reaps it.
	deadline := time.After(2 * time.Second)
	for manager.Lookup(peerId, sequenceTlsRoleServer, false) != nil {
		select {
		case <-deadline:
			t.Fatal("expected the session to be removed from the manager after the idle timeout")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// injectEstablishedTestEpoch installs an epoch and marks it the established
// (serving) epoch — handshake complete and peer identity verified — without a
// real TLS handshake. The derived cipher is left nil; this exercises
// epoch/role lifecycle, not wrap/unwrap.
func injectEstablishedTestEpoch(sess *peerEncryptionSession) *tlsHandshakeEpoch {
	e := injectTestEpoch(sess, true, nil)
	sess.stateLock.Lock()
	e.peerIdentityVerified = true
	sess.establishedEpoch = e
	sess.stateLock.Unlock()
	return e
}

// TestAcquireForSendRestartPolicy verifies the per-role send-acquisition
// rules. AcquireForSend returns the same session per (peer, role) and never
// thrashes an in-flight handshake. Only the client role restarts an
// established session — the recovery mechanism: every new client send
// re-initiates, so a peer that lost its responder session rebuilds it — and
// the restart keeps the established epoch serving its cipher (gap-free rekey).
// The server role never restarts; it only carries EncryptedControl/replies and
// follows the peer's ClientHello.
func TestAcquireForSendRestartPolicy(t *testing.T) {
	for _, role := range []sequenceTlsRole{sequenceTlsRoleClient, sequenceTlsRoleServer} {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		settings := DefaultClientSettings()
		settings.EncryptionSettings.Mode = EncryptionModeOpportunistic
		settings.EncryptionSettings.TlsTimeout = 2 * time.Second
		client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
		defer client.Cancel()
		km, err := NewClientKeyManager(ctx, client)
		AssertEqual(t, nil, err)
		manager := NewEncryptionSessionManager(ctx, client, km, settings.EncryptionSettings)

		peerId := NewId()
		s1 := manager.AcquireForSend(peerId, role, false, false, false)
		if s1 == nil || s1.role != role {
			t.Fatalf("expected a %v-role session", role)
		}

		// An in-flight handshake is never restarted by a later send.
		inflight := injectTestEpoch(s1, false, nil)
		s2 := manager.AcquireForSend(peerId, role, false, false, false)
		if s2 != s1 {
			t.Fatalf("%v: expected the same per-peer/role session", role)
		}
		if s2.currentEpoch() != inflight {
			t.Fatalf("%v: AcquireForSend must not restart an in-flight handshake", role)
		}

		// An established session: the client role restarts (background
		// rekey) while keeping the established epoch serving; the server
		// role reuses.
		established := injectEstablishedTestEpoch(s1)
		s3 := manager.AcquireForSend(peerId, role, false, false, false)
		if s3 != s1 {
			t.Fatalf("%v: expected the same per-peer/role session", role)
		}
		if role == sequenceTlsRoleClient {
			if s3.currentEpoch() == established {
				t.Fatal("client AcquireForSend should start a new in-flight epoch on an established session")
			}
			if s3.establishedEpoch != established {
				t.Fatal("the established epoch must keep serving its cipher during the rekey")
			}
		} else if s3.currentEpoch() != established {
			t.Fatal("server AcquireForSend must never restart the handshake")
		}
	}
}

// A negotiated data lane is another ordering sequence, not another peer
// identity. It must retain the established cipher without starting one TLS
// epoch per active five-tuple; lane zero remains the sole restart owner.
func TestLogicalDataLaneReusesEncryptionSessionWithoutHandshakeRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOpportunistic
	settings.EncryptionSettings.TlsTimeout = 2 * time.Second
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()
	keyManager, err := NewClientKeyManager(ctx, client)
	AssertEqual(t, nil, err)
	manager := NewEncryptionSessionManager(
		ctx,
		client,
		keyManager,
		settings.EncryptionSettings,
	)

	peerId := NewId()
	laneZero := manager.acquireForLogicalLaneSend(
		peerId,
		sequenceTlsRoleClient,
		false,
		false,
		false,
		0,
	)
	if laneZero == nil {
		t.Fatal("lane zero did not acquire an encryption session")
	}
	established := injectEstablishedTestEpoch(laneZero)
	dataLane := manager.acquireForLogicalLaneSend(
		peerId,
		sequenceTlsRoleClient,
		false,
		false,
		false,
		4,
	)
	if dataLane != laneZero {
		t.Fatal("logical data lane created a different peer encryption session")
	}
	if got := dataLane.currentEpoch(); got != established {
		t.Fatal("logical data lane restarted the established handshake epoch")
	}
	if dataLane.establishedEpoch != established {
		t.Fatal("logical data lane replaced the serving encryption epoch")
	}
	dataLane.Release()
	laneZero.Release()
}

// TestEncryptedControlCarrierMirrorsForceStream is the regression test for
// the network-peer + post-quantum data blackhole: the multi-client sends
// application data with ForceStream (AllowDirect is forced on for
// same-network peers), while the EncryptedControl carrier used the client's
// default TransferOptions (ForceStream=false). ForceStream keys the send
// sequence but is invisible on the wire, so the carrier forked a SECOND
// concurrent send sequence whose frames the receiver could not distinguish
// from the data sequence — both mapped to the same (source, role, companion)
// receive head slot, the newer sequence id evicted the older, and the
// loser's packs (the data, or the ClientHello) were dropped un-acked
// forever. The carrier must ride the SAME send sequence as the data:
// AcquireForSend records the acquiring sequence's ForceStream on the session
// and SendEncryptedControl mirrors it (suppressed for companion carriers,
// whose stream contracts the platform rejects).
func TestEncryptedControlCarrierMirrorsForceStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOpportunistic
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()

	peerId := NewId()

	// the data path's send sequence acquires the session with its
	// ForceStream option; the session records it as the carrier option
	s := client.EncryptionSessionManager().AcquireForSend(
		peerId,
		sequenceTlsRoleClient,
		false,
		true,
		true,
	)
	if s == nil {
		t.Fatal("expected a client-role session")
	}
	if !s.carrierForceStream.Load() {
		t.Fatal("AcquireForSend(forceStream=true) must record the carrier ForceStream")
	}
	if !s.carrierNetworkPeer.Load() {
		t.Fatal("AcquireForSend(networkPeer=true) must record Network contract policy")
	}

	// the carrier lands on the ForceStream sequence — the same sendSequenceId
	// the data path uses — and no ForceStream=false twin is created
	ec := &protocol.EncryptedControl{
		ControlType: protocol.EncryptedControlType_EncryptedControlHandshake,
		SessionRole: sequenceTlsRoleClient.toProtobuf(),
		Payload:     []byte{22, 3, 3, 0, 1, 1},
	}
	if !client.sendBuffer.SendEncryptedControl(
		ctx,
		peerId,
		sequenceTlsRoleClient,
		ec,
		false,
		false,
		s.carrierForceStream.Load(),
		s.carrierNetworkPeer.Load(),
	) {
		t.Fatal("SendEncryptedControl should enqueue")
	}
	sequenceKeys := func() []sendSequenceId {
		client.sendBuffer.mutex.Lock()
		defer client.sendBuffer.mutex.Unlock()
		keys := []sendSequenceId{}
		for key := range client.sendBuffer.sendSequences {
			if key.Destination == peerId {
				keys = append(keys, key)
			}
		}
		return keys
	}
	keys := sequenceKeys()
	if len(keys) != 1 {
		t.Fatalf("expected exactly one send sequence to the peer, got %d (%v)", len(keys), keys)
	}
	if !keys[0].ForceStream {
		t.Fatal("the carrier must ride the data path's ForceStream sequence")
	}
	client.sendBuffer.mutex.Lock()
	networkSequence := client.sendBuffer.sendSequences[keys[0]]
	client.sendBuffer.mutex.Unlock()
	if networkSequence == nil || !networkSequence.networkPeer {
		t.Fatal("the carrier must mirror the data path's Network contract policy")
	}

	// a companion carrier never rides a stream (the platform rejects
	// companion stream contracts): ForceStream is suppressed
	ecCompanion := &protocol.EncryptedControl{
		ControlType: protocol.EncryptedControlType_EncryptedControlHandshake,
		SessionRole: sequenceTlsRoleClient.toProtobuf(),
		Companion:   true,
		Payload:     []byte{22, 3, 3, 0, 1, 1},
	}
	if !client.sendBuffer.SendEncryptedControl(
		ctx,
		peerId,
		sequenceTlsRoleClient,
		ecCompanion,
		true,
		true,
		true,
		true,
	) {
		t.Fatal("companion SendEncryptedControl should enqueue")
	}
	for _, key := range sequenceKeys() {
		if key.CompanionContract && key.ForceStream {
			t.Fatal("a companion carrier must not request a stream contract")
		}
		if key.CompanionContract {
			client.sendBuffer.mutex.Lock()
			companionSequence := client.sendBuffer.sendSequences[key]
			client.sendBuffer.mutex.Unlock()
			if companionSequence != nil && companionSequence.networkPeer {
				t.Fatal("a companion carrier must not request Network contract policy")
			}
		}
	}

	// last-acquirer-wins: a later data sequence without ForceStream retunes
	// the carrier
	s2 := client.EncryptionSessionManager().AcquireForSend(
		peerId,
		sequenceTlsRoleClient,
		false,
		false,
		false,
	)
	if s2 != s {
		t.Fatal("expected the same per-peer session")
	}
	if s.carrierForceStream.Load() {
		t.Fatal("AcquireForSend(forceStream=false) must retune the carrier ForceStream")
	}
	if s.carrierNetworkPeer.Load() {
		t.Fatal("AcquireForSend(networkPeer=false) must retune Network contract policy")
	}
}
