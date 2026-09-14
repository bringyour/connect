package connect

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/urnetwork/connect/v2026/protocol"
)

// ClientKeyManager owns the client's long-lived Ed25519 identity
// keypair. The public half is published once to the platform (via a
// `ClientKey` control message) and served back to other clients via an
// unauthenticated public-key lookup API. The private half stays in
// this process — it never leaves.
//
// Trust model. The platform is not trusted to honestly distribute
// per-client public keys for new ClientIds. The expectation is that
// the (ClientId, public key) binding is registered at account creation
// and, from then on, any peer that wants to talk to this client
// fetches the binding through the lookup API rather than relying on
// values shipped inside contracts. With that binding in hand, the
// network operator can no longer substitute a per-peer TLS cert in a
// contract to mount a MITM: substituting the cert without also
// forging a signature under the (out-of-platform-control) client key
// produces a signature-verification failure on the sender side.
//
// Two uses for the key in the per-peer encryption flow:
//
//  1. Cert binding (Option 1 of the design). This client signs its
//     TLS-server cert chain with the client key. The signature is
//     published alongside the cert in `EncryptedKey` and propagated
//     into contracts as
//     `Contract.destination_client_key_signed_tls_certificate`. A
//     sender opening a session to us verifies the contract's cert
//     chain against this signature using our public client key (fetched
//     out-of-band), then verifies that the cert TLS presents during the
//     handshake matches a cert in the (now-validated) chain.
//
//  2. In-handshake identity proof (Option 4 of the design). After the
//     per-peer TLS handshake completes, each side derives 32 bytes of
//     keying material via `tls.ConnectionState.ExportKeyingMaterial`
//     under the label `urnetwork-sequence-identity-proof`, signs it
//     with its client key, and sends the signature to the peer in an
//     `EncryptedControl{IdentityProof, payload=signature}`. Each side
//     verifies the peer's signature against the peer's known public
//     client key. Only after the peer's signature has verified does
//     the AEAD cipher become observable from `Cipher()`. This binds
//     the actual session-key material to the long-lived identity:
//     even if cert distribution is fully compromised, an attacker who
//     terminates TLS to us on one leg and re-handshakes to the real
//     peer on the other gets two different exporters, and cannot
//     produce a signature over our exporter without our peer's
//     private key.
type ClientKeyManager struct {
	ctx    context.Context
	cancel context.CancelFunc
	client *Client

	privateKey ed25519.PrivateKey
	publicKey  ed25519.PublicKey

	controlSync         *ControlSync
	registrationSync    *ControlSyncOob
	registrationUpdates *MonitorValue[clientKeyRegistrationGeneration]
	registrationReady   *MonitorValue[bool]

	stateLock sync.RWMutex
	closed    bool
	workers   *lifecycleAdmission
	// Nil test barriers expose SetSeed admission and manager join entry.
	beforePublisherAdmissionLockForTest func()
	beforeCloseWaitForTest              func()
}

// NewClientKeyManager constructs the manager and seeds (or generates)
// the client's long-lived Ed25519 identity keypair. When
// `client.settings.ClientKeySeed` is set to a valid 32-byte seed, the
// keypair is loaded from it (`ed25519.NewKeyFromSeed`); otherwise a
// fresh seed is generated. Callers can read the running seed back via
// `Seed()` to persist it across restarts.
func NewClientKeyManager(ctx context.Context, client *Client) (*ClientKeyManager, error) {
	if client.settings.ClientKeyRegistrationRequired {
		control, ok := client.ClientOob().(*ApiOutOfBandControl)
		if !ok || control == nil || control.api == nil {
			return nil, errors.New("processed client key registration requires the actual ApiOutOfBandControl")
		}
	}
	var pub ed25519.PublicKey
	var priv ed25519.PrivateKey
	seed := client.settings.ClientKeySeed
	if len(seed) == ed25519.SeedSize {
		priv = ed25519.NewKeyFromSeed(seed)
		pub = priv.Public().(ed25519.PublicKey)
	} else {
		if 0 < len(seed) {
			// Caller supplied a seed of the wrong size — surface as
			// an explicit error rather than silently falling back to
			// fresh-generation, which would persist the wrong key on
			// the next save round-trip and likely break peer trust
			// silently.
			return nil, fmt.Errorf(
				"client key seed length %d (expected %d)",
				len(seed), ed25519.SeedSize,
			)
		}
		var err error
		pub, priv, err = ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("generate client key: %w", err)
		}
	}
	managerCtx, cancel := context.WithCancel(ctx)
	m := &ClientKeyManager{
		ctx:         managerCtx,
		cancel:      cancel,
		client:      client,
		privateKey:  priv,
		publicKey:   pub,
		controlSync: NewControlSync(managerCtx, client, "client-key"),
		workers:     newLifecycleAdmission(),
	}
	if client.settings.ClientKeyRegistrationRequired {
		m.registrationSync = NewControlSyncOob(managerCtx, client, "client-key-registration")
		m.registrationUpdates = NewMonitorValue(clientKeyRegistrationGeneration{publicKey: [ed25519.PublicKeySize]byte(pub), generation: 1})
		m.registrationReady = NewMonitorValue(false)
	}
	// Publish the public identity key once the client is fully wired.
	// `publishClientKey` waits on `client.ReadyNotify()` internally;
	// the goroutine launch here mirrors the pattern in
	// `NewContractManager` (`providePing`) and
	// `NewEncryptionSessionManager` (`publishEncryptedKey`).
	if !m.startPublisherWithLock() {
		cancel()
		return nil, fmt.Errorf("client key manager closed during construction")
	}
	return m, nil
}

// Admits the constructor's single processed-registration owner, or one legacy
// publisher. Later legacy callers hold stateLock; processed rotations coalesce.
func (self *ClientKeyManager) startPublisherWithLock() bool {
	if !self.workers.start() {
		return false
	}
	go func() {
		defer self.workers.finish()
		HandleError(self.publishClientKey)
	}()
	return true
}

// Seed returns the 32-byte Ed25519 private-key seed for the local
// client identity key. Sufficient to fully reconstruct the keypair
// via `ed25519.NewKeyFromSeed`. Sensitive: hand it only to local
// persistent storage that the user has authorized.
func (self *ClientKeyManager) Seed() []byte {
	self.stateLock.RLock()
	defer self.stateLock.RUnlock()

	if self.privateKey == nil {
		return nil
	}
	return self.privateKey.Seed()
}

// PublicKey returns the local client's 32-byte Ed25519 public key.
// Safe to publish; this is the value the platform stores per
// `client_id` and serves via the public-key lookup API.
func (self *ClientKeyManager) PublicKey() ed25519.PublicKey {
	self.stateLock.RLock()
	defer self.stateLock.RUnlock()

	return ed25519.PublicKey(bytes.Clone(self.publicKey))
}

// Sign produces an Ed25519 signature of `data` under the local client
// key. Used for cert-chain binding and post-handshake identity
// proofs; both verifications go through `VerifyClientKeySignature`
// (or `VerifyCertChainSignature` for the cert-chain shape).
func (self *ClientKeyManager) Sign(data []byte) []byte {
	self.stateLock.RLock()
	defer self.stateLock.RUnlock()

	return ed25519.Sign(self.privateKey, data)
}

// SetSeed replaces the local client's long-lived Ed25519 identity keypair
// with the key derived from seed and republishes the public key.
func (self *ClientKeyManager) SetSeed(seed []byte) error {
	if len(seed) != ed25519.SeedSize {
		return fmt.Errorf("client key seed length %d (expected %d)", len(seed), ed25519.SeedSize)
	}

	privateKey := ed25519.NewKeyFromSeed(bytes.Clone(seed))
	publicKey := privateKey.Public().(ed25519.PublicKey)

	if self.beforePublisherAdmissionLockForTest != nil {
		self.beforePublisherAdmissionLockForTest()
	}
	self.stateLock.Lock()
	if self.closed {
		self.stateLock.Unlock()
		return fmt.Errorf("client key manager closed")
	}
	if self.registrationSync != nil {
		if !bytes.Equal(self.publicKey, publicKey) {
			current := self.registrationUpdates.Value()
			if current.generation == math.MaxUint64 {
				self.stateLock.Unlock()
				return errors.New("client key publication generation reached its finite limit")
			}
			// Invalidate the old key before publishing the new local identity.
			self.registrationReady.Set(false)
			self.privateKey = privateKey
			self.publicKey = publicKey
			self.registrationUpdates.Set(clientKeyRegistrationGeneration{publicKey: [ed25519.PublicKeySize]byte(publicKey), generation: current.generation + 1})
		}
		self.stateLock.Unlock()
		return nil
	}
	if !self.startPublisherWithLock() {
		self.stateLock.Unlock()
		return fmt.Errorf("client key manager closed")
	}
	self.privateKey = privateKey
	self.publicKey = publicKey
	self.stateLock.Unlock()
	return nil
}

// Close prevents later key rotations and requests publication teardown
// without joining transfer-buffer ownership.
func (self *ClientKeyManager) Close() {
	self.stateLock.Lock()
	if self.closed {
		self.stateLock.Unlock()
		return
	}
	self.closed = true
	if self.registrationReady != nil {
		self.registrationReady.Set(false)
	}
	self.stateLock.Unlock()

	self.cancel()
	self.workers.close()
	self.controlSync.Close()
	if self.registrationSync != nil {
		self.registrationSync.Close()
	}
}

// closeAndWait joins every admitted publisher and its control retry workers,
// or returns when ctx expires.
func (self *ClientKeyManager) closeAndWait(ctx context.Context) error {
	self.Close()
	if self.beforeCloseWaitForTest != nil {
		self.beforeCloseWaitForTest()
	}
	var result error
	if err := waitForLifecycleDone(
		ctx,
		self.workers.Done(),
		"client key publishers",
	); err != nil {
		result = errors.Join(result, err)
	}
	if err := self.controlSync.closeAndWait(ctx); err != nil {
		result = errors.Join(result, err)
	}
	if self.registrationSync != nil {
		if err := self.registrationSync.closeAndWait(ctx); err != nil {
			result = errors.Join(result, err)
		}
	}
	return result
}

// SignCertChain signs the canonical encoding of a PEM cert chain
// (concatenation of the PEM blocks in order). Verifiers use
// `VerifyCertChainSignature` with the same chain and the publishing
// client's public key.
func (self *ClientKeyManager) SignCertChain(chain [][]byte) []byte {
	return self.Sign(canonicalCertChainBytes(chain))
}

// canonicalCertChainBytes builds the byte sequence that
// `SignCertChain` and `VerifyCertChainSignature` sign over: every PEM
// block concatenated in order. PEM blocks are self-delimiting
// (`-----BEGIN CERTIFICATE-----` / `-----END CERTIFICATE-----`
// fences plus newlines), so the concatenation has no boundary
// ambiguity.
func canonicalCertChainBytes(chain [][]byte) []byte {
	var n int
	for _, b := range chain {
		n += len(b)
	}
	buf := make([]byte, 0, n)
	for _, b := range chain {
		buf = append(buf, b...)
	}
	return buf
}

// VerifyClientKeySignature returns true iff `sig` is a valid Ed25519
// signature of `data` under `peerPub`. Returns false for any
// malformed input (wrong key or signature size) so callers can use it
// as a single guard without manual length checks.
func VerifyClientKeySignature(peerPub ed25519.PublicKey, data, sig []byte) bool {
	if len(peerPub) != ed25519.PublicKeySize {
		return false
	}
	if len(sig) != ed25519.SignatureSize {
		return false
	}
	return ed25519.Verify(peerPub, data, sig)
}

// VerifyCertChainSignature returns true iff `sig` is a valid Ed25519
// signature, under `peerPub`, of the canonical encoding of `chain`.
// Pair with `ClientKeyManager.SignCertChain` on the publishing side.
func VerifyCertChainSignature(peerPub ed25519.PublicKey, chain [][]byte, sig []byte) bool {
	return VerifyClientKeySignature(peerPub, canonicalCertChainBytes(chain), sig)
}

// publishClientKey sends a `ClientKey` control message to the platform
// with this client's public key. Idempotent on the platform side —
// every call replaces the stored key for this client. Uses
// `ControlSync` so the publish retries until acked.
//
// Started as a goroutine from `NewClientKeyManager`'s caller. Waits
// on the client's `ReadyNotify()` before its first send so the
// `sendBuffer` is wired up; without this gate the send path races the
// buffer wiring in `Client.initBuffers`.
func (self *ClientKeyManager) publishClientKey() {
	if self.registrationSync != nil {
		self.publishRegisteredClientKey()
		return
	}
	if self.controlSync == nil {
		return
	}
	select {
	case <-self.client.ReadyNotify():
	case <-self.ctx.Done():
		return
	}
	if self.client.settings.beforeClientKeyPublishForTest != nil {
		self.client.settings.beforeClientKeyPublishForTest()
	}
	frame, err := ToFrame(&protocol.ClientKey{
		PublicKey: []byte(self.PublicKey()),
	}, self.client.settings.ProtocolVersion)
	if err != nil {
		self.client.log.Errorf("[key]%s could not build ClientKey frame: %s\n", self.client.ClientTag(), err)
		return
	}
	self.controlSync.Send(frame, nil, nil)
}
