package connect

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
)

// TestClientKeyManagerGenerateFresh verifies that constructing a
// ClientKeyManager with no `ClientKeySeed` in settings produces a
// keypair whose `Seed()` and `PublicKey()` are mutually consistent
// (the public key derived from `ed25519.NewKeyFromSeed(Seed())`
// matches `PublicKey()`).
func TestClientKeyManagerGenerateFresh(t *testing.T) {
	ctx := context.Background()
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()

	m, err := NewClientKeyManager(ctx, client)
	AssertEqual(t, nil, err)

	seed := m.Seed()
	AssertEqual(t, ed25519.SeedSize, len(seed))

	pub := m.PublicKey()
	AssertEqual(t, ed25519.PublicKeySize, len(pub))

	// Public key must be the one derived from the seed.
	derived := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)
	AssertEqual(t, true, ed25519.PublicKey(pub).Equal(derived))
}

// TestClientKeyManagerLoadSeed verifies that a 32-byte seed in
// `settings.ClientKeySeed` is loaded into the manager — `Seed()`
// returns the same bytes, and `PublicKey()` is the public half
// derived from that seed.
func TestClientKeyManagerLoadSeed(t *testing.T) {
	ctx := context.Background()
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff

	seed := make([]byte, ed25519.SeedSize)
	_, err := rand.Read(seed)
	AssertEqual(t, nil, err)
	settings.ClientKeySeed = seed

	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()

	m, err := NewClientKeyManager(ctx, client)
	AssertEqual(t, nil, err)

	AssertEqual(t, seed, m.Seed())

	expectedPub := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)
	AssertEqual(t, true, ed25519.PublicKey(m.PublicKey()).Equal(expectedPub))
}

// TestClientKeyManagerLoadSeedWrongSize verifies that a non-zero
// seed of the wrong length is a construction error, not a silent
// fall-back to generation — silently generating would persist the
// wrong key on the next save round-trip and break peer trust.
func TestClientKeyManagerLoadSeedWrongSize(t *testing.T) {
	ctx := context.Background()
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	settings.ClientKeySeed = []byte{0, 1, 2, 3} // wrong size
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()

	m, err := NewClientKeyManager(ctx, client)
	if err == nil {
		t.Fatalf("expected error for wrong seed size, got manager %v", m)
	}
}

// TestClientKeyManagerSignVerifyRoundTrip exercises the basic
// Sign/Verify path: a manager signs arbitrary bytes, and
// `VerifyClientKeySignature` succeeds against the public key and the
// same bytes, fails against tampered bytes.
func TestClientKeyManagerSignVerifyRoundTrip(t *testing.T) {
	ctx := context.Background()
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()

	m, err := NewClientKeyManager(ctx, client)
	AssertEqual(t, nil, err)

	data := []byte("identity proof exporter material")
	sig := m.Sign(data)
	AssertEqual(t, ed25519.SignatureSize, len(sig))

	AssertEqual(t, true, VerifyClientKeySignature(m.PublicKey(), data, sig))

	// Tampered data must fail.
	tampered := append([]byte(nil), data...)
	tampered[0] ^= 0x01
	AssertEqual(t, false, VerifyClientKeySignature(m.PublicKey(), tampered, sig))

	// Tampered signature must fail.
	badSig := append([]byte(nil), sig...)
	badSig[0] ^= 0x01
	AssertEqual(t, false, VerifyClientKeySignature(m.PublicKey(), data, badSig))

	// Wrong public key must fail.
	_, otherPriv, err := ed25519.GenerateKey(rand.Reader)
	AssertEqual(t, nil, err)
	otherPub := otherPriv.Public().(ed25519.PublicKey)
	AssertEqual(t, false, VerifyClientKeySignature(otherPub, data, sig))
}

// TestClientKeyManagerCertChainSignature exercises the cert-chain
// variant. The signature is computed over the canonical
// concatenation of PEM blocks; `VerifyCertChainSignature` rebuilds
// the same concatenation and verifies. Tampering any block must
// fail.
func TestClientKeyManagerCertChainSignature(t *testing.T) {
	ctx := context.Background()
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()

	m, err := NewClientKeyManager(ctx, client)
	AssertEqual(t, nil, err)

	chain := [][]byte{
		[]byte("-----BEGIN CERTIFICATE-----\nAAA=\n-----END CERTIFICATE-----\n"),
		[]byte("-----BEGIN CERTIFICATE-----\nBBB=\n-----END CERTIFICATE-----\n"),
	}
	sig := m.SignCertChain(chain)
	AssertEqual(t, ed25519.SignatureSize, len(sig))

	AssertEqual(t, true, VerifyCertChainSignature(m.PublicKey(), chain, sig))

	// Tampering any block must invalidate.
	tampered := make([][]byte, len(chain))
	copy(tampered, chain)
	tampered[1] = []byte("-----BEGIN CERTIFICATE-----\nCCC=\n-----END CERTIFICATE-----\n")
	AssertEqual(t, false, VerifyCertChainSignature(m.PublicKey(), tampered, sig))

	// Reordering blocks must invalidate (concatenation is
	// position-sensitive).
	reordered := [][]byte{chain[1], chain[0]}
	AssertEqual(t, false, VerifyCertChainSignature(m.PublicKey(), reordered, sig))

	// Dropping a block must invalidate.
	AssertEqual(t, false, VerifyCertChainSignature(m.PublicKey(), chain[:1], sig))
}

// TestVerifyClientKeySignatureMalformedInput documents the
// defensive return-false-on-malformed contract of
// `VerifyClientKeySignature`: wrong key size, wrong signature size,
// empty inputs all return false rather than panicking.
func TestVerifyClientKeySignatureMalformedInput(t *testing.T) {
	// Empty key.
	AssertEqual(t, false, VerifyClientKeySignature(nil, []byte("data"), make([]byte, ed25519.SignatureSize)))

	// Wrong-size key.
	AssertEqual(t, false, VerifyClientKeySignature(make([]byte, 10), []byte("data"), make([]byte, ed25519.SignatureSize)))

	// Wrong-size signature.
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	AssertEqual(t, nil, err)
	AssertEqual(t, false, VerifyClientKeySignature(pub, []byte("data"), []byte{0, 1, 2}))

	// Empty signature.
	AssertEqual(t, false, VerifyClientKeySignature(pub, []byte("data"), nil))
}

// TestCanonicalCertChainBytes documents the concatenation contract:
// PEM blocks are joined in order without separators (PEM frames
// self-delimit).
func TestCanonicalCertChainBytes(t *testing.T) {
	chain := [][]byte{
		[]byte("AAA"),
		[]byte("BB"),
		[]byte("C"),
	}
	got := canonicalCertChainBytes(chain)
	AssertEqual(t, []byte("AAABBC"), got)

	// Empty chain → empty bytes (and nil-safe).
	AssertEqual(t, 0, len(canonicalCertChainBytes(nil)))
	AssertEqual(t, 0, len(canonicalCertChainBytes([][]byte{})))
}
