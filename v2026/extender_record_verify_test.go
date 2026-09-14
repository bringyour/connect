package connect

import (
	"crypto/ed25519"
	"encoding/hex"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/v2026/protocol"
)

// The refusal paths of the record layer (EXTENDER.md B1, B2).
//
// The happy paths are in extender_record_test.go. Everything here is a way a
// hostile or corrupt input reaches the verifier, and each one must be a plain
// refusal rather than a key that silently parses short or a body that decodes
// into something the signature did not cover.

// A persisted key is parsed only when it is a full seed or a full public key.
// Both forms trim the whitespace a file or an environment variable adds, so a
// key file with a trailing newline is the same key.
func TestExtenderKeyHexRefusesAnythingButAFullKey(t *testing.T) {
	seed, err := NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	publicKey, err := ExtenderPublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}

	for _, spacing := range []string{"%s", " %s", "%s\n", "\t %s \r\n"} {
		seedHex := strings.Replace(spacing, "%s", ExtenderKeySeedHex(seed), 1)
		parsedSeed, err := ParseExtenderKeySeedHex(seedHex)
		if err != nil {
			t.Errorf("ParseExtenderKeySeedHex(%q) = %v", seedHex, err)
			continue
		}
		if string(parsedSeed) != string(seed) {
			t.Errorf("ParseExtenderKeySeedHex(%q) returned another seed", seedHex)
		}
		publicKeyHex := strings.Replace(spacing, "%s", hex.EncodeToString(publicKey), 1)
		parsedPublicKey, err := ParseExtenderPublicKeyHex(publicKeyHex)
		if err != nil {
			t.Errorf("ParseExtenderPublicKeyHex(%q) = %v", publicKeyHex, err)
			continue
		}
		if !parsedPublicKey.Equal(publicKey) {
			t.Errorf("ParseExtenderPublicKeyHex(%q) returned another key", publicKeyHex)
		}
	}

	// a short, long or non-hex key is refused rather than padded
	refused := []string{
		"",
		"zz",
		hex.EncodeToString(seed[:ed25519.SeedSize-1]),
		hex.EncodeToString(append(append([]byte(nil), seed...), 0)),
	}
	for _, keyHex := range refused {
		if parsed, err := ParseExtenderKeySeedHex(keyHex); err == nil {
			t.Errorf("ParseExtenderKeySeedHex(%q) = %x, expected a refusal", keyHex, parsed)
		}
		if parsed, err := ParseExtenderPublicKeyHex(keyHex); err == nil {
			t.Errorf("ParseExtenderPublicKeyHex(%q) = %x, expected a refusal", keyHex, parsed)
		}
	}

	// the seed length is checked by the derivations too, not only by the parse
	for _, badSeed := range [][]byte{nil, {}, seed[:1], append(append([]byte(nil), seed...), 0)} {
		if _, err := ExtenderPrivateKeyFromSeed(badSeed); err == nil {
			t.Errorf("ExtenderPrivateKeyFromSeed(%d bytes) was accepted", len(badSeed))
		}
		if _, err := ExtenderPublicKeyFromSeed(badSeed); err == nil {
			t.Errorf("ExtenderPublicKeyFromSeed(%d bytes) was accepted", len(badSeed))
		}
	}
}

// A challenge signature is refused before ed25519 sees it when the key is not
// a full ed25519 key or the challenge is empty, so a truncated response frame
// cannot pass verification by handing in nothing to verify.
func TestExtenderChallengeVerifyRefusesAShortKeyOrAnEmptyChallenge(t *testing.T) {
	seed, err := NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	privateKey, err := ExtenderPrivateKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	publicKey := privateKey.Public().(ed25519.PublicKey)
	challenge, err := NewExtenderChallenge()
	if err != nil {
		t.Fatal(err)
	}
	signature := SignExtenderChallenge(privateKey, challenge)
	if !VerifyExtenderChallenge(publicKey, challenge, signature) {
		t.Fatal("the challenge signature does not verify")
	}

	cases := []struct {
		name      string
		publicKey ed25519.PublicKey
		challenge []byte
	}{
		{name: "nil key", publicKey: nil, challenge: challenge},
		{name: "short key", publicKey: publicKey[:ed25519.PublicKeySize-1], challenge: challenge},
		{
			name:      "long key",
			publicKey: append(append(ed25519.PublicKey(nil), publicKey...), 0),
			challenge: challenge,
		},
		{name: "nil challenge", publicKey: publicKey, challenge: nil},
		{name: "empty challenge", publicKey: publicKey, challenge: []byte{}},
	}
	for _, c := range cases {
		if VerifyExtenderChallenge(c.publicKey, c.challenge, signature) {
			t.Errorf("%s verified", c.name)
		}
	}
}

// The key set takes only full ed25519 keys: a short entry is dropped rather
// than stored, so a malformed hello key list can never widen the anchor.
func TestExtenderRootKeySetSkipsKeysThatAreNotEd25519(t *testing.T) {
	seed, err := NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	publicKey, err := ExtenderPublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	keySet := NewExtenderRootKeySet(
		nil,
		ed25519.PublicKey{},
		ed25519.PublicKey(publicKey[:ed25519.PublicKeySize-1]),
		publicKey,
		append(append(ed25519.PublicKey(nil), publicKey...), 0),
	)
	if keySet.Len() != 1 {
		t.Fatalf("key set holds %d keys, expected only the full one", keySet.Len())
	}
	if !keySet.PublicKeys()[0].Equal(publicKey) {
		t.Fatal("the key set kept another key")
	}
	// the accessor hands back a clone, so a caller cannot rewrite the anchor
	publicKeys := keySet.PublicKeys()
	publicKeys[0] = nil
	if keySet.Len() != 1 || !keySet.PublicKeys()[0].Equal(publicKey) {
		t.Fatal("mutating the returned slice changed the key set")
	}
}

// Two roots whose key ids collide are both tried, so a hint that names the
// wrong one of them is still a verification rather than a rejection.
func TestExtenderRootKeySetTriesEveryKeyUnderOneKeyId(t *testing.T) {
	first, firstPublic := newTestRootKey(t)
	_, secondPublic := newTestRootKey(t)
	keySet := NewExtenderRootKeySet(secondPublic, firstPublic)

	body := []byte("record body")
	signature := ed25519.Sign(first, extenderSigningBytes(ExtenderRecordSignatureDomain, body))
	// the hint names the other key's id, which is what a stale rotation looks
	// like on the wire
	verified, err := keySet.Verify(
		ExtenderRecordSignatureDomain, body, signature, ExtenderKeyId(secondPublic))
	if err != nil {
		t.Fatal(err)
	}
	if !verified.Equal(firstPublic) {
		t.Fatal("the fallback returned another key")
	}
	// a hint nobody carries still falls back to every accepted key
	verified, err = keySet.Verify(
		ExtenderRecordSignatureDomain, body, signature, []byte("no such id"))
	if err != nil {
		t.Fatal(err)
	}
	if !verified.Equal(firstPublic) {
		t.Fatal("the unhinted fallback returned another key")
	}
}

// A revocation is refused when it is missing, when its signature does not
// verify, and when the signed bytes are not a revocation body -- the body is
// decoded only after the signature covers it.
func TestExtenderRevocationRefusals(t *testing.T) {
	rootPrivateKey, rootPublicKey := newTestRootKey(t)
	keySet := NewExtenderRootKeySet(rootPublicKey)

	if _, err := keySet.VerifyRevocation(nil); err == nil {
		t.Error("a missing revocation verified")
	}

	// a body that is signed but is not a revocation body
	notABody := []byte{0xff, 0xff, 0xff, 0xff}
	signed := &protocol.ExtenderRevocation{
		Body: notABody,
		RootSignature: ed25519.Sign(
			rootPrivateKey,
			extenderSigningBytes(ExtenderRevocationSignatureDomain, notABody),
		),
		RootKeyId: ExtenderKeyId(rootPublicKey),
	}
	if _, err := keySet.VerifyRevocation(signed); err == nil {
		t.Error("a signed body that is not a revocation body decoded")
	}
	// the same shape for a record
	signedRecord := &protocol.ExtenderRecord{
		Body: notABody,
		RootSignature: ed25519.Sign(
			rootPrivateKey,
			extenderSigningBytes(ExtenderRecordSignatureDomain, notABody),
		),
		RootKeyId: ExtenderKeyId(rootPublicKey),
	}
	if _, err := keySet.VerifyRecord(signedRecord); err == nil {
		t.Error("a signed body that is not a record body decoded")
	}

	// a revocation under the record domain does not verify, and the reverse
	publicKey := newTestExtenderKey(t)
	revocationBody, err := proto.Marshal(&protocol.ExtenderRevocationBody{
		PublicKey:   publicKey,
		IssueTimeMs: 1,
		NetworkHost: testExtenderNetworkHost,
	})
	if err != nil {
		t.Fatal(err)
	}
	crossDomain := &protocol.ExtenderRevocation{
		Body: revocationBody,
		RootSignature: ed25519.Sign(
			rootPrivateKey,
			extenderSigningBytes(ExtenderRecordSignatureDomain, revocationBody),
		),
		RootKeyId: ExtenderKeyId(rootPublicKey),
	}
	if _, err := keySet.VerifyRevocation(crossDomain); err == nil {
		t.Error("a record signature verified as a revocation")
	}

	// an empty key set accepts nothing at all
	empty := NewExtenderRootKeySet()
	if _, err := empty.VerifyRevocation(signed); err == nil {
		t.Error("an empty key set verified a revocation")
	}
	if _, err := empty.VerifyRecord(signedRecord); err == nil {
		t.Error("an empty key set verified a record")
	}
}

// The network host comparison ignores case, a trailing dot and surrounding
// whitespace on both sides, and matches nothing when either side is empty, so
// a record cannot join a space by naming it blank.
func TestExtenderNetworkHostCheckNormalizesBothSides(t *testing.T) {
	cases := []struct {
		networkHost  string
		allowedHosts []string
		want         bool
	}{
		{networkHost: " space.example ", allowedHosts: []string{"space.example"}, want: true},
		{networkHost: "space.example", allowedHosts: []string{" space.example. "}, want: true},
		{networkHost: "SPACE.Example.", allowedHosts: []string{"space.example"}, want: true},
		{networkHost: "space.example", allowedHosts: []string{"other.example", "space.example"}, want: true},
		{networkHost: "   ", allowedHosts: []string{"space.example"}, want: false},
		{networkHost: ".", allowedHosts: []string{"space.example"}, want: false},
		{networkHost: "space.example", allowedHosts: []string{"   "}, want: false},
		{networkHost: "space.example", allowedHosts: []string{""}, want: false},
		{networkHost: "space.example", allowedHosts: nil, want: false},
		{networkHost: "", allowedHosts: []string{""}, want: false},
	}
	for _, c := range cases {
		if allowed := ExtenderNetworkHostAllowed(c.networkHost, c.allowedHosts...); allowed != c.want {
			t.Errorf(
				"ExtenderNetworkHostAllowed(%q, %v) = %v, expected %v",
				c.networkHost, c.allowedHosts, allowed, c.want)
		}
	}
}

func newTestRootKey(t *testing.T) (ed25519.PrivateKey, ed25519.PublicKey) {
	t.Helper()
	seed, err := NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	privateKey, err := ExtenderPrivateKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	return privateKey, privateKey.Public().(ed25519.PublicKey)
}
