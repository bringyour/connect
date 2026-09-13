package connect

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/protocol"
)

// Extender identity keys, signed directory records and the root key set that
// validates them (EXTENDER.md B1, B2).
//
// Every signature is ed25519 over a domain string concatenated with opaque
// bytes. Records and revocations sign the already serialized body rather than
// the enclosing message, so a re-serialization with a different field order or
// an unknown field cannot change what was signed: the body travels as the
// exact bytes that were signed and is decoded only after verification.
//
// The functions here are safe for concurrent use; ExtenderRootKeySet is
// immutable after construction.

const (
	// signature domain of a record body
	ExtenderRecordSignatureDomain = "ur-extender-record-v1"
	// signature domain of a revocation body
	ExtenderRevocationSignatureDomain = "ur-extender-revocation-v1"
	// signature domain of a probe challenge
	ExtenderChallengeSignatureDomain = "ur-extender-challenge-v1"
)

// Length of a key id, the leading bytes of the sha256 of a public key.
const ExtenderKeyIdByteCount = 8

// Length of the challenge a prober sends in the extender header (A4).
const ExtenderChallengeByteCount = 32

// Creates one identity seed.
func NewExtenderKeySeed() ([]byte, error) {
	seed := make([]byte, ed25519.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		return nil, err
	}
	return seed, nil
}

// Hex encoding of a seed, the form persisted in local state and the vault.
func ExtenderKeySeedHex(seed []byte) string {
	return hex.EncodeToString(seed)
}

// Parses the persisted form, rejecting anything that is not a full seed.
func ParseExtenderKeySeedHex(seedHex string) ([]byte, error) {
	seed, err := hex.DecodeString(strings.TrimSpace(seedHex))
	if err != nil {
		return nil, err
	}
	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("extender key seed must be %d bytes, got %d", ed25519.SeedSize, len(seed))
	}
	return seed, nil
}

// Derives the signing key of a seed.
func ExtenderPrivateKeyFromSeed(seed []byte) (ed25519.PrivateKey, error) {
	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("extender key seed must be %d bytes, got %d", ed25519.SeedSize, len(seed))
	}
	return ed25519.NewKeyFromSeed(seed), nil
}

// Derives the published key of a seed.
func ExtenderPublicKeyFromSeed(seed []byte) (ed25519.PublicKey, error) {
	privateKey, err := ExtenderPrivateKeyFromSeed(seed)
	if err != nil {
		return nil, err
	}
	return privateKey.Public().(ed25519.PublicKey), nil
}

// Parses a hex public key, rejecting anything that is not an ed25519 key.
func ParseExtenderPublicKeyHex(publicKeyHex string) (ed25519.PublicKey, error) {
	publicKey, err := hex.DecodeString(strings.TrimSpace(publicKeyHex))
	if err != nil {
		return nil, err
	}
	if len(publicKey) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("extender public key must be %d bytes, got %d", ed25519.PublicKeySize, len(publicKey))
	}
	return ed25519.PublicKey(publicKey), nil
}

// Short stable identifier of a public key, used to select a root key without
// trying every one. It is a hint only; verification never trusts it.
func ExtenderKeyId(publicKey []byte) []byte {
	sum := sha256.Sum256(publicKey)
	return slices.Clone(sum[0:ExtenderKeyIdByteCount])
}

// Concatenates the domain separator and the opaque bytes that are signed.
func extenderSigningBytes(domain string, body []byte) []byte {
	signingBytes := make([]byte, 0, len(domain)+len(body))
	signingBytes = append(signingBytes, []byte(domain)...)
	signingBytes = append(signingBytes, body...)
	return signingBytes
}

// Creates a probe challenge (A4).
func NewExtenderChallenge() ([]byte, error) {
	challenge := make([]byte, ExtenderChallengeByteCount)
	if _, err := rand.Read(challenge); err != nil {
		return nil, err
	}
	return challenge, nil
}

// Signs the challenge an extender was given in the request header.
func SignExtenderChallenge(privateKey ed25519.PrivateKey, challenge []byte) []byte {
	return ed25519.Sign(privateKey, extenderSigningBytes(ExtenderChallengeSignatureDomain, challenge))
}

// Checks a challenge response against the extender's published key.
func VerifyExtenderChallenge(publicKey ed25519.PublicKey, challenge []byte, signature []byte) bool {
	if len(publicKey) != ed25519.PublicKeySize || len(challenge) == 0 {
		return false
	}
	return ed25519.Verify(publicKey, extenderSigningBytes(ExtenderChallengeSignatureDomain, challenge), signature)
}

// Serializes and signs a record body with a root key.
func SignExtenderRecord(
	rootPrivateKey ed25519.PrivateKey,
	body *protocol.ExtenderRecordBody,
) (*protocol.ExtenderRecord, error) {
	bodyBytes, err := proto.Marshal(body)
	if err != nil {
		return nil, err
	}
	rootPublicKey := rootPrivateKey.Public().(ed25519.PublicKey)
	return &protocol.ExtenderRecord{
		Body:          bodyBytes,
		RootSignature: ed25519.Sign(rootPrivateKey, extenderSigningBytes(ExtenderRecordSignatureDomain, bodyBytes)),
		RootKeyId:     ExtenderKeyId(rootPublicKey),
	}, nil
}

// Serializes and signs a revocation body with a root key.
func SignExtenderRevocation(
	rootPrivateKey ed25519.PrivateKey,
	body *protocol.ExtenderRevocationBody,
) (*protocol.ExtenderRevocation, error) {
	bodyBytes, err := proto.Marshal(body)
	if err != nil {
		return nil, err
	}
	rootPublicKey := rootPrivateKey.Public().(ed25519.PublicKey)
	return &protocol.ExtenderRevocation{
		Body:          bodyBytes,
		RootSignature: ed25519.Sign(rootPrivateKey, extenderSigningBytes(ExtenderRevocationSignatureDomain, bodyBytes)),
		RootKeyId:     ExtenderKeyId(rootPublicKey),
	}, nil
}

// The accepted root public keys of one network space (B4). Rotation is a list,
// not a single key, so records signed by a retiring key stay valid while the
// new key is published.
type ExtenderRootKeySet struct {
	rootPublicKeys []ed25519.PublicKey
	// key id to every key with that id; a collision is possible in principle
	// and costs only an extra verification
	rootKeyIdPublicKeys map[string][]ed25519.PublicKey
}

// Builds a key set, skipping keys that are not ed25519 keys.
func NewExtenderRootKeySet(rootPublicKeys ...ed25519.PublicKey) *ExtenderRootKeySet {
	keySet := &ExtenderRootKeySet{
		rootPublicKeys:      []ed25519.PublicKey{},
		rootKeyIdPublicKeys: map[string][]ed25519.PublicKey{},
	}
	for _, rootPublicKey := range rootPublicKeys {
		if len(rootPublicKey) != ed25519.PublicKeySize {
			continue
		}
		keySet.rootPublicKeys = append(keySet.rootPublicKeys, rootPublicKey)
		keyId := string(ExtenderKeyId(rootPublicKey))
		keySet.rootKeyIdPublicKeys[keyId] = append(keySet.rootKeyIdPublicKeys[keyId], rootPublicKey)
	}
	return keySet
}

// Builds a key set from the hex form carried by the network space and hello.
func NewExtenderRootKeySetFromHex(rootPublicKeyHexes ...string) (*ExtenderRootKeySet, error) {
	rootPublicKeys := []ed25519.PublicKey{}
	for _, rootPublicKeyHex := range rootPublicKeyHexes {
		if strings.TrimSpace(rootPublicKeyHex) == "" {
			continue
		}
		rootPublicKey, err := ParseExtenderPublicKeyHex(rootPublicKeyHex)
		if err != nil {
			return nil, err
		}
		rootPublicKeys = append(rootPublicKeys, rootPublicKey)
	}
	return NewExtenderRootKeySet(rootPublicKeys...), nil
}

// The accepted keys, in the order given.
func (self *ExtenderRootKeySet) PublicKeys() []ed25519.PublicKey {
	return slices.Clone(self.rootPublicKeys)
}

// Count of accepted keys.
func (self *ExtenderRootKeySet) Len() int {
	return len(self.rootPublicKeys)
}

// Verify checks a domain-separated signature over opaque bytes. The key id is
// a hint: the keys that carry it are tried first and every other accepted key
// after, so a stale or wrong id costs time but never a false rejection. The
// key that verified is returned.
func (self *ExtenderRootKeySet) Verify(
	domain string,
	body []byte,
	signature []byte,
	rootKeyId []byte,
) (ed25519.PublicKey, error) {
	if len(self.rootPublicKeys) == 0 {
		return nil, fmt.Errorf("no extender root keys are accepted")
	}
	signingBytes := extenderSigningBytes(domain, body)
	hintedPublicKeys := self.rootKeyIdPublicKeys[string(rootKeyId)]
	for _, rootPublicKey := range hintedPublicKeys {
		if ed25519.Verify(rootPublicKey, signingBytes, signature) {
			return rootPublicKey, nil
		}
	}
	for _, rootPublicKey := range self.rootPublicKeys {
		alreadyTried := false
		for _, hintedPublicKey := range hintedPublicKeys {
			if hintedPublicKey.Equal(rootPublicKey) {
				alreadyTried = true
				break
			}
		}
		if alreadyTried {
			continue
		}
		if ed25519.Verify(rootPublicKey, signingBytes, signature) {
			return rootPublicKey, nil
		}
	}
	return nil, fmt.Errorf("extender signature does not verify under any accepted root key")
}

// Verifies a record and decodes the body that was signed.
func (self *ExtenderRootKeySet) VerifyRecord(
	record *protocol.ExtenderRecord,
) (*protocol.ExtenderRecordBody, error) {
	if record == nil {
		return nil, fmt.Errorf("extender record is missing")
	}
	if _, err := self.Verify(
		ExtenderRecordSignatureDomain,
		record.Body,
		record.RootSignature,
		record.RootKeyId,
	); err != nil {
		return nil, err
	}
	body := &protocol.ExtenderRecordBody{}
	if err := proto.Unmarshal(record.Body, body); err != nil {
		return nil, err
	}
	return body, nil
}

// Verifies a revocation and decodes the body that was signed.
func (self *ExtenderRootKeySet) VerifyRevocation(
	revocation *protocol.ExtenderRevocation,
) (*protocol.ExtenderRevocationBody, error) {
	if revocation == nil {
		return nil, fmt.Errorf("extender revocation is missing")
	}
	if _, err := self.Verify(
		ExtenderRevocationSignatureDomain,
		revocation.Body,
		revocation.RootSignature,
		revocation.RootKeyId,
	); err != nil {
		return nil, err
	}
	body := &protocol.ExtenderRevocationBody{}
	if err := proto.Unmarshal(revocation.Body, body); err != nil {
		return nil, err
	}
	return body, nil
}

// Reports whether a signed record or revocation belongs to a network space the
// caller is part of (B2). The comparison ignores case and a trailing dot. An
// empty candidate matches nothing: a record must name its space.
func ExtenderNetworkHostAllowed(networkHost string, allowedNetworkHosts ...string) bool {
	normalize := func(host string) string {
		return strings.ToLower(strings.TrimSuffix(strings.TrimSpace(host), "."))
	}
	candidate := normalize(networkHost)
	if candidate == "" {
		return false
	}
	for _, allowedNetworkHost := range allowedNetworkHosts {
		if allowed := normalize(allowedNetworkHost); allowed != "" && allowed == candidate {
			return true
		}
	}
	return false
}
