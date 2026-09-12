package connect

import (
	"crypto/ed25519"
	"crypto/sha256"
	"slices"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/protocol"
)

// A seed round-trips through its persisted hex form and yields one key pair.
func TestExtenderKeySeedRoundTrips(t *testing.T) {
	seed, err := NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	if len(seed) != ed25519.SeedSize {
		t.Fatalf("seed is %d bytes, expected %d", len(seed), ed25519.SeedSize)
	}
	parsedSeed, err := ParseExtenderKeySeedHex(ExtenderKeySeedHex(seed))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(seed, parsedSeed) {
		t.Fatal("the seed did not round trip through hex")
	}
	publicKey, err := ExtenderPublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	privateKey, err := ExtenderPrivateKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	if !publicKey.Equal(privateKey.Public()) {
		t.Fatal("the public key does not belong to the private key")
	}
	parsedPublicKey, err := ParseExtenderPublicKeyHex(ExtenderKeySeedHex(publicKey))
	if err != nil {
		t.Fatal(err)
	}
	if !publicKey.Equal(parsedPublicKey) {
		t.Fatal("the public key did not round trip through hex")
	}

	badCases := []string{"", "zz", ExtenderKeySeedHex(seed[0 : len(seed)-1])}
	for _, badSeedHex := range badCases {
		if _, err := ParseExtenderKeySeedHex(badSeedHex); err == nil {
			t.Fatalf("%q was accepted as a seed", badSeedHex)
		}
	}
}

// The key id is the leading bytes of the sha256 of the public key (B2).
func TestExtenderKeyIdIsTheLeadingHashBytes(t *testing.T) {
	seed, err := NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	publicKey, err := ExtenderPublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(publicKey)
	keyId := ExtenderKeyId(publicKey)
	if len(keyId) != ExtenderKeyIdByteCount {
		t.Fatalf("key id is %d bytes, expected %d", len(keyId), ExtenderKeyIdByteCount)
	}
	if !slices.Equal(keyId, sum[0:ExtenderKeyIdByteCount]) {
		t.Fatal("the key id is not the leading hash bytes")
	}
	keyId[0] ^= 0xff
	if slices.Equal(ExtenderKeyId(publicKey), keyId) {
		t.Fatal("the key id shares storage with its caller")
	}
}

// Builds one signed record for tests.
func newTestExtenderRecord(t *testing.T, rootSeed []byte, networkHost string) (*protocol.ExtenderRecord, *protocol.ExtenderRecordBody) {
	t.Helper()
	rootPrivateKey, err := ExtenderPrivateKeyFromSeed(rootSeed)
	if err != nil {
		t.Fatal(err)
	}
	extenderSeed, err := NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	extenderPublicKey, err := ExtenderPublicKeyFromSeed(extenderSeed)
	if err != nil {
		t.Fatal(err)
	}
	body := &protocol.ExtenderRecordBody{
		PublicKey: extenderPublicKey,
		Addresses: []*protocol.ExtenderAddress{
			{
				Ip:        "192.0.2.10",
				IpVersion: 4,
				Carriers:  []string{ExtenderCarrierTcp, ExtenderCarrierQuic, ExtenderCarrierDns},
			},
			{
				Ip:        "2001:db8::10",
				IpVersion: 6,
				Carriers:  []string{ExtenderCarrierTcp},
			},
		},
		TcpPort:      443,
		UdpPort:      443,
		DnsPort:      53,
		DnsTld:       "x.example.",
		CountryCode:  "us",
		IssueTimeMs:  uint64(time.Now().UnixMilli()),
		ExpireTimeMs: uint64(time.Now().Add(14 * 24 * time.Hour).UnixMilli()),
		NetworkHost:  networkHost,
	}
	record, err := SignExtenderRecord(rootPrivateKey, body)
	if err != nil {
		t.Fatal(err)
	}
	return record, body
}

// A record round-trips through the signature and the opaque body, and a
// mutated body no longer verifies (B2).
func TestExtenderRecordRoundTripsAndDetectsMutation(t *testing.T) {
	rootSeed, err := NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	rootPublicKey, err := ExtenderPublicKeyFromSeed(rootSeed)
	if err != nil {
		t.Fatal(err)
	}
	record, body := newTestExtenderRecord(t, rootSeed, "host.example")

	if !slices.Equal(record.RootKeyId, ExtenderKeyId(rootPublicKey)) {
		t.Fatal("the record does not name the signing key")
	}
	keySet := NewExtenderRootKeySet(rootPublicKey)
	verifiedBody, err := keySet.VerifyRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(verifiedBody, body) {
		t.Fatal("the verified body is not the signed body")
	}

	mutatedBody := proto.Clone(body).(*protocol.ExtenderRecordBody)
	mutatedBody.Addresses[0].Ip = "192.0.2.11"
	mutatedBodyBytes, err := proto.Marshal(mutatedBody)
	if err != nil {
		t.Fatal(err)
	}
	mutatedRecord := &protocol.ExtenderRecord{
		Body:          mutatedBodyBytes,
		RootSignature: record.RootSignature,
		RootKeyId:     record.RootKeyId,
	}
	if _, err := keySet.VerifyRecord(mutatedRecord); err == nil {
		t.Fatal("a mutated record body verified")
	}

	truncatedRecord := &protocol.ExtenderRecord{
		Body:          record.Body,
		RootSignature: record.RootSignature[0 : len(record.RootSignature)-1],
		RootKeyId:     record.RootKeyId,
	}
	if _, err := keySet.VerifyRecord(truncatedRecord); err == nil {
		t.Fatal("a truncated signature verified")
	}
	if _, err := keySet.VerifyRecord(nil); err == nil {
		t.Fatal("a missing record verified")
	}
}

// A revocation carries its own signature domain, so a record signature can
// never stand in for it (B2).
func TestExtenderRevocationRoundTripsUnderItsOwnDomain(t *testing.T) {
	rootSeed, err := NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	rootPrivateKey, err := ExtenderPrivateKeyFromSeed(rootSeed)
	if err != nil {
		t.Fatal(err)
	}
	rootPublicKey, err := ExtenderPublicKeyFromSeed(rootSeed)
	if err != nil {
		t.Fatal(err)
	}
	extenderSeed, err := NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	extenderPublicKey, err := ExtenderPublicKeyFromSeed(extenderSeed)
	if err != nil {
		t.Fatal(err)
	}
	body := &protocol.ExtenderRevocationBody{
		PublicKey:   extenderPublicKey,
		IssueTimeMs: uint64(time.Now().UnixMilli()),
		NetworkHost: "host.example",
	}
	revocation, err := SignExtenderRevocation(rootPrivateKey, body)
	if err != nil {
		t.Fatal(err)
	}
	keySet := NewExtenderRootKeySet(rootPublicKey)
	verifiedBody, err := keySet.VerifyRevocation(revocation)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(verifiedBody, body) {
		t.Fatal("the verified body is not the signed body")
	}

	// the same bytes signed for a record must not verify as a revocation
	recordShaped, err := SignExtenderRecord(rootPrivateKey, &protocol.ExtenderRecordBody{})
	if err != nil {
		t.Fatal(err)
	}
	crossDomain := &protocol.ExtenderRevocation{
		Body:          recordShaped.Body,
		RootSignature: recordShaped.RootSignature,
		RootKeyId:     recordShaped.RootKeyId,
	}
	if _, err := keySet.VerifyRevocation(crossDomain); err == nil {
		t.Fatal("a record signature verified as a revocation")
	}
}

// The key set selects by key id and falls back to every accepted key, and a
// key outside the set is rejected (B2, B4).
func TestExtenderRootKeySetSelectsAndFallsBack(t *testing.T) {
	seeds := [][]byte{}
	publicKeys := []ed25519.PublicKey{}
	for range 3 {
		seed, err := NewExtenderKeySeed()
		if err != nil {
			t.Fatal(err)
		}
		publicKey, err := ExtenderPublicKeyFromSeed(seed)
		if err != nil {
			t.Fatal(err)
		}
		seeds = append(seeds, seed)
		publicKeys = append(publicKeys, publicKey)
	}
	keySet := NewExtenderRootKeySet(publicKeys[0], publicKeys[1])
	if keySet.Len() != 2 {
		t.Fatalf("key set holds %d keys, expected two", keySet.Len())
	}

	// the second key signs, so selection by key id has to find it
	record, _ := newTestExtenderRecord(t, seeds[1], "host.example")
	if _, err := keySet.VerifyRecord(record); err != nil {
		t.Fatal(err)
	}

	// a wrong key id still verifies, by trying every accepted key
	wrongIdRecord := &protocol.ExtenderRecord{
		Body:          record.Body,
		RootSignature: record.RootSignature,
		RootKeyId:     ExtenderKeyId(publicKeys[2]),
	}
	if _, err := keySet.VerifyRecord(wrongIdRecord); err != nil {
		t.Fatalf("a record with a stale key id was rejected: %v", err)
	}
	missingIdRecord := &protocol.ExtenderRecord{
		Body:          record.Body,
		RootSignature: record.RootSignature,
	}
	if _, err := keySet.VerifyRecord(missingIdRecord); err != nil {
		t.Fatalf("a record with no key id was rejected: %v", err)
	}

	// a key that is not accepted is rejected however it is named
	rejectedRecord, _ := newTestExtenderRecord(t, seeds[2], "host.example")
	if _, err := keySet.VerifyRecord(rejectedRecord); err == nil {
		t.Fatal("a record signed by a key outside the set verified")
	}
	if _, err := NewExtenderRootKeySet().VerifyRecord(record); err == nil {
		t.Fatal("an empty key set verified a record")
	}

	hexKeySet, err := NewExtenderRootKeySetFromHex(
		ExtenderKeySeedHex(publicKeys[0]),
		"",
		ExtenderKeySeedHex(publicKeys[1]),
	)
	if err != nil {
		t.Fatal(err)
	}
	if hexKeySet.Len() != 2 {
		t.Fatalf("hex key set holds %d keys, expected two", hexKeySet.Len())
	}
	if _, err := hexKeySet.VerifyRecord(record); err != nil {
		t.Fatal(err)
	}
	if _, err := NewExtenderRootKeySetFromHex("not-hex"); err == nil {
		t.Fatal("a malformed hex key was accepted")
	}
}

// A challenge signature verifies only under the key that made it and only for
// the challenge that was sent (A4).
func TestExtenderChallengeSignatureIsBoundToItsKeyAndChallenge(t *testing.T) {
	seed, err := NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	privateKey, err := ExtenderPrivateKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	publicKey, err := ExtenderPublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	otherSeed, err := NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	otherPublicKey, err := ExtenderPublicKeyFromSeed(otherSeed)
	if err != nil {
		t.Fatal(err)
	}

	challenge, err := NewExtenderChallenge()
	if err != nil {
		t.Fatal(err)
	}
	if len(challenge) != ExtenderChallengeByteCount {
		t.Fatalf("challenge is %d bytes, expected %d", len(challenge), ExtenderChallengeByteCount)
	}
	signature := SignExtenderChallenge(privateKey, challenge)
	if !VerifyExtenderChallenge(publicKey, challenge, signature) {
		t.Fatal("the challenge signature does not verify")
	}
	if VerifyExtenderChallenge(otherPublicKey, challenge, signature) {
		t.Fatal("the challenge signature verified under the wrong key")
	}
	otherChallenge, err := NewExtenderChallenge()
	if err != nil {
		t.Fatal(err)
	}
	if VerifyExtenderChallenge(publicKey, otherChallenge, signature) {
		t.Fatal("the challenge signature verified for another challenge")
	}
	if VerifyExtenderChallenge(publicKey, nil, signature) {
		t.Fatal("an empty challenge verified")
	}
}

// A record is applied only in the network space it names (B2).
func TestExtenderNetworkHostCheck(t *testing.T) {
	cases := []struct {
		networkHost         string
		allowedNetworkHosts []string
		allowed             bool
	}{
		{networkHost: "host.example", allowedNetworkHosts: []string{"host.example"}, allowed: true},
		{networkHost: "HOST.example.", allowedNetworkHosts: []string{"host.example"}, allowed: true},
		{networkHost: "host.example", allowedNetworkHosts: []string{"other.example", "host.example"}, allowed: true},
		{networkHost: "other.example", allowedNetworkHosts: []string{"host.example"}, allowed: false},
		{networkHost: "", allowedNetworkHosts: []string{"host.example"}, allowed: false},
		{networkHost: "host.example", allowedNetworkHosts: []string{""}, allowed: false},
		{networkHost: "host.example", allowedNetworkHosts: nil, allowed: false},
	}
	for _, c := range cases {
		if allowed := ExtenderNetworkHostAllowed(c.networkHost, c.allowedNetworkHosts...); allowed != c.allowed {
			t.Errorf(
				"ExtenderNetworkHostAllowed(%q, %v) = %t, expected %t",
				c.networkHost,
				c.allowedNetworkHosts,
				allowed,
				c.allowed,
			)
		}
	}
}

// The response frame is self-delimiting, so the reader is left exactly on the
// first byte that follows it (A3).
func TestExtenderResponseFrameIsSelfDelimiting(t *testing.T) {
	response := &protocol.ExtenderResponse{
		PublicKey: []byte{1, 2, 3},
		Carriers:  []string{ExtenderCarrierTcp, ExtenderCarrierQuic},
	}
	frameBytes, err := ExtenderResponseFrame(response)
	if err != nil {
		t.Fatal(err)
	}
	trailing := []byte("the inner bytes")
	reader := &sliceReader{data: append(slices.Clone(frameBytes), trailing...)}
	readResponse, err := ReadExtenderResponseFrame(reader)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(readResponse, response) {
		t.Fatal("the response did not round trip")
	}
	remaining := make([]byte, len(trailing))
	if _, err := reader.Read(remaining); err != nil {
		t.Fatal(err)
	}
	if string(remaining) != string(trailing) {
		t.Fatalf("remaining = %q, expected %q", remaining, trailing)
	}
}

// sliceReader hands out bytes one call at a time, so a frame reader that
// over-reads is caught.
type sliceReader struct {
	data []byte
}

func (self *sliceReader) Read(b []byte) (int, error) {
	n := min(len(b), len(self.data))
	copy(b[0:n], self.data[0:n])
	self.data = self.data[n:]
	return n, nil
}
