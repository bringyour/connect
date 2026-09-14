package connect

// Golden vectors for the canonical `/verify` signed-message encodings
// (sn/VALIDATOR.md Appendix A). The expected hex strings below were derived
// by hand from the Appendix A layouts (and cross-checked with an independent
// non-Go Ed25519/encoding implementation) — they pin the byte format across
// the server, validator, and any future implementation. Do not regenerate
// them from this package's own output.
//
// Fixed inputs, chosen so every field is recognizable in the hex:
//
//	ctx          = "urnetwork/verify/v1" (19 bytes)
//	             = 75726e6574776f726b2f7665726966792f7631
//	vpk          = 0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20 (32 bytes, 0x01..0x20)
//	client_nonce = a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf (32 bytes, 0xa0..0xbf)
//	server_nonce = c0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedf (32 bytes, 0xc0..0xdf)
//	trail_id     = 404142434445464748494a4b4c4d4e4f (16 bytes, 0x40..0x4f)
//	client_id_1  = 505152535455565758595a5b5c5d5e5f (16 bytes, 0x50..0x5f)
//	client_id_2  = 606162636465666768696a6b6c6d6e6f (16 bytes, 0x60..0x6f)
//	client_id_3  = 707172737475767778797a7b7c7d7e7f (16 bytes, 0x70..0x7f)
//	M            = 8 (0x08), server_key_id = 7 (0x07)
//	time_ms_1    = 0x1122334455667788, time_ms_2 = 0x99aabbccddeeff00 (big-endian)

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

const (
	testVerifyCtxHex         = "75726e6574776f726b2f7665726966792f7631"
	testVerifyVpkHex         = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
	testVerifyClientNonceHex = "a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf"
	testVerifyServerNonceHex = "c0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedf"
	testVerifyTrailIdHex     = "404142434445464748494a4b4c4d4e4f"
	testVerifyClientId1Hex   = "505152535455565758595a5b5c5d5e5f"
	testVerifyClientId2Hex   = "606162636465666768696a6b6c6d6e6f"
	testVerifyClientId3Hex   = "707172737475767778797a7b7c7d7e7f"
)

func testVerifyHexBytes(t *testing.T, hexStr string) []byte {
	b, err := hex.DecodeString(hexStr)
	AssertEqual(t, nil, err)
	return b
}

func testVerifyHexId(t *testing.T, hexStr string) Id {
	return RequireIdFromBytes(testVerifyHexBytes(t, hexStr))
}

// TestVerifyCtxBytes pins the context string itself: 19 ASCII bytes, written
// verbatim at the start of every canonical message.
func TestVerifyCtxBytes(t *testing.T) {
	AssertEqual(t, 19, len(VerifyCtx))
	AssertEqual(t, testVerifyCtxHex, hex.EncodeToString([]byte(VerifyCtx)))
}

// TestBuildVerifySeedMessageGolden pins the SEED (A.1) layout, 85 bytes:
//
//	[0,19)   ctx
//	[19]     msg_type 0x01
//	[20,52)  vpk
//	[52,84)  client_nonce
//	[84]     M = 0x08
func TestBuildVerifySeedMessageGolden(t *testing.T) {
	message, err := BuildVerifySeedMessage(
		testVerifyHexBytes(t, testVerifyVpkHex),
		testVerifyHexBytes(t, testVerifyClientNonceHex),
		8,
	)
	AssertEqual(t, nil, err)
	AssertEqual(t, 85, len(message))
	expectedHex := testVerifyCtxHex +
		"01" +
		testVerifyVpkHex +
		testVerifyClientNonceHex +
		"08"
	AssertEqual(t, expectedHex, hex.EncodeToString(message))
}

// TestBuildVerifyExtendMessageGolden pins the EXTEND (A.2) layout at depth
// k=2 (confirmed seed hop + the pending hop being claimed), 134 bytes:
//
//	[0,19)    ctx
//	[19]      msg_type 0x02
//	[20,36)   trail_id
//	[36,68)   server_nonce
//	[68,100)  vpk
//	[100]     M = 0x08
//	[101]     k = 0x02
//	[102,118) client_id_1
//	[118,134) client_id_2
func TestBuildVerifyExtendMessageGolden(t *testing.T) {
	message, err := BuildVerifyExtendMessage(
		testVerifyHexId(t, testVerifyTrailIdHex),
		testVerifyHexBytes(t, testVerifyServerNonceHex),
		testVerifyHexBytes(t, testVerifyVpkHex),
		8,
		[]Id{
			testVerifyHexId(t, testVerifyClientId1Hex),
			testVerifyHexId(t, testVerifyClientId2Hex),
		},
	)
	AssertEqual(t, nil, err)
	AssertEqual(t, 134, len(message))
	expectedHex := testVerifyCtxHex +
		"02" +
		testVerifyTrailIdHex +
		testVerifyServerNonceHex +
		testVerifyVpkHex +
		"08" +
		"02" +
		testVerifyClientId1Hex +
		testVerifyClientId2Hex
	AssertEqual(t, expectedHex, hex.EncodeToString(message))
}

// TestBuildVerifyAssignMessageGolden pins the ASSIGN (A.3) layout at depth 3
// (two confirmed hops + the newly assigned pending hop last), 151 bytes:
//
//	[0,19)    ctx
//	[19]      msg_type 0x03
//	[20]      server_key_id = 0x07
//	[21,37)   trail_id
//	[37,69)   server_nonce
//	[69,101)  vpk
//	[101]     M = 0x08
//	[102]     depth = 0x03
//	[103,119) client_id_1
//	[119,135) client_id_2
//	[135,151) client_id_3 (the pending next hop)
func TestBuildVerifyAssignMessageGolden(t *testing.T) {
	message, err := BuildVerifyAssignMessage(
		7,
		testVerifyHexId(t, testVerifyTrailIdHex),
		testVerifyHexBytes(t, testVerifyServerNonceHex),
		testVerifyHexBytes(t, testVerifyVpkHex),
		8,
		[]Id{
			testVerifyHexId(t, testVerifyClientId1Hex),
			testVerifyHexId(t, testVerifyClientId2Hex),
			testVerifyHexId(t, testVerifyClientId3Hex),
		},
	)
	AssertEqual(t, nil, err)
	AssertEqual(t, 151, len(message))
	expectedHex := testVerifyCtxHex +
		"03" +
		"07" +
		testVerifyTrailIdHex +
		testVerifyServerNonceHex +
		testVerifyVpkHex +
		"08" +
		"03" +
		testVerifyClientId1Hex +
		testVerifyClientId2Hex +
		testVerifyClientId3Hex
	AssertEqual(t, expectedHex, hex.EncodeToString(message))
}

// TestBuildVerifyFinalMessageGolden pins the FINAL (A.4) layout at M=2, 150
// bytes. There is no separate count byte — M itself is the hop-pair count.
// Times are big-endian uint64:
//
//	[0,19)    ctx
//	[19]      msg_type 0x04
//	[20]      server_key_id = 0x07
//	[21,37)   trail_id
//	[37,69)   server_nonce
//	[69,101)  vpk
//	[101]     M = 0x02
//	[102,118) client_id_1
//	[118,126) time_ms_1 = 1122334455667788 (big-endian)
//	[126,158) egress_ip_hash_1 = 0xaa×32 (D27)
//	[158,174) client_id_2
//	[174,182) time_ms_2 = 99aabbccddeeff00 (big-endian)
//	[182,214) egress_ip_hash_2 = 0xbb×32
func TestBuildVerifyFinalMessageGolden(t *testing.T) {
	message, err := BuildVerifyFinalMessage(
		7,
		testVerifyHexId(t, testVerifyTrailIdHex),
		testVerifyHexBytes(t, testVerifyServerNonceHex),
		testVerifyHexBytes(t, testVerifyVpkHex),
		2,
		[]VerifyProofHop{
			{
				ClientId:     testVerifyHexId(t, testVerifyClientId1Hex),
				TimeMs:       0x1122334455667788,
				EgressIpHash: testVerifyFill32(0xaa),
			},
			{
				ClientId:     testVerifyHexId(t, testVerifyClientId2Hex),
				TimeMs:       0x99aabbccddeeff00,
				EgressIpHash: testVerifyFill32(0xbb),
			},
		},
	)
	AssertEqual(t, nil, err)
	AssertEqual(t, 214, len(message))
	expectedHex := testVerifyCtxHex +
		"04" +
		"07" +
		testVerifyTrailIdHex +
		testVerifyServerNonceHex +
		testVerifyVpkHex +
		"02" +
		testVerifyClientId1Hex +
		"1122334455667788" +
		strings.Repeat("aa", 32) +
		testVerifyClientId2Hex +
		"99aabbccddeeff00" +
		strings.Repeat("bb", 32)
	AssertEqual(t, expectedHex, hex.EncodeToString(message))
}

// testVerifyFill32 returns a [32]byte filled with b (D27 egress-IP-hash test vectors).
func testVerifyFill32(b byte) [32]byte {
	var out [32]byte
	for i := range out {
		out[i] = b
	}
	return out
}

// TestVerifyFinalDigestGolden pins the compact artifact identity: sha256 over
// the exact golden FINAL message above. FINAL signatures cover the raw
// canonical message. The
// expected hex was computed with an independent non-Go sha256 over the same
// message bytes.
func TestVerifyFinalDigestGolden(t *testing.T) {
	message, err := BuildVerifyFinalMessage(
		7,
		testVerifyHexId(t, testVerifyTrailIdHex),
		testVerifyHexBytes(t, testVerifyServerNonceHex),
		testVerifyHexBytes(t, testVerifyVpkHex),
		2,
		[]VerifyProofHop{
			{
				ClientId:     testVerifyHexId(t, testVerifyClientId1Hex),
				TimeMs:       0x1122334455667788,
				EgressIpHash: testVerifyFill32(0xaa),
			},
			{
				ClientId:     testVerifyHexId(t, testVerifyClientId2Hex),
				TimeMs:       0x99aabbccddeeff00,
				EgressIpHash: testVerifyFill32(0xbb),
			},
		},
	)
	AssertEqual(t, nil, err)
	digest := VerifyFinalDigest(message)
	AssertEqual(
		t,
		"5fd6b821ebe7815e67b6b1ea65de0d4cf2d6acb5646895928fa10149e97c6395",
		hex.EncodeToString(digest[:]),
	)
}

// TestVerifySeedSignatureGolden pins the deterministic Ed25519 (RFC 8032)
// signature over a SEED message. The signing key is derived from the fixed
// seed 0x42×32; vpk is that key's public half, so the vector is exactly the
// §4.1 flow. The expected public key and signature were computed with an
// independent non-Go RFC 8032 implementation over the same message bytes.
func TestVerifySeedSignatureGolden(t *testing.T) {
	keySeed := make([]byte, ed25519.SeedSize)
	for i := range keySeed {
		keySeed[i] = 0x42
	}
	signingKey := ed25519.NewKeyFromSeed(keySeed)
	vpk := signingKey.Public().(ed25519.PublicKey)
	AssertEqual(
		t,
		"2152f8d19b791d24453242e15f2eab6cb7cffa7b6a5ed30097960e069881db12",
		hex.EncodeToString(vpk),
	)

	message, err := BuildVerifySeedMessage(vpk, testVerifyHexBytes(t, testVerifyClientNonceHex), 8)
	AssertEqual(t, nil, err)
	expectedMessageHex := testVerifyCtxHex +
		"01" +
		"2152f8d19b791d24453242e15f2eab6cb7cffa7b6a5ed30097960e069881db12" +
		testVerifyClientNonceHex +
		"08"
	AssertEqual(t, expectedMessageHex, hex.EncodeToString(message))

	seedSig := SignVerifyMessage(signingKey, message)
	AssertEqual(
		t,
		"e54aa51676a585dd9ed2b714cad985fc84f78e951bb05833190999eeb81934720bfd3dc443044eece951cfd14a98f56615223437a1efc65409d58e8273ac4608",
		hex.EncodeToString(seedSig),
	)
	AssertEqual(t, true, VerifyVerifyMessageSignature(vpk, message, seedSig))
}

// TestVerifyMessageSignRoundTrip signs each of the four canonical message
// types and verifies: the right key verifies, a tampered message fails, a
// tampered signature fails, a wrong key fails, and malformed key/signature
// sizes return false defensively.
func TestVerifyMessageSignRoundTrip(t *testing.T) {
	publicKey, signingKey, err := ed25519.GenerateKey(nil)
	AssertEqual(t, nil, err)
	otherPublicKey, _, err := ed25519.GenerateKey(nil)
	AssertEqual(t, nil, err)

	serverNonce := testVerifyHexBytes(t, testVerifyServerNonceHex)
	trailId := testVerifyHexId(t, testVerifyTrailIdHex)
	trailClientIds := []Id{
		testVerifyHexId(t, testVerifyClientId1Hex),
		testVerifyHexId(t, testVerifyClientId2Hex),
	}

	seedMessage, err := BuildVerifySeedMessage(publicKey, testVerifyHexBytes(t, testVerifyClientNonceHex), 8)
	AssertEqual(t, nil, err)
	extendMessage, err := BuildVerifyExtendMessage(trailId, serverNonce, publicKey, 8, trailClientIds)
	AssertEqual(t, nil, err)
	assignMessage, err := BuildVerifyAssignMessage(7, trailId, serverNonce, publicKey, 8, trailClientIds)
	AssertEqual(t, nil, err)
	finalMessage, err := BuildVerifyFinalMessage(7, trailId, serverNonce, publicKey, 2, []VerifyProofHop{
		{ClientId: trailClientIds[0], TimeMs: 1000},
		{ClientId: trailClientIds[1], TimeMs: 2000},
	})
	AssertEqual(t, nil, err)

	for _, message := range [][]byte{seedMessage, extendMessage, assignMessage, finalMessage} {
		signature := SignVerifyMessage(signingKey, message)
		AssertEqual(t, ed25519.SignatureSize, len(signature))
		AssertEqual(t, true, VerifyVerifyMessageSignature(publicKey, message, signature))

		// tampered message must fail
		tamperedMessage := append([]byte(nil), message...)
		tamperedMessage[0] ^= 0x01
		AssertEqual(t, false, VerifyVerifyMessageSignature(publicKey, tamperedMessage, signature))

		// tampered signature must fail
		tamperedSignature := append([]byte(nil), signature...)
		tamperedSignature[0] ^= 0x01
		AssertEqual(t, false, VerifyVerifyMessageSignature(publicKey, message, tamperedSignature))

		// wrong key must fail
		AssertEqual(t, false, VerifyVerifyMessageSignature(otherPublicKey, message, signature))

		// malformed inputs return false, not panic
		AssertEqual(t, false, VerifyVerifyMessageSignature(nil, message, signature))
		AssertEqual(t, false, VerifyVerifyMessageSignature(publicKey[:16], message, signature))
		AssertEqual(t, false, VerifyVerifyMessageSignature(publicKey, message, signature[:32]))
		AssertEqual(t, false, VerifyVerifyMessageSignature(publicKey, message, nil))
	}
}

// TestBuildVerifyMessageBadInput exercises the encoding-level input checks:
// every fixed-width field must have its exact Appendix A size, hop counts
// must fit the 1-byte count, and FINAL must carry exactly M hops.
func TestBuildVerifyMessageBadInput(t *testing.T) {
	vpk := testVerifyHexBytes(t, testVerifyVpkHex)
	clientNonce := testVerifyHexBytes(t, testVerifyClientNonceHex)
	serverNonce := testVerifyHexBytes(t, testVerifyServerNonceHex)
	trailId := testVerifyHexId(t, testVerifyTrailIdHex)
	clientId1 := testVerifyHexId(t, testVerifyClientId1Hex)
	tooManyClientIds := make([]Id, 256)

	cases := []struct {
		name  string
		build func() ([]byte, error)
	}{
		{"seed short vpk", func() ([]byte, error) {
			return BuildVerifySeedMessage(vpk[:31], clientNonce, 8)
		}},
		{"seed long client nonce", func() ([]byte, error) {
			return BuildVerifySeedMessage(vpk, append(clientNonce, 0x00), 8)
		}},
		{"extend short server nonce", func() ([]byte, error) {
			return BuildVerifyExtendMessage(trailId, serverNonce[:31], vpk, 8, []Id{clientId1})
		}},
		{"extend empty vpk", func() ([]byte, error) {
			return BuildVerifyExtendMessage(trailId, serverNonce, nil, 8, []Id{clientId1})
		}},
		{"extend empty trail", func() ([]byte, error) {
			return BuildVerifyExtendMessage(trailId, serverNonce, vpk, 8, []Id{})
		}},
		{"extend trail over 255", func() ([]byte, error) {
			return BuildVerifyExtendMessage(trailId, serverNonce, vpk, 8, tooManyClientIds)
		}},
		{"assign empty trail", func() ([]byte, error) {
			return BuildVerifyAssignMessage(7, trailId, serverNonce, vpk, 8, nil)
		}},
		{"assign trail over 255", func() ([]byte, error) {
			return BuildVerifyAssignMessage(7, trailId, serverNonce, vpk, 8, tooManyClientIds)
		}},
		{"assign short vpk", func() ([]byte, error) {
			return BuildVerifyAssignMessage(7, trailId, serverNonce, vpk[:16], 8, []Id{clientId1})
		}},
		{"final hop count under M", func() ([]byte, error) {
			return BuildVerifyFinalMessage(7, trailId, serverNonce, vpk, 3, []VerifyProofHop{{ClientId: clientId1, TimeMs: 1}})
		}},
		{"final zero M", func() ([]byte, error) {
			return BuildVerifyFinalMessage(7, trailId, serverNonce, vpk, 0, []VerifyProofHop{})
		}},
		{"final short server nonce", func() ([]byte, error) {
			return BuildVerifyFinalMessage(7, trailId, serverNonce[:16], vpk, 1, []VerifyProofHop{{ClientId: clientId1, TimeMs: 1}})
		}},
	}
	for _, c := range cases {
		message, err := c.build()
		if err == nil {
			t.Errorf("%s: expected error, got message %x", c.name, message)
		}
		if message != nil {
			t.Errorf("%s: expected nil message on error", c.name)
		}
	}
}

// TestVerifySeedArgsJson pins the JSON wire shape of the SEED request body:
// snake_case names (`M` verbatim per the spec), ids as uuid strings, byte
// fields as standard base64.
func TestVerifySeedArgsJson(t *testing.T) {
	seedArgs := &VerifySeedArgs{
		ClientId:    testVerifyHexId(t, testVerifyClientId1Hex),
		Vpk:         testVerifyHexBytes(t, testVerifyVpkHex),
		ClientNonce: testVerifyHexBytes(t, testVerifyClientNonceHex),
		SeedSig:     testVerifyHexBytes(t, "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"),
		M:           8,
	}
	seedArgsJson, err := json.Marshal(seedArgs)
	AssertEqual(t, nil, err)
	expectedJson := `{"client_id":"50515253-5455-5657-5859-5a5b5c5d5e5f",` +
		`"vpk":"AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA=",` +
		`"client_nonce":"oKGio6SlpqeoqaqrrK2ur7CxsrO0tba3uLm6u7y9vr8=",` +
		`"seed_sig":"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8gISIjJCUmJygpKissLS4vMDEyMzQ1Njc4OTo7PD0+Pw==",` +
		`"M":8}`
	AssertEqual(t, expectedJson, string(seedArgsJson))

	var parsedSeedArgs VerifySeedArgs
	err = json.Unmarshal(seedArgsJson, &parsedSeedArgs)
	AssertEqual(t, nil, err)
	AssertEqual(t, *seedArgs, parsedSeedArgs)
}

// TestVerifyWireJsonRoundTrip round-trips the EXTEND request body and the
// ASSIGN/FINAL response bodies (including a full proof) through JSON.
func TestVerifyWireJsonRoundTrip(t *testing.T) {
	extendArgs := &VerifyExtendArgs{
		ClientId: testVerifyHexId(t, testVerifyClientId3Hex),
		TrailId:  testVerifyHexId(t, testVerifyTrailIdHex),
		Trail: []Id{
			testVerifyHexId(t, testVerifyClientId1Hex),
			testVerifyHexId(t, testVerifyClientId2Hex),
		},
		ExtendSig: testVerifyHexBytes(t, testVerifyServerNonceHex),
	}
	extendArgsJson, err := json.Marshal(extendArgs)
	AssertEqual(t, nil, err)
	var parsedExtendArgs VerifyExtendArgs
	err = json.Unmarshal(extendArgsJson, &parsedExtendArgs)
	AssertEqual(t, nil, err)
	AssertEqual(t, *extendArgs, parsedExtendArgs)

	assignResult := &VerifyAssignResult{
		TrailId:     testVerifyHexId(t, testVerifyTrailIdHex),
		ServerNonce: testVerifyHexBytes(t, testVerifyServerNonceHex),
		Trail: []Id{
			testVerifyHexId(t, testVerifyClientId1Hex),
		},
		NextHop:     testVerifyHexId(t, testVerifyClientId2Hex),
		M:           8,
		ServerKeyId: 7,
		AssignSig:   testVerifyHexBytes(t, testVerifyClientNonceHex),
	}
	assignResultJson, err := json.Marshal(assignResult)
	AssertEqual(t, nil, err)
	var parsedAssignResult VerifyAssignResult
	err = json.Unmarshal(assignResultJson, &parsedAssignResult)
	AssertEqual(t, nil, err)
	AssertEqual(t, *assignResult, parsedAssignResult)

	finalResult := &VerifyFinalResult{
		Status: VerifyStatusComplete,
		Proof: &VerifyProof{
			Header: VerifyProofHeader{
				TrailId:     testVerifyHexId(t, testVerifyTrailIdHex),
				ServerNonce: testVerifyHexBytes(t, testVerifyServerNonceHex),
				Vpk:         testVerifyHexBytes(t, testVerifyVpkHex),
				M:           2,
			},
			Hops: []VerifyProofHop{
				{ClientId: testVerifyHexId(t, testVerifyClientId1Hex), TimeMs: 0x1122334455667788},
				{ClientId: testVerifyHexId(t, testVerifyClientId2Hex), TimeMs: 0x99aabbccddeeff00},
			},
			ServerKeyId: 7,
			FinalSig:    testVerifyHexBytes(t, testVerifyClientNonceHex),
			VerifierSig: testVerifyHexBytes(t, testVerifyServerNonceHex),
		},
	}
	finalResultJson, err := json.Marshal(finalResult)
	AssertEqual(t, nil, err)
	var parsedFinalResult VerifyFinalResult
	err = json.Unmarshal(finalResultJson, &parsedFinalResult)
	AssertEqual(t, nil, err)
	AssertEqual(t, VerifyStatusComplete, parsedFinalResult.Status)
	AssertEqual(t, *finalResult.Proof, *parsedFinalResult.Proof)
}
