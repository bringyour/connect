package connect

// verify_wire.go — shared wire types and canonical signed-message encodings for
// the `/verify` routing-verification protocol (see sn/VALIDATOR.md).
//
// This file is imported by both the API server (route handlers, ASSIGN/FINAL
// signing) and the validator binary (SEED/EXTEND signing, proof verification),
// so the two sides can never disagree on bytes. Everything here is
// gomobile-safe: stdlib only, no Client dependency.
//
// Two layers:
//   - JSON wire types for the `POST /verify` request/response bodies
//     (VALIDATOR.md §4.1/§4.2). Byte fields ([]byte) are base64 on the wire by
//     the standard encoding/json convention; ids are uuid strings (`Id`).
//   - Canonical binary messages (VALIDATOR.md Appendix A) — the exact byte
//     strings the four Ed25519 signatures are computed over. Signatures are
//     never over JSON. All multi-byte integers are big-endian.
//
// All functions are pure and safe for concurrent use.

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

// VerifyCtx is the global domain-separation context prepended verbatim (19
// ASCII bytes) to every canonical `/verify` message (VALIDATOR.md §3.2,
// Appendix A).
const VerifyCtx = "urnetwork/verify/v1"

// The 1-byte message types, written immediately after the context string
// (VALIDATOR.md §3.2).
const (
	VerifyMsgTypeSeed   byte = 0x01
	VerifyMsgTypeExtend byte = 0x02
	VerifyMsgTypeAssign byte = 0x03
	VerifyMsgTypeFinal  byte = 0x04
)

// VerifyNonceSize is the fixed size in bytes of both the `client_nonce`
// (SEED) and the per-trail `server_nonce` (VALIDATOR.md Appendix A).
const VerifyNonceSize = 32

// VerifyStatusComplete is the `status` value of a final `/verify` response
// (VALIDATOR.md §4.2 step 7).
const VerifyStatusComplete = "complete"

// Trail depth parameter defaults (VALIDATOR.md §5.5). The server clamps a
// requested M to [VerifyMMin, VerifyMMax]; the response echoes the effective
// M (`VerifyAssignResult.M`).
const (
	VerifyMDefault = 8
	VerifyMMin     = 4
	VerifyMMax     = 16
)

// VerifySeedArgs is the SEED request body for `POST /verify` (VALIDATOR.md
// §4.1) — starts a new trail through the validator-chosen entry provider.
// `ClientId` is the validator's own client id, so the server can check
// `Vpk` equals that client's registered Ed25519 key without a reverse key
// index. `SeedSig` is an Ed25519 signature by the validator's private key
// over `BuildVerifySeedMessage(Vpk, ClientNonce, M)`.
type VerifySeedArgs struct {
	ClientId    Id     `json:"client_id"`
	Vpk         []byte `json:"vpk"`
	ClientNonce []byte `json:"client_nonce"`
	SeedSig     []byte `json:"seed_sig"`
	M           int    `json:"M"`
}

// VerifyExtendArgs is the EXTEND request body for `POST /verify`
// (VALIDATOR.md §4.2) — claims the pending hop. `Trail` is the ordered
// confirmed hops plus the single pending hop being claimed
// (`VerifyAssignResult.Trail` + `VerifyAssignResult.NextHop`). `ClientId` is
// the validator's own client id (same role as in `VerifySeedArgs`).
// `ExtendSig` is an Ed25519 signature by the validator's private key over
// `BuildVerifyExtendMessage(TrailId, serverNonce, vpk, m, Trail)`.
type VerifyExtendArgs struct {
	ClientId  Id     `json:"client_id"`
	TrailId   Id     `json:"trail_id"`
	Trail     []Id   `json:"trail"`
	ExtendSig []byte `json:"extend_sig"`
}

// VerifyAssignResult is the non-final `POST /verify` response (VALIDATOR.md
// §4.1 step 7, §4.2 step 7 else-branch) — the server's signed commitment to
// the next hop. `Trail` is the ordered confirmed hops (ids only; times are
// published only in the final proof) and `NextHop` is the newly assigned
// pending hop, so the next EXTEND submits `Trail` + `NextHop`. `M` is the
// server-clamped effective trail depth (needed to build the EXTEND message).
// `AssignSig` is the server's Ed25519 signature over
// `BuildVerifyAssignMessage(ServerKeyId, TrailId, ServerNonce, vpk, M,
// Trail + NextHop)`, verifiable with the `/verify/keys` key for
// `ServerKeyId`.
type VerifyAssignResult struct {
	TrailId     Id     `json:"trail_id"`
	ServerNonce []byte `json:"server_nonce"`
	Trail       []Id   `json:"trail"`
	NextHop     Id     `json:"next_hop"`
	M           int    `json:"M"`
	ServerKeyId byte   `json:"server_key_id"`
	AssignSig   []byte `json:"assign_sig"`
}

// VerifyFinalResult is the final `POST /verify` response (VALIDATOR.md §4.2
// step 7): `Status` is `VerifyStatusComplete` and `Proof` is the published
// proof.
type VerifyFinalResult struct {
	Status string       `json:"status"`
	Proof  *VerifyProof `json:"proof"`
}

// VerifyProofHeader is the published proof header (VALIDATOR.md §3.3).
type VerifyProofHeader struct {
	TrailId     Id     `json:"trail_id"`
	ServerNonce []byte `json:"server_nonce"`
	Vpk         []byte `json:"vpk"`
	M           int    `json:"M"`
}

// VerifyProofHop is one confirmed hop of a completed trail: the canonical
// provider, its server-stamped confirmation time in unix milliseconds UTC
// (VALIDATOR.md §3.3, §3.4), and the hash of the provider's egress IP at the
// subnet's configured prefix granularity (VALIDATOR.md §8.1, D27 — used for the
// head routable-IP score). The egress-IP-hash is inside the signed FINAL
// message (BuildVerifyFinalMessage), so the server attests which IP-prefix each
// hop egressed from and a fleet cannot forge the IPs it routed.
type VerifyProofHop struct {
	ClientId     Id       `json:"client_id"`
	TimeMs       uint64   `json:"time_ms"`
	EgressIpHash [32]byte `json:"egress_ip_hash"`
}

// VerifyProof is the published, non-repudiable completed trail (VALIDATOR.md
// §3.3). `Coverage` is deterministic measurement metadata (v1 = M-1, the
// server-assigned confirmed hops with the seed excluded). `FinalSig` is the
// server's Ed25519 signature over the raw canonical
// `BuildVerifyFinalMessage(...)` bytes. `VerifierSig` is the validator's
// depth-M EXTEND signature (the wire name `verifier_sig` is historical — it is
// the validator's signature). `ServerKeyId` selects the published server key
// that verifies `FinalSig` across rotations.
type VerifyProof struct {
	Header      VerifyProofHeader `json:"header"`
	Hops        []VerifyProofHop  `json:"hops"`
	ServerKeyId byte              `json:"server_key_id"`
	Coverage    uint64            `json:"coverage"`
	FinalSig    []byte            `json:"final_sig"`
	VerifierSig []byte            `json:"verifier_sig"`
}

// BuildVerifySeedMessage returns the canonical SEED message (VALIDATOR.md
// A.1), signed by the validator:
//
//	CTX(19) ‖ 0x01 ‖ vpk(32) ‖ client_nonce(32) ‖ M(1)
func BuildVerifySeedMessage(vpk []byte, clientNonce []byte, m byte) ([]byte, error) {
	if len(vpk) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("vpk must be %d bytes: %d", ed25519.PublicKeySize, len(vpk))
	}
	if len(clientNonce) != VerifyNonceSize {
		return nil, fmt.Errorf("client_nonce must be %d bytes: %d", VerifyNonceSize, len(clientNonce))
	}
	message := make([]byte, 0, len(VerifyCtx)+1+ed25519.PublicKeySize+VerifyNonceSize+1)
	message = append(message, VerifyCtx...)
	message = append(message, VerifyMsgTypeSeed)
	message = append(message, vpk...)
	message = append(message, clientNonce...)
	message = append(message, m)
	return message, nil
}

// BuildVerifyExtendMessage returns the canonical EXTEND message at depth
// `k = len(trailClientIds)` (VALIDATOR.md A.2), signed by the validator:
//
//	CTX(19) ‖ 0x02 ‖ trail_id(16) ‖ server_nonce(32) ‖ vpk(32) ‖ M(1)
//	        ‖ k(1) ‖ client_id_1(16) ‖ … ‖ client_id_k(16)
//
// `trailClientIds` is the confirmed hops plus the pending hop being claimed
// at position k. No times — the server is authoritative for time
// (VALIDATOR.md §3.4). The depth-M EXTEND message is what `verifier_sig` in
// the published proof signs.
func BuildVerifyExtendMessage(trailId Id, serverNonce []byte, vpk []byte, m byte, trailClientIds []Id) ([]byte, error) {
	if len(serverNonce) != VerifyNonceSize {
		return nil, fmt.Errorf("server_nonce must be %d bytes: %d", VerifyNonceSize, len(serverNonce))
	}
	if len(vpk) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("vpk must be %d bytes: %d", ed25519.PublicKeySize, len(vpk))
	}
	k := len(trailClientIds)
	if k == 0 || 255 < k {
		return nil, fmt.Errorf("trail must have 1 to 255 client ids: %d", k)
	}
	message := make([]byte, 0, len(VerifyCtx)+1+16+VerifyNonceSize+ed25519.PublicKeySize+2+16*k)
	message = append(message, VerifyCtx...)
	message = append(message, VerifyMsgTypeExtend)
	message = append(message, trailId[:]...)
	message = append(message, serverNonce...)
	message = append(message, vpk...)
	message = append(message, m)
	message = append(message, byte(k))
	for _, clientId := range trailClientIds {
		message = append(message, clientId[:]...)
	}
	return message, nil
}

// BuildVerifyAssignMessage returns the canonical ASSIGN message at
// `depth = len(trailClientIds)` (VALIDATOR.md A.3), signed by the server:
//
//	CTX(19) ‖ 0x03 ‖ server_key_id(1) ‖ trail_id(16) ‖ server_nonce(32) ‖ vpk(32)
//	        ‖ M(1) ‖ depth(1) ‖ client_id_1(16) ‖ … ‖ client_id_depth(16)
//
// `trailClientIds` is the confirmed hops plus the newly assigned (pending)
// next hop in the last position.
func BuildVerifyAssignMessage(serverKeyId byte, trailId Id, serverNonce []byte, vpk []byte, m byte, trailClientIds []Id) ([]byte, error) {
	if len(serverNonce) != VerifyNonceSize {
		return nil, fmt.Errorf("server_nonce must be %d bytes: %d", VerifyNonceSize, len(serverNonce))
	}
	if len(vpk) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("vpk must be %d bytes: %d", ed25519.PublicKeySize, len(vpk))
	}
	depth := len(trailClientIds)
	if depth == 0 || 255 < depth {
		return nil, fmt.Errorf("trail must have 1 to 255 client ids: %d", depth)
	}
	message := make([]byte, 0, len(VerifyCtx)+2+16+VerifyNonceSize+ed25519.PublicKeySize+2+16*depth)
	message = append(message, VerifyCtx...)
	message = append(message, VerifyMsgTypeAssign)
	message = append(message, serverKeyId)
	message = append(message, trailId[:]...)
	message = append(message, serverNonce...)
	message = append(message, vpk...)
	message = append(message, m)
	message = append(message, byte(depth))
	for _, clientId := range trailClientIds {
		message = append(message, clientId[:]...)
	}
	return message, nil
}

// BuildVerifyFinalMessage returns the canonical FINAL message at depth M
// (VALIDATOR.md A.4), signed by the server:
//
//	CTX(19) ‖ 0x04 ‖ server_key_id(1) ‖ trail_id(16) ‖ server_nonce(32) ‖ vpk(32)
//	        ‖ M(1) ‖ (client_id_1(16) ‖ time_ms_1(8)) ‖ … ‖ (client_id_M(16) ‖ time_ms_M(8))
//
// Unlike EXTEND/ASSIGN there is no separate count byte: M itself is the hop
// count, so `len(hops)` must equal `m`. Times are the server-stamped unix
// milliseconds, encoded as big-endian uint64.
func BuildVerifyFinalMessage(serverKeyId byte, trailId Id, serverNonce []byte, vpk []byte, m byte, hops []VerifyProofHop) ([]byte, error) {
	if len(serverNonce) != VerifyNonceSize {
		return nil, fmt.Errorf("server_nonce must be %d bytes: %d", VerifyNonceSize, len(serverNonce))
	}
	if len(vpk) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("vpk must be %d bytes: %d", ed25519.PublicKeySize, len(vpk))
	}
	if m == 0 || len(hops) != int(m) {
		return nil, fmt.Errorf("hops must have exactly M=%d entries: %d", m, len(hops))
	}
	// each hop contributes client_id(16) + time_ms(8) + egress_ip_hash(32) = 56 bytes
	message := make([]byte, 0, len(VerifyCtx)+2+16+VerifyNonceSize+ed25519.PublicKeySize+1+56*len(hops))
	message = append(message, VerifyCtx...)
	message = append(message, VerifyMsgTypeFinal)
	message = append(message, serverKeyId)
	message = append(message, trailId[:]...)
	message = append(message, serverNonce...)
	message = append(message, vpk...)
	message = append(message, m)
	for _, hop := range hops {
		message = append(message, hop.ClientId[:]...)
		message = binary.BigEndian.AppendUint64(message, hop.TimeMs)
		message = append(message, hop.EgressIpHash[:]...) // VALIDATOR.md §8.1/§3.3 (D27)
	}
	return message, nil
}

// VerifyFinalDigest is the canonical compact artifact identity for a FINAL
// message: sha256 over the exact `BuildVerifyFinalMessage` bytes. Release-1.0
// FINAL signatures cover the raw message, like SEED/EXTEND/ASSIGN; this digest
// is retained for indexing and audit references only.
func VerifyFinalDigest(finalMessage []byte) [32]byte {
	return sha256.Sum256(finalMessage)
}

// SignVerifyMessage signs a canonical `/verify` message (the exact bytes
// from one of the BuildVerify*Message functions) with an Ed25519 private
// key. Panics if the key is not a valid Ed25519 private key, like
// `ed25519.Sign`.
func SignVerifyMessage(signingKey ed25519.PrivateKey, verifyMessage []byte) []byte {
	return ed25519.Sign(signingKey, verifyMessage)
}

// VerifyVerifyMessageSignature reports whether `signature` is a valid
// Ed25519 signature over a canonical `/verify` message under `publicKey`.
// Malformed input (wrong-size key or signature) returns false rather than
// panicking.
func VerifyVerifyMessageSignature(publicKey []byte, verifyMessage []byte, signature []byte) bool {
	if len(publicKey) != ed25519.PublicKeySize {
		return false
	}
	if len(signature) != ed25519.SignatureSize {
		return false
	}
	return ed25519.Verify(ed25519.PublicKey(publicKey), verifyMessage, signature)
}
