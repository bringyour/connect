package connect

// api_verify.go — control-plane API bindings for the `/verify` and `/sn`
// (subnet) routes (sn/VALIDATOR.md §3.5, sn/PLAN.md §5).
//
// These follow the standard `BringYourApi` idiom: an Args struct, a Result
// struct, a Callback alias, an async method, and a blocking Sync method.
// Note the trail steps themselves (`POST /verify`, SEED/EXTEND) are
// deliberately NOT bound here — those requests must egress through a
// specific provider tunnel, not the api client. Their wire types live in
// verify_wire.go.

import (
	"fmt"
)

type VerifyKeysCallback ApiCallback[*VerifyKeysResult]

// VerifyServerKey is one published server Ed25519 signing key
// (VALIDATOR.md §3.5). `ServerKeyId` is the 1-byte rotation id carried in
// every ASSIGN/FINAL message; `PublicKey` is the raw 32-byte Ed25519 public
// key (base64 in JSON).
type VerifyServerKey struct {
	ServerKeyId byte   `json:"server_key_id"`
	PublicKey   []byte `json:"public_key"`
}

// VerifyKeysResult lists all historical server verify keys so proofs remain
// verifiable across key rotations.
type VerifyKeysResult struct {
	Keys []*VerifyServerKey `json:"keys"`
}

// VerifyKeys fetches the published server Ed25519 keys from the
// unauthenticated `GET /verify/keys` route. Third parties use these to
// verify `assign_sig`/`final_sig` on trails and published proofs.
//
// The route is unauthenticated by design; no `byJwt` is sent.
func (self *BringYourApi) VerifyKeys(callback VerifyKeysCallback) {
	go HandleError(func() {
		HttpGetWithStrategy(
			self.ctx,
			self.clientStrategy,
			fmt.Sprintf("%s/verify/keys", self.apiUrl),
			"",
			&VerifyKeysResult{},
			callback,
		)
	})
}

func (self *BringYourApi) VerifyKeysSync() (*VerifyKeysResult, error) {
	return HttpGetWithStrategy(
		self.ctx,
		self.clientStrategy,
		fmt.Sprintf("%s/verify/keys", self.apiUrl),
		"",
		&VerifyKeysResult{},
		NewNoopApiCallback[*VerifyKeysResult](),
	)
}

type SnSetWalletCallback ApiCallback[*SnSetWalletResult]

// SnSetWalletArgs sets the caller network's subnet coldkey — the ss58
// address pool payouts for this network are claimable by. Kept separate from
// the payout wallet types on purpose.
type SnSetWalletArgs struct {
	ColdkeySs58 string `json:"coldkey_ss58"`
	ClientId    *Id    `json:"client_id,omitempty"`
}

type SnSetWalletError struct {
	Message string `json:"message"`
}

type SnSetWalletResult struct {
	Error *SnSetWalletError `json:"error,omitempty"`
}

// SnSetWallet sets the subnet wallet via the authenticated
// `POST /sn/wallet` route.
func (self *BringYourApi) SnSetWallet(snSetWallet *SnSetWalletArgs, callback SnSetWalletCallback) {
	go HandleError(func() {
		HttpPostWithStrategy(
			self.ctx,
			self.clientStrategy,
			fmt.Sprintf("%s/sn/wallet", self.apiUrl),
			snSetWallet,
			self.ByJwt(),
			&SnSetWalletResult{},
			callback,
		)
	})
}

func (self *BringYourApi) SnSetWalletSync(snSetWallet *SnSetWalletArgs) (*SnSetWalletResult, error) {
	return HttpPostWithStrategy(
		self.ctx,
		self.clientStrategy,
		fmt.Sprintf("%s/sn/wallet", self.apiUrl),
		snSetWallet,
		self.ByJwt(),
		&SnSetWalletResult{},
		NewNoopApiCallback[*SnSetWalletResult](),
	)
}

type SnPoolClaimCallback ApiCallback[*SnPoolClaimResult]

// SnPoolClaimArgs selects the epoch to fetch the caller's pool claim for.
// Sent as the `epoch` query parameter of `GET /sn/pool/claim`.
type SnPoolClaimArgs struct {
	Epoch uint64 `json:"epoch"`
}

// SnPoolClaimResult is the caller's merkle pool-payout claim for an epoch
// (sn/PLAN.md §5): everything needed to call `claim` on the subnet contract.
// `NoId`, `Coldkey`, `PayoutRoot`, and each `Proof` element are raw 32-byte
// values (base64 in JSON); `ContractAddress` is the 0x-hex EVM address.
type SnPoolClaimResult struct {
	Epoch                  uint64            `json:"epoch"`
	NoId                   []byte            `json:"no_id"`
	Coldkey                []byte            `json:"coldkey"`
	ShareBps               int               `json:"share_bps"`
	Proof                  [][]byte          `json:"proof"`
	PayoutRoot             []byte            `json:"payout_root"`
	ContractAddress        string            `json:"contract_address"`
	ChainId                uint64            `json:"chain_id"`
	ClaimOpenBlock         uint64            `json:"claim_open_block"`
	ArtifactHash           string            `json:"artifact_hash,omitempty"`
	ArtifactUri            string            `json:"artifact_uri,omitempty"`
	SettlementVaultAddress string            `json:"settlement_vault_address,omitempty"`
	Error                  *SnPoolClaimError `json:"error,omitempty"`
}

type SnPoolClaimError struct {
	Message string `json:"message"`
}

// SnPoolClaim fetches the caller network's pool claim for an epoch via the
// authenticated `GET /sn/pool/claim?epoch=N` route.
func (self *BringYourApi) SnPoolClaim(snPoolClaim *SnPoolClaimArgs, callback SnPoolClaimCallback) {
	go HandleError(func() {
		HttpGetWithStrategy(
			self.ctx,
			self.clientStrategy,
			fmt.Sprintf("%s/sn/pool/claim?epoch=%d", self.apiUrl, snPoolClaim.Epoch),
			self.ByJwt(),
			&SnPoolClaimResult{},
			callback,
		)
	})
}

func (self *BringYourApi) SnPoolClaimSync(snPoolClaim *SnPoolClaimArgs) (*SnPoolClaimResult, error) {
	return HttpGetWithStrategy(
		self.ctx,
		self.clientStrategy,
		fmt.Sprintf("%s/sn/pool/claim?epoch=%d", self.apiUrl, snPoolClaim.Epoch),
		self.ByJwt(),
		&SnPoolClaimResult{},
		NewNoopApiCallback[*SnPoolClaimResult](),
	)
}

type SnEpochCallback ApiCallback[*SnEpochResult]

// SnEpochResult is the current subnet epoch state mirrored from chain
// (sn/PLAN.md §5), so clients do not need their own RPC. Blocks are subnet
// contract-chain block numbers — the contract clock is authoritative.
type SnEpochResult struct {
	Epoch               uint64 `json:"epoch"`
	StartBlock          uint64 `json:"start_block"`
	CommitDeadlineBlock uint64 `json:"commit_deadline_block"`
	TrailsDeadlineBlock uint64 `json:"trails_deadline_block"`
	FinalizeBlock       uint64 `json:"finalize_block"`
	TEpochBlocks        uint64 `json:"t_epoch_blocks"`
	ChainId             uint64 `json:"chain_id"`
	ContractAddress     string `json:"contract_address"`
}

// SnEpoch fetches the current epoch state via `GET /sn/epoch`. The route
// does not require auth; the JWT is attached when set.
func (self *BringYourApi) SnEpoch(callback SnEpochCallback) {
	go HandleError(func() {
		HttpGetWithStrategy(
			self.ctx,
			self.clientStrategy,
			fmt.Sprintf("%s/sn/epoch", self.apiUrl),
			self.ByJwt(),
			&SnEpochResult{},
			callback,
		)
	})
}

func (self *BringYourApi) SnEpochSync() (*SnEpochResult, error) {
	return HttpGetWithStrategy(
		self.ctx,
		self.clientStrategy,
		fmt.Sprintf("%s/sn/epoch", self.apiUrl),
		self.ByJwt(),
		&SnEpochResult{},
		NewNoopApiCallback[*SnEpochResult](),
	)
}
