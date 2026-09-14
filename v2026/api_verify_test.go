package connect

// JSON-shape tests for the `/verify/keys` and `/sn` control-plane API
// bindings: exact wire-name pins for the small types and marshal/unmarshal
// round-trips with fixed values for all of them.

import (
	"encoding/json"
	"testing"
)

// TestVerifyKeysResultJson pins the `GET /verify/keys` result shape:
// `server_key_id` as a number, `public_key` as base64.
func TestVerifyKeysResultJson(t *testing.T) {
	keysResult := &VerifyKeysResult{
		Keys: []*VerifyServerKey{
			{
				ServerKeyId: 7,
				PublicKey:   testVerifyHexBytes(t, testVerifyVpkHex),
			},
		},
	}
	keysResultJson, err := json.Marshal(keysResult)
	AssertEqual(t, nil, err)
	expectedJson := `{"keys":[{"server_key_id":7,"public_key":"AQIDBAUGBwgJCgsMDQ4PEBESExQVFhcYGRobHB0eHyA="}]}`
	AssertEqual(t, expectedJson, string(keysResultJson))

	var parsedKeysResult VerifyKeysResult
	err = json.Unmarshal(keysResultJson, &parsedKeysResult)
	AssertEqual(t, nil, err)
	AssertEqual(t, 1, len(parsedKeysResult.Keys))
	AssertEqual(t, *keysResult.Keys[0], *parsedKeysResult.Keys[0])
}

// TestSnSetWalletJson pins the `POST /sn/wallet` args/result shapes,
// including error omission when unset.
func TestSnSetWalletJson(t *testing.T) {
	clientId, err := ParseId("00000000-0000-0000-0000-000000000042")
	AssertEqual(t, nil, err)
	setWalletArgs := &SnSetWalletArgs{
		ColdkeySs58: "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
		ClientId:    &clientId,
	}
	setWalletArgsJson, err := json.Marshal(setWalletArgs)
	AssertEqual(t, nil, err)
	AssertEqual(
		t,
		`{"coldkey_ss58":"5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY","client_id":"00000000-0000-0000-0000-000000000042"}`,
		string(setWalletArgsJson),
	)
	var parsedSetWalletArgs SnSetWalletArgs
	err = json.Unmarshal(setWalletArgsJson, &parsedSetWalletArgs)
	AssertEqual(t, nil, err)
	AssertEqual(t, *setWalletArgs, parsedSetWalletArgs)

	// empty result omits the error
	emptyResultJson, err := json.Marshal(&SnSetWalletResult{})
	AssertEqual(t, nil, err)
	AssertEqual(t, `{}`, string(emptyResultJson))

	errorResultJson, err := json.Marshal(&SnSetWalletResult{
		Error: &SnSetWalletError{
			Message: "invalid ss58",
		},
	})
	AssertEqual(t, nil, err)
	AssertEqual(t, `{"error":{"message":"invalid ss58"}}`, string(errorResultJson))

	var parsedErrorResult SnSetWalletResult
	err = json.Unmarshal(errorResultJson, &parsedErrorResult)
	AssertEqual(t, nil, err)
	AssertEqual(t, "invalid ss58", parsedErrorResult.Error.Message)
}

// TestSnPoolClaimJson round-trips the `GET /sn/pool/claim` args and result
// with fixed values and pins the args wire name.
func TestSnPoolClaimJson(t *testing.T) {
	poolClaimArgs := &SnPoolClaimArgs{
		Epoch: 42,
	}
	poolClaimArgsJson, err := json.Marshal(poolClaimArgs)
	AssertEqual(t, nil, err)
	AssertEqual(t, `{"epoch":42}`, string(poolClaimArgsJson))

	poolClaimResult := &SnPoolClaimResult{
		Epoch:    42,
		NoId:     testVerifyHexBytes(t, "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"),
		Coldkey:  testVerifyHexBytes(t, "202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"),
		ShareBps: 1250,
		Proof: [][]byte{
			testVerifyHexBytes(t, testVerifyClientNonceHex),
			testVerifyHexBytes(t, testVerifyServerNonceHex),
		},
		PayoutRoot:             testVerifyHexBytes(t, testVerifyVpkHex),
		ContractAddress:        "0x00000000000000000000000000000000000009c4",
		ChainId:                964,
		ClaimOpenBlock:         123456,
		ArtifactHash:           "sha256:0123",
		ArtifactUri:            "https://no.example/sn/artifacts/sha256:0123",
		SettlementVaultAddress: "0x0000000000000000000000000000000000000abc",
		Error: &SnPoolClaimError{
			Message: "claim not open",
		},
	}
	poolClaimResultJson, err := json.Marshal(poolClaimResult)
	AssertEqual(t, nil, err)
	var parsedPoolClaimResult SnPoolClaimResult
	err = json.Unmarshal(poolClaimResultJson, &parsedPoolClaimResult)
	AssertEqual(t, nil, err)
	AssertEqual(t, *poolClaimResult, parsedPoolClaimResult)
}

// TestSnEpochResultJson pins the `GET /sn/epoch` result wire names and
// round-trips fixed values.
func TestSnEpochResultJson(t *testing.T) {
	epochResult := &SnEpochResult{
		Epoch:               42,
		StartBlock:          1000000,
		CommitDeadlineBlock: 1001200,
		TrailsDeadlineBlock: 1007200,
		FinalizeBlock:       1014400,
		TEpochBlocks:        14400,
		ChainId:             964,
		ContractAddress:     "0x00000000000000000000000000000000000009c4",
	}
	epochResultJson, err := json.Marshal(epochResult)
	AssertEqual(t, nil, err)
	expectedJson := `{"epoch":42,` +
		`"start_block":1000000,` +
		`"commit_deadline_block":1001200,` +
		`"trails_deadline_block":1007200,` +
		`"finalize_block":1014400,` +
		`"t_epoch_blocks":14400,` +
		`"chain_id":964,` +
		`"contract_address":"0x00000000000000000000000000000000000009c4"}`
	AssertEqual(t, expectedJson, string(epochResultJson))

	var parsedEpochResult SnEpochResult
	err = json.Unmarshal(epochResultJson, &parsedEpochResult)
	AssertEqual(t, nil, err)
	AssertEqual(t, *epochResult, parsedEpochResult)
}
