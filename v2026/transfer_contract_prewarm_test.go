package connect

import (
	"testing"
)

// the opening contract is the one that blocks. Contracts after it are queued
// the moment their predecessor is taken, so only the first has nothing ahead of
// it -- and every new destination starts a new sequence, so web browsing pays
// that cost constantly.
func TestPrewarmOpeningContractDefaultsOn(t *testing.T) {
	AssertEqual(t, DefaultSendBufferSettings().PrewarmOpeningContract, true)
}

// off has to restore the previous on-demand behavior exactly, so the developer
// menu can A/B whether pre-warming is what improved a measurement
func TestPrewarmOpeningContractCanBeDisabled(t *testing.T) {
	sendBufferSettings := DefaultSendBufferSettings()
	sendBufferSettings.PrewarmOpeningContract = false

	AssertEqual(t, sendBufferSettings.PrewarmOpeningContract, false)
}

// the pre-warm asks for a floor, not a size: the contract manager's own ramp
// decides how large the opening contract actually is, and that ramp starts at
// InitialContractTransferByteCount
func TestPrewarmDoesNotOverrideTheContractRamp(t *testing.T) {
	contractManagerSettings := DefaultContractManagerSettings()
	contractManager := &ContractManager{settings: contractManagerSettings}

	sendBufferSettings := DefaultSendBufferSettings()
	// the floor the pre-warm passes
	minByteCount := ByteCount(float32(sendBufferSettings.MinMessageByteCount) / sendBufferSettings.ContractFillFraction)

	// a floor that small must not shrink the opening contract below the ramp
	AssertEqual(t,
		contractManager.contractByteCount(ContractKey{}, 0, minByteCount),
		contractManagerSettings.InitialContractTransferByteCount,
	)
}
