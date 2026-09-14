package connect

import (
	"testing"
)

// the ramp still reaches the standard size, and the opening contract stays
// well below it -- the point is a bounded opening, not no opening bound
func TestContractSizeRamp(t *testing.T) {
	settings := DefaultContractManagerSettings()

	AssertEqual(t, settings.InitialContractTransferByteCount < settings.StandardContractTransferByteCount, true)
	AssertEqual(t, settings.StandardContractTransferByteCount, mib(128))

	contractManager := &ContractManager{settings: settings}

	// sequence 0 is the opening contract
	AssertEqual(t, contractManager.contractByteCount(ContractKey{}, 0, 0), settings.InitialContractTransferByteCount)

	// the ramp is monotonic and tops out at the standard size
	previous := contractManager.contractByteCount(ContractKey{}, 0, 0)
	for contractSeqIndex := uint64(1); contractSeqIndex <= settings.ContractTransferByteSeqScale; contractSeqIndex += 1 {
		current := contractManager.contractByteCount(ContractKey{}, contractSeqIndex, 0)
		AssertEqual(t, previous < current, true)
		previous = current
	}
	AssertEqual(t, previous, settings.StandardContractTransferByteCount)

	// past the scale it stays at standard
	AssertEqual(t, contractManager.contractByteCount(ContractKey{}, settings.ContractTransferByteSeqScale+10, 0), settings.StandardContractTransferByteCount)
}

// a caller asking for more than the ramp allows still gets what it needs,
// otherwise a large single message could never be sent
func TestContractSizeHonorsMinimum(t *testing.T) {
	contractManager := &ContractManager{settings: DefaultContractManagerSettings()}

	AssertEqual(t, contractManager.contractByteCount(ContractKey{}, 0, mib(64)), mib(64))
}

// the opening contract has to cover a typical web response, or every new
// destination pays a second blocking negotiation a few packets in
func TestInitialContractCoversATypicalResponse(t *testing.T) {
	settings := DefaultContractManagerSettings()

	// ~80% of the contract is usable, so compare against the usable budget
	usable := ByteCount(float64(settings.InitialContractTransferByteCount) * 0.8)

	// a 500KiB response is unremarkable for a page subresource
	AssertEqual(t, kib(500) <= usable, true)
}
