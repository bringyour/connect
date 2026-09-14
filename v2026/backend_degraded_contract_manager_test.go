package connect

import (
	"context"
	"testing"

	"github.com/urnetwork/connect/v2026/protocol"
)

// These exercise the actual call site in ContractManager.CreateContract (see
// transfer_contract_manager.go) rather than the isolated gate expression
// covered by backend_degraded_gate_test.go: a real OOB round-trip must drive
// noteBackendFailure/noteBackendSuccess, not just a value that happens to be
// wired up in a test double for some other assertion.
//
// The degradation state is package-level, so these must not run in parallel.

// alwaysSuccessOob is a minimal OutOfBandControl that acks synchronously with
// no error and no result frames, mirroring how testOobControl in
// transfer_control_oob_test.go consumes and releases the input frames.
type alwaysSuccessOob struct{}

func (alwaysSuccessOob) SendControl(frames []*protocol.Frame, callback OobResultFunction) {
	for _, frame := range frames {
		MessagePoolReturn(frame.MessageBytes)
	}
	callback(nil, nil)
}

func newBackendDegradedContractManagerTestClient(t *testing.T, oob OutOfBandControl) *Client {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	client := NewClient(ctx, NewId(), oob, DefaultClientSettings())
	t.Cleanup(func() {
		client.Cancel()
		cancel()
	})
	return client
}

// NoContractClientOob.SendControl fails synchronously ("Not supported."), so
// CreateContract's failure branch — including noteBackendFailure — runs
// inline, before CreateContract returns. That makes the threshold crossing
// deterministic without needing to wait on a goroutine.
func TestContractManagerCreateContract_OobFailureAccumulatesTowardDegraded(t *testing.T) {
	resetBackendDegraded()
	defer resetBackendDegraded()

	client := newBackendDegradedContractManagerTestClient(t, NewNoContractClientOob())
	contractManager := client.ContractManager()
	contractKey := ContractKey{Destination: DestinationId(NewId())}

	if isBackendDegraded() {
		t.Fatal("precondition: fresh client must not start degraded")
	}

	for i := 0; i < backendDegradedFailThreshold-1; i++ {
		contractManager.CreateContract(contractKey, uint64(i), ByteCount(1024))
	}
	if isBackendDegraded() {
		t.Fatalf("degraded before %d OOB failures were recorded", backendDegradedFailThreshold)
	}

	contractManager.CreateContract(contractKey, uint64(backendDegradedFailThreshold), ByteCount(1024))
	if !isBackendDegraded() {
		t.Fatalf("not degraded after %d consecutive OOB failures from CreateContract", backendDegradedFailThreshold)
	}
}

// A successful CreateContract round-trip must clear degradation immediately,
// the same as any other backend success — this is the recovery half of the
// same call site.
func TestContractManagerCreateContract_OobSuccessClearsDegraded(t *testing.T) {
	resetBackendDegraded()
	defer resetBackendDegraded()

	for i := 0; i < backendDegradedFailThreshold; i++ {
		noteBackendFailure()
	}
	if !isBackendDegraded() {
		t.Fatal("precondition: should be degraded before the successful round-trip")
	}

	client := newBackendDegradedContractManagerTestClient(t, alwaysSuccessOob{})
	contractManager := client.ContractManager()
	contractKey := ContractKey{Destination: DestinationId(NewId())}

	contractManager.CreateContract(contractKey, 0, ByteCount(1024))

	if isBackendDegraded() {
		t.Fatal("a successful CreateContract OOB round-trip must clear the degraded state")
	}
	if got := consecutiveBackendFails.Load(); got != 0 {
		t.Fatalf("consecutive failures = %d after a successful CreateContract, want 0", got)
	}
}

// Regression guard on the client-shutdown carve-out immediately above the
// failure branch: CreateContract does not call noteBackendFailure when the
// error is due to the client already being done (see the `<-self.client.Done()`
// case), since that is a local shutdown, not a backend signal.
func TestContractManagerCreateContract_ClientDoneDoesNotNoteFailure(t *testing.T) {
	resetBackendDegraded()
	defer resetBackendDegraded()

	ctx, cancel := context.WithCancel(context.Background())
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	contractKey := ContractKey{Destination: DestinationId(NewId())}

	// close the client's lifecycle before the OOB failure is observed
	client.Cancel()
	cancel()
	<-client.Done()

	contractManager := client.ContractManager()
	contractManager.CreateContract(contractKey, 0, ByteCount(1024))

	if got := consecutiveBackendFails.Load(); got != 0 {
		t.Fatalf("consecutive failures = %d after a post-shutdown CreateContract error, want 0 (shutdown is not a backend signal)", got)
	}
	if isBackendDegraded() {
		t.Fatal("a post-shutdown CreateContract error must not read as backend degradation")
	}
}
