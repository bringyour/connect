// Contract-wait cancellation owns a local queue wait, not the shared backend
// health signal. Fresh-process controls make predecessor ordering deterministic.
package connect

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"
)

const contractWaitBackendOrderEnv = "URNETWORK_TEST_CONTRACT_WAIT_BACKEND_ORDER"

// A closed predecessor may legitimately leave a recent host-wide outage. The
// next sequence still reaches its local wait without issuing a gated request.
func TestSendSequenceContractWaitCancellationAfterDegradedPredecessor(t *testing.T) {
	runContractWaitBackendOrder(t, "degraded-predecessor")
}

// Recovery after wait admission must not turn local cancellation into an
// acquisition error or require a second request before cancellation can finish.
func TestSendSequenceContractWaitCancellationAfterBackendRecoveryAtWait(t *testing.T) {
	runContractWaitBackendOrder(t, "recovery-at-wait")
}

// A later backend failure cannot change the already-owned local cancellation
// boundary. This also covers the healthy request path before degradation.
func TestSendSequenceContractWaitCancellationAfterBackendFailureAtWait(t *testing.T) {
	runContractWaitBackendOrder(t, "failure-at-wait")
}

// Re-executes only this exact root, so unrelated test clients cannot mutate the
// process-wide signal. The child uses the same binary, real Client and wait
// helper; the process owner joins it on every exit, including cancellation.
func runContractWaitBackendOrder(t *testing.T, order string) {
	t.Helper()
	if selected := os.Getenv(contractWaitBackendOrderEnv); selected != "" {
		if selected != order {
			t.Fatalf("unexpected contract wait backend order %q", selected)
		}
		testContractWaitBackendOrder(t, order)
		return
	}

	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	command := exec.CommandContext(
		ctx,
		executable,
		"-test.run=^"+t.Name()+"$",
		"-test.count=1",
		"-test.timeout=30s",
	)
	command.Env = append(os.Environ(), contractWaitBackendOrderEnv+"="+order)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("isolated contract wait order %s: %v\n%s", order, err, output)
	}
	if err := ctx.Err(); err != nil {
		t.Fatalf("isolated contract wait order %s outlived its owner: %v", order, err)
	}
}

// Publishes through the real ContractManager callback and joins its synchronous
// fixture before returning. No raw atomic reset can race a surviving publisher.
func publishContractWaitBackendResult(t *testing.T, success bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var oob OutOfBandControl = NewNoContractClientOob()
	requestCount := backendDegradedFailThreshold
	if success {
		oob = alwaysSuccessOob{}
		requestCount = 1
	}
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	settings.ControlPingTimeout = 0
	settings.Log = NewNoopLogger()
	client := NewClient(ctx, NewId(), oob, settings)
	defer func() {
		cancel()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if err := client.CloseAndWait(cleanupCtx); err != nil {
			t.Errorf("join backend predecessor fixture: %v", err)
		}
	}()
	key := ContractKey{Destination: DestinationId(NewId())}
	for i := 0; i < requestCount; i++ {
		client.ContractManager().CreateContract(key, uint64(i), ByteCount(1024))
	}
	if degraded := isBackendDegraded(); degraded == success {
		t.Fatalf("backend callback success=%t left degraded=%t", success, degraded)
	}
}

// Each observation is made while the exact queue-take owner is held. There is
// no polling, elapsed-time outcome or global reset in the ordering proof.
func testContractWaitBackendOrder(t *testing.T, order string) {
	t.Helper()
	if isBackendDegraded() {
		t.Fatal("fresh contract wait child unexpectedly inherited backend health")
	}
	if order != "failure-at-wait" {
		publishContractWaitBackendResult(t, false)
		if !isBackendDegraded() {
			t.Fatal("joined predecessor did not retain its recent backend outage")
		}
	}
	testSendSequenceContractWaitCancellation(t, func(oob *contractErrorOob) {
		switch order {
		case "degraded-predecessor", "recovery-at-wait":
			if !isBackendDegraded() || oob.errorsSent.Load() != 0 {
				t.Fatalf("degraded queue wait issued requests: degraded=%t count=%d",
					isBackendDegraded(), oob.errorsSent.Load())
			}
			select {
			case <-oob.requestSeen:
				t.Fatal("degraded queue wait passed through the outbound request barrier")
			default:
			}
			if order == "recovery-at-wait" {
				publishContractWaitBackendResult(t, true)
			}
		case "failure-at-wait":
			if isBackendDegraded() || oob.errorsSent.Load() != 1 {
				t.Fatalf("healthy queue wait did not own its first request: degraded=%t count=%d",
					isBackendDegraded(), oob.errorsSent.Load())
			}
			select {
			case <-oob.requestSeen:
			default:
				t.Fatal("healthy queue wait omitted its outbound request")
			}
			publishContractWaitBackendResult(t, false)
		default:
			t.Fatalf("unknown contract wait backend order %q", order)
		}
	})
}
