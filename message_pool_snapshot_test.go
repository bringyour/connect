// The public counters describe every root in a process. Unit assertions that
// attribute a delta to one owner must exclude unrelated asynchronous returns.
package connect

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"
)

const messagePoolSnapshotRootEnv = "URNETWORK_TEST_MESSAGE_POOL_SNAPSHOT_ROOT"
const messagePoolSnapshotForeignEnv = "URNETWORK_TEST_MESSAGE_POOL_SNAPSHOT_FOREIGN"
const messagePoolPacketSnapshotTestRoot = "TestMessagePoolPacketOutstandingCountTracksRootOwnershipWithoutAllocating"

// Use the existing same-binary, exact-root re-execution pattern. The parent
// waits for termination on every exit and preserves the child's verbose result.
func messagePoolSnapshotInFreshProcess(t *testing.T) bool {
	t.Helper()
	if root := os.Getenv(messagePoolSnapshotRootEnv); root != "" {
		if root != t.Name() {
			t.Fatalf("pool snapshot child selected %q, running %q", root, t.Name())
		}
		return false
	}
	output, err := runMessagePoolSnapshotChild(t, t.Name(), "", true)
	if err != nil {
		t.Fatalf("exclusive pool snapshot %s: %v\n%s", t.Name(), err, output)
	}
	t.Logf("exclusive pool snapshot child:\n%s", output)
	return true
}

// No process-global pool, counter or configuration is replaced. Both the
// ownership test and its controlled failure execute the real public API.
// Direct children execute the assertion; the positive control uses its dispatch.
func runMessagePoolSnapshotChild(t *testing.T, root, foreign string, direct bool) ([]byte, error) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	bodyTimeout := 30 * time.Second
	if root == "TestMessagePool" {
		bodyTimeout = 5 * time.Minute
	}
	if deadline, ok := t.Deadline(); ok {
		bodyTimeout = min(bodyTimeout, time.Until(deadline)-10*time.Second)
	}
	if bodyTimeout <= 0 {
		return nil, errors.New("pool snapshot has no remaining child and join budget")
	}
	ctx, cancel := context.WithTimeout(t.Context(), bodyTimeout+5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable,
		"-test.run=^"+regexp.QuoteMeta(root)+"$",
		"-test.v",
		"-test.count=1",
		"-test.timeout="+bodyTimeout.String(),
	)
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, messagePoolSnapshotRootEnv+"=") && !strings.HasPrefix(entry, messagePoolSnapshotForeignEnv+"=") {
			command.Env = append(command.Env, entry)
		}
	}
	selected := ""
	if direct {
		selected = root
	}
	command.Env = append(command.Env, messagePoolSnapshotRootEnv+"="+selected, messagePoolSnapshotForeignEnv+"="+foreign)
	output, err := command.CombinedOutput()
	return output, errors.Join(err, ctx.Err())
}

// Both signs of foreign activity falsify a process-wide delta even though
// Get/Return accounting is correct. The same unchanged assertions pass when
// the test owns its process and therefore every measured packet root.
func TestMessagePoolSnapshotRequiresExclusiveProcessOwnership(t *testing.T) {
	for _, control := range []struct {
		mode    string
		literal string
	}{
		{mode: "return", literal: "packet outstanding after take = 1, want 5"},
		{mode: "take", literal: "packet outstanding after take = 5, want 1"},
	} {
		output, err := runMessagePoolSnapshotChild(t, messagePoolPacketSnapshotTestRoot, control.mode, true)
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 1 || !strings.Contains(string(output), control.literal) || !strings.Contains(string(output), "--- FAIL: "+messagePoolPacketSnapshotTestRoot) {
			t.Fatalf("foreign %s did not reproduce the exact global-delta failure: %v\n%s", control.mode, err, output)
		}
		t.Logf("expected foreign %s failure after joined transition:\n%s", control.mode, output)
		output, err = runMessagePoolSnapshotChild(t, messagePoolPacketSnapshotTestRoot, control.mode, false)
		if err != nil || !strings.Contains(string(output), "--- PASS: "+messagePoolPacketSnapshotTestRoot) || !strings.Contains(string(output), "exclusive pool snapshot child:") {
			t.Fatalf("exclusive dispatch with foreign %s did not preserve the real accounting assertions: %v\n%s", control.mode, err, output)
		}
		t.Logf("exclusive dispatch with joined foreign %s owner:\n%s", control.mode, output)
	}
}
