// Pins the local suite's output-filter status and binary-text contracts.
package connect

import (
	"os"
	"strings"
	"testing"
)

// Every live test pipeline snapshots both zsh statuses before selecting the
// downstream failure ahead of a secondary upstream SIGPIPE.
func TestTestScriptPreservesPipelineFailures(t *testing.T) {
	contentBytes, err := os.ReadFile("test.sh")
	if err != nil {
		t.Fatal(err)
	}
	content := string(contentBytes)

	// The contract is that EVERY pipeline captures both statuses, so the
	// counts are pinned against each other rather than against a literal: a
	// new quarantine stage adds one of each and must keep them equal. A
	// literal would have to be edited for every stage added, and editing it
	// is indistinguishable from silencing the check.
	pipelineCount := strings.Count(content, "| grep --binary-files=text --line-buffered")
	if pipelineCount < minTestScriptPipelines {
		t.Fatalf("test.sh has %d filtered pipelines; want at least %d", pipelineCount, minTestScriptPipelines)
	}
	for _, signature := range []string{
		`pipeline_status=("${pipestatus[@]}")`,
		`test_pipeline_status "${pipeline_status[1]}" "${pipeline_status[2]}" || exit $?`,
	} {
		if count := strings.Count(content, signature); count != pipelineCount {
			t.Errorf("%q count = %d; want %d, one per filtered pipeline", signature, count, pipelineCount)
		}
	}
}

// minTestScriptPipelines is the floor the suite has always had: the main run
// plus the timing-sensitive groups that run in their own processes. It exists
// so deleting a stage outright still fails the test.
const minTestScriptPipelines = 4
