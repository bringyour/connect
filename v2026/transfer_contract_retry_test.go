package connect

import (
	"testing"
	"time"
)

func TestCreateContractRetryBackoff(t *testing.T) {
	interval := 1 * time.Second
	maximum := 5 * time.Second
	want := []time.Duration{
		2 * time.Second,
		4 * time.Second,
		5 * time.Second,
		5 * time.Second,
	}
	for i, expected := range want {
		interval = nextCreateContractRetryInterval(interval, maximum)
		if interval != expected {
			t.Fatalf("step %d interval = %v, want %v", i, interval, expected)
		}
	}

	// A zero max is interpreted by the caller as the initial interval, which
	// preserves constant retries for old manually-constructed settings.
	if got := nextCreateContractRetryInterval(3*time.Second, 3*time.Second); got != 3*time.Second {
		t.Fatalf("constant compatibility interval = %v", got)
	}
}
