package connect

import (
	"testing"
	"time"
)

func TestWakeupTime(t *testing.T) {
	wakeupEpoch := 1 * time.Second
	cases := []struct {
		input    time.Duration
		expected time.Duration
	}{
		{
			input:    0,
			expected: 0,
		},
		{
			input:    1,
			expected: wakeupEpoch,
		},
		{
			input:    wakeupEpoch - 1,
			expected: wakeupEpoch,
		},
		{
			input:    wakeupEpoch,
			expected: wakeupEpoch,
		},
		{
			input:    wakeupEpoch + 1,
			expected: 2 * wakeupEpoch,
		},
	}

	for _, c := range cases {
		inputTime := time.Unix(0, int64(c.input))
		actual := time.Duration(WakeupTime(inputTime, wakeupEpoch).UnixNano())
		AssertEqual(t, c.expected, actual)
	}
}

func TestResetWakeupTimerDoesNotAllocate(t *testing.T) {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	resetWakeupTimer(timer, time.Hour, time.Second)

	allocs := testing.AllocsPerRun(1000, func() {
		resetWakeupTimer(timer, time.Hour, time.Second)
	})

	AssertEqual(t, 0.0, allocs)
}
