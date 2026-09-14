package connect

import (
	"time"
)

// all regular protocol ping messages should align to this schedule
// this ensures the radio wakes up for as little as possible

// FIXME when the server ping matches the client ping, we can increase this to 10s
// const WakeupEpoch = 1 * time.Second

func WakeupAfter(d time.Duration, epoch time.Duration) <-chan time.Time {
	now := time.Now()
	t := WakeupTime(now.Add(d), epoch)
	return time.After(t.Sub(now))
}

// resetWakeupTimer resets a reusable timer onto the regular wakeup schedule.
// It is for hot selects where another arm often wins; constructing WakeupAfter
// there would abandon one newly allocated timer per message.
func resetWakeupTimer(timer *time.Timer, d time.Duration, epoch time.Duration) {
	now := time.Now()
	timer.Reset(WakeupTime(now.Add(d), epoch).Sub(now))
}

func WakeupTime(t time.Time, epoch time.Duration) time.Time {
	r := (time.Duration(t.UnixNano()) * time.Nanosecond) % epoch
	if r == 0 {
		return t
	}
	return t.Add(epoch - r)
}
