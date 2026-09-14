package connect

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// TestMultiClientMonitorPairingUnableToConnect reproduces the "connect grid dots
// grow unbounded while unable to connect" report. It runs the REAL multi client
// against a synthetic generator that always has fresh candidate providers whose
// transports never deliver anything (pings never ack), i.e. a device that cannot
// connect, at compressed timescales. A mirror of the sdk ConnectGrid bookkeeping
// subscribes to the monitor: every non-terminal provider event must eventually be
// followed by a terminal event for the same client id (which is what lets the
// grid arm its RemoveTimeout and reclaim the dot). The mirror reaps terminal
// points after RemoveTimeout exactly like ConnectGrid.run. If the live point set
// grows without bound (stuck InEvaluation/Added points that never resolve), the
// event stream is the leak; if it stays bounded here, the leak is above (rpc/ui).
func TestMultiClientMonitorPairingUnableToConnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	generator := &leakTestGenerator{}

	settings := DefaultMultiClientSettings()
	// compress the timescales so ~1 simulated hour of rounds runs in seconds
	settings.PingWriteTimeout = 200 * time.Millisecond
	settings.PingTimeout = 500 * time.Millisecond
	settings.WindowResizeTimeout = 300 * time.Millisecond
	settings.WindowExpandTimeout = 400 * time.Millisecond
	settings.WindowEnumerateErrorTimeout = 100 * time.Millisecond
	settings.WindowExpandArgsTimeout = 2 * time.Second
	settings.StatsWindowMaxUnhealthyDuration = 1 * time.Second
	settings.StatsWindowWarnUnhealthyDuration = 500 * time.Millisecond
	settings.StatsWindowGraceperiod = 1 * time.Second

	multi := NewRemoteUserNatMultiClient(
		ctx,
		generator,
		func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {},
		protocol.ProvideMode_Public,
		settings,
	)
	defer multi.Close()

	// mirror of sdk ConnectGrid bookkeeping
	const removeTimeout = 800 * time.Millisecond
	mirror := newGridMirror(removeTimeout)

	monitor := multi.Monitor()
	unsub := monitor.AddMonitorEventCallback(mirror.monitorEventCallback)
	defer unsub()
	// mimic ConnectGrid.listenToWindow: initialize with the current values
	windowExpandEvent, providerEvents := monitor.Events()
	mirror.monitorEventCallback(windowExpandEvent, providerEvents, true)

	// run "unable to connect" for a while, sampling the mirror
	testDuration := 30 * time.Second
	sampleInterval := 2 * time.Second
	endTime := time.Now().Add(testDuration)
	samples := []int{}
	for time.Now().Before(endTime) {
		select {
		case <-ctx.Done():
			t.Fatal("multi client died")
		case <-time.After(sampleInterval):
		}
		live, stuck, maxLive := mirror.sample(settings.PingTimeout + settings.WindowExpandTimeout + removeTimeout + 2*time.Second)
		samples = append(samples, live)
		fmt.Printf("[leaktest] live=%d stuck=%d maxLive=%d totalSeen=%d\n", live, stuck, maxLive, mirror.totalSeen())
	}

	live, stuck, maxLive := mirror.sample(settings.PingTimeout + settings.WindowExpandTimeout + removeTimeout + 2*time.Second)
	fmt.Printf("[leaktest] final live=%d stuck=%d maxLive=%d totalSeen=%d\n", live, stuck, maxLive, mirror.totalSeen())

	// a healthy pairing keeps the live set within a small multiple of the
	// combined hard max; give slack for in-flight evaluations across both windows.
	//
	// DERIVED from the settings under test rather than copied out of them. This
	// was the literal `3 * (12 + 4)`, the quality and speed WindowSizeHardMax
	// values at the time it was written, which silently becomes wrong the moment
	// either default moves.
	bound := 3 * (settings.WindowSizes[WindowTypeQuality].WindowSizeHardMax +
		settings.WindowSizes[WindowTypeSpeed].WindowSizeHardMax)
	if maxLive > bound {
		t.Errorf("live point set grew beyond bound: maxLive=%d > %d", maxLive, bound)
	}
	// stuck = non-terminal points older than every timeout in the pipeline; a
	// correct stream has none
	if stuck > 0 {
		t.Errorf("found %d stuck non-terminal points (InEvaluation/Added with no terminal event)", stuck)
	}
	// growth check: the last sample should not be materially larger than the
	// steady state reached early on
	if len(samples) >= 4 {
		early := max(samples[1], samples[2])
		last := samples[len(samples)-1]
		if last > 2*max(early, 8) {
			t.Errorf("live point set is growing: early=%d last=%d samples=%v", early, last, samples)
		}
	}
}

// gridMirror replicates the sdk ConnectGrid provider point bookkeeping:
// non-terminal events keep a point alive; terminal events arm an end time
// RemoveTimeout in the future; points past their end time are reaped.
type gridMirror struct {
	removeTimeout time.Duration

	mu      sync.Mutex
	points  map[Id]*mirrorPoint
	seen    map[Id]bool
	maxLive int
}

type mirrorPoint struct {
	state     ProviderState
	firstTime time.Time
	lastTime  time.Time
	endTime   time.Time // zero when non-terminal
}

func newGridMirror(removeTimeout time.Duration) *gridMirror {
	return &gridMirror{
		removeTimeout: removeTimeout,
		points:        map[Id]*mirrorPoint{},
		seen:          map[Id]bool{},
	}
}

// connect.MonitorEventFunction
func (self *gridMirror) monitorEventCallback(windowExpandEvent *WindowExpandEvent, providerEvents map[Id]*ProviderEvent, reset bool) {
	self.mu.Lock()
	defer self.mu.Unlock()

	now := time.Now()
	if reset {
		clear(self.points)
	}
	for clientId, providerEvent := range providerEvents {
		self.seen[clientId] = true
		point, ok := self.points[clientId]
		if ok {
			if point.state != providerEvent.State {
				point.state = providerEvent.State
				point.lastTime = now
				if providerEvent.State.IsTerminal() {
					point.endTime = now.Add(self.removeTimeout)
				} else {
					point.endTime = time.Time{}
				}
			}
		} else {
			point = &mirrorPoint{
				state:     providerEvent.State,
				firstTime: now,
				lastTime:  now,
			}
			if providerEvent.State.IsTerminal() {
				point.endTime = now.Add(self.removeTimeout)
			}
			self.points[clientId] = point
		}
	}
	self.reapLocked(now)
	if len(self.points) > self.maxLive {
		self.maxLive = len(self.points)
	}
}

func (self *gridMirror) reapLocked(now time.Time) {
	for clientId, point := range self.points {
		if !point.endTime.IsZero() && !now.Before(point.endTime) {
			delete(self.points, clientId)
		}
	}
}

// sample returns (live points, stuck non-terminal points older than stuckAge, max live)
func (self *gridMirror) sample(stuckAge time.Duration) (int, int, int) {
	self.mu.Lock()
	defer self.mu.Unlock()
	now := time.Now()
	self.reapLocked(now)
	stuck := 0
	for _, point := range self.points {
		if point.endTime.IsZero() && stuckAge < now.Sub(point.lastTime) {
			stuck += 1
		}
	}
	return len(self.points), stuck, self.maxLive
}

func (self *gridMirror) states() map[ProviderState]int {
	self.mu.Lock()
	defer self.mu.Unlock()
	states := map[ProviderState]int{}
	for _, point := range self.points {
		states[point.state] += 1
	}
	return states
}

func (self *gridMirror) totalSeen() int {
	self.mu.Lock()
	defer self.mu.Unlock()
	return len(self.seen)
}

// leakTestGenerator simulates "unable to connect": an unlimited supply of fresh
// candidate providers whose clients have no transports, so every evaluation ping
// is written into a void and fails by timeout.
type leakTestGenerator struct {
}

func (self *leakTestGenerator) NextDestinations(count int, excludeDestinations []MultiHopId, rankMode string) (map[MultiHopId]DestinationStats, error) {
	next := map[MultiHopId]DestinationStats{}
	for range count {
		next[RequireMultiHopId(NewId())] = DestinationStats{
			EstimatedBytesPerSecond: ByteCount(0),
			Tier:                    0,
		}
	}
	return next, nil
}

func (self *leakTestGenerator) NewClientArgs() (*MultiClientGeneratorClientArgs, error) {
	return &MultiClientGeneratorClientArgs{
		ClientId:   NewId(),
		ClientAuth: nil,
	}, nil
}

func (self *leakTestGenerator) RemoveClientArgs(args *MultiClientGeneratorClientArgs) {
}

func (self *leakTestGenerator) RemoveClientWithArgs(client *Client, args *MultiClientGeneratorClientArgs) {
}

func (self *leakTestGenerator) NewClientSettings() *ClientSettings {
	settings := DefaultClientSettings()
	settings.SendBufferSettings.SequenceBufferSize = 0
	settings.SendBufferSettings.AckBufferSize = 0
	settings.ReceiveBufferSettings.SequenceBufferSize = 0
	settings.ForwardBufferSettings.SequenceBufferSize = 0
	return settings
}

func (self *leakTestGenerator) NewClient(ctx context.Context, args *MultiClientGeneratorClientArgs, clientSettings *ClientSettings) (*Client, error) {
	// a client with no transports: sends are accepted into the buffer and go
	// nowhere, like a device with no route to any provider
	client := NewClient(ctx, args.ClientId, NewNoContractClientOob(), clientSettings)
	return client, nil
}

func (self *leakTestGenerator) FixedDestinationSize() (int, bool) {
	return 0, false
}
