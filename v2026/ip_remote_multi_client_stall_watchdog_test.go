package connect

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// the whole point of the watchdog is that a stall is noticed on roughly its own
// timescale. Detecting at 3s is worth nothing if it is consulted every 15s,
// which is what made a stalled exit take 15-30s to recover on a device.
func TestSendStallPollTimeoutTracksTheStallTimeout(t *testing.T) {
	resizeTimeout := 15 * time.Second

	pollTimeout := sendStallPollTimeout(3*time.Second, resizeTimeout)

	// comfortably inside the stall timeout, so detection latency is bounded by
	// the timeout itself rather than by the resize cadence
	AssertEqual(t, pollTimeout < 3*time.Second, true)
	AssertEqual(t, pollTimeout < resizeTimeout, true)
}

// a very small timeout must not turn the watchdog into a busy loop
func TestSendStallPollTimeoutHasAFloor(t *testing.T) {
	pollTimeout := sendStallPollTimeout(1*time.Millisecond, 15*time.Second)

	AssertEqual(t, 250*time.Millisecond <= pollTimeout, true)
}

// disabled idles at the resize cadence rather than exiting, so turning stall
// detection back on from the developer menu takes effect without a reconnect
func TestSendStallPollTimeoutWhenDisabled(t *testing.T) {
	resizeTimeout := 15 * time.Second

	AssertEqual(t, sendStallPollTimeout(0, resizeTimeout), resizeTimeout)
	AssertEqual(t, sendStallPollTimeout(-1*time.Second, resizeTimeout), resizeTimeout)
}

// a bare window must be able to read its reliability settings -- the watchdog
// calls this on every pass, and the suite constructs windows without a parent
func TestWindowReliabilitySettingsBareWindow(t *testing.T) {
	window := &multiClientWindow{
		settings: &MultiClientSettings{SendStallTimeout: 3 * time.Second},
	}

	AssertEqual(t, window.reliabilitySettings().SendStallTimeout, 3*time.Second)
}

// the parent's runtime override wins, which is what makes the developer menu
// toggle able to switch the fix off against a live freeze
func TestWindowReliabilitySettingsUsesTheOverride(t *testing.T) {
	window := &multiClientWindow{
		settings: &MultiClientSettings{SendStallTimeout: 3 * time.Second},
		reliabilitySettingsFunc: func() *ReliabilitySettings {
			return &ReliabilitySettings{SendStallTimeout: 0}
		},
	}

	AssertEqual(t, window.reliabilitySettings().SendStallTimeout, time.Duration(0))
}

// watchdogTestWindow wraps a stalled-capable bare channel in a bare window, the
// same fixture idiom stallTestChannel establishes, so the conviction pass can
// be driven directly.
func watchdogTestWindow(clients ...*multiClientChannel) *multiClientWindow {
	windowClients := map[Id]*multiClientChannel{}
	for i, client := range clients {
		// bare channels identify as the zero id; key uniquely for the fixture
		id := Id{}
		id[0] = byte(i + 1)
		windowClients[id] = client
	}
	return &multiClientWindow{
		settings: &MultiClientSettings{},
		clients:  windowClients,
	}
}

// receivingSibling is a healthy window-mate with fresh receive progress: the
// corroboration a stall conviction now requires, because return traffic
// arriving anywhere proves the tunnel carries packets and makes a stalled
// exit's silence evidence about the exit rather than about the phone.
func receivingSibling() *multiClientChannel {
	sibling := stallTestChannel()
	sibling.stateLock.Lock()
	sibling.lastReceiveAckTime = time.Now()
	sibling.stateLock.Unlock()
	return sibling
}

// the hard verdict executes at detection time: a stalled client is errored
// with the distinctive reason and cancelled by the watchdog pass itself,
// rather than waiting for the resize sweep to classify it (which measured
// 3-18s detection-to-removal depending on where the sweep was)
func TestWatchSendStallsConvictsAndCancels(t *testing.T) {
	stallTimeout := 20 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := stallTestChannel()
	client.ctx, client.cancel = context.WithCancel(ctx)

	client.addSend(1440, udpTestPath(4))
	time.Sleep(stallTimeout + 30*time.Millisecond)

	window := watchdogTestWindow(client, receivingSibling())

	AssertEqual(t, window.convictSendStalls(stallTimeout), true)

	// cancelled directly, the DropExit cancel-then-reap idiom
	AssertEqual(t, client.IsDone(), true)

	// the reason wins the endErr slot over Cancel's "Done." (addError keeps
	// the first error), so the removal log names the actual cause
	client.stateLock.Lock()
	endErr := client.endErr
	client.stateLock.Unlock()
	AssertEqual(t, endErr != nil, true)
	AssertEqual(t, strings.HasPrefix(endErr.Error(), "send stalled"), true)

	// HARD evidence must not be budgeted: the storm breaker keys verdict
	// removals on the "Blackhole " prefix, and the stall reason must never
	// carry it
	AssertEqual(t, blackholeVerdictErr(endErr), false)
}

// The uplink-corroboration hold: with no sibling receiving, a stall verdict
// says as much about the phone as about the exit, so it must be held -- and
// the stall clock must NOT be refreshed, so the same evidence convicts on the
// first pass after a sibling proves the uplink. This is the gate that stops
// one cellular blip shorter than the uplink gate's bar from executing every
// loaded exit at once (three in three minutes in the field, 2026-08-03).
func TestWatchSendStallsHeldWithoutReceivingSibling(t *testing.T) {
	stallTimeout := 20 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := stallTestChannel()
	client.ctx, client.cancel = context.WithCancel(ctx)

	client.addSend(1440, udpTestPath(4))
	time.Sleep(stallTimeout + 30*time.Millisecond)

	// a window-mate exists but has NO recent receive: silence everywhere is
	// the phone's silence
	quietSibling := stallTestChannel()
	window := watchdogTestWindow(client, quietSibling)

	AssertEqual(t, window.convictSendStalls(stallTimeout), false)
	AssertEqual(t, client.IsDone(), false)
	client.stateLock.Lock()
	endErr := client.endErr
	client.stateLock.Unlock()
	AssertEqual(t, endErr == nil, true)

	// the evidence carried: the moment the sibling receives, the very next
	// pass convicts without waiting out a fresh bar
	quietSibling.stateLock.Lock()
	quietSibling.lastReceiveAckTime = time.Now()
	quietSibling.stateLock.Unlock()
	AssertEqual(t, window.convictSendStalls(stallTimeout), true)
	AssertEqual(t, client.IsDone(), true)
}

// a healthy (non-stalled) client is untouched by the conviction pass
func TestWatchSendStallsLeavesHealthyClients(t *testing.T) {
	stallTimeout := 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := stallTestChannel()
	client.ctx, client.cancel = context.WithCancel(ctx)

	// outstanding send inside the window: merely slow, not stalled
	client.addSend(1440, udpTestPath(4))

	window := watchdogTestWindow(client)

	AssertEqual(t, window.convictSendStalls(stallTimeout), false)
	AssertEqual(t, client.IsDone(), false)

	client.stateLock.Lock()
	endErr := client.endErr
	client.stateLock.Unlock()
	AssertEqual(t, endErr == nil, true)
}

// the storm breaker's verdict classification: the two hard-evidence reasons
// this package introduces must never classify as budgeted blackhole verdicts,
// while a real verdict line still does
func TestWatchSendStallsAndCpingReasonsAreNotVerdicts(t *testing.T) {
	AssertEqual(t, blackholeVerdictErr(errors.New("send stalled: no ack progress for 3s")), false)
	AssertEqual(t, blackholeVerdictErr(errors.New("cping timeout")), false)
	// positive control: the verdict prefix is still classified
	AssertEqual(t, blackholeVerdictErr(errors.New("Blackhole no send ack")), true)
}

// Source anchors: the conviction is only real if the watchdog pass errors AND
// cancels the channel itself and the loop routes through it -- a helper that
// is correct but uncalled is the failure mode this suite pins against.
func TestWatchSendStallsSourceAnchors(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}
	body, ok := functionBody(source, "func (self *multiClientWindow) convictSendStalls(")
	if !ok {
		t.Fatal("could not find convictSendStalls")
	}
	if !strings.Contains(body, "client.addError(") {
		t.Error("convictSendStalls does not addError: the removal would log as a bare Done.")
	}
	if !strings.Contains(body, "client.Cancel()") {
		t.Error("convictSendStalls does not cancel directly: removal latency is back on the resize sweep")
	}
	if !strings.Contains(body, `"send stalled`) {
		t.Error("convictSendStalls does not use the distinctive stall reason")
	}
	if strings.Contains(body, `"Blackhole `) {
		t.Error("the stall reason must never carry the budgeted verdict prefix")
	}

	body, ok = functionBody(source, "func (self *multiClientWindow) watchSendStalls(")
	if !ok {
		t.Fatal("could not find watchSendStalls")
	}
	if !strings.Contains(body, "self.convictSendStalls(") {
		t.Error("watchSendStalls does not run the conviction pass")
	}
	if !strings.Contains(body, "self.resizeMonitor.NotifyAll()") {
		t.Error("watchSendStalls does not wake resize for the reap and backfill")
	}

	// The watchdog owns the complete verdict, including active-probe, uplink,
	// shared-fate, and quarantine gates. A raw resize-side read bypassed those
	// gates and removed an explicitly held one-bar route.
	body, ok = functionBody(source, "func (self *multiClientWindow) resize()")
	if !ok {
		t.Fatal("could not find resize")
	}
	if strings.Contains(body, ".sendStalled(") {
		t.Error("resize bypasses the send-stall watchdog verdict gates")
	}
}

// The cping timeout must never convict. An earlier pass here added
// addError("cping timeout") on the belief that the bare return was already
// removing the channel with an unlabeled reason -- it was not (HandleError
// runs its cancel handler only on panic), and the conviction it introduced
// executed every fixture client at CPingTimeout in TestMultiClientUdp4 and
// would have removed a production exit for one lost ack. This pins the
// restored semantics: an unanswered ping ends the ping loop and nothing else.
func TestCpingTimeoutSourceAnchors(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}
	body, ok := functionBody(source, "func (self *multiClientChannel) ping(")
	if !ok {
		t.Fatal("could not find ping")
	}
	if strings.Contains(body, `addError(errors.New("cping timeout"))`) {
		t.Error("the cping timeout branch convicts: one lost ping ack would remove a live exit and every flow on it")
	}
	if strings.Contains(body, "defer self.cancel()") {
		t.Error("ping still cancels the channel on every return, including its explicitly non-convicting timeout")
	}
	if !strings.Contains(body, "unanswered: ping loop ended") {
		t.Error("the cping timeout is silent again: the log line is the observability the removed conviction was after")
	}
}

// A real lifecycle pin for the source invariant above: an admitted ping whose
// callback never arrives is the ordinary DATAGRAM-loss case. When its bounded
// wait expires, only this optional monitor stops; the channel and its flow
// context must remain alive for Transfer retransmission and the evidence
// classifiers to make their own decisions.
func TestCpingTimeoutLeavesChannelAlive(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	channel := stallTestChannel()
	channel.ctx = ctx
	channel.cancel = cancel
	channel.log = DefaultLogger()
	channel.args = &multiClientChannelArgs{Destination: RequireMultiHopId(NewId())}
	channel.settings.CPingTimeout = 25 * time.Millisecond
	channel.settings.CPingWriteTimeout = 25 * time.Millisecond
	var lateAck func(error)
	channel.pingSendForTest = func(_ time.Duration, ackCallback func(error)) (bool, error) {
		lateAck = ackCallback
		return true, nil
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		channel.ping()
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cping did not end after its unanswered-ack timeout")
	}
	select {
	case <-ctx.Done():
		t.Fatal("one unanswered cping canceled the live channel")
	default:
	}
	lateAckDone := make(chan struct{})
	go func() {
		defer close(lateAckDone)
		lateAck(nil)
	}()
	select {
	case <-lateAckDone:
	case <-time.After(time.Second):
		t.Fatal("late cping Ack retained its callback after the ping loop ended")
	}
}

// A concrete send/ack error remains hard evidence. Removing the blanket defer
// must not turn a reported Transfer failure into the same harmless timeout.
func TestCpingAckErrorStillCancelsWithCause(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	channel := stallTestChannel()
	channel.ctx = ctx
	channel.cancel = cancel
	channel.log = DefaultLogger()
	channel.args = &multiClientChannelArgs{Destination: RequireMultiHopId(NewId())}
	channel.settings.CPingTimeout = time.Second
	wantErr := errors.New("cping transfer failed")
	channel.pingSendForTest = func(_ time.Duration, ackCallback func(error)) (bool, error) {
		go ackCallback(wantErr)
		return true, nil
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		channel.ping()
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cping did not finish after its Transfer error")
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("a reported cping Transfer error did not cancel the channel")
	}
	_, gotErr := channel.WindowStats()
	if !errors.Is(gotErr, wantErr) {
		t.Fatalf("channel error=%v, want %v", gotErr, wantErr)
	}
}
