package connect

import (
	"context"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

// The per-destination ceiling is its resend queue over the effective
// acknowledgement round trip, and that divisor is the network's round trip
// plus terms of ours. Which of the two dominates decides between raising the
// queue, which costs memory proportionally — forty simultaneous downloaders at
// a 16 MiB queue would retain over a gigabyte on one provider, so it would
// have to be a share of a budget rather than a constant — and shrinking the
// divisor, which costs nothing. The sequence has always measured the quantity
// and nothing outside could read it.
//
// This pins the reader, including the distinction that makes it usable: a zero
// mean with no sampled sequence means no evidence, and a zero mean with
// sampled sequences means a measured sub-millisecond path. Every other reader
// on the window folds the empty case into a resend floor, which is right for
// timing and wrong for measurement.
func TestDestinationSendStatsCarryTheRoundTripMean(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	assertMessagePoolOwnership(t)

	const ackDelay = 20 * time.Millisecond

	newSettings := func() *ClientSettings {
		settings := DefaultClientSettings()
		settings.EncryptionSettings.Mode = EncryptionModeOff
		settings.SendBufferSettings.AckTimeout = 60 * time.Second
		settings.SendBufferSettings.IdleTimeout = 60 * time.Second
		settings.ReceiveBufferSettings.GapTimeout = 60 * time.Second
		settings.ReceiveBufferSettings.IdleTimeout = 60 * time.Second
		return settings
	}
	senderId := NewId()
	receiverId := NewId()
	sender := NewClient(ctx, senderId, NewNoContractClientOob(), newSettings())
	receiver := NewClient(ctx, receiverId, NewNoContractClientOob(), newSettings())
	sender.ContractManager().AddNoContractPeer(receiverId)
	receiver.ContractManager().AddNoContractPeer(senderId)

	senderOut := make(Route, 64)
	senderIn := make(Route, 64)
	receiverIn := make(Route, 64)
	receiverOut := make(Route, 64)
	sender.RouteManager().UpdateTransport(NewSendGatewayTransport(), []Route{senderOut})
	sender.RouteManager().UpdateTransport(NewReceiveGatewayTransport(), []Route{senderIn})
	receiver.RouteManager().UpdateTransport(NewReceiveGatewayTransport(), []Route{receiverIn})
	receiver.RouteManager().UpdateTransport(NewSendGatewayTransport(), []Route{receiverOut})
	receiver.AddReceiveCallback(func(TransferPath, []*protocol.Frame, Peer) {})

	pumpsDone := []chan struct{}{}
	pump := func(from Route, to Route, delay time.Duration) {
		done := make(chan struct{})
		pumpsDone = append(pumpsDone, done)
		go func() {
			defer close(done)
			for {
				select {
				case transferFrameBytes := <-from:
					if 0 < delay {
						time.Sleep(delay)
					}
					select {
					case to <- transferFrameBytes:
					case <-ctx.Done():
						MessagePoolReturn(transferFrameBytes)
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	pump(senderOut, receiverIn, 0)
	// the acknowledgement half carries the round trip this row reads back
	pump(receiverOut, senderIn, ackDelay)
	t.Cleanup(func() {
		cancel()
		for _, done := range pumpsDone {
			<-done
		}
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		if err := sender.CloseAndWait(closeCtx); err != nil {
			t.Errorf("close the sender: %v", err)
		}
		if err := receiver.CloseAndWait(closeCtx); err != nil {
			t.Errorf("close the receiver: %v", err)
		}
		for _, route := range []Route{senderOut, senderIn, receiverIn, receiverOut} {
			draining := true
			for draining {
				select {
				case transferFrameBytes := <-route:
					MessagePoolReturn(transferFrameBytes)
				default:
					draining = false
				}
			}
		}
	})

	// nothing has been acknowledged: no evidence, and the reader must say so
	// rather than reporting a zero that reads as a fast path
	before := sender.DestinationSendStats(receiverId)
	if before.Rtt.Sampled() {
		t.Errorf("the round trip reports %d samples before anything was sent", before.Rtt.SampleCount)
	}
	if before.Rtt.Mean != 0 || 0 < before.RttSequenceCount {
		t.Errorf(
			"the mean round trip is %s over %d sequences with no samples behind it",
			before.Rtt.Mean,
			before.RttSequenceCount,
		)
	}

	payload := string(make([]byte, 1024))
	for range 16 {
		frame := RequireToFrameWithDefaultProtocolVersion(
			&protocol.SimpleMessage{Content: payload},
		)
		admitted, _ := sender.SendWithTimeoutDetailed(
			frame,
			receiverId,
			nil,
			2*time.Second,
			sendPackRecoveryOption{upstreamRecoverable: true, retainAfterAckTimeout: true},
		)
		if !admitted {
			MessagePoolReturn(frame.MessageBytes)
			t.Fatal("a Pack toward the receiver was not admitted")
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(10 * ackDelay)

	after := sender.DestinationSendStats(receiverId)
	if !after.Rtt.Sampled() {
		t.Fatalf(
			"the round trip is unsampled after %d writes and %d resend writes; the window measures it and this reader is the only path to it",
			after.WriteCount,
			after.ResendWriteCount,
		)
	}
	// the pump's own delay is the floor of what the window can measure
	if after.Rtt.Mean < ackDelay {
		t.Errorf(
			"the mean round trip is %s over %d samples, below the %s the acknowledgement path delays by",
			after.Rtt.Mean,
			after.Rtt.SampleCount,
			ackDelay,
		)
	}
	// the evidence travels with the value: an estimate whose newest sample is
	// older than the run describes a path that no longer exists
	if 5*time.Second < after.Rtt.NewestSampleAge {
		t.Errorf(
			"the newest round trip sample is %s old on a run of well under that; freshness is not being carried",
			after.Rtt.NewestSampleAge,
		)
	}
	t.Logf(
		"%d sequences, %d writes: mean round trip %s, minimum %s, over %d samples from %d sequences, newest sample %s old, against a %s acknowledgement delay; mean less minimum is %s",
		after.SequenceCount,
		after.WriteCount,
		after.Rtt.Mean,
		after.Rtt.Min,
		after.Rtt.SampleCount,
		after.RttSequenceCount,
		after.Rtt.NewestSampleAge,
		ackDelay,
		after.Rtt.Mean-after.Rtt.Min,
	)
}

// THROUGHPUTFIX §34.3 row `TestRttEstimateCarriesItsMinimum`. The mean alone
// cannot separate latency we add from a backlog somewhere in the
// acknowledgement path, and the difference decides whether shrinking the
// divisor is worth a factor of four and a half at no memory or is nothing: two
// mebibytes over 90 ms is 186 Mb/s and over 20 is 838.
//
// The minimum separates them in one reading. Acknowledgements queueing behind
// a serial element read a minimum near the imposed delay with a mean far above
// it and samples that rise across the run; latency genuinely added to each
// acknowledgement reads a minimum as high as the mean.
//
// The window keeps a monotonic-minimum deque already, so this pins that the
// estimate carries it, under the same lock and coalesce as the mean, and that
// it is unreadable as a real value when nothing has been sampled.
func TestRttEstimateCarriesItsMinimum(t *testing.T) {
	assertMessagePoolOwnership(t)

	settings := DefaultClientSettings()
	rttWindow := NewRttWindow(
		DefaultLogger(),
		settings.SendBufferSettings.RttWindowSize,
		settings.SendBufferSettings.RttWindowTimeout,
		settings.SendBufferSettings.RttScale,
		settings.SendBufferSettings.MinResendInterval,
		settings.SendBufferSettings.RttMinResendInterval,
		settings.SendBufferSettings.MaxResendInterval,
	)

	// unsampled: the minimum must be as unreadable as the mean, since a zero
	// minimum over no samples and a measured zero are different facts
	empty := rttWindow.Estimate()
	if empty.Sampled() {
		t.Errorf("an untouched window reports %d samples", empty.SampleCount)
	}
	if empty.Min != 0 || empty.Mean != 0 {
		t.Errorf("an untouched window reports mean %s and minimum %s", empty.Mean, empty.Min)
	}

	// a backlog's shape: one fast sample and a rising series behind it, which
	// is what a serial delay element produces
	sampleTime := time.Now()
	samples := []time.Duration{
		20 * time.Millisecond,
		30 * time.Millisecond,
		40 * time.Millisecond,
		50 * time.Millisecond,
		60 * time.Millisecond,
	}
	meanTotal := time.Duration(0)
	for _, sample := range samples {
		rttWindow.closeSendTime(uint64(sampleTime.Add(-sample).UnixMilli()), sampleTime)
		meanTotal += sample
	}
	estimate := rttWindow.estimate(sampleTime)

	if !estimate.Sampled() {
		t.Fatal("the window reports no samples after five were closed")
	}
	if estimate.SampleCount != len(samples) {
		t.Errorf("the window reports %d samples, want %d", estimate.SampleCount, len(samples))
	}
	// the smallest live sample, not the newest and not the mean. The stamp is
	// truncated to milliseconds on the wire, so a sample carries up to a
	// millisecond of that truncation and the comparison allows it.
	const truncation = 2 * time.Millisecond
	if estimate.Min < samples[0] || samples[0]+truncation < estimate.Min {
		t.Errorf(
			"the estimate's minimum is %s over samples %v, want the smallest of them, %s; without it a mean of %s cannot be told from acknowledgements queueing behind a serial element",
			estimate.Min,
			samples,
			samples[0],
			estimate.Mean,
		)
	}
	wantMean := meanTotal / time.Duration(len(samples))
	if estimate.Mean < wantMean || wantMean+truncation < estimate.Mean {
		t.Errorf("the estimate's mean is %s, want %s", estimate.Mean, wantMean)
	}
	if estimate.Mean <= estimate.Min {
		t.Errorf(
			"the estimate's mean %s is not above its minimum %s on a rising series; the two are not being read from the same window",
			estimate.Mean,
			estimate.Min,
		)
	}
	t.Logf(
		"samples %v: mean %s, minimum %s, %d samples; mean less minimum is %s of queueing",
		samples,
		estimate.Mean,
		estimate.Min,
		estimate.SampleCount,
		estimate.Mean-estimate.Min,
	)
}
