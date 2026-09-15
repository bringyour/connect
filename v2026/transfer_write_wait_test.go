package connect

import (
	"context"
	"testing"
	"time"
)

// THROUGHPUTFIX: the reader that makes a flat round trip mean something.
//
// The hypothesis under test elsewhere is that a live flow's throughput is its
// window over an effective acknowledgement round trip, and that the round trip
// includes the flow's own acknowledgements queueing in the shared transport
// writer behind other traffic. If a run comes back with a flat round trip,
// that is two different results depending on the writer: loaded and unmoved
// refutes the mechanism, never near saturation says nothing at all. Nothing
// else in a run separates them.
//
// So the estimate carries its evidence the way the round trip does. Written
// nothing reads unsampled; written freely reads many samples with a mean near
// zero; written into a full writer reads a mean at the wait. A wait rather
// than a queue depth because the comparison it exists for is against a round
// trip, and a wait is already in those units.
func TestWriteWaitEstimateSeparatesAnIdleWriterFromALoadedOne(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	assertMessagePoolOwnership(t)

	client := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		if err := client.CloseAndWait(closeCtx); err != nil {
			t.Errorf("close the client: %v", err)
		}
	})
	destinationId := NewId()
	destination := DestinationId(destinationId)

	writer := client.RouteManager().OpenMultiRouteWriter(destination)
	t.Cleanup(func() { client.RouteManager().CloseMultiRouteWriter(writer) })
	reporter, ok := writer.(WriteWaitReporter)
	if !ok {
		t.Fatalf("the shared writer %T does not report its write wait", writer)
	}

	// nothing written: unsampled, and unreadable as a fast writer
	empty := reporter.WriteWaitEstimate()
	if empty.Sampled() {
		t.Errorf("an unused writer reports %d samples", empty.SampleCount)
	}
	if empty.Mean != 0 || empty.Min != 0 {
		t.Errorf("an unused writer reports mean %s and minimum %s", empty.Mean, empty.Min)
	}

	// a route with room: every write takes the non-blocking path and the wait
	// is about nothing, which is a writer that was never near saturation
	route := make(Route, 8)
	client.RouteManager().UpdateTransport(
		NewSendClientTransport(destination),
		[]Route{route},
	)
	t.Cleanup(func() {
		draining := true
		for draining {
			select {
			case transferFrameBytes := <-route:
				MessagePoolReturn(transferFrameBytes)
			default:
				draining = false
			}
		}
	})
	for range 4 {
		if err := writer.Write(ctx, MessagePoolCopy([]byte("frame")), time.Second); err != nil {
			t.Fatalf("write into a writer with room: %v", err)
		}
		// a successful write takes the buffer, and taking it off the route
		// makes this the owner again
		MessagePoolReturn(<-route)
	}
	idle := reporter.WriteWaitEstimate()
	if !idle.Sampled() {
		t.Fatal("four writes through the writer left it unsampled")
	}
	if 5*time.Millisecond < idle.Mean {
		t.Errorf("a writer with room reports a %s mean wait over %d samples", idle.Mean, idle.SampleCount)
	}

	// a full route: the next write waits for a reader, and the wait is what a
	// live acknowledgement would have spent behind other traffic
	const blockedWait = 40 * time.Millisecond
	for len(route) < cap(route) {
		route <- MessagePoolCopy([]byte("filler"))
	}
	go func() {
		time.Sleep(blockedWait)
		MessagePoolReturn(<-route)
	}()
	if err := writer.Write(ctx, MessagePoolCopy([]byte("blocked")), 5*time.Second); err != nil {
		t.Fatalf("write into a full writer: %v", err)
	}
	loaded := reporter.WriteWaitEstimate()

	if loaded.SampleCount <= idle.SampleCount {
		t.Fatalf("the blocked write added no sample: %d against %d", loaded.SampleCount, idle.SampleCount)
	}
	// the mean rose because one packet waited; the minimum did not, because
	// the earlier ones did not
	if loaded.Mean <= idle.Mean {
		t.Errorf(
			"a writer that made a packet wait %s reports a %s mean, no more than the %s it reported with room",
			blockedWait,
			loaded.Mean,
			idle.Mean,
		)
	}
	if 5*time.Millisecond < loaded.Min {
		t.Errorf(
			"the smallest wait is %s after some packets went straight through; a queue raises the mean and leaves the floor",
			loaded.Min,
		)
	}
	if time.Second < loaded.NewestSampleAge {
		t.Errorf("the newest write is %s old on a run of well under that", loaded.NewestSampleAge)
	}
	t.Logf(
		"with room: mean %s min %s over %d; after one %s block: mean %s min %s over %d, newest %s old",
		idle.Mean, idle.Min, idle.SampleCount,
		blockedWait,
		loaded.Mean, loaded.Min, loaded.SampleCount, loaded.NewestSampleAge,
	)
}
