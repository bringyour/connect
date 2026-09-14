// The feed server's bounds and stream ends (EXTENDER.md A8, D4).
//
// Every stream here is a pipe, which makes the ordering exact: the server's
// write blocks until the client reads, so the test decides when the server may
// make progress. Nothing waits on a clock for a result -- the one timeout
// under test is observed through the end of the stream it is supposed to end.

package gossip

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026"
	"github.com/urnetwork/connect/v2026/protocol"
)

// How long a feed assertion waits for a stream that should already have ended.
const testFeedTimeout = 10 * time.Second

// The server's caps and budgets are what bound one extender's feed, so they
// are pinned rather than left to whatever the constructor happens to do (D4).
func TestGossipFeedServerDefaults(t *testing.T) {
	settings := DefaultFeedServerSettings()
	if settings.MaxSampleCount != connect.ExtenderFeedMaxSampleCount {
		t.Errorf(
			"max sample = %d, expected the protocol cap %d",
			settings.MaxSampleCount,
			connect.ExtenderFeedMaxSampleCount,
		)
	}
	if connect.ExtenderFeedMaxSampleCount != 32 {
		t.Errorf("the protocol sample cap is %d, expected 32", connect.ExtenderFeedMaxSampleCount)
	}
	if settings.MaxSubscriberCount != 256 {
		t.Errorf("max subscribers = %d, expected 256", settings.MaxSubscriberCount)
	}
	if settings.KeepaliveTimeout != 30*time.Second {
		t.Errorf("keepalive timeout = %s, expected 30s", settings.KeepaliveTimeout)
	}
	if settings.RequestTimeout != 30*time.Second {
		t.Errorf("request timeout = %s, expected 30s", settings.RequestTimeout)
	}
	if settings.WriteTimeout != 30*time.Second {
		t.Errorf("write timeout = %s, expected 30s", settings.WriteTimeout)
	}
}

// The sample is the server's bound, not the client's: a client that asks for
// more gets the cap, and a client that asks for none still gets the end of the
// sample it must wait for (D4).
func TestGossipFeedClampsTheSample(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	rootKey := newTestKey(t)
	directory := newTestDirectory(t, rootKey)
	for i := range 5 {
		applyTestFeedRecord(t, directory, rootKey, newTestKey(t), fmt.Sprintf("198.51.100.%d", i+1))
	}

	settings := DefaultFeedServerSettings()
	settings.MaxSampleCount = 2
	settings.KeepaliveTimeout = time.Minute
	feed := NewFeedServer(ctx, directory, nil, settings)
	t.Cleanup(feed.Close)

	cases := []struct {
		sampleCount uint32
		expect      int
	}{
		{sampleCount: 8, expect: 2},
		{sampleCount: 3, expect: 2},
		{sampleCount: 2, expect: 2},
		{sampleCount: 1, expect: 1},
		// no sample at all is still a complete sample
		{sampleCount: 0, expect: 0},
	}
	for _, c := range cases {
		client := newTestFeedRequestClient(t, feed, &protocol.ExtenderFeedRequest{
			SampleCount: c.sampleCount,
			Subscribe:   false,
		})
		frames := client.readSample(t)
		if len(frames) != c.expect {
			t.Errorf("sample of %d = %d records, expected %d", c.sampleCount, len(frames), c.expect)
		}
		for _, frame := range frames {
			if frame.GetRecord() == nil {
				t.Errorf("sample of %d carried %v, expected a record", c.sampleCount, frame)
			}
		}
	}
}

// A client that did not subscribe takes its bootstrap and goes, which is what
// lets one extender serve many of them (D4, D6).
func TestGossipFeedEndsANonSubscriberStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	rootKey := newTestKey(t)
	directory := newTestDirectory(t, rootKey)
	applyTestFeedRecord(t, directory, rootKey, newTestKey(t), "198.51.100.11")

	settings := DefaultFeedServerSettings()
	settings.KeepaliveTimeout = time.Minute
	feed := NewFeedServer(ctx, directory, nil, settings)
	t.Cleanup(feed.Close)

	client := newTestFeedClient(t, feed, false)
	if frames := client.readSample(t); len(frames) != 1 {
		t.Fatalf("the sample carried %d records, expected 1", len(frames))
	}
	// no subscriber slot was ever taken, and the stream ends with the sample
	if subscriberCount := feed.SubscriberCount(); subscriberCount != 0 {
		t.Errorf("subscribers = %d, expected none", subscriberCount)
	}
	select {
	case <-client.done:
	case <-time.After(testFeedTimeout):
		t.Fatal("a client that did not subscribe kept its stream")
	}
	if frame, err := connect.ReadExtenderFeedFrame(client.conn); err == nil {
		t.Fatalf("a client that did not subscribe was served %v", frame)
	}
}

// A subscription carries revocations as well as records, because a client that
// only heard records would keep dialing an extender the space has withdrawn
// (D4, B5).
func TestGossipFeedStreamsRevocations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	rootKey := newTestKey(t)
	extenderKey := newTestKey(t)
	directory := newTestDirectory(t, rootKey)

	settings := DefaultFeedServerSettings()
	settings.KeepaliveTimeout = time.Minute
	feed := NewFeedServer(ctx, directory, nil, settings)
	t.Cleanup(feed.Close)

	client := newTestFeedClient(t, feed, true)
	if frames := client.readSample(t); len(frames) != 0 {
		t.Fatalf("the sample of an empty directory carried %v", frames)
	}

	issueTime := time.Now()
	applyTestFeedRecord(t, directory, rootKey, extenderKey, "198.51.100.21")
	revocation, err := connect.SignExtenderRevocation(
		rootKey.privateKey,
		&protocol.ExtenderRevocationBody{
			PublicKey:   extenderKey.publicKey,
			IssueTimeMs: uint64(issueTime.Add(time.Second).UnixMilli()),
			NetworkHost: testNetworkHost,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := directory.ApplyRevocationSource(revocation, connect.ExtenderSourceGossip); err != nil {
		t.Fatal(err)
	}

	// the record and the revocation arrive in the order they were applied
	recordFrame, err := connect.ReadExtenderFeedFrame(client.conn)
	if err != nil {
		t.Fatal(err)
	}
	if recordFrame.GetRecord() == nil {
		t.Fatalf("the stream carried %v, expected a record", recordFrame)
	}
	revocationFrame, err := connect.ReadExtenderFeedFrame(client.conn)
	if err != nil {
		t.Fatal(err)
	}
	if revocationFrame.GetRevocation() == nil {
		t.Fatalf("the stream carried %v, expected a revocation", revocationFrame)
	}
	body, err := directory.RootKeys().VerifyRevocation(revocationFrame.GetRevocation())
	if err != nil {
		t.Fatal(err)
	}
	if !ed25519.PublicKey(body.PublicKey).Equal(extenderKey.publicKey) {
		t.Fatalf("the streamed revocation is not the one that was applied")
	}
}

// A subscriber that goes away frees its slot, so the cap bounds what is being
// served right now rather than what was ever served (D4).
func TestGossipFeedFreesTheSubscriberSlot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	rootKey := newTestKey(t)
	directory := newTestDirectory(t, rootKey)
	settings := DefaultFeedServerSettings()
	settings.MaxSubscriberCount = 1
	settings.KeepaliveTimeout = time.Minute
	feed := NewFeedServer(ctx, directory, nil, settings)
	t.Cleanup(feed.Close)

	first := newTestFeedClient(t, feed, true)
	first.readEndOfSample(t)
	if subscriberCount := feed.SubscriberCount(); subscriberCount != 1 {
		t.Fatalf("subscribers = %d, expected 1", subscriberCount)
	}

	// the client goes away, which is what releases the slot
	first.conn.Close()
	select {
	case <-first.done:
	case <-time.After(testFeedTimeout):
		t.Fatal("a departed client kept its subscription")
	}
	if subscriberCount := feed.SubscriberCount(); subscriberCount != 0 {
		t.Fatalf("subscribers = %d after the client went away, expected none", subscriberCount)
	}

	// the next client takes the freed slot and is a live subscriber, not a
	// client over the cap
	second := newTestFeedClient(t, feed, true)
	second.readEndOfSample(t)
	if subscriberCount := feed.SubscriberCount(); subscriberCount != 1 {
		t.Fatalf("subscribers = %d, expected the freed slot to be taken", subscriberCount)
	}
	applyTestFeedRecord(t, directory, rootKey, newTestKey(t), "198.51.100.31")
	frame, err := connect.ReadExtenderFeedFrame(second.conn)
	if err != nil {
		t.Fatal(err)
	}
	if frame.GetRecord() == nil {
		t.Fatalf("the second subscriber was served %v, expected a record", frame)
	}
}

// Closing the server ends every open stream, so the extender can close the
// connections behind them (A8, D4).
func TestGossipFeedCloseEndsALiveSubscriber(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	rootKey := newTestKey(t)
	directory := newTestDirectory(t, rootKey)
	settings := DefaultFeedServerSettings()
	settings.KeepaliveTimeout = time.Minute
	feed := NewFeedServer(ctx, directory, nil, settings)
	t.Cleanup(feed.Close)

	client := newTestFeedClient(t, feed, true)
	client.readEndOfSample(t)

	feed.Close()
	select {
	case <-client.done:
	case <-time.After(testFeedTimeout):
		t.Error("the server close did not end a live subscriber")
	}
}

// A connection that opens the feed service and then says nothing is dropped on
// the request budget, so it cannot hold a stream open for free (D4).
func TestGossipFeedDropsASilentClient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	rootKey := newTestKey(t)
	directory := newTestDirectory(t, rootKey)
	settings := DefaultFeedServerSettings()
	// the budget under test; the stream ending is what proves it ran
	settings.RequestTimeout = 200 * time.Millisecond
	settings.KeepaliveTimeout = time.Minute
	feed := NewFeedServer(ctx, directory, nil, settings)
	t.Cleanup(feed.Close)

	client := newTestFeedConn(t, feed)
	select {
	case <-client.done:
	case <-time.After(testFeedTimeout):
		t.Fatal("a client that sent no request kept its stream")
	}
	if frame, err := connect.ReadExtenderFeedFrame(client.conn); err == nil {
		t.Fatalf("a client that sent no request was served %v", frame)
	}
}

// One record applied to the directory, which is what a subscriber sees.
func applyTestFeedRecord(
	t *testing.T,
	directory *connect.ExtenderDirectory,
	rootKey *testKey,
	extenderKey *testKey,
	ip string,
) {
	t.Helper()
	record := signTestRecord(t, rootKey, extenderKey, ip, 8443, time.Now())
	if _, err := directory.ApplyRecord(record, connect.ExtenderSourceGossip); err != nil {
		t.Fatal(err)
	}
}
