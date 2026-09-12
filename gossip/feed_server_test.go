// The feed server (EXTENDER.md A8, D4).
//
// One test drives the phase 3 feed client through a real extender, which is
// the whole path a client takes. The caps are pinned over a pipe instead,
// because what they bound is the server's own bookkeeping and a pipe makes the
// ordering exact: the server's write blocks until the client reads, so a test
// decides when the server may make progress.

package gossip

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/urnetwork/connect"
	"github.com/urnetwork/connect/extender"
	"github.com/urnetwork/connect/protocol"
)

// A client takes the sample with the server's own record first, then reads the
// live stream and the keepalive of an idle subscription (D4).
func TestGossipFeedServesTheSampleAndStreams(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	rootKey := newTestKey(t)
	extenderKey := newTestKey(t)
	otherKey := newTestKey(t)

	tcpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tcpPort := tcpListener.Addr().(*net.TCPAddr).Port

	directory := newTestDirectory(t, rootKey)
	issueTime := time.Now()
	for _, c := range []struct {
		key *testKey
		ip  string
	}{
		{key: extenderKey, ip: "127.0.0.1"},
		{key: otherKey, ip: "192.0.2.50"},
	} {
		record := signTestRecord(t, rootKey, c.key, c.ip, tcpPort, issueTime)
		if _, err := directory.ApplyRecord(record, connect.ExtenderSourceGossip); err != nil {
			t.Fatal(err)
		}
	}

	feedSettings := DefaultFeedServerSettings()
	// an idle subscription keepalives at a test cadence
	feedSettings.KeepaliveTimeout = 200 * time.Millisecond
	feed := NewFeedServer(ctx, directory, extenderKey.publicKey, feedSettings)
	t.Cleanup(feed.Close)
	newTestExtenderServer(t, ctx, extenderKey, tcpListener, tcpPort, func(settings *extender.ExtenderSettings) {
		settings.FeedConnHandler = feed.Serve
	})

	dialCtx, dialCancel := context.WithTimeout(ctx, 30*time.Second)
	defer dialCancel()
	stream, err := connect.DialExtenderFeed(
		dialCtx,
		connect.DefaultConnectSettings(),
		&connect.ExtenderConfig{
			Profile: connect.ExtenderProfile{
				ConnectMode: connect.ExtenderConnectModeTcpTls,
				ServerName:  "127.0.0.1",
				Port:        tcpPort,
			},
			Ip: netip.MustParseAddr("127.0.0.1"),
			// the outer leaf is verified against the record key (B3, E5)
			PublicKey: extenderKey.publicKey,
		},
		&protocol.ExtenderFeedRequest{
			SampleCount: 8,
			Subscribe:   true,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stream.Close() })

	readCtx, readCancel := context.WithTimeout(ctx, 30*time.Second)
	defer readCancel()
	sampleKeys := []ed25519.PublicKey{}
	for {
		frame, err := stream.Next(readCtx)
		if err != nil {
			t.Fatal(err)
		}
		if frame.GetEndOfSample() {
			break
		}
		if frame.GetRecord() == nil {
			t.Fatalf("the sample carried %v, expected a record", frame)
		}
		body, err := directory.RootKeys().VerifyRecord(frame.GetRecord())
		if err != nil {
			t.Fatal(err)
		}
		sampleKeys = append(sampleKeys, ed25519.PublicKey(body.PublicKey))
	}
	if len(sampleKeys) != 2 {
		t.Fatalf("the sample carried %d records, expected 2", len(sampleKeys))
	}
	if !sampleKeys[0].Equal(extenderKey.publicKey) {
		t.Fatalf("the sample did not lead with the extender's own record")
	}

	// a record applied after the sample is streamed on the same connection
	streamedKey := newTestKey(t)
	streamedRecord := signTestRecord(t, rootKey, streamedKey, "192.0.2.51", tcpPort, issueTime)
	if _, err := directory.ApplyRecord(streamedRecord, connect.ExtenderSourceGossip); err != nil {
		t.Fatal(err)
	}
	frame, err := stream.Next(readCtx)
	if err != nil {
		t.Fatal(err)
	}
	streamedBody, err := directory.RootKeys().VerifyRecord(frame.GetRecord())
	if err != nil {
		t.Fatal(err)
	}
	if !ed25519.PublicKey(streamedBody.PublicKey).Equal(streamedKey.publicKey) {
		t.Fatalf("the streamed record is not the one that was applied")
	}

	// and an idle subscription is kept alive
	if frame, err = stream.Next(readCtx); err != nil {
		t.Fatal(err)
	}
	if !frame.GetKeepalive() {
		t.Fatalf("the idle subscription carried %v, expected a keepalive", frame)
	}
}

// Over the subscriber cap a client still gets its sample, and only the
// subscription is refused (D4).
func TestGossipFeedRefusesOverTheSubscriberCap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	rootKey := newTestKey(t)
	directory := newTestDirectory(t, rootKey)
	settings := DefaultFeedServerSettings()
	settings.MaxSubscriberCount = 1
	settings.KeepaliveTimeout = time.Minute
	feed := NewFeedServer(ctx, directory, nil, settings)
	t.Cleanup(feed.Close)

	held := newTestFeedClient(t, feed, true)
	held.readEndOfSample(t)
	if subscriberCount := feed.SubscriberCount(); subscriberCount != 1 {
		t.Fatalf("subscribers = %d, expected 1", subscriberCount)
	}

	// the second client is over the cap: it is served its sample and then the
	// stream ends
	refused := newTestFeedClient(t, feed, true)
	refused.readEndOfSample(t)
	select {
	case <-refused.done:
	case <-time.After(10 * time.Second):
		t.Fatal("a client over the subscriber cap kept its subscription")
	}
	if _, err := connect.ReadExtenderFeedFrame(refused.conn); err == nil {
		t.Fatal("a client over the subscriber cap was still served")
	}

	// the held subscription is untouched
	select {
	case <-held.done:
		t.Fatal("the held subscription ended")
	default:
	}
}

// A subscriber that stops reading is disconnected rather than waited on, once
// it falls a whole buffer behind (D4).
func TestGossipFeedDisconnectsASlowSubscriber(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	rootKey := newTestKey(t)
	directory := newTestDirectory(t, rootKey)
	settings := DefaultFeedServerSettings()
	// nothing else may end the stream while the buffer fills
	settings.KeepaliveTimeout = time.Minute
	settings.WriteTimeout = 0
	feed := NewFeedServer(ctx, directory, nil, settings)
	t.Cleanup(feed.Close)

	client := newTestFeedClient(t, feed, true)
	client.readEndOfSample(t)

	// the server takes one message and blocks writing it, the buffer takes the
	// next, and the one after that overflows and cuts the subscription off
	issueTime := time.Now()
	for i := range connect.ExtenderDirectorySubscribeBufferCount + 2 {
		record := signTestRecord(
			t,
			rootKey,
			newTestKey(t),
			fmt.Sprintf("198.51.100.%d", i),
			443,
			issueTime,
		)
		if _, err := directory.ApplyRecord(record, connect.ExtenderSourceGossip); err != nil {
			t.Fatal(err)
		}
	}
	// releasing the blocked write is what lets the server see the closed
	// subscription
	if _, err := connect.ReadExtenderFeedFrame(client.conn); err != nil {
		t.Fatal(err)
	}
	select {
	case <-client.done:
	case <-time.After(10 * time.Second):
		t.Fatal("a subscriber that fell behind was not disconnected")
	}
}

// One feed client over a pipe, with the server half served in its own
// goroutine exactly as the extender serves it (A8).
type testFeedClient struct {
	conn net.Conn
	done chan struct{}
}

func newTestFeedClient(t *testing.T, feed *FeedServer, subscribe bool) *testFeedClient {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	client := &testFeedClient{
		conn: clientConn,
		done: make(chan struct{}),
	}
	go func() {
		defer close(client.done)
		// the extender closes the stream when the handler returns (A8)
		defer serverConn.Close()
		feed.Serve(serverConn)
	}()
	t.Cleanup(func() {
		clientConn.Close()
		<-client.done
	})
	if err := connect.WriteExtenderFeedRequest(clientConn, &protocol.ExtenderFeedRequest{
		SampleCount: 8,
		Subscribe:   subscribe,
	}); err != nil {
		t.Fatal(err)
	}
	return client
}

// Reads through the sample, which leaves the stream on the live subscription.
func (self *testFeedClient) readEndOfSample(t *testing.T) {
	t.Helper()
	for {
		frame, err := connect.ReadExtenderFeedFrame(self.conn)
		if err != nil {
			t.Fatal(err)
		}
		if frame.GetEndOfSample() {
			return
		}
	}
}
