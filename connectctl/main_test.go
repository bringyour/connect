package main

import (
	"fmt"
	"testing"

	"github.com/urnetwork/connect"
	"github.com/urnetwork/connect/protocol"
)

func TestSnapshotSinkReceiveDoesNotRetainBorrowedFrames(t *testing.T) {
	frame := &protocol.Frame{
		MessageType:  protocol.MessageType_TestSimpleMessage,
		MessageBytes: []byte("original"),
	}
	frames := []*protocol.Frame{frame}
	wantSummary := fmt.Sprint(frames)

	snapshot := snapshotSinkReceive(
		connect.SourceId(connect.NewId()),
		frames,
		connect.Peer{ProvideMode: protocol.ProvideMode_Network},
	)

	frame.MessageType = protocol.MessageType_IpIpPacketFromProvider
	frame.MessageBytes = []byte("reused")
	frames[0] = nil

	if snapshot.frameSummary != wantSummary {
		t.Fatalf("frame summary changed after borrowed frame reuse: got %q want %q", snapshot.frameSummary, wantSummary)
	}
}

// A full printer queue drops immediately instead of blocking the shared
// client receive pump.
func TestEnqueueSinkReceiveDropsWhenFull(t *testing.T) {
	receives := make(chan *sinkReceive, 1)
	first := &sinkReceive{frameSummary: "first"}
	second := &sinkReceive{frameSummary: "second"}

	if !enqueueSinkReceive(receives, first) {
		t.Fatal("first receive was not admitted")
	}
	if enqueueSinkReceive(receives, second) {
		t.Fatal("second receive was admitted to a full queue")
	}
	if got := <-receives; got != first {
		t.Fatalf("queued receive = %p, want %p", got, first)
	}
}

// The cli provider derives its family-pinned urls from --connect_url, and the
// extender activation derives its family api urls from --api_url, by the same
// sdk rule: suffix the service label, keep scheme, port and path, and give up
// on anything with no label to suffix.
func TestFamilyServiceUrl(t *testing.T) {
	cases := []struct {
		serviceUrl string
		ipVersion  int
		want       string
	}{
		{"wss://connect.example.com/", 4, "wss://connect-v4.example.com/"},
		{"wss://connect.example.com/", 6, "wss://connect-v6.example.com/"},
		{"wss://g2-connect.example.com/secret", 4, "wss://g2-connect-v4.example.com/secret"},
		{"wss://connect.space.example:8443/", 6, "wss://connect-v6.space.example:8443/"},
		{"https://api.example.com", 4, "https://api-v4.example.com"},
		{"https://beta-api.example.com/secret", 6, "https://beta-api-v6.example.com/secret"},
		{"ws://127.0.0.1:8080/", 4, ""},
		{"wss://[::1]:8080/", 6, ""},
		{"wss://localhost/", 4, ""},
		{"wss://connect-v4.example.com/", 6, ""},
		{"wss://connect.example.com/", 5, ""},
		{"", 4, ""},
		{"not a url", 4, ""},
	}
	for _, c := range cases {
		if got := familyServiceUrl(c.serviceUrl, c.ipVersion); got != c.want {
			t.Errorf("familyServiceUrl(%q, %d) = %q, want %q", c.serviceUrl, c.ipVersion, got, c.want)
		}
	}
}
