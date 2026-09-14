package connect

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// Feed codec and stream tests (EXTENDER.md D4). The full dial through a real
// extender lives in `extender/extender_feed_client_test.go`, which cannot live
// here because connect root must not import its own subpackage.

// The request and every frame variant round-trip through the length prefix.
func TestExtenderFeedCodecRoundTrips(t *testing.T) {
	var buffer bytes.Buffer
	request := &protocol.ExtenderFeedRequest{
		SampleCount: 16,
		Subscribe:   true,
	}
	if err := WriteExtenderFeedRequest(&buffer, request); err != nil {
		t.Fatal(err)
	}
	readRequest, err := ReadExtenderFeedRequest(&buffer)
	if err != nil {
		t.Fatal(err)
	}
	if readRequest.SampleCount != 16 || !readRequest.Subscribe {
		t.Fatalf("request = %+v, expected the one written", readRequest)
	}

	frames := []*protocol.ExtenderFeedFrame{
		{Frame: &protocol.ExtenderFeedFrame_EndOfSample{EndOfSample: true}},
		{Frame: &protocol.ExtenderFeedFrame_Keepalive{Keepalive: true}},
		{Frame: &protocol.ExtenderFeedFrame_Record{Record: &protocol.ExtenderRecord{
			Body:          []byte("body"),
			RootSignature: []byte("signature"),
			RootKeyId:     []byte("keyid"),
		}}},
		{Frame: &protocol.ExtenderFeedFrame_Revocation{Revocation: &protocol.ExtenderRevocation{
			Body: []byte("body"),
		}}},
	}
	buffer.Reset()
	for _, frame := range frames {
		if err := WriteExtenderFeedFrame(&buffer, frame); err != nil {
			t.Fatal(err)
		}
	}
	for i := range frames {
		frame, err := ReadExtenderFeedFrame(&buffer)
		if err != nil {
			t.Fatal(err)
		}
		switch i {
		case 0:
			if !frame.GetEndOfSample() {
				t.Fatal("the end of sample frame did not round trip")
			}
		case 1:
			if !frame.GetKeepalive() {
				t.Fatal("the keepalive frame did not round trip")
			}
		case 2:
			if string(frame.GetRecord().Body) != "body" {
				t.Fatal("the record frame did not round trip")
			}
		case 3:
			if string(frame.GetRevocation().Body) != "body" {
				t.Fatal("the revocation frame did not round trip")
			}
		}
	}
	if _, err := ReadExtenderFeedFrame(&buffer); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, expected the stream to end", err)
	}
}

// A frame larger than the cap is refused rather than read, because the framing
// is lost after an over-long length (D4).
func TestExtenderFeedCodecRefusesAnOverlongFrame(t *testing.T) {
	lengthBytes := []byte{0xff, 0xff, 0xff, 0xff}
	if _, err := ReadExtenderFeedFrame(bytes.NewReader(lengthBytes)); err == nil {
		t.Fatal("an overlong frame was accepted")
	}
	oversize := &protocol.ExtenderFeedFrame{
		Frame: &protocol.ExtenderFeedFrame_Record{Record: &protocol.ExtenderRecord{
			Body: make([]byte, ExtenderFeedMaxFrameByteCount+1),
		}},
	}
	if err := WriteExtenderFeedFrame(io.Discard, oversize); err == nil {
		t.Fatal("an oversize frame was written")
	}
}

// One in-process feed server over a pipe, speaking the exported codec exactly
// as the phase 5a server will.
func newTestFeedStream(t *testing.T) (*ExtenderFeedStream, net.Conn) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	stream := &ExtenderFeedStream{
		conn:   clientConn,
		reader: bufio.NewReader(clientConn),
	}
	t.Cleanup(func() {
		stream.Close()
		serverConn.Close()
	})
	return stream, serverConn
}

// The sample ends at end_of_sample, a keepalive is surfaced, and a
// subscription keeps delivering (D4).
func TestExtenderFeedStreamReadsTheSampleThenTheSubscription(t *testing.T) {
	stream, serverConn := newTestFeedStream(t)

	serverErrs := make(chan error, 1)
	go func() {
		serverErrs <- func() error {
			frames := []*protocol.ExtenderFeedFrame{
				{Frame: &protocol.ExtenderFeedFrame_Record{Record: &protocol.ExtenderRecord{Body: []byte("one")}}},
				{Frame: &protocol.ExtenderFeedFrame_Record{Record: &protocol.ExtenderRecord{Body: []byte("two")}}},
				{Frame: &protocol.ExtenderFeedFrame_EndOfSample{EndOfSample: true}},
				{Frame: &protocol.ExtenderFeedFrame_Keepalive{Keepalive: true}},
				{Frame: &protocol.ExtenderFeedFrame_Revocation{Revocation: &protocol.ExtenderRevocation{Body: []byte("gone")}}},
			}
			for _, frame := range frames {
				if err := WriteExtenderFeedFrame(serverConn, frame); err != nil {
					return err
				}
			}
			return nil
		}()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sampleBodies := []string{}
	for {
		frame, err := stream.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if frame.GetEndOfSample() {
			break
		}
		if record := frame.GetRecord(); record != nil {
			sampleBodies = append(sampleBodies, string(record.Body))
			continue
		}
		t.Fatalf("unexpected frame before the end of the sample: %+v", frame)
	}
	if len(sampleBodies) != 2 || sampleBodies[0] != "one" || sampleBodies[1] != "two" {
		t.Fatalf("sample = %v, expected both records", sampleBodies)
	}

	frame, err := stream.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !frame.GetKeepalive() {
		t.Fatalf("frame = %+v, expected a keepalive", frame)
	}
	frame, err = stream.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if revocation := frame.GetRevocation(); revocation == nil || string(revocation.Body) != "gone" {
		t.Fatalf("frame = %+v, expected the subscribed revocation", frame)
	}
	if err := <-serverErrs; err != nil {
		t.Fatal(err)
	}
}

// A canceled read ends promptly and reports the cancellation, so a closing
// client is not parked on an idle subscription.
func TestExtenderFeedStreamNextHonorsCancellation(t *testing.T) {
	stream, _ := newTestFeedStream(t)

	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 1)
	go func() {
		_, err := stream.Next(ctx)
		errs <- err
	}()
	cancel()
	select {
	case err := <-errs:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, expected the cancellation", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a canceled read did not end")
	}
}
