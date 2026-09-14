package connect

import (
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/v2026/protocol"
)

func TestParseFilteredTransferPath(t *testing.T) {
	sourceId := NewId()
	destinationId := NewId()
	streamId := NewId()

	marshal := func(transferFrame *protocol.TransferFrame) []byte {
		b, err := proto.Marshal(transferFrame)
		AssertEqual(t, nil, err)
		return b
	}

	// a full transfer frame with a pack payload: the parser must skip the
	// unknown (unfiltered) fields the same as the protobuf decoder
	fullFrame := func(path TransferPath) []byte {
		return marshal(&protocol.TransferFrame{
			TransferPath: path.ToProtobuf(),
			Pack: &protocol.Pack{
				MessageId:      NewId().Bytes(),
				SequenceId:     NewId().Bytes(),
				SequenceNumber: 1234567,
				Frames: []*protocol.Frame{
					{
						MessageType:  protocol.MessageType_TestSimpleMessage,
						MessageBytes: make([]byte, 1024),
					},
				},
			},
		})
	}

	paths := []TransferPath{
		NewTransferPath(sourceId, destinationId, Id{}),
		NewTransferPath(sourceId, destinationId, streamId),
		SourceId(sourceId),
		DestinationId(destinationId),
		{},
	}
	for _, expectedPath := range paths {
		b := fullFrame(expectedPath)

		path, exists, ok := parseFilteredTransferPath(b)
		AssertEqual(t, true, ok)
		AssertEqual(t, true, exists)
		AssertEqual(t, expectedPath, path)

		// the wrapper must agree with the full unmarshal path
		path, err := FilteredTransferPath(b)
		AssertEqual(t, nil, err)
		AssertEqual(t, expectedPath, path)
	}

	// ack frames have a different payload field
	ackFrame := marshal(&protocol.TransferFrame{
		TransferPath: NewTransferPath(sourceId, destinationId, Id{}).ToProtobuf(),
		Ack: &protocol.Ack{
			MessageId:  NewId().Bytes(),
			SequenceId: NewId().Bytes(),
		},
	})
	path, exists, ok := parseFilteredTransferPath(ackFrame)
	AssertEqual(t, true, ok)
	AssertEqual(t, true, exists)
	AssertEqual(t, NewTransferPath(sourceId, destinationId, Id{}), path)

	// missing transfer path
	noPath := marshal(&protocol.TransferFrame{})
	_, exists, ok = parseFilteredTransferPath(noPath)
	AssertEqual(t, true, ok)
	AssertEqual(t, false, exists)
	_, err := FilteredTransferPath(noPath)
	if err == nil {
		t.Fatal("expected an error for a missing transfer path")
	}

	// malformed inputs must not panic, and the wrapper must return an error
	// (either from the fast-path id check or the full unmarshal fallback)
	malformed := [][]byte{
		{0x0a},             // truncated length-delimited field
		{0x0a, 0xff},       // length past the end
		{0x08, 0x01},       // transfer_path with the wrong wire type
		{0xff, 0xff, 0xff}, // bad tag varint
	}
	for _, b := range malformed {
		_, _, _ = parseFilteredTransferPath(b)
		if _, err := FilteredTransferPath(b); err == nil {
			t.Fatalf("expected an error for malformed input %v", b)
		}
	}

	// an id with the wrong length falls back to the full unmarshal, which
	// surfaces the id error
	badId := marshal(&protocol.TransferFrame{
		TransferPath: &protocol.TransferPath{
			SourceId: []byte{1, 2, 3},
		},
	})
	_, _, ok = parseFilteredTransferPath(badId)
	AssertEqual(t, false, ok)
	if _, err := FilteredTransferPath(badId); err == nil {
		t.Fatal("expected an error for a bad id length")
	}
}
