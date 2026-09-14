package connect

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/v2026/protocol"
)

// The feed client (EXTENDER.md D4).
//
// The feed is the bounded bootstrap of a directory: a client opens the feed
// service on one extender, asks for up to `SampleCount` active records, and
// reads them until `end_of_sample`. With `Subscribe` set the same stream then
// carries every record and revocation the server applies, with a keepalive on
// an idle stream, until the client closes.
//
// Frames are 4-byte big-endian length-prefixed protobuf of at most 64 KiB in
// both directions. The codec is exported because the feed server of phase 5a
// speaks the same framing from the other side of the stream the extender hands
// it (A8).
//
// An ExtenderFeedStream is owned by one reader. `Next` is not safe to call
// concurrently with itself; `Close` is safe from any goroutine.

// The largest feed frame, request and response alike (D4).
const ExtenderFeedMaxFrameByteCount = 64 * 1024

// Sample size a client asks for by default, and the server cap (D4).
const (
	DefaultExtenderFeedSampleCount = 16
	ExtenderFeedMaxSampleCount     = 32
)

// Writes one length-prefixed protobuf frame.
func writeExtenderFeedMessage(writer io.Writer, message proto.Message) error {
	messageBytes, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	if ExtenderFeedMaxFrameByteCount < len(messageBytes) {
		return fmt.Errorf(
			"extender feed frame is %d bytes, at most %d",
			len(messageBytes),
			ExtenderFeedMaxFrameByteCount,
		)
	}
	frameBytes := make([]byte, 4+len(messageBytes))
	binary.BigEndian.PutUint32(frameBytes[0:4], uint32(len(messageBytes)))
	copy(frameBytes[4:], messageBytes)
	_, err = writer.Write(frameBytes)
	return err
}

// Reads exactly one length-prefixed protobuf frame. A length over the cap is
// an error rather than a truncation, because the stream framing is lost after
// it.
func readExtenderFeedMessage(reader io.Reader, message proto.Message) error {
	lengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(reader, lengthBytes); err != nil {
		return err
	}
	messageByteCount := int(binary.BigEndian.Uint32(lengthBytes))
	if ExtenderFeedMaxFrameByteCount < messageByteCount {
		return fmt.Errorf(
			"extender feed frame is %d bytes, at most %d",
			messageByteCount,
			ExtenderFeedMaxFrameByteCount,
		)
	}
	messageBytes := make([]byte, messageByteCount)
	if _, err := io.ReadFull(reader, messageBytes); err != nil {
		return err
	}
	return proto.Unmarshal(messageBytes, message)
}

// The client's opening frame.
func WriteExtenderFeedRequest(writer io.Writer, request *protocol.ExtenderFeedRequest) error {
	if request == nil {
		return fmt.Errorf("extender feed request is missing")
	}
	return writeExtenderFeedMessage(writer, request)
}

// The server side of the opening frame.
func ReadExtenderFeedRequest(reader io.Reader) (*protocol.ExtenderFeedRequest, error) {
	request := &protocol.ExtenderFeedRequest{}
	if err := readExtenderFeedMessage(reader, request); err != nil {
		return nil, err
	}
	return request, nil
}

// One server frame.
func WriteExtenderFeedFrame(writer io.Writer, frame *protocol.ExtenderFeedFrame) error {
	if frame == nil {
		return fmt.Errorf("extender feed frame is missing")
	}
	return writeExtenderFeedMessage(writer, frame)
}

// One server frame from the client side.
func ReadExtenderFeedFrame(reader io.Reader) (*protocol.ExtenderFeedFrame, error) {
	frame := &protocol.ExtenderFeedFrame{}
	if err := readExtenderFeedMessage(reader, frame); err != nil {
		return nil, err
	}
	return frame, nil
}

// One open feed stream. It owns the carrier underneath, so Close releases the
// whole dial.
type ExtenderFeedStream struct {
	conn      net.Conn
	reader    *bufio.Reader
	response  *protocol.ExtenderResponse
	closeOnce sync.Once
}

// Opens the feed service on one extender and sends the request (A8, D4). The
// caller owns the returned stream.
func DialExtenderFeed(
	ctx context.Context,
	connectSettings *ConnectSettings,
	extenderConfig *ExtenderConfig,
	request *protocol.ExtenderFeedRequest,
) (*ExtenderFeedStream, error) {
	if request == nil {
		return nil, fmt.Errorf("extender feed request is missing")
	}
	conn, response, err := DialExtender(
		ctx,
		connectSettings,
		extenderConfig,
		&ExtenderDial{
			Service: ExtenderServiceFeed,
		},
	)
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			conn.Close()
		}
	}()

	if err := withConnWritePhaseDeadline(ctx, conn, connectSettings.ConnectTimeout, func() error {
		return WriteExtenderFeedRequest(conn, request)
	}); err != nil {
		return nil, err
	}

	success = true
	return &ExtenderFeedStream{
		conn:     conn,
		reader:   bufio.NewReader(conn),
		response: response,
	}, nil
}

// The extender's response to the feed dial, which carries its identity key and
// the carriers it serves (A4).
func (self *ExtenderFeedStream) Response() *protocol.ExtenderResponse {
	return self.response
}

// The next server frame. `end_of_sample` and `keepalive` are returned like any
// other frame, so the caller sees when the sample is complete and when an idle
// subscription is still alive. A ctx deadline or cancellation ends the read.
func (self *ExtenderFeedStream) Next(ctx context.Context) (*protocol.ExtenderFeedFrame, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// cancellation is delivered as an immediate read deadline, which is the
	// only way to interrupt a blocked stream read
	stopCancel := context.AfterFunc(ctx, func() {
		self.conn.SetReadDeadline(time.Now())
	})
	defer stopCancel()
	if deadline, ok := ctx.Deadline(); ok {
		if err := self.conn.SetReadDeadline(deadline); err != nil {
			return nil, err
		}
	} else if err := self.conn.SetReadDeadline(time.Time{}); err != nil {
		return nil, err
	}
	frame, err := ReadExtenderFeedFrame(self.reader)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, err
	}
	return frame, nil
}

func (self *ExtenderFeedStream) Close() error {
	self.closeOnce.Do(func() {
		self.conn.Close()
	})
	return nil
}
