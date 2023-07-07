package connect

import (

	"google.golang.org/protobuf/proto"

	"bringyour.com/protocol"
)


/*

sends frames to destinations with properties:
1. as long the sending client is active, frames are eventually delivered up to timeout
2. frames are received in order of send
3. sender is notified when frames are received


*/



// protobuf protocol

// send package to any destination
// package type - ip, something else
// egress worker to edgress a standard ip packet 



// ideal flow:
// auth connect connection
// send to destination (server appends a from user)
// request contract to destination
// 


// on receive, transfer worker to convert ip messages to new user space sockets



// FIRST PROTO
// client has client id
// send message to another client id


// Client(transit)
// receive callback
// send has a transmit buffer, applies ack logic, and resends messages that have not been ackd after some resend time



type SendPack struct {
	// this is repacked by the send buffer into a Pack,
	// with destination and frame from the tframe, and other pack properties filled in by the buffer
	TransferFrame
	// called (true) when the pack is ack'd, or (false) if not ack'd (closed before ack)
	ackCallback func(bool)
}


// destination id for control messages
CONTROL_ID = ULID("")


type Client struct {
	clientId ULID
	
	
	// frame bytes
	SendPacks chan SendPack

	ForwardTransferFrames chan []byte

	// timeout params

	// receive chan []byte

	// the structured reader/writer read byte chunks
	// do it this way to be compatible with framed io like websockets
	readers chan STRUCTUREDREADER
	writers chan STRUCTUREDWRITER

	// receive callback
	local func(*Pack)

	forward func([]byte)
}



// Forward(TransferFrame)

// Send(TransferFrame, destination)

// SendControl(TransferFrame)


func (self *Client) Run(ctx context.Context) error {

	// FIXME create a cancel context and use the cancel context below

	
	sendBuffer := NewSendBuffer(SendFrames)
	receiveBuffer := NewReceiveBuffer(SendFrames)

	cancel = self.ctx.Cancel()
	go sendBuffer.timeout(cancel)
	go receiveBuffer.timeout(cancel)


	// receive
	go func() {
		reader := EMPTY_STRUCTURED_READER
		for {
			select {
			case self.ctx.Done():
				return
			case nextReader, ok <- readers:
				if !ok {
					return
				}
				reader = nextReader
			case frameBytes <- reader:
				frame := PARSE_TRANSFER_FRAME(frameBytes)
				if frame.DestinationId == self.clientID {
					switch frame.MessageType {
					case ACK:
						ack := PARSE(frame.messageBytes)
						
						// FIXME advance the send buffer
						self.sendBuffer.ack(ack)
					case PACK:
						pack := PARSE(frame)
						// receive buffer sends the acks
						self.receiveBuffer.pack(pack, local)
						
					}
				} else {
					forward(frameBytes)
				}
			}
		}
	}()

	// send
	go func() {
		writer = EMPTY_STRUCTURED_WRITER

		write := func(frameBytes []byte) {
			defer func() {
				if recover() {
					// drop this frame
					// the transport closed
					// blocking wait for a new writer
					select {
					case self.ctx.Done():
						return
					case nextWriter, ok <- writers:
						if !ok {
							return
						}
						writer = nextWriter
					}
				}
			}()
			writer <- frameBytes
		}

		for {
			select {
			case self.ctx.Done():
				return
			case nextWriter, ok <- writers:
				if !ok {
					return
				}
				writer = nextWriter
			case frameBytes, ok <- self.ForwardFrames:
				if !ok {
					return
				}
				write(frameBytes)
			case sendPack, ok <- self.SendPacks:

				// FIXME append to the send buffer
				// TODO IMPORTANT sequence id per destination
				// this writes the pack
				// this assigns the sequence id
				// if there is no prior unack'd pack, the sequence id can reset to 0
				self.sendBuffer.pack(sendPack)

				//self.writer <- packBytes
			}
		}
	}()


}


// FIXME
// timeout is strictly increasing with pack
// sendbuffer timeout
// send acks again up to a certain timeout
// if pending timeout elapses, close the ctx


// store a list of values in ack time ascending
// when ack, remove from front and append to end
// min next ack time is front of list
func timeout() {
	for {
		timeout = MIN(min next ack time, FIRST.enterTime + timeout)
		select {
		case time.After(timeout):
			firstPack := FIRST()
			if EXPIRED() {
				cancel(ERROR)
			}
			// send acks for all whose NOW - last ack time >= ack timeout
			RESEND_ALL()
		}
	}
}



