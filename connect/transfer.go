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
4. high throughput and low resource usage


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
	Send chan SendPack

	// transfer frame bytes
	Forward chan []byte

	// timeout params

	// receive chan []byte

	// the structured reader/writer read byte chunks
	// do it this way to be compatible with framed io like websockets
	readers chan chan []byte
	writers chan chan []byte

	// receive callback
	receiveCallback func(*Pack)

	// transfer frame bytes
	forwardCallback func([]byte)


	// allow send/receive without an active contract
	receiveNoContract map[*ULID]bool{CONTROL_ID: true}
	sendNoContract map[*ULID]bool{CONTROL_ID: true}
}


// if return false, that means buffer is full. try again later

// Forward(TransferFrame)(bool)

// Send(TransferFrame, destination)(bool)

// SendControl(TransferFrame)(bool)


func (self *Client) Run(ctx context.Context) error {

	// FIXME create a cancel context and use the cancel context below


	cancelCtx, cancel := context.WithCancelCause(ctx)

	
	sendBuffer := NewSendBuffer(Forward)
	receiveBuffer := NewReceiveBuffer(Forward)

	go sendBuffer.timeout(cancelCtx, cancel)
	go receiveBuffer.timeout(cancelCtx, cancel)


	// receive
	go func() {
		reader := make(chan []byte, 0)
		for {
			select {
			case cancelCtx.Done():
				return
			case nextReader, ok <- readers:
				if !ok {
					return
				}
				reader = nextReader
			case frameBytes <- reader:
				// use a filtered decode approach as described here
				// https://stackoverflow.com/questions/46136140/protobuf-lazy-decoding-of-sub-message
				filteredFrame := PARSE_FILTERED_TRANSFER_FRAME(frameBytes)
				if filteredFrame.DestinationId == self.clientID {
					frame := PARSE_FRAME()
					switch frame.MessageType {
					case ACK:
						ack := PARSE(frame.messageBytes)
						
						// FIXME advance the send buffer
						self.sendBuffer.ack(ack)
					case PACK:
						// receive buffer sends the acks
						self.receiveBuffer.pack(pack, receiveCallback)
						
					}
				} else {
					forwardCallback(frameBytes)
				}
			}
		}
	}()

	// send
	go func() {
		writer = make(chan []byte, 0)

		write := func(frameBytes []byte) {
			defer func() {
				if recover() {
					// the transport closed
					// blocking wait for a new writer
					select {
					case cancelCtx.Done():
						return
					case nextWriter, ok <- writers:
						if !ok {
							return
						}
						writer = nextWriter
						write(frameBytes)
					}
				}
			}()
			writer <- frameBytes
		}

		for {
			select {
			case cancelCtx.Done():
				return
			case nextWriter, ok <- writers:
				if !ok {
					return
				}
				writer = nextWriter
			case frameBytes, ok <- self.ForwardTransferFrames:
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
				self.sendBuffer.pack(sendPack.transferFrame, sendPack.ackCallback)

				//self.writer <- packBytes
			}
		}
	}()


}







// ContractManager
// secret key for client id
// Client
// AddContract determines whether source or dest based on client id









type SendBuffer struct {
	// timeout
	// max size bytes

}

func NewSendBuffer(forward chan []byte) *SendBuffer {

}

func (self *SendBuffer) pack(transferFrame *TransferFrame, ackCallback func(*Ack)) {

	// if full, wait for up to timeout for a free spot

	// if active contract, send now

}

func (self *SendBuffer) ack(ack *Ack) {

}



type SendSequence struct {
	contract *Contract
}

func (self *SendSequence) pack(transferFrame *TransferFrame, ackCallback func(*Ack)) {
	if self.updateContract(len(frameBytes)) {
		SEND(packBytes)
	}
}

func updateContract(bytesCount int) bool {
	if sendNoContract(destinationId) {
		return true
	}
	if contract != nil {
		if contract.update(bytesCount) {
			return true
		}
		contractManager.Close(contract)
	}

	MAX_PACK_BYTE_COUNT := 256
	endTime = time + timeout
	for {
		contractManager.NewContract(destinationId, bytesCount + MAX_PACK_BYTE_COUNT)
		select {
		case nextContract <- contractManager.Contract(destinationId):
			// the contract pack bytes are counted also
			contractPack := PACK(contract)
			if nextContract.update(bytesCount + len(contractPackFrameBytes)) {
				contract = nextContract
				// append the contract to the sequence
				SEND(contractPack)
				return true
			} else {
				// this contract doesn't fit the message
				contractManager.Close(nextContract)
			}
		case After(endTime - time):
		}
		if endTime <= time {
			// no contract in time
			CLOSE_SEQUENCE()
			return false
		}
	}
}


// store a list of values in ack time ascending
// when ack, remove from front and append to end
// min next ack time is front of list
func (self *SendSequence) timeout(ctx context.Context) {
	for {
		firstPack := FIRST()
		if firstPack {
			if FIRST.enterTime + sendTimeout <= time {
				CLOSE_SEQUENCE()
				return
			}
			// resend all whose NOW - last send time >= send timeout
			RESEND_PACKS()
			timeout = MIN(min next send time, FIRST.enterTime + sendTimeout)
			select {
			case cancelCtx.Done:
				return
			case added:
			case time.After(timeout):
			}
		} else {
			select {
			case cancelCtx.Done:
				return
			case added:
			}
		}
	}
}

func CLOSE_SEQUENCE() {
	// count unacked bytes to the contract
	// call the ackCallback(nil)
}








// FIXME attach a contract tracker to the send/receive buffer

type ReceiveBuffer struct {
	// timeout
	// max size bytes

	// receiveAllowNoContract func(*ULID)(bool)
}

func NewReceiveBuffer(forward chan []byte) *ReceiveBuffer {

}


// FIXME whitelist certain source_ids to not require contracts
func (self *ReceiveBuffer) pack(pack *Pack, receiveCallback func(*Pack)) {
	
	seq := activeSequence(pack)
}



type ReceiveSequence struct {

}


func (self *ReceiveSequence) pack(pack *Pack, receiveCallback func(*Pack)) {

	resetSequence := false
	for _, frame := range pack.Frames {
		if frame.MessageType == MessageType.PACK_RESET_SEQUENCE {
			resetSequence = true
			break
		}
	}


	// if full, drop the pack. the sender will retransmit later
	// reject if the source_id of the contract does not match the source_id of the pack



	// look for PACK_RESET_SEQUENCE

	if lastReceivedSeq + 1 == first {
			lastReceivedSeq = first.seq

			// if CreateContractResult, add contract to contract manager

			// register contracts
			for _, frames := range pack.Frames {
				if CONTRACT {
					// this will check the hmac with the local provider secret key
					contractManager.AddContract(contract)
				}
			}
			if updateContract(len(frameBytes)) {
				if CREATE_CONTRACT_RESULT {
					contractManager.Add(Contract)
				}
				RECEIVE()
				ACK()
			}
		} else if pack.seq < lastReceivedSeq {
			// already received
			SEND_ACK()
		}
}


func updateContract(bytesCount int) bool {
	if receiveNoContract(destinationId) {
		return true
	}
	if contract != nil {
		if contract.update(bytesCount) {
			return true
		}
		contractManager.Close(contract)
	}

	endTime = time + timeout
	for {
		select {
		case nextContract <- contractManager.Contract(sourceId):
			if nextContract.update(bytesCount) {
				contract = nextContract
				return true
			} else {
				// this contract doesn't fit the message
				contractManager.Close(nextContract)
			}
		case After(endTime - time):
		}
		if endTime <= time {
			// no contract in time
			CLOSE_SEQUENCE()
			return false
		}
	}
}


func (self *ReceiveSequence) timeout(ctx context.Context) {
	// receive timeout is where we have a pack that cannot be released due to missing earlier sequence

	for {
		firstPack := FIRST()
		if firstPack {
			// sequence gap
			if first.enterTime + receiveTimeout <= time {
				CLOSE_SEQUENCE()
				return
			}
			RESEND_ACKS()

			timeout = MIN(min next ack time, FIRST.enterTime + receiveTimeout)
			select {
			case cancelCtx.Done:
				return
			case added:
			case time.After(timeout):
			}
		} else {
			select {
			case cancelCtx.Done:
				return
			case added:
			}
		}
	}
}

func CLOSE_SEQUENCE() {
	// count unacked bytes to the contract
}



// FIXME
// timeout is strictly increasing with pack
// sendbuffer timeout
// send acks again up to a certain timeout
// if pending timeout elapses, close the ctx


// store a list of values in ack time ascending
// when ack, remove from front and append to end
// min next ack time is front of list
/*
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
*/


