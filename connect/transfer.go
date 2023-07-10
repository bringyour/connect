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
4. senders are verified with pre-exchanged keys
5. sender and receiver account for mutual transfer with a shared contract
6. high throughput and low resource usage


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



// ACL:
// - reject if source id does not match network id
// - reject if not an active contract between sender and receiver




type SendPack struct {
	// this is repacked by the send buffer into a Pack,
	// with destination and frame from the tframe, and other pack properties filled in by the buffer
	TransferFrame
	// called (true) when the pack is ack'd, or (false) if not ack'd (closed before ack)
	ackCallback func(err error)
}

type ReceivePack struct {
	// this is repacked by the send buffer into a Pack,
	// with destination and frame from the tframe, and other pack properties filled in by the buffer
	TransferFrame

	receiveCallback func(sourceId *ULID, frames []Frame, err error)
}



// destination id for control messages
ControlId = ULID([]byte{
	0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00,
})


type Client struct {
	clientId ULID

	// FIXME signing key
	// FIXME control signing key
	
	
	// frame bytes
	Send chan SendPack

	// transfer frame bytes
	Forward chan []byte

	// timeout params

	// receive chan []byte

	// the structured reader/writer read byte chunks
	// do it this way to be compatible with framed io like websockets
	readers chan [](chan []byte)
	writers chan [](chan []byte)

	// receive callback
	receiveCallback func(*Pack)

	// transfer frame bytes
	forwardCallback func([]byte)


	contractManager := NewContractManager(self)
}


// if return false, that means buffer is full. try again later

// Forward(TransferFrame)(bool)

// Send(Frame, destination)(bool)

// SendControl(Frame)(bool)


func (self *Client) Run(ctx context.Context) error {

	// FIXME create a cancel context and use the cancel context below


	// cancelCtx, cancel := context.WithCancelCause(ctx)

	
	
	

	// go sendBuffer.timeout(cancelCtx, cancel)
	// go receiveBuffer.timeout(cancelCtx, cancel)

	write := func(messageBytes []byte) {
		// ignore rejects
		Forward <- messageBytes
	}


	



	// receive
	go func() {
		receiveBuffer := NewReceiveBuffer(self)

		// FIXME see select from multi using 
		// FIXME https://stackoverflow.com/questions/19992334/how-to-listen-to-n-channels-dynamic-select-statement
		// FIXME record stats for each route
		multiRouteReaders := make(chan []byte, 0)
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
					if signer.Verify(frame.sourceId, frame.messageBytes) {
						transferFrame := PARSE_FRAME()
						switch frame.MessageType {
						case ACK:
							ack := PARSE(frame.messageBytes)
							
							// FIXME advance the send buffer
							self.sendBuffer.ack(transferFrame)
						case PACK:
							// receive buffer sends the acks
							self.receiveBuffer.pack(ReceivePack(transferFrame, receiveCallback))
						}
					}
				} else {
					forwardCallback(frameBytes)
				}
			}
		}
	}()

	// send
	go func() {
		multiRouteWriters = make(chan []byte, 0)

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
					case After(timeout):
						return
					}
				}
			}()
			// if the channel is closed, the defer recovery will retry with the next channel
			// FIXME write to each of the routes

			// non-blocking write to the channels
			// https://stackoverflow.com/questions/48953236/how-to-implement-non-blocking-write-to-an-unbuffered-channel

			// FIXME submit to all routes


			// FIXME
			// blocking submit to first available
			// we do this to limit the sender to the actual sending rate of the fastest channel
			// https://stackoverflow.com/questions/19992334/how-to-listen-to-n-channels-dynamic-select-statement
			firstRoute := nil
			// order the routes by the best stats in the last N time
			// so that the best stats one will be chosen first if available
			// TODO no need to order, the best will be chosen in the non-blocking below if it is available 


			// then non-blocking submit to the rest of the routes
			for _, route := range routes {
				if firstRoute == route {
					continue
				}
				select {
					case route.writer <- frameBytes:
						route.stats(1, 0, 0)
					default:
						// the channel is full
						route.stats(0, 1, 0)
				}
			}

		}

		sendBuffer := NewSendBuffer(self)

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
				self.sendBuffer.pack(sendPack)

				//self.writer <- packBytes
			}
		}
	}()


}




// FrameSigner
// verify(source_id, frame)
// sign(frameBytes)


// ContractManager
// secret key for client id
// Client
// AddContract determines whether source or dest based on client id





// FIXME create contract error callback
// FIXME allow ui to pop something that says upgrade plan
type ContractManager struct {
	mutex sync.Mutex

	clientId *ULID
	standardTransferBytes int

	// FIXME write

	providerSecretKey []byte

	// source id -> contract id -> contract
	sourceContracts map[*ULID][]Contract
	// source id -> contract id -> contract
	destinationContracts map[*ULID][]Contract

	sourceChannels map[*ULID]chan *Contract
	destinationChannels map[*ULID]chan *Contract


	// allow send/receive without an active contract
	receiveNoContractClientIds map[*ULID]bool{CONTROL_ID: true}
	sendNoContractClientIds map[*ULID]bool{CONTROL_ID: true}

}

func AddContract(contract *Contract) {
	mutex.Lock()
	defer mutex.Unlock()

	if clientId == contract.destinationId {
		// verify the signature
			
		// add to source
		// add to source channel


	} else {
		// add to dest
		// add to dest channel
	}
}

func Contract(sourceId *ULID, destinationId *ULID) chan *Contract {
	if clientId == destinationId {
		// return source
	} else {
		// return destination
	}
}


func CreateContract(destionationId *ULID) {
	// look at destinationContracts and last contract to get previous contract id
	createContract := CreateContract{
			destionationId: destinationId,
			transferByteCount: standardTransferByteCount,
			previosContractId: previosContractId,
		}
		SEND(createContract)
}


func Close(contract *Contract) {
	// mark the contract as closed. it will wait for all acked
}

func Complete(contract *Complete) {
	// send the CloseContract
}


type Contract struct {
	
	contractId *ULID
	transferByteCount uint64

	unackedBytes uint64
	ackedBytes uint64

}
func (self *Contract) updateContract(byteCount int) {
	// add to the acked bytes

}
func (self *Contract) updateContractUnacked(byteCount int) {
	// remove from acked bytes, add to unacked bytes
}











type SendBuffer struct {
	// timeout
	// max size bytes

}

func NewSendBuffer(forward chan []byte) *SendBuffer {

}

func (self *SendBuffer) pack(sendPack *SendPack) {

	// if full, wait for up to timeout for a free spot

	// if active contract, send now


	// if no active sequence, create
	// go seq.pack()
	// seq <- sendPack

}

func (self *SendBuffer) ack(ack *Ack) {
	// FIXME updateContract on ack
}



type SendSequence struct {
	contract *Contract

	channel
	// init channel with one SendPack, PACK_RESET_SEQUENCE
}

func (self *SendSequence) run() {
	for {
		select {
		case message <- channel:
			switch v := message.(type) {
			case SendPack:
				if self.updateContract(len(frameBytes)) {
					SEND(packBytes, contract)
				} else {
					// no contract
					ackCallback(nil)
					close(channel)
					for {
						select {
						case sendPack <- channel:
							sendPack.ackCallback(nil)
						default:
							return
						}
					}
				}
			case Ack:
				// find the index of the message in sequence
				// ack callback (implicitly) all messages before it and the message

				delete(contractMessages, messageId)
				if 0 == len(contractMessages) {
					contractManager.Complete(contract)
				}
			}

		}

		// FIXME on read timeout, Complete all pending contracts, call ack callbacks with nil
		contract.updateContractUnacked(messageByteCount)
		delete(contractMessages, messageId)
		if 0 == len(contractMessages) {
			contractManager.Complete(contract)
		}
		
	}
	/*
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
	*/
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

	// the max overhead of the pack frame
	// this is needed because the size of the contract pack is counted against the contract
	packByteOverhead := 256
	endTime = time + timeout
	for {
		newContract := func(destinationId) {
			// var previousContractId
			// if contract != nil {
			// 	previosContractId = contract.contractId
			// }
			conctractManager.CreateContract(destinationId)
		}

		next := func(nextContract *Contract)(bool) {
			// the contract pack bytes are counted also
			contractPack := PACK(contract)
			if nextContract.update(bytesCount + len(contractPackFrameBytes)) {
				contract = nextContract
				// append the contract to the sequence
				SEND(contractPack, contract)
				// queue up the next contract
				newContract(destinationId)
				return true
			} else {
				// this contract doesn't fit the message
				contractManager.Close(nextContract)
				return false
			}
		}

		select {
		case nextContract <- contractManager.Contract(sourceId, destinationId):
			if next(nextContract) {
				return true
			}
		default:
		}

		if contractManager.standardTransferByteCount < bytesCount + packByteOverhead {
			// this pack does not fit into a standard contract
			// TODO allow requesting larger contracts
			return false
		}

		newContract(destinationId)

		select {
		case nextContract <- contractManager.Contract(source, destinationId):
			if next(nextContract) {
				return true
			}
		case After(endTime - time):
		}

		if endTime <= time {
			return false
		}
	}
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


// FIXME channel of ReceiveFrames
func (self *ReceiveSequence) pack() {
	for {
		select {
		case receiveFrame <- channel:
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
					if CREATE_CONTRACT_RESULT { // TODO how to verify this is from the control id?
						contractManager.AddContract(Contract)
					}
					RECEIVE()
					ACK()
				} else {
					// no contract
					receiveCallback(sourceId, message, false)
					// keep the sequence open for new packs with contracts

					// close(channel)
					// for {
					// 	select {
					// 	case receivePack <- channel:
					// 		receivePack.receiveCallback(nil)
					// 	default:
					// 		return
					// 	}
					// }
				}
			} else if pack.seq < lastReceivedSeq {
				// already received
				SEND_ACK()
			} else {
				// update the future buffer
			}
		}
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
		contractManager.Complete(contract)
	}

	endTime = time + timeout
	for {
		select {
		case nextContract <- contractManager.Contract(sourceId, destinationId):
			if nextContract.update(bytesCount) {
				contract = nextContract
				return true
			} else {
				// this contract doesn't fit the message
				contractManager.Close(nextContract)
				contractManager.Complete(contract)
			}
		default:
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


