package connect

import (

	"google.golang.org/protobuf/proto"
	"github.com/oklog/ulid/v2"

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


// Forwarding ACL:
// - reject if source id does not match network id
// - reject if not an active contract between sender and receiver




type AckFunction func(err error)
type ReceiveFunction func(sourceId *ulid.ULID, frames []*protocol.Frame, err error)
type ForwardFunction(sourceId *ulid.ULID, destinationId *ulid.ULID, transferFrameBytes []byte)





// destination id for control messages
ControlId = ULID([]byte{
	0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00,
})


type Client struct {
	ctx context.Context

	// clientId ULID

	// FIXME signing key
	// FIXME control signing key
	
	
	// frame bytes
	sendPacks chan *SendPack

	// transfer frame bytes
	// Forward chan []byte

	// timeout params

	// receive chan []byte

	// the structured reader/writer read byte chunks
	// do it this way to be compatible with framed io like websockets
	// readers chan [](chan []byte)
	// writers chan [](chan []byte)

	// receive callback
	receiveCallback func(*Pack)

	// transfer frame bytes
	forwardCallback func([]byte)


	// contractManager *ContractManager
	// routeManager *RouteManager
}

func (self *Client) SendWithTimeout(frame *protocol.Frame, destinationId *ulid.ULID, ackCallback AckFunction, timeout time.Duration) bool {
	sendPack := &SendPack{
		frame: frame,
		destinationId: destinationId,
		ackCallback: ackCallback,
	}
	if timeout < 0 {
		sendPacks <- sendPack
		return true
	} else if 0 == timeout {
		select {
		case sendPacks <- sendPack:
			return true
		default:
			// full
			if ackCallback != nil {
				ackCallback(errors.New("Send buffer full."))
			}
			return false
		}
	} else {
		select {
		case sendPacks <- sendPack:
			return true
		case time.After(timeout):
			// full
			if ackCallback != nil {
				ackCallback(errors.New("Send buffer full."))
			}
			return false
		}
	}
}

func (self *Client) SendControlWithTimeout(frame *protocol.Frame, ackCallback AckFunction) bool {
	return self.SendNonBlocking(frame, ControlId, ackCallback)
}

func (self *Client) Send(frame *protocol.Frame, destinationId *ulid.ULID, ackCallback AckFunction) bool {
	return SendWithTimeout(frame, destinationId, ackCallback, -1)
}

func (self *Client) SendControl(frame *protocol.Frame, ackCallback AckFunction) bool {
	return self.Send(frame, ControlId, ackCallback)
}

func (self *Client) Run(routeManager *RouteManager, contractManager *ContractManager) {
	defer func() {
		close(self.sendPacks)
		for {
			select {
			case sendPack, ok <- self.sendPacks:
				if !ok {
					return
				}
				if sendPack.ackCallback != nil {
					sendPack.ackCallback(errors.New("Client closed."))
				}
			}
		}
	}()

	sendBuffer := NewSendBuffer(self)
	receiveBuffer := NewReceiveBuffer(self)

	// receive
	go func() {
		routeNotifier := self.routeManager.OpenRouteNotifier(clientId)
		defer routeNotifier.Close()
		multiRouteReader := CreateMultiRouteReader(routeNotifier)

		for {
			transferFrameBytes := multiRouteReader.read(self.ctx, -1)
			// use a filtered decode approach as described here
			// https://stackoverflow.com/questions/46136140/protobuf-lazy-decoding-of-sub-message
			filteredTransferFrame := &protocol.FilteredTransferFrame{}
			if err := protobuf.Decode(transferFrameBytes, &filteredTransferFrame); err != nil {
				// bad protobuf
				continue
			}
			sourceId := UlidFromProto(filteredTransferFrame.GetSourceId())
			if sourceId == nil {
				// bad protobuf
				continue
			}
			destinationId := UlidFromProto(filteredTransferFrame.GetDestinationId())
			if destinationId == nil {
				// bad protobuf
				continue
			}
			if destinationId.Equals(self.clientId) {
				transferFrame := &TransferFrame{}
				if err := protobuf.Decode(transferFrameBytes, &transferFrame); err != nil {
					// bad protobuf
					continue
				}
				frame = transferFrame.getFrame()
				if !self.routeManager.Verify(sourceId, frame.GetMessageBytes(), transferFrame.GetMessageHmac()) {
					// bad signature
					continue
				}
				switch frame.GetMessageType() {
				case ACK:
					ack := &Ack{}
					if err := protobuf.Decode(frame.GetMessageBytes(), &ack); err != nil {
						// bad protobuf
						continue
					}
					messageId := UlidFromProto(ack.GetMessageId())
					if messageId == nil {
						// bad protobuf
						continue
					}
					sendBuffer.ack(messageId)
				case PACK:
					pack := &Pack{}
					if err := protobuf.Decode(frame.GetMessageBytes(), &pack); err != nil {
						// bad protobuf
						continue
					}
					receiveBuffer.pack(ReceivePack(sourceId, destinationId, pack, self.receiveCallback))
				}
			} else {
				if self.forwardCallback != nil {
					self.forwardCallback(sourceId, destinationId, transferFrameBytes)
				}
			}
		}
	}()

	// send
	for {
		select {
		case self.ctx.Done():
			return
		case sendPack <- self.SendPacks:
			self.sendBuffer.pack(sendPack)
		}
	}
}




type SendPack struct {
	// frame and destination is repacked by the send buffer into a Pack,
	// with destination and frame from the tframe, and other pack properties filled in by the buffer
	frame *protocol.Frame
	destinationId *ulid.ULID
	// called (true) when the pack is ack'd, or (false) if not ack'd (closed before ack)
	ackCallback AckFunction
}


type SendBuffer struct {
	ctx *context.Context
	contractManager *ContractManager
	routeManager *routeManager

	contractTimeout time.Duration
	resendInterval time.Duration
	ackTimeout time.Duration

	mutex sync.Mutex
	// destination id -> send sequence
	sendSequences map[*ulid.ULID]*SendSequence
}

// resend timeout is the initial time between successive send attempts. Does linear backoff
// on ack timeout, no longer attempt to retransmit and notify of ack failure
func NewSendBuffer(context, routeManager, contractManager, resendTimeout time.Duraton, ackTimeout time.Duration) *SendBuffer {
	return &SendBuffer{
		client: client,
		resendTimeout: resendTimeout,
		ackTimeout: ackTimeout,
	}
}

func (self *SendBuffer) pack(sendPack *SendPack) {
	self.mutex.Lock()
	defer self.mutex.Unlock()


	initSendSequence := func()(*SendSequence) {
		sendSequence, ok := sendSequences[sendPack.destinationId]
		if ok {
			return sendSequence
		}
		sendSequence = NewSendSequence(ctx, routeManager, contractManager, destinationId)
		sendSequences[sendPack.destinationId] = sendSequence
		go func() {
			sendSequence.run(multiRouteWriter, contractNotifier)

			self.mutex.Lock()
			defer self.mutex.Unlock()
			if sendSequence == sendSequences[sendPack.destinationId] {
				delete(sendSequences, sendPack.destinationId)
			}
		}
		return sendSequence
	}

	func() {
		defer func() {
			if recover() {
				// send sequence was closed
				initSendSequence().messages <- sendPack
			}
		}()
		initSendSequence().messages <- sendPack
	}
}

func (self *SendBuffer) ack(ack *Ack) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	sendSequence, ok := sendSequences[sendPack.destinationId]
	if !ok {
		return
	}

	func() {
		defer func() {
			if recover() {
				// send sequence was closed
				return
			}
		}()
		sendSequence.messages <- ack
	}
}

func (self *SendBuffer) Close() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	// close all open sequences
	for _, sendSequence := range self.sendSequences {
		sendSequence.Close()
	}
}







type SendSequence struct {
	ctx
	routeManager
	contractManager
	destinationId

	contractTimeout time.Duration
	resendInterval time.Duration
	ackTimeout time.Duration
	idleTimeout time.Duration


	contract *Contract
	// routes []Route

	messages chan any


		// heap ordered by `resendTime` ascending
	pq []*SendItem

	sendItems map[*ulid.ULID]*SendItem
	// in order of `sequenceId`
	sendItemsSequence []*SendItem

	nextSequenceId uint64

	// contract id -> message id -> present
	messagesByContract map[*ulid.ULID]map[*ulid.ULID]bool
}

func NewSendSequence(ctx, routeManager, contractManager, destinationId) {



	heap.Init(&pq)
}

add := func(sendItem *SendItem) {
		sendItems[sendItem.messageId] = sendItem
		heap.Push(pq, sendItem)
}
remove := func(messageId *ulid.ULID) *SendItem {
	// remove from map
	sendItem, ok := sendItems[messageId]
	if !ok {
		return nil
	}
	delete(sendItems, messageId)
	if i, found := sort.Find(pq, sendItem.resendTime); found {
		for ; i < len(pg) && pg[i].resendTime == sendItem.resendTime; i += i {
			if pg[i].messageId.Equals(messageId) {
				heap.Remove(pg, i)
				return sendItem
			}
		} 
	}
	panic("Message was not found in the heap.")
}
send := func(sendPack sendPack) {
	// convert to SendItem
	// add to buffer data structs
	// write to multi route
}

func (self *SendSequence) run() {
	// self.contractNotifier = self.contractManager.CreateNotifier(clientId, destinationId)
	// defer self.contractNotifier.Close()

	


	


	







	// buffer messageId -> sendPack
	// buffer - heap by next resend time

	defer func() {
		close(messages)

		// close contract
		for _, contractItem := range self.contractItems {
			contractManager.Complete(contractItem.contract)
		}

		// drain the buffer
		for _, sendItem := range pq {
			if sendItem.ackCallback != nil {
				send.ackCallback(nil)
			}
		}

		// drain the channel
		for {
			select {
			case message, ok <- message:
				if !ok {
					return
				}
				switch v := message.(type) {
				case SendPack:
					v.ackCallback(nil)
				}
			default:
				return
			}
		}
	}()


	routeNotifier := self.routeManager.OpenRouteNotifier(sendPack.destinationId)
	defer routeNotifier.Close()
	multiRouteWriter := CreateMultiRouterWriter(routeNotifier)
	contractNotifier := self.contractManager.OpenContractNotifier(self.routeManager.ClientId, sendPack.destinationId)
	defer contractNotifier.Close()




	





	// init channel with one SendPack, PACK_RESET_SEQUENCE
	send(PACK_RESET_SEQUENCE)


	

	for {

		// resend all applicable

		// compute timeout

		// if idle timeout, return


		select {
		case message, ok <- self.messages:
			if !ok {
				return
			}
			switch v := message.(type) {
			case SendPack:
				if self.updateContract(len(frameBytes)) {
					// this applies the write back pressure
					send(sendPack)
				} else {
					// no contract
					ackCallback(nil)
					return
				}
			case Ack:
				if sendItem := remove(messageId); sendItem != nil {

					// implicitly ack all earlier items in the sequence
					j := len(sendItemsSequence)
					for i, implicitSendItem := range self.sendItemsSequence {
						if sendItem.sequenceId < implicitSendItem.sequenceId {
							j = i
							break
						}
						remove(implicitSendItem.messageId)
						implicitSendItem.ackCallback(true)
						if contractItem, ok := contractItems[implicitSendItem.contractId]; ok {
							delete(contractItem.messages, messageId)
							if 0 == len(contractItem.messages) {
								contractManager.Complete(contractItem.contract)
							}
						}
					}
					self.sendItemsSequence = self.sendItemsSequence[j:]
				}
			}

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


struct SendItem {
	messageId *ulid.ULID
	contractId *ulid.ULID
	sequenceId uint64
	enterTime uint64
	resendTime uint64
	transferFrameBytes []byte
	ackCallback AckFunction
}





type ReceivePack struct {
	sourceId *ulid.ULID
	destinationId *ulid.ULID
	pack *protocol.Pack
	receiveCallback ReceiveFunction
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




type RouteManager struct {
	clientId *ULID

}

func CreateRouteNotifier(destinationId *ULID) *RouteNotifier {

}

func (self *RouteNotifier) Routes() chan []Route {

}

// fixme sign bytes

// fixme verify bytes



type MultiRouteWriter struct {
	ctx context.Context
	routeNotifier
	routes []Route
}


func (self *MultiRouteWriter) write(frameBytes []byte, timeout time.Duration) {
	multiRouteWriters = make(chan []byte, 0)

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


type MultiRouteReader struct {
	ctx context.Context
	routeNotifier
	routes []Route
}

// blocking read
func (self *MultiRouteReader) read(timeout time.Duration) []byte {

		// FIXME see select from multi using 
		// FIXME https://stackoverflow.com/questions/19992334/how-to-listen-to-n-channels-dynamic-select-statement
		// FIXME record stats for each route
}








type Route struct {
	// write/read transfer frame bytes
	Frames chan []byte
}

func (self *Route) stats(send int, drop int, receive int) {

}





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


	// control signer key
	// client signer key

}


func (self *ContractManager) CreateContractNotifier(sourceId *ULID, destinationId *ULID) *ContractNotifier {

}

func (self *ContractNotifier) Contract() chan *Contract {
	if clientId == destinationId {
		// return source
	} else {
		// return destination
	}
}

func (self *ContractNotifier) Close() {
	
}

func (self *ContractNotifier) NewContract() {
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





