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
	clientId *ULID

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

	// allow multiple receive callbacks
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
				// if !self.routeManager.Verify(sourceId, frame.GetMessageBytes(), transferFrame.GetMessageHmac()) {
				// 	// bad signature
				// 	continue
				// }
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
	idleTimeout time.Duration

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
			// clean up
			if sendSequence == sendSequences[sendPack.destinationId] {
				delete(sendSequences, sendPack.destinationId)
			}
		}
		return sendSequence
	}

	func() {
		defer func() {
			if err := recover(); err != nil {
				// send sequence was closed
				delete(sendSequences, sendPack.destinationId)
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
			if err := recover(); err != nil {
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





type SendItem struct {
	messageId *ulid.ULID
	contractId *ulid.ULID
	sequenceId uint64
	enterTime uint64
	resendTime uint64
	sendCount int
	transferFrameBytes []byte
	ackCallback AckFunction
}


type ResendQueue struct {

	pq []*SendItem

	sendItems map[*ulid.ULID]*SendItem
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

	// remove from contract groups and close contract

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




type SendSequence struct {
	ctx
	routeManager
	contractManager
	destinationId

	contractTimeout time.Duration
	resendInterval time.Duration
	ackTimeout time.Duration
	idleTimeout time.Duration


	contract *SendContract
	openContracts map[*ulid.ULID]*SendContract

	// routes []Route

	messages chan any


		// heap ordered by `resendTime` ascending
		// fixme priority queue type
	// pq []*SendItem

	resendQueue *ResendQueue

	// sendItems map[*ulid.ULID]*SendItem
	// in order of `sequenceId`
	sendItemsSequence []*SendItem

	nextSequenceId uint64

	// contract id -> message id -> present
	messagesByContract map[*ulid.ULID]map[*ulid.ULID]bool
}

func NewSendSequence(ctx, routeManager, contractManager, destinationId) {



	heap.Init(&pq)
}

send := func(multiRouteWriter, sendPack sendPack) {
	enterTime := time.Now()

	messageId := ulid.New()
	sequenceId := nextSequenceId
	nextSequenceId += 1
	contactId := contract.contractId

	pack := protocol.Pack{
		messageId: messageId,
		sequenceId: sequenceId,
		frames: []*Frame{sendPack.frame}
	}

	packBytes := ENCODE(pack)
	// packHmac := routeManager.sign(packBytes)

	transferFrame := protocol.TransferFrame{
		destinationId: destinationId,
		sourceId: routeManager.ClientId,
		messageHmac: packHmac,
		frame: Frame{
			MessageType: Pack,
			messageBytes: packBytes,
		}
	}

	transferFrameBytes := ENCODE(transferFrame)

	sendItem := SendItem {
		messageId: messageId,
		contractId: contractId,
		sequenceId: sequenceId,
		enterTime: enterTime,
		sendTime: enterTime,
		resendTime: enterTime + resendInterval,
		sendCount: 1,
		transferFrameBytes: transferFrameBytes,
		ackCallback: ackCallback,
	}
	resendQueue.add(sendItem)

	multiRouteWriter.write(transferFrameBytes)
}


func (self *SendSequence) run() {
	defer func() {
		self.Close()

		// close contract
		for _, sendContract := range self.openContracts {
			contractManager.Complete(sendContract.contractId, sendContract.ackedByteCount, sendContract.unackedByteCount)
		}

		// drain the buffer
		for _, sendItem := range pq {
			if sendItem.ackCallback != nil {
				send.ackCallback(nil)
			}
		}

		// drain the channel
		func() {
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
	}()


	routeNotifier := self.routeManager.OpenRouteNotifier(sendPack.destinationId)
	defer routeNotifier.Close()
	multiRouteWriter := CreateMultiRouterWriter(routeNotifier)
	contractNotifier := self.contractManager.OpenContractNotifier(self.routeManager.ClientId, sendPack.destinationId)
	defer contractNotifier.Close()


	self.nextSequenceId = 0
	// init channel with PACK_RESET_SEQUENCE to reset the sequenceId
	send(Frame{
		messageType: PACK_RESET_SEQUENCE
	})

	
	for {
		sendTime := time.Now()

		timeout := idleTimeout

		for 0 < resendQueue.Len() {
			sendItem := resendQueue.Pop()

			itemAckTimeout := sendItem.sendTime + ackTimeout - sendTime

			if itemAckTimeout <= 0 {
				// message took too long to ack
				// close the sequence
				return
			}

			if sendTime < sendItem.resendTime {
				resendQueue.Push(sendItem)
				itemResendTimeout := sendItem.resendTime - sendTime
				if itemResendTimeout < timeout {
					timeout = itemResendTimeout
				}
				if itemAckTimeout < timeout {
					timeout = itemAckTimeout
				}
				break
			}

			// resend
			multiRouteWriter.write(sendItem.transferFrameBytes)
			sendContracts[sendItem.contractId].updateResend(len(sendItem.transferFrameBytes))
			sendItem.sendCount += 1
			sendItem.sendTime = sendTime
			// linear backoff
			itemResendTimeout := sendItem.sendCount * resendInterval
			if itemResendTimeout < itemAckTimeout {
				sendItem.resendTime = sendTime + itemResendTimeout
			} else {
				sendItem.resendTime = sendTime + itemAckTimeout
			}
			resendQueue.Push(sendItem)
		}

		select {
		case ctx.Done():
			return
		case message, ok <- self.messages:
			if !ok {
				return
			}
			switch v := message.(type) {
			case SendPack:
				if !self.destinationId.Equals(sendPack.destinationId) {
					panic("In wrong sequence.")
				}
				if self.updateContract(contractNotifier, len(frameBytes)) {
					// this applies the write back pressure
					send(sendPack)
				} else {
					// no contract
					// close the sequence
					sendPack.ackCallback(nil)
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
						resendQueue.remove(implicitSendItem.messageId)
						implicitSendItem.ackCallback(true)

						sendContract := sendContracts[implicitSendItem.contractId]
						sendContract.ack(len(implicitSendItem.transferFrameBytes))
						if sendContract.unackedByteCount == 0 {
							contractManager.Complete(sendContract.contractId, sendContract.ackedByteCount, sendContract.unackedByteCount)
						}
					}
					self.sendItemsSequence = self.sendItemsSequence[j:]
				}
			}
		case time.After(timeout):
			if 0 == resendQueue.Len() {
				// idle timeout
				// close the sequence
				return
			}
		}
	}
}

func (self *SendSequence) updateContract(contractNotifier, frameBytesCount int) bool {
	// `sendNoContract` is a mutual configuration 
	// where both sides configure themselves to require no contract from each other
	if contractManager.sendNoContract(destinationId) {
		return true
	}
	if contract != nil && contract.update(frameBytesCount) {
		return true
	}
	// else new contract

	// the max overhead of the pack frame
	// this is needed because the size of the contract pack is counted against the contract
	maxContractFrameBytesCount := 256
	for {
		next := func(storedContract *protocol.StoredContract)(bool) {
			sendContract := NewSendContract(storedContract)

			// the contract pack bytes are counted also
			contractFrameBytes := PACK(storedContract)
			if maxContractFrameBytesCount < len(contractFrameBytes) {
				panic("Bad estimate for contract max size could result in infinite contract retries.")
			}

			if sendContract.update(frameBytesCount + len(contractFrameBytes)) {
				
				contract = sendContract
				sendContracts[sendContract.contractId] = sendContract

				// append the contract to the sequence
				send(SendPack{
					destinationId: destinationId,
					frame: Frame{
						MessageType: PACK_CONTRACT,
						MessageBytes: contractFrameBytes,
					}
					ackCallback: nil,
				})

				// async queue up the next contract
				conctractManager.CreateContract(destinationId)

				return true
			} else {
				// this contract doesn't fit the message
				// just close it since it was never send to the other side
				contractManager.Complete(sendContract.contractId, 0, 0)
				return false
			}
		}

		select {
		case nextContract <- contractNotifier.Contract():
			if next(nextContract) {
				return true
			}
		default:
		}

		if contractManager.standardTransferByteCount < bytesCount + maxContractFrameBytesCount {
			// this pack does not fit into a standard contract
			// TODO allow requesting larger contracts
			return false
		}

		conctractManager.CreateContract(destinationId)

		select {
		case ctx.Done():
			return false
		case nextContract <- contractNotifier.Contract():
			if next(nextContract) {
				return true
			}
		case After(contractTimeout):
			return false
		}
	}
}

func (self *SendSequence) Close() {
	defer func() {
		if err := recover(); err != nil {
			// already closed
			return
		}
	}()
	close(self.messages)
}







type ReceivePack struct {
	sourceId *ulid.ULID
	pack *protocol.Pack
	receiveCallback ReceiveFunction
}

type ReceiveBuffer struct {
	ctx *context.Context
	contractManager *ContractManager
	routeManager *routeManager

	ackInterval time.Duration
	gapTimeout time.Duration
	idleTimeout time.Duration

	mutex sync.Mutex
	// source id -> receive sequence
	receiveSequences map[*ulid.ULID]*ReceiveSequence
}

func NewReceiveBuffer() *ReceiveBuffer {

}


func (self *ReceiveBuffer) pack(receivePack *ReceivePack) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	// force a new sequence on PACK_RESET_SEQUENCE
	for _, frame := range receivePack.Frames {
		if frame.MessageType == MessageType.PACK_RESET_SEQUENCE {
			if receiveSequence, ok := receiveSequences[receivePack.sourceId]; ok {
				receiveSequence.Close()
				delete(receiveSequences, receivePack.sourceId)
			}
			break
		}
	}

	initReceiveSequence := func()(*ReceiveSequence) {
		receiveSequence, ok := receiveSequences[receivePack.sourceId]
		if ok {
			return receiveSequence
		}
		receiveSequence = NewReceiveSequence(ctx, routeManager, contractManager, sourceId)
		receiveSequences[receivePack.sourceId] = receiveSequence
		go func() {
			receiveSequence.run(multiRouteWriter, contractNotifier)

			self.mutex.Lock()
			defer self.mutex.Unlock()
			// clean up
			if receiveSequence == receiveSequences[receivePack.sourceId] {
				delete(receiveSequences, receivePack.sourceId)
			}
		}
		return receiveSequence
	}

	func() {
		defer func() {
			if err := recover(); err != nil {
				// receive sequence was closed
				delete(receiveSequences, receivePack.sourceId)
				initReceiveSequence().messages <- receivePack
			}
		}()
		initReceiveSequence().messages <- receivePack
	}
}

func (self *ReceiveBuffer) Close() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	// close all open sequences
	for _, receiveSequence := range self.receiveSequences {
		receiveSequence.Close()
	}
}




type ReceiveSequence struct {

	receiveQueue *ReceiveQueue

}

func (self *ReceiveSequence) ack(messageId *ulid.ULID) {

	

}


// FIXME channel of ReceiveFrames
func (self *ReceiveSequence) run() {

	defer func() {
		self.Close()

		// close contract
		if receiveContract != nil {
			contractManager.Complete(receiveContract.contractId, receiveContract.ackedByteCount, receiveContract.unackedByteCount)
		}

		// drain the buffer
		for _, receiveItem := range pq {
			if sendItem.ackCallback != nil {
				send.ackCallback(nil)
			}
			peerAudit.updateDiscardedBytes(len(frameBytes))
		}

		// drain the channel
		func() {
			for {
				select {
				case message, ok <- message:
					if !ok {
						return
					}
					switch v := message.(type) {
					case SendPack:
						v.ackCallback(nil)
						peerAudit.updateDiscardedBytes(len(frameBytes))
					}
				default:
					return
				}
			}
		}()

		// FIXME submit peer audit
		routeManager.CompletePeerAudit(peerAudit)
		// FIXME if peer audit has any bad marks, downgrade the connection
		if BAD_PEER_AUDIT {
			// this will remove any direct connections between the two peers
			// forces the platform to reauth any fast track connections
			routeManager.DowngradeConnection(sourceId)
		}
	}()


	routeNotifier := self.routeManager.OpenRouteNotifier(sendPack.destinationId)
	defer routeNotifier.Close()
	multiRouteWriter := CreateMultiRouterWriter(routeNotifier)


	self.nextSequenceId = 0


	for {
		receiveTime = time.Now()
		timeout = idleTimeout
			
		for {
			receiveItem := receiveQueue.Pop()

			itemGapTimeout = item.receiveTime + gapTimeout - receiveTime
			if itemGapTimeout < 0 {
				// did not receive a preceding message in time
				// close sequence
				return
			}

			if receiveItem.sequenceId != nextSequenceId {
				if itemGapTimeout < timeout {
					timeout = itemGapTimeout
				}
				break
			}

			// head of sequence

			nextSequenceId += 1

			// register contracts
			for _, frames := range pack.Frames {
				if CONTRACT {
					// close out the previous contract
					if receiveContract != nil {
						contractManager.Complete(receiveContract, receiveContract.ackedByteCount, receiveContract.unackedByteCount)
						contract = nil
					}

					// this will check the hmac with the local provider secret key
					if !contractManager.Verify(contact) {
						// bad contract
						// close sequence
						peerAudit.updateBadContract(1)
						return
					}

					receiveContract = NewReceiveContract(contract)
				}
			}
			if updateContract(len(frameBytes)) {
				receiveCallback(true)
				ack(multiRouteWriter, messageId)
			} else {
				// no contract
				// close the sequence
				receiveCallback(false)
				peerAudit.updateDiscardedBytes(len(receiveItem.frameBytes))
				return
			}
		}


		select {
		case message <- messages:
			switch v := message.(type) {
			case ReceivePack:
				if nextSequenceId <= pack.seq {
					// replace with the latest value
					if receiveItem := receiveQueue.Remove(sequenceId); receiveItem != nil {
						peerAudit.updateDiscardedBytes(len(receiveItem.frameBytes))
					}

					// store only up to a max size in the receive queue
					if receiveQueue.ByteSize() + len(frameBytes) < receiveQueueMaxByteCount {
						// add to the receiveQueue
						receiveQueue.Push(receivePack)
					} else {
						// drop the message
						peerAudit.updateDiscardedBytes(len(frameBytes))
					}
				} else {
					// already received
					peerAudit.updateResendBytes(len(frameBytes))
					// check the abuse limits
					resendByteCountAbuse := peerAudit.sendByteCount * resendAbuseMultiple <= peerAudit.resendByteCount
					resedCountAbuse := peerAudit.sendCount * resendAbuseMultiple <= peerAudit.resendCount
					if resendByteCountAbuse || resendCountAbuse {
						// close the sequence
						// the network acl will check abuse statistics and prevent future contracts from opening
						return
					} else {
						ack(multiRouteWriter, messageId)
					}
				}
			}
		case time.After(timeout):
			if 0 == receiveQueue.Len() {
				// idle timeout
				// close the sequence
				return
			}
		}
	}
}


func updateContract(bytesCount int) bool {
	if receiveNoContract(destinationId) {
		return true
	}
	if receiveContract != nil && receiveContract(bytesCount) {
		return true
	}
	return false
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
	client *Client

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



	// FIXME no just write to one
	// FIXME randomize the selection order to do round robin

	/*	
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
	*/
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
	client *Client

	mutex sync.Mutex

	
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


func CreateContractManager(client *Client) {
	// register receiveCallback
	/*
	if sourceId.Equals(ControlId) && CREATE_CONTRACT_RESULT {
		contractManager.AddContract(Contract)
	}
	*/
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





