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

	sendTimeouts *SendTimeouts
	receiveTimeouts *ReceiveTimeouts

	sendPacks chan *SendPack

	receiveCallbacks CallbackList[ReceiveFunction]
	forwardCallbacks CallbackList[ForwardFunction]
}

func NewClient(clientId *ulid.ULID, ctx *context.Context, sendBufferSize int) {
	return &Client{
		clientId: clientId,
		ctx: ctx,
		sendTimeouts: DefaultSendTimeouts(),
		receiveTimeouts: DefaultReceiveTimeouts(),
		sendPacks: make(chan *SendPack, sendBufferSize),
	}
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


// ReceiveFunction
func (self *Client) receive(sourceId *ulid.ULID, frames []*protocol.Frame, err error) {
	for _, receiveCallback := range self.receiveCallbacks.get() {
		receiveCallback(sourceId, frames, err)
	}
}

// ForwardFunction
func (self *Client) forward(sourceId *ulid.ULID, destinationId *ulid.ULID, transferFrameBytes []byte) {
	for _, forwardCallback := range self.forwardCallbacks.get() {
		forwardCallback(sourceId, destinationId, transferFrameBytes)
	}
}

func addReceiveCallback(receiveCallback ReceiveFunction) {
	self.receiveCallbacks.add(receiveCallback)
}

func removeReceiveCallback(receiveCallback ReceiveFunction) {
	self.receiveCallbacks.remove(receiveCallback)
}

func addForwardCallback(forwardCallback ForwardFunction) {
	self.forwardCallbacks.add(forwardCallback)
}

func removeForwardCallback(forwardCallback ForwardFunction) {
	self.forwardCallbacks.remove(forwardCallback)
}

func (self *Client) run(routeManager *RouteManager, contractManager *ContractManager) {
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

	sendBuffer := NewSendBuffer(self.ctx, routeManager, contractManager, self.sendTimeouts)
	receiveBuffer := NewReceiveBuffer(self.ctx, routeManager, contractManager, self.receiveTimeouts)

	// receive
	go func() {
		routeNotifier := self.routeManager.OpenRouteNotifier(clientId)
		defer routeNotifier.Close()
		multiRouteReader := CreateMultiRouteReader(routeNotifier)

		for {
			transferFrameBytes := multiRouteReader.read(self.ctx, -1)
			// at this point, the route is expected to have already parsed the transfer frame
			// and applied basic validation and source/destination checks
			// decode a minimal subset of the full message needed to make a routing decision
			filteredTransferFrame := &protocol.FilteredTransferFrame{}
			if err := protobuf.Decode(transferFrameBytes, &filteredTransferFrame); err != nil {
				// bad protobuf (unexpected, see route note above)
				continue
			}
			sourceId := UlidFromProto(filteredTransferFrame.GetSourceId())
			if sourceId == nil {
				// bad protobuf (unexpected, see route note above)
				continue
			}
			destinationId := UlidFromProto(filteredTransferFrame.GetDestinationId())
			if destinationId == nil {
				// bad protobuf (unexpected, see route note above)
				continue
			}
			if destinationId.Equals(self.clientId) {
				transferFrame := &TransferFrame{}
				if err := protobuf.Decode(transferFrameBytes, &transferFrame); err != nil {
					// bad protobuf
					peerAudit.updateBadMessage(1)
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
						peerAudit.updateBadMessage(1)
						continue
					}
					messageId := UlidFromProto(ack.GetMessageId())
					if messageId == nil {
						// bad protobuf
						peerAudit.updateBadMessage(1)
						continue
					}
					sendBuffer.ack(messageId)
				case PACK:
					pack := &Pack{}
					if err := protobuf.Decode(frame.GetMessageBytes(), &pack); err != nil {
						// bad protobuf
						peerAudit.updateBadMessage(1)
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


// makes a copy of the list on update
type CallbackList[T] struct {
	mutex sync.Mutex
	callbacks []T
}

func (self *CallbackList[T]) get() []T {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return callbacks
}

func (self *CallbackList[T]) add(callback T) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	i := slices.Index(self.callbacks, callback)
	if 0 <= i {
		// already present
		return
	}
	nextCallbacks := slices.Clone(self.callbacks)
	nextCallbacks = append(nextCallbacks, callback)
	self.callbacks = nextCallbacks
}

func  (self *CallbackList[T]) remove(callback T) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	i := slices.Index(self.callbacks, callback)
	if i < 0 {
		// not present
		return
	}
	nextCallbacks := slices.Clone(self.callbacks)
	nextCallbacks = slices.Delete(nextCallbacks, i, i)
	self.callbacks = nextCallbacks
}



type SendTimeouts struct {
	contractTimeout time.Duration
	// resend timeout is the initial time between successive send attempts. Does linear backoff
	resendInterval time.Duration
	// on ack timeout, no longer attempt to retransmit and notify of ack failure
	ackTimeout time.Duration
	idleTimeout time.Duration
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

	sendTimeouts *SendTimeouts

	mutex sync.Mutex
	// destination id -> send sequence
	sendSequences map[*ulid.ULID]*SendSequence
}

func NewSendBuffer(ctx *context.Context, routeManager *RouteManager, contractManager *ContractManager, sendTimeouts *SendTimeouts) *SendBuffer {
	return &SendBuffer{
		ctx: ctx,
		contractManager: contractManager,
		routeManager: routeManager,
		sendTimeouts: sendTimeouts,
		sendSequences: map[*ulid.ULID]*SendSequence{},
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











type SendSequence struct {
	ctx *context.Context
	routeManager *RouteManager
	contractManager *ContractManager
	destinationId *ulid.ULID

	contractTimeout time.Duration
	resendInterval time.Duration
	ackTimeout time.Duration
	idleTimeout time.Duration

	sendContract *SendContract
	sendContracts map[*ulid.ULID]*SendContract

	// routes []Route

	packs chan *SendPack
	acks chan *protocol.Ack

	resendQueue *ResendQueue
	sendItemsSequence []*SendItem

	nextSequenceId uint64
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
	contractNotifier := self.contractManager.OpenContractNotifier(self.routeManager.ClientId(), sendPack.destinationId)
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

		ack := func(messageId *ulid.ULID) {
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

		// FIXME
		// FIXME
		if maxResendBufferByteCount <= resendQueue.ByteCount() {
			// wait for acks

			select {
			case ctx.Done():
				return
			case ack, ok <- self.acks:
				if !ok {
					return
				}
				ack(messageId)
			case time.After(timeout):
			}
		} else {
			select {
			case ctx.Done():
				return
			case ack, ok <- self.acks:
				if !ok {
					return
				}
				ack(messageId)
			case sendPack, ok <- self.packs:
				if !ok {
					return
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
			case time.After(timeout):
				if 0 == resendQueue.Len() {
					// idle timeout
					// close the sequence
					return
				}
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
	maxContractMessageBytesCount := 256
	for {
		next := func(storedContract *protocol.Contract)(bool) {
			sendContract, err := NewSequenceContract(contract)
			if err != nil {
				// malformed, drop
				return false
			}

			contractMessageBytes, _ := protobuf.Marshal(contract)

			if maxContractMessageBytesCount < len(contractMessageBytes) {
				panic("Bad estimate for contract max size could result in infinite contract retries.")
			}

			if sendContract.update(frameBytesCount + len(contractMessageBytes)) {
				
				contract = sendContract
				sendContracts[sendContract.contractId] = sendContract

				// append the contract to the sequence
				send(SendPack{
					destinationId: destinationId,
					frame: Frame{
						MessageType: PACK_CONTRACT,
						MessageBytes: contractMessageBytes,
					}
					ackCallback: nil,
				})

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
			// async queue up the next contract
			conctractManager.CreateContract(destinationId)
			if next(nextContract) {
				return true
			}
		default:
		}

		if contractManager.StandardTransferByteCount < bytesCount + maxContractMessageBytesCount {
			// this pack does not fit into a standard contract
			// TODO allow requesting larger contracts
			return false
		}

		conctractManager.CreateContract(destinationId)

		select {
		case ctx.Done():
			return false
		case nextContract <- contractNotifier.Contract():
			// async queue up the next contract
			conctractManager.CreateContract(destinationId)
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



type SendItem struct {
	messageId *ulid.ULID
	contractId *ulid.ULID
	sequenceId uint64
	enterTime uint64
	resendTime uint64
	sendCount int
	transferFrameBytes []byte
	ackCallback AckFunction

	// the index of the item in the heap
	heapIndex int
}


type ResendQueue struct {
	orderedSendItems []*SendItem
	sendItems map[*ulid.ULID]*SendItem
}

func (self *ResendQueue) add(sendItem *SendItem) {
	self.sendItems[sendItem.messageId] = sendItem
	heap.Push(self, sendItem)
}
func (self *ResendQueue) remove(messageId *ulid.ULID) *SendItem {
	sendItem, ok := sendItems[messageId]
	if !ok {
		return nil
	}
	delete(sendItems, messageId)

	heap.Remove(pg, sendItem.heapIndex)
}

// heap.Interface

func (self *ResendQueue) Push(x any) {
	sendItem := x.(*SendItem)
	sendItem.heapIndex = len(self.orderedSendItems)
	self.orderedSendItems = append(self.orderedSendItems, sendItem)
}

func (self *ResendQueue) Pop() any {
	i := len(self.orderedSendItems) - 1
	sendItem := self.orderedSendItems[i]
	self.orderedSendItems[i] = nil
	self.orderedSendItems = self.orderedSendItems[:n-1]
}

// sort.Interface

func (self *ResendQueue) Len() int {
	return len(self.orderedSendItems)
}

func (self *ResendQueue) Less(i int, j int) bool {
	// invert to change the behavior of Pop to return the min element
	return !(orderedSendItem[i].resendTime < orderedSendItems[j].resendTime)
}

func (self *ResendQueue) Swap(i int, j int) {
	a := orderedSendItem[i]
	b := orderedSendItem[j]
	b.heapIndex = i
	orderedSendItem[i] = b
	a.heapIndex = j
	orderedSendItem[j] = a
}




type ReceiveTimeouts struct {
	ackInterval time.Duration
	gapTimeout time.Duration
	idleTimeout time.Duration

	sequenceBufferSize int
	sequenceQueueMaxByteCount int
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

	ctx *context.Context
	routeManager *RouteManager
	contractManager *ContractManager
	sourceId *ulid.ULID

	receiveQueueMaxByteCount int
	gapTimeout time.Duration
	idleTimeout time.Duration

	packs chan any

	


	receiveContract *ReceiveContract

	receiveQueue *ReceiveQueue


}



func (self *ReceiveSequence) receive(receivePack *ReceivePack) {
	// pre condition: the sequenceId and messageId have been removed from the receiveQueue



	receiveItem := &ReceiveItem{

	}

	sendQueue.add(receiveItem)
	



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

		routeManager.completePeerAudit(sourceId)
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

			itemGapTimeout = receiveItem.receiveTime + gapTimeout - receiveTime
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
						receiveContract = nil
					}

					// this will check the hmac with the local provider secret key
					if !contractManager.Verify(contact) {
						// bad contract
						// close sequence
						peerAudit.updateBadContract(1)
						return
					}

					receiveContract = NewSequenceContract(contract)
				}
			}
			if updateContract(len(frameBytes)) {
				peerAudit.updateSendBytes(len(frameBytes))
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
					if receiveItem := receiveQueue.remove(messageId); receiveItem != nil {
						peerAudit.updateResendBytes(len(receiveItem.frameBytes))
					}
					if receiveItem := receiveQueue.removeBySequenceId(sequenceId); receiveItem != nil {
						peerAudit.updateResendBytes(len(receiveItem.frameBytes))
					}

					// store only up to a max size in the receive queue
					if receiveQueue.ByteSize() + len(frameBytes) < receiveQueueMaxByteCount {
						// add to the receiveQueue
						receive(receivePack)
					} else {
						// drop the message
						peerAudit.updateDiscardedBytes(len(frameBytes))
					}
				} else {
					// already received
					peerAudit.updateResendBytes(len(frameBytes))
					ack(multiRouteWriter, messageId)
				}

				// check the abuse limits
				resendByteCountAbuse := peerAudit.sendByteCount * resendAbuseMultiple <= peerAudit.resendByteCount
				resedCountAbuse := peerAudit.sendCount * resendAbuseMultiple <= peerAudit.resendCount
				if resendByteCountAbuse || resendCountAbuse {
					// close the sequence
					return
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


func (self *ReceiveSequence) updateContract(bytesCount int) bool {
	if receiveNoContract(destinationId) {
		return true
	}
	if receiveContract != nil && receiveContract(bytesCount) {
		return true
	}
	return false
}




type ReceiveItem struct {
	contractId *ulid.ULID
	messageId *ulid.ULID
	
	sequenceId uint64
	receiveTime uint64
	frames []*protocol.Frame
	receiveCallback ReceiveFunction

	// the index of the item in the heap
	heapIndex int
}


type ReceiveQueue struct {
	orderedReceiveItems []*ReceiveItem
	receiveItems map[*ulid.ULID]*ReceiveItem
}

func (self *ReceiveQueue) add(sendItem *SendItem) {
	self.receiveItems[receiveItem.messageId] = receiveItem
	heap.Push(self, receiveItem)
}
func (self *ReceiveQueue) remove(messageId *ulid.ULID) *SendItem {
	receiveItem, ok := receiveItems[messageId]
	if !ok {
		return nil
	}
	delete(self.receiveItems, messageId)

	heap.Remove(self.orderedReceiveItems, receiveItem.heapIndex)
}
func (self *ReceiveQueue) removeBySequenceId(sequenceId int) *SendItem {
	i, found := sort.Find(len(self.orderedReceiveItems), func(i int)(int) {
		return sequenceId - self.orderedReceiveItems[i].sequenceId
	})
	if found && sequenceId == self.orderedReceiveItems[i].sequenceId {
		return remove(self.orderedReceiveItems[i].messageId)
	}
	return nil
}

// heap.Interface

func (self *ReceiveQueue) Push(x any) {
	receiveItem := x.(*ReceiveItem)
	receiveItem.heapIndex = len(self.orderedSendItems)
	self.orderedSendItems = append(self.orderedSendItems, receiveItem)
}

func (self *ReceiveQueue) Pop() any {
	i := len(self.orderedReceiveItems) - 1
	receiveItem := self.orderedReceiveItems[i]
	self.orderedReceiveItems[i] = nil
	self.orderedReceiveItems = self.orderedReceiveItems[:n-1]
}

// sort.Interface

func (self *ReceiveQueue) Len() int {
	return len(self.orderedReceiveItems)
}

func (self *ReceiveQueue) Less(i int, j int) bool {
	// invert to change the behavior of Pop to return the min element
	return !(orderedReceiveItems[i].sequenceId < orderedReceiveItems[j].sequenceId)
}

func (self *ReceiveQueue) Swap(i int, j int) {
	a := orderedReceiveItems[i]
	b := orderedReceiveItems[j]
	b.heapIndex = i
	orderedReceiveItems[i] = b
	a.heapIndex = j
	orderedReceiveItems[j] = a
}





type SequenceContract struct {
	contractId *ulid.ULID
	transferByteCount int
	ackedByteCount int
	unackedByteCount int
}

func NewSequenceContract(contract *protocol.Contract) (*SequenceContract, error) {
	var storedContract StoredContract
	err := protoful.Unmarshal(contract.StoredContractBytes, &storedContract)
	if err != nil {
		return err
	}
	return &SequenceContract{
		contractId: storedContract.ContractId(),
		transferByteCount: storedContract.TransferByteCount(),
		ackedByteCount: 0,
		unackedByteCount: 0,
	}, nil
}

func (self *SequenceContract) update(frameByteCount int) {
	if transferByteCount < ackedByteCount + unackedByteCount + frameByteCount {
		// doesn't fit in contract
		return false
	}
	unackedByteCount += frameByteCount
	return true
}

func (self *SequenceContract) ack(frameByteCount int) {
	if unackedByteCount < frameByteCount {
		panic("Bad accounting.")
	}
	unackedByteCount -= frameByteCount
	ackedByteCount += frameByteCount
}






type ReceiverPeerAudit {
    bad_contract_count
    discarded_byte_count
    discarded_count
    bad_message_byte_count
    bad_message_count
    send_byte_count
    send_count
    resend_byte_count
    resend_count
}




type RouteManager struct {
	client *Client

}

// the peer audit auto completes after a timeout unless forced complete with `completePeerAudit`
func (self *RouteManager) peerAudit(sourceId *ulid.ULID, update func(*ReceiverPeerAudit)) {
}

func (self *RouteManager) completePeerAudit(sourceId *ulid.ULID) {
	// FIXME submit peer audit
	routeManager.CompletePeerAudit(peerAudit)
	// FIXME if peer audit has any bad marks, downgrade the connection
	if BAD_PEER_AUDIT {
		// this will remove any direct connections between the two peers
		// forces the platform to reauth any fast track connections
		routeManager.DowngradeConnection(sourceId)
	}
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

func (self *ContractNotifier) Contract() chan *protocol.Contract {
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





