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


// Each link should apply the forwarding ACL:
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


// FIXME support the context deadline

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
	defer sendBuffer.Close()
	receiveBuffer := NewReceiveBuffer(self.ctx, routeManager, contractManager, self.receiveTimeouts)
	defer receiveBuffer.Close()

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







type SendBufferSettings struct {
	contractTimeout time.Duration
	// resend timeout is the initial time between successive send attempts. Does linear backoff
	resendInterval time.Duration
	// on ack timeout, no longer attempt to retransmit and notify of ack failure
	ackTimeout time.Duration
	idleTimeout time.Duration

	sequenceBufferSize int

	resendQueueMaxByteCount int
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

	sendBufferSettings *SendBufferSettings

	mutex sync.Mutex
	// destination id -> send sequence
	sendSequences map[*ulid.ULID]*SendSequence
}

func NewSendBuffer(ctx *context.Context,
		routeManager *RouteManager,
		contractManager *ContractManager,
		sendBufferSettings *SendBufferSettings) *SendBuffer {
	return &SendBuffer{
		ctx: ctx,
		contractManager: contractManager,
		routeManager: routeManager,
		sendBufferSettings: sendBufferSettings,
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
		sendSequence = NewSendSequence(
			ctx,
			routeManager,
			contractManager,
			sendPack.destinationId,
			sendBufferSettings,
		)
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

	if !initSendSequence().pack(sendPack) {
		delete(sendSequences, sendPack.destinationId)
		initSendSequence().pack(sendPack)
	}
}

func (self *SendBuffer) ack(ack *Ack) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	sendSequence, ok := sendSequences[sendPack.destinationId]
	if !ok {
		return
	}

	// ignore if send sequence was closed
	sendSequence.ack(ack)
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

	sendBufferSettings *SendBufferSettings

	sendContract *SendContract
	sendContracts map[*ulid.ULID]*SendContract

	done chan struct{}
	packs chan *SendPack
	acks chan *protocol.Ack

	resendQueue *ResendQueue
	sendItemsSequence []*SendItem
	nextSequenceId int

	idleCondition *IdleCondition

	multiRouteWriter *MultiRouteWriter
	contractNotifier *ContractNotifier
}

func NewSendSequence(
		ctx *context.Context,
		routeManager *RouteManager,
		contractManager *ContractManager,
		destinationId *ulid.ULID,
		sendBufferSettings *SendBufferSettings) {
	return &SendSequence{
		ctx: ctx,
		routeManager: routeManager,
		contractManager: contractManager,
		destinationId: destinationId,
		sendBufferSettings: sendBufferSettings,
		sendContract: nil,
		sendContracts: map[*ulid.ULID]*SendContract{},
		done: make(chan struct{}, 0),
		packs: make(chan *SendPack, sendBufferSettings.sequenceBufferSize),
		packs: make(chan *protocol.Ack, sendBufferSettings.sequenceBufferSize),
		resendQueue: NewResendQueue(),
		sendItemsSequence: []*SendItem{},
		nextSequenceId: 0,
		idleCondition: NewIdleCondition(),
	}
}

func (self *SendSequence) pack(sendPack *SendPack) (success bool) {
	if !self.idleCondition.updateOpen() {
		success = false
		return
	}
	defer self.idleCondition.updateClose()
	defer func() {
		// this means there was some error in sequence processing
		if err := recover(); err != nil {
			success = false
		}
	}()
	self.packs <- sendPack
	success = true
	return
}

func (self *SendSequence) ack(ack *protocol.Ack) (success bool) {
	if !self.idleCondition.updateOpen() {
		success = false
		return
	}
	defer self.idleCondition.updateClose()
	defer func() {
		// this means there was some error in sequence processing
		if err := recover(); err != nil {
			success = false
		}
	}()
	self.acks <- ack
	success = true
	return
}

func (self *SendSequence) send(frame *protocol.Frame, ackCallback AckFunction) {
	sendTime := time.Now()
	messageId := ulid.New()
	sequenceId := self.nextSequenceId
	self.nextSequenceId += 1
	contactId := self.sendContract.contractId
	first := 0 == self.resendQueue.Len()

	pack := &protocol.Pack{
		messageId: []byte(messageId),
		sequenceId: sequenceId,
		first: first,
		frames: []*Frame{frame}
	}

	packBytes, _ := protobuf.Marshal(pack)

	transferFrame := &protocol.TransferFrame{
		destinationId: []byte(self.destinationId),
		sourceId: []byte(routeManager.ClientId()),
		frame: &protocol.Frame{
			MessageType: protocol.MessageType.PACK,
			messageBytes: packBytes,
		}
	}

	transferFrameBytes, _ := protobuf.Marshal(transferFrame)

	self.multiRouteWriter.write(transferFrameBytes)

	sendItem := SendItem {
		messageId: messageId,
		contractId: contractId,
		sequenceId: sequenceId,
		sendTime: sendTime,
		resendTime: sendTime + resendInterval,
		sendCount: 1,
		first: first,
		transferFrameBytes: transferFrameBytes,
		ackCallback: ackCallback,
	}
	resendQueue.add(sendItem)
}

func (self *SendSequence) setFirst(transferFrameBytes []byte) {
	// TODO this could avoid the memory copy by modifying the raw bytes
	// TODO this is expected to be done infrequently if resends are infrequent, so not an issue

	var transferFrame protocol.TransferFrame
	protobuf.Unmarshal(transferFrameBytes, &transferFrame)

	var pack protocol.Pack
	protobuf.Unmarshal(transferFrame.Frames()[0].MessageBytes(), &pack)

	pack.SetFirst(true)

	packBytes, _ := protobuf.Marshal(pack)
	transferFrame.Frames()[0] = packBytes

	transferFrameBytesWithFirst, _ := protobuf.Marshal(transferFrame)

	return transferFrameBytesWithFirst
}

func (self *SendSequence) run() {
	defer func() {
		self.Close()

		close(self.packs)
		close(self.acks)

		// close contract
		for _, sendContract := range self.openContracts {
			contractManager.Complete(
				sendContract.contractId,
				sendContract.ackedByteCount,
				sendContract.unackedByteCount,
			)
		}

		// drain the buffer
		for _, sendItem := range self.resendQueue.orderedSendItems {
			if sendItem.ackCallback != nil {
				sendItem.ackCallback(errors.New("Closed"))
			}
		}

		// drain the packs
		func() {
			for {
				select {
				case sendPack, ok <- self.packs:
					if !ok {
						return
					}
					if sendPack.ackCallback != nil {
						sendPack.ackCallback(errors.New("Closed"))
					}
				}
			}
		}()
	}()

	routeNotifier := self.routeManager.OpenRouteNotifier(sendPack.destinationId)
	defer routeNotifier.Close()
	self.multiRouteWriter := CreateMultiRouterWriter(routeNotifier)
	self.contractNotifier := self.contractManager.OpenContractNotifier(
		self.routeManager.ClientId(),
		self.destinationId,
	)
	defer self.contractNotifier.Close()

	// init channel with PACK_RESET_SEQUENCE to reset the sequenceId
	send(Frame{
		MessageType: protocol.MessageType.PACK_RESET_SEQUENCE,
		MessageBytes: []byte{},
	}, nil)

	for {
		sendTime := time.Now()
		var timeout time.Duration

		if == resendQueue.Len() { 
			timeout = self.sendBufferSettings.idleTimeout
		} else {
			timeout = self.sendBufferSettings.ackTimeout

			for 0 < self.resendQueue.Len() {
				sendItem := self.resendQueue.removeFirst()

				itemAckTimeout := sendItem.sendTime + ackTimeout - sendTime

				if itemAckTimeout <= 0 {
					// message took too long to ack
					// close the sequence
					return
				}

				if sendTime < sendItem.resendTime {
					// put back on the queue to send later
					self.resendQueue.add(sendItem)
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
				if self.sendItemsSequence[0].sequenceId == sendItem.sequenceId && !sendItem.first {
					// set `first=true`
					transferFrameBytesWithFirst := self.setFirst(sendItem.transferFrameBytes)
					multiRouteWriter.write(transferFrameBytesWithFirst)
				} else {
					multiRouteWriter.write(sendItem.transferFrameBytes)
				}
				sendContracts[sendItem.contractId].updateResend(len(sendItem.transferFrameBytes))
				sendItem.sendCount += 1
				// linear backoff
				itemResendTimeout := sendItem.sendCount * resendInterval
				if itemResendTimeout < itemAckTimeout {
					sendItem.resendTime = sendTime + itemResendTimeout
				} else {
					sendItem.resendTime = sendTime + itemAckTimeout
				}
				self.resendQueue.add(sendItem)
			}
		}

		ack := func(messageId *ulid.ULID) {
			if _, ok := self.resendQueue.sendItems[messageId]; !ok {
				// message not pending ack
				return
			}
			// implicitly ack all earlier items in the sequence
			i := 0
			for ; i < len(self.sendItemsSequence); i += 1 {
				implicitSendItem := self.sendItemsSequence[i]
				if sendItem.sequenceId < implicitSendItem.sequenceId {
					break
				}
				self.resendQueue.remove(implicitSendItem.messageId)
				if implicitSendItem.ackCallback != nil {
					implicitSendItem.ackCallback(nil)
				}

				sendContract := self.sendContracts[implicitSendItem.contractId]
				sendContract.ack(len(implicitSendItem.transferFrameBytes))
				if sendContract.unackedByteCount == 0 {
					self.contractManager.Complete(
						sendContract.contractId,
						sendContract.ackedByteCount,
						sendContract.unackedByteCount,
					)
				}
				self.sendItemsSequence[i] = nil
			}
			self.sendItemsSequence = self.sendItemsSequence[i:]
		}

		if resendQueueMaxByteCount <= self.resendQueue.byteCount() {
			// wait for acks
			select {
			case <- ctx.Done():
				return
			case <- self.done:
				return
			case ack <- self.acks:
				ack(messageId)
			case time.After(timeout):
				// resend
			}
		} else {
			checkpointId := idleCondition.checkpoint()
			select {
			case <- ctx.Done():
				return
			case <- self.done:
				return
			case ack <- self.acks:
				ack(messageId)
			case sendPack <- self.packs:
				for _, frame := range sendPack.frames {
					if len(frame.MessageBytes) < self.sendBufferSettings.minMessageByteCount {
						// bad message
						// close the sequence
						return	
					}
				}
				if self.updateContract(len(sendPack.frame.MessageBytes)) {
					send(sendPack)
				} else {
					// no contract
					// close the sequence
					if sendPack.ackCallback != nil {
						sendPack.ackCallback(errors.New("No contract"))
					}
					return
				}
			case time.After(timeout):
				if 0 == resendQueue.Len() {
					// idle timeout
					if idleCondition.close(checkpointId) {
						// close the sequence
						return
					}
					// else there pending updates
				}
			}
		}
	}
}

func (self *SendSequence) updateContract(messageBytesCount int) bool {
	// `sendNoContract` is a mutual configuration 
	// both sides must configure themselves to require no contract from each other
	if contractManager.sendNoContract(destinationId) {
		return true
	}
	if self.sendContract != nil {
		if self.sendContract.update(messageBytesCount) {
			return true
		} else {
			contractManager.Complete(
				self.sendContract.contractId,
				self.sendContract.ackedByteCount,
				self.sendContract.unackedByteCount,
			)
			self.sendContract = nil
		}
	}
	// new contract

	// the max overhead of the pack frame
	// this is needed because the size of the contract pack is counted against the contract
	maxContractMessageBytesCount := 256

	if contractManager.StandardTransferByteCount() < messageBytesCount + maxContractMessageBytesCount {
		// this pack does not fit into a standard contract
		// TODO allow requesting larger contracts
		return false
	}

	next := func(contract *protocol.Contract)(bool) {
		sendContract, err := NewSequenceContract(contract)
		if err != nil {
			// malformed, drop
			return false
		}

		contractMessageBytes, _ := protobuf.Marshal(contract)

		if maxContractMessageBytesCount < len(contractMessageBytes) {
			panic("Bad estimate for contract max size could result in infinite contract retries.")
		}

		if sendContract.update(messageBytesCount + len(contractMessageBytes)) {
			self.sendContract = sendContract
			self.sendContracts[sendContract.contractId] = sendContract

			// append the contract to the sequence
			send(Frame{
				MessageType: protocol.MessageType.PACK_CONTRACT,
				MessageBytes: contractMessageBytes,
			}, nil)

			return true
		} else {
			// this contract doesn't fit the message
			// just close it since it was never send to the other side
			contractManager.Complete(sendContract.contractId, 0, 0)
			return false
		}
	}

	endTime := time.Now() + self.sendBufferSettings.contractTimeout
	for {
		select {
		case nextContract <- contractNotifier.Contract():
			// async queue up the next contract
			conctractManager.CreateContract(destinationId)
			if next(nextContract) {
				return true
			}
		default:
		}

		conctractManager.CreateContract(destinationId)

		timeout := endTime - time.Now()
		if timeout <= 0 {
			return false
		}

		select {
		case ctx.Done():
			return false
		case nextContract <- contractNotifier.Contract():
			// async queue up the next contract
			conctractManager.CreateContract(destinationId)
			if next(nextContract) {
				return true
			}
		case After(timeout):
			return false
		}
	}
}

func (self *SendSequence) Close() {
	defer recover()
	close(self.done)
}


type SendItem struct {
	messageId *ulid.ULID
	contractId *ulid.ULID
	sequenceId uint64
	sendTime uint64
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
	byteCount int
}

func NewResendQueue() *ResendQueue {
	resendQueue := &ResendQueue{
		orderedSendItems: []*SendItem{},
		sendItems: map[*ulid.ULID]*SendItem{},
		byteCount: 0,
	}
	heap.Init(resendQueue)
	return resendQueue
}

func (self *ResendQueue) add(sendItem *SendItem) {
	self.sendItems[sendItem.messageId] = sendItem
	heap.Push(self, sendItem)
	self.byteCount += len(sendItem.transferFrameBytes)
}

func (self *ResendQueue) remove(messageId *ulid.ULID) *SendItem {
	sendItem, ok := sendItems[messageId]
	if !ok {
		return nil
	}
	delete(self.sendItems, messageId)
	heap.Remove(pg, sendItem.heapIndex)
	self.byteCount -= sendItem.byteCount
}

func (self *ResendQueue) removeFirst() *SendItem {
	first := heap.Pop(self)
	if first == nil {
		return nil
	}
	sendItem := first.(*SendItem)
	delete(self.sendItems, sendItem.messageId)
	self.byteCount -= sendItem.byteCount
	return sendItem
}

func (self *ResendQueue) byteCount() {
	return self.byteCount
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
	return orderedSendItem[i].resendTime < orderedSendItems[j].resendTime
}

func (self *ResendQueue) Swap(i int, j int) {
	a := orderedSendItem[i]
	b := orderedSendItem[j]
	b.heapIndex = i
	orderedSendItem[i] = b
	a.heapIndex = j
	orderedSendItem[j] = a
}




type ReceiveBufferSettings struct {
	ackInterval time.Duration
	gapTimeout time.Duration
	idleTimeout time.Duration

	sequenceBufferSize int
	receiveQueueMaxByteCount int

	// max legit fraction of sends that are resends
	resendAbuseMultiple float
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

	done chan struct{}
	packs chan *ReceivePack

	


	receiveContract *ReceiveContract

	receiveQueue *ReceiveQueue

	// FIXME idle condition

	nextSequenceId int



}



func (self *ReceiveSequence) receive(receivePack *ReceivePack) {
	// pre condition: the sequenceId and messageId have been removed from the receiveQueue

	sequenceId := receivePack.pack.SequenceId()

	


	receiveItem := &ReceiveItem{

	}

	sendQueue.add(receiveItem)
	



}



func (self *ReceiveSequence) ack(messageId *ulid.ULID) {

	// send an ack

	multiRouteWriter.write(ackBytes)

}



func (self *ReceiveSequence) run() {
	defer func() {
		self.Close()

		close(self.packs)

		// close contract
		if self.receiveContract != nil {
			self.contractManager.Complete(
				self.receiveContract.contractId,
				self.receiveContract.ackedByteCount,
				self.receiveContract.unackedByteCount,
			)
		}

		// drain the buffer
		for _, receiveItem := range self.receiveQueue.orderedReceiveItems {
			if receiveItem.receiveCallback != nil {
				receiveItem.receiveCallbace(self.sourceId, receiveItem.frames, errors.New("Closed"))
			}
			self.routeManager.receiverPeerAudit(self.sourceId, func(a *ReceiverPeerAudit) {
				messageByteCount := 0
				for _, frame := range receiveItem.frames {
					messageByteCount += len(frame.MessageBytes)
				}
				a.discard(messageByteCount)
			})
		}

		// drain the channel
		func() {
			for {
				select {
				case receivePack, ok <- self.packs:
					if (!ok) {
						return
					}
					if receivePack.receiveCallback != nil {
						receivePack.receiveCallbace(self.sourceId, receivePack.frames, errors.New("Closed"))
					}
					self.routeManager.receiverPeerAudit(self.sourceId, func(a *ReceiverPeerAudit) {
						messageByteCount := 0
						for _, frame := range receivePack.frames {
							messageByteCount += len(frame.MessageBytes)
						}
						a.discard(messageByteCount)
					})
				}
			}
		}()

		routeManager.completeReceiverPeerAudit(sourceId)
	}()

	routeNotifier := self.routeManager.OpenRouteNotifier(sendPack.destinationId)
	defer routeNotifier.Close()
	multiRouteWriter := CreateMultiRouteWriter(routeNotifier)

	for {
		receiveTime := time.Now()
		var timeout time.Duration
		
		if 0 == receiveQueue.Len() {
			timeout = self.receiveBufferSettings.idleTimeout
		} else {
			timeout = self.receiveBufferSettings.gapTimeout
			for 0 < receiveQueue.Len() {
				receiveItem := receiveQueue.removeFirst()

				itemGapTimeout := receiveItem.receiveTime + self.receiveBufferSettings.gapTimeout - receiveTime
				if itemGapTimeout < 0 {
					// did not receive a preceding message in time
					// close sequence
					return
				}

				if receiveItem.first && self.nextSequenceId < receiveItem.sequenceId {
					// the sender has indicated this item is first in the sequence
					// this would happen if the receiver lost state (e.g. recreated from zero state)
					self.nextSequenceId = receiveItem.sequenceId
				}

				if receiveItem.sequenceId != self.nextSequenceId {
					if itemGapTimeout < timeout {
						timeout = itemGapTimeout
					}
					break
				}

				// this item is the head of sequence
				self.nextSequenceId += 1

				messageByteCount := 0
				for _, frame := range receiveItem.frames {
					messageByteCount += len(frame.MessageBytes)
				}

				// register contracts
				for _, frame := range receiveItem.frames {
					if protocol.MessageType.PACK_CONTRACT == frame.MessageType {
						// close out the previous contract
						if self.receiveContract != nil {
							self.contractManager.Complete(
								self.receiveContract,
								self.receiveContract.ackedByteCount,
								self.receiveContract.unackedByteCount,
							)
							self.receiveContract = nil
						}

						var contract protocol.Contract
						err := protobuf.Unmarshal(frame.MessageBytes, &contract)
						if err != nil {
							// bad message
							// close sequence
							if receiveItem.receiveCallback != nil {
								receiveItem.receiveCallback(self.sourceId, receiveItem.frames, errors.New("Bad message"))
							}
							self.routeManager.receiverPeerAudit(self.sourceId, func(a *ReceiverPeerAudit) {
								a.badMessage(messageByteCount)
							})
							return
						}

						// check the hmac with the local provider secret key
						if !self.contractManager.Verify(contract.StoredContractHmac(), contract.StoredContractBytes()) {
							// bad contract
							// close sequence
							if receiveItem.receiveCallback != nil {
								receiveItem.receiveCallback(self.sourceId, receiveItem.frames, errors.New("Bad contract"))
							}
							self.routeManager.receiverPeerAudit(self.sourceId, func(a *ReceiverPeerAudit) {
								a.badContract(messageByteCount)
							})
							return
						}

						self.receiveContract = NewSequenceContract(contract)
					}
				}
				if self.updateContract(messageByteCount) {
					self.routeManager.receiverPeerAudit(self.sourceId, func(a *ReceiverPeerAudit) {
						a.received(messageByteCount)
					})
					if receiveItem.receiveCallback != nil {
						receiveItem.receiveCallback(self.sourceId, receiveItem.frames, nil)
					}
					self.ack(receiveItem.messageId)
				} else {
					// no contract
					// close the sequence
					if receiveItem.receiveCallback != nil {
						receiveItem.receiveCallback(self.sourceId, receiveItem.frames, errors.New("No contract"))
					}
					self.routeManager.receiverPeerAudit(self.sourceId, func(a *ReceiverPeerAudit) {
						a.discard(messageByteCount)
					})
					return
				}
			}
		}

		select {
		case <- ctx.Done():
			return
		case <- self.done:
			return
		case receivePack <- self.packs:
			// every message must count against the contract to avoid abuse
			for _, frame := range receivePack.pack.Frames() {
				if len(frame.MessageBytes) < self.receiveBufferSettings.minMessageByteCount {
					// bad message
					// close the sequence
					self.routeManager.receiverPeerAudit(self.sourceId, func(a *ReceiverPeerAudit) {
						a.badMessage(messageByteCount)
					})
					return	
				}
			}
			if self.nextSequenceId <= receivePack.sequenceId {
				// replace with the latest value (check both messageId and sequenceId)
				if receiveItem := receiveQueue.remove(messageId); receiveItem != nil {
					self.routeManager.receiverPeerAudit(self.sourceId, func(a *ReceiverPeerAudit) {
						messageByteCount := 0
						for _, frame := range receiveItem.frames {
							messageByteCount += len(frame.MessageBytes)
						}
						a.resend(messageByteCount)
					})
				}
				if receiveItem := receiveQueue.removeBySequenceId(sequenceId); receiveItem != nil {
					self.routeManager.receiverPeerAudit(self.sourceId, func(a *ReceiverPeerAudit) {
						messageByteCount := 0
						for _, frame := range receiveItem.frames {
							messageByteCount += len(frame.MessageBytes)
						}
						a.resend(messageByteCount)
					})
				}

				// always allow at least one item in the receive queue
				// remove later items to fit
				for 0 < receiveQueue.Len() && receiveQueueMaxByteCount <= receiveQueue.ByteSize() + len(frameBytes) {
					lastReceiveItem := self.receiveQueue.peekLast()
					if receivePack.sequenceId < lastReceiveItem.sequenceId {
						self.receiveQueue.remove(lastReceiveItem.messageId)
					}
				}

				// store only up to a max size in the receive queue
				if 0 == receiveQueue.Len() || receiveQueue.ByteSize() + len(frameBytes) < receiveQueueMaxByteCount {
					// add to the receiveQueue
					receive(receivePack)
				} else {
					// drop the message
					peerAudit.updateDiscardedBytes(len(frameBytes))
				}
			} else {
				// already received
				peerAudit.updateResendBytes(len(frameBytes))
				self.ack(receiveItem.messageId)
			}
		case time.After(timeout):
			if 0 == receiveQueue.Len() {
				// idle timeout
				// close the sequence
				return
			}
		}

		// check the resend abuse limits
		// resends can appear normal but waste bandwidth
		abuse := false
		self.routeManager.receiverPeerAudit(self.sourceId, func(a *ReceiverPeerAudit) {
			if self.receiveBufferSettings.resendAbuseThreshold <= a.resendCount {
				resendByteCountAbuse := a.sendByteCount * self.receiveBufferSettings.resendAbuseMultiple <= a.resendByteCount
				resedCountAbuse := a.sendCount * self.receiveBufferSettings.resendAbuseMultiple <= a.resendCount
				abuse = resendByteCountAbuse || resendCountAbuse
			}
		})
		if abuse {
			// close the sequence
			self.routeManager.downgradeConnection(self.sourceId)
			return
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

func (self *ReceiveSequence) close() {
	defer recover()
	close(self.done)
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
	byteCount int
}

func NewReceiveQueue() *ReceiveQueue {
	receiveQueue := &ReceiveQueue{
		orderedReceiveItems: []*ReceiveItem{},
		receiveItems: map[*ulid.ULID]*ReceiveItem{},
		byteCount: 0,
	}
	heap.Init(receiveQueue)
	return receiveQueue
}

func (self *ReceiveQueue) add(receiveItem *ReceiveItem) {
	self.receiveItems[receiveItem.messageId] = receiveItem
	heap.Push(self, receiveItem)
	for _, frame := receiveItem.frames {
		self.byteCount += len(frame.MessageBytes)
	}
}

func (self *ReceiveQueue) remove(messageId *ulid.ULID) *ReceiveItem {
	receiveItem, ok := receiveItems[messageId]
	if !ok {
		return nil
	}
	delete(self.receiveItems, messageId)
	heap.Remove(self.orderedReceiveItems, receiveItem.heapIndex)
	for _, frame := receiveItem.frames {
		self.byteCount -= len(frame.MessageBytes)
	}
}

func (self *ReceiveQueue) removeBySequenceId(sequenceId int) *ReceiveItem {
	i, found := sort.Find(len(self.orderedReceiveItems), func(i int)(int) {
		return sequenceId - self.orderedReceiveItems[i].sequenceId
	})
	if found && sequenceId == self.orderedReceiveItems[i].sequenceId {
		return remove(self.orderedReceiveItems[i].messageId)
	}
	return nil
}

func (self *ReceiveQueue) removeFirst() *ReceiveItem {
	first := heap.Pop(self)
	if first == nil {
		return nil
	}
	receiveItem := first.(*ReceiveItem)
	delete(self.receiveItems, receiveItem.messageId)
	for _, frame := receiveItem.frames {
		self.byteCount -= len(frame.MessageBytes)
	}
	return receiveItem
}

// heap.Interface

func (self *ReceiveQueue) Push(x any) {
	receiveItem := x.(*ReceiveItem)
	receiveItem.heapIndex = len(self.orderedReceiveItems)
	self.orderedReceiveItems = append(self.orderedReceiveItems, receiveItem)
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
	return orderedReceiveItems[i].sequenceId < orderedReceiveItems[j].sequenceId
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



// sequences close after a time with no messages
// this coordinates the idle shutdown adding messages to the sequence channels
type SequenceIdleCondition struct {

}


// idleGate
func close(checkpointId int) bool {
	lock
	if self.modId != checkpointId {
		return false
	}
	if 0 < pendingCount {
		return false
	}
	closing = true
}

func updatePending() bool {
	lock
	if closing {
		return false
	}
	modId += 1
	pendingCount += 1
	return true
}

func updateComplete() {
	pendingCount -= 1
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
func (self *RouteManager) receiverPeerAudit(sourceId *ulid.ULID, update func(*ReceiverPeerAudit)) {
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





