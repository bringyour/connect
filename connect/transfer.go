package connect

import (

	"google.golang.org/protobuf/proto"
	"github.com/oklog/ulid/v2"

	"bringyour.com/protocol"
)


/*
Sends frames to destinations with properties:
- as long the sending client is active, frames are eventually delivered up to timeout
- frames are received in order of send
- sender is notified when frames are received
- sender and receiver account for mutual transfer with a shared contract
- support for multiple routes to the destination
- senders are verified with pre-exchanged keys
- high throughput and bounded resource usage

*/

/*
Each route should apply the forwarding ACL:
- reject if source id does not match network id
- reject if not an active contract between sender and receiver

*/

type AckFunction func(err error)
// provideMode is the mode of where these frames are from: network, friends and family, public
// provideMode nil means no contract
type ReceiveFunction func(sourceId *ulid.ULID, frames []*protocol.Frame, provideMode *protocol.ProvideMode, err error)
type ForwardFunction(sourceId *ulid.ULID, destinationId *ulid.ULID, transferFrameBytes []byte)


// destination id for control messages
ControlId = ULID([]byte{
	0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00,
})


// FIXME support the context deadline

// note all callbacks are wrapped to check for nil and recover from errors

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
	safeAckCallback := func(err error) {
		if ackCallback != nil {
			defer recover()
			ackCallback(err)
		}
	}
	sendPack := &SendPack{
		frame: frame,
		destinationId: destinationId,
		ackCallback: safeAckCallback,
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
			safeAckCallback(errors.New("Send buffer full."))
			return false
		}
	} else {
		select {
		case sendPacks <- sendPack:
			return true
		case time.After(timeout):
			// full
			safeAckCallback(errors.New("Send buffer full."))
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
		func() {
			defer recover()
			receiveCallback(sourceId, frames, err)
		}()
	}
}

// ForwardFunction
func (self *Client) forward(sourceId *ulid.ULID, destinationId *ulid.ULID, transferFrameBytes []byte) {
	for _, forwardCallback := range self.forwardCallbacks.get() {
		func() {
			defer recover()
			forwardCallback(sourceId, destinationId, transferFrameBytes)
		}()
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
				sendPack.ackCallback(errors.New("Client closed."))
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
				self.forward(sourceId, destinationId, transferFrameBytes)
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
	routeManager *routeManager
	contractManager *ContractManager
	

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
		routeManager: routeManager,
		contractManager: contractManager,
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
		acks: make(chan *protocol.Ack, sendBufferSettings.sequenceBufferSize),
		resendQueue: NewResendQueue(),
		sendItemsSequence: []*SendItem{},
		nextSequenceId: 0,
		idleCondition: NewIdleCondition(),
	}
}

func (self *SendSequence) Pack(sendPack *SendPack) (success bool) {
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

func (self *SendSequence) Ack(ack *protocol.Ack) (success bool) {
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
	contactId := self.sendContract.contractId
	first := 0 == len(self.sendItemsSequence)

	self.nextSequenceId += 1

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

	self.sendItemsSequence = append(self.sendItemsSequence, sendItem)
	resendQueue.add(sendItem)

	self.multiRouteWriter.write(transferFrameBytes)
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

func (self *SendSequence) Run() {
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
			sendItem.ackCallback(errors.New("Closed"))
		}

		// drain the packs
		func() {
			for {
				select {
				case sendPack, ok <- self.packs:
					if !ok {
						return
					}
					sendPack.ackCallback(errors.New("Closed"))
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
				implicitSendItem.ackCallback(nil)

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
					sendPack.ackCallback(errors.New("No contract"))
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
				MessageType: protocol.MessageType.CONTRACT,
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
		if nextContract, err := contractManager.takeContract(self.ctx, sourceId, destinationId, 0); err != nil {
			// async queue up the next contract
			conctractManager.CreateContract(destinationId)
			if next(nextContract) {
				return true
			}
		}

		conctractManager.CreateContract(destinationId)

		timeout := endTime - time.Now()
		if timeout <= 0 {
			return false
		}

		if nextContract, err := contractManager.takeContract(self.ctx, sourceId, destinationId, timeout); err != nil {
			// async queue up the next contract
			conctractManager.CreateContract(destinationId)
			if next(nextContract) {
				return true
			}
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

	receiveBufferSettings *ReceiveBufferSettings

	mutex sync.Mutex
	// source id -> receive sequence
	receiveSequences map[*ulid.ULID]*ReceiveSequence
}

func NewReceiveBuffer(ctx *context.Context,
		routeManager *RouteManager,
		contractManager *ContractManager,
		receiveBufferSettings *SendBufferSettings) *SendBuffer {
	return &ReceiveBuffer{
		ctx: ctx,
		routeManager: routeManager,
		contractManager: contractManager,
		receiveBufferSettings: receiveBufferSettings,
		receiveSequences: map[*ulid.ULID]*ReceiveSequence{},
	}
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

	// FIXME this is a destinationId
	sourceId *ulid.ULID

	receiveBufferSettings *ReceiveBufferSettings

	receiveContract *ReceiveContract

	done chan struct{}
	packs chan *ReceivePack

	receiveQueue *ReceiveQueue
	nextSequenceId int

	idleCondition *IdleCondition

	multiRouteWriter *MultiRouteWriter
	contractNotifier *ContractNotifier
}

func NewReceiveSequence(
		ctx *context.Context,
		routeManager *RouteManager,
		contractManager *ContractManager,
		sourceId *ulid.ULID,
		receiveBufferSettings *ReceiveBufferSettings) {
	return &ReceiveSequence{
		ctx: ctx,
		routeManager: routeManager,
		contractManager: contractManager,
		sourceId: sourceId,
		receiveBufferSettings: receiveBufferSettings,
		receiveContract: nil,
		done: make(chan struct{}, 0),
		packs: make(chan *ReceivePack, sendBufferSettings.sequenceBufferSize),
		receiveQueue: NewReceiveQueue(),
		nextSequenceId: 0,
		idleCondition: NewIdleCondition(),
	}
}

func (self *ReceiveSequence) Pack(receivePack *ReceivePack) (success bool) {
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

func (self *ReceiveSequence) receive(receivePack *ReceivePack) error {
	// pre condition: the sequenceId and messageId have been removed from the receiveQueue

	receiveTime := time.Now()

	sequenceId := receivePack.pack.SequenceId()
	contractId := self.receiveContract.contractId
	messageId := UlidFromProto(receivePack.pack.MessageId())
	if messageId == nil {
		return errors.New("Bad message_id")
	}

	receiveItem := &ReceiveItem{
		contractId: contractId,
		messageId: messageId,
		sequenceId: sequenceId,
		receiveTime: receiveTime,
		frames: receivePack.pack.Frames(),
		receiveCallback: receivePack.receiveCallback,
	}

	self.receiveQueue.add(receiveItem)
}

func (self *ReceiveSequence) ack(messageId *ulid.ULID) {
	ack := &protocol.Ack{
		messageId: []byte(messageId),
	}

	ackBytes, _ := protobuf.Marshal(ack)

	transferFrame := &protocol.TransferFrame{
		destinationId: []byte(self.sourceId),
		sourceId: []byte(self.routeManager.ClientId()),
		frame: &protocol.Frame{
			MessageType: protocol.MessageType.ACK,
			messageBytes: ackBytes,
		}
	}

	transferFrameBytes, _ := protobuf.Marshal(transferFrame)

	multiRouteWriter.write(transferFrameBytes)
}

func (self *ReceiveSequence) Run() {
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
					if protocol.MessageType.CONTRACT == frame.MessageType {
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
							receiveItem.receiveCallback(self.sourceId, receiveItem.frames, errors.New("Bad message"))
							self.routeManager.receiverPeerAudit(self.sourceId, func(a *ReceiverPeerAudit) {
								a.badMessage(messageByteCount)
							})
							return
						}

						// check the hmac with the local provider secret key
						if !self.contractManager.Verify(contract.StoredContractHmac(), contract.StoredContractBytes(), contract.ProvideMode()) {
							// bad contract
							// close sequence
							receiveItem.receiveCallback(self.sourceId, receiveItem.frames, errors.New("Bad contract"))
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
					receiveItem.receiveCallback(self.sourceId, receiveItem.frames, nil)
					self.ack(receiveItem.messageId)
				} else {
					// no contract
					// close the sequence
					receiveItem.receiveCallback(self.sourceId, receiveItem.frames, errors.New("No contract"))
					self.routeManager.receiverPeerAudit(self.sourceId, func(a *ReceiverPeerAudit) {
						a.discard(messageByteCount)
					})
					return
				}
			}
		}

		checkpointId := idleCondition.checkpoint()
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
					err := receive(receivePack)
					if err != nil {
						// bad message
						// close the sequence
						self.routeManager.receiverPeerAudit(self.sourceId, func(a *ReceiverPeerAudit) {
							a.badMessage(messageByteCount)
						})
						return	
					}
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
				if idleCondition.close(checkpointId) {
					// close the sequence
					return
				}
				// else there pending updates
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
	// `receiveNoContract` is a mutual configuration 
	// both sides must configure themselves to require no contract from each other
	if self.contractManager.receiveNoContract(self.sourceId) {
		return true
	}
	if self.receiveContract != nil && self.receiveContract.update(bytesCount) {
		return true
	}
	return false
}

func (self *ReceiveSequence) Close() {
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
	modId int
	updateOpenCount int
	closed bool
	mutex sync.Mutex
}

func NewSequenceIdleCondition() *SequenceIdleCondition {
	return &SequenceIdleCondition{
		modId: 0,
		updateOpenCount: 0,
		closed: false,
	}
}

func (self *SequenceIdleCondition) checkpoint() {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return self.modId
}

func (self *SequenceIdleCondition) close(checkpointId int) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if self.modId != checkpointId {
		return false
	}
	if 0 < self.updateOpenCount {
		return false
	}
	self.closed = true
	return true
}

func (self *SequenceIdleCondition) updateOpen() bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if self.closed {
		return false
	}
	self.modId += 1
	self.updateOpenCount += 1
	return true
}

func (self *SequenceIdleCondition) updateClose() {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.updateOpenCount -= 1
}


type ReceiverPeerAudit struct {
    badContractCount int
    discardedByteCount int
    discardedCount int
    badMessageByteCount int
    badMessageCount int
    sendByteCount int
    sendCount int
    resendByteCount int
    resendCount int
}


type Route = chan []byte


type RouteManager struct {
	client *Client

	writerMatchState *MatchState
	readerMatchState *MatchState

	// FIXME audits
}

func NewRouteManager(client *Client) *RouteManager {
	return &RouteManager{
		client: client,
		writerMatchState: NewMatchState(true, Transport.MatchesSend),
		// equal weighting for receive is usually good enough
		readerMatchState: NewMatchState(false, Transport.MatchesReceive),
		// FIXME audits
	}
}

// the peer audit auto completes after a timeout unless forced complete with `completePeerAudit`
func (self *RouteManager) receiverPeerAudit(sourceId *ulid.ULID, update func(*ReceiverPeerAudit)) {
	// FIXME
}

func (self *RouteManager) completePeerAudit(sourceId *ulid.ULID) {
	// FIXME
	/*
	// FIXME submit peer audit
	routeManager.CompletePeerAudit(peerAudit)
	// FIXME if peer audit has any bad marks, downgrade the connection
	if BAD_PEER_AUDIT {
		// this will remove any direct connections between the two peers
		// forces the platform to reauth any fast track connections
		routeManager.DowngradeConnection(sourceId)
	}
	*/
}

func (self *RouteManager) OpenMultiRouteWriter(destinationId *ULID) MultiRouteWriter {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	return self.writerMatchState.OpenMultiRouteSelector(destinationId).(MultiRouteWriter)
}

func (self *RouteManager) CloseMultiRouteWriter(w MultiRouteWriter) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.writerMatchState.CloseMultiRouteSelector(w.(*MultiRouteSelector))
}

func (self *RouteManager) OpenMultiRouteReader(destinationId *ULID) MultiRouteReader {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	return self.readerMatchState.OpenMultiRouteSelector(destinationId).(MultiRouteReader)
}

func (self *RouteManager) CloseMultiRouteReader(r MultiRouteReader) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.readerMatchState.CloseMultiRouteSelector(r.(*MultiRouteSelector))
}

func (self *RouteManager) updateTransport(transport Transport, routes []Route) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.writerMatchState.updateTransport(transport, routes)
	self.readerMatchState.updateTransport(transport, routes)
}

func (self *RouteManager) removeTransport(transport Transport) {
	self.updateTransport(transport, nil)
}

func (self *RouteManager) getTransportStats(transport Transport) (writerStats *RouteStats, readerStats *RouteStats) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	writerStats = self.writerMatchState.getTransportStats(transport)
	readerStats = self.readerMatchState.getTransportStats(transport)
	return
}


type MatchState struct {
	weightedRoutes bool
	matches func(Transport, *ulid.ULID)(bool)

	transportRoutes map[Trasport][]Route

	destinationMultiRouteSelectors map[*ulid.ULID]map[*MultiRouteSelector]bool

	transportMatchedDestinations map[Transport]map[*ulid.ULID]bool
}

// note weighted routes typically are used by the sender not receiver
func NewMatchState(weightedRoutes bool, matches func(Transport, *ulid.ULID)(bool)) *MatchState {
	return &MatchState{
		weightedRoutes: weightedRoutes,
		matches: matches,
		destinationMultiRouteSelectors: map[*ulid.ULID]map[*MultiRouteSelector]bool{},
		transportMatchedDestinations: map[Transport]map[*ulid.ULID]bool{},
	}
}

func (self *MatchState) getTransportStats(transport Transport) *RouteStats {
	destinationIds, ok := transportMatchedDestinations[transport]
	if !ok {
		return nil
	}
	netStats := NewRouteStats()
	for destinationId, _ := range destinationIds {
		if multiRouteSelectors, ok := destinationMultiRouteSelectors[destinationId]; ok {
			for _, multiRouteSelector := range multiRouteSelectors {
				if stats := multiRouteSelector.getTransportStats(transport); stats != nil {
					netStats.sendCount += stats.sendCount
					netStats.sendByteCount += stats.sendByteCount
					netStats.receiveCount += stats.receiveCount
					netStats.receiveByteCount += stats.receiveByteCount
				}
			}
		}
	}
	return netStats
}

func (self *MatchState) OpenMultiRouteSelector(destinationId *ULID) *MultiRouteSelector {
	multiRouteSelector := NewMultiRouteSelector(destinationId, self.weightedRoutes)

	multiRouteSelectors, ok := self.destinationMultiRouteSelectors[destinationId]
	if !ok {
		multiRouteSelectors = map[*MultiRouteSelector]bool{}
		self.destinationMultiRouteSelectors[destinationId] = multiRouteSelectors
	}
	multiRouteSelectors[multiRouteSelector] = true

	for transport, routes := range self.transportRoutes {
		matchedDestinations, ok := self.transportMatchedDestinations[transport]
		if !ok {
			matchedDestinations := map[*ulid.ULID]bool{}
			self.transportMatchedDestinations[transport] = matchedDestinations
		}

		// use the latest matches state
		if self.matches(transport, destinationId) {
			matchedDestinations[destinationId] = true
			multiRouteSelector.updateTransport(transport, routes)
		}
	}
}

func (self *MatchState) CloseMultiRouteSelector(multiRouteSelector *MultiRouteSelector) {
	// TODO readers do not need to prioritize routes

	destinationId := multiRouteSelector.destinationId
	multiRouteSelectors, ok := self.destinationMultiRouteSelectors[destinationId]
	if !ok {
		// not present
		return
	}
	delete(multiRouteSelectors, multiRouteSelector)

	if len(multiRouteSelectors) == 0 {
		// clean up the destination
		for transport, matchedDestinations := range self.transportMatchedDestinations {
			delete(matchedDestinations, destinationId)
		}
	}
}

func (self *MatchState) updateTransport(transport Transport, routes []Route) {
	if routes == nil {
		for currentMatchedDestinations, ok := transportMatchedDestinations[transport]; ok {
			for destinationId, _ := range currentMatchedDestinations {
				if multiRouteSelectors, ok := destinationMultiRouteSelectors[destinationId]; ok {
					for multiRouteSelector, _ := range multiRouteSelectors {
						multiRouteSelector.updateTransport(transport, nil)
					}
				}
			}
		}

		delete(self.transportMatchedDestinations, transport)
		delete(self.transportRoutes, transport)
	} else {
		matchedDestinations := map[*ulid.ULID]bool{}

		for destinationId, multiRouteSelectors := range self.destinationMultiRouteSelectors {
			if transport.MatchesSend(destinationId) {
				matchedDestinations[destinationId] = true
				for multiRouteSelector, _ := range multiRouteSelectors {
					multiRouteWriter.updateTransport(transport, routes)
				}
			}
		}

		for currentMatchedDestinations, ok := transportMatchedDestinations[transport]; ok {
			for destinationId, _ := range currentMatchedDestinations {
				if _, ok := currentMatchedDestinations[destinationId]; !ok {
					// no longer matches
					if multiRouteWriters, ok := destinationMultiRouteSelectors[destinationId]; ok {
						for multiRouteSelector, _ := range multiRouteSelectors {
							multiRouteSelector.updateTransport(transport, nil)
						}
					}
				}
			}
		}

		self.transportMatchedDestinations[transport] = matchedDestinations
		self.transportRoutes[transport] = routes
	}
}


type Transport interface {
	Priority() int
	
	CanEvalRouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) bool
	// returns the fraction of route weight that should be allocated to this transport
	// the remaining are the lower priority transports
	// call `rematchTransport` to re-evaluate the weights. this is used for a control loop where the weight is adjusted to match the actual distribution
	RouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) float32
		
	MatchesSend(destinationId *ulid.ULID)
	MatchesReceive(destinationId *ulid.ULID)
}


type MultiRouteWriter interface {
	Write(ctx context.Context, transportFrameBytes []byte, timeout time.Duration) (*Route, error)
}


type MultiRouteReader interface {
	Read(ctx context.Context, timeout time.Duration) ([]byte, *Route, error)
}


type MultiRouteSelector struct {
	destinationId *ulid.ULID
	weightedRoutes bool

	transportUpdate *Monitor
	done chan struct{}

	mutex sync.Mutex
	transportRoutes map[Transport][]*Route
	routeStats map[Link]*RouteStats
	routeActive map[Link]bool
	routeWeight map[Link]float
}

func NewMultiRouteSelector(destinationId *ulid.ULID, weightedRoutes bool) *MultiRouteSelector {
	return &MultiRouteSelector{
		destinationId: destinationId,
		weightedRoutes: weightedRoutes,
		transportUpdate: NewMonitor(),
		done: make(chan struct{}),
		transportRoutes: map[Transport][]*Route{},
		routeStats: map[Link]*RouteStats{},
		routeActives: map[Link]bool{},
		routeWeights: map[Link]float{},
	}
}

func (self *MultiRouteSelector) getTransportStats(transport Transport) *RouteStats {
	currentRoutes, ok := self.transportRoutes[transport]
	if !ok {
		return nil
	}
	netStats := NewRouteStats()
	for _, currentRoute := range currentRoutes {
		stats := self.routeStats[currentRoute]
		netStats.sendCount += stats.sendCount
		netStats.sendByteCount += stats.sendByteCount
		netStats.receiveCount += stats.receiveCount
		netStats.receiveByteCount += stats.receiveByteCount
	}
	return netStats
}

// if weightedRoutes, this applies new priorities and weights. calling this resets all route stats.
// the reason to reset weightedRoutes is that the weight calculation needs to consider only the stats since the previous weight change
func (self *MultiRouteSelector) updateTransport(transport Transport, links []Link) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if routes == nil {
		currentRoutes, ok := self.transportRoutes[transport]
		if !ok {
			// transport not set
			return
		}
		for _, currentRoute := range currentRoutes {
			delete(routeStats, currentRoute)
			delete(routeActive, currentRoute)
			delete(routeWeights, currentRoute)
		}
		delete(self.transportRoutes, transport)
	} else {
		if currentRoutes, ok := self.transportRoutes[transport]; ok {
			for _, currentRoute := range currentRoutes {
				if slices.Index(routes, currentRoute) < 0 {
					// no longer present
					delete(routeStats, currentRoute)
					delete(routeActive, currentRoute)
					delete(routeWeights, currentRoute)
				}
			}
			for _, route := routes {
				if slices.Index(currentRoutes, route) < 0 {
					// new route
					self.routeActives[route] = true
				}
			}
		} else {
			for _, route := routes {
				// new route
				self.routeActives[route] = true
			}
		}
		// the following will be updated with the new routes in the weighting below
		// - routeStats
		// - routeActive
		// - routeWeights
		self.transportRoutes[transport] = routes
	}

	if self.weightedRoutes {
		updatedRouteWeight := map[Link]float{}

		transportStats := map[Transport]*RouteStats{}
		for transport, currentRoutes := range self.transportRoutes {
			netStats := NewRouteStats()
			for _, currentRoute := range currentRoutes {
				stats := self.routeStats[currentRoute]
				netStats.sendCount += stats.sendCount
				netStats.sendByteCount += stats.sendByteCount
				netStats.receiveCount += stats.receiveCount
				netStats.receiveByteCount += stats.receiveByteCount
			}
			transportStats[transport] = netStats
		}

		orderedTransports := maps.Keys(self.transportRoutes)
		// shuffle the same priority values
		rand.Shuffle(len(orderedTransports), func(i int, j int) {
			t := orderedTransports[i]
			orderedTransports[i] = orderedTransports[j]
			orderedTransports[j] = t
		})
		slices.SortStableFunction(orderedTransports, func(a Transport, b Transport)(bool) {
			return a.Priority() < b.Priority()
		})

		n := len(orderedTransports)

		allCanEval := true
		for i := 0; i < n; i += 1 {
			transport := orderedTransports[i]
			routeStats := transportStats[transport]
			remainingStats := map[Transport]*RouteStats{}
			for j := i + 1; j < n; j += 1 {
				remainingStats[orderedTransports[j]] = transportStats[orderedTransports[j]]
			}
			canEval := transport.CanEvalRouteWeight(routeStats, remainingStats)
			allCanEval &= canEval
		}

		if allCanEval {
			var allWeight float32
			allWeight = 1.0
			for i := 0; i < n; i += 1 {
				transport := orderedTransports[i]
				routeStats := transportStats[transport]
				remainingStats := map[Transport]*RouteStats{}
				for j := i + 1; j < n; j += 1 {
					remainingStats[orderedTransports[j]] = transportStats[orderedTransports[j]]
				}
				weight := transport.RouteWeight(routeStats, remainingStats)
				for _, route := range self.transportRoutes[transport] {
					updatedRouteWeight[route] = allWeight * weight
				}
				allWeight *= (1.0 - weight)
			}

			self.routeWeight = updatedRouteWeight

			updatedRouteStats := map[Link]*RouteStats{}
			for transport, currentRoutes := range self.transportRoutes {
				for _, currentRoute := range currentRoutes {
					// reset the stats
					updatedRouteStats[currentRoute] = NewRouteStats()
				}
			}
			self.routeStats = updatedRouteStats
		}
	}

	self.transportUpdate.notifyAll()
}

func (self *MultiRouteSelector) getActiveRoutes() []*Route {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	activeRoutes := []*Route{}
	for _, routes := range self.transportRoutes {
		if self.routeActive[route] {
			activeRoutes = append(activeRoutes, route)
		}
	}

	rand.Shuffle(len(activeRoutes), func(i int, j int) {
		t := activeRoutes[i]
		activeRoutes[i] = activeRoutes[j]
		activeRoutes[j] = t
	})

	if self.weightedRoutes {
		// prioritize the routes (weighted shuffle)
		// if all weights are equal, this is the same as a shuffle
		n := len(activeRoutes)
		for i := 0; i < n - 1; i += 1 {
			j := func ()(int) {
				var net float32
				net = 0
				for j := i; j < n; j += 1 {
					net += self.routeWeight(activeRoutes[j])
				}
				r := rand.Float32()
				rnet := r * net
				net = 0
				for j := i; j < n; j += 1 {
					net += self.routeWeight(activeRoutes[j])
					if rnet < net {
						return j
					}
				}
				panic()
			}
			t := activeRoutes[i]
			activeRoutes[i] = activeRoutes[j]
			activeRoutes[j] = t
		}
	}

	return activeRoutes
}

func (self *MultiRouteSelector) setActive(route Route, active bool) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if _, ok := routeActive[route]; ok {
		self.routeActive[route] = false
	}
}

func (self *MultiRouteSelector) updateSendStats(route Route, sendCount int, sendByteCount int) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if stats, ok := routeStats[route]; ok {
		stats.sendCount += sendCount
		stats.sendByteCount += sendByteCount
	}
}

func (self *MultiRouteSelector) updateReceiveStats(route Route, receiveCount int, receiveByteCount int) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if stats, ok := routeStats[route]; ok {
		stats.receiveCount += receiveCount
		stats.receiveByteCount += receiveByteCount
	}
}

// MultiRouteWriter
func (self *MultiRouteSelector) Write(ctx context.Context, transportFrameBytes []byte, timeout time.Duration) error {
	// write to the first channel available, in random priority
	enterTime := time.Now()
	for {
		notify := self.transportUpdate.NotifyChannel()
		activeRoutes := self.getActiveRoutes()

		// select cases are in order:
		// - ctx.Done
		// - done
		// - route writes...
		// - transport update
		// - timeout (may not exist)

		selectCases := make([]reflect.SelectCase, 0, 4 + len(activeRoutes))

		// add the context done case
		contextDoneIndex := len(selectCases)
		selectCases = append(selectCases, SelectCase{
			Dir: reflect.SelectRecv,
			Chan: reflect.ValueOf(self.ctx.Done),
		})

		// add the done case
		doneIndex := len(selectCases)
		selectCases = append(selectCases, SelectCase{
			Dir: reflect.SelectRecv,
			Chan: reflect.ValueOf(self.done),
		})

		// add all the route
		routeStartIndex := len(selectCases)
		if 0 < len(activeRoutes) {
			sendValue := reflect.ValueOf(transportFrameBytes)
			for _, route := range activeRoutes {
				selectCases = append(selectCases, SelectCase{
					Dir: reflect.SelectSend,
					Chan: reflect.ValueOf(route.link),
					Send: sendValue,
				})
			}
		}

		// add the update case
		transportUpdateIndex := len(selectCases)
		selectCases = append(selectCases, SelectCase{
			Dir: reflect.SelectRecv,
			Chan: reflect.ValueOf(notify),
		})

		timeoutIndex := len(selectCases)
		if 0 <= remainingTimeout {
			remainingTimeout := enterTime + timeout - time.Now()
			if remainingTimeout <= 0 {
				// add a default case
				selectCases = append(selectCases, SelectCase{
					Dir: reflect.SelectDefault,
				})
			} else {
				// add a timeout case
				selectCases = append(selectCases, SelectCase{
					Dir: reflect.SelectRecv,
					Chan: reflect.ValueOf(time.Afte(remainingTimeout)),
				})
			}
		}

		chosenIndex, _, ok := reflect.Select(selectCases)

		switch chosenIndex {
		case contextDoneIndex:
			return errors.New("Context done")
		case doneIndex:
			return errors.New("Done")
		case transportUpdateIndex:
			// new routes, try again
		case timeoutIndex:
			return errors.New("Timeout")
		default:
			// a route
			routeIndex := chosenIndex - routeStartIndex
			route := activeRoutes[routeIndex]
			if ok {
				self.updateSendStats(route.link, 1, len(transportFrameBytes))
				return nil
			} else {
				// mark the route as closed, try again
				self.setActive(route.link, false)
			}
		}
	}
}

// MultiRouteReader
func (self *MultiRouteSelector) Read(ctx context.Context, timeout time.Duration) ([]byte, error) {
	// read from the first channel available, in random priority
	enterTime := time.Now()
	for {
		notify := self.transportUpdate.NotifyChannel()
		activeRoutes := self.getActiveRoutes()
		
		// select cases are in order:
		// - ctx.Done
		// - done
		// - route reads...
		// - transport update
		// - timeout (may not exist)

		selectCases := make([]reflect.SelectCase, 0, 4 + len(activeRoutes))

		// add the context done case
		contextDoneIndex := len(selectCases)
		selectCases = append(selectCases, SelectCase{
			Dir: reflect.SelectRecv,
			Chan: reflect.ValueOf(self.ctx.Done),
		})

		// add the done case
		doneIndex := len(selectCases)
		selectCases = append(selectCases, SelectCase{
			Dir: reflect.SelectRecv,
			Chan: reflect.ValueOf(self.done),
		})

		// add all the route
		routeStartIndex := len(selectCases)
		if 0 < len(activeRoutes) {
			for _, route := range activeRoutes {
				selectCases = append(selectCases, SelectCase{
					Dir: reflect.SelectRecv,
					Chan: reflect.ValueOf(route.link),
				})
			}
		}

		// add the update case
		transportUpdateIndex := len(selectCases)
		selectCases = append(selectCases, SelectCase{
			Dir: reflect.SelectRecv,
			Chan: reflect.ValueOf(notify),
		})

		timeoutIndex := len(selectCases)
		if 0 <= remainingTimeout {
			remainingTimeout := enterTime + timeout - time.Now()
			if remainingTimeout <= 0 {
				// add a default case
				selectCases = append(selectCases, SelectCase{
					Dir: reflect.SelectDefault,
				})
			} else {
				// add a timeout case
				selectCases = append(selectCases, SelectCase{
					Dir: reflect.SelectRecv,
					Chan: reflect.ValueOf(time.Afte(remainingTimeout)),
				})
			}
		}

		chosenIndex, value, ok := reflect.Select(selectCases)

		switch chosenIndex {
		case contextDoneIndex:
			return nil, errors.New("Context done")
		case doneIndex:
			return nil, errors.New("Done")
		case transportUpdateIndex:
			// new routes, try again
		case timeoutIndex:
			return nil, errors.New("Timeout")
		default:
			// a route
			routeIndex := chosenIndex - routeStartIndex
			route := activeRoutes[routeIndex]
			if ok {
				transportFrameBytes := value.([]byte)
				self.updateReceiveStats(route.link, 1, len(transportFrameBytes))
				return transportFrameBytes, nil
			} else {
				// mark the route as closed, try again
				self.setActive(route.link, false)
			}
		}
	}
}

func (self *MultiRouteSelector) Close() {
	defer recover()
	close(self.done)
}


type RouteStats struct {
	sendCount int
	sendByteCount int
	receiveCount int
	receiveByteCount int
}

func NewRouteStats() *RouteStats {
	return &RouteStats{
		sendCount: 0,
		sendByteCount: 0,
		receiveCount: 0,
		receiveByteCount: 0,
	}
}


type ContractErrorFunction = func()


type ContractManagerSettings struct {
	standardTransferBytes int
}


type ContractManager struct {
	client *Client

	contractManagerSettings *ContractManagerSettings

	mutex sync.Mutex

	provideSecretKeys map[*protocol.ProvideMode][]byte

	destinationContracts map[*ulid.ULID]*ContractQueue
	
	receiveNoContractClientIds map[*ULID]bool
	sendNoContractClientIds map[*ULID]bool

	contractErrorCallbacks CallbackList[ContractErrorFunction]
}

func NewContractManager(client *Client, contractSettings *ContractSettings) {
	// at a minimum messages to/from the platform (CONTROL_ID) do not need a contract
	// this is because the platform is needed to create contracts
	receiveNoContractClientIds := map[*ULID]bool{
		CONTROL_ID: true,
	}
	sendNoContractClientIds := map[*ULID]bool{
		CONTROL_ID: true,
	}

	contractManager := &ContractManager{
		client: client,
		contractManagerSettings: DefaultContractManagerSettings(),
		providerSecretKeys: map[*protocol.ProvideMode][]byte{},
		destinationContracts: map[*ulid.ULID][]*protocol.Contract{},
		contractUpdate: NewMonitor(),
		receiveNoContractClientIds: receiveNoContractClientIds,
		sendNoContractClientIds: sendNoContractClientIds,
		contractErrorCallbacks: NewCallbackList[ContractErrorFunction](),
	}

	client.addReceiveCallback(contractManager.receive)

	return contractManager
}

func (self *ContractManager) StandatdTransferBytes() int {
	return self.contractManagerSettings.standardTransferBytes
}

func (self *ContractManager) addContractErrorCallback(contractErrorCallback ContractErrorFunction) {
	self.contractErrorCallbacks.add(contractErrorCallback)
}

func (self *ContractManager) removeContractErrorCallback(contractErrorCallback ContractErrorFunction) {
	self.contractErrorCallbacks.remove(contractErrorCallback)
}

// ReceiveFunction
func (self *ContractManager) receive(sourceId *ulid.ULID, frames []*protocol.Frame, provideMode *protocol.ProvideMode, err error) {
	if CONTROL_ID.Equals(sourceId) {
		for _, frame := range frames {
			switch frame.MessageType() {
			case protocol.MessageType.CREATE_CONTRACT_RESULT:
				var createContractResult CreateContractResult
				if err := protobuf.Unmarshal(frame.MessageBytes(), &createContractResult); err != nil {
					if contractError := createContractResult.Error(); contractError != nil {
						self.error(contractError)
					} else contract := createContractResult.Contract(); contract != nil {
						self.addContract(contract)
					}
				}
			}
		}
	}
}

// ContractErrorFunction
func (self *ContractManager) error(contractError *protocol.ContractError) {
	for _, contractErrorCallback := range self.contractErrorCallbacks.get() {
		func() {
			defer recover()
			contractErrorCallback(contractError)
		}()
	}
}

func (self *ContractManager) setProvideModes(provideModes map[*protocol.ProvideMode]bool) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	currentProvideModes := map.Keys(self.provideSecretKeys)
	for _, provideMode := range currentProvideModes {
		if allow, ok := provideModes[provideMode]; !ok || !allow {
			delete(self.provideSecretKeys, provideMode)
		}
	}

	for provideMode, allow := range provideModes {
		if allow {
			if provideSecretKey, ok := self.provideSecretKeys[provideMode]; !ok {
				// generate a new key
				provideSecretKey = make([]byte, 32)
		    	_, err = rand.Read(provideSecretKey)
		    	if err != nil {
		    		panic(err)
		    	}
				self.provideSecretKeys[provideMode] = provideSecretKey
			}
		}
	}

	provideKeys := []*protocol.ProvideKey{}
	for provideMode, provideSecretKey := range self.provideSecretKeys {
		provideKeys = append(provideKeys, &protocol.ProvideKey{
			Mode: provideMode,
			ProvideSecretKey: provideSecretKey,
		})
	}
	provide := &protocol.Provide{
		Keys: provideKeys,
	}
	self.client.SendControl(ToFrame(provide), nil)
}

func (self *ContractManager) Verify(storedContractHmac []byte, storedContractBytes []byte, provideMode *protocol.ProvideMode) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	provideSecretKey, ok := self.provideSecretKeys[provideMode]
	if !ok {
		// provide mode is not enabled
		return false
	}

	mac := hmac.New(sha256.New, key)
	expectedHmac := mac.Sum(storedContractBytes)
	return hmac.Equal(storedContractHmac, expectedHmac)
}

func (self *ContractManager) sendNoContract(destinationId *ulid.ULID) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if allow, ok := self.sendNoContractClientIds[destinationId]; ok {
		return allow
	}
	return false
}

func (self *ContractManager) receiveNoContract(sourceId *ulid.ULID) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if allow, ok := self.receiveNoContractClientIds[sourceId]; ok {
		return allow
	}
	return false
}

func (self *ContractManager) takeContract(ctx context.Context, destinationId *ulid.ULID, timeout time.Duration) (*protocol.Contract, error) {
	contractQueue := openContractQueue(destionationId)
	defer closeContractQueue(destinationId)

	enterTime := time.Now()
	for {
		notify := contractQueue.updateMonitor.NotifyChannel()
		contract := contractQueue.poll()

		if contract != nil {
			return contract
		}

		if timeout < 0 {
			select {
			case ctx.Done:
				return nil
			case notify:
			}
		} else {
			remainingTimeout := enterTime + timeout - time.Now()
			if remainingTimeout <= 0 {
				return nil
			} else {
				select {
				case ctx.Done:
					return nil
				case notify:
				case time.After(remainingTimeout):
					return nil
				}
			}
		}
	}
}

func (self *ContractManager) addContract(contract *protocol.Contract) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	destinationId := UlidFromProto(contract.GetDestinationId())

	contractQueue := openContractQueue(destionationId)
	defer closeContractQueue(destinationId)

	contractQueue.add(contract)
}

func (self *ContractManager) openContractQueue(destionationId *ulid.ULID) *protocol.ContractQueue {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	contractQueue, ok := self.destinationContracts[destinationId]
	if !ok {
		contractQueue = NewContractQueue()
		self.destinationContracts[destinationId] = contractQueue
	}
	contractQueue.open()

	return contractQueue
}

func (self *ContractManager) closeContractQueue(destinationId *ulid.ULID) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	contractQueue, ok := self.destinationContracts[destinationId]
	if !ok {
		panic("Open and close must be equally paired")
	}
	contractQueue.close()
	if contractQueue.empty() {
		delete(self.destinationContracts, destinationId)
	}
}

func (self *ContractManager) CreateContract(destinationId *ulid.ULID) {
	// look at destinationContracts and last contract to get previous contract id
	createContract := CreateContract{
		destionationId: []byte(destinationId),
		transferByteCount: self.contractManagerSettings.transferByteCount,
	}
	self.client.Send(createContract)
}

func (self *ContractManager) Complete(contractId *ulid.ULID, ackedByteCount int, unackedByteCount int) {
	closeContract := CloseContract{
		ContractId: []byte(contractId),
		AckedByteCount: ackedByteCount,
		UnackedByteCount: unackedByteCount,
	}
	self.client.SendControl(ToFrame(closeContract), nil)
}

func (self *ContractManager) Close() {
	// pending contracts will just timeout on the platform
	self.client.removeReceiveCallback(self.receive)
}


type ContractQueue struct {
	updateMonitor *Monitor

	mutex sync.Mutex
	openCount int
	contracts []*protocol.Contract
}

func NewContractQueue() *ContractQueue {
	return &ContractQueue{
		updateMonitor: NewMonitor(),
		openCount: 0,
		contracts: []*protocol.Contract{},
	}
}

func (self *ContractQueue) open() {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.openCount += 1
}

func (self *ContractQueue) close() {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.openCount -= 1
}

func (self *ContractQueue) poll() *protocol.Contract {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if len(self.contracts) == 0 {
		return nil
	}

	contract := self.contracts[0]
	self.contracts[0] = nil
	self.contracts = self.contracts[1:]
	return contract
}

func (self *ContractQueue) add(contract *protocol.Contract) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.contracts = append(self.contracts, contract)

	self.updateMonitor.notifyAll()
}

func (self *ContractQueue) empty() bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	return 0 == self.openCount && 0 == len(self.contracts)
}
