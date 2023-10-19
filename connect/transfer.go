package connect

import (
	"context"
	"time"
	"sync"
	"errors"
	"container/heap"
	"sort"
	"math"
	"math/rand"
	"reflect"
	"crypto/hmac"
	"crypto/sha256"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

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
- return transfer is accounted to the sender
- support for multiple routes to the destination
- senders are verified with pre-exchanged keys
- high throughput and bounded resource usage

*/

/*
Each transport should apply the forwarding ACL:
- reject if source id does not match network id
- reject if not an active contract between sender and receiver

*/


// The transfer speed of each client is limited by its slowest destination.
// All traffic is multiplexed to a single connection, and blocking
// the connection ultimately limits the rate of `SendWithTimeout`.
// In this a client is similar to a socket. Multiple clients
// can be active in parallel, each limited by their slowest destination.


type AckFunction = func(err error)
// provideMode is the mode of where these frames are from: network, friends and family, public
// provideMode nil means no contract
type ReceiveFunction = func(sourceId Id, frames []*protocol.Frame, provideMode protocol.ProvideMode)
type ForwardFunction = func(sourceId Id, destinationId Id, transferFrameBytes []byte)
type ContractErrorFunction = func(protocol.ContractError)


// destination id for control messages
var ControlId = Id([]byte{
	0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00,
})


// in this case there are no intermediary hops
// the contract is signed with the local provide keys
var DirectStreamId = Id([]byte{
	0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00,
})


const DefaultClientBufferSize = 32


func DefaultSendBufferSettings() *SendBufferSettings {
	return &SendBufferSettings{
		ContractTimeout: 30 * time.Second,
		ContractRetryInterval: 5 * time.Second,
		ResendInterval: 2 * time.Second,
		AckTimeout: 30 * time.Second,
		IdleTimeout: 120 * time.Second,
		SequenceBufferSize: 30,
		ResendQueueMaxByteCount: 1024 * 1024,
		MinMessageByteCount: 1,
	}
}


func DefaultReceiveBufferSettings() *ReceiveBufferSettings {
	return &ReceiveBufferSettings {
		AckInterval: 5 * time.Second,
		GapTimeout: 30 * time.Second,
		IdleTimeout: 120 * time.Second,
		SequenceBufferSize: 30,
		ReceiveQueueMaxByteCount: 1024 * 1024,
		MinMessageByteCount: 1,
		ResendAbuseThreshold: 4,
		ResendAbuseMultiple: 0.5,
		MaxPeerAuditDuration: 60 * time.Second,
	}
}


func DefaultForwardBufferSettings() *ForwardBufferSettings {
	return &ForwardBufferSettings {
		IdleTimeout: 120 * time.Second,
		SequenceBufferSize: 30,
	}
}


func DefaultContractManagerSettings() *ContractManagerSettings {
	return &ContractManagerSettings{
		StandardTransferByteCount: 8 * 1024 * 1024 * 1024,
	}
}


// comparable
type TransferPath struct {
	source Path
	destination Path
}


// comparable
type Path struct {
	ClientId Id
	StreamId Id
}


type SendPack struct {
	// frame and destination is repacked by the send buffer into a Pack,
	// with destination and frame from the tframe, and other pack properties filled in by the buffer
	Frame *protocol.Frame
	DestinationId Id
	// called (true) when the pack is ack'd, or (false) if not ack'd (closed before ack)
	AckCallback AckFunction
	MessageByteCount int
}


type ReceivePack struct {
	SourceId Id
	Pack *protocol.Pack
	ReceiveCallback ReceiveFunction
	MessageByteCount int
}


type ForwardPack struct {
	DestinationId Id
	TransferFrameBytes []byte
}


// note all callbacks are wrapped to check for nil and recover from errors
type Client struct {
	clientId Id

	// TODO support the context deadline
	ctx context.Context
	cancel context.CancelFunc

	sendBufferSettings *SendBufferSettings
	receiveBufferSettings *ReceiveBufferSettings
	forwardBufferSettings *ForwardBufferSettings

	sendPacks chan *SendPack
	forwardPacks chan *ForwardPack

	receiveCallbacks *CallbackList[ReceiveFunction]
	forwardCallbacks *CallbackList[ForwardFunction]
}

func NewClientWithDefaults(clientId Id, ctx context.Context) *Client {
	return NewClient(clientId, ctx, DefaultClientBufferSize)
}

func NewClient(clientId Id, ctx context.Context, clientBufferSize int) *Client {
	return &Client{
		clientId: clientId,
		ctx: ctx,
		sendBufferSettings: DefaultSendBufferSettings(),
		receiveBufferSettings: DefaultReceiveBufferSettings(),
		forwardBufferSettings: DefaultForwardBufferSettings(),
		sendPacks: make(chan *SendPack, clientBufferSize),
		forwardPacks: make(chan *ForwardPack, clientBufferSize),
		receiveCallbacks: NewCallbackList[ReceiveFunction](),
		forwardCallbacks: NewCallbackList[ForwardFunction](),
	}
}

func (self *Client) ClientId() Id {
	return self.clientId
}

func (self *Client) ForwardWithTimeout(transferFrameBytes []byte, timeout time.Duration) bool {
	var filteredTransferFrame protocol.FilteredTransferFrame
	if err := proto.Unmarshal(transferFrameBytes, &filteredTransferFrame); err != nil {
		// bad protobuf
		return false
	}
	destinationId, err := IdFromBytes(filteredTransferFrame.GetDestinationId())
	if err != nil {
		// bad protobuf
		return false
	}

	forwardPack := &ForwardPack{
		DestinationId: destinationId,
		TransferFrameBytes: transferFrameBytes,
	}
	if timeout < 0 {
		self.forwardPacks <- forwardPack
		return true
	} else if 0 == timeout {
		select {
		case self.forwardPacks <- forwardPack:
			return true
		default:
			// full
			return false
		}
	} else {
		select {
		case <- self.ctx.Done():
			return false
		case self.forwardPacks <- forwardPack:
			return true
		case <- time.After(timeout):
			// full
			return false
		}
	}
}

func (self *Client) Forward(transferFrameBytes []byte) {
	self.ForwardWithTimeout(transferFrameBytes, -1)
}

func (self *Client) SendWithTimeout(frame *protocol.Frame, destinationId Id, ackCallback AckFunction, timeout time.Duration) bool {
	safeAckCallback := func(err error) {
		if ackCallback != nil {
			defer recover()
			ackCallback(err)
		}
	}
	messageByteCount := len(frame.MessageBytes)
	sendPack := &SendPack{
		Frame: frame,
		DestinationId: destinationId,
		AckCallback: safeAckCallback,
		MessageByteCount: messageByteCount,
	}
	if timeout < 0 {
		self.sendPacks <- sendPack
		return true
	} else if 0 == timeout {
		select {
		case self.sendPacks <- sendPack:
			return true
		default:
			// full
			safeAckCallback(errors.New("Send buffer full."))
			return false
		}
	} else {
		select {
		case <- self.ctx.Done():
			return false
		case self.sendPacks <- sendPack:
			return true
		case <- time.After(timeout):
			// full
			safeAckCallback(errors.New("Send buffer full."))
			return false
		}
	}
}

func (self *Client) SendControlWithTimeout(frame *protocol.Frame, ackCallback AckFunction, timeout time.Duration) bool {
	return self.SendWithTimeout(frame, ControlId, ackCallback, timeout)
}

func (self *Client) Send(frame *protocol.Frame, destinationId Id, ackCallback AckFunction) {
	self.SendWithTimeout(frame, destinationId, ackCallback, -1)
}

func (self *Client) SendControl(frame *protocol.Frame, ackCallback AckFunction) {
	self.Send(frame, ControlId, ackCallback)
}

// ReceiveFunction
func (self *Client) receive(sourceId Id, frames []*protocol.Frame, provideMode protocol.ProvideMode) {
	for _, receiveCallback := range self.receiveCallbacks.Get() {
		func() {
			defer recover()
			receiveCallback(sourceId, frames, provideMode)
		}()
	}
}

// ForwardFunction
func (self *Client) forward(sourceId Id, destinationId Id, transferFrameBytes []byte) {
	for _, forwardCallback := range self.forwardCallbacks.Get() {
		func() {
			defer recover()
			forwardCallback(sourceId, destinationId, transferFrameBytes)
		}()
	}
}

func (self *Client) AddReceiveCallback(receiveCallback ReceiveFunction) {
	self.receiveCallbacks.Add(receiveCallback)
}

func (self *Client) RemoveReceiveCallback(receiveCallback ReceiveFunction) {
	self.receiveCallbacks.Remove(receiveCallback)
}

func (self *Client) AddForwardCallback(forwardCallback ForwardFunction) {
	self.forwardCallbacks.Add(forwardCallback)
}

func (self *Client) RemoveForwardCallback(forwardCallback ForwardFunction) {
	self.forwardCallbacks.Remove(forwardCallback)
}

func (self *Client) Run(routeManager *RouteManager, contractManager *ContractManager) {
	defer func() {
		close(self.sendPacks)
		for {
			select {
			case sendPack, ok := <- self.sendPacks:
				if !ok {
					return
				}
				sendPack.AckCallback(errors.New("Client closed."))
			}
		}
	}()

	sendBuffer := NewSendBuffer(self.ctx, self, routeManager, contractManager, self.sendBufferSettings)
	defer sendBuffer.Close()
	receiveBuffer := NewReceiveBuffer(self.ctx, self, routeManager, contractManager, self.receiveBufferSettings)
	defer receiveBuffer.Close()
	forwardBuffer := NewForwardBuffer(self.ctx, self, routeManager, contractManager, self.forwardBufferSettings)
	defer forwardBuffer.Close()

	// receive
	go func() {
		multiRouteReader := routeManager.OpenMultiRouteReader(self.clientId)
		defer routeManager.CloseMultiRouteReader(multiRouteReader)

		updatePeerAudit := func(sourceId Id, callback func(*PeerAudit)) {
			// immediately send peer audits at this level
			peerAudit := NewSequencePeerAudit(self, sourceId, 0)
			peerAudit.Update(callback)
			peerAudit.Complete()
		}

		for {
			transferFrameBytes, err := multiRouteReader.Read(self.ctx, -1)
			if err != nil {
				break
			}
			// at this point, the route is expected to have already parsed the transfer frame
			// and applied basic validation and source/destination checks
			// because of this, errors in parsing the `FilteredTransferFrame` are not expected
			// decode a minimal subset of the full message needed to make a routing decision
			var filteredTransferFrame protocol.FilteredTransferFrame
			if err := proto.Unmarshal(transferFrameBytes, &filteredTransferFrame); err != nil {
				// bad protobuf (unexpected, see route note above)
				continue
			}
			sourceId, err := IdFromBytes(filteredTransferFrame.GetSourceId())
			if err != nil {
				// bad protobuf (unexpected, see route note above)
				continue
			}
			destinationId, err := IdFromBytes(filteredTransferFrame.GetDestinationId())
			if err != nil {
				// bad protobuf (unexpected, see route note above)
				continue
			}
			if destinationId == self.clientId {
				// the transports have typically not parsed the full `TransferFrame`
				// on error, discard the message and report the peer
				var transferFrame protocol.TransferFrame
				if err := proto.Unmarshal(transferFrameBytes, &transferFrame); err != nil {
					// bad protobuf
					updatePeerAudit(sourceId, func(a *PeerAudit) {
						a.badMessage(len(transferFrameBytes))
					})
					continue
				}
				frame := transferFrame.GetFrame()

				// TODO apply source verification+decryption with pke

				switch frame.GetMessageType() {
				case protocol.MessageType_TransferAck:
					var ack protocol.Ack
					if err := proto.Unmarshal(frame.GetMessageBytes(), &ack); err != nil {
						// bad protobuf
						updatePeerAudit(sourceId, func(a *PeerAudit) {
							a.badMessage(len(transferFrameBytes))
						})
						continue
					}
					sendBuffer.Ack(sourceId, &ack)
				case protocol.MessageType_TransferPack:
					var pack protocol.Pack
					if err := proto.Unmarshal(frame.GetMessageBytes(), &pack); err != nil {
						// bad protobuf
						updatePeerAudit(sourceId, func(a *PeerAudit) {
							a.badMessage(len(transferFrameBytes))
						})
						continue
					}
					messageByteCount := 0
					for _, frame := range pack.Frames {
						messageByteCount += len(frame.MessageBytes)
					}
					receiveBuffer.Pack(&ReceivePack{
						SourceId: sourceId,
						Pack: &pack,
						ReceiveCallback: self.receive,
						MessageByteCount: messageByteCount,
					})
				default:
					updatePeerAudit(sourceId, func(a *PeerAudit) {
						a.badMessage(len(transferFrameBytes))
					})
				}
			} else {
				self.forward(sourceId, destinationId, transferFrameBytes)
			}
		}
	}()

	// forward
	go func() {
		for {
			select {
			case <- self.ctx.Done():
				return
			case forwardPack := <- self.forwardPacks:
				forwardBuffer.Pack(forwardPack)
			}
		}
	}()

	// send
	for {
		select {
		case <- self.ctx.Done():
			return
		case sendPack := <- self.sendPacks:
			if sendPack.DestinationId == self.clientId {
				// loopback
				func() {
					defer func() {
						if err := recover(); err != nil {
							sendPack.AckCallback(err.(error))
						}
					}()
					self.receive(
						self.clientId,
						[]*protocol.Frame{sendPack.Frame},
						protocol.ProvideMode_Network,
					)
					sendPack.AckCallback(nil)
				}()
			} else {
				sendBuffer.Pack(sendPack)
			}
		}
	}
}

func (self *Client) Close() {
	self.cancel()
}


type SendBufferSettings struct {
	ContractTimeout time.Duration
	ContractRetryInterval time.Duration

	// TODO replace this with round trip time estimation
	// resend timeout is the initial time between successive send attempts. Does linear backoff
	ResendInterval time.Duration

	// on ack timeout, no longer attempt to retransmit and notify of ack failure
	AckTimeout time.Duration
	IdleTimeout time.Duration

	SequenceBufferSize int

	ResendQueueMaxByteCount int

	MinMessageByteCount int
}


type SendBuffer struct {
	ctx context.Context
	client *Client
	routeManager *RouteManager
	contractManager *ContractManager
	
	sendBufferSettings *SendBufferSettings

	mutex sync.Mutex
	// destination id -> send sequence
	sendSequences map[Id]*SendSequence
}

func NewSendBuffer(ctx context.Context,
		client *Client,
		routeManager *RouteManager,
		contractManager *ContractManager,
		sendBufferSettings *SendBufferSettings) *SendBuffer {
	return &SendBuffer{
		ctx: ctx,
		client: client,
		routeManager: routeManager,
		contractManager: contractManager,
		sendBufferSettings: sendBufferSettings,
		sendSequences: map[Id]*SendSequence{},
	}
}

func (self *SendBuffer) Pack(sendPack *SendPack) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	initSendSequence := func()(*SendSequence) {
		sendSequence, ok := self.sendSequences[sendPack.DestinationId]
		if ok {
			return sendSequence
		}
		sendSequence = NewSendSequence(
			self.ctx,
			self.client,
			self.routeManager,
			self.contractManager,
			sendPack.DestinationId,
			self.sendBufferSettings,
		)
		self.sendSequences[sendPack.DestinationId] = sendSequence
		go func() {
			sendSequence.Run()

			self.mutex.Lock()
			defer self.mutex.Unlock()
			// clean up
			if sendSequence == self.sendSequences[sendPack.DestinationId] {
				delete(self.sendSequences, sendPack.DestinationId)
			}
		}()
		return sendSequence
	}

	if !initSendSequence().Pack(sendPack) {
		// sequence closed
		delete(self.sendSequences, sendPack.DestinationId)
		initSendSequence().Pack(sendPack)
	}
}

func (self *SendBuffer) Ack(sourceId Id, ack *protocol.Ack) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	sendSequence, ok := self.sendSequences[sourceId]
	if !ok {
		// ignore
		return
	}

	sendSequence.Ack(ack)
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
	ctx context.Context
	cancel context.CancelFunc

	client *Client
	routeManager *RouteManager
	contractManager *ContractManager
	destinationId Id

	sendBufferSettings *SendBufferSettings

	sendContract *sequenceContract
	sendContracts map[Id]*sequenceContract

	packs chan *SendPack
	acks chan *protocol.Ack

	resendQueue *resendQueue
	sendItems []*sendItem
	nextSequenceId uint64

	idleCondition *IdleCondition

	multiRouteWriter MultiRouteWriter
}

func NewSendSequence(
		ctx context.Context,
		client *Client,
		routeManager *RouteManager,
		contractManager *ContractManager,
		destinationId Id,
		sendBufferSettings *SendBufferSettings) *SendSequence {
	cancelCtx, cancel := context.WithCancel(ctx)

	return &SendSequence{
		ctx: cancelCtx,
		cancel: cancel,
		client: client,
		routeManager: routeManager,
		contractManager: contractManager,
		destinationId: destinationId,
		sendBufferSettings: sendBufferSettings,
		sendContract: nil,
		sendContracts: map[Id]*sequenceContract{},
		packs: make(chan *SendPack, sendBufferSettings.SequenceBufferSize),
		acks: make(chan *protocol.Ack, sendBufferSettings.SequenceBufferSize),
		resendQueue: newResendQueue(),
		sendItems: []*sendItem{},
		nextSequenceId: 0,
		idleCondition: NewIdleCondition(),
	}
}

func (self *SendSequence) Pack(sendPack *SendPack) (success bool) {
	if !self.idleCondition.UpdateOpen() {
		success = false
		return
	}
	defer self.idleCondition.UpdateClose()
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
	if !self.idleCondition.UpdateOpen() {
		success = false
		return
	}
	defer self.idleCondition.UpdateClose()
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

func (self *SendSequence) Run() {
	defer func() {
		self.Close()

		close(self.packs)
		close(self.acks)

		// close contract
		for _, sendContract := range self.sendContracts {
			self.contractManager.Complete(
				sendContract.contractId,
				sendContract.ackedByteCount,
				sendContract.unackedByteCount,
			)
		}

		// drain the buffer
		for _, item := range self.resendQueue.orderedItems {
			item.ackCallback(errors.New("Closed"))
		}

		// drain the packs
		func() {
			for {
				select {
				case sendPack, ok := <- self.packs:
					if !ok {
						return
					}
					sendPack.AckCallback(errors.New("Closed"))
				}
			}
		}()
	}()

	self.multiRouteWriter = self.routeManager.OpenMultiRouteWriter(self.destinationId)
	defer self.routeManager.CloseMultiRouteWriter(self.multiRouteWriter)

	for {
		sendTime := time.Now()
		var timeout time.Duration

		if self.resendQueue.Len() == 0 { 
			timeout = self.sendBufferSettings.IdleTimeout
		} else {
			timeout = self.sendBufferSettings.AckTimeout

			for 0 < self.resendQueue.Len() {
				item := self.resendQueue.removeFirst()

				itemAckTimeout := item.sendTime.Add(self.sendBufferSettings.AckTimeout).Sub(sendTime)

				if itemAckTimeout <= 0 {
					// message took too long to ack
					// close the sequence
					return
				}

				if sendTime.Before(item.resendTime) {
					// put back on the queue to send later
					self.resendQueue.add(item)
					itemResendTimeout := item.resendTime.Sub(sendTime)
					if itemResendTimeout < timeout {
						timeout = itemResendTimeout
					}
					if itemAckTimeout < timeout {
						timeout = itemAckTimeout
					}
					break
				}

				// resend
				var transferFrameBytes []byte
				if self.sendItems[0].sequenceId == item.sequenceId && !item.head {
					// set `first=true`
					transferFrameBytes = self.setHead(item.transferFrameBytes)
					
				} else {
					transferFrameBytes = item.transferFrameBytes
				}
				err := self.multiRouteWriter.Write(self.ctx, transferFrameBytes, -1)
				if err != nil {
					// close sequence
					return
				}
				item.sendCount += 1
				// linear backoff
				itemResendTimeout := self.sendBufferSettings.ResendInterval * time.Duration(item.sendCount)
				if itemResendTimeout < itemAckTimeout {
					item.resendTime = sendTime.Add(itemResendTimeout)
				} else {
					item.resendTime = sendTime.Add(itemAckTimeout)
				}
				self.resendQueue.add(item)
			}
		}

		if self.sendBufferSettings.ResendQueueMaxByteCount <= self.resendQueue.byteCount {
			// wait for acks
			select {
			case <- self.ctx.Done():
				return
			case ack := <- self.acks:
				messageId, err := IdFromBytes(ack.MessageId)
				if err != nil {
					// bad message
					// close sequence
					return
				}
				self.receiveAck(messageId)
			case <- time.After(timeout):
				// resend
			}
		} else {
			checkpointId := self.idleCondition.Checkpoint()
			select {
			case <- self.ctx.Done():
				return
			case ack := <- self.acks:
				messageId, err := IdFromBytes(ack.MessageId)
				if err != nil {
					// bad message
					// close sequence
					return
				}
				self.receiveAck(messageId)
			case sendPack := <- self.packs:
				if sendPack.MessageByteCount < self.sendBufferSettings.MinMessageByteCount {
					// bad message
					// close the sequence
					return
				}
				if self.updateContract(len(sendPack.Frame.MessageBytes)) {
					err := self.send(sendPack.Frame, sendPack.AckCallback)
					if err != nil {
						// close the sequence
						return
					}
				} else {
					// no contract
					// close the sequence
					sendPack.AckCallback(errors.New("No contract"))
					return
				}
			case <- time.After(timeout):
				if 0 == self.resendQueue.Len() {
					// idle timeout
					if self.idleCondition.Close(checkpointId) {
						// close the sequence
						return
					}
					// else there are pending updates
				}
			}
		}
	}
}

func (self *SendSequence) updateContract(messageByteCount int) bool {
	// `sendNoContract` is a mutual configuration 
	// both sides must configure themselves to require no contract from each other
	if self.contractManager.SendNoContract(self.destinationId) {
		return true
	}
	if self.sendContract != nil {
		if self.sendContract.update(messageByteCount) {
			return true
		} else {
			self.contractManager.Complete(
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
	maxContractMessageByteCount := 256

	if self.contractManager.StandardTransferByteCount() < messageByteCount + maxContractMessageByteCount {
		// this pack does not fit into a standard contract
		// TODO allow requesting larger contracts
		return false
	}

	next := func(contract *protocol.Contract)(bool) {
		sendContract, err := newSequenceContract(contract)
		if err != nil {
			// malformed, drop
			return false
		}

		contractMessageBytes, _ := proto.Marshal(contract)

		if len(contractMessageBytes) < self.sendBufferSettings.MinMessageByteCount {
			panic("Contract does not meet the minimum message size requirement.")
		}

		if maxContractMessageByteCount < len(contractMessageBytes) {
			panic("Bad estimate for contract max size could result in infinite contract retries.")
		}

		if sendContract.update(messageByteCount + len(contractMessageBytes)) {
			self.sendContract = sendContract
			self.sendContracts[sendContract.contractId] = sendContract

			// append the contract to the sequence
			err := self.send(&protocol.Frame{
				MessageType: protocol.MessageType_TransferContract,
				MessageBytes: contractMessageBytes,
			}, nil)
			return err == nil
		} else {
			// this contract doesn't fit the message
			// just close it since it was never send to the other side
			self.contractManager.Complete(sendContract.contractId, 0, 0)
			return false
		}
	}

	endTime := time.Now().Add(self.sendBufferSettings.ContractTimeout)
	for {
		if nextContract := self.contractManager.TakeContract(self.ctx, self.destinationId, 0); nextContract != nil {
			// async queue up the next contract
			self.contractManager.CreateContract(self.destinationId)
			if next(nextContract) {
				return true
			}
		}

		self.contractManager.CreateContract(self.destinationId)

		timeout := endTime.Sub(time.Now())
		if timeout <= 0 {
			return false
		}

		if self.sendBufferSettings.ContractRetryInterval < timeout {
			timeout = self.sendBufferSettings.ContractRetryInterval
		}

		if nextContract := self.contractManager.TakeContract(self.ctx, self.destinationId, timeout); nextContract != nil {
			// async queue up the next contract
			self.contractManager.CreateContract(self.destinationId)
			if next(nextContract) {
				return true
			}
		}
	}
}

func (self *SendSequence) send(frame *protocol.Frame, ackCallback AckFunction) error {
	sendTime := time.Now()
	messageId := Id(ulid.Make())
	sequenceId := self.nextSequenceId
	contractId := self.sendContract.contractId
	head := 0 == len(self.sendItems)

	self.nextSequenceId += 1

	pack := &protocol.Pack{
		MessageId: messageId.Bytes(),
		SequenceId: sequenceId,
		Head: head,
		Frames: []*protocol.Frame{frame},
	}

	packBytes, _ := proto.Marshal(pack)

	clientId := self.client.ClientId()
	transferFrame := &protocol.TransferFrame{
		TransferPath: &protocol.TransferPath{
			DestinationId: self.destinationId.Bytes(),
			SourceId: clientId.Bytes(),
			StreamId: DirectStreamId.Bytes(),
		},
		Frame: &protocol.Frame{
			MessageType: protocol.MessageType_TransferPack,
			MessageBytes: packBytes,
		},
	}

	transferFrameBytes, _ := proto.Marshal(transferFrame)

	item := &sendItem{
		messageId: messageId,
		contractId: contractId,
		sequenceId: sequenceId,
		sendTime: sendTime,
		resendTime: sendTime.Add(self.sendBufferSettings.ResendInterval),
		sendCount: 1,
		head: head,
		transferFrameBytes: transferFrameBytes,
		ackCallback: ackCallback,
	}

	self.sendItems = append(self.sendItems, item)
	self.resendQueue.add(item)

	return self.multiRouteWriter.Write(self.ctx, transferFrameBytes, -1)
}

func (self *SendSequence) setHead(transferFrameBytes []byte) []byte {
	// TODO this could avoid the memory copy by modifying the raw bytes
	// TODO this is expected to be done infrequently if resends are infrequent, so not an issue

	var transferFrame protocol.TransferFrame
	proto.Unmarshal(transferFrameBytes, &transferFrame)

	var pack protocol.Pack
	proto.Unmarshal(transferFrame.Frame.MessageBytes, &pack)

	pack.Head = true

	packBytes, _ := proto.Marshal(&pack)
	transferFrame.Frame.MessageBytes = packBytes

	transferFrameBytesWithHead, _ := proto.Marshal(&transferFrame)

	return transferFrameBytesWithHead
}

func (self *SendSequence) receiveAck(messageId Id) {
	item, ok := self.resendQueue.messageItems[messageId]
	if !ok {
		// message not pending ack
		return
	}
	// acks are cumulative
	// implicitly ack all earlier items in the sequence
	i := 0
	for ; i < len(self.sendItems); i += 1 {
		implicitItem := self.sendItems[i]
		if item.sequenceId < implicitItem.sequenceId {
			break
		}
		self.resendQueue.remove(implicitItem.messageId)
		implicitItem.ackCallback(nil)

		sendContract := self.sendContracts[implicitItem.contractId]
		sendContract.ack(len(implicitItem.transferFrameBytes))
		if sendContract.unackedByteCount == 0 {
			self.contractManager.Complete(
				sendContract.contractId,
				sendContract.ackedByteCount,
				sendContract.unackedByteCount,
			)
		}
		self.sendItems[i] = nil
	}
	self.sendItems = self.sendItems[i:]
}

func (self *SendSequence) Close() {
	self.cancel()
}


type sendItem struct {
	messageId Id
	contractId Id
	sequenceId uint64
	head bool
	sendTime time.Time
	resendTime time.Time
	sendCount int
	transferFrameBytes []byte
	ackCallback AckFunction

	// the index of the item in the heap
	heapIndex int
}


type resendQueue struct {
	orderedItems []*sendItem
	// message_id -> item
	messageItems map[Id]*sendItem
	byteCount int
}

func newResendQueue() *resendQueue {
	resendQueue := &resendQueue{
		orderedItems: []*sendItem{},
		messageItems: map[Id]*sendItem{},
		byteCount: 0,
	}
	heap.Init(resendQueue)
	return resendQueue
}

func (self *resendQueue) add(item *sendItem) {
	self.messageItems[item.messageId] = item
	heap.Push(self, item)
	self.byteCount += len(item.transferFrameBytes)
}

func (self *resendQueue) remove(messageId Id) *sendItem {
	item, ok := self.messageItems[messageId]
	if !ok {
		return nil
	}
	delete(self.messageItems, messageId)
	heap.Remove(self, item.heapIndex)
	self.byteCount -= len(item.transferFrameBytes)
	return item
}

func (self *resendQueue) removeFirst() *sendItem {
	first := heap.Pop(self)
	if first == nil {
		return nil
	}
	item := first.(*sendItem)
	delete(self.messageItems, item.messageId)
	self.byteCount -= len(item.transferFrameBytes)
	return item
}

// heap.Interface

func (self *resendQueue) Push(x any) {
	item := x.(*sendItem)
	item.heapIndex = len(self.orderedItems)
	self.orderedItems = append(self.orderedItems, item)
}

func (self *resendQueue) Pop() any {
	n := len(self.orderedItems)
	i := n - 1
	item := self.orderedItems[i]
	self.orderedItems[i] = nil
	self.orderedItems = self.orderedItems[:n-1]
	return item
}

// sort.Interface

func (self *resendQueue) Len() int {
	return len(self.orderedItems)
}

func (self *resendQueue) Less(i int, j int) bool {
	return self.orderedItems[i].resendTime.Before(self.orderedItems[j].resendTime)
}

func (self *resendQueue) Swap(i int, j int) {
	a := self.orderedItems[i]
	b := self.orderedItems[j]
	b.heapIndex = i
	self.orderedItems[i] = b
	a.heapIndex = j
	self.orderedItems[j] = a
}


type ReceiveBufferSettings struct {
	AckInterval time.Duration
	GapTimeout time.Duration
	IdleTimeout time.Duration

	SequenceBufferSize int
	// this is the max memory used per source
	ReceiveQueueMaxByteCount int

	MinMessageByteCount int

	// min number of resends before checking abuse
	ResendAbuseThreshold int
	// max legit fraction of sends that are resends
	ResendAbuseMultiple float32

	MaxPeerAuditDuration time.Duration
}


type ReceiveBuffer struct {
	ctx context.Context
	client *Client
	contractManager *ContractManager
	routeManager *RouteManager

	receiveBufferSettings *ReceiveBufferSettings

	mutex sync.Mutex
	// source id -> receive sequence
	receiveSequences map[Id]*ReceiveSequence
}

func NewReceiveBuffer(ctx context.Context,
		client *Client,
		routeManager *RouteManager,
		contractManager *ContractManager,
		receiveBufferSettings *ReceiveBufferSettings) *ReceiveBuffer {
	return &ReceiveBuffer{
		ctx: ctx,
		client: client,
		routeManager: routeManager,
		contractManager: contractManager,
		receiveBufferSettings: receiveBufferSettings,
		receiveSequences: map[Id]*ReceiveSequence{},
	}
}

func (self *ReceiveBuffer) Pack(receivePack *ReceivePack) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	// new sequence
	if receivePack.Pack.SequenceId == 0 {
		if receiveSequence, ok := self.receiveSequences[receivePack.SourceId]; ok {
			receiveSequence.Close()
			delete(self.receiveSequences, receivePack.SourceId)
		}
	}

	initReceiveSequence := func()(*ReceiveSequence) {
		receiveSequence, ok := self.receiveSequences[receivePack.SourceId]
		if ok {
			return receiveSequence
		}
		receiveSequence = NewReceiveSequence(
			self.ctx,
			self.client,
			self.routeManager,
			self.contractManager,
			receivePack.SourceId,
			self.receiveBufferSettings,
		)
		self.receiveSequences[receivePack.SourceId] = receiveSequence
		go func() {
			receiveSequence.Run()

			self.mutex.Lock()
			defer self.mutex.Unlock()
			// clean up
			if receiveSequence == self.receiveSequences[receivePack.SourceId] {
				delete(self.receiveSequences, receivePack.SourceId)
			}
		}()
		return receiveSequence
	}

	if !initReceiveSequence().Pack(receivePack) {
		delete(self.receiveSequences, receivePack.SourceId)
		initReceiveSequence().Pack(receivePack)
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
	ctx context.Context
	cancel context.CancelFunc

	client *Client
	routeManager *RouteManager
	contractManager *ContractManager

	sourceId Id

	receiveBufferSettings *ReceiveBufferSettings

	receiveContract *sequenceContract

	packs chan *ReceivePack

	receiveQueue *receiveQueue
	nextSequenceId uint64

	idleCondition *IdleCondition

	multiRouteWriter MultiRouteWriter
}

func NewReceiveSequence(
		ctx context.Context,
		client *Client,
		routeManager *RouteManager,
		contractManager *ContractManager,
		sourceId Id,
		receiveBufferSettings *ReceiveBufferSettings) *ReceiveSequence {
	cancelCtx, cancel := context.WithCancel(ctx)
	return &ReceiveSequence{
		ctx: cancelCtx,
		cancel: cancel,
		client: client,
		routeManager: routeManager,
		contractManager: contractManager,
		sourceId: sourceId,
		receiveBufferSettings: receiveBufferSettings,
		receiveContract: nil,
		packs: make(chan *ReceivePack, receiveBufferSettings.SequenceBufferSize),
		receiveQueue: newReceiveQueue(),
		nextSequenceId: 0,
		idleCondition: NewIdleCondition(),
	}
}

func (self *ReceiveSequence) Pack(receivePack *ReceivePack) (success bool) {
	if !self.idleCondition.UpdateOpen() {
		success = false
		return
	}
	defer self.idleCondition.UpdateClose()
	defer func() {
		// this means there was some error in sequence processing
		if err := recover(); err != nil {
			success = false
		}
	}()
	self.packs <- receivePack
	success = true
	return
}

func (self *ReceiveSequence) Run() {
	peerAudit := NewSequencePeerAudit(
		self.client,
		self.sourceId,
		self.receiveBufferSettings.MaxPeerAuditDuration,
	)

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
		for _, item := range self.receiveQueue.orderedItems {
			peerAudit.Update(func(a *PeerAudit) {
				a.discard(item.messageByteCount)
			})
		}

		// drain the channel
		func() {
			for {
				select {
				case receivePack, ok := <- self.packs:
					if !ok {
						return
					}
					peerAudit.Update(func(a *PeerAudit) {
						a.discard(receivePack.MessageByteCount)
					})
				}
			}
		}()

		peerAudit.Complete()
	}()

	multiRouteWriter := self.routeManager.OpenMultiRouteWriter(self.sourceId)
	self.routeManager.CloseMultiRouteWriter(multiRouteWriter)

	for {
		receiveTime := time.Now()
		var timeout time.Duration
		
		if 0 == self.receiveQueue.Len() {
			timeout = self.receiveBufferSettings.IdleTimeout
		} else {
			timeout = self.receiveBufferSettings.GapTimeout
			for 0 < self.receiveQueue.Len() {
				item := self.receiveQueue.removeFirst()

				itemGapTimeout := item.receiveTime.Add(self.receiveBufferSettings.GapTimeout).Sub(receiveTime)
				if itemGapTimeout < 0 {
					// did not receive a preceding message in time
					// close sequence
					return
				}

				if item.head && self.nextSequenceId < item.sequenceId {
					// the sender has indicated this item is first in the sequence
					// this would happen if the receiver lost state (e.g. recreated from zero state)
					self.nextSequenceId = item.sequenceId
				}

				if item.sequenceId != self.nextSequenceId {
					if itemGapTimeout < timeout {
						timeout = itemGapTimeout
					}
					break
				}

				// this item is the head of sequence
				self.nextSequenceId += 1

				// register contracts
				for _, frame := range item.frames {
					if frame.MessageType == protocol.MessageType_TransferContract {
						// close out the previous contract
						if self.receiveContract != nil {
							self.contractManager.Complete(
								self.receiveContract.contractId,
								self.receiveContract.ackedByteCount,
								self.receiveContract.unackedByteCount,
							)
							self.receiveContract = nil
						}

						var contract protocol.Contract
						err := proto.Unmarshal(frame.MessageBytes, &contract)
						if err != nil {
							// bad message
							// close sequence
							peerAudit.Update(func(a *PeerAudit) {
								a.badMessage(item.messageByteCount)
							})
							return
						}

						// check the hmac with the local provider secret key
						if !self.contractManager.Verify(
								contract.StoredContractHmac,
								contract.StoredContractBytes,
								contract.ProvideMode) {
							// bad contract
							// close sequence
							peerAudit.Update(func(a *PeerAudit) {
								a.badContract(item.messageByteCount)
							})
							return
						}

						self.receiveContract, err = newSequenceContract(&contract)
						if err != nil {
							// bad contract
							// close sequence
							peerAudit.Update(func(a *PeerAudit) {
								a.badContract(item.messageByteCount)
							})
							return
						}
					}
				}
				if self.updateContract(item.messageByteCount) {
					peerAudit.Update(func(a *PeerAudit) {
						a.received(item.messageByteCount)
					})
					item.receiveCallback(
						self.sourceId,
						item.frames,
						self.receiveContract.provideMode,
					)
					self.ack(item.messageId)
				} else {
					// no contract
					// close the sequence
					peerAudit.Update(func(a *PeerAudit) {
						a.discard(item.messageByteCount)
					})
					return
				}
			}
		}

		checkpointId := self.idleCondition.Checkpoint()
		select {
		case <- self.ctx.Done():
			return
		case receivePack := <- self.packs:
			messageId, err := IdFromBytes(receivePack.Pack.MessageId)
			if err != nil {
				// bad message
				// close the sequence
				peerAudit.Update(func(a *PeerAudit) {
					a.badMessage(receivePack.MessageByteCount)
				})
				return	
			}
			// every message must count against the contract to avoid abuse
			if receivePack.MessageByteCount < self.receiveBufferSettings.MinMessageByteCount {
				// bad message
				// close the sequence
				peerAudit.Update(func(a *PeerAudit) {
					a.badMessage(receivePack.MessageByteCount)
				})
				return	
			}
			if self.nextSequenceId <= receivePack.Pack.SequenceId {
				// replace with the latest value (check both messageId and sequenceId)
				if item := self.receiveQueue.remove(messageId); item != nil {
					peerAudit.Update(func(a *PeerAudit) {
						a.resend(item.messageByteCount)
					})
				}
				if item := self.receiveQueue.removeBySequenceId(receivePack.Pack.SequenceId); item != nil {
					peerAudit.Update(func(a *PeerAudit) {
						a.resend(item.messageByteCount)
					})
				}

				// store only up to a max size in the receive queue
				canBuffer := func(byteCount int)(bool) {
					// always allow at least one item in the receive queue
					if 0 == self.receiveQueue.Len() {
						return true
					}
					return self.receiveQueue.byteCount + byteCount < self.receiveBufferSettings.ReceiveQueueMaxByteCount
				}
				
				// remove later items to fit
				for !canBuffer(receivePack.MessageByteCount) {
					lastItem := self.receiveQueue.peekLast()
					if receivePack.Pack.SequenceId < lastItem.sequenceId {
						self.receiveQueue.remove(lastItem.messageId)
					}
				}

				if canBuffer(receivePack.MessageByteCount) {
					// add to the receiveQueue
					err := self.receive(receivePack)
					if err != nil {
						// bad message
						// close the sequence
						peerAudit.Update(func(a *PeerAudit) {
							a.badMessage(receivePack.MessageByteCount)
						})
						return	
					}
				} else {
					// drop the message
					peerAudit.Update(func(a *PeerAudit) {
						a.discard(receivePack.MessageByteCount)
					})
				}
			} else {
				// already received
				peerAudit.Update(func(a *PeerAudit) {
					a.resend(receivePack.MessageByteCount)
				})
				self.ack(messageId)
			}
		case <- time.After(timeout):
			if 0 == self.receiveQueue.Len() {
				// idle timeout
				if self.idleCondition.Close(checkpointId) {
					// close the sequence
					return
				}
				// else there are pending updates
			}
		}

		// check the resend abuse limits
		// resends can appear normal but waste bandwidth
		abuse := false
		peerAudit.Update(func(a *PeerAudit) {
			if self.receiveBufferSettings.ResendAbuseThreshold <= a.ResendCount {
				resendByteCountAbuse := int(float32(a.SendByteCount) * self.receiveBufferSettings.ResendAbuseMultiple) <= a.ResendByteCount
				resendCountAbuse := int(float32(a.SendCount) * self.receiveBufferSettings.ResendAbuseMultiple) <= a.ResendCount
				abuse = resendByteCountAbuse || resendCountAbuse
				a.Abuse = abuse
			}
		})
		if abuse {
			// close the sequence
			self.routeManager.DowngradeReceiverConnection(self.sourceId)
			return
		}
	}
}

func (self *ReceiveSequence) updateContract(byteCount int) bool {
	// `receiveNoContract` is a mutual configuration 
	// both sides must configure themselves to require no contract from each other
	if self.contractManager.ReceiveNoContract(self.sourceId) {
		return true
	}
	if self.receiveContract != nil && self.receiveContract.update(byteCount) {
		return true
	}
	return false
}

func (self *ReceiveSequence) receive(receivePack *ReceivePack) error {
	// pre condition: the sequenceId and messageId have been removed from the receiveQueue

	receiveTime := time.Now()

	sequenceId := receivePack.Pack.SequenceId
	contractId := self.receiveContract.contractId
	messageId, err := IdFromBytes(receivePack.Pack.MessageId)
	if err != nil {
		return errors.New("Bad message_id")
	}

	item := &receiveItem{
		contractId: contractId,
		messageId: messageId,
		sequenceId: sequenceId,
		receiveTime: receiveTime,
		frames: receivePack.Pack.Frames,
		messageByteCount: receivePack.MessageByteCount,
		receiveCallback: receivePack.ReceiveCallback,
	}

	self.receiveQueue.add(item)
	return nil
}

func (self *ReceiveSequence) ack(messageId Id) error {
	ack := &protocol.Ack{
		MessageId: messageId.Bytes(),
	}

	ackBytes, _ := proto.Marshal(ack)

	clientId := self.client.ClientId()
	transferFrame := &protocol.TransferFrame{
		TransferPath: &protocol.TransferPath{
			DestinationId: self.sourceId.Bytes(),
			SourceId: clientId.Bytes(),
			StreamId: DirectStreamId.Bytes(),
		},
		Frame: &protocol.Frame{
			MessageType: protocol.MessageType_TransferAck,
			MessageBytes: ackBytes,
		},
	}

	transferFrameBytes, _ := proto.Marshal(transferFrame)

	err := self.multiRouteWriter.Write(self.ctx, transferFrameBytes, -1)
	return err
}

func (self *ReceiveSequence) Close() {
	self.cancel()
}


type receiveItem struct {
	contractId Id
	messageId Id
	
	sequenceId uint64
	head bool
	receiveTime time.Time
	frames []*protocol.Frame
	messageByteCount int
	receiveCallback ReceiveFunction

	// the index of the item in the heap
	heapIndex int
}


// ordered by sequenceId
type receiveQueue struct {
	orderedItems []*receiveItem
	// message_id -> item
	messageItems map[Id]*receiveItem
	byteCount int
}

func newReceiveQueue() *receiveQueue {
	receiveQueue := &receiveQueue{
		orderedItems: []*receiveItem{},
		messageItems: map[Id]*receiveItem{},
		byteCount: 0,
	}
	heap.Init(receiveQueue)
	return receiveQueue
}

func (self *receiveQueue) add(item *receiveItem) {
	self.messageItems[item.messageId] = item
	heap.Push(self, item)
	self.byteCount += item.messageByteCount
}

func (self *receiveQueue) remove(messageId Id) *receiveItem {
	item, ok := self.messageItems[messageId]
	if !ok {
		return nil
	}
	delete(self.messageItems, messageId)
	heap.Remove(self, item.heapIndex)
	self.byteCount -= item.messageByteCount
	return item
}

func (self *receiveQueue) removeBySequenceId(sequenceId uint64) *receiveItem {
	i, found := sort.Find(len(self.orderedItems), func(i int)(int) {
		d := sequenceId - self.orderedItems[i].sequenceId
		if d < 0 {
			return -1
		} else if 0 < d {
			return 1
		} else {
			return 0
		}
	})
	if found && sequenceId == self.orderedItems[i].sequenceId {
		return self.remove(self.orderedItems[i].messageId)
	}
	return nil
}

func (self *receiveQueue) removeFirst() *receiveItem {
	first := heap.Pop(self)
	if first == nil {
		return nil
	}
	item := first.(*receiveItem)
	delete(self.messageItems, item.messageId)
	self.byteCount -= item.messageByteCount
	return item
}

func (self *receiveQueue) peekLast() *receiveItem {
	if len(self.orderedItems) == 0 {
		return nil
	}
	return self.orderedItems[0]
}

// heap.Interface

func (self *receiveQueue) Push(x any) {
	item := x.(*receiveItem)
	item.heapIndex = len(self.orderedItems)
	self.orderedItems = append(self.orderedItems, item)
}

func (self *receiveQueue) Pop() any {
	n := len(self.orderedItems)
	i := n - 1
	item := self.orderedItems[i]
	self.orderedItems[i] = nil
	self.orderedItems = self.orderedItems[:n-1]
	return item
}

// sort.Interface

func (self *receiveQueue) Len() int {
	return len(self.orderedItems)
}

func (self *receiveQueue) Less(i int, j int) bool {
	return self.orderedItems[i].sequenceId < self.orderedItems[j].sequenceId
}

func (self *receiveQueue) Swap(i int, j int) {
	a := self.orderedItems[i]
	b := self.orderedItems[j]
	b.heapIndex = i
	self.orderedItems[i] = b
	a.heapIndex = j
	self.orderedItems[j] = a
}


type sequenceContract struct {
	contractId Id
	transferByteCount int
	provideMode protocol.ProvideMode
	
	ackedByteCount int
	unackedByteCount int
}

func newSequenceContract(contract *protocol.Contract) (*sequenceContract, error) {
	var storedContract protocol.StoredContract
	err := proto.Unmarshal(contract.StoredContractBytes, &storedContract)
	if err != nil {
		return nil, err
	}
	contractId, err := IdFromBytes(storedContract.ContractId)
	if err != nil {
		return nil, err
	}
	return &sequenceContract{
		contractId: contractId,
		transferByteCount: int(storedContract.TransferByteCount),
		provideMode: contract.ProvideMode,
		ackedByteCount: 0,
		unackedByteCount: 0,
	}, nil
}

func (self *sequenceContract) update(byteCount int) bool {
	if self.transferByteCount < self.ackedByteCount + self.unackedByteCount + byteCount {
		// doesn't fit in contract
		return false
	}
	self.unackedByteCount += byteCount
	return true
}

func (self *sequenceContract) ack(byteCount int) {
	if self.unackedByteCount < byteCount {
		panic("Bad accounting.")
	}
	self.unackedByteCount -= byteCount
	self.ackedByteCount += byteCount
}


type ForwardBufferSettings struct {
	IdleTimeout time.Duration

	SequenceBufferSize int
}


type ForwardBuffer struct {
	ctx context.Context
	client *Client
	contractManager *ContractManager
	routeManager *RouteManager

	forwardBufferSettings *ForwardBufferSettings

	mutex sync.Mutex
	// destination id -> forward sequence
	forwardSequences map[Id]*ForwardSequence
}

func NewForwardBuffer(ctx context.Context,
		client *Client,
		routeManager *RouteManager,
		contractManager *ContractManager,
		forwardBufferSettings *ForwardBufferSettings) *ForwardBuffer {
	return &ForwardBuffer{
		ctx: ctx,
		client: client,
		routeManager: routeManager,
		contractManager: contractManager,
		forwardBufferSettings: forwardBufferSettings,
		forwardSequences: map[Id]*ForwardSequence{},
	}
}

func (self *ForwardBuffer) Pack(forwardPack *ForwardPack) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	initForwardSequence := func()(*ForwardSequence) {
		forwardSequence, ok := self.forwardSequences[forwardPack.DestinationId]
		if ok {
			return forwardSequence
		}
		forwardSequence = NewForwardSequence(
			self.ctx,
			self.client,
			self.routeManager,
			self.contractManager,
			forwardPack.DestinationId,
			self.forwardBufferSettings,
		)
		self.forwardSequences[forwardPack.DestinationId] = forwardSequence
		go func() {
			forwardSequence.Run()

			self.mutex.Lock()
			defer self.mutex.Unlock()
			// clean up
			if forwardSequence == self.forwardSequences[forwardPack.DestinationId] {
				delete(self.forwardSequences, forwardPack.DestinationId)
			}
		}()
		return forwardSequence
	}

	if !initForwardSequence().Pack(forwardPack) {
		delete(self.forwardSequences, forwardPack.DestinationId)
		initForwardSequence().Pack(forwardPack)
	}
}

func (self *ForwardBuffer) Close() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	// close all open sequences
	for _, forwardSequence := range self.forwardSequences {
		forwardSequence.Close()
	}
}


type ForwardSequence struct {
	ctx context.Context
	cancel context.CancelFunc

	client *Client
	routeManager *RouteManager
	contractManager *ContractManager

	destinationId Id

	forwardBufferSettings *ForwardBufferSettings

	packs chan *ForwardPack

	idleCondition *IdleCondition

	multiRouteWriter MultiRouteWriter
}

func NewForwardSequence(
		ctx context.Context,
		client *Client,
		routeManager *RouteManager,
		contractManager *ContractManager,
		destinationId Id,
		forwardBufferSettings *ForwardBufferSettings) *ForwardSequence {
	cancelCtx, cancel := context.WithCancel(ctx)
	return &ForwardSequence{
		ctx: cancelCtx,
		cancel: cancel,
		client: client,
		routeManager: routeManager,
		contractManager: contractManager,
		destinationId: destinationId,
		forwardBufferSettings: forwardBufferSettings,
		packs: make(chan *ForwardPack, forwardBufferSettings.SequenceBufferSize),
		idleCondition: NewIdleCondition(),
	}
}

func (self *ForwardSequence) Pack(forwardPack *ForwardPack) (success bool) {
	if !self.idleCondition.UpdateOpen() {
		success = false
		return
	}
	defer self.idleCondition.UpdateClose()
	defer func() {
		// this means there was some error in sequence processing
		if err := recover(); err != nil {
			success = false
		}
	}()
	self.packs <- forwardPack
	success = true
	return
}

func (self *ForwardSequence) Run() {
	defer func() {
		self.Close()

		close(self.packs)
	}()

	self.multiRouteWriter = self.routeManager.OpenMultiRouteWriter(self.destinationId)
	defer self.routeManager.CloseMultiRouteWriter(self.multiRouteWriter)

	for {
		checkpointId := self.idleCondition.Checkpoint()
		select {
		case <- self.ctx.Done():
			return
		case forwardPack := <- self.packs:
			self.multiRouteWriter.Write(self.ctx, forwardPack.TransferFrameBytes, -1)
		case <- time.After(self.forwardBufferSettings.IdleTimeout):
			if self.idleCondition.Close(checkpointId) {
				// close the sequence
				return
			}
			// else there are pending updates
		}
	}
}

func (self *ForwardSequence) Close() {
	self.cancel()
}


type PeerAudit struct {
	startTime time.Time
	lastModifiedTime time.Time
	Abuse bool
    BadContractCount int
    DiscardedByteCount int
    DiscardedCount int
    BadMessageByteCount int
    BadMessageCount int
    SendByteCount int
    SendCount int
    ResendByteCount int
    ResendCount int
}

func NewPeerAudit(startTime time.Time) *PeerAudit {
	return &PeerAudit{
		startTime: startTime,
		lastModifiedTime: startTime,
		BadContractCount: 0,
	    DiscardedByteCount: 0,
	    DiscardedCount: 0,
	    BadMessageByteCount: 0,
	    BadMessageCount: 0,
	    SendByteCount: 0,
	    SendCount: 0,
	    ResendByteCount: 0,
	    ResendCount: 0,
	}
}

func (self *PeerAudit) badMessage(byteCount int) {

}

func (self *PeerAudit) discard(byteCount int) {

}

func (self *PeerAudit) badContract(byteCount int) {

}

func (self *PeerAudit) received(byteCount int) {

}

func (self *PeerAudit) resend(byteCount int) {

}



type SequencePeerAudit struct {
	client *Client
	peerId Id
	maxAuditDuration time.Duration

	peerAudit *PeerAudit
}

func NewSequencePeerAudit(client *Client, peerId Id, maxAuditDuration time.Duration) *SequencePeerAudit {
	return &SequencePeerAudit{
		client: client,
		peerId: peerId,
		maxAuditDuration: maxAuditDuration,
		peerAudit: nil,
	}
}

func (self *SequencePeerAudit) Update(callback func(*PeerAudit)) {
	auditTime := time.Now()

	if self.peerAudit != nil && self.maxAuditDuration <= auditTime.Sub(self.peerAudit.startTime) {
		self.Complete()
	}
	if self.peerAudit == nil {
		self.peerAudit = NewPeerAudit(auditTime)
	}

	callback(self.peerAudit)
	self.peerAudit.lastModifiedTime = auditTime
	// TODO auto complete the peer audit after timeout
}

func (self *SequencePeerAudit) Complete() {
	if self.peerAudit == nil {
		return
	}

	self.client.SendControl(RequireToFrame(&protocol.PeerAudit{
		PeerId: self.peerId.Bytes(),
		Duration: uint32(math.Ceil((self.peerAudit.lastModifiedTime.Sub(self.peerAudit.startTime)).Seconds())),
		Abuse: self.peerAudit.Abuse,
		BadContractCount: uint32(self.peerAudit.BadContractCount),
	    DiscardedByteCount: uint32(self.peerAudit.DiscardedByteCount),
	    DiscardedCount: uint32(self.peerAudit.DiscardedCount),
	    BadMessageByteCount: uint32(self.peerAudit.BadMessageByteCount),
	    BadMessageCount: uint32(self.peerAudit.BadMessageCount),
	    SendByteCount: uint32(self.peerAudit.SendByteCount),
	    SendCount: uint32(self.peerAudit.SendCount),
	    ResendByteCount: uint32(self.peerAudit.ResendByteCount),
	    ResendCount: uint32(self.peerAudit.ResendCount),
	}), nil)
	self.peerAudit = nil
}


// routes are expected to have flow control and error detection and rejection
type Route = chan []byte


type Transport interface {
	// lower priority takes precedence
	Priority() int
	
	CanEvalRouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) bool
	// returns the fraction of route weight that should be allocated to this transport
	// the remaining are the lower priority transports
	// call `rematchTransport` to re-evaluate the weights. this is used for a control loop where the weight is adjusted to match the actual distribution
	RouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) float32
	
	MatchesSend(destinationId Id) bool
	MatchesReceive(destinationId Id) bool

	// request that p2p and direct connections be re-established that include the source
	// connections will be denied for sources that have bad audits
	Downgrade(sourceId Id)
}


type MultiRouteWriter interface {
	Write(ctx context.Context, transportFrameBytes []byte, timeout time.Duration) error
}


type MultiRouteReader interface {
	Read(ctx context.Context, timeout time.Duration) ([]byte, error)
}


type RouteManager struct {
	client *Client

	mutex sync.Mutex
	writerMatchState *MatchState
	readerMatchState *MatchState
}

func NewRouteManager(client *Client) *RouteManager {
	return &RouteManager{
		client: client,
		writerMatchState: NewMatchState(true, Transport.MatchesSend),
		// `weightedRoutes=false` because unless there is a cpu limit this is not needed
		readerMatchState: NewMatchState(false, Transport.MatchesReceive),
	}
}

func (self *RouteManager) DowngradeReceiverConnection(sourceId Id) {
	self.readerMatchState.Downgrade(sourceId)
}

func (self *RouteManager) OpenMultiRouteWriter(destinationId Id) MultiRouteWriter {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	return MultiRouteWriter(self.writerMatchState.OpenMultiRouteSelector(destinationId))
}

func (self *RouteManager) CloseMultiRouteWriter(w MultiRouteWriter) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.writerMatchState.CloseMultiRouteSelector(w.(*MultiRouteSelector))
}

func (self *RouteManager) OpenMultiRouteReader(destinationId Id) MultiRouteReader {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	return MultiRouteReader(self.readerMatchState.OpenMultiRouteSelector(destinationId))
}

func (self *RouteManager) CloseMultiRouteReader(r MultiRouteReader) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.readerMatchState.CloseMultiRouteSelector(r.(*MultiRouteSelector))
}

func (self *RouteManager) UpdateTransport(transport Transport, routes []Route) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.writerMatchState.updateTransport(transport, routes)
	self.readerMatchState.updateTransport(transport, routes)
}

func (self *RouteManager) RemoveTransport(transport Transport) {
	self.UpdateTransport(transport, nil)
}

func (self *RouteManager) getTransportStats(transport Transport) (writerStats *RouteStats, readerStats *RouteStats) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	writerStats = self.writerMatchState.getTransportStats(transport)
	readerStats = self.readerMatchState.getTransportStats(transport)
	return
}

func (self *RouteManager) Close() {
	// transports close individually and remove themselves via `updateTransport`
}


type MatchState struct {
	weightedRoutes bool
	matches func(Transport, Id)(bool)

	transportRoutes map[Transport][]Route

	destinationMultiRouteSelectors map[Id]map[*MultiRouteSelector]bool

	transportMatchedDestinations map[Transport]map[Id]bool
}

// note weighted routes typically are used by the sender not receiver
func NewMatchState(weightedRoutes bool, matches func(Transport, Id)(bool)) *MatchState {
	return &MatchState{
		weightedRoutes: weightedRoutes,
		matches: matches,
		destinationMultiRouteSelectors: map[Id]map[*MultiRouteSelector]bool{},
		transportMatchedDestinations: map[Transport]map[Id]bool{},
	}
}

func (self *MatchState) getTransportStats(transport Transport) *RouteStats {
	destinationIds, ok := self.transportMatchedDestinations[transport]
	if !ok {
		return nil
	}
	netStats := NewRouteStats()
	for destinationId, _ := range destinationIds {
		if multiRouteSelectors, ok := self.destinationMultiRouteSelectors[destinationId]; ok {
			for multiRouteSelector, _  := range multiRouteSelectors {
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

func (self *MatchState) OpenMultiRouteSelector(destinationId Id) *MultiRouteSelector {
	ctx, cancel := context.WithCancel(context.Background())
	multiRouteSelector := NewMultiRouteSelector(ctx, cancel, destinationId, self.weightedRoutes)

	multiRouteSelectors, ok := self.destinationMultiRouteSelectors[destinationId]
	if !ok {
		multiRouteSelectors = map[*MultiRouteSelector]bool{}
		self.destinationMultiRouteSelectors[destinationId] = multiRouteSelectors
	}
	multiRouteSelectors[multiRouteSelector] = true

	for transport, routes := range self.transportRoutes {
		matchedDestinations, ok := self.transportMatchedDestinations[transport]
		if !ok {
			matchedDestinations := map[Id]bool{}
			self.transportMatchedDestinations[transport] = matchedDestinations
		}

		// use the latest matches state
		if self.matches(transport, destinationId) {
			matchedDestinations[destinationId] = true
			multiRouteSelector.updateTransport(transport, routes)
		}
	}

	return multiRouteSelector
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
		for _, matchedDestinations := range self.transportMatchedDestinations {
			delete(matchedDestinations, destinationId)
		}
	}
}

func (self *MatchState) updateTransport(transport Transport, routes []Route) {
	if routes == nil {
		if currentMatchedDestinations, ok := self.transportMatchedDestinations[transport]; ok {
			for destinationId, _ := range currentMatchedDestinations {
				if multiRouteSelectors, ok := self.destinationMultiRouteSelectors[destinationId]; ok {
					for multiRouteSelector, _ := range multiRouteSelectors {
						multiRouteSelector.updateTransport(transport, nil)
					}
				}
			}
		}

		delete(self.transportMatchedDestinations, transport)
		delete(self.transportRoutes, transport)
	} else {
		matchedDestinations := map[Id]bool{}

		for destinationId, multiRouteSelectors := range self.destinationMultiRouteSelectors {
			if transport.MatchesSend(destinationId) {
				matchedDestinations[destinationId] = true
				for multiRouteSelector, _ := range multiRouteSelectors {
					multiRouteSelector.updateTransport(transport, routes)
				}
			}
		}

		if currentMatchedDestinations, ok := self.transportMatchedDestinations[transport]; ok {
			for destinationId, _ := range currentMatchedDestinations {
				if _, ok := currentMatchedDestinations[destinationId]; !ok {
					// no longer matches
					if multiRouteSelectors, ok := self.destinationMultiRouteSelectors[destinationId]; ok {
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

func (self *MatchState) Downgrade(sourceId Id) {
	// FIXME request downgrade from the transports
}


type MultiRouteSelector struct {
	ctx context.Context
	cancel context.CancelFunc

	destinationId Id
	weightedRoutes bool

	transportUpdate *Monitor

	mutex sync.Mutex
	transportRoutes map[Transport][]Route
	routeStats map[Route]*RouteStats
	routeActive map[Route]bool
	routeWeight map[Route]float32
}

func NewMultiRouteSelector(ctx context.Context, cancel context.CancelFunc, destinationId Id, weightedRoutes bool) *MultiRouteSelector {
	return &MultiRouteSelector{
		ctx: ctx,
		cancel: cancel,
		destinationId: destinationId,
		weightedRoutes: weightedRoutes,
		transportUpdate: NewMonitor(),
		transportRoutes: map[Transport][]Route{},
		routeStats: map[Route]*RouteStats{},
		routeActive: map[Route]bool{},
		routeWeight: map[Route]float32{},
	}
}

func (self *MultiRouteSelector) getTransportStats(transport Transport) *RouteStats {
	self.mutex.Lock()
	defer self.mutex.Unlock()

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
func (self *MultiRouteSelector) updateTransport(transport Transport, routes []Route) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if routes == nil {
		currentRoutes, ok := self.transportRoutes[transport]
		if !ok {
			// transport not set
			return
		}
		for _, currentRoute := range currentRoutes {
			delete(self.routeStats, currentRoute)
			delete(self.routeActive, currentRoute)
			delete(self.routeWeight, currentRoute)
		}
		delete(self.transportRoutes, transport)
	} else {
		if currentRoutes, ok := self.transportRoutes[transport]; ok {
			for _, currentRoute := range currentRoutes {
				if slices.Index(routes, currentRoute) < 0 {
					// no longer present
					delete(self.routeStats, currentRoute)
					delete(self.routeActive, currentRoute)
					delete(self.routeWeight, currentRoute)
				}
			}
			for _, route := range routes {
				if slices.Index(currentRoutes, route) < 0 {
					// new route
					self.routeActive[route] = true
				}
			}
		} else {
			for _, route := range routes {
				// new route
				self.routeActive[route] = true
			}
		}
		// the following will be updated with the new routes in the weighting below
		// - routeStats
		// - routeActive
		// - routeWeights
		self.transportRoutes[transport] = routes
	}

	if self.weightedRoutes {
		updatedRouteWeight := map[Route]float32{}

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
		slices.SortStableFunc(orderedTransports, func(a Transport, b Transport)(bool) {
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
			allCanEval = allCanEval && canEval
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

			updatedRouteStats := map[Route]*RouteStats{}
			for _, currentRoutes := range self.transportRoutes {
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

func (self *MultiRouteSelector) getActiveRoutes() []Route {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	activeRoutes := []Route{}
	for _, routes := range self.transportRoutes {
		for _, route := range routes {
			if self.routeActive[route] {
				activeRoutes = append(activeRoutes, route)
			}
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
					net += self.routeWeight[activeRoutes[j]]
				}
				r := rand.Float32()
				rnet := r * net
				net = 0
				for j := i; j < n; j += 1 {
					net += self.routeWeight[activeRoutes[j]]
					if rnet < net {
						return j
					}
				}
				panic("Incorrect weights")
			}()
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

	if _, ok := self.routeActive[route]; ok {
		self.routeActive[route] = false
	}
}

func (self *MultiRouteSelector) updateSendStats(route Route, sendCount int, sendByteCount int) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if stats, ok := self.routeStats[route]; ok {
		stats.sendCount += sendCount
		stats.sendByteCount += sendByteCount
	}
}

func (self *MultiRouteSelector) updateReceiveStats(route Route, receiveCount int, receiveByteCount int) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if stats, ok := self.routeStats[route]; ok {
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
		// - self.ctx.Done
		// - route writes...
		// - transport update
		// - timeout (may not exist)

		selectCases := make([]reflect.SelectCase, 0, 4 + len(activeRoutes))

		// add the context done case
		contextDoneIndex := len(selectCases)
		selectCases = append(selectCases, reflect.SelectCase{
			Dir: reflect.SelectRecv,
			Chan: reflect.ValueOf(ctx.Done()),
		})

		// add the done case
		doneIndex := len(selectCases)
		selectCases = append(selectCases, reflect.SelectCase{
			Dir: reflect.SelectRecv,
			Chan: reflect.ValueOf(self.ctx.Done()),
		})

		// add all the route
		routeStartIndex := len(selectCases)
		if 0 < len(activeRoutes) {
			sendValue := reflect.ValueOf(transportFrameBytes)
			for _, route := range activeRoutes {
				selectCases = append(selectCases, reflect.SelectCase{
					Dir: reflect.SelectSend,
					Chan: reflect.ValueOf(route),
					Send: sendValue,
				})
			}
		}

		// add the update case
		transportUpdateIndex := len(selectCases)
		selectCases = append(selectCases, reflect.SelectCase{
			Dir: reflect.SelectRecv,
			Chan: reflect.ValueOf(notify),
		})

		timeoutIndex := len(selectCases)
		if 0 <= timeout {
			remainingTimeout := enterTime.Add(timeout).Sub(time.Now())
			if remainingTimeout <= 0 {
				// add a default case
				selectCases = append(selectCases, reflect.SelectCase{
					Dir: reflect.SelectDefault,
				})
			} else {
				// add a timeout case
				selectCases = append(selectCases, reflect.SelectCase{
					Dir: reflect.SelectRecv,
					Chan: reflect.ValueOf(time.After(remainingTimeout)),
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
				self.updateSendStats(route, 1, len(transportFrameBytes))
				return nil
			} else {
				// mark the route as closed, try again
				self.setActive(route, false)
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
		// - self.ctx.Done
		// - route reads...
		// - transport update
		// - timeout (may not exist)

		selectCases := make([]reflect.SelectCase, 0, 4 + len(activeRoutes))

		// add the context done case
		contextDoneIndex := len(selectCases)
		selectCases = append(selectCases, reflect.SelectCase{
			Dir: reflect.SelectRecv,
			Chan: reflect.ValueOf(self.ctx.Done()),
		})

		// add the done case
		doneIndex := len(selectCases)
		selectCases = append(selectCases, reflect.SelectCase{
			Dir: reflect.SelectRecv,
			Chan: reflect.ValueOf(self.ctx.Done()),
		})

		// add all the route
		routeStartIndex := len(selectCases)
		if 0 < len(activeRoutes) {
			for _, route := range activeRoutes {
				selectCases = append(selectCases, reflect.SelectCase{
					Dir: reflect.SelectRecv,
					Chan: reflect.ValueOf(route),
				})
			}
		}

		// add the update case
		transportUpdateIndex := len(selectCases)
		selectCases = append(selectCases, reflect.SelectCase{
			Dir: reflect.SelectRecv,
			Chan: reflect.ValueOf(notify),
		})

		timeoutIndex := len(selectCases)
		if 0 <= timeout {
			remainingTimeout := enterTime.Add(timeout).Sub(time.Now())
			if remainingTimeout <= 0 {
				// add a default case
				selectCases = append(selectCases, reflect.SelectCase{
					Dir: reflect.SelectDefault,
				})
			} else {
				// add a timeout case
				selectCases = append(selectCases, reflect.SelectCase{
					Dir: reflect.SelectRecv,
					Chan: reflect.ValueOf(time.After(remainingTimeout)),
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
				transportFrameBytes := value.Bytes()
				self.updateReceiveStats(route, 1, len(transportFrameBytes))
				return transportFrameBytes, nil
			} else {
				// mark the route as closed, try again
				self.setActive(route, false)
			}
		}
	}
}

func (self *MultiRouteSelector) Close() {
	self.cancel()
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


type ContractManagerSettings struct {
	StandardTransferByteCount int
}


type ContractManager struct {
	client *Client

	contractManagerSettings *ContractManagerSettings

	mutex sync.Mutex

	provideSecretKeys map[protocol.ProvideMode][]byte

	destinationContracts map[Id]*ContractQueue
	
	receiveNoContractClientIds map[Id]bool
	sendNoContractClientIds map[Id]bool

	contractErrorCallbacks *CallbackList[ContractErrorFunction]
}

func NewContractManagerWithDefaults(client *Client) *ContractManager {
	return NewContractManager(client, DefaultContractManagerSettings())
}

func NewContractManager(client *Client, contractManagerSettings *ContractManagerSettings) *ContractManager {
	// at a minimum 
	// - messages to/from the platform (ControlId) do not need a contract
	//   this is because the platform is needed to create contracts
	// - messages to self do not need a contract
	receiveNoContractClientIds := map[Id]bool{
		ControlId: true,
		client.ClientId(): true,
	}
	sendNoContractClientIds := map[Id]bool{
		ControlId: true,
		client.ClientId(): true,
	}

	contractManager := &ContractManager{
		client: client,
		contractManagerSettings: contractManagerSettings,
		provideSecretKeys: map[protocol.ProvideMode][]byte{},
		destinationContracts: map[Id]*ContractQueue{},
		receiveNoContractClientIds: receiveNoContractClientIds,
		sendNoContractClientIds: sendNoContractClientIds,
		contractErrorCallbacks: NewCallbackList[ContractErrorFunction](),
	}

	client.AddReceiveCallback(contractManager.receive)

	return contractManager
}

func (self *ContractManager) StandardTransferByteCount() int {
	return self.contractManagerSettings.StandardTransferByteCount
}

func (self *ContractManager) addContractErrorCallback(contractErrorCallback ContractErrorFunction) {
	self.contractErrorCallbacks.Add(contractErrorCallback)
}

func (self *ContractManager) removeContractErrorCallback(contractErrorCallback ContractErrorFunction) {
	self.contractErrorCallbacks.Remove(contractErrorCallback)
}

// ReceiveFunction
func (self *ContractManager) receive(sourceId Id, frames []*protocol.Frame, provideMode protocol.ProvideMode) {
	switch sourceId {
	case ControlId:
		for _, frame := range frames {
			switch frame.MessageType {
			case protocol.MessageType_TransferCreateContractResult:
				var createContractResult protocol.CreateContractResult
				if err := proto.Unmarshal(frame.MessageBytes, &createContractResult); err != nil {
					if contractError := createContractResult.Error; contractError != nil {
						self.error(*contractError)
					} else if contract := createContractResult.Contract; contract != nil {
						self.addContract(contract)
					}
				}
			}
		}
	}
}

// ContractErrorFunction
func (self *ContractManager) error(contractError protocol.ContractError) {
	for _, contractErrorCallback := range self.contractErrorCallbacks.Get() {
		func() {
			defer recover()
			contractErrorCallback(contractError)
		}()
	}
}

func (self *ContractManager) setProvideModes(provideModes map[protocol.ProvideMode]bool) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	currentProvideModes := maps.Keys(self.provideSecretKeys)
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
		    	_, err := rand.Read(provideSecretKey)
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
	self.client.SendControl(RequireToFrame(provide), nil)
}

func (self *ContractManager) Verify(storedContractHmac []byte, storedContractBytes []byte, provideMode protocol.ProvideMode) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	provideSecretKey, ok := self.provideSecretKeys[provideMode]
	if !ok {
		// provide mode is not enabled
		return false
	}

	mac := hmac.New(sha256.New, provideSecretKey)
	expectedHmac := mac.Sum(storedContractBytes)
	return hmac.Equal(storedContractHmac, expectedHmac)
}

func (self *ContractManager) SendNoContract(destinationId Id) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if allow, ok := self.sendNoContractClientIds[destinationId]; ok {
		return allow
	}
	return false
}

func (self *ContractManager) ReceiveNoContract(sourceId Id) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if allow, ok := self.receiveNoContractClientIds[sourceId]; ok {
		return allow
	}
	return false
}

func (self *ContractManager) TakeContract(ctx context.Context, destinationId Id, timeout time.Duration) *protocol.Contract {
	contractQueue := self.openContractQueue(destinationId)
	defer self.closeContractQueue(destinationId)

	enterTime := time.Now()
	for {
		notify := contractQueue.updateMonitor.NotifyChannel()
		contract := contractQueue.poll()

		if contract != nil {
			return contract
		}

		if timeout < 0 {
			select {
			case <- ctx.Done():
				return nil
			case <- notify:
			}
		} else {
			remainingTimeout := enterTime.Add(timeout).Sub(time.Now())
			if remainingTimeout <= 0 {
				return nil
			} else {
				select {
				case <- ctx.Done():
					return nil
				case <- notify:
				case <- time.After(remainingTimeout):
					return nil
				}
			}
		}
	}
}

func (self *ContractManager) addContract(contract *protocol.Contract) error {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	var storedContract protocol.StoredContract
	err := proto.Unmarshal(contract.StoredContractBytes, &storedContract)
	if err != nil {
		return err
	}

	sourceId, err := IdFromBytes(storedContract.SourceId)
	if err != nil {
		return err
	}

	if self.client.ClientId() != sourceId {
		return errors.New("Contract source must be this client.")
	}

	destinationId, err := IdFromBytes(storedContract.DestinationId)
	if err != nil {
		return err
	}

	contractQueue := self.openContractQueue(destinationId)
	defer self.closeContractQueue(destinationId)

	contractQueue.add(contract)
	return nil
}

func (self *ContractManager) openContractQueue(destinationId Id) *ContractQueue {
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

func (self *ContractManager) closeContractQueue(destinationId Id) {
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

func (self *ContractManager) CreateContract(destinationId Id) {
	// look at destinationContracts and last contract to get previous contract id
	createContract := &protocol.CreateContract{
		DestinationId: destinationId.Bytes(),
		TransferByteCount: uint32(self.contractManagerSettings.StandardTransferByteCount),
	}
	self.client.SendControl(RequireToFrame(createContract), nil)
}

func (self *ContractManager) Complete(contractId Id, ackedByteCount int, unackedByteCount int) {
	closeContract := &protocol.CloseContract{
		ContractId: contractId.Bytes(),
		AckedByteCount: uint32(ackedByteCount),
		UnackedByteCount: uint32(unackedByteCount),
	}
	self.client.SendControl(RequireToFrame(closeContract), nil)
}

func (self *ContractManager) Close() {
	// FIXME close known pending contracts
	// pending contracts in flight will just timeout on the platform
	self.client.RemoveReceiveCallback(self.receive)
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
