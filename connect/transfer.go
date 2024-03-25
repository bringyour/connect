package connect

import (
	"context"
	"time"
	"sync"
	"errors"
	"math"
	"fmt"
	// "runtime/debug"

	"google.golang.org/protobuf/proto"

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


var transferLog = LogFn(LogLevelInfo, "transfer")

const DebugForwardMessages = true


type AckFunction = func(err error)
// provideMode is the mode of where these frames are from: network, friends and family, public
// provideMode nil means no contract
type ReceiveFunction = func(sourceId Id, frames []*protocol.Frame, provideMode protocol.ProvideMode)
type ForwardFunction = func(sourceId Id, destinationId Id, transferFrameBytes []byte)
type ContractErrorFunction = func(protocol.ContractError)


// destination id for control messages
var ControlId = Id{}


// in this case there are no intermediary hops
// the contract is signed with the local provide keys
var DirectStreamId = Id{}


func DefaultClientSettings() *ClientSettings {
	return &ClientSettings{
		SendBufferSize: 32,
		ForwardBufferSize: 32,
		ReadTimeout: 30 * time.Second,
		BufferTimeout: 30 * time.Second,
		SendBufferSettings: DefaultSendBufferSettings(),
		ReceiveBufferSettings: DefaultReceiveBufferSettings(),
		ForwardBufferSettings: DefaultForwardBufferSettings(),
		ContractManagerSettings: DefaultContractManagerSettings(),
	}
}


func DefaultSendBufferSettings() *SendBufferSettings {
	return &SendBufferSettings{
		ContractTimeout: 30 * time.Second,
		ContractRetryInterval: 5 * time.Second,
		// this should be greater than the rtt under load
		// TODO use an rtt estimator based on the ack times
		ResendInterval: 2 * time.Second,
		// no backoff
		ResendBackoffScale: 0.0,
		AckTimeout: 300 * time.Second,
		IdleTimeout: 300 * time.Second,
		// pause on resend for selectively acked messaged
		SelectiveAckTimeout: 15 * time.Second,
		SequenceBufferSize: 32,
		AckBufferSize: 32,
		MinMessageByteCount: ByteCount(1),
		// this includes transport reconnections
		WriteTimeout: 30 * time.Second,
		ResendQueueMaxByteCount: mib(1),
		ContractFillFraction: 0.5,
	}
}


func DefaultReceiveBufferSettings() *ReceiveBufferSettings {
	return &ReceiveBufferSettings {
		GapTimeout: 60 * time.Second,
		IdleTimeout: 300 * time.Second,
		SequenceBufferSize: 32,
		AckBufferSize: 256,
		AckCompressTimeout: 10 * time.Millisecond,
		MinMessageByteCount: ByteCount(1),
		ResendAbuseThreshold: 4,
		ResendAbuseMultiple: 0.5,
		MaxPeerAuditDuration: 60 * time.Second,
		// this includes transport reconnections
		WriteTimeout: 30 * time.Second,
		ReceiveQueueMaxByteCount: mib(2),
	}
}


func DefaultForwardBufferSettings() *ForwardBufferSettings {
	return &ForwardBufferSettings {
		IdleTimeout: 300 * time.Second,
		SequenceBufferSize: 32,
		WriteTimeout: 1 * time.Second,
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
	TransferOptions

	// frame and destination is repacked by the send buffer into a Pack,
	// with destination and frame from the tframe, and other pack properties filled in by the buffer
	Frame *protocol.Frame
	DestinationId Id
	// called (true) when the pack is ack'd, or (false) if not ack'd (closed before ack)
	AckCallback AckFunction
	MessageByteCount ByteCount
}


type ReceivePack struct {
	SourceId Id
	SequenceId Id
	Pack *protocol.Pack
	ReceiveCallback ReceiveFunction
	MessageByteCount ByteCount
}


type ForwardPack struct {
	DestinationId Id
	TransferFrameBytes []byte
}



type TransferOptions struct {
	// items can choose to not be acked
	// in this case, the ack callback is called on send, and no retry is done
	// when false, items may arrive out of order amongst un-acked sequence neighbors
	Ack bool
	// use a companion contract
	// a companion contract replies to an existing contract
	// using this option limits the destination to clients that have an active contract to the sender
	CompanionContract bool
}

func DefaultTransferOpts() TransferOptions {
	return TransferOptions{
		Ack: true,
		CompanionContract: false,
	}
}


type transferOptionsSetAck struct {
	Ack bool
}

func NoAck() transferOptionsSetAck {
	return transferOptionsSetAck{
		Ack: false,
	}
}


type transferOptionsSetCompanionContract struct {
	CompanionContract bool
}

func CompanionContract() transferOptionsSetCompanionContract {
	return transferOptionsSetCompanionContract{
		CompanionContract: true,
	}
}



type ClientSettings struct {
	SendBufferSize int
	ForwardBufferSize int
	ReadTimeout time.Duration
	BufferTimeout time.Duration

	SendBufferSettings *SendBufferSettings
	ReceiveBufferSettings *ReceiveBufferSettings
	ForwardBufferSettings *ForwardBufferSettings
	ContractManagerSettings *ContractManagerSettings
}


// note all callbacks are wrapped to check for nil and recover from errors
type Client struct {
	ctx context.Context
	cancel context.CancelFunc

	clientId Id

	clientSettings *ClientSettings

	receiveCallbacks *CallbackList[ReceiveFunction]
	forwardCallbacks *CallbackList[ForwardFunction]

	routeManager *RouteManager
	contractManager *ContractManager
	sendBuffer *SendBuffer
	receiveBuffer *ReceiveBuffer
	forwardBuffer *ForwardBuffer
}

func NewClientWithDefaults(ctx context.Context, clientId Id) *Client {
	return NewClient(ctx, clientId, DefaultClientSettings())
}

func NewClient(ctx context.Context, clientId Id, clientSettings *ClientSettings) *Client {
	cancelCtx, cancel := context.WithCancel(ctx)
	client := &Client{
		ctx: cancelCtx,
		cancel: cancel,
		clientId: clientId,
		clientSettings: clientSettings,
		receiveCallbacks: NewCallbackList[ReceiveFunction](),
		forwardCallbacks: NewCallbackList[ForwardFunction](),
	}

	routeManager := NewRouteManager(ctx)
	contractManager := NewContractManager(ctx, client, clientSettings.ContractManagerSettings)

	client.initBuffers(routeManager, contractManager)

	go client.run()

	return client
}

func (self *Client) initBuffers(routeManager *RouteManager, contractManager *ContractManager) {
	self.routeManager = routeManager
	self.contractManager = contractManager
	self.sendBuffer = NewSendBuffer(self.ctx, self, routeManager, contractManager, self.clientSettings.SendBufferSettings)
	self.receiveBuffer = NewReceiveBuffer(self.ctx, self, routeManager, contractManager, self.clientSettings.ReceiveBufferSettings)
	self.forwardBuffer = NewForwardBuffer(self.ctx, self, routeManager, contractManager, self.clientSettings.ForwardBufferSettings)
}

func (self *Client) RouteManager() *RouteManager {
	return self.routeManager
}

func (self *Client) ContractManager() *ContractManager {
	return self.contractManager
}

func (self *Client) ClientId() Id {
	return self.clientId
}

func (self *Client) ReportAbuse(sourceId Id) {
	peerAudit := NewSequencePeerAudit(self, sourceId, 0)
	peerAudit.Update(func (peerAudit *PeerAudit) {
		peerAudit.Abuse = true
	})
	peerAudit.Complete()
}

func (self *Client) ForwardWithTimeout(transferFrameBytes []byte, timeout time.Duration) bool {
	var filteredTransferFrame protocol.FilteredTransferFrame
	if err := proto.Unmarshal(transferFrameBytes, &filteredTransferFrame); err != nil {
		// bad protobuf
		return false
	}
	destinationId, err := IdFromBytes(filteredTransferFrame.TransferPath.DestinationId)
	if err != nil {
		// bad protobuf
		return false
	}

	forwardPack := &ForwardPack{
		DestinationId: destinationId,
		TransferFrameBytes: transferFrameBytes,
	}

	success, err := self.forwardBuffer.Pack(forwardPack, self.clientSettings.BufferTimeout)
	if err != nil {
		return false
	}
	return success
}

func (self *Client) Forward(transferFrameBytes []byte) bool {
	return self.ForwardWithTimeout(transferFrameBytes, -1)
}

func (self *Client) SendWithTimeout(
	frame *protocol.Frame,
	destinationId Id,
	ackCallback AckFunction,
	timeout time.Duration,
	opts ...any,
) bool {
	safeAckCallback := func(err error) {
		if ackCallback != nil {
			defer recover()
			ackCallback(err)
		}
	}

	transferOpts := DefaultTransferOpts()
	for _, opt := range opts {
		switch v := opt.(type) {
		case TransferOptions:
			transferOpts = v
		case transferOptionsSetAck:
			transferOpts.Ack = v.Ack
		case transferOptionsSetCompanionContract:
			transferOpts.CompanionContract = v.CompanionContract
		}
	}

	messageByteCount := ByteCount(len(frame.MessageBytes))
	sendPack := &SendPack{
		TransferOptions: transferOpts,
		Frame: frame,
		DestinationId: destinationId,
		AckCallback: safeAckCallback,
		MessageByteCount: messageByteCount,
	}

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
		return true
	} else {
		success, err := self.sendBuffer.Pack(sendPack, timeout)
		if err != nil {
			return false
		}
		return success
	}
}

func (self *Client) SendControlWithTimeout(frame *protocol.Frame, ackCallback AckFunction, timeout time.Duration) bool {
	return self.SendWithTimeout(frame, ControlId, ackCallback, timeout)
}

func (self *Client) Send(frame *protocol.Frame, destinationId Id, ackCallback AckFunction) bool {
	return self.SendWithTimeout(frame, destinationId, ackCallback, -1)
}

func (self *Client) SendControl(frame *protocol.Frame, ackCallback AckFunction) bool {
	return self.Send(frame, ControlId, ackCallback)
}

// ReceiveFunction
func (self *Client) receive(sourceId Id, frames []*protocol.Frame, provideMode protocol.ProvideMode) {
	transferLog("!! DISPATCH RECEIVE")
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

func (self *Client) AddReceiveCallback(receiveCallback ReceiveFunction) func() {
	callbackId := self.receiveCallbacks.Add(receiveCallback)
	return func() {
		self.receiveCallbacks.Remove(callbackId)
	}
}

// func (self *Client) RemoveReceiveCallback(receiveCallback ReceiveFunction) {
// 	self.receiveCallbacks.Remove(receiveCallback)
// }

func (self *Client) AddForwardCallback(forwardCallback ForwardFunction) func() {
	callbackId := self.forwardCallbacks.Add(forwardCallback)
	return func() {
		self.forwardCallbacks.Remove(callbackId)
	}
}

// func (self *Client) RemoveForwardCallback(forwardCallback ForwardFunction) {
// 	self.forwardCallbacks.Remove(forwardCallback)
// }

func (self *Client) run() {
	defer self.cancel()
	
	// receive
	multiRouteReader := self.routeManager.OpenMultiRouteReader(self.clientId)
	defer self.routeManager.CloseMultiRouteReader(multiRouteReader)

	updatePeerAudit := func(sourceId Id, callback func(*PeerAudit)) {
		// immediately send peer audits at this level
		peerAudit := NewSequencePeerAudit(self, sourceId, 0)
		peerAudit.Update(callback)
		peerAudit.Complete()
	}

	for {
		select {
		case <- self.ctx.Done():
			return
		default:
		}
		// fmt.Printf("read (->%s) active=%v inactive=%v\n", self.clientId, multiRouteReader.GetActiveRoutes(), multiRouteReader.GetInactiveRoutes())
			
		transferFrameBytes, err := multiRouteReader.Read(self.ctx, self.clientSettings.ReadTimeout)
		if err != nil {
			continue
		}
		// at this point, the route is expected to have already parsed the transfer frame
		// and applied basic validation and source/destination checks
		// because of this, errors in parsing the `FilteredTransferFrame` are not expected
		// decode a minimal subset of the full message needed to make a routing decision
		filteredTransferFrame := &protocol.FilteredTransferFrame{}
		if err := proto.Unmarshal(transferFrameBytes, filteredTransferFrame); err != nil {
			// bad protobuf (unexpected, see route note above)
			continue
		}
		if filteredTransferFrame.TransferPath == nil {
			// bad protobuf (unexpected, see route note above)
			continue
		}
		sourceId, err := IdFromBytes(filteredTransferFrame.TransferPath.SourceId)
		if err != nil {
			// bad protobuf (unexpected, see route note above)
			continue
		}
		destinationId, err := IdFromBytes(filteredTransferFrame.TransferPath.DestinationId)
		if err != nil {
			// bad protobuf (unexpected, see route note above)
			continue
		}

		transferLog("CLIENT READ %s -> %s via %s\n", sourceId.String(), destinationId.String(), self.clientId.String())

		transferLog("[%s] Read %s -> %s", self.clientId.String(), sourceId.String(), destinationId.String())
		if destinationId == self.clientId {
			// the transports have typically not parsed the full `TransferFrame`
			// on error, discard the message and report the peer
			transferFrame := &protocol.TransferFrame{}
			if err := proto.Unmarshal(transferFrameBytes, transferFrame); err != nil {
				// bad protobuf
				updatePeerAudit(sourceId, func(a *PeerAudit) {
					a.badMessage(ByteCount(len(transferFrameBytes)))
				})
				continue
			}
			frame := transferFrame.GetFrame()

			// TODO apply source verification+decryption with pke

			switch frame.GetMessageType() {
			case protocol.MessageType_TransferAck:
				transferLog("GOT ACK AT TOP\n")
				ack := &protocol.Ack{}
				if err := proto.Unmarshal(frame.GetMessageBytes(), ack); err != nil {
					// bad protobuf
					updatePeerAudit(sourceId, func(a *PeerAudit) {
						a.badMessage(ByteCount(len(transferFrameBytes)))
					})
					continue
				}
				transferLog("[%s] Receive ack %s ->: %s", self.clientId.String(), sourceId.String(), ack)
				err := self.sendBuffer.Ack(sourceId, ack, self.clientSettings.BufferTimeout)
				if err != nil {
					transferLog("TIMEOUT ACK\n")
				}
			case protocol.MessageType_TransferPack:
				pack := &protocol.Pack{}
				if err := proto.Unmarshal(frame.GetMessageBytes(), pack); err != nil {
					// bad protobuf
					updatePeerAudit(sourceId, func(a *PeerAudit) {
						a.badMessage(ByteCount(len(transferFrameBytes)))
					})
					continue
				}
				sequenceId, err := IdFromBytes(pack.SequenceId)
				if err != nil {
					// bad protobuf
					continue
				}
				messageByteCount := ByteCount(0)
				for _, frame := range pack.Frames {
					messageByteCount += ByteCount(len(frame.MessageBytes))
				}
				transferLog("[%s] Receive pack %s ->: %s", self.clientId.String(), sourceId.String(), pack.Frames)
				success, err := self.receiveBuffer.Pack(&ReceivePack{
					SourceId: sourceId,
					SequenceId: sequenceId,
					Pack: pack,
					ReceiveCallback: self.receive,
					MessageByteCount: messageByteCount,
				}, self.clientSettings.BufferTimeout)
				if err != nil || !success {
					transferLog("TIMEOUT RECEIVE PACK\n")
				}
			default:
				transferLog("[%s] Receive unknown -> %s: %s", self.clientId.String(), sourceId.String(), frame)
				updatePeerAudit(sourceId, func(a *PeerAudit) {
					a.badMessage(ByteCount(len(transferFrameBytes)))
				})
			}
		} else {
			if DebugForwardMessages {
				transferFrame := &protocol.TransferFrame{}
				if err := proto.Unmarshal(transferFrameBytes, transferFrame); err != nil {
					// bad protobuf
					updatePeerAudit(sourceId, func(a *PeerAudit) {
						a.badMessage(ByteCount(len(transferFrameBytes)))
					})
					continue
				}
				frame := transferFrame.GetFrame()

				// TODO apply source verification+decryption with pke

				switch frame.GetMessageType() {
				case protocol.MessageType_TransferAck:
					ack := &protocol.Ack{}
					if err := proto.Unmarshal(frame.GetMessageBytes(), ack); err != nil {
						// bad protobuf
						updatePeerAudit(sourceId, func(a *PeerAudit) {
							a.badMessage(ByteCount(len(transferFrameBytes)))
						})
						continue
					}
					transferLog("[%s] Forward ack %s ->: %s", self.clientId.String(), sourceId.String(), ack)
				case protocol.MessageType_TransferPack:
					pack := &protocol.Pack{}
					if err := proto.Unmarshal(frame.GetMessageBytes(), pack); err != nil {
						// bad protobuf
						updatePeerAudit(sourceId, func(a *PeerAudit) {
							a.badMessage(ByteCount(len(transferFrameBytes)))
						})
						continue
					}
					messageByteCount := ByteCount(0)
					for _, frame := range pack.Frames {
						messageByteCount += ByteCount(len(frame.MessageBytes))
					}
					transferLog("[%s] Forward pack %s ->: %s", self.clientId.String(), sourceId.String(), pack.Frames)
				default:
					transferLog("[%s] Forward unknown -> %s: %s", self.clientId.String(), sourceId.String(), frame)
				}
			}

			transferLog("CLIENT FORWARD\n")

			transferLog("[%s] Forward %s -> %s", self.clientId.String(), sourceId.String(), destinationId.String())
			self.forward(sourceId, destinationId, transferFrameBytes)
		}
	}
}

func (self *Client) ResendQueueSize(destinationId Id, companionContract bool) (int, ByteCount, Id) {
	if self.sendBuffer == nil {
		return 0, 0, Id{}
	} else {
		return self.sendBuffer.ResendQueueSize(destinationId, companionContract)
	}
}

func (self *Client) ReceiveQueueSize(sourceId Id, sequenceId Id) (int, ByteCount) {
	if self.receiveBuffer == nil {
		return 0, 0
	} else {
		return self.receiveBuffer.ReceiveQueueSize(sourceId, sequenceId)
	}
}

func (self *Client) Close() {
	self.cancel()

	self.sendBuffer.Close()
	self.receiveBuffer.Close()
	self.forwardBuffer.Close()

	self.routeManager.Close()
	self.contractManager.Close()
}

func (self *Client) Cancel() {
	// debug.PrintStack()

	fmt.Printf("CANCEL 02\n")
	self.cancel()

	self.sendBuffer.Cancel()
	self.receiveBuffer.Cancel()
	self.forwardBuffer.Cancel()
}

func (self *Client) Flush() {
	self.sendBuffer.Flush()
	self.receiveBuffer.Flush()
	self.forwardBuffer.Flush()

	self.contractManager.Flush()
}


type SendBufferSettings struct {
	ContractTimeout time.Duration
	ContractRetryInterval time.Duration

	// TODO replace this with round trip time estimation
	// resend timeout is the initial time between successive send attempts. Does linear backoff
	ResendInterval time.Duration
	ResendBackoffScale float64

	// on ack timeout, no longer attempt to retransmit and notify of ack failure
	AckTimeout time.Duration
	IdleTimeout time.Duration

	SelectiveAckTimeout time.Duration

	SequenceBufferSize int
	AckBufferSize int

	MinMessageByteCount ByteCount

	WriteTimeout time.Duration

	ResendQueueMaxByteCount ByteCount

	// as this ->1, there is more risk that noack messages will get dropped due to out of sync contracts
	ContractFillFraction float32
}


type sendSequenceId struct {
	DestinationId Id
	CompanionContract bool
}

type SendBuffer struct {
	ctx context.Context
	client *Client
	routeManager *RouteManager
	contractManager *ContractManager
	
	sendBufferSettings *SendBufferSettings

	mutex sync.Mutex
	sendSequences map[sendSequenceId]*SendSequence
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
		sendSequences: map[sendSequenceId]*SendSequence{},
	}
}

func (self *SendBuffer) Pack(sendPack *SendPack, timeout time.Duration) (bool, error) {
	sendSequenceId := sendSequenceId{
		DestinationId: sendPack.DestinationId,
		CompanionContract: sendPack.TransferOptions.CompanionContract,
	}

	initSendSequence := func(skip *SendSequence)(*SendSequence) {
		self.mutex.Lock()
		defer self.mutex.Unlock()

		sendSequence, ok := self.sendSequences[sendSequenceId]
		if ok {
			if skip == nil || skip != sendSequence {
				return sendSequence
			} else {
				sendSequence.Close()
				delete(self.sendSequences, sendSequenceId)
			}
		}
		sendSequence = NewSendSequence(
			self.ctx,
			self.client,
			self.routeManager,
			self.contractManager,
			sendPack.DestinationId,
			sendPack.TransferOptions.CompanionContract,
			self.sendBufferSettings,
		)
		self.sendSequences[sendSequenceId] = sendSequence
		go func() {
			sendSequence.Run()

			self.mutex.Lock()
			defer self.mutex.Unlock()
			// clean up
			if sendSequence == self.sendSequences[sendSequenceId] {
				sendSequence.Close()
				delete(self.sendSequences, sendSequenceId)
			}
		}()
		return sendSequence
	}

	sendSequence := initSendSequence(nil)
	if success, err := sendSequence.Pack(sendPack, timeout); err == nil {
		return success, nil
	} else {
		// sequence closed
		return initSendSequence(sendSequence).Pack(sendPack, timeout)
	}
}

func (self *SendBuffer) Ack(sourceId Id, ack *protocol.Ack, timeout time.Duration) (returnErr error) {
	sendSequence := func(companionContract bool)(*SendSequence) {
		self.mutex.Lock()
		defer self.mutex.Unlock()
		return self.sendSequences[sendSequenceId{
			DestinationId: sourceId,
			CompanionContract: companionContract,
		}]
	}

	if seq := sendSequence(false); seq != nil {
		if err := seq.Ack(ack, timeout); err != nil {
			returnErr = err
		}
	}
	if seq := sendSequence(true); seq != nil {
		if err := seq.Ack(ack, timeout); err != nil {
			returnErr = err
		}
	}
	return
}

func (self *SendBuffer) ResendQueueSize(destinationId Id, companionContract bool) (int, ByteCount, Id) {
	sendSequence := func()(*SendSequence) {
		self.mutex.Lock()
		defer self.mutex.Unlock()
		return self.sendSequences[sendSequenceId{
			DestinationId: destinationId,
			CompanionContract: companionContract,
		}]
	}

	if seq := sendSequence(); seq != nil {
		return seq.ResendQueueSize()
	}
	return 0, 0, Id{}
}

func (self *SendBuffer) Close() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	// cancel all open sequences
	// the control of the sequence will close it
	for _, sendSequence := range self.sendSequences {
		sendSequence.Cancel()
	}
}

func (self *SendBuffer) Cancel() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	// cancel all open sequences
	for _, sendSequence := range self.sendSequences {
		sendSequence.Cancel()
	}
}

func (self *SendBuffer) Flush() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	// cancel all open sequences
	for sendSequenceId, sendSequence := range self.sendSequences {
		if sendSequenceId.DestinationId != ControlId {
			sendSequence.Cancel()
		}
	}
}


type SendSequence struct {
	ctx context.Context
	cancel context.CancelFunc

	client *Client
	clientId Id
	routeManager *RouteManager
	contractManager *ContractManager

	destinationId Id
	companionContract bool
	sequenceId Id

	sendBufferSettings *SendBufferSettings

	// the head contract. this contract is also in `openSendContracts`
	sendContract *sequenceContract
	// contracts are closed when the data are acked
	// these contracts are waiting for acks to close
	openSendContracts map[Id]*sequenceContract

	packs chan *SendPack
	acks chan *protocol.Ack

	resendQueue *resendQueue
	sendItems []*sendItem
	nextSequenceNumber uint64

	idleCondition *IdleCondition

	multiRouteWriter MultiRouteWriter
}

func NewSendSequence(
		ctx context.Context,
		client *Client,
		routeManager *RouteManager,
		contractManager *ContractManager,
		destinationId Id,
		companionContract bool,
		sendBufferSettings *SendBufferSettings) *SendSequence {
	cancelCtx, cancel := context.WithCancel(ctx)

	return &SendSequence{
		ctx: cancelCtx,
		cancel: cancel,
		client: client,
		clientId: client.ClientId(),
		routeManager: routeManager,
		contractManager: contractManager,
		destinationId: destinationId,
		companionContract: companionContract,
		sequenceId: NewId(),
		sendBufferSettings: sendBufferSettings,
		sendContract: nil,
		openSendContracts: map[Id]*sequenceContract{},
		packs: make(chan *SendPack, sendBufferSettings.SequenceBufferSize),
		acks: make(chan *protocol.Ack, sendBufferSettings.AckBufferSize),
		resendQueue: newResendQueue(),
		sendItems: []*sendItem{},
		nextSequenceNumber: 0,
		idleCondition: NewIdleCondition(),
	}
}

func (self *SendSequence) ResendQueueSize() (int, ByteCount, Id) {
	count, byteSize := self.resendQueue.QueueSize()
	return count, byteSize, self.sequenceId
}

// success, error
func (self *SendSequence) Pack(sendPack *SendPack, timeout time.Duration) (bool, error) {
	if !self.idleCondition.UpdateOpen() {
		// fmt.Printf("PACK 01\n")
		return false, nil
	}
	defer self.idleCondition.UpdateClose()

	if timeout < 0 {
		select {
		case <- self.ctx.Done():
			// fmt.Printf("PACK 02\n")
			return false, errors.New("Done.")
		case self.packs <- sendPack:
			// fmt.Printf("PACK 03\n")
			return true, nil
		}
	} else if timeout == 0 {
		select {
		case <- self.ctx.Done():
			// fmt.Printf("PACK 04\n")
			return false, errors.New("Done.")
		case self.packs <- sendPack:
			// fmt.Printf("PACK 05\n")
			return true, nil
		default:
			// fmt.Printf("PACK 06\n")
			return false, nil
		}
	} else {
		select {
		case <- self.ctx.Done():
			// fmt.Printf("PACK 07\n")
			return false, errors.New("Done.")
		case self.packs <- sendPack:
			// fmt.Printf("PACK 08\n")
			return true, nil
		case <- time.After(timeout):
			// fmt.Printf("PACK 09\n")
			return false, nil
		}
	}
}

func (self *SendSequence) Ack(ack *protocol.Ack, timeout time.Duration) error {
	sequenceId, err := IdFromBytes(ack.SequenceId)
	if err != nil {
		return err
	}
	if self.sequenceId != sequenceId {
		// ack is for a different send sequence that no longer exists
		return nil
	}

	if timeout < 0 {
		select {
		case <- self.ctx.Done():
			return nil
		case self.acks <- ack:
			return nil
		}
	} else if timeout == 0 {
		select {
		case <- self.ctx.Done():
			return nil
		case self.acks <- ack:
			return nil
		default:
			return nil
		}
	} else {
		select {
		case <- self.ctx.Done():
			return nil
		case self.acks <- ack:
			return nil
		case <- time.After(timeout):
			return fmt.Errorf("Timeout")
		}
	}
}

func (self *SendSequence) Run() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("ERROR: %s\n", r)
		}

		transferLog("[%s] Send sequence exit -> %s\n", self.clientId.String(), self.destinationId.String())

		self.cancel()

		// close contract
		for _, sendContract := range self.openSendContracts {
			fmt.Printf("COMPLETE CONTRACT SEND 3\n")
			self.contractManager.CompleteContract(
				sendContract.contractId,
				sendContract.ackedByteCount,
				sendContract.unackedByteCount,
			)
		}

		// drain the buffer
		for _, item := range self.resendQueue.orderedItems {
			item.ackCallback(errors.New("Send sequence closed."))
		}

		// drain the channel
		func() {
			for {
				select {
				case sendPack, ok := <- self.packs:
					if !ok {
						return
					}
					sendPack.AckCallback(errors.New("Send sequence closed."))
				}
			}
		}()

		// used contracts were closed above
		// the contract queue can be safely closed
		self.contractManager.CloseContractQueue(self.destinationId)
	}()

	self.multiRouteWriter = self.routeManager.OpenMultiRouteWriter(self.destinationId)
	defer self.routeManager.CloseMultiRouteWriter(self.multiRouteWriter)

	for {

		// drain all pending acks before processing the resend queue
		func() {
			for {
				select {
				case <- self.ctx.Done():
					return
				case ack, ok := <- self.acks:
					if !ok {
						return
					}
					if messageId, err := IdFromBytes(ack.MessageId); err == nil {
						self.receiveAck(messageId, ack.Selective)
					}
				default:
					return
				}
			}
		}()


		sendTime := time.Now()
		var timeout time.Duration

		if self.resendQueue.Len() == 0 { 
			timeout = self.sendBufferSettings.IdleTimeout
		} else {
			timeout = self.sendBufferSettings.AckTimeout

			for {
				item := self.resendQueue.PeekFirst()
				if item == nil {
					break
				}

				itemAckTimeout := item.sendTime.Add(self.sendBufferSettings.AckTimeout).Sub(sendTime)

				if itemAckTimeout <= 0 {
					// message took too long to ack
					// close the sequence
					fmt.Printf("EXIT 01\n")
					return
				}

				if sendTime.Before(item.resendTime) {
					itemResendTimeout := item.resendTime.Sub(sendTime)
					if itemResendTimeout < timeout {
						timeout = itemResendTimeout
					}
					if itemAckTimeout < timeout {
						timeout = itemAckTimeout
					}
					break
				}

				self.resendQueue.RemoveByMessageId(item.messageId)

				// resend
				var transferFrameBytes []byte
				if self.sendItems[0].sequenceNumber == item.sequenceNumber && !item.head {
					transferLog("!!!! SET HEAD %d", item.sequenceNumber)
					// set `first=true`
					transferFrameBytes_, err := self.setHead(item.transferFrameBytes)
					if err != nil {
						transferLog("!!!! SET HEAD ERROR %s", err)
						fmt.Printf("EXIT 02\n")
						return
					}
					transferFrameBytes = transferFrameBytes_
				} else {
					transferFrameBytes = item.transferFrameBytes
				}
				transferLog("!!!! RESEND %d [%s]", item.sequenceNumber, self.destinationId.String())
				// fmt.Printf("buffer resend write message %s->\n", self.clientId)
				// a := time.Now()
				if err := self.multiRouteWriter.Write(self.ctx, transferFrameBytes, self.sendBufferSettings.WriteTimeout); err != nil {
					transferLog("!! WRITE TIMEOUT A\n")
					// d := time.Now().Sub(a)
					// fmt.Printf("buffer resend write message %s-> (%s) %.2fms\n", self.clientId, err, float64(d) / float64(time.Millisecond))
				} else {
					transferLog("WROTE RESEND: %s, %s -> messageId=%s clientId=%s sequenceId=%s\n", item.messageType, self.clientId.String(), item.messageId.String(), self.destinationId.String(), self.sequenceId.String())
					// d := time.Now().Sub(a)
					// fmt.Printf("buffer resend write message %s-> %.2fms\n", self.clientId, float64(d) / float64(time.Millisecond))
				}

				item.sendCount += 1
				// linear backoff
				// itemResendTimeout := self.sendBufferSettings.ResendInterval
				itemResendTimeout := time.Duration(float64(self.sendBufferSettings.ResendInterval) * (1 + self.sendBufferSettings.ResendBackoffScale * float64(item.sendCount)))
				if itemResendTimeout < itemAckTimeout {
					item.resendTime = sendTime.Add(itemResendTimeout)
				} else {
					item.resendTime = sendTime.Add(itemAckTimeout)
				}
				self.resendQueue.Add(item)
			}
		}

		checkpointId := self.idleCondition.Checkpoint()

		// approximate since this cannot consider the next message byte size
		canQueue := func()(bool) {
	        // always allow at least one item in the resend queue
	        queueSize, queueByteCount := self.resendQueue.QueueSize()
	        if 0 == queueSize {
	            return true
	        }
	        return queueByteCount < self.sendBufferSettings.ResendQueueMaxByteCount
		}
		if !canQueue() {
			transferLog("AT SEND LIMIT %d %d bytes\n", len(self.resendQueue.orderedItems), self.resendQueue.byteCount)
			
			// wait for acks
			select {
			case <- self.ctx.Done():
				fmt.Printf("EXIT 04\n")
			    return
			case ack, ok := <- self.acks:
				if !ok {
					fmt.Printf("EXIT 05\n")
					return
				}
				if messageId, err := IdFromBytes(ack.MessageId); err == nil {
					self.receiveAck(messageId, ack.Selective)
				}
			case <- time.After(timeout):
				if 0 == self.resendQueue.Len() {
					// idle timeout
					if self.idleCondition.Close(checkpointId) {
						// close the sequence
						fmt.Printf("EXIT 06\n")
					    return
					}
					// else there are pending updates
				}
			}
		} else {
			select {
			case <- self.ctx.Done():
				fmt.Printf("EXIT 07\n")
				return
			case ack, ok := <- self.acks:
				if !ok {
					fmt.Printf("EXIT 08\n")
					return
				}
				if messageId, err := IdFromBytes(ack.MessageId); err == nil {
					self.receiveAck(messageId, ack.Selective)
				}
			case sendPack, ok := <- self.packs:
				if !ok {
					fmt.Printf("EXIT 09\n")
					return
				}

				// note messages of `size < MinMessageByteCount` get counted as `MinMessageByteCount` against the contract
				if self.updateContract(sendPack.MessageByteCount) {
					transferLog("[%s] Have contract, sending -> %s: %s", self.clientId.String(), self.destinationId.String(), sendPack.Frame)
					self.send(sendPack.Frame, sendPack.AckCallback, sendPack.Ack)
					// ignore the error since there will be a retry
				} else {
					// no contract
					// close the sequence
					fmt.Printf("EXIT 10\n")
					sendPack.AckCallback(errors.New("No contract"))
					return
				}
			case <- time.After(timeout):
				if 0 == self.resendQueue.Len() {
					// idle timeout
					if self.idleCondition.Close(checkpointId) {
						fmt.Printf("EXIT 11\n")
						// close the sequence
						return
					}
					// else there are pending updates
				}
			}
		}
	}
}

func (self *SendSequence) updateContract(messageByteCount ByteCount) bool {
	// `sendNoContract` is a mutual configuration 
	// both sides must configure themselves to require no contract from each other
	if self.contractManager.SendNoContract(self.destinationId) {
		return true
	}
	if self.sendContract != nil && self.sendContract.update(messageByteCount) {
		return true
	}
	// new contract

	// the max overhead of the pack frame
	// this is needed because the size of the contract pack is counted against the contract
	maxContractMessageByteCount := ByteCount(256)

	effectiveContractTransferByteCount := ByteCount(float32(self.contractManager.StandardContractTransferByteCount()) * self.sendBufferSettings.ContractFillFraction)
	if effectiveContractTransferByteCount < messageByteCount + maxContractMessageByteCount {
		// this pack does not fit into a standard contract
		// TODO allow requesting larger contracts
		panic("Message too large for contract. It can never be sent.")
	}

	// sends the 
	setNextContract := func(contract *protocol.Contract)(bool) {
		nextSendContract, err := newSequenceContract(
			contract,
			self.sendBufferSettings.MinMessageByteCount,
			self.sendBufferSettings.ContractFillFraction,
		)
		if err != nil {
			// malformed, drop
			return false
		}

		contractMessageBytes, _ := proto.Marshal(contract)

		if maxContractMessageByteCount < ByteCount(len(contractMessageBytes)) {
			panic("Bad estimate for contract max size could result in infinite contract retries.")
		}

		if nextSendContract.update(ByteCount(len(contractMessageBytes))) && nextSendContract.update(messageByteCount) {
			transferLog("[%s] Send contract %s -> %s", self.clientId.String(), nextSendContract.contractId.String(), self.destinationId.String())

			self.setContract(nextSendContract)

			// append the contract to the sequence
			self.send(&protocol.Frame{
				MessageType: protocol.MessageType_TransferContract,
				MessageBytes: contractMessageBytes,
			}, func(error){}, true)

			return true
		} else {
			// this contract doesn't fit the message
			// the contract was requested with the correct size, so this is an error somewhere
			// just close it and let the platform time out the other side
			fmt.Printf("COMPLETE CONTRACT SEND 4\n")
			self.contractManager.CompleteContract(nextSendContract.contractId, 0, 0)
			return false
		}
	}

	nextContract := func(timeout time.Duration)(bool) {
		if contract := self.contractManager.TakeContract(self.ctx, self.destinationId, timeout); contract != nil && setNextContract(contract) {
			// async queue up the next contract
			self.contractManager.CreateContract(self.destinationId, self.companionContract)

			return true
		} else {
			return false
		}
	}

	if nextContract(0) {
		return true
	}

	endTime := time.Now().Add(self.sendBufferSettings.ContractTimeout)
	for {
		timeout := endTime.Sub(time.Now())
		if timeout <= 0 {
			return false
		}

		// async queue up the next contract
		self.contractManager.CreateContract(self.destinationId, self.companionContract)

		if nextContract(min(timeout, self.sendBufferSettings.ContractRetryInterval)) {
			return true
		}
	}
}

func (self *SendSequence) setContract(nextSendContract *sequenceContract) {
	// do not close the current contract unless it has no pending data
	// the contract is stracked in `openSendContracts` and will be closed on ack
	if self.sendContract != nil && self.sendContract.unackedByteCount == 0 {
		fmt.Printf("COMPLETE CONTRACT SEND 1\n")
		self.contractManager.CompleteContract(
			self.sendContract.contractId,
			self.sendContract.ackedByteCount,
			self.sendContract.unackedByteCount,
		)
		delete(self.openSendContracts, self.sendContract.contractId)
		self.sendContract = nil
	}
	self.openSendContracts[nextSendContract.contractId] = nextSendContract
	self.sendContract = nextSendContract
}

func (self *SendSequence) send(
	frame *protocol.Frame,
	ackCallback AckFunction,
	ack bool,
) {
	sendTime := time.Now()
	messageId := NewId()
	
	var contractId *Id
	if self.sendContract != nil {
		contractId = &self.sendContract.contractId
	}

	var head bool
	var sequenceNumber uint64
	if ack {
		head = (0 == len(self.sendItems))
		sequenceNumber = self.nextSequenceNumber
		self.nextSequenceNumber += 1
	} else {
		head = false
		sequenceNumber = 0
	}

	pack := &protocol.Pack{
		MessageId: messageId.Bytes(),
		SequenceId: self.sequenceId.Bytes(),
		SequenceNumber: sequenceNumber,
		Head: head,
		Frames: []*protocol.Frame{frame},
		Nack: !ack,
	}

	packBytes, _ := proto.Marshal(pack)

	transferFrame := &protocol.TransferFrame{
		TransferPath: &protocol.TransferPath{
			DestinationId: self.destinationId.Bytes(),
			SourceId: self.clientId.Bytes(),
			StreamId: DirectStreamId.Bytes(),
		},
		Frame: &protocol.Frame{
			MessageType: protocol.MessageType_TransferPack,
			MessageBytes: packBytes,
		},
	}

	transferFrameBytes, _ := proto.Marshal(transferFrame)

	item := &sendItem{
		transferItem: transferItem{
			messageId: messageId,
			sequenceNumber: sequenceNumber,
			messageByteCount: ByteCount(len(frame.MessageBytes)),
		},
		contractId: contractId,
		sendTime: sendTime,
		resendTime: sendTime.Add(self.sendBufferSettings.ResendInterval),
		sendCount: 1,
		head: head,
		transferFrameBytes: transferFrameBytes,
		ackCallback: ackCallback,
		// selectiveAckEnd: time.Time{},
		messageType: frame.MessageType,
	}

	if ack {
		self.sendItems = append(self.sendItems, item)
		self.resendQueue.Add(item)
	} else {
		// immediately ack
		self.ackItem(item)
		ackCallback(nil)
	}

	// ignore the write error
	self.multiRouteWriter.Write(
		self.ctx,
		item.transferFrameBytes,
		self.sendBufferSettings.WriteTimeout,
	)
}

func (self *SendSequence) setHead(transferFrameBytes []byte) ([]byte, error) {
	var transferFrame protocol.TransferFrame
	err := proto.Unmarshal(transferFrameBytes, &transferFrame)
	if err != nil {
		return nil, err
	}

	var pack protocol.Pack
	err = proto.Unmarshal(transferFrame.Frame.MessageBytes, &pack)
	if err != nil {
		return nil, err
	}

	pack.Head = true

	packBytes, err := proto.Marshal(&pack)
	if err != nil {
		return nil, err
	}
	transferFrame.Frame.MessageBytes = packBytes

	transferFrameBytesWithHead, err := proto.Marshal(&transferFrame)
	if err != nil {
		return nil, err
	}

	return transferFrameBytesWithHead, nil
}

func (self *SendSequence) receiveAck(messageId Id, selective bool) {
	transferLog("RECEIVE ACK %t\n", selective)

	item := self.resendQueue.GetByMessageId(messageId)
	if item == nil {
		transferLog("RECEIVE ACK MISS\n")
		// message not pending ack
		return
	}

	if selective {
		removed := self.resendQueue.RemoveByMessageId(messageId)
		if removed == nil {
			panic(errors.New("Missing item"))
		}
		item.resendTime = time.Now().Add(self.sendBufferSettings.SelectiveAckTimeout)
		self.resendQueue.Add(item)
		return
	}

	// acks are cumulative
	// implicitly ack all earlier items in the sequence
	i := 0
	for ; i < len(self.sendItems); i += 1 {
		implicitItem := self.sendItems[i]
		if item.sequenceNumber < implicitItem.sequenceNumber {
			transferLog("!!!! ACK END")
			break
		}
		// self.ackedSequenceNumbers[implicitItem.sequenceNumber] = true
		transferLog("!!!! ACK %d", implicitItem.sequenceNumber)
		removed := self.resendQueue.RemoveByMessageId(implicitItem.messageId)
		if removed == nil {
			panic(errors.New("Missing item"))
		}
		implicitItem.ackCallback(nil)

		self.ackItem(implicitItem)
		self.sendItems[i] = nil
	}
	self.sendItems = self.sendItems[i:]
}

func (self *SendSequence) ackItem(item *sendItem) {
	if item.contractId != nil {
		itemSendContract := self.openSendContracts[*item.contractId]
		itemSendContract.ack(item.messageByteCount)
		// not current and closed
		if self.sendContract != itemSendContract && itemSendContract.unackedByteCount == 0 {
			fmt.Printf("COMPLETE CONTRACT SEND 2\n")
			self.contractManager.CompleteContract(
				itemSendContract.contractId,
				itemSendContract.ackedByteCount,
				itemSendContract.unackedByteCount,
			)
			delete(self.openSendContracts, itemSendContract.contractId)
		}
	}
}

func (self *SendSequence) Close() {
	// debug.PrintStack()
	self.cancel()
	self.idleCondition.WaitForClose()
	close(self.packs)
	close(self.acks)
}

func (self *SendSequence) Cancel() {
	// debug.PrintStack()
	fmt.Printf("CANCEL 01\n")
	self.cancel()
}


type sendItem struct {
	transferItem

	contractId *Id
	head bool
	sendTime time.Time
	resendTime time.Time
	sendCount int
	transferFrameBytes []byte
	ackCallback AckFunction

	messageType protocol.MessageType
}


// a send event queue which is the union of:
// - resend times
// - ack timeouts
type resendQueue = transferQueue[*sendItem]

func newResendQueue() *resendQueue {
	return newTransferQueue[*sendItem](func(a *sendItem, b *sendItem)(int) {
		if a.resendTime.Before(b.resendTime) {
			return -1
		} else if b.resendTime.Before(a.resendTime) {
			return 1
		} else {
			return 0
		}
	})
}


type ReceiveBufferSettings struct {
	GapTimeout time.Duration
	IdleTimeout time.Duration

	SequenceBufferSize int
	AckBufferSize int

	AckCompressTimeout time.Duration

	MinMessageByteCount ByteCount

	// min number of resends before checking abuse
	ResendAbuseThreshold int
	// max legit fraction of sends that are resends
	ResendAbuseMultiple float64

	MaxPeerAuditDuration time.Duration

	WriteTimeout time.Duration

	ReceiveQueueMaxByteCount ByteCount
}


type receiveSequenceId struct {
	SourceId Id
	SequenceId Id
}


type ReceiveBuffer struct {
	ctx context.Context
	client *Client
	contractManager *ContractManager
	routeManager *RouteManager

	receiveBufferSettings *ReceiveBufferSettings

	mutex sync.Mutex
	// source id -> receive sequence
	receiveSequences map[receiveSequenceId]*ReceiveSequence
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
		receiveSequences: map[receiveSequenceId]*ReceiveSequence{},
	}
}

func (self *ReceiveBuffer) Pack(receivePack *ReceivePack, timeout time.Duration) (bool, error) {
	receiveSequenceId := receiveSequenceId{
		SourceId: receivePack.SourceId,
		SequenceId: receivePack.SequenceId,
	}

	initReceiveSequence := func(skip *ReceiveSequence)(*ReceiveSequence) {
		self.mutex.Lock()
		defer self.mutex.Unlock()

		receiveSequence, ok := self.receiveSequences[receiveSequenceId]
		if ok {
			if skip == nil || skip != receiveSequence {
				return receiveSequence
			} else {
				receiveSequence.Close()
				delete(self.receiveSequences, receiveSequenceId)
			}
		}
		receiveSequence = NewReceiveSequence(
			self.ctx,
			self.client,
			self.routeManager,
			self.contractManager,
			receivePack.SourceId,
			receivePack.SequenceId,
			self.receiveBufferSettings,
		)
		self.receiveSequences[receiveSequenceId] = receiveSequence
		go func() {
			receiveSequence.Run()

			self.mutex.Lock()
			defer self.mutex.Unlock()
			// clean up
			if receiveSequence == self.receiveSequences[receiveSequenceId] {
				receiveSequence.Close()
				delete(self.receiveSequences, receiveSequenceId)
			}
		}()
		return receiveSequence
	}

	receiveSequence := initReceiveSequence(nil)
	if success, err := receiveSequence.Pack(receivePack, timeout); err == nil {
		return success, nil
	} else {
		// sequence closed
		return initReceiveSequence(receiveSequence).Pack(receivePack, timeout)
	}
}

func (self *ReceiveBuffer) ReceiveQueueSize(sourceId Id, sequenceId Id) (int, ByteCount) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	receiveSequenceId := receiveSequenceId{
		SourceId: sourceId,
		SequenceId: sequenceId,
	}
	if receiveSequence, ok := self.receiveSequences[receiveSequenceId]; ok {
		return receiveSequence.ReceiveQueueSize()
	}
	return 0, 0
}

func (self *ReceiveBuffer) Close() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	// cancel all open sequences
	// the control of the sequence will close it
	for _, receiveSequence := range self.receiveSequences {
		receiveSequence.Cancel()
	}
}

func (self *ReceiveBuffer) Cancel() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	// cancel all open sequences
	for _, receiveSequence := range self.receiveSequences {
		receiveSequence.Cancel()
	}
}

func (self *ReceiveBuffer) Flush() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	// cancel all open sequences
	for receiveSequenceId, receiveSequence := range self.receiveSequences {
		if receiveSequenceId.SourceId != ControlId {
			receiveSequence.Cancel()
		}
	}
}


type ReceiveSequence struct {
	ctx context.Context
	cancel context.CancelFunc

	client *Client
	clientId Id
	routeManager *RouteManager
	contractManager *ContractManager

	sourceId Id
	sequenceId Id

	receiveBufferSettings *ReceiveBufferSettings

	receiveContract *sequenceContract
	usedContractIds map[Id]bool

	packs chan *ReceivePack

	receiveQueue *receiveQueue
	nextSequenceNumber uint64

	idleCondition *IdleCondition

	peerAudit *SequencePeerAudit

	sendAcks chan *sendAck
}

func NewReceiveSequence(
		ctx context.Context,
		client *Client,
		routeManager *RouteManager,
		contractManager *ContractManager,
		sourceId Id,
		sequenceId Id,
		receiveBufferSettings *ReceiveBufferSettings) *ReceiveSequence {
	cancelCtx, cancel := context.WithCancel(ctx)
	return &ReceiveSequence{
		ctx: cancelCtx,
		cancel: cancel,
		client: client,
		clientId: client.ClientId(),
		routeManager: routeManager,
		contractManager: contractManager,
		sourceId: sourceId,
		sequenceId: sequenceId,
		receiveBufferSettings: receiveBufferSettings,
		receiveContract: nil,
		usedContractIds: map[Id]bool{},
		packs: make(chan *ReceivePack, receiveBufferSettings.SequenceBufferSize),
		receiveQueue: newReceiveQueue(),
		nextSequenceNumber: 0,
		idleCondition: NewIdleCondition(),
		sendAcks: make(chan *sendAck, receiveBufferSettings.AckBufferSize),
	}
}

func (self *ReceiveSequence) ReceiveQueueSize() (int, ByteCount) {
	return self.receiveQueue.QueueSize()
}

// success, error
func (self *ReceiveSequence) Pack(receivePack *ReceivePack, timeout time.Duration) (bool, error) {
	if !self.idleCondition.UpdateOpen() {
		return false, nil
	}
	defer self.idleCondition.UpdateClose()

	if timeout < 0 {
		select {
		case <- self.ctx.Done():
			return false, errors.New("Done.")
		case self.packs <- receivePack:
			return true, nil
		}
	} else if timeout == 0 {
		select {
		case <- self.ctx.Done():
			return false, errors.New("Done.")
		case self.packs <- receivePack:
			return true, nil
		default:
			return false, nil
		}
	} else {
		select {
		case <- self.ctx.Done():
			return false, errors.New("Done.")
		case self.packs <- receivePack:
			return true, nil
		case <- time.After(timeout):
			return false, nil
		}
	}
}

func (self *ReceiveSequence) Run() {
	defer func() {
		transferLog("[%s] Receive sequence exit %s ->\n", self.clientId.String(), self.sourceId.String())

		self.cancel()

		// close contract
		if self.receiveContract != nil {
			fmt.Printf("COMPLETE CONTRACT RECEIVE 1 (%s)\n", self.receiveContract.contractId)
			self.contractManager.CompleteContract(
				self.receiveContract.contractId,
				self.receiveContract.ackedByteCount,
				self.receiveContract.unackedByteCount,
			)
			self.receiveContract = nil
		}

		// drain the buffer
		for _, item := range self.receiveQueue.orderedItems {
			self.peerAudit.Update(func(a *PeerAudit) {
				a.discard(item.messageByteCount)
			})
		}

		// drain the channel
		func() {
			for {
				select {
				case _, ok := <- self.packs:
					if !ok {
						return
					}
				}
			}
		}()

		self.peerAudit.Complete()
	}()

	self.peerAudit = NewSequencePeerAudit(
		self.client,
		self.sourceId,
		self.receiveBufferSettings.MaxPeerAuditDuration,
	)

	go self.compressAndSendAcks()

	for {
		receiveTime := time.Now()
		var timeout time.Duration
		
		if queueSize, _ := self.receiveQueue.QueueSize(); 0 == queueSize {
			timeout = self.receiveBufferSettings.IdleTimeout
		} else {
			timeout = self.receiveBufferSettings.GapTimeout
			for {
				item := self.receiveQueue.PeekFirst()
				if item == nil {
					break
				}

				itemGapTimeout := item.receiveTime.Add(self.receiveBufferSettings.GapTimeout).Sub(receiveTime)
				if itemGapTimeout < 0 {
					transferLog("!! EXIT H")
					transferLog("[%s] Gap timeout", self.clientId.String())
					// did not receive a preceding message in time
					// quence
					return
				}

				if self.nextSequenceNumber < item.sequenceNumber {
					transferLog("[%s] Head of sequence is not next %d <> %d: %s", self.clientId.String(), self.nextSequenceNumber, item.sequenceNumber, item.frames)
					if itemGapTimeout < timeout {
						timeout = itemGapTimeout
					}
					break
				}
				// item.sequenceNumber <= self.nextSequenceNumber

				self.receiveQueue.RemoveByMessageId(item.messageId)
				
				
				if self.nextSequenceNumber == item.sequenceNumber {
					// this item is the head of sequence
					transferLog("[%s] Receive next in sequence %d: %s", self.clientId.String(), item.sequenceNumber, item.frames)
				
					if err := self.registerContracts(item); err != nil {
						return
					}
					transferLog("[%s] Receive head of sequence %d.", self.clientId.String(), item.sequenceNumber)
					if self.updateContract(item) {
						self.nextSequenceNumber = item.sequenceNumber + 1
						self.receiveHead(item)
					} else {
						// no valid contract
						// drop the message and let the client send a contract at the head
						// FIXME send a "needs contract" ack
						fmt.Printf("Dropped head waiting for contract.\n")
					}
				} else {
					// this item is a resend of a previous item
					if item.ack {
						self.sendAck(item.sequenceNumber, item.messageId, false)
					}
				}
			}
		}

		checkpointId := self.idleCondition.Checkpoint()
		select {
		case <- self.ctx.Done():
			transferLog("!! EXIT A")
			return
		case receivePack, ok := <- self.packs:
			if !ok {
				transferLog("!! EXIT B")
				return
			}

			if receivePack.Pack.Nack {
				received, err := self.receiveNack(receivePack)
				if err != nil {
					// bad message
					// close the sequence
					self.peerAudit.Update(func(a *PeerAudit) {
						a.badMessage(receivePack.MessageByteCount)
					})
					transferLog("!! EXIT D")
					return	
				} else if !received {
					transferLog("!!!! 5")
					// drop the message
					self.peerAudit.Update(func(a *PeerAudit) {
						a.discard(receivePack.MessageByteCount)
					})

					transferLog("RECEIVE DROPPED A MESSAGE\n")
				}

			// note messages of `size < MinMessageByteCount` get counted as `MinMessageByteCount` against the contract
			} else {
				received, err := self.receive(receivePack)
				if err != nil {
					// bad message
					// close the sequence
					self.peerAudit.Update(func(a *PeerAudit) {
						a.badMessage(receivePack.MessageByteCount)
					})
					transferLog("!! EXIT D")
					return	
				} else if !received {
					transferLog("!!!! 5")
					// drop the message
					self.peerAudit.Update(func(a *PeerAudit) {
						a.discard(receivePack.MessageByteCount)
					})

					transferLog("RECEIVE DROPPED A MESSAGE\n")
				}
			}
		case <- time.After(timeout):
			if 0 == self.receiveQueue.Len() {
				// idle timeout
				if self.idleCondition.Close(checkpointId) {
					transferLog("!! EXIT F")
					// close the sequence
					return
				}
				// else there are pending updates
			}
		}


		// FIXME audit SendCount is currently not being updated
		/*
		// check the resend abuse limits
		// resends can appear normal but waste bandwidth
		abuse := false
		self.peerAudit.Update(func(a *PeerAudit) {
			if self.receiveBufferSettings.ResendAbuseThreshold <= a.ResendCount {
				resendByteCountAbuse := ByteCount(float64(a.SendByteCount) * self.receiveBufferSettings.ResendAbuseMultiple) <= a.ResendByteCount
				resendCountAbuse := int(float64(a.SendCount) * self.receiveBufferSettings.ResendAbuseMultiple) <= a.ResendCount
				abuse = resendByteCountAbuse || resendCountAbuse
				a.Abuse = abuse
			}
		})
		if abuse {
			// close the sequence
			self.routeManager.DowngradeReceiverConnection(self.sourceId)
			return
		}
		*/
	}

	transferLog("!! EXIT G")
}

func (self *ReceiveSequence) sendAck(sequenceNumber uint64, messageId Id, selective bool) {
	self.sendAcks <- &sendAck{
		sequenceNumber: sequenceNumber,
		messageId: messageId,
		selective: selective,
	}
}

func (self *ReceiveSequence) compressAndSendAcks() {
	defer self.cancel()

	multiRouteWriter := self.routeManager.OpenMultiRouteWriter(self.sourceId)
	defer self.routeManager.CloseMultiRouteWriter(multiRouteWriter)

	writeAck := func(sendAck *sendAck) {
		ack := &protocol.Ack{
			MessageId: sendAck.messageId.Bytes(),
			SequenceId: self.sequenceId.Bytes(),
			Selective: sendAck.selective,
		}

		ackBytes, _ := proto.Marshal(ack)

		transferFrame := &protocol.TransferFrame{
			TransferPath: &protocol.TransferPath{
				DestinationId: self.sourceId.Bytes(),
				SourceId: self.clientId.Bytes(),
				StreamId: DirectStreamId.Bytes(),
			},
			Frame: &protocol.Frame{
				MessageType: protocol.MessageType_TransferAck,
				MessageBytes: ackBytes,
			},
		}

		transferFrameBytes, _ := proto.Marshal(transferFrame)

		// fmt.Printf("buffer write ack %s->\n", self.clientId)
		if err := multiRouteWriter.Write(self.ctx, transferFrameBytes, self.receiveBufferSettings.WriteTimeout); err != nil {
			transferLog("!! WRITE TIMEOUT D\n")
		} else {
			transferLog("WROTE ACK: %s -> messageId=%s clientId=%s sequenceId=%s\n", self.clientId.String(), sendAck.messageId.String(), self.sourceId.String(), self.sequenceId.String())
		}
	}

	for {
		select {
		case <- self.ctx.Done():
			return
		default:
		}

		compressStartTime := time.Now()

		// the most recently sent ack
		var headAck *sendAck
		acks := map[uint64]*sendAck{}
		selectiveAcks := map[uint64]*sendAck{}

		addAck := func(sendAck *sendAck) {
			if sendAck.selective {
				selectiveAcks[sendAck.sequenceNumber] = sendAck
			} else {
				acks[sendAck.sequenceNumber] = sendAck
			}
		}

		// wait for one ack
		select {
		case <- self.ctx.Done():
			return
		case sendAck := <- self.sendAcks:
			addAck(sendAck)
		}

		CollapseLoop:
		for {
			timeout := self.receiveBufferSettings.AckCompressTimeout - time.Now().Sub(compressStartTime)
			if timeout <= 0 {
				for {
					select {
					case <- self.ctx.Done():
						return
					case sendAck := <- self.sendAcks:
						addAck(sendAck)
					default:
						break CollapseLoop
					}
				}
			} else {
				select {
				case <- self.ctx.Done():
					return
				case sendAck := <- self.sendAcks:
					addAck(sendAck)
				case <- time.After(timeout):
				}
			}
		}

		if 0 < len(acks) {
			// write
			// - the max ack
			// - selective acks greater than the max ack
			maxAckSequenceNumber := uint64(0)
			for sequenceNumber, _ := range acks {
				if maxAckSequenceNumber < sequenceNumber {
					maxAckSequenceNumber = sequenceNumber
				}
			}

			if headAck == nil || headAck.sequenceNumber < maxAckSequenceNumber {
				headAck = acks[maxAckSequenceNumber]
			}
			// else send the previous head to ack the most messages possible
			writeAck(headAck)
			for sequenceNumber, sendAck := range selectiveAcks {
				if headAck.sequenceNumber < sequenceNumber {
					writeAck(sendAck)
				}
			}
		} else {
			// there is no max ack, so write all the selective acks
			for _, sendAck := range selectiveAcks {
				writeAck(sendAck)
			}
		}
	}
}

func (self *ReceiveSequence) receive(receivePack *ReceivePack) (bool, error) {
	receiveTime := time.Now()

	sequenceNumber := receivePack.Pack.SequenceNumber
	var contractId *Id
	if self.receiveContract != nil {
		contractId = &self.receiveContract.contractId
	}
	messageId, err := IdFromBytes(receivePack.Pack.MessageId)
	if err != nil {
		return false, errors.New("Bad message_id")
	}

	transferLog("!!!! R %d", sequenceNumber)

	item := &receiveItem{
		transferItem: transferItem{
			messageId: messageId,
			sequenceNumber: sequenceNumber,
			messageByteCount: receivePack.MessageByteCount,
		},
		
		contractId: contractId,
		receiveTime: receiveTime,
		frames: receivePack.Pack.Frames,
		receiveCallback: receivePack.ReceiveCallback,
		head: receivePack.Pack.Head,
		ack: !receivePack.Pack.Nack,
	}


	// this case happens when the receiver is reformed or loses state.
	// the sequence id guarantees the sender is the same for the sequence
	// past head items are retransmits. Future head items depend on previous ack,
	// which represent some state the sender has that the receiver is missing
	// advance the receiver state to the latest from the sender
	if item.head && self.nextSequenceNumber < item.sequenceNumber {
		transferLog("HEAD ADVANCE %d -> %d\n", self.nextSequenceNumber, item.sequenceNumber)
		self.nextSequenceNumber = item.sequenceNumber
	}


	if item := self.receiveQueue.RemoveBySequenceNumber(sequenceNumber); item != nil {
		transferLog("!!!! 2")
		self.peerAudit.Update(func(a *PeerAudit) {
			a.resend(item.messageByteCount)
		})
	}

	// replace with the latest value (check both messageId and sequenceNumber)
	if item := self.receiveQueue.RemoveByMessageId(messageId); item != nil {
		transferLog("!!!! 1")
		self.peerAudit.Update(func(a *PeerAudit) {
			a.resend(item.messageByteCount)
		})
	}


	if sequenceNumber <= self.nextSequenceNumber {
		if self.nextSequenceNumber == sequenceNumber {
			// this item is the head of sequence

			transferLog("[%s] Receive next in sequence [%d]\n", self.clientId.String(), item.sequenceNumber)

			self.nextSequenceNumber = sequenceNumber + 1	

			if err := self.registerContracts(item); err != nil {
				return false, err
			}
			if self.updateContract(item) {
				self.receiveHead(item)
				return true, nil
			} else {
				return false, fmt.Errorf("Bad contract.")
			}
		} else {
			// this item is a resend of a previous item
			if item.ack {
				self.sendAck(sequenceNumber, messageId, false)
			}
			return false, nil
		}
	} else {
		// store only up to a max size in the receive queue
		canQueue := func(byteCount ByteCount)(bool) {
	        // always allow at least one item in the receive queue
	        queueSize, queueByteCount := self.receiveQueue.QueueSize()
	        if 0 == queueSize {
	            return true
	        }
	        return queueByteCount + byteCount < self.receiveBufferSettings.ReceiveQueueMaxByteCount
		}

		// remove later items to fit
		for !canQueue(receivePack.MessageByteCount) {
	        transferLog("!!!! 3")
	        lastItem := self.receiveQueue.PeekLast()
	        if receivePack.Pack.SequenceNumber < lastItem.sequenceNumber {
	           	self.receiveQueue.RemoveByMessageId(lastItem.messageId)
	        } else {
	        	break
	        }
		}

		if canQueue(receivePack.MessageByteCount) {
			self.receiveQueue.Add(item)
			self.sendAck(sequenceNumber, messageId, true)
			transferLog("ACK C SELECTIVE\n")
			return true, nil
		} else {
			return false, nil
		}
	}
}

func (self *ReceiveSequence) receiveNack(receivePack *ReceivePack) (bool, error) {

	receiveTime := time.Now()

	sequenceNumber := receivePack.Pack.SequenceNumber
	var contractId *Id
	if self.receiveContract != nil {
		contractId = &self.receiveContract.contractId
	}
	messageId, err := IdFromBytes(receivePack.Pack.MessageId)
	if err != nil {
		return false, errors.New("Bad message_id")
	}

	item := &receiveItem{
		transferItem: transferItem{
			messageId: messageId,
			sequenceNumber: sequenceNumber,
			messageByteCount: receivePack.MessageByteCount,
		},
		contractId: contractId,
		receiveTime: receiveTime,
		frames: receivePack.Pack.Frames,
		receiveCallback: receivePack.ReceiveCallback,
		head: receivePack.Pack.Head,
		ack: !receivePack.Pack.Nack,
	}

	if err := self.registerContracts(item); err != nil {
		return false, err
	}
	if self.updateContract(item) {
		self.receiveHead(item)
		return true, nil
	} else {
		return false, fmt.Errorf("Bad contract.")
	}
}

func (self *ReceiveSequence) receiveHead(item *receiveItem) {
	// fmt.Printf("[%d] receive\n", item.sequenceNumber)
	self.peerAudit.Update(func(a *PeerAudit) {
		a.received(item.messageByteCount)
	})
	var provideMode protocol.ProvideMode
	if self.receiveContract != nil {
		self.receiveContract.ack(item.messageByteCount)
		provideMode = self.receiveContract.provideMode
	} else {
		// no contract peers are considered in network
		provideMode = protocol.ProvideMode_Network
	}
	transferLog("!! RECEIVED MESSAGE, CALLING RECEIVE %s %s", item.receiveCallback, item.frames)
	item.receiveCallback(
		self.sourceId,
		item.frames,
		provideMode,
	)
	if item.ack {
		self.sendAck(item.sequenceNumber, item.messageId, false)
		transferLog("ACK A\n")
	}
}

func (self *ReceiveSequence) registerContracts(item *receiveItem) error {
	for _, frame := range item.frames {
		if frame.MessageType == protocol.MessageType_TransferContract {

			var contract protocol.Contract
			err := proto.Unmarshal(frame.MessageBytes, &contract)
			if err != nil {
				transferLog("!! EXIT I")
				// bad message
				// close sequence
				self.peerAudit.Update(func(a *PeerAudit) {
					a.badMessage(item.messageByteCount)
				})
				return err
			}

			// check the hmac with the local provider secret key
			if !self.contractManager.Verify(
					contract.StoredContractHmac,
					contract.StoredContractBytes,
					contract.ProvideMode) {
				fmt.Printf("CONTRACT VERIFICATION FAILED (%s)\n", contract.ProvideMode)
				transferLog("!! EXIT J")
				// bad contract
				// close sequence
				self.peerAudit.Update(func(a *PeerAudit) {
					a.badContract()
				})
				return nil
			}

			nextReceiveContract, err := newSequenceContract(
				&contract,
				self.receiveBufferSettings.MinMessageByteCount,
				1.0,
			)
			if err != nil {
				transferLog("!! EXIT K")
				// bad contract
				// close sequence
				self.peerAudit.Update(func(a *PeerAudit) {
					a.badContract()
				})
				return err
			}

			if err := self.setContract(nextReceiveContract); err != nil {
				// the next contract has already been used
				// bad contract
				// close sequence
				self.peerAudit.Update(func(a *PeerAudit) {
					a.badContract()
				})
				return err
			}
		}
	}
	return nil
}

func (self *ReceiveSequence) setContract(nextReceiveContract *sequenceContract) error {
	// close out the previous contract
	if self.receiveContract != nil {
		fmt.Printf("COMPLETE CONTRACT RECEIVE 2\n")
		self.contractManager.CompleteContract(
			self.receiveContract.contractId,
			self.receiveContract.ackedByteCount,
			self.receiveContract.unackedByteCount,
		)
		self.receiveContract = nil
	}
	// track all the contracts used to avoid reusing contracts
	if self.usedContractIds[nextReceiveContract.contractId] {
		return errors.New("Already used contract.")
	}
	self.usedContractIds[nextReceiveContract.contractId] = true
	self.receiveContract = nextReceiveContract
	return nil
}

func (self *ReceiveSequence) updateContract(item *receiveItem) bool {
	// `receiveNoContract` is a mutual configuration 
	// both sides must configure themselves to require no contract from each other
	if self.contractManager.ReceiveNoContract(self.sourceId) {
		return true
	}
	if self.receiveContract != nil && self.receiveContract.update(item.messageByteCount) {
		return true
	}
	return false
}

func (self *ReceiveSequence) Close() {
	self.cancel()
	self.idleCondition.WaitForClose()
	close(self.packs)
}

func (self *ReceiveSequence) Cancel() {
	self.cancel()
}


type sendAck struct {
	sequenceNumber uint64
	messageId Id
	selective bool
}


type receiveItem struct {
	transferItem

	contractId *Id
	head bool
	receiveTime time.Time
	frames []*protocol.Frame
	receiveCallback ReceiveFunction
	ack bool
}


// ordered by sequenceNumber
type receiveQueue = transferQueue[*receiveItem]

func newReceiveQueue() *receiveQueue {
	return newTransferQueue[*receiveItem](func(a *receiveItem, b *receiveItem)(int) {
		if a.sequenceNumber < b.sequenceNumber {
			return -1
		} else if b.sequenceNumber < a.sequenceNumber {
			return 1
		} else {
			return 0
		}
	})
}


type sequenceContract struct {
	contractId Id
	transferByteCount ByteCount
	effectiveTransferByteCount ByteCount
	provideMode protocol.ProvideMode

	minUpdateByteCount ByteCount

	sourceId Id
	destinationId Id
	
	ackedByteCount ByteCount
	unackedByteCount ByteCount
}

func newSequenceContract(contract *protocol.Contract, minUpdateByteCount ByteCount, contractFillFraction float32) (*sequenceContract, error) {
	storedContract := &protocol.StoredContract{}
	err := proto.Unmarshal(contract.StoredContractBytes, storedContract)
	if err != nil {
		return nil, err
	}

	contractId, err := IdFromBytes(storedContract.ContractId)
	if err != nil {
		return nil, err
	}

	sourceId, err := IdFromBytes(storedContract.SourceId)
	if err != nil {
		return nil, err
	}

	destinationId, err := IdFromBytes(storedContract.DestinationId)
	if err != nil {
		return nil, err
	}

	return &sequenceContract{
		contractId: contractId,
		transferByteCount: ByteCount(storedContract.TransferByteCount),
		effectiveTransferByteCount: ByteCount(float32(storedContract.TransferByteCount) * contractFillFraction),
		provideMode: contract.ProvideMode,
		minUpdateByteCount: minUpdateByteCount,
		sourceId: sourceId,
		destinationId: destinationId,
		ackedByteCount: ByteCount(0),
		unackedByteCount: ByteCount(0),
	}, nil
}

func (self *sequenceContract) update(byteCount ByteCount) bool {
	var roundedByteCount ByteCount
	if byteCount < self.minUpdateByteCount {
		roundedByteCount = self.minUpdateByteCount
	} else {
		roundedByteCount = byteCount
	}

	if self.effectiveTransferByteCount < self.ackedByteCount + self.unackedByteCount + roundedByteCount {
		// doesn't fit in contract
		fmt.Printf("DEBIT CONTRACT (%s) FAILED +%d->%d (%d/%d total %.1f%% full)\n", self.contractId.String(), roundedByteCount, self.ackedByteCount + self.unackedByteCount + roundedByteCount, self.ackedByteCount + self.unackedByteCount, self.effectiveTransferByteCount, 100.0 * float32(self.ackedByteCount + self.unackedByteCount) / float32(self.effectiveTransferByteCount))
		return false
	}
	self.unackedByteCount += roundedByteCount
	fmt.Printf("DEBIT CONTRACT (%s) PASSED +%d->%d (%d/%d total %.1f%% full)\n", self.contractId.String(), roundedByteCount, self.ackedByteCount + self.unackedByteCount, self.ackedByteCount + self.unackedByteCount, self.effectiveTransferByteCount, 100.0 * float32(self.ackedByteCount + self.unackedByteCount) / float32(self.effectiveTransferByteCount))
	return true
}

func (self *sequenceContract) ack(byteCount ByteCount) {
	if self.unackedByteCount < byteCount {
		panic("Bad accounting.")
	}
	self.unackedByteCount -= byteCount
	self.ackedByteCount += byteCount
}


type ForwardBufferSettings struct {
	IdleTimeout time.Duration

	SequenceBufferSize int

	WriteTimeout time.Duration
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

func (self *ForwardBuffer) Pack(forwardPack *ForwardPack, timeout time.Duration) (bool, error) {
	initForwardSequence := func(skip *ForwardSequence)(*ForwardSequence) {
		self.mutex.Lock()
		defer self.mutex.Unlock()

		forwardSequence, ok := self.forwardSequences[forwardPack.DestinationId]
		if ok {
			if skip == nil || skip != forwardSequence {
				return forwardSequence
			} else {
				forwardSequence.Close()
				delete(self.forwardSequences, forwardPack.DestinationId)
			}
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
				forwardSequence.Close()
				delete(self.forwardSequences, forwardPack.DestinationId)
			}
		}()
		return forwardSequence
	}

	forwardSequence := initForwardSequence(nil)
	if success, err := forwardSequence.Pack(forwardPack, timeout); err == nil {
		return success, nil
	} else {
		// sequence closed
		return initForwardSequence(forwardSequence).Pack(forwardPack, timeout)
	}
}

func (self *ForwardBuffer) Close() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	// cancel all open sequences
	// the control of the sequence will close it
	for _, forwardSequence := range self.forwardSequences {
		forwardSequence.Cancel()
	}
}

func (self *ForwardBuffer) Cancel() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	// cancel all open sequences
	for _, forwardSequence := range self.forwardSequences {
		forwardSequence.Cancel()
	}
}

func (self *ForwardBuffer) Flush() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	// cancel all open sequences
	for destinationId, forwardSequence := range self.forwardSequences {
		if destinationId != ControlId {
			forwardSequence.Cancel()
		}
	}
}


type ForwardSequence struct {
	ctx context.Context
	cancel context.CancelFunc

	client *Client
	clientId Id
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
		clientId: client.ClientId(),
		routeManager: routeManager,
		contractManager: contractManager,
		destinationId: destinationId,
		forwardBufferSettings: forwardBufferSettings,
		packs: make(chan *ForwardPack, forwardBufferSettings.SequenceBufferSize),
		idleCondition: NewIdleCondition(),
	}
}

// success, error
func (self *ForwardSequence) Pack(forwardPack *ForwardPack, timeout time.Duration) (bool, error) {
	if !self.idleCondition.UpdateOpen() {
		return false, nil
	}
	defer self.idleCondition.UpdateClose()

	if timeout < 0 {
		select {
		case <- self.ctx.Done():
			return false, errors.New("Done.")
		case self.packs <- forwardPack:
			return true, nil
		}
	} else if timeout == 0 {
		select {
		case <- self.ctx.Done():
			return false, errors.New("Done.")
		case self.packs <- forwardPack:
			return true, nil
		default:
			return false, nil
		}
	} else {
		select {
		case <- self.ctx.Done():
			return false, errors.New("Done.")
		case self.packs <- forwardPack:
			return true, nil
		case <- time.After(timeout):
			return false, nil
		}
	}	
}

func (self *ForwardSequence) Run() {
	defer self.cancel()

	self.multiRouteWriter = self.routeManager.OpenMultiRouteWriter(self.destinationId)
	defer self.routeManager.CloseMultiRouteWriter(self.multiRouteWriter)

	for {
		checkpointId := self.idleCondition.Checkpoint()
		select {
		case <- self.ctx.Done():
			return
		case forwardPack, ok := <- self.packs:
			if !ok {
				return
			}
			// fmt.Printf("buffer forward write message %s->\n", self.clientId)
			if err := self.multiRouteWriter.Write(self.ctx, forwardPack.TransferFrameBytes, self.forwardBufferSettings.WriteTimeout); err != nil {
				transferLog("!! WRITE TIMEOUT E: active=%v inactive=%v (%s)\n", self.multiRouteWriter.GetActiveRoutes(), self.multiRouteWriter.GetInactiveRoutes(), err)
			}
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
	self.idleCondition.WaitForClose()
	close(self.packs)
}

func (self *ForwardSequence) Cancel() {
	self.cancel()
}


type PeerAudit struct {
	startTime time.Time
	lastModifiedTime time.Time
	Abuse bool
    BadContractCount int
    DiscardedByteCount ByteCount
    DiscardedCount int
    BadMessageByteCount ByteCount
    BadMessageCount int
    SendByteCount ByteCount
    SendCount int
    ResendByteCount ByteCount
    ResendCount int
}

func NewPeerAudit(startTime time.Time) *PeerAudit {
	return &PeerAudit{
		startTime: startTime,
		lastModifiedTime: startTime,
		BadContractCount: 0,
	    DiscardedByteCount: ByteCount(0),
	    DiscardedCount: 0,
	    BadMessageByteCount: ByteCount(0),
	    BadMessageCount: 0,
	    SendByteCount: ByteCount(0),
	    SendCount: 0,
	    ResendByteCount: ByteCount(0),
	    ResendCount: 0,
	}
}

func (self *PeerAudit) badMessage(byteCount ByteCount) {
	self.BadMessageCount += 1
	self.BadMessageByteCount += byteCount
}

func (self *PeerAudit) discard(byteCount ByteCount) {
	self.DiscardedCount += 1
	self.DiscardedByteCount += byteCount
}

func (self *PeerAudit) badContract() {
	self.BadContractCount += 1
}

func (self *PeerAudit) received(byteCount ByteCount) {
	self.SendCount += 1
	self.SendByteCount += byteCount
}

func (self *PeerAudit) resend(byteCount ByteCount) {
	self.ResendCount += 1
	self.ResendByteCount += byteCount
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
		Duration: uint64(math.Ceil((self.peerAudit.lastModifiedTime.Sub(self.peerAudit.startTime)).Seconds())),
		Abuse: self.peerAudit.Abuse,
		BadContractCount: uint64(self.peerAudit.BadContractCount),
	    DiscardedByteCount: uint64(self.peerAudit.DiscardedByteCount),
	    DiscardedCount: uint64(self.peerAudit.DiscardedCount),
	    BadMessageByteCount: uint64(self.peerAudit.BadMessageByteCount),
	    BadMessageCount: uint64(self.peerAudit.BadMessageCount),
	    SendByteCount: uint64(self.peerAudit.SendByteCount),
	    SendCount: uint64(self.peerAudit.SendCount),
	    ResendByteCount: uint64(self.peerAudit.ResendByteCount),
	    ResendCount: uint64(self.peerAudit.ResendCount),
	}), nil)
	self.peerAudit = nil
}

