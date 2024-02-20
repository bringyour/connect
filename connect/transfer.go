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
	// "runtime/debug"
	"fmt"
	"slices"

	"golang.org/x/exp/maps"

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
	}
}


func DefaultSendBufferSettings() *SendBufferSettings {
	return &SendBufferSettings{
		ContractTimeout: 30 * time.Second,
		ContractRetryInterval: 5 * time.Second,
		// this should be greater than the rtt under load
		// TODO use an rtt estimator based on the ack times
		ResendInterval: 2 * time.Second,
		ResendBackoffScale: 0.25,
		AckTimeout: 300 * time.Second,
		IdleTimeout: 300 * time.Second,
		// pause on resend for selectively acked messaged
		SelectiveAckTimeout: 300 * time.Second,
		SequenceBufferSize: 32,
		// not being able to receive acks will aggravate retries
		AckBufferSize: 8 * 1024,
		MinMessageByteCount: ByteCount(1),
		// this includes transport reconnections
		WriteTimeout: 30 * time.Second,
		ResendQueueMaxByteCount: mib(1),
	}
}


func DefaultReceiveBufferSettings() *ReceiveBufferSettings {
	return &ReceiveBufferSettings {
		GapTimeout: 60 * time.Second,
		IdleTimeout: 300 * time.Second,
		SequenceBufferSize: 32,
		AckBufferSize: 256,
		AckCompressTimeout: 0 * time.Millisecond,
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


func DefaultContractManagerSettings() *ContractManagerSettings {
	return &ContractManagerSettings{
		StandardTransferByteCount: gib(8),
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
}

func DefaultTransferOpts() TransferOptions {
	return TransferOptions{
		Ack: true,
	}
}

func NoAck() TransferOptions {
	return TransferOptions{
		Ack: false,
	}
}


type ClientSettings struct {
	SendBufferSize int
	ForwardBufferSize int
	ReadTimeout time.Duration
	BufferTimeout time.Duration
}


// note all callbacks are wrapped to check for nil and recover from errors
type Client struct {
	ctx context.Context
	cancel context.CancelFunc

	clientId Id
	instanceId Id


	clientSettings *ClientSettings

	sendBufferSettings *SendBufferSettings
	receiveBufferSettings *ReceiveBufferSettings
	forwardBufferSettings *ForwardBufferSettings

	// sendPacks chan *SendPack
	// forwardPacks chan *ForwardPack

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
	return &Client{
		ctx: cancelCtx,
		cancel: cancel,
		clientId: clientId,
		instanceId: NewId(),
		clientSettings: clientSettings,
		sendBufferSettings: DefaultSendBufferSettings(),
		receiveBufferSettings: DefaultReceiveBufferSettings(),
		forwardBufferSettings: DefaultForwardBufferSettings(),
		// sendPacks: make(chan *SendPack, clientSettings.SendBufferSize),
		// forwardPacks: make(chan *ForwardPack, clientSettings.ForwardBufferSize),
		receiveCallbacks: NewCallbackList[ReceiveFunction](),
		forwardCallbacks: NewCallbackList[ForwardFunction](),
	}
}

func (self *Client) ClientId() Id {
	return self.clientId
}

func (self *Client) InstanceId() Id {
	return self.instanceId
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
	// if timeout < 0 {
	// 	select {
	// 	case <- self.ctx.Done():
	// 		return false
	// 	case self.forwardPacks <- forwardPack:
	// 		return true
	// 	}
	// } else if 0 == timeout {
	// 	select {
	// 	case <- self.ctx.Done():
	// 		return false
	// 	case self.forwardPacks <- forwardPack:
	// 		return true
	// 	default:
	// 		// full
	// 		return false
	// 	}
	// } else {
	// 	select {
	// 	case <- self.ctx.Done():
	// 		return false
	// 	case self.forwardPacks <- forwardPack:
	// 		return true
	// 	case <- time.After(timeout):
	// 		// full
	// 		return false
	// 	}
	// }

	err = self.forwardBuffer.Pack(forwardPack, self.clientSettings.BufferTimeout)
	if err != nil {
		fmt.Printf("TIMEOUT FORWARD PACK\n")
		return false
	}
	return true
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
	// if timeout < 0 {
	// 	select {
	// 	case <- self.ctx.Done():
	// 		safeAckCallback(errors.New("Closed."))
	// 		return false
	// 	case self.sendPacks <- sendPack:
	// 		return true
	// 	}
	// } else if 0 == timeout {
	// 	select {
	// 	case <- self.ctx.Done():
	// 		safeAckCallback(errors.New("Closed."))
	// 		return false
	// 	case self.sendPacks <- sendPack:
	// 		return true
	// 	default:
	// 		// full
	// 		safeAckCallback(errors.New("Send buffer full."))
	// 		return false
	// 	}
	// } else {
	// 	select {
	// 	case <- self.ctx.Done():
	// 		safeAckCallback(errors.New("Closed."))
	// 		return false
	// 	case self.sendPacks <- sendPack:
	// 		return true
	// 	case <- time.After(timeout):
	// 		// full
	// 		safeAckCallback(errors.New("Send buffer full."))
	// 		return false
	// 	}
	// }


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
		err := self.sendBuffer.Pack(sendPack, timeout)
		if err != nil {
			fmt.Printf("TIMEOUT SEND PACK\n")
			return false
		}
		return true
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

func (self *Client) Setup(routeManager *RouteManager, contractManager *ContractManager) {
	self.routeManager = routeManager
	self.contractManager = contractManager
	self.sendBuffer = NewSendBuffer(self.ctx, self, routeManager, contractManager, self.sendBufferSettings)
	self.receiveBuffer = NewReceiveBuffer(self.ctx, self, routeManager, contractManager, self.receiveBufferSettings)
	self.forwardBuffer = NewForwardBuffer(self.ctx, self, routeManager, contractManager, self.forwardBufferSettings)
}

func (self *Client) Run() {
	defer self.cancel()


	// forward
	// go func() {
	// 	defer self.cancel()

	// 	for {
	// 		select {
	// 		case <- self.ctx.Done():
	// 			return
	// 		case forwardPack, ok := <- self.forwardPacks:
	// 			if !ok {
	// 				return
	// 			}
	// 			err := forwardBuffer.Pack(forwardPack, self.clientSettings.BufferTimeout)
	// 			if err != nil {
	// 				fmt.Printf("TIMEOUT FORWARD PACK\n")
	// 			}
	// 		}
	// 	}
	// }()

	// send
	// go func() {
	// 	defer self.cancel()

	// 	for {
	// 		select {
	// 		case <- self.ctx.Done():
	// 			return
	// 		case sendPack, ok := <- self.sendPacks:
	// 			if !ok {
	// 				return
	// 			}
	// 			if sendPack.DestinationId == self.clientId {
	// 				// loopback
	// 				func() {
	// 					defer func() {
	// 						if err := recover(); err != nil {
	// 							sendPack.AckCallback(err.(error))
	// 						}
	// 					}()
	// 					self.receive(
	// 						self.clientId,
	// 						[]*protocol.Frame{sendPack.Frame},
	// 						protocol.ProvideMode_Network,
	// 					)
	// 					sendPack.AckCallback(nil)
	// 				}()
	// 			} else {
	// 				err := sendBuffer.Pack(sendPack, self.clientSettings.BufferTimeout)
	// 				if err != nil {
	// 					fmt.Printf("TIMEOUT SEND PACK\n")
	// 				}
	// 			}
	// 		}
	// 	}
	// }()

	
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
		transferFrameBytes, err := multiRouteReader.Read(self.ctx, self.clientSettings.ReadTimeout)
		if err != nil {
			fmt.Printf("READ TIMEOUT: active=%v inactive=%v (%s)\n", multiRouteReader.GetActiveRoutes(), multiRouteReader.GetInactiveRoutes(), err)
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

		fmt.Printf("CLIENT READ %s -> %s via %s\n", sourceId.String(), destinationId.String(), self.clientId.String())

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
				fmt.Printf("GOT ACK AT TOP\n")
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
					fmt.Printf("TIMEOUT ACK\n")
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
				err = self.receiveBuffer.Pack(&ReceivePack{
					SourceId: sourceId,
					SequenceId: sequenceId,
					Pack: pack,
					ReceiveCallback: self.receive,
					MessageByteCount: messageByteCount,
				}, self.clientSettings.BufferTimeout)
				if err != nil {
					fmt.Printf("TIMEOUT RECEIVE PACK\n")
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

			fmt.Printf("CLIENT FORWARD\n")

			transferLog("[%s] Forward %s -> %s", self.clientId.String(), sourceId.String(), destinationId.String())
			self.forward(sourceId, destinationId, transferFrameBytes)
		}
	}
}

func (self *Client) ResendQueueSize(destinationId Id) (int, ByteCount, Id) {
	if self.sendBuffer == nil {
		return 0, 0, Id{}
	} else {
		return self.sendBuffer.ResendQueueSize(destinationId)
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

	// close(self.sendPacks)
	// close(self.forwardPacks)


	self.sendBuffer.Close()
	self.receiveBuffer.Close()
	self.forwardBuffer.Close()

	// for {
	// 	select {
	// 	case sendPack, ok := <- self.sendPacks:
	// 		if !ok {
	// 			return
	// 		}
	// 		sendPack.AckCallback(errors.New("Client closed."))
	// 	}
	// }
}

func (self *Client) Cancel() {
	self.cancel()

	self.sendBuffer.Cancel()
	self.receiveBuffer.Cancel()
	self.forwardBuffer.Cancel()
}

func (self *Client) Reset() {
	// FIXME close buffers and sequences
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

func (self *SendBuffer) Pack(sendPack *SendPack, timeout time.Duration) error {
	initSendSequence := func(skip *SendSequence)(*SendSequence) {
		self.mutex.Lock()
		defer self.mutex.Unlock()

		sendSequence, ok := self.sendSequences[sendPack.DestinationId]
		if ok {
			if skip == nil || skip != sendSequence {
				return sendSequence
			} else {
				sendSequence.Close()
				delete(self.sendSequences, sendPack.DestinationId)
			}
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
				sendSequence.Close()
				delete(self.sendSequences, sendPack.DestinationId)
			}
		}()
		return sendSequence
	}

	sendSequence := initSendSequence(nil)
	if open, err := sendSequence.Pack(sendPack, timeout); !open {
		// sequence closed
		_, err := initSendSequence(sendSequence).Pack(sendPack, timeout)
		return err
	} else {
		return err
	}
}

func (self *SendBuffer) Ack(sourceId Id, ack *protocol.Ack, timeout time.Duration) error {
	self.mutex.Lock()
	sendSequence, ok := self.sendSequences[sourceId]
	if !ok {
		// sequence gone, ignore
		return nil
	}
	self.mutex.Unlock()

	return sendSequence.Ack(ack, timeout)
}

func (self *SendBuffer) ResendQueueSize(destinationId Id) (int, ByteCount, Id) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if sendSequence, ok := self.sendSequences[destinationId]; ok {
		return sendSequence.ResendQueueSize()
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


type SendSequence struct {
	ctx context.Context
	cancel context.CancelFunc

	client *Client
	clientId Id
	routeManager *RouteManager
	contractManager *ContractManager

	destinationId Id
	sequenceId Id

	sendBufferSettings *SendBufferSettings

	sendContract *sequenceContract
	sendContracts map[Id]*sequenceContract

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
		sequenceId: NewId(),
		sendBufferSettings: sendBufferSettings,
		sendContract: nil,
		sendContracts: map[Id]*sequenceContract{},
		packs: make(chan *SendPack, sendBufferSettings.SequenceBufferSize),
		acks: make(chan *protocol.Ack, sendBufferSettings.AckBufferSize),
		resendQueue: newResendQueue(),
		sendItems: []*sendItem{},
		nextSequenceNumber: 0,
		idleCondition: NewIdleCondition(),
	}
}

func (self *SendSequence) ResendQueueSize() (int, ByteCount, Id) {
	count, byteSize := self.resendQueue.resendQueueSize()
	return count, byteSize, self.sequenceId
}

func (self *SendSequence) Pack(sendPack *SendPack, timeout time.Duration) (bool, error) {
	if !self.idleCondition.UpdateOpen() {
		return false, nil
	}
	defer self.idleCondition.UpdateClose()

	if timeout < 0 {
		select {
		case <- self.ctx.Done():
			return false, nil
		case self.packs <- sendPack:
			return true, nil
		}
	} else if timeout == 0 {
		select {
		case <- self.ctx.Done():
			return false, nil
		case self.packs <- sendPack:
			return true, nil
		default:
			return false, nil
		}
	} else {
		select {
		case <- self.ctx.Done():
			return false, nil
		case self.packs <- sendPack:
			return true, nil
		case <- time.After(timeout):
			return true, fmt.Errorf("Timeout")
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
		fmt.Printf("[%s] Send sequence exit -> %s\n", self.clientId.String(), self.destinationId.String())

		self.cancel()

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
			item.ackCallback(errors.New("Send sequence closed."))
		}
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
				if self.sendItems[0].sequenceNumber == item.sequenceNumber && !item.head {
					transferLog("!!!! SET HEAD %d", item.sequenceNumber)
					// set `first=true`
					transferFrameBytes_, err := self.setHead(item.transferFrameBytes)
					if err != nil {
						transferLog("!!!! SET HEAD ERROR %s", err)
						return
					}
					transferFrameBytes = transferFrameBytes_
				} else {
					transferFrameBytes = item.transferFrameBytes
				}
				transferLog("!!!! RESEND %d [%s]", item.sequenceNumber, self.destinationId.String())
				if err := self.multiRouteWriter.Write(self.ctx, transferFrameBytes, self.sendBufferSettings.WriteTimeout); err != nil {
					fmt.Printf("!! WRITE TIMEOUT A\n")
				} else {
					clientId := self.client.ClientId()
					fmt.Printf("WROTE RESEND: %s, %s -> messageId=%s clientId=%s sequenceId=%s\n", item.messageType, clientId.String(), item.messageId.String(), self.destinationId.String(), self.sequenceId.String())
				}

				item.sendCount += 1
				// linear backoff
				// FIXME use constant backoff
				// itemResendTimeout := self.sendBufferSettings.ResendInterval
				itemResendTimeout := time.Duration(float64(self.sendBufferSettings.ResendInterval) * (1 + self.sendBufferSettings.ResendBackoffScale * float64(item.sendCount)))
				if itemResendTimeout < itemAckTimeout {
					item.resendTime = sendTime.Add(itemResendTimeout)
				} else {
					item.resendTime = sendTime.Add(itemAckTimeout)
				}
				self.resendQueue.add(item)
			}
		}

		checkpointId := self.idleCondition.Checkpoint()
		if self.sendBufferSettings.ResendQueueMaxByteCount < self.resendQueue.byteCount {
			fmt.Printf("AT SEND LIMIT %d %d bytes\n", len(self.resendQueue.orderedItems), self.resendQueue.byteCount)
			
			// wait for acks
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
		} else {
			fmt.Printf("NO SEND LIMIT\n")

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
			case sendPack, ok := <- self.packs:
				if !ok {
					return
				}
				// note messages of `size < MinMessageByteCount` get counted as `MinMessageByteCount` against the contract
				if self.updateContract(sendPack.MessageByteCount) {
					transferLog("[%s] Have contract, sending -> %s: %s", self.clientId.String(), self.destinationId.String(), sendPack.Frame)
					item := self.send(sendPack.Frame, sendPack.AckCallback, sendPack.Ack)
					if err := self.multiRouteWriter.Write(self.ctx, item.transferFrameBytes, self.sendBufferSettings.WriteTimeout); err != nil {
						fmt.Printf("!! WRITE TIMEOUT B\n")
					} else {
						fmt.Printf("WROTE FRAME\n")
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

func (self *SendSequence) updateContract(messageByteCount ByteCount) bool {
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
	maxContractMessageByteCount := ByteCount(256)

	if self.contractManager.StandardTransferByteCount() < messageByteCount + maxContractMessageByteCount {
		// this pack does not fit into a standard contract
		// TODO allow requesting larger contracts
		return false
	}

	next := func(contract *protocol.Contract)(bool) {
		sendContract, err := newSequenceContract(contract, self.sendBufferSettings.MinMessageByteCount)
		if err != nil {
			// malformed, drop
			return false
		}

		contractMessageBytes, _ := proto.Marshal(contract)

		if maxContractMessageByteCount < ByteCount(len(contractMessageBytes)) {
			panic("Bad estimate for contract max size could result in infinite contract retries.")
		}

		if sendContract.update(ByteCount(len(contractMessageBytes))) && sendContract.update(messageByteCount) {
			transferLog("[%s] Send contract %s -> %s", self.clientId.String(), sendContract.contractId.String(), self.destinationId.String())
			self.sendContract = sendContract
			self.sendContracts[sendContract.contractId] = sendContract

			// append the contract to the sequence
			item := self.send(&protocol.Frame{
				MessageType: protocol.MessageType_TransferContract,
				MessageBytes: contractMessageBytes,
			}, func(error){}, true)
			if err := self.multiRouteWriter.Write(self.ctx, item.transferFrameBytes, self.sendBufferSettings.WriteTimeout); err != nil {
				fmt.Printf("!! WRITE TIMEOUT C\n")
			} else {
				fmt.Printf("WROTE CONTRACT\n")
			}

			return true
		} else {
			// this contract doesn't fit the message
			// just close it since it was never send to the other side
			self.contractManager.Complete(sendContract.contractId, 0, 0)
			return false
		}
	}

	if nextContract := self.contractManager.TakeContract(self.ctx, self.destinationId, 0); nextContract != nil {
		// async queue up the next contract
		if next(nextContract) {
			return true
		}
	}

	endTime := time.Now().Add(self.sendBufferSettings.ContractTimeout)
	for {
		timeout := endTime.Sub(time.Now())
		if timeout <= 0 {
			return false
		}

		self.contractManager.CreateContract(self.destinationId)

		if self.sendBufferSettings.ContractRetryInterval < timeout {
			timeout = self.sendBufferSettings.ContractRetryInterval
		}

		if nextContract := self.contractManager.TakeContract(self.ctx, self.destinationId, timeout); nextContract != nil {
			// async queue up the next contract
			if next(nextContract) {
				return true
			}
		}
	}
}

func (self *SendSequence) send(frame *protocol.Frame, ackCallback AckFunction, ack bool) *sendItem {
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
		sequenceNumber: sequenceNumber,
		sendTime: sendTime,
		resendTime: sendTime.Add(self.sendBufferSettings.ResendInterval),
		sendCount: 1,
		head: head,
		messageByteCount: ByteCount(len(frame.MessageBytes)),
		transferFrameBytes: transferFrameBytes,
		ackCallback: ackCallback,
		// selectiveAckEnd: time.Time{},
		messageType: frame.MessageType,
	}

	if ack {
		self.sendItems = append(self.sendItems, item)
		self.resendQueue.add(item)
	} else {
		// immediately ack
		ackCallback(nil)
	}

	return item
}

func (self *SendSequence) setHead(transferFrameBytes []byte) ([]byte, error) {
	// TODO this could avoid the memory copy by modifying the raw bytes
	// TODO this is expected to be done infrequently if resends are infrequent, so not an issue

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
	fmt.Printf("RECEIVE ACK %t\n", selective)

	item, ok := self.resendQueue.messageItems[messageId]
	if !ok {
		fmt.Printf("RECEIVE ACK MISS\n")
		transferLog("!!!! NO ACK")
		// message not pending ack
		return
	}

	if selective {
		self.resendQueue.remove(messageId)
		item.resendTime = time.Now().Add(self.sendBufferSettings.SelectiveAckTimeout)
		self.resendQueue.add(item)
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
		self.resendQueue.remove(implicitItem.messageId)
		implicitItem.ackCallback(nil)

		if implicitItem.contractId != nil {
			itemSendContract := self.sendContracts[*implicitItem.contractId]
			itemSendContract.ack(implicitItem.messageByteCount)
			// not current and closed
			if self.sendContract != itemSendContract && itemSendContract.unackedByteCount == 0 {
				self.contractManager.Complete(
					itemSendContract.contractId,
					itemSendContract.ackedByteCount,
					itemSendContract.unackedByteCount,
				)
			}
		}
		self.sendItems[i] = nil
	}
	self.sendItems = self.sendItems[i:]
}

func (self *SendSequence) Close() {
	self.cancel()

	close(self.packs)
	close(self.acks)

	// drain the packs
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
}

func (self *SendSequence) Cancel() {
	self.cancel()
}


type sendItem struct {
	messageId Id
	contractId *Id
	sequenceNumber uint64
	head bool
	sendTime time.Time
	resendTime time.Time
	sendCount int
	messageByteCount ByteCount
	transferFrameBytes []byte
	ackCallback AckFunction

	// selectiveAckEnd time.Time

	// the index of the item in the heap
	heapIndex int

	messageType protocol.MessageType
}


// a send event queue which is the union of:
// - resend times
// - ack timeouts
type resendQueue struct {
	orderedItems []*sendItem
	// message_id -> item
	messageItems map[Id]*sendItem
	byteCount ByteCount
	stateLock sync.Mutex
}

func newResendQueue() *resendQueue {
	resendQueue := &resendQueue{
		orderedItems: []*sendItem{},
		messageItems: map[Id]*sendItem{},
		byteCount: ByteCount(0),
	}
	heap.Init(resendQueue)
	return resendQueue
}

func (self *resendQueue) resendQueueSize() (int, ByteCount) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	return len(self.orderedItems), self.byteCount
}

func (self *resendQueue) add(item *sendItem) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.messageItems[item.messageId] = item
	heap.Push(self, item)
	self.byteCount += ByteCount(len(item.transferFrameBytes))
}

func (self *resendQueue) remove(messageId Id) *sendItem {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	item, ok := self.messageItems[messageId]
	if !ok {
		return nil
	}
	delete(self.messageItems, messageId)
	item_ := heap.Remove(self, item.heapIndex)
	if item != item_ {
		panic("Heap invariant broken.")
	}
	self.byteCount -= ByteCount(len(item.transferFrameBytes))
	return item
}

func (self *resendQueue) removeFirst() *sendItem {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	first := heap.Pop(self)
	if first == nil {
		return nil
	}
	item := first.(*sendItem)
	delete(self.messageItems, item.messageId)
	self.byteCount -= ByteCount(len(item.transferFrameBytes))
	return item
}

// heap.Interface

func (self *resendQueue) Push(x any) {
	item := x.(*sendItem)
	item.heapIndex = len(self.orderedItems)
	self.orderedItems = append(self.orderedItems, item)
}

func (self *resendQueue) Pop() any {
	i := len(self.orderedItems) - 1
	item := self.orderedItems[i]
	self.orderedItems[i] = nil
	self.orderedItems = self.orderedItems[:i]
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

func (self *ReceiveBuffer) Pack(receivePack *ReceivePack, timeout time.Duration) error {
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
	if open, err := receiveSequence.Pack(receivePack, timeout); !open {
		_, err := initReceiveSequence(receiveSequence).Pack(receivePack, timeout)
		return err
	} else {
		return err
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
		packs: make(chan *ReceivePack, receiveBufferSettings.SequenceBufferSize),
		receiveQueue: newReceiveQueue(),
		nextSequenceNumber: 0,
		idleCondition: NewIdleCondition(),
		sendAcks: make(chan *sendAck, receiveBufferSettings.AckBufferSize),
	}
}

func (self *ReceiveSequence) ReceiveQueueSize() (int, ByteCount) {
	return self.receiveQueue.receiveQueueSize()
}

func (self *ReceiveSequence) Pack(receivePack *ReceivePack, timeout time.Duration) (bool, error) {
	if !self.idleCondition.UpdateOpen() {
		return false, nil
	}
	defer self.idleCondition.UpdateClose()

	if timeout < 0 {
		select {
		case <- self.ctx.Done():
			return false, nil
		case self.packs <- receivePack:
			return true, nil
		}
	} else if timeout == 0 {
		select {
		case <- self.ctx.Done():
			return false, nil
		case self.packs <- receivePack:
			return true, nil
		default:
			return false, nil
		}
	} else {
		select {
		case <- self.ctx.Done():
			return false, nil
		case self.packs <- receivePack:
			return true, nil
		case <- time.After(timeout):
			return true, fmt.Errorf("Timeout")
		}
	}
}

func (self *ReceiveSequence) Run() {
	defer func() {
		fmt.Printf("[%s] Receive sequence exit %s ->\n", self.clientId.String(), self.sourceId.String())

		self.cancel()

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
			self.peerAudit.Update(func(a *PeerAudit) {
				a.discard(item.messageByteCount)
			})
		}

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
		
		if 0 == self.receiveQueue.Len() {
			timeout = self.receiveBufferSettings.IdleTimeout
		} else {
			timeout = self.receiveBufferSettings.GapTimeout
			for 0 < self.receiveQueue.Len() {
				item := self.receiveQueue.removeFirst()

				itemGapTimeout := item.receiveTime.Add(self.receiveBufferSettings.GapTimeout).Sub(receiveTime)
				if itemGapTimeout < 0 {
					transferLog("!! EXIT H")
					transferLog("[%s] Gap timeout", self.clientId.String())
					// did not receive a preceding message in time
					// close sequence
					return
				}

				if self.nextSequenceNumber < item.sequenceNumber {
					transferLog("[%s] Head of sequence is not next %d <> %d: %s", self.clientId.String(), self.nextSequenceNumber, item.sequenceNumber, item.frames)
					// put back
					self.receiveQueue.add(item)
					if itemGapTimeout < timeout {
						timeout = itemGapTimeout
					}
					break
				}
				// item.sequenceNumber <= self.nextSequenceNumber

				
				
				if self.nextSequenceNumber == item.sequenceNumber {
					// this item is the head of sequence
					transferLog("[%s] Receive next in sequence %d: %s", self.clientId.String(), item.sequenceNumber, item.frames)

					self.nextSequenceNumber = item.sequenceNumber + 1	
				
				
					if err := self.registerContracts(item); err != nil {
						return
					}
					transferLog("[%s] Receive head of sequence %d.", self.clientId.String(), item.sequenceNumber)
					if self.updateContract(item) {
						self.receiveHead(item)
					} else {
						transferLog("!! EXIT M")
						// no contract
						// close the sequence
						self.peerAudit.Update(func(a *PeerAudit) {
							a.discard(item.messageByteCount)
						})
						return
					}
				}
				// else this item is a resend of a previous item
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
			messageId, err := IdFromBytes(receivePack.Pack.MessageId)
			if err != nil {
				// bad message
				// close the sequence
				self.peerAudit.Update(func(a *PeerAudit) {
					a.badMessage(receivePack.MessageByteCount)
				})
				transferLog("!! EXIT C")
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

					fmt.Printf("RECEIVE DROPPED A MESSAGE\n")
				}

			// note messages of `size < MinMessageByteCount` get counted as `MinMessageByteCount` against the contract
			} else if self.nextSequenceNumber <= receivePack.Pack.SequenceNumber {
				// store only up to a max size in the receive queue
				canBuffer := func(byteCount ByteCount)(bool) {
			        // always allow at least one item in the receive queue
			        if 0 == self.receiveQueue.Len() {
			            return true
			        }
			        return self.receiveQueue.byteCount + byteCount < self.receiveBufferSettings.ReceiveQueueMaxByteCount
				}

				// remove later items to fit
				for !canBuffer(receivePack.MessageByteCount) {
			        transferLog("!!!! 3")
			        lastItem := self.receiveQueue.peekLast()
			        if receivePack.Pack.SequenceNumber < lastItem.sequenceNumber {
			           	self.receiveQueue.remove(lastItem.messageId)
			        } else {
			        	break
			        }
				}

				if canBuffer(receivePack.MessageByteCount) {
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

						fmt.Printf("RECEIVE DROPPED A MESSAGE\n")
					}
				} else {
					transferLog("!!!! 5b")
					// drop the message
					self.peerAudit.Update(func(a *PeerAudit) {
						a.discard(receivePack.MessageByteCount)
					})

					fmt.Printf("DROPPED A MESSAGE LIMIT\n")
				}
			} else {
				transferLog("!!!! 6")
				// already received
				self.peerAudit.Update(func(a *PeerAudit) {
					a.resend(receivePack.MessageByteCount)
				})
				self.sendAck(receivePack.Pack.SequenceNumber, messageId, false)
				fmt.Printf("ACK B\n")
				// if err != nil {
				// 	transferLog("!! EXIT E")
				// 	// could not send ack
				// 	return
				// }
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

		if err := multiRouteWriter.Write(self.ctx, transferFrameBytes, self.receiveBufferSettings.WriteTimeout); err != nil {
			fmt.Printf("!! WRITE TIMEOUT D\n")
		} else {
			fmt.Printf("WROTE ACK: %s -> messageId=%s clientId=%s sequenceId=%s\n", clientId.String(), sendAck.messageId.String(), self.sourceId.String(), self.sequenceId.String())
		}
	}

	for {
		select {
		case <- self.ctx.Done():
			return
		default:
		}

		compressStartTime := time.Now()

		acks := map[uint64]*sendAck{}
		selectiveAcks := map[uint64]*sendAck{}

		addAck := func(sendAck *sendAck) {
			if sendAck.selective {
				selectiveAcks[sendAck.sequenceNumber] = sendAck
			} else {
				acks[sendAck.sequenceNumber] = sendAck
			}
		}

		CollapseLoop:
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

		for {
			timeout := self.receiveBufferSettings.AckCompressTimeout - time.Now().Sub(compressStartTime)
			if timeout <= 0 {
				break
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

			if sendAck, ok := acks[maxAckSequenceNumber]; ok {
				writeAck(sendAck)
			}
			for sequenceNumber, sendAck := range selectiveAcks {
				if maxAckSequenceNumber < sequenceNumber {
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

func (self *ReceiveSequence) receiveHead(item *receiveItem) {
	self.peerAudit.Update(func(a *PeerAudit) {
		a.received(item.messageByteCount)
	})
	var provideMode protocol.ProvideMode
	if self.receiveContract != nil {
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
		fmt.Printf("ACK A\n")
	}
}


func (self *ReceiveSequence) registerContracts(item *receiveItem) error {
	// FIXME
	if true {
		return nil
	}

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
				transferLog("!! EXIT I")
				// bad message
				// close sequence
				self.peerAudit.Update(func(a *PeerAudit) {
					a.badMessage(item.messageByteCount)
				})
				return err
			}

			// FIXME
			/*
			// check the hmac with the local provider secret key
			if !self.contractManager.Verify(
					contract.StoredContractHmac,
					contract.StoredContractBytes,
					contract.ProvideMode) {
				transferLog("!! EXIT J")
				// bad contract
				// close sequence
				self.peerAudit.Update(func(a *PeerAudit) {
					a.badContract()
				})
				return
			}
			*/

			self.receiveContract, err = newSequenceContract(&contract, self.receiveBufferSettings.MinMessageByteCount)
			if err != nil {
				transferLog("!! EXIT K")
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

func (self *ReceiveSequence) receive(receivePack *ReceivePack) (bool, error) {
	// pre condition: the sequenceNumber and messageId have been removed from the receiveQueue

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
		contractId: contractId,
		messageId: messageId,
		sequenceNumber: sequenceNumber,
		receiveTime: receiveTime,
		frames: receivePack.Pack.Frames,
		messageByteCount: receivePack.MessageByteCount,
		receiveCallback: receivePack.ReceiveCallback,
		head: receivePack.Pack.Head,
		ack: !receivePack.Pack.Nack,
	}

	if item.head && self.nextSequenceNumber < item.sequenceNumber {
		fmt.Printf("HEAD ADVANCE %d -> %d\n", self.nextSequenceNumber, item.sequenceNumber)
		self.nextSequenceNumber = item.sequenceNumber
	}


	// replace with the latest value (check both messageId and sequenceNumber)
	if item := self.receiveQueue.remove(messageId); item != nil {
		transferLog("!!!! 1")
		self.peerAudit.Update(func(a *PeerAudit) {
			a.resend(item.messageByteCount)
		})
	}
	if item := self.receiveQueue.removeBySequenceNumber(receivePack.Pack.SequenceNumber); item != nil {
		transferLog("!!!! 2")
		self.peerAudit.Update(func(a *PeerAudit) {
			a.resend(item.messageByteCount)
		})
	}


	if sequenceNumber <= self.nextSequenceNumber {
		if self.nextSequenceNumber == sequenceNumber {
			// this item is the head of sequence

			fmt.Printf("[%s] Receive next in sequence [%d]\n", self.clientId.String(), item.sequenceNumber)

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
			return false, nil
		}
	} else {
		self.receiveQueue.add(item)
		self.sendAck(sequenceNumber, messageId, true)
		fmt.Printf("ACK C SELECTIVE\n")
		return true, nil
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
		contractId: contractId,
		messageId: messageId,
		sequenceNumber: sequenceNumber,
		receiveTime: receiveTime,
		frames: receivePack.Pack.Frames,
		messageByteCount: receivePack.MessageByteCount,
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

func (self *ReceiveSequence) sendAck(sequenceNumber uint64, messageId Id, selective bool) {
	self.sendAcks <- &sendAck{
		sequenceNumber: sequenceNumber,
		messageId: messageId,
		selective: selective,
	}
}

func (self *ReceiveSequence) Close() {
	self.cancel()

	close(self.packs)

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
	contractId *Id
	messageId Id
	
	sequenceNumber uint64
	head bool
	receiveTime time.Time
	frames []*protocol.Frame
	messageByteCount ByteCount
	receiveCallback ReceiveFunction

	// the index of the item in the heap
	heapIndex int

	ack bool
}


// ordered by sequenceNumber
type receiveQueue struct {
	orderedItems []*receiveItem
	// message_id -> item
	messageItems map[Id]*receiveItem
	byteCount ByteCount
	stateLock sync.Mutex
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

func (self *receiveQueue) receiveQueueSize() (int, ByteCount) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	return len(self.orderedItems), self.byteCount
}

func (self *receiveQueue) add(item *receiveItem) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.messageItems[item.messageId] = item
	heap.Push(self, item)
	transferLog("!!!! P %d %d", item.sequenceNumber, item.heapIndex)
	self.byteCount += item.messageByteCount
}

func (self *receiveQueue) remove(messageId Id) *receiveItem {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	item, ok := self.messageItems[messageId]
	if !ok {
		return nil
	}
	delete(self.messageItems, messageId)
	item_ := heap.Remove(self, item.heapIndex)
	if item != item_ {
		panic("Heap invariant broken.")
	}

	// self.orderedItems = slices.DeleteFunc(self.orderedItems, func(i *receiveItem)(bool) {
	// 	return i.messageId == messageId
	// })
	// heap.Init(self)
	self.byteCount -= item.messageByteCount
	return item
}

func (self *receiveQueue) removeBySequenceNumber(sequenceNumber uint64) *receiveItem {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	i, found := sort.Find(len(self.orderedItems), func(i int)(int) {
		d := sequenceNumber - self.orderedItems[i].sequenceNumber
		if d < 0 {
			return -1
		} else if 0 < d {
			return 1
		} else {
			return 0
		}
	})
	if found && sequenceNumber == self.orderedItems[i].sequenceNumber {
		return self.remove(self.orderedItems[i].messageId)
	}
	return nil
}

func (self *receiveQueue) removeFirst() *receiveItem {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	first := heap.Pop(self)
	if first == nil {
		return nil
	}
	item := first.(*receiveItem)
	transferLog("!!!! POP %d %d: %d", item.sequenceNumber, item.heapIndex, self.Len())
	delete(self.messageItems, item.messageId)
	self.byteCount -= item.messageByteCount
	return item
}

func (self *receiveQueue) peekLast() *receiveItem {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

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
	return self.orderedItems[i].sequenceNumber < self.orderedItems[j].sequenceNumber
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
	transferByteCount ByteCount
	provideMode protocol.ProvideMode

	minUpdateByteCount ByteCount

	sourceId Id
	destinationId Id
	
	ackedByteCount ByteCount
	unackedByteCount ByteCount
}

func newSequenceContract(contract *protocol.Contract, minUpdateByteCount ByteCount) (*sequenceContract, error) {
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
	if self.transferByteCount < self.ackedByteCount + self.unackedByteCount + roundedByteCount {
		// doesn't fit in contract
		return false
	}
	self.unackedByteCount += roundedByteCount
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

func (self *ForwardBuffer) Pack(forwardPack *ForwardPack, timeout time.Duration) error {
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
	if open, err := forwardSequence.Pack(forwardPack, timeout); !open {
		_, err := initForwardSequence(forwardSequence).Pack(forwardPack, timeout)
		return err
	} else {
		return err
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

func (self *ForwardSequence) Pack(forwardPack *ForwardPack, timeout time.Duration) (bool, error) {
	if !self.idleCondition.UpdateOpen() {
		return false, nil
	}
	defer self.idleCondition.UpdateClose()

	if timeout < 0 {
		select {
		case <- self.ctx.Done():
			return false, nil
		case self.packs <- forwardPack:
			return true, nil
		}
	} else if timeout == 0 {
		select {
		case <- self.ctx.Done():
			return false, nil
		case self.packs <- forwardPack:
			return true, nil
		default:
			return false, nil
		}
	} else {
		select {
		case <- self.ctx.Done():
			return false, nil
		case self.packs <- forwardPack:
			return true, nil
		case <- time.After(timeout):
			return true, fmt.Errorf("Timeout")
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
		case forwardPack := <- self.packs:
			if err := self.multiRouteWriter.Write(self.ctx, forwardPack.TransferFrameBytes, self.forwardBufferSettings.WriteTimeout); err != nil {
				fmt.Printf("!! WRITE TIMEOUT E: active=%v inactive=%v (%s)\n", self.multiRouteWriter.GetActiveRoutes(), self.multiRouteWriter.GetInactiveRoutes(), err)
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
    // FIXME rename to Transfer*
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
	GetActiveRoutes() []Route
	GetInactiveRoutes() []Route
}


type MultiRouteReader interface {
	Read(ctx context.Context, timeout time.Duration) ([]byte, error)
	GetActiveRoutes() []Route
	GetInactiveRoutes() []Route
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
		transportRoutes: map[Transport][]Route{},
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
			if self.matches(transport, destinationId) {
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
		if stats, ok := self.routeStats[currentRoute]; ok {
			netStats.sendCount += stats.sendCount
			netStats.sendByteCount += stats.sendByteCount
			netStats.receiveCount += stats.receiveCount
			netStats.receiveByteCount += stats.receiveByteCount
		}
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
				if stats, ok := self.routeStats[currentRoute]; ok {
					netStats.sendCount += stats.sendCount
					netStats.sendByteCount += stats.sendByteCount
					netStats.receiveCount += stats.receiveCount
					netStats.receiveByteCount += stats.receiveByteCount
				}
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
		slices.SortStableFunc(orderedTransports, func(a Transport, b Transport)(int) {
			return a.Priority() - b.Priority()
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

	self.transportUpdate.NotifyAll()
}

func (self *MultiRouteSelector) GetActiveRoutes() []Route {
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

func (self *MultiRouteSelector) GetInactiveRoutes() []Route {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	inactiveRoutes := []Route{}
	for _, routes := range self.transportRoutes {
		for _, route := range routes {
			if !self.routeActive[route] {
				inactiveRoutes = append(inactiveRoutes, route)
			}
		}
	}

	return inactiveRoutes
}

func (self *MultiRouteSelector) setActive(route Route, active bool) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if _, ok := self.routeActive[route]; ok {
		self.routeActive[route] = false
	}
}

func (self *MultiRouteSelector) updateSendStats(route Route, sendCount int, sendByteCount ByteCount) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	stats, ok := self.routeStats[route]
	if !ok {
		stats = NewRouteStats()
		self.routeStats[route] = stats
	}
	stats.sendCount += sendCount
	stats.sendByteCount += sendByteCount
}

func (self *MultiRouteSelector) updateReceiveStats(route Route, receiveCount int, receiveByteCount ByteCount) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	stats, ok := self.routeStats[route]
	if !ok {
		stats = NewRouteStats()
		self.routeStats[route] = stats
	}
	stats.receiveCount += receiveCount
	stats.receiveByteCount += receiveByteCount
}

// MultiRouteWriter
func (self *MultiRouteSelector) Write(ctx context.Context, transportFrameBytes []byte, timeout time.Duration) error {
	// write to the first channel available, in random priority
	enterTime := time.Now()
	for {
		notify := self.transportUpdate.NotifyChannel()
		activeRoutes := self.GetActiveRoutes()

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

		// add the update case
		transportUpdateIndex := len(selectCases)
		selectCases = append(selectCases, reflect.SelectCase{
			Dir: reflect.SelectRecv,
			Chan: reflect.ValueOf(notify),
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

		// note writing to a channel does not return an ok value
		chosenIndex, _, _ := reflect.Select(selectCases)

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
			self.updateSendStats(route, 1, ByteCount(len(transportFrameBytes)))
			return nil
		}
	}
}

// MultiRouteReader
func (self *MultiRouteSelector) Read(ctx context.Context, timeout time.Duration) ([]byte, error) {
	// read from the first channel available, in random priority
	enterTime := time.Now()
	for {
		notify := self.transportUpdate.NotifyChannel()
		activeRoutes := self.GetActiveRoutes()
		
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

		// add the update case
		transportUpdateIndex := len(selectCases)
		selectCases = append(selectCases, reflect.SelectCase{
			Dir: reflect.SelectRecv,
			Chan: reflect.ValueOf(notify),
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
				self.updateReceiveStats(route, 1, ByteCount(len(transportFrameBytes)))
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
	sendByteCount ByteCount
	receiveCount int
	receiveByteCount ByteCount
}

func NewRouteStats() *RouteStats {
	return &RouteStats{
		sendCount: 0,
		sendByteCount: ByteCount(0),
		receiveCount: 0,
		receiveByteCount: ByteCount(0),
	}
}


type ContractManagerSettings struct {
	StandardTransferByteCount ByteCount
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

func (self *ContractManager) StandardTransferByteCount() ByteCount {
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
			if message, err := FromFrame(frame); err == nil {
				switch v := message.(type) {
				case *protocol.CreateContractResult:
					if contractError := v.Error; contractError != nil {
						self.error(*contractError)
					} else if contract := v.Contract; contract != nil {
						err := self.addContract(contract)
						if err != nil {
							panic(err)
						}
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

func (self *ContractManager) SetProvideModes(provideModes map[protocol.ProvideMode]bool) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	currentProvideModes := maps.Keys(self.provideSecretKeys)
	for _, provideMode := range currentProvideModes {
		if allow, ok := provideModes[provideMode]; !ok || !allow {
			delete(self.provideSecretKeys, provideMode)
		}
	}

	provideKeys := []*protocol.ProvideKey{}
	for provideMode, allow := range provideModes {
		if allow {
			provideSecretKey, ok := self.provideSecretKeys[provideMode]
			if !ok {
				// generate a new key
				provideSecretKey = make([]byte, 32)
		    	_, err := rand.Read(provideSecretKey)
		    	if err != nil {
		    		panic(err)
		    	}
				self.provideSecretKeys[provideMode] = provideSecretKey
			}
			provideKeys = append(provideKeys, &protocol.ProvideKey{
				Mode: provideMode,
				ProvideSecretKey: provideSecretKey,
			})
		}
	}

	provide := &protocol.Provide{
		Keys: provideKeys,
	}
	self.client.SendControl(RequireToFrame(provide), func(err error) {
		transferLog("Set provide complete (%s)", err)
	})
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

func (self *ContractManager) GetProvideSecretKey(provideMode protocol.ProvideMode) ([]byte, bool) {
	provideSecretKey, ok := self.provideSecretKeys[provideMode]
	return provideSecretKey, ok
}

func (self *ContractManager) RequireProvideSecretKey(provideMode protocol.ProvideMode) []byte {
	secretKey, ok := self.GetProvideSecretKey(provideMode)
	if !ok {
		panic(fmt.Errorf("Missing provide secret for %s", provideMode))
	}
	return secretKey
}

func (self *ContractManager) AddNoContractPeer(clientId Id) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.sendNoContractClientIds[clientId] = true
	self.receiveNoContractClientIds[clientId] = true
}

func (self *ContractManager) SendNoContract(destinationId Id) bool {
	// FIXME
	if true {
		return true
	}


	self.mutex.Lock()
	defer self.mutex.Unlock()

	if allow, ok := self.sendNoContractClientIds[destinationId]; ok {
		return allow
	}
	return false
}

func (self *ContractManager) ReceiveNoContract(sourceId Id) bool {
	// FIXME
	if true {
		return true
	}


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
		} else if timeout == 0 {
			return nil
		} else {
			remainingTimeout := enterTime.Add(timeout).Sub(time.Now())
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

func (self *ContractManager) addContract(contract *protocol.Contract) error {
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
		TransferByteCount: uint64(self.contractManagerSettings.StandardTransferByteCount),
	}
	self.client.SendControl(RequireToFrame(createContract), nil)
}

func (self *ContractManager) Complete(contractId Id, ackedByteCount ByteCount, unackedByteCount ByteCount) {
	closeContract := &protocol.CloseContract{
		ContractId: contractId.Bytes(),
		AckedByteCount: uint64(ackedByteCount),
		UnackedByteCount: uint64(unackedByteCount),
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

	self.updateMonitor.NotifyAll()
}

func (self *ContractQueue) empty() bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	return 0 == self.openCount && 0 == len(self.contracts)
}
