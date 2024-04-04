package connect

import (
	"context"
	"time"
	"sync"
	"errors"
	"math"
	"fmt"
	// "runtime/debug"
	"runtime"
	"reflect"
	"strings"

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

// use 0 for deadlock testing
const DefaultTransferBufferSize = 32


type AckFunction = func(err error)
// provideMode is the mode of where these frames are from: network, friends and family, public
// provideMode nil means no contract
type ReceiveFunction = func(sourceId Id, frames []*protocol.Frame, provideMode protocol.ProvideMode)
type ForwardFunction = func(sourceId Id, destinationId Id, transferFrameBytes []byte)


// destination id for control messages
var ControlId = Id{}


// in this case there are no intermediary hops
// the contract is signed with the local provide keys
var DirectStreamId = Id{}


func DefaultClientSettings() *ClientSettings {
	return &ClientSettings{
		SendBufferSize: DefaultTransferBufferSize,
		ForwardBufferSize: DefaultTransferBufferSize,
		ReadTimeout: 30 * time.Second,
		BufferTimeout: 30 * time.Second,
		ControlWriteTimeout: 30 * time.Second,
		SendBufferSettings: DefaultSendBufferSettings(),
		ReceiveBufferSettings: DefaultReceiveBufferSettings(),
		ForwardBufferSettings: DefaultForwardBufferSettings(),
		ContractManagerSettings: DefaultContractManagerSettings(),
	}
}


func DefaultClientSettingsNoNetworkEvents() *ClientSettings {
	clientSettings := DefaultClientSettings()
	clientSettings.ContractManagerSettings = DefaultContractManagerSettingsNoNetworkEvents()
	return clientSettings
}


func DefaultSendBufferSettings() *SendBufferSettings {
	return &SendBufferSettings{
		CreateContractTimeout: 30 * time.Second,
		CreateContractRetryInterval: 5 * time.Second,
		// this should be greater than the rtt under load
		// TODO use an rtt estimator based on the ack times
		ResendInterval: 1 * time.Second,
		// no backoff
		ResendBackoffScale: 0,
		AckTimeout: 60 * time.Second,
		IdleTimeout: 60 * time.Second,
		// pause on resend for selectively acked messaged
		SelectiveAckTimeout: 5 * time.Second,
		SequenceBufferSize: DefaultTransferBufferSize,
		AckBufferSize: DefaultTransferBufferSize,
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
		// the receive idle timeout should be a bit longer than the send idle timeout
		IdleTimeout: 120 * time.Second,
		SequenceBufferSize: DefaultTransferBufferSize,
		// AckBufferSize: DefaultTransferBufferSize,
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
		SequenceBufferSize: DefaultTransferBufferSize,
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
	ControlWriteTimeout time.Duration

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
	clientTag string
	clientOob OutOfBandControl

	settings *ClientSettings

	receiveCallbacks *CallbackList[ReceiveFunction]
	forwardCallbacks *CallbackList[ForwardFunction]

	loopback chan *SendPack

	routeManager *RouteManager
	contractManager *ContractManager
	sendBuffer *SendBuffer
	receiveBuffer *ReceiveBuffer
	forwardBuffer *ForwardBuffer

	contractManagerUnsub func()
}

func NewClientWithDefaults(
	ctx context.Context,
	clientId Id,
	clientOob OutOfBandControl,
) *Client {
	return NewClient(
		ctx,
		clientId,
		clientOob,
		DefaultClientSettings(),
	)
}

func NewClient(
	ctx context.Context,
	clientId Id,
	clientOob OutOfBandControl,
	settings *ClientSettings,
) *Client {
	clientTag := clientId.String()
	return NewClientWithTag(ctx, clientId, clientTag, clientOob, settings)
}

func NewClientWithTag(
	ctx context.Context,
	clientId Id,
	clientTag string,
	clientOob OutOfBandControl,
	settings *ClientSettings,
) *Client {
	cancelCtx, cancel := context.WithCancel(ctx)
	client := &Client{
		ctx: cancelCtx,
		cancel: cancel,
		clientId: clientId,
		clientTag: clientTag,
		clientOob: clientOob,
		settings: settings,
		receiveCallbacks: NewCallbackList[ReceiveFunction](),
		forwardCallbacks: NewCallbackList[ForwardFunction](),
		loopback: make(chan *SendPack),
	}

	routeManager := NewRouteManager(ctx, clientTag)
	contractManager := NewContractManager(ctx, client, settings.ContractManagerSettings)

	client.contractManagerUnsub = client.AddReceiveCallback(contractManager.Receive)

	client.initBuffers(routeManager, contractManager)

	go client.run()

	return client
}

func (self *Client) initBuffers(routeManager *RouteManager, contractManager *ContractManager) {
	self.routeManager = routeManager
	self.contractManager = contractManager
	self.sendBuffer = NewSendBuffer(self.ctx, self, routeManager, contractManager, self.settings.SendBufferSettings)
	self.receiveBuffer = NewReceiveBuffer(self.ctx, self, routeManager, contractManager, self.settings.ReceiveBufferSettings)
	self.forwardBuffer = NewForwardBuffer(self.ctx, self, routeManager, contractManager, self.settings.ForwardBufferSettings)
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

func (self *Client) ClientTag() string {
	return self.clientTag
}

func (self *Client) ClientOob() OutOfBandControl {
	return self.clientOob
}

func (self *Client) ReportAbuse(sourceId Id) {
	peerAudit := NewSequencePeerAudit(self, sourceId, 0)
	peerAudit.Update(func (peerAudit *PeerAudit) {
		peerAudit.Abuse = true
	})
	peerAudit.Complete()
}

func (self *Client) ForwardWithTimeout(transferFrameBytes []byte, timeout time.Duration) bool {
	success, err := self.ForwardWithTimeoutDetailed(transferFrameBytes, timeout)
	return success && err == nil
}

func (self *Client) ForwardWithTimeoutDetailed(transferFrameBytes []byte, timeout time.Duration) (bool, error) {
	select {
	case <- self.ctx.Done():
		return false, errors.New("Done")
	default:
	}

	var filteredTransferFrame protocol.FilteredTransferFrame
	if err := proto.Unmarshal(transferFrameBytes, &filteredTransferFrame); err != nil {
		// bad protobuf
		return false, err
	}
	destinationId, err := IdFromBytes(filteredTransferFrame.TransferPath.DestinationId)
	if err != nil {
		// bad protobuf
		return false, err
	}

	forwardPack := &ForwardPack{
		DestinationId: destinationId,
		TransferFrameBytes: transferFrameBytes,
	}

	return self.forwardBuffer.Pack(forwardPack, timeout)
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
	success, err := self.SendWithTimeoutDetailed(frame, destinationId, ackCallback, timeout, opts...)
	return success && err == nil
}

func (self *Client) SendWithTimeoutDetailed(
	frame *protocol.Frame,
	destinationId Id,
	ackCallback AckFunction,
	timeout time.Duration,
	opts ...any,
) (bool, error) {
	select {
	case <- self.ctx.Done():
		return false, errors.New("Done")
	default:
	}

	safeAckCallback := func(err error) {
		if ackCallback != nil {
			HandleError(func() {
				ackCallback(err)
			})
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
		if timeout < 0 {
			select {
			case <- self.ctx.Done():
				return false, errors.New("Done")
			case self.loopback <- sendPack:
				return true, nil
			}
		} else if timeout == 0 {
			select {
			case <- self.ctx.Done():
				return false, errors.New("Done")
			case self.loopback <- sendPack:
				return true, nil
			default:
				return false, nil
			}
		} else {
			select {
			case <- self.ctx.Done():
				return false, errors.New("Done")
			case self.loopback <- sendPack:
				return true, nil
			case <- time.After(timeout):
				return false, nil
			}
		}
	} else {
		return self.sendBuffer.Pack(sendPack, timeout)
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
	// transferLog("!! DISPATCH RECEIVE")
	for _, receiveCallback := range self.receiveCallbacks.Get() {
		receiveCallbackName := runtime.FuncForPC(reflect.ValueOf(receiveCallback).Pointer()).Name()
		TraceWithReturn(
			fmt.Sprintf("[c]receive callback %s %s", self.clientTag, receiveCallbackName),
			func()(any) {
				return HandleError(func() {
					receiveCallback(sourceId, frames, provideMode)
				})
			},
		)	
	}
}

// ForwardFunction
func (self *Client) forward(sourceId Id, destinationId Id, transferFrameBytes []byte) {
	for _, forwardCallback := range self.forwardCallbacks.Get() {
		forwardCallbackName := runtime.FuncForPC(reflect.ValueOf(forwardCallback).Pointer()).Name()
		TraceWithReturn(
			fmt.Sprintf("[c]forward callback %s %s", self.clientTag, forwardCallbackName),
			func()(any) {
				return HandleError(func() {
					forwardCallback(sourceId, destinationId, transferFrameBytes)
				})
			},
		)
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

	// loopback messages must be serialized
	go func() {
		for {
			select {
			case <- self.ctx.Done():
				return
			case sendPack := <- self.loopback:
				HandleError(func() {
					self.receive(
						self.clientId,
						[]*protocol.Frame{sendPack.Frame},
						protocol.ProvideMode_Network,
					)
					sendPack.AckCallback(nil)
				}, func(err error) {
					sendPack.AckCallback(err)
				})
			}
		}
	}()

	for {
		select {
		case <- self.ctx.Done():
			return
		default:
		}
		// fmt.Printf("read (->%s) active=%v inactive=%v\n", self.clientId, multiRouteReader.GetActiveRoutes(), multiRouteReader.GetInactiveRoutes())
			
		var transferFrameBytes []byte
		err := TraceWithReturn(
			fmt.Sprintf("[c]multi route read %s<-", self.clientTag),
			func()(error) {
				var err error
				transferFrameBytes, err = multiRouteReader.Read(self.ctx, self.settings.ReadTimeout)
				return err
			},
		)
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

		fmt.Printf("[cr] %s %s<-%s\n", self.clientTag, destinationId.String(), sourceId.String())

		transferLog("CLIENT READ %s -> %s via %s\n", sourceId.String(), destinationId.String(), self.clientTag)

		transferLog("[%s] Read %s -> %s", self.clientTag, sourceId.String(), destinationId.String())
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
				transferLog("[%s] Receive ack %s ->: %s", self.clientTag, sourceId.String(), ack)
				TraceWithReturn(
					fmt.Sprintf("[cr]ack %s %s<-%s", self.clientTag, destinationId.String(), sourceId.String()),
					func()(bool) {
						return self.sendBuffer.Ack(sourceId, ack, self.settings.BufferTimeout)
					},
				)
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
				messageByteCount := MessageByteCount(pack.Frames)
				TraceWithReturn(
					fmt.Sprintf("[cr]pack %s %s<-%s", self.clientTag, destinationId.String(), sourceId.String()),
					func()(bool) {
						success, err := self.receiveBuffer.Pack(&ReceivePack{
							SourceId: sourceId,
							SequenceId: sequenceId,
							Pack: pack,
							ReceiveCallback: self.receive,
							MessageByteCount: messageByteCount,
						}, self.settings.BufferTimeout)
						return success && err == nil
					},
				)
				// transferLog("[%s] Receive pack %s ->: %s", self.clientTag, sourceId.String(), pack.Frames)
			default:
				transferLog("[%s] Receive unknown -> %s: %s", self.clientTag, sourceId.String(), frame)
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
					transferLog("[%s] Forward ack %s ->: %s", self.clientTag, sourceId.String(), ack)
				case protocol.MessageType_TransferPack:
					pack := &protocol.Pack{}
					if err := proto.Unmarshal(frame.GetMessageBytes(), pack); err != nil {
						// bad protobuf
						updatePeerAudit(sourceId, func(a *PeerAudit) {
							a.badMessage(ByteCount(len(transferFrameBytes)))
						})
						continue
					}
					messageByteCount := MessageByteCount(pack.Frames)
					transferLog("[%s] Forward pack %s ->: %s (%d)", self.clientTag, sourceId.String(), pack.Frames, messageByteCount)
				default:
					transferLog("[%s] Forward unknown -> %s: %s", self.clientTag, sourceId.String(), frame)
				}
			}

			transferLog("CLIENT FORWARD\n")

			transferLog("[%s] Forward %s -> %s", self.clientTag, sourceId.String(), destinationId.String())
			

			Trace(
				fmt.Sprintf("[cr]forward %s %s<-%s", self.clientTag, destinationId.String(), sourceId.String()),
				func() {
					self.forward(sourceId, destinationId, transferFrameBytes)
				},
			)
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

func (self *Client) IsDone() bool {
	select {
	case <- self.ctx.Done():
		return true
	default:
		return false
	}
}

func (self *Client) Done() <-chan struct{} {
	return self.ctx.Done()
}

func (self *Client) Ctx() context.Context {
	return self.ctx
}

// this does not need to be called if `Cancel` is called
func (self *Client) Close() {
	fmt.Printf("CLOSE 32\n")
	self.cancel()

	self.sendBuffer.Close()
	self.receiveBuffer.Close()
	self.forwardBuffer.Close()

	// self.routeManager.Close()
	// self.contractManager.Close()

	self.contractManagerUnsub()
}

func (self *Client) Cancel() {
	// debug.PrintStack()

	fmt.Printf("CANCEL 32\n")
	self.cancel()

	self.sendBuffer.Cancel()
	self.receiveBuffer.Cancel()
	self.forwardBuffer.Cancel()
}

func (self *Client) Flush() {
	self.sendBuffer.Flush()
	self.receiveBuffer.Flush()
	self.forwardBuffer.Flush()

	self.contractManager.Flush(false)
}


type SendBufferSettings struct {
	CreateContractTimeout time.Duration
	CreateContractRetryInterval time.Duration

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
				sendSequence.Cancel()
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
			HandleError(sendSequence.Run)

			self.mutex.Lock()
			defer self.mutex.Unlock()
			sendSequence.Close()
			// clean up
			if sendSequence == self.sendSequences[sendSequenceId] {
				delete(self.sendSequences, sendSequenceId)
			}
		}()
		return sendSequence
	}

	var sendSequence *SendSequence
	var success bool
	var err error
	for i := 0; i < 2; i += 1 {
		select {
		case <- self.ctx.Done():
			return false, errors.New("Done.")
		default:
		}
		sendSequence = initSendSequence(sendSequence)
		if success, err = sendSequence.Pack(sendPack, timeout); err == nil {
			return success, nil
		}
		// sequence closed
	}
	return success, err
}

func (self *SendBuffer) Ack(sourceId Id, ack *protocol.Ack, timeout time.Duration) bool {
	sendSequence := func(companionContract bool)(*SendSequence) {
		self.mutex.Lock()
		defer self.mutex.Unlock()
		return self.sendSequences[sendSequenceId{
			DestinationId: sourceId,
			CompanionContract: companionContract,
		}]
	}
	
	anyFound := false
	anySuccess := false
	if seq := sendSequence(false); seq != nil {
		anyFound = true
		if success, err := seq.Ack(ack, timeout); success && err == nil {
			anySuccess = true
		}
	}
	if seq := sendSequence(true); seq != nil {
		anyFound = true
		if success, err := seq.Ack(ack, timeout); success && err == nil {
			anySuccess = true
		}
	}
	if !anyFound {
		fmt.Printf("[sb]ack miss sequence does not exist\n")
	}
	return anySuccess
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
	clientTag string
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
		clientTag: client.ClientTag(),
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

	select {
	case <- self.ctx.Done():
		return false, errors.New("Done.")
	default:
	}

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

func (self *SendSequence) Ack(ack *protocol.Ack, timeout time.Duration) (bool, error) {
	sequenceId, err := IdFromBytes(ack.SequenceId)
	if err != nil {
		return false, err
	}
	if self.sequenceId != sequenceId {
		// ack is for a different send sequence that no longer exists
		return false, nil
	}

	select {
	case <- self.ctx.Done():
		return false, errors.New("Done.")
	default:
	}

	if timeout < 0 {
		select {
		case <- self.ctx.Done():
			return false, errors.New("Done.")
		case self.acks <- ack:
			return true, nil
		}
	} else if timeout == 0 {
		select {
		case <- self.ctx.Done():
			return false, errors.New("Done.")
		case self.acks <- ack:
			return true, nil
		default:
			return false, nil
		}
	} else {
		select {
		case <- self.ctx.Done():
			return false, errors.New("Done.")
		case self.acks <- ack:
			return true, nil
		case <- time.After(timeout):
			return false, nil
		}
	}
}

func (self *SendSequence) Run() {
	defer func() {
		fmt.Printf("[%s] Send sequence exit -> %s\n", self.clientTag, self.destinationId.String())

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

		// flush queued up contracts
		// remove used contract ids because all used contracts were closed above
		self.contractManager.FlushContractQueue(self.destinationId, true)
	}()

	fmt.Printf("[s]OPEN MULTI ROUTE WRITER %s -> %s\n", self.clientId, self.destinationId)
	self.multiRouteWriter = self.routeManager.OpenMultiRouteWriter(self.destinationId)
	defer self.routeManager.CloseMultiRouteWriter(self.multiRouteWriter)

	ackWindow := newSequenceAckWindow()
	go func() {
		defer self.cancel()

		for {
			select {
			case <- self.ctx.Done():
				return
			case ack, ok := <- self.acks:
				if !ok {
					return
				}
				if messageId, err := IdFromBytes(ack.MessageId); err == nil {
					if sequenceNumber, ok := self.resendQueue.ContainsMessageId(messageId); ok {
						ack := &sequenceAck{
							messageId: messageId,
							sequenceNumber: sequenceNumber,
							selective: ack.Selective,
						}
						ackWindow.Update(ack)
					}
				}
			}
		}
	}()

	for {
		// apply the acks
		ackSnapshot := ackWindow.Snapshot(true)
		if 0 < ackSnapshot.ackUpdateCount {
			self.receiveAck(ackSnapshot.headAck.messageId, false)
		}
		for messageId, _ := range ackSnapshot.selectiveAcks {
			self.receiveAck(messageId, true)
		}


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
					var err error
					transferFrameBytes, err = self.setHead(item)
					if err != nil {
						transferLog("!!!! SET HEAD ERROR %s", err)
						fmt.Printf("EXIT 02\n")
						return
					}
				} else {
					transferFrameBytes = item.transferFrameBytes
				}
				transferLog("!!!! RESEND %d [%s]", item.sequenceNumber, self.destinationId.String())
				// fmt.Printf("buffer resend write message %s->\n", self.clientId)
				// a := time.Now()
				fmt.Printf("[s]resend %d %s->%s\n", item.sequenceNumber, self.clientTag, self.destinationId.String())
				TraceWithReturn(fmt.Sprintf("[s]multi route write %s->%s", self.clientTag, self.destinationId.String()), func()(error) {
					return self.multiRouteWriter.Write(self.ctx, transferFrameBytes, self.sendBufferSettings.WriteTimeout)
					
				})
				/*
				if err != nil {
					transferLog("!! WRITE TIMEOUT A\n")
					// d := time.Now().Sub(a)
					// fmt.Printf("buffer resend write message %s-> (%s) %.2fms\n", self.clientId, err, float64(d) / float64(time.Millisecond))
				} else {
					transferLog("WROTE RESEND: %s, %s -> messageId=%s clientId=%s sequenceId=%s\n", item.messageType, self.clientTag, item.messageId.String(), self.destinationId.String(), self.sequenceId.String())
					// d := time.Now().Sub(a)
					// fmt.Printf("buffer resend write message %s-> %.2fms\n", self.clientId, float64(d) / float64(time.Millisecond))
				}
				*/

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
			// case ack, ok := <- self.acks:
			// 	if !ok {
			// 		fmt.Printf("EXIT 05\n")
			// 		return
			// 	}
			// 	fmt.Printf("[s]ack 02 %s->%s\n", self.clientTag, self.destinationId.String())
			// 	if messageId, err := IdFromBytes(ack.MessageId); err == nil {
			// 		self.receiveAck(messageId, ack.Selective)
			// 	} else {
			// 		fmt.Printf("[s]ack bad message %s->%s = %s\n", self.clientTag, self.destinationId.String(), err)
			// 	}
			case <- ackSnapshot.ackNotify:
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
			// case ack, ok := <- self.acks:
			// 	if !ok {
			// 		fmt.Printf("EXIT 08\n")
			// 		return
			// 	}
			// 	fmt.Printf("[s]ack 03 %s->%s\n", self.clientTag, self.destinationId.String())
			// 	if messageId, err := IdFromBytes(ack.MessageId); err == nil {
			// 		self.receiveAck(messageId, ack.Selective)
			// 	} else {
			// 		fmt.Printf("[s]ack bad message %s->%s = %s\n", self.clientTag, self.destinationId.String(), err)
			// 	}
			case <- ackSnapshot.ackNotify:
			case sendPack, ok := <- self.packs:
				if !ok {
					fmt.Printf("EXIT 09\n")
					return
				}

				// note messages of `size < MinMessageByteCount` get counted as `MinMessageByteCount` against the contract
				if self.updateContract(sendPack.MessageByteCount) {
					transferLog("[%s] Have contract, sending -> %s: %s", self.clientTag, self.destinationId.String(), sendPack.Frame)
					self.send(sendPack.Frame, sendPack.AckCallback, sendPack.Ack)
					// ignore the error since there will be a retry
				} else {
					// no contract
					// close the sequence
					fmt.Printf("[s]Could not create contract (%s->%s). Exiting sequence.\n", self.clientTag, self.destinationId.String())
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
	if self.contractManager.SendNoContract(self.destinationId, self.companionContract) {
		return true
	}
	if self.sendContract != nil && self.sendContract.update(messageByteCount) {
		return true
	}

/*
	// create new contract
	if true {
		contractId := NewId()

		storedContract := &protocol.StoredContract{
			ContractId: contractId.Bytes(),
			TransferByteCount: uint64(self.client.settings.ContractManagerSettings.StandardContractTransferByteCount),
			SourceId: self.client.ClientId().Bytes(),
			DestinationId: self.destinationId.Bytes(),
		}
		storedContractBytes, _ := proto.Marshal(storedContract)
		
		contract := &protocol.Contract{
			StoredContractBytes: storedContractBytes,
			StoredContractHmac: []byte{},
			ProvideMode: protocol.ProvideMode_Public,
		}

		// contract := self.contractManager.TakeContract(self.ctx, self.destinationId, -1)
		contractMessageBytes, _ := proto.Marshal(contract)

		nextSendContract, _ := newSequenceContract(
			"s",
			contract,
			self.sendBufferSettings.MinMessageByteCount,
			self.sendBufferSettings.ContractFillFraction,
		)

		nextSendContract.update(messageByteCount)
		nextSendContract.update(0)
		self.setContract(nextSendContract)

		// append the contract to the sequence
		self.send(&protocol.Frame{
			MessageType: protocol.MessageType_TransferContract,
			MessageBytes: contractMessageBytes,
		}, func(error){}, true)

		return true
	}
	*/


	return TraceWithReturn(
		fmt.Sprintf("[s]create contract c=%t %s->%s", self.companionContract, self.clientTag, self.destinationId.String()),
		func()(bool) {
			// the max overhead of the pack frame
			// this is needed because the size of the contract pack is counted against the contract
			// maxContractMessageByteCount := ByteCount(256)

			effectiveContractTransferByteCount := ByteCount(float32(self.contractManager.StandardContractTransferByteCount()) * self.sendBufferSettings.ContractFillFraction)
			if effectiveContractTransferByteCount < messageByteCount + self.sendBufferSettings.MinMessageByteCount /*+ maxContractMessageByteCount*/ {
				// this pack does not fit into a standard contract
				// TODO allow requesting larger contracts
				panic("Message too large for contract. It can never be sent.")
			}


			setNextContract := func(contract *protocol.Contract)(bool) {
				nextSendContract, err := newSequenceContract(
					"s",
					contract,
					self.sendBufferSettings.MinMessageByteCount,
					self.sendBufferSettings.ContractFillFraction,
				)
				if err != nil {
					// malformed
					fmt.Printf("[s]next contract malformed = %s\n", err)
					return false
				}

				// contractMessageBytes, _ := proto.Marshal(contract)

				// if maxContractMessageByteCount < ByteCount(len(contractMessageBytes)) {
				// 	panic("Bad estimate for contract max size could result in infinite contract retries.")
				// }

				// note `update(0)` will use `MinMessageByteCount` byte count
				// the min message byte count is used to avoid spam
				if nextSendContract.update(0) && nextSendContract.update(messageByteCount) {
					transferLog("[%s] Send contract %s -> %s", self.clientTag, nextSendContract.contractId.String(), self.destinationId.String())

					fmt.Printf("SET SEND CONTRACT\n")
					self.setContract(nextSendContract)

					// append the contract to the sequence
					self.sendWithSetContract(nil, func(error){}, true, true)

					return true
				} else {
					// this contract doesn't fit the message
					// the contract was requested with the correct size, so this is an error somewhere
					// just close it and let the platform time out the other side
					fmt.Printf("COMPLETE CONTRACT SEND 5\n")
					self.contractManager.CompleteContract(nextSendContract.contractId, 0, 0)
					return false
				}
			}

			nextContract := func(timeout time.Duration)(bool) {
				return TraceWithReturn("[s]next contract", func()(bool) {
					if contract := self.contractManager.TakeContract(self.ctx, self.destinationId, timeout); contract != nil && setNextContract(contract) {
						// async queue up the next contract
						self.contractManager.CreateContract(
							self.destinationId,
							self.companionContract,
							self.client.settings.ControlWriteTimeout,
						)

						return true
					} else {
						return false
					}
				})
			}

			if nextContract(0) {
				return true
			}

			endTime := time.Now().Add(self.sendBufferSettings.CreateContractTimeout)
			for {
				select {
				case <- self.ctx.Done():
					return false
				default:
				}

				timeout := endTime.Sub(time.Now())
				if timeout <= 0 {
					return false
				}

				// async queue up the next contract
				self.contractManager.CreateContract(
					self.destinationId,
					self.companionContract,
					self.client.settings.ControlWriteTimeout,
				)

				if nextContract(min(timeout, self.sendBufferSettings.CreateContractRetryInterval)) {
					return true
				}
			}
		},
	)
}

func (self *SendSequence) setContract(nextSendContract *sequenceContract) {
	if self.sendContract != nil && self.sendContract.contractId == nextSendContract.contractId {
		return
	}

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
		// self.sendContract = nil
	}
	self.openSendContracts[nextSendContract.contractId] = nextSendContract
	self.sendContract = nextSendContract
}

func (self *SendSequence) send(
	frame *protocol.Frame,
	ackCallback AckFunction,
	ack bool,
) {
	self.sendWithSetContract(frame, ackCallback, ack, false)
}

func (self *SendSequence) sendWithSetContract(
	frame *protocol.Frame,
	ackCallback AckFunction,
	ack bool,
	setContract bool,
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

	var contractFrame *protocol.Frame
	if (head || setContract) && self.sendContract != nil {
		contractMessageBytes, _ := proto.Marshal(self.sendContract.contract)
		contractFrame =  &protocol.Frame{
			MessageType: protocol.MessageType_TransferContract,
			MessageBytes: contractMessageBytes,
		}
	}

	frames := []*protocol.Frame{}
	if frame != nil {
		frames = append(frames, frame)
	}

	pack := &protocol.Pack{
		MessageId: messageId.Bytes(),
		SequenceId: self.sequenceId.Bytes(),
		SequenceNumber: sequenceNumber,
		Head: head,
		Frames: frames,
		ContractFrame: contractFrame,
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

	messageByteCount := MessageByteCount(pack.Frames)

	item := &sendItem{
		transferItem: transferItem{
			messageId: messageId,
			sequenceNumber: sequenceNumber,
			messageByteCount: messageByteCount,
		},
		contractId: contractId,
		sendTime: sendTime,
		resendTime: sendTime.Add(self.sendBufferSettings.ResendInterval),
		sendCount: 1,
		head: head,
		hasContractFrame: (contractFrame != nil),
		transferFrameBytes: transferFrameBytes,
		ackCallback: ackCallback,
		// selectiveAckEnd: time.Time{},
		// messageType: frame.MessageType,
	}


	err := TraceWithReturn(fmt.Sprintf("[s]multi route write %s->%s", self.clientTag, self.destinationId.String()), func()(error) {
		return self.multiRouteWriter.Write(
			self.ctx,
			item.transferFrameBytes,
			self.sendBufferSettings.WriteTimeout,
		)
	})

	if ack {
		self.sendItems = append(self.sendItems, item)
		self.resendQueue.Add(item)
		// ignore the write error since the item will be resent
	} else {
		// immediately ack
		if err == nil {
			self.ackItem(item)
		} else {
			item.ackCallback(err)
		}
	}
}

func (self *SendSequence) setHead(item *sendItem) ([]byte, error) {
	fmt.Printf("[s]set head %s->%s\n", self.clientTag, self.destinationId.String())

	var transferFrame protocol.TransferFrame
	err := proto.Unmarshal(item.transferFrameBytes, &transferFrame)
	if err != nil {
		return nil, err
	}

	var pack protocol.Pack
	err = proto.Unmarshal(transferFrame.Frame.MessageBytes, &pack)
	if err != nil {
		return nil, err
	}

	pack.Head = true
	// attach the contract frame to the head
	if item.contractId != nil && !item.hasContractFrame {
		sendContract := self.openSendContracts[*item.contractId]
		contractMessageBytes, _ := proto.Marshal(sendContract.contract)
		pack.ContractFrame =  &protocol.Frame{
			MessageType: protocol.MessageType_TransferContract,
			MessageBytes: contractMessageBytes,
		}
	}

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
		fmt.Printf("[s]ack miss %s->%s\n", self.clientTag, self.destinationId.String())
		transferLog("RECEIVE ACK MISS\n")
		// message not pending ack
		return
	}

	if selective {
		fmt.Printf("[s]ack selective %s->%s\n", self.clientTag, self.destinationId.String())
		removed := self.resendQueue.RemoveByMessageId(messageId)
		if removed == nil {
			panic(errors.New("Missing item"))
		}
		item.resendTime = time.Now().Add(self.sendBufferSettings.SelectiveAckTimeout)
		self.resendQueue.Add(item)
		return
	}

	fmt.Printf("[s]ack %d %s->%s\n", item.sequenceNumber, self.clientTag, self.destinationId.String())

	// acks are cumulative
	// implicitly ack all earlier items in the sequence
	i := 0
	for ; i < len(self.sendItems); i += 1 {
		implicitItem := self.sendItems[i]
		if item.sequenceNumber < implicitItem.sequenceNumber {
			fmt.Printf("[s]ack %d <> %d/%d (stop) %s->%s\n", item.sequenceNumber, implicitItem.sequenceNumber, self.nextSequenceNumber - 1, self.clientTag, self.destinationId.String())
			transferLog("!!!! ACK END")
			break
		}
		a, b := self.resendQueue.QueueSize()
		// self.ackedSequenceNumbers[implicitItem.sequenceNumber] = true
		transferLog("!!!! ACK %d", implicitItem.sequenceNumber)
		removed := self.resendQueue.RemoveByMessageId(implicitItem.messageId)
		if removed == nil {
			panic(errors.New("Missing item"))
		}

		self.ackItem(implicitItem)
		self.sendItems[i] = nil

		c, d := self.resendQueue.QueueSize()
		fmt.Printf("[s]ack %d <> %d/%d (pass %d->%d %dB->%dB) %s->%s\n", item.sequenceNumber, implicitItem.sequenceNumber, self.nextSequenceNumber - 1, a, c, b, d, self.clientTag, self.destinationId.String())
	}
	self.sendItems = self.sendItems[i:]
	a, b := self.resendQueue.QueueSize()
	fmt.Printf("[s]ack %d/%d (stop %d %dB %d) %s->%s\n", item.sequenceNumber, self.nextSequenceNumber - 1, a, b, len(self.sendItems), self.clientTag, self.destinationId.String())
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
	item.ackCallback(nil)
}

func (self *SendSequence) Close() {
	// debug.PrintStack()
	fmt.Printf("CLOSE 33\n")
	self.cancel()
	self.idleCondition.WaitForClose()
	close(self.packs)
	close(self.acks)
}

func (self *SendSequence) Cancel() {
	// debug.PrintStack()
	fmt.Printf("3\n")
	self.cancel()
}

type sendItem struct {
	transferItem

	contractId *Id
	head bool
	hasContractFrame bool
	sendTime time.Time
	resendTime time.Time
	sendCount int
	transferFrameBytes []byte
	ackCallback AckFunction

	// messageType protocol.MessageType
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
	// AckBufferSize int

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
				receiveSequence.Cancel()
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
			HandleError(receiveSequence.Run)

			self.mutex.Lock()
			defer self.mutex.Unlock()
			receiveSequence.Close()
			// clean up
			if receiveSequence == self.receiveSequences[receiveSequenceId] {
				delete(self.receiveSequences, receiveSequenceId)
			}
		}()
		return receiveSequence
	}

	var receiveSequence *ReceiveSequence
	var success bool
	var err error
	for i := 0; i < 2; i += 1 {
		select {
		case <- self.ctx.Done():
			return false, errors.New("Done.")
		default:
		}
		receiveSequence = initReceiveSequence(receiveSequence)
		if success, err = receiveSequence.Pack(receivePack, timeout); err == nil {
			return success, nil
		}
		// sequence closed
	}
	return success, err
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
	clientTag string
	routeManager *RouteManager
	contractManager *ContractManager

	sourceId Id
	sequenceId Id

	receiveBufferSettings *ReceiveBufferSettings

	receiveContract *sequenceContract
	// usedContractIds map[Id]bool

	packs chan *ReceivePack

	receiveQueue *receiveQueue
	nextSequenceNumber uint64

	idleCondition *IdleCondition

	peerAudit *SequencePeerAudit

	ackWindow *sequenceAckWindow
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
		clientTag: client.ClientTag(),
		routeManager: routeManager,
		contractManager: contractManager,
		sourceId: sourceId,
		sequenceId: sequenceId,
		receiveBufferSettings: receiveBufferSettings,
		receiveContract: nil,
		// usedContractIds: map[Id]bool{},
		packs: make(chan *ReceivePack, receiveBufferSettings.SequenceBufferSize),
		receiveQueue: newReceiveQueue(),
		nextSequenceNumber: 0,
		idleCondition: NewIdleCondition(),
		ackWindow: newSequenceAckWindow(),
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

	select {
	case <- self.ctx.Done():
		return false, errors.New("Done.")
	default:
	}

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
		transferLog("[%s] Receive sequence exit %s ->\n", self.clientTag, self.sourceId.String())

		self.cancel()

		// close contract
		if self.receiveContract != nil {
			fmt.Printf("CHECKPOINT CONTRACT RECEIVE 1 (%s)\n", self.receiveContract.contractId)
			// the sender may send again with this contract (set as head)
			// checkpoint the contract but do not close it
			self.contractManager.CheckpointContract(
				self.receiveContract.contractId,
				self.receiveContract.ackedByteCount,
				self.receiveContract.unackedByteCount,
			)
			// self.receiveContract = nil
		}

		// drain the buffer
		for _, item := range self.receiveQueue.orderedItems {
			self.peerAudit.Update(func(a *PeerAudit) {
				a.discard(item.messageByteCount)
			})
		}

		// drain the channel
		// func() {
		// 	for {
		// 		select {
		// 		case _, ok := <- self.packs:
		// 			if !ok {
		// 				return
		// 			}
		// 		}
		// 	}
		// }()

		self.contractManager.CloseSourceContract(self.sourceId)

		self.peerAudit.Complete()
	}()

	self.peerAudit = NewSequencePeerAudit(
		self.client,
		self.sourceId,
		self.receiveBufferSettings.MaxPeerAuditDuration,
	)

	// compress and send acks
	go func() {
		defer self.cancel()

		fmt.Printf("[r]OPEN MULTI ROUTE WRITER %s -> %s\n", self.clientId, self.sourceId)
		multiRouteWriter := self.routeManager.OpenMultiRouteWriter(self.sourceId)
		defer self.routeManager.CloseMultiRouteWriter(multiRouteWriter)

		writeAck := func(sendAck *sequenceAck) {
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
			TraceWithReturn(fmt.Sprintf("[r]multi route write (ack %d) %s->%s", sendAck.sequenceNumber, self.clientTag, self.sourceId.String()), func()(error) {
				return multiRouteWriter.Write(self.ctx, transferFrameBytes, self.receiveBufferSettings.WriteTimeout)
			})
		}

		for {
			select {
			case <- self.ctx.Done():
				return
			default:
			}

			ackSnapshot := self.ackWindow.Snapshot(false)
			if ackSnapshot.ackUpdateCount == 0 && len(ackSnapshot.selectiveAcks) == 0 {
				// wait for one ack
				select {
				case <- self.ctx.Done():
					return
				case <- ackSnapshot.ackNotify:
				}
			}

			if 0 < self.receiveBufferSettings.AckCompressTimeout {
				select {
				case <- self.ctx.Done():
					return
				case <- time.After(self.receiveBufferSettings.AckCompressTimeout):
				}
			}

			ackSnapshot = self.ackWindow.Snapshot(true)
			if 0 < ackSnapshot.ackUpdateCount {
				writeAck(ackSnapshot.headAck)
			}
			for messageId, sequenceNumber := range ackSnapshot.selectiveAcks {
				writeAck(&sequenceAck{
					messageId: messageId,
					sequenceNumber: sequenceNumber,
					selective: true,
				})
			}
		}
	}()

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
					fmt.Printf("!! EXIT H\n")
					transferLog("[%s] Gap timeout", self.clientTag)
					// did not receive a preceding message in time
					// quence
					return
				}

				if self.nextSequenceNumber < item.sequenceNumber {
					transferLog("[%s] Head of sequence is not next %d <> %d: %s", self.clientTag, self.nextSequenceNumber, item.sequenceNumber, item.frames)
					if itemGapTimeout < timeout {
						timeout = itemGapTimeout
					}
					break
				}
				// item.sequenceNumber <= self.nextSequenceNumber

				self.receiveQueue.RemoveByMessageId(item.messageId)
				
				
				if self.nextSequenceNumber == item.sequenceNumber {
					// this item is the head of sequence
					transferLog("[%s] Receive next in sequence %d: %s", self.clientTag, item.sequenceNumber, item.frames)
				
					if err := self.registerContracts(item); err != nil {
						fmt.Printf("EXIT 21 %s\n", err)
						return
					}
					transferLog("[%s] Receive head of sequence %d.", self.clientTag, item.sequenceNumber)
					if self.updateContract(item) {
						fmt.Printf("[r]seq+ %d->%d (queue) %s<-%s\n", self.nextSequenceNumber, self.nextSequenceNumber + 1, self.clientTag, self.sourceId.String())
						self.nextSequenceNumber = self.nextSequenceNumber + 1
						self.receiveHead(item)
					} else {
						// no valid contract. it should have been attached to the head
						fmt.Printf("[r]drop head no contract %s<-%s\n", self.clientTag, self.sourceId.String())
						return
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
			fmt.Printf("!! EXIT A\n")
			return
		case receivePack, ok := <- self.packs:
			if !ok {
				fmt.Printf("!! EXIT B\n")
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
					fmt.Printf("!! EXIT D\n")
					return	
				} else if !received {

					fmt.Printf("[r]drop nack %s<-%s\n", self.clientTag, self.sourceId.String())
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
					fmt.Printf("!! EXIT D\n")
					return	
				} else if !received {

					fmt.Printf("[r]drop ack %s<-%s\n", self.clientTag, self.sourceId.String())
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
					fmt.Printf("!! EXIT F\n")
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

	fmt.Printf("!! EXIT G\n")
}

func (self *ReceiveSequence) sendAck(sequenceNumber uint64, messageId Id, selective bool) {
	ack := &sequenceAck{
		sequenceNumber: sequenceNumber,
		messageId: messageId,
		selective: selective,
	}
	self.ackWindow.Update(ack)
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
		contractFrame: receivePack.Pack.ContractFrame,
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
		fmt.Printf("[r]seq= %d->%d %s<-%s\n", self.nextSequenceNumber, item.sequenceNumber, self.clientTag, self.sourceId.String())
		self.nextSequenceNumber = item.sequenceNumber
		// the head must have a contract frame to reset the contract
	}


	if removedItem := self.receiveQueue.RemoveBySequenceNumber(sequenceNumber); removedItem != nil {
		transferLog("!!!! 2")
		self.peerAudit.Update(func(a *PeerAudit) {
			a.resend(removedItem.messageByteCount)
		})
	}

	// replace with the latest value (check both messageId and sequenceNumber)
	if removedItem := self.receiveQueue.RemoveByMessageId(messageId); removedItem != nil {
		transferLog("!!!! 1")
		self.peerAudit.Update(func(a *PeerAudit) {
			a.resend(removedItem.messageByteCount)
		})
	}

	if sequenceNumber <= self.nextSequenceNumber {
		if self.nextSequenceNumber == sequenceNumber {
			// this item is the head of sequence

			transferLog("[%s] Receive next in sequence [%d]\n", self.clientTag, item.sequenceNumber)

			fmt.Printf("[r]seq+ %d->%d %s<-%s\n", self.nextSequenceNumber, self.nextSequenceNumber + 1, self.clientTag, self.sourceId.String())
			self.nextSequenceNumber = self.nextSequenceNumber + 1

			if err := self.registerContracts(item); err != nil {
				return false, err
			}
			if self.updateContract(item) {
				self.receiveHead(item)
				return true, nil
			} else {
				// no valid contract. it should have been attached to the head
				fmt.Printf("[r]drop queue head no contract %s<-%s\n", self.clientTag, self.sourceId.String())
				return false, errors.New("No contract")
			}
		} else {
			fmt.Printf("[r]drop (past sequence number %d <> %d ack=%t) %s<-%s\n", sequenceNumber, self.nextSequenceNumber, item.ack, self.clientTag, self.sourceId.String())
			// this item is a resend of a previous item
			if item.ack {
				self.sendAck(sequenceNumber, messageId, false)
			}
			return true, nil
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
			fmt.Printf("[r]drop ack cannot queue %s<-%s\n", self.clientTag, self.sourceId.String())
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
		contractFrame: receivePack.Pack.ContractFrame,
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
		// no valid contract
		// drop the message. since this is a nack it will not block the sequence
		fmt.Printf("[r]drop nack no contract %s<-%s\n", self.clientTag, self.sourceId.String())
		return false, nil
	}
}

func (self *ReceiveSequence) receiveHead(item *receiveItem) {
	frameMessageTypes := []string{}
	for _, frame := range item.frames {
		frameMessageTypes = append(frameMessageTypes, fmt.Sprintf("%v", frame.MessageType))
	}
	frameMessageTypesStr := strings.Join(frameMessageTypes, ", ")
	if item.ack {
		fmt.Printf("[r]head %d (%s) %s<-%s\n", item.sequenceNumber, frameMessageTypesStr, self.clientTag, self.sourceId.String())
	} else {
		fmt.Printf("[r]head nack (%s) %s<-%s\n", frameMessageTypesStr, self.clientTag, self.sourceId.String())
	}
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
	// transferLog("!! RECEIVED MESSAGE, CALLING RECEIVE %s %s", item.receiveCallback, item.frames)
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
	if item.contractFrame == nil {
		return nil
	}

	var contract protocol.Contract
	err := proto.Unmarshal(item.contractFrame.MessageBytes, &contract)
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
		"r",
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

	return nil
}

func (self *ReceiveSequence) setContract(nextReceiveContract *sequenceContract) error {
	// contract already set
	if self.receiveContract != nil && self.receiveContract.contractId == nextReceiveContract.contractId {
		return nil
	}

	// close out the previous contract
	if self.receiveContract != nil {
		fmt.Printf("COMPLETE CONTRACT RECEIVE 2\n")
		self.contractManager.CompleteContract(
			self.receiveContract.contractId,
			self.receiveContract.ackedByteCount,
			self.receiveContract.unackedByteCount,
		)
		// self.receiveContract = nil
	}
	// FIXME this needs to be done with some kind of async verification to the control
	/*
	// track all the contracts used to avoid reusing contracts
	if self.usedContractIds[nextReceiveContract.contractId] {
		return errors.New("Already used contract.")
	}
	self.usedContractIds[nextReceiveContract.contractId] = true
	*/
	self.receiveContract = nextReceiveContract
	self.contractManager.OpenSourceContract(self.sourceId)
	return nil
}

func (self *ReceiveSequence) updateContract(item *receiveItem) bool {
	// always use a contract if present
	// the sender may send contracts even if `receiveNoContract` is set locally
	if self.receiveContract != nil && self.receiveContract.update(item.messageByteCount) {
		return true
	}
	// `receiveNoContract` is a mutual configuration 
	// both sides must configure themselves to require no contract from each other
	if self.contractManager.ReceiveNoContract(self.sourceId) {
		return true
	}
	return false
}

func (self *ReceiveSequence) Close() {
	// debug.PrintStack()
	fmt.Printf("CLOSE 31\n")
	self.cancel()
	self.idleCondition.WaitForClose()
	close(self.packs)
}

func (self *ReceiveSequence) Cancel() {
	// debug.PrintStack()
	fmt.Printf("CANCEL 31\n")
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
	contractFrame *protocol.Frame
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


type sequenceAck struct {
	sequenceNumber uint64
	messageId Id
	selective bool
}

type sequenceAckWindowSnapshot struct {
	ackNotify <-chan struct{}
	headAck *sequenceAck
	ackUpdateCount int
	selectiveAcks map[Id]uint64
}

type sequenceAckWindow struct {
	ackMonitor *Monitor
	ackLock sync.Mutex
	headAck *sequenceAck
	ackUpdateCount int
	selectiveAcks map[Id]uint64
}

func newSequenceAckWindow() *sequenceAckWindow {
	return &sequenceAckWindow{
		ackMonitor: NewMonitor(),
		headAck: nil,
		ackUpdateCount: 0,
		selectiveAcks: map[Id]uint64{},
	}
}

func (self *sequenceAckWindow) Update(ack *sequenceAck) {
	self.ackLock.Lock()
	defer self.ackLock.Unlock()

	if self.headAck == nil || self.headAck.sequenceNumber < ack.sequenceNumber {
		if ack.selective {
			self.selectiveAcks[ack.messageId] = ack.sequenceNumber
		} else {
			self.ackUpdateCount += 1
			self.headAck = ack
			// no need to clean up `selectiveAcks` here
			// selective acks with sequence number <= head are ignored in a final pass during update
		}
	} else {
		// past the head
		// resend the head
		self.ackUpdateCount += 1
	}

	self.ackMonitor.NotifyAll()
}

func (self *sequenceAckWindow) Snapshot(reset bool) *sequenceAckWindowSnapshot {
	self.ackLock.Lock()
	defer self.ackLock.Unlock()

	var selectiveAcksAfterHead map[Id]uint64
	if 0 < self.ackUpdateCount {
		selectiveAcksAfterHead = map[Id]uint64{}
		for messageId, sequenceNumber := range self.selectiveAcks {
			if self.headAck.sequenceNumber < sequenceNumber {
				selectiveAcksAfterHead[messageId] = sequenceNumber
			}
		}
	} else {
		selectiveAcksAfterHead = maps.Clone(self.selectiveAcks)
	}

	snapshot := &sequenceAckWindowSnapshot{
		ackNotify: self.ackMonitor.NotifyChannel(),
		headAck: self.headAck,
		ackUpdateCount: self.ackUpdateCount,
		selectiveAcks: selectiveAcksAfterHead,
	}

	if reset {
		// keep the head ack in place
		self.ackUpdateCount = 0
		self.selectiveAcks = map[Id]uint64{}
	}

	return snapshot
}


type sequenceContract struct {
	tag string
	contract *protocol.Contract
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

func newSequenceContract(tag string, contract *protocol.Contract, minUpdateByteCount ByteCount, contractFillFraction float32) (*sequenceContract, error) {
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
		tag: tag,
		contract: contract,
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
	effectiveByteCount := max(self.minUpdateByteCount, byteCount)

	if self.effectiveTransferByteCount < self.ackedByteCount + self.unackedByteCount + effectiveByteCount {
		// doesn't fit in contract
		fmt.Printf("[%s]DEBIT CONTRACT (%s) FAILED +%d->%d (%d/%d total %.1f%% full)\n", self.tag, self.contractId.String(), effectiveByteCount, self.ackedByteCount + self.unackedByteCount + effectiveByteCount, self.ackedByteCount + self.unackedByteCount, self.effectiveTransferByteCount, 100.0 * float32(self.ackedByteCount + self.unackedByteCount) / float32(self.effectiveTransferByteCount))
		return false
	}
	self.unackedByteCount += effectiveByteCount
	fmt.Printf("[%s]DEBIT CONTRACT (%s) PASSED +%d->%d (%d/%d total %.1f%% full)\n", self.tag, self.contractId.String(), effectiveByteCount, self.ackedByteCount + self.unackedByteCount, self.ackedByteCount + self.unackedByteCount, self.effectiveTransferByteCount, 100.0 * float32(self.ackedByteCount + self.unackedByteCount) / float32(self.effectiveTransferByteCount))
	return true
}

func (self *sequenceContract) ack(byteCount ByteCount) {
	effectiveByteCount := max(self.minUpdateByteCount, byteCount)

	if self.unackedByteCount < effectiveByteCount {
		// debug.PrintStack()
		panic(fmt.Errorf("Bad accounting %d <> %d", self.unackedByteCount, effectiveByteCount))
	}
	self.unackedByteCount -= effectiveByteCount
	self.ackedByteCount += effectiveByteCount
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
				forwardSequence.Cancel()
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
			HandleError(forwardSequence.Run)

			self.mutex.Lock()
			defer self.mutex.Unlock()
			forwardSequence.Close()
			// clean up
			if forwardSequence == self.forwardSequences[forwardPack.DestinationId] {
				delete(self.forwardSequences, forwardPack.DestinationId)
			}
		}()
		return forwardSequence
	}

	var forwardSequence *ForwardSequence
	var success bool
	var err error
	for i := 0; i < 2; i += 1 {
		select {
		case <- self.ctx.Done():
			return false, errors.New("Done.")
		default:
		}
		forwardSequence = initForwardSequence(forwardSequence)
		if success, err = forwardSequence.Pack(forwardPack, timeout); err == nil {
			return success, nil
		}
		// sequence closed
	}
	return success, err
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
	clientTag string
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
		clientTag: client.ClientTag(),
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

	select {
	case <- self.ctx.Done():
		return false, errors.New("Done.")
	default:
	}

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

	fmt.Printf("[f]OPEN MULTI ROUTE WRITER %s -> %s\n", self.clientId, self.destinationId)
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
			err := TraceWithReturn(fmt.Sprintf("[f]multi route write %s->%s", self.clientTag, self.destinationId.String()), func()(error) {
				return self.multiRouteWriter.Write(self.ctx, forwardPack.TransferFrameBytes, self.forwardBufferSettings.WriteTimeout)
			})
			if err != nil {
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

	peerAudit := &protocol.PeerAudit{
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
	}
	self.client.ClientOob().SendControl(
		[]*protocol.Frame{RequireToFrame(peerAudit)},
		func(resultFrames []*protocol.Frame, err error){},
	)
	self.peerAudit = nil
}


// contract frames are not counted towards the message byte count
// this is required since contracts can be attached post-hoc
func MessageByteCount(frames []*protocol.Frame) ByteCount {
	// messageByteCount := ByteCount(0)
	// for _, frame := range frames {
	// 	if frame.MessageType != protocol.MessageType_TransferContract {
	// 		messageByteCount += ByteCount(len(frame.MessageBytes))
	// 	}
	// }
	// return messageByteCount
	messageByteCount := ByteCount(0)
	for _, frame := range frames {
		messageByteCount += ByteCount(len(frame.MessageBytes))
	}
	return messageByteCount
}

// func MessageFrames(frames []*protocol.Frame) []*protocol.Frame {
// 	messages := []*protocol.Frame{}
// 	for _, frame := range frames {
// 		if frame.MessageType != protocol.MessageType_TransferContract {
// 			messages = append(messages, frame)
// 		}
// 	}
// 	return messages
// }
