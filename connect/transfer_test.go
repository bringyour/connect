package connect

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	mathrand "math/rand"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/go-playground/assert/v2"

	"bringyour.com/protocol"
)

func TestSendReceiveSenderReset(t *testing.T) {
	// in this case two senders with the same client_id send after each other
	// The receiver should be able to reset using the new sequence_id

	// timeout between receives or acks
	timeout := 60 * time.Second
	// number of messages
	n := 16 * 1024

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	aClientId := NewId()
	bClientId := NewId()

	aSend := make(chan []byte)
	bSend := make(chan []byte)

	aConditioner, bReceive := newConditioner(ctx, aSend)
	bConditioner, aReceive := newConditioner(ctx, bSend)

	aConditioner.update(func() {
		aConditioner.randomDelay = 5 * time.Second
		aConditioner.lossProbability = 0.5
	})

	bConditioner.update(func() {
		bConditioner.randomDelay = 5 * time.Second
		bConditioner.lossProbability = 0.5
	})

	aSendTransport := NewSendGatewayTransport()
	aReceiveTransport := NewReceiveGatewayTransport()

	bSendTransport := NewSendGatewayTransport()
	bReceiveTransport := NewReceiveGatewayTransport()

	provideModes := map[protocol.ProvideMode]bool{
		protocol.ProvideMode_Network: true,
	}

	clientSettingsA := DefaultClientSettings()
	clientSettingsA.SendBufferSettings.SequenceBufferSize = 0
	clientSettingsA.SendBufferSettings.AckBufferSize = 0
	clientSettingsA.SendBufferSettings.AckTimeout = 90 * time.Second
	clientSettingsA.SendBufferSettings.IdleTimeout = 180 * time.Second
	clientSettingsA.ReceiveBufferSettings.SequenceBufferSize = 0
	clientSettingsA.ReceiveBufferSettings.GapTimeout = 90 * time.Second
	clientSettingsA.ReceiveBufferSettings.IdleTimeout = 180 * time.Second
	// clientSettingsA.ReceiveBufferSettings.AckBufferSize = 0
	clientSettingsA.ForwardBufferSettings.SequenceBufferSize = 0
	clientSettingsA.ForwardBufferSettings.IdleTimeout = 180 * time.Second
	a := NewClient(ctx, aClientId, NewNoContractClientOob(), clientSettingsA)
	aRouteManager := a.RouteManager()
	aContractManager := a.ContractManager()
	// aRouteManager := NewRouteManager(a)
	// aContractManager := NewContractManagerWithDefaults(a)
	defer a.Cancel()
	// a.Setup(aRouteManager, aContractManager)
	// go a.Run()

	aRouteManager.UpdateTransport(aSendTransport, []Route{aSend})
	aRouteManager.UpdateTransport(aReceiveTransport, []Route{aReceive})

	aContractManager.SetProvideModes(provideModes)

	clientSettingsB := DefaultClientSettings()
	clientSettingsB.SendBufferSettings.SequenceBufferSize = 0
	clientSettingsB.SendBufferSettings.AckBufferSize = 0
	clientSettingsB.SendBufferSettings.AckTimeout = 90 * time.Second
	clientSettingsB.SendBufferSettings.IdleTimeout = 180 * time.Second
	clientSettingsB.ReceiveBufferSettings.SequenceBufferSize = 0
	clientSettingsB.ReceiveBufferSettings.GapTimeout = 90 * time.Second
	clientSettingsB.ReceiveBufferSettings.IdleTimeout = 180 * time.Second
	// clientSettingsB.ReceiveBufferSettings.AckBufferSize = 0
	clientSettingsB.ForwardBufferSettings.SequenceBufferSize = 0
	clientSettingsB.ForwardBufferSettings.IdleTimeout = 180 * time.Second
	b := NewClient(ctx, bClientId, NewNoContractClientOob(), clientSettingsB)
	bRouteManager := b.RouteManager()
	bContractManager := b.ContractManager()
	// bRouteManager := NewRouteManager(b)
	// bContractManager := NewContractManagerWithDefaults(b)
	defer b.Cancel()
	// b.Setup(bRouteManager, bContractManager)
	// go b.Run()

	bRouteManager.UpdateTransport(bSendTransport, []Route{bSend})
	bRouteManager.UpdateTransport(bReceiveTransport, []Route{bReceive})

	bContractManager.SetProvideModes(provideModes)

	acks := make(chan error)
	receives := make(chan *protocol.SimpleMessage)

	b.AddReceiveCallback(func(source TransferPath, frames []*protocol.Frame, provideMode protocol.ProvideMode) {
		for _, frame := range frames {
			switch v := RequireFromFrame(frame).(type) {
			case *protocol.SimpleMessage:
				receives <- v
			}
		}
	})

	var ackCount int
	var waitingAckCount int
	var receiveCount int
	var waitingReceiveCount int
	var receiveMessages map[string]bool

	aReceive <- requireTransferFrameBytes(
		requireContractResultInitialPack(
			protocol.ProvideMode_Network,
			bContractManager.RequireProvideSecretKey(protocol.ProvideMode_Network),
			aClientId,
			bClientId,
		),
		ControlId,
		aClientId,
	)

	go func() {
		for i := 0; i < n; i += 1 {
			message := &protocol.SimpleMessage{
				Content: fmt.Sprintf("hi %d", i),
			}
			frame := RequireToFrame(message)
			a.Send(frame, DestinationId(bClientId), func(err error) {
				acks <- err
			})
		}
	}()

	ackCount = 0
	waitingAckCount = -1
	receiveCount = 0
	waitingReceiveCount = -1
	receiveMessages = map[string]bool{}
	for receiveCount < n || ackCount < n {
		if receiveCount < n && waitingReceiveCount < receiveCount {
			fmt.Printf("[0] waiting for %d/%d\n", receiveCount+1, n)
			waitingReceiveCount = receiveCount
		} else if ackCount < n && waitingAckCount < ackCount {
			fmt.Printf("[0] waiting for ack %d/%d\n", ackCount+1, n)
		}

		select {
		case <-ctx.Done():
			return
		case message := <-receives:
			receiveMessages[message.Content] = true
			assert.Equal(t, fmt.Sprintf("hi %d", receiveCount), message.Content)
			receiveCount += 1
		case err := <-acks:
			assert.Equal(t, err, nil)
			ackCount += 1
		case <-time.After(timeout):
			t.Fatal("Timeout.")
		}
	}
	for i := 0; i < n; i += 1 {
		message := fmt.Sprintf("hi %d", i)
		found := receiveMessages[message]
		assert.Equal(t, found, true)
	}

	assert.Equal(t, n, len(receiveMessages))
	assert.Equal(t, n, ackCount)

	a.Cancel()
	aRouteManager.RemoveTransport(aSendTransport)
	aRouteManager.RemoveTransport(aReceiveTransport)

	select {
	case <-time.After(1 * time.Second):
	}

	a2 := NewClient(ctx, aClientId, NewNoContractClientOob(), clientSettingsA)
	// a2 := NewClientWithDefaults(ctx, aClientId, NewNoContractClientOob())
	a2RouteManager := a2.RouteManager()
	a2ContractManager := a2.ContractManager()
	// a2RouteManager := NewRouteManager(a2)
	// a2ContractManager := NewContractManagerWithDefaults(a2)
	// a2.Setup(a2RouteManager, a2ContractManager)
	defer a2.Cancel()
	// go a2.Run()

	a2RouteManager.UpdateTransport(aSendTransport, []Route{aSend})
	a2RouteManager.UpdateTransport(aReceiveTransport, []Route{aReceive})

	a2ContractManager.SetProvideModes(provideModes)

	aReceive <- requireTransferFrameBytes(
		requireContractResultInitialPack(
			protocol.ProvideMode_Network,
			bContractManager.RequireProvideSecretKey(protocol.ProvideMode_Network),
			aClientId,
			bClientId,
		),
		ControlId,
		aClientId,
	)

	select {
	case message := <-receives:
		// an older message was delivered
		assert.Equal(t, message, nil)
	default:
	}

	go func() {
		for i := 0; i < n; i += 1 {
			message := &protocol.SimpleMessage{
				Content: fmt.Sprintf("hi %d", i),
			}
			frame := RequireToFrame(message)
			a2.Send(frame, DestinationId(bClientId), func(err error) {
				acks <- err
			})
		}
	}()

	ackCount = 0
	waitingAckCount = -1
	receiveCount = 0
	waitingReceiveCount = -1
	receiveMessages = map[string]bool{}
	for receiveCount < n || ackCount < n {
		if receiveCount < n && waitingReceiveCount < receiveCount {
			fmt.Printf("[1] waiting for %d/%d\n", receiveCount+1, n)
			waitingReceiveCount = receiveCount
		} else if ackCount < n && waitingAckCount < ackCount {
			fmt.Printf("[1] waiting for ack %d/%d\n", ackCount+1, n)
		}

		select {
		case <-ctx.Done():
			return
		case message := <-receives:
			receiveMessages[message.Content] = true
			assert.Equal(t, fmt.Sprintf("hi %d", receiveCount), message.Content)
			receiveCount += 1
		case err := <-acks:
			assert.Equal(t, err, nil)
			ackCount += 1
		case <-time.After(timeout):
			t.Fatal("Timeout.")
		}
	}
	for i := 0; i < n; i += 1 {
		message := fmt.Sprintf("hi %d", i)
		found := receiveMessages[message]
		assert.Equal(t, found, true)
	}

	fmt.Printf("[2] done\n")

	assert.Equal(t, n, len(receiveMessages))
	assert.Equal(t, n, ackCount)

	a2.Cancel()
	b.Cancel()
	cancel()
}

func createContractResultInitialPack(
	provideMode protocol.ProvideMode,
	provideSecretKey []byte,
	sourceId Id,
	destinationId Id,
) (*protocol.Frame, error) {
	contractId := NewId()
	contractByteCount := 8 * 1024 * 1024 * 1024

	storedContract := &protocol.StoredContract{
		ContractId:        contractId.Bytes(),
		TransferByteCount: uint64(contractByteCount),
		SourceId:          sourceId.Bytes(),
		DestinationId:     destinationId.Bytes(),
	}
	storedContractBytes, err := proto.Marshal(storedContract)
	if err != nil {
		return nil, err
	}
	mac := hmac.New(sha256.New, provideSecretKey)
	storedContractHmac := mac.Sum(storedContractBytes)

	message := &protocol.CreateContractResult{
		Contract: &protocol.Contract{
			StoredContractBytes: storedContractBytes,
			StoredContractHmac:  storedContractHmac,
			ProvideMode:         provideMode,
		},
	}

	frame, err := ToFrame(message)
	if err != nil {
		return nil, err
	}

	messageId := NewId()
	sequenceId := NewId()
	pack := &protocol.Pack{
		MessageId:      messageId.Bytes(),
		SequenceId:     sequenceId.Bytes(),
		SequenceNumber: 0,
		Head:           true,
		Frames:         []*protocol.Frame{frame},
	}

	return ToFrame(pack)
}

func requireContractResultInitialPack(
	provideMode protocol.ProvideMode,
	provideSecretKey []byte,
	sourceId Id,
	destinationId Id,
) *protocol.Frame {
	frame, err := createContractResultInitialPack(provideMode, provideSecretKey, sourceId, destinationId)
	if err != nil {
		panic(err)
	}
	return frame
}

func createTransferFrameBytes(frame *protocol.Frame, sourceId Id, destinationId Id) ([]byte, error) {
	transferFrame := &protocol.TransferFrame{
		TransferPath: &protocol.TransferPath{
			SourceId:      sourceId.Bytes(),
			DestinationId: destinationId.Bytes(),
			// StreamId: DirectStreamId.Bytes(),
		},
		Frame: frame,
	}

	return proto.Marshal(transferFrame)
}

func requireTransferFrameBytes(frame *protocol.Frame, sourceId Id, destinationId Id) []byte {
	b, err := createTransferFrameBytes(frame, sourceId, destinationId)
	if err != nil {
		panic(err)
	}

	var filteredTransferFrame protocol.FilteredTransferFrame
	if err := proto.Unmarshal(b, &filteredTransferFrame); err != nil {
		panic(err)
	}
	sourceId_, err := IdFromBytes(filteredTransferFrame.TransferPath.SourceId)
	if err != nil {
		panic(err)
	}
	destinationId_, err := IdFromBytes(filteredTransferFrame.TransferPath.DestinationId)
	if err != nil {
		panic(err)
	}

	if sourceId != sourceId_ {
		panic(fmt.Errorf("%s <> %s", sourceId, sourceId_))
	}

	if destinationId != destinationId_ {
		panic(fmt.Errorf("%s <> %s", destinationId, destinationId_))
	}

	return b
}

type conditioner struct {
	ctx             context.Context
	fixedDelay      time.Duration
	randomDelay     time.Duration
	hold            bool
	inversionWindow time.Duration
	invertFraction  float32
	lossProbability float32
	monitor         *Monitor
	mutex           sync.Mutex
}

func newConditioner(ctx context.Context, in chan []byte) (*conditioner, chan []byte) {
	c := &conditioner{
		ctx:             ctx,
		fixedDelay:      0,
		randomDelay:     0,
		lossProbability: 0,
		monitor:         NewMonitor(),
	}
	out := make(chan []byte)
	go c.run(in, out)
	return c, out
}

func (self *conditioner) update(callback func()) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	callback()
	self.monitor.NotifyAll()
}

func (self *conditioner) calcLoss() bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	return mathrand.Float32() < self.lossProbability
}

func (self *conditioner) calcDelay() time.Duration {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	delay := self.fixedDelay
	if 0 < self.randomDelay {
		delay += time.Duration(mathrand.Intn(int(self.randomDelay)))
	}
	return delay
}

func (self *conditioner) run(in chan []byte, out chan []byte) {
	// defer close(out)

	for {
		select {
		case <-self.ctx.Done():
			return
		case <-self.monitor.NotifyChannel():
			continue
		case b, ok := <-in:
			if !ok {
				return
			}

			if self.calcLoss() {
				continue
			}

			delay := self.calcDelay()

			if delay <= 0 {
				select {
				case <-self.ctx.Done():
					return
				case out <- b:
				}
			} else {
				go func() {
					select {
					case <-self.ctx.Done():
						return
					case <-time.After(delay):
					}

					select {
					case <-self.ctx.Done():
						return
					case out <- b:
					}
				}()
			}
		}
	}
}

// FIXME TestAckTimeout
