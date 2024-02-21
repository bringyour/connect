package connect

import (
	"context"
    "testing"
    "time"
    mathrand "math/rand"
    "fmt"
    "crypto/hmac"
	"crypto/sha256"

	"google.golang.org/protobuf/proto"

    "github.com/go-playground/assert/v2"

    "bringyour.com/protocol"
)


func TestSendReceiveSenderReset(t *testing.T) {
	// in this case two senders with the same client_id send after each other
	// The receiver should be able to reset using the new sequence_id

	// timeout between receives or acks
	timeout := 30 * time.Second
	// number of messages
	n := 256


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
		aConditioner.lossProbability = 0.25
	})

	bConditioner.update(func() {
		bConditioner.randomDelay = 5 * time.Second
		bConditioner.lossProbability = 0.25
	})

	aSendTransport := newSendTransport()
	aReceiveTransport := newReceiveTransport()

	bSendTransport := newSendTransport()
	bReceiveTransport := newReceiveTransport()

	provideModes := map[protocol.ProvideMode]bool{
        protocol.ProvideMode_Network: true,
    }


	a := NewClientWithDefaults(ctx, aClientId)
	aRouteManager := NewRouteManager(a)
	aContractManager := NewContractManagerWithDefaults(a)
	defer func() {
		a.Cancel()
		aRouteManager.Close()
		aContractManager.Close()
	}()
	a.Setup(aRouteManager, aContractManager)
	go a.Run()

	aRouteManager.UpdateTransport(aSendTransport, []Route{aSend})
	aRouteManager.UpdateTransport(aReceiveTransport, []Route{aReceive})

    aContractManager.SetProvideModes(provideModes)


	b := NewClientWithDefaults(ctx, bClientId)
	bRouteManager := NewRouteManager(b)
	bContractManager := NewContractManagerWithDefaults(b)
	defer func() {
		b.Cancel()
		bRouteManager.Close()
		bContractManager.Close()
	}()
	b.Setup(bRouteManager, bContractManager)
	go b.Run()

	bRouteManager.UpdateTransport(bSendTransport, []Route{bSend})
	bRouteManager.UpdateTransport(bReceiveTransport, []Route{bReceive})

	bContractManager.SetProvideModes(provideModes)


	acks := make(chan error)
	receives := make(chan *protocol.SimpleMessage)

	b.AddReceiveCallback(func(sourceId Id, frames []*protocol.Frame, provideMode protocol.ProvideMode) {
		for _, frame := range frames {
			message, err := FromFrame(frame)
			if err != nil {
				panic(err)
			}
			switch v := message.(type) {
			case *protocol.SimpleMessage:
				receives <- v
			}
		}
	})

	var ackCount int
	var receiveCount int
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
			frame, err := ToFrame(message)
			if err != nil {
				panic(err)
			}
			a.SendWithTimeout(frame, bClientId, func(err error) {
				acks <- err
			}, timeout)
		}
	}()

	ackCount = 0
	receiveCount = 0
	receiveMessages = map[string]bool{}
	for len(receiveMessages) < n || ackCount < n {
		select {
		case <- ctx.Done():
			return
		case message := <- receives:
			receiveMessages[message.Content] = true
			assert.Equal(t, fmt.Sprintf("hi %d", receiveCount), message.Content)
			receiveCount += 1
		case err := <- acks:
			assert.Equal(t, err, nil)
			ackCount += 1
		case <- time.After(timeout):
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


	a2 := NewClientWithDefaults(ctx, aClientId)
	a2RouteManager := NewRouteManager(a2)
	a2ContractManager := NewContractManagerWithDefaults(a2)
	a2.Setup(a2RouteManager, a2ContractManager)
	go a2.Run()

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


	go func() {
		for i := 0; i < n; i += 1 {
			message := &protocol.SimpleMessage{
				Content: fmt.Sprintf("hi %d", i),
			}
			frame, err := ToFrame(message)
			if err != nil {
				panic(err)
			}
			a2.SendWithTimeout(frame, bClientId, func(err error) {
				acks <- err
			}, timeout)
		}
	}()

	ackCount = 0
	receiveCount = 0
	receiveMessages = map[string]bool{}
	for len(receiveMessages) < n || ackCount < n {
		select {
		case <- ctx.Done():
			return
		case message := <- receives:
			receiveMessages[message.Content] = true
			assert.Equal(t, fmt.Sprintf("hi %d", receiveCount), message.Content)
			receiveCount += 1
		case err := <- acks:
			assert.Equal(t, err, nil)
			ackCount += 1
		case <- time.After(timeout):
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
		ContractId: contractId.Bytes(),
		TransferByteCount: uint64(contractByteCount),
		SourceId: sourceId.Bytes(),
		DestinationId: destinationId.Bytes(),
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
			StoredContractHmac: storedContractHmac,
			ProvideMode: provideMode,
		},
	}

	frame, err := ToFrame(message)
	if err != nil {
		return nil, err
	}

	messageId := NewId()
	sequenceId := NewId()
	pack := &protocol.Pack{
		MessageId: messageId.Bytes(),
		SequenceId: sequenceId.Bytes(),
		SequenceNumber: 0,
		Head: true,
		Frames: []*protocol.Frame{frame},
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
			SourceId: sourceId.Bytes(),
			DestinationId: destinationId.Bytes(),
			StreamId: DirectStreamId.Bytes(),
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
		panic(fmt.Errorf("%s <> %s", sourceId.String(), sourceId_.String()))
	}

	if destinationId != destinationId_ {
		panic(fmt.Errorf("%s <> %s", destinationId.String(), destinationId_.String()))
	}

	return b
}



type conditioner struct {
	ctx context.Context
	fixedDelay time.Duration
	randomDelay time.Duration
	hold bool
	inversionWindow time.Duration
	invertFraction float32
	lossProbability float32
	monitor *Monitor
}

func newConditioner(ctx context.Context, in chan []byte) (*conditioner, chan []byte) {
	c := &conditioner{
		ctx: ctx,
		fixedDelay: 0,
		randomDelay: 0,
		lossProbability: 0,
		monitor: NewMonitor(),
	}
	out := make(chan []byte)
	go c.run(in, out)
	return c, out
}

func (self *conditioner) update(callback func()) {
	callback()
	self.monitor.NotifyAll()
}

func (self *conditioner) run(in chan []byte, out chan []byte) {
	defer close(out)

	for {
		select {
		case <- self.ctx.Done():
			return
		case <- self.monitor.NotifyChannel():
			continue
		case b, ok := <- in:
			if !ok {
				return
			}

			if mathrand.Float32() < self.lossProbability {
				continue
			}

			delay := self.fixedDelay
			if 0 < self.randomDelay {
				delay += time.Duration(mathrand.Intn(int(self.randomDelay)))
			}

			if delay <= 0 {
				select {
				case <- self.ctx.Done():
					return
				case out <- b:
				}
			} else {
				go func() {
					select {
					case <- self.ctx.Done():
						return
					case <- time.After(delay):
					}

					select {
					case <- self.ctx.Done():
						return
					case out <- b:
					}
				}()
			}


				
		}
	}
}




// conforms to `Transport`
type sendTransport struct {
}

func newSendTransport() *sendTransport {
	return &sendTransport{}
}

func (self *sendTransport) Priority() int {
	return 100
}

func (self *sendTransport) CanEvalRouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) bool {
	return true
}

func (self *sendTransport) RouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) float32 {
	// uniform weight
	return 1.0 / float32(1 + len(remainingStats))
}

func (self *sendTransport) MatchesSend(destinationId Id) bool {
	return true
}

func (self *sendTransport) MatchesReceive(destinationId Id) bool {
	return false
}

func (self *sendTransport) Downgrade(sourceId Id) {
	// nothing to downgrade
}


// conforms to `Transport`
type receiveTransport struct {
}

func newReceiveTransport() *receiveTransport {
	return &receiveTransport{}
}

func (self *receiveTransport) Priority() int {
	return 100
}

func (self *receiveTransport) CanEvalRouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) bool {
	return true
}

func (self *receiveTransport) RouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) float32 {
	// uniform weight
	return 1.0 / float32(1 + len(remainingStats))
}

func (self *receiveTransport) MatchesSend(destinationId Id) bool {
	return false
}

func (self *receiveTransport) MatchesReceive(destinationId Id) bool {
	return true
}

func (self *receiveTransport) Downgrade(sourceId Id) {
	// nothing to downgrade
}



