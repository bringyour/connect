package connect

import (
	"context"
    "testing"
    "time"
    // mathrand "math/rand"
    "fmt"
    "crypto/hmac"
	"crypto/sha256"

	"google.golang.org/protobuf/proto"

    "github.com/go-playground/assert/v2"

    "bringyour.com/protocol"
)



func TestSendReceiveRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// client a
	// client b
	// connected with a randomized channel

	// send createcontractresult into a

	// send test messages from a to b
	// check that each message gets acked
	// on b, check that each message gets received

	// create a new a2 with same client_id but different instance_id
	// send test messages from a to b
	// verify the same (acked and received)

	timeout := 30 * time.Second
	n := 10

	aClientId := NewId()
	bClientId := NewId()

	aSend := make(chan []byte)
	bSend := make(chan []byte)

	_, bReceive := newConditioner(ctx, aSend)
	_, aReceive := newConditioner(ctx, bSend)

	aSendTransport := newSendTransport(aSend)
	aReceiveTransport := newReceiveTransport(aReceive)

	bSendTransport := newSendTransport(bSend)
	bReceiveTransport := newReceiveTransport(bReceive)

	provideModes := map[protocol.ProvideMode]bool{
        protocol.ProvideMode_Network: true,
    }
	


	a := NewClientWithDefaults(ctx, aClientId)
	aRouteManager := NewRouteManager(a)
	aContractManager := NewContractManagerWithDefaults(a)
	defer func() {
		a.Close()
		aRouteManager.Close()
		aContractManager.Close()
	}()

	go a.Run(aRouteManager, aContractManager)

	aRouteManager.UpdateTransport(aSendTransport, []Route{aSend})
	aRouteManager.UpdateTransport(aReceiveTransport, []Route{aReceive})

    aContractManager.SetProvideModes(provideModes)



	b := NewClientWithDefaults(ctx, bClientId)
	bRouteManager := NewRouteManager(b)
	bContractManager := NewContractManagerWithDefaults(b)
	defer func() {
		b.Close()
		bRouteManager.Close()
		bContractManager.Close()
	}()

	go b.Run(bRouteManager, bContractManager)

	bRouteManager.UpdateTransport(bSendTransport, []Route{bSend})
	bRouteManager.UpdateTransport(bReceiveTransport, []Route{bReceive})

	bContractManager.SetProvideModes(provideModes)





	acks := make(chan error)
	receives := make(chan []*protocol.Frame)

	b.AddReceiveCallback(func(sourceId Id, frames []*protocol.Frame, provideMode protocol.ProvideMode) {
		receives <- frames
	})

	var ackCount int
	var receiveCount int
	


	
	a.SendWithTimeout(requireContractResult(
		protocol.ProvideMode_Network,
		aContractManager.RequireProvideSecretKey(protocol.ProvideMode_Network),
		aClientId,
		bClientId,
	), aClientId, nil, timeout)
	

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
	for receiveCount < n || ackCount < n {
		select {
		case <- ctx.Done():
			return
		case <- receives:
			receiveCount += 1
		case <- acks:
			ackCount += 1
		case <- time.After(timeout):
			t.FailNow()
		}
	}

	assert.Equal(t, n, receiveCount)
	assert.Equal(t, n, ackCount)


	a.Close()
	aRouteManager.RemoveTransport(aSendTransport)
	aRouteManager.RemoveTransport(aReceiveTransport)


	a2 := NewClientWithDefaults(ctx, aClientId)
	a2RouteManager := NewRouteManager(a2)
	a2ContractManager := NewContractManagerWithDefaults(a2)
	defer func() {
		a2.Close()
		a2RouteManager.Close()
		a2ContractManager.Close()
	}()

	go a2.Run(a2RouteManager, a2ContractManager)

	a2RouteManager.UpdateTransport(aSendTransport, []Route{aSend})
	a2RouteManager.UpdateTransport(aReceiveTransport, []Route{aReceive})

	a2ContractManager.SetProvideModes(provideModes)





	a2.SendWithTimeout(requireContractResult(
		protocol.ProvideMode_Network,
		a2ContractManager.RequireProvideSecretKey(protocol.ProvideMode_Network),
		aClientId,
		bClientId,
	), aClientId, nil, timeout)


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
	for receiveCount < n || ackCount < n {
		select {
		case <- ctx.Done():
			return
		case <- receives:
			receiveCount += 1
		case <- acks:
			ackCount += 1
		case <- time.After(timeout):
			t.FailNow()
		}
	}

	assert.Equal(t, n, receiveCount)
	assert.Equal(t, n, ackCount)

	a2.Close()
	b.Close()
}



func createContractResult(
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

	return ToFrame(message)
}


func requireContractResult(
	provideMode protocol.ProvideMode,
	provideSecretKey []byte,
	sourceId Id,
	destinationId Id,
) *protocol.Frame {
	frame, err := createContractResult(provideMode, provideSecretKey, sourceId, destinationId)
	if err != nil {
		panic(err)
	}
	return frame
}




type conditioner struct {
	ctx context.Context
	fixedDelay time.Duration
	randomDelay time.Duration
	hold bool
	inversionWindow time.Duration
	invertFraction float32
	lossFraction float32
	monitor *Monitor
}

func newConditioner(ctx context.Context, in chan []byte) (*conditioner, chan []byte) {
	c := &conditioner{
		ctx: ctx,
		fixedDelay: 0,
		randomDelay: 5 * time.Second,
		hold: false,
		inversionWindow: 10 * time.Second,
		invertFraction: 1.0,
		lossFraction: 0.5,
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

	// startTime := time.Now()

	for {
		select {
		case <- self.ctx.Done():
			return
		case b, ok := <- in:
			if !ok {
				return
			}
			// FIXME condition

			select {
			case <- self.ctx.Done():
				return
			case out <- b:
			}
		}
	}
}




// conforms to `Transport`
type sendTransport struct {
	send chan []byte
}

func newSendTransport(send chan []byte) *sendTransport {
	return &sendTransport{
		send: send,
	}
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
	receive chan []byte
}

func newReceiveTransport(receive chan []byte) *receiveTransport {
	return &receiveTransport{
		receive: receive,
	}
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


