package connect

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	mathrand "math/rand"
	"os"
	"sync"
	"testing"
	"time"

	// "google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/v2026/protocol"
)

// encryptionMode selects how the SendSequence <-> ReceiveSequence TLS
// encryption is configured for a test. It is used to run the same scenario
// under both unencrypted and encrypted settings.
type encryptionMode int

const (
	encryptionModeOff encryptionMode = iota
	// encryptionModeOn: both sides Encrypt=true,
	// EncryptAllowUnwrappedFallback=false. The handshake must succeed for
	// any application data to flow (the SendSequence gates app packs on
	// session readiness).
	encryptionModeOn
	// encryptionModeOnAllowFallback: both sides Encrypt=true,
	// EncryptAllowUnwrappedFallback=true. The handshake is still expected
	// to succeed under normal conditions, but app packs are allowed to flow
	// in parallel during the handshake (gating off) and the sender will opt
	// out gracefully if the handshake fails.
	encryptionModeOnAllowFallback
	// encryptionModeFallback exercises the opt-out path: encryption is enabled
	// on the sender but the handshake is expected to time out, so the sender
	// falls back to plaintext.
	encryptionModeFallback
)

type ackBoundaryReuseLogger struct {
	reused <-chan struct{}
}

func (self ackBoundaryReuseLogger) Info(args ...any) {
}

func (self ackBoundaryReuseLogger) Infof(format string, args ...any) {
	// receiveAck emits its first level-2 pass log after returning the
	// acknowledged target to sendItemPool. Hold that exact boundary until a
	// concurrent sequence has taken and overwritten the pooled object.
	<-self.reused
}

func (self ackBoundaryReuseLogger) Warningf(format string, args ...any) {
}

func (self ackBoundaryReuseLogger) Errorf(format string, args ...any) {
}

func (self ackBoundaryReuseLogger) V(level int32) Verbose {
	return ackBoundaryReuseVerbose{enabled: level == 2}
}

type ackBoundaryReuseVerbose struct {
	enabled bool
}

func (self ackBoundaryReuseVerbose) Enabled() bool {
	return self.enabled
}

func (self ackBoundaryReuseVerbose) Info(args ...any) {
}

func (self ackBoundaryReuseVerbose) Infof(format string, args ...any) {
}

func TestCumulativeAckBoundarySurvivesImmediateSendItemReuse(t *testing.T) {
	clearSendItemPool()
	t.Cleanup(clearSendItemPool)

	reusedReady := make(chan struct{})
	reusedItem := make(chan *sendItem, 1)
	go func() {
		item := <-sendItemPool
		// Model another SendSequence taking the just-acknowledged object and
		// assigning a much newer sequence number before the original
		// cumulative loop advances.
		*item = sendItem{
			transferItem: transferItem{
				messageId:      NewId(),
				sequenceNumber: 100,
			},
		}
		reusedItem <- item
		close(reusedReady)
	}()

	client := &Client{
		clientTag: "ack-boundary-test",
		log:       ackBoundaryReuseLogger{reused: reusedReady},
	}
	target := &sendItem{
		transferItem: transferItem{
			messageId:      NewId(),
			sequenceNumber: 1,
		},
	}
	later := &sendItem{
		transferItem: transferItem{
			messageId:      NewId(),
			sequenceNumber: 2,
		},
	}
	sequence := &SendSequence{
		client:           client,
		log:              client.log,
		resendQueue:      newResendQueue(nil, 0),
		sendItems:        []*sendItem{target, later},
		flightController: newSendFlightController(DefaultSendBufferSettings()),
	}
	sequence.resendQueue.Add(target)
	sequence.resendQueue.Add(later)

	sequence.receiveAck(target.messageId, false, sequenceTag{}, false)

	if len(sequence.sendItems) != 1 || sequence.sendItems[0] != later {
		t.Fatalf("cumulative ack crossed its snapshotted boundary: remaining=%d", len(sequence.sendItems))
	}
	if _, ok := sequence.resendQueue.ContainsMessageId(later.messageId); !ok {
		t.Fatal("later send item was incorrectly acknowledged after target reuse")
	}

	// Keep the concurrently reused object live until the assertion completes.
	<-reusedItem
}

func TestSendItemPoolIsBoundedAndClearsAckLifetimeState(t *testing.T) {
	if cap(sendItemPool) != sendItemPoolCapacity {
		t.Fatalf("send-item pool capacity = %d, expected %d", cap(sendItemPool), sendItemPoolCapacity)
	}
	contractId := NewId()
	item := &sendItem{
		transferItem: transferItem{
			messageId:        NewId(),
			messageByteCount: 123,
			sequenceNumber:   456,
		},
		contractId:         &contractId,
		sendCount:          7,
		transferFrameBytes: MessagePoolCopy([]byte("wire")),
	}
	item.acks.add(sendAckRecord{callback: func(error) {}})
	item.messagePoolReturn()

	if item.messageId != (Id{}) ||
		item.messageByteCount != 0 ||
		item.sequenceNumber != 0 ||
		item.contractId != nil ||
		item.sendCount != 0 ||
		item.transferFrameBytes != nil ||
		item.acks.count != 0 {
		t.Fatal("send-item reuse retained wire, contract, sequence, or acknowledgement state")
	}
	ClearMessagePools()
	if len(sendItemPool) != 0 {
		t.Fatal("host memory-pressure clearing retained reusable send items")
	}
}

func TestSendReceiveSenderReset(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testing in short mode")
	}
	runSendReceiveSenderReset(t, encryptionModeOff)
}

func TestSendReceiveSenderResetEncrypted(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testing in short mode")
	}
	runSendReceiveSenderReset(t, encryptionModeOn)
}

func TestSendReceiveSenderResetEncryptedAllowFallback(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testing in short mode")
	}
	runSendReceiveSenderReset(t, encryptionModeOnAllowFallback)
}

func runSendReceiveSenderReset(t *testing.T, encMode encryptionMode) {
	// in this case two senders with the same client_id send after each other
	// The receiver should be able to reset using the new sequence_id

	// timeout between receives or acks
	// receive timeout. Large enough to absorb -race instrumentation overhead,
	// which can slow per-message processing by 5-10x.
	timeout := 5 * time.Minute
	// number of messages
	n := 1024
	stress := os.Getenv("CONNECT_TRANSFER_STRESS") != ""
	if stress {
		n = 16 * 1024
	}

	contractCount := 1
	// random delay / loss; the encrypted scenarios use a tighter conditioner
	// because TLS records still need to be delivered eventually for the
	// session to complete the handshake.
	var conditionerDelay time.Duration
	var conditionerLoss float32
	switch encMode {
	case encryptionModeOff:
		if stress {
			conditionerDelay = 5 * time.Second
			conditionerLoss = 0.5
		} else {
			conditionerDelay = 200 * time.Millisecond
			conditionerLoss = 0.1
		}
	case encryptionModeOn, encryptionModeOnAllowFallback:
		if stress {
			conditionerDelay = 200 * time.Millisecond
			conditionerLoss = 0.1
		} else {
			conditionerDelay = 100 * time.Millisecond
			conditionerLoss = 0.05
		}
		contractCount = 2
	case encryptionModeFallback:
		// no loss/delay so the opt-out and follow-on plaintext frames flow;
		// the handshake is forced to fail by configuring a tiny timeout
		// in `applyTestEncryptionSettings`.
		conditionerDelay = 0
		conditionerLoss = 0
		contractCount = 2
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	aClientId := NewId()
	bClientId := NewId()

	aSend := make(chan []byte)
	bSend := make(chan []byte)

	aConditioner, bReceive := newConditioner(ctx, aSend)
	bConditioner, aReceive := newConditioner(ctx, bSend)

	aConditioner.update(func() {
		aConditioner.randomDelay = conditionerDelay
		aConditioner.lossProbability = conditionerLoss
	})

	bConditioner.update(func() {
		bConditioner.randomDelay = conditionerDelay
		bConditioner.lossProbability = conditionerLoss
	})

	aSendTransport := NewSendGatewayTransport()
	aReceiveTransport := NewReceiveGatewayTransport()

	bSendTransport := NewSendGatewayTransport()
	bReceiveTransport := NewReceiveGatewayTransport()

	provideModes := map[protocol.ProvideMode]bool{
		protocol.ProvideMode_Network: true,
	}

	clientSettingsA := DefaultClientSettingsWithBufferSize(n)
	clientSettingsA.SendBufferSettings.AckTimeout = 300 * time.Second
	clientSettingsA.SendBufferSettings.IdleTimeout = 300 * time.Second
	clientSettingsA.ReceiveBufferSettings.GapTimeout = 300 * time.Second
	clientSettingsA.ReceiveBufferSettings.IdleTimeout = 300 * time.Second
	clientSettingsA.ForwardBufferSettings.IdleTimeout = 300 * time.Second
	clientSettingsA.ContractManagerSettings.LegacyCreateContract = true
	applyTestEncryptionSettings(clientSettingsA, encMode)
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

	clientSettingsB := DefaultClientSettingsWithBufferSize(n)
	clientSettingsB.SendBufferSettings.AckTimeout = 300 * time.Second
	clientSettingsB.SendBufferSettings.IdleTimeout = 300 * time.Second
	clientSettingsB.ReceiveBufferSettings.GapTimeout = 300 * time.Second
	clientSettingsB.ReceiveBufferSettings.IdleTimeout = 300 * time.Second
	clientSettingsB.ForwardBufferSettings.IdleTimeout = 300 * time.Second
	clientSettingsB.ContractManagerSettings.LegacyCreateContract = true
	applyTestEncryptionSettings(clientSettingsB, encMode)
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

	// Callbacks are receive-side delivery and cannot block. These exact bounded
	// collectors cover both sender generations; an overflow is a test failure,
	// not backpressure into either Client pump.
	acks := make(chan error, 2*n)
	receives := make(chan *protocol.SimpleMessage, 2*n)
	asyncErrors := make(chan error, 1)
	recordAsyncError := func(err error) {
		select {
		case asyncErrors <- err:
		default:
		}
	}
	ackCallback := func(err error) {
		select {
		case acks <- err:
		default:
			recordAsyncError(fmt.Errorf("ack callback collector overflow: %v", err))
		}
	}

	b.AddReceiveCallback(func(source TransferPath, frames []*protocol.Frame, peer Peer) {
		for _, frame := range frames {
			m, err := FromFrame(frame)
			if err != nil {
				recordAsyncError(fmt.Errorf("decode received frame: %w", err))
				return
			}
			switch v := m.(type) {
			case *protocol.SimpleMessage:
				select {
				case receives <- v:
				default:
					recordAsyncError(fmt.Errorf("receive callback collector overflow"))
				}
			}
		}
	})

	var ackCount int
	var receiveCount int
	var receiveMessages map[string]bool

	for range contractCount {
		err := aContractManager.HandleControlFrame(
			ContractKey{
				Destination: DestinationId(bClientId),
			},
			requireContractResult(
				protocol.ProvideMode_Network,
				bContractManager.RequireProvideSecretKey(protocol.ProvideMode_Network),
				aClientId,
				bClientId,
			),
		)
		AssertEqual(t, err, nil)
	}
	// aReceive <- requireTransferFrameBytes(
	// 	requireContractResultInitialPack(
	// 		protocol.ProvideMode_Network,
	// 		bContractManager.RequireProvideSecretKey(protocol.ProvideMode_Network),
	// 		aClientId,
	// 		bClientId,
	// 	),
	// 	ControlId,
	// 	aClientId,
	// )

	sendMessages := func(client *Client) <-chan struct{} {
		done := make(chan struct{})
		go func() {
			defer close(done)
			for i := 0; i < n; i += 1 {
				message := &protocol.SimpleMessage{
					Content: fmt.Sprintf("hi %d", i),
				}
				frame, err := ToFrame(message, DefaultProtocolVersion)
				if err != nil {
					recordAsyncError(fmt.Errorf("encode message %d: %w", i, err))
					return
				}
				if !client.Send(frame, bClientId, ackCallback) {
					recordAsyncError(fmt.Errorf("send message %d", i))
					return
				}
			}
		}()
		return done
	}
	sendDone := sendMessages(a)

	ackCount = 0
	receiveCount = 0
	receiveMessages = map[string]bool{}
	for receiveCount < n || ackCount < n {
		select {
		case <-ctx.Done():
			return
		case message := <-receives:
			receiveMessages[message.Content] = true
			AssertEqual(t, fmt.Sprintf("hi %d", receiveCount), message.Content)
			receiveCount += 1
		case err := <-acks:
			AssertEqual(t, err, nil)
			ackCount += 1
		case asyncErr := <-asyncErrors:
			t.Fatalf("asynchronous send/receive worker: %v", asyncErr)
		case <-time.After(timeout):
			t.Fatal("Timeout.")
		}
	}
	select {
	case <-sendDone:
	case asyncErr := <-asyncErrors:
		t.Fatalf("asynchronous sender: %v", asyncErr)
	case <-time.After(timeout):
		t.Fatal("sender did not finish")
	}
	for i := 0; i < n; i += 1 {
		message := fmt.Sprintf("hi %d", i)
		found := receiveMessages[message]
		AssertEqual(t, found, true)
	}

	AssertEqual(t, n, len(receiveMessages))
	AssertEqual(t, n, ackCount)

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

	for range contractCount {
		err := a2ContractManager.HandleControlFrame(
			ContractKey{
				Destination: DestinationId(bClientId),
			},
			requireContractResult(
				protocol.ProvideMode_Network,
				bContractManager.RequireProvideSecretKey(protocol.ProvideMode_Network),
				aClientId,
				bClientId,
			),
		)
		AssertEqual(t, err, nil)
	}
	// aReceive <- requireTransferFrameBytes(
	// 	requireContractResultInitialPack(
	// 		protocol.ProvideMode_Network,
	// 		bContractManager.RequireProvideSecretKey(protocol.ProvideMode_Network),
	// 		aClientId,
	// 		bClientId,
	// 	),
	// 	ControlId,
	// 	aClientId,
	// )

	select {
	case message := <-receives:
		// an older message was delivered
		AssertEqual(t, message, nil)
	default:
	}

	sendDone = sendMessages(a2)

	ackCount = 0
	receiveCount = 0
	receiveMessages = map[string]bool{}
	for receiveCount < n || ackCount < n {
		select {
		case <-ctx.Done():
			return
		case message := <-receives:
			receiveMessages[message.Content] = true
			AssertEqual(t, fmt.Sprintf("hi %d", receiveCount), message.Content)
			receiveCount += 1
		case err := <-acks:
			AssertEqual(t, err, nil)
			ackCount += 1
		case asyncErr := <-asyncErrors:
			t.Fatalf("asynchronous send/receive worker: %v", asyncErr)
		case <-time.After(timeout):
			t.Fatal("Timeout.")
		}
	}
	select {
	case <-sendDone:
	case asyncErr := <-asyncErrors:
		t.Fatalf("asynchronous sender: %v", asyncErr)
	case <-time.After(timeout):
		t.Fatal("replacement sender did not finish")
	}
	for i := 0; i < n; i += 1 {
		message := fmt.Sprintf("hi %d", i)
		found := receiveMessages[message]
		AssertEqual(t, found, true)
	}

	AssertEqual(t, n, len(receiveMessages))
	AssertEqual(t, n, ackCount)

	a2.Cancel()
	b.Cancel()
	cancel()
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
		ContractId:        contractId.Bytes(),
		TransferByteCount: uint64(contractByteCount),
		SourceId:          sourceId.Bytes(),
		DestinationId:     destinationId.Bytes(),
	}
	storedContractBytes, err := ProtoMarshal(storedContract)
	if err != nil {
		return nil, err
	}
	defer MessagePoolReturn(storedContractBytes)
	storedContractHmac := SignStoredContract(DefaultContractManagerSettings(), provideSecretKey, storedContractBytes)

	message := &protocol.CreateContractResult{
		Contract: &protocol.Contract{
			StoredContractBytes: storedContractBytes,
			StoredContractHmac:  storedContractHmac,
			ProvideMode:         provideMode,
		},
	}

	return ToFrame(message, DefaultProtocolVersion)
	// if err != nil {
	// 	return nil, err
	// }
	// defer MessagePoolReturn(frame.MessageBytes)

	// messageId := NewId()
	// sequenceId := NewId()
	// pack := &protocol.Pack{
	// 	MessageId:      messageId.Bytes(),
	// 	SequenceId:     sequenceId.Bytes(),
	// 	SequenceNumber: 0,
	// 	Head:           true,
	// 	Frames:         []*protocol.Frame{frame},
	// }

	// return ToFrame(pack, DefaultProtocolVersion)
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

func createTransferFrameBytes(frame *protocol.Frame, sourceId Id, destinationId Id) ([]byte, error) {
	transferFrame := &protocol.TransferFrame{
		TransferPath: &protocol.TransferPath{
			SourceId:      sourceId.Bytes(),
			DestinationId: destinationId.Bytes(),
			// StreamId: DirectStreamId.Bytes(),
		},
		Frame: frame,
	}

	return ProtoMarshal(transferFrame)
}

func requireTransferFrameBytes(frame *protocol.Frame, sourceId Id, destinationId Id) []byte {
	b, err := createTransferFrameBytes(frame, sourceId, destinationId)
	if err != nil {
		panic(err)
	}

	var filteredTransferFrame protocol.FilteredTransferFrame
	if err := ProtoUnmarshal(b, &filteredTransferFrame); err != nil {
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

// applyTestEncryptionSettings configures the per-client encryption settings.
// In the new design, encryption is a binary property of the per-peer
// `EncryptionSessionManager` session: cipher set → all traffic encrypted;
// cipher nil → all traffic plaintext. There is no per-frame fallback flag.
func applyTestEncryptionSettings(clientSettings *ClientSettings, encMode encryptionMode) {
	switch encMode {
	case encryptionModeOff:
		clientSettings.EncryptionSettings.Mode = EncryptionModeOff
	case encryptionModeOn, encryptionModeOnAllowFallback:
		clientSettings.EncryptionSettings.Mode = EncryptionModeOpportunistic
		clientSettings.EncryptionSettings.TlsTimeout = 60 * time.Second
	case encryptionModeFallback:
		// both sides enable encryption with a timeout too tight for the
		// handshake to complete; the sessions stay in the cipher-nil state
		// and all traffic flows in plaintext. (The peer-without-encryption
		// fallback — Encrypt=false on one side, an inert session manager —
		// is pinned by TestSendReceiveEncryptedPeerWithoutEncryption.)
		clientSettings.EncryptionSettings.Mode = EncryptionModeOpportunistic
		clientSettings.EncryptionSettings.TlsTimeout = 50 * time.Millisecond
	}
}

// TestSendReceiveEncryptedFallback exercises the opt-out path. The TLS
// handshake is forced to time out (tight TlsTimeout on both sides) and
// traffic flows in the plaintext cipher-nil state.
func TestSendReceiveEncryptedFallback(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testing in short mode")
	}
	runSendReceiveSenderReset(t, encryptionModeFallback)
}

// TestVerifyPeerCertificateAgainstContract covers the sender-side TLS
// certificate verification against the contract's PEM commitment.
func TestVerifyPeerCertificateAgainstContract(t *testing.T) {
	cert, _ := generateSequenceTlsCertificate()
	leaf := cert.Leaf
	leafPem := pemEncodeCertificate(leaf.Raw)

	// no commitment in the contract -> verification is skipped, no error
	ok, err := verifyPeerCertificateAgainstContract([]*x509.Certificate{leaf}, nil)
	AssertEqual(t, err, nil)
	AssertEqual(t, ok, true)

	// matching commitment -> success
	ok, err = verifyPeerCertificateAgainstContract([]*x509.Certificate{leaf}, [][]byte{leafPem})
	AssertEqual(t, err, nil)
	AssertEqual(t, ok, true)

	// mismatched commitment -> failure
	otherCert, _ := generateSequenceTlsCertificate()
	otherLeafPem := pemEncodeCertificate(otherCert.Leaf.Raw)
	ok, err = verifyPeerCertificateAgainstContract([]*x509.Certificate{leaf}, [][]byte{otherLeafPem})
	AssertNotEqual(t, err, nil)
	AssertEqual(t, ok, false)

	// peer presented no cert but contract has a commitment -> failure
	ok, err = verifyPeerCertificateAgainstContract(nil, [][]byte{leafPem})
	AssertNotEqual(t, err, nil)
	AssertEqual(t, ok, false)
}

func pemEncodeCertificate(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// TestMinimumMessageLenLimitFitsWorstCaseHandshake preserves the measured
// full-carrier regression and its deliberate 8-KiB safety boundary. Every H1
// receive cap and framer must admit the largest current
// `EncryptedControl{Handshake}` carrier; otherwise retransmission repeats the
// same rejected message and silently deadlocks the handshake.
func TestMinimumMessageLenLimitFitsWorstCaseHandshake(t *testing.T) {
	settings := DefaultClientSettings()
	limit := settings.MinimumMessageLenLimit()

	const observedRuntimeHandshakeCarrierByteCount = ByteCount(4950)
	if limit < observedRuntimeHandshakeCarrierByteCount {
		t.Fatalf(
			"MinimumMessageLenLimit %d < observed runtime handshake carrier %d",
			limit,
			observedRuntimeHandshakeCarrierByteCount,
		)
	}

	const requiredSafetyBoundary = ByteCount(8 * 1024)
	if limit != requiredSafetyBoundary {
		t.Fatalf(
			"MinimumMessageLenLimit %d, want bounded 8-KiB safety boundary %d",
			limit,
			requiredSafetyBoundary,
		)
	}

	platformSettings := DefaultPlatformTransportSettings()
	if platformSettings.H1MaxMessageByteCount != limit {
		t.Fatalf(
			"default platform H1 receive cap %d, want shared minimum %d",
			platformSettings.H1MaxMessageByteCount,
			limit,
		)
	}
	if platformSettings.FramerSettings.MaxMessageLen != int(limit) {
		t.Fatalf(
			"default platform framer MaxMessageLen %d, want shared minimum %d",
			platformSettings.FramerSettings.MaxMessageLen,
			limit,
		)
	}
}

// FIXME TestAckTimeout

func TestSendBufferSharesWireIdentityAcrossLocalRoutes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	defer client.Cancel()

	peerId := NewId()
	destination := peerId
	send := func(content string, opts ...any) {
		frame := &protocol.Frame{
			MessageType:  protocol.MessageType_TransferExchangeSignals,
			MessageBytes: []byte(content),
		}
		if !client.SendWithTimeout(frame, destination, nil, time.Second, opts...) {
			t.Fatalf("send %q failed", content)
		}
	}
	onlySequence := func(stage string) (sendSequenceId, *SendSequence) {
		client.sendBuffer.mutex.Lock()
		defer client.sendBuffer.mutex.Unlock()
		var exactCount int
		var wireCount int
		var foundId sendSequenceId
		var foundSequence *SendSequence
		for id, sequence := range client.sendBuffer.sendSequences {
			if id.Destination == destination {
				exactCount += 1
				foundId = id
				foundSequence = sequence
			}
		}
		for wireId, sequence := range client.sendBuffer.wireSendSequences {
			if wireId.Destination == destination {
				wireCount += 1
				if sequence != foundSequence {
					t.Fatalf("%s: wire map points to a different sequence", stage)
				}
			}
		}
		// NewClient may asynchronously publish a control/key frame to a
		// different destination. Inspect only the peer under test while still
		// requiring both indexes for that peer to agree exactly.
		if exactCount != 1 || wireCount != 1 {
			t.Fatalf(
				"%s: peer sequence maps exact=%d wire=%d, want one each (global %d/%d)",
				stage,
				exactCount,
				wireCount,
				len(client.sendBuffer.sendSequences),
				len(client.sendBuffer.wireSendSequences),
			)
		}
		return foundId, foundSequence
	}

	send("plain")
	plainId, plain := onlySequence("plain")
	if plainId.ForceStream {
		t.Fatal("first sequence unexpectedly forced a stream")
	}

	// ForceStream is stamped on every Pack (the sequence lane), so the
	// receiver keys its head slot per lane: a ForceStream sequence COEXISTS
	// with the plain one instead of retiring it.
	send("stream", ForceStream())
	var stream *SendSequence
	func() {
		client.sendBuffer.mutex.Lock()
		defer client.sendBuffer.mutex.Unlock()
		exactCount := 0
		wireCount := 0
		for id, sequence := range client.sendBuffer.sendSequences {
			if id.Destination == destination {
				exactCount += 1
				if id.ForceStream {
					stream = sequence
				}
			}
		}
		for wireId := range client.sendBuffer.wireSendSequences {
			if wireId.Destination == destination {
				wireCount += 1
			}
		}
		if exactCount != 2 || wireCount != 2 {
			t.Fatalf("lane coexistence: exact=%d wire=%d, want two each", exactCount, wireCount)
		}
	}()
	select {
	case <-plain.ctx.Done():
		t.Fatal("ForceStream=false lane sequence was retired; lanes must coexist")
	default:
	}

	// Intermediaries are contract-acquisition metadata, not logical sequence
	// identity. A later multihop send therefore joins the existing stream lane.
	via := RequireMultiHopId(NewId(), peerId)
	frame := &protocol.Frame{
		MessageType:  protocol.MessageType_TransferExchangeSignals,
		MessageBytes: []byte("via intermediary"),
	}
	if !client.SendMultiHopWithTimeout(frame, via, nil, time.Second, ForceStream()) {
		t.Fatal("multi-hop enqueue failed")
	}
	func() {
		client.sendBuffer.mutex.Lock()
		defer client.sendBuffer.mutex.Unlock()
		viaCount := 0
		for id, sequence := range client.sendBuffer.sendSequences {
			if id.Destination == destination && id.ForceStream {
				viaCount += 1
				if sequence != stream {
					t.Fatal("multihop send replaced the existing stream sequence")
				}
			}
		}
		if viaCount != 1 {
			t.Fatalf("force-stream lane sequences = %d, want one shared sequence", viaCount)
		}
	}()
	select {
	case <-stream.ctx.Done():
		t.Fatal("multihop send canceled the existing stream sequence")
	default:
	}
	// The plain lane is distinct and remains untouched.
	select {
	case <-plain.ctx.Done():
		t.Fatal("plain lane sequence was retired by another lane's intermediaries fork")
	default:
	}
}

// TestSendReceiveEncryptedForceStreamData is the end-to-end regression test
// for the network-peer + post-quantum data blackhole. Same-network peers
// force AllowDirect on, so the multi-client sends application data with the
// `ForceStream` transfer option, while encryption's EncryptedControl carrier
// used the client's default TransferOptions (ForceStream=false). ForceStream
// keys the send sequence but is invisible on the wire, so the carrier forked
// a SECOND concurrent send sequence to the peer whose frames the receiver
// could not distinguish from the data sequence — both mapped to the same
// (source, role, companion) receive head slot, the newer sequence id evicted
// the older, and the loser's packs were dropped un-acked forever. Ordering
// is deterministic here: the data Pack constructs the fs=true sequence,
// whose construction acquires the session and emits the ClientHello, so the
// fs=false carrier (when it wrongly existed) always minted a NEWER sequence
// id and permanently starved the data.
//
// The test mimics the platform by serving contracts under BOTH ForceStream
// contract keys (the platform serves CreateContract for any key), sends data
// with ForceStream(), and requires (a) every message to be delivered and
// (b) exactly one client-role/non-companion send sequence to the peer — the
// carrier riding the data sequence, not a fork.
func TestSendReceiveEncryptedForceStreamData(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testing in short mode")
	}

	timeout := 5 * time.Minute
	n := 64

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	aClientId := NewId()
	bClientId := NewId()

	aSend := make(chan []byte)
	bSend := make(chan []byte)

	// no conditioning: the fork is structural, not loss-dependent
	_, bReceive := newConditioner(ctx, aSend)
	_, aReceive := newConditioner(ctx, bSend)

	aSendTransport := NewSendGatewayTransport()
	aReceiveTransport := NewReceiveGatewayTransport()
	bSendTransport := NewSendGatewayTransport()
	bReceiveTransport := NewReceiveGatewayTransport()

	provideModes := map[protocol.ProvideMode]bool{
		protocol.ProvideMode_Network: true,
	}

	newSettings := func() *ClientSettings {
		clientSettings := DefaultClientSettingsWithBufferSize(n)
		clientSettings.SendBufferSettings.AckTimeout = 300 * time.Second
		clientSettings.SendBufferSettings.IdleTimeout = 300 * time.Second
		clientSettings.ReceiveBufferSettings.GapTimeout = 300 * time.Second
		clientSettings.ReceiveBufferSettings.IdleTimeout = 300 * time.Second
		clientSettings.ForwardBufferSettings.IdleTimeout = 300 * time.Second
		clientSettings.ContractManagerSettings.LegacyCreateContract = true
		applyTestEncryptionSettings(clientSettings, encryptionModeOn)
		return clientSettings
	}

	a := NewClient(ctx, aClientId, NewNoContractClientOob(), newSettings())
	defer a.Cancel()
	a.RouteManager().UpdateTransport(aSendTransport, []Route{aSend})
	a.RouteManager().UpdateTransport(aReceiveTransport, []Route{aReceive})
	a.ContractManager().SetProvideModes(provideModes)

	b := NewClient(ctx, bClientId, NewNoContractClientOob(), newSettings())
	defer b.Cancel()
	b.RouteManager().UpdateTransport(bSendTransport, []Route{bSend})
	b.RouteManager().UpdateTransport(bReceiveTransport, []Route{bReceive})
	b.ContractManager().SetProvideModes(provideModes)

	acks := make(chan error, n)
	receives := make(chan *protocol.SimpleMessage, n)
	asyncErrors := make(chan error, 1)
	recordAsyncError := func(err error) {
		select {
		case asyncErrors <- err:
		default:
		}
	}

	b.AddReceiveCallback(func(source TransferPath, frames []*protocol.Frame, peer Peer) {
		for _, frame := range frames {
			m, err := FromFrame(frame)
			if err != nil {
				recordAsyncError(fmt.Errorf("decode ForceStream receive: %w", err))
				return
			}
			switch v := m.(type) {
			case *protocol.SimpleMessage:
				select {
				case receives <- v:
				default:
					recordAsyncError(fmt.Errorf("ForceStream receive collector overflow"))
				}
			}
		}
	})

	// the platform serves CreateContract for any contract key: feed the
	// data path's ForceStream key and the default key the EncryptedControl
	// carrier would use if it (wrongly) diverged from the data path
	for _, forceStream := range []bool{true, false} {
		for range 2 {
			err := a.ContractManager().HandleControlFrame(
				ContractKey{
					Destination: DestinationId(bClientId),
					ForceStream: forceStream,
				},
				requireContractResult(
					protocol.ProvideMode_Network,
					b.ContractManager().RequireProvideSecretKey(protocol.ProvideMode_Network),
					aClientId,
					bClientId,
				),
			)
			AssertEqual(t, err, nil)
		}
	}

	sendDone := make(chan struct{})
	go func() {
		defer close(sendDone)
		for i := 0; i < n; i += 1 {
			message := &protocol.SimpleMessage{
				Content: fmt.Sprintf("hi %d", i),
			}
			frame, err := ToFrame(message, DefaultProtocolVersion)
			if err != nil {
				recordAsyncError(fmt.Errorf("encode ForceStream message %d: %w", i, err))
				return
			}
			success := a.SendWithTimeout(frame, bClientId, func(err error) {
				select {
				case acks <- err:
				default:
					recordAsyncError(fmt.Errorf("ForceStream ack collector overflow: %v", err))
				}
			}, -1, ForceStream())
			if !success {
				recordAsyncError(fmt.Errorf("send ForceStream message %d", i))
				return
			}
		}
	}()

	receiveMessages := map[string]bool{}
	ackCount := 0
	receiveCount := 0
	deadline := time.Now().Add(timeout)
	for receiveCount < n || ackCount < n {
		// fast starvation guard: the pipe is in-memory and unconditioned, so
		// zero deliveries after a minute means the data sequence is being
		// dropped at the receiver, not slow
		progressTimeout := time.Minute
		if 0 < receiveCount {
			progressTimeout = time.Until(deadline)
		}
		select {
		case <-ctx.Done():
			return
		case message := <-receives:
			receiveMessages[message.Content] = true
			receiveCount += 1
		case err := <-acks:
			AssertEqual(t, err, nil)
			ackCount += 1
		case asyncErr := <-asyncErrors:
			t.Fatalf("asynchronous ForceStream worker: %v", asyncErr)
		case <-time.After(progressTimeout):
			t.Fatalf("Timeout: %d/%d received, %d/%d acked — the ForceStream data sequence is starved (EncryptedControl carrier fork)", receiveCount, n, ackCount, n)
		}
	}
	select {
	case <-sendDone:
	case asyncErr := <-asyncErrors:
		t.Fatalf("asynchronous ForceStream sender: %v", asyncErr)
	case <-time.After(timeout):
		t.Fatal("ForceStream sender did not finish")
	}
	for i := 0; i < n; i += 1 {
		message := fmt.Sprintf("hi %d", i)
		AssertEqual(t, receiveMessages[message], true)
	}

	// the carrier must ride the data path's sequence: exactly one
	// client-role, non-companion send sequence to the peer
	func() {
		a.sendBuffer.mutex.Lock()
		defer a.sendBuffer.mutex.Unlock()
		clientRoleSequences := []sendSequenceId{}
		for key := range a.sendBuffer.sendSequences {
			if key.Destination == bClientId &&
				key.EncryptionRole == sequenceTlsRoleClient &&
				!key.EncryptionCompanion &&
				!key.CompanionContract {
				clientRoleSequences = append(clientRoleSequences, key)
			}
		}
		if len(clientRoleSequences) != 1 {
			t.Fatalf("expected exactly one client-role send sequence to the peer (the EncryptedControl carrier must ride the data sequence), got %d: %v", len(clientRoleSequences), clientRoleSequences)
		}
		if !clientRoleSequences[0].ForceStream {
			t.Fatal("the surviving sequence must be the data path's ForceStream sequence")
		}
	}()
}
