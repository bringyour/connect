package connect

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// contractErrorOob answers every CreateContract with a terminal error result
// until `errorCount` errors have been sent, then grants valid contracts. It
// models the platform answering a retryable account/setup request with the
// legacy InsufficientBalance result, one error frame per request.
type contractErrorOob struct {
	clientId Id
	// answer this many requests with an error; < 0 means all of them
	errorCount  int64
	errorsSent  atomic.Int64
	requestSeen chan struct{}
	requestOnce sync.Once
}

func (self *contractErrorOob) SendControl(frames []*protocol.Frame, callback func([]*protocol.Frame, error)) {
	var out []*protocol.Frame
	defer func() {
		for _, frame := range frames {
			MessagePoolReturn(frame.MessageBytes)
		}
		for _, frame := range out {
			MessagePoolReturn(frame.MessageBytes)
		}
	}()
	for _, frame := range frames {
		message, err := FromFrame(frame)
		if err != nil {
			continue
		}
		createContract, ok := message.(*protocol.CreateContract)
		if !ok {
			continue
		}
		if self.requestSeen != nil {
			self.requestOnce.Do(func() { close(self.requestSeen) })
		}

		if self.errorCount < 0 || self.errorsSent.Load() < self.errorCount {
			self.errorsSent.Add(1)
			// Legacy and account failures use InsufficientBalance. A distinct
			// Reliability result is tested at the multi-client window boundary;
			// it is intentionally terminal for that selected route.
			contractError := protocol.ContractError_InsufficientBalance
			result := &protocol.CreateContractResult{
				Error: &contractError,
			}
			if resultFrame, err := ToFrame(result, DefaultProtocolVersion); err == nil {
				out = append(out, resultFrame)
			}
			continue
		}

		// grant: the send side verifies only that the contract's source is
		// this client (the receiver does the cryptographic verification)
		destinationId, err := IdFromBytes(createContract.DestinationId)
		if err != nil {
			continue
		}
		storedContract := &protocol.StoredContract{
			ContractId:        NewId().Bytes(),
			TransferByteCount: createContract.TransferByteCount,
			SourceId:          self.clientId.Bytes(),
			DestinationId:     destinationId.Bytes(),
		}
		storedContractBytes, err := ProtoMarshal(storedContract)
		if err != nil {
			continue
		}
		result := &protocol.CreateContractResult{
			Contract: &protocol.Contract{
				StoredContractBytes: storedContractBytes,
				ProvideMode:         protocol.ProvideMode_Network,
			},
		}
		if resultFrame, err := ToFrame(result, DefaultProtocolVersion); err == nil {
			out = append(out, resultFrame)
		}
		MessagePoolReturn(storedContractBytes)
	}
	callback(out, nil)
}

// contractFailureLogger separates error and verbose lines so cancellation
// tests can assert severity, not just message text.
type contractFailureLogger struct {
	stateLock    sync.Mutex
	errorLines   []string
	verboseLines []string
}

func (self *contractFailureLogger) Info(args ...any) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.verboseLines = append(self.verboseLines, fmt.Sprint(args...))
}
func (self *contractFailureLogger) Infof(format string, args ...any) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.verboseLines = append(self.verboseLines, fmt.Sprintf(format, args...))
}
func (self *contractFailureLogger) Warningf(format string, args ...any) {}
func (self *contractFailureLogger) Errorf(format string, args ...any) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.errorLines = append(self.errorLines, fmt.Sprintf(format, args...))
}
func (self *contractFailureLogger) V(level int32) Verbose {
	return &contractFailureVerbose{log: self}
}

// contractFailureVerbose records verbose diagnostics separately from errors.
type contractFailureVerbose struct {
	log *contractFailureLogger
}

func (self *contractFailureVerbose) Enabled() bool { return true }
func (self *contractFailureVerbose) Info(args ...any) {
	self.log.stateLock.Lock()
	defer self.log.stateLock.Unlock()
	self.log.verboseLines = append(self.log.verboseLines, fmt.Sprint(args...))
}
func (self *contractFailureVerbose) Infof(format string, args ...any) {
	self.log.stateLock.Lock()
	defer self.log.stateLock.Unlock()
	self.log.verboseLines = append(self.log.verboseLines, fmt.Sprintf(format, args...))
}

// errorLinesContaining returns a stable snapshot filtered by one diagnostic key.
func (self *contractFailureLogger) errorLinesContaining(substring string) []string {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	matched := []string{}
	for _, line := range self.errorLines {
		if strings.Contains(line, substring) {
			matched = append(matched, line)
		}
	}
	return matched
}

// verboseLinesContaining returns a stable snapshot filtered by one diagnostic key.
func (self *contractFailureLogger) verboseLinesContaining(substring string) []string {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	matched := []string{}
	for _, line := range self.verboseLines {
		if strings.Contains(line, substring) {
			matched = append(matched, line)
		}
	}
	return matched
}

func contractErrorTestSettings() *ClientSettings {
	settings := DefaultClientSettings()
	// short retries against a much longer overall budget: the assertion is
	// about WHICH bound ends the wait, so keep them far apart
	settings.SendBufferSettings.CreateContractTimeout = 20 * time.Second
	settings.SendBufferSettings.CreateContractRetryInterval = 100 * time.Millisecond
	settings.SendBufferSettings.CreateContractRetryMaxInterval = 200 * time.Millisecond
	settings.SendBufferSettings.AckTimeout = 60 * time.Second
	return settings
}

// TestSendSequenceSurvivesTransientContractErrors pins that transient error
// results followed by a granted contract must not break the send.
//
// This remains a normal path in live setups: balance and legacy setup failures
// can look terminal for a few attempts and then start granting. That is why a
// generic client-side fail-fast on repeated error results is wrong: a
// 3-consecutive-errors exit was tried here and broke five server/proxy
// integration tests whose contract acquisition legitimately errors before
// succeeding. Only the platform's explicit ContractError_Reliability verdict
// is allowed to retire a multi-client route.
func TestSendSequenceSurvivesTransientContractErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientId := NewId()
	oob := &contractErrorOob{
		clientId:   clientId,
		errorCount: 2,
	}
	settings := contractErrorTestSettings()
	client := NewClient(ctx, clientId, oob, settings)
	defer client.Cancel()

	out := make(chan []byte, 1024)
	client.RouteManager().UpdateTransport(NewSendGatewayTransport(), []Route{out})
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-out:
			}
		}
	}()

	frame, err := ToFrame(&protocol.SimpleMessage{Content: "data"}, DefaultProtocolVersion)
	if err != nil {
		t.Fatal(err)
	}

	sent := make(chan struct{}, 1)
	success := client.SendWithTimeout(
		frame,
		NewId(),
		func(err error) {},
		-1,
	)
	if !success {
		t.Fatal("the send must enqueue")
	}

	// the sequence took a contract when the granted result lands: observe the
	// wire (the pack goes out only under a contract)
	go func() {
		// drain is in the main goroutine above; here poll for the contract
		// having been granted
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if 2 <= oob.errorsSent.Load() {
				select {
				case sent <- struct{}{}:
				default:
				}
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	select {
	case <-sent:
	case <-time.After(10 * time.Second):
		t.Fatal("the transient errors were never even requested through")
	}

	// the real assertion: the sequence must still acquire the granted
	// contract rather than having failed fast on the transient errors
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		contractKey := ContractKey{
			Destination: DestinationId(clientId),
		}
		_ = contractKey
		stats := client.ContractManager().LocalStats()
		if 0 < stats.ContractOpenCount {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("transient errors below the terminal count must be forgiven: the granted contract was never taken, so the wait failed fast on a transient")
}

// A sequence canceled while it owns a contract wait is ordinary lifecycle
// teardown. It must return the cancellation cause without emitting the same
// error used for a live acquisition failure.
func TestSendSequenceContractWaitCancellationIsNotAnError(t *testing.T) {
	testSendSequenceContractWaitCancellation(t, nil)
}

// Observes the real queue-take boundary even when host-wide backend health
// correctly suppresses outbound requests. The observer runs in the test owner.
func testSendSequenceContractWaitCancellation(
	t *testing.T,
	atWait func(*contractErrorOob),
) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientId := NewId()
	destination := NewId()
	requestSeen := make(chan struct{})
	waitEntered := make(chan struct{})
	allowWait := make(chan struct{})
	var waitOnce sync.Once
	var releaseOnce sync.Once
	releaseWait := func() { releaseOnce.Do(func() { close(allowWait) }) }
	log := &contractFailureLogger{}
	oob := &contractErrorOob{
		clientId:    clientId,
		errorCount:  -1,
		requestSeen: requestSeen,
	}
	settings := contractErrorTestSettings()
	settings.Log = log
	settings.EncryptionSettings.Mode = EncryptionModeOff
	// Prewarming is not this Pack's owned wait. Backend degradation may also
	// suppress its Oob request without preventing the actual TakeContract call.
	settings.SendBufferSettings.PrewarmOpeningContract = false
	settings.SendBufferSettings.beforeTakeContractForTest = func(id sendSequenceId) {
		if id.Destination != destination {
			return
		}
		waitOnce.Do(func() {
			close(waitEntered)
			<-allowWait
		})
	}
	client := NewClient(ctx, clientId, oob, settings)
	route := make(chan []byte, 1)
	defer func() {
		cancel()
		releaseWait()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if err := client.CloseAndWait(cleanupCtx); err != nil {
			t.Errorf("join contract cancellation fixture: %v", err)
		}
		for {
			select {
			case message := <-route:
				MessagePoolReturn(message)
			default:
				return
			}
		}
	}()

	client.RouteManager().UpdateTransport(
		NewSendGatewayTransport(),
		[]Route{route},
	)
	frame, err := ToFrame(
		&protocol.SimpleMessage{Content: "cancel contract wait"},
		DefaultProtocolVersion,
	)
	if err != nil {
		t.Fatal(err)
	}
	ackErrs := make(chan error, 1)
	if !client.SendWithTimeout(frame, destination, func(err error) {
		ackErrs <- err
	}, -1) {
		MessagePoolReturn(frame.MessageBytes)
		t.Fatal("the send must enqueue before cancellation")
	}

	select {
	case <-waitEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("the send sequence never reached the owned contract wait")
	}
	if atWait != nil {
		atWait(oob)
	}

	// Cancel only the sequence that owns the in-flight pack. Canceling the whole
	// client lets SendBuffer teardown race the sequence's own classification and
	// legitimately complete the callback with its broader "Send sequence
	// closed." cause instead.
	client.sendBuffer.mutex.Lock()
	var sequence *SendSequence
	for id, candidate := range client.sendBuffer.sendSequences {
		if id.Destination == destination {
			sequence = candidate
			break
		}
	}
	client.sendBuffer.mutex.Unlock()
	if sequence == nil {
		t.Fatal("the sequence disappeared before the cancellation boundary")
	}
	sequence.Cancel()
	releaseWait()

	select {
	case err := <-ackErrs:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ack error = %v, want context cancellation", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("canceled contract wait did not complete its ack callback")
	}

	if lines := log.errorLinesContaining("could not create contract"); len(lines) != 0 {
		t.Fatalf("ordinary cancellation emitted contract failure errors: %v", lines)
	}
	lines := log.verboseLinesContaining("contract creation canceled = context canceled")
	if len(lines) != 1 {
		t.Fatalf("cancellation diagnostics = %v, want one verbose root-cause line", lines)
	}
}

// A live sequence whose configured acquisition budget is exhausted is still
// an operational failure and must remain visible at error severity.
func TestSendSequenceContractWaitExhaustionRemainsAnErrorAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientId := NewId()
	destination := NewId()
	log := &contractFailureLogger{}
	oob := &contractErrorOob{
		clientId:   clientId,
		errorCount: -1,
	}
	settings := contractErrorTestSettings()
	settings.Log = log
	settings.EncryptionSettings.Mode = EncryptionModeOff
	// A zero budget enters the real exhaustion branch deterministically; no
	// scheduler timing or abbreviated wall-clock wait decides the result.
	settings.SendBufferSettings.CreateContractTimeout = 0
	classificationReached := make(chan struct{})
	allowClassification := make(chan struct{})
	defer func() {
		select {
		case <-allowClassification:
		default:
			close(allowClassification)
		}
	}()
	var classificationOnce sync.Once
	settings.SendBufferSettings.beforeContractFailureClassifyForTest = func(id sendSequenceId) {
		if id.Destination != destination {
			return
		}
		classificationOnce.Do(func() { close(classificationReached) })
		<-allowClassification
	}
	client := NewClient(ctx, clientId, oob, settings)
	defer client.Cancel()

	client.RouteManager().UpdateTransport(
		NewSendGatewayTransport(),
		[]Route{make(chan []byte, 1)},
	)
	frame, err := ToFrame(
		&protocol.SimpleMessage{Content: "exhaust contract wait"},
		DefaultProtocolVersion,
	)
	if err != nil {
		t.Fatal(err)
	}
	ackErrs := make(chan error, 1)
	if !client.SendWithTimeout(frame, destination, func(err error) {
		ackErrs <- err
	}, -1) {
		t.Fatal("the send must enqueue before acquisition exhausts")
	}

	select {
	case <-classificationReached:
	case <-time.After(10 * time.Second):
		t.Fatal("live exhaustion did not reach the classification boundary")
	}
	client.sendBuffer.mutex.Lock()
	var sequence *SendSequence
	for id, candidate := range client.sendBuffer.sendSequences {
		if id.Destination == destination {
			sequence = candidate
			break
		}
	}
	client.sendBuffer.mutex.Unlock()
	if sequence == nil {
		t.Fatal("the exhausted sequence disappeared before classification")
	}
	// The outcome is already a live exhaustion. This later cancellation must
	// not hide it by consulting the sequence context during classification.
	sequence.Cancel()
	close(allowClassification)

	select {
	case err := <-ackErrs:
		if err == nil || err.Error() != "No contract" {
			t.Fatalf("ack error = %v, want live No contract failure", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("exhausted contract wait did not complete its ack callback")
	}
	if err := ctx.Err(); err != nil {
		t.Fatalf("test context ended before the live failure was classified: %v", err)
	}

	lines := log.errorLinesContaining("exit could not create contract")
	if len(lines) != 1 {
		t.Fatalf("live failure diagnostics = %v, want one error line", lines)
	}
	if lines := log.verboseLinesContaining("contract creation canceled"); len(lines) != 0 {
		t.Fatalf("live exhaustion was misclassified as cancellation: %v", lines)
	}
}
