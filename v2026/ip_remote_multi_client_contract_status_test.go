package connect

import (
	"errors"
	"sync"
	"testing"

	"github.com/urnetwork/connect/v2026/protocol"
)

// contractStatusTestGenerator records runtime exclusions behind the same
// concurrent contract as the production API generator.
type contractStatusTestGenerator struct {
	testingEmptyMultiClientGenerator

	stateLock sync.Mutex
	fixed     bool
	excluded  []Id
}

// FixedDestinationSize distinguishes explicit destinations, which must remain
// redialable, from discovery destinations, which can be replaced.
func (self *contractStatusTestGenerator) FixedDestinationSize() (int, bool) {
	if self.fixed {
		return 1, true
	}
	return 0, false
}

// ExcludeClientId records one runtime exclusion.
func (self *contractStatusTestGenerator) ExcludeClientId(clientId Id) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.excluded = append(self.excluded, clientId)
}

// excludedClientIds returns a stable test snapshot.
func (self *contractStatusTestGenerator) excludedClientIds() []Id {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return append([]Id{}, self.excluded...)
}

func newContractStatusWindowFixture(destination Id) (*multiClientWindow, *multiClientChannel) {
	settings := DefaultMultiClientSettings()
	window := &multiClientWindow{
		contractStatusCallbacks: NewCallbackList[*contractStatusCallbackWorker](),
		resizeMonitor:           NewMonitor(),
	}
	client := &multiClientChannel{
		args: &multiClientChannelArgs{
			Destination: RequireMultiHopId(NewId(), destination),
		},
		settings:     settings,
		eventBuckets: []*multiClientEventBucket{},
		packetStats:  &clientWindowStats{},
	}
	return window, client
}

func contractStatusClientState(client *multiClientChannel) (warning bool, err error) {
	client.stateLock.Lock()
	defer client.stateLock.Unlock()
	return client.warning, client.endErr
}

// A Reliability result is the platform's authoritative statement that the
// selected destination has gone stale. It must poison only the exact channel
// that requested that contract, and wake resize so the normal removal,
// migration, and replacement path runs immediately.
func TestContractReliabilityFailureMarksWindowClientBad(t *testing.T) {
	destination := NewId()
	window, client := newContractStatusWindowFixture(destination)
	wake := window.resizeMonitor.NotifyChannel()
	reliability := protocol.ContractError_Reliability
	manager := &ContractManager{
		client:                          &Client{log: loggerOrDefault(nil)},
		contractStatusCallbacks:         NewCallbackList[*contractStatusCallbackWorker](),
		contractStatusDispatchCallbacks: NewCallbackList[ContractStatusFunction](),
	}
	manager.addContractStatusDispatchCallback(func(status *ContractStatus) {
		window.contractStatusFromClient(client, status)
	})
	frame, err := ToFrame(
		&protocol.CreateContractResult{Error: &reliability},
		DefaultProtocolVersion,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.HandleControlFrame(
		ContractKey{Destination: DestinationId(destination)},
		frame,
	); err != nil {
		t.Fatal(err)
	}

	warning, err := contractStatusClientState(client)
	if !warning {
		t.Fatal("reliability failure did not exclude the channel from new-flow selection")
	}
	if !errors.Is(err, errContractReliability) {
		t.Fatalf("channel error = %v, want contract reliability failure", err)
	}
	select {
	case <-wake:
	default:
		t.Fatal("reliability failure did not wake window resize")
	}
}

// Contract errors that describe the account, authorization, generic setup, or
// a malformed contract are not evidence that this provider route is bad. They
// remain observable through ContractStatus without mutating window health.
func TestOnlyContractReliabilityFailureMarksWindowClientBad(t *testing.T) {
	for _, contractError := range []protocol.ContractError{
		protocol.ContractError_NoPermission,
		protocol.ContractError_InsufficientBalance,
		protocol.ContractError_Setup,
		protocol.ContractError_Trust,
		protocol.ContractError_Invalid,
	} {
		t.Run(contractError.String(), func(t *testing.T) {
			destination := NewId()
			window, client := newContractStatusWindowFixture(destination)
			wake := window.resizeMonitor.NotifyChannel()
			window.contractStatusFromClient(client, &ContractStatus{
				Key:   ContractKey{Destination: DestinationId(destination)},
				Error: &contractError,
			})

			if warning, err := contractStatusClientState(client); warning || err != nil {
				t.Fatalf("%s changed channel health: warning=%t err=%v", contractError, warning, err)
			}
			select {
			case <-wake:
				t.Fatalf("%s woke window resize", contractError)
			default:
			}
		})
	}
}

// Even an explicit Reliability result cannot poison a neighboring exit. The
// result key must name the destination at the tail of the emitting channel.
func TestContractReliabilityFailureIsScopedToItsDestination(t *testing.T) {
	window, client := newContractStatusWindowFixture(NewId())
	reliability := protocol.ContractError_Reliability
	window.contractStatusFromClient(client, &ContractStatus{
		Key:   ContractKey{Destination: DestinationId(NewId())},
		Error: &reliability,
	})

	if warning, err := contractStatusClientState(client); warning || err != nil {
		t.Fatalf("another destination changed channel health: warning=%t err=%v", warning, err)
	}
}

// A terminal discovery destination must be excluded before resize is woken,
// or the first replacement pass can rediscover the same failed provider.
func TestContractReliabilityFailureExcludesDiscoveryDestination(t *testing.T) {
	destination := NewId()
	window, client := newContractStatusWindowFixture(destination)
	generator := &contractStatusTestGenerator{}
	window.generator = generator
	wake := window.resizeMonitor.NotifyChannel()
	reliability := protocol.ContractError_Reliability

	window.contractStatusFromClient(client, &ContractStatus{
		Key:   ContractKey{Destination: DestinationId(destination)},
		Error: &reliability,
	})

	select {
	case <-wake:
		excluded := generator.excludedClientIds()
		if len(excluded) != 1 || excluded[0] != destination {
			t.Fatalf("exclusions at resize wake = %v, want only the failed destination", excluded)
		}
	default:
		t.Fatal("reliability failure did not wake resize")
	}
}

// An explicitly selected destination has no alternate discovery candidate.
// Reliability still retires its current channel, but the generator must be
// allowed to redial the fixed destination.
func TestContractReliabilityFailureKeepsFixedDestinationRedialable(t *testing.T) {
	destination := NewId()
	window, client := newContractStatusWindowFixture(destination)
	generator := &contractStatusTestGenerator{fixed: true}
	window.generator = generator
	reliability := protocol.ContractError_Reliability

	window.contractStatusFromClient(client, &ContractStatus{
		Key:   ContractKey{Destination: DestinationId(destination)},
		Error: &reliability,
	})

	if excluded := generator.excludedClientIds(); 0 < len(excluded) {
		t.Fatalf("fixed destination exclusions = %v, want none", excluded)
	}
	if warning, err := contractStatusClientState(client); !warning || !errors.Is(err, errContractReliability) {
		t.Fatalf("fixed channel state = warning %t, err %v; want terminal reliability", warning, err)
	}
}

// Duplicate status frames can arrive from several contract lanes at once.
// They describe one destination lifecycle transition, so exclusion, terminal
// accounting, and replacement wake must remain bounded to one.
func TestConcurrentContractReliabilityFailuresRetireDestinationOnce(t *testing.T) {
	destination := NewId()
	window, client := newContractStatusWindowFixture(destination)
	generator := &contractStatusTestGenerator{}
	window.generator = generator
	reliability := protocol.ContractError_Reliability
	status := &ContractStatus{
		Key:   ContractKey{Destination: DestinationId(destination)},
		Error: &reliability,
	}

	start := make(chan struct{})
	var wait sync.WaitGroup
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			window.contractStatusFromClient(client, status)
		}()
	}
	close(start)
	wait.Wait()

	if excluded := generator.excludedClientIds(); len(excluded) != 1 || excluded[0] != destination {
		t.Fatalf("concurrent exclusions = %v, want one failed destination", excluded)
	}
	client.stateLock.Lock()
	errorCount := 0
	for _, eventBucket := range client.eventBuckets {
		errorCount += len(eventBucket.errs)
	}
	client.stateLock.Unlock()
	if errorCount != 1 {
		t.Fatalf("terminal error count = %d, want 1", errorCount)
	}
	lateWake := window.resizeMonitor.NotifyChannel()
	window.contractStatusFromClient(client, status)
	select {
	case <-lateWake:
		t.Fatal("duplicate terminal status scheduled another replacement wake")
	default:
	}
}
