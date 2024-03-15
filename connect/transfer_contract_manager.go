package connect

import (
	"context"
	"time"
	"sync"
	"errors"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/rand"
	"fmt"

	"golang.org/x/exp/maps"

	"google.golang.org/protobuf/proto"

	"bringyour.com/protocol"
)


// manage contracts which are embedded into each transfer sequence


func DefaultContractManagerSettings() *ContractManagerSettings {
	return &ContractManagerSettings{
		StandardTransferByteCount: gib(8),
	}
}


type ContractManagerSettings struct {
	StandardTransferByteCount ByteCount
}


type ContractManager struct {
	ctx context.Context
	client *Client

	contractManagerSettings *ContractManagerSettings

	mutex sync.Mutex

	provideSecretKeys map[protocol.ProvideMode][]byte

	destinationContracts map[Id]*ContractQueue
	
	receiveNoContractClientIds map[Id]bool
	sendNoContractClientIds map[Id]bool

	contractErrorCallbacks *CallbackList[ContractErrorFunction]

	clientUnsub func()
}

func NewContractManagerWithDefaults(ctx context.Context, client *Client) *ContractManager {
	return NewContractManager(ctx, client, DefaultContractManagerSettings())
}

func NewContractManager(ctx context.Context, client *Client, contractManagerSettings *ContractManagerSettings) *ContractManager {
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
		ctx: ctx,
		client: client,
		contractManagerSettings: contractManagerSettings,
		provideSecretKeys: map[protocol.ProvideMode][]byte{},
		destinationContracts: map[Id]*ContractQueue{},
		receiveNoContractClientIds: receiveNoContractClientIds,
		sendNoContractClientIds: sendNoContractClientIds,
		contractErrorCallbacks: NewCallbackList[ContractErrorFunction](),
	}

	clientUnsub := client.AddReceiveCallback(contractManager.receive)
	contractManager.clientUnsub = clientUnsub

	return contractManager
}

func (self *ContractManager) StandardTransferByteCount() ByteCount {
	return self.contractManagerSettings.StandardTransferByteCount
}

func (self *ContractManager) addContractErrorCallback(contractErrorCallback ContractErrorFunction) func() {
	callbackId := self.contractErrorCallbacks.Add(contractErrorCallback)
	return func() {
		self.contractErrorCallbacks.Remove(callbackId)
	}
}

// func (self *ContractManager) removeContractErrorCallback(contractErrorCallback ContractErrorFunction) {
// 	self.contractErrorCallbacks.Remove(contractErrorCallback)
// }

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
			case <- self.ctx.Done():
				return nil
			case <- ctx.Done():
				return nil
			case <- notify:
			}
		} else if timeout == 0 {
			return nil
		} else {
			remainingTimeout := enterTime.Add(timeout).Sub(time.Now())
			select {
			case <- self.ctx.Done():
				return nil
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
	// self.client.RemoveReceiveCallback(self.receive)
	self.clientUnsub()
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
