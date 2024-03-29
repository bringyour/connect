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
	// "slices"
	// "runtime/debug"

	"golang.org/x/exp/maps"

	"google.golang.org/protobuf/proto"

	"bringyour.com/protocol"
)


// manage contracts which are embedded into each transfer sequence


type ContractManagerStats struct {
	ContractOpenCount int64
	ContractCloseCount int64
	// contract id -> byte count
	ContractOpenByteCounts map[Id]ByteCount
	// contract id -> destination id
	ContractOpenDestinationIds map[Id]Id
	ContractCloseByteCount ByteCount
	ReceiveContractCloseByteCount ByteCount
}

func NewContractManagerStats() *ContractManagerStats{
	return &ContractManagerStats{
		ContractOpenCount: 0,
		ContractCloseCount: 0,
		ContractOpenByteCounts: map[Id]ByteCount{},
		ContractOpenDestinationIds: map[Id]Id{},
		ContractCloseByteCount: 0,
		ReceiveContractCloseByteCount: 0,
	}
}

func (self *ContractManagerStats) ContractOpenByteCount() ByteCount {
	netContractOpenByteCount := ByteCount(0)
	for _, contractOpenByteCount := range self.ContractOpenByteCounts {
		netContractOpenByteCount += contractOpenByteCount
	}
	return netContractOpenByteCount
}


func DefaultContractManagerSettings() *ContractManagerSettings {
	// NETWORK EVENT: enable contracts 2024-01-01-00:00:00Z
	networkEventTimeEnableContracts, err := time.Parse(time.RFC3339, "2024-04-01T00:00:00Z")
	if err != nil {
		panic(err)
	}
	return &ContractManagerSettings{
		StandardContractTransferByteCount: mib(32),

		NetworkEventTimeEnableContracts: networkEventTimeEnableContracts,
	}
}

func DefaultContractManagerSettingsNoNetworkEvents() *ContractManagerSettings {
	settings := DefaultContractManagerSettings()
	settings.NetworkEventTimeEnableContracts = time.Time{}
	return settings
}


type ContractManagerSettings struct {
	StandardContractTransferByteCount ByteCount

	// enable contracts on the network
	// this can be removed after wide adoption
	NetworkEventTimeEnableContracts time.Time
}

func (self *ContractManagerSettings) ContractsEnabled() bool {
	return self.NetworkEventTimeEnableContracts.Before(time.Now())
}


type ContractManager struct {
	ctx context.Context
	client *Client

	settings *ContractManagerSettings

	mutex sync.Mutex

	provideSecretKeys map[protocol.ProvideMode][]byte

	destinationContracts map[Id]*contractQueue
	
	receiveNoContractClientIds map[Id]bool
	sendNoContractClientIds map[Id]bool

	contractErrorCallbacks *CallbackList[ContractErrorFunction]

	localStats *ContractManagerStats

	clientUnsub func()
}

func NewContractManagerWithDefaults(ctx context.Context, client *Client) *ContractManager {
	return NewContractManager(ctx, client, DefaultContractManagerSettings())
}

func NewContractManager(ctx context.Context, client *Client, settings *ContractManagerSettings) *ContractManager {
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
		settings: settings,
		provideSecretKeys: map[protocol.ProvideMode][]byte{},
		destinationContracts: map[Id]*contractQueue{},
		receiveNoContractClientIds: receiveNoContractClientIds,
		sendNoContractClientIds: sendNoContractClientIds,
		contractErrorCallbacks: NewCallbackList[ContractErrorFunction](),
		localStats: NewContractManagerStats(),
	}

	clientUnsub := client.AddReceiveCallback(contractManager.receive)
	contractManager.clientUnsub = clientUnsub

	return contractManager
}

func (self *ContractManager) StandardContractTransferByteCount() ByteCount {
	return self.settings.StandardContractTransferByteCount
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
	// for _, frame := range frames {
	// 	fmt.Printf("CONTRACT MANAGER RECEIVE %s-> %s\n", sourceId.String(), frame.MessageType)
	// }
	switch sourceId {
	case ControlId:
		for _, frame := range frames {
			if message, err := FromFrame(frame); err == nil {
				switch v := message.(type) {
				case *protocol.CreateContractResult:
					// fmt.Printf("GOT CONTRACT RESULT %s\n", v)
					if contractError := v.Error; contractError != nil {
						// fmt.Printf("CONTRACT ERROR %s\n", contractError)
						self.error(*contractError)
					} else if contract := v.Contract; contract != nil {
						TraceWithReturn(
							"[contract]add",
							func()(error) {
								return self.addContract(contract)
							},
						)
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

// clients must enable `ProvideMode_Stream` to allow return traffic
func (self *ContractManager) SetProvideModesWithReturnTraffic(provideModes map[protocol.ProvideMode]bool) {
	updatedProvideModes := map[protocol.ProvideMode]bool{}
	maps.Copy(updatedProvideModes, provideModes)
	updatedProvideModes[protocol.ProvideMode_Stream] = true
	self.SetProvideModes(updatedProvideModes)
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
	// best practice to make async sends into the client while being called from a client loop
	go self.client.SendControlWithTimeout(
		RequireToFrame(provide),
		func(err error) {},
		self.client.settings.ControlWriteTimeout,
	)
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
	if !self.settings.ContractsEnabled() {
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
	if !self.settings.ContractsEnabled() {
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
		contract := contractQueue.Poll()

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
			if remainingTimeout <= 0 {
				return nil
			}
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

/*
// must be called for a contract that was previously taken
func (self *ContractManager) ReturnContract(ctx context.Context, destinationId Id, contract *protocol.Contract) {
	contractQueue := self.openContractQueue(destinationId)
	defer self.closeContractQueue(destinationId)

	contractQueue.Push(contract)
}
*/

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

	contractId, err := IdFromBytes(storedContract.ContractId)
	if err != nil {
		return err
	}

	func() {
		contractQueue := self.openContractQueue(destinationId)
		defer self.closeContractQueue(destinationId)

		contractQueue.Add(contract, &storedContract)
	}()

	func() {
		self.mutex.Lock()
		defer self.mutex.Unlock()

		self.localStats.ContractOpenCount += 1
		self.localStats.ContractOpenByteCounts[contractId] = ByteCount(storedContract.TransferByteCount)
		self.localStats.ContractOpenDestinationIds[contractId] = destinationId
	}()

	return nil
}

func (self *ContractManager) openContractQueue(destinationId Id) *contractQueue {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	contractQueue, ok := self.destinationContracts[destinationId]
	if !ok {
		contractQueue = newContractQueue()
		self.destinationContracts[destinationId] = contractQueue
	}
	contractQueue.Open()

	return contractQueue
}

func (self *ContractManager) closeContractQueue(destinationId Id) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	contractQueue, ok := self.destinationContracts[destinationId]
	if !ok {
		panic("Open and close must be equally paired")
	}
	contractQueue.Close()
	if contractQueue.IsDone() {
		delete(self.destinationContracts, destinationId)
	}
}

func (self *ContractManager) CreateContract(destinationId Id, companionContract bool) {
	
	// look at destinationContracts and last contract to get previous contract id
	contractQueue := self.openContractQueue(destinationId)
	defer self.closeContractQueue(destinationId)

	fmt.Printf("Request contract size %d\n", self.settings.StandardContractTransferByteCount)
	createContract := &protocol.CreateContract{
		DestinationId: destinationId.Bytes(),
		TransferByteCount: uint64(self.settings.StandardContractTransferByteCount),
		Companion: companionContract,
		UsedContractIds: contractQueue.UsedContractIdBytes(),
	}

	// best practice to make async sends into the client while being called from a client loop
	go self.client.SendControlWithTimeout(
		RequireToFrame(createContract),
		nil,
		self.client.settings.ControlWriteTimeout,
	)
}

func (self *ContractManager) CompleteContract(contractId Id, ackedByteCount ByteCount, unackedByteCount ByteCount) {
	fmt.Printf("COMPLETE CONTRACT\n")
	
	closeContract := &protocol.CloseContract{
		ContractId: contractId.Bytes(),
		AckedByteCount: uint64(ackedByteCount),
		UnackedByteCount: uint64(unackedByteCount),
	}
	// best practice to make async sends into the client while being called from a client loop
	go self.client.SendControlWithTimeout(
		RequireToFrame(closeContract),
		nil,
		self.client.settings.ControlWriteTimeout,
	)

	opened := false
	var destinationId Id

	func() {
		self.mutex.Lock()
		defer self.mutex.Unlock()

		if _, ok := self.localStats.ContractOpenByteCounts[contractId]; ok {
			// opened via the contract manager
			opened = true
			destinationId = self.localStats.ContractOpenDestinationIds[contractId]
			self.localStats.ContractCloseCount += 1
			delete(self.localStats.ContractOpenByteCounts, contractId)
			delete(self.localStats.ContractOpenDestinationIds, contractId)
			self.localStats.ContractCloseByteCount += ackedByteCount
		} else {
			self.localStats.ReceiveContractCloseByteCount += ackedByteCount
		}
	}()

	if opened {
		contractQueue := self.openContractQueue(destinationId)
		defer self.closeContractQueue(destinationId)

		// the contract is partially closed on the platform now
		// it can be safely removed from the local used list
		contractQueue.RemoveUsedContract(contractId)
	}
	
}

func (self *ContractManager) LocalStats() *ContractManagerStats {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	return &ContractManagerStats{
		ContractOpenCount: self.localStats.ContractOpenCount,
		ContractCloseCount: self.localStats.ContractCloseCount,
		ContractOpenByteCounts: maps.Clone(self.localStats.ContractOpenByteCounts),
		ContractOpenDestinationIds: maps.Clone(self.localStats.ContractOpenDestinationIds),
		ContractCloseByteCount: self.localStats.ContractCloseByteCount,
		ReceiveContractCloseByteCount: self.localStats.ReceiveContractCloseByteCount,
	}
}

func (self *ContractManager) ResetLocalStats() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.localStats = NewContractManagerStats()
}

func (self *ContractManager) Close() {
	// debug.PrintStack()
	self.clientUnsub()
	// pending contracts in flight will just timeout on the platform
}

func (self *ContractManager) Flush(resetUsedContractIds bool) []Id {
	// close queued contracts
	contracts := func()([]*protocol.Contract) {
		self.mutex.Lock()
		defer self.mutex.Unlock()

		contracts := []*protocol.Contract{}
		for _, contractQueue := range self.destinationContracts {
			for _, contract := range contractQueue.Flush(resetUsedContractIds) {
				contracts = append(contracts, contract)
			}
		}
		return contracts
	}()

	return self.closeContracts(contracts)
}

func (self *ContractManager) FlushContractQueue(destinationId Id, resetUsedContractIds bool) []Id {
	contractQueue := self.openContractQueue(destinationId)
	defer self.closeContractQueue(destinationId)

	contracts := contractQueue.Flush(resetUsedContractIds)

	return self.closeContracts(contracts)
}

func (self *ContractManager) closeContracts(contracts []*protocol.Contract) []Id {
	contractIds := []Id{}
	for _, contract := range contracts {
		var storedContract protocol.StoredContract
		if err := proto.Unmarshal(contract.StoredContractBytes, &storedContract); err == nil {
			if contractId, err := IdFromBytes(storedContract.ContractId); err == nil {
				contractIds = append(contractIds, contractId)
				self.CompleteContract(contractId, ByteCount(0), ByteCount(0))
			}
		}
	}
	return contractIds
}


type contractQueue struct {
	updateMonitor *Monitor

	mutex sync.Mutex
	openCount int
	contracts map[Id]*protocol.Contract
	// remember all added contract ids
	usedContractIds map[Id]bool
}

func newContractQueue() *contractQueue {
	return &contractQueue{
		updateMonitor: NewMonitor(),
		openCount: 0,
		contracts: map[Id]*protocol.Contract{},
		usedContractIds: map[Id]bool{},
	}
}

func (self *contractQueue) Open() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.openCount += 1
}

func (self *contractQueue) Close() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.openCount -= 1
}

func (self *contractQueue) Poll() *protocol.Contract {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if len(self.contracts) == 0 {
		return nil
	}

	contractIds := maps.Keys(self.contracts)
	// choose arbitrarily
	contractId := contractIds[0]
	contract := self.contracts[contractId]
	delete(self.contracts, contractId)
	return contract
}

/*
func (self *contractQueue) Push(contract *protocol.Contract) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.contracts = append([]*protocol.Contract{contract}, self.contracts...)
}
*/

func (self *contractQueue) Add(contract *protocol.Contract, storedContract *protocol.StoredContract) error {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	contractId, err := IdFromBytes(storedContract.ContractId)
	if err != nil {
		return err
	}

	if self.usedContractIds[contractId] {
		fmt.Printf("[contract]add already used %s\n", contractId.String())
		// update contract
		if _, ok := self.contracts[contractId]; ok {
			self.contracts[contractId] = contract
			self.updateMonitor.NotifyAll()
		}
	} else {
		fmt.Printf("[contract]add %s\n", contractId.String())
		self.usedContractIds[contractId] = true
		self.contracts[contractId] = contract
		self.updateMonitor.NotifyAll()
	}
	return nil
}

func (self *contractQueue) RemoveUsedContract(contractId Id) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	delete(self.usedContractIds, contractId)
}

func (self *contractQueue) Flush(removeUsedContractIds bool) []*protocol.Contract {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	contracts := maps.Values(self.contracts)
	self.contracts = map[Id]*protocol.Contract{}
	if removeUsedContractIds {
		self.usedContractIds = map[Id]bool{}
	}

	return contracts
}

func (self *contractQueue) IsDone() bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if 0 < self.openCount {
		return false
	}

	return 0 == len(self.contracts) && 0 == len(self.usedContractIds)
}

func (self *contractQueue) UsedContractIdBytes() [][]byte {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	usedContractIdBytes := [][]byte{}
	for contractId, _ := range self.usedContractIds {
		usedContractIdBytes = append(usedContractIdBytes, contractId.Bytes())
	}
	return usedContractIdBytes
}

