package connect

import (
	"context"
	"testing"
	"time"
	// mathrand "math/rand"
	"crypto/hmac"
	"crypto/sha256"

	"github.com/go-playground/assert/v2"

	"google.golang.org/protobuf/proto"

	"bringyour.com/protocol"
)

func TestTakeContract(t *testing.T) {
	// in parallel, add contracts, take contracts, and optionally return contract
	// make sure all created contracts get eventually taken

	k := 4
	n := 64
	// contractReturnP := float32(0.5)
	timeout := 30 * time.Second

	ctx := context.Background()
	clientId := NewId()
	client := NewClientWithDefaults(ctx, clientId, NewNoContractClientOob())
	defer client.Cancel()
	contractManager := client.ContractManager()

	destinationId := NewId()

	contractManager.SetProvideModesWithReturnTraffic(map[protocol.ProvideMode]bool{
		protocol.ProvideMode_Network: true,
		protocol.ProvideMode_Public:  true,
	})

	contracts := make(chan *protocol.Contract)

	go func() {
		for i := 0; i < k*n; i += 1 {
			contractId := NewId()
			contractByteCount := gib(1)

			relationship := protocol.ProvideMode_Public
			provideSecretKey, ok := contractManager.GetProvideSecretKey(relationship)
			assert.Equal(t, true, ok)

			storedContract := &protocol.StoredContract{
				ContractId:        contractId.Bytes(),
				TransferByteCount: uint64(contractByteCount),
				SourceId:          clientId.Bytes(),
				DestinationId:     destinationId.Bytes(),
			}
			storedContractBytes, err := proto.Marshal(storedContract)
			assert.Equal(t, nil, err)
			mac := hmac.New(sha256.New, provideSecretKey)
			storedContractHmac := mac.Sum(storedContractBytes)

			verified := contractManager.Verify(storedContractHmac, storedContractBytes, relationship)
			assert.Equal(t, true, verified)

			result := &protocol.CreateContractResult{
				Contract: &protocol.Contract{
					StoredContractBytes: storedContractBytes,
					StoredContractHmac:  storedContractHmac,
					ProvideMode:         relationship,
				},
			}
			frame, err := ToFrame(result)
			assert.Equal(t, nil, err)

			contractManager.Receive(SourceId(ControlId), []*protocol.Frame{frame}, protocol.ProvideMode_Network)
		}
	}()

	for j := 0; j < k; j += 1 {
		go func() {
			for i := 0; i < n; {

				contractKey := ContractKey{
					Destination: DestinationId(destinationId),
				}
				if contract := contractManager.TakeContract(ctx, contractKey, timeout); contract != nil {
					// if mathrand.Float32() < contractReturnP {
					// 	// put back
					// 	contractManager.ReturnContract(ctx, destinationId, contract)
					// } else {
					select {
					case contracts <- contract:
					case <-time.After(timeout):
						t.FailNow()
					}
					i += 1
					// }
				}

			}

		}()
	}

	contractIds := map[Id]bool{}

	for i := 0; i < k*n; i += 1 {
		select {
		case contract := <-contracts:
			var storedContract protocol.StoredContract
			err := proto.Unmarshal(contract.StoredContractBytes, &storedContract)
			assert.Equal(t, nil, err)

			contractId, err := IdFromBytes(storedContract.ContractId)
			assert.Equal(t, nil, err)

			assert.Equal(t, false, contractIds[contractId])
			contractIds[contractId] = true

		case <-time.After(timeout):
			t.FailNow()
		}
	}

	assert.Equal(t, k*n, len(contractIds))

	// no more
	contractKey := ContractKey{
		Destination: DestinationId(destinationId),
	}
	contract := contractManager.TakeContract(ctx, contractKey, 0)
	assert.Equal(t, nil, contract)

	// all the contracts are accounted for
}
