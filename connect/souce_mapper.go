package connect

import (
	"context"
	"fmt"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jellydator/ttlcache/v3"
)

type RealEndpointAddress struct {
	realSource   netip.Addr
	transferPath TransferPath
}

type SourceMapper struct {
	lastAddress    *atomic.Uint64
	cache          *ttlcache.Cache[netip.Addr, RealEndpointAddress]
	forwardMapping map[TransferPath]netip.Addr
	mappingLock    *sync.RWMutex
}

// NewSourceMapper creates a new SourceMapper.
// The SourceMapper is used to map the source address of a packet to a new source address.
func NewSourceMapper(ctx context.Context) *SourceMapper {
	cache := ttlcache.New[netip.Addr, RealEndpointAddress](
		// if no packet is received within 5 minutes, the mapping will be removed.
		ttlcache.WithTTL[netip.Addr, RealEndpointAddress](5*time.Minute),
		// the cache will be able to store up to 20000 mappings.
		ttlcache.WithCapacity[netip.Addr, RealEndpointAddress](20_000),
	)

	mu := new(sync.RWMutex)
	forwardMapping := make(map[TransferPath]netip.Addr)

	cache.OnEviction(func(ctx context.Context, er ttlcache.EvictionReason, i *ttlcache.Item[netip.Addr, RealEndpointAddress]) {
		mu.Lock()
		defer mu.Unlock()

		// delete the forwardMapping
		delete(forwardMapping, i.Value().transferPath)
	})

	cache.OnInsertion(func(ctx context.Context, i *ttlcache.Item[netip.Addr, RealEndpointAddress]) {
		mu.Lock()
		defer mu.Unlock()

		// add the forwardMapping
		forwardMapping[i.Value().transferPath] = i.Key()
	})

	go cache.Start()

	go func() {
		<-ctx.Done()
		cache.Stop()
	}()

	return &SourceMapper{
		cache:          cache,
		lastAddress:    new(atomic.Uint64),
		mappingLock:    mu,
		forwardMapping: forwardMapping,
	}
}

// GetSourceAddressMapping returns the mapped address for the given source address.
// If the source address is not already mapped, a new address will be generated and returned.
func (sm *SourceMapper) GetSourceAddressMapping(sourceAddress netip.Addr, tp TransferPath) netip.Addr {

	sm.mappingLock.RLock()
	mappedAddress, found := sm.forwardMapping[tp]
	sm.mappingLock.RUnlock()
	if found {
		return mappedAddress
	}

	nextAddressInt := sm.lastAddress.Add(1)

	nextAddress := netip.AddrFrom4([4]byte{
		byte(nextAddressInt >> 24),
		byte(nextAddressInt >> 16),
		byte(nextAddressInt >> 8),
		byte(nextAddressInt),
	})

	fmt.Println("new mapping", sourceAddress, nextAddress, tp)
	sm.cache.Set(
		nextAddress,
		RealEndpointAddress{
			realSource:   sourceAddress,
			transferPath: tp,
		},
		0,
	)

	return nextAddress
}

func (sm *SourceMapper) GetRealEndpointAddress(sourceAddress netip.Addr) (RealEndpointAddress, bool) {
	v := sm.cache.Get(sourceAddress)

	if v == nil {
		return RealEndpointAddress{}, false
	}

	return v.Value(), true
}
