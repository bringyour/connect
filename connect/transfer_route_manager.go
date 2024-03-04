package connect

import (
	"context"
	"time"
	"sync"
	"errors"
	mathrand "math/rand"
	"reflect"
	"slices"

	"golang.org/x/exp/maps"
)


// manage multiple routes to a destination, allowing weighted reads and writes to the routes
// this assumes the source is a single client


// routes are expected to have flow control and error detection and rejection
type Route = chan []byte


// each transport must have a unique local id
// This solves an issue where some transports can be implemented with zero state.
// Zero state transports makes it ambiguous whether the transport pointer can be used as a key.
// see https://github.com/golang/go/issues/65878
type Transport interface {
    TransportId() Id
    
    // lower priority takes precedence
    Priority() int
    
    CanEvalRouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) bool
    // returns the fraction of route weight that should be allocated to this transport
    // the remaining are the lower priority transports
    // call `rematchTransport` to re-evaluate the weights. this is used for a control loop where the weight is adjusted to match the actual distribution
    RouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) float32
    
    MatchesSend(destinationId Id) bool
    MatchesReceive(destinationId Id) bool

    // request that p2p and direct connections be re-established that include the source
    // connections will be denied for sources that have bad audits
    Downgrade(sourceId Id)
}


type MultiRouteWriter interface {
    Write(ctx context.Context, transportFrameBytes []byte, timeout time.Duration) error
    GetActiveRoutes() []Route
    GetInactiveRoutes() []Route
}


type MultiRouteReader interface {
    Read(ctx context.Context, timeout time.Duration) ([]byte, error)
    GetActiveRoutes() []Route
    GetInactiveRoutes() []Route
}


type RouteManager struct {
	ctx context.Context

    mutex sync.Mutex
    writerMatchState *MatchState
    readerMatchState *MatchState
}

func NewRouteManager(ctx context.Context) *RouteManager {
    return &RouteManager{
    	ctx: ctx,
        writerMatchState: NewMatchState(ctx, true, Transport.MatchesSend),
        // `weightedRoutes=false` because unless there is a cpu limit this is not needed
        readerMatchState: NewMatchState(ctx, false, Transport.MatchesReceive),
    }
}

func (self *RouteManager) DowngradeReceiverConnection(sourceId Id) {
    self.readerMatchState.Downgrade(sourceId)
}

func (self *RouteManager) OpenMultiRouteWriter(destinationId Id) MultiRouteWriter {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    return MultiRouteWriter(self.writerMatchState.openMultiRouteSelector(destinationId))
}

func (self *RouteManager) CloseMultiRouteWriter(w MultiRouteWriter) {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    self.writerMatchState.closeMultiRouteSelector(w.(*MultiRouteSelector))
}

func (self *RouteManager) OpenMultiRouteReader(destinationId Id) MultiRouteReader {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    return MultiRouteReader(self.readerMatchState.openMultiRouteSelector(destinationId))
}

func (self *RouteManager) CloseMultiRouteReader(r MultiRouteReader) {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    self.readerMatchState.closeMultiRouteSelector(r.(*MultiRouteSelector))
}

func (self *RouteManager) UpdateTransport(transport Transport, routes []Route) {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    self.writerMatchState.updateTransport(transport, routes)
    self.readerMatchState.updateTransport(transport, routes)
}

func (self *RouteManager) RemoveTransport(transport Transport) {
    self.UpdateTransport(transport, nil)
}

func (self *RouteManager) getTransportStats(transport Transport) (writerStats *RouteStats, readerStats *RouteStats) {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    writerStats = self.writerMatchState.getTransportStats(transport)
    readerStats = self.readerMatchState.getTransportStats(transport)
    return
}

func (self *RouteManager) Close() {
    // transports close individually and remove themselves via `updateTransport`
}


type MatchState struct {
	ctx context.Context
    weightedRoutes bool
    matches func(Transport, Id)(bool)

    transportRoutes map[Transport][]Route

    // destination id -> multi route selectors
    destinationMultiRouteSelectors map[Id]map[*MultiRouteSelector]bool

    // transport -> destination ids
    transportMatchedDestinations map[Transport]map[Id]bool
}

// note weighted routes typically are used by the sender not receiver
func NewMatchState(ctx context.Context, weightedRoutes bool, matches func(Transport, Id)(bool)) *MatchState {
    return &MatchState{
    	ctx: ctx,
        weightedRoutes: weightedRoutes,
        matches: matches,
        transportRoutes: map[Transport][]Route{},
        destinationMultiRouteSelectors: map[Id]map[*MultiRouteSelector]bool{},
        transportMatchedDestinations: map[Transport]map[Id]bool{},
    }
}

func (self *MatchState) getTransportStats(transport Transport) *RouteStats {
    destinationIds, ok := self.transportMatchedDestinations[transport]
    if !ok {
        return nil
    }
    netStats := NewRouteStats()
    for destinationId, _ := range destinationIds {
        if multiRouteSelectors, ok := self.destinationMultiRouteSelectors[destinationId]; ok {
            for multiRouteSelector, _  := range multiRouteSelectors {
                if stats := multiRouteSelector.getTransportStats(transport); stats != nil {
                    netStats.sendCount += stats.sendCount
                    netStats.sendByteCount += stats.sendByteCount
                    netStats.receiveCount += stats.receiveCount
                    netStats.receiveByteCount += stats.receiveByteCount
                }
            }
        }
    }
    return netStats
}

func (self *MatchState) openMultiRouteSelector(destinationId Id) *MultiRouteSelector {
    // fmt.Printf("create selector transports=%d\n", len(self.transportRoutes))

    multiRouteSelector := NewMultiRouteSelector(self.ctx, destinationId, self.weightedRoutes)

    multiRouteSelectors, ok := self.destinationMultiRouteSelectors[destinationId]
    if !ok {
        multiRouteSelectors = map[*MultiRouteSelector]bool{}
        self.destinationMultiRouteSelectors[destinationId] = multiRouteSelectors
    }
    multiRouteSelectors[multiRouteSelector] = true

    for transport, routes := range self.transportRoutes {
        matchedDestinations, ok := self.transportMatchedDestinations[transport]
        if !ok {
            matchedDestinations := map[Id]bool{}
            self.transportMatchedDestinations[transport] = matchedDestinations
        }

        // use the latest matches state
        if self.matches(transport, destinationId) {
            matchedDestinations[destinationId] = true
            multiRouteSelector.updateTransport(transport, routes)
        }
    }

    return multiRouteSelector
}

func (self *MatchState) closeMultiRouteSelector(multiRouteSelector *MultiRouteSelector) {
    // TODO readers do not need to prioritize routes

    destinationId := multiRouteSelector.destinationId
    multiRouteSelectors, ok := self.destinationMultiRouteSelectors[destinationId]
    if !ok {
        // not present
        return
    }
    delete(multiRouteSelectors, multiRouteSelector)

    if len(multiRouteSelectors) == 0 {
        // clean up the destination
        for _, matchedDestinations := range self.transportMatchedDestinations {
            delete(matchedDestinations, destinationId)
        }
    }
}

func (self *MatchState) updateTransport(transport Transport, routes []Route) {
    // c := 0
    // for _, multiRouteSelectors := range self.destinationMultiRouteSelectors {
    //  c += len(multiRouteSelectors)
    // }
    // fmt.Printf("update transport selectors=%d routes=%v\n", c, routes)

    if len(routes) == 0 {
        if currentMatchedDestinations, ok := self.transportMatchedDestinations[transport]; ok {
            for destinationId, _ := range currentMatchedDestinations {
                if multiRouteSelectors, ok := self.destinationMultiRouteSelectors[destinationId]; ok {
                    for multiRouteSelector, _ := range multiRouteSelectors {
                        multiRouteSelector.updateTransport(transport, nil)
                    }
                }
            }
        }

        delete(self.transportMatchedDestinations, transport)
        delete(self.transportRoutes, transport)
    } else {
        matchedDestinations := map[Id]bool{}

        currentMatchedDestinations, ok := self.transportMatchedDestinations[transport]
        if !ok {
            currentMatchedDestinations = map[Id]bool{}
        }

        for destinationId, multiRouteSelectors := range self.destinationMultiRouteSelectors {
            if self.matches(transport, destinationId) {
                matchedDestinations[destinationId] = true
                for multiRouteSelector, _ := range multiRouteSelectors {
                    multiRouteSelector.updateTransport(transport, routes)
                }
            } else if _, ok := currentMatchedDestinations[destinationId]; ok {
                // no longer matches
                for multiRouteSelector, _ := range multiRouteSelectors {
                    multiRouteSelector.updateTransport(transport, nil)
                }
            }
        }

        self.transportMatchedDestinations[transport] = matchedDestinations
        self.transportRoutes[transport] = routes
    }
}

func (self *MatchState) Downgrade(sourceId Id) {
    // FIXME request downgrade from the transports
}


type MultiRouteSelector struct {
    ctx context.Context
    cancel context.CancelFunc

    destinationId Id
    weightedRoutes bool

    transportUpdate *Monitor

    mutex sync.Mutex
    transportRoutes map[Transport][]Route
    routeStats map[Route]*RouteStats
    routeActive map[Route]bool
    routeWeight map[Route]float32
}

func NewMultiRouteSelector(ctx context.Context, destinationId Id, weightedRoutes bool) *MultiRouteSelector {
	cancelCtx, cancel := context.WithCancel(ctx)
    return &MultiRouteSelector{
        ctx: cancelCtx,
        cancel: cancel,
        destinationId: destinationId,
        weightedRoutes: weightedRoutes,
        transportUpdate: NewMonitor(),
        transportRoutes: map[Transport][]Route{},
        routeStats: map[Route]*RouteStats{},
        routeActive: map[Route]bool{},
        routeWeight: map[Route]float32{},
    }
}

func (self *MultiRouteSelector) getTransportStats(transport Transport) *RouteStats {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    currentRoutes, ok := self.transportRoutes[transport]
    if !ok {
        return nil
    }
    netStats := NewRouteStats()
    for _, currentRoute := range currentRoutes {
        if stats, ok := self.routeStats[currentRoute]; ok {
            netStats.sendCount += stats.sendCount
            netStats.sendByteCount += stats.sendByteCount
            netStats.receiveCount += stats.receiveCount
            netStats.receiveByteCount += stats.receiveByteCount
        }
    }
    return netStats
}

// if weightedRoutes, this applies new priorities and weights. calling this resets all route stats.
// the reason to reset weightedRoutes is that the weight calculation needs to consider only the stats since the previous weight change
func (self *MultiRouteSelector) updateTransport(transport Transport, routes []Route) {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    // activeRoutes := func()([]Route) {
    //  activeRoutes := []Route{}
    //  for _, routes := range self.transportRoutes {
    //      for _, route := range routes {
    //          if self.routeActive[route] {
    //              activeRoutes = append(activeRoutes, route)
    //          }
    //      }
    //  }
    //  return activeRoutes
    // }

    // preTransportCount := len(self.transportRoutes)
    // preActiveRouteCount := len(activeRoutes())

    if len(routes) == 0 {
        if currentRoutes, ok := self.transportRoutes[transport]; ok {
            for _, currentRoute := range currentRoutes {
                delete(self.routeStats, currentRoute)
                delete(self.routeActive, currentRoute)
                delete(self.routeWeight, currentRoute)
            }
            delete(self.transportRoutes, transport)
        } else {
            // transport is not active. nothing to do
            return
        }
    } else {
        if currentRoutes, ok := self.transportRoutes[transport]; ok {
            for _, currentRoute := range currentRoutes {
                if slices.Index(routes, currentRoute) < 0 {
                    // no longer present
                    delete(self.routeStats, currentRoute)
                    delete(self.routeActive, currentRoute)
                    delete(self.routeWeight, currentRoute)
                }
            }
            for _, route := range routes {
                if slices.Index(currentRoutes, route) < 0 {
                    // new route
                    self.routeActive[route] = true
                }
            }
        } else {
            for _, route := range routes {
                // new route
                self.routeActive[route] = true
            }
        }
        // the following will be updated with the new routes in the weighting below
        // - routeStats
        // - routeActive
        // - routeWeights
        self.transportRoutes[transport] = routes
    }

    if self.weightedRoutes {
        self.updateRouteWeights()
    }

    self.transportUpdate.NotifyAll()


    // postTransportCount := len(self.transportRoutes)
    // postActiveRouteCount := len(activeRoutes())
        
    // fmt.Printf("updated transports=%d->%d routes=%d->%d\n", preTransportCount, postTransportCount, preActiveRouteCount, postActiveRouteCount)
}

func (self *MultiRouteSelector) updateRouteWeights() {
    updatedRouteWeight := map[Route]float32{}

    transportStats := map[Transport]*RouteStats{}
    for transport, currentRoutes := range self.transportRoutes {
        netStats := NewRouteStats()
        for _, currentRoute := range currentRoutes {
            if stats, ok := self.routeStats[currentRoute]; ok {
                netStats.sendCount += stats.sendCount
                netStats.sendByteCount += stats.sendByteCount
                netStats.receiveCount += stats.receiveCount
                netStats.receiveByteCount += stats.receiveByteCount
            }
        }
        transportStats[transport] = netStats
    }

    orderedTransports := maps.Keys(self.transportRoutes)
    // shuffle the same priority values
    mathrand.Shuffle(len(orderedTransports), func(i int, j int) {
        t := orderedTransports[i]
        orderedTransports[i] = orderedTransports[j]
        orderedTransports[j] = t
    })
    slices.SortStableFunc(orderedTransports, func(a Transport, b Transport)(int) {
        return a.Priority() - b.Priority()
    })

    n := len(orderedTransports)

    allCanEval := true
    for i := 0; i < n; i += 1 {
        transport := orderedTransports[i]
        routeStats := transportStats[transport]
        remainingStats := map[Transport]*RouteStats{}
        for j := i + 1; j < n; j += 1 {
            remainingStats[orderedTransports[j]] = transportStats[orderedTransports[j]]
        }
        canEval := transport.CanEvalRouteWeight(routeStats, remainingStats)
        allCanEval = allCanEval && canEval
    }

    if allCanEval {
        var allWeight float32
        allWeight = 1.0
        for i := 0; i < n; i += 1 {
            transport := orderedTransports[i]
            routeStats := transportStats[transport]
            remainingStats := map[Transport]*RouteStats{}
            for j := i + 1; j < n; j += 1 {
                remainingStats[orderedTransports[j]] = transportStats[orderedTransports[j]]
            }
            weight := transport.RouteWeight(routeStats, remainingStats)
            for _, route := range self.transportRoutes[transport] {
                updatedRouteWeight[route] = allWeight * weight
            }
            allWeight *= (1.0 - weight)
        }

        self.routeWeight = updatedRouteWeight

        updatedRouteStats := map[Route]*RouteStats{}
        for _, currentRoutes := range self.transportRoutes {
            for _, currentRoute := range currentRoutes {
                // reset the stats
                updatedRouteStats[currentRoute] = NewRouteStats()
            }
        }
        self.routeStats = updatedRouteStats
    }
}

func (self *MultiRouteSelector) GetActiveRoutes() []Route {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    activeRoutes := []Route{}
    for _, routes := range self.transportRoutes {
        for _, route := range routes {
            if self.routeActive[route] {
                activeRoutes = append(activeRoutes, route)
            }
        }
    }

    if self.weightedRoutes {
        // prioritize the routes (weighted shuffle)
        // if all weights are equal, this is the same as a shuffle
        WeightedShuffle(activeRoutes, self.routeWeight)
    } else {
        mathrand.Shuffle(len(activeRoutes), func(i int, j int) {
            activeRoutes[i], activeRoutes[j] = activeRoutes[j], activeRoutes[i]
        })
    }

    return activeRoutes
}

func (self *MultiRouteSelector) GetInactiveRoutes() []Route {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    inactiveRoutes := []Route{}
    for _, routes := range self.transportRoutes {
        for _, route := range routes {
            if !self.routeActive[route] {
                inactiveRoutes = append(inactiveRoutes, route)
            }
        }
    }

    return inactiveRoutes
}

func (self *MultiRouteSelector) setActive(route Route, active bool) {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    if _, ok := self.routeActive[route]; ok {
        self.routeActive[route] = false
    }
}

func (self *MultiRouteSelector) updateSendStats(route Route, sendCount int, sendByteCount ByteCount) {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    stats, ok := self.routeStats[route]
    if !ok {
        stats = NewRouteStats()
        self.routeStats[route] = stats
    }
    stats.sendCount += sendCount
    stats.sendByteCount += sendByteCount
}

func (self *MultiRouteSelector) updateReceiveStats(route Route, receiveCount int, receiveByteCount ByteCount) {
    self.mutex.Lock()
    defer self.mutex.Unlock()

    stats, ok := self.routeStats[route]
    if !ok {
        stats = NewRouteStats()
        self.routeStats[route] = stats
    }
    stats.receiveCount += receiveCount
    stats.receiveByteCount += receiveByteCount
}

// MultiRouteWriter
func (self *MultiRouteSelector) Write(ctx context.Context, transportFrameBytes []byte, timeout time.Duration) error {
    // write to the first channel available, in random priority
    enterTime := time.Now()
    for {
        notify := self.transportUpdate.NotifyChannel()
        activeRoutes := self.GetActiveRoutes()

        // non-blocking priority 
        for _, route := range activeRoutes {
            select {
            case route <- transportFrameBytes:
                self.updateSendStats(route, 1, ByteCount(len(transportFrameBytes)))
                return nil
            default:
            }
        }

        // select cases are in order:
        // - ctx.Done
        // - self.ctx.Done
        // - route writes...
        // - transport update
        // - timeout (may not exist)

        selectCases := make([]reflect.SelectCase, 0, 4 + len(activeRoutes))

        // add the context done case
        contextDoneIndex := len(selectCases)
        selectCases = append(selectCases, reflect.SelectCase{
            Dir: reflect.SelectRecv,
            Chan: reflect.ValueOf(ctx.Done()),
        })

        // add the done case
        doneIndex := len(selectCases)
        selectCases = append(selectCases, reflect.SelectCase{
            Dir: reflect.SelectRecv,
            Chan: reflect.ValueOf(self.ctx.Done()),
        })

        // add the update case
        transportUpdateIndex := len(selectCases)
        selectCases = append(selectCases, reflect.SelectCase{
            Dir: reflect.SelectRecv,
            Chan: reflect.ValueOf(notify),
        })

        // add all the route
        routeStartIndex := len(selectCases)
        if 0 < len(activeRoutes) {
            sendValue := reflect.ValueOf(transportFrameBytes)
            for _, route := range activeRoutes {
                selectCases = append(selectCases, reflect.SelectCase{
                    Dir: reflect.SelectSend,
                    Chan: reflect.ValueOf(route),
                    Send: sendValue,
                })
            }
        }

        timeoutIndex := len(selectCases)
        if 0 <= timeout {
            remainingTimeout := enterTime.Add(timeout).Sub(time.Now())
            if remainingTimeout <= 0 {
                // add a default case
                selectCases = append(selectCases, reflect.SelectCase{
                    Dir: reflect.SelectDefault,
                })
            } else {
                // add a timeout case
                selectCases = append(selectCases, reflect.SelectCase{
                    Dir: reflect.SelectRecv,
                    Chan: reflect.ValueOf(time.After(remainingTimeout)),
                })
            }
        }

        // fmt.Printf("write (->%s) select from %d routes (%d transports)\n", self.destinationId, timeoutIndex - routeStartIndex, len(self.transportRoutes))


        // note writing to a channel does not return an ok value
        // a := time.Now()
        chosenIndex, _, _ := reflect.Select(selectCases)
        // d := time.Now().Sub(a)
        // fmt.Printf("write (->%s) selected from %d routes (%.2fms)\n", self.destinationId, timeoutIndex - routeStartIndex, float64(d) / float64(time.Millisecond))

        switch chosenIndex {
        case contextDoneIndex:
            return errors.New("Context done")
        case doneIndex:
            return errors.New("Done")
        case transportUpdateIndex:
            // new routes, try again
        case timeoutIndex:
            return errors.New("Timeout")
        default:
            // a route
            routeIndex := chosenIndex - routeStartIndex
            route := activeRoutes[routeIndex]
            self.updateSendStats(route, 1, ByteCount(len(transportFrameBytes)))
            return nil
        }
    }
}

// MultiRouteReader
func (self *MultiRouteSelector) Read(ctx context.Context, timeout time.Duration) ([]byte, error) {
    // read from the first channel available, in random priority
    enterTime := time.Now()
    for {
        notify := self.transportUpdate.NotifyChannel()
        activeRoutes := self.GetActiveRoutes()

        // non-blocking priority
        retry := false
        for _, route := range activeRoutes {
            select {
            case transportFrameBytes, ok := <- route:
                if ok {
                    self.updateReceiveStats(route, 1, ByteCount(len(transportFrameBytes)))
                    return transportFrameBytes, nil
                } else {
                    // mark the route as closed, try again
                    self.setActive(route, false)
                    retry = true
                }
            default:
            }
        }
        if retry {
            continue
        }

        // select cases are in order:
        // - ctx.Done
        // - self.ctx.Done
        // - route reads...
        // - transport update
        // - timeout (may not exist)

        selectCases := make([]reflect.SelectCase, 0, 4 + len(activeRoutes))

        // add the context done case
        contextDoneIndex := len(selectCases)
        selectCases = append(selectCases, reflect.SelectCase{
            Dir: reflect.SelectRecv,
            Chan: reflect.ValueOf(self.ctx.Done()),
        })

        // add the done case
        doneIndex := len(selectCases)
        selectCases = append(selectCases, reflect.SelectCase{
            Dir: reflect.SelectRecv,
            Chan: reflect.ValueOf(self.ctx.Done()),
        })

        // add the update case
        transportUpdateIndex := len(selectCases)
        selectCases = append(selectCases, reflect.SelectCase{
            Dir: reflect.SelectRecv,
            Chan: reflect.ValueOf(notify),
        })

        // add all the route
        routeStartIndex := len(selectCases)
        if 0 < len(activeRoutes) {
            for _, route := range activeRoutes {
                selectCases = append(selectCases, reflect.SelectCase{
                    Dir: reflect.SelectRecv,
                    Chan: reflect.ValueOf(route),
                })
            }
        }

        timeoutIndex := len(selectCases)
        if 0 <= timeout {
            remainingTimeout := enterTime.Add(timeout).Sub(time.Now())
            if remainingTimeout <= 0 {
                // add a default case
                selectCases = append(selectCases, reflect.SelectCase{
                    Dir: reflect.SelectDefault,
                })
            } else {
                // add a timeout case
                selectCases = append(selectCases, reflect.SelectCase{
                    Dir: reflect.SelectRecv,
                    Chan: reflect.ValueOf(time.After(remainingTimeout)),
                })
            }
        }

        // a := time.Now()
        chosenIndex, value, ok := reflect.Select(selectCases)
        // d := time.Now().Sub(a)
        // fmt.Printf("read (->%s) selected from %d routes (%.2fms)\n", self.destinationId, timeoutIndex - routeStartIndex, float64(d) / float64(time.Millisecond))


        switch chosenIndex {
        case contextDoneIndex:
            return nil, errors.New("Context done")
        case doneIndex:
            return nil, errors.New("Done")
        case transportUpdateIndex:
            // new routes, try again
        case timeoutIndex:
            return nil, errors.New("Timeout")
        default:
            // a route
            routeIndex := chosenIndex - routeStartIndex
            route := activeRoutes[routeIndex]
            if ok {
                transportFrameBytes := value.Bytes()
                self.updateReceiveStats(route, 1, ByteCount(len(transportFrameBytes)))
                return transportFrameBytes, nil
            } else {
                // mark the route as closed, try again
                self.setActive(route, false)
            }
        }
    }
}

func (self *MultiRouteSelector) Close() {
    self.cancel()
}


type RouteStats struct {
    sendCount int
    sendByteCount ByteCount
    receiveCount int
    receiveByteCount ByteCount
}

func NewRouteStats() *RouteStats {
    return &RouteStats{
        sendCount: 0,
        sendByteCount: ByteCount(0),
        receiveCount: 0,
        receiveByteCount: ByteCount(0),
    }
}


// conforms to `Transport`
type sendGatewayTransport struct {
	transportId Id
}

func NewSendGatewayTransport() *sendGatewayTransport {
	return &sendGatewayTransport{
		transportId: NewId(),
	}
}

func (self *sendGatewayTransport) TransportId() Id {
	return self.transportId
}

func (self *sendGatewayTransport) Priority() int {
	return 100
}

func (self *sendGatewayTransport) CanEvalRouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) bool {
	return true
}

func (self *sendGatewayTransport) RouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) float32 {
	// uniform weight
	return 1.0 / float32(1 + len(remainingStats))
}

func (self *sendGatewayTransport) MatchesSend(destinationId Id) bool {
	return true
}

func (self *sendGatewayTransport) MatchesReceive(destinationId Id) bool {
	return false
}

func (self *sendGatewayTransport) Downgrade(sourceId Id) {
	// nothing to downgrade
}


// conforms to `Transport`
type receiveGatewayTransport struct {
	transportId Id
}

func NewReceiveGatewayTransport() *receiveGatewayTransport {
	return &receiveGatewayTransport{
		transportId: NewId(),
	}
}

func (self *receiveGatewayTransport) TransportId() Id {
	return self.transportId
}

func (self *receiveGatewayTransport) Priority() int {
	return 100
}

func (self *receiveGatewayTransport) CanEvalRouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) bool {
	return true
}

func (self *receiveGatewayTransport) RouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) float32 {
	// uniform weight
	return 1.0 / float32(1 + len(remainingStats))
}

func (self *receiveGatewayTransport) MatchesSend(destinationId Id) bool {
	return false
}

func (self *receiveGatewayTransport) MatchesReceive(destinationId Id) bool {
	return true
}

func (self *receiveGatewayTransport) Downgrade(sourceId Id) {
	// nothing to downgrade
}

