package connect

import (
    "context"
    "time"
    "sync"
    "reflect"
    "errors"
    "fmt"
    "slices"
    "math"

    "golang.org/x/exp/maps"

    "bringyour.com/protocol"
)


// multi client is a sender approach to mitigate bad destinations
// it maintains a window of compatible clients chosen using specs
// (e.g. from a desription of the intent of use)
// - the clients are rate limited by the number of outstanding acks (nacks)
// - the size of allowed outstanding nacks increases with each ack,
// scaling up successful destinations to use the full transfer buffer
// - the clients are chosen with probability weighted by their 
// net frame count statistics (acks - nacks)


// FIXME on receive, only send packet if client is matched to the destination
// parse the dest on the packet. then make sure client equals dest


type multiClientChannelUpdate struct {
    lock sync.Mutex
    client *multiClientChannel
}


type MultiClientTesting interface {
    NextDesintationIds(count int, excludedClientIds []Id) map[Id]ByteCount
    NewClientArgs() (Id, *ClientAuth)
    SetTransports(client *Client)
}


type parsedPacket struct {
    packet []byte
    ipPath *IpPath
}

func newParsedPacket(packet []byte) (*parsedPacket, error) {
    ipPath, err := ParseIpPath(packet)
    if err != nil {
        return nil, err
    }
    return &parsedPacket{
        packet: packet,
        ipPath: ipPath,
    }, nil
}


func DefaultMultiClientSettings() *MultiClientSettings {
    return &MultiClientSettings{
        WindowSizeMin: 4,
        WindowSizeMax: 16,
        // reconnects per source
        WindowSizeReconnectScale: 1.0,
        // MultiWriteTimeoutExpandSize: 2,
        ClientNackInitialLimit: 1,
        ClientNackMaxLimit: 64 * 1024,
        ClientNackScale: 2,
        SendTimeout: 60 * time.Second,
        WriteTimeout: 30 * time.Second,
        // MultiWriteTimeout: 1 * time.Second,
        AckTimeout: 15 * time.Second,
        WindowExpandTimeout: 2 * time.Second,
        WindowEnumerateEmptyTimeout: 1 * time.Second,
        WindowEnumerateErrorTimeout: 1 * time.Second,
        StatsWindowDuration: 120 * time.Second,
        StatsWindowBucketDuration: 1 * time.Second,
    }
}


type MultiClientSettings struct {
    WindowSizeMin int
    WindowSizeMax int
    WindowSizeReconnectScale float64
    // MultiWriteTimeoutExpandSize int

    ClientNackInitialLimit int
    ClientNackMaxLimit int
    ClientNackScale float64
    ClientWriteTimeout time.Duration

    SendTimeout time.Duration
    WriteTimeout time.Duration
    // MultiWriteTimeout time.Duration
    AckTimeout time.Duration
    WindowExpandTimeout time.Duration
    WindowEnumerateEmptyTimeout time.Duration
    WindowEnumerateErrorTimeout time.Duration

    StatsWindowDuration time.Duration
    StatsWindowBucketDuration time.Duration
}


type RemoteUserNatMultiClient struct {
    ctx context.Context
    cancel context.CancelFunc

    api *BringYourApi
    platformUrl string

    specs []*ProviderSpec

    receivePacketCallback ReceivePacketFunction

    settings *MultiClientSettings
    testing MultiClientTesting

    window *multiClientWindow

    stateLock sync.Mutex
    ip4PathUpdates map[Ip4Path]*multiClientChannelUpdate
    ip6PathUpdates map[Ip6Path]*multiClientChannelUpdate
    updateIp4Paths map[*multiClientChannelUpdate]map[Ip4Path]bool
    updateIp6Paths map[*multiClientChannelUpdate]map[Ip6Path]bool
    clientUpdates map[*multiClientChannel]*multiClientChannelUpdate
}

func NewRemoteUserNatMultiClientWithDefaults(
    ctx context.Context,
    apiUrl string,
    byJwt string,
    platformUrl string,
    specs []*ProviderSpec,
    receivePacketCallback ReceivePacketFunction,
) *RemoteUserNatMultiClient {
    return NewRemoteUserNatMultiClient(
        ctx,
        apiUrl,
        byJwt,
        platformUrl,
        specs,
        receivePacketCallback,
        DefaultMultiClientSettings(),
    )
}

func NewRemoteUserNatMultiClient(
    ctx context.Context,
    apiUrl string,
    byJwt string,
    platformUrl string,
    specs []*ProviderSpec,
    receivePacketCallback ReceivePacketFunction,
    settings *MultiClientSettings,
) *RemoteUserNatMultiClient {
    return NewRemoteUserNatMultiClientWithTesting(
        ctx,
        apiUrl,
        byJwt,
        platformUrl,
        specs,
        receivePacketCallback,
        settings,
        nil,
    )
}

func NewRemoteUserNatMultiClientWithTesting(
    ctx context.Context,
    apiUrl string,
    byJwt string,
    platformUrl string,
    specs []*ProviderSpec,
    receivePacketCallback ReceivePacketFunction,
    settings *MultiClientSettings,
    testing MultiClientTesting,
) *RemoteUserNatMultiClient {
    cancelCtx, cancel := context.WithCancel(ctx)

    api := NewBringYourApi(apiUrl)
    api.SetByJwt(byJwt)

    window := newMultiClientWindow(
        cancelCtx,
        cancel,
        api,
        platformUrl,
        specs,
        receivePacketCallback,
        settings,
        testing,
    )

    return &RemoteUserNatMultiClient{
        ctx: cancelCtx,
        cancel: cancel,
        api: api,
        platformUrl: platformUrl,
        specs: specs,
        receivePacketCallback: receivePacketCallback,
        settings: settings,
        testing: testing,
        window: window,
        ip4PathUpdates: map[Ip4Path]*multiClientChannelUpdate{},
        ip6PathUpdates: map[Ip6Path]*multiClientChannelUpdate{},
        updateIp4Paths: map[*multiClientChannelUpdate]map[Ip4Path]bool{},
        updateIp6Paths: map[*multiClientChannelUpdate]map[Ip6Path]bool{},
        clientUpdates: map[*multiClientChannel]*multiClientChannelUpdate{},
    }
}


func (self *RemoteUserNatMultiClient) updateClientPath(ipPath *IpPath, callback func(*multiClientChannelUpdate)) {
    reserveUpdate := func()(*multiClientChannelUpdate) {
        self.stateLock.Lock()
        defer self.stateLock.Unlock()

        switch ipPath.Version {
        case 4:
            ip4Path := ipPath.ToIp4Path()
            update, ok := self.ip4PathUpdates[ip4Path]
            if !ok {
                update = &multiClientChannelUpdate{}
                self.ip4PathUpdates[ip4Path] = update
            }
            return update
        case 6:
            ip6Path := ipPath.ToIp6Path()
            update, ok := self.ip6PathUpdates[ip6Path]
            if !ok {
                update = &multiClientChannelUpdate{}
                self.ip6PathUpdates[ip6Path] = update
            }
            return update
        default:
            panic(fmt.Errorf("Bad protocol version %d", ipPath.Version))
        }
    }

    updatePaths := func(previousClient *multiClientChannel, update *multiClientChannelUpdate) {
        self.stateLock.Lock()
        defer self.stateLock.Unlock()

        if previousClient != update.client {
            if previousClient != nil {
                delete(self.clientUpdates, previousClient)
            }
            if update.client != nil {
                self.clientUpdates[update.client] = update
            }
        }


        client := update.client
        if client != nil {
            switch ipPath.Version {
            case 4:
                ip4Path := ipPath.ToIp4Path()
                self.ip4PathUpdates[ip4Path] = update
                ip4Paths, ok := self.updateIp4Paths[update]
                if !ok {
                    ip4Paths = map[Ip4Path]bool{}
                    self.updateIp4Paths[update] = ip4Paths
                }
                ip4Paths[ip4Path] = true
            case 6:
                ip6Path := ipPath.ToIp6Path()
                self.ip6PathUpdates[ip6Path] = update
                ip6Paths, ok := self.updateIp6Paths[update]
                if !ok {
                    ip6Paths = map[Ip6Path]bool{}
                    self.updateIp6Paths[update] = ip6Paths
                }
                ip6Paths[ip6Path] = true
            default:
                panic(fmt.Errorf("Bad protocol version %d", ipPath.Version))
            }
        } else {
            switch ipPath.Version {
            case 4:
                ip4Path := ipPath.ToIp4Path()
                delete(self.ip4PathUpdates, ip4Path)
                if ip4Paths, ok := self.updateIp4Paths[update]; ok {
                    delete(ip4Paths, ip4Path)
                    if len(ip4Paths) == 0 {
                        delete(self.updateIp4Paths, update)
                    }
                }
            case 6:
                ip6Path := ipPath.ToIp6Path()
                delete(self.ip6PathUpdates, ip6Path)
                if ip6Paths, ok := self.updateIp6Paths[update]; ok {
                    delete(ip6Paths, ip6Path)
                    if len(ip6Paths) == 0 {
                        delete(self.updateIp6Paths, update)
                    }
                }
            default:
                panic(fmt.Errorf("Bad protocol version %d", ipPath.Version))
            }
        }
    }

    for {
        update := reserveUpdate()
        success := func()(bool) {
            update.lock.Lock()
            defer update.lock.Unlock()

            if updateInLock := reserveUpdate(); update != updateInLock {
                return false
            }

            previousClient := update.client
            callback(update)
            updatePaths(previousClient, update)
            return true
        }()
        if success {
            return
        }
    }
}


/*
func (self *RemoteUserNatMultiClient) getPathClient(ipPath *IpPath) *multiClientChannel {
    self.stateLock.Lock()
    defer self.stateLock.Unlock()

    switch ipPath.Version {
    case 4:
        ip4Path := ipPath.ToIp4Path()
        return self.ip4PathClients[ip4Path]
    case 6:
        ip6Path := ipPath.ToIp6Path()
        return self.ip6PathClients[ip6Path]
    default:
        panic(fmt.Errorf("Bad protocol version %d", ipPath.Version))
    }
}

func (self *RemoteUserNatMultiClient) setPathClient(ipPath *IpPath, client *multiClientChannel) {
    self.stateLock.Lock()
    defer self.stateLock.Unlock()
    
    switch ipPath.Version {
    case 4:
        ip4Path := ipPath.ToIp4Path()
        self.ip4PathClients[ip4Path] = client
        ip4Paths, ok := self.clientIp4Paths[client]
        if !ok {
            ip4Paths = map[Ip4Path]bool{}
            self.clientIp4Paths[client] = ip4Paths
        }
        ip4Paths[ip4Path] = true
    case 6:
        ip6Path := ipPath.ToIp6Path()
        self.ip6PathClients[ip6Path] = client
        ip6Paths, ok := self.clientIp6Paths[client]
        if !ok {
            ip6Paths = map[Ip6Path]bool{}
            self.clientIp6Paths[client] = ip6Paths
        }
        ip6Paths[ip6Path] = true
    default:
        panic(fmt.Errorf("Bad protocol version %d", ipPath.Version))
    }
}

func (self *RemoteUserNatMultiClient) removePathClient(ipPath *IpPath, client *multiClientChannel) {
    self.stateLock.Lock()
    defer self.stateLock.Unlock()
    
    switch ipPath.Version {
    case 4:
        ip4Path := ipPath.ToIp4Path()
        delete(self.ip4PathClients, ip4Path)
        if ip4Paths, ok := self.clientIp4Paths[client]; ok {
            delete(ip4Paths, ip4Path)
            if len(ip4Paths) == 0 {
                delete(self.clientIp4Paths, client)
            }
        }
    case 6:
        ip6Path := ipPath.ToIp6Path()
        delete(self.ip6PathClients, ip6Path)
        if ip6Paths, ok := self.clientIp6Paths[client]; ok {
            delete(ip6Paths, ip6Path)
            if len(ip6Paths) == 0 {
                delete(self.clientIp6Paths, client)
            }
        }
    default:
        panic(fmt.Errorf("Bad protocol version %d", ipPath.Version))
    }
}
*/


// remove the client from paths
// no need to lock the clients

func (self *RemoteUserNatMultiClient) removeClient(client *multiClientChannel) {
    self.stateLock.Lock()
    defer self.stateLock.Unlock()

    if update, ok := self.clientUpdates[client]; ok {
        delete(self.clientUpdates, client)
    
        if ip4Paths, ok := self.updateIp4Paths[update]; ok {
            delete(self.updateIp4Paths, update)
            for ip4Path, _ := range ip4Paths {
                delete(self.ip4PathUpdates, ip4Path)
            }
        }

        if ip6Paths, ok := self.updateIp6Paths[update]; ok {
            delete(self.updateIp6Paths, update)
            for ip6Path, _ := range ip6Paths {
                delete(self.ip6PathUpdates, ip6Path)
            }
        }
    }
}


// func (self *RemoteUserNatMultiClient) SendPacket(source Path, provideMode protocol.ProvideMode, packet []byte) {
//     HandleError(func() {
//         self._SendPacket(source, provideMode, packet)
//     })
// }

// `SendPacketFunction`
func (self *RemoteUserNatMultiClient) SendPacket(source Path, provideMode protocol.ProvideMode, packet []byte) {
    parsedPacket, err := newParsedPacket(packet)
    if err != nil {
        fmt.Printf("[multi] Send bad packet (%s).\n", err)
        // bad packet
        return
    } 
    // else {
    //     fmt.Printf("[multi] Send packet\n")
    // }

    self.updateClientPath(parsedPacket.ipPath, func(update *multiClientChannelUpdate) {
        endTime := time.Now().Add(self.settings.SendTimeout)

        if update.client != nil {
            select {
            case <- self.ctx.Done():
                return
            case <- update.client.Done():
                // now we can change the routing of this path
                update.client = nil
            // the client was already selected so do not limit sending by the nack limit
            // at this point the limit is the send buffer
            case update.client.SendNoLimit() <- parsedPacket:
                return
            case <- time.After(self.settings.WriteTimeout):
                fmt.Printf("[multi] Existing path timeout %s->%s\n", update.client.args.clientId, update.client.args.destinationId)

                // now we can change the routing of this path
                // self.removePathClient(parsedPacket.ipPath, client)
                update.client = nil
            }
        }

        for {
            timeout := endTime.Sub(time.Now())

            orderedClients, removedClients, windowUpdate := self.window.OrderedClients(self.settings.WindowExpandTimeout)
            fmt.Printf("[multi] Window =%d -%d\n", len(orderedClients), len(removedClients))

            for _, client := range removedClients {
                fmt.Printf("[multi] Remove client %s->%s.\n", client.args.clientId, client.args.destinationId)
                
                self.removeClient(client)
            }

            for _, client := range orderedClients {
                select {
                case client.Send() <- parsedPacket:
                    // fmt.Printf("[multi] Set client %s->%s.\n", client.args.clientId, client.args.destinationId)

                    // lock the path to the client
                    // self.setPathClient(parsedPacket.ipPath, client)
                    update.client = client
                    return
                default:
                }
            }

            if timeout <= 0 {
                return
            }

            // select cases are in order:
            // - self.ctx.Done
            // - client writes...
            // - timeout

            selectCases := make([]reflect.SelectCase, 0, 2 + 2 * len(orderedClients))

            // add the done case
            doneIndex := len(selectCases)
            selectCases = append(selectCases, reflect.SelectCase{
                Dir: reflect.SelectRecv,
                Chan: reflect.ValueOf(self.ctx.Done()),
            })

            // add all the clients
            clientStartIndex := len(selectCases)
            if 0 < len(orderedClients) {
                sendValue := reflect.ValueOf(parsedPacket)
                for _, client := range orderedClients {
                    selectCases = append(selectCases, reflect.SelectCase{
                        Dir: reflect.SelectSend,
                        Chan: reflect.ValueOf(client.Send()),
                        Send: sendValue,
                    })
                }
            }
            clientEndIndex := len(selectCases)

            // add a timeout case
            timeoutIndex := len(selectCases)
            selectCases = append(selectCases, reflect.SelectCase{
                Dir: reflect.SelectRecv,
                Chan: reflect.ValueOf(time.After(timeout)),
            })

            // add a timeout case
            windowUpdateIndex := len(selectCases)
            selectCases = append(selectCases, reflect.SelectCase{
                Dir: reflect.SelectRecv,
                Chan: reflect.ValueOf(windowUpdate),
            })


            // add all the client dones
            clientDoneStartIndex := len(selectCases)
            if 0 < len(orderedClients) {
                for _, client := range orderedClients {
                    selectCases = append(selectCases, reflect.SelectCase{
                        Dir: reflect.SelectRecv,
                        Chan: reflect.ValueOf(client.Done()),
                    })
                }
            }
            clientDoneEndIndex := len(selectCases)




            chosenIndex, _, _ := reflect.Select(selectCases)

            if chosenIndex == doneIndex {
                // return errors.New("Done")
                return
            } else if chosenIndex == timeoutIndex {
                fmt.Printf("[multi] Timeout\n")

                // return errors.New("Timeout")
                // self.window.ExpandBy(self.settings.MultiWriteTimeoutExpandSize)
                // retry
            } else if chosenIndex == windowUpdateIndex {
                fmt.Printf("[multi] Update\n")
            } else if clientDoneStartIndex <= chosenIndex && chosenIndex < clientDoneEndIndex {
                fmt.Printf("[multi] Done\n")
                // retry
            } else if clientStartIndex <= chosenIndex && chosenIndex < clientEndIndex {
                // a route
                clientIndex := chosenIndex - clientStartIndex
                client := orderedClients[clientIndex]

                // fmt.Printf("[multi] Set client after select %s->%s.\n", client.args.clientId, client.args.destinationId)

                // lock the path to the client
                // self.setPathClient(parsedPacket.ipPath, client)
                update.client = client
                return
            }
        }
    })

}

// `connect.ReceiveFunction`
// func (self *RemoteUserNatMultiClient) ClientReceive(sourceId Id, frames []*protocol.Frame, provideMode protocol.ProvideMode) {
//     // the client channels have already filtered the messages for the actual destinations only

//     source := Path{ClientId: sourceId}

//     for _, frame := range frames {
//         switch frame.MessageType {
//         case protocol.MessageType_IpIpPacketFromProvider:
//             ipPacketFromProvider_, err := FromFrame(frame)
//             if err != nil {
//                 panic(err)
//             }
//             ipPacketFromProvider := ipPacketFromProvider_.(*protocol.IpPacketFromProvider)

//             fmt.Printf("remote user nat client r packet %s<-\n", source)

//             self.receivePacketCallback(source, IpProtocolUnknown, ipPacketFromProvider.IpPacket.PacketBytes)
//         }
//     }
// }

func (self *RemoteUserNatMultiClient) Close() {
    self.cancel()
}


type multiClientWindow struct {
    ctx context.Context
    cancel context.CancelFunc

    api *BringYourApi
    platformUrl string

    specs []*ProviderSpec
    receivePacketCallback ReceivePacketFunction

    settings *MultiClientSettings
    testing MultiClientTesting

    clientChannelArgs chan *multiClientChannelArgs

    stateLock sync.Mutex
    destinationClients map[Id]*multiClientChannel

    windowUpdate *Monitor
}

func newMultiClientWindow(
    ctx context.Context,
    cancel context.CancelFunc,
    api *BringYourApi,
    platformUrl string,
    specs []*ProviderSpec,
    receivePacketCallback ReceivePacketFunction,
    settings *MultiClientSettings,
    testing MultiClientTesting,
) *multiClientWindow {
    window := &multiClientWindow{
        ctx: ctx,
        cancel: cancel,
        api: api,
        platformUrl: platformUrl,
        specs: specs,
        receivePacketCallback: receivePacketCallback,
        settings: settings,
        testing: testing,
        clientChannelArgs: make(chan *multiClientChannelArgs),
        destinationClients: map[Id]*multiClientChannel{},
        windowUpdate: NewMonitor(),
    }

    go HandleError(window.randomEnumerateClientArgs, cancel)
    go HandleError(window.resize, cancel)

    return window
}

func (self *multiClientWindow) randomEnumerateClientArgs() {
    // continually reset the visited set when there are no more
    visitedDestinationIds := map[Id]bool{}
    for {

        destinationIds := map[Id]bool{}
        for {
            next := func(count int) (map[Id]ByteCount, error) {
                if self.testing != nil {
                    clientIdEstimatedBytesPerSecond := self.testing.NextDesintationIds(
                        count,
                        maps.Keys(visitedDestinationIds),
                    )
                    return clientIdEstimatedBytesPerSecond, nil
                } else {
                    findProviders2 := &FindProviders2Args{
                        Specs: self.specs,
                        ExcludeClientIds: maps.Keys(visitedDestinationIds),
                        Count: count,
                    }

                    result, err := self.api.FindProviders2Sync(findProviders2)
                    if err != nil {
                        return nil, err
                    }

                    clientIdEstimatedBytesPerSecond := map[Id]ByteCount{}
                    for _, provider := range result.Providers {
                        clientIdEstimatedBytesPerSecond[provider.ClientId] = provider.EstimatedBytesPerSecond
                    }

                    return clientIdEstimatedBytesPerSecond, nil
                }
            }

            nextDestinationIds, err := next(1)
            fmt.Printf("[multi] Window enumerate found %v (%v).\n", nextDestinationIds, err)
            if err != nil {
                select {
                case <- self.ctx.Done():
                    return
                case <- time.After(self.settings.WindowEnumerateErrorTimeout):
                    fmt.Printf("[multi] Window enumerate error timeout.\n")
                }
            } else if 0 < len(nextDestinationIds) {
                for destinationId, _ := range nextDestinationIds {
                    destinationIds[destinationId] = true
                    visitedDestinationIds[destinationId] = true
                }
                break
            } else {
                // reset
                visitedDestinationIds = map[Id]bool{}
                select {
                case <- self.ctx.Done():
                    return
                case <- time.After(self.settings.WindowEnumerateEmptyTimeout):
                    fmt.Printf("[multi] Window enumerate empty timeout.\n")
                }
            }
        }

        // remove destinations that are already in the window
        self.stateLock.Lock()
        for destinationId, _ := range self.destinationClients {
            delete(destinationIds, destinationId)
        }
        self.stateLock.Unlock()
        
        fmt.Printf("[multi] Window next destinations %d\n", len(destinationIds))
        
        for destinationId, _ := range destinationIds {
            if self.testing != nil {
                clientId, clientAuth := self.testing.NewClientArgs()
                args := &multiClientChannelArgs{
                    platformUrl: self.platformUrl,
                    destinationId: destinationId,
                    clientId: clientId,
                    clientAuth: clientAuth,
                }
                select {
                case <- self.ctx.Done():
                    return
                case self.clientChannelArgs <- args:
                }
            } else {
                auth := func() (string, error) {
                    // note the derived client id will be inferred by the api jwt
                    authNetworkClient := &AuthNetworkClientArgs{
                        Description: "multi client",
                        DeviceSpec: "multi client",
                    }

                    result, err := self.api.AuthNetworkClientSync(authNetworkClient)
                    if err != nil {
                        return "", err
                    }

                    if result.Error != nil {
                        return "", errors.New(result.Error.Message)
                    }

                    return result.ByClientJwt, nil
                }

                if byJwtStr, err := auth(); err == nil {
                    byJwt, err := ParseByJwtUnverified(byJwtStr)
                    if err != nil {
                        // in this case we cannot clean up the client because we don't know the client id
                        panic(err)
                    }

                    clientAuth := &ClientAuth{
                        ByJwt: byJwtStr,
                        InstanceId: NewId(),
                        AppVersion: "0.0.0-multi",
                    }
                    args := &multiClientChannelArgs{
                        platformUrl: self.platformUrl,
                        destinationId: destinationId,
                        clientId: byJwt.ClientId,
                        clientAuth: clientAuth,
                    }

                    select {
                    case <- self.ctx.Done():
                        removeClientAuth(args.clientId, self.api)
                        return
                    case self.clientChannelArgs <- args:
                    }
                } else {
                    fmt.Printf("[multi] Could not auth client.\n")
                }
            }
        }
    }
}

func (self *multiClientWindow) resize() {
    for {
        select {
        case <- self.ctx.Done():
            return
        default:
        }





        clients := []*multiClientChannel{}

        // removedClients := []*multiClientChannel{}
        netSourceCount := 0
        // nonNegativeClients := []*multiClientChannel{}
        weights := map[*multiClientChannel]float32{}
        durations := map[*multiClientChannel]time.Duration{}


        for _, client := range self.clients() {
            if stats, err := client.WindowStats(); err == nil {
                clients = append(clients, client)
                netSourceCount += stats.sourceCount
                // byte count per second
                weights[client] = stats.ByteCountPerSecond()
                durations[client] = stats.duration
                // if 0 <= weight {
                //     nonNegativeClients = append(nonNegativeClients, client)
                // }
            }
        }

        slices.SortFunc(clients, func(a *multiClientChannel, b *multiClientChannel)(int) {
            // descending weight
            aWeight := weights[a]
            bWeight := weights[b]
            if aWeight < bWeight {
                return 1
            } else if bWeight < aWeight {
                return -1
            } else {
                return 0
            }
        })






        targetWindowSize := min(
            self.settings.WindowSizeMax,
            int(math.Ceil(
                float64(self.settings.WindowSizeMin) + 
                float64(netSourceCount) * self.settings.WindowSizeReconnectScale,
            )),
        )
        fmt.Printf("[multi] Resize ->%d\n", targetWindowSize)

        if len(clients) < targetWindowSize {
            // expand

            n := targetWindowSize - len(clients)
            fmt.Printf("[multi] Expand +%d\n", n)
            
            self.expand(n)
        } else if targetWindowSize < len(clients) {
            // collapse the lowest weighted
            
            n := len(clients) - targetWindowSize
            fmt.Printf("[multi] Collapse -%d\n", n)

            for _, client := range clients[targetWindowSize:] {
                client.Cancel()
            }
            clients = clients[:targetWindowSize]
        } else {
            // optimize by removing the lowest quartile

            // q1 := len(clients) / 4
            // q2 := 2 * len(clients) / 4
            q3 := 3 * len(clients) / 4
            // collapse clients in the fourth quartile with duration of the half the stats window
            if q3 + 1 < len(clients) {
                q4Clients := clients[q3 + 1:]
                clients = clients[:q3 + 1]
                for _, client := range q4Clients {
                    if self.settings.StatsWindowDuration / 2 <= durations[client] {
                        client.Cancel()
                        // removedClients = append(removedClients, client)
                    } else {
                        clients = append(clients, client)
                    }
                }
            }
        }

        select {
        case <- time.After(1 * time.Second):
        }

    }
}

func (self *multiClientWindow) expand(n int) {
    endTime := time.Now().Add(self.settings.WindowExpandTimeout)
    for i := 0; i < n; i += 1 {
        timeout := endTime.Sub(time.Now())
        if timeout < 0 {
            fmt.Printf("[multi] Expand window timeout\n")
            return
        }

        select {
        case <- self.ctx.Done():
            return
        // case <- update:
        //     // continue
        case args := <- self.clientChannelArgs:
            fmt.Printf("[multi] Expand got args %v\n", args)

            self.stateLock.Lock()
            _, ok := self.destinationClients[args.destinationId]
            self.stateLock.Unlock()

            if ok {
                // already have a client in the window for this destination
                removeClientAuth(args.clientId, self.api)
            } else if client, err := newMultiClientChannel(
                    self.ctx,
                    args,
                    self.api,
                    self.receivePacketCallback,
                    self.settings,
                    self.testing,
            ); err == nil {
                go HandleError(func() {
                    defer removeClientAuth(args.clientId, self.api)
                    client.Run()
                    client.Cancel()
                }, self.cancel)

                self.stateLock.Lock()
                self.destinationClients[args.destinationId] = client
                self.stateLock.Unlock()
                
                self.windowUpdate.NotifyAll()
            } else {
                removeClientAuth(args.clientId, self.api)
            }
        case <- time.After(timeout):
            fmt.Printf("[multi] Expand window timeout waiting for args\n")
            return
        }
    }
}

func (self *multiClientWindow) clients() []*multiClientChannel {
    self.stateLock.Lock()
    defer self.stateLock.Unlock()
    return maps.Values(self.destinationClients)
}

func (self *multiClientWindow) OrderedClients(timeout time.Duration) ([]*multiClientChannel, []*multiClientChannel, chan struct{}) {

    update := self.windowUpdate.NotifyChannel()

    clients := []*multiClientChannel{}

    removedClients := []*multiClientChannel{}
    // netSourceCount := 0
    // nonNegativeClients := []*multiClientChannel{}
    weights := map[*multiClientChannel]float32{}
    // durations := map[*multiClientChannel]time.Duration{}

    for _, client := range self.clients() {
        if stats, err := client.WindowStats(); err != nil {
            removedClients = append(removedClients, client)
        } else {
            clients = append(clients, client)
            // netSourceCount += stats.sourceCount
            weight := stats.ByteCountPerSecond()
            weights[client] = weight
            // durations[client] = stats.duration
            // if 0 <= weight {
            //     nonNegativeClients = append(nonNegativeClients, client)
            // }
        }
    }

    if 0 < len(removedClients) {
        self.stateLock.Lock()
        for _, client := range removedClients {
            delete(self.destinationClients, client.DestinationId())
        }
        self.stateLock.Unlock()

        self.windowUpdate.NotifyAll()
    }

    for _, client := range clients {
        if weight := weights[client]; weight < 0 {
            weights[client] = 0
        } else if weight == 0 {
            // use the estimate
            weights[client] = client.EstimatedByteCountPerSecond()
        }
    }

    WeightedShuffle(clients, weights)

    return clients, removedClients, update
}


type multiClientChannelArgs struct {
    platformUrl string
    destinationId Id
    clientId Id
    clientAuth *ClientAuth
}

func removeClientAuth(clientId Id, api *BringYourApi) {
    removeNetworkClient := &RemoveNetworkClientArgs{
        ClientId: clientId,
    }

    api.RemoveNetworkClient(removeNetworkClient, NewApiCallback(func(result *RemoveNetworkClientResult, err error) {
    }))
}


type multiClientEventType int
const (
    multiClientEventTypeAck multiClientEventType = 1
    multiClientEventTypeNack multiClientEventType = 2
    multiClientEventTypeError multiClientEventType = 3
    multiClientEventTypeSource multiClientEventType = 4
)


type multiClientEventBucket struct {
    createTime time.Time
    eventTime time.Time

    ackCount int
    ackByteCount ByteCount
    nackCount int
    nackByteCount ByteCount
    errs []error
    ip4Paths map[Ip4Path]bool
    ip6Paths map[Ip6Path]bool
}

func newMultiClientEventBucket() *multiClientEventBucket {
    now := time.Now()
    return &multiClientEventBucket{
        createTime: now,
        eventTime: now,
    }
}

type clientWindowStats struct {
    sourceCount int
    ackCount int
    nackCount int
    ackByteCount ByteCount
    nackByteCount ByteCount
    duration time.Duration
}

func (self *clientWindowStats) ByteCountPerSecond() float32 {
    return float32(self.ackByteCount - self.nackByteCount) / float32(self.duration / time.Second)
}


type multiClientChannel struct {
    ctx context.Context
    cancel context.CancelFunc

    args *multiClientChannelArgs

    api *BringYourApi

    send chan *parsedPacket
    sendNoLimit chan *parsedPacket

    receivePacketCallback ReceivePacketFunction

    settings *MultiClientSettings

    sourceFilter map[Path]bool

    client *Client

    stateLock sync.Mutex
    eventBuckets []*multiClientEventBucket
    // destination -> source -> count
    ip4DestinationSourceCount map[Ip4Path]map[Ip4Path]int
    ip6DestinationSourceCount map[Ip6Path]map[Ip6Path]int
    packetStats *clientWindowStats
    endErr error

    maxNackCount int

    eventUpdate *Monitor
}

func newMultiClientChannel(
    ctx context.Context,
    args *multiClientChannelArgs,
    api *BringYourApi,
    receivePacketCallback ReceivePacketFunction,
    settings *MultiClientSettings,
    testing MultiClientTesting,
) (*multiClientChannel, error) {
    cancelCtx, cancel := context.WithCancel(ctx)

    byJwt, err := ParseByJwtUnverified(args.clientAuth.ByJwt)
    if err != nil {
        return nil, err
    }

    fmt.Printf("[multi] NEW CLIENT\n")

    clientSettings := DefaultClientSettings()
    clientSettings.SendBufferSettings.AckTimeout = settings.AckTimeout
    client := NewClient(cancelCtx, byJwt.ClientId, clientSettings)
    if testing != nil {
        testing.SetTransports(client)
    } else {
        // fmt.Printf("[multi] new platform transport %s %v\n", args.platformUrl, args.clientAuth)
        NewPlatformTransportWithDefaults(
            cancelCtx,
            args.platformUrl,
            args.clientAuth,
            client.RouteManager(),
        )
    }

    sourceFilter := map[Path]bool{
        Path{ClientId:args.destinationId}: true,
    }

    clientChannel := &multiClientChannel{
        ctx: cancelCtx,
        cancel: cancel,
        args: args,
        api: api,
        send: make(chan *parsedPacket),
        sendNoLimit: make(chan *parsedPacket),
        receivePacketCallback: receivePacketCallback,
        settings: settings,
        sourceFilter: sourceFilter,
        client: client,
        eventBuckets: []*multiClientEventBucket{},
        ip4DestinationSourceCount: map[Ip4Path]map[Ip4Path]int{},
        ip6DestinationSourceCount: map[Ip6Path]map[Ip6Path]int{},
        packetStats: &clientWindowStats{},
        endErr: nil,
        maxNackCount: settings.ClientNackInitialLimit,
        eventUpdate: NewMonitor(),
    }

    // go clientChannel.run()

    client.AddReceiveCallback(clientChannel.clientReceive)

    return clientChannel, nil
}

func (self *multiClientChannel) Run() {
    defer func() {
        self.cancel()
        self.addError(errors.New("Done."))
    }()

    send := func(parsedPacket *parsedPacket) {
        // fmt.Printf("[multi] Send ->%s\n", self.args.destinationId)
        ipPacketToProvider := &protocol.IpPacketToProvider{
            IpPacket: &protocol.IpPacket{
                PacketBytes: parsedPacket.packet,
            },
        }
        if frame, err := ToFrame(ipPacketToProvider); err != nil {
            self.addError(err)
        } else {
            packetByteCount := ByteCount(len(parsedPacket.packet))
            self.addSendNack(packetByteCount)
            self.addSource(parsedPacket.ipPath)
            ackCallback := func(err error) {
                // fmt.Printf("[multi] ack callback (%v)\n", err)
                if err == nil {
                    self.addSendAck(packetByteCount)
                } else {
                    self.addError(err)
                }
            }

            // fmt.Printf("[multi] Send ->%s\n", self.args.destinationId)

            opts := []any{}
            switch parsedPacket.ipPath.Protocol {
            case IpProtocolUdp:
                opts = append(opts, NoAck())
            }
            success := self.client.SendWithTimeout(
                frame,
                self.args.destinationId,
                ackCallback,
                self.settings.WriteTimeout,
                opts...,
            )
            if !success {
                fmt.Printf("[multi] Timeout ->%s\n", self.args.destinationId)

                self.addError(errors.New("Send timeout."))
            }
        }
    }

    for {
        update := self.eventUpdate.NotifyChannel()
        if self.isMaxAcks() {
            select {
            case <- self.ctx.Done():
                return
            case parsedPacket, ok := <- self.sendNoLimit:
                if !ok {
                    return
                }
                send(parsedPacket)
            case <- update:
            }
        } else {
            select {
            case <- self.ctx.Done():
                return
            case parsedPacket, ok := <- self.sendNoLimit:
                if !ok {
                    return
                }
                send(parsedPacket)
            case parsedPacket, ok := <- self.send:
                if !ok {
                    return
                }
                send(parsedPacket)
            case <- update:
            }
        }
    }
}

func (self *multiClientChannel) EstimatedByteCountPerSecond() float32 {
    // FIXME use the value from the args
    return 1.0
}

func (self *multiClientChannel) DestinationId() Id {
    return self.args.destinationId
}

func (self *multiClientChannel) Done() <-chan struct{} {
    return self.ctx.Done()
}

func (self *multiClientChannel) Send() chan *parsedPacket {
    return self.send
}

func (self *multiClientChannel) SendNoLimit() chan *parsedPacket {
    return self.sendNoLimit
}

func (self *multiClientChannel) isMaxAcks() bool {
    self.stateLock.Lock()
    defer self.stateLock.Unlock()

    return (self.maxNackCount <= self.packetStats.nackCount)
}

func (self *multiClientChannel) eventBucket() *multiClientEventBucket {
    // must be called with stateLock

    now := time.Now()

    var eventBucket *multiClientEventBucket
    if n := len(self.eventBuckets); 0 < n {
        eventBucket = self.eventBuckets[n - 1]
        if eventBucket.createTime.Add(self.settings.StatsWindowBucketDuration).Before(now) {
            // expired
            eventBucket = nil
        }
    }
    
    if eventBucket == nil {
        eventBucket = newMultiClientEventBucket()
        self.eventBuckets = append(self.eventBuckets, eventBucket)
    }

    eventBucket.eventTime = now

    return eventBucket
}

func (self *multiClientChannel) addSendNack(ackByteCount ByteCount) {
    self.stateLock.Lock()
    defer self.stateLock.Unlock()

    self.packetStats.nackCount += 1
    self.packetStats.nackByteCount += ackByteCount

    eventBucket := self.eventBucket()
    eventBucket.nackCount += 1
    eventBucket.nackByteCount += ackByteCount

    self.eventUpdate.NotifyAll()
}

func (self *multiClientChannel) addSendAck(ackByteCount ByteCount) {
    self.stateLock.Lock()
    defer self.stateLock.Unlock()

    self.packetStats.nackCount -= 1
    self.packetStats.nackByteCount -= ackByteCount
    self.packetStats.ackCount += 1
    self.packetStats.ackByteCount += ackByteCount

    if self.maxNackCount < self.settings.ClientNackMaxLimit {
        self.maxNackCount = min(
            self.settings.ClientNackMaxLimit,
            int(math.Ceil(float64(self.maxNackCount) * self.settings.ClientNackScale)),
        )
    }

    eventBucket := self.eventBucket()
    eventBucket.ackCount += 1
    eventBucket.ackByteCount += ackByteCount

    self.eventUpdate.NotifyAll()
}

func (self *multiClientChannel) addReceiveAck(ackByteCount ByteCount) {
    self.stateLock.Lock()
    defer self.stateLock.Unlock()

    self.packetStats.ackCount += 1
    self.packetStats.ackByteCount += ackByteCount

    eventBucket := self.eventBucket()
    eventBucket.ackCount += 1
    eventBucket.ackByteCount += ackByteCount

    self.eventUpdate.NotifyAll()
}

func (self *multiClientChannel) addSource(ipPath *IpPath) {
    self.stateLock.Lock()
    defer self.stateLock.Unlock()

    source := ipPath.Source()
    destination := ipPath.Destination()
    switch source.Version {
    case 4:
        sourceCount, ok := self.ip4DestinationSourceCount[destination.ToIp4Path()]
        if !ok {
            sourceCount = map[Ip4Path]int{}
            self.ip4DestinationSourceCount[destination.ToIp4Path()] = sourceCount
        }
        sourceCount[source.ToIp4Path()] += 1
    case 6:
        sourceCount, ok := self.ip6DestinationSourceCount[destination.ToIp6Path()]
        if !ok {
            sourceCount = map[Ip6Path]int{}
            self.ip6DestinationSourceCount[destination.ToIp6Path()] = sourceCount
        }
        sourceCount[source.ToIp6Path()] += 1
    default:
        panic(fmt.Errorf("Bad protocol version %d", source.Version))
    }

    eventBucket := self.eventBucket()
    switch ipPath.Version {
    case 4:
        if eventBucket.ip4Paths == nil {
            eventBucket.ip4Paths = map[Ip4Path]bool{}
        }
        eventBucket.ip4Paths[ipPath.ToIp4Path()] = true
    case 6:
        if eventBucket.ip6Paths == nil {
            eventBucket.ip6Paths = map[Ip6Path]bool{}
        }
        eventBucket.ip6Paths[ipPath.ToIp6Path()] = true
    default:
        panic(fmt.Errorf("Bad protocol version %d", source.Version))
    }

    self.eventUpdate.NotifyAll()
}

func (self *multiClientChannel) addError(err error) {
    self.stateLock.Lock()
    defer self.stateLock.Unlock()

    if self.endErr == nil {
        self.endErr = err
    }

    eventBucket := self.eventBucket()
    eventBucket.errs = append(eventBucket.errs, err)

    self.eventUpdate.NotifyAll()
}

func (self *multiClientChannel) WindowStats() (stats *clientWindowStats, returnErr error) {
    func() {
        self.stateLock.Lock()
        defer self.stateLock.Unlock()
        
        windowStart := time.Now().Add(-self.settings.StatsWindowDuration)

        // remove events before the window start
        i := 0
        for i < len(self.eventBuckets) {
            eventBucket := self.eventBuckets[i]
            // fmt.Printf("[multi] event advance %v %s\n", event, windowStart)
            if windowStart.Before(eventBucket.eventTime) {
                break
            }

            self.packetStats.ackCount -= eventBucket.ackCount
            self.packetStats.ackByteCount -= eventBucket.ackByteCount

            for ip4Path, _ := range eventBucket.ip4Paths {
                source := ip4Path.Source()
                destination := ip4Path.Destination()
                
                sourceCount, ok := self.ip4DestinationSourceCount[destination]
                if ok {
                    count := sourceCount[source]
                    if count - 1 <= 0 {
                        delete(sourceCount, source)
                    } else {
                        sourceCount[source] = count - 1
                    }
                    if len(sourceCount) == 0 {
                        delete(self.ip4DestinationSourceCount, destination)
                    }
                }
            }

            for ip6Path, _ := range eventBucket.ip6Paths {
                source := ip6Path.Source()
                destination := ip6Path.Destination()

                sourceCount, ok := self.ip6DestinationSourceCount[destination]
                if ok {
                    count := sourceCount[source]
                    if count - 1 <= 0 {
                        delete(sourceCount, source)
                    } else {
                        sourceCount[source] = count - 1
                    }
                    if len(sourceCount) == 0 {
                        delete(self.ip6DestinationSourceCount, destination)
                    }
                }
            }

            self.eventBuckets[i] = nil
            i += 1
        }
        if 0 < i {
            self.eventBuckets = self.eventBuckets[i:]
        }


        duration := time.Duration(0)
        if 0 < len(self.eventBuckets) {
            duration = time.Now().Sub(self.eventBuckets[0].createTime)
        }


        maxSourceCount := 0
        for _, sourceCounts := range self.ip4DestinationSourceCount {
            maxSourceCount = max(maxSourceCount, len(sourceCounts))
        }
        for _, sourceCounts := range self.ip6DestinationSourceCount {
            maxSourceCount = max(maxSourceCount, len(sourceCounts))
        }

        stats = &clientWindowStats{
            sourceCount: maxSourceCount,
            ackCount: self.packetStats.ackCount,
            nackCount: self.packetStats.nackCount,
            ackByteCount: self.packetStats.ackByteCount,
            nackByteCount: self.packetStats.nackByteCount,
            duration: duration,
        }
        returnErr = self.endErr
    }()

    self.eventUpdate.NotifyAll()
    return
}

// `connect.ReceiveFunction`
func (self *multiClientChannel) clientReceive(sourceId Id, frames []*protocol.Frame, provideMode protocol.ProvideMode) {
    select {
    case <- self.ctx.Done():
        return
    default:
    }

    source := Path{ClientId: sourceId}

    // only process frames from the destinations
    if allow := self.sourceFilter[source]; !allow {
        fmt.Printf("[multi] Receive drop %d %s<-\n", len(frames), self.args.destinationId)
        return
    }

    for _, frame := range frames {
        switch frame.MessageType {
        case protocol.MessageType_IpIpPacketFromProvider:
            ipPacketFromProvider_, err := FromFrame(frame)
            if err != nil {
                panic(err)
            }
            ipPacketFromProvider := ipPacketFromProvider_.(*protocol.IpPacketFromProvider)

            // fmt.Printf("[multi] Receive allow %s<-\n", self.args.destinationId)

            packet := ipPacketFromProvider.IpPacket.PacketBytes

            self.addReceiveAck(len(packet))

            self.receivePacketCallback(source, IpProtocolUnknown, packet)
        default:
            fmt.Printf("[multi] Receive drop 1 %s<-\n", self.args.destinationId)
        }
    }
}

func (self *multiClientChannel) Cancel() {
    self.cancel()
    self.client.Cancel()
}
