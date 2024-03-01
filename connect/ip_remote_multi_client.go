package connect

import (
    "context"
    "time"
    "sync"
    "reflect"
    "errors"
    "fmt"

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
        // reconnects per source
        WindowSizeReconnectScale: 1.0,
        MultiWriteTimeoutExpandSize: 2,
        ClientNackInitialLimit: 1,
        ClientNackMaxLimit: 8 * 1024,
        // linear addition on each ack
        ClientNackScale: 100,
        WriteTimeout: 5 * time.Second,
        MultiWriteTimeout: 1 * time.Second,
        WindowExpandTimeout: 1 * time.Second,
        WindowEnumerateEmptyTimeout: 1 * time.Second,
        WindowEnumerateErrorTimeout: 1 * time.Second,
        StatsWindowDuration: 300 * time.Second,
    }
}


type MultiClientSettings struct {
    WindowSizeMin int
    WindowSizeReconnectScale float32
    MultiWriteTimeoutExpandSize int

    ClientNackInitialLimit int
    ClientNackMaxLimit int
    ClientNackScale float32
    ClientWriteTimeout time.Duration

    WriteTimeout time.Duration
    MultiWriteTimeout time.Duration
    WindowExpandTimeout time.Duration
    WindowEnumerateEmptyTimeout time.Duration
    WindowEnumerateErrorTimeout time.Duration

    StatsWindowDuration time.Duration
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
    ip4PathClients map[Ip4Path]*multiClientChannel 
    ip6PathClients map[Ip6Path]*multiClientChannel
    clientIp4Paths map[*multiClientChannel]map[Ip4Path]bool
    clientIp6Paths map[*multiClientChannel]map[Ip6Path]bool
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
        ip4PathClients: map[Ip4Path]*multiClientChannel{},
        ip6PathClients: map[Ip6Path]*multiClientChannel{},
        clientIp4Paths: map[*multiClientChannel]map[Ip4Path]bool{},
        clientIp6Paths: map[*multiClientChannel]map[Ip6Path]bool{},
    }
}

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

func (self *RemoteUserNatMultiClient) removeClient(client *multiClientChannel) {
    self.stateLock.Lock()
    defer self.stateLock.Unlock()
    
    if ip4Paths, ok := self.clientIp4Paths[client]; ok {
        delete(self.clientIp4Paths, client)
        for ip4Path, _ := range ip4Paths {
            delete(self.ip4PathClients, ip4Path)
        }
    }

    if ip6Paths, ok := self.clientIp6Paths[client]; ok {
        delete(self.clientIp6Paths, client)
        for ip6Path, _ := range ip6Paths {
            delete(self.ip6PathClients, ip6Path)
        }
    }

    // client.Close()
}


func (self *RemoteUserNatMultiClient) SendPacket(source Path, provideMode protocol.ProvideMode, packet []byte) {
    HandleError(func() {
        self._SendPacket(source, provideMode, packet)
    })
}

// `SendPacketFunction`
func (self *RemoteUserNatMultiClient) _SendPacket(source Path, provideMode protocol.ProvideMode, packet []byte) {
    parsedPacket, err := newParsedPacket(packet)
    if err != nil {
        // bad packet
        return
    }

    if client := self.getPathClient(parsedPacket.ipPath); client != nil {
        select {
        case <- self.ctx.Done():
            return
        // the client was already selected so do not limit sending by the nack limit
        // at this point the limit is the send buffer
        case client.SendNoLimit() <- parsedPacket:
            return
        case <- time.After(self.settings.WriteTimeout):
            fmt.Printf("[multi] Existing path timeout %s->%s\n", client.args.clientId, client.args.destinationId)

            // now we can change the routing of this path
            self.removePathClient(parsedPacket.ipPath, client)
        }
    }

    for {
        orderedClients, removedClients := self.window.OrderedClients(self.settings.WindowExpandTimeout)
        fmt.Printf("[multi] Window =%d -%d\n", len(orderedClients), len(removedClients))

        for _, client := range removedClients {
            fmt.Printf("[multi] Remove client %s->%s.\n", client.args.clientId, client.args.destinationId)
            
            self.removeClient(client)
        }

        for _, client := range orderedClients {
            select {
            case client.Send() <- parsedPacket:
                fmt.Printf("[multi] Set client %s->%s.\n", client.args.clientId, client.args.destinationId)

                // lock the path to the client
                self.setPathClient(parsedPacket.ipPath, client)
                return
            default:
            }
        }

        // select cases are in order:
        // - self.ctx.Done
        // - client writes...
        // - timeout

        selectCases := make([]reflect.SelectCase, 0, 2 + len(orderedClients))

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

        // add a timeout case
        timeoutIndex := len(selectCases)
        selectCases = append(selectCases, reflect.SelectCase{
            Dir: reflect.SelectRecv,
            Chan: reflect.ValueOf(time.After(self.settings.MultiWriteTimeout)),
        })

        chosenIndex, _, _ := reflect.Select(selectCases)

        switch chosenIndex {
        case doneIndex:
            // return errors.New("Done")
            return
        case timeoutIndex:
            fmt.Printf("[multi] Timeout expand\n")

            // return errors.New("Timeout")
            self.window.ExpandBy(self.settings.MultiWriteTimeoutExpandSize)
            return
        default:
            // a route
            clientIndex := chosenIndex - clientStartIndex
            client := orderedClients[clientIndex]

            fmt.Printf("[multi] Set client after select %s->%s.\n", client.args.clientId, client.args.destinationId)

            // lock the path to the client
            self.setPathClient(parsedPacket.ipPath, client)
            return
        }
    }
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

    return window
}

func (self *multiClientWindow) randomEnumerateClientArgs() {
    // continually reset the visited set when there are no more
    visitedDestinationIds := map[Id]bool{}
    for {

        destinationIds := []Id{}
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
                    destinationIds = append(destinationIds, destinationId)
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

        fmt.Printf("[multi] Window next destinations %d\n", len(destinationIds))
        
        for _, destinationId := range destinationIds {
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

func (self *multiClientWindow) ExpandBy(expandStep int) {
    self.stateLock.Lock()
    windowSize := len(self.destinationClients)
    self.stateLock.Unlock()

    self.expandTo(windowSize + expandStep)
}

func (self *multiClientWindow) expandTo(targetWindowSize int) {
    endTime := time.Now().Add(self.settings.WindowExpandTimeout)
    for {
        update := self.windowUpdate.NotifyChannel()
        self.stateLock.Lock()
        windowSize := len(self.destinationClients)
        self.stateLock.Unlock()

        fmt.Printf("[multi] Expand %d->%d\n", windowSize, targetWindowSize)

        if targetWindowSize <= windowSize {
            fmt.Printf("[multi] Expand done\n")
            return
        }

        timeout := endTime.Sub(time.Now())
        if timeout < 0 {
            fmt.Printf("[multi] Expand window timeout\n")
            return
        }

        select {
        case <- self.ctx.Done():
            return
        case <- update:
            // continue
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

func (self *multiClientWindow) OrderedClients(timeout time.Duration) ([]*multiClientChannel, []*multiClientChannel) {
    self.stateLock.Lock()
    clients := maps.Values(self.destinationClients)
    self.stateLock.Unlock()

    removedClients := []*multiClientChannel{}

    netSourceCount := 0
    clientWeights := map[*multiClientChannel]float32{}
    for _, client := range clients {
        stats, err := client.WindowStats()
        if err != nil {
            removedClients = append(removedClients, client)
            self.stateLock.Lock()
            delete(self.destinationClients, client.DestinationId())
            self.stateLock.Unlock()
            self.windowUpdate.NotifyAll()
        } else {
            netSourceCount += stats.sourceCount
            clientWeights[client] = float32(stats.ackByteCount - stats.nackByteCount)
        }
    }

    targetWindowSize := int(
        float32(self.settings.WindowSizeMin) + 
        float32(netSourceCount) * self.settings.WindowSizeReconnectScale,
    )
    if len(clients) - len(removedClients) < targetWindowSize {
        self.expandTo(targetWindowSize)

        self.stateLock.Lock()
        clients = maps.Values(self.destinationClients)
        self.stateLock.Unlock()
    }

    // now order the clients based on the stats
    WeightedShuffle(clients, clientWeights)

    return clients, removedClients
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


type multiClientEvent struct {
    eventType multiClientEventType
    // ack bool
    ackByteCount ByteCount
    err error
    ipPath *IpPath
    eventTime time.Time
}


type clientWindowStats struct {
    sourceCount int
    ackCount int
    nackCount int
    ackByteCount ByteCount
    nackByteCount ByteCount
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
    // FIXME need to minimize memory here, use buckets
    // FIXME need to bucket the events
    events []*multiClientEvent
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

    clientSettings := DefaultClientSettings()
    clientSettings.SendBufferSettings.AckTimeout = settings.WriteTimeout
    client := NewClient(cancelCtx, byJwt.ClientId, clientSettings)
    if testing != nil {
        testing.SetTransports(client)
    } else {
        fmt.Printf("[multi] new platform transport %s %v\n", args.platformUrl, args.clientAuth)
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
        events: []*multiClientEvent{},
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
        ipPacketToProvider := &protocol.IpPacketToProvider{
            IpPacket: &protocol.IpPacket{
                PacketBytes: parsedPacket.packet,
            },
        }
        if frame, err := ToFrame(ipPacketToProvider); err != nil {
            self.addError(err)
        } else {
            packetByteCount := ByteCount(len(parsedPacket.packet))
            self.addNack(packetByteCount)
            self.addSource(parsedPacket.ipPath)
            ackCallback := func(err error) {
                fmt.Printf("[multi] ack callback (%v)\n", err)
                if err == nil {
                    self.addAck(packetByteCount)
                } else {
                    self.addError(err)
                }
            }

            fmt.Printf("[multi] Send ->%s\n", self.args.destinationId)

            success := self.client.SendWithTimeout(
                frame,
                self.args.destinationId,
                ackCallback,
                self.settings.WriteTimeout,
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
            }
        }
    }
}

func (self *multiClientChannel) DestinationId() Id {
    return self.args.destinationId
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

func (self *multiClientChannel) addNack(ackByteCount ByteCount) {
    self.stateLock.Lock()
    defer self.stateLock.Unlock()

    self.packetStats.nackCount += 1
    self.packetStats.nackByteCount += ackByteCount

    self.events = append(self.events, &multiClientEvent{
        eventType: multiClientEventTypeNack,
        ackByteCount: ackByteCount,
        eventTime: time.Now(),
    })

    self.eventUpdate.NotifyAll()
}

func (self *multiClientChannel) addAck(ackByteCount ByteCount) {
    self.stateLock.Lock()
    defer self.stateLock.Unlock()

    self.packetStats.nackCount -= 1
    self.packetStats.nackByteCount -= ackByteCount
    self.packetStats.ackCount += 1
    self.packetStats.ackByteCount += ackByteCount

    self.maxNackCount = min(
        self.settings.ClientNackMaxLimit,
        int(float32(self.maxNackCount) + self.settings.ClientNackScale),
    )

    self.events = append(self.events, &multiClientEvent{
        eventType: multiClientEventTypeAck,
        ackByteCount: ackByteCount,
        eventTime: time.Now(),
    })

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

    self.events = append(self.events, &multiClientEvent{
        eventType: multiClientEventTypeSource,
        ipPath: ipPath,
        eventTime: time.Now(),
    })

    self.eventUpdate.NotifyAll()
}

func (self *multiClientChannel) addError(err error) {
    self.stateLock.Lock()
    defer self.stateLock.Unlock()

    if self.endErr == nil {
        self.endErr = err
    }

    self.events = append(self.events, &multiClientEvent{
        eventType: multiClientEventTypeError,
        err: err,
        eventTime: time.Now(),
    })

    self.eventUpdate.NotifyAll()
}

func (self *multiClientChannel) WindowStats() (stats *clientWindowStats, returnErr error) {
    changed := false

    func() {
        self.stateLock.Lock()
        defer self.stateLock.Unlock()
        
        windowStart := time.Now().Add(-self.settings.StatsWindowDuration)

        // remove events before the window start
        i := 0
        for i < len(self.events) {
            event := self.events[i]
            // fmt.Printf("[multi] event advance %v %s\n", event, windowStart)
            if windowStart.Before(event.eventTime) {
                break
            }
            switch event.eventType {
            // `multiClientEventTypeNack` only ack or error can remove nack

            case multiClientEventTypeAck:
                self.packetStats.ackCount -= 1
                self.packetStats.ackByteCount -= event.ackByteCount
                changed = true

            case multiClientEventTypeSource:
                source := event.ipPath.Source()
                destination := event.ipPath.Destination()
                switch source.Version {
                case 4:
                    sourceCount, ok := self.ip4DestinationSourceCount[destination.ToIp4Path()]
                    if ok {
                        count := sourceCount[source.ToIp4Path()]
                        if count - 1 <= 0 {
                            delete(sourceCount, source.ToIp4Path())
                        } else {
                            sourceCount[source.ToIp4Path()] = count - 1
                        }
                        if len(sourceCount) == 0 {
                            delete(self.ip4DestinationSourceCount, destination.ToIp4Path())
                        }
                    }
                case 6:
                    sourceCount, ok := self.ip6DestinationSourceCount[destination.ToIp6Path()]
                    if ok {
                        count := sourceCount[source.ToIp6Path()]
                        if count - 1 <= 0 {
                            delete(sourceCount, source.ToIp6Path())
                        } else {
                            sourceCount[source.ToIp6Path()] = count - 1
                        }
                        if len(sourceCount) == 0 {
                            delete(self.ip6DestinationSourceCount, destination.ToIp6Path())
                        }
                    }
                default:
                    panic(fmt.Errorf("Bad protocol version %d", source.Version))
                }
                changed = true

            // `multiClientEventTypeError` nothing to remove, ended
            }
            self.events[i] = nil
            i += 1
        }
        self.events = self.events[i:]

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
        }
        returnErr = self.endErr
    }()

    if changed {
        self.eventUpdate.NotifyAll()
    }
    return
}

// `connect.ReceiveFunction`
func (self *multiClientChannel) clientReceive(sourceId Id, frames []*protocol.Frame, provideMode protocol.ProvideMode) {
    source := Path{ClientId: sourceId}

    // only process frames from the destinations
    if allow := self.sourceFilter[source]; !allow {
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

            fmt.Printf("[multi] Receive %s<-\n", self.args.destinationId)
            
            self.receivePacketCallback(source, IpProtocolUnknown, ipPacketFromProvider.IpPacket.PacketBytes)
        }
    }
}

/*
func (self *multiClientChannel) Close() {
    self.cancel()

    close(self.send)
    close(self.sendNoLimit)
}
*/
