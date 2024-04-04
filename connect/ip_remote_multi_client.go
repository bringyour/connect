package connect

import (
    "context"
    "time"
    "sync"
    // "reflect"
    "errors"
    "fmt"
    "slices"
    "math"
    mathrand "math/rand"
    "strings"

    "golang.org/x/exp/maps"

    "github.com/golang/glog"

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


// for each `NewClientArgs`, 
//   `RemoveClientWithArgs` will be called if a client was created for the args,
//   else `RemoveClientArgs`
type MultiClientGenerator interface {
    // client id -> estimated byte count per second
    NextDestintationIds(count int, excludedClientIds []Id) (map[Id]ByteCount, error)
    // client id, client auth
    NewClientArgs() (*MultiClientGeneratorClientArgs, error)
    RemoveClientArgs(args *MultiClientGeneratorClientArgs)
    RemoveClientWithArgs(client *Client, args *MultiClientGeneratorClientArgs)
    NewClientSettings() *ClientSettings
    NewClient(ctx context.Context, args *MultiClientGeneratorClientArgs, clientSettings *ClientSettings) (*Client, error)
}


func DefaultMultiClientSettings() *MultiClientSettings {
    return &MultiClientSettings{
        WindowSizeMin: 4,
        WindowSizeMax: 32,
        // reconnects per source
        WindowSizeReconnectScale: 0.5,
        // ClientNackInitialLimit: 1,
        // ClientNackMaxLimit: 64 * 1024,
        // ClientNackScale: 4,
        // SendTimeout: 60 * time.Second,
        // WriteTimeout: 30 * time.Second,
        WriteRetryTimeout: 200 * time.Millisecond,
        AckTimeout: 30 * time.Second,
        WindowResizeTimeout: 1 * time.Second,
        StatsWindowGraceperiod: 5 * time.Second,
        StatsWindowEntropy: 0.25,
        WindowExpandTimeout: 2 * time.Second,
        WindowEnumerateEmptyTimeout: 1 * time.Second,
        WindowEnumerateErrorTimeout: 1 * time.Second,
        StatsWindowDuration: 120 * time.Second,
        StatsWindowBucketDuration: 10 * time.Second,
        StatsSampleWeightsCount: 8,
    }
}


type MultiClientSettings struct {
    WindowSizeMin int
    WindowSizeMax int
    // reconnects per source
    WindowSizeReconnectScale float64
    // ClientNackInitialLimit int
    // ClientNackMaxLimit int
    // ClientNackScale float64
    // ClientWriteTimeout time.Duration
    // SendTimeout time.Duration
    // WriteTimeout time.Duration
    WriteRetryTimeout time.Duration
    AckTimeout time.Duration
    WindowResizeTimeout time.Duration
    StatsWindowGraceperiod time.Duration
    StatsWindowEntropy float32
    WindowExpandTimeout time.Duration
    WindowEnumerateEmptyTimeout time.Duration
    WindowEnumerateErrorTimeout time.Duration
    StatsWindowDuration time.Duration
    StatsWindowBucketDuration time.Duration
    StatsSampleWeightsCount int
}


type RemoteUserNatMultiClient struct {
    ctx context.Context
    cancel context.CancelFunc

    generator MultiClientGenerator

    receivePacketCallback ReceivePacketFunction

    settings *MultiClientSettings

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
    generator MultiClientGenerator,
    receivePacketCallback ReceivePacketFunction,
) *RemoteUserNatMultiClient {
    return NewRemoteUserNatMultiClient(
        ctx,
        generator,
        receivePacketCallback,
        DefaultMultiClientSettings(),
    )
}

func NewRemoteUserNatMultiClient(
    ctx context.Context,
    generator MultiClientGenerator,
    receivePacketCallback ReceivePacketFunction,
    settings *MultiClientSettings,
) *RemoteUserNatMultiClient {
    cancelCtx, cancel := context.WithCancel(ctx)

    window := newMultiClientWindow(
        cancelCtx,
        cancel,
        generator,
        receivePacketCallback,
        settings,
    )

    return &RemoteUserNatMultiClient{
        ctx: cancelCtx,
        cancel: cancel,
        generator: generator,
        receivePacketCallback: receivePacketCallback,
        settings: settings,
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
            ip4Paths, ok := self.updateIp4Paths[update]
            if !ok {
                ip4Paths = map[Ip4Path]bool{}
                self.updateIp4Paths[update] = ip4Paths
            }
            ip4Paths[ip4Path] = true
            return update
        case 6:
            ip6Path := ipPath.ToIp6Path()
            update, ok := self.ip6PathUpdates[ip6Path]
            if !ok {
                update = &multiClientChannelUpdate{}
                self.ip6PathUpdates[ip6Path] = update
            }
            ip6Paths, ok := self.updateIp6Paths[update]
            if !ok {
                ip6Paths = map[Ip6Path]bool{}
                self.updateIp6Paths[update] = ip6Paths
            }
            ip6Paths[ip6Path] = true
            return update
        default:
            panic(fmt.Errorf("Bad protocol version %d", ipPath.Version))
        }
    }

    updatePaths := func(previousClient *multiClientChannel, update *multiClientChannelUpdate) {
        self.stateLock.Lock()
        defer self.stateLock.Unlock()


        client := update.client

        if previousClient != client {
            if previousClient != nil {
                delete(self.clientUpdates, previousClient)
            }
            if client != nil {
                self.clientUpdates[client] = update
            }
        }

        if client == nil {
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

    // the client state lock can be acquired from inside the update lock
    // ** important ** the update lock cannot be acquired from inside the client state lock
    for {
        // spin to acquire the correct update lock
        update := reserveUpdate()
        success := func()(bool) {
            update.lock.Lock()
            defer update.lock.Unlock()

            // update might have changed
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

// remove a client from all paths
// this acts as a drop. it does not lock the client update
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

// `SendPacketFunction`
func (self *RemoteUserNatMultiClient) SendPacket(source Path, provideMode protocol.ProvideMode, packet []byte, timeout time.Duration) bool {
    parsedPacket, err := newParsedPacket(packet)
    if err != nil {
        // bad packet
        glog.Infof("[multi]send bad packet = %s\n", err)
        return false
    }

    success := false
    self.updateClientPath(parsedPacket.ipPath, func(update *multiClientChannelUpdate) {
        enterTime := time.Now()

        if update.client != nil {
            var err error
            success, err = update.client.SendDetailed(parsedPacket, timeout)
            if err == nil {
                return
            }
            // find a new client
            update.client = nil
        }

        for {
            orderedClients, removedClients := self.window.OrderedClients()
            
            for _, client := range removedClients {
                glog.V(2).Infof("[multi]remove client %s->%s.\n", client.args.ClientId, client.args.DestinationId) 
                self.removeClient(client)
            }

            for _, client := range orderedClients {
                if client.Send(parsedPacket, 0) {
                    // lock the path to the client
                    update.client = client
                    success = true
                    return
                }
            }

            var retryTimeout time.Duration
            if 0 <= timeout {
                remainingTimeout := enterTime.Add(timeout).Sub(time.Now())

                if remainingTimeout <= 0 {
                    // drop
                    success = false
                    return
                }


                retryTimeout = min(remainingTimeout, self.settings.WriteRetryTimeout)
                
            } else {
                retryTimeout = self.settings.WriteRetryTimeout
            }

            if 0 < len(orderedClients) {
                // distribute the timeout evenly via wait
                retryTimeoutPerClient := retryTimeout / time.Duration(len(orderedClients))
                for _, client := range orderedClients {
                    if client.Send(parsedPacket, retryTimeoutPerClient) {
                        // lock the path to the client
                        update.client = client
                        success = true
                        return
                    }
                    select {
                    case <- self.ctx.Done():
                        // drop
                        success = false
                        return
                    default:
                    }
                }
            } else {
                select {
                case <- self.ctx.Done():
                    // drop
                    success = false
                    return
                case <- time.After(retryTimeout):
                }
            }
        }
    })
    return success
}

func (self *RemoteUserNatMultiClient) Close() {
    self.cancel()
}


type multiClientChannelUpdate struct {
    lock sync.Mutex
    client *multiClientChannel
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


type MultiClientGeneratorClientArgs struct {
    ClientId Id
    ClientAuth *ClientAuth
}


func DefaultApiMultiClientGeneratorSettings() *ApiMultiClientGeneratorSettings {
    return &ApiMultiClientGeneratorSettings{
        InitTimeout: 5 * time.Second,
    }
}


type ApiMultiClientGeneratorSettings struct {
    InitTimeout time.Duration
}


type ApiMultiClientGenerator struct {
    specs []*ProviderSpec
    apiUrl string
    byJwt string
    platformUrl string
    deviceDescription string
    deviceSpec string
    appVersion string
    clientSettingsGenerator func()(*ClientSettings)
    settings *ApiMultiClientGeneratorSettings

    api *BringYourApi
}

func NewApiMultiClientGeneratorWithDefaults(
    specs []*ProviderSpec,
    apiUrl string,
    byJwt string,
    platformUrl string,
    deviceDescription string,
    deviceSpec string,
    appVersion string,
) *ApiMultiClientGenerator {
    return NewApiMultiClientGenerator(
        specs,
        apiUrl,
        byJwt,
        platformUrl,
        deviceDescription,
        deviceSpec,
        appVersion,
        DefaultClientSettings,
        DefaultApiMultiClientGeneratorSettings(),
    )
}

func NewApiMultiClientGenerator(
    specs []*ProviderSpec,
    apiUrl string,
    byJwt string,
    platformUrl string,
    deviceDescription string,
    deviceSpec string,
    appVersion string,
    clientSettingsGenerator func()(*ClientSettings),
    settings *ApiMultiClientGeneratorSettings,
) *ApiMultiClientGenerator {
    api := NewBringYourApi(apiUrl)
    api.SetByJwt(byJwt)

    return &ApiMultiClientGenerator{
        specs: specs,
        apiUrl: apiUrl,
        byJwt: byJwt,
        platformUrl: platformUrl,
        deviceDescription: deviceDescription,
        deviceSpec: deviceSpec,
        appVersion: appVersion,
        clientSettingsGenerator: clientSettingsGenerator,
        settings: settings,
        api: api,
    }
}

func (self *ApiMultiClientGenerator) NextDestintationIds(count int, excludedClientIds []Id) (map[Id]ByteCount, error) {
    findProviders2 := &FindProviders2Args{
        Specs: self.specs,
        ExcludeClientIds: excludedClientIds,
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

func (self *ApiMultiClientGenerator) NewClientArgs() (*MultiClientGeneratorClientArgs, error) {
    auth := func() (string, error) {
        // note the derived client id will be inferred by the api jwt
        authNetworkClient := &AuthNetworkClientArgs{
            Description: self.deviceDescription,
            DeviceSpec: self.deviceSpec,
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
            AppVersion: self.appVersion,
        }
        return &MultiClientGeneratorClientArgs{
            ClientId: byJwt.ClientId,
            ClientAuth: clientAuth,
        }, nil
    } else {
        return nil, err
    }
}

func (self *ApiMultiClientGenerator) RemoveClientArgs(args *MultiClientGeneratorClientArgs) {
    removeNetworkClient := &RemoveNetworkClientArgs{
        ClientId: args.ClientId,
    }

    self.api.RemoveNetworkClient(removeNetworkClient, NewApiCallback(func(result *RemoveNetworkClientResult, err error) {
    }))
}

func (self *ApiMultiClientGenerator) RemoveClientWithArgs(client *Client, args *MultiClientGeneratorClientArgs) {
    self.RemoveClientArgs(args)
}

func (self *ApiMultiClientGenerator) NewClientSettings() *ClientSettings {
    return self.clientSettingsGenerator()
}

func (self *ApiMultiClientGenerator) NewClient(
    ctx context.Context,
    args *MultiClientGeneratorClientArgs,
    clientSettings *ClientSettings,
) (*Client, error) {
    byJwt, err := ParseByJwtUnverified(args.ClientAuth.ByJwt)
    if err != nil {
        return nil, err
    }
    clientOob := NewApiOutOfBandControl(ctx, args.ClientAuth.ByJwt, self.apiUrl)
    client := NewClient(ctx, byJwt.ClientId, clientOob, clientSettings)
    NewPlatformTransportWithDefaults(
        client.Ctx(),
        self.platformUrl,
        args.ClientAuth,
        client.RouteManager(),
    )
    // enable return traffic for this client
    ack := make(chan struct{})
    client.ContractManager().SetProvideModesWithReturnTrafficWithAckCallback(
        map[protocol.ProvideMode]bool{},
        func(err error) {
            close(ack)
        },
    )
    select {
    case <- ack:
    case <- time.After(self.settings.InitTimeout):
    }
    return client, nil
}


type multiClientWindow struct {
    ctx context.Context
    cancel context.CancelFunc

    generator MultiClientGenerator
    receivePacketCallback ReceivePacketFunction

    settings *MultiClientSettings

    clientChannelArgs chan *multiClientChannelArgs

    stateLock sync.Mutex
    destinationClients map[Id]*multiClientChannel
}

func newMultiClientWindow(
    ctx context.Context,
    cancel context.CancelFunc,
    generator MultiClientGenerator,
    receivePacketCallback ReceivePacketFunction,
    settings *MultiClientSettings,
) *multiClientWindow {
    window := &multiClientWindow{
        ctx: ctx,
        cancel: cancel,
        generator: generator,
        receivePacketCallback: receivePacketCallback,
        settings: settings,
        clientChannelArgs: make(chan *multiClientChannelArgs),
        destinationClients: map[Id]*multiClientChannel{},
    }

    go HandleError(window.randomEnumerateClientArgs, cancel)
    go HandleError(window.resize, cancel)

    return window
}

func (self *multiClientWindow) randomEnumerateClientArgs() {
    // continually reset the visited set when there are no more
    visitedDestinationIds := map[Id]bool{}
    for {
        destinationIdEstimatedBytesPerSecond := map[Id]ByteCount{}
        for {
            next := func(count int) (map[Id]ByteCount, error) {
                return self.generator.NextDestintationIds(
                    count,
                    maps.Keys(visitedDestinationIds),
                )
            }

            nextDestinationIdEstimatedBytesPerSecond, err := next(1)
            if err != nil {
                select {
                case <- self.ctx.Done():
                    return
                case <- time.After(self.settings.WindowEnumerateErrorTimeout):
                    glog.V(2).Infof("[multi]window enumerate error timeout.\n")
                }
            } else if 0 < len(nextDestinationIdEstimatedBytesPerSecond) {
                for destinationId, estimatedBytesPerSecond := range nextDestinationIdEstimatedBytesPerSecond {
                    destinationIdEstimatedBytesPerSecond[destinationId] = estimatedBytesPerSecond
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
                    glog.V(2).Infof("[multi]window enumerate empty timeout.\n")
                }
            }
        }

        // remove destinations that are already in the window
        func() {
            self.stateLock.Lock()
            defer self.stateLock.Unlock()
            for destinationId, _ := range self.destinationClients {
                delete(destinationIdEstimatedBytesPerSecond, destinationId)
            }
        }()
        
        for destinationId, estimatedBytesPerSecond := range destinationIdEstimatedBytesPerSecond {

            if clientArgs, err := self.generator.NewClientArgs(); err == nil {
                args := &multiClientChannelArgs{
                    DestinationId: destinationId,
                    EstimatedBytesPerSecond: estimatedBytesPerSecond,
                    MultiClientGeneratorClientArgs: *clientArgs,
                }
                select {
                case <- self.ctx.Done():
                    self.generator.RemoveClientArgs(clientArgs)
                    return
                case self.clientChannelArgs <- args:
                }
            } else {
                glog.V(2).Infof("[multi]could not auth client.\n")
            }
        
        }
    }
}

func (self *multiClientWindow) resize() {
    for {
        select {
        case <- self.ctx.Done():
            return
        case <- time.After(self.settings.WindowResizeTimeout):
        }

        clients := []*multiClientChannel{}

        netSourceCount := 0
        weights := map[*multiClientChannel]float32{}
        durations := map[*multiClientChannel]time.Duration{}

        for _, client := range self.clients() {
            if stats, err := client.WindowStats(); err == nil {
                clients = append(clients, client)
                netSourceCount = max(netSourceCount, stats.sourceCount)
                // byte count per second
                weights[client] = float32(stats.ByteCountPerSecond())
                durations[client] = stats.duration
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

        if len(clients) < targetWindowSize {
            // expand
            n := targetWindowSize - len(clients)
            glog.Infof("[multi]expand +%d ->%d\n", n, targetWindowSize)
            self.expand(n)
        } else {
            collapseLowestWeighted := func(windowSize int)(int) {
                // try to remove the lowest weighted clients to resize the window to `windowSize`
                // clients in the graceperiod or with activity cannot be removed

                self.stateLock.Lock()
                defer self.stateLock.Unlock()

                n := 0

                collapseClients := clients[windowSize:]
                clients = clients[:windowSize]
                for _, client := range collapseClients {
                    if self.settings.StatsWindowGraceperiod <= durations[client] && weights[client] <= 0 {
                        client.Cancel()
                        n += 1
                    } else {
                        clients = append(clients, client)
                    }
                }

                destinationClients := map[Id]*multiClientChannel{}
                for _, client := range clients {
                    destinationClients[client.DestinationId()] = client
                }
                self.destinationClients = destinationClients

                return n
            }

            if targetWindowSize < len(clients) {
                n := collapseLowestWeighted(targetWindowSize)
                if 0 < n {
                    glog.Infof("[multi]collapse -%d ->%d\n", n, len(clients))
                }
            } else if q3 := 3 * len(clients) / 4; q3 + 1 < len(clients) {
                // optimize by removing unused from q4
                n := collapseLowestWeighted(q3 + 1)
                if 0 < n {
                    glog.Infof("[multi]optimize -%d ->%d\n", n, len(clients))
                }
            }
        }
    }
}

func (self *multiClientWindow) expand(n int) {
    endTime := time.Now().Add(self.settings.WindowExpandTimeout)
    for i := 0; i < n; i += 1 {
        timeout := endTime.Sub(time.Now())
        if timeout < 0 {
            glog.V(2).Infof("[multi]expand window timeout\n")
            return
        }

        select {
        case <- self.ctx.Done():
            return
        // case <- update:
        //     // continue
        case args := <- self.clientChannelArgs:
            glog.V(2).Infof("[multi]expand new client\n")

            var ok bool
            func() {
                self.stateLock.Lock()
                defer self.stateLock.Unlock()
                _, ok = self.destinationClients[args.DestinationId]
            }()

            if ok {
                // already have a client in the window for this destination
                self.generator.RemoveClientArgs(&args.MultiClientGeneratorClientArgs)
            } else {
                client, err := newMultiClientChannel(
                    self.ctx,
                    args,
                    self.generator,
                    self.receivePacketCallback,
                    self.settings,
                )
                if err == nil {
                    func () {
                        self.stateLock.Lock()
                        defer self.stateLock.Unlock()
                        self.destinationClients[args.DestinationId] = client
                    }()
                } else {
                    self.generator.RemoveClientArgs(&args.MultiClientGeneratorClientArgs)
                }
            }
        case <- time.After(timeout):
            glog.V(2).Infof("[multi]expand window timeout waiting for args\n")
            return
        }
    }
}

func (self *multiClientWindow) clients() []*multiClientChannel {
    self.stateLock.Lock()
    defer self.stateLock.Unlock()
    return maps.Values(self.destinationClients)
}

func (self *multiClientWindow) OrderedClients() ([]*multiClientChannel, []*multiClientChannel) {

    clients := []*multiClientChannel{}

    removedClients := []*multiClientChannel{}
    weights := map[*multiClientChannel]float32{}
    durations := map[*multiClientChannel]time.Duration{}

    for _, client := range self.clients() {
        if stats, err := client.WindowStats(); err != nil {
            glog.Infof("[multi]remove client = %s\n", err)
            removedClients = append(removedClients, client)
        } else {
            clients = append(clients, client)
            weights[client] = float32(stats.ByteCountPerSecond())
            durations[client] = stats.duration
        }
    }

    if 0 < len(removedClients) {
        func() {
            self.stateLock.Lock()
            defer self.stateLock.Unlock()
            for _, client := range removedClients {
                client.Cancel()
                delete(self.destinationClients, client.DestinationId())
            }
        }()
    }

    // iterate and adjust weights for clients with weights >= 0
    nonNegativeClients := []*multiClientChannel{}
    for _, client := range clients {
        if weight := weights[client]; 0 <= weight {
            if duration := durations[client]; duration < self.settings.StatsWindowGraceperiod {
                // use the estimate
                weights[client] = float32(client.EstimatedByteCountPerSecond())
            } else if 0 == weight {
                // not used, use the estimate
                weights[client] = float32(client.EstimatedByteCountPerSecond())
            }
            nonNegativeClients = append(nonNegativeClients, client)
        }
    }

    if glog.V(1) {
        self.statsSampleWeights(weights)
    }

    WeightedShuffleWithEntropy(nonNegativeClients, weights, self.settings.StatsWindowEntropy)

    return nonNegativeClients, removedClients
}

func (self *multiClientWindow) statsSampleWeights(weights map[*multiClientChannel]float32) {
    // randonly sample log statistics for weights
    if mathrand.Intn(self.settings.StatsSampleWeightsCount) == 0 {
        // sample the weights
        weightValues := maps.Values(weights)
        slices.SortFunc(weightValues, func(a float32, b float32)(int) {
            // descending
            if a < b {
                return 1
            } else if b < a {
                return -1
            } else {
                return 0
            }
        })
        net := float32(0)
        for _, weight := range weightValues {
            net += weight
        }
        if 0 < net {
            var sb strings.Builder
            netThresh := float32(0.99)
            netp := float32(0)
            netCount := 0
            for i, weight := range weightValues {
                p := 100 * weight / net
                netp += p
                netCount += 1
                if 0 < i {
                    sb.WriteString(" ")
                }
                sb.WriteString(fmt.Sprintf("[%d]%.2f", i, p))
                if netThresh * 100 <= netp {
                    break
                }
            }

            glog.Infof("[multi]sample weights: %s (+%d more in window <%.0f%%)\n", sb.String(), len(weights) - netCount, 100 * (1 - netThresh))
        } else {
            glog.Infof("[multi]sample weights: zero (%d in window)\n", len(weights))
        }
    }
}


type multiClientChannelArgs struct {
    MultiClientGeneratorClientArgs

    DestinationId Id
    EstimatedBytesPerSecond ByteCount
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

    // internal
    bucketCount int
}

func (self *clientWindowStats) ByteCountPerSecond() ByteCount {
    seconds := float64(self.duration / time.Second)
    if seconds <= 0 {
        return ByteCount(0)
    }
    return ByteCount(float64(self.ackByteCount - self.nackByteCount) / seconds)
}


type multiClientChannel struct {
    ctx context.Context
    cancel context.CancelFunc

    args *multiClientChannelArgs

    api *BringYourApi

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

    clientReceiveUnsub func()
}

func newMultiClientChannel(
    ctx context.Context,
    args *multiClientChannelArgs,
    generator MultiClientGenerator,
    receivePacketCallback ReceivePacketFunction,
    settings *MultiClientSettings,
) (*multiClientChannel, error) {
    cancelCtx, cancel := context.WithCancel(ctx)

    clientSettings := generator.NewClientSettings()
    clientSettings.SendBufferSettings.AckTimeout = settings.AckTimeout
    
    client, err := generator.NewClient(
        cancelCtx,
        &args.MultiClientGeneratorClientArgs,
        clientSettings,
    )
    if err != nil {
        return nil, err
    }
    go HandleError(func() {
        select {
        case <- cancelCtx.Done():
        case <- client.Done():
        }
        client.Cancel()
        generator.RemoveClientWithArgs(client, &args.MultiClientGeneratorClientArgs)
    }, cancel)

    sourceFilter := map[Path]bool{
        Path{ClientId:args.DestinationId}: true,
    }

    clientChannel := &multiClientChannel{
        ctx: cancelCtx,
        cancel: cancel,
        args: args,
        receivePacketCallback: receivePacketCallback,
        settings: settings,
        sourceFilter: sourceFilter,
        client: client,
        eventBuckets: []*multiClientEventBucket{},
        ip4DestinationSourceCount: map[Ip4Path]map[Ip4Path]int{},
        ip6DestinationSourceCount: map[Ip6Path]map[Ip6Path]int{},
        packetStats: &clientWindowStats{},
        endErr: nil,
    }

    clientReceiveUnsub := client.AddReceiveCallback(clientChannel.clientReceive)
    clientChannel.clientReceiveUnsub = clientReceiveUnsub

    return clientChannel, nil
}

func (self *multiClientChannel) Send(parsedPacket *parsedPacket, timeout time.Duration) bool {
    success, err := self.SendDetailed(parsedPacket, timeout)
    return success && err == nil
}

func (self *multiClientChannel) SendDetailed(parsedPacket *parsedPacket, timeout time.Duration) (bool, error) {
    ipPacketToProvider := &protocol.IpPacketToProvider{
        IpPacket: &protocol.IpPacket{
            PacketBytes: parsedPacket.packet,
        },
    }
    if frame, err := ToFrame(ipPacketToProvider); err != nil {
        self.addError(err)
        return false, err
    } else {
        packetByteCount := ByteCount(len(parsedPacket.packet))
        self.addSendNack(packetByteCount)
        self.addSource(parsedPacket.ipPath)
        ackCallback := func(err error) {
            if err == nil {
                self.addSendAck(packetByteCount)
            } else {
                self.addError(err)
            }
        }

        opts := []any{}
        switch parsedPacket.ipPath.Protocol {
        case IpProtocolUdp:
            opts = append(opts, NoAck())
        }
        return self.client.SendWithTimeoutDetailed(
            frame,
            self.args.DestinationId,
            ackCallback,
            timeout,
            opts...,
        )
    }
}

func (self *multiClientChannel) EstimatedByteCountPerSecond() ByteCount {
    return self.args.EstimatedBytesPerSecond
}

func (self *multiClientChannel) Done() <-chan struct{} {
    return self.ctx.Done()
}

func (self *multiClientChannel) DestinationId() Id {
    return self.args.DestinationId
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

    self.coalesceEventBuckets()
}

func (self *multiClientChannel) addSendAck(ackByteCount ByteCount) {
    self.stateLock.Lock()
    defer self.stateLock.Unlock()

    self.packetStats.nackCount -= 1
    self.packetStats.nackByteCount -= ackByteCount
    self.packetStats.ackCount += 1
    self.packetStats.ackByteCount += ackByteCount

    eventBucket := self.eventBucket()
    eventBucket.ackCount += 1
    eventBucket.ackByteCount += ackByteCount

    self.coalesceEventBuckets()
}

func (self *multiClientChannel) addReceiveAck(ackByteCount ByteCount) {
    self.stateLock.Lock()
    defer self.stateLock.Unlock()

    self.packetStats.ackCount += 1
    self.packetStats.ackByteCount += ackByteCount

    eventBucket := self.eventBucket()
    eventBucket.ackCount += 1
    eventBucket.ackByteCount += ackByteCount

    self.coalesceEventBuckets()
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

    self.coalesceEventBuckets()
}

func (self *multiClientChannel) addError(err error) {
    self.stateLock.Lock()
    defer self.stateLock.Unlock()

    if self.endErr == nil {
        self.endErr = err
    }

    eventBucket := self.eventBucket()
    eventBucket.errs = append(eventBucket.errs, err)

    self.coalesceEventBuckets()
}

func (self *multiClientChannel) coalesceEventBuckets() {
    // must be called with `stateLock`

    // if there is no activity (no new buckets), keep historical buckets around
    minBucketCount := 1 + int(self.settings.StatsWindowDuration / self.settings.StatsWindowBucketDuration)

    windowStart := time.Now().Add(-self.settings.StatsWindowDuration)

    // remove events before the window start
    i := 0
    for i < len(self.eventBuckets) && minBucketCount < len(self.eventBuckets) {
        eventBucket := self.eventBuckets[i]
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
}

func (self *multiClientChannel) WindowStats() (*clientWindowStats, error) {
    return self.windowStatsWithCoalesce(true)
}

func (self *multiClientChannel) windowStatsWithCoalesce(coalesce bool) (*clientWindowStats, error) {
    self.stateLock.Lock()
    defer self.stateLock.Unlock()
    
    if coalesce {
        self.coalesceEventBuckets()
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

    stats := &clientWindowStats{
        sourceCount: maxSourceCount,
        ackCount: self.packetStats.ackCount,
        nackCount: self.packetStats.nackCount,
        ackByteCount: self.packetStats.ackByteCount,
        nackByteCount: self.packetStats.nackByteCount,
        duration: duration,
        bucketCount: len(self.eventBuckets),
    }
    err := self.endErr

    return stats, err
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
        glog.V(2).Infof("[multi]receive drop %d %s<-\n", len(frames), self.args.DestinationId)
        return
    }

    for _, frame := range frames {
        switch frame.MessageType {
        case protocol.MessageType_IpIpPacketFromProvider:
            if ipPacketFromProvider_, err := FromFrame(frame); err == nil {
                ipPacketFromProvider := ipPacketFromProvider_.(*protocol.IpPacketFromProvider)
                
                packet := ipPacketFromProvider.IpPacket.PacketBytes

                self.addReceiveAck(ByteCount(len(packet)))

                self.receivePacketCallback(source, IpProtocolUnknown, packet)
            } else {
                glog.V(2).Infof("[multi]receive drop %s<- = %s\n", self.args.DestinationId, err)
            }
        default:
            // unknown message, drop
        }
    }
}

func (self *multiClientChannel) Cancel() {
    self.addError(errors.New("Done."))
    self.cancel()
    self.client.Cancel()
}

func (self *multiClientChannel) Close() {
    self.addError(errors.New("Done."))
    self.cancel()
    self.client.Close()

    self.clientReceiveUnsub()
}


