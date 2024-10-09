package connect

import (
	"context"
	"sync"
	"time"
	// "reflect"
	"errors"
	"fmt"
	"math"
	mathrand "math/rand"
	"slices"
	"strings"

	"golang.org/x/exp/maps"

	"google.golang.org/protobuf/proto"

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

// TODO surface window stats to show to users

// for each `NewClientArgs`,
//
//	`RemoveClientWithArgs` will be called if a client was created for the args,
//	else `RemoveClientArgs`
type MultiClientGenerator interface {
	// path -> estimated byte count per second
	// the enumeration should typically
	// 1. not repeat final destination ids from any path
	// 2. not repeat intermediary elements from any path
	NextDestinations(count int, excludeDestinations []MultiHopId) (map[MultiHopId]ByteCount, error)
	// client id, client auth
	NewClientArgs() (*MultiClientGeneratorClientArgs, error)
	RemoveClientArgs(args *MultiClientGeneratorClientArgs)
	RemoveClientWithArgs(client *Client, args *MultiClientGeneratorClientArgs)
	NewClientSettings() *ClientSettings
	NewClient(ctx context.Context, args *MultiClientGeneratorClientArgs, clientSettings *ClientSettings) (*Client, error)
}

func DefaultMultiClientSettings() *MultiClientSettings {
	return &MultiClientSettings{
		WindowSizeMin: 2,
		// TODO increase this when p2p is deployed
		WindowSizeMinP2pOnly: 0,
		WindowSizeMax:        8,
		// reconnects per source
		WindowSizeReconnectScale: 1.0,
		WriteRetryTimeout:        200 * time.Millisecond,
		// this includes the time to establish the transport
		PingWriteTimeout: 5 * time.Second,
		PingTimeout:      10 * time.Second,
		// a lower ack timeout helps cycle through bad providers faster
		AckTimeout:             15 * time.Second,
		BlackholeTimeout:       15 * time.Second,
		WindowResizeTimeout:    5 * time.Second,
		StatsWindowGraceperiod: 5 * time.Second,
		StatsWindowEntropy:     0.25,
		WindowExpandTimeout:    15 * time.Second,
		// wait this time before enumerating potential clients again
		WindowEnumerateEmptyTimeout:  60 * time.Second,
		WindowEnumerateErrorTimeout:  1 * time.Second,
		WindowExpandScale:            2.0,
		WindowCollapseScale:          0.5,
		WindowExpandMaxOvershotScale: 4.0,
		StatsWindowDuration:          120 * time.Second,
		StatsWindowBucketDuration:    10 * time.Second,
		StatsSampleWeightsCount:      8,
		StatsSourceCountSelection:    0.95,
		ClientAffinityTimeout:        0 * time.Second,

		RemoteUserNatMultiClientMonitorSettings: *DefaultRemoteUserNatMultiClientMonitorSettings(),
	}
}

type MultiClientSettings struct {
	WindowSizeMin int
	// the minimumum number of items in the windows that must be connected via p2p only
	WindowSizeMinP2pOnly int
	WindowSizeMax        int
	// reconnects per source
	WindowSizeReconnectScale float64
	// ClientNackInitialLimit int
	// ClientNackMaxLimit int
	// ClientNackScale float64
	// ClientWriteTimeout time.Duration
	// SendTimeout time.Duration
	// WriteTimeout time.Duration
	WriteRetryTimeout            time.Duration
	PingWriteTimeout             time.Duration
	PingTimeout                  time.Duration
	AckTimeout                   time.Duration
	BlackholeTimeout             time.Duration
	WindowResizeTimeout          time.Duration
	StatsWindowGraceperiod       time.Duration
	StatsWindowEntropy           float32
	WindowExpandTimeout          time.Duration
	WindowEnumerateEmptyTimeout  time.Duration
	WindowEnumerateErrorTimeout  time.Duration
	WindowExpandScale            float64
	WindowCollapseScale          float64
	WindowExpandMaxOvershotScale float64
	StatsWindowDuration          time.Duration
	StatsWindowBucketDuration    time.Duration
	StatsSampleWeightsCount      int
	StatsSourceCountSelection    float64
	// lower affinity is more private
	// however, there may be some applications that assume the same ip across multiple connections
	// in those cases, we would need some small affinity
	ClientAffinityTimeout time.Duration

	RemoteUserNatMultiClientMonitorSettings
}

type RemoteUserNatMultiClient struct {
	ctx    context.Context
	cancel context.CancelFunc

	generator MultiClientGenerator

	receivePacketCallback ReceivePacketFunction

	settings *MultiClientSettings

	window *multiClientWindow

	securityPolicy *SecurityPolicy
	// the provide mode of the source packets
	// for locally generated packets this is `ProvideMode_Network`
	provideMode protocol.ProvideMode

	stateLock      sync.Mutex
	ip4PathUpdates map[Ip4Path]*multiClientChannelUpdate
	ip6PathUpdates map[Ip6Path]*multiClientChannelUpdate
	updateIp4Paths map[*multiClientChannelUpdate]map[Ip4Path]bool
	updateIp6Paths map[*multiClientChannelUpdate]map[Ip6Path]bool
	clientUpdates  map[*multiClientChannel]*multiClientChannelUpdate
}

func NewRemoteUserNatMultiClientWithDefaults(
	ctx context.Context,
	generator MultiClientGenerator,
	receivePacketCallback ReceivePacketFunction,
	provideMode protocol.ProvideMode,
) *RemoteUserNatMultiClient {
	return NewRemoteUserNatMultiClient(
		ctx,
		generator,
		receivePacketCallback,
		provideMode,
		DefaultMultiClientSettings(),
	)
}

func NewRemoteUserNatMultiClient(
	ctx context.Context,
	generator MultiClientGenerator,
	receivePacketCallback ReceivePacketFunction,
	provideMode protocol.ProvideMode,
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
		ctx:                   cancelCtx,
		cancel:                cancel,
		generator:             generator,
		receivePacketCallback: receivePacketCallback,
		settings:              settings,
		window:                window,
		securityPolicy:        DefaultSecurityPolicy(),
		provideMode:           provideMode,
		ip4PathUpdates:        map[Ip4Path]*multiClientChannelUpdate{},
		ip6PathUpdates:        map[Ip6Path]*multiClientChannelUpdate{},
		updateIp4Paths:        map[*multiClientChannelUpdate]map[Ip4Path]bool{},
		updateIp6Paths:        map[*multiClientChannelUpdate]map[Ip6Path]bool{},
		clientUpdates:         map[*multiClientChannel]*multiClientChannelUpdate{},
	}
}

func (self *RemoteUserNatMultiClient) Monitor() *RemoteUserNatMultiClientMonitor {
	return self.window.monitor
}

func (self *RemoteUserNatMultiClient) updateClientPath(ipPath *IpPath, callback func(*multiClientChannelUpdate)) {
	reserveUpdate := func() *multiClientChannelUpdate {
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
		success := func() bool {
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
/*
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
*/

// `SendPacketFunction`
func (self *RemoteUserNatMultiClient) SendPacket(
	source TransferPath,
	provideMode protocol.ProvideMode,
	packet []byte,
	timeout time.Duration,
) bool {
	minRelationship := max(provideMode, self.provideMode)

	ipPath, r, err := self.securityPolicy.Inspect(minRelationship, packet)
	if err != nil {
		glog.Infof("[multi]send bad packet = %s\n", err)
		return false
	}
	switch r {
	case SecurityPolicyResultAllow:
		parsedPacket := &parsedPacket{
			packet: packet,
			ipPath: ipPath,
		}
		return self.sendPacket(source, provideMode, parsedPacket, timeout)
	default:
		// TODO upgrade port 53 and port 80 here with protocol specific conversions
		glog.Infof("[multi]drop packet ipv%d p%v -> %s:%d\n", ipPath.Version, ipPath.Protocol, ipPath.DestinationIp, ipPath.DestinationPort)
		return false
	}
}

func (self *RemoteUserNatMultiClient) sendPacket(
	source TransferPath,
	provideMode protocol.ProvideMode,
	parsedPacket *parsedPacket,
	timeout time.Duration,
) (success bool) {
	self.updateClientPath(parsedPacket.ipPath, func(update *multiClientChannelUpdate) {
		enterTime := time.Now()

		if update.client != nil {
			var err error
			success, err = update.client.SendDetailed(parsedPacket, timeout)
			if err == nil {
				return
			}
			glog.Infof("[multi]send error = %s\n", err)
			// find a new client
			update.client.ClearAffinity()
			update.client = nil
		}

		defer func() {
			if update.client != nil {
				update.client.UpdateAffinity()
			}
		}()

		for {
			orderedClients := self.window.OrderedClients()

			// for _, client := range removedClients {
			// 	glog.V(2).Infof("[multi]remove client %s->%s.\n", client.args.ClientId, client.args.Destination)
			// 	self.removeClient(client)
			// }

			// in order, try
			// - the temporal affinity
			//   connections initiated near each other in time will use the same client
			// - clients in order

			if 0 < self.settings.ClientAffinityTimeout {
				// this implementation is a best-effort at affinity
				// it will not ensure affinity between parallel sends in all cases
				var mostRecentAffinityClient *multiClientChannel
				for _, client := range orderedClients {
					affinityCount, affinityTime := client.MostRecentAffinity()
					if 0 < affinityCount && enterTime.Sub(affinityTime) < self.settings.ClientAffinityTimeout {
						if mostRecentAffinityClient == nil {
							mostRecentAffinityClient = client
						} else if _, mostRecentAffinityTime := mostRecentAffinityClient.MostRecentAffinity(); mostRecentAffinityTime.Before(affinityTime) {
							mostRecentAffinityClient = client
						}
					}
				}
				if mostRecentAffinityClient != nil {
					affinityTimeout := min(self.settings.WriteRetryTimeout, self.settings.ClientAffinityTimeout)
					if 0 <= timeout {
						remainingTimeout := enterTime.Add(timeout).Sub(time.Now())

						if remainingTimeout <= 0 {
							// drop
							success = false
							return
						}

						affinityTimeout = min(affinityTimeout, remainingTimeout)
					}
					if mostRecentAffinityClient.Send(parsedPacket, affinityTimeout) {
						glog.V(2).Infof("[multi]use affinity client ipv%d p%v -> %s:%d\n", parsedPacket.ipPath.Version, parsedPacket.ipPath.Protocol, parsedPacket.ipPath.DestinationIp, parsedPacket.ipPath.DestinationPort)
						// lock the path to the client
						update.client = mostRecentAffinityClient
						success = true
						return
					}
					// else choose a new client
				}
				// else choose a new client
			}

			for _, client := range orderedClients {
				if client.Send(parsedPacket, 0) {
					glog.V(2).Infof("[multi]use new client ipv%d p%v -> %s:%d\n", parsedPacket.ipPath.Version, parsedPacket.ipPath.Protocol, parsedPacket.ipPath.DestinationIp, parsedPacket.ipPath.DestinationPort)
					// lock the path to the client
					update.client = client
					success = true
					return
				}
			}

			retryTimeout := self.settings.WriteRetryTimeout
			if 0 <= timeout {
				remainingTimeout := enterTime.Add(timeout).Sub(time.Now())

				if remainingTimeout <= 0 {
					// drop
					success = false
					return
				}

				retryTimeout = min(remainingTimeout, retryTimeout)
			}

			if 0 < len(orderedClients) {
				// distribute the timeout evenly via wait
				retryTimeoutPerClient := retryTimeout / time.Duration(len(orderedClients))
				for _, client := range orderedClients {
					var err error
					success, err = client.SendDetailed(parsedPacket, retryTimeoutPerClient)
					if success && err == nil {
						// lock the path to the client
						glog.V(2).Infof("[multi]wait for new client ipv%d p%v -> %s:%d\n", parsedPacket.ipPath.Version, parsedPacket.ipPath.Protocol, parsedPacket.ipPath.DestinationIp, parsedPacket.ipPath.DestinationPort)
						update.client = client
						success = true
						return
					} else if err != nil {
						glog.Infof("[multi]send error = %s\n", err)
					}
					select {
					case <-self.ctx.Done():
						// drop
						success = false
						return
					default:
					}
				}
			} else {
				select {
				case <-self.ctx.Done():
					// drop
					success = false
					return
				case <-time.After(retryTimeout):
				}
			}
		}
	})
	return
}

func (self *RemoteUserNatMultiClient) Shuffle() {
	self.window.shuffle()
}

func (self *RemoteUserNatMultiClient) Close() {
	self.cancel()
}

type multiClientChannelUpdate struct {
	lock   sync.Mutex
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
	ClientId   Id
	ClientAuth *ClientAuth
	P2pOnly    bool
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
	specs          []*ProviderSpec
	clientStrategy *ClientStrategy

	excludeClientIds []Id

	apiUrl      string
	byJwt       string
	platformUrl string

	deviceDescription       string
	deviceSpec              string
	appVersion              string
	clientSettingsGenerator func() *ClientSettings
	settings                *ApiMultiClientGeneratorSettings

	api *BringYourApi
}

func NewApiMultiClientGeneratorWithDefaults(
	ctx context.Context,
	specs []*ProviderSpec,
	clientStrategy *ClientStrategy,
	excludeClientIds []Id,
	apiUrl string,
	byJwt string,
	platformUrl string,
	deviceDescription string,
	deviceSpec string,
	appVersion string,
) *ApiMultiClientGenerator {
	return NewApiMultiClientGenerator(
		ctx,
		specs,
		clientStrategy,
		excludeClientIds,
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
	ctx context.Context,
	specs []*ProviderSpec,
	clientStrategy *ClientStrategy,
	excludeClientIds []Id,
	apiUrl string,
	byJwt string,
	platformUrl string,
	deviceDescription string,
	deviceSpec string,
	appVersion string,
	clientSettingsGenerator func() *ClientSettings,
	settings *ApiMultiClientGeneratorSettings,
) *ApiMultiClientGenerator {
	api := NewBringYourApi(ctx, clientStrategy, apiUrl)
	api.SetByJwt(byJwt)

	return &ApiMultiClientGenerator{
		specs:                   specs,
		clientStrategy:          clientStrategy,
		excludeClientIds:        excludeClientIds,
		apiUrl:                  apiUrl,
		byJwt:                   byJwt,
		platformUrl:             platformUrl,
		deviceDescription:       deviceDescription,
		deviceSpec:              deviceSpec,
		appVersion:              appVersion,
		clientSettingsGenerator: clientSettingsGenerator,
		settings:                settings,
		api:                     api,
	}
}

func (self *ApiMultiClientGenerator) NextDestinations(count int, excludeDestinations []MultiHopId) (map[MultiHopId]ByteCount, error) {
	excludeClientIds := slices.Clone(self.excludeClientIds)
	excludeDestinationsIds := [][]Id{}
	for _, excludeDestination := range excludeDestinations {
		excludeDestinationsIds = append(excludeDestinationsIds, excludeDestination.Ids())
	}
	findProviders2 := &FindProviders2Args{
		Specs:               self.specs,
		ExcludeClientIds:    excludeClientIds,
		ExcludeDestinations: excludeDestinationsIds,
		Count:               count,
	}

	result, err := self.api.FindProviders2Sync(findProviders2)
	if err != nil {
		return nil, err
	}

	clientIdEstimatedBytesPerSecond := map[MultiHopId]ByteCount{}
	for _, provider := range result.Providers {
		ids := []Id{}
		if 0 < len(provider.IntermediaryIds) {
			ids = append(ids, provider.IntermediaryIds...)
		}
		ids = append(ids, provider.ClientId)
		// use the tail if the length exceeds the allowed maximum
		if MaxMultihopLength < len(ids) {
			ids = ids[len(ids)-MaxMultihopLength:]
		}
		if destination, err := NewMultiHopId(ids...); err == nil {
			clientIdEstimatedBytesPerSecond[destination] = provider.EstimatedBytesPerSecond
		}
	}

	return clientIdEstimatedBytesPerSecond, nil
}

func (self *ApiMultiClientGenerator) NewClientArgs() (*MultiClientGeneratorClientArgs, error) {
	auth := func() (string, error) {
		// note the derived client id will be inferred by the api jwt
		authNetworkClient := &AuthNetworkClientArgs{
			Description: self.deviceDescription,
			DeviceSpec:  self.deviceSpec,
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
			ByJwt:      byJwtStr,
			InstanceId: NewId(),
			AppVersion: self.appVersion,
		}
		return &MultiClientGeneratorClientArgs{
			ClientId:   byJwt.ClientId,
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
	clientOob := NewApiOutOfBandControl(ctx, self.clientStrategy, args.ClientAuth.ByJwt, self.apiUrl)
	client := NewClient(ctx, byJwt.ClientId, clientOob, clientSettings)
	settings := DefaultPlatformTransportSettings()
	if args.P2pOnly {
		settings.TransportGenerator = func() (sendTransport Transport, receiveTransport Transport) {
			// only use the platform transport for control
			sendTransport = NewSendClientTransport(DestinationId(ControlId))
			receiveTransport = NewReceiveGatewayTransport()
			return
		}
	}
	NewPlatformTransport(
		client.Ctx(),
		self.clientStrategy,
		client.RouteManager(),
		self.platformUrl,
		args.ClientAuth,
		settings,
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
	case <-ack:
	case <-time.After(self.settings.InitTimeout):
		client.Cancel()
		return nil, errors.New("Could not enable return traffic for client.")
	}
	return client, nil
}

type multiClientWindow struct {
	ctx    context.Context
	cancel context.CancelFunc

	generator             MultiClientGenerator
	receivePacketCallback ReceivePacketFunction

	settings *MultiClientSettings

	clientChannelArgs chan *multiClientChannelArgs

	monitor *RemoteUserNatMultiClientMonitor

	stateLock          sync.Mutex
	destinationClients map[MultiHopId]*multiClientChannel
}

func newMultiClientWindow(
	ctx context.Context,
	cancel context.CancelFunc,
	generator MultiClientGenerator,
	receivePacketCallback ReceivePacketFunction,
	settings *MultiClientSettings,
) *multiClientWindow {
	window := &multiClientWindow{
		ctx:                   ctx,
		cancel:                cancel,
		generator:             generator,
		receivePacketCallback: receivePacketCallback,
		settings:              settings,
		clientChannelArgs:     make(chan *multiClientChannelArgs, settings.WindowSizeMin),
		monitor:               NewRemoteUserNatMultiClientMonitor(&settings.RemoteUserNatMultiClientMonitorSettings),
		destinationClients:    map[MultiHopId]*multiClientChannel{},
	}

	go HandleError(window.randomEnumerateClientArgs, cancel)
	go HandleError(window.resize, cancel)

	return window
}

func (self *multiClientWindow) randomEnumerateClientArgs() {
	defer func() {
		close(self.clientChannelArgs)

		// drain the channel
		func() {
			for {
				select {
				case args, ok := <-self.clientChannelArgs:
					if !ok {
						return
					}
					self.generator.RemoveClientArgs(&args.MultiClientGeneratorClientArgs)
				}
			}
		}()
	}()

	// continually reset the visited set when there are no more
	visitedDestinations := map[MultiHopId]bool{}
	for {
		destinationEstimatedBytesPerSecond := map[MultiHopId]ByteCount{}
		for len(destinationEstimatedBytesPerSecond) == 0 {
			// exclude destinations that are already in the window
			func() {
				self.stateLock.Lock()
				defer self.stateLock.Unlock()
				for destination, _ := range self.destinationClients {
					visitedDestinations[destination] = true
				}
			}()
			nextDestinationEstimatedBytesPerSecond, err := self.generator.NextDestinations(
				1,
				maps.Keys(visitedDestinations),
			)
			if err != nil {
				glog.Infof("[multi]window enumerate error timeout = %s\n", err)
				select {
				case <-self.ctx.Done():
					return
				case <-time.After(self.settings.WindowEnumerateErrorTimeout):
				}
			} else {
				for destination, estimatedBytesPerSecond := range nextDestinationEstimatedBytesPerSecond {
					destinationEstimatedBytesPerSecond[destination] = estimatedBytesPerSecond
					visitedDestinations[destination] = true
				}
				// remove destinations that are already in the window
				func() {
					self.stateLock.Lock()
					defer self.stateLock.Unlock()
					for destination, _ := range self.destinationClients {
						delete(destinationEstimatedBytesPerSecond, destination)
					}
				}()

				if len(destinationEstimatedBytesPerSecond) == 0 {
					// reset
					visitedDestinations = map[MultiHopId]bool{}
					glog.Infof("[multi]window enumerate empty timeout.\n")
					select {
					case <-self.ctx.Done():
						return
					case <-time.After(self.settings.WindowEnumerateEmptyTimeout):
					}
				}
			}
		}

		for destination, estimatedBytesPerSecond := range destinationEstimatedBytesPerSecond {
			if clientArgs, err := self.generator.NewClientArgs(); err == nil {
				args := &multiClientChannelArgs{
					Destination:                    destination,
					EstimatedBytesPerSecond:        estimatedBytesPerSecond,
					MultiClientGeneratorClientArgs: *clientArgs,
				}
				select {
				case <-self.ctx.Done():
					self.generator.RemoveClientArgs(clientArgs)
					return
				case self.clientChannelArgs <- args:
				}
			} else {
				glog.Infof("[multi]create client args error = %s\n", err)
			}
		}
	}
}

func (self *multiClientWindow) resize() {
	// based on the most recent expand failure
	expandOvershotScale := float64(1.0)
	for {
		startTime := time.Now()

		clients := []*multiClientChannel{}

		maxSourceCount := 0
		weights := map[*multiClientChannel]float32{}
		durations := map[*multiClientChannel]time.Duration{}

		removedClients := []*multiClientChannel{}

		for _, client := range self.clients() {
			if stats, err := client.WindowStats(); err == nil {
				clients = append(clients, client)
				maxSourceCount = max(maxSourceCount, stats.sourceCount)
				// byte count per second
				weights[client] = float32(stats.ByteCountPerSecond())
				durations[client] = stats.duration
			} else {
				glog.Infof("[multi]remove client = %s\n", err)
				removedClients = append(removedClients, client)
			}
		}

		if 0 < len(removedClients) {
			func() {
				self.stateLock.Lock()
				defer self.stateLock.Unlock()

				for _, client := range removedClients {
					client.Cancel()
					delete(self.destinationClients, client.Destination())
				}
			}()
			for _, client := range removedClients {
				self.monitor.AddProviderEvent(client.ClientId(), ProviderStateRemoved)
			}
		}

		slices.SortFunc(clients, func(a *multiClientChannel, b *multiClientChannel) int {
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
			max(
				self.settings.WindowSizeMin,
				int(math.Ceil(float64(maxSourceCount)*self.settings.WindowSizeReconnectScale)),
			),
		)

		// expand and collapse have scale thresholds to avoid jittery resizing
		// too much resing wastes device resources
		expandWindowSize := min(
			self.settings.WindowSizeMax,
			max(
				self.settings.WindowSizeMin,
				int(math.Ceil(self.settings.WindowExpandScale*float64(len(clients)))),
			),
		)
		collapseWindowSize := int(math.Ceil(self.settings.WindowCollapseScale * float64(len(clients))))

		collapseLowestWeighted := func(windowSize int) []*multiClientChannel {
			// try to remove the lowest weighted clients to resize the window to `windowSize`
			// clients in the graceperiod or with activity cannot be removed

			self.stateLock.Lock()
			defer self.stateLock.Unlock()

			// n := 0

			removedClients := []*multiClientChannel{}
			// collapseClients := clients[windowSize:]
			// clients = clients[:windowSize]
			for _, client := range clients[windowSize:] {
				if self.settings.StatsWindowGraceperiod <= durations[client] && weights[client] <= 0 {
					client.Cancel()
					// n += 1
					removedClients = append(removedClients, client)
					delete(self.destinationClients, client.Destination())
				}
				//  else {
				// 	clients = append(clients, client)
				// }
			}

			// destinationClients := map[MultiHopId]*multiClientChannel{}
			// for _, client := range clients {
			// 	destinationClients[client.Destination()] = client
			// }
			// self.destinationClients = destinationClients

			return removedClients
		}

		p2pOnlyWindowSize := 0
		for _, client := range clients {
			if client.IsP2pOnly() {
				p2pOnlyWindowSize += 1
			}
		}
		if expandWindowSize <= targetWindowSize && len(clients) < expandWindowSize || p2pOnlyWindowSize < self.settings.WindowSizeMinP2pOnly {
			// collapse badly performing clients before expanding
			removedClients := collapseLowestWeighted(0)
			if 0 < len(removedClients) {
				glog.Infof("[multi]window optimize -%d ->%d\n", len(removedClients), len(clients))
				for _, client := range removedClients {
					self.monitor.AddProviderEvent(client.ClientId(), ProviderStateRemoved)
				}
			}

			// expand
			n := expandWindowSize - len(clients)
			self.monitor.AddWindowExpandEvent(false, expandWindowSize)
			overN := int(math.Ceil(expandOvershotScale * float64(n)))
			glog.Infof("[multi]window expand +%d(%d) %d->%d\n", n, overN, len(clients), expandWindowSize)
			self.expand(len(clients), p2pOnlyWindowSize, expandWindowSize, overN)

			// evaluate the next overshot scale
			func() {
				self.stateLock.Lock()
				defer self.stateLock.Unlock()
				n = len(self.destinationClients) - len(clients)
			}()
			if n <= 0 {
				expandOvershotScale = self.settings.WindowExpandMaxOvershotScale
			} else {
				// overN = s * n
				expandOvershotScale = min(
					self.settings.WindowExpandMaxOvershotScale,
					float64(overN)/float64(n),
				)
			}
		} else if targetWindowSize <= collapseWindowSize && collapseWindowSize < len(clients) {
			self.monitor.AddWindowExpandEvent(true, collapseWindowSize)
			removedClients := collapseLowestWeighted(collapseWindowSize)
			if 0 < len(removedClients) {
				glog.Infof("[multi]window collapse -%d ->%d\n", len(removedClients), len(clients))
				for _, client := range removedClients {
					self.monitor.AddProviderEvent(client.ClientId(), ProviderStateRemoved)
				}
			}
			self.monitor.AddWindowExpandEvent(true, collapseWindowSize)
		} else {
			self.monitor.AddWindowExpandEvent(true, len(clients))
			glog.Infof("[multi]window stable =%d\n", len(clients))
		}

		timeout := self.settings.WindowResizeTimeout - time.Now().Sub(startTime)
		if timeout <= 0 {
			select {
			case <-self.ctx.Done():
				return
			default:
			}
		} else {
			select {
			case <-self.ctx.Done():
				return
			case <-time.After(timeout):
			}
		}
	}
}

func (self *multiClientWindow) expand(currentWindowSize int, currentP2pOnlyWindowSize int, targetWindowSize int, n int) {
	mutex := sync.Mutex{}
	addedCount := 0

	endTime := time.Now().Add(self.settings.WindowExpandTimeout)
	pendingPingDones := []chan struct{}{}
	added := 0
	addedP2pOnly := 0
	for i := 0; i < n; i += 1 {
		timeout := endTime.Sub(time.Now())
		if timeout < 0 {
			glog.Infof("[multi]expand window timeout\n")
			return
		}

		select {
		case <-self.ctx.Done():
			return
		// case <- update:
		//     // continue
		case args, ok := <-self.clientChannelArgs:
			if !ok {
				return
			}
			func() {
				self.stateLock.Lock()
				defer self.stateLock.Unlock()
				_, ok = self.destinationClients[args.Destination]
			}()

			if ok {
				// already have a client in the window for this destination
				self.generator.RemoveClientArgs(&args.MultiClientGeneratorClientArgs)
			} else {
				// randomly set to p2p only to meet the minimum requirement
				if !args.MultiClientGeneratorClientArgs.P2pOnly {
					a := max(self.settings.WindowSizeMin-(currentWindowSize+added), 0)
					b := max(self.settings.WindowSizeMinP2pOnly-(currentP2pOnlyWindowSize+addedP2pOnly), 0)
					var p2pOnlyP float32
					if a+b == 0 {
						p2pOnlyP = 0
					} else {
						p2pOnlyP = float32(b) / float32(a+b)
					}
					args.MultiClientGeneratorClientArgs.P2pOnly = mathrand.Float32() < p2pOnlyP
				}

				client, err := newMultiClientChannel(
					self.ctx,
					args,
					self.generator,
					self.receivePacketCallback,
					self.settings,
				)
				if err == nil {
					added += 1
					if client.IsP2pOnly() {
						addedP2pOnly += 1
					}

					self.monitor.AddProviderEvent(args.ClientId, ProviderStateInEvaluation)

					// send an initial ping on the client and let the ack timeout close it
					pingDone := make(chan struct{})
					success, err := client.SendDetailedMessage(
						&protocol.IpPing{},
						self.settings.PingWriteTimeout,
						func(err error) {
							close(pingDone)
							if err == nil {
								glog.Infof("[multi]expand new client\n")

								self.monitor.AddProviderEvent(args.ClientId, ProviderStateAdded)
								var replacedClient *multiClientChannel
								func() {
									self.stateLock.Lock()
									defer self.stateLock.Unlock()
									replacedClient = self.destinationClients[args.Destination]
									self.destinationClients[args.Destination] = client
								}()
								if replacedClient != nil {
									replacedClient.Cancel()
									self.monitor.AddProviderEvent(replacedClient.ClientId(), ProviderStateRemoved)
								}
								func() {
									mutex.Lock()
									defer mutex.Unlock()
									addedCount += 1
									self.monitor.AddWindowExpandEvent(
										targetWindowSize <= currentWindowSize+addedCount,
										targetWindowSize,
									)
								}()
							} else {
								glog.Infof("[multi]create ping error = %s\n", err)
								client.Cancel()
								self.monitor.AddProviderEvent(args.ClientId, ProviderStateEvaluationFailed)
							}
						},
					)
					if err != nil {
						glog.Infof("[multi]create client ping error = %s\n", err)
						client.Cancel()
					} else if !success {
						client.Cancel()
						self.monitor.AddProviderEvent(args.ClientId, ProviderStateEvaluationFailed)
					} else {
						// async wait for the ping
						pendingPingDones = append(pendingPingDones, pingDone)
						go func() {
							select {
							case <-pingDone:
							case <-time.After(self.settings.PingTimeout):
								glog.V(2).Infof("[multi]expand window timeout waiting for ping\n")
								client.Cancel()
							}
						}()
					}
				} else {
					glog.Infof("[multi]create client error = %s\n", err)
					self.generator.RemoveClientArgs(&args.MultiClientGeneratorClientArgs)
					self.monitor.AddProviderEvent(args.ClientId, ProviderStateEvaluationFailed)
				}
			}
		case <-time.After(timeout):
			glog.V(2).Infof("[multi]expand window timeout waiting for args\n")
		}
	}

	// wait for pending pings
	for _, pingDone := range pendingPingDones {
		select {
		case <-self.ctx.Done():
			return
		case <-pingDone:
		}
	}

	return
}

func (self *multiClientWindow) shuffle() {
	for _, client := range self.clients() {
		client.Cancel()
	}
}

func (self *multiClientWindow) clients() []*multiClientChannel {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return maps.Values(self.destinationClients)
}

func (self *multiClientWindow) OrderedClients() []*multiClientChannel {

	clients := []*multiClientChannel{}

	weights := map[*multiClientChannel]float32{}
	durations := map[*multiClientChannel]time.Duration{}

	for _, client := range self.clients() {
		if stats, err := client.WindowStats(); err == nil {
			clients = append(clients, client)
			weights[client] = float32(stats.ByteCountPerSecond())
			durations[client] = stats.duration
		}
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

	return nonNegativeClients
}

func (self *multiClientWindow) statsSampleWeights(weights map[*multiClientChannel]float32) {
	// randonly sample log statistics for weights
	if mathrand.Intn(self.settings.StatsSampleWeightsCount) == 0 {
		// sample the weights
		weightValues := maps.Values(weights)
		slices.SortFunc(weightValues, func(a float32, b float32) int {
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
				if netThresh*100 <= netp {
					break
				}
			}

			glog.Infof("[multi]sample weights: %s (+%d more in window <%.0f%%)\n", sb.String(), len(weights)-netCount, 100*(1-netThresh))
		} else {
			glog.Infof("[multi]sample weights: zero (%d in window)\n", len(weights))
		}
	}
}

type multiClientChannelArgs struct {
	MultiClientGeneratorClientArgs

	Destination             MultiHopId
	EstimatedBytesPerSecond ByteCount
}

type multiClientEventType int

const (
	multiClientEventTypeAck    multiClientEventType = 1
	multiClientEventTypeNack   multiClientEventType = 2
	multiClientEventTypeError  multiClientEventType = 3
	multiClientEventTypeSource multiClientEventType = 4
)

type multiClientEventBucket struct {
	createTime time.Time
	eventTime  time.Time

	sendAckCount        int
	sendAckByteCount    ByteCount
	sendNackCount       int
	sendNackByteCount   ByteCount
	receiveAckCount     int
	receiveAckByteCount ByteCount
	sendAckTime         time.Time
	errs                []error
	ip4Paths            map[Ip4Path]bool
	ip6Paths            map[Ip6Path]bool
}

func newMultiClientEventBucket() *multiClientEventBucket {
	now := time.Now()
	return &multiClientEventBucket{
		createTime: now,
		eventTime:  now,
	}
}

type clientWindowStats struct {
	sourceCount         int
	sendAckCount        int
	sendAckByteCount    ByteCount
	sendNackCount       int
	sendNackByteCount   ByteCount
	receiveAckCount     int
	receiveAckByteCount ByteCount
	ackByteCount        ByteCount
	duration            time.Duration
	sendAckDuration     time.Duration

	// internal
	bucketCount int
}

func (self *clientWindowStats) ByteCountPerSecond() ByteCount {
	seconds := float64(self.duration / time.Second)
	if seconds <= 0 {
		return ByteCount(0)
	}
	return ByteCount(float64(self.sendAckByteCount-self.sendNackByteCount+self.receiveAckByteCount) / seconds)
}

type multiClientChannel struct {
	ctx    context.Context
	cancel context.CancelFunc

	args *multiClientChannelArgs

	api *BringYourApi

	receivePacketCallback ReceivePacketFunction

	settings *MultiClientSettings

	// sourceFilter map[TransferPath]bool

	client *Client

	stateLock    sync.Mutex
	eventBuckets []*multiClientEventBucket
	// destination -> source -> count
	ip4DestinationSourceCount map[Ip4Path]map[Ip4Path]int
	ip6DestinationSourceCount map[Ip6Path]map[Ip6Path]int
	packetStats               *clientWindowStats
	endErr                    error

	affinityCount int
	affinityTime  time.Time

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
		case <-cancelCtx.Done():
		case <-client.Done():
		}
		client.Cancel()
		generator.RemoveClientWithArgs(client, &args.MultiClientGeneratorClientArgs)
	}, cancel)

	// sourceFilter := map[TransferPath]bool{
	//     Path{ClientId:args.DestinationId}: true,
	// }

	clientChannel := &multiClientChannel{
		ctx:                   cancelCtx,
		cancel:                cancel,
		args:                  args,
		receivePacketCallback: receivePacketCallback,
		settings:              settings,
		// sourceFilter: sourceFilter,
		client:                    client,
		eventBuckets:              []*multiClientEventBucket{},
		ip4DestinationSourceCount: map[Ip4Path]map[Ip4Path]int{},
		ip6DestinationSourceCount: map[Ip6Path]map[Ip6Path]int{},
		packetStats:               &clientWindowStats{},
		endErr:                    nil,
		affinityCount:             0,
		affinityTime:              time.Time{},
	}
	go HandleError(clientChannel.detectBlackhole, cancel)

	clientReceiveUnsub := client.AddReceiveCallback(clientChannel.clientReceive)
	clientChannel.clientReceiveUnsub = clientReceiveUnsub

	return clientChannel, nil
}

func (self *multiClientChannel) ClientId() Id {
	return self.client.ClientId()
}

func (self *multiClientChannel) IsP2pOnly() bool {
	return self.args.MultiClientGeneratorClientArgs.P2pOnly
}

func (self *multiClientChannel) UpdateAffinity() {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.affinityCount += 1
	self.affinityTime = time.Now()
}

func (self *multiClientChannel) ClearAffinity() {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.affinityCount = 0
	self.affinityTime = time.Time{}
}

func (self *multiClientChannel) MostRecentAffinity() (int, time.Time) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	return self.affinityCount, self.affinityTime
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

		opts := []any{
			ForceStream(),
		}
		switch parsedPacket.ipPath.Protocol {
		case IpProtocolUdp:
			opts = append(opts, NoAck())
		}
		return self.client.SendMultiHopWithTimeoutDetailed(
			frame,
			self.args.Destination,
			ackCallback,
			timeout,
			opts...,
		)
	}
}

func (self *multiClientChannel) SendDetailedMessage(message proto.Message, timeout time.Duration, ackCallback func(error)) (bool, error) {
	if frame, err := ToFrame(message); err != nil {
		return false, err
	} else {
		return self.client.SendMultiHopWithTimeoutDetailed(
			frame,
			self.args.Destination,
			ackCallback,
			timeout,
			ForceStream(),
		)
	}
}

func (self *multiClientChannel) EstimatedByteCountPerSecond() ByteCount {
	return self.args.EstimatedBytesPerSecond
}

func (self *multiClientChannel) Done() <-chan struct{} {
	return self.ctx.Done()
}

func (self *multiClientChannel) Destination() MultiHopId {
	return self.args.Destination
}

func (self *multiClientChannel) detectBlackhole() {
	// within a timeout window, if there are sent data but none received,
	// error out. This is similar to an ack timeout.
	defer self.cancel()

	for {
		if windowStats, err := self.WindowStats(); err != nil {
			return
		} else {
			timeout := self.settings.BlackholeTimeout - windowStats.sendAckDuration
			if timeout <= 0 {
				timeout = self.settings.BlackholeTimeout

				if 0 < windowStats.sendAckCount && windowStats.receiveAckCount <= 0 {
					// the client has sent data but received nothing back
					// this looks like a blackhole
					glog.Infof("[multi]routing %s blackhole: %d %dB <> %d %dB\n",
						self.args.Destination,
						windowStats.sendAckCount,
						windowStats.sendAckByteCount,
						windowStats.receiveAckCount,
						windowStats.receiveAckByteCount,
					)
					self.addError(fmt.Errorf("Blackhole (%d %dB)",
						windowStats.sendAckCount,
						windowStats.sendAckByteCount,
					))
					return
				} else {
					glog.Infof(
						"[multi]routing ok %s: %d %dB <> %d %dB\n",
						self.args.Destination,
						windowStats.sendAckCount,
						windowStats.sendAckByteCount,
						windowStats.receiveAckCount,
						windowStats.receiveAckByteCount,
					)
				}
			}

			select {
			case <-self.ctx.Done():
				return
			case <-self.client.Done():
				return
			case <-time.After(timeout):
			}
		}
	}
}

func (self *multiClientChannel) eventBucket() *multiClientEventBucket {
	// must be called with stateLock

	now := time.Now()

	var eventBucket *multiClientEventBucket
	if n := len(self.eventBuckets); 0 < n {
		eventBucket = self.eventBuckets[n-1]
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

	self.packetStats.sendNackCount += 1
	self.packetStats.sendNackByteCount += ackByteCount

	eventBucket := self.eventBucket()
	eventBucket.sendNackCount += 1
	eventBucket.sendNackByteCount += ackByteCount

	self.coalesceEventBuckets()
}

func (self *multiClientChannel) addSendAck(ackByteCount ByteCount) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.packetStats.sendNackCount -= 1
	self.packetStats.sendNackByteCount -= ackByteCount
	self.packetStats.sendAckCount += 1
	self.packetStats.sendAckByteCount += ackByteCount

	eventBucket := self.eventBucket()
	if eventBucket.sendAckCount == 0 {
		eventBucket.sendAckTime = time.Now()
	}
	eventBucket.sendAckCount += 1
	eventBucket.sendAckByteCount += ackByteCount

	self.coalesceEventBuckets()
}

func (self *multiClientChannel) addReceiveAck(ackByteCount ByteCount) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.packetStats.receiveAckCount += 1
	self.packetStats.receiveAckByteCount += ackByteCount

	eventBucket := self.eventBucket()
	eventBucket.receiveAckCount += 1
	eventBucket.receiveAckByteCount += ackByteCount

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
	minBucketCount := 1 + int(self.settings.StatsWindowDuration/self.settings.StatsWindowBucketDuration)

	windowStart := time.Now().Add(-self.settings.StatsWindowDuration)

	// remove events before the window start
	i := 0
	for i < len(self.eventBuckets) && minBucketCount < len(self.eventBuckets) {
		eventBucket := self.eventBuckets[i]
		if windowStart.Before(eventBucket.eventTime) {
			break
		}

		self.packetStats.sendAckCount -= eventBucket.sendAckCount
		self.packetStats.sendAckByteCount -= eventBucket.sendAckByteCount
		self.packetStats.receiveAckCount -= eventBucket.receiveAckCount
		self.packetStats.receiveAckByteCount -= eventBucket.receiveAckByteCount

		for ip4Path, _ := range eventBucket.ip4Paths {
			source := ip4Path.Source()
			destination := ip4Path.Destination()

			sourceCount, ok := self.ip4DestinationSourceCount[destination]
			if ok {
				count := sourceCount[source]
				if count-1 <= 0 {
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
				if count-1 <= 0 {
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
	sendAckDuration := time.Duration(0)
	for _, eventBucket := range self.eventBuckets {
		if 0 < eventBucket.sendAckCount {
			sendAckDuration = time.Now().Sub(eventBucket.sendAckTime)
			break
		}
	}

	// public internet resource ports
	isPublicPort := func(port int) bool {
		switch port {
		case 443:
			return true
		default:
			return false
		}
	}

	netSourceCounts := []int{}
	for ip4Path, sourceCounts := range self.ip4DestinationSourceCount {
		if isPublicPort(ip4Path.DestinationPort) {
			netSourceCounts = append(netSourceCounts, len(sourceCounts))
		}
	}
	for ip6Path, sourceCounts := range self.ip6DestinationSourceCount {
		if isPublicPort(ip6Path.DestinationPort) {
			netSourceCounts = append(netSourceCounts, len(sourceCounts))
		}
	}
	slices.Sort(netSourceCounts)
	maxSourceCount := 0
	if selectionIndex := int(math.Ceil(self.settings.StatsSourceCountSelection * float64(len(netSourceCounts)-1))); selectionIndex < len(netSourceCounts) {
		maxSourceCount = netSourceCounts[selectionIndex]
	}
	if glog.V(2) {
		for ip4Path, sourceCounts := range self.ip4DestinationSourceCount {
			if isPublicPort(ip4Path.DestinationPort) {
				if len(sourceCounts) == maxSourceCount {
					glog.Infof("[multi]max source count %d = %v\n", maxSourceCount, ip4Path)
				}
			}
		}
		for ip6Path, sourceCounts := range self.ip6DestinationSourceCount {
			if isPublicPort(ip6Path.DestinationPort) {
				if len(sourceCounts) == maxSourceCount {
					glog.Infof("[multi]max source count %d = %v\n", maxSourceCount, ip6Path)
				}
			}
		}
	}
	stats := &clientWindowStats{
		sourceCount:         maxSourceCount,
		sendAckCount:        self.packetStats.sendAckCount,
		sendNackCount:       self.packetStats.sendNackCount,
		sendAckByteCount:    self.packetStats.sendAckByteCount,
		sendNackByteCount:   self.packetStats.sendNackByteCount,
		receiveAckCount:     self.packetStats.receiveAckCount,
		receiveAckByteCount: self.packetStats.receiveAckByteCount,
		duration:            duration,
		sendAckDuration:     sendAckDuration,
		bucketCount:         len(self.eventBuckets),
	}
	err := self.endErr
	if err == nil {
		select {
		case <-self.ctx.Done():
			err = errors.New("Done.")
		case <-self.client.Done():
			err = errors.New("Done.")
		default:
		}
	}

	return stats, err
}

// `connect.ReceiveFunction`
func (self *multiClientChannel) clientReceive(source TransferPath, frames []*protocol.Frame, provideMode protocol.ProvideMode) {
	select {
	case <-self.ctx.Done():
		return
	default:
	}

	// only process frames from the destinations
	// if allow := self.sourceFilter[source]; !allow {
	//     glog.V(2).Infof("[multi]receive drop %d %s<-\n", len(frames), self.args.DestinationId)
	//     return
	// }

	for _, frame := range frames {
		switch frame.MessageType {
		case protocol.MessageType_IpIpPacketFromProvider:
			if ipPacketFromProvider_, err := FromFrame(frame); err == nil {
				ipPacketFromProvider := ipPacketFromProvider_.(*protocol.IpPacketFromProvider)

				packet := ipPacketFromProvider.IpPacket.PacketBytes

				self.addReceiveAck(ByteCount(len(packet)))

				self.receivePacketCallback(source, IpProtocolUnknown, packet)
			} else {
				glog.V(2).Infof("[multi]receive drop %s<- = %s\n", self.args.Destination, err)
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
