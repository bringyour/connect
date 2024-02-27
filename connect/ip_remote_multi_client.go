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




/*
Score is Acked - unacked
Max unpacked per sender
If no available senders, wait for timeout, then expand
*/

type ProviderSpec struct {
	// FIXME
}




type parsedPacket struct {
	packet []byte
	ipPath *ipPath
}

func newParsedPacket(packet []byte) (*parsedPacket, error) {
	// FIXME
	return nil, nil
}


type MultiClientSettings struct {

	WindowSizeMin int
	WindowSizeReconnectScale float32

	ClientNackInitialLimit int
	ClientNackMaxLimit int
	ClientNackScale float32

	WriteTimeout time.Duration
	MultiWriteTimeout time.Duration
	WindowExpandTimeout time.Duration

}


type RemoteUserNatMultiClient struct {
	ctx context.Context
	cancel context.CancelFunc
	specs []*ProviderSpec

	settings *MultiClientSettings



    receivePacketCallback ReceivePacketFunction


    window *multiClientWindow


	ip4PathClients map[ip4Path]*multiClientChannel 
	ip6PathClients map[ip6Path]*multiClientChannel
	clientIp4Paths map[*multiClientChannel]map[ip4Path]bool
	clientIp6Paths map[*multiClientChannel]map[ip6Path]bool
}

func NewRemoteUserNatMultiClient(
	ctx context.Context,
	specs []*ProviderSpec,
	settings *MultiClientSettings,
) *RemoteUserNatMultiClient {
	cancelCtx, cancel := context.WithCancel(ctx)

	return &RemoteUserNatMultiClient{
		ctx: cancelCtx,
		cancel: cancel,
		specs: specs,
		settings: settings,
	}
}

// func NewRemoteUserNatMultiClientFromDescription(description string) {

// }


func (self *RemoteUserNatMultiClient) getPathClient(ipPath *ipPath) *multiClientChannel {
	// FIXME
	return nil
}

func (self *RemoteUserNatMultiClient) setPathClient(ipPath *ipPath, client *multiClientChannel) {
	// FIXME
}

func (self *RemoteUserNatMultiClient) removePathClient(ipPath *ipPath, client *multiClientChannel) {
	// FIXME
}

func (self *RemoteUserNatMultiClient) removeClient(client *multiClientChannel) {
	// FIXME
}



// `SendPacketFunction`
func (self *RemoteUserNatMultiClient) SendPacket(source Path, provideMode protocol.ProvideMode, packet []byte) {

	// not source and provide mode are about the logical origin of the packet
	// can ignore them after security checks

	// frame

	// if transfer path is already assigned to a client, and the client is still in the window, use it

	// expand window size to match target size

	// order senders by acked - nacked in window (window send count)
	// in order, try non blocking write

	// use multi select write
	// on multiwritetimeout, add new client


	// the max nack count of the client expands with each successful ack
	// max nack starts at 1
	// a failed ack closes the client immediately


	// expand window to match target size







	parsedPacket, err := newParsedPacket(packet)
	if err != nil {
		// bad packet
		return
	}

	if client := self.getPathClient(parsedPacket.ipPath); client != nil {
		select {
		case <- self.ctx.Done():
			return
		case client.Send() <- parsedPacket:
			return
		case <- time.After(WriteTimeout):
			// now we can change the routing of this path
			self.removePathClient(parsedPacket.ipPath, client)
		}
	}

	for {
		orderedClients, removedClients := self.window.OrderedClients(ExpandTimeout)
		for _, client := range removedClients {
			self.removeClient(client)
		}

		for _, client := range orderedClients {
			select {
			case client.Send() <- parsedPacket:
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
            Chan: reflect.ValueOf(time.After(MultiWriteTimeout)),
        })

        chosenIndex, _, _ := reflect.Select(selectCases)

        switch chosenIndex {
        case doneIndex:
            // return errors.New("Done")
            return
        case timeoutIndex:
            // return errors.New("Timeout")
            return
        default:
            // a route
            clientIndex := chosenIndex - clientStartIndex
            client := orderedClients[clientIndex]
            // lock the path to the client
            self.setPathClient(parsedPacket.ipPath, client)
			return
        }
	}
}

// `connect.ReceiveFunction`
func (self *RemoteUserNatMultiClient) ClientReceive(sourceId Id, frames []*protocol.Frame, provideMode protocol.ProvideMode) {
	// the client channels have already filtered the messages for the actual destinations only

	source := Path{ClientId: sourceId}

    for _, frame := range frames {
        switch frame.MessageType {
        case protocol.MessageType_IpIpPacketFromProvider:
            ipPacketFromProvider_, err := FromFrame(frame)
            if err != nil {
                panic(err)
            }
            ipPacketFromProvider := ipPacketFromProvider_.(*protocol.IpPacketFromProvider)

            fmt.Printf("remote user nat client r packet %s<-\n", source)

            self.receivePacketCallback(source, IpProtocolUnknown, ipPacketFromProvider.IpPacket.PacketBytes)
        }
    }
}




// client send queue frame
// don't read if max nack count <= nack count 
// windowStats(dst), int and error




type multiClientWindow struct {
	ctx context.Context

	specs []*ProviderSpec
	receivePacketCallback ReceivePacketFunction

	clientChannelArgs chan *multiClientChannelArgs

	stateLock sync.Mutex
	destinationClients map[Id]*multiClientChannel

	windowUpdate *Monitor
}

func newMultiClientWindow(
	ctx context.Context,
	specs []*ProviderSpec,
	receivePacketCallback ReceivePacketFunction,
) *multiClientWindow {
	window := &multiClientWindow{
		ctx: ctx,
		specs: specs,
		receivePacketCallback: receivePacketCallback,
		clientChannelArgs: make(chan *multiClientChannelArgs),
		destinationClients: map[Id]*multiClientChannel{},
	}

	go window.randomEnumerateClientArgs()

	return window
}

func (self *multiClientWindow) randomEnumerateClientArgs() {
	// keep track of all client ids so far
	// reset visited client ids if no new
	visitedDestinationIds := map[Id]bool{}
	for {

		var destinationId Id
		for {
			nextDestinationIds := APINEXT(self.specs, maps.Keys(visitedDestinationIds), 1)
			if 0 < len(nextDestinationIds) {
				destinationId = nextDestinationIds[0]
				visitedDestinationIds[destinationId] = true
				break
			}
			// reset
			visitedDestinationIds = map[Id]bool{}
			select {
			case <- self.ctx.Done():
				return
			case <- time.After(WindowEnumerateEmptyTimeout):
			}
		}

		
		// FIXME login client id
		// FIXME are we parsing client id from the jwt locally?
		// ParseByJwtUnverified
		APILOGINCLIENT()

		var clientId Id
		clientAuth := &ClientAuth{}
		args := &multiClientChannelArgs{
			destinationId: destinationId,
			clientId: clientId,
			clientAuth: clientAuth,
		} 

		select {
		case <- self.ctx.Done():
			args.Close()
			return
		case self.clientChannelArgs <- args:
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
	endTime := time.Now().Add(ExpandTimeout)
	for {
		update := self.windowUpdate.NotifyChannel()
		self.stateLock.Lock()
		windowSize := len(self.destinationClients)
		self.stateLock.Unlock()

		if targetWindowSize <= windowSize {
			return
		}

		timeout := endTime.Sub(time.Now())
		if timeout < 0 {
			return
		}

		select {
		case <- self.ctx.Done():
			return
		case <- update:
			// continue
		case args := <- self.clientChannelArgs:
			self.stateLock.Lock()
			client, ok := self.destinationClients[args.destinationId]
			self.stateLock.Unlock()

			if ok {
				// already iterated this destination
				args.Close()
			} else {
				client = newMultiClientChannel(self.ctx, args, self.receivePacketCallback)
				self.stateLock.Lock()
				self.destinationClients[args.destinationId] = client
				self.stateLock.Unlock()
				self.windowUpdate.NotifyAll()
			}
		case <- time.After(timeout):
			return
		}
	}
}

func (self *multiClientWindow) OrderedClients(timeout time.Duration) ([]*multiClientChannel, []*multiClientChannel) {
	self.stateLock.Lock()
	clients := maps.Values(self.destinationClients)
	self.stateLock.Unlock()

	// expand
	netSrcCount := 0
	clientWeights := map[*multiClientChannel]float32{}
	for _, client := range clients {
		stats, err := client.windowStats()
		if err != nil {
			self.stateLock.Lock()
			delete(self.destinationClients, client.destinationId)
			self.stateLock.Unlock()
			self.windowUpdate.NotifyAll()
		} else {
			netSrcCount += stats.srcCount
			clientWeights[client] = float32(stats.netPacketCount)
		}
	}

	targetWindowSize := minSize + netSrcCount * scale
	expandTo(targetWindowSize)

	// now order the clients based on the stats
	WeightedShuffle(clients, clientWeight)

	return clients
}





type multiClientChannelArgs struct {
	destinationId Id
	clientId Id
	clientAuth *ClientAuth
}

func (self *multiClientChannelArgs) Close() {
	// FIXME remove the client usind the api

	APIREMOVECLIENT(self.clientId)
}


type multiClientEventType int
const (
	multiClientEventTypeAck multiClientEventType = 1
	multiClientEventTypeNack multiClientEventType = 2
	multiClientEventTypeError multiClientEventType = 3
	multiClientEventTypeSrc multiClientEventType = 4
)


type multiClientEvent struct {
	eventType multiClientEventType
	ack bool
	err error
	ipPath *ipPath
	eventTime time.Time
}


type clientWindowStats struct {
	srcCount int
	ackCount int
	nackCount int
	ackByteCount ByteCount
	nackByteCount ByteCount
}


type multiClientChannel struct {
	ctx context.Context
	cancel context.CancelFunc

	args *multiClientChannelArgs

	send chan *parsedPacket

	receivePacketCallback ReceivePacketFunction
	sourceFilter map[Path]bool

	client *Client

	

	stateLock sync.Mutex
	events []*multiClientEvent
	srcCount map[src]int
	packetStats *clientWindowStats
	endErr error

	maxNackCount int

	eventUpdate *Monitor
}

func newMultiClientChannel(
	ctx context.Context,
	args *multiClientChannelArgs,
	receivePacketCallback ReceivePacketFunction,
) *multiClientChannel {
	cancelCtx, cancel := context.WithCancel(ctx)


	client := NewClientWithDefaults(cancelCtx)

	transport := NewPlatformaTransport(cancelCtx, args.clientAuth, client.RouteManager())



	sourceFilter := map[Path]bool{
		args.destinationId: true,
	}

    clientChannel := &multiClientChannel{
    	ctx: cancelCtx,
    	cancel: cancel,
    	args: args,
    	send: make(chan *parsedPacket),
    	receivePacketCallback: receivePacketCallback,
    	sourceFilter: sourceFilter,
    	client: client,
    	events: []*multiClientEvent{},
		srcCount: map[src]int{},
		packetStats: &clientWindowStats{},
		endErr: nil,
    }


	go clientChannel.run()

	client.AddReceiveCallback(clientChannel)

	return client
}


func (self *multiClientChannel) run() {
	defer func() {
		self.cancel()
		self.addError(errors.New("Done."))
		self.args.Close()
	}()

	for {
		update := self.eventUpdate.NotifyChannel()
		if self.isMaxAcks() {
			select {
			case <- self.ctx.Done():
				return
			case <- update:
			}
		} else {
			select {
			case <- self.ctx.Done():
				return
			case parsedPacket := <- self.send:
				self.addNack()
				self.addSrc(parsedPacket.ipPath)
				success := self.client.SendWithTimeout(frame, destinationId, func(error) {
					if err == nil {
						self.addAck()
					} else {
						self.addError(err)
						return
					}
				})
				if !success {
					self.addError(errors.New("Send failed."))
				}
			}
		}
	}
}


func (self *multiClientChannel) Send() chan *parsedPacket {
	return self.send
}


func (self *multiClientChannel) isMaxAcks() {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	return (self.maxNackCount <= self.packetStats.nackCount)
}


func (self *multiClientChannel) addNack() {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.events = append(self.events, &multiClientEvent{
		eventType: multiClientEventTypeAck,
		ack: false,
	})
}

func (self *multiClientChannel) addAck() {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.events = append(self.events, &multiClientEvent{
		eventType: multiClientEventTypeAck,
		ack: true,
	})
}

func (self *multiClientChannel) addSrc(ipPath *ipPath) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.events = append(self.events, &multiClientEvent{
		eventType: multiClientEventTypeSrc,
		ipPath: ipPath,
	})
}

func (self *multiClientChannel) addError(err error) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.events = append(self.events, &multiClientEvent{
		eventType: multiClientEventTypeError,
		err: err,
	})
}




func (self *multiClientChannel) windowStats() (*clientWindowStats, error) {
	
	changed := false
	self.stateLock.Lock()
	// forward the event window
	i := 0
	for i < len(self.events) {
		event := self.events[i]
		if now < event.eventTime {
			break
		}
		switch event.eventType {
		case Nack:
			self.packetStats.nackCount += 1
			changed = true

		case Ack:
			self.packetStats.nackCount -= 1
			self.packetStats.ackCount += 1
			self.maxNackCount = min(self.maxNackCount + NACKSCALE, MAXNACKCOUNT)
			changed = true

		case Src:
			self.srcCount[ipPath.Src()] += 1
			changed = true

		case Error:
			if self.endErr == nil {
				self.endErr = err
				changed = true
			}
		}
		self.events[i] = nil
	}
	self.events = self.events[:i]

	out := &clientWindowStats{
		srcCount: len(self.srcCount),
		ackCount: self.packetStats.ackCount,
		nackCount: self.packetStats.nackCount,
		ackByteCount: self.packetStats.ackByteCount,
		nackByteCount: self.packetStats.nackByteCount,
	}
	outErr := self.endErr

	self.stateLock.Unlock()

	if changed {
		self.eventUpdate.NotifyAll()
	}

	return out, outErr
}


func (self *multiClientChannel) ClientReceive(sourceId Id, frames []*protocol.Frame, provideMode protocol.ProvideMode) {
	source := Path{ClientId: sourceId}

    // only process frames from the destinations
    if allow := self.sourceFilter[source]; !allow {
        return
    }

    self.receivePacketCallback(sourceId, frames, provideMode)
}





func APINEXT(specs []*ProviderSpec, excludeClientIds []Id, count int) []Id {
	// FIXME
	return []Id{}
}


func APILOGINCLIENT() {
	// FIXME
}


func APIREMOVECLIENT(clientId Id) {
	// FIXME
}





