package main

// for this sim, the client sends a number to the egress, and the egress echos the number back


import (
	"context"
	"time"
	"sync"
	"slices"
	"errors"
	mathrand "math/rand"
	"fmt"
	"runtime"


	"golang.org/x/exp/maps"

)




func main() {
	ctx := context.Background()

	timeout := 300 * time.Second
	// this is a tradeoff between memory and stats accuracy
	packetInterval := 50 * time.Millisecond
	senderCount := 100
	sendSize := 10000
	sendDuration := 5 * time.Second


	runtime.GOMAXPROCS(2 * senderCount)


	statsWindowSim := &StatisticalHopWindow{
		ctx: ctx,

		timeout: timeout,
		packetInterval: packetInterval,

		senderCount: senderCount,
		sendSize: sendSize,
		sendDuration: sendDuration,

		egressWindowSize: 5,
		egressStatsWindow: 60 * time.Second,
		// TODO use a fraction of the total transfer in the stats window
		egressStatsWindowEstimateNetTransfer: 1000,
		// TODO use a fraction of the total transfer in the stats window
		egressStatsWindowEstimateNetTransferToDst: 1000,
		dstWeight: 0.8,

		egressWindowContractTimeout: 1 * time.Second,
		egressWindowExpandReconnectCount: 5,
		egressWindowExpandStep: 5,
	}

	if err := statsWindowSim.Run(); err != nil {
		panic(err)
	}


}




type EgressFunction func(connectionTuple ConnectionTuple)(*Egress)


type Id int64

var id Id
var idLock = sync.Mutex{}
func NewId() Id {
	idLock.Lock()
	defer idLock.Unlock()
	id += 1
	return id
}



type Packet struct {
	Index int
}

func NewPacket(index int) *Packet {
	return &Packet{
		Index: index,
	}
}


type ConnectionTuple struct {
	SrcIp Id
	SrcPort int
	DstIp Id
	DstPort int
}

func NewConnectionTuple(srcIp Id, srcPort int, dstIp Id, dstPort int) ConnectionTuple {
	return ConnectionTuple{
		SrcIp: srcIp,
		SrcPort: srcPort,
		DstIp: dstIp,
		DstPort: dstPort,
	}
}

func (self *ConnectionTuple) Reverse() ConnectionTuple {
	return ConnectionTuple{
		SrcIp: self.DstIp,
		SrcPort: self.DstPort,
		DstIp: self.SrcIp,
		DstPort: self.SrcPort,
	}
}

func (self *ConnectionTuple) Dst() ConnectionTuple {
	return ConnectionTuple{
		SrcIp: 0,
		SrcPort: 0,
		DstIp: self.DstIp,
		DstPort: self.DstPort,
	}
}






type Sender struct {
	ctx context.Context

	stats *PacketIntervalWindow

	size int
	delay time.Duration

	readTimeout time.Duration
	resendTimeout time.Duration
}

func NewSender(
	ctx context.Context,
	stats *PacketIntervalWindow,
	size int,
	sendDuration time.Duration,
) *Sender {
	return &Sender{
		ctx: ctx,
		stats: stats,
		size: size,
		delay: sendDuration / time.Duration(size),
		readTimeout: 1 * time.Second,
		resendTimeout: 10 * time.Millisecond,
	}
}

func (self *Sender) Run(connectEgress EgressFunction) {
	// ip tuple of where the data is being sent
	connectionTuple := NewConnectionTuple(
		NewId(), 1,
		NewId(), 1,
	)

	// routing hops
	clientId := NewId()
	

	connect := func()(out chan *Packet, in chan *Packet, egressId Id) {
		connectionTuple.SrcPort += 1
		// fmt.Printf("Get egress start\n")
		egress := connectEgress(connectionTuple)
		// fmt.Printf("Get egress done\n")
		egressId = egress.EgressId
		fmt.Printf("Connect to %d from %s\n", egressId, connectionTuple)

		out = make(chan *Packet)
		in = make(chan *Packet)
		egress.Connect(connectionTuple, out, in)
		return
	}


	var sendSeq func()

	sendSeq = func() {

		// fmt.Printf("Connect start\n")
		out, in, egressId := connect()
		closeOut := sync.OnceFunc(func() {
			close(out)
		})
		// defer close(out)
		defer closeOut()
		// fmt.Printf("Connect done\n")

		for i := 0; i < self.size; i += 1 {

			readTimeoutTime := time.Now().Add(self.readTimeout)

			var sendTime time.Time
			send := func() {
				packet := NewPacket(i)
				sendTime = time.Now()
				self.stats.AddPacket(sendTime, clientId, egressId, connectionTuple, 1)
				select {
				case <- self.ctx.Done():
				case <- time.After(readTimeoutTime.Sub(time.Now())):
				case out <- packet:
				}
			}

			send()


			ack := false
			for !ack {
				resendTime := sendTime.Add(self.resendTimeout)
				select {
				case <- self.ctx.Done():
					return
				case packet, ok := <- in:
					if !ok {
						// reconnect
						fmt.Printf("Reconnect 1\n")
						closeOut()
						sendSeq()
						return
					} else if packet.Index == i {
						ack = true
						self.stats.AddPacket(time.Now(), egressId, clientId, connectionTuple.Reverse(), 1)
					}
				case <- time.After(resendTime.Sub(time.Now())):
					send()
				case <- time.After(readTimeoutTime.Sub(time.Now())):
					// reconnect
					fmt.Printf("Reconnect 2\n")
					closeOut()
					sendSeq()
					return
				}
			}

			select {
			case <- self.ctx.Done():
				return
			case <- time.After(self.delay):
			}
		}




	}

	

	sendSeq()


}


type BlackholeState struct {
	Active bool
	StartTime time.Time
	EndTime time.Time
}


type EgressRandomSettings struct {
	dropProbabilityPerSecond float64
	dropMin time.Duration
	dropMax time.Duration

	blockProbabilityPerDst float64
	blockDelay time.Duration
	blockMin time.Duration
	blockMax time.Duration
}


type Egress struct {
	ctx context.Context

	EgressId Id

	forever time.Duration

	rand *EgressRandomSettings

	stateLock sync.Mutex
	drop BlackholeState
	blockDst map[ConnectionTuple]BlackholeState
}

func NewEgress(
	ctx context.Context,
	egressId Id,
	forever time.Duration,
	rand *EgressRandomSettings,
) *Egress {
	egress := &Egress{
		ctx: ctx,
		EgressId: egressId,

		forever: forever,

		rand: rand,

		stateLock: sync.Mutex{},
		drop: BlackholeState{},
		blockDst: map[ConnectionTuple]BlackholeState{},
	}

	go func() {
		for {
			select {
			case <- ctx.Done():
				return
			case <- time.After(time.Second):
				egress.maybeDropPerSecond()
			}
		}
	}()

	return egress
}

func (self *Egress) testDropPerSecond() (out BlackholeState) {
	if mathrand.Float64() < self.rand.dropProbabilityPerSecond {
		out.Active = true

		out.StartTime = time.Now()
		out.EndTime = out.StartTime.Add(
			time.Duration(mathrand.Int63n(int64((self.rand.dropMax - self.rand.dropMin) / time.Second))) * time.Second,
		)
	}
	return
}

func (self *Egress) testBlockPerDst() (out BlackholeState) {
	if mathrand.Float64() < self.rand.blockProbabilityPerDst {
		out.Active = true

		out.StartTime = time.Now().Add(
			time.Duration(mathrand.Int63n(int64(self.rand.blockDelay / time.Second))) * time.Second,
		)
		out.EndTime = out.StartTime.Add(
			time.Duration(mathrand.Int63n(int64((self.rand.blockMax - self.rand.blockMin) / time.Second))) * time.Second,
		)
	}
	return
}

func (self *Egress) maybeDropPerSecond() {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if !self.drop.Active {
		self.drop = self.testDropPerSecond()
	}		
}

func (self *Egress) maybeBlockDst(dst ConnectionTuple) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	block := self.blockDst[dst]
	if !block.Active {
		self.blockDst[dst] = self.testBlockPerDst()
	}
}

// type egress struct
// 
func (self *Egress) Connect(
	connectionTuple ConnectionTuple,
	in chan *Packet,
	out chan *Packet,
	// readTimeout time.Duration,
) {
	// fmt.Printf("CONNECT\n")
	go func() {
		defer close(out)

		self.maybeBlockDst(connectionTuple.Dst())

		for {
			self.stateLock.Lock()
			drop := self.drop
			block := self.blockDst[connectionTuple.Dst()]
			self.stateLock.Unlock()
			
			var blockedTimeout time.Duration
			var blocked bool
			if block.Active {
				startTimeout := time.Now().Sub(block.StartTime)
				endTimeout := time.Now().Sub(block.EndTime)
				if 0 <= startTimeout {
					blockedTimeout = startTimeout
					blocked = true
				} else if 0 <= endTimeout {
					blockedTimeout = endTimeout
					blocked = false
				} else {
					blockedTimeout = self.forever
				}
			} else {
				blockedTimeout = self.forever
			}

			var droppedTimeout time.Duration
			var dropped bool
			if drop.Active {
				startTimeout := time.Now().Sub(drop.StartTime)
				endTimeout := time.Now().Sub(drop.EndTime)
				if 0 <= startTimeout {
					droppedTimeout = startTimeout
					dropped = true
				} else if 0 <= endTimeout {
					droppedTimeout = endTimeout
					dropped = false
				} else {
					droppedTimeout = self.forever
				}
			} else {
				droppedTimeout = self.forever
			}

			// readTimeoutTime := time.Now().Add(readTimeout)

			select {
			case <- self.ctx.Done():
				return
			case <- time.After(blockedTimeout):
			case <- time.After(droppedTimeout):
			// case <- time.After(readTimeoutTime.Sub(time.Now())):
			// 	fmt.Printf("Read timeout\n")
			// 	return
			case packet, ok := <- in:
				// fmt.Printf("GOT A PACKET\n")
				if !ok {
					// fmt.Printf("Closed\n")
					return
				} else if blocked {
					// blackhole
					fmt.Printf("Blackhole blocked\n")
				} else if dropped {
					// blackhole
					fmt.Printf("Blackhole dropped\n")
				} else {
					select {
					case <- self.ctx.Done():
					case out <- packet:
					}
				}
			}
		}
	}()
}



type StatisticalHopWindow struct {

	ctx context.Context

	timeout time.Duration
	packetInterval time.Duration

	senderCount int
	sendSize int
	sendDuration time.Duration

	egressWindowSize int
	egressStatsWindow time.Duration
	egressStatsWindowEstimateNetTransfer int64
	egressStatsWindowEstimateNetTransferToDst int64
	dstWeight float64

	egressWindowContractTimeout time.Duration
	egressWindowExpandReconnectCount int
	egressWindowExpandStep int
}



// at the end computes amount of data sent / time
func (self *StatisticalHopWindow) Run() error {

	cancelCtx, cancel := context.WithCancel(self.ctx)
	defer cancel()

	newEgress := func()(*Egress) {
		return NewEgress(
			cancelCtx,

			NewId(),
			self.timeout,

			&EgressRandomSettings{
				dropProbabilityPerSecond: 0.01,
				dropMin: 60 * time.Second,
				dropMax: 3600 * time.Second,

				blockProbabilityPerDst: 0.01,
				blockDelay: 15 * time.Second,
				blockMin: 60 * time.Second,
				blockMax: 3600 * time.Second,
			},
		)
	}

	netTransferEstimate := func(egressId Id)(int64) {
		return self.egressStatsWindowEstimateNetTransfer
	}

	netTransferToDstEstimate := func(egressId Id)(int64) {
		return self.egressStatsWindowEstimateNetTransferToDst
	}


	stats := NewPacketIntervalWindow(self.packetInterval, self.timeout)

	stateLock := sync.Mutex{}
	egressWindow := []*Egress{}

	contractEgressWindow := func() {
		stateLock.Lock()
		defer stateLock.Unlock()

		if len(egressWindow) <= self.egressWindowSize {
			return
		}

		newEgressWindow := map[*Egress]bool{}
		netTransfer := map[*Egress]int64{}
		for _, egress := range egressWindow {
			newEgressWindow[egress] = true
			t := stats.NetTransfer(egress.EgressId, self.egressStatsWindow)
			if t == 0 {
				t = netTransferEstimate(egress.EgressId)
			}
			netTransfer[egress] = t
		}

		minEgress := slices.MinFunc(maps.Keys(newEgressWindow), func(a *Egress, b *Egress)(int) {
			c := netTransfer[a] - netTransfer[b]
			if c < 0 {
				return -1
			} else if 0 < c {
				return 1
			} else {
				return 0
			}
		})
		delete(newEgressWindow, minEgress)

		egressWindow = maps.Keys(newEgressWindow)
	}

	chooseEgress := func(connectionTuple ConnectionTuple)(*Egress) {
		stateLock.Lock()
		defer stateLock.Unlock()

		// fmt.Printf("CHOOSE EGRESS\n")

		statsConnectionTuples := stats.GetConnectionTuplesForDst(connectionTuple, self.egressStatsWindow)

		targetWindowSize := self.egressWindowSize + (len(statsConnectionTuples) / self.egressWindowExpandReconnectCount) * self.egressWindowExpandStep

		fmt.Printf("Target window size %d\n", targetWindowSize)

		for len(egressWindow) < targetWindowSize {
			fmt.Printf("Expand window size\n")
			egressWindow = append(egressWindow, newEgress())
		}

		netTransfer := map[Id]int64{}
		net := int64(0)
		netTransferToDst := map[Id]int64{}
		netToDst := int64(0)
		for _, egress := range egressWindow {
			t := stats.NetTransfer(egress.EgressId, self.egressStatsWindow)
			if t == 0 {
				t = netTransferEstimate(egress.EgressId)
			}
			netTransfer[egress.EgressId] = t
			net += t

			tToDst := stats.NetTransferToDst(egress.EgressId, self.egressStatsWindow, connectionTuple)
			if tToDst == 0 {
				tToDst = netTransferToDstEstimate(egress.EgressId)
			}
			netTransferToDst[egress.EgressId] = tToDst
			netToDst += tToDst
		}

		ps := []float64{}
		for _, egress := range egressWindow {
			p := (1.0 - self.dstWeight) * float64(netTransfer[egress.EgressId]) / float64(net) + self.dstWeight * float64(netTransferToDst[egress.EgressId]) / float64(netToDst)	
			ps = append(ps, p)
		}
		fmt.Printf("ps = %s\n", ps)
		r := mathrand.Float64()
		for i, p := range ps {
			r -= p
			if r <= 0 {
				fmt.Printf("Choose [%d]\n", i)
				return egressWindow[i]
			}
		}
		// r was ~ 1 and there was some floating point error
		return egressWindow[len(egressWindow) - 1]
	}

	go func() {
		for {
			select {
			case <- cancelCtx.Done():
				return
			case <- time.After(self.egressWindowContractTimeout):
			}
			contractEgressWindow()
		}
	}()

	doneSender := make(chan *Sender)

	for i := 0; i < self.senderCount; i += 1 {
		sender := NewSender(cancelCtx, stats, self.sendSize, self.sendDuration)
		go func() {
			defer func() {
				doneSender <- sender
			}()
			sender.Run(chooseEgress)
		}()
	}

	endTime := time.Now().Add(self.timeout)
	doneSenders := []*Sender{}
	for len(doneSenders) < self.senderCount {
		select {
		case <- cancelCtx.Done():
			return errors.New("Timeout")
		case <- time.After(endTime.Sub(time.Now())):
			return errors.New("Timeout")
		case sender := <- doneSender:
			doneSenders = append(doneSenders, sender)
		}
	}

	stats.PrintSummary()
	return nil
}



type PacketMeta struct {
	eventTime time.Time
	srcId Id
	dstId Id
	connectionTuple ConnectionTuple
	dst ConnectionTuple
	size int64
}



// TODO track drop events
// TODO track block dst events
type PacketIntervalWindow struct {
	interval time.Duration
	duration time.Duration

	stateLock sync.Mutex
	packetMetas []*PacketMeta
}

func NewPacketIntervalWindow(interval time.Duration, duration time.Duration) *PacketIntervalWindow {
	return &PacketIntervalWindow{
		interval: interval,
		duration: duration,
	}
}

func (self *PacketIntervalWindow) AddPacket(eventTime time.Time, srcId Id, dstId Id, connectionTuple ConnectionTuple, size int64) {
	// fmt.Printf("Packet\n")
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.packetMetas = append(self.packetMetas, &PacketMeta{
		eventTime: eventTime,
		srcId: srcId,
		dstId: dstId,
		connectionTuple: connectionTuple,
		dst: connectionTuple.Dst(),
		size: size,
	})
	
}

func (self *PacketIntervalWindow) NetTransfer(egressId Id, egressStatsWindow time.Duration) int64 {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	endTime := time.Now()
	startTime := endTime.Add(-egressStatsWindow)

	net := int64(0)
	for _, packetMeta := range self.packetMetas {
		if packetMeta.dstId == egressId && !startTime.After(packetMeta.eventTime) && packetMeta.eventTime.Before(endTime) {
			net += packetMeta.size
		}
	}

	return net
}

func (self *PacketIntervalWindow) NetTransferToDst(egressId Id, egressStatsWindow time.Duration, connectionTuple ConnectionTuple) int64 {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	dst := connectionTuple.Dst()

	endTime := time.Now()
	startTime := endTime.Add(-egressStatsWindow)

	netToDst := int64(0)
	for _, packetMeta := range self.packetMetas {
		if packetMeta.dstId == egressId && dst == packetMeta.dst && !startTime.After(packetMeta.eventTime) && packetMeta.eventTime.Before(endTime) {
			netToDst += packetMeta.size
		}
	}

	return netToDst
}

func (self *PacketIntervalWindow) GetConnectionTuplesForDst(connectionTuple ConnectionTuple, egressStatsWindow time.Duration) []ConnectionTuple {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	dst := connectionTuple.Dst()

	endTime := time.Now()
	startTime := endTime.Add(-egressStatsWindow)

	connectionTuples := map[ConnectionTuple]bool{}
	for _, packetMeta := range self.packetMetas {
		if !startTime.After(packetMeta.eventTime) && packetMeta.eventTime.Before(endTime) && dst == packetMeta.dst {
			connectionTuples[packetMeta.connectionTuple] = true
		}
	}

	return maps.Keys(connectionTuples)
}

func (self *PacketIntervalWindow) PrintSummary() {
	fmt.Printf("Done\n")
}


