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


	statsWindowSim := &StatisticalHopWindow{
		ctx: ctx,

		timeout: timeout,
		packetInterval: packetInterval,

		senderCount: senderCount,
		sendSize: sendSize,
		sendDuration: sendDuration,

		egressWindowSize: 5,
		egressStatsWindow: 500 * time.Millisecond,
		// TODO use a fraction of the total transfer in the stats window
		egressStatsWindowEstimateNetTransfer: 1,
		// TODO use a fraction of the total transfer in the stats window
		egressStatsWindowEstimateNetTransferToDst: 1,
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

func (self *ConnectionTuple) DstTuple() ConnectionTuple {
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
		readTimeout: 40 * time.Millisecond,
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
	var out chan *Packet
	var in chan *Packet
	var egressId Id

	connect := func() {
		if out != nil {
			close(out)
		}

		connectionTuple.SrcPort += 1
		// fmt.Printf("Get egress start\n")
		egress := connectEgress(connectionTuple)
		// fmt.Printf("Get egress done\n")
		egressId = egress.EgressId

		out = make(chan *Packet)
		in = make(chan *Packet)
		egress.Connect(connectionTuple, out, in)
	}

	// fmt.Printf("Connect start\n")
	connect()
	// fmt.Printf("Connect done\n")

	for i := 0; i < self.size; i += 1 {

		readTimeoutTime := time.Now().Add(self.readTimeout)

		var sendTime time.Time
		send := func() {
			packet := NewPacket(i)
			self.stats.AddPacket(time.Now(), clientId, egressId, connectionTuple, 1)
			select {
			case <- self.ctx.Done():
			case <- time.After(readTimeoutTime.Sub(time.Now())):
			case out <- packet:
				sendTime = time.Now()
			}
		}

		send()


		ack := false
		for !ack {
			resendTime := sendTime.Add(self.resendTimeout)
			select {
			case <- self.ctx.Done():
				return
			case packet := <- in:
				if packet.Index == i {
					ack = true
					self.stats.AddPacket(time.Now(), egressId, clientId, connectionTuple.Reverse(), 1)
				}
			case <- time.After(resendTime.Sub(time.Now())):
				send()
			case <- time.After(readTimeoutTime.Sub(time.Now())):
				// reconnect
				fmt.Printf("Reconnect\n")
				i = 0
				connect()
			}
		}

		select {
		case <- self.ctx.Done():
			return
		case <- time.After(self.delay):
		}
	}

	if out != nil {
		close(out)
	}

}


type Egress struct {
	ctx context.Context

	EgressId Id

	forever time.Duration

	dropProbabilityPerPacket float64
	dropMin time.Duration
	dropMax time.Duration

	blockProbabilityPerDst float64
	blockDelay time.Duration
	blockMin time.Duration
	blockMax time.Duration
}

// TODO
func NewEgress() {

	// TODO
	// evaluates whether the egress is dropped
	// runs every second, need to change dropProbabilityPerSecond
	// go egress.drop()
}

// type egress struct
// 
func (self *Egress) Connect(
	connectionTuple ConnectionTuple,
	in chan *Packet,
	out chan *Packet,
) {
	// fmt.Printf("CONNECT\n")
	go func() {
		defer close(out)

		// TODO the block state and the drop state need to be on the object
		// TODO not connection local

		testDropPerPacket := func()(drop bool, startTime time.Time, endTime time.Time) {
			if mathrand.Float64() < self.dropProbabilityPerPacket {
				drop = true

				startTime = time.Now()
				endTime = startTime.Add(
					time.Duration(mathrand.Int63n(int64((self.dropMax - self.dropMin) / time.Second))) * time.Second,
				)
			}
			return
		}

		testBlockPerDst := func()(block bool, startTime time.Time, endTime time.Time) {
			if mathrand.Float64() < self.blockProbabilityPerDst {
				block = true

				startTime = time.Now().Add(
					time.Duration(mathrand.Int63n(int64(self.blockDelay / time.Second))) * time.Second,
				)
				endTime = startTime.Add(
					time.Duration(mathrand.Int63n(int64((self.blockMax - self.blockMin) / time.Second))) * time.Second,
				)
			}
			return
		}

		block, blockStartTime, blockEndTime := testBlockPerDst()
		var drop bool
		var dropStartTime time.Time
		var dropEndTime time.Time

		for {
			var blockedTimeout time.Duration
			var blocked bool
			if block {
				startTimeout := time.Now().Sub(blockStartTime)
				endTimeout := time.Now().Sub(blockEndTime)
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
			if drop {
				startTimeout := time.Now().Sub(dropStartTime)
				endTimeout := time.Now().Sub(dropEndTime)
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

			select {
			case <- self.ctx.Done():
				return
			case <- time.After(blockedTimeout):
			case <- time.After(droppedTimeout):
			case packet, ok := <- in:
				// fmt.Printf("GOT A PACKET\n")
				if !ok {
					return
				}
				if blocked || dropped {
					// drop it
				} else if drop, dropStartTime, dropEndTime = testDropPerPacket(); drop {
					// drop it
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
		return &Egress{
			ctx: cancelCtx,

			EgressId: NewId(),
			forever: self.timeout,

			dropProbabilityPerPacket: 0.00001,
			dropMin: 60 * time.Second,
			dropMax: 3600 * time.Second,

			blockProbabilityPerDst: 0.001,
			blockDelay: 15 * time.Second,
			blockMin: 60 * time.Second,
			blockMax: 3600 * time.Second,
		}
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
		for self.egressWindowSize < len(egressWindow) {
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
		}
		egressWindow = maps.Keys(newEgressWindow)
	}

	chooseEgress := func(connectionTuple ConnectionTuple)(*Egress) {
		stateLock.Lock()
		defer stateLock.Unlock()

		// fmt.Printf("CHOOSE EGRESS\n")

		statsConnectionTuples := stats.GetConnectionTuplesForDst(connectionTuple, self.egressStatsWindow)

		targetWindowSize := self.egressWindowSize + (len(statsConnectionTuples) / self.egressWindowExpandReconnectCount) * self.egressWindowExpandStep

		for len(egressWindow) < targetWindowSize {
			fmt.Printf("Expand window size\n")
			// add S new egress
			for i := 0; i < self.egressWindowExpandStep; i += 1 {
				egressWindow = append(egressWindow, newEgress())
			}
		}

		netTransfer := map[*Egress]int64{}
		net := int64(0)
		netTransferToDst := map[*Egress]int64{}
		netToDst := int64(0)
		for _, egress := range egressWindow {
			t := stats.NetTransfer(egress.EgressId, self.egressStatsWindow)
			if t == 0 {
				t = netTransferEstimate(egress.EgressId)
			}
			netTransfer[egress] = t
			net += t

			tToDst := stats.NetTransferToDst(egress.EgressId, self.egressStatsWindow, connectionTuple)
			if tToDst == 0 {
				tToDst = netTransferToDstEstimate(egress.EgressId)
			}
			netTransferToDst[egress] = tToDst
			netToDst += tToDst
		}

		ps := []float64{}
		for _, egress := range egressWindow {
			p := (1.0 - self.dstWeight) * float64(netTransfer[egress]) / float64(net) + self.dstWeight * float64(netTransferToDst[egress]) / float64(netToDst)	
			ps = append(ps, p)
		}
		fmt.Printf("ps = %s\n", ps)
		r := mathrand.Float64()
		for i, p := range ps {
			r -= p
			if r <= 0 {
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




// TODO track drop events
// TODO track block dst events
type PacketIntervalWindow struct {
	interval time.Duration
	duration time.Duration
}

func NewPacketIntervalWindow(interval time.Duration, duration time.Duration) *PacketIntervalWindow {
	return &PacketIntervalWindow{
		interval: interval,
		duration: duration,
	}
}

func (self *PacketIntervalWindow) AddPacket(eventTime time.Time, srcId Id, dstId Id, connectionTuple ConnectionTuple, size int) {
	// fmt.Printf("Packet\n")
}

func (self *PacketIntervalWindow) NetTransfer(egressId Id, egressStatsWindow time.Duration) int64 {
	return 0
}

func (self *PacketIntervalWindow) NetTransferToDst(egressId Id, egressStatsWindow time.Duration, connectionTuple ConnectionTuple) int64 {
	return 0
}

func (self *PacketIntervalWindow) GetConnectionTuplesForDst(connectionTuple ConnectionTuple, egressStatsWindow time.Duration) []ConnectionTuple {
	return []ConnectionTuple{}
}

func (self *PacketIntervalWindow) PrintSummary() {
	fmt.Printf("Done\n")
}


