package main

// for this sim, the client sends a number to the egress, and the egress echos the number back

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	mathrand "math/rand"
	"os"
	"runtime"
	"slices"
	"sync"
	"time"

	"golang.org/x/exp/maps"
)

const schdulerEpoch = 100 * time.Millisecond
const maxRtt = 1 * time.Second

func main() {
	ctx := context.Background()

	timeout := 300 * time.Second
	// this is a tradeoff between performance and stats accuracy
	packetInterval := 50 * time.Millisecond
	senderCount := 100
	sendSize := 1000
	sendDuration := 5 * time.Second

	runtime.GOMAXPROCS(4 * senderCount)

	egressStatsWindow := 15 * time.Second
	egressStatsReconnectWindow := 120 * time.Second
	dropProbabilityPerSend := 0.2
	blockProbabilityPerDst := 0.75
	egressInitialCapacityWeight := 0.1
	egressInitialCapacityToDstWeight := 0.1

	statsWindowSim := &StatisticalHopWindow{
		ctx: ctx,

		timeout:        timeout,
		packetInterval: packetInterval,

		senderCount:  senderCount,
		sendSize:     sendSize,
		sendDuration: sendDuration,

		egressWindowSize: 5,
		// TODO have a sweep of this that looks for no activity in past N time and removes down to initial size
		egressWindowMaxSize:        10,
		egressStatsWindow:          egressStatsWindow,
		egressStatsReconnectWindow: egressStatsReconnectWindow,
		// expressed as a fraction of maximum transfer
		egressStatsWindowEstimateNetTransfer: int64(egressInitialCapacityWeight * float64(sendSize) * float64(egressStatsWindow) / float64(sendDuration)),
		// expressed as a fraction of maximum transfer
		egressStatsWindowEstimateNetTransferToDst: int64(egressInitialCapacityToDstWeight * float64(sendSize) * float64(egressStatsWindow) / float64(sendDuration)),
		dstWeight: 0.75,

		egressWindowContractTimeout:      1 * time.Second,
		egressWindowContractGracePeriod:  5 * time.Second,
		egressWindowExpandReconnectCount: 3,
		egressWindowExpandStep:           2,

		rand: &EgressRandomSettings{
			// p = 1 - pow(1 - K, sendDuration / time.Second)
			// K = 1 - (1 - p)^(time.Second / sendDuration)
			dropProbabilityPerSecond: 1 - math.Pow(
				1-dropProbabilityPerSend,
				float64(time.Second)/float64(sendDuration),
			),
			dropMin: 60 * time.Second,
			dropMax: 3600 * time.Second,

			blockProbabilityPerDst: blockProbabilityPerDst,
			blockDelay:             3 * time.Second,
			blockMin:               60 * time.Second,
			blockMax:               3600 * time.Second,
		},
	}

	if err := statsWindowSim.Run(); err != nil {
		panic(err)
	}

}

type EgressFunction func(connectionTuple ConnectionTuple) *Egress

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
	SrcIp   Id  `json:"src,omitempty"`
	SrcPort int `json:"src_port,omitempty"`
	DstIp   Id  `json:"dst,omitempty"`
	DstPort int `json:"dst_port,omitempty"`
}

func NewConnectionTuple(srcIp Id, srcPort int, dstIp Id, dstPort int) ConnectionTuple {
	return ConnectionTuple{
		SrcIp:   srcIp,
		SrcPort: srcPort,
		DstIp:   dstIp,
		DstPort: dstPort,
	}
}

func (self *ConnectionTuple) Reverse() ConnectionTuple {
	return ConnectionTuple{
		SrcIp:   self.DstIp,
		SrcPort: self.DstPort,
		DstIp:   self.SrcIp,
		DstPort: self.SrcPort,
	}
}

func (self *ConnectionTuple) Dst() ConnectionTuple {
	return ConnectionTuple{
		SrcIp:   0,
		SrcPort: 0,
		DstIp:   self.DstIp,
		DstPort: self.DstPort,
	}
}

type Sender struct {
	ctx context.Context

	stats *PacketIntervalWindow

	size  int
	delay time.Duration

	readTimeout time.Duration

	// no resend needed. the connection is perfect.
	// resendTimeout time.Duration
}

func NewSender(
	ctx context.Context,
	stats *PacketIntervalWindow,
	size int,
	sendDuration time.Duration,
) *Sender {
	// delay := sendDuration / time.Duration(size)
	return &Sender{
		ctx:         ctx,
		stats:       stats,
		size:        size,
		delay:       sendDuration / time.Duration(size),
		readTimeout: time.Second,
		// resendTimeout: delay / 2,
	}
}

func (self *Sender) Run(connectEgress EgressFunction) {
	// ip tuple of where the data is being sent
	clientId := NewId()

	connectionTuple := NewConnectionTuple(
		NewId(), 1,
		NewId(), 1,
	)

	self.stats.AddEvent(&EventMeta{
		eventTime: time.Now(),
		eventType: EventTypeSenderStart,
		clientId:  clientId,
		dst:       connectionTuple.Dst(),
	})

	connect := func() (out chan *Packet, in chan *Packet, egressId Id) {
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
				self.stats.AddPacket(&PacketMeta{
					eventTime:       sendTime,
					srcClientId:     clientId,
					dstClientId:     egressId,
					connectionTuple: connectionTuple,
					dst:             connectionTuple.Dst(),
					size:            1,
					index:           i,
					seqSize:         self.size,
				})
				select {
				case <-self.ctx.Done():
				case <-time.After(readTimeoutTime.Sub(time.Now())):
				case out <- packet:
				}
			}

			send()

			ack := false
			for !ack {
				// resendTime := sendTime.Add(self.resendTimeout)
				select {
				case <-self.ctx.Done():
					return
				case packet, ok := <-in:
					if !ok {
						// reconnect
						fmt.Printf("Reconnect 1\n")
						closeOut()
						sendSeq()
						return
					} else if packet.Index == i {
						ack = true
						ackTime := time.Now()
						rtt := ackTime.Sub(sendTime)
						// fmt.Printf("Found RTT %dns\n", rtt / time.Nanosecond)
						if rtt < schdulerEpoch {
							rtt = schdulerEpoch
						} else if maxRtt < rtt {
							rtt = maxRtt
						}
						// self.resendTimeout = rtt
						self.readTimeout = 2 * rtt
						reverseConnectionTuple := connectionTuple.Reverse()
						self.stats.AddPacket(&PacketMeta{
							eventTime:       ackTime,
							srcClientId:     egressId,
							dstClientId:     clientId,
							connectionTuple: reverseConnectionTuple,
							dst:             reverseConnectionTuple.Dst(),
							size:            1,
							index:           i,
							seqSize:         self.size,
						})
					}
				// case <- time.After(resendTime.Sub(time.Now())):
				// 	send()
				case <-time.After(readTimeoutTime.Sub(time.Now())):
					// reconnect
					fmt.Printf("Reconnect 2\n")
					closeOut()
					sendSeq()
					return
				}
			}

			select {
			case <-self.ctx.Done():
				return
			case <-time.After(self.delay):
			}
		}

	}

	sendSeq()

	self.stats.AddEvent(&EventMeta{
		eventTime: time.Now(),
		eventType: EventTypeSenderEnd,
		clientId:  clientId,
		dst:       connectionTuple.Dst(),
	})

}

type BlackholeState struct {
	Active    bool
	StartTime time.Time
	EndTime   time.Time
}

type EgressRandomSettings struct {
	dropProbabilityPerSecond float64
	dropMin                  time.Duration
	dropMax                  time.Duration

	blockProbabilityPerDst float64
	blockDelay             time.Duration
	blockMin               time.Duration
	blockMax               time.Duration
}

type Egress struct {
	ctx    context.Context
	cancel context.CancelFunc

	EgressId Id

	forever time.Duration

	rand  *EgressRandomSettings
	stats *PacketIntervalWindow

	CreateTime time.Time

	stateLock sync.Mutex
	drop      BlackholeState
	blockDst  map[ConnectionTuple]BlackholeState
}

func NewEgress(
	ctx context.Context,
	egressId Id,
	forever time.Duration,
	rand *EgressRandomSettings,
	stats *PacketIntervalWindow,
) *Egress {
	cancelCtx, cancel := context.WithCancel(ctx)

	egress := &Egress{
		ctx:      cancelCtx,
		cancel:   cancel,
		EgressId: egressId,

		forever: forever,

		rand:  rand,
		stats: stats,

		CreateTime: time.Now(),

		stateLock: sync.Mutex{},
		drop:      BlackholeState{},
		blockDst:  map[ConnectionTuple]BlackholeState{},
	}

	go func() {
		previousEvalTime := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
				t := time.Now()
				egress.maybeDrop(t.Sub(previousEvalTime))
				previousEvalTime = t
			}
		}
	}()

	return egress
}

func (self *Egress) testDrop(elapsed time.Duration) (out BlackholeState) {
	p := 1 - math.Pow(
		1-self.rand.dropProbabilityPerSecond,
		float64(elapsed)/float64(time.Second),
	)

	if mathrand.Float64() < p {
		out.Active = true

		out.StartTime = time.Now()
		out.EndTime = out.StartTime.Add(
			time.Duration(mathrand.Int63n(int64((self.rand.dropMax-self.rand.dropMin)/time.Second))) * time.Second,
		)
	}
	return
}

func (self *Egress) testBlockPerDst() (out BlackholeState) {
	if mathrand.Float64() < self.rand.blockProbabilityPerDst {
		out.Active = true

		out.StartTime = time.Now().Add(
			time.Duration(mathrand.Int63n(int64(self.rand.blockDelay/time.Second))) * time.Second,
		)
		out.EndTime = out.StartTime.Add(
			time.Duration(mathrand.Int63n(int64((self.rand.blockMax-self.rand.blockMin)/time.Second))) * time.Second,
		)
	}
	return
}

func (self *Egress) maybeDrop(elapsed time.Duration) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if !self.drop.Active {
		drop := self.testDrop(elapsed)
		self.drop = drop
		if drop.Active {
			self.stats.AddEvent(&EventMeta{
				eventTime: drop.StartTime,
				eventType: EventTypeEgressDropStart,
				clientId:  self.EgressId,
			})
			self.stats.AddEvent(&EventMeta{
				eventTime: drop.EndTime,
				eventType: EventTypeEgressDropEnd,
				clientId:  self.EgressId,
			})
		}
	}
}

func (self *Egress) maybeBlockPerDst(dst ConnectionTuple) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	block := self.blockDst[dst]
	if !block.Active {
		block := self.testBlockPerDst()
		self.blockDst[dst] = block
		if block.Active {
			self.stats.AddEvent(&EventMeta{
				eventTime: block.StartTime,
				eventType: EventTypeEgressBlockStart,
				clientId:  self.EgressId,
				dst:       dst,
			})
			self.stats.AddEvent(&EventMeta{
				eventTime: block.EndTime,
				eventType: EventTypeEgressBlockEnd,
				clientId:  self.EgressId,
				dst:       dst,
			})
		}
	}
}

// type egress struct
func (self *Egress) Connect(
	connectionTuple ConnectionTuple,
	in chan *Packet,
	out chan *Packet,
	// readTimeout time.Duration,
) {
	// fmt.Printf("CONNECT\n")
	go func() {
		defer close(out)

		self.maybeBlockPerDst(connectionTuple.Dst())

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

			_ = blockedTimeout + droppedTimeout

			select {
			case <-self.ctx.Done():
				return
			case <-time.After(blockedTimeout):
				fmt.Printf("Blocked timeout change\n")
			case <-time.After(droppedTimeout):
				fmt.Printf("Dropped timeout change\n")

			// case <- time.After(readTimeoutTime.Sub(time.Now())):
			// 	fmt.Printf("Read timeout\n")
			// 	return
			case packet, ok := <-in:
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
					case <-self.ctx.Done():
					case out <- packet:
					}
				}
			}
		}
	}()
}

func (self *Egress) Close() {
	self.cancel()
}

type StatisticalHopWindow struct {
	ctx context.Context

	timeout        time.Duration
	packetInterval time.Duration

	senderCount  int
	sendSize     int
	sendDuration time.Duration

	egressWindowSize                          int
	egressWindowMaxSize                       int
	egressStatsWindow                         time.Duration
	egressStatsReconnectWindow                time.Duration
	egressStatsWindowEstimateNetTransfer      int64
	egressStatsWindowEstimateNetTransferToDst int64
	dstWeight                                 float64

	egressWindowContractTimeout      time.Duration
	egressWindowContractGracePeriod  time.Duration
	egressWindowExpandReconnectCount int
	egressWindowExpandStep           int

	rand *EgressRandomSettings
}

// at the end computes amount of data sent / time
func (self *StatisticalHopWindow) Run() error {

	cancelCtx, cancel := context.WithCancel(self.ctx)
	defer cancel()

	stats := NewPacketIntervalWindow(self.packetInterval, self.timeout)

	newEgress := func() *Egress {
		return NewEgress(
			cancelCtx,

			NewId(),
			self.timeout,

			self.rand,
			stats,
		)
	}

	netTransferEstimate := func(egressId Id) int64 {
		return self.egressStatsWindowEstimateNetTransfer
	}

	netTransferToDstEstimate := func(egressId Id) int64 {
		return self.egressStatsWindowEstimateNetTransferToDst
	}

	stateLock := sync.Mutex{}
	egressWindow := []*Egress{}

	// FIXME
	// egressAddedTime := map[Id]time.Time{}
	// egressWindowExpandTime := time.Now()

	contractEgressWindow := func() {
		stateLock.Lock()
		defer stateLock.Unlock()

		if len(egressWindow) <= self.egressWindowMaxSize {
			return
		}

		// if time.Now().Before(egressWindowExpandTime.Add(self.egressWindowContractTimeout)) {
		// 	return
		// }

		fmt.Printf("Contract window size\n")

		now := time.Now()

		newEgressWindow := map[*Egress]bool{}
		netTransfer := map[*Egress]int64{}
		egressEligibleToContract := []*Egress{}
		for _, egress := range egressWindow {
			newEgressWindow[egress] = true

			if egress.CreateTime.Add(self.egressWindowContractGracePeriod).Before(now) {
				egressEligibleToContract = append(egressEligibleToContract, egress)
				t := stats.NetTransfer(egress.EgressId, self.egressStatsWindow)
				// for clean up, use the actual transfer data
				// if t == 0 {
				// 	t = netTransferEstimate(egress.EgressId)
				// }
				netTransfer[egress] = t
			}
		}

		if 0 < len(egressEligibleToContract) {
			minEgress := slices.MinFunc(egressEligibleToContract, func(a *Egress, b *Egress) int {
				c := netTransfer[a] - netTransfer[b]
				if c < 0 {
					return -1
				} else if 0 < c {
					return 1
				}

				d := a.CreateTime.Sub(b.CreateTime)
				if d < 0 {
					return -1
				} else if 0 < d {
					return 1
				}

				return 0
			})
			delete(newEgressWindow, minEgress)

			egressWindow = maps.Keys(newEgressWindow)

			minEgress.Close()

			stats.AddEvent(&EventMeta{
				eventTime: time.Now(),
				eventType: EventTypeWindowContract,
				clientId:  minEgress.EgressId,
			})
		}
	}

	chooseEgress := func(connectionTuple ConnectionTuple) *Egress {
		stateLock.Lock()
		defer stateLock.Unlock()

		dst := connectionTuple.Dst()

		// fmt.Printf("CHOOSE EGRESS\n")

		egressIds := map[Id]bool{}
		for _, egress := range egressWindow {
			egressIds[egress.EgressId] = true
		}

		statsConnectionTuples := stats.GetConnectionTuplesForDst(dst, egressIds, self.egressStatsReconnectWindow)

		targetWindowSize := self.egressWindowSize + (len(statsConnectionTuples)/self.egressWindowExpandReconnectCount)*self.egressWindowExpandStep

		fmt.Printf("Target window size (%d) %d\n", len(statsConnectionTuples), targetWindowSize)

		for len(egressWindow) < targetWindowSize {
			fmt.Printf("Expand window size\n")
			egress := newEgress()
			egressWindow = append(egressWindow, egress)
			// egressWindowExpandTime = time.Now()
			// egressAddedTime[egress.EgressId] = time.Now()
			stats.AddEvent(&EventMeta{
				eventTime: time.Now(),
				eventType: EventTypeWindowExpand,
				clientId:  egress.EgressId,
			})
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

			tToDst := stats.NetTransferToDst(egress.EgressId, self.egressStatsWindow, dst)
			if tToDst == 0 {
				tToDst = netTransferToDstEstimate(egress.EgressId)
			}
			netTransferToDst[egress.EgressId] = tToDst
			netToDst += tToDst
		}

		ps := []float64{}
		if 0 < net && 0 < netToDst {
			for _, egress := range egressWindow {
				pNet := float64(netTransfer[egress.EgressId]) / float64(net)
				pNetTpDst := float64(netTransferToDst[egress.EgressId]) / float64(netToDst)
				p := (1-self.dstWeight)*pNet + self.dstWeight*pNetTpDst
				ps = append(ps, p)
			}
		} else {
			for i := 0; i < len(egressWindow); i += 1 {
				p := 1 / float64(len(egressWindow))
				ps = append(ps, p)
			}
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
		return egressWindow[len(egressWindow)-1]
	}

	go func() {
		for {
			select {
			case <-cancelCtx.Done():
				return
			case <-time.After(self.egressWindowContractTimeout):
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
		case <-cancelCtx.Done():
			return errors.New("Timeout")
		case <-time.After(endTime.Sub(time.Now())):
			return errors.New("Timeout")
		case sender := <-doneSender:
			doneSenders = append(doneSenders, sender)
		}
	}

	stats.AddEvent(&EventMeta{
		eventTime: time.Now(),
		eventType: EventTypeSimEnd,
	})

	stats.PrintSummary()

	export := stats.Export()
	fmt.Printf("Exported %d packets, %d events.\n", len(export.Packets), len(export.Events))
	if exportBytes, err := json.Marshal(export); err == nil {
		if err := os.WriteFile("export.json", exportBytes, 0777); err != nil {
			panic(err)
		}
	}

	return nil
}

type PacketMeta struct {
	eventTime       time.Time
	srcClientId     Id
	dstClientId     Id
	connectionTuple ConnectionTuple
	dst             ConnectionTuple
	index           int
	size            int64
	seqSize         int
}

const (
	EventTypeSenderStart      string = "sender-start"
	EventTypeSenderEnd        string = "sender-end"
	EventTypeWindowExpand     string = "expand"
	EventTypeWindowContract   string = "contract"
	EventTypeEgressDropStart  string = "drop-start"
	EventTypeEgressDropEnd    string = "drop-end"
	EventTypeEgressBlockStart string = "block-start"
	EventTypeEgressBlockEnd   string = "block-end"
	EventTypeSimEnd           string = "sim-end"
)

type EventMeta struct {
	eventTime time.Time
	// eventEndTime time.Time
	eventType string
	clientId  Id
	dst       ConnectionTuple
}

type PacketMetaExport struct {
	EventTimeOffsetMillis int64           `json:"event_time_offset_millis"`
	SrcClientId           Id              `json:"src_client_id,omitempty"`
	DstClientId           Id              `json:"dst_client_id,omitempty"`
	ConnectionTuple       ConnectionTuple `json:"connection_tuple,omitempty"`
	Index                 int             `json:"index"`
	Size                  int64           `json:"size"`
	SeqSize               int             `json:"seq_size"`
}

type EventMetaExport struct {
	EventTimeOffsetMillis int64 `json:"event_time_offset_millis"`
	// EventDuration time.Duration `json:"event_duration,omitempty"`
	EventType string          `json:"event_type,omitempty"`
	ClientId  Id              `json:"client_id,omitempty"`
	Dst       ConnectionTuple `json:"dst,omitempty"`
}

type PacketIntervalWindowExport struct {
	Packets []*PacketMetaExport `json:"packets,omitempty"`
	Events  []*EventMetaExport  `json:"events,omitempty"`
}

type PacketIntervalWindow struct {
	startTime time.Time
	interval  time.Duration
	duration  time.Duration

	stateLock   sync.Mutex
	packetMetas []*PacketMeta
	eventMetas  []*EventMeta
}

func NewPacketIntervalWindow(interval time.Duration, duration time.Duration) *PacketIntervalWindow {
	return &PacketIntervalWindow{
		startTime: time.Now(),
		interval:  interval,
		duration:  duration,
	}
}

func (self *PacketIntervalWindow) AddPacket(packetMeta *PacketMeta) {
	// fmt.Printf("Packet\n")
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.packetMetas = append(self.packetMetas, packetMeta)
}

func (self *PacketIntervalWindow) AddEvent(eventMeta *EventMeta) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.eventMetas = append(self.eventMetas, eventMeta)
}

func (self *PacketIntervalWindow) NetTransfer(egressId Id, egressStatsWindow time.Duration) int64 {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	endTime := time.Now()
	startTime := endTime.Add(-egressStatsWindow)

	net := int64(0)
	for _, packetMeta := range self.packetMetas {
		if packetMeta.dstClientId == egressId && !startTime.After(packetMeta.eventTime) && packetMeta.eventTime.Before(endTime) {
			net += packetMeta.size
		}
	}

	return net
}

func (self *PacketIntervalWindow) NetTransferToDst(egressId Id, egressStatsWindow time.Duration, dst ConnectionTuple) int64 {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	endTime := time.Now()
	startTime := endTime.Add(-egressStatsWindow)

	netToDst := int64(0)
	for _, packetMeta := range self.packetMetas {
		if packetMeta.dstClientId == egressId && dst == packetMeta.dst && !startTime.After(packetMeta.eventTime) && packetMeta.eventTime.Before(endTime) {
			netToDst += packetMeta.size
		}
	}

	return netToDst
}

func (self *PacketIntervalWindow) GetConnectionTuplesForDst(dst ConnectionTuple, egressIds map[Id]bool, egressStatsWindow time.Duration) []ConnectionTuple {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	endTime := time.Now()
	startTime := endTime.Add(-egressStatsWindow)

	connectionTuples := map[ConnectionTuple]bool{}
	for _, packetMeta := range self.packetMetas {
		if !startTime.After(packetMeta.eventTime) && packetMeta.eventTime.Before(endTime) && dst == packetMeta.dst && egressIds[packetMeta.dstClientId] {
			connectionTuples[packetMeta.connectionTuple] = true
		}
	}

	return maps.Keys(connectionTuples)
}

func (self *PacketIntervalWindow) PrintSummary() {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	fmt.Printf(
		"Done. %d packets. %d events.\n",
		len(self.packetMetas),
		len(self.eventMetas),
	)
}

func (self *PacketIntervalWindow) Export() *PacketIntervalWindowExport {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	packets := []*PacketMetaExport{}
	for _, packetMeta := range self.packetMetas {
		packet := &PacketMetaExport{
			EventTimeOffsetMillis: int64(packetMeta.eventTime.Sub(self.startTime) / time.Millisecond),
			SrcClientId:           packetMeta.srcClientId,
			DstClientId:           packetMeta.dstClientId,
			ConnectionTuple:       packetMeta.connectionTuple,
			Index:                 packetMeta.index,
			Size:                  packetMeta.size,
			SeqSize:               packetMeta.seqSize,
		}
		packets = append(packets, packet)
	}
	slices.SortStableFunc(packets, func(a *PacketMetaExport, b *PacketMetaExport) int {
		c := a.EventTimeOffsetMillis - b.EventTimeOffsetMillis
		if c < 0 {
			return -1
		} else if 0 < c {
			return 1
		} else {
			return 0
		}
	})

	events := []*EventMetaExport{}
	for _, eventMeta := range self.eventMetas {
		event := &EventMetaExport{
			EventTimeOffsetMillis: int64(eventMeta.eventTime.Sub(self.startTime) / time.Millisecond),
			// EventDuration: eventMeta.eventEndTime.Sub(eventMeta.eventTime),
			EventType: eventMeta.eventType,
			ClientId:  eventMeta.clientId,
			Dst:       eventMeta.dst,
		}
		events = append(events, event)
	}
	slices.SortStableFunc(events, func(a *EventMetaExport, b *EventMetaExport) int {
		c := a.EventTimeOffsetMillis - b.EventTimeOffsetMillis
		if c < 0 {
			return -1
		} else if 0 < c {
			return 1
		} else {
			return 0
		}
	})

	return &PacketIntervalWindowExport{
		Packets: packets,
		Events:  events,
	}
}
