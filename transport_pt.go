package connect

import (
	"context"
	"fmt"
	"net"
	"os"
	"slices"
	"sync"
	"time"
	// "runtime/debug"

	mathrand "math/rand"

	"golang.org/x/crypto/sha3"
	"golang.org/x/net/dns/dnsmessage"
)

// "whodis" protocol refers to a suite of pre-authentication packet translation techniques
// that use various side channels that are open on networks without authentication

// packet transformation converts a quic packet to other represenations
// pt does not implement any delivery guarantees

// dns:
// convert to dns txt record requests and responses
// each response is paired uniquely with the latest request
// the client pumps empty requests at a constant rate to give the server requests to pair with
// in the case of a dns proxy,
//   the server is set up as a dns zone, so that each request is ultimately sent
//   to the server, via the dns backbone if necessary

// there is one master translation with several modes because all the translations must share the same ports

// the dns pt header is 18 bytes: [16 random][1 count][1 index]

type PacketTranslationMode string

const (
	PacketTranslationModeNone PacketTranslationMode = ""
	// form packets to look like dns requests/responses on the wire
	PacketTranslationModeDns PacketTranslationMode = "dns"
	// uses a constant amount of upload bandwidth to establish a reply pump via dns zones
	PacketTranslationModeDnsPump                PacketTranslationMode = "dnspump"
	PacketTranslationModeDecode53               PacketTranslationMode = "decode53"
	PacketTranslationModeDecode53RequireDnsPump PacketTranslationMode = "decode53pumpreq"
)

// note the caller must set DnsTld
func DefaultPacketTranslationSettings() *PacketTranslationSettings {
	return &PacketTranslationSettings{
		DnsTlds: [][]byte{},
		// a good baseline is 100 pumps per second
		DnsPumpTimeout: 1 * time.Second / time.Duration(100),
		// DnsReadTimeout: 1 * time.Second:
		DnsStateTimeout: 5 * time.Second,

		DnsMaxCombinePerAddress:       64,
		DnsMaxCombine:                 512,
		DnsMaxCombineFragmentCount:    32,
		DnsMaxCombinedPacketByteCount: 64 * 1024,
		DnsMaxCombineBytesPerAddress:  MemoryScaledByteCount(kib(256), kib(64)),
		DnsMaxCombineBytes:            MemoryScaledByteCount(mib(2), kib(512)),

		DnsMaxPumpHostsPerAddress: MemoryScaledCount(1024, 256),
		DnsMaxPumpHosts:           int64(MemoryScaledCount(8192, 2048)),

		WritePacketsPerSecond: 200,
		SequenceBufferSize:    32,
	}
}

type PacketTranslationSettings struct {
	// Log, when set, is used by the packet translation.
	// nil resolves to `DefaultLogger()`.
	Log Logger

	DnsTlds [][]byte
	// DnsAddr        *net.UDPAddr
	DnsPumpTimeout time.Duration
	// DnsReadTimeout time.Duration
	DnsStateTimeout time.Duration

	DnsMaxCombinePerAddress int
	DnsMaxCombine           int64
	// The count limits above bound map/heap cardinality. These limits also
	// bound attacker-controlled fragment fanout and retained pooled capacity.
	DnsMaxCombineFragmentCount    int
	DnsMaxCombinedPacketByteCount ByteCount
	DnsMaxCombineBytesPerAddress  ByteCount
	DnsMaxCombineBytes            ByteCount

	DnsMaxPumpHostsPerAddress int
	DnsMaxPumpHosts           int64

	WritePacketsPerSecond int
	SequenceBufferSize    int

	// DnsOtherHandler, when set, receives every well-formed query on a decode53
	// socket that is not part of the translation (EXTENDER.md A6). The query
	// bytes are a copy the handler may retain, and the address is the datagram
	// source to answer on. It runs on the decoder's read loop and must not
	// block: the extender's forwarder admits the query and hands it to its own
	// workers. Nil ignores such queries, which is the behavior before A6.
	DnsOtherHandler func(query []byte, addr net.Addr)
}

type packet struct {
	data        []byte
	addr        net.Addr
	writeResult chan error
}

// finishPacketWrite publishes the wire disposition for a logical PacketConn
// write. The channel is buffered because a deadline or cancellation may have
// already released the caller while the encoder still owned the packet copy.
func finishPacketWrite(p *packet, err error) {
	if p.writeResult != nil {
		p.writeResult <- err
	}
}

// implements PacketConn
type packetTranslation struct {
	ctx    context.Context
	cancel context.CancelFunc
	log    Logger

	operationLock   sync.Mutex
	operationClosed bool
	operationWg     sync.WaitGroup
	workerWg        sync.WaitGroup
	closeOnce       sync.Once
	closeErr        error

	ptMode       PacketTranslationMode
	packetConn   net.PacketConn
	headerPrefix []byte

	settings *PacketTranslationSettings

	dnsClient      bool
	dnsPumpQueue   *pumpQueue
	dnsRequirePump bool

	in      chan *packet
	out     chan *packet
	forward chan *packet

	deadlineLock         sync.Mutex
	readDeadline         time.Time
	readDeadlineWakeup   bool
	writeDeadline        time.Time
	readDeadlineMonitor  *Monitor
	writeDeadlineMonitor *Monitor

	deadlineAfterForTest func(time.Duration) <-chan time.Time
}

func NewPacketTranslation(
	ctx context.Context,
	ptMode PacketTranslationMode,
	packetConn net.PacketConn,
	settings *PacketTranslationSettings,
) (*packetTranslation, error) {
	if settings.DnsStateTimeout <= 0 {
		return nil, fmt.Errorf("DNS state timeout must be positive")
	}
	if settings.DnsMaxCombinePerAddress <= 0 || settings.DnsMaxCombine <= 0 ||
		settings.DnsMaxCombineFragmentCount <= 0 ||
		settings.DnsMaxCombinedPacketByteCount <= 0 ||
		settings.DnsMaxCombineBytesPerAddress <= 0 || settings.DnsMaxCombineBytes <= 0 {
		return nil, fmt.Errorf("DNS combine limits must be positive")
	}
	if settings.DnsMaxPumpHostsPerAddress <= 0 || settings.DnsMaxPumpHosts <= 0 {
		return nil, fmt.Errorf("DNS pump limits must be positive")
	}
	return NewPacketTranslationWithPrefix(
		ctx,
		ptMode,
		packetConn,
		settings,
		nil,
	)
}

// the caller should close in when done
func NewPacketTranslationWithPrefix(
	ctx context.Context,
	ptMode PacketTranslationMode,
	packetConn net.PacketConn,
	settings *PacketTranslationSettings,
	headerPrefix []byte,
) (*packetTranslation, error) {
	for _, tld := range settings.DnsTlds {
		if len(tld) == 0 || tld[len(tld)-1] != '.' || tld[0] == '.' {
			return nil, fmt.Errorf("TLD must be canonical (end with a ., not start with a .): %s", string(tld))
		}
	}

	cancelCtx, cancel := context.WithCancel(ctx)

	pt := &packetTranslation{
		ctx: cancelCtx,
		cancel: func() {
			// debug.PrintStack()
			cancel()
		},
		log:                  loggerOrDefault(settings.Log),
		ptMode:               ptMode,
		packetConn:           packetConn,
		headerPrefix:         headerPrefix,
		settings:             settings,
		in:                   make(chan *packet, settings.SequenceBufferSize),
		out:                  make(chan *packet, settings.SequenceBufferSize),
		forward:              make(chan *packet, settings.SequenceBufferSize),
		readDeadlineMonitor:  NewMonitor(),
		writeDeadlineMonitor: NewMonitor(),
	}

	switch ptMode {
	case PacketTranslationModeDns, PacketTranslationModeDnsPump:
		pt.dnsClient = true
		pt.startWorker(pt.encodeDns)
		pt.startWorker(pt.decodeDns)
	case PacketTranslationModeDecode53:
		pt.dnsClient = false
		pt.dnsPumpQueue = newPumpQueue(settings)
		pt.startWorker(pt.encodeDns)
		pt.startWorker(pt.decodeDns)
	case PacketTranslationModeDecode53RequireDnsPump:
		pt.dnsClient = false
		pt.dnsPumpQueue = newPumpQueue(settings)
		pt.dnsRequirePump = true
		pt.startWorker(pt.encodeDns)
		pt.startWorker(pt.decodeDns)
	default:
		cancel()
		return nil, fmt.Errorf("Unsupported packet translation mode: %s", ptMode)
	}
	context.AfterFunc(cancelCtx, func() {
		_ = pt.close()
	})

	return pt, nil
}

// startWorker accounts for every top-level owner before it can observe
// cancellation. Close joins these workers before draining their shared queues.
func (self *packetTranslation) startWorker(run func()) {
	self.workerWg.Add(1)
	go func() {
		defer self.workerWg.Done()
		HandleError(run, self.cancel)
	}()
}

// beginOperation prevents a PacketConn call from publishing into a queue after
// shutdown has joined the queue consumers and begun its final drain.
func (self *packetTranslation) beginOperation() bool {
	self.operationLock.Lock()
	defer self.operationLock.Unlock()
	if self.operationClosed || self.ctx.Err() != nil {
		return false
	}
	self.operationWg.Add(1)
	return true
}

func (self *packetTranslation) newHeader() [18]byte {
	var header [18]byte
	if 0 < len(self.headerPrefix) {
		copy(header[0:len(self.headerPrefix)], self.headerPrefix)
		mathrand.Read(header[len(self.headerPrefix):16])
	} else {
		mathrand.Read(header[0:16])
	}
	return header
}

func (self *packetTranslation) newHeaderWithContentAddress(p []byte) [18]byte {
	var header [18]byte
	if 0 < len(self.headerPrefix) {
		copy(header[0:len(self.headerPrefix)], self.headerPrefix)
		sha3.ShakeSum128(header[len(self.headerPrefix):16], p)
	} else {
		sha3.ShakeSum128(header[0:16], p)
	}
	return header
}

func (self *packetTranslation) encodeDns() {
	defer self.cancel()

	var buf [1024]byte
	var id uint16
	var mostRecentAddr net.Addr
	var dnsPumpTimer *time.Timer
	var paceTimer *time.Timer
	defer func() {
		if dnsPumpTimer != nil {
			dnsPumpTimer.Stop()
		}
		if paceTimer != nil {
			paceTimer.Stop()
		}
	}()
	waitPace := func(timeout time.Duration) bool {
		maxJitterNanoseconds := int(2 * timeout / time.Nanosecond)
		if maxJitterNanoseconds <= 0 {
			return true
		}
		randTimeout := time.Duration(mathrand.Intn(maxJitterNanoseconds)) * time.Nanosecond
		timerC := resetOrCreateTimer(&paceTimer, randTimeout)
		select {
		case <-timerC:
			return true
		case <-self.ctx.Done():
			paceTimer.Stop()
			return false
		}
	}
	for {
		if self.dnsClient {
			writeOne := func(p *packet) (writeErr error) {
				defer func() { finishPacketWrite(p, writeErr) }()
				defer MessagePoolReturn(p.data)

				// fmt.Printf("WRITE ONE\n")

				tld := self.settings.DnsTlds[mathrand.Intn(len(self.settings.DnsTlds))]

				c := encodeDnsRequestCount(p.data, tld)

				// fmt.Printf("WRITE ONE (%d)\n", c)

				header := self.newHeaderWithContentAddress(p.data)
				header[16] = uint8(c)

				n := 0
				for i := 0; i < c; i += 1 {
					startTime := time.Now()

					header[17] = uint8(i)
					m, packetData, err := encodeDnsRequest(id, header, p.data[n:], buf, tld)
					id += 1
					n += m
					if err != nil {
						return err
					}

					// fmt.Printf("PACKET WRITE TO: %v\n", string(packetData))

					_, err = self.packetConn.WriteTo(packetData, p.addr)
					if err != nil {
						return err
					}
					endTime := time.Now()
					if 0 < self.settings.WritePacketsPerSecond {
						writeDuration := endTime.Sub(startTime)
						timeout := time.Second/time.Duration(self.settings.WritePacketsPerSecond) - writeDuration
						if 0 < timeout && !waitPace(timeout) {
							return self.ctx.Err()
						}
					}

					// _, err = self.packetConn.WriteTo(packetData, p.addr)
					// if err != nil {
					// 	self.log.Infof("[pt]write err = %s\n", err)
					// 	return err
					// }

					// self.log.Infof("[pt]write raw\n")

				}
				if n != len(p.data) {
					return fmt.Errorf("Header count estimate incorrect.")
				}
				return nil
			}

			if self.ptMode == PacketTranslationModeDnsPump {
				timerC := resetOrCreateTimer(&dnsPumpTimer, self.settings.DnsPumpTimeout)
				select {
				case <-self.ctx.Done():
					dnsPumpTimer.Stop()
					return
				case p := <-self.out:
					dnsPumpTimer.Stop()
					// each write includes one pump header
					mostRecentAddr = p.addr
					if err := writeOne(p); err != nil {
						select {
						case <-self.ctx.Done():
						default:
							self.log.Infof("[pt]write err = %s\n", err)
						}
						return
					}
				case <-timerC:
					// pump one header the server can use to repsond to
					if mostRecentAddr != nil {
						startTime := time.Now()

						tld := self.settings.DnsTlds[mathrand.Intn(len(self.settings.DnsTlds))]

						header := self.newHeader()

						_, packetData, err := encodeDnsRequest(
							id,
							header,
							make([]byte, 0),
							buf,
							tld,
						)
						id += 1
						if err != nil {
							// drop the packet
							break
						}

						// fmt.Printf("PACKET WRITE TO: %v\n", string(packetData))

						_, err = self.packetConn.WriteTo(packetData, mostRecentAddr)
						if err != nil {
							select {
							case <-self.ctx.Done():
							default:
								self.log.Infof("[pt]write err = %s\n", err)
							}
							return
						}
						if 0 < self.settings.WritePacketsPerSecond {
							endTime := time.Now()
							writeDuration := endTime.Sub(startTime)
							timeout := time.Second/time.Duration(self.settings.WritePacketsPerSecond) - writeDuration
							if 0 < timeout && !waitPace(timeout) {
								return
							}
						}
					} else {
						self.log.Infof("[pt]cannot pump dns due to missing most recent addr\n")
					}
				}
			} else {
				select {
				case <-self.ctx.Done():
					return
				case p := <-self.out:
					if err := writeOne(p); err != nil {
						select {
						case <-self.ctx.Done():
						default:
							self.log.Infof("[pt]write err = %s\n", err)
						}
						return
					}
				}
			}
		} else {
			writeOne := func(p *packet) (writeErr error) {
				defer func() { finishPacketWrite(p, writeErr) }()
				defer MessagePoolReturn(p.data)

				minUpdateTime := time.Now().Add(-self.settings.DnsStateTimeout)
				self.dnsPumpQueue.RemoveOlder(minUpdateTime)

				longestTld := self.settings.DnsTlds[0]
				for _, tld := range self.settings.DnsTlds[1:] {
					if len(longestTld) < len(tld) {
						longestTld = tld
					}
				}

				c := encodeDnsResponseCount(p.data, longestTld)

				var pumpItems []*pumpItem
				if self.dnsRequirePump {
					pumpItems = self.dnsPumpQueue.RemoveLastN(p.addr, c)

					if pumpItems == nil {
						// drop the packet since there aren't enough pump headers
						return nil
					}
				} else {

					pumpItems = make([]*pumpItem, c)

					i := 0
					for ; i < c; i += 1 {
						item := self.dnsPumpQueue.RemoveLast(p.addr)
						if item == nil {
							break
						}
						pumpItems[i] = item
					}
					// fill the rest with new headers
					for ; i < c; i += 1 {
						header := self.newHeader()
						tld := self.settings.DnsTlds[mathrand.Intn(len(self.settings.DnsTlds))]
						item := &pumpItem{
							id:     id,
							header: header,
							tld:    tld,
						}
						pumpItems[i] = item
						id += 1
					}
				}

				header := self.newHeaderWithContentAddress(p.data)
				header[16] = uint8(c)

				// on error, stop sending all since one is dropped
				n := 0
				for i := 0; i < c; i += 1 {
					startTime := time.Now()

					header[17] = uint8(i)

					item := pumpItems[i]

					m, packetData, err := encodeDnsResponse(
						item.id,
						item.header,
						header,
						p.data[n:],
						buf,
						item.tld,
					)
					n += m
					if err != nil {
						return err
					}

					// fmt.Printf("PACKET WRITE TO: %v\n", string(packetData))

					_, err = self.packetConn.WriteTo(packetData, p.addr)
					if err != nil {
						return err
					}

					endTime := time.Now()
					if 0 < self.settings.WritePacketsPerSecond {
						writeDuration := endTime.Sub(startTime)
						timeout := time.Second/time.Duration(self.settings.WritePacketsPerSecond) - writeDuration
						if 0 < timeout && !waitPace(timeout) {
							return self.ctx.Err()
						}
					}
				}
				if n != len(p.data) {
					return fmt.Errorf("Header count estimate incorrect.")
				}
				return nil
			}

			select {
			case <-self.ctx.Done():
				return
			case p := <-self.out:
				if err := writeOne(p); err != nil {
					select {
					case <-self.ctx.Done():
					default:
						self.log.Infof("[pt]write err = %s\n", err)
					}
					return
				}
			case p := <-self.forward:
				// fmt.Printf("PACKET WRITE TO: %v\n", string(p.data))

				_, err := self.packetConn.WriteTo(p.data, p.addr)
				MessagePoolReturn(p.data)
				if err != nil {
					return
				}
			}
		}
	}
}

func (self *packetTranslation) decodeDns() {
	type readData struct {
		addr   net.Addr
		header [18]byte
		data   []byte
		tld    []byte
	}

	readPipeline := make(chan *readData, self.settings.SequenceBufferSize)
	pumpPipeline := make(chan *pumpItem, self.settings.SequenceBufferSize)
	var childWorkers sync.WaitGroup
	defer func() {
		self.cancel()
		childWorkers.Wait()
		// No producer or consumer remains after the join, so this drain is the
		// final disposition for every readData item that never reached combine.
		for {
			select {
			case r, ok := <-readPipeline:
				if !ok {
					return
				}
				MessagePoolReturn(r.data)
			default:
				return
			}
		}
	}()
	runChild := func(run func()) {
		childWorkers.Add(1)
		go func() {
			defer childWorkers.Done()
			HandleError(run, self.cancel)
		}()
	}

	runChild(func() {
		defer self.cancel()

		dnsCombineQueue := newCombineQueue(self.settings)
		defer func() {
			// release any partially-assembled packets when the consumer exits
			for _, item := range dnsCombineQueue.orderedItems {
				for _, p := range item.packets {
					if p != nil {
						MessagePoolReturn(p.data)
					}
				}
			}
		}()
		expiryTimer := time.NewTimer(time.Hour)
		if !expiryTimer.Stop() {
			<-expiryTimer.C
		}
		defer expiryTimer.Stop()
		var expiry <-chan time.Time
		resetExpiry := func() {
			if !expiryTimer.Stop() && expiry != nil {
				select {
				case <-expiryTimer.C:
				default:
				}
			}
			if oldest, ok := dnsCombineQueue.OldestUpdateTime(); ok {
				delay := time.Until(oldest.Add(self.settings.DnsStateTimeout))
				expiryTimer.Reset(max(time.Duration(0), delay))
				expiry = expiryTimer.C
			} else {
				expiry = nil
			}
		}

		for {
			select {
			case <-self.ctx.Done():
				return
			case <-expiry:
				dnsCombineQueue.RemoveOlder(time.Now().Add(-self.settings.DnsStateTimeout))
				resetExpiry()
			case r := <-readPipeline:
				minUpdateTime := time.Now().Add(-self.settings.DnsStateTimeout)
				dnsCombineQueue.RemoveOlder(minUpdateTime)

				out, limit, err := dnsCombineQueue.Combine(r.addr, r.header, r.data)
				resetExpiry()
				if err != nil {
					// drop the packet
					MessagePoolReturn(r.data)
					self.log.Errorf("[pt]combine err = %s\n", err)
					continue
				}

				if limit {
					// drop the packet
					MessagePoolReturn(r.data)
					// fmt.Printf("PACKET READ ONE DROP LIMIT\n")
					self.log.Errorf("[pt]combine limit\n")
					continue
				}

				if out == nil {
					// packet not combined
					continue
				}

				// fmt.Printf("PACKET COMBINE ONE %s\n", string(out.data))

				select {
				case <-self.ctx.Done():
					MessagePoolReturn(out.data)
					return
				case self.in <- out:
				}
			}
		}
	})

	runChild(func() {
		defer self.cancel()
		if self.dnsPumpQueue == nil {
			<-self.ctx.Done()
			return
		}
		expiryTimer := time.NewTimer(time.Hour)
		if !expiryTimer.Stop() {
			<-expiryTimer.C
		}
		defer expiryTimer.Stop()
		var expiry <-chan time.Time
		resetExpiry := func() {
			if !expiryTimer.Stop() && expiry != nil {
				select {
				case <-expiryTimer.C:
				default:
				}
			}
			if oldest, ok := self.dnsPumpQueue.OldestUpdateTime(); ok {
				delay := time.Until(oldest.Add(self.settings.DnsStateTimeout))
				expiryTimer.Reset(max(time.Duration(0), delay))
				expiry = expiryTimer.C
			} else {
				expiry = nil
			}
		}

		for {
			select {
			case <-self.ctx.Done():
				return
			case <-expiry:
				self.dnsPumpQueue.RemoveOlder(time.Now().Add(-self.settings.DnsStateTimeout))
				resetExpiry()
			case item := <-pumpPipeline:
				minUpdateTime := time.Now().Add(-self.settings.DnsStateTimeout)
				self.dnsPumpQueue.RemoveOlder(minUpdateTime)
				// if limit, drop the pump header but continue to process the packet
				self.dnsPumpQueue.Add(item)
				resetExpiry()
			}
		}
	})

	packetData := make([]byte, 2048)
	var buf [1024]byte

	for {
		// self.packetConn.SetReadDeadline(time.Now().Add(self.settings.DnsReadTimeout))
		n, addr, err := self.packetConn.ReadFrom(packetData)
		if err != nil {
			select {
			case <-self.ctx.Done():
			default:
				self.log.Infof("[pt]read err = %s\n", err)
			}
			return
		}
		// self.log.Infof("[pt]read raw\n")

		var header [18]byte
		var data []byte
		var tld []byte

		if self.dnsClient {
			_, _, header, data, err = decodeDnsResponse(
				packetData[:n],
				buf,
				self.settings.DnsTlds,
			)
		} else {
			var id uint16
			var otherData bool
			id, header, data, tld, err, otherData = decodeDnsRequest(
				packetData[:n],
				buf,
				self.settings.DnsTlds,
			)
			if otherData {
				// a normal non-pt dns request
				self.handleDnsOther(packetData[:n], addr)
				continue
			}

			if 0 < len(self.headerPrefix) && !slices.Equal(self.headerPrefix, header[0:len(self.headerPrefix)]) {
				// the header does not match the prefix, drop
				continue
			}

			if self.log.V(2).Enabled() {
				self.log.Infof("[pt]decode one: %v, %v (%d/%d), (%d), %s, %s, %v\n", id, header, header[17], header[16], len(data), string(tld), err, otherData)
			}

			item := &pumpItem{
				addr:   addr,
				id:     id,
				header: header,
				tld:    tld,
			}

			select {
			case pumpPipeline <- item:
			case <-self.ctx.Done():
				return
			// if limit, drop the pump header but continue to process the packet
			default:
			}
		}

		if c := uint8(header[16]); c == 0 {
			// just a pump
			continue
		}

		// dataCopy := make([]byte, len(data))
		// copy(dataCopy, data)
		r := &readData{
			addr:   addr,
			header: header,
			data:   MessagePoolCopy(data),
			tld:    tld,
		}

		select {
		case readPipeline <- r:
		case <-self.ctx.Done():
			MessagePoolReturn(r.data)
			return
		}
	}
}

func (self *packetTranslation) handleDnsOther(packetData []byte, addr net.Addr) (err error) {
	p := &dnsmessage.Parser{}
	_, err = p.Start(packetData)
	if err != nil {
		return
	}

	var qs []dnsmessage.Question
	qs, err = p.AllQuestions()
	if err != nil {
		return
	}
	for _, q := range qs {
		switch q.Type {
		case dnsmessage.TypeNS:
			// FIXME handle NS and zone requests by writing packets to forward

			// else unknown
		}
	}

	if handler := self.settings.DnsOtherHandler; handler != nil {
		// the read buffer is reused by the next read, so the handler is given
		// bytes of its own; a query that does not parse never reaches it,
		// which is the cheapest place to drop junk under a flood
		handler(slices.Clone(packetData), addr)
	}

	return
}

func (self *packetTranslation) currentReadDeadline() (time.Time, bool, <-chan struct{}) {
	self.deadlineLock.Lock()
	defer self.deadlineLock.Unlock()
	return self.readDeadline, self.readDeadlineWakeup, self.readDeadlineMonitor.NotifyChannel()
}

func (self *packetTranslation) currentWriteDeadline() (time.Time, <-chan struct{}) {
	self.deadlineLock.Lock()
	defer self.deadlineLock.Unlock()
	return self.writeDeadline, self.writeDeadlineMonitor.NotifyChannel()
}

// deadlineAfter returns the timeout signal used by PacketConn deadline waits.
// The test seam makes timer-driven branches deterministic without changing
// production deadline behavior.
func (self *packetTranslation) deadlineAfter(timeout time.Duration) <-chan time.Time {
	if self.deadlineAfterForTest != nil {
		return self.deadlineAfterForTest(timeout)
	}
	return time.After(timeout)
}

// packetTranslationDeadlineError preserves the net.PacketConn deadline error
// contract while identifying which operation expired.
func packetTranslationDeadlineError(operation string) error {
	return &net.OpError{
		Op:  operation,
		Net: "packet translation",
		Err: os.ErrDeadlineExceeded,
	}
}

// packetTranslationClosedError preserves the net.PacketConn closed-connection
// contract for operations released by packet translation shutdown.
func packetTranslationClosedError(operation string) error {
	return &net.OpError{
		Op:  operation,
		Net: "packet translation",
		Err: net.ErrClosed,
	}
}

// logReadDeadline distinguishes an elapsed read deadline from an already
// expired deadline used to wake a blocked PacketConn reader. quic-go uses the
// latter during Transport.Close; retain it at V(1) without reporting a
// misleading operational timeout at INFO.
func (self *packetTranslation) logReadDeadline(wakeup bool) {
	if wakeup {
		if verbose := self.log.V(1); verbose.Enabled() {
			verbose.Infof("[pt]read packet deadline wakeup\n")
		}
		return
	}
	self.log.Infof("[pt]read packet timeout\n")
}

// waitForWireWrite keeps PacketConn.WriteTo synchronous with the translated
// wire writes it represents. In particular, a caller may close the PacketConn
// immediately after a successful return without losing an accepted QUIC close
// packet from the encoder queue.
func (self *packetTranslation) waitForWireWrite(
	writeResult <-chan error,
	packetByteCount int,
) (int, error) {
	for {
		// Prefer an already-published disposition over a concurrent shutdown.
		select {
		case err := <-writeResult:
			if err != nil {
				return 0, err
			}
			return packetByteCount, nil
		default:
		}

		writeDeadline, deadlineChanged := self.currentWriteDeadline()
		if writeDeadline.IsZero() {
			select {
			case err := <-writeResult:
				if err != nil {
					return 0, err
				}
				return packetByteCount, nil
			case <-self.ctx.Done():
				return 0, packetTranslationClosedError("write")
			case <-deadlineChanged:
			}
			continue
		}

		timeout := time.Until(writeDeadline)
		if timeout <= 0 {
			self.log.Infof("[pt]write packet timeout\n")
			return 0, packetTranslationDeadlineError("write")
		}
		select {
		case err := <-writeResult:
			if err != nil {
				return 0, err
			}
			return packetByteCount, nil
		case <-self.ctx.Done():
			return 0, packetTranslationClosedError("write")
		case <-self.deadlineAfter(timeout):
			self.log.Infof("[pt]write packet timeout\n")
			return 0, packetTranslationDeadlineError("write")
		case <-deadlineChanged:
		}
	}
}

func (self *packetTranslation) WriteTo(packetData []byte, addr net.Addr) (n int, err error) {
	if !self.beginOperation() {
		return 0, packetTranslationClosedError("write")
	}
	defer self.operationWg.Done()

	// packetDataCopy := make([]byte, len(packetData))
	// copy(packetDataCopy, packetData)
	packetDataCopy := MessagePoolCopy(packetData)
	queued := false
	defer func() {
		if !queued {
			MessagePoolReturn(packetDataCopy)
		}
	}()

	p := &packet{
		data:        packetDataCopy,
		addr:        addr,
		writeResult: make(chan error, 1),
	}

	for {
		writeDeadline, deadlineChanged := self.currentWriteDeadline()
		if writeDeadline.IsZero() {
			select {
			case <-self.ctx.Done():
				err = packetTranslationClosedError("write")
				return
			case self.out <- p:
				queued = true
				self.log.V(2).Infof("[pt]write packet\n")
				return self.waitForWireWrite(p.writeResult, len(packetData))
			case <-deadlineChanged:
			}
			continue
		}

		timeout := writeDeadline.Sub(time.Now())
		if timeout <= 0 {
			err = packetTranslationDeadlineError("write")
			self.log.Infof("[pt]write packet timeout\n")
			return
		}
		// Ready fast path: a deadline must be checked first, but a write that can
		// complete now does not need to allocate a timer.
		select {
		case <-self.ctx.Done():
			err = packetTranslationClosedError("write")
			return
		case self.out <- p:
			queued = true
			self.log.V(2).Infof("[pt]write packet\n")
			return self.waitForWireWrite(p.writeResult, len(packetData))
		case <-deadlineChanged:
			continue
		default:
		}
		select {
		case <-self.ctx.Done():
			err = packetTranslationClosedError("write")
			return
		case self.out <- p:
			queued = true
			self.log.V(2).Infof("[pt]write packet\n")
			return self.waitForWireWrite(p.writeResult, len(packetData))
		case <-self.deadlineAfter(timeout):
			err = packetTranslationDeadlineError("write")
			self.log.Infof("[pt]write packet timeout\n")
			return
		case <-deadlineChanged:
		}
	}
}

func (self *packetTranslation) ReadFrom(packetData []byte) (n int, addr net.Addr, err error) {
	if !self.beginOperation() {
		return 0, nil, packetTranslationClosedError("read")
	}
	defer self.operationWg.Done()

	for {
		readDeadline, deadlineWakeup, deadlineChanged := self.currentReadDeadline()
		if readDeadline.IsZero() {
			select {
			case <-self.ctx.Done():
				err = packetTranslationClosedError("read")
				return
			case p := <-self.in:
				addr = p.addr
				n = copy(packetData, p.data)
				MessagePoolReturn(p.data)
				self.log.V(2).Infof("[pt]read packet\n")
				return
			case <-deadlineChanged:
			}
			continue
		}

		timeout := readDeadline.Sub(time.Now())
		if timeout <= 0 {
			err = packetTranslationDeadlineError("read")
			self.logReadDeadline(deadlineWakeup)
			return
		}
		// Ready fast path: preserve expired-deadline behavior while avoiding a
		// timer allocation when a packet is already queued.
		select {
		case <-self.ctx.Done():
			err = packetTranslationClosedError("read")
			return
		case p := <-self.in:
			addr = p.addr
			n = copy(packetData, p.data)
			MessagePoolReturn(p.data)
			self.log.V(2).Infof("[pt]read packet\n")
			return
		case <-deadlineChanged:
			continue
		default:
		}
		select {
		case <-self.ctx.Done():
			err = packetTranslationClosedError("read")
			return
		case p := <-self.in:
			addr = p.addr
			n = copy(packetData, p.data)
			MessagePoolReturn(p.data)
			self.log.V(2).Infof("[pt]read packet\n")
			return
		case <-self.deadlineAfter(timeout):
			err = packetTranslationDeadlineError("read")
			self.logReadDeadline(deadlineWakeup)
			return
		case <-deadlineChanged:
		}
	}
}

func (self *packetTranslation) LocalAddr() net.Addr {
	return self.packetConn.LocalAddr()
}

func (self *packetTranslation) SetDeadline(t time.Time) error {
	self.deadlineLock.Lock()
	defer self.deadlineLock.Unlock()
	self.readDeadline = t
	self.readDeadlineWakeup = !t.IsZero() && !t.After(time.Now())
	self.writeDeadline = t
	self.readDeadlineMonitor.NotifyAll()
	self.writeDeadlineMonitor.NotifyAll()
	return nil
}

func (self *packetTranslation) SetReadDeadline(t time.Time) error {
	self.deadlineLock.Lock()
	defer self.deadlineLock.Unlock()
	self.readDeadline = t
	self.readDeadlineWakeup = !t.IsZero() && !t.After(time.Now())
	self.readDeadlineMonitor.NotifyAll()
	return nil
}

func (self *packetTranslation) SetWriteDeadline(t time.Time) error {
	self.deadlineLock.Lock()
	defer self.deadlineLock.Unlock()
	self.writeDeadline = t
	self.writeDeadlineMonitor.NotifyAll()
	return nil
}

func (self *packetTranslation) Close() error {
	return self.close()
}

func (self *packetTranslation) close() error {
	self.closeOnce.Do(func() {
		self.operationLock.Lock()
		self.operationClosed = true
		self.operationLock.Unlock()
		self.cancel()
		self.closeErr = self.packetConn.Close()
		self.operationWg.Wait()
		self.workerWg.Wait()
		returnPacketTranslationQueue(self.in)
		returnPacketTranslationQueue(self.out)
		returnPacketTranslationQueue(self.forward)
	})
	return self.closeErr
}

// returnPacketTranslationQueue releases ownership after every producer and
// consumer of a retired packet queue has joined.
func returnPacketTranslationQueue(queue chan *packet) {
	for {
		select {
		case queued := <-queue:
			if queued != nil {
				finishPacketWrite(queued, net.ErrClosed)
				MessagePoolReturn(queued.data)
			}
		default:
			return
		}
	}
}

func (self *packetTranslation) SetReadBuffer(bytes int) error {
	conn, ok := self.packetConn.(interface{ SetReadBuffer(int) error })
	if !ok {
		return fmt.Errorf("Set read buffer not supporter on underlying packet conn: %T", self.packetConn)
	}
	return conn.SetReadBuffer(bytes)
}

func (self *packetTranslation) SetWriteBuffer(bytes int) error {
	conn, ok := self.packetConn.(interface{ SetWriteBuffer(int) error })
	if !ok {
		return fmt.Errorf("Set write buffer not supporter on underlying packet conn: %T", self.packetConn)
	}
	return conn.SetWriteBuffer(bytes)
}

// func keyFromHeader(header [18]byte) (key [17]byte) {
// 	key = [17]byte(header)
// 	return key
// }
