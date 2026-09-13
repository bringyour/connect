package extender

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/dns/dnsmessage"

	"github.com/urnetwork/connect"
)

// The udp 53 forwarder (EXTENDER.md A6).
//
// The dns carrier is the packet translation on udp 53, and every query that
// does not follow the translation reaches this forwarder instead. It resolves
// the question over the extender's own DoH cache and answers on the same
// socket, so a prober that points a resolver at the extender gets realistic
// answers rather than silence, exactly as the reverse proxy answers a plain
// https prober.
//
// The forwarder never recurses on its own and never becomes an open resolver
// worth abusing: one in-flight query per source address, a per-source rate with
// a burst, a total rate, a bounded pool of workers and a bounded answer. A
// query over any of those bounds is dropped without an answer, which is what a
// rate-limited resolver looks like from outside.
//
// Nothing here may block the translation's read loop, which is the one
// goroutine that drains the socket for the carrier as well. The handler
// therefore does only the admission accounting and a non-blocking handoff; a
// full pool drops the query.
//
// The type is safe for concurrent use.

// Sources whose limiter state is kept. Over this, states that are neither in
// flight nor rate limited are dropped, which is what a missing state means.
const extenderDnsMaxSourceCount = 4096

// One admitted query on its way to a worker.
type extenderDnsQuery struct {
	queryBytes []byte
	addr       net.Addr
	source     string
}

// The limiter state of one source address.
type extenderDnsSource struct {
	bucket   tokenBucket
	inFlight bool
}

type extenderDnsForwarder struct {
	server     *ExtenderServer
	packetConn net.PacketConn
	queries    chan *extenderDnsQuery

	stateLock    sync.Mutex
	totalBucket  tokenBucket
	sourceStates map[string]*extenderDnsSource

	dohOnce  sync.Once
	dohCache *connect.DohCache
}

// Builds the forwarder of one udp 53 socket and starts its workers. The socket
// is owned by the carrier; the forwarder only answers on it.
func newExtenderDnsForwarder(server *ExtenderServer, packetConn net.PacketConn) *extenderDnsForwarder {
	workerCount := max(1, server.settings.DnsForwardWorkerCount)
	self := &extenderDnsForwarder{
		server:       server,
		packetConn:   packetConn,
		queries:      make(chan *extenderDnsQuery, workerCount),
		sourceStates: map[string]*extenderDnsSource{},
	}
	for range workerCount {
		if !server.beginWorker() {
			break
		}
		go func() {
			defer server.endWorker()
			connect.HandleError(func() {
				self.run()
			})
		}()
	}
	return self
}

// One worker, which owns a query until its answer is written.
func (self *extenderDnsForwarder) run() {
	for {
		select {
		case <-self.server.ctx.Done():
			return
		case query := <-self.queries:
			self.resolveQuery(query)
		}
	}
}

// handleQuery is the packet translation hook. It runs on the decoder's read
// loop, so it only admits the query and hands it on (A6).
func (self *extenderDnsForwarder) handleQuery(queryBytes []byte, addr net.Addr) {
	source := connectionSourceAddress(remoteAddressString(addr))
	if err := self.admit(source, time.Now()); err != nil {
		self.server.reportError("dns query", err)
		return
	}
	query := &extenderDnsQuery{
		queryBytes: queryBytes,
		addr:       addr,
		source:     source,
	}
	select {
	case self.queries <- query:
	default:
		// every worker is busy; a dropped query is what a loaded resolver does
		self.release(source)
		self.server.reportError("dns query", fmt.Errorf("the dns forward workers are busy"))
	}
}

// Admits one query against the in-flight rule and both rate limits, taking the
// source's in-flight slot on success (A6).
func (self *extenderDnsForwarder) admit(source string, now time.Time) error {
	settings := self.server.settings
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	sourceState, ok := self.sourceStates[source]
	if !ok {
		sourceState = &extenderDnsSource{}
		self.sourceStates[source] = sourceState
	}
	if sourceState.inFlight {
		return fmt.Errorf("%s already has a query in flight", source)
	}
	// both buckets are refilled before either is taken, so a query refused by
	// one does not spend the other's token
	sourceAvailable := sourceState.bucket.refill(
		now,
		settings.DnsMaxQueryRatePerSource,
		float64(settings.DnsMaxQueryBurstPerSource),
	)
	totalAvailable := self.totalBucket.refill(
		now,
		settings.DnsMaxQueryRate,
		float64(settings.DnsMaxQueryBurst),
	)
	if !sourceAvailable {
		self.pruneSourceStatesWithLock(now)
		return fmt.Errorf("%s is over its query rate", source)
	}
	if !totalAvailable {
		self.pruneSourceStatesWithLock(now)
		return fmt.Errorf("the extender is over its query rate")
	}
	sourceState.bucket.take()
	self.totalBucket.take()
	sourceState.inFlight = true
	self.pruneSourceStatesWithLock(now)
	return nil
}

// Releases the in-flight slot of a source once its answer is written.
func (self *extenderDnsForwarder) release(source string) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if sourceState, ok := self.sourceStates[source]; ok {
		sourceState.inFlight = false
	}
}

// Drops the states that carry nothing: a source that is neither in flight nor
// short of tokens is indistinguishable from one that has never been seen, so a
// flood of addresses cannot grow the map without bound.
func (self *extenderDnsForwarder) pruneSourceStatesWithLock(now time.Time) {
	if len(self.sourceStates) <= extenderDnsMaxSourceCount {
		return
	}
	settings := self.server.settings
	burst := float64(settings.DnsMaxQueryBurstPerSource)
	for source, sourceState := range self.sourceStates {
		if sourceState.inFlight {
			continue
		}
		sourceState.bucket.refill(now, settings.DnsMaxQueryRatePerSource, burst)
		if burst <= sourceState.bucket.tokens || settings.DnsMaxQueryRatePerSource <= 0 {
			delete(self.sourceStates, source)
		}
	}
}

// Resolves one admitted query and writes its answer on the carrier socket.
func (self *extenderDnsForwarder) resolveQuery(query *extenderDnsQuery) {
	defer self.release(query.source)

	var parser dnsmessage.Parser
	header, err := parser.Start(query.queryBytes)
	if err != nil {
		self.server.reportError("dns query", err)
		return
	}
	question, err := parser.Question()
	if err != nil {
		self.server.reportError("dns query", err)
		return
	}

	if question.Type == dnsmessage.TypeALL {
		// ANY is a reflection amplifier and answers nothing useful (A6)
		self.answerStatus(query, header, question, dnsmessage.RCodeRefused, false)
		return
	}

	// the name keeps the case it was asked in: a 0x20 client checks that the
	// answer echoes its own query exactly
	name := strings.TrimSuffix(question.Name.String(), ".")
	if name == "" {
		self.server.reportError("dns query", fmt.Errorf("the query has no name"))
		return
	}

	forwardCtx := self.server.ctx
	var forwardCancel context.CancelFunc
	if 0 < self.server.settings.DnsForwardTimeout {
		forwardCtx, forwardCancel = context.WithTimeout(forwardCtx, self.server.settings.DnsForwardTimeout)
	} else {
		forwardCtx, forwardCancel = context.WithCancel(forwardCtx)
	}
	defer forwardCancel()

	responseBytes, ok := self.forward(forwardCtx, question.Type, name)
	if !ok || len(responseBytes) < 2 {
		// a resolver that did not answer is silence here too; the client
		// retries or moves to another resolver
		self.server.reportError("dns query", fmt.Errorf("%s %s did not resolve", question.Type, name))
		return
	}

	maxByteCount := self.server.settings.DnsMaxResponseByteCount
	if 0 < maxByteCount && maxByteCount < len(responseBytes) {
		// over what the datagram may carry: answer the header with TC set and
		// no sections, which is what tells the client to ask again over tcp
		self.answerStatus(query, header, question, dnsmessage.RCodeSuccess, true)
		return
	}

	// the upstream answer carries the id of the query the cache made
	answerBytes := make([]byte, len(responseBytes))
	copy(answerBytes, responseBytes)
	binary.BigEndian.PutUint16(answerBytes[0:2], header.ID)
	self.writeAnswer(query, answerBytes)
}

// Answers one query with a status and no sections: the ANY refusal and the
// truncation of an oversized answer (A6).
func (self *extenderDnsForwarder) answerStatus(
	query *extenderDnsQuery,
	header dnsmessage.Header,
	question dnsmessage.Question,
	rcode dnsmessage.RCode,
	truncated bool,
) {
	builder := dnsmessage.NewBuilder(nil, dnsmessage.Header{
		ID:                 header.ID,
		Response:           true,
		OpCode:             header.OpCode,
		Truncated:          truncated,
		RecursionDesired:   header.RecursionDesired,
		RecursionAvailable: true,
		RCode:              rcode,
	})
	builder.EnableCompression()
	if err := builder.StartQuestions(); err != nil {
		self.server.reportError("dns answer", err)
		return
	}
	if err := builder.Question(question); err != nil {
		self.server.reportError("dns answer", err)
		return
	}
	answerBytes, err := builder.Finish()
	if err != nil {
		self.server.reportError("dns answer", err)
		return
	}
	self.writeAnswer(query, answerBytes)
}

// Writes one answer on the carrier socket, which the translation reads from
// and writes to concurrently.
func (self *extenderDnsForwarder) writeAnswer(query *extenderDnsQuery, answerBytes []byte) {
	if _, err := self.packetConn.WriteTo(answerBytes, query.addr); err != nil {
		self.server.reportError("dns answer", err)
	}
}

// The resolver of one question: the configured forward, or this extender's own
// DoH cache built on first use (A6).
func (self *extenderDnsForwarder) forward(
	ctx context.Context,
	qType dnsmessage.Type,
	name string,
) ([]byte, bool) {
	if forward := self.server.settings.DnsForward; forward != nil {
		return forward(ctx, qType, name)
	}
	self.dohOnce.Do(func() {
		dohSettings := self.server.settings.DohSettings
		if dohSettings == nil {
			dohSettings = connect.DefaultDohSettings()
		}
		self.dohCache = connect.NewDohCache(dohSettings)
	})
	if self.dohCache == nil {
		return nil, false
	}
	return self.dohCache.Forward(ctx, qType, name)
}

// Releases the forwarder's own resolver. The workers exit with the server
// context, which CloseAndWait joins.
func (self *extenderDnsForwarder) close() {
	self.dohOnce.Do(func() {})
	if self.dohCache != nil {
		self.dohCache.Close()
	}
}

// tokenBucket admits at a fixed rate up to a burst, refilling from the time it
// was last used rather than on a tick, so an idle source costs nothing while it
// is idle. A rate or burst of zero or less disables the bucket.
type tokenBucket struct {
	tokens     float64
	updateTime time.Time
}

// refill adds the tokens earned since the last refill and reports whether one
// is available now. A disabled bucket is always available.
func (self *tokenBucket) refill(now time.Time, ratePerSecond float64, burst float64) bool {
	if ratePerSecond <= 0 || burst <= 0 {
		return true
	}
	if self.updateTime.IsZero() {
		self.tokens = burst
	} else if elapsed := now.Sub(self.updateTime); 0 < elapsed {
		self.tokens = min(burst, self.tokens+elapsed.Seconds()*ratePerSecond)
	}
	self.updateTime = now
	return 1 <= self.tokens
}

// take consumes the token a refill reported. A disabled bucket holds none, so
// this is its no-op.
func (self *tokenBucket) take() {
	if 1 <= self.tokens {
		self.tokens -= 1
	}
}
