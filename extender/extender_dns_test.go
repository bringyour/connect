// The udp 53 forwarder tests of EXTENDER.md section 5 phase 1: a query that is
// not the translation is answered from the configured forward, ANY is refused,
// an oversized answer is truncated, every bound of A6 is crossed, and the dns
// carrier keeps working through all of it.

package extender

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"

	"github.com/urnetwork/connect"
)

// The name every forwarder test asks for. It is not under the translation tld,
// so it reaches the forwarder rather than the carrier.
const testForwardName = "resolve.example."

// The id a fake forward answers with, which the forwarder must replace with
// the id of the query it answers (A6).
const testForwardResponseId = 0xfeed

// Builds one query datagram.
func newTestDnsQuery(t *testing.T, id uint16, name string, qType dnsmessage.Type) []byte {
	t.Helper()
	builder := dnsmessage.NewBuilder(nil, dnsmessage.Header{
		ID:               id,
		RecursionDesired: true,
	})
	builder.EnableCompression()
	if err := builder.StartQuestions(); err != nil {
		t.Fatal(err)
	}
	if err := builder.Question(dnsmessage.Question{
		Name:  dnsmessage.MustNewName(name),
		Type:  qType,
		Class: dnsmessage.ClassINET,
	}); err != nil {
		t.Fatal(err)
	}
	queryBytes, err := builder.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return queryBytes
}

// Builds one answer as a resolver would return it, with answerCount copies of
// the address so a test can make the answer as large as it needs.
func newTestDnsAnswer(
	t *testing.T,
	name string,
	qType dnsmessage.Type,
	addr netip.Addr,
	answerCount int,
) []byte {
	t.Helper()
	builder := dnsmessage.NewBuilder(nil, dnsmessage.Header{
		ID:                 testForwardResponseId,
		Response:           true,
		RecursionAvailable: true,
	})
	builder.EnableCompression()
	if err := builder.StartQuestions(); err != nil {
		t.Fatal(err)
	}
	question := dnsmessage.Question{
		Name:  dnsmessage.MustNewName(name),
		Type:  qType,
		Class: dnsmessage.ClassINET,
	}
	if err := builder.Question(question); err != nil {
		t.Fatal(err)
	}
	if err := builder.StartAnswers(); err != nil {
		t.Fatal(err)
	}
	resourceHeader := dnsmessage.ResourceHeader{
		Name:  question.Name,
		Class: dnsmessage.ClassINET,
		TTL:   60,
	}
	for range answerCount {
		var err error
		if qType == dnsmessage.TypeAAAA {
			err = builder.AAAAResource(resourceHeader, dnsmessage.AAAAResource{AAAA: addr.As16()})
		} else {
			err = builder.AResource(resourceHeader, dnsmessage.AResource{A: addr.As4()})
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	answerBytes, err := builder.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return answerBytes
}

// One client socket on the fixture's udp 53 carrier.
func newTestDnsClient(t *testing.T, fixture *extenderFixture) *net.UDPConn {
	t.Helper()
	serverAddr := net.UDPAddrFromAddrPort(netip.AddrPortFrom(fixture.ip, uint16(fixture.dnsPort)))
	conn, err := net.DialUDP("udp", nil, serverAddr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		conn.Close()
	})
	return conn
}

// Sends one query and reads its answer, failing when none arrives.
func exchangeTestDnsQuery(t *testing.T, conn *net.UDPConn, queryBytes []byte) []byte {
	t.Helper()
	if _, err := conn.Write(queryBytes); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	answerBytes := make([]byte, 8192)
	n, err := conn.Read(answerBytes)
	if err != nil {
		t.Fatalf("no answer: %v", err)
	}
	return answerBytes[0:n]
}

// A query that is not the translation is resolved and answered on the same
// socket, carrying the id of the query rather than the resolver's (A6).
func TestExtenderForwardsANonTranslationQuery(t *testing.T) {
	cases := []struct {
		qType dnsmessage.Type
		addr  netip.Addr
	}{
		{qType: dnsmessage.TypeA, addr: netip.MustParseAddr("192.0.2.9")},
		{qType: dnsmessage.TypeAAAA, addr: netip.MustParseAddr("2001:db8::9")},
	}
	for _, family := range testLoopbackFamilies {
		for _, c := range cases {
			answerBytes := newTestDnsAnswer(t, testForwardName, c.qType, c.addr, 1)
			var forwardNames chan string
			fixture := newExtenderFixture(t, family.loopbackIp, func(settings *ExtenderSettings) {
				forwardNames = make(chan string, 4)
				settings.DnsForward = func(
					ctx context.Context,
					qType dnsmessage.Type,
					name string,
				) ([]byte, bool) {
					if qType != c.qType {
						return nil, false
					}
					forwardNames <- name
					return answerBytes, true
				}
			})

			conn := newTestDnsClient(t, fixture)
			queryBytes := newTestDnsQuery(t, 0x1234, testForwardName, c.qType)
			responseBytes := exchangeTestDnsQuery(t, conn, queryBytes)

			select {
			case name := <-forwardNames:
				if name != strings.TrimSuffix(testForwardName, ".") {
					t.Fatalf("forwarded name = %q", name)
				}
			default:
				t.Fatal("the forward was not asked to resolve")
			}

			var parser dnsmessage.Parser
			header, err := parser.Start(responseBytes)
			if err != nil {
				t.Fatal(err)
			}
			if header.ID != 0x1234 {
				t.Fatalf("answer id = %#x, expected the query id", header.ID)
			}
			if !header.Response {
				t.Fatal("the answer is not a response")
			}
			if header.Truncated {
				t.Fatal("a small answer was truncated")
			}
			if _, err := parser.AllQuestions(); err != nil {
				t.Fatal(err)
			}
			answers, err := parser.AllAnswers()
			if err != nil {
				t.Fatal(err)
			}
			if len(answers) != 1 {
				t.Fatalf("answer count = %d, expected one", len(answers))
			}
			switch resource := answers[0].Body.(type) {
			case *dnsmessage.AResource:
				if netip.AddrFrom4(resource.A) != c.addr {
					t.Fatalf("answer address = %v, expected %v", resource.A, c.addr)
				}
			case *dnsmessage.AAAAResource:
				if netip.AddrFrom16(resource.AAAA) != c.addr {
					t.Fatalf("answer address = %v, expected %v", resource.AAAA, c.addr)
				}
			default:
				t.Fatalf("answer body = %T", resource)
			}
		}
	}
}

// ANY is refused rather than resolved, and the forward is never asked (A6).
func TestExtenderRefusesAnAnyQuery(t *testing.T) {
	forwardCalls := make(chan struct{}, 1)
	fixture := newExtenderFixture(t, "127.0.0.1", func(settings *ExtenderSettings) {
		settings.DnsForward = func(context.Context, dnsmessage.Type, string) ([]byte, bool) {
			forwardCalls <- struct{}{}
			return nil, false
		}
	})

	conn := newTestDnsClient(t, fixture)
	queryBytes := newTestDnsQuery(t, 0x2222, testForwardName, dnsmessage.TypeALL)
	responseBytes := exchangeTestDnsQuery(t, conn, queryBytes)

	var parser dnsmessage.Parser
	header, err := parser.Start(responseBytes)
	if err != nil {
		t.Fatal(err)
	}
	if header.ID != 0x2222 {
		t.Fatalf("answer id = %#x, expected the query id", header.ID)
	}
	if header.RCode != dnsmessage.RCodeRefused {
		t.Fatalf("rcode = %v, expected REFUSED", header.RCode)
	}
	select {
	case <-forwardCalls:
		t.Fatal("an ANY query was resolved")
	default:
	}
}

// An answer over the response bound is replaced by a truncated header, which is
// what tells a client to ask again over tcp (A6).
func TestExtenderTruncatesAnOversizedAnswer(t *testing.T) {
	fixture := newExtenderFixture(t, "127.0.0.1", func(settings *ExtenderSettings) {
		settings.DnsMaxResponseByteCount = 128
		answerBytes := newTestDnsAnswer(
			t,
			testForwardName,
			dnsmessage.TypeA,
			netip.MustParseAddr("192.0.2.10"),
			64,
		)
		settings.DnsForward = func(context.Context, dnsmessage.Type, string) ([]byte, bool) {
			return answerBytes, true
		}
	})

	conn := newTestDnsClient(t, fixture)
	queryBytes := newTestDnsQuery(t, 0x3333, testForwardName, dnsmessage.TypeA)
	responseBytes := exchangeTestDnsQuery(t, conn, queryBytes)
	if 128 < len(responseBytes) {
		t.Fatalf("answer is %d bytes, over the bound", len(responseBytes))
	}

	var parser dnsmessage.Parser
	header, err := parser.Start(responseBytes)
	if err != nil {
		t.Fatal(err)
	}
	if header.ID != 0x3333 {
		t.Fatalf("answer id = %#x, expected the query id", header.ID)
	}
	if !header.Truncated {
		t.Fatal("the oversized answer is not truncated")
	}
	if _, err := parser.AllQuestions(); err != nil {
		t.Fatal(err)
	}
	answers, err := parser.AllAnswers()
	if err != nil {
		t.Fatal(err)
	}
	if 0 < len(answers) {
		t.Fatalf("the truncated answer kept %d answers", len(answers))
	}
}

// A second query from a source whose first is still resolving is dropped, and
// the source is served again once the first answer is written (A6).
func TestExtenderForwarderAllowsOneQueryInFlightPerSource(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	fixture := newExtenderFixture(t, "127.0.0.1", func(settings *ExtenderSettings) {
		answerBytes := newTestDnsAnswer(
			t,
			testForwardName,
			dnsmessage.TypeA,
			netip.MustParseAddr("192.0.2.11"),
			1,
		)
		settings.DnsForward = func(ctx context.Context, _ dnsmessage.Type, _ string) ([]byte, bool) {
			select {
			case started <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-ctx.Done():
				return nil, false
			}
			return answerBytes, true
		}
	})

	conn := newTestDnsClient(t, fixture)
	if _, err := conn.Write(newTestDnsQuery(t, 0x4444, testForwardName, dnsmessage.TypeA)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the first query never reached the forward")
	}

	if _, err := conn.Write(newTestDnsQuery(t, 0x4445, testForwardName, dnsmessage.TypeA)); err != nil {
		t.Fatal(err)
	}
	extenderErr, ok := fixture.nextError()
	if !ok {
		t.Fatal("the second query was not dropped")
	}
	if !strings.Contains(extenderErr.Error(), "already has a query in flight") {
		t.Fatalf("attributed error = %v, expected the in flight rule", extenderErr)
	}

	close(release)
	if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	responseBytes := make([]byte, 4096)
	n, err := conn.Read(responseBytes)
	if err != nil {
		t.Fatalf("the first query was never answered: %v", err)
	}
	var parser dnsmessage.Parser
	header, err := parser.Start(responseBytes[0:n])
	if err != nil {
		t.Fatal(err)
	}
	if header.ID != 0x4444 {
		t.Fatalf("answer id = %#x, expected the first query", header.ID)
	}
}

// A query over the per-source rate is dropped, and a query over the total rate
// is dropped whatever source it came from (A6).
func TestExtenderForwarderEnforcesItsQueryRates(t *testing.T) {
	cases := []struct {
		description string
		configure   func(settings *ExtenderSettings)
		expect      string
	}{
		{
			description: "per source",
			configure: func(settings *ExtenderSettings) {
				// one query, and the next token is a thousand seconds away
				settings.DnsMaxQueryBurstPerSource = 1
				settings.DnsMaxQueryRatePerSource = 0.001
			},
			expect: "is over its query rate",
		},
		{
			description: "total",
			configure: func(settings *ExtenderSettings) {
				settings.DnsMaxQueryBurst = 1
				settings.DnsMaxQueryRate = 0.001
			},
			expect: "the extender is over its query rate",
		},
	}
	for _, c := range cases {
		fixture := newExtenderFixture(t, "127.0.0.1", func(settings *ExtenderSettings) {
			answerBytes := newTestDnsAnswer(
				t,
				testForwardName,
				dnsmessage.TypeA,
				netip.MustParseAddr("192.0.2.12"),
				1,
			)
			settings.DnsForward = func(context.Context, dnsmessage.Type, string) ([]byte, bool) {
				return answerBytes, true
			}
			c.configure(settings)
		})

		conn := newTestDnsClient(t, fixture)
		// the first query is answered, which also releases the in flight slot
		exchangeTestDnsQuery(t, conn, newTestDnsQuery(t, 0x5555, testForwardName, dnsmessage.TypeA))

		if _, err := conn.Write(newTestDnsQuery(t, 0x5556, testForwardName, dnsmessage.TypeA)); err != nil {
			t.Fatal(err)
		}
		extenderErr, ok := fixture.nextError()
		if !ok {
			t.Fatalf("%s: the query over the rate was not dropped", c.description)
		}
		if !strings.Contains(extenderErr.Error(), c.expect) {
			t.Fatalf("%s attributed error = %v, expected %q", c.description, extenderErr, c.expect)
		}
	}
}

// Every bucket is refilled before either is taken, so a query refused by one
// limit does not spend the other's token (A6).
func TestExtenderForwarderRefillsBothBucketsBeforeTakingEither(t *testing.T) {
	settings := DefaultExtenderSettings()
	settings.DnsMaxQueryBurstPerSource = 1
	settings.DnsMaxQueryRatePerSource = 1
	settings.DnsMaxQueryBurst = 2
	settings.DnsMaxQueryRate = 1
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := NewExtenderServer(ctx, nil, nil, nil, nil, settings)
	defer server.Close()
	forwarder := &extenderDnsForwarder{
		server:       server,
		sourceStates: map[string]*extenderDnsSource{},
	}

	now := time.Now()
	if err := forwarder.admit("192.0.2.20", now); err != nil {
		t.Fatalf("the first query was refused: %v", err)
	}
	forwarder.release("192.0.2.20")
	if err := forwarder.admit("192.0.2.20", now); err == nil {
		t.Fatal("a source over its burst was admitted")
	}
	// the refused query must not have spent a total token, so the second one
	// is still there for another source
	if err := forwarder.admit("192.0.2.21", now); err != nil {
		t.Fatalf("the second total token was refused: %v", err)
	}
	forwarder.release("192.0.2.21")
	if err := forwarder.admit("192.0.2.22", now); err == nil {
		t.Fatal("a query over the total burst was admitted")
	}
	// a second later one token of each has been earned back
	if err := forwarder.admit("192.0.2.22", now.Add(time.Second)); err != nil {
		t.Fatalf("the refilled token was refused: %v", err)
	}
}

// recordingPacketConn stands in for the carrier socket, so a forwarder test
// can drive sources the loopback does not have.
type recordingPacketConn struct {
	answers chan []byte
}

func newRecordingPacketConn() *recordingPacketConn {
	return &recordingPacketConn{
		answers: make(chan []byte, 16),
	}
}

// The forwarder never reads the socket; the translation owns that side.
func (self *recordingPacketConn) ReadFrom([]byte) (int, net.Addr, error) {
	return 0, nil, net.ErrClosed
}

func (self *recordingPacketConn) WriteTo(answerBytes []byte, _ net.Addr) (int, error) {
	select {
	case self.answers <- append([]byte(nil), answerBytes...):
	default:
	}
	return len(answerBytes), nil
}

func (self *recordingPacketConn) LocalAddr() net.Addr {
	return &net.UDPAddr{IP: net.ParseIP("192.0.2.1"), Port: 53}
}

func (self *recordingPacketConn) Close() error {
	return nil
}

func (self *recordingPacketConn) SetDeadline(time.Time) error {
	return nil
}

func (self *recordingPacketConn) SetReadDeadline(time.Time) error {
	return nil
}

func (self *recordingPacketConn) SetWriteDeadline(time.Time) error {
	return nil
}

// Every query is handed to a bounded pool of workers, and a query that arrives
// with the pool full is dropped rather than queued (A6). Each query comes from
// its own source, which the one in flight rule requires.
func TestExtenderForwarderDropsWhenItsWorkersAreBusy(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	started := make(chan struct{}, 4)
	extenderErrors := make(chan error, 8)

	settings := DefaultExtenderSettings()
	settings.DnsForwardWorkerCount = 1
	// the pool is what refuses here, not the rates
	settings.DnsMaxQueryRatePerSource = 0
	settings.DnsMaxQueryRate = 0
	settings.DnsForwardTimeout = 60 * time.Second
	settings.DnsForward = func(ctx context.Context, _ dnsmessage.Type, _ string) ([]byte, bool) {
		started <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, false
	}
	settings.ErrorHandler = func(stage string, err error) {
		select {
		case extenderErrors <- err:
		default:
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := NewExtenderServer(ctx, nil, nil, nil, nil, settings)
	defer server.CloseAndWait()
	forwarder := newExtenderDnsForwarder(server, newRecordingPacketConn())
	defer forwarder.close()

	queryBytes := newTestDnsQuery(t, 0x6666, testForwardName, dnsmessage.TypeA)
	sourceAddr := func(lastByte int) net.Addr {
		return &net.UDPAddr{IP: net.ParseIP(fmt.Sprintf("192.0.2.%d", lastByte)), Port: 1053}
	}

	// the one worker takes the first query and stalls in the forward
	forwarder.handleQuery(queryBytes, sourceAddr(31))
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the first query never reached the forward")
	}
	// the second fills the queue the worker drains
	forwarder.handleQuery(queryBytes, sourceAddr(32))
	select {
	case err := <-extenderErrors:
		t.Fatalf("the queued query was dropped: %v", err)
	default:
	}
	// the third finds both full
	forwarder.handleQuery(queryBytes, sourceAddr(33))
	select {
	case err := <-extenderErrors:
		if !strings.Contains(err.Error(), "workers are busy") {
			t.Fatalf("attributed error = %v, expected the busy worker pool", err)
		}
	default:
		t.Fatal("a query with the pool full was not dropped")
	}
	// the dropped source keeps no in flight slot, so it is admitted again once
	// the pool drains
	forwarder.release("192.0.2.33")
}

// A forward that never answers holds only its worker: the dns carrier, which
// shares the same socket and the same read loop, keeps working (A6).
func TestExtenderForwarderNeverBlocksTheDnsCarrier(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	fixture := newExtenderFixture(t, "127.0.0.1", func(settings *ExtenderSettings) {
		settings.DnsForwardWorkerCount = 1
		settings.DnsForwardTimeout = 60 * time.Second
		settings.DnsForward = func(ctx context.Context, _ dnsmessage.Type, _ string) ([]byte, bool) {
			select {
			case started <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-ctx.Done():
			}
			return nil, false
		}
	})
	defer close(release)

	conn := newTestDnsClient(t, fixture)
	if _, err := conn.Write(newTestDnsQuery(t, 0x7777, testForwardName, dnsmessage.TypeA)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("the query never reached the forward")
	}

	// the translation's read loop is the same goroutine the stalled query
	// arrived on; the dns carrier must still complete a request through it
	client := connect.NewExtenderHttpClient(
		fixture.connectSettings(),
		fixture.extenderConfig(connect.ExtenderCarrierDns),
	)
	defer client.CloseIdleConnections()
	response, err := client.Get("https://dest.example/")
	if err != nil {
		if extenderErr, ok := fixture.nextError(); ok {
			t.Fatalf("%v; extender: %v", err, extenderErr)
		}
		t.Fatal(err)
	}
	response.Body.Close()
}
