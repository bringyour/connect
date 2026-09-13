package connect

import (
	"context"
	"crypto/tls"
	"fmt"
	mathrand "math/rand"
	"net"
	"net/http"
	"strconv"
	"time"

	quic "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

// net_http_alt.go -- the api's two alt dialers (EXTENDER.md L4).
//
// Alt serves the api over H3 on udp 443 and over the dns translation (whodis)
// on udp 4053, and on 53 through the router. Both dialers here reach it the
// same way: the alt host decides which addresses the packets go to, the
// request's own host name stays the sni, and the platform's verified tls
// config verifies the certificate exactly as a direct api dial does. Nothing
// about the request changes; only the path it takes.
//
// Both are api only. The platform transport reaches alt through its own H3
// modes (transport_family.go), so these dialers carry no websocket dialer at
// all and every websocket dial skips them.
//
// The addresses come from the strategy's own resolver -- the network space DoH
// cache when the host is protected, the egress-aware resolver otherwise -- for
// both families, and are raced happy eyeballs style by the same helper the
// platform H3 dial uses. The whodis dialer races 53 before 4053, so a network
// that passes public dns still reaches alt on the port the router forwards.

// The dialer priorities of L4: alt h3 runs after the tcp dialers (0 to 50) and
// before the extender carriers (100, 110, 120); alt whodis runs last, since it
// is the slowest path and the one that looks least like ordinary traffic.
const (
	altH3DialerPriority     = 60
	altWhodisDialerPriority = 130
)

// The alt dialers' minimum weight, the same floor the resilient dialers carry:
// a path that is never chosen while direct tcp works must still be tried often
// enough to learn that it works when tcp stops.
const altDialerMinimumWeight = 0.25

// newAltDialers builds the two api-only alt dialers of one strategy (L4). None
// when no alt url is configured, which is every space that has not deployed
// alt yet.
func newAltDialers(clientStrategy *ClientStrategy, settings *ClientStrategySettings) []*clientDialer {
	altHost, _ := altUrlHostPort(settings.AltUrl)
	if altHost == "" {
		return nil
	}
	dialers := []*clientDialer{}
	for _, altDialer := range []struct {
		description string
		priority    int
		whodis      bool
	}{
		{description: "alt h3", priority: altH3DialerPriority, whodis: false},
		{description: "alt whodis", priority: altWhodisDialerPriority, whodis: true},
	} {
		whodis := altDialer.whodis
		dialers = append(dialers, &clientDialer{
			description:   altDialer.description,
			createTime:    time.Now(),
			minimumWeight: altDialerMinimumWeight,
			priority:      altDialer.priority,
			// no dialTlsContext: there is no websocket over alt from here
			httpClientFactory: func() *http.Client {
				return newAltHttpClient(clientStrategy, settings, whodis)
			},
			settings: settings,
		})
	}
	return dialers
}

// One alt dialer's api client: an http3 round tripper whose quic dial goes to
// the alt addresses while everything above it -- the request, the host header,
// the sni and the certificate verification -- stays the api's.
func newAltHttpClient(
	clientStrategy *ClientStrategy,
	settings *ClientStrategySettings,
	whodis bool,
) *http.Client {
	transport := &http3.Transport{
		// ServerName is left empty so http3 fills it from the request host,
		// which is the api name alt holds the certificate for (L1, L3)
		TLSClientConfig: newClientTlsConfig(settings.TlsConfig, nil),
		QUICConfig: &quic.Config{
			HandshakeIdleTimeout: settings.ConnectTimeout + settings.TlsTimeout,
			MaxIdleTimeout:       settings.IdleConnTimeout,
		},
		Dial: func(
			ctx context.Context,
			addr string,
			tlsConfig *tls.Config,
			quicConfig *quic.Config,
		) (*quic.Conn, error) {
			return clientStrategy.dialAltQuic(ctx, whodis, tlsConfig, quicConfig)
		},
	}
	return &http.Client{
		Transport: transport,
		Timeout:   settings.RequestTimeout,
	}
}

// dialAltQuic opens one quic connection to the alt host for an api request
// (L4). The returned connection is owned by the http3 transport; the socket,
// the translation and the quic transport under it are released when it dies or
// when the strategy closes.
func (self *ClientStrategy) dialAltQuic(
	ctx context.Context,
	whodis bool,
	tlsConfig *tls.Config,
	quicConfig *quic.Config,
) (*quic.Conn, error) {
	candidates, err := self.altDialCandidates(ctx, whodis)
	if err != nil {
		return nil, err
	}
	wrap := func(_ context.Context, packetConn net.PacketConn) (net.PacketConn, error) {
		return packetConn, nil
	}
	if whodis {
		tld := altDnsTld(self.settings)
		wrap = func(attemptCtx context.Context, packetConn net.PacketConn) (net.PacketConn, error) {
			ptSettings := DefaultPacketTranslationSettings()
			ptSettings.Log = self.settings.ConnectSettings.Log
			ptSettings.DnsTlds = [][]byte{tld}
			// the cleanup owns the translation; keep its encoder alive while
			// cancellation closes quic gracefully, exactly as the platform
			// dns carrier does
			return NewPacketTranslation(
				context.WithoutCancel(attemptCtx),
				PacketTranslationModeDns,
				packetConn,
				ptSettings,
			)
		}
	}
	dial := func(attemptCtx context.Context, udpAddr *net.UDPAddr) (*h3DialAttempt, error) {
		return dialAltQuicAttempt(
			attemptCtx,
			&self.settings.ConnectSettings,
			udpAddr,
			wrap,
			tlsConfig,
			quicConfig,
		)
	}
	var attempt *h3DialAttempt
	if len(candidates) == 1 {
		attempt, err = dial(ctx, candidates[0])
	} else {
		attempt, err = raceH3Dial(ctx, candidates, dial)
	}
	if err != nil {
		return nil, err
	}
	conn := attempt.conn
	go HandleError(func() {
		select {
		case <-conn.Context().Done():
		case <-self.ctx.Done():
		}
		attempt.close()
	})
	return conn, nil
}

// The addresses one alt dial may use, in dial order (L2, L4): every address of
// the alt host that the family pin and the family policy permit, v6 first
// interleaved, on each carrier port -- 53 before 4053 for whodis. One
// resolution serves every port.
func (self *ClientStrategy) altDialCandidates(
	ctx context.Context,
	whodis bool,
) ([]*net.UDPAddr, error) {
	altHost, altPort := altUrlHostPort(self.settings.AltUrl)
	if altHost == "" {
		return nil, fmt.Errorf("the alt url names no host")
	}
	ports := []int{altPort}
	if whodis {
		ports = altDnsPorts(altPort, DefaultDnsPort)
	} else if altPort <= 0 {
		ports = []int{DefaultAltH3Port}
	}
	// a family-pinned strategy dials alt on its family only: an api call that
	// crossed the other family would prove the wrong address to the operator
	udpAddrs, err := self.resolveControlUDPAddrs(
		ctx,
		net.JoinHostPort(altHost, strconv.Itoa(ports[0])),
		self.settings.ipFamily,
	)
	if err != nil {
		return nil, err
	}
	candidates := []*net.UDPAddr{}
	for _, port := range ports {
		for _, udpAddr := range udpAddrs {
			candidates = append(candidates, &net.UDPAddr{
				IP:   udpAddr.IP,
				Port: port,
				Zone: udpAddr.Zone,
			})
		}
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("the alt host resolved no address")
	}
	return candidates, nil
}

// The encoding tld of the whodis dialer, one at random per dial as the
// platform dns carrier picks one. The connect default when the settings name
// none.
func altDnsTld(settings *ClientStrategySettings) []byte {
	if 0 < len(settings.DnsTlds) {
		return settings.DnsTlds[mathrand.Intn(len(settings.DnsTlds))]
	}
	return []byte(DefaultExtenderDnsTld)
}

// dialAltQuicAttempt opens the endpoint for one candidate address, wraps it for
// the carrier and completes the quic dial on it. The returned attempt owns its
// socket, translation, transport and connection, so one close releases all of
// them. Dial rather than DialEarly: a race must not be won by cached 0-RTT
// parameters before the peer has answered.
func dialAltQuicAttempt(
	ctx context.Context,
	connectSettings *ConnectSettings,
	udpAddr *net.UDPAddr,
	wrap h3PacketConnWrapper,
	tlsConfig *tls.Config,
	quicConfig *quic.Config,
) (*h3DialAttempt, error) {
	// the same endpoint policy the extender carriers use: an injected factory
	// wins, so a headless host keeps one source identity, and otherwise the
	// wildcard of the destination's family
	packetConn, err := openExtenderPacketConn(ctx, connectSettings, udpAddr)
	if err != nil {
		// ownership transfers for every non-nil result, including a rejected
		// one
		if packetConn != nil {
			packetConn.Close()
		}
		return nil, err
	}
	if packetConn == nil {
		return nil, fmt.Errorf("alt packet connection factory returned nil")
	}
	attempt := &h3DialAttempt{
		udpAddr:    udpAddr,
		packetConn: packetConn,
	}
	success := false
	defer func() {
		if !success {
			attempt.close()
		}
	}()
	// bind to the physical egress interface so an api dial never loops into
	// the tunnel this process provides (R1); a no-op off windows and when no
	// egress index is set. a bind failure is not fatal, and the log line is
	// where an unpinned socket becomes visible. an injected endpoint is the
	// embedder's, with its own binding, so it is left alone
	injectedPacketConn := connectSettings.DialContextSettings != nil &&
		connectSettings.DialContextSettings.PacketConnFactory != nil
	if udpConn, ok := packetConn.(*net.UDPConn); ok && !injectedPacketConn {
		if bindErr := applyEgress(udpConn); bindErr != nil {
			loggerOrDefault(connectSettings.Log).Infof(
				"[net]alt egress bind failed, the api connection may loop into the tunnel: %s\n",
				bindErr,
			)
		}
	}
	wrapped, err := wrap(ctx, attempt.packetConn)
	if err != nil {
		return nil, err
	}
	attempt.packetConn = wrapped

	attempt.quicTransport = &quic.Transport{
		Conn: attempt.packetConn,
	}
	// per attempt: a race runs several dials against one config
	conn, err := attempt.quicTransport.Dial(ctx, udpAddr, tlsConfig.Clone(), quicConfig.Clone())
	if err != nil {
		return nil, err
	}
	attempt.conn = conn
	success = true
	return attempt, nil
}
