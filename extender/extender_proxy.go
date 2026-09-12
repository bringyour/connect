package extender

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/urnetwork/connect"
)

// The whitelist and the reverse proxy fallback (EXTENDER.md A5).
//
// An extender terminates the outer tls for every server name, so it sees every
// request a prober makes. A request that is not the extender protocol is
// answered by fetching the same request from the real site the name belongs
// to, over the extender's own egress with normal ca verification, and relaying
// the answer back. A prober that speaks plain https therefore gets the real
// site behind the extender's self-signed certificate, which is the whole point
// of the whitelist: an extender looks like a misconfigured host, not like a
// service that only speaks one private protocol.
//
// The whitelist is the union of the bundled spoof names, which are the names a
// client fronts an extender with, and the operator patterns. It is only about
// what may be proxied. An extender request's destination is a different
// question and stays with the operator patterns alone, so a spoof name is
// never a valid destination.
//
// Bounds are per client connection and per source address, so one prober
// cannot turn an extender into an open relay: the request body and the
// concurrency bounds refuse with 503 before anything upstream is opened, while
// the response bound cuts the body, because the status and the headers have
// already been written by then and a 503 is no longer available.
//
// The type is safe for concurrent use.

// Headers that belong to one hop and must not be relayed.
var proxyHopByHopHeaders = []string{
	"Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te",
	"Trailer",
	"Transfer-Encoding",
	"Upgrade",
}

// Relay buffer of one proxied response.
const proxyRelayBufferByteCount = 4096

type extenderProxy struct {
	server *ExtenderServer
	// the whitelist, lowercased: exact names and `*.` patterns (A5)
	whitelistPatterns []string

	stateLock              sync.Mutex
	connectionCount        int
	sourceConnectionCounts map[string]int
	// one upstream transport per dial network, so a family's connection pool
	// is never reused for the other family (A7)
	networkTransports map[string]*http.Transport
}

func newExtenderProxy(server *ExtenderServer) *extenderProxy {
	spoofDomains := server.settings.SpoofDomains
	if spoofDomains == nil {
		spoofDomains = connect.SpoofDomains()
	}
	whitelistPatterns := []string{}
	for _, pattern := range slices.Concat(spoofDomains, server.allowedHosts) {
		if pattern = strings.ToLower(strings.TrimSpace(pattern)); pattern != "" {
			whitelistPatterns = append(whitelistPatterns, pattern)
		}
	}
	return &extenderProxy{
		server:                 server,
		whitelistPatterns:      whitelistPatterns,
		sourceConnectionCounts: map[string]int{},
		networkTransports:      map[string]*http.Transport{},
	}
}

// Reports whether a requested server name may be proxied (A5). The name
// becomes the upstream authority, and tls accepts any bytes as a server name,
// so it must be a host name before it is matched at all.
func (self *extenderProxy) isWhitelisted(serverName string) bool {
	serverName = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(serverName), "."))
	if !isHostName(serverName) {
		return false
	}
	for _, pattern := range self.whitelistPatterns {
		if hostMatchesPattern(serverName, pattern) {
			return true
		}
	}
	return false
}

// Reports whether a name is a plain dns host name: non-empty labels of at most
// 63 bytes, each of letters, digits and hyphens, and at most 253 bytes overall.
// Anything else cannot be an upstream authority.
func isHostName(name string) bool {
	if name == "" || 253 < len(name) {
		return false
	}
	labelByteCount := 0
	for i := 0; i < len(name); i += 1 {
		b := name[i]
		switch {
		case b == '.':
			if labelByteCount == 0 {
				return false
			}
			labelByteCount = 0
		case 'a' <= b && b <= 'z', '0' <= b && b <= '9', b == '-':
			labelByteCount += 1
			if 63 < labelByteCount {
				return false
			}
		default:
			return false
		}
	}
	return 0 < labelByteCount
}

// Matches one host against an exact name or a `*.` wildcard. A wildcard does
// not match the bare name, which is the v1 behavior of the allowed hosts.
func hostMatchesPattern(host string, pattern string) bool {
	if host == pattern {
		return true
	}
	if strings.HasPrefix(pattern, "*.") {
		return strings.HasSuffix(host, pattern[1:])
	}
	return false
}

// Reserves one proxied slot for a source address (A5).
func (self *extenderProxy) beginConnection(source string) bool {
	settings := self.server.settings
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if 0 < settings.ProxyMaxConnectionCount && settings.ProxyMaxConnectionCount <= self.connectionCount {
		return false
	}
	if 0 < settings.ProxyMaxConnectionCountPerSource &&
		settings.ProxyMaxConnectionCountPerSource <= self.sourceConnectionCounts[source] {
		return false
	}
	self.connectionCount += 1
	self.sourceConnectionCounts[source] += 1
	return true
}

// Releases one proxied slot.
func (self *extenderProxy) endConnection(source string) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.connectionCount -= 1
	if count := self.sourceConnectionCounts[source] - 1; 0 < count {
		self.sourceConnectionCounts[source] = count
	} else {
		delete(self.sourceConnectionCounts, source)
	}
}

// The upstream client of one dial network. The dial goes through the same seam
// the forward uses, narrowed to the family of the client's socket (A5, A7).
func (self *extenderProxy) transportForNetwork(network string) *http.Transport {
	cached := func() *http.Transport {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		return self.networkTransports[network]
	}()
	if cached != nil {
		return cached
	}

	settings := self.server.settings
	var tlsConfig *tls.Config
	if settings.ProxyTlsConfig != nil {
		tlsConfig = settings.ProxyTlsConfig.Clone()
	}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _ string, address string) (net.Conn, error) {
			return self.server.dialContext()(ctx, network, address)
		},
		TLSClientConfig:       tlsConfig,
		ForceAttemptHTTP2:     true,
		IdleConnTimeout:       settings.ProxyIdleTimeout,
		TLSHandshakeTimeout:   settings.ProxyIdleTimeout,
		ResponseHeaderTimeout: settings.ProxyIdleTimeout,
	}

	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if cached, ok := self.networkTransports[network]; ok {
		// another request built the same network first; keep one pool
		transport.CloseIdleConnections()
		return cached
	}
	self.networkTransports[network] = transport
	return transport
}

// Releases every upstream pool at shutdown.
func (self *extenderProxy) close() {
	transports := func() []*http.Transport {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		transports := make([]*http.Transport, 0, len(self.networkTransports))
		for _, transport := range self.networkTransports {
			transports = append(transports, transport)
		}
		clear(self.networkTransports)
		return transports
	}()
	for _, transport := range transports {
		transport.CloseIdleConnections()
	}
}

// serve answers one request that is not an extender request: the real site of
// the requested name when it is on the whitelist, and 403 otherwise (A5).
func (self *extenderProxy) serve(w http.ResponseWriter, req *http.Request) {
	server := self.server

	serverName := extenderRequestServerName(req)
	if !self.isWhitelisted(serverName) {
		refuseRequest(
			server,
			w,
			req,
			http.StatusForbidden,
			"proxy name",
			fmt.Errorf("server name %q is not on the whitelist", serverName),
		)
		return
	}

	source := connectionSourceAddress(req.RemoteAddr)
	if !self.beginConnection(source) {
		refuseRequest(
			server,
			w,
			req,
			http.StatusServiceUnavailable,
			"proxy limit",
			fmt.Errorf("%s is over the proxied connection bound", source),
		)
		return
	}
	defer self.endConnection(source)

	requestBody, err := self.readRequestBody(req)
	if err != nil {
		refuseRequest(server, w, req, http.StatusServiceUnavailable, "proxy request", err)
		return
	}

	upstreamCtx, upstreamCancel := context.WithCancel(req.Context())
	defer upstreamCancel()
	upstreamRequest, err := self.newUpstreamRequest(upstreamCtx, req, serverName, requestBody)
	if err != nil {
		refuseRequest(server, w, req, http.StatusServiceUnavailable, "proxy request", err)
		return
	}

	transport := self.transportForNetwork(forwardNetwork(req.RemoteAddr))
	upstreamResponse, err := transport.RoundTrip(upstreamRequest)
	if err != nil {
		refuseRequest(server, w, req, http.StatusServiceUnavailable, "proxy upstream", err)
		return
	}
	defer upstreamResponse.Body.Close()

	for name, values := range upstreamResponse.Header {
		if slices.ContainsFunc(proxyHopByHopHeaders, func(hopHeader string) bool {
			return strings.EqualFold(hopHeader, name)
		}) {
			continue
		}
		// the relayed body may be cut by the response bound, so its length is
		// this server's to decide
		if strings.EqualFold(name, "Content-Length") {
			continue
		}
		for _, value := range values {
			w.Header().Add(name, value)
		}
	}
	w.WriteHeader(upstreamResponse.StatusCode)
	self.relayResponse(w, req, upstreamResponse.Body, upstreamCancel)
}

// The request body, bounded by the A5 request bound. It is read whole so a
// body over the bound is refused before anything upstream is opened.
func (self *extenderProxy) readRequestBody(req *http.Request) (io.Reader, error) {
	maxByteCount := self.server.settings.ProxyMaxRequestByteCount
	if req.Body == nil || req.Body == http.NoBody {
		return nil, nil
	}
	if 0 < maxByteCount && maxByteCount < req.ContentLength {
		return nil, fmt.Errorf("request body is %d bytes, at most %d", req.ContentLength, maxByteCount)
	}
	var reader io.Reader = req.Body
	if 0 < maxByteCount {
		reader = io.LimitReader(req.Body, maxByteCount+1)
	}
	bodyBytes, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if 0 < maxByteCount && maxByteCount < int64(len(bodyBytes)) {
		return nil, fmt.Errorf("request body is over %d bytes", maxByteCount)
	}
	return bytes.NewReader(bodyBytes), nil
}

// The same request addressed to the real site: `https://<sni>` with the host
// header set to the name, so a virtual host answers as it would for a client
// that reached it directly (A5).
func (self *extenderProxy) newUpstreamRequest(
	ctx context.Context,
	req *http.Request,
	serverName string,
	requestBody io.Reader,
) (*http.Request, error) {
	upstreamUrl := &url.URL{
		Scheme: "https",
		Host:   serverName,
	}
	if req.URL != nil {
		upstreamUrl.Path = req.URL.Path
		upstreamUrl.RawQuery = req.URL.RawQuery
	}
	upstreamRequest, err := http.NewRequestWithContext(
		ctx,
		req.Method,
		upstreamUrl.String(),
		requestBody,
	)
	if err != nil {
		return nil, err
	}
	for name, values := range req.Header {
		if slices.ContainsFunc(proxyHopByHopHeaders, func(hopHeader string) bool {
			return strings.EqualFold(hopHeader, name)
		}) {
			continue
		}
		for _, value := range values {
			upstreamRequest.Header.Add(name, value)
		}
	}
	upstreamRequest.Host = serverName
	return upstreamRequest, nil
}

// Relays the upstream body, cutting it at the A5 response bound and releasing
// the exchange when the site stops sending for the idle bound. Both are
// visible to the client only as a short body, because the status and headers
// have already been written.
func (self *extenderProxy) relayResponse(
	w http.ResponseWriter,
	req *http.Request,
	body io.Reader,
	upstreamCancel context.CancelFunc,
) {
	settings := self.server.settings
	proxyConn := extenderRequestProxyConn(req)
	if settings.ProxyIdleTimeout <= 0 {
		self.copyBody(w, body, proxyConn, nil)
		return
	}

	// the watchdog cancels the upstream request when no relayed step lands
	// within the idle bound, which is what ends a site that stalls mid body
	progress := make(chan struct{}, 1)
	relayDone := make(chan struct{})
	go connect.HandleError(func() {
		for {
			select {
			case <-relayDone:
				return
			case <-progress:
			case <-time.After(settings.ProxyIdleTimeout):
				self.server.reportError("proxy relay", fmt.Errorf("the upstream site stopped sending"))
				upstreamCancel()
				return
			}
		}
	})
	self.copyBody(w, body, proxyConn, progress)
	close(relayDone)
}

// Copies the upstream body within the per-connection relay budget, signaling
// progress so an idle watchdog can tell a slow site from a stalled one.
func (self *extenderProxy) copyBody(
	w http.ResponseWriter,
	body io.Reader,
	proxyConn *extenderProxyConn,
	progress chan struct{},
) {
	maxByteCount := self.server.settings.ProxyMaxResponseByteCount
	flusher, _ := w.(http.Flusher)
	buffer := make([]byte, proxyRelayBufferByteCount)
	for {
		n, err := body.Read(buffer)
		if 0 < n {
			relayByteCount := n
			if 0 < maxByteCount {
				relayByteCount = proxyConn.takeRelayBudget(maxByteCount, n)
			}
			if 0 < relayByteCount {
				if _, writeErr := w.Write(buffer[0:relayByteCount]); writeErr != nil {
					return
				}
				if flusher != nil {
					flusher.Flush()
				}
			}
			if relayByteCount < n {
				self.server.reportError("proxy relay", fmt.Errorf(
					"the relayed response reached %d bytes",
					maxByteCount,
				))
				return
			}
			if progress != nil {
				select {
				case progress <- struct{}{}:
				default:
				}
			}
		}
		if err != nil {
			return
		}
	}
}

// extenderProxyConn is the per-client-connection state of the reverse proxy.
// The relayed budget is per connection, so a keep-alive prober cannot reset it
// by asking again on the same connection (A5).
//
// It is safe for concurrent use, which h2 and h3 need: several streams of one
// connection share one budget.
type extenderProxyConn struct {
	stateLock        sync.Mutex
	relayedByteCount int64
}

func newExtenderProxyConn() *extenderProxyConn {
	return &extenderProxyConn{}
}

// Claims up to byteCount of the connection's remaining relay budget, returning
// how much of it may be written. A short result means the budget ran out.
func (self *extenderProxyConn) takeRelayBudget(maxByteCount int64, byteCount int) int {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	remaining := maxByteCount - self.relayedByteCount
	if remaining <= 0 {
		return 0
	}
	if remaining < int64(byteCount) {
		byteCount = int(remaining)
	}
	self.relayedByteCount += int64(byteCount)
	return byteCount
}

// The connection values every request of a terminated connection carries: the
// requested name, which is not on the request itself on tcp (A3), and the
// proxy budget of that connection (A5).
type extenderServerNameContextKey struct{}
type extenderProxyConnContextKey struct{}

func newExtenderRequestContext(ctx context.Context, serverName string) context.Context {
	ctx = context.WithValue(ctx, extenderServerNameContextKey{}, serverName)
	return context.WithValue(ctx, extenderProxyConnContextKey{}, newExtenderProxyConn())
}

// The requested server name of one request: the terminated connection's name
// on tcp, where the served connection is no longer a *tls.Conn, and the
// request's own connection state on the udp carriers.
func extenderRequestServerName(req *http.Request) string {
	if serverName, ok := req.Context().Value(extenderServerNameContextKey{}).(string); ok && serverName != "" {
		return serverName
	}
	if req.TLS != nil {
		return req.TLS.ServerName
	}
	return ""
}

// The proxy budget of the connection this request arrived on. A request with
// no connection state gets a budget of its own, which is the conservative
// reading of a per-connection bound.
func extenderRequestProxyConn(req *http.Request) *extenderProxyConn {
	if proxyConn, ok := req.Context().Value(extenderProxyConnContextKey{}).(*extenderProxyConn); ok {
		return proxyConn
	}
	return newExtenderProxyConn()
}

// Answers one refused request with no body, closing an http/1.1 connection
// after it (A4, A5).
func refuseRequest(
	server *ExtenderServer,
	w http.ResponseWriter,
	req *http.Request,
	statusCode int,
	stage string,
	err error,
) {
	server.reportError(stage, err)
	if req.ProtoMajor == 1 {
		w.Header().Set("Connection", "close")
		w.Header().Set("Content-Length", "0")
	}
	w.WriteHeader(statusCode)
}
