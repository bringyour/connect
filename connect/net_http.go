package connect

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	// "errors"
	"fmt"
	"io"
	mathrand "math/rand"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/nettest"

	"golang.org/x/exp/maps"

	"github.com/gorilla/websocket"

	"github.com/golang/glog"
)

// censorship-resistant strategies for making https connections
// this uses a random walk of best practices to obfuscate and double-encrypt https connections

// note the net_* files are migrated from net.IP/IPNet to netip.Addr/Prefix
// TODO generally the entire package should migrate over to these newer structs

type DialTlsContextFunction = func(ctx context.Context, network string, addr string) (net.Conn, error)

func DefaultClientStrategySettings() *ClientStrategySettings {
	return &ClientStrategySettings{
		ExposeServerIps:       true,
		ExposeServerHostNames: true,

		EnableNormal:    true,
		EnableResilient: true,

		ParallelBlockSize: 4,

		ExpandExtenderProfileCount: 8,
		ExtenderNetworks:           []netip.Prefix{},
		ExtenderHostnames:          []string{},
		ExpandExtenderRateLimit:    1 * time.Second,
		MaxExtenderCount:           128,
		ExtenderMinimumWeight:      0.1,
		ExtenderDropTimeout:        5 * time.Minute,

		DohSettings: DefaultDohSettings(),

		HelloRetryTimeout: 5 * time.Second,

		ConnectSettings: *DefaultConnectSettings(),
	}
}

func DefaultConnectSettings() *ConnectSettings {
	return &ConnectSettings{
		RequestTimeout:   30 * time.Second,
		ConnectTimeout:   5 * time.Second,
		TlsTimeout:       5 * time.Second,
		HandshakeTimeout: 5 * time.Second,
		IdleConnTimeout:  90 * time.Second,
		KeepAliveTimeout: 5 * time.Second,
		KeepAliveConfig: net.KeepAliveConfig{
			Enable:   true,
			Idle:     5 * time.Second,
			Interval: 5 * time.Second,
			Count:    1,
		},
		TlsConfig: nil,
	}
}

type ConnectSettings struct {
	RequestTimeout   time.Duration
	ConnectTimeout   time.Duration
	TlsTimeout       time.Duration
	HandshakeTimeout time.Duration
	IdleConnTimeout  time.Duration
	KeepAliveTimeout time.Duration
	KeepAliveConfig  net.KeepAliveConfig

	TlsConfig *tls.Config
}

type ClientStrategySettings struct {
	// expose consistent ips
	// if true, enables ech
	ExposeServerIps bool
	// expose server names
	// if true, enables non-ech
	// TODO set this to default false
	ExposeServerHostNames bool
	// note that extenders and proxy are the only strategies that will be enabled if
	// `ExposeServerIps == false` and `ExposeServerNames == false`

	EnableNormal bool
	// tls frag, retransmit, tls frag + retransmit
	EnableResilient bool

	// for gets and ws connects
	ParallelBlockSize int

	// the number of new profiles to add per expand
	ExpandExtenderProfileCount int
	ExtenderNetworks           []netip.Prefix
	// these are evaluated with DoH to grow the extender ips
	ExtenderHostnames       []string
	ExpandExtenderRateLimit time.Duration
	MaxExtenderCount        int
	// extender minimum weight
	ExtenderMinimumWeight float32
	// drop dialers that have not had a successful connect in this timeout
	ExtenderDropTimeout time.Duration

	DohSettings *DohSettings

	HelloRetryTimeout time.Duration

	ConnectSettings
}

// stores statistics on client strategies
type ClientStrategy struct {
	ctx context.Context

	settings *ClientStrategySettings

	mutex sync.Mutex
	// dialers are only updated inside the mutex
	dialers             map[*clientDialer]bool
	resolvedExtenderIps []netip.Addr

	// custom extenders
	// these take precedence over other extenders
	extenderIpSecrets map[netip.Addr]string
}

func NewClientStrategyWithDefaults(ctx context.Context) *ClientStrategy {
	return NewClientStrategy(ctx, DefaultClientStrategySettings())
}

// extender udp 53 to platform extender
func NewClientStrategy(ctx context.Context, settings *ClientStrategySettings) *ClientStrategy {
	// create dialers to match settings
	dialers := map[*clientDialer]bool{}
	resolvedExtenderIps := []netip.Addr{}

	if settings.EnableNormal {
		// TODO ECH support
		if settings.ExposeServerHostNames && settings.ExposeServerIps {
			tlsDialer := &tls.Dialer{
				NetDialer: &net.Dialer{
					Timeout:         settings.ConnectTimeout,
					KeepAlive:       settings.KeepAliveTimeout,
					KeepAliveConfig: settings.KeepAliveConfig,
				},
				Config: settings.TlsConfig,
			}

			dialer := &clientDialer{
				description:    "normal",
				minimumWeight:  0.5,
				priority:       25,
				dialTlsContext: tlsDialer.DialContext,
				settings:       settings,
			}
			dialers[dialer] = true
		}
	}
	if settings.EnableResilient {
		// TODO ECH support
		if settings.ExposeServerHostNames && settings.ExposeServerIps {
			// fragment+reorder
			dialer1 := &clientDialer{
				description:    "fragment+reorder",
				minimumWeight:  0.25,
				priority:       50,
				dialTlsContext: NewResilientDialTlsContext(&settings.ConnectSettings, true, true),
				settings:       settings,
			}
			// fragment
			// this is the highest priority because it has no performance impact and additional security benefits
			dialer2 := &clientDialer{
				description:    "fragment",
				minimumWeight:  0.25,
				priority:       0,
				dialTlsContext: NewResilientDialTlsContext(&settings.ConnectSettings, true, false),
				settings:       settings,
			}
			// reorder
			dialer3 := &clientDialer{
				description:    "reorder",
				minimumWeight:  0.25,
				priority:       50,
				dialTlsContext: NewResilientDialTlsContext(&settings.ConnectSettings, false, true),
				settings:       settings,
			}

			dialers[dialer1] = true
			dialers[dialer2] = true
			dialers[dialer3] = true
		}
	}

	return &ClientStrategy{
		ctx:                 ctx,
		settings:            settings,
		dialers:             dialers,
		resolvedExtenderIps: resolvedExtenderIps,
		extenderIpSecrets:   map[netip.Addr]string{},
	}

}

func (self *ClientStrategy) SetCustomExtenders(extenderIpSecrets map[netip.Addr]string) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.extenderIpSecrets = maps.Clone(extenderIpSecrets)
	for dialer, _ := range self.dialers {
		if dialer.IsExtender() {
			delete(self.dialers, dialer)
		}
	}
}

func (self *ClientStrategy) CustomExtenders() map[netip.Addr]string {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	return maps.Clone(self.extenderIpSecrets)
}

func (self *ClientStrategy) dialerWeights() map[*clientDialer]float32 {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	weights := map[*clientDialer]float32{}

	if len(self.extenderIpSecrets) == 0 {
		for dialer, _ := range self.dialers {
			w := dialer.Weight()
			weights[dialer] = w
		}
	} else {
		for dialer, _ := range self.dialers {
			if dialer.IsExtender() {
				weights[dialer] = 1.0
			}
		}
	}

	return weights
}

type httpResult struct {
	response *http.Response

	// status string
	// statusCode int
	// header http.Header
	// trailer http.Header
	bodyBytes []byte
}

type evalResult struct {
	dialer *clientDialer
	wsConn *websocket.Conn
	err    error

	httpResult
}

func newEvalResultFromHttpResponse(response *http.Response, err error) *evalResult {
	if err == nil {
		defer response.Body.Close()
		bodyBytes, err := io.ReadAll(response.Body)
		return &evalResult{
			err: err,
			httpResult: httpResult{
				response: response,
				// status: response.Status,
				// statusCode: response.StatusCode,
				// header: response.Header.Clone(),
				// trailer: response.Trailer.Clone(),
				bodyBytes: bodyBytes,
			},
		}
	} else {
		return &evalResult{
			err: err,
		}
	}
}

func (self *evalResult) Close() {
	if self.wsConn != nil {
		self.wsConn.Close()
		// if wsConn is set, the response does not need to be closed
		// https://pkg.go.dev/github.com/gorilla/websocket#Dialer.DialContext
	}
	// else if self.response != nil {
	// 	self.response.Body.Close()
	// }
}

func (self *ClientStrategy) parallelEval(ctx context.Context, eval func(ctx context.Context, dialer *clientDialer) *evalResult) *evalResult {
	// in this order:
	// 1. try all dialers that previously worked sequentially
	// 2. try dialers that previously failed in parallel blocks
	// 3. expand the extenders and try new extenders in parallel blocks

	handleCtx, handleCancel := context.WithTimeout(ctx, self.settings.RequestTimeout)
	defer handleCancel()
	// merge handleCtx with self.ctx
	go func() {
		defer handleCancel()
		select {
		case <-handleCtx.Done():
			return
		case <-self.ctx.Done():
			return
		}
	}()

	out := make(chan *evalResult)

	run := func(dialer *clientDialer) {
		result := eval(handleCtx, dialer)
		if result != nil {
			result.dialer = dialer
		}

		select {
		case out <- result:
		case <-handleCtx.Done():
			if result != nil {
				result.Close()
			}
		}
	}

	self.collapseExtenderDialers()

	dialerWeights := self.dialerWeights()

	serialDialers := []*clientDialer{}
	parallelDialers := []*clientDialer{}

	for dialer, _ := range dialerWeights {
		if dialer.IsLastSuccess() {
			serialDialers = append(serialDialers, dialer)
		} else {
			parallelDialers = append(parallelDialers, dialer)
		}
	}

	slices.SortStableFunc(serialDialers, func(a *clientDialer, b *clientDialer) int {
		return a.priority - b.priority
	})
	for _, dialer := range serialDialers {
		select {
		case <-handleCtx.Done():
			return nil
		default:
		}

		result := eval(handleCtx, dialer)
		if result != nil {
			if result.err == nil {
				glog.Infof("[net][p]select: %s\n", dialer.String())
				return result
			}
			glog.V(2).Infof("[net][p]select: %s = %s\n", dialer.String(), result.err)
			result.Close()
		}
	}

	// note parallel dialers is in the original weighted order
	WeightedShuffle(parallelDialers, dialerWeights)
	n := min(len(parallelDialers), self.settings.ParallelBlockSize)
	for _, dialer := range parallelDialers[0:n] {
		go run(dialer)
	}
	for _, dialer := range parallelDialers[n:] {
		select {
		case <-handleCtx.Done():
			return nil
		case result := <-out:
			if result != nil {
				if result.err == nil {
					glog.Infof("[net][p]select: %s\n", result.dialer.String())
					return result
				}
				glog.V(2).Infof("[net][p]select: %s = %s\n", result.dialer.String(), result.err)
				result.Close()
			}
			go run(dialer)
		}
	}

	// keep trying as long as there is time left
	for {
		select {
		case <-handleCtx.Done():
			return nil
		default:
		}

		expandStartTime := time.Now()

		self.collapseExtenderDialers()
		expandedDialers, _ := self.expandExtenderDialers()
		if len(expandedDialers) == 0 {
			break
		}

		m := min(len(expandedDialers), self.settings.ParallelBlockSize-n)
		n += m
		for _, dialer := range expandedDialers[0:m] {
			go run(dialer)
		}
		for _, dialer := range expandedDialers[m:] {
			select {
			case <-handleCtx.Done():
				return nil
			case result := <-out:
				if result.err == nil {
					glog.Infof("[net][p]select: %s\n", result.dialer.String())
					return result
				}
				glog.V(2).Infof("[net][p]select: %s = %s\n", result.dialer.String(), result.err)
				result.Close()
				go run(dialer)
			}
		}

		// the rate limit is important when when the connect timeout is small
		// e.g. local closes due to disconnected network
		expandEndTime := time.Now()
		expandDuration := expandEndTime.Sub(expandStartTime)
		if timeout := self.settings.ExpandExtenderRateLimit - expandDuration; 0 < timeout {
			select {
			case <-handleCtx.Done():
				return nil
			case <-time.After(timeout):
			}
		}
	}

	for range n {
		select {
		case <-handleCtx.Done():
			return nil
		case result := <-out:
			if result.err == nil {
				return result
			}
			result.Close()
		}
	}

	return &evalResult{
		err: fmt.Errorf("No successful strategy found."),
	}
}

func (self *ClientStrategy) serialEval(ctx context.Context, eval func(ctx context.Context, dialer *clientDialer) *evalResult, helloEval func(ctx context.Context, dialer *clientDialer) *evalResult) *evalResult {
	handleCtx, handleCancel := context.WithTimeout(ctx, self.settings.RequestTimeout)
	defer handleCancel()
	// merge handleCtx with self.ctx
	go func() {
		defer handleCancel()
		select {
		case <-handleCtx.Done():
			return
		case <-self.ctx.Done():
			return
		}
	}()

	// keep trying as long as there is time left
	for {
		select {
		case <-handleCtx.Done():
			return nil
		default:
		}

		self.collapseExtenderDialers()

		dialerWeights := self.dialerWeights()

		serialDialers := []*clientDialer{}

		for dialer, _ := range dialerWeights {
			if dialer.IsLastSuccess() {
				serialDialers = append(serialDialers, dialer)
			}
		}

		slices.SortStableFunc(serialDialers, func(a *clientDialer, b *clientDialer) int {
			return a.priority - b.priority
		})
		for _, dialer := range serialDialers {
			select {
			case <-handleCtx.Done():
				return nil
			default:
			}

			result := eval(handleCtx, dialer)
			if result != nil {
				if result.err == nil {
					glog.Infof("[net][s]select: %s\n", dialer.String())
					return result
				}
				glog.V(2).Infof("[net][s]select: %s = %s\n", dialer.String(), result.err)
				result.Close()
			}
		}

		// it's more efficient to iterate with a parallel hello
		// keep retrying hello until at least one dialer is success
		for {
			helloStartTime := time.Now()
			result := self.parallelEval(handleCtx, helloEval)
			if result != nil {
				result.Close()
			}
			helloEndTime := time.Now()

			// check if any dialer succeeded
			successCount := 0
			for dialer, _ := range self.dialerWeights() {
				if dialer.IsLastSuccess() {
					successCount += 1
				}
			}
			if 0 < successCount {
				break
			}

			timeout := self.settings.HelloRetryTimeout - helloEndTime.Sub(helloStartTime)
			if 0 < timeout {
				select {
				case <-handleCtx.Done():
					return nil
				case <-time.After(timeout):
				}
			} else {
				select {
				case <-handleCtx.Done():
					return nil
				default:
				}
			}
		}
		// if result.err != nil {
		// 	return &evalResult{
		// 		err: result.err,
		// 	}
		// }
	}

	return &evalResult{
		err: fmt.Errorf("No successful strategy found."),
	}
}

func (self *ClientStrategy) HttpParallel(request *http.Request) (*httpResult, error) {
	eval := func(handleCtx context.Context, dialer *clientDialer) *evalResult {
		httpClient := dialer.HttpClient()
		response, err := httpClient.Do(request.WithContext(handleCtx))
		if glog.V(2) {
			if err != nil {
				glog.Infof("[net]http parallel %s %s = %s\n", request.Method, request.URL, err)
			} else {
				glog.Infof("[net]http parallel %s %s = %s\n", request.Method, request.URL, response.Status)
			}
		}

		dialer.Update(handleCtx, err)

		return newEvalResultFromHttpResponse(response, err)
	}

	result := self.parallelEval(request.Context(), eval)
	if result == nil {
		return nil, fmt.Errorf("Timeout.")
	}
	return &result.httpResult, result.err
}

func (self *ClientStrategy) HttpSerial(request *http.Request, helloRequest *http.Request) (*httpResult, error) {
	// in this order:
	// 1. try all dialers that previously worked sequentially
	// 2. retest and expand dialers using get of the hello request.
	//    This is a basic ping to the server, which is run in parallel.
	// 3. continue from 1 until timeout

	eval := func(handleCtx context.Context, dialer *clientDialer) *evalResult {
		httpClient := dialer.HttpClient()
		response, err := httpClient.Do(request.WithContext(handleCtx))
		if glog.V(2) {
			if err != nil {
				glog.Infof("[net]http serial %s %s = %s\n", request.Method, request.URL, err)
			} else {
				glog.Infof("[net]http serial %s %s = %s\n", request.Method, request.URL, response.Status)
			}
		}

		dialer.Update(handleCtx, err)

		return newEvalResultFromHttpResponse(response, err)
	}
	helloEval := func(handleCtx context.Context, dialer *clientDialer) *evalResult {
		httpClient := dialer.HttpClient()
		response, err := httpClient.Do(helloRequest.WithContext(handleCtx))
		if glog.V(2) {
			if err != nil {
				glog.Infof("[net]http serial hello %s %s = %s\n", helloRequest.Method, helloRequest.URL, err)
			} else {
				glog.Infof("[net]http serial hello %s %s = %s\n", helloRequest.Method, helloRequest.URL, response.Status)
			}
		}

		dialer.Update(handleCtx, err)

		return newEvalResultFromHttpResponse(response, err)
	}

	result := self.serialEval(request.Context(), eval, helloEval)
	if result == nil {
		return nil, fmt.Errorf("Timeout.")
	}
	return &result.httpResult, result.err
}

func (self *ClientStrategy) WsDialContext(ctx context.Context, url string, requestHeader http.Header) (*websocket.Conn, *http.Response, error) {
	eval := func(handleCtx context.Context, dialer *clientDialer) *evalResult {
		wsDialer := dialer.WsDialer(self.settings)
		wsConn, response, err := wsDialer.DialContext(handleCtx, url, requestHeader)
		if glog.V(2) {
			if err != nil {
				glog.Infof("[net]ws dial %s = %s\n", url, err)
			} else {
				glog.Infof("[net]ws dial %s = %s\n", url, response.Status)
			}
		}

		dialer.Update(handleCtx, err)

		return &evalResult{
			wsConn: wsConn,
			err:    err,
			httpResult: httpResult{
				// status: response.Status,
				// statusCode: response.StatusCode,
				// header: response.Header.Clone(),
				response: response,
			},
		}
	}

	result := self.parallelEval(ctx, eval)
	if result == nil {
		return nil, nil, fmt.Errorf("Timeout.")
	}
	return result.wsConn, result.response, result.err
}

func (self *ClientStrategy) collapseExtenderDialers() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	for dialer, _ := range self.dialers {
		if dialer.IsExtender() && dialer.IsLastSuccess() {
			if self.settings.ExtenderDropTimeout <= time.Now().Sub(dialer.lastErrorTime) {
				delete(self.dialers, dialer)
			}
		}
	}
}

func (self *ClientStrategy) expandExtenderDialers() (expandedDialers []*clientDialer, expandedExtenderIps []netip.Addr) {

	// - distribute new ips evenly over new profiles
	// - distribute existing ids as weighted where needed
	// - `extenderIpSecrets` overrides new ips

	self.mutex.Lock()
	defer self.mutex.Unlock()

	if self.settings.ExpandExtenderProfileCount <= 0 {
		return []*clientDialer{}, []netip.Addr{}
	}

	visitedExtenderProfiles := map[ExtenderProfile]bool{}
	visitedExtenderIps := map[netip.Addr]bool{}

	for dialer, _ := range self.dialers {
		if dialer.IsExtender() {
			visitedExtenderProfiles[dialer.extenderConfig.Profile] = true
			visitedExtenderIps[dialer.extenderConfig.Ip] = true
		}
	}

	if self.settings.MaxExtenderCount <= len(visitedExtenderProfiles) {
		// at maximum extenders
		return []*clientDialer{}, []netip.Addr{}
	}

	extenderProfiles := EnumerateExtenderProfiles(
		min(self.settings.ExpandExtenderProfileCount, self.settings.MaxExtenderCount-len(visitedExtenderProfiles)),
		visitedExtenderProfiles,
	)

	extenderConfigs := []*ExtenderConfig{}
	if len(self.extenderIpSecrets) == 0 {

		// filter resolved ips by visited
		unusedExtenderIps := []netip.Addr{}
		for _, ip := range self.resolvedExtenderIps {
			if !visitedExtenderIps[ip] {
				unusedExtenderIps = append(unusedExtenderIps, ip)
			}
		}

		deviceIpv4 := nettest.SupportsIPv4()
		deviceIpv6 := nettest.SupportsIPv6()

		// expand the ips to have one new ip per profile
		if len(unusedExtenderIps) < len(extenderProfiles) {
			// iterate these for ips not used
			for _, network := range self.settings.ExtenderNetworks {
				if network.Addr().Is4() && deviceIpv4 || network.Addr().Is6() && deviceIpv6 {
					for ip := network.Addr(); network.Contains(ip); ip = ip.Next() {
						if !visitedExtenderIps[ip] {
							visitedExtenderIps[ip] = true
							expandedExtenderIps = append(expandedExtenderIps, ip)
						}
					}
				}
			}

			mathrand.Shuffle(len(expandedExtenderIps), func(i int, j int) {
				expandedExtenderIps[i], expandedExtenderIps[j] = expandedExtenderIps[j], expandedExtenderIps[i]
			})

			if len(extenderProfiles) <= len(expandedExtenderIps) {
				expandedExtenderIps = expandedExtenderIps[0:len(extenderProfiles)]
			}

			// if not enough ips, use DoH to load ips for the extender hostnames
			if len(expandedExtenderIps) < len(extenderProfiles) && 0 < len(self.settings.ExtenderHostnames) {

				// the network can be both ipv4 and ipv6
				if deviceIpv4 {
					ips := DohQuery(self.ctx, 4, "A", self.settings.DohSettings, self.settings.ExtenderHostnames...)
					for ip, _ := range ips {
						if !visitedExtenderIps[ip] {
							visitedExtenderIps[ip] = true
							expandedExtenderIps = append(expandedExtenderIps, ip)
						}
					}
				}
				if deviceIpv6 {
					ips := DohQuery(self.ctx, 6, "AAAA", self.settings.DohSettings, self.settings.ExtenderHostnames...)
					for ip, _ := range ips {
						if !visitedExtenderIps[ip] {
							visitedExtenderIps[ip] = true
							expandedExtenderIps = append(expandedExtenderIps, ip)
						}
					}
				}
			}

			unusedExtenderIps = append(unusedExtenderIps, expandedExtenderIps...)
		}

		// unused ips first
		mathrand.Shuffle(len(unusedExtenderIps), func(i int, j int) {
			unusedExtenderIps[i], unusedExtenderIps[j] = unusedExtenderIps[j], unusedExtenderIps[i]
		})
		n := min(len(extenderProfiles), len(unusedExtenderIps))
		for i := range n {
			extenderConfig := &ExtenderConfig{
				Profile: extenderProfiles[i],
				Ip:      unusedExtenderIps[i],
			}
			extenderConfigs = append(extenderConfigs, extenderConfig)
		}

		// existing ips distributed as weighted
		if n < len(extenderProfiles) {
			weights := map[netip.Addr]float32{}

			netWeight := float32(0)
			for dialer, _ := range self.dialers {
				if dialer.IsExtender() {
					w := dialer.Weight()
					weights[dialer.extenderConfig.Ip] = w
					netWeight += w
				}
			}
			for _, ip := range unusedExtenderIps {
				w := self.settings.ExtenderMinimumWeight
				weights[ip] = w
				netWeight += w
			}

			if 0 < len(weights) {
				ips := maps.Keys(weights)
				mathrand.Shuffle(len(ips), func(i int, j int) {
					ips[i], ips[j] = ips[j], ips[i]
				})

				for _, extenderProfile := range extenderProfiles[n:] {
					v := mathrand.Float32() * netWeight
					i := 0
					for i < len(ips)-1 {
						v -= weights[ips[i]]
						if v <= 0 {
							break
						}
						i += 1
					}
					extenderConfig := &ExtenderConfig{
						Profile: extenderProfile,
						Ip:      ips[i],
					}
					extenderConfigs = append(extenderConfigs, extenderConfig)
				}
			}
		}
	} else {
		ips := maps.Keys(self.extenderIpSecrets)
		for _, extenderProfile := range extenderProfiles {
			ip := ips[mathrand.Intn(len(ips))]
			extenderConfig := &ExtenderConfig{
				Profile: extenderProfile,
				Ip:      ip,
				Secret:  self.extenderIpSecrets[ip],
			}
			extenderConfigs = append(extenderConfigs, extenderConfig)
		}
	}

	for _, extenderConfig := range extenderConfigs {
		dialTlsContext := NewExtenderDialTlsContext(
			&self.settings.ConnectSettings,
			extenderConfig,
		)

		dialer := &clientDialer{
			minimumWeight:  self.settings.ExtenderMinimumWeight,
			priority:       100,
			dialTlsContext: dialTlsContext,
			extenderConfig: extenderConfig,
			settings:       self.settings,
		}
		expandedDialers = append(expandedDialers, dialer)
	}

	for _, dialer := range expandedDialers {
		self.dialers[dialer] = true
	}
	self.resolvedExtenderIps = append(self.resolvedExtenderIps, expandedExtenderIps...)

	return
}

// non-extender dialers are never dropped
type clientDialer struct {
	description   string
	minimumWeight float32
	// 0 is max
	priority int

	dialTlsContext DialTlsContextFunction

	extenderConfig *ExtenderConfig

	mutex           sync.Mutex
	successCount    uint64
	errorCount      uint64
	lastSuccessTime time.Time
	lastErrorTime   time.Time

	httpClient      *http.Client
	websocketDialer *websocket.Dialer

	settings *ClientStrategySettings
}

func (self *clientDialer) HttpClient() *http.Client {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if self.httpClient == nil {
		transport := &http.Transport{
			DialTLSContext:        self.dialTlsContext,
			IdleConnTimeout:       self.settings.ConnectSettings.IdleConnTimeout,
			TLSHandshakeTimeout:   self.settings.ConnectSettings.TlsTimeout,
			ResponseHeaderTimeout: self.settings.ConnectTimeout,
			ExpectContinueTimeout: self.settings.ConnectTimeout,
			DisableKeepAlives:     false,
		}
		self.httpClient = &http.Client{
			Transport: transport,
			Timeout:   self.settings.RequestTimeout,
		}
	}
	return self.httpClient
}

func (self *clientDialer) WsDialer(settings *ClientStrategySettings) *websocket.Dialer {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if self.websocketDialer == nil {
		self.websocketDialer = &websocket.Dialer{
			NetDialTLSContext: self.dialTlsContext,
			HandshakeTimeout:  settings.HandshakeTimeout,
		}
	}
	return self.websocketDialer
}

func (self *clientDialer) Weight() float32 {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	c := self.successCount + self.errorCount
	if 0 < c {
		return max(float32(float64(self.successCount)/float64(c)), self.minimumWeight)
	} else {
		return self.minimumWeight
	}
}

func (self *clientDialer) Update(handleCtx context.Context, err error) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if err == nil {
		self.successCount += 1
		self.lastSuccessTime = time.Now()
	} else {
		select {
		case <-handleCtx.Done():
			// ignore any error is the context is canceled
		default:
			self.errorCount += 1
			self.lastErrorTime = time.Now()
		}
	}
}

func (self *clientDialer) IsExtender() bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	return self.extenderConfig != nil
}

func (self *clientDialer) IsLastSuccess() bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	return !self.lastSuccessTime.Before(self.lastErrorTime)
}

func (self *clientDialer) String() string {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if self.extenderConfig != nil {
		return fmt.Sprintf("extender (%v) success=%d error=%d", self.extenderConfig, self.successCount, self.errorCount)
	} else {
		return fmt.Sprintf("%s success=%d error=%d", self.description, self.successCount, self.errorCount)
	}
}

type ApiCallback[R any] interface {
	Result(result R, err error)
}

// for internal use
type simpleApiCallback[R any] struct {
	callback func(result R, err error)
}

func NewApiCallback[R any](callback func(result R, err error)) ApiCallback[R] {
	return &simpleApiCallback[R]{
		callback: callback,
	}
}

func NewNoopApiCallback[R any]() ApiCallback[R] {
	return &simpleApiCallback[R]{
		callback: func(result R, err error) {},
	}
}

func (self *simpleApiCallback[R]) Result(result R, err error) {
	self.callback(result, err)
}

type ApiCallbackResult[R any] struct {
	Result R
	Error  error
}

func NewBlockingApiCallback[R any](ctx context.Context) (ApiCallback[R], chan ApiCallbackResult[R]) {
	c := make(chan ApiCallbackResult[R])
	apiCallback := NewApiCallback[R](func(result R, err error) {
		r := ApiCallbackResult[R]{
			Result: result,
			Error:  err,
		}
		select {
		case <-ctx.Done():
		case c <- r:
		}
	})
	return apiCallback, c
}

func HttpPostWithStrategy[R any](ctx context.Context, clientStrategy *ClientStrategy, requestUrl string, args any, byJwt string, result R, callback ApiCallback[R]) (R, error) {
	var requestBodyBytes []byte
	if args == nil {
		requestBodyBytes = make([]byte, 0)
	} else {
		var err error
		requestBodyBytes, err = json.Marshal(args)
		if err != nil {
			var empty R
			callback.Result(empty, err)
			return empty, err
		}
	}

	request, err := http.NewRequestWithContext(ctx, "POST", requestUrl, bytes.NewReader(requestBodyBytes))
	if err != nil {
		var empty R
		callback.Result(empty, err)
		return empty, err
	}

	request.Header.Add("Content-Type", "text/json")

	if byJwt != "" {
		auth := fmt.Sprintf("Bearer %s", byJwt)
		request.Header.Add("Authorization", auth)
	}

	helloRequest, err := HelloRequestFromUrl(ctx, requestUrl, byJwt)
	if err != nil {
		var empty R
		callback.Result(empty, err)
		return empty, err
	}

	r, err := clientStrategy.HttpSerial(request, helloRequest)
	if err != nil {
		var empty R
		callback.Result(empty, err)
		return empty, err
	}

	if http.StatusOK != r.response.StatusCode {
		// the response body is the error message
		err = fmt.Errorf("%s: %s", r.response.Status, strings.TrimSpace(string(r.bodyBytes)))
		var empty R
		callback.Result(empty, err)
		return empty, err
	}

	err = json.Unmarshal(r.bodyBytes, &result)
	if err != nil {
		var empty R
		callback.Result(empty, err)
		return empty, err
	}

	callback.Result(result, nil)
	return result, nil
}

func HelloRequestFromUrl(ctx context.Context, requestUrl string, byJwt string) (*http.Request, error) {

	u, err := url.Parse(requestUrl)
	if err != nil {
		return nil, err
	}
	helloUrl := fmt.Sprintf("%s://%s/hello", u.Scheme, u.Host)

	req, err := http.NewRequestWithContext(ctx, "GET", helloUrl, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "text/json")

	if byJwt != "" {
		auth := fmt.Sprintf("Bearer %s", byJwt)
		req.Header.Add("Authorization", auth)
	}

	return req, nil
}

func HttpGetWithStrategy[R any](ctx context.Context, clientStrategy *ClientStrategy, requestUrl string, byJwt string, result R, callback ApiCallback[R]) (R, error) {
	request, err := http.NewRequestWithContext(ctx, "GET", requestUrl, nil)
	if err != nil {
		var empty R
		callback.Result(empty, err)
		return empty, err
	}

	request.Header.Add("Content-Type", "text/json")

	if byJwt != "" {
		auth := fmt.Sprintf("Bearer %s", byJwt)
		request.Header.Add("Authorization", auth)
	}

	r, err := clientStrategy.HttpParallel(request)
	if err != nil {
		var empty R
		callback.Result(empty, err)
		return empty, err
	}

	if http.StatusOK != r.response.StatusCode {
		// the response body is the error message
		err = fmt.Errorf("%s: %s", r.response.Status, strings.TrimSpace(string(r.bodyBytes)))
		var empty R
		callback.Result(empty, err)
		return empty, err
	}

	err = json.Unmarshal(r.bodyBytes, &result)
	if err != nil {
		var empty R
		callback.Result(empty, err)
		return empty, err
	}

	callback.Result(result, nil)
	return result, nil
}
