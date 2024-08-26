package connect


import (
	"time"
	"context"
	"sync"
	"net"
	"net/netip"
	"net/http"
	"net/url"
	"crypto/tls"
	"encoding/json"
	"bytes"
	"fmt"
	"io"
	"strings"
	"errors"
	mathrand "math/rand"

	"golang.org/x/net/nettest"

	"golang.org/x/exp/maps"

	"github.com/gorilla/websocket"

	// "github.com/golang/glog"
)


// censorship-resistant strategies for making https connections
// this uses a random walk of best practices to obfuscate and double-encrypt https connections

// note the net_* files are migrated from net.IP/IPNet to netip.Addr/Prefix
// TODO generally the entire package should migrate over to these newer structs



type DialTlsContextFunction = func(ctx context.Context, network string, addr string) (net.Conn, error)




func DefaultClientStrategySettings() *ClientStrategySettings {
	return &ClientStrategySettings{
		EnableNormal: true,
		EnableResilient: true,

		ParallelBlockSize: 4,

		MinimumWeight: 0.25,

		ExpandExtenderProfileCount: 8,
		ExtenderNetworks: []netip.Prefix{},
		ExtenderHostnames: []string{},
		ExpandExtenderRateLimit: 1 * time.Second,
		MaxExtenderCount: 128,
		ExtenderMinimumWeight: 0.1,
		ExtenderDropTimeout: 5 * time.Minute,

		
		DohSettings: DefaultDohSettings(),
		ConnectSettings: *DefaultConnectSettings(),
	}
}

func DefaultConnectSettings() *ConnectSettings {
	return &ConnectSettings{
		RequestTimeout: 60 * time.Second,
		ConnectTimeout: 2 * time.Second,
		TlsTimeout: 4 * time.Second,
		HandshakeTimeout: 4 * time.Second,
		TlsConfig: nil,
	}
}





type ConnectSettings struct {
	RequestTimeout time.Duration
	ConnectTimeout time.Duration
	TlsTimeout time.Duration
	HandshakeTimeout time.Duration

	TlsConfig *tls.Config
}



type ClientStrategySettings struct {
	EnableNormal bool
	// tls frag, retransmit, tls frag + retransmit
	EnableResilient bool

	// for gets and ws connects
	ParallelBlockSize int

	// non-extender minimum weight
	MinimumWeight float32

	// the number of new profiles to add per expand
	ExpandExtenderProfileCount int
	ExtenderNetworks []netip.Prefix
	// these are evaluated with DoH to grow the extender ips
	ExtenderHostnames []string
	ExpandExtenderRateLimit time.Duration
	MaxExtenderCount int
	// extender minimum weight
	ExtenderMinimumWeight float32
	// drop dialers that have not had a successful connect in this timeout
	ExtenderDropTimeout time.Duration
	
	DohSettings *DohSettings

	ConnectSettings
}




// stores statistics on client strategies
type ClientStrategy struct {
	ctx context.Context

	settings *ClientStrategySettings

	mutex sync.Mutex
	// dialers are only updated inside the mutex
	dialers map[*clientDialer]bool
	resolvedExtenderIps []netip.Addr

	// custom extenders
	// these take precedence over other extenders
	extenderIpSecrets map[netip.Addr]string
}


func DefaultClientStrategy(ctx context.Context) *ClientStrategy {
	return NewClientStrategy(ctx, DefaultClientStrategySettings())
}


// extender udp 53 to platform extender
func NewClientStrategy(ctx context.Context, settings *ClientStrategySettings) *ClientStrategy {
	// create dialers to match settings
	dialers := map[*clientDialer]bool{}
	resolvedExtenderIps := []netip.Addr{}

	if settings.EnableNormal {
		tlsDialer := &tls.Dialer{
			NetDialer: &net.Dialer{
				Timeout: settings.ConnectTimeout,
			},
			Config: settings.TlsConfig,
		}
		
		dialer := &clientDialer{
			dialTlsContext: tlsDialer.DialContext,
		}
		dialers[dialer] = true
	}
	if settings.EnableResilient {
		// fragment+reorder
		dialer1 := &clientDialer{
			dialTlsContext: NewResilientDialTlsContext(&settings.ConnectSettings, true, true),
		}
		// fragment
		dialer2 := &clientDialer{
			dialTlsContext: NewResilientDialTlsContext(&settings.ConnectSettings, true, false),
		}
		// reorder
		dialer3 := &clientDialer{
			dialTlsContext: NewResilientDialTlsContext(&settings.ConnectSettings, false, true),
		}

		dialers[dialer1] = true
		dialers[dialer2] = true
		dialers[dialer3] = true
	}



	return &ClientStrategy{
		ctx: ctx, 
		settings: settings,
		dialers: dialers,
		resolvedExtenderIps: resolvedExtenderIps,
		extenderIpSecrets: map[netip.Addr]string{},
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
			if dialer.IsExtender() {
				w = max(w, self.settings.ExtenderMinimumWeight)
			} else {
				w = max(w, self.settings.MinimumWeight)
			}
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

func (self *ClientStrategy) httpClient(dialTlsContext DialTlsContextFunction) *http.Client {
	transport := &http.Transport{
	  	DialTLSContext: dialTlsContext,
	}
	return &http.Client{
		Transport: transport,
		Timeout: self.settings.RequestTimeout,
	}
}

func (self *ClientStrategy) wsDialer(dialTlsContext DialTlsContextFunction) *websocket.Dialer {
	return &websocket.Dialer{
		NetDialTLSContext: dialTlsContext,
		HandshakeTimeout: self.settings.HandshakeTimeout,
	}
}

type evalResult struct {
	response *http.Response
	wsConn *websocket.Conn
	err error
}

func (self *evalResult) Close() {
	if self.wsConn != nil {
		self.wsConn.Close()
		// if wsConn is set, the response does not need to be closed
		// https://pkg.go.dev/github.com/gorilla/websocket#Dialer.DialContext
	} else if self.response != nil {
		self.response.Body.Close()
	}
}


func (self *ClientStrategy) parallelEval(ctx context.Context, eval func(ctx context.Context, dialer *clientDialer)(*evalResult)) *evalResult {	
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
		case <- handleCtx.Done():
			return
		case <- self.ctx.Done():
			return
		}
	}()

	out := make(chan *evalResult)

	run := func(dialer *clientDialer) {
		result := eval(handleCtx, dialer)
		
		select {
		case out <- result:
		case <- handleCtx.Done():
			if result != nil {
				result.Close()
			}
		}
	}

	self.collapseExtenderDialers()

	dialerWeights := self.dialerWeights()
	// descending
	orderedDialers := maps.Keys(dialerWeights)
	WeightedShuffle(orderedDialers, dialerWeights)

	parallelDialers := []*clientDialer{}

	for _, dialer := range orderedDialers {
		if dialer.IsLastSuccess() {
			select {
			case <- handleCtx.Done():
				return nil
			default:
			}

			result := eval(handleCtx, dialer)
			if result == nil {
				return nil
			}
			if result.err == nil {
				return result
			}
		} else {
			parallelDialers = append(parallelDialers, dialer)
		}
	}

	// note parallel dialers is in the original weighted order
	n := min(len(parallelDialers), self.settings.ParallelBlockSize)
	for _, dialer := range parallelDialers[0:n] {
		go run(dialer)
	}
	for _, dialer := range parallelDialers[n:] {
		select {
		case <- handleCtx.Done():
			return nil
		case result := <- out:
			if result.err == nil {
				return result
			} else {
				go run(dialer)
			}
		}
	}

	// keep trying as long as there is time left
	for {
		select {
		case <- handleCtx.Done():
			return nil
		default:
		}

		expandStartTime := time.Now()

		self.collapseExtenderDialers()
		expandedDialers, _ := self.expandExtenderDialers()
		if len(expandedDialers) == 0 {
			break
		}

		m := min(len(expandedDialers), self.settings.ParallelBlockSize - n)
		n += m
		for _, dialer := range expandedDialers[0:m] {
			go run(dialer)
		}
		for _, dialer := range expandedDialers[m:] {
			select {
			case <- handleCtx.Done():
				return nil
			case result := <- out:
				if result.err == nil {
					return result
				} else {
					go run(dialer)
				}
			}
		}

		// the rate limit is important when when the connect timeout is small
		// e.g. local closes due to disconnected network
		expandEndTime := time.Now()
		expandDuration := expandEndTime.Sub(expandStartTime)
		if timeout := self.settings.ExpandExtenderRateLimit - expandDuration; 0 < timeout {
			select {
			case <- handleCtx.Done():
				return nil
			case <- time.After(timeout):
			}
		}
	}

	for range n {
		select {
		case <- handleCtx.Done():
			return nil
		case result := <- out:
			if result.err == nil {
				return result
			}
		}
	}

	return &evalResult{
		err: fmt.Errorf("No successful strategy found."),
	}
}

func (self *ClientStrategy) serialEval(ctx context.Context, eval func(ctx context.Context, dialer *clientDialer)(*evalResult), helloEval func(ctx context.Context, dialer *clientDialer)(*evalResult)) *evalResult {
	handleCtx, handleCancel := context.WithTimeout(ctx, self.settings.RequestTimeout)
	defer handleCancel()
	// merge handleCtx with self.ctx
	go func() {
		defer handleCancel()
		select {
		case <- handleCtx.Done():
			return
		case <- self.ctx.Done():
			return
		}
	}()


	// keep trying as long as there is time left
	for {
		select {
		case <- handleCtx.Done():
			return nil
		default:
		}

		self.collapseExtenderDialers()

		dialerWeights := self.dialerWeights()
		// descending
		orderedDialers := maps.Keys(dialerWeights)
		WeightedShuffle(orderedDialers, dialerWeights)

		for _, dialer := range orderedDialers {
			if dialer.IsLastSuccess() {
				select {
				case <- handleCtx.Done():
					return nil
				default:
				}
				
				result := eval(handleCtx, dialer)
				if result == nil {
					return nil
				}
				if result.err == nil {
					return result
				}
			}
		}

		// it's more efficient to iterate with a parallel hello
		result := self.parallelEval(handleCtx, helloEval)
		if result == nil {
			return nil
		}
		result.Close()
		if result.err != nil {
			return &evalResult{
				err: result.err,
			}
		}
	}

	return &evalResult{
		err: fmt.Errorf("No successful strategy found."),
	}
}



func (self *ClientStrategy) HttpParallel(request *http.Request) (*http.Response, error) {
	eval := func(handleCtx context.Context, dialer *clientDialer)(*evalResult) {		
		httpClient := self.httpClient(dialer.dialTlsContext)
		response, err := httpClient.Do(request.WithContext(handleCtx))

		dialer.Update(handleCtx, err)

		return &evalResult{
			response: response,
			err: err,
		}
	}

	result := self.parallelEval(request.Context(), eval)
	if result == nil {
		return nil, fmt.Errorf("Timeout.")
	}
	return result.response, result.err
}


func (self *ClientStrategy) HttpSerial(request *http.Request, helloRequest *http.Request) (*http.Response, error) {
	// in this order:
	// 1. try all dialers that previously worked sequentially
	// 2. retest and expand dialers using get of the hello request.
	//    This is a basic ping to the server, which is run in parallel.
	// 3. continue from 1 until timeout

	eval := func(handleCtx context.Context, dialer *clientDialer)(*evalResult) {		
		httpClient := self.httpClient(dialer.dialTlsContext)
		response, err := httpClient.Do(request.WithContext(handleCtx))

		dialer.Update(handleCtx, err)

		return &evalResult{
			response: response,
			err: err,
		}
	}
	helloEval := func(handleCtx context.Context, dialer *clientDialer)(*evalResult) {		
		httpClient := self.httpClient(dialer.dialTlsContext)
		response, err := httpClient.Do(helloRequest.WithContext(handleCtx))

		dialer.Update(handleCtx, err)

		return &evalResult{
			response: response,
			err: err,
		}
	}

	result := self.serialEval(request.Context(), eval, helloEval)
	if result == nil {
		return nil, fmt.Errorf("Timeout.")
	}
	return result.response, result.err
}


func (self *ClientStrategy) WsDialContext(ctx context.Context, url string, requestHeader http.Header) (*websocket.Conn, *http.Response, error) {
    eval := func(handleCtx context.Context, dialer *clientDialer)(*evalResult) {
    	wsDialer := self.wsDialer(dialer.dialTlsContext)
    	wsConn, response, err := wsDialer.DialContext(handleCtx, url, requestHeader)

		dialer.Update(handleCtx, err)

		return &evalResult{
			wsConn: wsConn,
			response: response,
			err: err,
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
		min(self.settings.ExpandExtenderProfileCount, self.settings.MaxExtenderCount - len(visitedExtenderProfiles)),
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
				Ip: unusedExtenderIps[i],
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
					w = max(w, self.settings.ExtenderMinimumWeight)
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
					for i < len(ips) - 1 {
						v -= weights[ips[i]]
						if v <= 0 {
							break
						}
						i += 1
					}
					extenderConfig := &ExtenderConfig{
						Profile: extenderProfile,
						Ip: ips[i],
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
				Ip: ip,
				Secret: self.extenderIpSecrets[ip],
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
			dialTlsContext: dialTlsContext,
			extenderConfig: extenderConfig,
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
	dialTlsContext DialTlsContextFunction
	extenderConfig *ExtenderConfig

	mutex sync.Mutex
	successCount int
	errorCount int
	lastSuccessTime time.Time
	lastErrorTime time.Time
}

func (self *clientDialer) Weight() float32 {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	c := self.successCount + self.errorCount
	if 0 < c {
		 return float32(self.successCount) / float32(c)
	} else {
		return 0
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
		case <- handleCtx.Done():
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

	return self.lastSuccessTime.After(self.lastErrorTime)
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
		callback: func(result R, err error){},
	}
}

func (self *simpleApiCallback[R]) Result(result R, err error) {
	self.callback(result, err)
}


type ApiCallbackResult[R any] struct {
	Result R
	Error error
}


func NewBlockingApiCallback[R any](ctx context.Context) (ApiCallback[R], chan ApiCallbackResult[R]) {
	c := make(chan ApiCallbackResult[R])
	apiCallback := NewApiCallback[R](func(result R, err error) {
		r := ApiCallbackResult[R]{
			Result: result,
			Error: err,
		}
		select {
		case <- ctx.Done():
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
	defer r.Body.Close()

	responseBodyBytes, err := io.ReadAll(r.Body)

	if http.StatusOK != r.StatusCode {
		// the response body is the error message
		errorMessage := strings.TrimSpace(string(responseBodyBytes))
		err = errors.New(errorMessage)
		callback.Result(result, err)
		return result, err
	}

	if err != nil {
		callback.Result(result, err)
		return result, err
	}

	err = json.Unmarshal(responseBodyBytes, &result)
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
	helloUrl := fmt.Sprintf("%s%s/hello", u.Scheme, u.Host)

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
	defer r.Body.Close()

	responseBodyBytes, err := io.ReadAll(r.Body)
	

	err = json.Unmarshal(responseBodyBytes, &result)
	if err != nil {
		var empty R
		callback.Result(empty, err)
		return empty, err
	}

	callback.Result(result, nil)
	return result, nil
}




