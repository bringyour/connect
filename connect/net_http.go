package connect


import (

	"golang.org/x/net/nettest"
)


// censorship-resistant strategies for making https connections
// this uses a random walk of best practices to obfuscate and double-encrypt https connections

// note the net_* files are migrated from net.IP/IPNet to netip.Addr/Prefix
// TODO generally the entire package should migrate over to these newer structs



type DialTlsContextFunction = func(ctx context.Context, network string, addr string) (net.Conn, error)




func DefaultClientStrategySettings() *ClientStrategySettings {
	// FIXME
}







type ClientStrategySettings struct {
	EnableNormal bool
	// tls frag, retransmit, tls frag + retransmit
	EnableResilient bool
	// FIXME
	// EnableUdp bool


	// for gets and ws connects
	ParallelBlockSize int



	// non-extender minimum weight
	// all dialers are weighted 0 to 1
	// success / (success + error)
	MinimumWeight float


	// transportMode can be Tcp or Quic
	// extender profile is a transport mode, server name, and port 
	ExpandExtenderProfileCount int

	ExtenderNetworks []netip.Prefix
	// these are evaluated with DoH to grow the extender ips
	ExtenderHostnames []string
	ExpandExtenderIpCount int

	ExpandExtenderRateLimit time.Duration

	MaxExtenderCount int

	// success / (success + error)
	ExtenderMinimumWeight float

	// drop dialers that have not had a successful connect in this timeout
	ExtenderDropTimeout time.Duration






	TlsConfig *tls.Config

	RequestTimeout time.Duration
	ConnectTimeout time.Duration
	TlsTimeout time.Duration
	HandshakeTimeout time.Duration
	

	DohSettings *DohSettings
	
}




// stores statistics on client strategies
type ClientStrategy struct {
	ctx context.Context

	settings *ClientStrategySettings

	mutex sync.Mutex
	// dialers are only updated inside the mutex
	dialers map[*clientDialer]bool
	resolvedExtenderIps []netip.Addr

	extenderIpSecrets map[netip.Addr]string
}


func DefaultClientStrategy() *ClientStrategy {
	return NewClientStrategy(DefaultClientStrategySettings())
}


// extender udp 53 to platform extender
func NewClientStrategy(ctx context.Context, settings *ClientStrategySettings) *ClientStrategy {
	
	// create dialers to match settings
	dialers := map[*clientDialer]bool{}
	resolvedExtenderIps := []netip.Addr{}

	if settings.EnableNormal {
		// FIXME normal dial context
		tlsDialer := &tls.Dialer{
			NetDialer: &net.Dialer{
				// FIXME
			},
			Config: settings.tlsConfig,
		}
		
		dialer := &clientDialer{
			dialTlsContext: tlsDialer.DialContext,
		}
		dialers[dialer] = true
	}
	if settings.EnableResilient {
		netDialer := &net.Dialer{
			// FIXME
		}

		// fragment+reorder
		dialer1 := &clientDialer{
			dialTlsContext: NewResilientDialTlsContext(netDialer, true, true),
		}
		// fragment
		dialer2 := &clientDialer{
			dialTlsContext: NewResilientDialTlsContext(netDialer, true, false),
		}
		// reorder
		dialer3 := &clientDialer{
			dialTlsContext: NewResilientDialTlsContext(netDialer, false, true),
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

	for dialer, _ := range self.dialers {
		c := dialer.successCount + dialer.errorCount
		w := float32(0)
		if 0 < c {
			 w = float32(dialer.successCount) / float32(c)
		}
		if dialer.IsExtender() {
			w = max(w, self.settings.ExtenderMinimumWeight)
		} else {
			w = max(w, self.settings.MinimumWeight)
		}
		weights[dialer] = w
	}

	return weights
}

func (self *ClientStrategy) httpClient(dialerTlsContext DialTlsContextFunction) *http.Client {
	// see https://medium.com/@nate510/don-t-use-go-s-default-http-client-4804cb19f779
	dialer := &net.Dialer{
    	Timeout: defaultHttpConnectTimeout,
  	}
	transport := &http.Transport{
	  	// DialTLSContext: NewResilientTlsDialContext(dialer),
	  	// DialContext: NewExtenderDialContext(ExtenderConnectModeQuic, dialer, TestExtenderConfig()),
	  	DialTLSContext: dialerTlsContext,
	  	TLSHandshakeTimeout: defaultHttpTlsTimeout,
	}
	return &http.Client{
		Transport: transport,
		Timeout: defaultHttpTimeout,
	}
}

func (self *ClientStrategy) wsDialer(dialerTlsContext DialTlsContextFunction) *websocket.Dialer {
	return &websocket.Dialer{
		NetDialTLSContext: dialerTlsContext,
		HandshakeTimeout: self.settings.WsHandshakeTimeout,
	}
}

func (self *ClientStrategy) updateDialer(handleCtx context.Context, dialer *clientDialer, err error) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	
	if err == nil {
		dialer.successCount += 1
		dialer.lastSuccessTime = time.Now()
	} else {
		select {
		case <- handleCtx.Done():
			// ignore any error is the context is canceled
		default:
			dialer.errorCount += 1
			dialer.lastErrorTime = time.Now()
		}
	}
}

type parallelResult struct {
	response *net.Response
	wsConn *websocket.Conn
	err error
}

func (self *parallelResult) Close() {
	if self.wsConn != nil {
		self.wsConn.Close()
		// if wsConn is set, the response does not need to be closed
		// https://pkg.go.dev/github.com/gorilla/websocket#Dialer.DialContext
	} else if self.response != nil {
		self.response.Body.Close()
	}
}


func (self *ClientStrategy) parallelEval(ctx context.Context, eval func(ctx context.Context, dialer *clientDialer)(*parallelResult)) *parallelResult {	
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

	out := make(chan *parallelResult)

	run := func(dialer *clientDialer) {
		result := eval(ctx, dialer)

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
	orderedDialers := WeightedShuffle(maps.Keys(dialerWeights), dialerWeights)

	parallelDialers := []*clientDialer{}

	for _, dialer := range orderedDialers {
		if dialer.IsLastSuccess() {
			run(dialer)
			select {
			case <- handleCtx.Done():
				return nil
			case result := <- out:
				if result.err == nil {
					return result
				}
			}
		} else {
			parallelDialers = append(parallelDialers, dialer)
		}
	}

	// note parallel dialers is in the original weighted order
	n := min(len(parallelDialers), settings.ParallelBlockSize)
	for _, dialer := range paralleDialers[0:n] {
		go run(dialer)
	}
	for _, dialer := range paralleDialers[n:] {
		select {
		case <- handleCtx.Done():
			return nil
		case result := <- out:
			if result.err == nil {
				return result, nil
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

		m := min(len(expandedDialers), settings.ParallelBlockSize - n)
		n += m
		i := 0
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

	for _ := range n {
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

	return nil
}



func (self *ClientStrategy) HttpParallel(request *net.Request) (*net.Response, error) {
	eval := func(handleCtx context.Context, dialer *clientDialer)(*parallelResult) {		
		httpClient := self.httpClient(dialer.dialTlsContext)
		response, err := httpClient.Do(request.WithContext(handleCtx))

		self.updateDialer(handleCtx, dialer, err)

		return &parallelResult{
			response: response,
			err: err,
		}
	}

	result := self.parallelEval(request.Context, eval)
	if result == nil {
		return nil, nil
	}
	return result.response, result.err
}


func (self *ClientStrategy) HttpSerial(request *net.Request, helloRequest *net.Request) (*net.Response, error) {
	// in this order:
	// 1. try all dialers that previously worked sequentially
	// 2. retest and expand dialers using get of the hello request.
	//    This is a basic ping to the server, which is run in parallel.
	// 3. continue from 1 until timeout

	handleCtx, handleCancel := context.WithTimeout(request.Context, self.settings.DefaultHttpTimeout)
	defer handleCancel()
	// merge handleCtx with self.ctx
	go func() {
		defer handleCancel()
		select {
		case <- handleCtx.Done():
			return
		case <- self.ctx.Done():
			return
		case <- request.Context.Done():
			return
		}
	}()

	type httpResult struct {
		response *net.Response
		err error
	}

	out := make(chan *httpResult)

	run := func(dialer *clientDialer) {		
		httpClient := self.httpClient(dialer.dialTlsContext)
		response, err := httpClient.Do(request.WithContext(handleCtx))

		self.updateDialer(handleCtx, dialer, err)

		result := &httpResult{
			response: response,
			err: err,
		}

		select {
		case out <- result:
		case <- handleCtx.Done():
			if result != nil {
				result.Body.Close()
			}
		}
	}

	// keep trying as long as there is time left
	for {
		select {
		case <- handleCtx.Done():
			return nil, nil
		default:
		}

		self.collapseExtenderDialers()

		dialerWeights := self.getDialerWeights()
		// descending
		orderedDialers := WeightedShuffle(maps.Keys(dialerWeights))

		for _, dialer := range orderedDialers {
			if dialer.IsLastSuccess() {
				run(dialer)
				select {
				case <- handleCtx.Done():
					return nil, nil
				case result := <- out:
					if result.err == nil {
						return result.result, nil
					}
				}
				attemptCount += 1
			}
		}

		// it's more efficient to iterate with a hello get
		result, err := self.Get(handleCtx, helloRequest)
		if result == nil {
			return nil, err
		}
		result.Close()
	}

	return nil, nil
}


func (self *ClientStrategy) WsDialContext(ctx context.Context, url string, requestHeader http.Header) (*websocket.Conn, *http.Response, error) {
    eval := func(handleCtx context.Context, dialer *clientDialer)(*parallelResult) {
    	wsDialer := self.wsDialer(dialer.dialTlsContext)
    	wsConn, response, err := wsDialer.DialContext(handleCtx, url, requestHeader)

		self.updateDialer(handleCtx, dialer, err)

		return &parallelResult{
			wsConn: wsConn,
			response: response,
			err: err,
		}
	}

	result := self.parallelEval(ctx, eval)
	if result == nil {
		return nil, nil
	}
	return result.wsConn, result.response, result.err
}

func (self *ClientStrategy) collapseExtenderDialers() {
	mutex.Lock()
	defer mutex.Unlock()

	for dialer, _ := range self.dialers {
		if dialer.IsExtender() && dialer.IsLastSuccess() {
			if self.settings.ExtenderDropTimeout <= time.Now().Sub(dialer.lastErrorTime) {
				delete(self.dialers, dialers)
			}
		}
	}
}

func (self *ClientStrategy) expandExtenderDialers() (expandedDialers []*clientDialer, expandedExtenderIps []netip.Addr) {

	// - distribute new ips evenly over new profiles
	// - distribute existing ids as weighted where needed
	// - `extenderIpSecrets` overrides new ips

	mutex.Lock()
	defer mutex.Unlock()

	if settings.ExpandExtenderProfileCount <= 0 {
		return []*clientDialer{}, []net.Id{}
	}

	visitedExtenderProfiles := map[ExtenderProfile]bool{}
	visitedExtenderIps := []net.Id{}
	visitedExtenderIps := map[netip.Addr]bool{}

	for _, dialer := self.dialers {
		if dialier.IsExtender() {
			visitedExtenderProfiles[dialer.extenderProfile] = true
			visitedExtenderIps[dialer.extenderIp] = true
		}
	}

	if settings.MaxExtenderCount <= len(visitedExtenderProfiles) {
		// at maximum extenders
		return []*clientDialer{}, []net.Id{}
	}

	extenderProfiles := EnumerateExtenderProfiles(
		min(settings.ExpandExtenderProfileCount, settings.MaxExtenderCount - len(visitedExtenderProfiles)),
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
			for _, network := range settings.ExtenderNetworks {
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

			if settings.ExpandExtenderIpCount <= len(expandedExtenderIps) {
				expandedExtenderIps = expandedExtenderIps[0:settings.ExpandExtenderIpCount]
			}


			// if not enough ips, use DoH to load ips for the extender hostnames
			if len(expandedExtenderIps) < settings.ExpandExtenderIpCount && 0 < len(settings.ExtenderHostnames) {

				// the network can be both ipv4 and ipv6
				if deviceIpv4 {
					ips := DohQuery(ctx, 4, settings.ExtenderHostnames, "A")
					for _, ip := range ips {
						if !visitedExtenderIps[ip] {
							visitedExtenderIps[ip] = true
							expandedExtenderIps = append(expandedExtenderIps, ip)
						}
					}
				}
				if deviceIpv6 {
					ips := DohQuery(ctx, 6, settings.ExtenderHostnames, "AAAA")
					for _, ip := range ips {
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
				ExtenderProfile: extenderProfiles[i],
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
					c := dialer.successCount + dialer.errorCount
					w := float32(0)
					if 0 < c {
						 w = float32(dialer.successCount) / float32(c)
					}
					w = max(w, self.settings.ExtenderMinimumWeight)
					weights[dialer.extenderIp] = w
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

				for _, extenderProfile := range extenderProfiles[n:len(extenderProfiles)] {
					v := mathrand.Float32(netWeight)
					i := 0
					for i < len(ips); i += 1 {
						v -= weights[ips[i]]
						if v <= 0 {
							break
						}
					}
					if i == len(ips) {
						i = len(ips) - 1
					}
					extenderConfig := &ExtenderConfig{
						ExtenderProfile: extenderProfile,
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
				ExtenderProfile: extenderProfile,
				Ip: ip,
				Secret: self.extenderIpSecrets[ip],
			}
			extenderConfigs = append(extenderConfigs, extenderConfig)
		}
	}

	for _, extenderConfig := range extenderConfigs {
		dialer := &net.Dialer{
			// FIXME
	        Timeout:   30 * time.Second,
	        KeepAlive: 30 * time.Second,
	    }
	    dialTlsContext: NewExtenderDialTlsContext(
		    dialer,
		    extenderConfig,
		    settings.tlsConfig,
		)

		dialer := &clientDialer{
			dialTlsContext: dialTlsContext,
			extenderProfile: extenderProfile,
			extenderIp: extenderIp,
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
	extenderProfile ExtenderProfile
	extenderIp netip.Addr
	extenderSecret string


	// these are locked under the client stategy mutex
	successCount int
	errorCount int
	lastSuccessTime time.Time
	lastErrorTime time.Time
}

func (self *clientDialer) IsLastSuccess() bool {
	dialer.lastSuccessTime < dialer.lastErrorTime
}





type ApiCallback[R any] interface {
	Result(result R, err error)
}


// for internal use
type simpleApiCallback[R any] struct {
	callback func(result R, err error)
}

func NewApiCallback[R any](callback func(result R, err error)) apiCallback[R] {
	return &simpleApiCallback[R]{
		callback: callback,
	}
}

func NewNoopApiCallback[R any]() apiCallback[R] {
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


func NewBlockingApiCallback[R any](ctx context.Context) (apiCallback[R], chan ApiCallbackResult[R]) {
	c := make(chan ApiCallbackResult[R])
	apiCallback := NewApiCallback[R](func(result R, err error) {
		result := ApiCallbackResult[R]{
			Result: result,
			Error: err,
		}
		select {
		case <- ctx.Done():
		case c <- result:
		}
	})
	return apiCallback, c
}





func HttpPostWithStrategy(ctx context.Context, clientStrategy *ClientStrategy, url string, args any, byJwt string, result R, callback apiCallback[R]) (R, error) {
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

	request, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(requestBodyBytes))
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

	helloRequest, err := HelloRequestFromUrl(ctx, url, byJwt)
	if err != nil {
		var empty R
		callback.Result(empty, err)
		return empty, err
	}

	r, err := clientStrategy.HttpParallel(request, helloRequest)
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

func HelloRequestFromUrl(ctx context.Context, url string, byJwt string) (*http.Request, error) {

	u, err := url.Parse(url)
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


func HttpGetWithStrategy[R any](ctx context.Context, clientStrategy *ClientStrategy, url string, byJwt string, result R, callback apiCallback[R]) (R, error) {
	request, err := http.NewRequestWithContext(ctx, "GET", url, nil)
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




