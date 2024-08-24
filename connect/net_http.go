package connect





// censorship-resistant strategies for making https connections
// this uses a random walk of best practices to obfuscate and double-encrypt https connections



// FIXME use netip.Addr instead of net.IP
// FIXME use netip.Prefix instead of net.IPNet


type DialTlsContextFunction = func(ctx context.Context, network string, addr string) (net.Conn, error)


/*
const defaultHttpTimeout = 60 * time.Second
const defaultHttpConnectTimeout = 5 * time.Second
const defaultHttpTlsTimeout = 5 * time.Second


func defaultClient() *http.Client {
	// see https://medium.com/@nate510/don-t-use-go-s-default-http-client-4804cb19f779
	dialer := &net.Dialer{
    	Timeout: defaultHttpConnectTimeout,
  	}
	transport := &http.Transport{
	  	// DialTLSContext: NewResilientTlsDialContext(dialer),
	  	DialContext: NewExtenderDialContext(ExtenderConnectModeQuic, dialer, TestExtenderConfig()),
	  	TLSHandshakeTimeout: defaultHttpTlsTimeout,
	}
	return &http.Client{
		Transport: transport,
		Timeout: defaultHttpTimeout,
	}
}
*/



type apiCallback[R any] interface {
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


func NewBlockingApiCallback[R any]() (apiCallback[R], chan ApiCallbackResult[R]) {
	c := make(chan ApiCallbackResult[R])
	apiCallback := NewApiCallback[R](func(result R, err error) {
		c <- ApiCallbackResult[R]{
			Result: result,
			Error: err,
		}
	})
	return apiCallback, c
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

	ExtenderNetworks []net.IpNet
	// these are evaluated with DoH to grow the extender ips
	ExtenderHostnames []string
	ExpandExtenderIpCount int

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

func DefaultClientStrategySettings() *ClientStrategySettings {

}


// non-extender dialers are never dropped
type clientDialer struct {
	dialTlsContext DialTlsContextFunction
	extenderProfile ExtenderProfile
	extenderIp net.IP
	extenderSecret string


	// these are locked under the client stategy mutex
	successCount int
	errorCount int
	lastSuccessTime time.Time
	lastErrorTime time.Time
}

func IsLastSuccess() bool {
	dialer.lastSuccessTime < dialer.lastErrorTime
}



// stores statistics on client strategies
type ClientStrategy struct {

	// handhake timeout
	// connect timeout
	ctx context.Context


	settings *ClientStrategySettings


	mutex sync.Mutex
	// dialers are only updated inside the mutex
	dialers map[*clientDialer]bool
	resolvedExtenderIps []net.Ip

	extenderIpSecrets map[net.IP]string


	// FIXME dialers
	// TlsDialContexts []DialContextFunc
	// FIXME statistics
	// FIXME stats mutex

}

// typical context list will be:
// normal
// resilient
// extender tls 443 using resolved or baked in ips
// extender quic 443 using resolved or backed in ips
// TODO udp port 53 as a default strategy


func DefaultClientStrategy() *ClientStrategy {
	return NewClientStrategy(DefaultClientStrategySettings())
}


// extender udp 53 to platform extender
func NewClientStrategy(ctx context.Context, settings *ClientStrategySettings) *ClientStrategy {
	
	// create dialers to match settings
	dialers := []*clientDialer{}
	resolvedExtenderIps := []net.Ip{}

	if settings.EnableNormal {
		// FIXME normal dial context
	}
	if settings.EnableResilient {
		// FIXME tls frag, retransmit, tls frag + retransmit
	}



	return &ClientStrategy{
		ctx, 
		settings: settings,
		dialers: dialers,
		resolvedExtenderIps,
	}


}


func (self *ClientStrategy) SetCustomExtenders(extenderIpSecrets map[net.IP]string) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.extenderIpSecrets = Copy(extenderIpSecrets)
}

func (self *ClientStrategy) CustomExtenders() map[net.IP]string {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	return Copy(self.extenderIpSecrets)
}



// the normal strategy is not mixed with extenders


// FIXME rankDialers

func (self *ClientStrategy) dialerWeights() map[*clientDialer]float32 {
	// FIXME
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

// FIXME verify error for connect timeout versus context close
func (self *ClientStrategy) updateDialerHttp(dialer *clientDialer, response *net.Response, err error) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	
	if err == nil {
		dialer.successCount += 1
		dialer.lastSuccessTime = time.Now()
	} else if urlErr, ok := err.(*url.Error); ok {
		// timeouts (FIXME meaning context close) are neither failure or success
		if !urlErr.Err.Timeout() {
			dialer.errorCount += 1
			dialer.lastErrorTime = time.Now()
		}
	} else {
		dialer.errorCount += 1
		dialer.lastErrorTime = time.Now()
	}
}

// FIXME verify error for connect timeout versus context close
func (self *ClientStrategy) updateDialerWs(dialer *clientDialer, wsConn *websocket.Conn, response *net.Response, err error) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	
	if err == nil {
		dialer.successCount += 1
		dialer.lastSuccessTime = time.Now()
	} else if urlErr, ok := err.(*url.Error); ok {
		// timeouts (FIXME meaning context close) are neither failure or success
		if !urlErr.Err.Timeout() {
			dialer.errorCount += 1
			dialer.lastErrorTime = time.Now()
		}
	} else {
		dialer.errorCount += 1
		dialer.lastErrorTime = time.Now()
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


func (self *ClientStrategy) parallel(ctx context.Context, get func(ctx context.Context, dialer *clientDialer)(*parallelResult)) *parallelResult {
	
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
		result := get(ctx, dialer)

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
	i := 0
	for i < len(parallelDialers) && i < settings.ParallelBlockSize; i += 1 {
		go run(paralleDialers[i])
	}
	for i < len(parallelDialers); i += 1 {
		select {
		case <- handleCtx.Done():
			return nil
		case result := <- out:
			if result.err == nil {
				return result, nil
			} else {
				go run(paralleDialers[i])
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

		self.collapseExtenderDialers()
		expandedDialers, _ := self.expandExtenderDialers()
		if len(expandedDialers) == 0 {
			break
		}
		i := 0
		for i < len(expandedDialers) && i < settings.ParallelBlockSize; i += 1 {
			go run(expandedDialers[i])
		}
		for i < len(expandedDialers); i += 1 {
			select {
			case <- handleCtx.Done():
				return nil
			case result := <- out:
				if result.err == nil {
					return result
				} else {
					go run(expandedDialers[i])
				}
			}
		}
	}

	return nil
}



func (self *ClientStrategy) Get(request *net.Request) (*net.Response, error) {
	get := func(ctx context.Context, dialer *clientDialer)(*parallelResult) {		
		httpClient := self.httpClient(dialer.dialTlsContext)
		response, err := httpClient.Do(request.WithContext(ctx))

		self.updateDialerHttp(dialer, response, err)

		return &parallelResult{
			response: response,
			err: err,
		}
	}

	result := self.parallel(request.Context, get)
	if result == nil {
		return nil, nil
	}
	return result.response, result.err
}


func (self *ClientStrategy) Post(request *net.Request, helloRequest *net.Request) (*net.Response, error) {
	// try ordered strategies sequentially

	// if run out, call expand(), try again


	// weighted sort all dialers
	// sequentially try one at a time with success or error, try the next until one succeeds
	// if no more to try, call expand() and keep going
	// drop strategies that have not had a successful connect in DropTimeout

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

		self.updateDialerHttp(dialer, response, err)

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
    get := func(ctx context.Context, dialer *clientDialer)(*parallelResult) {
    	wsDialer := self.wsDialer(dialer.dialTlsContext)
    	wsConn, response, err := wsDialer.DialContext(ctx, url, requestHeader)

		self.updateDialerWs(dialer, wsConn, response, err)

		return &parallelResult{
			wsConn: wsConn,
			response: response,
			err: err,
		}
	}

	result := self.parallel(ctx, get)
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

// look up dns to find new extender ips
func (self *ClientStrategy) expandExtenderDialers() ([]*clientDialer, []net.IP) {
	mutex.Lock()
	defer mutex.Unlock()

	if settings.ExpandExtenderProfileCount <= 0 {
		return []*clientDialer{}, []net.Id{}
	}


	// find current extender profiles
	// find current ips

	// expand ips
	// expand profiles
	// now randomly expand the strategy pool by N by randomly pairing available ips with available profiles


	// FIXME enumerate the dialers and pull these out
	visitedExtenderProfiles := map[ExtenderProfile]bool{}


	visitedExtenderIps := []net.Id{}
	visitedExtenderIpv4s := map[[4]byte]bool{}
	visitedExtenderIpv6s := map[[16]byte]bool{}

	for _, dialer := self.dialers {
		if dialier.IsExtender() {
			visitedExtenderProfiles[dialer.extenderProfile] = true
			ip := dialer.extenderIp
			if ip.IsIpv6() {
				ipv6 := ip.ToIpv6()
				visitedExtenderIpv6s[ipv6] = true
			} else {
				ipv4 := ip.ToIpv4()
				visitedExtenderIpv4s[ipv4] = true
			}
		}
	}



	if settings.MaxExtenderCount <= len(visitedExtenderProfiles) {
		// at maximum extenders
		return []*clientDialer{}, []net.Id{}
	}


	deviceIpv4 := IsIpv4()
	deviceIpv6 := IsIpv6()




	extenderProfiles := EnumerateExtenderProfiles(min(settings.ExpandExtenderProfileCount, settings.MaxExtenderCount - len(visitedExtenderProfiles)), visitedExtenderProfiles)


	extenderConfigs := []*ExtenderConfig{}

	if len(self.extenderIpSecrets) == 0 {

		// filter resolvedExtenderIps by visited
		unusedExtenderIps := []net.Id{}
		for _, ip := range self.resolvedExtenderIps {
			if ip.IsIpv6() {
				ipv6 := ip.ToIpv6()
				if !visitedExtenderIpv6s[ipv6] {
					unusedExtenderIps = append(unusedExtenderIps, ip)
				}
			} else {
				ipv4 := ip.ToIpv4()
				if !visitedExtenderIpv4s[ipv4] {
					unusedExtenderIps = append(unusedExtenderIps, ip)
				}
			}
		}

		// expand the ips to have one new ip per profile
		expandedExtenderIps := []net.IP{}
		if len(unusedExtenderIps < len(extenderProfiles)) {


			// iterate these for ips not used
			for _, network := range settings.ExtenderNetworks {
				

				if network.IP.IsIp4() && IsIpv4() {
					_, bits := network.Mask.Bits()
					prefix := netip.PrefixFrom(network.IP, bits)
					for addr := prefix.Addr(); prefix.Contains(addr); addr = addr.Next() {
						ipv6 := addr.As4()
						if !visitedExtenderIpv4s[ipv4] {
							visitedExtenderIpv4s[ipv4] = true
							expandedExtenderIps = append(expandedExtenderIps, ip)
						}
					}
				}
				if network.IP.IsIp6() && IsIpv6() {

					_, bits := network.Mask.Bits()
					prefix := netip.PrefixFrom(network.IP, bits)
					for addr := prefix.Addr(); prefix.Contains(addr); addr = addr.Next() {
						ipv6 := addr.As16()
						if !visitedExtenderIpv6s[ipv6] {
							visitedExtenderIpv6s[ipv6] = true
							expandedExtenderIps = append(expandedExtenderIps, ip)
						}
					}

				}
				

			}

			Shuffle(expandedExtenderIps)

			if settings.ExpandExtenderIpCount <= len(expandedExtenderIps) {
				expandedExtenderIps = expandedExtenderIps[0:settings.ExpandExtenderIpCount]
			}



			// if not enough ips, use DoH to load ips for the extender hostnames
			if len(expandedExtenderIps) < settings.ExpandExtenderIpCount && 0 < len(settings.ExtenderHostnames) {

				loadDoh(result map[string]map[string][]net.IP) {
					for _, serverResult := range result {
						for _, ips := range serverResult {
							for _, ip := range ips {
								if ip.IPv6() {
									ipv6 := ip.ToIpv6()
									if !visitedExtenderIpv6s[ipv6] {
										visitedExtenderIpv6s[ipv6] = true
										expandedExtenderIps = append(expandedExtenderIps, ip)
									}
								} else {
									ipv4 := ip.ToIpv4()
									if !visitedExtenderIpv4s[ipv4] {
										visitedExtenderIpv4s[ipv4] = true
										expandedExtenderIps = append(expandedExtenderIps, ip)
									}
								}
							}
						}
					}
				}

				// the network can be both ipv4 and ipv6
				if IsIpv4() {
					result, err := DohQuery(ctx, 4, settings.ExtenderHostnames, "A")
					if err == nil {
						loadDoh(result)	
					}
				}
				if IsIpv6() {
					result, err := DohQuery(ctx, 6, settings.ExtenderHostnames, "AAAA")
					if err == nil {
						loadDoh(result)	
					}
				}
				


				
			}

			unusedExtenderIps = append(unusedExtenderIps, expandedExtenderIps...)
		}

		
		// use unused ips first
		Shuffle(unusedExtenderIps)
		n := min(len(extenderProfiles), len(unusedExtenderIps))
		for i := range n {
			extenderConfig := &ExtenderConfig{
				ExtenderProfile: extenderProfiles[i],
				Ip: unusedExtenderIps[i],
			}
			extenderConfigs = append(extenderConfigs, extenderConfig)
		}
		if n < len(extenderProfiles) {
			for i := len(extenderProfiles) - n; i < len(extenderProfiles); i += 1 {
				// weight by success - error descending
				extenderProfileIps[extenderProfiles[i]] = WeightedSelect(visitedExtenderIps)
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




	expandedDialers := []*clientDialer{}
	for _, extenderConfig := range extenderConfigs {
		dialer := &net.Dialer{
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



	self.dialers = append(self.dialers, extenderDialers...)
	self.resolvedExtenderIps = append(self.resolvedExtenderIps, expandedExtenderIps...)






	return expandedDialers, expandedExtenderIps
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

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(requestBodyBytes))
	if err != nil {
		var empty R
		callback.Result(empty, err)
		return empty, err
	}

	req.Header.Add("Content-Type", "text/json")

	if byJwt != "" {
		auth := fmt.Sprintf("Bearer %s", byJwt)
		req.Header.Add("Authorization", auth)
	}

	client := defaultClient()
	r, err := client.Do(req)
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


func HttpGetWithStrategy[R any](ctx context.Context, clientStrategy *ClientStrategy, url string, byJwt string, result R, callback apiCallback[R]) (R, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		var empty R
		callback.Result(empty, err)
		return empty, err
	}

	req.Header.Add("Content-Type", "text/json")

	if byJwt != "" {
		auth := fmt.Sprintf("Bearer %s", byJwt)
		req.Header.Add("Authorization", auth)
	}

	client := defaultClient()
	r, err := client.Do(req)
	if err != nil {
		var empty R
		callback.Result(empty, err)
		return empty, err
	}

	responseBodyBytes, err := io.ReadAll(r.Body)
	r.Body.Close()

	err = json.Unmarshal(responseBodyBytes, &result)
	if err != nil {
		var empty R
		callback.Result(empty, err)
		return empty, err
	}

	callback.Result(result, nil)
	return result, nil
}




