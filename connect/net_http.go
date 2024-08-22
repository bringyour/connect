package connect





// censorship-resistant strategies for making https connections
// this uses a random walk of best practices to obfuscate and double-encrypt https connections



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


	LockInMinimumWeight float


	// non-extender minimum weight
	// all dialers are weighted 0 to 1
	// success / (success + error)
	MinimumWeight float


	// transportMode can be Tcp or Quic
	// extender profile is a transport mode, server name, and port 
	ExtenderProfiles []*ExtenderProfiles
	ExpandExtenderProfileCount int

	ExtenderNetworks []net.IpNet
	// these are evaluated with DoH to grow the extender ips
	ExtenderHostnames []string
	InitialExtenderIpCount int
	ExpandExtenderIpCount int

	ExpandExtenderCount int
	
	ParallelBlockSize int

	// success / (success + error)
	MinimumExtenderWeight float
	MinimumExtenderWeightCount int

	// drop dialers that have not had a successful connect in this timeout
	ExtenderDropTimeout time.Duration


	DefaultHttpTimeout time.Duration
	DefaultHttpConnectTimeout time.Duration
	DefaultHttpTlsTimeout time.Duration
	
	
}


// non-extender dialers are never dropped
type clientDialer struct {
	Dialer
	ExtenderProfile

	dialer DialContextFunc
	extenderProfile ExtenderProfile

	successCount int
	errorCount int
	lastSuccessTime time.Time
	lastErrorTime time.Time
}



// stores statistics on client strategies
type ClientStrategy struct {

	// handhake timeout
	// connect timeout


	settings *ClientStrategySettings


	mutex sync.Mutex
	// dialers are only updated inside the mutex
	dialers []*clientDialer


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
	
}


// extender udp 53 to platform extender
func NewClientStrategy() *ClientStrategy {
	if len(extenderConfigs) == 0 {
		// normal, resilient, extender udp 53 to platform extenders (extender.bringyour.com)
	} else {
		// use extender configs as given
		// typically the system will use the following defaults
		// extender tls 443
		// extender quic 443
		// extender udp 53
	}

}



// the normal strategy is not mixed with extenders

// look up dns to find new extender ips
func (self *ClientStrategy) expand() []*clientDialer {


	// expand ips
	// expand profiles
	// now randomly expand the strategy pool by N by randomly pairing available ips with available profiles


	// drop strategies that have not had a successful connect in DropTimeout
}

// FIXME rankDialers

func (self *ClientStrategy) rankedDialers() map[*clientDialer]float32 {

}


func (self *ClientStrategy) Get(ctx context.Context, request *net.Request) (*net.Response, error) {
	// if first strategy has a net score below threshold, run all in parallel in blocks of N
	// else sequentially try strategy while above threshold, then the remaining in parallel in blocks of N

	// if run out, call expand(), try again


	// find all dialers with weight >= LockInMinimumWeight
	// weighted sort lock in and try those in order
	// then, weighted sort the rest
	// in blocks of ParallelBlockSize, run dialers in parallel
	// when one finishes with success or error, try the next, until one succeeds
	// if no more to try, call expand() and keep going with the parallel evaluation



}


func (self *ClientStrategy) Post(ctx context.Context, request *net.Request) (*net.Response, error) {
	// try ordered strategies sequentially

	// if run out, call expand(), try again


	// weighted sort all dialers
	// sequentially try one at a time with success or error, try the next until one succeeds
	// if no more to try, call expand() and keep going
}


func (self *ClientStrategy) WsDialContext(ctx context.Context, url string, requestHeader http.Header) (*websocket.Conn, error) {

	// find all dialers with weight >= LockInMinimumWeight
	// weighted sort lock in and try those in order
	// then, weighted sort the rest
	// in blocks of ParallelBlockSize, run dialers in parallel
	// when one finishes with success or error, try the next, until one succeeds
	// if no more to try, call expand() and keep going with the parallel evaluation

	// success is websocket connects

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




