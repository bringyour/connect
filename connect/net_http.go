


// stores statistics on client strategies
type ClientStrategy struct {

	// handhake timeout
	// connect timeout

	TlsDialContexts []DialContextFunc
	// FIXME statistics
	// FIXME stats mutex
}

// typical context list will be:
// normal
// resilient
// extender udp 53 to platform extender
// - or if there is an extender -
// extender tls 443
// extender quic 443
// extender udp 53
func NewClientStrategy(extenderConfigs []*ExtenderConfig) *ClientStrategy {
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



func (self *ClientStrategy) Get() {
	// if first strategy has a net score below threshold, run all in parallel
	// else sequentially try strategy while above threshold, then the remaining in parallel
}


func (self *ClientStrategy) Post() {
	// try ordered strategies sequentially
}


func (self *ClientStrategy) WsDialContext(ctx context.Context, url string) (*Websocket, error) {

}

