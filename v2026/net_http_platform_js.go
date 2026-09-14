//go:build js

package connect

import (
	"net/http"
)

// Under js/wasm there are no sockets. Go's net/http reaches the browser's
// fetch only when the transport carries no custom dialer, and every dialer
// strategy here (ClientHello shaping, egress-bound sockets, in-process name
// resolution) is a custom dialer, so a strategy request built the native way
// silently falls back to a socket round trip that can never complete: the
// call hangs in its retry loop and the caller sees a request that was never
// sent. Each strategy request goes out as ONE fetch instead; the browser owns
// TLS, DNS and the connection pool.
func (self *ClientStrategy) httpPlatformDirect(request *http.Request) (*httpResult, bool) {
	client := &http.Client{Transport: platformDirectHttpTransport()}
	response, err := client.Do(request)
	if self.log.V(2).Enabled() {
		if err != nil {
			self.log.Infof("[net]http fetch %s %s = %s\n", request.Method, request.URL, err)
		} else {
			self.log.Infof("[net]http fetch %s %s = %s\n", request.Method, request.URL, response.Status)
		}
	}
	result := newEvalResultFromHttpResponse(response, err, self.settings.MaxHttpResponseBodyBytes)
	// the one response is the selected response: read its body now, the way
	// parallelEval does for the winning route
	result.Selected()
	httpResult, resultErr := materializeHttpResult(result)
	if resultErr != nil {
		println("[net]http fetch", request.Method, request.URL.String(), "=", resultErr.Error())
		return nil, true
	}
	return httpResult, true
}

// The one transport the browser can drive: no dialers, so net/http uses fetch.
func platformDirectHttpTransport() http.RoundTripper {
	return &http.Transport{}
}
