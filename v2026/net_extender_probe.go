package connect

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/netip"

	"github.com/urnetwork/connect/v2026/protocol"
)

// Extender probes (EXTENDER.md C2, C3).
//
// The operator proves an extender before it publishes a record for it, and
// again on the uptime tick. Two probes do that, and both run from the outside
// exactly as a client would: no in-process shortcut, so what passes is what a
// client can use.
//
// ProbeExtenderCarrier proves one carrier and the identity key behind it. The
// challenge is the liveness half -- the extender must sign bytes chosen now --
// and the outer certificate is the identity half, since a leaf that verifies
// under the expected key can only come from the holder of that key (B3).
// ProbeExtenderForward proves the forward itself: a verified `GET /hello`
// through the tcp carrier, whose answer also reports the address the operator
// saw, which is the address the extender is published under.
//
// Both bound their work by the caller's context and the connect settings
// timeouts, so a probe of a black hole costs a known amount of time.
//
// Neither probe leaves anything behind: the carrier probe closes its stream as
// soon as the response is verified, and the forward probe closes its idle
// connections.

// The hello fields a probe reads. The server's HelloResult carries more, and
// an unknown field is ignored, so this stays valid as hello grows.
type extenderHelloResult struct {
	ClientAddress string `json:"client_address"`
}

// ProbeExtenderCarrier dials one carrier of one extender with a fresh
// challenge and verifies what comes back (C2).
//
// ip, connectMode and port name the carrier; dnsTld is the encoding tld of the
// dns carrier and is ignored by the others; serverName is the spoof name
// presented as the outer sni. destinationHost and destinationPort are the
// forward destination of the probe request, which must match an operator
// pattern of the extender (A5), so the operator passes its own api host here.
//
// expectPublicKey, when set, is required twice: the outer leaf must be signed
// by it, and the response must publish it and carry a challenge signature that
// verifies under it. Empty accepts any extender and verifies nothing, which is
// what a probe of an extender with no identity can assert.
//
// The returned response is the extender's own, including the carriers it says
// it serves. The probe stream is closed before returning; nothing is forwarded
// over it.
func ProbeExtenderCarrier(
	ctx context.Context,
	connectSettings *ConnectSettings,
	ip netip.Addr,
	connectMode ExtenderConnectMode,
	port int,
	dnsTld string,
	serverName string,
	expectPublicKey []byte,
	destinationHost string,
	destinationPort int,
) (*protocol.ExtenderResponse, error) {
	if !ip.IsValid() {
		return nil, fmt.Errorf("extender address is not valid")
	}
	if destinationHost == "" {
		return nil, fmt.Errorf("extender probe has no destination")
	}

	probeCtx, probeCancel := probeContext(ctx, connectSettings)
	defer probeCancel()

	challenge, err := NewExtenderChallenge()
	if err != nil {
		return nil, err
	}

	extenderConfig := &ExtenderConfig{
		Profile: ExtenderProfile{
			ConnectMode: connectMode,
			ServerName:  serverName,
			Port:        port,
			DnsTld:      dnsTld,
		},
		Ip:        ip,
		PublicKey: expectPublicKey,
	}
	conn, response, err := DialExtender(
		probeCtx,
		connectSettings,
		extenderConfig,
		&ExtenderDial{
			DestinationHost: destinationHost,
			DestinationPort: destinationPort,
			Challenge:       challenge,
			Service:         ExtenderServiceForward,
		},
	)
	if err != nil {
		// ownership transfers for every non-nil result, including a rejected one
		if conn != nil {
			conn.Close()
		}
		return nil, err
	}
	// the probe never forwards; release the stream and the destination
	// connection the extender opened for it
	conn.Close()

	if response == nil {
		return nil, fmt.Errorf("extender sent no response")
	}
	if 0 < len(expectPublicKey) {
		if string(response.PublicKey) != string(expectPublicKey) {
			return nil, fmt.Errorf("extender published another identity key")
		}
		if !VerifyExtenderChallenge(expectPublicKey, challenge, response.ChallengeSignature) {
			return nil, fmt.Errorf("extender challenge signature does not verify")
		}
	}
	return response, nil
}

// ProbeExtenderForward proves that the tcp carrier forwards by performing a
// verified `GET /hello` to the api url through it, and returns the caller
// address the api saw (C2).
//
// tlsConfig is the inner configuration, which is verified normally: it is the
// platform's pinned roots in production and the fixture roots in tests. nil
// keeps the connect settings configuration. expectPublicKey, when set, also
// requires the outer leaf to be signed by the extender identity key (B3).
func ProbeExtenderForward(
	ctx context.Context,
	connectSettings *ConnectSettings,
	ip netip.Addr,
	port int,
	serverName string,
	expectPublicKey []byte,
	apiUrl string,
	tlsConfig *tls.Config,
) (string, error) {
	if !ip.IsValid() {
		return "", fmt.Errorf("extender address is not valid")
	}

	probeCtx, probeCancel := probeContext(ctx, connectSettings)
	defer probeCancel()

	// the inner tls is the caller's, and only the inner tls is verified: the
	// outer leaf is issued by the extender for a name it does not own
	probeSettings := *connectSettings
	if tlsConfig != nil {
		probeSettings.TlsConfig = tlsConfig
	}
	client := NewExtenderHttpClient(&probeSettings, &ExtenderConfig{
		Profile: ExtenderProfile{
			ConnectMode: ExtenderConnectModeTcpTls,
			ServerName:  serverName,
			Port:        port,
		},
		Ip:        ip,
		PublicKey: expectPublicKey,
	})
	defer client.CloseIdleConnections()

	request, err := HelloRequestFromUrl(probeCtx, apiUrl, "")
	if err != nil {
		return "", err
	}
	response, err := client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("extender hello answered with status %d", response.StatusCode)
	}
	// hello is small; a compromised extender cannot make the probe read forever
	bodyBytes, err := io.ReadAll(io.LimitReader(response.Body, extenderHelloMaxByteCount))
	if err != nil {
		return "", err
	}
	helloResult := &extenderHelloResult{}
	if err := json.Unmarshal(bodyBytes, helloResult); err != nil {
		return "", err
	}
	if helloResult.ClientAddress == "" {
		return "", fmt.Errorf("extender hello carried no client address")
	}
	return helloResult.ClientAddress, nil
}

// Read ceiling of a hello answer through an extender.
const extenderHelloMaxByteCount = 64 * 1024

// The probe budget: the caller's deadline, further bounded by the request
// timeout so a probe of an unresponsive address ends on its own.
func probeContext(
	ctx context.Context,
	connectSettings *ConnectSettings,
) (context.Context, context.CancelFunc) {
	if 0 < connectSettings.RequestTimeout {
		return context.WithTimeout(ctx, connectSettings.RequestTimeout)
	}
	return context.WithCancel(ctx)
}
