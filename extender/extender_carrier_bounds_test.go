// What the udp carriers share with the tcp carrier (EXTENDER.md A6, A8, A9):
// the connection caps are counted on every carrier, a reserved service owns
// its stream only until it returns, and the dns carrier always has an encoding
// tld to translate.

package extender

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/urnetwork/connect"
)

// A quic connection is counted and capped like a tcp one, so a source cannot
// step around the concurrent caps by moving to a udp carrier (A9).
func TestExtenderQuicCarrierRefusesOverItsConnectionCaps(t *testing.T) {
	cases := []struct {
		description string
		configure   func(settings *ExtenderSettings)
	}{
		{
			description: "per source",
			configure: func(settings *ExtenderSettings) {
				settings.MaxConnectionCountPerSource = 1
				settings.MaxConnectionCount = 0
			},
		},
		{
			description: "total",
			configure: func(settings *ExtenderSettings) {
				settings.MaxConnectionCountPerSource = 0
				settings.MaxConnectionCount = 1
			},
		},
	}
	for _, c := range cases {
		fixture := newExtenderFixture(t, "127.0.0.1", c.configure)

		// a completed request proves the connection was accepted, counted and
		// served, which is what the next one runs into
		heldTransport := newRawExtenderH3Transport(fixture, testSpoofName)
		request, err := http.NewRequest(http.MethodGet, "https://"+testSpoofName+"/", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := heldTransport.RoundTrip(request)
		if err != nil {
			heldTransport.Close()
			t.Fatalf("%s: %v", c.description, err)
		}
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if count := fixture.server.ConnectionCount(); count != 1 {
			heldTransport.Close()
			t.Fatalf("%s connections = %d, expected the quic connection to be counted", c.description, count)
		}

		refusedTransport := newRawExtenderH3Transport(fixture, testSpoofName)
		refusedRequest, err := http.NewRequest(http.MethodGet, "https://"+testSpoofName+"/", nil)
		if err != nil {
			t.Fatal(err)
		}
		refusedResponse, err := refusedTransport.RoundTrip(refusedRequest)
		if err == nil {
			refusedResponse.Body.Close()
			t.Errorf("%s: a quic connection was served over the cap", c.description)
		}
		refusedTransport.Close()
		heldTransport.Close()
	}
}

// A reserved service owns its stream only until its handler returns, and the
// extender closes the stream afterward (A8). A listener implementation that
// keeps the connection therefore has to wait for its consumer inside the
// handler.
func TestExtenderClosesAServiceStreamAfterTheHandlerReturns(t *testing.T) {
	serviceConns := make(chan net.Conn, 1)
	fixture := newExtenderFixture(t, "127.0.0.1", func(settings *ExtenderSettings) {
		settings.GossipConnHandler = func(conn net.Conn) {
			// the handler keeps the stream and returns straight away
			serviceConns <- conn
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, _, err := connect.DialExtender(
		ctx,
		fixture.connectSettings(),
		fixture.extenderConfig(connect.ExtenderCarrierTcp),
		&connect.ExtenderDial{Service: connect.ExtenderServiceGossip},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var serviceConn net.Conn
	select {
	case serviceConn = <-serviceConns:
	case <-time.After(10 * time.Second):
		t.Fatal("the gossip service never received the stream")
	}

	// the read would block for as long as the client says nothing, so it ends
	// only because the extender closed the stream the handler returned
	serviceReads := make(chan error, 1)
	go func() {
		buffer := make([]byte, 1)
		_, err := serviceConn.Read(buffer)
		serviceReads <- err
	}()
	select {
	case err := <-serviceReads:
		if err == nil {
			t.Fatal("the stream the service returned still carries bytes")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the extender did not close the service stream after the handler returned")
	}

	// and the client sees the same end of stream
	clientReads := make(chan error, 1)
	go func() {
		buffer := make([]byte, 1)
		_, err := conn.Read(buffer)
		clientReads <- err
	}()
	select {
	case err := <-clientReads:
		if err == nil {
			t.Fatal("the client stream is still open after the service handler returned")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the client was not released with the service stream")
	}
}

// The dns carrier always translates under some tld: an empty list falls back
// to the connect default rather than leaving the carrier with nothing to
// decode (A6).
func TestExtenderDnsCarrierFallsBackToTheDefaultTld(t *testing.T) {
	for _, family := range testLoopbackFamilies {
		fixture := newExtenderFixture(t, family.loopbackIp, func(settings *ExtenderSettings) {
			settings.DnsTlds = nil
		})
		extenderConfig := fixture.extenderConfig(connect.ExtenderCarrierDns)
		extenderConfig.Profile.DnsTld = connect.DefaultExtenderDnsTld
		client := connect.NewExtenderHttpClient(fixture.connectSettings(), extenderConfig)

		response, err := client.Get("https://dest.example/hello")
		if err != nil {
			if extenderErr, ok := fixture.nextError(); ok {
				t.Fatalf("%s: %v; extender: %v", family.loopbackIp, err, extenderErr)
			}
			t.Fatalf("%s: %v", family.loopbackIp, err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatalf("%s: %v", family.loopbackIp, err)
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("%s status = %d, expected %d", family.loopbackIp, response.StatusCode, http.StatusOK)
		}
		if !strings.Contains(string(body), "dest.example") {
			t.Fatalf("%s body = %q", family.loopbackIp, body)
		}
		forwardNetwork, err := fixture.nextForwardNetwork()
		if err != nil {
			t.Fatal(err)
		}
		if forwardNetwork != family.forwardNetwork {
			t.Fatalf("%s forward network = %q, expected %q", family.loopbackIp, forwardNetwork, family.forwardNetwork)
		}
		client.CloseIdleConnections()
	}
}
