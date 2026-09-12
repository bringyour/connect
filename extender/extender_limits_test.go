// The limits of EXTENDER.md A9: concurrent connections per source, total
// connections, and the header read deadline.

package extender

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// Opens one connection to the tcp carrier and completes the outer handshake,
// which proves the extender accepted and counted it.
func dialHandshakedExtenderConn(t *testing.T, fixture *extenderFixture) *tls.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", fixture.authority(fixture.tcpPort))
	if err != nil {
		t.Fatal(err)
	}
	tlsConn := tls.Client(conn, &tls.Config{
		ServerName:         testServerName,
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS13,
	})
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		conn.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		tlsConn.Close()
	})
	return tlsConn
}

// Tries one more connection and reports whether the extender refused it. A
// refusal closes the socket before any handshake, so the handshake fails.
func extenderRefusesAnotherConn(t *testing.T, fixture *extenderFixture) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", fixture.authority(fixture.tcpPort))
	if err != nil {
		// a refused connect is also a refusal
		return true
	}
	defer conn.Close()
	tlsConn := tls.Client(conn, &tls.Config{
		ServerName:         testServerName,
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS13,
	})
	return tlsConn.HandshakeContext(ctx) != nil
}

// A source over its concurrent cap is refused while it holds the cap, and
// accepted again once it releases (A9).
func TestExtenderRefusesASourceOverItsConcurrentCap(t *testing.T) {
	fixture := newExtenderFixture(t, "127.0.0.1", func(settings *ExtenderSettings) {
		settings.MaxConnectionCountPerSource = 1
		settings.MaxConnectionCount = 0
	})

	held := dialHandshakedExtenderConn(t, fixture)
	if !extenderRefusesAnotherConn(t, fixture) {
		t.Fatal("a second connection from the same source was accepted over the cap")
	}

	// releasing the held connection frees the slot; the extender must observe
	// the close before it can accept again, so wait for the attributed drop
	held.Close()
	if _, ok := fixture.nextError(); !ok {
		t.Fatal("the released connection was not attributed")
	}
	if extenderRefusesAnotherConn(t, fixture) {
		t.Fatal("a connection was refused after the source released its slot")
	}
}

// The total cap refuses a connection from any source (A9).
func TestExtenderRefusesOverTheTotalCap(t *testing.T) {
	fixture := newExtenderFixture(t, "127.0.0.1", func(settings *ExtenderSettings) {
		settings.MaxConnectionCountPerSource = 0
		settings.MaxConnectionCount = 1
	})

	dialHandshakedExtenderConn(t, fixture)
	if !extenderRefusesAnotherConn(t, fixture) {
		t.Fatal("a connection was accepted over the total cap")
	}
}

// A client that completes the handshake and never sends a header is dropped at
// the header deadline (A9).
func TestExtenderDropsAClientThatSendsNoHeader(t *testing.T) {
	fixture := newExtenderFixture(t, "127.0.0.1", func(settings *ExtenderSettings) {
		settings.HeaderTimeout = 250 * time.Millisecond
	})

	conn := dialHandshakedExtenderConn(t, fixture)
	if _, err := io.ReadAll(conn); err != nil && !strings.Contains(err.Error(), "closed") {
		// a reset is as good as a clean close here; either means dropped
		t.Logf("read after the header deadline: %v", err)
	}
	extenderErr, ok := fixture.nextError()
	if !ok {
		t.Fatal("the header deadline did not drop the connection")
	}
	if !strings.Contains(extenderErr.Error(), "header length") {
		t.Fatalf("attributed error = %v, expected the header length stage", extenderErr)
	}
}
