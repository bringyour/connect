// The refusals of the v1 framing (EXTENDER.md A3).
//
// The length-prefixed header is accepted for one release so an old client
// still reaches a v2 extender. v1 has no response frame, so every refusal is a
// silent close -- there is nothing on the wire to tell one reason from another,
// which is exactly why they need a test: a v1 path that forwarded where v2
// refuses would be invisible.

package extender

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/protocol"
)

// Writes one v1 framed header on a terminated tcp connection and reports what
// the extender did with the connection afterward.
func sendV1Header(t *testing.T, fixture *extenderFixture, headerBytes []byte) error {
	t.Helper()
	conn, err := (&net.Dialer{}).DialContext(
		context.Background(), "tcp", fixture.authority(fixture.tcpPort))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	outerConn := tls.Client(conn, &tls.Config{
		ServerName:         testServerName,
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS13,
	})
	if err := outerConn.HandshakeContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	frameBytes := make([]byte, 4+len(headerBytes))
	binary.BigEndian.PutUint32(frameBytes[0:4], uint32(len(headerBytes)))
	copy(frameBytes[4:], headerBytes)
	if _, err := outerConn.Write(frameBytes); err != nil {
		return err
	}
	// a refused v1 connection is closed with nothing written, so the read ends
	// rather than answering; an accepted one would relay the destination's
	// handshake instead
	if err := outerConn.SetReadDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 1)
	_, err = outerConn.Read(buffer)
	return err
}

// A v1 header the extender will not honor closes the connection without
// forwarding: a bad secret, a destination outside the whitelist, and bytes
// that are not a header at all.
func TestExtenderV1RefusesWhatV2Refuses(t *testing.T) {
	fixture := newExtenderFixture(t, "127.0.0.1", nil)

	wrongSecret, err := proto.Marshal(newTestExtenderHeader("dest.example", 443, "another-secret"))
	if err != nil {
		t.Fatal(err)
	}
	noSecret, err := proto.Marshal(&protocol.ExtenderHeader{
		DestinationHost: "dest.example",
		DestinationPort: 443,
	})
	if err != nil {
		t.Fatal(err)
	}
	blockedHost, err := proto.Marshal(newTestExtenderHeader("blocked.example", 443, testSecret))
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name        string
		headerBytes []byte
	}{
		{name: "a header signed with another secret", headerBytes: wrongSecret},
		{name: "a header with no signature at all", headerBytes: noSecret},
		{name: "a destination outside the whitelist", headerBytes: blockedHost},
		{name: "bytes that are not a header", headerBytes: []byte{0xff, 0xff, 0xff, 0xff}},
		{name: "an empty header", headerBytes: []byte{}},
	}
	for _, c := range cases {
		err := sendV1Header(t, fixture, c.headerBytes)
		if err == nil {
			t.Errorf("%s was not refused", c.name)
			continue
		}
		// the refusal is a close, not an answer
		if err != io.EOF && !strings.Contains(err.Error(), "closed") &&
			!strings.Contains(err.Error(), "reset") {
			t.Errorf("%s closed with %v", c.name, err)
		}
		// nothing was forwarded for any of them
		if networks := fixture.forwardNetworksSeen(); 0 < len(networks) {
			t.Errorf("%s forwarded over %v", c.name, networks)
		}
	}
}

// A v1 header at the cap is still read, and one byte over it is not a v1 frame
// at all: the discriminator sends it to the http server, which answers the
// bytes as a request rather than forwarding them (A3).
func TestExtenderV1LengthDiscriminatesAtTheCap(t *testing.T) {
	fixture := newExtenderFixture(t, "127.0.0.1", nil)

	// a length of exactly the cap is the v1 path; the body that follows is not
	// a header, so the connection is closed without forwarding
	atCap := make([]byte, 1024)
	if err := sendV1Header(t, fixture, atCap); err == nil {
		t.Error("a v1 frame at the cap was not refused")
	}
	if networks := fixture.forwardNetworksSeen(); 0 < len(networks) {
		t.Errorf("a v1 frame at the cap forwarded over %v", networks)
	}
	if extenderErr, ok := fixture.nextError(); !ok {
		t.Error("the refused v1 frame was not attributed")
	} else if !strings.Contains(extenderErr.Error(), "header") {
		t.Errorf("the refused v1 frame was attributed to %v", extenderErr)
	}
}
