package extender

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/quic-go/quic-go/http3"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect"
	"github.com/urnetwork/connect/protocol"
)

// The extender request handler (EXTENDER.md A3, A4, A7, A8).
//
// One handler serves every carrier: the http/1.1 and h2 servers on the tcp
// carrier and the h3 server on the udp carriers. An accepted request is
// answered with 200 and the ExtenderResponse, after which the stream is taken
// over -- hijacked on tcp, taken with HTTPStreamer on h3 -- and carries the
// inner bytes. Every refusal is 403 with no body and closes the connection.
//
// An extender request that arrives over h2 is refused, because an h2 stream
// cannot be hijacked. Any request that is not an extender request is refused
// in this phase; phase 1b answers it with the reverse proxy (A5).

type extenderHandler struct {
	server *ExtenderServer
}

func (self *extenderHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	server := self.server

	if !isExtenderRequest(req) {
		// A5 replaces this with a reverse proxy to the requested name in
		// phase 1b; until then an extender only speaks the extender protocol.
		self.refuse(w, req, "request", fmt.Errorf("%s %s is not an extender request", req.Method, req.URL.Path))
		return
	}
	if req.ProtoMajor == 2 {
		self.refuse(w, req, "request", fmt.Errorf("an extender request cannot use h2"))
		return
	}

	header, err := readExtenderHeader(req)
	if err != nil {
		self.refuse(w, req, "header decode", err)
		return
	}
	if !server.IsAllowedSecret(header) {
		self.refuse(w, req, "header authorization", fmt.Errorf("secret signature is not allowed"))
		return
	}

	var serviceConnHandler func(conn net.Conn)
	switch header.Service {
	case connect.ExtenderServiceForward:
		if !server.IsAllowedHost(header.DestinationHost) {
			self.refuse(w, req, "destination authorization", fmt.Errorf("host %q is not allowed", header.DestinationHost))
			return
		}
	case connect.ExtenderServiceGossip:
		serviceConnHandler = server.settings.GossipConnHandler
	case connect.ExtenderServiceFeed:
		serviceConnHandler = server.settings.FeedConnHandler
	default:
		self.refuse(w, req, "service", fmt.Errorf("service %d is not known", header.Service))
		return
	}
	if header.Service != connect.ExtenderServiceForward && serviceConnHandler == nil {
		self.refuse(w, req, "service", fmt.Errorf("service %d is not available", header.Service))
		return
	}

	responseFrameBytes, err := connect.ExtenderResponseFrame(&protocol.ExtenderResponse{
		PublicKey:          server.PublicKey(),
		ChallengeSignature: server.SignChallenge(header.Challenge),
		Carriers:           server.Carriers(),
	})
	if err != nil {
		self.refuse(w, req, "response", err)
		return
	}

	w.Header().Set("Content-Type", connect.ExtenderContentType)
	if req.ProtoMajor == 1 {
		// http/1.1 would otherwise chunk a flushed body, and the bytes after
		// the response are raw. h3 must not carry a content length at all:
		// there the response body and the raw bytes that follow are the same
		// DATA stream, and a length would bound the reader the client keeps.
		w.Header().Set("Content-Length", strconv.Itoa(len(responseFrameBytes)))
	}
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(responseFrameBytes); err != nil {
		server.reportError("response", err)
		return
	}

	clientConn, err := takeOverConn(w, req)
	if err != nil {
		server.reportError("take over", err)
		return
	}
	defer clientConn.Close()

	handleCtx, handleCancel := context.WithCancel(req.Context())
	defer handleCancel()

	if serviceConnHandler != nil {
		// the service owns the stream until it returns (A8)
		serviceConnHandler(clientConn)
		return
	}

	forwardConn, err := server.dialForward(handleCtx, req.RemoteAddr, header)
	if err != nil {
		return
	}
	defer forwardConn.Close()

	server.relay(handleCtx, handleCancel, clientConn, forwardConn)
}

// Refuses with 403 and no body, closing the connection (A4).
func (self *extenderHandler) refuse(w http.ResponseWriter, req *http.Request, stage string, err error) {
	self.server.reportError(stage, err)
	if req.ProtoMajor == 1 {
		w.Header().Set("Connection", "close")
		w.Header().Set("Content-Length", "0")
	}
	w.WriteHeader(http.StatusForbidden)
}

// The A3 shape: POST / with the extender content type.
func isExtenderRequest(req *http.Request) bool {
	if req.Method != http.MethodPost {
		return false
	}
	if req.URL == nil || req.URL.Path != "/" {
		return false
	}
	contentType := req.Header.Get("Content-Type")
	if i := strings.IndexByte(contentType, ';'); 0 <= i {
		contentType = contentType[0:i]
	}
	return strings.EqualFold(strings.TrimSpace(contentType), connect.ExtenderContentType)
}

// Reads the serialized header, which every carrier delimits with the request
// content length.
func readExtenderHeader(req *http.Request) (*protocol.ExtenderHeader, error) {
	if req.ContentLength < 0 {
		return nil, fmt.Errorf("extender header has no content length")
	}
	if connect.ExtenderMaxHeaderByteCount < req.ContentLength {
		return nil, fmt.Errorf(
			"extender header is %d bytes, at most %d",
			req.ContentLength,
			connect.ExtenderMaxHeaderByteCount,
		)
	}
	headerBytes := make([]byte, req.ContentLength)
	if _, err := io.ReadFull(req.Body, headerBytes); err != nil {
		return nil, err
	}
	header := &protocol.ExtenderHeader{}
	if err := proto.Unmarshal(headerBytes, header); err != nil {
		return nil, err
	}
	return header, nil
}

// Takes the stream over after the response: the h3 stream on the udp carriers,
// the hijacked connection on tcp. The buffered reader of a hijack is kept,
// because it can already hold the first inner bytes.
func takeOverConn(w http.ResponseWriter, req *http.Request) (net.Conn, error) {
	if streamer, ok := w.(http3.HTTPStreamer); ok {
		return newStreamConn(
			streamer.HTTPStream(),
			streamAddr{network: "udp", address: ""},
			streamAddr{network: "udp", address: req.RemoteAddr},
		), nil
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return nil, fmt.Errorf("the extender response writer cannot be taken over")
	}
	if flusher, ok := w.(http.Flusher); ok {
		// the hijack releases the connection without flushing the buffered
		// response body
		flusher.Flush()
	}
	conn, bufrw, err := hijacker.Hijack()
	if err != nil {
		return nil, err
	}
	return newConnWithReader(conn, bufrw.Reader), nil
}

// streamAddr names the endpoint of a taken-over h3 stream, which has no socket
// address of its own.
type streamAddr struct {
	network string
	address string
}

func (self streamAddr) Network() string {
	return self.network
}

func (self streamAddr) String() string {
	return self.address
}
