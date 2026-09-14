package connect

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type cancelTestRoundTripper func(request *http.Request) (*http.Response, error)

func (self cancelTestRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return self(request)
}

type cancelTestWinnerBody struct {
	releaseLoser chan struct{}
	loserReady   chan struct{}
	releaseOnce  sync.Once
	read         bool
}

func (self *cancelTestWinnerBody) Read(buffer []byte) (int, error) {
	if self.read {
		return 0, io.EOF
	}
	self.read = true
	self.releaseOnce.Do(func() {
		close(self.releaseLoser)
	})
	<-self.loserReady
	return copy(buffer, []byte("{}")), io.EOF
}

func (self *cancelTestWinnerBody) Close() error {
	return nil
}

// A client request context controls the complete request lifetime, including
// reading its response body. Go's standard HTTP/1 transport watches that
// context while its read loop owns an outstanding body and closes the
// connection on cancellation. Its HTTP/2 transport watches the same context
// per stream, aborts only that stream, and leaves the multiplexed connection
// reusable. This is the ownership that lets a canceled parallel loser avoid a
// potentially blocking Body.Close without leaking its transport resources.
func TestParallelHttpLoserCancellationReleasesStandardTransport(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		for _, test := range []struct {
			name  string
			http2 bool
		}{
			{name: "http1"},
			{name: "http2", http2: true},
		} {
			t.Run(test.name, func(t *testing.T) {
				testCtx, testCancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer testCancel()

				serverRequestCanceled := make(chan struct{})
				var cancelOnce sync.Once
				var requestCount atomic.Int32
				var connectionCount atomic.Int32
				server := newFamilyHttptestUnstartedServer(t, ipVersion, http.HandlerFunc(func(
					responseWriter http.ResponseWriter,
					request *http.Request,
				) {
					if requestCount.Add(1) != 1 {
						_, _ = io.WriteString(responseWriter, "followup")
						return
					}

					responseWriter.Header().Set("Content-Length", "7")
					responseWriter.WriteHeader(http.StatusOK)
					_, _ = io.WriteString(responseWriter, "pending")
					responseWriter.(http.Flusher).Flush()
					<-request.Context().Done()
					cancelOnce.Do(func() {
						close(serverRequestCanceled)
					})
				}))
				server.EnableHTTP2 = test.http2
				server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
					if state == http.StateNew {
						connectionCount.Add(1)
					}
				}
				server.StartTLS()
				defer server.Close()

				transportClient := server.Client()
				defer transportClient.CloseIdleConnections()
				loserReady := make(chan struct{})
				releaseLoser := make(chan struct{})
				var loserRequestContext context.Context
				loserProtocolMajor := 0
				loserClient := &http.Client{
					Transport: cancelTestRoundTripper(func(request *http.Request) (*http.Response, error) {
						<-releaseLoser
						loserRequestContext = request.Context()
						response, err := transportClient.Transport.RoundTrip(request)
						if response != nil {
							loserProtocolMajor = response.ProtoMajor
						}
						close(loserReady)
						return response, err
					}),
					Timeout: 30 * time.Minute,
				}

				settings := DefaultClientStrategySettings()
				settings.RequestTimeout = 30 * time.Minute
				settings.ParallelBlockSize = 2
				winnerDialer := &clientDialer{
					description:   "winner",
					minimumWeight: 1,
					settings:      settings,
					httpClient: &http.Client{Transport: cancelTestRoundTripper(func(
						request *http.Request,
					) (*http.Response, error) {
						return &http.Response{
							StatusCode:    http.StatusOK,
							Status:        "200 OK",
							Header:        http.Header{},
							ContentLength: 2,
							Body: &cancelTestWinnerBody{
								releaseLoser: releaseLoser,
								loserReady:   loserReady,
							},
							Request: request,
						}, nil
					})},
				}
				loserDialer := &clientDialer{
					description:   "loser",
					minimumWeight: 1,
					settings:      settings,
					httpClient:    loserClient,
				}
				strategyCtx, strategyCancel := context.WithCancel(context.Background())
				defer strategyCancel()
				strategy := &ClientStrategy{
					ctx:      strategyCtx,
					log:      loggerOrDefault(nil),
					settings: settings,
					dialers: map[*clientDialer]bool{
						winnerDialer: true,
						loserDialer:  true,
					},
					extenderIpSecrets: map[netip.Addr]string{},
				}

				request, err := http.NewRequestWithContext(testCtx, http.MethodGet, server.URL, nil)
				if err != nil {
					t.Fatal(err)
				}
				result, err := strategy.HttpParallel(request)
				if err != nil {
					t.Fatalf("parallel request: %v", err)
				}
				if string(result.bodyBytes) != "{}" {
					t.Fatalf("selected body = %q, expected winner", result.bodyBytes)
				}
				if loserRequestContext == nil || loserRequestContext.Err() == nil {
					t.Fatal("losing HTTP attempt context was not canceled")
				}
				expectedProtocolMajor := 1
				if test.http2 {
					expectedProtocolMajor = 2
				}
				if loserProtocolMajor != expectedProtocolMajor {
					t.Fatalf(
						"losing response protocol major = %d, expected %d",
						loserProtocolMajor,
						expectedProtocolMajor,
					)
				}
				select {
				case <-serverRequestCanceled:
				case <-testCtx.Done():
					t.Fatalf("server did not observe losing request cancellation: %v", testCtx.Err())
				}

				followupRequest, err := http.NewRequestWithContext(testCtx, http.MethodGet, server.URL, nil)
				if err != nil {
					t.Fatal(err)
				}
				followupResponse, err := transportClient.Do(followupRequest)
				if err != nil {
					t.Fatalf("follow-up request through canceled loser's transport: %v", err)
				}
				followupBody, readErr := io.ReadAll(followupResponse.Body)
				closeErr := followupResponse.Body.Close()
				if readErr != nil {
					t.Fatal(readErr)
				}
				if closeErr != nil {
					t.Fatal(closeErr)
				}
				if string(followupBody) != "followup" {
					t.Fatalf("follow-up body = %q", followupBody)
				}
				if test.http2 && connectionCount.Load() != 1 {
					t.Fatalf(
						"HTTP/2 stream cancellation used %d connections, expected reuse",
						connectionCount.Load(),
					)
				}
			})
		}
	})
}
