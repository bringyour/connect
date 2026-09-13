package connect

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/netip"
	"sync"
	"testing"
	"time"
)

// Provides a transport seam for exact request-attempt assertions.
type serialTestRoundTripper func(request *http.Request) (*http.Response, error)

func (self serialTestRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return self(request)
}

type serialTestCloseRecorder struct {
	closeCount int
}

func (self *serialTestCloseRecorder) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (self *serialTestCloseRecorder) Close() error {
	self.closeCount += 1
	return nil
}

type serialTestReadErrorCloseRecorder struct {
	err        error
	closeCount int
}

func (self *serialTestReadErrorCloseRecorder) Read([]byte) (int, error) {
	return 0, self.err
}

func (self *serialTestReadErrorCloseRecorder) Close() error {
	self.closeCount += 1
	return nil
}

type serialTestBlockingCloseBody struct {
	closeEntered chan struct{}
	releaseClose chan struct{}
}

func (self *serialTestBlockingCloseBody) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (self *serialTestBlockingCloseBody) Close() error {
	close(self.closeEntered)
	<-self.releaseClose
	return nil
}

type serialTestCanceledReadBlockingCloseBody struct {
	ctx          context.Context
	readEntered  chan struct{}
	closeEntered chan struct{}
	releaseClose chan struct{}
}

func (self *serialTestCanceledReadBlockingCloseBody) Read([]byte) (int, error) {
	close(self.readEntered)
	<-self.ctx.Done()
	return 0, self.ctx.Err()
}

func (self *serialTestCanceledReadBlockingCloseBody) Close() error {
	close(self.closeEntered)
	<-self.releaseClose
	return nil
}

func newParallelEvalCancellationTestStrategy() (
	*ClientStrategy,
	*clientDialer,
	context.CancelFunc,
) {
	strategyCtx, strategyCancel := context.WithCancel(context.Background())
	settings := DefaultClientStrategySettings()
	settings.RequestTimeout = 30 * time.Minute
	settings.ParallelBlockSize = 2
	winnerDialer := &clientDialer{
		description:   "winner",
		minimumWeight: 1,
		settings:      settings,
	}
	loserDialer := &clientDialer{
		description:   "loser",
		minimumWeight: 1,
		settings:      settings,
	}
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
	return strategy, winnerDialer, strategyCancel
}

func parallelEvalWithSuccessfulHttpLoser(
	strategy *ClientStrategy,
	winnerDialer *clientDialer,
	loserBody io.ReadCloser,
) *evalResult {
	releaseLoser := make(chan struct{})
	loserEvalReturned := make(chan struct{})
	return strategy.parallelEval(context.Background(), false, func(
		_ context.Context,
		dialer *clientDialer,
	) *evalResult {
		if dialer == winnerDialer {
			return &evalResult{
				materialize: func() error {
					close(releaseLoser)
					<-loserEvalReturned
					return nil
				},
			}
		}

		<-releaseLoser
		defer close(loserEvalReturned)
		return &evalResult{
			httpResult: httpResult{
				response: &http.Response{
					ProtoMajor: 2,
					Body:       loserBody,
				},
			},
		}
	})
}

// No outcome is not a successful outcome. Treating every cold dialer as a
// previous success serializes first-use discovery and lets one black hole hide
// all of the healthy paths that should be probed in parallel.
func TestClientDialerWithoutOutcomeIsNotLastSuccess(t *testing.T) {
	dialer := &clientDialer{}
	if dialer.IsLastSuccess() {
		t.Fatal("new dialer was classified as a previously successful route")
	}
}

// Discovery cache maintenance retains a route whose latest outcome worked and
// expires only a failed route whose error has aged past the configured window.
func TestCollapseExtenderDialersRetainsSuccessAndExpiresFailure(t *testing.T) {
	settings := DefaultClientStrategySettings()
	settings.ExtenderDropTimeout = time.Minute
	now := time.Now()
	healthyDialer := &clientDialer{
		extenderConfig:  &ExtenderConfig{},
		successCount:    1,
		lastSuccessTime: now,
		settings:        settings,
	}
	expiredFailedDialer := &clientDialer{
		extenderConfig:  &ExtenderConfig{},
		successCount:    1,
		errorCount:      1,
		lastSuccessTime: now.Add(-2 * time.Minute),
		lastErrorTime:   now.Add(-time.Minute - time.Second),
		settings:        settings,
	}
	recentFailedDialer := &clientDialer{
		extenderConfig: &ExtenderConfig{},
		errorCount:     1,
		lastErrorTime:  now,
		settings:       settings,
	}
	strategy := &ClientStrategy{
		settings: settings,
		dialers: map[*clientDialer]bool{
			healthyDialer:       true,
			expiredFailedDialer: true,
			recentFailedDialer:  true,
		},
	}

	strategy.collapseExtenderDialers()

	if !strategy.dialers[healthyDialer] {
		t.Fatal("healthy discovered extender was removed")
	}
	if strategy.dialers[expiredFailedDialer] {
		t.Fatal("expired failed extender was retained")
	}
	if !strategy.dialers[recentFailedDialer] {
		t.Fatal("recently failed extender was removed before its drop timeout")
	}
}

// A route that worked previously can become a black hole. Its next POST must
// not inherit the whole request deadline, because doing so prevents every
// other proven route from being attempted.
func TestSerialEvalReservesRequestBudgetFromStalePreferredDialer(t *testing.T) {
	strategyCtx, strategyCancel := context.WithCancel(context.Background())
	defer strategyCancel()

	settings := DefaultClientStrategySettings()
	settings.RequestTimeout = 30 * time.Minute
	now := time.Now()
	staleDialer := &clientDialer{
		description:     "stale",
		minimumWeight:   1,
		priority:        0,
		successCount:    1,
		lastSuccessTime: now,
		settings:        settings,
	}
	healthyDialer := &clientDialer{
		description:     "healthy",
		minimumWeight:   1,
		priority:        1,
		successCount:    1,
		lastSuccessTime: now,
		settings:        settings,
	}
	strategy := &ClientStrategy{
		ctx:               strategyCtx,
		log:               loggerOrDefault(nil),
		settings:          settings,
		dialers:           map[*clientDialer]bool{staleDialer: true, healthyDialer: true},
		extenderIpSecrets: map[netip.Addr]string{},
	}

	var staleAttemptBudget time.Duration
	healthyAttempted := false
	eval := func(evalCtx context.Context, dialer *clientDialer) *evalResult {
		if dialer == staleDialer {
			deadline, ok := evalCtx.Deadline()
			if !ok {
				t.Fatal("stale route evaluation has no deadline")
			}
			staleAttemptBudget = time.Until(deadline)
			return &evalResult{err: errors.New("stale route")}
		}
		healthyAttempted = true
		return &evalResult{}
	}
	helloEval := func(context.Context, *clientDialer) *evalResult {
		t.Fatal("healthy proven route should avoid the hello fallback")
		return nil
	}

	result := strategy.serialEval(context.Background(), eval, helloEval)
	if result == nil || result.err != nil {
		t.Fatalf("healthy fallback result = %#v", result)
	}
	if !healthyAttempted {
		t.Fatal("healthy fallback route was not attempted")
	}
	maximumAttemptBudget := settings.RequestTimeout/3 + time.Second
	if maximumAttemptBudget < staleAttemptBudget {
		t.Fatalf(
			"stale preferred route received %s of a %s request budget",
			staleAttemptBudget,
			settings.RequestTimeout,
		)
	}
}

// Preferred GET/WebSocket routes use the same synchronous fast path before
// the parallel block. It needs the same deadline reservation or one stale
// route can prevent the parallel candidates from ever starting.
func TestParallelEvalReservesRequestBudgetFromStalePreferredDialer(t *testing.T) {
	strategyCtx, strategyCancel := context.WithCancel(context.Background())
	defer strategyCancel()

	settings := DefaultClientStrategySettings()
	settings.RequestTimeout = 30 * time.Minute
	now := time.Now()
	staleDialer := &clientDialer{
		description:     "stale",
		minimumWeight:   1,
		priority:        0,
		successCount:    1,
		lastSuccessTime: now,
		settings:        settings,
	}
	healthyDialer := &clientDialer{
		description:     "healthy",
		minimumWeight:   1,
		priority:        1,
		successCount:    1,
		lastSuccessTime: now,
		settings:        settings,
	}
	strategy := &ClientStrategy{
		ctx:               strategyCtx,
		log:               loggerOrDefault(nil),
		settings:          settings,
		dialers:           map[*clientDialer]bool{staleDialer: true, healthyDialer: true},
		extenderIpSecrets: map[netip.Addr]string{},
	}

	var staleAttemptBudget time.Duration
	eval := func(evalCtx context.Context, dialer *clientDialer) *evalResult {
		if dialer == staleDialer {
			deadline, ok := evalCtx.Deadline()
			if !ok {
				t.Fatal("stale route evaluation has no deadline")
			}
			staleAttemptBudget = time.Until(deadline)
			return &evalResult{err: errors.New("stale route")}
		}
		return &evalResult{}
	}

	result := strategy.parallelEval(context.Background(), false, eval)
	if result == nil || result.err != nil {
		t.Fatalf("healthy fallback result = %#v", result)
	}
	if result.dialer != healthyDialer {
		t.Fatalf("selected dialer = %v, expected healthy route", result.dialer)
	}
	maximumAttemptBudget := settings.RequestTimeout/3 + time.Second
	if maximumAttemptBudget < staleAttemptBudget {
		t.Fatalf(
			"stale preferred route received %s of a %s request budget",
			staleAttemptBudget,
			settings.RequestTimeout,
		)
	}
}

// Cancellation asks every parallel attempt to stop, but completion remains
// owned until each attempt stack has actually returned. Otherwise a caller can
// release the strategy or its transport while a dial still uses their state.
func TestParallelEvalCancellationJoinsAttemptWorker(t *testing.T) {
	testCtx, testCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer testCancel()

	strategyCtx, strategyCancel := context.WithCancel(context.Background())
	defer strategyCancel()
	settings := DefaultClientStrategySettings()
	settings.RequestTimeout = 30 * time.Minute
	settings.ParallelBlockSize = 1
	dialer := &clientDialer{
		description:   "blocked",
		minimumWeight: 1,
		settings:      settings,
	}
	strategy := &ClientStrategy{
		ctx:               strategyCtx,
		log:               loggerOrDefault(nil),
		settings:          settings,
		dialers:           map[*clientDialer]bool{dialer: true},
		extenderIpSecrets: map[netip.Addr]string{},
	}

	attemptEntered := make(chan struct{})
	attemptCanceled := make(chan struct{})
	releaseAttempt := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(releaseAttempt)
		})
	}
	t.Cleanup(release)

	requestCtx, requestCancel := context.WithCancel(context.Background())
	defer requestCancel()
	result := make(chan *evalResult, 1)
	go func() {
		result <- strategy.parallelEval(requestCtx, false, func(evalCtx context.Context, _ *clientDialer) *evalResult {
			close(attemptEntered)
			<-evalCtx.Done()
			close(attemptCanceled)
			<-releaseAttempt
			return &evalResult{err: evalCtx.Err()}
		})
	}()

	select {
	case <-testCtx.Done():
		t.Fatalf("wait for parallel attempt: %v", testCtx.Err())
	case <-attemptEntered:
	}
	requestCancel()
	select {
	case <-testCtx.Done():
		t.Fatalf("wait for parallel attempt cancellation: %v", testCtx.Err())
	case <-attemptCanceled:
	}
	select {
	case <-result:
		t.Fatal("parallel evaluation returned before its canceled attempt unwound")
	default:
	}

	release()
	select {
	case <-testCtx.Done():
		t.Fatalf("join parallel attempt: %v", testCtx.Err())
	case parallelResult := <-result:
		if parallelResult != nil {
			t.Fatalf("canceled parallel evaluation result = %#v", parallelResult)
		}
	}
}

// A successful parallel HTTP response can lose the selection race after its
// RoundTrip has returned. Canceling the shared request context already owns
// transport cleanup; synchronously closing that loser's body can wedge the
// worker join (HTTP/2 Body.Close may wait indefinitely on its write mutex).
func TestParallelEvalCancellationDoesNotCloseHttpLoser(t *testing.T) {
	strategy, winnerDialer, strategyCancel := newParallelEvalCancellationTestStrategy()
	defer strategyCancel()

	loserBody := &serialTestCloseRecorder{}
	result := parallelEvalWithSuccessfulHttpLoser(strategy, winnerDialer, loserBody)

	if result == nil || result.dialer != winnerDialer {
		t.Fatalf("selected result = %#v, expected winner", result)
	}
	if loserBody.closeCount != 0 {
		t.Fatalf("canceled HTTP loser body was synchronously closed %d time(s)", loserBody.closeCount)
	}
}

// Cancellation-specific disposal must not weaken ordinary result ownership.
// Before a request context is canceled, an unselected/failed HTTP response is
// still closed explicitly by the code that rejects it.
func TestEvalResultCloseStillClosesHttpResponse(t *testing.T) {
	body := &serialTestCloseRecorder{}
	result := &evalResult{
		httpResult: httpResult{
			response: &http.Response{Body: body},
		},
	}

	result.Close()

	if body.closeCount != 1 {
		t.Fatalf("ordinary HTTP response body close count = %d, expected 1", body.closeCount)
	}
}

// Reading a selected response to EOF transfers cleanup to the final result
// owner. Moving Close out of materialization must still close that response
// exactly once before HttpSerial or HttpParallel returns it to the caller.
func TestMaterializedHttpResultClosesSuccessfulResponse(t *testing.T) {
	body := &serialTestCloseRecorder{}
	response := &http.Response{Body: body}
	result := newEvalResultFromHttpResponse(response, nil, DefaultMaxHttpResponseBodyBytes)

	result.Selected()
	if result.err != nil {
		t.Fatalf("materialize successful response: %v", result.err)
	}
	if body.closeCount != 0 {
		t.Fatalf("response closed during materialization %d time(s)", body.closeCount)
	}
	_, err := materializeHttpResult(result)
	if err != nil {
		t.Fatal(err)
	}
	if body.closeCount != 1 {
		t.Fatalf("successful response body close count = %d, expected 1", body.closeCount)
	}
	if response.Body != nil {
		t.Fatal("closed successful response retained its body")
	}
}

// A body failure unrelated to cancellation retains ordinary caller ownership.
// It must not be discarded merely because canceled failures use transport
// cleanup instead of a potentially blocking synchronous Close.
func TestRejectedHttpResultClosesNonContextReadError(t *testing.T) {
	readErr := errors.New("body checksum failed")
	body := &serialTestReadErrorCloseRecorder{err: readErr}
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.invalid", nil)
	if err != nil {
		t.Fatal(err)
	}
	response := &http.Response{Body: body, Request: request}
	result := newEvalResultFromHttpResponse(response, nil, DefaultMaxHttpResponseBodyBytes)

	result.Selected()
	if !errors.Is(result.err, readErr) {
		t.Fatalf("materialize error = %v, expected %v", result.err, readErr)
	}
	result.releaseAfterUse(context.Background())

	if body.closeCount != 1 {
		t.Fatalf("non-context error response close count = %d, expected 1", body.closeCount)
	}
	if response.Body != nil {
		t.Fatal("closed failed response retained its body")
	}
}

// The synchronous-close bug is a liveness failure, not just an ownership
// detail: the worker join cannot return while a loser's Body.Close is wedged.
// Explicit barriers make the two possible outcomes deterministic without a
// scheduler delay: either parallelEval returns, or it entered the bad close.
func TestParallelEvalReturnsWhenCanceledHttpLoserCloseWouldBlock(t *testing.T) {
	testCtx, testCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer testCancel()

	strategy, winnerDialer, strategyCancel := newParallelEvalCancellationTestStrategy()
	defer strategyCancel()
	releaseClose := make(chan struct{})
	var releaseCloseOnce sync.Once
	releaseBlockedClose := func() {
		releaseCloseOnce.Do(func() {
			close(releaseClose)
		})
	}
	t.Cleanup(releaseBlockedClose)
	loserBody := &serialTestBlockingCloseBody{
		closeEntered: make(chan struct{}),
		releaseClose: releaseClose,
	}
	resultCh := make(chan *evalResult, 1)
	go func() {
		resultCh <- parallelEvalWithSuccessfulHttpLoser(strategy, winnerDialer, loserBody)
	}()

	select {
	case result := <-resultCh:
		if result == nil || result.dialer != winnerDialer {
			t.Fatalf("selected result = %#v, expected winner", result)
		}
	case <-loserBody.closeEntered:
		releaseBlockedClose()
		<-resultCh
		t.Fatal("parallel evaluation synchronously closed the canceled HTTP loser")
	case <-testCtx.Done():
		t.Fatalf("parallel evaluation made no completion decision: %v", testCtx.Err())
	}
}

// A cold serial request first discovers a route with a parallel hello. The
// nested evaluation cancels the hello's request context before returning its
// selected response, so closing that response synchronously can wedge startup
// before the real request is ever attempted. Explicit channels distinguish a
// returned serial result from entry into the forbidden close without timing
// or scheduler luck.
func TestSerialEvalColdHelloReturnsWithoutCanceledResponseClose(t *testing.T) {
	testCtx, testCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer testCancel()

	strategyCtx, strategyCancel := context.WithCancel(context.Background())
	defer strategyCancel()
	settings := DefaultClientStrategySettings()
	settings.RequestTimeout = 30 * time.Minute
	settings.ParallelBlockSize = 1
	dialer := &clientDialer{
		description:   "cold",
		minimumWeight: 1,
		settings:      settings,
	}
	strategy := &ClientStrategy{
		ctx:      strategyCtx,
		log:      loggerOrDefault(nil),
		settings: settings,
		dialers: map[*clientDialer]bool{
			dialer: true,
		},
		extenderIpSecrets: map[netip.Addr]string{},
	}

	releaseClose := make(chan struct{})
	var releaseCloseOnce sync.Once
	releaseBlockedClose := func() {
		releaseCloseOnce.Do(func() {
			close(releaseClose)
		})
	}
	t.Cleanup(releaseBlockedClose)
	helloBody := &serialTestBlockingCloseBody{
		closeEntered: make(chan struct{}),
		releaseClose: releaseClose,
	}
	requestAttempted := make(chan struct{})
	resultCh := make(chan *evalResult, 1)
	go func() {
		resultCh <- strategy.serialEval(
			context.Background(),
			func(context.Context, *clientDialer) *evalResult {
				close(requestAttempted)
				return &evalResult{}
			},
			func(evalCtx context.Context, helloDialer *clientDialer) *evalResult {
				request, err := http.NewRequestWithContext(
					evalCtx,
					http.MethodGet,
					"https://example.invalid/hello",
					nil,
				)
				if err != nil {
					return &evalResult{err: err}
				}
				helloDialer.Update(evalCtx, nil)
				return newEvalResultFromHttpResponse(&http.Response{
					StatusCode: http.StatusOK,
					Body:       helloBody,
					Request:    request,
				}, nil, DefaultMaxHttpResponseBodyBytes)
			},
		)
	}()

	select {
	case result := <-resultCh:
		if result == nil || result.dialer != dialer || result.err != nil {
			t.Fatalf("serial result = %#v, expected discovered route", result)
		}
	case <-helloBody.closeEntered:
		releaseBlockedClose()
		<-resultCh
		t.Fatal("serial evaluation synchronously closed the canceled hello response")
	case <-testCtx.Done():
		t.Fatalf("serial evaluation made no completion decision: %v", testCtx.Err())
	}
	select {
	case <-requestAttempted:
	default:
		t.Fatal("serial evaluation never attempted the request after route discovery")
	}
	select {
	case <-helloBody.closeEntered:
		t.Fatal("serial evaluation closed the canceled hello response")
	default:
	}
}

// HttpSerial and HttpParallel return only a materialized body, after their
// evaluation has canceled the selected attempt context. Final result release
// must honor that transport-ownership boundary just like cold-hello release.
func TestMaterializedHttpResultReturnsWithoutCanceledResponseClose(t *testing.T) {
	testCtx, testCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer testCancel()

	requestCtx, requestCancel := context.WithCancel(context.Background())
	request, err := http.NewRequestWithContext(
		requestCtx,
		http.MethodGet,
		"https://example.invalid",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	releaseClose := make(chan struct{})
	var releaseCloseOnce sync.Once
	releaseBlockedClose := func() {
		releaseCloseOnce.Do(func() {
			close(releaseClose)
		})
	}
	t.Cleanup(releaseBlockedClose)
	body := &serialTestBlockingCloseBody{
		closeEntered: make(chan struct{}),
		releaseClose: releaseClose,
	}
	result := newEvalResultFromHttpResponse(&http.Response{
		StatusCode: http.StatusOK,
		Body:       body,
		Request:    request,
	}, nil, DefaultMaxHttpResponseBodyBytes)
	result.Selected()
	if result.err != nil {
		t.Fatal(result.err)
	}
	requestCancel()

	resultCh := make(chan error, 1)
	go func() {
		_, resultErr := materializeHttpResult(result)
		resultCh <- resultErr
	}()
	select {
	case resultErr := <-resultCh:
		if resultErr != nil {
			t.Fatal(resultErr)
		}
	case <-body.closeEntered:
		releaseBlockedClose()
		<-resultCh
		t.Fatal("materialized result synchronously closed its canceled response")
	case <-testCtx.Done():
		t.Fatalf("materialized result made no completion decision: %v", testCtx.Err())
	}
	select {
	case <-body.closeEntered:
		t.Fatal("materialized result closed its canceled response")
	default:
	}
}

// A preferred route can return response headers before its body stalls. When
// the response request expires, its body read returns the context error while
// the parent strategy stays live. The selection path must try the next route
// even if the canceled HTTP/2 body would block forever in Close.
func TestSerialEvalCanceledPreferredBodyFallsBackWithoutClose(t *testing.T) {
	testCtx, testCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer testCancel()

	strategyCtx, strategyCancel := context.WithCancel(context.Background())
	defer strategyCancel()
	settings := DefaultClientStrategySettings()
	settings.RequestTimeout = 30 * time.Minute
	preferredDialer := &clientDialer{
		description:     "preferred",
		minimumWeight:   1,
		priority:        0,
		successCount:    1,
		lastSuccessTime: time.Now(),
		settings:        settings,
	}
	healthyDialer := &clientDialer{
		description:     "healthy",
		minimumWeight:   1,
		priority:        1,
		successCount:    1,
		lastSuccessTime: time.Now(),
		settings:        settings,
	}
	strategy := &ClientStrategy{
		ctx:      strategyCtx,
		log:      loggerOrDefault(nil),
		settings: settings,
		dialers: map[*clientDialer]bool{
			preferredDialer: true,
			healthyDialer:   true,
		},
		extenderIpSecrets: map[netip.Addr]string{},
	}

	readEntered := make(chan struct{})
	closeEntered := make(chan struct{})
	releaseClose := make(chan struct{})
	var releaseCloseOnce sync.Once
	releaseBlockedClose := func() {
		releaseCloseOnce.Do(func() {
			close(releaseClose)
		})
	}
	t.Cleanup(releaseBlockedClose)

	requestCtx, requestCancel := context.WithCancel(context.Background())
	defer requestCancel()
	type cancelableResponse struct {
		response *http.Response
		cancel   context.CancelFunc
	}
	responseCh := make(chan cancelableResponse, 1)
	healthyBody := &serialTestCloseRecorder{}
	resultCh := make(chan *evalResult, 1)
	go func() {
		resultCh <- strategy.serialEval(
			requestCtx,
			func(evalCtx context.Context, dialer *clientDialer) *evalResult {
				if dialer == healthyDialer {
					request, err := http.NewRequestWithContext(
						evalCtx,
						http.MethodPost,
						"https://example.invalid",
						nil,
					)
					if err != nil {
						return &evalResult{err: err}
					}
					return newEvalResultFromHttpResponse(&http.Response{
						StatusCode: http.StatusOK,
						Body:       healthyBody,
						Request:    request,
					}, nil, DefaultMaxHttpResponseBodyBytes)
				}

				responseCtx, responseCancel := context.WithCancel(evalCtx)
				request, err := http.NewRequestWithContext(
					responseCtx,
					http.MethodPost,
					"https://example.invalid",
					nil,
				)
				if err != nil {
					responseCancel()
					return &evalResult{err: err}
				}
				response := &http.Response{
					StatusCode: http.StatusOK,
					Body: &serialTestCanceledReadBlockingCloseBody{
						ctx:          responseCtx,
						readEntered:  readEntered,
						closeEntered: closeEntered,
						releaseClose: releaseClose,
					},
					Request: request,
				}
				responseCh <- cancelableResponse{response: response, cancel: responseCancel}
				return newEvalResultFromHttpResponse(response, nil, DefaultMaxHttpResponseBodyBytes)
			},
			func(context.Context, *clientDialer) *evalResult {
				return &evalResult{err: errors.New("unexpected hello fallback")}
			},
		)
	}()

	var first cancelableResponse
	select {
	case <-testCtx.Done():
		t.Fatalf("wait for preferred response: %v", testCtx.Err())
	case first = <-responseCh:
	}
	defer first.cancel()
	select {
	case <-testCtx.Done():
		t.Fatalf("wait for selected response body read: %v", testCtx.Err())
	case <-readEntered:
	}
	first.cancel()

	select {
	case result := <-resultCh:
		if result == nil || result.dialer != healthyDialer || result.err != nil {
			t.Fatalf("fallback result = %#v, expected healthy route", result)
		}
		result.Close()
	case <-closeEntered:
		releaseBlockedClose()
		select {
		case result := <-resultCh:
			if result != nil {
				result.Close()
			}
		case <-testCtx.Done():
			t.Fatalf("release blocked response close: %v", testCtx.Err())
		}
		t.Fatal("serial evaluation synchronously closed a context-canceled response body")
	case <-testCtx.Done():
		t.Fatalf("serial evaluation made no completion decision: %v", testCtx.Err())
	}
	if strategyCtx.Err() != nil || requestCtx.Err() != nil {
		t.Fatal("parent strategy context was canceled during route fallback")
	}
	if first.response.Request.Context().Err() == nil {
		t.Fatal("selected response request context was not canceled")
	}
	if first.response.Body != nil {
		t.Fatal("canceled response retained a body owned by the transport")
	}
	if healthyBody.closeCount != 1 {
		t.Fatalf("healthy response close count = %d, expected 1", healthyBody.closeCount)
	}
	select {
	case <-closeEntered:
		t.Fatal("serial evaluation closed the canceled response body")
	default:
	}
}

// A failed route may consume the complete POST body before another proven
// route is tried. Every route attempt must receive a fresh body reader.
func TestHttpSerialReplaysCompletePostBodyAfterRouteFailure(t *testing.T) {
	strategyCtx, strategyCancel := context.WithCancel(context.Background())
	defer strategyCancel()

	settings := DefaultClientStrategySettings()
	settings.RequestTimeout = time.Second
	now := time.Now()
	requestBodyBytes := []byte(`{"user_auth":"acceptance@example.invalid"}`)
	var firstBodyBytes []byte
	var secondBodyBytes []byte

	failedDialer := &clientDialer{
		description:     "failed",
		minimumWeight:   1,
		priority:        0,
		successCount:    1,
		lastSuccessTime: now,
		settings:        settings,
		httpClient: &http.Client{Transport: serialTestRoundTripper(func(request *http.Request) (*http.Response, error) {
			var err error
			firstBodyBytes, err = io.ReadAll(request.Body)
			if err != nil {
				return nil, err
			}
			return nil, errors.New("route failed after consuming request body")
		})},
	}
	healthyDialer := &clientDialer{
		description:     "healthy",
		minimumWeight:   1,
		priority:        1,
		successCount:    1,
		lastSuccessTime: now,
		settings:        settings,
		httpClient: &http.Client{Transport: serialTestRoundTripper(func(request *http.Request) (*http.Response, error) {
			var err error
			secondBodyBytes, err = io.ReadAll(request.Body)
			if err != nil {
				return nil, err
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Header:     http.Header{},
				Body:       io.NopCloser(bytes.NewReader([]byte(`{}`))),
				Request:    request,
			}, nil
		})},
	}
	strategy := &ClientStrategy{
		ctx:               strategyCtx,
		log:               loggerOrDefault(nil),
		settings:          settings,
		dialers:           map[*clientDialer]bool{failedDialer: true, healthyDialer: true},
		extenderIpSecrets: map[netip.Addr]string{},
	}

	request, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		"https://api.example.invalid/auth/login",
		bytes.NewReader(requestBodyBytes),
	)
	if err != nil {
		t.Fatal(err)
	}
	helloRequest, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		"https://api.example.invalid/hello",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	result, err := strategy.HttpSerial(request, helloRequest)
	if err != nil {
		t.Fatalf("serial POST failed: %v", err)
	}
	if result.response.StatusCode != http.StatusOK {
		t.Fatalf("serial POST status = %s", result.response.Status)
	}
	if !bytes.Equal(firstBodyBytes, requestBodyBytes) {
		t.Fatalf("first route body = %q, want %q", firstBodyBytes, requestBodyBytes)
	}
	if !bytes.Equal(secondBodyBytes, requestBodyBytes) {
		t.Fatalf("fallback route body = %q, want %q", secondBodyBytes, requestBodyBytes)
	}
}

// Parallel evaluation has the same preferred-route fast path and must not
// share one consumed body between its attempts either.
func TestHttpParallelReplaysCompleteBodyAfterRouteFailure(t *testing.T) {
	strategyCtx, strategyCancel := context.WithCancel(context.Background())
	defer strategyCancel()

	settings := DefaultClientStrategySettings()
	settings.RequestTimeout = time.Second
	now := time.Now()
	requestBodyBytes := []byte(`{"request":"complete"}`)
	var firstBodyBytes []byte
	var secondBodyBytes []byte

	failedDialer := &clientDialer{
		description:     "failed",
		minimumWeight:   1,
		priority:        0,
		successCount:    1,
		lastSuccessTime: now,
		settings:        settings,
		httpClient: &http.Client{Transport: serialTestRoundTripper(func(request *http.Request) (*http.Response, error) {
			var err error
			firstBodyBytes, err = io.ReadAll(request.Body)
			if err != nil {
				return nil, err
			}
			return nil, errors.New("route failed after consuming request body")
		})},
	}
	healthyDialer := &clientDialer{
		description:     "healthy",
		minimumWeight:   1,
		priority:        1,
		successCount:    1,
		lastSuccessTime: now,
		settings:        settings,
		httpClient: &http.Client{Transport: serialTestRoundTripper(func(request *http.Request) (*http.Response, error) {
			var err error
			secondBodyBytes, err = io.ReadAll(request.Body)
			if err != nil {
				return nil, err
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Header:     http.Header{},
				Body:       io.NopCloser(bytes.NewReader([]byte(`{}`))),
				Request:    request,
			}, nil
		})},
	}
	strategy := &ClientStrategy{
		ctx:               strategyCtx,
		log:               loggerOrDefault(nil),
		settings:          settings,
		dialers:           map[*clientDialer]bool{failedDialer: true, healthyDialer: true},
		extenderIpSecrets: map[netip.Addr]string{},
	}

	request, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		"https://api.example.invalid/auth/login",
		bytes.NewReader(requestBodyBytes),
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := strategy.HttpParallel(request)
	if err != nil {
		t.Fatalf("parallel request failed: %v", err)
	}
	if result.response.StatusCode != http.StatusOK {
		t.Fatalf("parallel request status = %s", result.response.Status)
	}
	if !bytes.Equal(firstBodyBytes, requestBodyBytes) {
		t.Fatalf("first route body = %q, want %q", firstBodyBytes, requestBodyBytes)
	}
	if !bytes.Equal(secondBodyBytes, requestBodyBytes) {
		t.Fatalf("fallback route body = %q, want %q", secondBodyBytes, requestBodyBytes)
	}
}

// A one-shot body cannot safely participate in multi-route evaluation. Reject
// it before the first route sees bytes rather than emitting a later empty POST.
func TestHttpSerialRejectsNonReplayableBodyBeforeDial(t *testing.T) {
	strategyCtx, strategyCancel := context.WithCancel(context.Background())
	defer strategyCancel()

	settings := DefaultClientStrategySettings()
	dialCount := 0
	dialer := &clientDialer{
		minimumWeight:   1,
		successCount:    1,
		lastSuccessTime: time.Now(),
		settings:        settings,
		httpClient: &http.Client{Transport: serialTestRoundTripper(func(request *http.Request) (*http.Response, error) {
			dialCount += 1
			return nil, errors.New("unexpected dial")
		})},
	}
	strategy := &ClientStrategy{
		ctx:               strategyCtx,
		log:               loggerOrDefault(nil),
		settings:          settings,
		dialers:           map[*clientDialer]bool{dialer: true},
		extenderIpSecrets: map[netip.Addr]string{},
	}
	request, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		"https://api.example.invalid/auth/login",
		io.NopCloser(bytes.NewReader([]byte(`{}`))),
	)
	if err != nil {
		t.Fatal(err)
	}
	helloRequest, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		"https://api.example.invalid/hello",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = strategy.HttpSerial(request, helloRequest)
	if err == nil || err.Error() != "http request body is not replayable" {
		t.Fatalf("non-replayable body error = %v", err)
	}
	if dialCount != 0 {
		t.Fatalf("non-replayable request reached %d route(s)", dialCount)
	}
}
