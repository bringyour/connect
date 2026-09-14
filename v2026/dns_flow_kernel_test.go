package connect

import (
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"

	"context"
)

// TestDnsFlowKernel is the measurement kernel for the dns flow (rtt +
// parallelism) auto-research loop. Env-gated so normal suites skip it; each
// invocation measures one configuration.
//
// Topology: a DohCache (the same resolver the device mux runs) over M local
// h2 doh servers with injectable per-request latency (simulated upstream
// rtt) and optional blackhole mode (accept, never answer — the dead-server
// tail). A burst of distinct names (defeating the answer cache and
// single-flight) measures per-query latency distribution, total wall time,
// and effective parallelism sampled live off the dns MemoryTarget
// (used / dohQueryReserveByteCount).
//
// Env knobs:
//
//	URNET_DNS_SERVERS      per-server injected latency ms, comma list ("5,50")
//	URNET_DNS_DEAD         comma list of server indices that blackhole
//	URNET_DNS_QUERIES      distinct names in the burst (default 64)
//	URNET_DNS_TARGET_KB    dns MemoryTarget capacity (default 2048 = the
//	                       2 MB device dns share; 0 = unlimited)
//	URNET_DNS_STAGGER_MS   DohServerStagger (default 750)
//	URNET_DNS_MAXSERVERS   MaxServersPerQuery (default 2, the mux value)
//	URNET_DNS_TIMEOUT_MS   per-request timeout (default 15000)
//	URNET_DNS_WARM         1 = pre-open all server connections (default 1)
//	URNET_DNS_PIPE_SLOTS   shared response-pipe parallelism (0 = unlimited)
//	URNET_DNS_PIPE_MS      service time held in the shared pipe per response
//	                       (0 disables the shared-pipe model)
//	URNET_DNS_SEED_FIRST   score used to stale-seed server 0 (0 = none)
//	URNET_DNS_PATH_WARM    1 = apply STAGGER_MS as the warm override over a
//	                       750ms cold stagger (and enable hedge reservation)
//	URNET_DNS_HEDGE_RESERVE HTTP slots reserved for timed hedges (default 4)
func TestDnsFlowKernel(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		if os.Getenv("URNET_DNSFLOW") == "" {
			t.Skip("dns flow kernel: set URNET_DNSFLOW=1")
		}

		serverLatencies := []time.Duration{}
		for _, part := range strings.Split(dnsFlowEnv("URNET_DNS_SERVERS", "5,50"), ",") {
			ms, err := strconv.Atoi(strings.TrimSpace(part))
			if err != nil {
				t.Fatalf("bad URNET_DNS_SERVERS: %v", err)
			}
			serverLatencies = append(serverLatencies, time.Duration(ms)*time.Millisecond)
		}
		dead := map[int]bool{}
		if deadList := dnsFlowEnv("URNET_DNS_DEAD", ""); deadList != "" {
			for _, part := range strings.Split(deadList, ",") {
				i, err := strconv.Atoi(strings.TrimSpace(part))
				if err != nil {
					t.Fatalf("bad URNET_DNS_DEAD: %v", err)
				}
				dead[i] = true
			}
		}
		queryCount := dnsFlowEnvInt("URNET_DNS_QUERIES", 64)
		targetByteCount := ByteCount(dnsFlowEnvInt("URNET_DNS_TARGET_KB", 2048)) * 1024
		staggerMs := dnsFlowEnvInt("URNET_DNS_STAGGER_MS", 750)
		maxServers := dnsFlowEnvInt("URNET_DNS_MAXSERVERS", 2)
		timeoutMs := dnsFlowEnvInt("URNET_DNS_TIMEOUT_MS", 15000)
		warm := dnsFlowEnvInt("URNET_DNS_WARM", 1)
		pipeSlots := dnsFlowEnvInt("URNET_DNS_PIPE_SLOTS", 0)
		pipeMs := dnsFlowEnvInt("URNET_DNS_PIPE_MS", 0)
		seedFirst := dnsFlowEnvInt("URNET_DNS_SEED_FIRST", 0)
		pathWarm := dnsFlowEnvInt("URNET_DNS_PATH_WARM", 0)
		hedgeReserve := dnsFlowEnvInt("URNET_DNS_HEDGE_RESERVE", 4)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// local h2 doh servers with injected latency / blackhole
		dohUrls := []string{}
		var tlsConfig *tls.Config
		var pipeSem chan struct{}
		var requestCount atomic.Int64
		var pipeInFlight atomic.Int64
		var pipePeak atomic.Int64
		if 0 < pipeSlots && 0 < pipeMs {
			pipeSem = make(chan struct{}, pipeSlots)
		}
		for i, latency := range serverLatencies {
			blackhole := dead[i]
			serverLatency := latency
			server := newFamilyHttptestUnstartedServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requestCount.Add(1)
				if blackhole {
					// accept and hold until the client gives up
					<-r.Context().Done()
					return
				}
				if 0 < serverLatency {
					select {
					case <-time.After(serverLatency):
					case <-r.Context().Done():
						return
					}
				}
				// Every server connection ultimately traverses the same tunnel.
				// Model its finite response service capacity separately from each
				// server's upstream latency: hedges multiply work on this shared
				// stage, while a blackholed server consumes no response bandwidth.
				if pipeSem != nil {
					select {
					case pipeSem <- struct{}{}:
						inFlight := pipeInFlight.Add(1)
						for peak := pipePeak.Load(); peak < inFlight && !pipePeak.CompareAndSwap(peak, inFlight); peak = pipePeak.Load() {
						}
						defer func() {
							pipeInFlight.Add(-1)
							<-pipeSem
						}()
					case <-r.Context().Done():
						return
					}
					select {
					case <-time.After(time.Duration(pipeMs) * time.Millisecond):
					case <-r.Context().Done():
						return
					}
				}
				wire, err := base64.RawURLEncoding.DecodeString(r.URL.Query().Get("dns"))
				if err != nil {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				var msg dnsmessage.Message
				if err := msg.Unpack(wire); err != nil {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				msg.Response = true
				msg.Authoritative = true
				// dual stack: answer A and AAAA so the kernel exercises both
				// record types (IPV6.md D3)
				if len(msg.Questions) == 1 {
					header := dnsmessage.ResourceHeader{
						Name:  msg.Questions[0].Name,
						Type:  msg.Questions[0].Type,
						Class: dnsmessage.ClassINET,
						TTL:   60,
					}
					switch msg.Questions[0].Type {
					case dnsmessage.TypeA:
						msg.Answers = []dnsmessage.Resource{{
							Header: header,
							Body:   &dnsmessage.AResource{A: [4]byte{127, 0, 0, 1}},
						}}
					case dnsmessage.TypeAAAA:
						msg.Answers = []dnsmessage.Resource{{
							Header: header,
							Body:   &dnsmessage.AAAAResource{AAAA: [16]byte{15: 1}},
						}}
					}
				}
				out, err := msg.Pack()
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				w.Header().Set("Content-Type", "application/dns-message")
				w.Write(out)
			}))
			server.EnableHTTP2 = true
			server.StartTLS()
			defer server.Close()
			dohUrls = append(dohUrls, server.URL+"/dns-query")
			if tlsConfig == nil {
				tlsConfig = server.Client().Transport.(*http.Transport).TLSClientConfig
			}
		}

		memoryTarget := NewMemoryTarget(targetByteCount)
		settings := DefaultDohSettings()
		settings.RequestTimeout = time.Duration(timeoutMs) * time.Millisecond
		settings.MemoryTarget = memoryTarget
		settings.DohServerStagger = time.Duration(staggerMs) * time.Millisecond
		if pathWarm == 1 {
			settings.DohServerStagger = 750 * time.Millisecond
			settings.DohServerWarmStagger = time.Duration(staggerMs) * time.Millisecond
			settings.DohPathWarm = func() bool { return true }
		}
		settings.MaxServersPerQuery = maxServers
		// Zero the count caps by default so this raw DohCache kernel isolates the
		// byte-target-derived caps. The production UpgradeMux adds wave caps (32
		// HTTP / 96 resolutions at this target); set URNET_DNS_HTTPCAP=32 and
		// URNET_DNS_RESCAP=96 to model that path rather than conflating the two.
		settings.MaxConcurrentHttpRequests = dnsFlowEnvInt("URNET_DNS_HTTPCAP", 0)
		settings.MaxConcurrentResolutions = dnsFlowEnvInt("URNET_DNS_RESCAP", 0)
		settings.DohServerRaceMaxInFlight = dnsFlowEnvInt("URNET_DNS_RACEMAX", settings.DohServerRaceMaxInFlight)
		settings.DohServerHedgeReserve = hedgeReserve
		if 0 < seedFirst && 0 < len(dohUrls) {
			settings.ServerStatsSeed = map[string]float64{dohUrls[0]: float64(seedFirst)}
		}
		settings.DnsResolverSettings = remoteDohResolverSettings(ipVersion, dohUrls...)
		settings.DnsResolverSettings.TlsConfig = tlsConfig
		dohCache := NewDohCache(settings)
		defer dohCache.Close()

		if warm == 1 {
			// pre-open every server connection (tcp+tls+h2) so the measured
			// burst isolates request rtt from connection setup
			prevStagger := settings.DohServerStagger
			prevMaxServers := settings.MaxServersPerQuery
			settings.DohServerStagger = 0
			settings.MaxServersPerQuery = 0
			for i := 0; i < 2; i += 1 {
				warmCtx, warmCancel := context.WithTimeout(ctx, 10*time.Second)
				dohCache.QueryResult(warmCtx, "A", fmt.Sprintf("warm%d.dnsflow.test", i))
				warmCancel()
			}
			settings.DohServerStagger = prevStagger
			settings.MaxServersPerQuery = prevMaxServers
		}

		// sample effective parallelism off the byte budget during the burst
		var peakUsed ByteCount
		samplerStop := make(chan struct{})
		var samplerWg sync.WaitGroup
		if 0 < targetByteCount {
			samplerWg.Add(1)
			go func() {
				defer samplerWg.Done()
				for {
					if used := memoryTarget.Used(); peakUsed < used {
						peakUsed = used
					}
					select {
					case <-samplerStop:
						return
					case <-time.After(2 * time.Millisecond):
					}
				}
			}()
		}

		// the burst: all distinct names launched at once
		type queryResult struct {
			latency       time.Duration
			authoritative bool
			addrCount     int
		}
		results := make([]queryResult, queryCount)
		burstStart := time.Now()
		var burstWg sync.WaitGroup
		for i := 0; i < queryCount; i += 1 {
			burstWg.Add(1)
			go func(i int) {
				defer burstWg.Done()
				queryStart := time.Now()
				// alternate record types so the run measures A and AAAA equally
				recordType := "A"
				if i%2 == 1 {
					recordType = "AAAA"
				}
				addrs, authoritative := dohCache.QueryResult(ctx, recordType, fmt.Sprintf("q%d.dnsflow.test", i))
				results[i] = queryResult{
					latency:       time.Since(queryStart),
					authoritative: authoritative,
					addrCount:     len(addrs),
				}
			}(i)
		}
		burstWg.Wait()
		wall := time.Since(burstStart)
		close(samplerStop)
		samplerWg.Wait()

		okCount := 0
		latencies := []time.Duration{}
		for _, result := range results {
			if result.authoritative && 0 < result.addrCount {
				okCount += 1
			}
			latencies = append(latencies, result.latency)
		}
		sort.Slice(latencies, func(i int, j int) bool {
			return latencies[i] < latencies[j]
		})
		pct := func(p float64) time.Duration {
			i := int(p * float64(len(latencies)-1))
			return latencies[i]
		}
		peakConcurrency := int64(0)
		if 0 < targetByteCount {
			peakConcurrency = int64(peakUsed) / dohQueryReserveByteCount
		}

		effectiveHttpCap := maxConcurrentHttpRequests(settings)
		effectiveResolutionCap := settings.MaxConcurrentResolutions
		if effectiveResolutionCap <= 0 {
			effectiveResolutionCap = 4 * dnsTargetHttpConcurrency(settings.MemoryTarget.Capacity(), 16)
		}
		fmt.Printf("[dnsflow] servers=%s dead=%s stagger_ms=%d maxsrv=%d target_kb=%d timeout_ms=%d warm=%d path_warm=%d queries=%d http_cap=%d resolution_cap=%d race_max=%d hedge_reserve=%d pipe_slots=%d pipe_ms=%d seed_first=%d | ok=%d p50_ms=%d p95_ms=%d p99_ms=%d max_ms=%d wall_ms=%d peak_conc=%d requests=%d pipe_peak=%d\n",
			dnsFlowEnv("URNET_DNS_SERVERS", "5,50"),
			dnsFlowEnv("URNET_DNS_DEAD", ""),
			staggerMs, maxServers, int(targetByteCount/1024), timeoutMs, warm, pathWarm, queryCount,
			effectiveHttpCap, effectiveResolutionCap, settings.DohServerRaceMaxInFlight, hedgeReserve,
			pipeSlots, pipeMs, seedFirst,
			okCount,
			pct(0.50).Milliseconds(),
			pct(0.95).Milliseconds(),
			pct(0.99).Milliseconds(),
			latencies[len(latencies)-1].Milliseconds(),
			wall.Milliseconds(),
			peakConcurrency,
			requestCount.Load(),
			pipePeak.Load())

		if okCount < queryCount {
			t.Fatalf("[dnsflow] %d/%d queries failed", queryCount-okCount, queryCount)
		}
	})
}

func dnsFlowEnv(name string, defaultValue string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return defaultValue
}

func dnsFlowEnvInt(name string, defaultValue int) int {
	if value := os.Getenv(name); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			return parsed
		}
	}
	return defaultValue
}
