// flightgate-load is the on-device download workload of the rig: N parallel
// HTTP GET streams of one URL, restarted as they finish, for a fixed number
// of seconds, printing the bytes received per second. It is built for
// android/arm64 with CGO disabled and pushed to /data/local/tmp so both
// devices run the identical binary whatever tools their firmware ships.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"time"
)

func main() {
	url := flag.String("url", "http://cachefly.cachefly.net/200mb.test", "download URL")
	streams := flag.Int("streams", 4, "parallel streams")
	seconds := flag.Int("seconds", 180, "run duration")
	flag.Parse()

	dns := flag.String("dns", "1.1.1.1:53", "resolver, reached through the tunnel like the payload")
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*seconds)*time.Second)
	defer cancel()
	// Android has no /etc/resolv.conf for the pure-Go resolver, so name the
	// resolver explicitly; it is dialed through the tunnel like the payload.
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	net.DefaultResolver = &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			return dialer.DialContext(ctx, "udp", *dns)
		},
	}
	transport := &http.Transport{
		DisableCompression: true,
		ForceAttemptHTTP2:  false,
		MaxIdleConns:       *streams,
		DialContext:        dialer.DialContext,
	}
	client := &http.Client{
		Transport: transport,
		// a redirect to another host would change what is measured
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	var total atomic.Int64
	var errors atomic.Int64
	for i := 0; i < *streams; i++ {
		go func() {
			buffer := make([]byte, 64*1024)
			for ctx.Err() == nil {
				request, err := http.NewRequestWithContext(ctx, "GET", *url, nil)
				if err != nil {
					return
				}
				response, err := client.Do(request)
				if err != nil {
					if errors.Load() < 3 {
						fmt.Fprintf(os.Stderr, "error: %v\n", err)
					}
					errors.Add(1)
					time.Sleep(500 * time.Millisecond)
					continue
				}
				for {
					n, err := response.Body.Read(buffer)
					total.Add(int64(n))
					if err != nil {
						if err != io.EOF && ctx.Err() == nil {
							errors.Add(1)
						}
						break
					}
				}
				response.Body.Close()
			}
		}()
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	last := int64(0)
	start := time.Now()
	for {
		select {
		case <-ctx.Done():
			fmt.Fprintf(os.Stdout, "done total_bytes=%d errors=%d\n", total.Load(), errors.Load())
			return
		case <-ticker.C:
			now := total.Load()
			fmt.Fprintf(os.Stdout, "%d bytes_per_second=%d errors=%d\n",
				int(time.Since(start).Seconds()), now-last, errors.Load())
			last = now
		}
	}
}
