// The production defaults of the extender settings (EXTENDER.md A5, A6, A9).
// Every bound a prober can reach is a number in one place, so the numbers are
// pinned here rather than only in the tests that cross each bound.

package extender

import (
	"slices"
	"testing"
	"time"

	"github.com/urnetwork/connect"
)

// Every default bound of A5, A6 and A9 has the value the section names. A
// change here is a change to what an unconfigured extender exposes.
func TestDefaultExtenderSettingsValues(t *testing.T) {
	settings := DefaultExtenderSettings()

	cases := []struct {
		name     string
		value    any
		expected any
	}{
		{name: "ReadTimeout", value: settings.ReadTimeout, expected: 30 * time.Second},
		{name: "WriteTimeout", value: settings.WriteTimeout, expected: 30 * time.Second},
		{name: "ValidFrom", value: settings.ValidFrom, expected: 180 * 24 * time.Hour},
		{name: "ValidFor", value: settings.ValidFor, expected: 180 * 24 * time.Hour},

		{name: "HeaderTimeout", value: settings.HeaderTimeout, expected: 10 * time.Second},
		{name: "QuicIdleTimeout", value: settings.QuicIdleTimeout, expected: 30 * time.Second},
		{name: "MaxConnectionCountPerSource", value: settings.MaxConnectionCountPerSource, expected: 64},
		{name: "MaxConnectionCount", value: settings.MaxConnectionCount, expected: 4096},

		{name: "ProxyMaxRequestByteCount", value: settings.ProxyMaxRequestByteCount, expected: int64(1024 * 1024)},
		{name: "ProxyMaxResponseByteCount", value: settings.ProxyMaxResponseByteCount, expected: int64(8 * 1024 * 1024)},
		{name: "ProxyMaxConnectionCountPerSource", value: settings.ProxyMaxConnectionCountPerSource, expected: 8},
		{name: "ProxyMaxConnectionCount", value: settings.ProxyMaxConnectionCount, expected: 256},
		{name: "ProxyIdleTimeout", value: settings.ProxyIdleTimeout, expected: 30 * time.Second},

		{name: "DnsMaxQueryRatePerSource", value: settings.DnsMaxQueryRatePerSource, expected: float64(10)},
		{name: "DnsMaxQueryBurstPerSource", value: settings.DnsMaxQueryBurstPerSource, expected: 20},
		{name: "DnsMaxQueryRate", value: settings.DnsMaxQueryRate, expected: float64(500)},
		{name: "DnsMaxQueryBurst", value: settings.DnsMaxQueryBurst, expected: 500},
		{name: "DnsMaxResponseByteCount", value: settings.DnsMaxResponseByteCount, expected: 4096},
		{name: "DnsForwardWorkerCount", value: settings.DnsForwardWorkerCount, expected: 64},
		{name: "DnsForwardTimeout", value: settings.DnsForwardTimeout, expected: 5 * time.Second},
	}
	for _, c := range cases {
		if c.value != c.expected {
			t.Errorf("%s = %v, expected %v", c.name, c.value, c.expected)
		}
	}

	if !slices.Equal(settings.DnsTlds, []string{connect.DefaultExtenderDnsTld}) {
		t.Errorf("DnsTlds = %v, expected the connect default alone", settings.DnsTlds)
	}
	if settings.DnsPrivilegedPort {
		t.Error("DnsPrivilegedPort is set by default; only a platform that can take 53 sets it (L2)")
	}
}
