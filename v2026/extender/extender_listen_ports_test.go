// The ports one extender binds (EXTENDER.md L2). The privileged dns bind is
// additive and only widens a carrier the caller already configured, and the
// widening is a copy: the configured port map belongs to the caller.

package extender

import (
	"context"
	"maps"
	"slices"
	"testing"

	"github.com/urnetwork/connect/v2026"
)

// 53 is added only when the caller configured a dns carrier and only when the
// platform can take the port, and the configured map is never changed by the
// widening (L2).
func TestExtenderListenPortsWidenOnlyForAConfiguredDnsCarrier(t *testing.T) {
	clonePorts := func(ports map[int][]connect.ExtenderConnectMode) map[int][]connect.ExtenderConnectMode {
		cloned := map[int][]connect.ExtenderConnectMode{}
		for port, connectModes := range ports {
			cloned[port] = slices.Clone(connectModes)
		}
		return cloned
	}

	cases := []struct {
		description       string
		dnsPrivilegedPort bool
		ports             map[int][]connect.ExtenderConnectMode
		expected          map[int][]connect.ExtenderConnectMode
	}{
		{
			description:       "the dns carrier without the privileged port",
			dnsPrivilegedPort: false,
			ports: map[int][]connect.ExtenderConnectMode{
				4053: {connect.ExtenderConnectModeDns},
			},
			expected: map[int][]connect.ExtenderConnectMode{
				4053: {connect.ExtenderConnectModeDns},
			},
		},
		{
			description:       "no dns carrier to widen",
			dnsPrivilegedPort: true,
			ports: map[int][]connect.ExtenderConnectMode{
				9443: {connect.ExtenderConnectModeTcpTls, connect.ExtenderConnectModeQuic},
			},
			expected: map[int][]connect.ExtenderConnectMode{
				9443: {connect.ExtenderConnectModeTcpTls, connect.ExtenderConnectModeQuic},
			},
		},
		{
			description:       "the dns carrier on an unprivileged port",
			dnsPrivilegedPort: true,
			ports: map[int][]connect.ExtenderConnectMode{
				9443: {connect.ExtenderConnectModeTcpTls},
				4053: {connect.ExtenderConnectModeDns},
			},
			expected: map[int][]connect.ExtenderConnectMode{
				9443:                   {connect.ExtenderConnectModeTcpTls},
				4053:                   {connect.ExtenderConnectModeDns},
				connect.DefaultDnsPort: {connect.ExtenderConnectModeDns},
			},
		},
		{
			description:       "the dns carrier already on the privileged port",
			dnsPrivilegedPort: true,
			ports: map[int][]connect.ExtenderConnectMode{
				connect.DefaultDnsPort: {connect.ExtenderConnectModeDns},
			},
			expected: map[int][]connect.ExtenderConnectMode{
				connect.DefaultDnsPort: {connect.ExtenderConnectModeDns},
			},
		},
		{
			description:       "another carrier already on the privileged port",
			dnsPrivilegedPort: true,
			ports: map[int][]connect.ExtenderConnectMode{
				connect.DefaultDnsPort: {connect.ExtenderConnectModeQuic},
				4053:                   {connect.ExtenderConnectModeDns},
			},
			expected: map[int][]connect.ExtenderConnectMode{
				connect.DefaultDnsPort: {connect.ExtenderConnectModeQuic, connect.ExtenderConnectModeDns},
				4053:                   {connect.ExtenderConnectModeDns},
			},
		},
		{
			description:       "no ports at all",
			dnsPrivilegedPort: true,
			ports:             nil,
			expected:          map[int][]connect.ExtenderConnectMode{},
		},
	}
	for _, c := range cases {
		configuredPorts := clonePorts(c.ports)
		settings := DefaultExtenderSettings()
		settings.DnsPrivilegedPort = c.dnsPrivilegedPort
		ctx, cancel := context.WithCancel(context.Background())
		server := NewExtenderServer(ctx, nil, nil, c.ports, nil, settings)

		listenPorts := server.listenPorts()
		if !maps.EqualFunc(listenPorts, c.expected, slices.Equal) {
			t.Errorf("%s: listen ports = %v, expected %v", c.description, listenPorts, c.expected)
		}
		// the caller's map is what an activation renders, so the widening must
		// not reach it
		if !maps.EqualFunc(c.ports, configuredPorts, slices.Equal) {
			t.Errorf("%s: the configured ports became %v, expected %v", c.description, c.ports, configuredPorts)
		}

		server.Close()
		cancel()
	}
}
