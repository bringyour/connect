package connect

import "testing"

// The extender names of one space, derived from an api url by the one label
// rule connectctl's standalone extender and the sdk's url-only space share
// (F1, G4).
func TestExtenderNamesFromApiUrl(t *testing.T) {
	cases := []struct {
		apiUrl      string
		apiHostName string
		networkHost string
		dnsName     string
		gossipName  string
	}{
		{
			apiUrl:      "https://api.example.com",
			apiHostName: "api.example.com",
			networkHost: "example.com",
			dnsName:     "extender.example.com",
			gossipName:  "gossip.example.com",
		},
		{
			// the env prefix rides the service label, so it survives the swap
			apiUrl:      "https://g2-api.example.com/secret",
			apiHostName: "g2-api.example.com",
			networkHost: "example.com",
			dnsName:     "g2-extender.example.com",
			gossipName:  "g2-gossip.example.com",
		},
		{
			apiUrl:      "http://api.space.example:8080",
			apiHostName: "api.space.example",
			networkHost: "space.example",
			dnsName:     "extender.space.example",
			gossipName:  "gossip.space.example",
		},
		{
			// the host is written with a trailing dot and in mixed case
			apiUrl:      "https://Api.Space.Example./x",
			apiHostName: "api.space.example",
			networkHost: "space.example",
			dnsName:     "extender.space.example",
			gossipName:  "gossip.space.example",
		},
		{
			// an ip literal is its own space host and derives no service name
			apiUrl:      "http://192.0.2.10:8080",
			apiHostName: "192.0.2.10",
			networkHost: "192.0.2.10",
		},
		{
			apiUrl:      "http://[2001:db8::1]:8080",
			apiHostName: "2001:db8::1",
			networkHost: "2001:db8::1",
		},
		{
			// a bare space host has no service label to replace
			apiUrl:      "https://example.com",
			apiHostName: "example.com",
			networkHost: "example.com",
		},
		{
			apiUrl:      "https://localhost:8080",
			apiHostName: "localhost",
			networkHost: "localhost",
		},
	}
	for _, c := range cases {
		apiHostName, err := ExtenderApiHostName(c.apiUrl)
		if err != nil {
			t.Errorf("ExtenderApiHostName(%q) = %v", c.apiUrl, err)
			continue
		}
		if apiHostName != c.apiHostName {
			t.Errorf("ExtenderApiHostName(%q) = %q, expected %q", c.apiUrl, apiHostName, c.apiHostName)
		}
		if networkHost := ExtenderNetworkHostName(apiHostName); networkHost != c.networkHost {
			t.Errorf("ExtenderNetworkHostName(%q) = %q, expected %q", apiHostName, networkHost, c.networkHost)
		}
		if dnsName := ExtenderServiceHostName(c.apiUrl, "extender"); dnsName != c.dnsName {
			t.Errorf("ExtenderServiceHostName(%q, extender) = %q, expected %q", c.apiUrl, dnsName, c.dnsName)
		}
		if gossipName := ExtenderServiceHostName(c.apiUrl, "gossip"); gossipName != c.gossipName {
			t.Errorf("ExtenderServiceHostName(%q, gossip) = %q, expected %q", c.apiUrl, gossipName, c.gossipName)
		}
	}
	if _, err := ExtenderApiHostName("not a url"); err == nil {
		t.Error("a url with no host was accepted")
	}
	if _, err := ExtenderApiHostName(""); err == nil {
		t.Error("an empty url was accepted")
	}
	if dnsName := ExtenderServiceHostName("not a url", "extender"); dnsName != "" {
		t.Errorf("a url with no host derived %q", dnsName)
	}
}
