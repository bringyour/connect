package connect

import (
	"net/netip"
	"slices"
	"strings"
	"testing"
)

// The bundled resource decodes to the operator's list (A10). A shipped client
// fronts every extender dial with one of these names, so the list must be
// large enough that one name is not a fingerprint, must be syntactically
// dialable, and must never carry a name of the operator's own -- a spoof name
// that resolves back to the operator would announce exactly what the outer
// name exists to hide.
func TestSpoofDomainsBundledResourceIsPopulated(t *testing.T) {
	spoofDomains, err := DecodeSpoofDomainsResource(extenderSpoofResource)
	if err != nil {
		t.Fatal(err)
	}
	if len(spoofDomains) <= spoofDomainsMinimumCount {
		t.Fatalf(
			"the bundled spoof list has %d entries, expected more than %d",
			len(spoofDomains), spoofDomainsMinimumCount)
	}
	if bundledCount := len(SpoofDomains()); bundledCount != len(spoofDomains) {
		t.Fatalf("SpoofDomains has %d entries, expected the %d of the resource",
			bundledCount, len(spoofDomains))
	}

	visited := map[string]bool{}
	for _, spoofDomain := range spoofDomains {
		if !isSpoofHostName(spoofDomain) {
			t.Errorf("%q is not a lowercase host name", spoofDomain)
		}
		for _, operatorName := range []string{"bringyour", "ur.network", "ur.io", "ur.xyz"} {
			if strings.Contains(spoofDomain, operatorName) {
				t.Errorf("%q carries the operator name %q", spoofDomain, operatorName)
			}
		}
		if visited[spoofDomain] {
			t.Errorf("%q appears more than once", spoofDomain)
		}
		visited[spoofDomain] = true
	}
}

// The bundled list is one name out of many, so a dial that picks one is not
// picking from a set small enough to identify the client.
const spoofDomainsMinimumCount = 3000

// Reports whether a name is a lowercase syntactic host name: at least two
// labels of letters, digits, underscores and inner hyphens, each 1 to 63
// bytes, and no trailing dot. This is what crypto/tls will carry as a server
// name and what a resolver would accept, which is the whole requirement on a
// spoof name. The underscore is deliberate: real names in the list carry one
// (the nist time servers), and both dns and sni carry it unchanged.
func isSpoofHostName(hostName string) bool {
	if hostName == "" || 253 < len(hostName) {
		return false
	}
	labels := strings.Split(hostName, ".")
	if len(labels) < 2 {
		return false
	}
	for _, label := range labels {
		if label == "" || 63 < len(label) {
			return false
		}
		if label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, c := range label {
			switch {
			case 'a' <= c && c <= 'z', '0' <= c && c <= '9', c == '-', c == '_':
			default:
				return false
			}
		}
	}
	// the last label is a tld, which is never all digits
	tld := labels[len(labels)-1]
	for _, c := range tld {
		if c < '0' || '9' < c {
			return true
		}
	}
	return false
}

// The generator's encoding round-trips, and the resource does not carry the
// names as plain strings.
func TestSpoofDomainsResourceRoundTrips(t *testing.T) {
	spoofDomains := []string{"one.example", "two.example", "three.example"}
	resource, err := EncodeSpoofDomainsResource(spoofDomains)
	if err != nil {
		t.Fatal(err)
	}
	decodedSpoofDomains, err := DecodeSpoofDomainsResource(resource)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(decodedSpoofDomains, spoofDomains) {
		t.Fatalf("decoded = %v, expected %v", decodedSpoofDomains, spoofDomains)
	}
	for _, spoofDomain := range spoofDomains {
		if containsBytes(resource, []byte(spoofDomain)) {
			t.Fatalf("%q appears in the resource as a plain string", spoofDomain)
		}
	}
	if _, err := DecodeSpoofDomainsResource([]byte{1, 2, 3}); err == nil {
		t.Fatal("a truncated resource decoded")
	}
	if _, err := DecodeSpoofDomainsResource(slices.Repeat([]byte{0}, 64)); err == nil {
		t.Fatal("a resource that is not gzip decoded")
	}
}

// The plain text form is normalized: comments and blanks dropped, lowercased,
// deduplicated in first-seen order.
func TestParseSpoofDomainsNormalizes(t *testing.T) {
	plainText := []byte(
		"# a comment\n" +
			"  One.Example  \n" +
			"\n" +
			"two.example # trailing comment\n" +
			"ONE.EXAMPLE\n" +
			"three.example",
	)
	spoofDomains := ParseSpoofDomains(plainText)
	expected := []string{"one.example", "two.example", "three.example"}
	if !slices.Equal(spoofDomains, expected) {
		t.Fatalf("parsed = %v, expected %v", spoofDomains, expected)
	}
	if 0 != len(ParseSpoofDomains(nil)) {
		t.Fatal("an empty list did not parse as empty")
	}
}

// The test seam installs a synthetic list and restores the bundled one.
func TestSpoofDomainsTestSeamInstallsAndRestores(t *testing.T) {
	bundledCount := len(SpoofDomains())
	restore := setSpoofDomainsForTest([]string{"seam.example"})
	spoofDomains := SpoofDomains()
	if !slices.Equal(spoofDomains, []string{"seam.example"}) {
		t.Fatalf("installed = %v", spoofDomains)
	}
	spoofDomains[0] = "mutated.example"
	if SpoofDomains()[0] != "seam.example" {
		t.Fatal("the caller mutated the installed list")
	}
	restore()
	if len(SpoofDomains()) != bundledCount {
		t.Fatal("the bundled list was not restored")
	}
}

// A dialer built from a directory candidate takes its outer name from the
// spoof list and its port from the record (A10, E2).
func TestExtenderConfigsForCandidateUseSpoofDomainsAndRecordPorts(t *testing.T) {
	restore := setSpoofDomainsForTest([]string{"one.example", "two.example"})
	defer restore()

	candidate := &ExtenderCandidate{
		Ip:       netip.MustParseAddr("192.0.2.10"),
		Carriers: []string{ExtenderCarrierTcp, ExtenderCarrierQuic, ExtenderCarrierDns},
		TcpPort:  8443,
		UdpPort:  9443,
		DnsPort:  5353,
		DnsTld:   "x.example.",
	}
	extenderConfigs := extenderConfigsForCandidate(candidate, "")
	if len(extenderConfigs) != 3 {
		t.Fatalf("configs = %d, expected one per carrier", len(extenderConfigs))
	}
	carriers := map[ExtenderConnectMode]bool{}
	for _, extenderConfig := range extenderConfigs {
		profile := extenderConfig.Profile
		if profile.ServerName != "one.example" && profile.ServerName != "two.example" {
			t.Fatalf("profile name = %q, expected a spoof name", profile.ServerName)
		}
		carriers[profile.ConnectMode] = true
		switch profile.ConnectMode {
		case ExtenderConnectModeTcpTls:
			if profile.Port != 8443 {
				t.Fatalf("tcp port = %d, expected the record port", profile.Port)
			}
			if profile.DnsTld != "" {
				t.Fatalf("tcp profile carries a dns tld %q", profile.DnsTld)
			}
		case ExtenderConnectModeQuic:
			if profile.Port != 9443 {
				t.Fatalf("quic port = %d, expected the record port", profile.Port)
			}
			if profile.Fragment || profile.Reorder {
				t.Fatal("a quic profile carries the tcp resilience flags")
			}
		case ExtenderConnectModeDns:
			if profile.Port != 5353 {
				t.Fatalf("dns port = %d, expected the record port", profile.Port)
			}
			if profile.DnsTld != "x.example." {
				t.Fatalf("dns tld = %q, expected the record tld", profile.DnsTld)
			}
			if profile.Fragment || profile.Reorder {
				t.Fatal("a dns profile carries the tcp resilience flags")
			}
		default:
			t.Fatalf("unexpected connect mode %q", profile.ConnectMode)
		}
	}
	for _, connectMode := range []ExtenderConnectMode{
		ExtenderConnectModeTcpTls,
		ExtenderConnectModeQuic,
		ExtenderConnectModeDns,
	} {
		if !carriers[connectMode] {
			t.Fatalf("no %s config was built", connectMode)
		}
	}
}

// With no bundled spoof list a dialer carries no outer name, so the dial
// presents no sni at all rather than naming the operator destination (A10).
func TestExtenderConfigsForCandidateWithoutSpoofDomains(t *testing.T) {
	restore := setSpoofDomainsForTest(nil)
	defer restore()

	candidate := &ExtenderCandidate{
		Ip:       netip.MustParseAddr("192.0.2.11"),
		Carriers: []string{ExtenderCarrierTcp},
		TcpPort:  ExtenderTcpPort,
	}
	extenderConfigs := extenderConfigsForCandidate(candidate, "")
	if len(extenderConfigs) != 1 {
		t.Fatalf("configs = %d, expected one", len(extenderConfigs))
	}
	if extenderConfigs[0].Profile.ServerName != "" {
		t.Fatalf("profile name = %q, expected none", extenderConfigs[0].Profile.ServerName)
	}
}

// A feed dial keeps its own rule: a spoof name when the list has one, and the
// extender ip otherwise, which crypto/tls also sends as no sni. A feed dial has
// no destination host, so there is nothing an empty name could leak (A10, E3).
func TestExtenderFeedConfigServerName(t *testing.T) {
	ip := netip.MustParseAddr("192.0.2.12")
	candidate := &ExtenderCandidate{
		Ip:      ip,
		TcpPort: ExtenderTcpPort,
	}

	restoreEmpty := setSpoofDomainsForTest(nil)
	extenderConfig := extenderFeedConfig(candidate, ExtenderConnectModeTcpTls)
	restoreEmpty()
	if extenderConfig == nil {
		t.Fatal("no feed config was built")
	}
	if extenderConfig.Profile.ServerName != ip.String() {
		t.Fatalf("feed name = %q, expected the extender ip", extenderConfig.Profile.ServerName)
	}

	restoreSpoof := setSpoofDomainsForTest([]string{"one.example"})
	extenderConfig = extenderFeedConfig(candidate, ExtenderConnectModeTcpTls)
	restoreSpoof()
	if extenderConfig == nil {
		t.Fatal("no feed config was built")
	}
	if extenderConfig.Profile.ServerName != "one.example" {
		t.Fatalf("feed name = %q, expected the spoof name", extenderConfig.Profile.ServerName)
	}
}

// The carrier name of each connect mode, as a record and a response carry it.
func TestExtenderCarrierNames(t *testing.T) {
	cases := []struct {
		connectMode ExtenderConnectMode
		carrier     string
	}{
		{connectMode: ExtenderConnectModeTcpTls, carrier: ExtenderCarrierTcp},
		{connectMode: ExtenderConnectModeQuic, carrier: ExtenderCarrierQuic},
		{connectMode: ExtenderConnectModeDns, carrier: ExtenderCarrierDns},
	}
	for _, c := range cases {
		if carrier := ExtenderCarrierForConnectMode(c.connectMode); carrier != c.carrier {
			t.Errorf("%s carrier = %q, expected %q", c.connectMode, carrier, c.carrier)
		}
	}
}

func containsBytes(haystack []byte, needle []byte) bool {
	for i := 0; i+len(needle) <= len(haystack); i += 1 {
		if slices.Equal(haystack[i:i+len(needle)], needle) {
			return true
		}
	}
	return false
}
