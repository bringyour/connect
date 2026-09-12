package connect

import (
	"slices"
	"testing"
)

// The bundled resource decodes, and ships empty until operations provide the
// list (A10).
func TestSpoofDomainsBundledResourceIsEmpty(t *testing.T) {
	spoofDomains, err := DecodeSpoofDomainsResource(extenderSpoofResource)
	if err != nil {
		t.Fatal(err)
	}
	if len(spoofDomains) != 0 {
		t.Fatalf("the bundled spoof list has %d entries, expected none", len(spoofDomains))
	}
	if len(SpoofDomains()) != 0 {
		t.Fatalf("SpoofDomains has %d entries, expected none", len(SpoofDomains()))
	}
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

// With no spoof names there is nothing to front an extender with, so discovery
// enumerates nothing (A10).
func TestEnumerateExtenderProfilesIsEmptyWithoutSpoofDomains(t *testing.T) {
	restore := setSpoofDomainsForTest(nil)
	defer restore()
	if profiles := EnumerateExtenderProfiles(8, map[ExtenderProfile]bool{}); len(profiles) != 0 {
		t.Fatalf("enumerated %d profiles without a spoof list", len(profiles))
	}
}

// Profiles draw their names from the spoof list and use the fixed carrier
// ports; nothing already visited is enumerated again (A10).
func TestEnumerateExtenderProfilesUsesSpoofDomainsAndFixedPorts(t *testing.T) {
	restore := setSpoofDomainsForTest([]string{"one.example", "two.example"})
	defer restore()

	profiles := EnumerateExtenderProfiles(64, map[ExtenderProfile]bool{})
	if len(profiles) == 0 {
		t.Fatal("no profiles were enumerated")
	}
	carriers := map[ExtenderConnectMode]bool{}
	for _, profile := range profiles {
		if profile.ServerName != "one.example" && profile.ServerName != "two.example" {
			t.Fatalf("profile name = %q, expected a spoof name", profile.ServerName)
		}
		carriers[profile.ConnectMode] = true
		switch profile.ConnectMode {
		case ExtenderConnectModeTcpTls:
			if profile.Port != ExtenderTcpPort {
				t.Fatalf("tcp port = %d, expected %d", profile.Port, ExtenderTcpPort)
			}
			if profile.DnsTld != "" {
				t.Fatalf("tcp profile carries a dns tld %q", profile.DnsTld)
			}
		case ExtenderConnectModeQuic:
			if profile.Port != ExtenderQuicPort {
				t.Fatalf("quic port = %d, expected %d", profile.Port, ExtenderQuicPort)
			}
			if profile.Fragment || profile.Reorder {
				t.Fatal("a quic profile carries the tcp resilience flags")
			}
		case ExtenderConnectModeDns:
			if profile.Port != ExtenderDnsPort {
				t.Fatalf("dns port = %d, expected %d", profile.Port, ExtenderDnsPort)
			}
			if profile.DnsTld != DefaultExtenderDnsTld {
				t.Fatalf("dns tld = %q, expected %q", profile.DnsTld, DefaultExtenderDnsTld)
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
			t.Fatalf("no %s profile was enumerated", connectMode)
		}
	}

	visited := map[ExtenderProfile]bool{}
	for _, profile := range profiles {
		visited[profile] = true
	}
	for _, profile := range EnumerateExtenderProfiles(8, visited) {
		if visited[profile] {
			t.Fatalf("profile %v was enumerated again", profile)
		}
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
