package main

import (
	"encoding/hex"
	"errors"
	"io/fs"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/docopt/docopt-go"

	"github.com/urnetwork/connect/v2026"
)

// The flags and the local wiring of the connectctl extender (EXTENDER.md G4).
//
// Everything here is a pure derivation from the options, so nothing binds a
// socket or reaches the network. The end-to-end path is
// TestExtenderCommandServesAndActivates.

// The synthetic space these cases derive their names from.
const testOptionsApiUrl = "https://api.space.example"

// The grammar accepts the extender command with every flag, and the repeatable
// host flag arrives as the slice extenderOptionsFromOpts reads.
func TestExtenderUsageParsesEveryFlag(t *testing.T) {
	argv := []string{
		"extender",
		"--jwt=test-jwt",
		"--api_url=" + testOptionsApiUrl,
		"--extender_key_file=/state/extender.key",
		"--listen_tcp=1443",
		"--listen_udp=2443",
		"--listen_dns=3053",
		"--dns_privileged_port",
		"--allowed_host=one.space.example",
		"--allowed_host=two.space.example",
		"--state_dir=/state",
	}
	opts, err := docopt.ParseArgs(connectCtlUsage(), argv, ConnectCtlVersion)
	if err != nil {
		t.Fatal(err)
	}
	if isExtender, err := opts.Bool("extender"); err != nil || !isExtender {
		t.Fatalf("the extender command did not select: %v %v", isExtender, err)
	}
	options, err := extenderOptionsFromOpts(opts)
	if err != nil {
		t.Fatal(err)
	}
	if options.jwt != "test-jwt" {
		t.Errorf("jwt = %q", options.jwt)
	}
	if options.apiUrl != testOptionsApiUrl {
		t.Errorf("api url = %q", options.apiUrl)
	}
	if options.keyFile != "/state/extender.key" {
		t.Errorf("key file = %q", options.keyFile)
	}
	if options.stateDir != "/state" {
		t.Errorf("state dir = %q", options.stateDir)
	}
	if options.tcpPort != 1443 || options.udpPort != 2443 || options.dnsPort != 3053 {
		t.Errorf("ports = %d/%d/%d", options.tcpPort, options.udpPort, options.dnsPort)
	}
	if !options.dnsPrivilegedPort {
		t.Error("the privileged dns port flag did not arrive")
	}
	expectedHosts := []string{"one.space.example", "two.space.example"}
	if !slices.Equal(options.allowedHosts, expectedHosts) {
		t.Errorf("allowed hosts = %v, expected %v", options.allowedHosts, expectedHosts)
	}
}

// Without the port flags the fixed carrier ports of A1 and L2 stand, and
// without --api_url the compiled default does.
func TestExtenderOptionDefaults(t *testing.T) {
	opts, err := docopt.ParseArgs(
		connectCtlUsage(), []string{"extender", "--jwt=test-jwt"}, ConnectCtlVersion)
	if err != nil {
		t.Fatal(err)
	}
	options, err := extenderOptionsFromOpts(opts)
	if err != nil {
		t.Fatal(err)
	}
	if options.apiUrl != DefaultApiUrl {
		t.Errorf("api url = %q, expected %q", options.apiUrl, DefaultApiUrl)
	}
	if options.tcpPort != connect.ExtenderTcpPort {
		t.Errorf("tcp port = %d, expected %d", options.tcpPort, connect.ExtenderTcpPort)
	}
	if options.udpPort != connect.ExtenderQuicPort {
		t.Errorf("udp port = %d, expected %d", options.udpPort, connect.ExtenderQuicPort)
	}
	if options.dnsPort != connect.ExtenderDnsPort {
		t.Errorf("dns port = %d, expected %d", options.dnsPort, connect.ExtenderDnsPort)
	}
	if options.dnsPrivilegedPort {
		t.Error("the privileged dns port defaulted on")
	}
	if 0 < len(options.allowedHosts) {
		t.Errorf("allowed hosts = %v, expected none", options.allowedHosts)
	}
	if options.keyFile != "" || options.stateDir != "" {
		t.Errorf("key file = %q, state dir = %q, expected neither", options.keyFile, options.stateDir)
	}
}

// A port flag outside the port range is refused rather than binding something
// else, and the message names the flag that was wrong.
func TestExtenderOptionsRefuseAPortOutOfRange(t *testing.T) {
	cases := []struct {
		flag  string
		value string
	}{
		{flag: "--listen_tcp", value: "0"},
		{flag: "--listen_tcp", value: "-1"},
		{flag: "--listen_tcp", value: "65536"},
		{flag: "--listen_udp", value: "0"},
		{flag: "--listen_udp", value: "70000"},
		{flag: "--listen_dns", value: "0"},
		{flag: "--listen_dns", value: "-53"},
	}
	for _, c := range cases {
		opts := docopt.Opts{
			"--jwt":     "test-jwt",
			"--api_url": testOptionsApiUrl,
			c.flag:      c.value,
		}
		options, err := extenderOptionsFromOpts(opts)
		if err == nil {
			t.Errorf("%s=%s was accepted as %+v", c.flag, c.value, options)
			continue
		}
		if !strings.Contains(err.Error(), c.flag) {
			t.Errorf("%s=%s failed with %q, which does not name the flag", c.flag, c.value, err)
		}
	}
}

// Without --jwt there is nothing to activate with, so the command refuses
// before it builds anything.
func TestExtenderOptionsRequireTheJwt(t *testing.T) {
	if _, err := extenderOptionsFromOpts(docopt.Opts{"--api_url": testOptionsApiUrl}); err == nil {
		t.Error("the extender started without a jwt")
	}
}

// Repeated host flags are trimmed and blanks dropped, so a shell that passes
// an empty value does not widen the whitelist with an empty pattern.
func TestExtenderOptionsTrimAllowedHosts(t *testing.T) {
	options, err := extenderOptionsFromOpts(docopt.Opts{
		"--jwt":          "test-jwt",
		"--allowed_host": []string{"  one.space.example  ", "", "   ", "two.space.example"},
	})
	if err != nil {
		t.Fatal(err)
	}
	expectedHosts := []string{"one.space.example", "two.space.example"}
	if !slices.Equal(options.allowedHosts, expectedHosts) {
		t.Errorf("allowed hosts = %v, expected %v", options.allowedHosts, expectedHosts)
	}
}

// Builds the run of one options set without starting anything, which is what
// the derivations below are read from.
func newTestExtenderRun(t *testing.T, options *extenderOptions) *extenderRun {
	t.Helper()
	apiHost, err := connect.ExtenderApiHostName(options.apiUrl)
	if err != nil {
		t.Fatal(err)
	}
	return &extenderRun{
		options:     options,
		apiHost:     apiHost,
		networkHost: connect.ExtenderNetworkHostName(apiHost),
	}
}

// The forward whitelist is the api host and one wildcard level under it, plus
// every flag, deduped so a flag that repeats a derived pattern adds nothing
// (A5).
func TestExtenderAllowedHostsUnionTheApiHostAndTheFlags(t *testing.T) {
	cases := []struct {
		apiUrl       string
		allowedHosts []string
		want         []string
	}{
		{
			apiUrl: testOptionsApiUrl,
			want:   []string{"api.space.example", "*.api.space.example"},
		},
		{
			apiUrl:       testOptionsApiUrl,
			allowedHosts: []string{"space.example", "*.space.example"},
			want: []string{
				"api.space.example",
				"*.api.space.example",
				"space.example",
				"*.space.example",
			},
		},
		{
			// a flag that repeats a derived pattern is not added twice
			apiUrl:       testOptionsApiUrl,
			allowedHosts: []string{"api.space.example", "other.example"},
			want:         []string{"api.space.example", "*.api.space.example", "other.example"},
		},
		{
			apiUrl: "http://192.0.2.10:8080",
			want:   []string{"192.0.2.10", "*.192.0.2.10"},
		},
	}
	for _, c := range cases {
		run := newTestExtenderRun(t, &extenderOptions{
			apiUrl:       c.apiUrl,
			allowedHosts: c.allowedHosts,
		})
		if allowedHosts := run.allowedHosts(); !slices.Equal(allowedHosts, c.want) {
			t.Errorf("allowedHosts(%q, %v) = %v, expected %v",
				c.apiUrl, c.allowedHosts, allowedHosts, c.want)
		}
	}
}

// The directory accepts the space host, and the api host too when the url
// names no service label, so a url-only space keys the same records (B2).
func TestExtenderNetworkHostsCoverTheApiHost(t *testing.T) {
	cases := []struct {
		apiUrl string
		want   []string
	}{
		{apiUrl: testOptionsApiUrl, want: []string{"space.example", "api.space.example"}},
		{apiUrl: "https://space.example", want: []string{"space.example"}},
		{apiUrl: "http://192.0.2.10:8080", want: []string{"192.0.2.10"}},
	}
	for _, c := range cases {
		run := newTestExtenderRun(t, &extenderOptions{apiUrl: c.apiUrl})
		if networkHosts := run.networkHosts(); !slices.Equal(networkHosts, c.want) {
			t.Errorf("networkHosts(%q) = %v, expected %v", c.apiUrl, networkHosts, c.want)
		}
	}
}

// tcp and quic share 443 by default, which is one listen port carrying both
// connect modes; distinct ports are distinct entries (A1).
func TestExtenderPortsGroupTheSharedCarrierPort(t *testing.T) {
	run := newTestExtenderRun(t, &extenderOptions{
		apiUrl:  testOptionsApiUrl,
		tcpPort: connect.ExtenderTcpPort,
		udpPort: connect.ExtenderQuicPort,
		dnsPort: connect.ExtenderDnsPort,
	})
	ports := run.ports()
	if len(ports) != 2 {
		t.Fatalf("ports = %v, expected the shared 443 and the dns port", ports)
	}
	sharedModes := []connect.ExtenderConnectMode{
		connect.ExtenderConnectModeTcpTls,
		connect.ExtenderConnectModeQuic,
	}
	if !slices.Equal(ports[connect.ExtenderTcpPort], sharedModes) {
		t.Errorf("port 443 = %v, expected %v", ports[connect.ExtenderTcpPort], sharedModes)
	}
	dnsModes := []connect.ExtenderConnectMode{connect.ExtenderConnectModeDns}
	if !slices.Equal(ports[connect.ExtenderDnsPort], dnsModes) {
		t.Errorf("dns port = %v, expected %v", ports[connect.ExtenderDnsPort], dnsModes)
	}

	split := newTestExtenderRun(t, &extenderOptions{
		apiUrl:  testOptionsApiUrl,
		tcpPort: 1443,
		udpPort: 2443,
		dnsPort: 3053,
	})
	splitPorts := split.ports()
	if len(splitPorts) != 3 {
		t.Fatalf("ports = %v, expected three", splitPorts)
	}
	for port, connectMode := range map[int]connect.ExtenderConnectMode{
		1443: connect.ExtenderConnectModeTcpTls,
		2443: connect.ExtenderConnectModeQuic,
		3053: connect.ExtenderConnectModeDns,
	} {
		if !slices.Equal(splitPorts[port], []connect.ExtenderConnectMode{connectMode}) {
			t.Errorf("port %d = %v, expected %v", port, splitPorts[port], connectMode)
		}
	}
}

// A url with a service label derives the bootstrap name and takes the one-shot
// sample, since a member node carries the live records; a url with nothing to
// derive from runs with no network client at all (E3, G4).
func TestExtenderNetworkClientSettingsFollowTheApiUrl(t *testing.T) {
	run := newTestExtenderRun(t, &extenderOptions{apiUrl: testOptionsApiUrl})
	settings := run.networkClientSettings()
	if settings == nil {
		t.Fatal("a url with a service label derived no network client")
	}
	if settings.ExtenderDnsName != "extender.space.example" {
		t.Errorf("dns name = %q", settings.ExtenderDnsName)
	}
	if settings.ApiUrl != testOptionsApiUrl {
		t.Errorf("api url = %q", settings.ApiUrl)
	}
	if settings.Subscribe {
		t.Error("a member node subscribed to the feed")
	}

	for _, apiUrl := range []string{
		"http://192.0.2.10:8080",
		"http://[2001:db8::10]:8080",
		"https://space.example",
	} {
		bare := newTestExtenderRun(t, &extenderOptions{apiUrl: apiUrl})
		if settings := bare.networkClientSettings(); settings != nil {
			t.Errorf("%s derived a network client with dns name %q", apiUrl, settings.ExtenderDnsName)
		}
	}
}

// The settings hook runs after the defaults are in place, which is how the
// end-to-end test keeps the bootstrap off the network.
func TestExtenderNetworkClientSettingsTakeTheHook(t *testing.T) {
	configured := false
	run := newTestExtenderRun(t, &extenderOptions{
		apiUrl: testOptionsApiUrl,
		configureNetworkClient: func(settings *connect.ExtenderNetworkClientSettings) {
			configured = true
			settings.ExtenderDnsName = "other.space.example"
		},
	})
	settings := run.networkClientSettings()
	if settings == nil {
		t.Fatal("no network client")
	}
	if !configured {
		t.Error("the hook did not run")
	}
	if settings.ExtenderDnsName != "other.space.example" {
		t.Errorf("dns name = %q, the hook did not win", settings.ExtenderDnsName)
	}
}

// The identity key is the file's seed when it is there, a new seed written to
// the file when it is not, and the state dir's dot file when no key file was
// named (B1).
func TestExtenderIdentityKeySeedUsesTheKeyFile(t *testing.T) {
	stateDir := t.TempDir()
	keyFile := filepath.Join(stateDir, "nested", "extender.key")
	run := newTestExtenderRun(t, &extenderOptions{apiUrl: testOptionsApiUrl, keyFile: keyFile})

	seed, err := run.identityKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	if len(seed) != len(mustNewKeySeed(t)) {
		t.Fatalf("seed length = %d", len(seed))
	}
	fileBytes, err := os.ReadFile(keyFile)
	if err != nil {
		t.Fatalf("the key file was not created: %v", err)
	}
	if strings.TrimSpace(string(fileBytes)) != connect.ExtenderKeySeedHex(seed) {
		t.Errorf("the key file holds %q", string(fileBytes))
	}
	info, err := os.Stat(keyFile)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Errorf("the key file mode is %v", info.Mode().Perm())
	}

	// the second read is the same identity, so the records the operator signed
	// for it stay valid across restarts
	again, err := run.identityKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(seed, again) {
		t.Error("the key file was rewritten with another identity")
	}
}

// With no --extender_key_file the key lives at the state dir's dot file.
func TestExtenderIdentityKeySeedFallsBackToTheStateDir(t *testing.T) {
	stateDir := t.TempDir()
	run := newTestExtenderRun(t, &extenderOptions{apiUrl: testOptionsApiUrl, stateDir: stateDir})
	seed, err := run.identityKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	fileBytes, err := os.ReadFile(filepath.Join(stateDir, extenderKeyFileName))
	if err != nil {
		t.Fatalf("the state dir key file was not created: %v", err)
	}
	if strings.TrimSpace(string(fileBytes)) != connect.ExtenderKeySeedHex(seed) {
		t.Errorf("the state dir key file holds %q", string(fileBytes))
	}
}

// With nowhere to keep it the identity is ephemeral rather than a failure, and
// each read is a different one.
func TestExtenderIdentityKeySeedIsEphemeralWithNoStore(t *testing.T) {
	run := newTestExtenderRun(t, &extenderOptions{apiUrl: testOptionsApiUrl})
	first, err := run.identityKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	second, err := run.identityKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	if slices.Equal(first, second) {
		t.Error("two ephemeral identities matched")
	}
}

// A key file that is not a seed is an error rather than a silent new identity,
// which would abandon every record the operator signed for the old key.
func TestExtenderIdentityKeySeedRefusesACorruptKeyFile(t *testing.T) {
	cases := []string{
		"not hex",
		"",
		hex.EncodeToString([]byte("short")),
	}
	for _, content := range cases {
		stateDir := t.TempDir()
		keyFile := filepath.Join(stateDir, extenderKeyFileName)
		if err := os.WriteFile(keyFile, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		run := newTestExtenderRun(t, &extenderOptions{apiUrl: testOptionsApiUrl, keyFile: keyFile})
		if seed, err := run.identityKeySeed(); err == nil {
			t.Errorf("the key file %q was read as the seed %x", content, seed)
		}
	}
}

func mustNewKeySeed(t *testing.T) []byte {
	t.Helper()
	seed, err := connect.NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	return seed
}

// The status line of one family: nothing before the first attempt, the address
// and expiry once activated, the error otherwise. A family of 0 is the plain
// api url's outcome, which names no version.
func TestExtenderFamilyStatusLine(t *testing.T) {
	expireTime := time.Date(2031, 3, 4, 5, 6, 7, 0, time.UTC)
	cases := []struct {
		family *connect.ExtenderFamilyActivationStatus
		want   string
	}{
		{
			family: &connect.ExtenderFamilyActivationStatus{IpVersion: 4},
			want:   "",
		},
		{
			family: &connect.ExtenderFamilyActivationStatus{
				IpVersion:  4,
				Activated:  true,
				Ip:         netip.MustParseAddr("198.51.100.7"),
				ExpireTime: expireTime,
			},
			want: "extender v4 activated at 198.51.100.7 until 2031-03-04T05:06:07Z",
		},
		{
			family: &connect.ExtenderFamilyActivationStatus{
				IpVersion:  6,
				Activated:  true,
				Ip:         netip.MustParseAddr("2001:db8::7"),
				ExpireTime: expireTime,
			},
			want: "extender v6 activated at 2001:db8::7 until 2031-03-04T05:06:07Z",
		},
		{
			family: &connect.ExtenderFamilyActivationStatus{
				IpVersion: 6,
				LastError: "the operator refused",
			},
			want: "extender v6 is not activated: the operator refused",
		},
		{
			// the plain api url names no family
			family: &connect.ExtenderFamilyActivationStatus{LastError: "no route"},
			want:   "extender is not activated: no route",
		},
		{
			family: &connect.ExtenderFamilyActivationStatus{
				Activated:  true,
				Ip:         netip.MustParseAddr("198.51.100.7"),
				ExpireTime: expireTime,
			},
			want: "extender activated at 198.51.100.7 until 2031-03-04T05:06:07Z",
		},
	}
	for _, c := range cases {
		if line := extenderFamilyStatusLine(c.family); line != c.want {
			t.Errorf("extenderFamilyStatusLine(%+v) = %q, expected %q", c.family, line, c.want)
		}
	}
}

// The directory file store: a file that is not there yet is an empty directory
// rather than an error, and what is saved comes back (E1).
func TestExtenderFileStoreRoundTripsAndToleratesNoFile(t *testing.T) {
	store := &extenderFileStore{path: filepath.Join(t.TempDir(), extenderDirectoryFileName)}
	stateBytes, err := store.Load()
	if err != nil {
		t.Fatalf("an absent store failed: %v", err)
	}
	if stateBytes != nil {
		t.Errorf("an absent store loaded %q", string(stateBytes))
	}

	saved := []byte(`{"version":1,"records":[],"addresses":[]}`)
	if err := store.Save(saved); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if string(loaded) != string(saved) {
		t.Errorf("loaded %q, expected %q", string(loaded), string(saved))
	}
	info, err := os.Stat(store.path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Errorf("the store mode is %v", info.Mode().Perm())
	}

	// a directory that cannot be read is an error, not an empty directory:
	// silently starting empty would drop every known extender
	unreadable := &extenderFileStore{path: t.TempDir()}
	if _, err := unreadable.Load(); err == nil {
		t.Error("a store that is a directory loaded as empty")
	} else if errors.Is(err, fs.ErrNotExist) {
		t.Errorf("a store that is a directory reported %v", err)
	}
}

// The store under --state_dir is created on demand, and no state dir means a
// memory directory rather than a file somewhere unexpected.
func TestExtenderDirectoryStoreFollowsTheStateDir(t *testing.T) {
	if run := newTestExtenderRun(t, &extenderOptions{apiUrl: testOptionsApiUrl}); run.directoryStore() != nil {
		t.Error("a run with no state dir kept a file store")
	}
	stateDir := filepath.Join(t.TempDir(), "nested")
	run := newTestExtenderRun(t, &extenderOptions{apiUrl: testOptionsApiUrl, stateDir: stateDir})
	store := run.directoryStore()
	if store == nil {
		t.Fatal("a run with a state dir kept no file store")
	}
	if _, err := os.Stat(stateDir); err != nil {
		t.Fatalf("the state dir was not created: %v", err)
	}
	fileStore, ok := store.(*extenderFileStore)
	if !ok {
		t.Fatalf("the store is %T", store)
	}
	if fileStore.path != filepath.Join(stateDir, extenderDirectoryFileName) {
		t.Errorf("the store path is %q", fileStore.path)
	}
}
