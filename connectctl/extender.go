package main

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/netip"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/docopt/docopt-go"

	"github.com/urnetwork/connect"
	"github.com/urnetwork/connect/extender"
	"github.com/urnetwork/connect/gossip"
)

// The standalone extender (EXTENDER.md G4).
//
// It is the provider extender role of G2 and G3 without a provider: the
// extender server on its three carriers, the gossip node with the in-process
// listener and the feed server behind the reserved services (A8, D2, D4), and
// the activation loop that proves the carriers to the operator and publishes
// what comes back (G3).
//
// Everything is derived from one `--api_url`. The family activation urls come
// from it by the sdk's label suffix rule, and the extender dns name and the
// network host -- which names the gossip topic and gates which records this
// directory accepts (B2, D1) -- come from connect's shared label rule, so this
// command and an sdk url-only space key the same network the same way.
//
// One identity key is the whole identity (B1): it signs the certificate
// authority the carrier leaves are issued under, it is what the operator signs
// a record for, and the mesh peer id is derived from it. It is persisted at
// `--extender_key_file` and created there when absent.

// The directory file under --state_dir, beside the other dot files the sdk
// keeps (E1, F1).
const extenderDirectoryFileName = ".extenders"

// The identity key file under --state_dir when --extender_key_file is not
// given (B1).
const extenderKeyFileName = ".extender_key"

// Budget of the startup hello that seeds the root keys, and of the wait for the
// carriers to bind.
const extenderStartTimeout = 30 * time.Second

type extenderOptions struct {
	jwt      string
	apiUrl   string
	keyFile  string
	stateDir string

	tcpPort int
	udpPort int
	dnsPort int
	// also bind the dns carrier on 53, which needs privilege on most hosts
	// (L2). The bind is never required: a failure leaves the carrier on its
	// unprivileged port.
	dnsPrivilegedPort bool
	// operator patterns this extender may forward to, on top of the api host
	// and one wildcard level under it (A5)
	allowedHosts []string

	// Listen and ListenPacket, when set, bind the carriers. The test binds
	// ephemeral loopback sockets through them; nil binds the configured ports.
	listen       func(network string, address string) (net.Listener, error)
	listenPacket func(network string, address string) (net.PacketConn, error)
	// dialContext, when set, is the inner dial of every control request this
	// command makes. The test maps its synthetic operator names to loopback.
	dialContext connect.DialContextFunction
	// configureNetworkClient, when set, adjusts the network client settings
	// before it is built. The test installs an in-process resolver so the dns
	// bootstrap never leaves the machine.
	configureNetworkClient func(settings *connect.ExtenderNetworkClientSettings)
	// onStart, when set, receives the running extender once every part is up.
	// The test reads the live objects through it.
	onStart func(run *extenderRun)
}

// Reads the extender command's flags. Zero ports take the fixed carrier ports
// of A1.
func extenderOptionsFromOpts(opts docopt.Opts) (*extenderOptions, error) {
	jwt, err := opts.String("--jwt")
	if err != nil {
		return nil, fmt.Errorf("the extender needs --jwt")
	}
	apiUrl, err := opts.String("--api_url")
	if err != nil {
		apiUrl = DefaultApiUrl
	}
	options := &extenderOptions{
		jwt:     jwt,
		apiUrl:  apiUrl,
		tcpPort: connect.ExtenderTcpPort,
		udpPort: connect.ExtenderQuicPort,
		dnsPort: connect.ExtenderDnsPort,
	}
	if keyFile, err := opts.String("--extender_key_file"); err == nil {
		options.keyFile = keyFile
	}
	if stateDir, err := opts.String("--state_dir"); err == nil {
		options.stateDir = stateDir
	}
	for flag, port := range map[string]*int{
		"--listen_tcp": &options.tcpPort,
		"--listen_udp": &options.udpPort,
		"--listen_dns": &options.dnsPort,
	} {
		value, err := opts.Int(flag)
		if err != nil {
			continue
		}
		if value <= 0 || 65535 < value {
			return nil, fmt.Errorf("%s must be a port", flag)
		}
		*port = value
	}
	if dnsPrivilegedPort, err := opts.Bool("--dns_privileged_port"); err == nil {
		options.dnsPrivilegedPort = dnsPrivilegedPort
	}
	if allowedHosts, ok := opts["--allowed_host"].([]string); ok {
		for _, allowedHost := range allowedHosts {
			if allowedHost = strings.TrimSpace(allowedHost); allowedHost != "" {
				options.allowedHosts = append(options.allowedHosts, allowedHost)
			}
		}
	}
	return options, nil
}

// One running extender: every part, and the shutdown that releases them in the
// reverse order they were built.
type extenderRun struct {
	options *extenderOptions

	publicKey ed25519.PublicKey
	// the host of --api_url, and the space host under it, which is what the
	// records and the topic are keyed by
	apiHost     string
	networkHost string

	clientStrategy *connect.ClientStrategy
	directory      *connect.ExtenderDirectory
	networkClient  *connect.ExtenderNetworkClient
	listener       *gossip.InProcessListener
	feedServer     *gossip.FeedServer
	node           *gossip.Node
	server         *extender.ExtenderServer
	activator      *connect.ExtenderActivator

	serveDone chan error
	closers   []func()
}

// extenderCommand is the command entry point: the flags, the signal context,
// and the exit status of a startup failure.
func extenderCommand(opts docopt.Opts) {
	options, err := extenderOptionsFromOpts(opts)
	if err != nil {
		Err.Printf("%s", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := runExtender(ctx, options); err != nil {
		Err.Printf("extender: %s", err)
		os.Exit(1)
	}
}

// runExtender is the extender command. It serves until the context is done,
// which main wires to SIGINT and SIGTERM.
func runExtender(ctx context.Context, options *extenderOptions) error {
	run, err := newExtenderRun(ctx, options)
	if err != nil {
		return err
	}
	defer run.close()
	return run.run(ctx)
}

// Builds every part of the extender and starts it. On any failure the parts
// already built are released before returning.
func newExtenderRun(ctx context.Context, options *extenderOptions) (*extenderRun, error) {
	apiHost, err := connect.ExtenderApiHostName(options.apiUrl)
	if err != nil {
		return nil, err
	}
	run := &extenderRun{
		options:     options,
		apiHost:     apiHost,
		networkHost: connect.ExtenderNetworkHostName(apiHost),
		serveDone:   make(chan error, 1),
	}
	success := false
	defer func() {
		if !success {
			run.close()
		}
	}()

	keySeed, err := run.identityKeySeed()
	if err != nil {
		return nil, err
	}
	if run.publicKey, err = connect.ExtenderPublicKeyFromSeed(keySeed); err != nil {
		return nil, err
	}
	Out.Printf("extender public key: %s", hex.EncodeToString(run.publicKey))

	// direct only: an activation that crossed an extender would tell the
	// operator that extender's address, not this host's (C2)
	strategySettings := connect.DefaultClientStrategySettings()
	if options.dialContext != nil {
		strategySettings.ConnectSettings.DialContextSettings = &connect.DialContextSettings{
			DialContext: options.dialContext,
		}
	}
	run.clientStrategy = connect.NewDirectClientStrategy(ctx, strategySettings, 0)
	run.closers = append(run.closers, run.clientStrategy.Close)

	directorySettings := connect.DefaultExtenderDirectorySettings()
	directorySettings.NetworkHosts = run.networkHosts()
	if store := run.directoryStore(); store != nil {
		directorySettings.Store = store
	}
	run.directory = connect.NewExtenderDirectory(ctx, directorySettings)
	run.closers = append(run.closers, run.directory.Close)

	// the root keys must be in place before the first activation applies this
	// extender's own record; the network client keeps them refreshed after
	// that (B4, E3)
	if err := run.refreshRootKeys(ctx); err != nil {
		Err.Printf("extender root keys: %s", err)
	}

	run.listener = gossip.NewInProcessListener(ctx, gossip.DefaultInProcessListenerSettings())
	run.closers = append(run.closers, run.listener.Close)
	run.feedServer = gossip.NewFeedServer(
		ctx, run.directory, run.publicKey, gossip.DefaultFeedServerSettings())
	run.closers = append(run.closers, run.feedServer.Close)

	nodeSettings := gossip.DefaultNodeSettings(gossip.NodeRoleExtender)
	nodeSettings.NetworkHost = run.networkHost
	nodeSettings.Directory = run.directory
	nodeSettings.IdentityKeySeed = keySeed
	// the mesh addresses are published per activated family, which has not
	// happened yet (D2)
	nodeSettings.ExtenderListener = run.listener
	if run.node, err = gossip.NewNode(ctx, nodeSettings); err != nil {
		return nil, err
	}
	run.closers = append(run.closers, run.node.Close)

	serverSettings := extender.DefaultExtenderSettings()
	serverSettings.IdentityKeySeed = keySeed
	serverSettings.GossipConnHandler = run.listener.Handle
	serverSettings.FeedConnHandler = run.feedServer.Serve
	serverSettings.ListenErrorHandler = func(carrier string, err error) {
		Err.Printf("extender %s carrier is not listening: %s", carrier, err)
	}
	serverSettings.Listen = options.listen
	serverSettings.ListenPacket = options.listenPacket
	serverSettings.DnsPrivilegedPort = options.dnsPrivilegedPort
	// an operator activated extender is open: it accepts every header and
	// forwards only to the whitelist (A4, A5)
	run.server = extender.NewExtenderServer(
		ctx,
		nil,
		run.allowedHosts(),
		run.ports(),
		&net.Dialer{},
		serverSettings,
	)
	run.closers = append(run.closers, run.server.CloseAndWait)
	go func() {
		run.serveDone <- run.server.ListenAndServe()
	}()

	// the activation offers the carriers that bound, so it waits for the binds
	// to settle (G2)
	select {
	case <-run.server.Listening():
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(extenderStartTimeout):
		return nil, fmt.Errorf("the extender carriers did not bind")
	}
	select {
	case err := <-run.serveDone:
		if err != nil {
			return nil, err
		}
	default:
	}
	Out.Printf("extender carriers: %s", strings.Join(run.server.Carriers(), ","))

	activatorSettings := connect.DefaultExtenderActivatorSettings()
	activatorSettings.ApiUrlV4 = familyServiceUrl(options.apiUrl, 4)
	activatorSettings.ApiUrlV6 = familyServiceUrl(options.apiUrl, 6)
	// a url with no service label to suffix has no api-v4 or api-v6 host; the
	// plain url activates one family per cycle instead, which the operator
	// derives from the caller address (C2)
	activatorSettings.ApiUrl = options.apiUrl
	if activatorSettings.ApiUrlV4 == "" && activatorSettings.ApiUrlV6 == "" {
		Out.Printf(
			"extender: %s has no api-v4 or api-v6 host; activating one family per cycle",
			options.apiUrl)
	}
	activatorSettings.HelloUrl = options.apiUrl
	activatorSettings.ByJwt = func() string { return options.jwt }
	activatorSettings.ClientStrategy = run.clientStrategy
	activatorSettings.PublicKey = run.publicKey
	activatorSettings.TcpPort = options.tcpPort
	activatorSettings.UdpPort = options.udpPort
	activatorSettings.DnsPort = options.dnsPort
	// the ports that actually bound, which is what the operator probes and the
	// record lists (L2)
	activatorSettings.DnsPorts = run.server.DnsPorts
	activatorSettings.Carriers = run.server.Carriers
	activatorSettings.Directory = run.directory
	activatorSettings.OnActivated = run.activated
	run.activator = connect.NewExtenderActivator(ctx, activatorSettings)
	run.closers = append(run.closers, run.activator.Close)

	if networkClientSettings := run.networkClientSettings(); networkClientSettings != nil {
		run.networkClient = connect.NewExtenderNetworkClient(
			ctx, run.clientStrategy, run.directory, networkClientSettings)
		run.closers = append(run.closers, run.networkClient.Close)
	}

	success = true
	if options.onStart != nil {
		options.onStart(run)
	}
	return run, nil
}

// Serves until the context is done, logging every activation status change.
// One family's line is printed only when it changes, so a change to the other
// family does not repeat it.
func (self *extenderRun) run(ctx context.Context) error {
	ipVersionLines := map[int]string{}
	for {
		// subscribe before the read, so a change that lands while the lines
		// below are printed wakes the next wait rather than being lost
		_, change := self.activator.ChangeMonitor().Get()
		for _, family := range self.activator.Status().Families {
			line := extenderFamilyStatusLine(family)
			if line == "" || ipVersionLines[family.IpVersion] == line {
				continue
			}
			ipVersionLines[family.IpVersion] = line
			Out.Printf("%s", line)
		}
		select {
		case <-ctx.Done():
			return nil
		case err := <-self.serveDone:
			// every carrier went away; there is nothing left to activate
			return err
		case <-change:
		}
	}
}

// One family's activation state as a log line, empty before its first attempt.
// A family of 0 is an outcome the operator named no family for, which only the
// plain api url can produce.
func extenderFamilyStatusLine(family *connect.ExtenderFamilyActivationStatus) string {
	name := "extender"
	if 0 < family.IpVersion {
		name = fmt.Sprintf("extender v%d", family.IpVersion)
	}
	switch {
	case family.Activated:
		return fmt.Sprintf(
			"%s activated at %s until %s",
			name, family.Ip, family.ExpireTime.Format(time.RFC3339))
	case family.LastError != "":
		return fmt.Sprintf("%s is not activated: %s", name, family.LastError)
	default:
		return ""
	}
}

// Releases every part that was built, newest first.
func (self *extenderRun) close() {
	for i := len(self.closers) - 1; 0 <= i; i -= 1 {
		self.closers[i]()
	}
	self.closers = nil
}

// Publishes the mesh address of one activated family (D2, G3). Only the tcp
// carrier carries the mesh, so a host whose tcp bind failed advertises nothing.
func (self *extenderRun) activated(ipVersion int, result *connect.ExtenderActivateResult) {
	if !slices.Contains(self.server.Carriers(), connect.ExtenderCarrierTcp) {
		return
	}
	ip, err := netip.ParseAddr(result.Ip)
	if err != nil {
		Err.Printf("extender activation carried no address: %s", err)
		return
	}
	listenAddrs, err := gossip.ExtenderListenAddrs([]netip.Addr{ip}, self.options.tcpPort)
	if err != nil {
		Err.Printf("extender mesh address: %s", err)
		return
	}
	if err := self.node.Listen(listenAddrs...); err != nil {
		Err.Printf("extender mesh listen: %s", err)
	}
}

// The identity key (B1): the file's seed, a new seed written to it, or an
// ephemeral one when there is nowhere to keep it.
func (self *extenderRun) identityKeySeed() ([]byte, error) {
	keyFile := strings.TrimSpace(self.options.keyFile)
	if keyFile == "" && strings.TrimSpace(self.options.stateDir) != "" {
		keyFile = filepath.Join(self.options.stateDir, extenderKeyFileName)
	}
	if keyFile == "" {
		Err.Printf("extender identity is ephemeral; pass --extender_key_file to keep it")
		return connect.NewExtenderKeySeed()
	}

	seedHex, err := os.ReadFile(keyFile)
	if err == nil {
		return connect.ParseExtenderKeySeedHex(string(seedHex))
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	seed, err := connect.NewExtenderKeySeed()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(keyFile), 0700); err != nil {
		return nil, err
	}
	if err := os.WriteFile(keyFile, []byte(connect.ExtenderKeySeedHex(seed)), 0600); err != nil {
		return nil, err
	}
	return seed, nil
}

// The directory store under --state_dir, or nil for a memory directory (E1).
func (self *extenderRun) directoryStore() connect.ExtenderDirectoryStore {
	stateDir := strings.TrimSpace(self.options.stateDir)
	if stateDir == "" {
		return nil
	}
	if err := os.MkdirAll(stateDir, 0700); err != nil {
		Err.Printf("extender state dir: %s", err)
		return nil
	}
	return &extenderFileStore{path: filepath.Join(stateDir, extenderDirectoryFileName)}
}

// The hosts whose records this directory accepts (B2): the space host the
// operator signs with, and the api host itself, which is the space host when
// the api url names no service label.
func (self *extenderRun) networkHosts() []string {
	networkHosts := []string{self.networkHost}
	if self.apiHost != self.networkHost {
		networkHosts = append(networkHosts, self.apiHost)
	}
	return networkHosts
}

// The operator patterns this extender forwards to (A5): the api host and one
// wildcard level under it, plus every --allowed_host. The activation also
// reports the operator's own list, which the status shows rather than applies,
// so a host this extender was not configured for is visible instead of silently
// opened.
func (self *extenderRun) allowedHosts() []string {
	allowedHosts := []string{self.apiHost, "*." + self.apiHost}
	for _, allowedHost := range self.options.allowedHosts {
		if !slices.Contains(allowedHosts, allowedHost) {
			allowedHosts = append(allowedHosts, allowedHost)
		}
	}
	return allowedHosts
}

// The carrier ports (A1). tcp and udp share port 443 by default, which is one
// entry with both connect modes.
func (self *extenderRun) ports() map[int][]connect.ExtenderConnectMode {
	ports := map[int][]connect.ExtenderConnectMode{}
	for _, carrier := range []struct {
		port        int
		connectMode connect.ExtenderConnectMode
	}{
		{port: self.options.tcpPort, connectMode: connect.ExtenderConnectModeTcpTls},
		{port: self.options.udpPort, connectMode: connect.ExtenderConnectModeQuic},
		{port: self.options.dnsPort, connectMode: connect.ExtenderConnectModeDns},
	} {
		ports[carrier.port] = append(ports[carrier.port], carrier.connectMode)
	}
	return ports
}

// The network client of this space (E3), or nil when the api url names an ip
// literal: there is no service label to derive an extender dns name from, and
// nothing to resolve.
func (self *extenderRun) networkClientSettings() *connect.ExtenderNetworkClientSettings {
	extenderDnsName := connect.ExtenderServiceHostName(self.options.apiUrl, "extender")
	if extenderDnsName == "" {
		return nil
	}
	settings := connect.DefaultExtenderNetworkClientSettings()
	settings.ExtenderDnsName = extenderDnsName
	settings.ApiUrl = self.options.apiUrl
	// this node is a member of the mesh, which carries the live records, so
	// the feed is a one-shot sample (D5)
	settings.Subscribe = false
	if self.options.configureNetworkClient != nil {
		self.options.configureNetworkClient(settings)
	}
	return settings
}

// Reads the root keys from hello and installs them as the directory's trust
// anchor (B4).
func (self *extenderRun) refreshRootKeys(ctx context.Context) error {
	helloCtx, cancel := context.WithTimeout(ctx, extenderStartTimeout)
	defer cancel()
	request, err := connect.HelloRequestFromUrl(helloCtx, self.options.apiUrl, self.options.jwt)
	if err != nil {
		return err
	}
	bodyBytes, err := connect.HttpGetWithStrategyRaw(
		helloCtx, self.clientStrategy, request.URL.String(), self.options.jwt)
	if err != nil {
		return err
	}
	helloResult := &struct {
		ExtenderRootPublicKeys []string `json:"extender_root_public_keys"`
	}{}
	if err := json.Unmarshal(bodyBytes, helloResult); err != nil {
		return err
	}
	if len(helloResult.ExtenderRootPublicKeys) == 0 {
		return fmt.Errorf("the operator published no extender root keys")
	}
	keySet, err := connect.NewExtenderRootKeySetFromHex(helloResult.ExtenderRootPublicKeys...)
	if err != nil {
		return err
	}
	self.directory.SetRootKeys(keySet)
	return nil
}

// extenderFileStore persists the directory envelope in one file (E1). A file
// that is not there yet is an empty directory, not an error.
type extenderFileStore struct {
	path string
}

func (self *extenderFileStore) Load() ([]byte, error) {
	stateBytes, err := os.ReadFile(self.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	return stateBytes, err
}

func (self *extenderFileStore) Save(stateBytes []byte) error {
	return os.WriteFile(self.path, stateBytes, 0600)
}
