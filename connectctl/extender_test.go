package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect"
	"github.com/urnetwork/connect/gossip"
	"github.com/urnetwork/connect/protocol"
)

// The connectctl extender command end to end (EXTENDER.md G4).
//
// One in-process operator, one extender on ephemeral loopback sockets, and a
// phase 3 feed client that reaches the running extender over its tcp carrier.
// Nothing binds a privileged port and nothing leaves the machine: the operator
// names resolve through the injected dial, and the dns bootstrap through an
// in-process resolver that answers with nothing.

// The space this fixture operator serves. The api host carries the service
// label, so the extender derives `extender.space.example` and the space host
// `space.example` from it exactly as the sdk does.
const testExtenderApiHost = "api.space.example"
const testExtenderNetworkHost = "space.example"

// The addresses the fixture operator publishes for this extender.
const (
	testExtenderIpv4 = "198.51.100.7"
	testExtenderIpv6 = "2001:db8::7"
)

// testExtenderOperator serves `/hello` and `/network/extender-activate` for
// both family hosts (C2, C7).
type testExtenderOperator struct {
	server         *httptest.Server
	rootPrivateKey ed25519.PrivateKey
	rootPublicKey  ed25519.PublicKey

	activations chan *connect.ExtenderActivateArgs

	stateLock   sync.Mutex
	issueSerial int
}

func newTestExtenderOperator(t *testing.T) *testExtenderOperator {
	t.Helper()
	seed, err := connect.NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	rootPrivateKey, err := connect.ExtenderPrivateKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	operator := &testExtenderOperator{
		rootPrivateKey: rootPrivateKey,
		rootPublicKey:  rootPrivateKey.Public().(ed25519.PublicKey),
		activations:    make(chan *connect.ExtenderActivateArgs, 16),
	}
	operator.server = httptest.NewServer(http.HandlerFunc(operator.handle))
	t.Cleanup(operator.server.Close)
	return operator
}

func (self *testExtenderOperator) handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/hello":
		json.NewEncoder(w).Encode(map[string]any{
			"client_address":            testExtenderIpv4,
			"extender_root_public_keys": []string{hex.EncodeToString(self.rootPublicKey)},
		})
	case connect.ExtenderActivatePath:
		args := &connect.ExtenderActivateArgs{}
		if err := json.NewDecoder(r.Body).Decode(args); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		select {
		case self.activations <- args:
		default:
		}
		result, err := self.activateResult(r.Host, args)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(result)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// The signed answer of one activation. The record names both families, as the
// operator's does from its address rows, and every record is newer than the
// last so the newest wins rule of B5 keeps the latest (C1, C2).
func (self *testExtenderOperator) activateResult(
	host string,
	args *connect.ExtenderActivateArgs,
) (*connect.ExtenderActivateResult, error) {
	publicKey, err := connect.ParseExtenderPublicKeyHex(args.PublicKeyHex)
	if err != nil {
		return nil, err
	}
	ipVersion := 4
	ip := testExtenderIpv4
	if strings.HasPrefix(host, "api-v6.") {
		ipVersion = 6
		ip = testExtenderIpv6
	}

	self.stateLock.Lock()
	self.issueSerial += 1
	issueTime := time.Now().Add(time.Duration(self.issueSerial) * time.Millisecond)
	self.stateLock.Unlock()
	expireTime := issueTime.Add(14 * 24 * time.Hour)

	addresses := []*protocol.ExtenderAddress{}
	for _, recordIp := range []string{testExtenderIpv4, testExtenderIpv6} {
		addressIpVersion := uint32(4)
		if netip.MustParseAddr(recordIp).Is6() {
			addressIpVersion = 6
		}
		addresses = append(addresses, &protocol.ExtenderAddress{
			Ip:        recordIp,
			IpVersion: addressIpVersion,
			Carriers:  args.Carriers,
		})
	}
	record, err := connect.SignExtenderRecord(self.rootPrivateKey, &protocol.ExtenderRecordBody{
		PublicKey:    publicKey,
		Addresses:    addresses,
		TcpPort:      uint32(args.TcpPort),
		UdpPort:      uint32(args.UdpPort),
		DnsPort:      uint32(args.DnsPort),
		DnsTld:       args.DnsTld,
		CountryCode:  "zz",
		IssueTimeMs:  uint64(issueTime.UnixMilli()),
		ExpireTimeMs: uint64(expireTime.UnixMilli()),
		NetworkHost:  testExtenderNetworkHost,
	})
	if err != nil {
		return nil, err
	}
	recordBytes, err := proto.Marshal(record)
	if err != nil {
		return nil, err
	}
	return &connect.ExtenderActivateResult{
		Activated:    true,
		Ip:           ip,
		IpVersion:    ipVersion,
		Carriers:     args.Carriers,
		ExpireTime:   &expireTime,
		AllowedHosts: []string{testExtenderNetworkHost, "*." + testExtenderNetworkHost},
		Record:       recordBase64(recordBytes),
	}, nil
}

func recordBase64(recordBytes []byte) string {
	return base64.StdEncoding.EncodeToString(recordBytes)
}

// The extender command binds its carriers, activates against the operator with
// the carriers that bound, runs the mesh node in the extender role, and serves
// the feed to a phase 3 client through the tcp carrier (G2, G3, G4, D4).
func TestExtenderCommandServesAndActivates(t *testing.T) {
	extenderCommandServesAndActivates(t, "127.0.0.1")
}

// The same over ipv6 loopback. Tests run on dual-stack hosts, so this is
// required rather than skipped: every carrier binds and the feed dial reaches
// it on the other family (A7).
func TestExtenderCommandServesAndActivatesOverIpv6(t *testing.T) {
	listener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Fatalf("ipv6 loopback is required for dual-stack tests: %v", err)
	}
	listener.Close()
	extenderCommandServesAndActivates(t, "::1")
}

func extenderCommandServesAndActivates(t *testing.T, loopbackIp string) {
	t.Helper()
	operator := newTestExtenderOperator(t)
	operatorAddress := operator.server.Listener.Addr().String()
	_, operatorPort, err := net.SplitHostPort(operatorAddress)
	if err != nil {
		t.Fatal(err)
	}

	// the extender identity is known up front, so the test can verify the
	// outer certificate against it exactly as a client with a record does (B3)
	stateDir := t.TempDir()
	keySeed, err := connect.NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	keyFile := filepath.Join(stateDir, "extender.key")
	if err := os.WriteFile(keyFile, []byte(connect.ExtenderKeySeedHex(keySeed)), 0600); err != nil {
		t.Fatal(err)
	}
	extenderPublicKey, err := connect.ExtenderPublicKeyFromSeed(keySeed)
	if err != nil {
		t.Fatal(err)
	}

	tcpListener, err := net.Listen("tcp", net.JoinHostPort(loopbackIp, "0"))
	if err != nil {
		t.Fatal(err)
	}
	quicPacketConn, err := net.ListenPacket("udp", net.JoinHostPort(loopbackIp, "0"))
	if err != nil {
		t.Fatal(err)
	}
	dnsPacketConn, err := net.ListenPacket("udp", net.JoinHostPort(loopbackIp, "0"))
	if err != nil {
		t.Fatal(err)
	}
	tcpPort := tcpListener.Addr().(*net.TCPAddr).Port
	quicPort := quicPacketConn.LocalAddr().(*net.UDPAddr).Port
	dnsPort := dnsPacketConn.LocalAddr().(*net.UDPAddr).Port

	runs := make(chan *extenderRun, 1)
	options := &extenderOptions{
		jwt:      "test-jwt",
		apiUrl:   fmt.Sprintf("http://%s:%s", testExtenderApiHost, operatorPort),
		keyFile:  keyFile,
		stateDir: stateDir,
		tcpPort:  tcpPort,
		udpPort:  quicPort,
		dnsPort:  dnsPort,
		listen: func(network string, address string) (net.Listener, error) {
			if address != fmt.Sprintf(":%d", tcpPort) {
				return nil, fmt.Errorf("unexpected extender listen %s %s", network, address)
			}
			return tcpListener, nil
		},
		listenPacket: func(network string, address string) (net.PacketConn, error) {
			switch address {
			case fmt.Sprintf(":%d", quicPort):
				return quicPacketConn, nil
			case fmt.Sprintf(":%d", dnsPort):
				return dnsPacketConn, nil
			default:
				return nil, fmt.Errorf("unexpected extender listen packet %s %s", network, address)
			}
		},
		// every operator name in this fixture is the one in-process server
		dialContext: func(ctx context.Context, network string, address string) (net.Conn, error) {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			if !strings.HasSuffix(host, testExtenderNetworkHost) {
				return nil, fmt.Errorf("unexpected dial %s %s", network, address)
			}
			return (&net.Dialer{}).DialContext(ctx, "tcp", operatorAddress)
		},
		configureNetworkClient: func(settings *connect.ExtenderNetworkClientSettings) {
			// the bootstrap name does not resolve in this fixture, and nothing
			// may reach a real resolver
			settings.ResolveDns = func(ctx context.Context, name string) ([]netip.Addr, error) {
				return nil, nil
			}
		},
		onStart: func(run *extenderRun) {
			runs <- run
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() {
		runDone <- runExtender(ctx, options)
	}()

	var run *extenderRun
	select {
	case run = <-runs:
	case err := <-runDone:
		t.Fatalf("the extender exited before it started: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("the extender did not start")
	}

	expectedCarriers := []string{
		connect.ExtenderCarrierTcp,
		connect.ExtenderCarrierQuic,
		connect.ExtenderCarrierDns,
	}
	if carriers := run.server.Carriers(); !slices.Equal(carriers, expectedCarriers) {
		t.Fatalf("carriers = %v, expected %v", carriers, expectedCarriers)
	}
	if role := run.node.Role(); role != gossip.NodeRoleExtender {
		t.Fatalf("node role = %q, expected extender", role)
	}

	select {
	case args := <-operator.activations:
		if args.PublicKeyHex != hex.EncodeToString(extenderPublicKey) {
			t.Fatalf("activation public key = %q", args.PublicKeyHex)
		}
		if !slices.Equal(args.Carriers, expectedCarriers) {
			t.Fatalf("activation carriers = %v, expected %v", args.Carriers, expectedCarriers)
		}
		if args.TcpPort != tcpPort || args.UdpPort != quicPort || args.DnsPort != dnsPort {
			t.Fatalf("activation ports = %d/%d/%d, expected %d/%d/%d",
				args.TcpPort, args.UdpPort, args.DnsPort, tcpPort, quicPort, dnsPort)
		}
		// the dns ports that actually bound, which is what the operator
		// probes one by one (L2)
		if !slices.Equal(args.DnsPorts, []int{dnsPort}) {
			t.Fatalf("activation dns ports = %v, expected %v", args.DnsPorts, []int{dnsPort})
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the extender did not activate")
	}

	// the record the activation returned is in the directory, which is what
	// the feed serves from
	deadline := time.After(60 * time.Second)
	for {
		_, change := run.activator.ChangeMonitor().Get()
		family := run.activator.Status().Family(4)
		if family != nil && family.Activated {
			if family.Ip.String() != testExtenderIpv4 {
				t.Fatalf("activated ip = %s", family.Ip)
			}
			break
		}
		select {
		case <-change:
		case <-deadline:
			t.Fatal("the extender did not report an activation")
		}
	}

	// a phase 3 feed client reaches the reserved service through the tcp
	// carrier and receives the sample (A8, D4)
	feedCtx, feedCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer feedCancel()
	stream, err := connect.DialExtenderFeed(
		feedCtx,
		connect.DefaultConnectSettings(),
		&connect.ExtenderConfig{
			Profile: connect.ExtenderProfile{
				ConnectMode: connect.ExtenderConnectModeTcpTls,
				Port:        tcpPort,
			},
			Ip:        netip.MustParseAddr(loopbackIp),
			PublicKey: extenderPublicKey,
		},
		&protocol.ExtenderFeedRequest{SampleCount: 16},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if !slices.Equal(stream.Response().PublicKey, extenderPublicKey) {
		t.Fatal("the extender answered with another identity key")
	}

	sampleKeys := [][]byte{}
	for {
		frame, err := stream.Next(feedCtx)
		if err != nil {
			t.Fatal(err)
		}
		if frame.GetEndOfSample() {
			break
		}
		if record := frame.GetRecord(); record != nil {
			body := &protocol.ExtenderRecordBody{}
			if err := proto.Unmarshal(record.Body, body); err != nil {
				t.Fatal(err)
			}
			sampleKeys = append(sampleKeys, body.PublicKey)
		}
	}
	if len(sampleKeys) == 0 {
		t.Fatal("the feed served an empty sample")
	}
	if !slices.Equal(sampleKeys[0], extenderPublicKey) {
		t.Fatal("the feed did not serve this extender's own record first")
	}

	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the extender did not stop")
	}
}
