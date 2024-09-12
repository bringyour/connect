package connect

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"time"

	"testing"

	"github.com/go-playground/assert/v2"
)

func TestExtender(t *testing.T) {

	// actual content server, port 443 (127.0.0.1)
	// https, self signed
	// one route, /hello

	// extender server, port 442

	// client

	// test uses extender http client to GET /hello

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	certPemBytes, keyPemBytes, err := selfSign([]string{"localhost"}, "Connect Test")
	assert.Equal(t, err, nil)

	tempDirPath, err := os.MkdirTemp("", "connect")
	assert.Equal(t, err, nil)

	certFile := filepath.Join(tempDirPath, "localhost.pem")
	keyFile := filepath.Join(tempDirPath, "localhost.key")
	os.WriteFile(certFile, certPemBytes, 0x777)
	os.WriteFile(keyFile, keyPemBytes, 0x777)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", 443),
		Handler: &testExtenderServer{},
	}
	defer server.Close()
	go server.ListenAndServeTLS(certFile, keyFile)

	extenderServer := NewExtenderServer(
		ctx,
		[]string{"montrose"},
		[]string{"localhost"},
		map[int][]ExtenderConnectMode{
			442: []ExtenderConnectMode{ExtenderConnectModeTcpTls, ExtenderConnectModeQuic},
		},
		&net.Dialer{},
	)
	defer extenderServer.Close()
	go extenderServer.ListenAndServe()

	select {
	case <-time.After(1 * time.Second):
	}

	localIp, err := netip.ParseAddr("127.0.0.1")
	assert.Equal(t, err, nil)

	connectSettings := DefaultConnectSettings()
	connectSettings.TlsConfig = &tls.Config{
		InsecureSkipVerify: true,
	}

	client := NewExtenderHttpClient(
		connectSettings,
		&ExtenderConfig{
			Profile: ExtenderProfile{
				ConnectMode: ExtenderConnectModeQuic,
				ServerName:  "bringyour.com",
				Port:        442,
			},
			Ip:     localIp,
			Secret: "montrose",
		},
	)

	r, err := client.Get("https://localhost/hello")

	assert.Equal(t, err, nil)
	assert.Equal(t, r.StatusCode, 200)

	body, err := io.ReadAll(r.Body)
	assert.Equal(t, err, nil)
	assert.Equal(t, string(body), "{}")

}

type testExtenderServer struct {
}

func (self *testExtenderServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {

	w.Header().Add("Content-Type", "application/json")
	w.Write([]byte("{}"))
}
