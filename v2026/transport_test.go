package connect

import (
	"testing"
)

func TestConnectHost(t *testing.T) {

	host, err := connectHost("http://connect.foo.bar")
	AssertEqual(t, err, nil)
	AssertEqual(t, host, "connect.foo.bar")

	host, err = connectHost("https://other-connect.bar.com")
	AssertEqual(t, err, nil)
	AssertEqual(t, host, "other-connect.bar.com")
}
