package connect

import (
	"context"

	"net/netip"

	"testing"

	"github.com/go-playground/assert/v2"
)

func TestDohQuery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultDohSettings()

	testIp1, err := netip.ParseAddr("1.1.1.1")
	assert.Equal(t, err, nil)
	testIp2, err := netip.ParseAddr("10.10.10.10")
	assert.Equal(t, err, nil)

	ips := DohQuery(ctx, 4, "A", settings, "test1.bringyour.com")
	assert.Equal(t, ips, map[netip.Addr]bool{
		testIp1: true,
		testIp2: true,
	})

}
