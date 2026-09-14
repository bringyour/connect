//go:build !darwin && !ios && !linux && !android

package connect

import "errors"

func newUdpSocketPollBackend() (udpSocketPollBackend, error) {
	return nil, errors.New("udp socket readiness poller unsupported")
}
