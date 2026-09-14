//go:build darwin || ios

package connect

import (
	"time"

	"golang.org/x/sys/unix"
)

type udpSocketKqueueBackend struct {
	fd       int
	osEvents [udpSocketPollEventCount]unix.Kevent_t
}

func newUdpSocketPollBackend() (udpSocketPollBackend, error) {
	fd, err := unix.Kqueue()
	if err != nil {
		return nil, err
	}
	return &udpSocketKqueueBackend{fd: fd}, nil
}

func (self *udpSocketKqueueBackend) add(fd int) error {
	change := unix.Kevent_t{
		Ident:  uint64(fd),
		Filter: unix.EVFILT_READ,
		Flags:  unix.EV_ADD | unix.EV_ENABLE,
	}
	_, err := unix.Kevent(self.fd, []unix.Kevent_t{change}, nil, nil)
	return err
}

func (self *udpSocketKqueueBackend) remove(fd int) {
	change := unix.Kevent_t{
		Ident:  uint64(fd),
		Filter: unix.EVFILT_READ,
		Flags:  unix.EV_DELETE,
	}
	_, _ = unix.Kevent(self.fd, []unix.Kevent_t{change}, nil, nil)
}

func (self *udpSocketKqueueBackend) wait(events []udpSocketPollEvent) (int, error) {
	timeout := unix.NsecToTimespec((100 * time.Millisecond).Nanoseconds())
	n, err := unix.Kevent(self.fd, nil, self.osEvents[:min(len(events), len(self.osEvents))], &timeout)
	for i := range n {
		event := self.osEvents[i]
		events[i] = udpSocketPollEvent{
			fd:       int(event.Ident),
			terminal: event.Flags&(unix.EV_ERROR|unix.EV_EOF) != 0,
		}
	}
	return n, err
}

func (self *udpSocketKqueueBackend) close() error {
	return unix.Close(self.fd)
}
