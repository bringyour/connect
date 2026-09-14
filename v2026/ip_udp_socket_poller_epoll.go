//go:build linux || android

package connect

import "golang.org/x/sys/unix"

type udpSocketEpollBackend struct {
	fd       int
	osEvents [udpSocketPollEventCount]unix.EpollEvent
}

func newUdpSocketPollBackend() (udpSocketPollBackend, error) {
	fd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, err
	}
	return &udpSocketEpollBackend{fd: fd}, nil
}

func (self *udpSocketEpollBackend) add(fd int) error {
	return unix.EpollCtl(self.fd, unix.EPOLL_CTL_ADD, fd, &unix.EpollEvent{
		Events: unix.EPOLLIN | unix.EPOLLERR | unix.EPOLLHUP,
		Fd:     int32(fd),
	})
}

func (self *udpSocketEpollBackend) remove(fd int) {
	_ = unix.EpollCtl(self.fd, unix.EPOLL_CTL_DEL, fd, nil)
}

func (self *udpSocketEpollBackend) wait(events []udpSocketPollEvent) (int, error) {
	n, err := unix.EpollWait(self.fd, self.osEvents[:min(len(events), len(self.osEvents))], 100)
	for i := range n {
		event := self.osEvents[i]
		events[i] = udpSocketPollEvent{
			fd:       int(event.Fd),
			terminal: event.Events&(unix.EPOLLERR|unix.EPOLLHUP) != 0,
		}
	}
	return n, err
}

func (self *udpSocketEpollBackend) close() error {
	return unix.Close(self.fd)
}
