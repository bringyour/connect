package connect

import (
	"context"
	"errors"
	"net"
	"sync"
	"syscall"
)

const udpSocketPollEventCount = 64

type udpSocketPollEvent struct {
	fd       int
	terminal bool
}

// udpSocketPollBackend is implemented with epoll on Linux/Android and kqueue
// on Darwin/iOS. The portable fallback returns nil from
// newUdpSocketPollBackend, leaving UdpSequence's established reader intact.
type udpSocketPollBackend interface {
	add(fd int) error
	remove(fd int)
	wait(events []udpSocketPollEvent) (int, error)
	close() error
}

type udpSocketReadRegistration struct {
	sequence *UdpSequence
	socket   net.Conn
	rawConn  syscall.RawConn
}

type udpSocketReadPollShard struct {
	ctx        context.Context
	backend    udpSocketPollBackend
	readBuffer []byte
	mutex      sync.RWMutex
	byFd       map[int]udpSocketReadRegistration
}

type udpSocketReadPoller struct {
	shards    []udpSocketReadPollShard
	waitGroup sync.WaitGroup
}

func newUdpSocketReadPoller(
	ctx context.Context,
	settings *UdpBufferSettings,
) *udpSocketReadPoller {
	if settings == nil || settings.SocketReadShardCount <= 0 {
		return nil
	}
	shards := make([]udpSocketReadPollShard, 0, settings.SocketReadShardCount)
	for range settings.SocketReadShardCount {
		backend, err := newUdpSocketPollBackend()
		if err != nil {
			for i := range shards {
				_ = shards[i].backend.close()
			}
			return nil
		}
		shards = append(shards, udpSocketReadPollShard{
			ctx:        ctx,
			backend:    backend,
			readBuffer: make([]byte, settings.ReadBufferByteCount),
			byFd:       map[int]udpSocketReadRegistration{},
		})
	}
	poller := &udpSocketReadPoller{shards: shards}
	for i := range poller.shards {
		shard := &poller.shards[i]
		poller.waitGroup.Add(1)
		go HandleError(func() {
			defer poller.waitGroup.Done()
			shard.run()
		})
	}
	return poller
}

// Completion means every readiness shard has stopped reading and can no
// longer publish a packet into the receive dispatcher.
func (self *udpSocketReadPoller) waitForLifecycle() {
	if self == nil {
		return
	}
	self.waitGroup.Wait()
}

func socketRawConn(socket net.Conn) (syscall.RawConn, bool) {
	syscallConn, ok := socket.(syscall.Conn)
	if !ok {
		return nil, false
	}
	rawConn, err := syscallConn.SyscallConn()
	return rawConn, err == nil
}

func (self *udpSocketReadPoller) register(sequence *UdpSequence, socket net.Conn) bool {
	if self == nil || sequence == nil || socket == nil || len(self.shards) == 0 {
		return false
	}
	rawConn, ok := socketRawConn(socket)
	if !ok {
		return false
	}
	shardIndex := sequence.receiveShard % len(self.shards)
	shard := &self.shards[shardIndex]
	fd := -1
	var registerErr error
	controlErr := rawConn.Control(func(rawFd uintptr) {
		fd = int(rawFd)
		shard.mutex.Lock()
		shard.byFd[fd] = udpSocketReadRegistration{
			sequence: sequence,
			socket:   socket,
			rawConn:  rawConn,
		}
		registerErr = shard.backend.add(fd)
		if registerErr != nil {
			delete(shard.byFd, fd)
		}
		shard.mutex.Unlock()
	})
	if controlErr != nil || registerErr != nil || fd < 0 {
		return false
	}
	sequence.socketReadPollShard = shardIndex
	sequence.socketReadPollFd = fd
	return true
}

func (self *udpSocketReadPoller) unregister(sequence *UdpSequence) {
	if self == nil || sequence == nil || len(self.shards) == 0 {
		return
	}
	shardIndex := sequence.socketReadPollShard
	if shardIndex < 0 || len(self.shards) <= shardIndex {
		return
	}
	fd := sequence.socketReadPollFd
	if fd < 0 {
		return
	}
	shard := &self.shards[shardIndex]
	shard.mutex.Lock()
	if registration, ok := shard.byFd[fd]; ok && registration.sequence == sequence {
		shard.backend.remove(fd)
		delete(shard.byFd, fd)
	}
	shard.mutex.Unlock()
	sequence.socketReadPollFd = -1
}

func (self *udpSocketReadPollShard) registration(fd int) (udpSocketReadRegistration, bool) {
	self.mutex.RLock()
	registration, ok := self.byFd[fd]
	self.mutex.RUnlock()
	return registration, ok
}

func (self *udpSocketReadPollShard) run() {
	defer self.backend.close()
	events := make([]udpSocketPollEvent, udpSocketPollEventCount)
	for {
		select {
		case <-self.ctx.Done():
			return
		default:
		}
		eventCount, err := self.backend.wait(events)
		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			select {
			case <-self.ctx.Done():
				return
			default:
			}
			return
		}
		for _, event := range events[:eventCount] {
			registration, ok := self.registration(event.fd)
			if !ok {
				continue
			}
			readErr := drainReadyUdpRegistration(
				event.fd,
				registration,
				self.readBuffer,
			)
			if event.terminal || (readErr != nil && !errors.Is(readErr, syscall.EAGAIN) &&
				!errors.Is(readErr, syscall.EWOULDBLOCK)) {
				registration.sequence.Close()
			}
		}
	}
}

// RawConn.Read keeps the descriptor valid for the complete drain callback.
// A readiness event may already be queued when another goroutine unregisters
// and closes the socket; pinning the descriptor prevents an fd reused by a new
// flow from being read into the old sequence.
func drainReadyUdpRegistration(
	fd int,
	registration udpSocketReadRegistration,
	buffer []byte,
) error {
	if registration.rawConn == nil {
		return syscall.EBADF
	}
	var readErr error
	rawErr := registration.rawConn.Read(func(rawFd uintptr) bool {
		if int(rawFd) != fd {
			readErr = syscall.EBADF
			return true
		}
		readErr = drainReadyUdpSocket(fd, registration.sequence, buffer)
		// The custom readiness backend, not the runtime poller, owns the next
		// wait. Even EAGAIN completes this RawConn operation.
		return true
	})
	if rawErr != nil {
		return rawErr
	}
	return readErr
}

func drainReadyUdpSocket(fd int, sequence *UdpSequence, buffer []byte) error {
	if sequence == nil || len(buffer) == 0 {
		return syscall.EINVAL
	}
	maxReads := max(1, sequence.udpBufferSettings.WriteBatchSize)
	for range maxReads {
		n, err := syscall.Read(SocketHandle(fd), buffer)
		if 0 < n {
			sequence.UpdateLastActivityTime()
			packets, packetsErr := sequence.DataPackets(buffer, n, sequence.udpBufferSettings.Mtu)
			if packetsErr != nil {
				return packetsErr
			}
			for _, packet := range packets {
				if sequence.receiveDispatcher == nil {
					sequence.singleDataPacket[0] = packet
					sequence.receiveBatch(sequence.singleDataPacket[:])
				} else if !sequence.receiveDispatcher.enqueue(sequence, packet) {
					MessagePoolReturn(packet)
				}
			}
		}
		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			return err
		}
		if n == 0 {
			return nil
		}
	}
	return nil
}
