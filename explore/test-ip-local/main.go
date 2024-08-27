package main

// ip-local <interface name>
// reads raw packets from the interface, writes

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sys/unix"

	"bringyour.com/connect"
	"bringyour.com/protocol"
)

// see https://android.googlesource.com/platform/development/+/master/samples/ToyVpn/server/linux/ToyVpnServer.cpp

func main() {
	log := connect.LogFn(connect.LogLevelInfo, "test-ip-local")
	connect.GlobalLogLevel = connect.LogLevelDebug

	interfaceName := "tun0"
	clientPath := connect.Path{
		ClientId: connect.Id{},
		StreamId: connect.Id{},
	}

	ctx, cancel := context.WithCancel(context.Background())

	// unix.O_NONBLOCK
	fd, err := unix.Open("/dev/net/tun", unix.O_RDWR, 0)
	if err != nil {
		panic(err)
	}
	defer unix.Close(fd)

	ifreq, err := unix.NewIfreq(interfaceName)
	if err != nil {
		panic(err)
	}
	// flags
	// IFF_TUN taptun interface
	// IFF_NO_PI no protocol info header on each read packet
	ifreq.SetUint16(unix.IFF_TUN | unix.IFF_NO_PI)

	if err := unix.IoctlIfreq(fd, unix.TUNSETIFF, ifreq); err != nil {
		panic(err)
	}

	signalNotify := make(chan os.Signal)
	signal.Notify(signalNotify, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		<-signalNotify
		cancel()
	}()

	// FIXME listen for quit signal

	// FIXME create remote nat

	remoteUserNat := connect.NewRemoteUserNatWithDefaults(ctx)
	go remoteUserNat.Run()

	remoteUserNat.AddSendPacketCallback(func(destination connect.Path, packet []byte) {
		// FIXME log the packet
		if destination == clientPath {
			//log("write %x", packet)
			_, err := unix.Write(fd, packet)
			if err != nil {
				panic(err)
			}
		} else {
			log("discard %x", packet)
		}
	})

	// FIXME start read loop (log packets)
	// FIXME start write loop (log packets)

	buffer := make([]byte, 2048)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		n, err := unix.Read(fd, buffer)
		if err != nil {
			panic(err)
		}
		if 0 < n {
			packet := make([]byte, n)
			copy(packet, buffer[0:n])
			// FIXME log
			remoteUserNat.Receive(clientPath, protocol.ProvideMode_PUBLIC, packet)
		}
	}

	// var ifr struct {
	//     name  [16]byte
	//     flags uint16
	//     _     [22]byte
	// }
	// copy(ifr.name[:], "tun0")
	// ifr.flags = unix.IFF_TUN | unix.IFF_NO_PI
	// _, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), unix.TUNSETIFF, uintptr(unsafe.Pointer(&ifr)))
	// if errno != 0 {
	//     log.Println("err to syscall:", errno)
	//     return
	// }
}
