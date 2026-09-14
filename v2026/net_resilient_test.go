package connect

import (
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type immediateWriteConn struct {
	writeCount atomic.Int64
}

func (self *immediateWriteConn) Read(buffer []byte) (int, error) {
	return 0, io.EOF
}

func (self *immediateWriteConn) Write(buffer []byte) (int, error) {
	self.writeCount.Add(1)
	return len(buffer), nil
}

func (self *immediateWriteConn) Close() error {
	return nil
}

func (self *immediateWriteConn) LocalAddr() net.Addr {
	return nil
}

func (self *immediateWriteConn) RemoteAddr() net.Addr {
	return nil
}

func (self *immediateWriteConn) SetDeadline(deadline time.Time) error {
	return nil
}

func (self *immediateWriteConn) SetReadDeadline(deadline time.Time) error {
	return nil
}

func (self *immediateWriteConn) SetWriteDeadline(deadline time.Time) error {
	return nil
}

func TestResilientTlsConnOffWritesDirectly(t *testing.T) {
	underlying := &immediateWriteConn{}
	conn := NewResilientTlsConn(underlying, true, true)
	_ = conn.Off()
	_ = conn.Off()

	message := []byte("application data")
	n, err := conn.Write(message)
	if err != nil {
		t.Fatal(err)
	}
	AssertEqual(t, n, len(message))
	AssertEqual(t, underlying.writeCount.Load(), int64(1))
	AssertEqual(t, conn.Enabled(), false)
}

func BenchmarkResilientTlsConnDisabledWrite(b *testing.B) {
	underlying := &immediateWriteConn{}
	conn := NewResilientTlsConn(underlying, true, true)
	_ = conn.Off()
	message := make([]byte, 3*1024)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := conn.Write(message); err != nil {
			b.Fatal(err)
		}
	}
}
