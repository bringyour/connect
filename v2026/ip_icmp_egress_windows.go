//go:build windows

package connect

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// the windows echo backend (see ICMP.md): the sanctioned unprivileged echo
// api is a transaction, not a socket. one process-wide engine goroutine —
// locked to its os thread — issues every IcmpSendEcho2Ex/Icmp6SendEcho2 call
// and sleeps alertably; requests arrive as user apcs (QueueUserAPC) and
// completions as the calls' apc routines, so the whole backend costs one os
// thread regardless of flow count.
//
// the kernel assigns the wire identifier and sequence per call, so the flow's
// inner identifier never appears on the wire; the reply is synthesized from
// the transaction result, which is faithful whenever the status is success.
// parity with the unix backend: only IP_SUCCESS from the flow destination
// delivers a reply; every other status (unreachable, ttl expired, timeout) is
// silence. duplicates are absorbed by the kernel.
//
// self-exclusion has no socket to bind: the call's source address is resolved
// from the egress interface (SetEgressInterfaceIndex, maintained by the
// windows service), and windows' strong-host routing keeps the echo off the
// wintun adapter. on-device verification items: the Icmp6SendEcho2 behavior
// with an unspecified source, and the source-pinned bypass while providing.

var (
	icmpModIphlpapi           = windows.NewLazySystemDLL("iphlpapi.dll")
	icmpProcIcmpCreateFile    = icmpModIphlpapi.NewProc("IcmpCreateFile")
	icmpProcIcmp6CreateFile   = icmpModIphlpapi.NewProc("Icmp6CreateFile")
	icmpProcIcmpCloseHandle   = icmpModIphlpapi.NewProc("IcmpCloseHandle")
	icmpProcIcmpSendEcho2Ex   = icmpModIphlpapi.NewProc("IcmpSendEcho2Ex")
	icmpProcIcmp6SendEcho2    = icmpModIphlpapi.NewProc("Icmp6SendEcho2")
	icmpProcIcmpParseReplies  = icmpModIphlpapi.NewProc("IcmpParseReplies")
	icmpProcIcmp6ParseReplies = icmpModIphlpapi.NewProc("Icmp6ParseReplies")

	icmpModKernel32      = windows.NewLazySystemDLL("kernel32.dll")
	icmpProcQueueUserAPC = icmpModKernel32.NewProc("QueueUserAPC")
	icmpProcSleepEx      = icmpModKernel32.NewProc("SleepEx")
)

const (
	icmpErrorIoPending = 997 // ERROR_IO_PENDING

	// ip status codes (ipexport.h)
	icmpIpSuccess = 0

	// per-transaction kernel timeout. a reply after this is silence, which
	// ping tolerates; bounding it recycles the outstanding slots and reply
	// buffers of a dead destination.
	icmpTransactionTimeoutMillis = 10 * 1000

	// reply buffer slack over the request payload: the parsed reply record,
	// options, the documented 8 error bytes, and the io status block
	icmpReplyBufferSlack = 128
)

// IP_OPTION_INFORMATION (ipexport.h)
type icmpIpOptionInformation struct {
	ttl         uint8
	tos         uint8
	flags       uint8
	optionsSize uint8
	optionsData uintptr
}

// ICMP_ECHO_REPLY (ipexport.h). field alignment matches the c layout on both
// 386 and amd64/arm64 because the pointer-sized fields are pointer-aligned in
// go exactly as in c.
type icmpEchoReply struct {
	address       uint32
	status        uint32
	roundTripTime uint32
	dataSize      uint16
	reserved      uint16
	// points into the reply buffer after IcmpParseReplies
	data    uintptr
	options icmpIpOptionInformation
}

// ICMPV6_ECHO_REPLY (ipexport.h) is parsed by explicit offsets: its leading
// IPV6_ADDRESS_EX is 1-packed (26 bytes: port 0, flowinfo 2, addr 6, scope
// 22) while the outer struct uses default packing, putting status at 28 and
// round trip time at 32 with the reply data following at 36. on-device
// verification item (see ICMP.md).
const (
	icmp6ReplyAddrOffset   = 6
	icmp6ReplyStatusOffset = 28
	icmp6ReplyDataOffset   = 36
)

// one in-flight echo transaction. reachable from the engine table until its
// completion apc runs, which keeps the request and reply buffers alive while
// the kernel writes them (go's heap is non-moving).
type icmpTransaction struct {
	flow           *icmpEgressTransactor
	sequenceNumber uint16
	ttl            uint8
	request        []byte
	reply          []byte
}

type icmpEchoReplyItem struct {
	sequenceNumber uint16
	payload        []byte
}

// the process-wide engine (see the file header)
type icmpEngine struct {
	mutex        sync.Mutex
	threadHandle windows.Handle
	nextKey      uintptr
	transactions map[uintptr]*icmpTransaction

	issueCallback    uintptr
	completeCallback uintptr
}

var icmpEngineInstance = sync.OnceValues(func() (*icmpEngine, error) {
	engine := &icmpEngine{
		transactions: map[uintptr]*icmpTransaction{},
	}
	// NewCallback allocations are process-permanent; exactly two exist
	engine.issueCallback = windows.NewCallback(engine.issueApc)
	engine.completeCallback = windows.NewCallback(engine.completeApc)

	started := make(chan error)
	go func() {
		// apcs execute on this locked thread while it sleeps alertably
		runtime.LockOSThread()
		threadHandle, err := func() (windows.Handle, error) {
			var threadHandle windows.Handle
			process := windows.CurrentProcess()
			err := windows.DuplicateHandle(
				process,
				windows.CurrentThread(),
				process,
				&threadHandle,
				0,
				false,
				windows.DUPLICATE_SAME_ACCESS,
			)
			return threadHandle, err
		}()
		if err != nil {
			started <- err
			return
		}
		engine.threadHandle = threadHandle
		started <- nil
		for {
			// alertable infinite sleep; every wake is an apc
			icmpProcSleepEx.Call(uintptr(windows.INFINITE), 1)
		}
	}()
	if err := <-started; err != nil {
		return nil, err
	}
	return engine, nil
})

// registers the transaction and queues its issue apc to the engine thread
func (self *icmpEngine) submit(tx *icmpTransaction) bool {
	key := func() uintptr {
		self.mutex.Lock()
		defer self.mutex.Unlock()
		self.nextKey += 1
		key := self.nextKey
		self.transactions[key] = tx
		return key
	}()
	r1, _, _ := icmpProcQueueUserAPC.Call(self.issueCallback, uintptr(self.threadHandle), key)
	if r1 == 0 {
		self.remove(key)
		return false
	}
	return true
}

func (self *icmpEngine) remove(key uintptr) *icmpTransaction {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	tx := self.transactions[key]
	delete(self.transactions, key)
	return tx
}

func (self *icmpEngine) get(key uintptr) *icmpTransaction {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return self.transactions[key]
}

// PAPCFUNC: issues one registered transaction on the engine thread
func (self *icmpEngine) issueApc(key uintptr) uintptr {
	tx := self.get(key)
	if tx == nil {
		return 0
	}
	flow := tx.flow

	options := &icmpIpOptionInformation{
		ttl: tx.ttl,
	}
	if options.ttl == 0 {
		options.ttl = 64
	}

	var r1 uintptr
	var callErr error
	if flow.ipVersion == 4 {
		r1, _, callErr = icmpProcIcmpSendEcho2Ex.Call(
			uintptr(flow.icmpHandle),
			0, // event
			self.completeCallback,
			key,
			uintptr(flow.sourceAddr4),
			uintptr(flow.destinationAddr4),
			uintptr(unsafe.Pointer(&tx.request[0])),
			uintptr(uint16(len(tx.request))),
			uintptr(unsafe.Pointer(options)),
			uintptr(unsafe.Pointer(&tx.reply[0])),
			uintptr(uint32(len(tx.reply))),
			uintptr(uint32(icmpTransactionTimeoutMillis)),
		)
	} else {
		r1, _, callErr = icmpProcIcmp6SendEcho2.Call(
			uintptr(flow.icmpHandle),
			0, // event
			self.completeCallback,
			key,
			uintptr(unsafe.Pointer(&flow.sourceSockaddr6)),
			uintptr(unsafe.Pointer(&flow.destinationSockaddr6)),
			uintptr(unsafe.Pointer(&tx.request[0])),
			uintptr(uint16(len(tx.request))),
			uintptr(unsafe.Pointer(options)),
			uintptr(unsafe.Pointer(&tx.reply[0])),
			uintptr(uint32(len(tx.reply))),
			uintptr(uint32(icmpTransactionTimeoutMillis)),
		)
	}
	if r1 == 0 {
		if errno, ok := callErr.(windows.Errno); !ok || uintptr(errno) != icmpErrorIoPending {
			// immediate failure: drop the transaction (silence)
			if self.remove(key) != nil {
				flow.outstanding.Add(-1)
			}
		}
	} else {
		// completed synchronously; the completion apc still runs
	}
	return 0
}

// PIO_APC_ROUTINE: parses one completed transaction and delivers a matching
// success reply to its flow
func (self *icmpEngine) completeApc(key uintptr, ioStatusBlock uintptr, reserved uintptr) uintptr {
	tx := self.remove(key)
	if tx == nil {
		return 0
	}
	flow := tx.flow
	flow.outstanding.Add(-1)

	if flow.ipVersion == 4 {
		r1, _, _ := icmpProcIcmpParseReplies.Call(
			uintptr(unsafe.Pointer(&tx.reply[0])),
			uintptr(uint32(len(tx.reply))),
		)
		if r1 == 0 {
			return 0
		}
		reply := (*icmpEchoReply)(unsafe.Pointer(&tx.reply[0]))
		if reply.status != icmpIpSuccess {
			return 0
		}
		var replyAddr [4]byte
		binary.LittleEndian.PutUint32(replyAddr[:], reply.address)
		if !net.IP(replyAddr[:]).Equal(flow.destinationIp) {
			return 0
		}
		if reply.data == 0 || reply.dataSize == 0 {
			flow.deliver(tx.sequenceNumber, nil)
			return 0
		}
		payload := unsafe.Slice((*byte)(unsafe.Pointer(reply.data)), int(reply.dataSize))
		flow.deliver(tx.sequenceNumber, payload)
	} else {
		r1, _, _ := icmpProcIcmp6ParseReplies.Call(
			uintptr(unsafe.Pointer(&tx.reply[0])),
			uintptr(uint32(len(tx.reply))),
		)
		if r1 == 0 {
			return 0
		}
		if len(tx.reply) < icmp6ReplyDataOffset {
			return 0
		}
		status := binary.LittleEndian.Uint32(tx.reply[icmp6ReplyStatusOffset : icmp6ReplyStatusOffset+4])
		if status != icmpIpSuccess {
			return 0
		}
		replyAddr := net.IP(tx.reply[icmp6ReplyAddrOffset : icmp6ReplyAddrOffset+16])
		if !replyAddr.Equal(flow.destinationIp) {
			return 0
		}
		// the v6 reply data follows the reply record and mirrors the request
		// length; bound by both
		payloadByteCount := min(len(tx.request), len(tx.reply)-icmp6ReplyDataOffset)
		flow.deliver(tx.sequenceNumber, tx.reply[icmp6ReplyDataOffset:icmp6ReplyDataOffset+payloadByteCount])
	}
	return 0
}

// one flow's transactor: an icmp handle plus the resolved source/destination
// addresses, feeding replies to ReadEcho through a bounded channel
type icmpEgressTransactor struct {
	ctx    context.Context
	cancel context.CancelFunc

	engine        *icmpEngine
	ipVersion     int
	destinationIp net.IP
	icmpHandle    uintptr

	sourceAddr4          uint32
	destinationAddr4     uint32
	sourceSockaddr6      windows.RawSockaddrInet6
	destinationSockaddr6 windows.RawSockaddrInet6

	outstanding      atomic.Int64
	outstandingLimit int64

	replies chan icmpEchoReplyItem
}

func newIcmpEgress(ctx context.Context, ipVersion int, destinationIp net.IP, settings *IcmpBufferSettings) (icmpEgress, error) {
	engine, err := icmpEngineInstance()
	if err != nil {
		return nil, err
	}

	var icmpHandle uintptr
	var callErr error
	switch ipVersion {
	case 4:
		icmpHandle, _, callErr = icmpProcIcmpCreateFile.Call()
	case 6:
		icmpHandle, _, callErr = icmpProcIcmp6CreateFile.Call()
	default:
		return nil, icmpEgressUnsupportedError(ipVersion)
	}
	// INVALID_HANDLE_VALUE
	if icmpHandle == ^uintptr(0) {
		return nil, fmt.Errorf("Icmp create failed: %v", callErr)
	}

	outstandingLimit := int64(settings.OutstandingLimit)
	if outstandingLimit <= 0 {
		outstandingLimit = 4
	}

	cancelCtx, cancel := context.WithCancel(ctx)
	transactor := &icmpEgressTransactor{
		ctx:              cancelCtx,
		cancel:           cancel,
		engine:           engine,
		ipVersion:        ipVersion,
		destinationIp:    destinationIp,
		icmpHandle:       icmpHandle,
		outstandingLimit: outstandingLimit,
		replies:          make(chan icmpEchoReplyItem, outstandingLimit),
	}

	index4, index6 := EgressInterfaceIndex()
	switch ipVersion {
	case 4:
		destinationIp4 := destinationIp.To4()
		if destinationIp4 == nil {
			transactor.Close()
			return nil, icmpEgressUnsupportedError(ipVersion)
		}
		// IPAddr is a network-order in_addr carried in a little-endian dword
		transactor.destinationAddr4 = binary.LittleEndian.Uint32(destinationIp4)
		if sourceIp := icmpEgressSourceIp(index4, windows.AF_INET); sourceIp != nil {
			transactor.sourceAddr4 = binary.LittleEndian.Uint32(sourceIp.To4())
		}
	case 6:
		destinationIp16 := destinationIp.To16()
		if destinationIp16 == nil {
			transactor.Close()
			return nil, icmpEgressUnsupportedError(ipVersion)
		}
		transactor.destinationSockaddr6 = windows.RawSockaddrInet6{
			Family: windows.AF_INET6,
			Addr:   [16]byte(destinationIp16),
		}
		transactor.sourceSockaddr6 = windows.RawSockaddrInet6{
			Family: windows.AF_INET6,
		}
		if sourceIp := icmpEgressSourceIp(index6, windows.AF_INET6); sourceIp != nil {
			transactor.sourceSockaddr6.Addr = [16]byte(sourceIp.To16())
		}
	}

	return transactor, nil
}

// resolves the first unicast address of the family on the egress interface.
// index 0 or no match resolves nil, which leaves the source unspecified (the
// stack routes by its own table).
func icmpEgressSourceIp(interfaceIndex uint32, family uint32) net.IP {
	if interfaceIndex == 0 {
		return nil
	}
	adapterAddresses, err := icmpEgressAdapterAddresses(family)
	if err != nil {
		return nil
	}
	for adapterAddress := adapterAddresses; adapterAddress != nil; adapterAddress = adapterAddress.Next {
		index := adapterAddress.IfIndex
		if family == windows.AF_INET6 {
			index = adapterAddress.Ipv6IfIndex
		}
		if index != interfaceIndex {
			continue
		}
		for unicastAddress := adapterAddress.FirstUnicastAddress; unicastAddress != nil; unicastAddress = unicastAddress.Next {
			if sourceIp := unicastAddress.Address.IP(); sourceIp != nil {
				return sourceIp
			}
		}
	}
	return nil
}

func icmpEgressAdapterAddresses(family uint32) (*windows.IpAdapterAddresses, error) {
	byteCount := uint32(15 * 1024)
	for i := 0; i < 3; i += 1 {
		buffer := make([]byte, byteCount)
		adapterAddresses := (*windows.IpAdapterAddresses)(unsafe.Pointer(&buffer[0]))
		err := windows.GetAdaptersAddresses(
			family,
			windows.GAA_FLAG_SKIP_ANYCAST|windows.GAA_FLAG_SKIP_MULTICAST|windows.GAA_FLAG_SKIP_DNS_SERVER,
			0,
			adapterAddresses,
			&byteCount,
		)
		if err == nil {
			return adapterAddresses, nil
		}
		if err != windows.ERROR_BUFFER_OVERFLOW {
			return nil, err
		}
	}
	return nil, windows.ERROR_BUFFER_OVERFLOW
}

// delivers one reply toward ReadEcho; a full channel drops (ping tolerates)
func (self *icmpEgressTransactor) deliver(sequenceNumber uint16, payload []byte) {
	item := icmpEchoReplyItem{
		sequenceNumber: sequenceNumber,
	}
	if 0 < len(payload) {
		// the transaction buffer is freed with the transaction; copy out
		item.payload = make([]byte, len(payload))
		copy(item.payload, payload)
	}
	select {
	case <-self.ctx.Done():
	case self.replies <- item:
	default:
	}
}

func (self *icmpEgressTransactor) WriteEcho(deadline time.Time, ttl int, sequenceNumber uint16, payload []byte) error {
	select {
	case <-self.ctx.Done():
		return fmt.Errorf("Done.")
	default:
	}
	if self.outstandingLimit <= self.outstanding.Load() {
		// over the cap: drop, which ping tolerates. not a flow error.
		return nil
	}

	tx := &icmpTransaction{
		flow:           self,
		sequenceNumber: sequenceNumber,
		ttl:            uint8(min(max(ttl, 0), 255)),
		request:        make([]byte, len(payload)),
		reply:          make([]byte, len(payload)+icmpReplyBufferSlack),
	}
	copy(tx.request, payload)
	if len(tx.request) == 0 {
		// the api requires a nonzero request buffer
		tx.request = []byte{0}
	}

	self.outstanding.Add(1)
	if !self.engine.submit(tx) {
		self.outstanding.Add(-1)
	}
	return nil
}

func (self *icmpEgressTransactor) ReadEcho(deadline time.Time) (sequenceNumber uint16, payload []byte, err error) {
	timeout := time.Until(deadline)
	if timeout <= 0 {
		return 0, nil, fmt.Errorf("Read deadline.")
	}
	select {
	case <-self.ctx.Done():
		return 0, nil, fmt.Errorf("Done.")
	case item := <-self.replies:
		return item.sequenceNumber, item.payload, nil
	case <-time.After(timeout):
		return 0, nil, fmt.Errorf("Read deadline.")
	}
}

func (self *icmpEgressTransactor) Close() {
	self.cancel()
	// outstanding transactions complete with a cancelled status and drop
	icmpProcIcmpCloseHandle.Call(self.icmpHandle)
}
