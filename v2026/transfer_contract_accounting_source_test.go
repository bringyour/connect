package connect

import (
	"testing"

	"github.com/urnetwork/connect/v2026/protocol"
)

func TestSendPackContractAccountingUsesSerializedFrames(t *testing.T) {
	pack := &SendPack{
		Frame: &protocol.Frame{
			MessageBytes: make([]byte, 49),
		},
		// Reproduce the stale cached size seen after a borrowed empty IpPing
		// frame escaped its receive callback and was reused before send.
		MessageByteCount: 0,
	}

	if got, want := pack.serializedMessageByteCount(), ByteCount(49); got != want {
		t.Fatalf("serialized byte count = %d, want %d", got, want)
	}

	second := &SendPack{
		Frame: &protocol.Frame{
			MessageBytes: make([]byte, 39),
		},
		MessageByteCount: 1,
	}
	if got, want := pack.serializedMessageByteCount()+second.serializedMessageByteCount(), ByteCount(88); got != want {
		t.Fatalf("coalesced serialized byte count = %d, want %d", got, want)
	}
}
