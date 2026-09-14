// Contract requests expose only their fixed sender sequence lane as
// capability metadata; routing and contract behavior remain unchanged.
package connect

import (
	"context"
	"testing"

	"github.com/urnetwork/connect/v2026/protocol"
)

// Captures decoded contract requests while retaining the out-of-band frame
// ownership contract used by production implementations.
type contractSenderRoleCaptureOob struct {
	createContract *protocol.CreateContract
}

func (self *contractSenderRoleCaptureOob) SendControl(
	frames []*protocol.Frame,
	callback OobResultFunction,
) {
	for _, frame := range frames {
		message, err := FromFrame(frame)
		MessagePoolReturn(frame.MessageBytes)
		if err != nil {
			continue
		}
		if createContract, ok := message.(*protocol.CreateContract); ok {
			self.createContract = createContract
		}
	}
	callback(nil, nil)
}

// Every contract lane stamps one concrete role, including the zero-valued
// local client role. The optional field therefore distinguishes a capable
// sender from a peer that predates the diagnostic without using an artifact
// version or endpoint identity.
func TestCreateContractReportsSenderSequenceRole(t *testing.T) {
	roles := []sequenceTlsRole{sequenceTlsRoleClient, sequenceTlsRoleServer}
	for _, role := range roles {
		capture := &contractSenderRoleCaptureOob{}
		client := NewClient(context.Background(), NewId(), capture, DefaultClientSettings())
		client.ContractManager().CreateContract(ContractKey{
			Destination:    DestinationId(NewId()),
			EncryptionRole: role,
		}, 0, ByteCount(1024))
		client.Cancel()

		if capture.createContract == nil {
			t.Fatalf("%s contract request was not captured", role)
		}
		if capture.createContract.SenderRole == nil {
			t.Fatalf("%s sender role is absent", role)
		}
		if got, want := capture.createContract.GetSenderRole(), role.toProtobuf(); got != want {
			t.Fatalf("sender role = %s, want %s", got, want)
		}
	}
}
