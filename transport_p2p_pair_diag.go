package connect

import (
	"fmt"
)

// selectedCandidatePairTypes reports the ICE candidate types of the selected
// pair as "local->remote" (host, srflx, prflx, relay), or "" when no pair is
// selected yet. Read once per connection for diagnostics.
func (self *peerConn) selectedCandidatePairTypes() string {
	if self == nil || self.pc == nil {
		return ""
	}
	sctp := self.pc.SCTP()
	if sctp == nil {
		return ""
	}
	dtls := sctp.Transport()
	if dtls == nil {
		return ""
	}
	ice := dtls.ICETransport()
	if ice == nil {
		return ""
	}
	pair, err := ice.GetSelectedCandidatePair()
	if err != nil || pair == nil || pair.Local == nil || pair.Remote == nil {
		return ""
	}
	return fmt.Sprintf("%s->%s", pair.Local.Typ.String(), pair.Remote.Typ.String())
}

// recordP2pSelectedPair stores the selected pair types of conn in stats when
// conn is a WebRTC peer connection. A no-op for any other carrier.
func recordP2pSelectedPair(stats *P2pDataPlaneStats, conn any) {
	if stats == nil {
		return
	}
	if pc, ok := conn.(*peerConn); ok {
		stats.recordSelectedCandidatePair(pc.selectedCandidatePairTypes())
	}
}
