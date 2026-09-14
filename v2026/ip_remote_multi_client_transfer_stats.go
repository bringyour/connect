package connect

// MultiClientTransferStats is one window client's Transfer counters, read for
// diagnostics only. The multi client owns its window clients and exposes no
// other way to reach their send-recovery and receive statistics; the device
// diagnostic logger (sdk transfer_diag.go) reads this once per interval.
type MultiClientTransferStats struct {
	WindowType   WindowType
	Destination  MultiHopId
	SendRecovery ClientSendRecoveryStatsSnapshot
	Receive      ClientReceiveStatsSnapshot
}

// ClientTransferStats snapshots every active window client. Safe to call
// concurrently with the multi client; each window lock is held only while its
// client list is copied and the per-client snapshots are atomic reads.
func (self *RemoteUserNatMultiClient) ClientTransferStats() []MultiClientTransferStats {
	if self == nil {
		return nil
	}
	out := []MultiClientTransferStats{}
	for _, windowType := range []WindowType{WindowTypeQuality, WindowTypeSpeed} {
		window := self.windows[windowType]
		if window == nil {
			continue
		}
		window.stateLock.Lock()
		channels := make([]*multiClientChannel, 0, len(window.clients))
		for _, channel := range window.clients {
			if channel != nil && channel.client != nil {
				channels = append(channels, channel)
			}
		}
		window.stateLock.Unlock()
		for _, channel := range channels {
			stats := MultiClientTransferStats{
				WindowType:   windowType,
				SendRecovery: channel.client.SendRecoveryStats(),
				Receive:      channel.client.ReceiveStats(),
			}
			if channel.args != nil {
				stats.Destination = channel.args.Destination
			}
			out = append(out, stats)
		}
	}
	return out
}
