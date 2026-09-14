// Opt-in durable registration uses the existing processed control operation.
// One publisher owns one in-flight key; rotations coalesce without retaining
// a goroutine or payload per rotation. Methods are safe for concurrent use.
package connect

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"

	"github.com/urnetwork/connect/v2026/protocol"
)

// The local publication generation also distinguishes an a-b-a rotation.
// Returning to the same key does not revive an older pending acknowledgment.
type clientKeyRegistrationGeneration struct {
	publicKey  [ed25519.PublicKeySize]byte
	generation uint64
}

// Only a processed response for the active key makes this true. Legacy
// delivery acknowledgments and disabled registration never imply readiness.
func (self *ClientKeyManager) Registered() bool {
	self.stateLock.RLock()
	defer self.stateLock.RUnlock()
	return !self.closed && self.ctx.Err() == nil && self.registrationReady != nil && self.registrationReady.Value()
}

// Waits for processed registration with caller and manager cancellation. A
// later rotation invalidates readiness again; callers still check their live
// member before admitting new demand.
func (self *ClientKeyManager) WaitForRegistration(ctx context.Context) error {
	if ctx == nil {
		return errors.New("client key registration wait has no context")
	}
	if self.registrationReady == nil {
		return errors.New("processed client key registration is not enabled")
	}
	for {
		ready, changed := self.registrationReady.Get()
		if err := errors.Join(ctx.Err(), self.ctx.Err()); err != nil {
			return err
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-self.ctx.Done():
			return self.ctx.Err()
		case <-changed:
		}
	}
}

// Serializes publication through processed completion. A seed change cannot
// overtake a paused old publisher, and an old response cannot mark the changed
// key ready. Transient errors use ControlSyncOob's existing bounded request,
// one-second retry and caller-owned cancellation, not transport acknowledgments.
func (self *ClientKeyManager) publishRegisteredClientKey() {
	select {
	case <-self.client.ReadyNotify():
	case <-self.ctx.Done():
		return
	}
	for {
		generation, _ := self.registrationUpdates.Get()
		if self.client.settings.beforeClientKeyPublishForTest != nil {
			self.client.settings.beforeClientKeyPublishForTest()
		}
		if self.ctx.Err() != nil {
			return
		}
		frame, err := ToFrame(&protocol.ClientKey{PublicKey: bytes.Clone(generation.publicKey[:])}, self.client.settings.ProtocolVersion)
		if err != nil {
			self.client.log.Errorf("[key]%s could not build registration frame: %s", self.client.ClientTag(), err)
			return
		}
		completed := make(chan struct{}, 1)
		self.registrationSync.Send(frame, func(err error) {
			if err == nil {
				select {
				case completed <- struct{}{}:
				default:
				}
			}
		})
		select {
		case <-self.ctx.Done():
			return
		case <-completed:
		}
		self.stateLock.Lock()
		if !self.closed && self.ctx.Err() == nil && self.registrationUpdates.Value() == generation {
			self.registrationReady.Set(true)
		}
		self.stateLock.Unlock()
		for {
			current, changed := self.registrationUpdates.Get()
			if current != generation {
				break
			}
			select {
			case <-self.ctx.Done():
				return
			case <-changed:
			}
		}
	}
}
