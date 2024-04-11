package connect

import (
    "time"
    "sync"
)

// events surfaced to the end user


type WindowExpandEvent struct {
    EventTime time.Time
    CurrentSize int
    TargetSize int
}


type ProviderState string
const (
    ProviderStateInEvaluation ProviderState = "InEvaluation"
    ProviderStateEvaluationFailed ProviderState = "EvaluationFailed"
    ProviderStateNotAdded ProviderState = "NotAdded"
    ProviderStateAdded ProviderState = "Added"
    ProviderStateRemoved ProviderState = "Removed"
)


type ProviderEvent struct {
    EventTime time.Time
    ClientId Id
    State ProviderState
}


func DefaultRemoteUserNatMultiClientMonitorSettings() *RemoteUserNatMultiClientMonitorSettings {
    return &RemoteUserNatMultiClientMonitorSettings{
        EventWindowDuration: 120 * time.Second,
    }
}


type RemoteUserNatMultiClientMonitorSettings struct {
    EventWindowDuration time.Duration
}


type RemoteUserNatMultiClientMonitor struct {
    settings *RemoteUserNatMultiClientMonitorSettings

    stateLock sync.Mutex

    windowExpandEvent *WindowExpandEvent
    providerEvents []*ProviderEvent
}

func NewRemoteUserNatMultiClientMonitorWithDefaults() *RemoteUserNatMultiClientMonitor {
    return NewRemoteUserNatMultiClientMonitor(DefaultRemoteUserNatMultiClientMonitorSettings())
}

func NewRemoteUserNatMultiClientMonitor(settings *RemoteUserNatMultiClientMonitorSettings) *RemoteUserNatMultiClientMonitor {
    return &RemoteUserNatMultiClientMonitor{
        settings: settings,
        windowExpandEvent: &WindowExpandEvent{
            EventTime: time.Now(),
            CurrentSize: 0,
            TargetSize: 0,
        },
    } 
}

// must be called with `stateLock`
func (self *RemoteUserNatMultiClientMonitor) coalesceProviderEvents() {
    windowStartTime := time.Now().Add(-self.settings.EventWindowDuration)

    i := 0
    for ; i < len(self.providerEvents) && self.providerEvents[i].EventTime.Before(windowStartTime); i += 1 {
        self.providerEvents[i] = nil
    }
    if 0 < i {
        self.providerEvents = self.providerEvents[i:]
    }
}

func (self *RemoteUserNatMultiClientMonitor) Events() (*WindowExpandEvent, map[Id]*ProviderEvent) {
    self.stateLock.Lock()
    defer self.stateLock.Unlock()

    self.coalesceProviderEvents()

    clientIdProviderEvents := map[Id]*ProviderEvent{}
    for _, providerEvent := range self.providerEvents {
        clientIdProviderEvents[providerEvent.ClientId] = providerEvent
    }

    return self.windowExpandEvent, clientIdProviderEvents
}

func (self *RemoteUserNatMultiClientMonitor) AddWindowExpandEvent(currentSize int, targetSize int) {
    self.stateLock.Lock()
    defer self.stateLock.Unlock()

    windowExpandEvent := &WindowExpandEvent{
        EventTime: time.Now(),
        CurrentSize: currentSize,
        TargetSize: targetSize,
    }

    self.windowExpandEvent = windowExpandEvent
}

func (self *RemoteUserNatMultiClientMonitor) AddProviderEvent(clientId Id, state ProviderState) {
    self.stateLock.Lock()
    defer self.stateLock.Unlock()

    providerEvent := &ProviderEvent{
        EventTime: time.Now(),
        ClientId: clientId,
        State: state,
    }

    self.providerEvents = append(self.providerEvents, providerEvent)
    self.coalesceProviderEvents()
}

