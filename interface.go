package futurama

import "time"

type StatInterface interface {
	GetStat(reset bool) map[string]interface{}
}

type StoreInterface interface {
	Open() error
	Close()
	Save(ev *Event) string
	Cancel(evId string) error
	UpdateStatus(evId string, status EventStatus) error
	UpdateForRetry(ev *Event, retryParam interface{}) error
}

type ConsumerInterface interface {
	Start()
	Stop()
	Events() <-chan []*Event
}

type TriggerResult struct {
	Status      EventStatus
	TriggerTime time.Time
	Data        interface{}
}

type TriggerInterface interface {
	Trigger(ev *Event) *TriggerResult
}
