package main

import (
	"../../"
	"github.com/satori/go.uuid"
)

type NoStore struct {
	eventChan chan []*futurama.Event
}

func (self *NoStore) Open() error{ return nil }
func (self *NoStore) Close() {}

func (self *NoStore) Save(ev *futurama.Event) string {
	ev.Id = uuid.NewV1().String()
	self.eventChan <- []*futurama.Event{ev}
	return ev.Id
}

func (self *NoStore) Cancel(evId string) error {
	ev := &futurama.Event{
		Id: evId,
		Status: futurama.EventStatus_CANCEL,
	}
	self.eventChan <- []*futurama.Event{ev}
	return nil
}

func (self *NoStore) UpdateStatus(evId string, status futurama.EventStatus) error {
	return nil
}

func (self *NoStore) UpdateForRetry(ev *futurama.Event, retryParan interface{}) error {
	self.eventChan <- []*futurama.Event{ev}
	return nil
}

func (self *NoStore) GetStat(reset bool) map[string]interface{} {
	return map[string]interface{}{}
}

