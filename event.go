package futurama

import (
	"fmt"
	"time"
)

const (
	EventStatus_DEFAULT = 1 + iota
	EventStatus_OK
	EventStatus_CANCEL
	EventStatus_ERROR
	EventStatus_RETRY
)

var eventStatusText = []string{
	"UNKNOWN",
	"DEFAULT",
	"OK",
	"CANCEL",
	"ERROR",
	"RETRY",
}

type EventStatus uint32

func (self EventStatus) String() string {
	if int(self) >= len(eventStatusText) {
		return fmt.Sprintf("EXTENDED(%d)", self)
	}
	return eventStatusText[self]
}

type Event struct {
	Id          string
	TriggerType string
	TriggerTime time.Time
	Owner       string
	Attempts    int
	Status      EventStatus
	Created     time.Time
	Updated     time.Time
	Completed   time.Time
	Locked      time.Time
	Data        interface{}

	timer *time.Timer
}

func NewEvent(triggerType string, triggerTime time.Time, data interface{}) *Event {
	return &Event{
		TriggerType: triggerType,
		TriggerTime: triggerTime,
		Status:      EventStatus_DEFAULT,
		Created:     time.Now(),
		Data:        data,
	}
}
func (self *Event) String() string {
	return fmt.Sprintf("%s %d", self.Id, self.TriggerTime.Unix())
}

func (self *Event) Stop() {
	self.timer.Stop()
	self.timer = nil
}

func (self *Event) GetKey() string {
	return self.Id
}
