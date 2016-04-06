package main

import (
	"../../"
)

type TriggerParam struct {
	Status futurama.EventStatus
	Retry  int
}

const TriggerType_NoStore = "nostore"

type Trigger struct{}

func (self *Trigger) Trigger(ev *futurama.Event) *futurama.TriggerResult {
	param := ev.Data.(*TriggerParam)
	var status futurama.EventStatus
	if int(param.Retry) <= ev.Attempts {
		status = futurama.EventStatus(param.Status)
	} else {
		status = futurama.EventStatus_RETRY
	}
	return &futurama.TriggerResult{Status: status}
}

