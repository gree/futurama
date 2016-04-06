package main

import (
	"../../"
)

type NoConsumer struct {
	C chan []*futurama.Event
}

func (self *NoConsumer) Start() {}
func (self *NoConsumer) Stop() {}

func (self *NoConsumer) Events() <-chan []*futurama.Event {
	return self.C
}

func (self *NoConsumer) GetStat(reset bool) map[string]interface{} {
	return map[string]interface{}{}
}

