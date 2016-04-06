package futurama

import (
	"encoding/json"
	"flag"
	"github.com/golang/glog"
	"time"
)

func init() {
	flag.Set("v", "100")
}

const (
	Test_TriggerType_Default = "test-default"
	Test_TriggerType_Retry   = "test-retry"
	Test_TriggerType_Panic   = "test-panic"
)

type TestTrigger_Schedule struct {
	C chan string
}

func (self *TestTrigger_Schedule) Trigger(ev *Event) *TriggerResult {
	glog.Infoln("TestTrigger_Schedule", ev.Id)
	defer func() {
		self.C <- ev.Id
	}()

	return &TriggerResult{Status: EventStatus_OK}
}

type TestTrigger_Retry struct {
	C chan string
}

func (self *TestTrigger_Retry) Trigger(ev *Event) *TriggerResult {
	glog.Infoln("TestTrigger_Retry", ev.Id)
	defer func() {
		self.C <- ev.Id
	}()

	data := ev.Data.(map[string]interface{})
	numRetry, _ := data["NumRetry"].(json.Number).Int64()
	retryTimeNano, _ := data["RetryTimeNano"].(json.Number).Int64()
	var retryTime time.Time
	if retryTimeNano == 0 {
		retryTime = time.Time{}
	} else {
		retryTime = time.Unix(0, retryTimeNano)
	}
	if int(numRetry) == ev.Attempts {
		return &TriggerResult{Status: EventStatus_OK}
	} else {
		return &TriggerResult{EventStatus_RETRY, retryTime, nil}
	}
}

type TestTrigger_Panic struct{}

func (self *TestTrigger_Panic) Trigger(ev *Event) *TriggerResult {
	glog.Infoln("TestTrigger_Panic", ev.Id)
	panic("Expected panic")
	return nil
}

func SetupQueue(cfg *Config) (*Queue, chan string) {
	c := make(chan string, 64)
	q, _ := CreateQueue(cfg, map[string]TriggerInterface{
		Test_TriggerType_Default: &TestTrigger_Schedule{c},
		Test_TriggerType_Retry:   &TestTrigger_Retry{c},
		Test_TriggerType_Panic:   &TestTrigger_Panic{},
	})
	q.Start()
	return q, c
}
