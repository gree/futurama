package main

import (
	"../../"
	"flag"
	"github.com/golang/glog"
	"time"
)

func main() {
	defer glog.Flush()
	flag.Parse()

	config := futurama.DefaultConfig()

	noConsumer := &NoConsumer{make(chan []*futurama.Event, 1024)}
	noStore    := &NoStore{noConsumer.C}
	q, err := futurama.CreateCustomQueue(config, map[string]futurama.TriggerInterface{
		TriggerType_NoStore: &Trigger{},
	}).Populate(noStore, noConsumer)
	if err != nil {
		glog.Errorln("Queue create:", err)
		return
	}
	if err := q.Start(); err != nil {
		glog.Errorln("Queue start:", err)
		return
	}
	defer q.Stop()

	glog.Infoln("Trigger an event after 3sec")
	q.Create(TriggerType_NoStore, time.Now().Add(3 * time.Second), &TriggerParam{futurama.EventStatus_OK, 2})
	time.Sleep(5 * time.Second)

	glog.Infoln("Trigger an event after 5sec")
	evId := q.Create(TriggerType_NoStore, time.Now().Add(5 * time.Second), &TriggerParam{futurama.EventStatus_OK, 0})
	glog.Infoln("Event created", evId)
	time.Sleep(2 * time.Second)

	q.Cancel(evId)

	time.Sleep(5 * time.Second)
}

