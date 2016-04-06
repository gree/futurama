package futurama

import (
	"fmt"
	"github.com/facebookgo/inject"
	"github.com/golang/glog"
	"time"
)

type QueueDepsContainer struct {
	Store    StoreInterface    `inject:""`
	Consumer ConsumerInterface `inject:""`
}

type Queue struct {
	QueueDepsContainer `inject:"inline"`

	scheduler *Scheduler
	stat      *Stat
	quitChan  chan chan bool

	nbTriggered Seq32
	nbGiveup    Seq32
}

func CreateCustomQueue(cfg *Config, triggers map[string]TriggerInterface) *Queue {
	scheduler := newScheduler(cfg, triggers)
	stat := NewStat(cfg.StatIntervalSec)
	stat.Add(scheduler)

	return &Queue{
		scheduler: scheduler,
		stat:      stat,
		quitChan:  make(chan chan bool),
	}
}

func CreateQueue(cfg *Config, triggers map[string]TriggerInterface) (*Queue, error) {
	q := CreateCustomQueue(cfg, triggers)
	store := NewMySQLStore(cfg)
	consumer := NewMySQLConsumer(cfg, store)
	return q.Populate(store, consumer)
}

func (self *Queue) Populate(store StoreInterface, consumer ConsumerInterface) (*Queue, error) {
	var g inject.Graph

	if err := g.Provide(
		&inject.Object{Value: store},
		&inject.Object{Value: consumer},
		&inject.Object{Value: self},
		&inject.Object{Value: self.scheduler},
	); err != nil {
		glog.Errorln("Inject:", err)
		return nil, err
	}

	if err := g.Populate(); err != nil {
		glog.Errorln("Populate:", err)
		return nil, err
	}

	if s, ok := store.(StatInterface); ok {
		self.stat.Add(s)
	}
	if s, ok := consumer.(StatInterface); ok {
		self.stat.Add(s)
	}
	return self, nil
}

func (self *Queue) Start() error {
	if self.Store == nil || self.Consumer == nil {
		return fmt.Errorf("Queue is not populated")
	}
	if err := self.Store.Open(); err != nil {
		return err
	}

	self.Consumer.Start()
	self.stat.Start()

	go func() {
		defer func() {
			glog.Infoln("Queue stop")
		}()

		for {
			select {
			case c := <-self.quitChan:
				self.stat.Stop()
				self.Consumer.Stop()
				self.scheduler.clear()
				self.Store.Close()
				close(c)
				return
			case eventList := <-self.Consumer.Events():
				for _, ev := range eventList {
					if ev.Status == EventStatus_DEFAULT {
						self.scheduler.add(ev)
					} else {
						self.scheduler.cancel(ev)
					}
				}
			}
		}
	}()

	glog.Infoln("Queue start")
	return nil
}

func (self *Queue) Stop() {
	glog.Infoln("Stop queue")
	c := make(chan bool)
	self.quitChan <- c
	<-c
}

func (self *Queue) Create(triggerType string, triggerTime time.Time, data interface{}) string {
	ev := NewEvent(triggerType, triggerTime, data)
	return self.Store.Save(ev)
}

func (self *Queue) Cancel(evId string) error {
	return self.Store.Cancel(evId)
}

func (self *Queue) GetStat() map[string]interface{} {
	return self.stat.GetStat(false)
}
