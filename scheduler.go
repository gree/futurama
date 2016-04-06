package futurama

import (
	"github.com/golang/glog"
	"runtime/debug"
	"sync"
	"time"
)

type SchedulerDepsContainer struct {
	Store StoreInterface `inject:""`
}

var SchedulerDeps = &SchedulerDepsContainer{}

type Scheduler struct {
	SchedulerDepsContainer `inject:"inline"`

	eventMutex sync.RWMutex
	events     *PQ
	maxRetry   int
	triggers   map[string]TriggerInterface

	nbDelayed   Seq32
	nbTriggered Seq32
	nbGiveup    Seq32
	nbRecovered Seq32
}

func newScheduler(cfg *Config, triggers map[string]TriggerInterface) *Scheduler {
	return &Scheduler{
		events:   NewPQ(true, cfg.MaxScheduledEvents),
		maxRetry: cfg.MaxRetry,
		triggers: triggers,
	}
}

func (self *Scheduler) clear() {
	glog.Infoln("Clear")
	self.eventMutex.Lock()
	defer self.eventMutex.Unlock()

	for {
		item := self.events.Top()
		if item == nil {
			return
		}
		pqItem := item.(PQItem)
		if removed := self.events.Remove(pqItem.GetKey()); removed != nil {
			removedEv := removed.(*Event)
			glog.Infoln("Clear removed", removedEv)
			removedEv.Stop()
		}
	}
}

func (self *Scheduler) add(ev *Event) {
	if ev == nil {
		glog.Warningln("Adding nil event")
		return
	}
	glog.Infoln(ev, "Add")

	self.eventMutex.Lock()

	if index := self.events.Lookup(ev.GetKey()); index >= 0 {
		self.eventMutex.Unlock()
		if glog.V(2) {
			glog.Infof("%s Event has been scheduled, index: %d", ev, index)
		}
		return
	}

	index, poped := self.events.Push(ev, ev.TriggerTime.UnixNano())
	self.eventMutex.Unlock()

	if index >= 0 {
		du := ev.TriggerTime.Sub(time.Now())
		if du.Seconds() < -2.0 {
			glog.Warningf("%s Scheduler is behind ev.TriggerTime: %s, triggering now", ev, du)
			self.nbDelayed.Next()
			du = 0
		}
		glog.Infof("%s Duration to trigger: %s", ev, du)
		ev.timer = time.AfterFunc(du, func() {
			defer func() {
				if r := recover(); r != nil {
					glog.Errorf("Recovered in time.AfterFunc, msg: %s ev: %s stack: %s", r, ev, debug.Stack())
					self.nbRecovered.Next()
					self.Store.UpdateStatus(ev.Id, EventStatus_ERROR)
				}
			}()
			self.trigger(ev.Id)
		})
		glog.Infoln(ev, "Event scheduled")
	}

	var popedEv *Event = nil
	if poped != nil {
		popedEv = poped.(*Event)
		popedEv.Stop()
	} else {
		if index == -1 {
			popedEv = ev
		}
	}

	if popedEv != nil {
		// TODO self.Store.RemoveOwnership(ev.Id)
		glog.Infof("%s Queue is full, poped event index: %d", popedEv, index)
	}
}

func (self *Scheduler) cancel(ev *Event) {
	glog.Infoln(ev, "Cancel")
	self.eventMutex.Lock()

	if removed := self.events.Remove(ev.Id); removed != nil {
		removedEv := removed.(*Event)
		removedEv.Stop()
		glog.Infoln(removedEv, "Cancelled scheduled event")
	}
	self.eventMutex.Unlock()
	self.Store.UpdateStatus(ev.Id, EventStatus_CANCEL)
}

func (self *Scheduler) trigger(evId string) {
	glog.Infoln(evId, "Trigger")
	self.eventMutex.Lock()
	removed := self.events.Remove(evId)
	if removed == nil {
		glog.Infoln(evId, "Event has been cancelled, not triggering")
		self.eventMutex.Unlock()
		return
	}
	self.eventMutex.Unlock()

	ev := removed.(*Event)
	trigger := self.getTrigger(ev.TriggerType)
	before := time.Now()
	result := trigger.Trigger(ev)
	glog.Infof("%s TriggerResult status: %s took: %s", ev, result.Status, time.Since(before))
	self.nbTriggered.Next()

	switch result.Status {
	case EventStatus_RETRY:
		if ev.Attempts >= self.maxRetry {
			glog.Infof("%s reached MaxRetry(%d), give up", ev, self.maxRetry)
			self.nbGiveup.Next()
			self.Store.UpdateStatus(ev.Id, EventStatus_ERROR)
		} else {
			if result.TriggerTime.IsZero() {
				ev.TriggerTime = backoff(ev.Attempts)
			} else {
				ev.TriggerTime = result.TriggerTime
			}
			ev.Attempts++
			self.Store.UpdateForRetry(ev, result.Data)
		}
	default:
		self.Store.UpdateStatus(ev.Id, result.Status)
	}
}

func (self *Scheduler) getTrigger(triggerType string) TriggerInterface {
	if trigger, ok := self.triggers[triggerType]; ok {
		return trigger
	}
	return noTrigger
}

func (self *Scheduler) GetStat(reset bool) map[string]interface{} {
	self.eventMutex.RLock()
	nbEvents := self.events.Len()
	nbCapacity := self.events.maxSize - nbEvents
	self.eventMutex.RUnlock()

	stat := map[string]interface{}{
		"nbEvents":    nbEvents,
		"nbCapacity":  nbCapacity,
		"nbDelayed":   self.nbDelayed.Get(),
		"nbTriggered": self.nbTriggered.Get(),
		"nbGiveup":    self.nbGiveup.Get(),
		"nbRecovered": self.nbRecovered.Get(),
	}

	if reset {
		self.nbDelayed.Reset()
		self.nbTriggered.Reset()
		self.nbGiveup.Reset()
		self.nbRecovered.Reset()
	}

	return stat
}

// default trigger
type NoTrigger struct{}

func (self *NoTrigger) Trigger(ev *Event) *TriggerResult {
	glog.Errorln("Calling NoTrigger")
	return &TriggerResult{
		Status: EventStatus_ERROR,
	}
}

var noTrigger = &NoTrigger{}
