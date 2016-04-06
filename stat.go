package futurama

import (
	"github.com/golang/glog"
	"reflect"
	"runtime"
	"runtime/debug"
	"sort"
	"sync"
	"time"
)

type Stat struct {
	m          sync.RWMutex
	knownStats map[string]StatInterface
	quitChan   chan chan bool

	collectIntervalSec int
}

func NewStat(collectIntervalSec int) *Stat {
	stat := &Stat{
		knownStats:         make(map[string]StatInterface),
		collectIntervalSec: collectIntervalSec,
		quitChan:           make(chan chan bool, 1),
	}
	stat.Add(&sys{time.Now()})
	return stat
}

func (self *Stat) Add(s StatInterface) {
	name := reflect.TypeOf(s).Elem().String()
	glog.Infoln("Add stat", name)
	self.m.Lock()
	self.knownStats[name] = s
	self.m.Unlock()
}

func (self *Stat) GetStat(reset bool) map[string]interface{} {
	self.m.RLock()
	defer self.m.RUnlock()

	res := make(map[string]interface{})
	for k, v := range self.knownStats {
		stat := v.GetStat(reset)
		for k2, v2 := range stat {
			res[k+"."+k2] = v2
		}
	}
	return res
}

func (self *Stat) log() {
	s := self.GetStat(true)
	mk := make([]string, len(s))
	i := 0
	for k, _ := range s {
		mk[i] = k
		i++
	}
	sort.Strings(mk)
	for _, k := range mk {
		glog.Infof("%s: %v", k, s[k])
	}
}

func (self *Stat) Start() {
	if self.collectIntervalSec == 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(time.Duration(self.collectIntervalSec) * time.Second)
		defer func() {
			glog.Infoln("Stat stop")
			if r := recover(); r != nil {
				glog.Errorf("Recovered in GetStat, msg: %s stack: %s", r, debug.Stack())
			}
		}()

		for {
			select {
			case c := <-self.quitChan:
				close(c)
				return
			case <-ticker.C:
				self.log()
			}
		}
	}()

	glog.Infof("Stat start get stats every %ds", self.collectIntervalSec)
}

func (self *Stat) Stop() {
	glog.Infoln("Stop stat")
	if self.collectIntervalSec != 0 {
		c := make(chan bool)
		self.quitChan <- c
		<-c
	}
	self.log()
}

type sys struct {
	startTime time.Time
}

func (self *sys) GetStat(reset bool) map[string]interface{} {
	stat := make(map[string]interface{})
	stat["StartTime"] = self.startTime
	stat["UpTime"] = time.Since(self.startTime).String()
	stat["CollectedTime"] = time.Now()
	stat["NumGoroutine"] = runtime.NumGoroutine()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	stat["MemAlloc"] = m.Alloc
	stat["HeapAlloc"] = m.HeapAlloc
	stat["NextGC"] = m.NextGC
	stat["NumGC"] = m.NumGC
	return stat
}
