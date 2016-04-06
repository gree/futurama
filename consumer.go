package futurama

import (
	"github.com/golang/glog"
	"github.com/satori/go.uuid"
	"runtime/debug"
	"time"
)

type MySQLConsumer struct {
	ownerId   string
	store     *MySQLStore
	cfg       *MySQLConfig
	quitChan  chan chan bool
	eventChan chan []*Event
	seq       Seq32

	nbRecovered Seq32
	lastSeq     int32
}

func NewMySQLConsumer(cfg *Config, store *MySQLStore) *MySQLConsumer {
	var ownerId string
	if cfg.ConsumerName == "" {
		ownerId = "Consumer:" + uuid.NewV1().String()
	} else {
		ownerId = cfg.ConsumerName
	}

	return &MySQLConsumer{
		ownerId:   ownerId,
		store:     store,
		cfg:       &cfg.MySQLConfig,
		quitChan:  make(chan chan bool, 1),
		eventChan: make(chan []*Event),
	}
}

func (self *MySQLConsumer) Start() {
	go func() {
		defer glog.Infoln("Consumer stop", self.ownerId)

		var (
			shouldStop    = false
			consumerSleep = time.Duration(self.cfg.ConsumerSleepMSec) * time.Millisecond
		)

		timeoutCheckerInterval := time.Duration(self.cfg.ConsumerLockTimeoutSec*1000/4+1) * time.Millisecond
		glog.Infoln("Check timeout events every", timeoutCheckerInterval, self.ownerId)
		timeoutChecker := time.NewTicker(timeoutCheckerInterval)
		for {
			func() {
				defer func() {
					if r := recover(); r != nil {
						glog.Errorf("Recovered in Consumer(%s), msg: %s stack: %s", self.ownerId, r, debug.Stack())
						self.nbRecovered.Next()
					}
				}()

				select {
				case c := <-self.quitChan:
					close(c)
					shouldStop = true
					return
				case <-timeoutChecker.C:
					self.store.resetDelayedEvents(self.ownerId)
				default:
					consumerSeq := self.seq.Next()
					if err, events := self.store.getEvents(consumerSeq, self.ownerId); err != nil {
						glog.Errorln("getEvents:", err, self.ownerId)
					} else {
						if len(events) > 0 {
							glog.Infof("Dispatch events nbEv: %d seq: %d %s", len(events), consumerSeq, self.ownerId)
							self.eventChan <- events
						}
					}
					time.Sleep(consumerSleep)
				}
			}()

			if shouldStop {
				break
			}
		}
	}()

	glog.Infoln("Consumer start", self.ownerId)
}

func (self *MySQLConsumer) Stop() {
	glog.Infoln("Stop consumer", self.ownerId)
	c := make(chan bool)
	self.quitChan <- c
	<-c
}

func (self *MySQLConsumer) Events() <-chan []*Event {
	return self.eventChan
}

func (self *MySQLConsumer) GetStat(reset bool) map[string]interface{} {
	stat := map[string]interface{}{
		"nbGetEvents": self.seq.Get() - self.lastSeq,
		"nbRecovered": self.nbRecovered.Get(),
	}
	if reset {
		self.nbRecovered.Reset()
		self.lastSeq = self.seq.Get()
	}

	return stat
}
