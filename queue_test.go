package futurama

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
)

func TestQueue_Multiple(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	cfg := DefaultConfig()
	TestOnly_ResetDb(&cfg.MySQLConfig)

	nbQueue := 2
	groupSize := 6000
	nbGroup := 10
	nbTotal := int32(groupSize*nbGroup + groupSize/2*nbGroup)
	maxDelaySec := 60
	var nbTriggered Seq32

	store := NewMySQLStore(cfg)
	store.Open()
	for i := 0; i < nbGroup; i++ {
		for j := 0; j < groupSize; j++ {
			r := rand.Intn((maxDelaySec - i) * 1000)
			du := time.Duration(r)*time.Millisecond + time.Second*time.Duration(i)
			triggerTime := time.Now().Add(du)
			ev := NewEvent(Test_TriggerType_Default, triggerTime, nil)
			store.Save(ev)
		}
	}

	assert := assert.New(t)

	doneChan := make(chan bool)
	quitChan := make(chan bool)
	qList := make([]*Queue, 0)
	for i := 0; i < nbQueue; i++ {
		cfg := DefaultConfig()
		cfg.ConsumerName = fmt.Sprintf("q%d", i)
		q, testChan := SetupQueue(cfg)
		qList = append(qList, q)
		go func() {
			for {
				select {
				case <-testChan:
					if nbTotal == nbTriggered.Next() {
						close(doneChan)
						return
					}
				case <-quitChan:
					return
				}
			}
		}()

		defer func() {
			stat := q.GetStat()
			assert.EqualValues(stat["futurama.MySQLStore.nbError"], 0)
			// assert.EqualValues(stat["futurama.Scheduler.nbDelayed"], 0)
			q.Stop()
			close(testChan)
		}()
	}

	go func() {
		idx := 0
		for i := 0; i < nbGroup; i++ {
			for j := 0; j < groupSize/2; j++ {
				r := rand.Intn((maxDelaySec - i) * 1000)
				du := time.Duration(r)*time.Millisecond + time.Second*time.Duration(i)
				triggerTime := time.Now().Add(du)
				idx = 1 - idx
				qList[idx].Create(Test_TriggerType_Default, triggerTime, nil)
			}
		}
	}()

	select {
	case <-doneChan:
	case <-time.After(200 * time.Second):
		assert.Fail("timeout")
	}

	close(quitChan)
	time.Sleep(time.Second)
	assert.Len(TestOnly_SelectEvents(&cfg.MySQLConfig), 0)
}
