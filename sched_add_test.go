package futurama

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestScheduler_Add_Normal(t *testing.T) {
	cfg := DefaultConfig()
	TestOnly_ResetDb(&cfg.MySQLConfig)
	q, testChan := SetupQueue(cfg)
	defer q.Stop()
	assert := assert.New(t)

	triggerTime := time.Now().Add(2 * time.Second)
	evId := q.Create(Test_TriggerType_Default, triggerTime, "")

	select {
	case id := <-testChan:
		assert.Equal(id, evId)
		assert.WithinDuration(time.Now(), triggerTime, 150*time.Millisecond)
	case <-time.After(time.Millisecond * 2100):
		assert.Fail("event is not triggered")
	}
	time.Sleep(500 * time.Millisecond)
	assert.Len(TestOnly_SelectEvents(&cfg.MySQLConfig), 0)

	stat := q.GetStat()
	assert.EqualValues(stat["futurama.MySQLStore.nbComplete"], 1)
	assert.EqualValues(stat["futurama.MySQLStore.nbSave"], 1)
	assert.EqualValues(stat["futurama.Scheduler.nbTriggered"], 1)
}

func TestScheduler_Add_UnknownTrigger(t *testing.T) {
	cfg := DefaultConfig()
	TestOnly_ResetDb(&cfg.MySQLConfig)
	q, testChan := SetupQueue(cfg)
	defer q.Stop()
	assert := assert.New(t)

	triggerTime := time.Now().Add(2 * time.Second)
	q.Create("_unknown_trigger_type_", triggerTime, "")

	select {
	case <-testChan:
		assert.Fail("No events should be triggered")
	case <-time.After(time.Millisecond * 2100):
	}
	time.Sleep(500 * time.Millisecond)
	assert.Len(TestOnly_SelectEvents(&cfg.MySQLConfig), 0)

	stat := q.GetStat()
	assert.EqualValues(stat["futurama.MySQLStore.nbComplete"], 1)
	assert.EqualValues(stat["futurama.MySQLStore.nbSave"], 1)
	assert.EqualValues(stat["futurama.Scheduler.nbTriggered"], 1)
}

func TestScheduler_Add_EventLockTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ConsumerLockTimeoutSec = 5
	TestOnly_ResetDb(&cfg.MySQLConfig)
	assert := assert.New(t)

	before := time.Now()
	evId1 := func() string {
		cfg.ConsumerName = "q1"
		q, _ := SetupQueue(cfg)
		defer q.Stop()
		triggerTime := before.Add(2 * time.Second)
		evId := q.Create(Test_TriggerType_Default, triggerTime, "")
		time.Sleep(500 * time.Millisecond)
		events := TestOnly_SelectEvents(&cfg.MySQLConfig)
		assert.Len(events, 1)
		assert.Equal(events[0].Id, evId)
		return evId
	}()

	cfg.ConsumerName = "q2"
	q, testChan := SetupQueue(cfg)
	defer q.Stop()
	triggerTime := before.Add(3 * time.Second)
	evId := q.Create(Test_TriggerType_Default, triggerTime, "")

	doneChan := make(chan bool)
	go func() {
		id2 := <-testChan
		assert.Equal(id2, evId)
		assert.WithinDuration(time.Now(), triggerTime, 150*time.Millisecond)
		id1 := <-testChan
		assert.Equal(id1, evId1)
		assert.True(time.Now().After(before.Add(time.Duration(cfg.ConsumerLockTimeoutSec) * time.Second)))
		close(doneChan)
	}()

	select {
	case <-doneChan:
		time.Sleep(500 * time.Millisecond)
		assert.Len(TestOnly_SelectEvents(&cfg.MySQLConfig), 0)
	case <-time.After(time.Duration(cfg.ConsumerLockTimeoutSec+2) * time.Second):
		assert.Fail("timeout")
	}

	stat := q.GetStat()
	assert.EqualValues(stat["futurama.MySQLStore.nbComplete"], 2)
	assert.EqualValues(stat["futurama.MySQLStore.nbSave"], 1)
	assert.EqualValues(stat["futurama.Scheduler.nbTriggered"], 2)
	assert.EqualValues(stat["futurama.Scheduler.nbDelayed"], 1)
}

func TestScheduler_Add_MultiProducers(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	cfg := DefaultConfig()
	TestOnly_ResetDb(&cfg.MySQLConfig)
	q, testChan := SetupQueue(cfg)
	defer q.Stop()
	assert := assert.New(t)

	groupSize := 8
	nbGroup := 15
	total := groupSize * nbGroup

	var evListMutex sync.Mutex
	evList := make(map[string]time.Time)

	before := time.Now()
	for i := 1; i <= nbGroup; i++ {
		go func(groupId int) {
			for j := 0; j < groupSize; j++ {
				r := rand.Intn(119)
				triggerTime := before.Add(time.Duration(groupId)*time.Second +
					time.Duration(j*r)*time.Millisecond)
				evId := q.Create(Test_TriggerType_Default, triggerTime, "")
				evListMutex.Lock()
				evList[evId] = triggerTime
				evListMutex.Unlock()
				time.Sleep(100 * time.Millisecond)
			}
		}(i)
	}

	doneChan := make(chan bool)
	go func() {
		for id := range testChan {
			triggerTime, ok := evList[id]
			assert.True(ok)
			if ok {
				assert.WithinDuration(time.Now(), triggerTime, 150*time.Millisecond)
			}
			total--
			if total == 0 {
				close(doneChan)
				return
			}
		}
	}()

	select {
	case <-doneChan:
	case <-time.After(time.Duration(nbGroup)*time.Second + 3500*time.Millisecond):
		assert.Fail("timeout")
	}
	assert.Len(TestOnly_SelectEvents(&cfg.MySQLConfig), 0)

	stat := q.GetStat()
	assert.EqualValues(stat["futurama.MySQLStore.nbError"], 0)
	assert.EqualValues(stat["futurama.Scheduler.nbDelayed"], 0)
}

func TestScheduler_Add_Panic(t *testing.T) {
	cfg := DefaultConfig()
	TestOnly_ResetDb(&cfg.MySQLConfig)
	q, testChan := SetupQueue(cfg)
	defer q.Stop()
	assert := assert.New(t)

	q.Create(Test_TriggerType_Panic, time.Now().Add(time.Second), "")
	time.Sleep(2 * time.Second)

	triggerTime := time.Now().Add(2 * time.Second)
	evId := q.Create(Test_TriggerType_Default, triggerTime, "")

	select {
	case id := <-testChan:
		assert.Equal(id, evId)
		assert.WithinDuration(time.Now(), triggerTime, 150*time.Millisecond)
	case <-time.After(time.Millisecond * 2100):
		assert.Fail("event is not triggered")
	}
	time.Sleep(500 * time.Millisecond)
	assert.Len(TestOnly_SelectEvents(&cfg.MySQLConfig), 0)

	stat := q.GetStat()
	assert.EqualValues(stat["futurama.MySQLStore.nbComplete"], 2)
	assert.EqualValues(stat["futurama.MySQLStore.nbSave"], 2)
	assert.EqualValues(stat["futurama.Scheduler.nbTriggered"], 1)
	assert.EqualValues(stat["futurama.Scheduler.nbRecovered"], 1)
}
