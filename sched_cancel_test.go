package futurama

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestScheduler_Cancel_InMemory(t *testing.T) {
	cfg := DefaultConfig()
	TestOnly_ResetDb(&cfg.MySQLConfig)
	q, testChan := SetupQueue(cfg)
	defer q.Stop()
	assert := assert.New(t)

	triggerTime := time.Now().Add(2 * time.Second)
	evId := q.Create(Test_TriggerType_Default, triggerTime, "")

	time.Sleep(500 * time.Millisecond)

	stat := q.GetStat()
	assert.EqualValues(stat["futurama.Scheduler.nbEvents"], 1)
	q.Cancel(evId)

	select {
	case id := <-testChan:
		assert.Equal(id, evId)
		assert.WithinDuration(time.Now(), triggerTime, 150*time.Millisecond)
		assert.Fail("event is triggered")
	case <-time.After(time.Millisecond * 2500):
	}
	assert.Len(TestOnly_SelectEvents(&cfg.MySQLConfig), 0)

	stat = q.GetStat()
	assert.EqualValues(stat["futurama.MySQLStore.nbComplete"], 1)
	assert.EqualValues(stat["futurama.MySQLStore.nbSave"], 1)
	assert.EqualValues(stat["futurama.MySQLStore.nbCancel"], 1)
	assert.EqualValues(stat["futurama.Scheduler.nbTriggered"], 0)
}

func TestScheduler_Cancel_InDb(t *testing.T) {
	cfg := DefaultConfig()
	TestOnly_ResetDb(&cfg.MySQLConfig)
	q, testChan := SetupQueue(cfg)
	defer q.Stop()
	assert := assert.New(t)

	triggerTime := time.Now().Add(7 * time.Second)
	evId := q.Create(Test_TriggerType_Default, triggerTime, "")

	time.Sleep(500 * time.Millisecond)

	stat := q.GetStat()
	assert.EqualValues(stat["futurama.Scheduler.nbEvents"], 0)
	q.Cancel(evId)

	select {
	case id := <-testChan:
		assert.Equal(id, evId)
		assert.WithinDuration(time.Now(), triggerTime, 150*time.Millisecond)
		assert.Fail("event is triggered")
	case <-time.After(time.Millisecond * 7500):
	}
	assert.Len(TestOnly_SelectEvents(&cfg.MySQLConfig), 0)

	stat = q.GetStat()
	assert.EqualValues(stat["futurama.MySQLStore.nbComplete"], 1)
	assert.EqualValues(stat["futurama.MySQLStore.nbSave"], 1)
	assert.EqualValues(stat["futurama.MySQLStore.nbCancel"], 1)
	assert.EqualValues(stat["futurama.Scheduler.nbTriggered"], 0)
}

func TestScheduler_Cancel_Unknown(t *testing.T) {
	cfg := DefaultConfig()
	TestOnly_ResetDb(&cfg.MySQLConfig)
	q, _ := SetupQueue(cfg)
	defer q.Stop()

	q.Cancel("unknown_id")

	time.Sleep(time.Second)

	stat := q.GetStat()
	assert := assert.New(t)
	assert.EqualValues(stat["futurama.MySQLStore.nbComplete"], 0)
	assert.EqualValues(stat["futurama.MySQLStore.nbSave"], 0)
	assert.EqualValues(stat["futurama.MySQLStore.nbCancel"], 1)
	assert.EqualValues(stat["futurama.Scheduler.nbTriggered"], 0)
}
