package futurama

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type RetryData struct {
	NumRetry      int
	RetryTimeNano int64
}

func TestScheduler_Retry_AndComplete(t *testing.T) {
	cfg := DefaultConfig()
	TestOnly_ResetDb(&cfg.MySQLConfig)
	q, testChan := SetupQueue(cfg)
	defer q.Stop()
	assert := assert.New(t)

	triggerTime := time.Now().Add(2 * time.Second)
	retryTime := triggerTime.Add(3 * time.Second)

	evId := q.Create(Test_TriggerType_Retry, triggerTime, &RetryData{1, retryTime.UnixNano()})

	doneChan := make(chan bool, 1)
	go func() {
		id := <-testChan
		assert.Equal(id, evId)
		assert.WithinDuration(time.Now(), triggerTime, 150*time.Millisecond)
		id = <-testChan
		assert.Equal(id, evId)
		assert.WithinDuration(time.Now(), retryTime, 150*time.Millisecond)
		close(doneChan)
	}()

	select {
	case <-doneChan:
	case <-time.After(7 * time.Second):
		assert.Fail("timeout")
	}

	time.Sleep(500 * time.Millisecond)
	assert.Len(TestOnly_SelectEvents(&cfg.MySQLConfig), 0)

	stat := q.GetStat()
	assert.EqualValues(1, stat["futurama.MySQLStore.nbComplete"])
	assert.EqualValues(1, stat["futurama.MySQLStore.nbSave"])
	assert.EqualValues(1, stat["futurama.MySQLStore.nbRetry"])
	assert.EqualValues(2, stat["futurama.Scheduler.nbTriggered"])
}

func TestScheduler_Retry_Giveup(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxRetry = 5
	TestOnly_ResetDb(&cfg.MySQLConfig)
	q, testChan := SetupQueue(cfg)
	defer q.Stop()
	assert := assert.New(t)

	triggerTime := time.Now().Add(2 * time.Second)
	evId := q.Create(Test_TriggerType_Retry, triggerTime, &RetryData{100, 0})

	doneChan := make(chan bool)
	go func() {
		numTriggered := 0
		for {
			id := <-testChan
			assert.Equal(id, evId)
			numTriggered++
			if numTriggered == cfg.MaxRetry+1 {
				close(doneChan)
				return
			}
		}
	}()

	select {
	case <-doneChan:
	case <-time.After(30 * time.Second):
		assert.Fail("timeout")
	}

	time.Sleep(500 * time.Millisecond)
	assert.Len(TestOnly_SelectEvents(&cfg.MySQLConfig), 0)

	stat := q.GetStat()
	assert.EqualValues(1, stat["futurama.MySQLStore.nbComplete"])
	assert.EqualValues(1, stat["futurama.MySQLStore.nbSave"])
	assert.EqualValues(5, stat["futurama.MySQLStore.nbRetry"])
	assert.EqualValues(6, stat["futurama.Scheduler.nbTriggered"])
	assert.EqualValues(1, stat["futurama.Scheduler.nbGiveup"])
}
