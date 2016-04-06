package futurama

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestConsumer_GetEvents(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ConsumerTimeWindowSec = 5
	cfg.ConsumerLockTimeoutSec = 10
	TestOnly_ResetDb(&cfg.MySQLConfig)
	store := NewMySQLStore(cfg)
	consumer := NewMySQLConsumer(cfg, store)
	store.Open()
	consumer.Start()

	defer func() {
		consumer.Stop()
		store.Close()
	}()

	timeWindow := time.Duration(cfg.ConsumerTimeWindowSec) * time.Second
	lockTimeout := time.Duration(cfg.ConsumerLockTimeoutSec) * time.Second

	before := time.Now()
	triggerTime1 := before.Add(timeWindow + time.Second*2)
	triggerTime2 := before.Add(timeWindow + timeWindow + time.Second*2)
	ev1 := NewEvent(Test_TriggerType_Default, triggerTime1, nil)
	evId1 := store.Save(ev1)
	ev2 := NewEvent(Test_TriggerType_Default, triggerTime2, nil)
	evId2 := store.Save(ev2)

	assert := assert.New(t)
	var lockedTime1, lockedTime2 time.Time
	select {
	case events := <-consumer.Events():
		assert.Len(events, 1)
		assert.Equal(events[0].Id, evId1)
		assert.WithinDuration(time.Now(), before.Add(2*time.Second), time.Second)
		lockedTime1 = time.Now()
	case <-time.After(5 * time.Second):
		assert.Fail("Did not get event")
	}

	select {
	case events := <-consumer.Events():
		assert.Len(events, 1)
		assert.Equal(events[0].Id, evId2)
		assert.WithinDuration(time.Now(), before.Add(timeWindow+2*time.Second), time.Second)
		lockedTime2 = time.Now()
	case <-time.After(timeWindow + 5*time.Second):
		assert.Fail("Did not get event")
	}

	// will get the events again after lockTimeout, because it should have been reset as "delayed"
	select {
	case events := <-consumer.Events():
		assert.Len(events, 1)
		assert.Equal(events[0].Id, evId1)
		assert.WithinDuration(time.Now(), lockedTime1.Add(lockTimeout), 3500*time.Millisecond)
	case <-time.After(lockTimeout):
		assert.Fail("Did not get event")
	}

	select {
	case events := <-consumer.Events():
		assert.Len(events, 1)
		assert.Equal(events[0].Id, evId2)
		assert.WithinDuration(time.Now(), lockedTime2.Add(lockTimeout), 3500*time.Millisecond)
	case <-time.After(lockTimeout):
		assert.Fail("Did not get event")
	}
}
