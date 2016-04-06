package futurama

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var evId string = ""

func TestStore_Save(t *testing.T) {
	cfg := DefaultConfig()
	TestOnly_ResetDb(&cfg.MySQLConfig)
	store := NewMySQLStore(cfg)
	store.Open()
	defer store.Close()

	triggerTime := time.Now()
	data := map[string]interface{}{
		"a": 1,
		"b": "text",
	}
	ev := NewEvent(Test_TriggerType_Default, triggerTime, data)
	evId = store.Save(ev)

	assert := assert.New(t)
	assert.NotEmpty(evId)

	evList := TestOnly_SelectEvents(&cfg.MySQLConfig)
	assert.Len(evList, 1)
	gotEv := evList[0]
	assert.Equal(gotEv.Id, evId)
	assert.Equal(gotEv.TriggerType, Test_TriggerType_Default)
	assert.WithinDuration(gotEv.TriggerTime, triggerTime, 50*time.Millisecond)
	assert.Equal(gotEv.Attempts, 0)
	assert.Equal(int(gotEv.Status), EventStatus_DEFAULT)
	assert.Equal(gotEv.Owner, "")
}

func TestStore_Cancel(t *testing.T) {
	TestStore_Save(t)

	assert := assert.New(t)
	assert.NotEmpty(evId)

	cfg := DefaultConfig()
	store := NewMySQLStore(cfg)
	store.Open()
	defer store.Close()

	err := store.Cancel(evId)
	assert.Empty(err)

	evList := TestOnly_SelectEvents(&cfg.MySQLConfig)
	assert.Len(evList, 1)
	gotEv := evList[0]
	assert.Equal(gotEv.Id, evId)
	assert.Equal(gotEv.TriggerType, Test_TriggerType_Default)
	assert.Equal(gotEv.Attempts, 0)
	assert.Equal(int(gotEv.Status), EventStatus_CANCEL)
	assert.Equal(gotEv.Owner, "")
}

func TestStore_UpdateStatus_Other(t *testing.T) {
	for i := EventStatus_DEFAULT; i <= EventStatus_RETRY; i++ {
		TestStore_Save(t)

		assert := assert.New(t)
		assert.NotEmpty(evId)

		cfg := DefaultConfig()
		store := NewMySQLStore(cfg)
		store.Open()
		defer store.Close()

		err := store.UpdateStatus(evId, EventStatus(i))
		assert.Empty(err)

		evList := TestOnly_SelectEvents(&cfg.MySQLConfig)
		assert.Len(evList, 0)
	}
}

func TestStore_UpdateForRetry(t *testing.T) {
	TestStore_Save(t)

	assert := assert.New(t)
	assert.NotEmpty(evId)

	cfg := DefaultConfig()
	store := NewMySQLStore(cfg)
	store.Open()
	defer store.Close()

	sql := fmt.Sprintf(`UPDATE %s SET owner=? where id=?`, cfg.TableName)
	if _, err := store.db.Exec(sql, cfg.ConsumerName, evId); err != nil {
		assert.Fail(err.Error())
		return
	}

	evList := TestOnly_SelectEvents(&cfg.MySQLConfig)
	assert.Len(evList, 1)
	assert.Equal(evList[0].Id, evId)
	assert.Equal(evList[0].Owner, cfg.ConsumerName)

	triggerTime := time.Now().Add(3 * time.Second)
	ev := NewEvent(Test_TriggerType_Default, triggerTime, nil)
	ev.Attempts = 10
	ev.Id = evId
	if err := store.UpdateForRetry(ev, nil); err != nil {
		assert.Fail(err.Error())
		return
	}

	evList = TestOnly_SelectEvents(&cfg.MySQLConfig)

	assert.Len(evList, 1)
	gotEv := evList[0]
	assert.Equal(gotEv.Id, evId)
	assert.Equal(gotEv.TriggerType, Test_TriggerType_Default)
	assert.WithinDuration(gotEv.TriggerTime, triggerTime, 50*time.Millisecond)
	assert.Equal(gotEv.Attempts, ev.Attempts)
	assert.Equal(int(gotEv.Status), EventStatus_DEFAULT)
	assert.Equal(gotEv.Owner, "")
}
