package futurama

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestStat_ZeroInterval(t *testing.T) {
	stat := NewStat(0)
	assert := assert.New(t)

	before := time.Now()
	stat.Start()
	assert.WithinDuration(time.Now(), before, 50*time.Millisecond)

	time.Sleep(3 * time.Second)
	stat.Stop()
	assert.WithinDuration(time.Now(), before.Add(3*time.Second), 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
}

func TestStat_Stat(t *testing.T) {
	stat := NewStat(3)
	assert := assert.New(t)

	before := time.Now()
	stat.Start()
	assert.WithinDuration(time.Now(), before, 50*time.Millisecond)

	time.Sleep(7 * time.Second)
	stat.Stop()
	assert.WithinDuration(time.Now(), before.Add(7*time.Second), 50*time.Millisecond)

	s := stat.GetStat(false)
	assert.WithinDuration(s["futurama.sys.StartTime"].(time.Time), before, 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
}
