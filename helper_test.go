package futurama

import (
	"github.com/golang/glog"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func TestHelper_EncoderPool_bufferOverwritten(t *testing.T) {
	encoder := newEncoderPool()

	o1 := struct {
		A int
		B int
	}{1, 2}
	b, _ := encoder.Marshal(o1)

	o2 := struct {
		A int
		B int
	}{3, 4}
	encoder.Marshal(o2)

	s := strings.TrimSpace(string(b))
	glog.Infoln(s)
	if s != `{"A":1,"B":2}` {
		t.Errorf("Buffer is overwritten! %s", s)
	}
}

func TestHelper_Backoff(t *testing.T) {
	expected := []float64{
		250, 500, 1000,
		2000, 4000, 8000,
		16000, 32000, 64000,
		128000, 256000, 512000,
		600000, 600000, 600000,
		600000, 600000, 600000,
		600000, 600000, 600000,
		600000, 600000, 600000,
	}

	for i := 0; i < 20; i++ {
		now := time.Now()
		b := backoff(i)
		du := b.Sub(now)
		glog.Infoln(du)
		assert.InDelta(t, du.Seconds()*1000, expected[i], 2*expected[i])
	}
}
