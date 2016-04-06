package futurama

import (
	"github.com/golang/glog"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSeq_increment(t *testing.T) {
	var a Seq32
	assert.Equal(t, a, Seq32(0))
	for x := 0; x < 10; x++ {
		go func() {
			for i := 0; i < 10; i++ {
				glog.Infoln(a.Next())
			}
		}()
	}
	time.Sleep(time.Second)
	assert.Equal(t, a.Get(), int32(100))
}

func TestSeq_overflow(t *testing.T) {
	var a = Seq32(SEQ_MASK_INT32 - 5)
	assert.Equal(t, a, Seq32(SEQ_MASK_INT32-5))
	for x := 0; x < 5; x++ {
		glog.Infoln(a.Next())
	}
	for x := 0; x < 5; x++ {
		v := a.Next()
		glog.Infoln(v)
		assert.Equal(t, v, int32(x))
	}
}

func TestSeq_reset(t *testing.T) {
	var a Seq32
	for x := 1; x < 5; x++ {
		v := a.Next()
		glog.Infoln(v)
		assert.Equal(t, v, int32(x))
	}
	a.Reset()
	for x := 1; x < 5; x++ {
		v := a.Next()
		glog.Infoln(v)
		assert.Equal(t, v, int32(x))
	}
}
