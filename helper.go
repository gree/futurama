package futurama

import (
	"bytes"
	"encoding/json"
	"math"
	"math/rand"
	"sync"
	"time"
)

func backoff(attempt int) time.Time {
	const RETRY_BACKOFF_CAP = 10 * time.Minute

	basePart := int(math.Pow(2, float64(attempt-2)) * 1000)
	randPart := rand.Intn(basePart)
	du := time.Duration(basePart+randPart) * time.Millisecond
	if du > RETRY_BACKOFF_CAP {
		du = RETRY_BACKOFF_CAP
	}
	return time.Now().Add(du)
}

type reusableEncoder struct {
	buf     *bytes.Buffer
	encoder *json.Encoder
}

func newReusableEncoder() (encoder *reusableEncoder) {
	encoder = new(reusableEncoder)
	encoder.buf = bytes.NewBuffer(nil)
	encoder.encoder = json.NewEncoder(encoder.buf)
	return
}

func (self *reusableEncoder) Marshal(v interface{}) ([]byte, error) {
	defer self.buf.Reset()

	if err := self.encoder.Encode(v); err != nil {
		return nil, err
	}

	marshalled := self.buf.Bytes()
	buf := make([]byte, len(marshalled))
	copy(buf, marshalled)
	return buf, nil
}

type EncoderPool struct {
	pool sync.Pool
}

func newEncoderPool() *EncoderPool {
	return_value := EncoderPool{}

	return_value.pool = sync.Pool{
		New: func() interface{} { return newReusableEncoder() },
	}

	return &return_value
}

func (self *EncoderPool) Marshal(v interface{}) ([]byte, error) {
	encoder := self.pool.Get().(*reusableEncoder)
	defer self.pool.Put(encoder)

	return encoder.Marshal(v)
}

var Encoder = newEncoderPool()
