package futurama

import (
	"sync/atomic"
)

const SEQ_MASK_INT32 = 0x7fffffff

type Seq32 uint32

func (self *Seq32) Next() int32 {
	return int32(atomic.AddUint32((*uint32)(self), 1) & SEQ_MASK_INT32)
}

func (self *Seq32) Get() int32 {
	return int32(atomic.LoadUint32((*uint32)(self)) & SEQ_MASK_INT32)
}

func (self *Seq32) Reset() {
	atomic.StoreUint32((*uint32)(self), 0)
}
