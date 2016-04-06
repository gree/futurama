package futurama

import (
	"container/heap"
)

type HeapItem struct {
	// public
	Value interface{}
	// private
	priority int64
	index    int
}

func NewHeapItem(priority int64, value interface{}) *HeapItem {
	return &HeapItem{value, priority, -1}
}

type CompareFunc func(a *HeapItem, b *HeapItem) bool
type Heap struct {
	items   []*HeapItem
	compare CompareFunc
}

func NewHeap(reversed bool) *Heap {
	var compare CompareFunc
	if reversed {
		compare = func(a *HeapItem, b *HeapItem) bool { return a.priority > b.priority }
	} else {
		compare = func(a *HeapItem, b *HeapItem) bool { return a.priority < b.priority }
	}
	return &Heap{make([]*HeapItem, 0), compare}
}

func (self *Heap) Len() int { return len(self.items) }

func (self *Heap) Less(i, j int) bool {
	return self.compare(self.items[i], self.items[j])
}

func (self *Heap) Swap(i, j int) {
	self.items[i], self.items[j] = self.items[j], self.items[i]
	self.items[i].index = i
	self.items[j].index = j
}

func (self *Heap) Push(x interface{}) {
	n := len(self.items)
	item := x.(*HeapItem)
	item.index = n
	self.items = append(self.items, item)
}

func (self *Heap) Pop() interface{} {
	n := len(self.items)
	item := self.items[n-1]
	item.index = -1 // for safety
	self.items = self.items[:n-1]
	return item
}

type PQItem interface {
	GetKey() string
}

type PQ struct {
	maxSize   int
	heap      *Heap
	lookupMap map[string]*HeapItem
}

func NewPQ(reversed bool, maxSize int) *PQ {
	return &PQ{
		maxSize:   maxSize,
		heap:      NewHeap(reversed),
		lookupMap: make(map[string]*HeapItem),
	}
}

func (self *PQ) Top() PQItem {
	if self.Len() == 0 {
		return nil
	}
	return self.heap.items[0].Value.(PQItem)
}

// meanings of returned values
// index >= 0, poped == nil: queue is not full, new item has been pushed
// index >= 0, poped != nil: queue is full, new item has been pushed, the item with lowest priority has been poped
// index < 0,  poped == nil: not possible for PQ (only if there is a wrong usage/implementation e.g. we set max == 0 ...)
// index < 0,  poped != nil: queue is full, new item has not been pushed because it has the lowest priority
func (self *PQ) Push(value PQItem, priority int64) (index int, poped PQItem) {
	index = -1
	poped = nil

	key := value.GetKey()
	if idx := self.Lookup(key); idx >= 0 {
		return
	}

	item := NewHeapItem(priority, value)
	len := self.heap.Len()
	if len == self.maxSize {
		if len > 0 && self.heap.compare(self.heap.items[0], item) {
			poped = heap.Pop(self.heap).(*HeapItem).Value.(PQItem)
			delete(self.lookupMap, poped.GetKey())
		} else {
			return
		}
	}

	heap.Push(self.heap, item)
	index = item.index
	self.lookupMap[key] = item
	return
}

func (self *PQ) Pop() PQItem {
	if self.heap.Len() == 0 {
		return nil
	}
	poped := heap.Pop(self.heap).(*HeapItem).Value.(PQItem)
	delete(self.lookupMap, poped.GetKey())
	return poped
}

func (self *PQ) Remove(key string) PQItem {
	if item, ok := self.lookupMap[key]; ok {
		if removed := heap.Remove(self.heap, item.index); removed != nil {
			delete(self.lookupMap, key)
			return removed.(*HeapItem).Value.(PQItem)
		}
	}
	return nil
}

func (self *PQ) Lookup(key string) int {
	if item, ok := self.lookupMap[key]; ok {
		return item.index
	} else {
		return -1
	}
}

func (self *PQ) Len() int {
	return self.heap.Len()
}
