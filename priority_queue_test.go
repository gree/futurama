package futurama

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/stretchr/testify/assert"
	"testing"
)

type StringPQItem string

func (self StringPQItem) GetKey() string {
	return string(self)
}

type IntPQItem int

func (self IntPQItem) GetKey() string {
	return fmt.Sprintf("%03d", int(self))
}

func TestPQ_pushpop(t *testing.T) {
	max := 10
	values := []StringPQItem{"a", "b", "c", "d", "e"}
	priorities := []int64{3, 5, 4, 1, 2}

	func() {
		pq := NewPQ(true, max)
		for i := 0; i < 5; i++ {
			index, poped := pq.Push(values[i], priorities[i])
			if index == -1 || poped != nil {
				t.Errorf("Not expecting anything to be poped")
			}
		}

		popedValues := make([]StringPQItem, 0)
		for i := 0; i < 5; i++ {
			popedValues = append(popedValues, pq.Pop().(StringPQItem))
		}
		glog.Infoln("poped values", popedValues)
		assert.Equal(t, popedValues, []StringPQItem{"b", "c", "a", "e", "d"})
	}()

	func() {
		pq := NewPQ(false, max)
		for i := 0; i < 5; i++ {
			index, poped := pq.Push(values[i], priorities[i])
			if index == -1 || poped != nil {
				t.Errorf("Not expecting anything to be poped")
			}
		}

		popedValues := make([]StringPQItem, 0)
		for i := 0; i < 5; i++ {
			popedValues = append(popedValues, pq.Pop().(StringPQItem))
		}
		glog.Infoln("poped values", popedValues)
		assert.Equal(t, popedValues, []StringPQItem{"d", "e", "a", "c", "b"})
	}()
}

func TestPQ_index(t *testing.T) {
	max := 10
	pq := NewPQ(true, max)
	pq.Push(StringPQItem("a"), 10)
	pq.Push(StringPQItem("b"), 15)
	pq.Push(StringPQItem("c"), 12)

	for i := 0; i < 3; i++ {
		if pq.heap.items[i].index != i {
			t.Errorf("Unexpected item index %d: %s", i, pq.heap.items[i])
		}
	}

	if removed := pq.Remove("c"); removed == nil {
		t.Errorf("Unexpected remove")
	}

	if removed := pq.Remove("X"); removed != nil {
		t.Errorf("Unexpected remove")
	}

	popedValues := make([]StringPQItem, 0)
	for i := 0; i < 2; i++ {
		popedValues = append(popedValues, pq.Pop().(StringPQItem))
	}
	glog.Infoln("poped values", popedValues)
	assert.Equal(t, popedValues, []StringPQItem{"b", "a"})
}

func TestPQ_limit(t *testing.T) {
	max := 10
	pq := NewPQ(true, max)

	if a := pq.Pop(); a != nil {
		t.Errorf("Expecting a = nil")
	}

	for i := 0; i < max; i++ {
		index, poped := pq.Push(IntPQItem(i), 10)
		if index != i {
			t.Errorf("Expecting index %d but got %d", i, index)
		}
		if poped != nil {
			t.Errorf("Not expecting anything to be poped")
		}
	}

	func() {
		// can not push a new item with same priority
		index, poped := pq.Push(IntPQItem(99), 10)
		glog.Infof("result from pq.Push, index %d poped %v", index, poped)
		if index != -1 {
			t.Errorf("Expecting index %d but got %d", -1, index)
		}
		if poped != nil {
			t.Errorf("Not expecting anything to be poped")
		}
	}()

	func() {
		// can push a new item with lower priority
		// an item should be poped out from pq
		index, poped := pq.Push(IntPQItem(99), 9)
		glog.Infof("result from pq.Push, index %d poped %v", index, poped)
		if index == -1 {
			t.Errorf("Not expecting index = %d", -1)
		}
		if poped == nil {
			t.Errorf("Not expecting poped = nil")
		}
	}()

	func() {
		// can not push a new item with higher priority
		index, poped := pq.Push(IntPQItem(99), 11)
		glog.Infof("result from pq.Push, index %d poped %v", index, poped)
		if index != -1 {
			t.Errorf("Not expecting index = %d", -1)
		}
		if poped != nil {
			t.Errorf("Not expecting poped = nil")
		}
	}()
}

func TestPQ_0capacity(t *testing.T) {
	pq := NewPQ(false, 0)

	if a := pq.Pop(); a != nil {
		t.Errorf("Expecting a = nil")
	}

	index, poped := pq.Push(StringPQItem("a"), 0)
	if index != -1 {
		t.Errorf("Expecting index = -1")
	}
	if poped != nil {
		t.Errorf("Expecting poped = nil")
	}

	if removed := pq.Remove("XX"); removed != nil {
		t.Errorf("Expecting removed = nil")
	}
}

func TestPQ_popedInOrder(t *testing.T) {
	pq := NewPQ(false, 5)
	func() {
		items := []StringPQItem{"a", "b", "c", "d", "e"}
		priorities := []int64{12, 14, 13, 11, 15}
		tops := []StringPQItem{"a", "a", "a", "d", "d"}

		for i, v := range items {
			index, poped := pq.Push(v, priorities[i])
			if index < 0 {
				t.Errorf("index should be positive", index, v)
			}
			if poped != nil {
				t.Errorf("poped should be nil", poped, v)
			}
			assert.Equal(t, pq.Len(), i+1)
			assert.Equal(t, pq.Top().(StringPQItem), tops[i])
		}
	}()

	func() { // these 5 items will be rejected, because queue is full and they have lower priorities
		items := []StringPQItem{"f", "g", "h", "i", "j"}
		priorities := []int64{2, 4, 3, 1, 5}

		for i, v := range items {
			index, poped := pq.Push(v, priorities[i])
			if index >= 0 {
				t.Errorf("index should be -1", index, v)
			}
			if poped != nil {
				t.Errorf("poped should be nil", poped, v)
			}
			assert.Equal(t, pq.Len(), 5)
			assert.Equal(t, pq.Top().(StringPQItem), StringPQItem("d"))
		}
	}()

	expected := []StringPQItem{"d", "a", "c", "b", "e"}
	got := make([]StringPQItem, 0)

	func() {
		items := []StringPQItem{"f", "g", "h", "i", "j"}
		priorities := []int64{102, 104, 103, 101, 105}
		tops := []StringPQItem{"a", "c", "b", "e", "i"}

		for i, v := range items {
			index, poped := pq.Push(v, priorities[i])
			if index < 0 {
				t.Errorf("index should be positive", index, v)
			}
			if poped == nil {
				t.Errorf("poped should NOT be nil", poped, v)
			}
			glog.Infof("poped value %s", poped)
			got = append(got, poped.(StringPQItem))

			assert.Equal(t, pq.Len(), 5)
			assert.Equal(t, pq.Top().(StringPQItem), tops[i])
		}
	}()

	assert.Equal(t, got, expected)
}

func TestPQ_popedInOrder_reversed(t *testing.T) {
	pq := NewPQ(true, 5)
	func() {
		items := []StringPQItem{"a", "b", "c", "d", "e"}
		priorities := []int64{12, 14, 13, 11, 15}
		tops := []StringPQItem{"a", "b", "b", "b", "e"}

		for i, v := range items {
			index, poped := pq.Push(v, priorities[i])
			if index < 0 {
				t.Errorf("index should be positive", index, v)
			}
			if poped != nil {
				t.Errorf("poped should be nil", poped, v)
			}
			assert.Equal(t, pq.Len(), i+1)
			assert.Equal(t, pq.Top().(StringPQItem), tops[i])
		}
	}()

	func() { // these 5 items will be rejected, because queue is full and they have lower priorities
		items := []StringPQItem{"f", "g", "h", "i", "j"}
		priorities := []int64{102, 104, 103, 101, 105}

		for i, v := range items {
			index, poped := pq.Push(v, priorities[i])
			if index >= 0 {
				t.Errorf("index should be -1", index, v)
			}
			if poped != nil {
				t.Errorf("poped should be nil", poped, v)
			}
			assert.Equal(t, pq.Len(), 5)
			assert.Equal(t, pq.Top().(StringPQItem), StringPQItem("e"))
		}
	}()

	expected := []StringPQItem{"e", "b", "c", "a", "d"}
	got := make([]StringPQItem, 0)

	func() {
		items := []StringPQItem{"f", "g", "h", "i", "j"}
		priorities := []int64{2, 4, 3, 1, 5}
		tops := []StringPQItem{"b", "c", "a", "d", "j"}

		for i, v := range items {
			index, poped := pq.Push(v, priorities[i])
			if index < 0 {
				t.Errorf("index should be positive", index, v)
			}
			if poped == nil {
				t.Errorf("poped should NOT be nil", poped, v)
			}
			glog.Infof("poped value %s", poped)
			got = append(got, poped.(StringPQItem))

			assert.Equal(t, pq.Len(), 5)
			assert.Equal(t, pq.Top().(StringPQItem), tops[i])
		}
	}()

	assert.Equal(t, got, expected)
}

func TestPQ_lookup(t *testing.T) {
	pq := NewPQ(true, 5)

	items := []StringPQItem{"a", "b", "c", "d", "e"}
	priorities := []int64{12, 14, 13, 11, 15}
	lookupIndexesBefore := []int{0, 0, 2, 3, 0}

	for i, v := range items {
		index, poped := pq.Push(v, priorities[i])
		if index < 0 {
			t.Errorf("index should be positive", index, v)
		}
		if poped != nil {
			t.Errorf("poped should be nil", poped, v)
		}
		lookupIndex := pq.Lookup(string(v))
		assert.Equal(t, lookupIndex, lookupIndexesBefore[i])
		glog.Infof("lookup index %d", lookupIndex)
	}

	lookupIndexesAfter := []int{4, 1, 2, 3, 0}
	for i, v := range items {
		lookupIndex := pq.Lookup(string(v))
		assert.Equal(t, lookupIndex, lookupIndexesAfter[i])
		glog.Infof("lookup index %d", lookupIndex)
	}
}

// NOTE: need to make sure following is working
// push(group1) -> remove -> push(sth else) -> remove(elements in group1)
func TestPQ_remove(t *testing.T) {
	pq := NewPQ(true, 100)

	items1 := []StringPQItem{"a", "b", "c", "d", "e"}
	priorities1 := []int64{12, 14, 13, 21, 25}

	for i, v := range items1 {
		index, poped := pq.Push(v, priorities1[i])
		if index < 0 {
			t.Errorf("index should be positive", index, v)
		}
		if poped != nil {
			t.Errorf("poped should be nil", poped, v)
		}
	}

	func() {
		removed := pq.Remove("unknown")
		if removed != nil {
			t.Errorf("should return nil for removing unknown element")
		}
	}()

	func() {
		removed := pq.Remove("d")
		assert.Equal(t, removed, items1[3])
	}()

	items2 := []StringPQItem{"f", "g", "h", "i", "j"}
	priorities2 := []int64{2, 4, 103, 101, 15}

	for i, v := range items2 {
		index, poped := pq.Push(v, priorities2[i])
		if index < 0 {
			t.Errorf("index should be positive", index, v)
		}
		if poped != nil {
			t.Errorf("poped should be nil", poped, v)
		}
	}

	func() {
		removed := pq.Remove("c")
		assert.Equal(t, removed, items1[2])
	}()

	func() {
		removed := pq.Remove("e")
		assert.Equal(t, removed, items1[4])
	}()

	func() {
		removed := pq.Remove("i")
		assert.Equal(t, removed, items2[3])
	}()
}
