package core

// transfer2go/utils - Go utilities for transfer2go
//
// Author - Rishi Shah <rishiloyola98245@gmail.com>

import (
	"container/heap"
)

// An Item is something we manage in a priority queue.
type Item struct {
	value    TransferRequest
	priority int
	index    int
	Id       int64 // Use timestamp of request as unique id
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].priority > pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update(item *Item, value TransferRequest, priority int) {
	item.value = value
	item.priority = priority
	heap.Fix(pq, item.index)
}

func InitHeap() PriorityQueue {
	// Create a priority queue
	pq := make(PriorityQueue, 0)
	heap.Init(&pq)
	return pq
}
