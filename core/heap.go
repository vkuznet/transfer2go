package core

// transfer2go/utils - Go utilities for transfer2go
//
// Author - Rishi Shah <rishiloyola98245@gmail.com>

import (
	"container/heap"
)

// An Item is something we manage in a priority queue.
type Item struct {
	Value    TransferRequest
	priority int
	index    int
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

// Delete request from PriorityQueue. The complexity is O(n) where n = heap.Len()
func (pq *PriorityQueue) Delete(id int64) bool {
	index := -1
	for _, item := range *pq {
		if item.Value.Id == id {
			index = item.index
			break
		}
	}
	// Varify request id exists in heap and then delete it
	if index < RequestQueue.Len() && index >= 0 {
		heap.Remove(&RequestQueue, index)
		return true
	}
	return false
}

// Get the entire list of requests
func (pq *PriorityQueue) GetAllRequest() []TransferRequest {
	var requests []TransferRequest
	for _, item := range *pq {
		requests = append(requests, item.Value)
	}
	return requests
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update(item *Item, value TransferRequest, priority int) {
	item.Value = value
	item.priority = priority
	heap.Fix(pq, item.index)
}
