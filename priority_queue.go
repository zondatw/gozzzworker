package gozzzworker

import "container/heap"

// TaskNode for PriorityQueue
type TaskNode struct {
	priority float64
	task     *Task
}

// PriorityQueue using minheap
type PriorityQueue []*TaskNode

func (pq PriorityQueue) Len() int {
	return len(pq)
}

func (pq PriorityQueue) Less(next, current int) bool {
	return pq[next].priority < pq[current].priority
}

func (pq PriorityQueue) Swap(next, current int) {
	pq[next], pq[current] = pq[current], pq[next]
}

func (pq *PriorityQueue) Push(x interface{}) {
	node := x.(*TaskNode)
	*pq = append(*pq, node)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	latest := len(old) - 1
	node := old[latest]
	old[latest] = nil // avoid memory leak
	*pq = old[0:latest]
	pq.updatePriority()
	return node
}

func (pq PriorityQueue) updatePriority() {
	for index := 0; index < pq.Len(); index++ {
		pq[index].priority -= 0.2
		heap.Fix(&pq, index)
	}
}
