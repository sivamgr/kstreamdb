package kstreamdb

type tickQueue struct {
	q        []TickData
	len      int
	capacity int
}

func newTickQueue(capacity int) *tickQueue {
	q := new(tickQueue)
	q.q = make([]TickData, capacity)
	q.capacity = capacity
	q.len = 0
	return q
}

func (q *tickQueue) isFull() bool {
	return (q.len >= q.capacity)
}

func (q *tickQueue) isEmpty() bool {
	return (q.len >= q.capacity)
}

func (q *tickQueue) clear() {
	q.len = 0
}

func (q *tickQueue) put(t TickData) {
	if !q.isFull() {
		q.q[q.len] = t
		q.len++
	}
}
