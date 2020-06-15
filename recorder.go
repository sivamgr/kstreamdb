package kstreamdb

import "time"

// RecordStream starts recording the tick stream in database
func (r *DB) RecordStream(s *Socket) {
	go r.doRecordingStream(s)
}

func (r *DB) doRecordingStream(s *Socket) {
	que := newTickQueue(200)
	ch := s.SubscribeTicks()
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case msg := <-ch:
			t := msg.(TickData)
			que.put(t)
			if que.isFull() {
				que.writeQueueToDb(r)
			}
		case <-ticker.C:
			que.writeQueueToDb(r)
		}
	}
}

func (q *tickQueue) writeQueueToDb(db *DB) {
	if !q.isEmpty() {
		db.Insert(q.q[0:q.len])
		q.clear()
	}
}
