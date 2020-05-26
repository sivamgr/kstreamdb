package kstreamdb

import "time"

// RecordStream starts recording the tick stream in database
func (r *DB) RecordStream(s *Socket) {
	go r.doRecordingStream(s)
}

func (r *DB) doRecordingStream(s *Socket) {
	que := newTickQueue(200)
	ch := s.SubscribeTicks()
	for {
		select {
		case msg := <-ch:
			t := msg.(TickData)
			que.put(t)
		case <-time.After(1 * time.Second):
			if !que.isEmpty() {
				r.Insert(que.q[0:que.len])
				que.clear()
			}
		}
	}
}

func (q *tickQueue) writeQueueToDb(db *DB) {
	if q.len > 0 {
		db.Insert(q.q[0:q.len])
		q.len = 0
	}
}
