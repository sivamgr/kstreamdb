package kstreamdb

import "time"

// RecordStream starts recording the tick stream in database
func (r *DB) RecordStream(s *Socket) {
	go r.doRecordingStream(s)
}

func (r *DB) doRecordingStream(s *Socket) {
	for {
		ch := s.SubscribeTicks()
		for {
			if msg, ok := <-ch; ok {
				t := msg.([]TickData)
				r.Insert(t)
			} else {
				// Channel Closed
				break
			}
		}
		// Retry connection after 10 seconds
		time.Sleep(10 * time.Second)
	}
}
