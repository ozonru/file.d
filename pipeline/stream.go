package pipeline

import (
	"sync"
)

// stream is a queue of events
// all events in queue are from one source and also has same field(eg json field "stream" in docker logs)
type stream struct {
	name     StreamName
	sourceId SourceId
	track    *track
	mu       *sync.Mutex
	cond     *sync.Cond

	first *Event
	last  *Event
}

func newStream(name StreamName, sourceId SourceId, track *track) *stream {
	stream := stream{
		name:     name,
		sourceId: sourceId,
		track:    track,
		mu:       &sync.Mutex{},
	}
	stream.cond = sync.NewCond(stream.mu)

	return &stream
}

func (s *stream) put(event *Event) {
	event.next = nil
	s.mu.Lock()
	if s.last == nil {
		s.last = event
		s.first = event

		s.cond.Signal()
	} else {
		s.last.next = event
		s.last = event
	}
	s.mu.Unlock()

	s.track.streamCh <- s
	return
}

func (s *stream) waitGet() *Event {
	s.mu.Lock()
	for s.first == nil {
		s.cond.Wait()
	}
	event := s.get()
	s.mu.Unlock()

	return event
}

func (s *stream) instantGet() *Event {
	s.mu.Lock()
	if s.first == nil {
		s.mu.Unlock()
		return nil
	}
	event := s.get()
	s.mu.Unlock()

	return event
}

func (s *stream) get() *Event {
	if s.first == s.last {
		result := s.first
		s.first = nil
		s.last = nil

		return result
	}

	result := s.first
	s.first = s.first.next

	return result
}
