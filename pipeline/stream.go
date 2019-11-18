package pipeline

import (
	"sync"
	"time"

	"gitlab.ozon.ru/sre/filed/logger"
)

var eventWaitTimeout = time.Second * 30

// stream is a queue of events
// events fall into stream based on rules defined by input plugin
// streams are used to allow event joins and other operations which needs sequential event input
// e.g. events from same file will be in same stream for "file" input plugin
// todo: remove dependency on streamer
type stream struct {
	chargeIndex int
	blockIndex  int

	name       StreamName
	sourceID   SourceID
	sourceName string
	streamer   *streamer
	blockTime  time.Time

	mu   *sync.Mutex
	cond *sync.Cond

	isDetaching bool
	isAttached  bool
	commitID    uint64
	awayID      uint64

	first *Event
	last  *Event
}

func newStream(name StreamName, sourceID SourceID, sourceName string, streamer *streamer) *stream {
	stream := stream{
		chargeIndex: -1,
		name:        name,
		sourceID:    sourceID,
		sourceName:  sourceName,
		streamer:    streamer,
		mu:          &sync.Mutex{},
	}
	stream.cond = sync.NewCond(stream.mu)

	return &stream
}

func (s *stream) leave() {
	if s.isDetaching {
		logger.Panicf("why detach? stream is already detaching")
	}
	if !s.isAttached {
		logger.Panicf("why detach? stream isn't attached")
	}
	s.isDetaching = true
	s.tryDropProcessor()
}

func (s *stream) commit(event *Event) {
	s.mu.Lock()
	// maxID is needed here because discarded events with bigger offsets may be
	// committed faster than events with lower offsets which are goes through output
	if event.SeqID < s.commitID {
		s.mu.Unlock()
		return
	}
	s.commitID = event.SeqID

	if s.isDetaching {
		s.tryDropProcessor()
	}
	s.mu.Unlock()
}

func (s *stream) tryDropProcessor() {
	if s.awayID != s.commitID {
		return
	}

	s.isAttached = false
	s.isDetaching = false

	if s.first != nil {
		s.streamer.makeCharged(s)
	}
}

func (s *stream) attach() {
	s.mu.Lock()
	if s.isAttached {
		logger.Panicf("why attach? processor is already attached")
	}
	if s.isDetaching {
		logger.Panicf("why attach? processor is detaching")
	}
	if s.first == nil {
		logger.Panicf("why attach? stream is empty")
	}
	s.isAttached = true
	s.isDetaching = false
	s.mu.Unlock()
}

func (s *stream) put(event *Event) {
	s.mu.Lock()
	event.stream = s
	event.stage = eventStageStream
	if s.first == nil {
		s.last = event
		s.first = event
		if !s.isAttached {
			s.streamer.makeCharged(s)
		}
		s.cond.Signal()
	} else {
		s.last.next = event
		s.last = event
	}
	s.mu.Unlock()
}

func (s *stream) blockGet() *Event {
	s.mu.Lock()
	if !s.isAttached {
		logger.Panicf("why wait get? stream isn't attached")
	}
	for s.first == nil {
		s.blockTime = time.Now()
		s.streamer.makeBlocked(s)
		s.cond.Wait()
		s.streamer.resetBlocked(s)
	}
	event := s.get()
	s.mu.Unlock()

	return event
}

func (s *stream) instantGet() *Event {
	s.mu.Lock()
	if !s.isAttached {
		logger.Panicf("why instant get? stream isn't attached")
	}
	if s.first == nil {
		s.leave()
		s.mu.Unlock()

		return nil
	}
	event := s.get()
	s.mu.Unlock()

	return event
}

func (s *stream) tryUnblock() bool {
	if s == nil {
		return false
	}

	s.mu.Lock()
	if time.Now().Sub(s.blockTime) < eventWaitTimeout {
		s.mu.Unlock()
		return false
	}

	// is it lock after put signal?
	if s.first != nil {
		s.mu.Unlock()
		return false
	}

	if s.awayID != s.commitID {
		logger.Panicf("why events are different? away event id=%d, commit event id=%d", s.awayID, s.commitID)
	}

	timeoutEvent := newTimoutEvent(s)
	s.last = timeoutEvent
	s.first = timeoutEvent

	s.cond.Signal()
	s.mu.Unlock()

	return true
}

func (s *stream) get() *Event {
	if s.isDetaching {
		logger.Panicf("why get while detaching?")
	}

	event := s.first
	if s.first == s.last {
		s.first = nil
		s.last = nil
	} else {
		s.first = s.first.next
	}

	event.stage = eventStageProcessor
	s.awayID = event.SeqID

	return event
}
