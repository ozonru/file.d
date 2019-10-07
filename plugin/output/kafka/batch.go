package kafka

import (
	"time"

	"github.com/Shopify/sarama"
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type batch struct {
	events    []*pipeline.Event
	messages  []*sarama.ProducerMessage
	seq       int64
	size      int
	timeout   time.Duration
	startTime time.Time
}

func newBatch(size int, timeout time.Duration) *batch {
	if size <= 0 {
		logger.Fatalf("why batch size is 0?")
	}

	return &batch{
		size:     size,
		timeout:  timeout,
		events:   make([]*pipeline.Event, 0, size),
		messages: make([]*sarama.ProducerMessage, size, size),
	}
}

func (b *batch) reset() {
	b.events = b.events[:0]
	b.startTime = time.Now()
}

func (b *batch) append(e *pipeline.Event) {
	b.events = append(b.events, e)
}

func (b *batch) isReady() bool {
	l := len(b.events)
	isFull := l == b.size
	isTimeout := l > 0 && time.Now().Sub(b.startTime) > b.timeout
	return isFull || isTimeout
}
