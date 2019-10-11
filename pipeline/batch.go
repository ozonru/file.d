package pipeline

import (
	"sync"
	"time"

	"gitlab.ozon.ru/sre/filed/logger"
)

type Batch struct {
	Events    []*Event
	seq       int64
	size      int
	timeout   time.Duration
	startTime time.Time
}

func newBatch(size int, timeout time.Duration) *Batch {
	if size <= 0 {
		logger.Fatalf("why batch size is 0?")
	}

	return &Batch{
		size:    size,
		timeout: timeout,
		Events:  make([]*Event, 0, size),
	}
}

func (b *Batch) reset() {
	b.Events = b.Events[:0]
	b.startTime = time.Now()
}

func (b *Batch) append(e *Event) {
	b.Events = append(b.Events, e)
}

func (b *Batch) isReady() bool {
	l := len(b.Events)
	isFull := l == b.size
	isTimeout := l > 0 && time.Now().Sub(b.startTime) > b.timeout
	return isFull || isTimeout
}

type Batcher struct {
	pipelineName        string
	outputType          string
	outFn               BatcherOutFn
	maintenanceFn       BatcherMaintenanceFn
	tail                Tail
	workerCount         int
	batchSize           int
	flushTimeout        time.Duration
	maintenanceInterval time.Duration

	shouldStop bool
	batch      *Batch

	// cycle of batches: freeBatches => fullBatches, fullBatches => freeBatches
	freeBatches chan *Batch
	fullBatches chan *Batch
	mu          *sync.Mutex
	cond        *sync.Cond

	outSeq    int64
	commitSeq int64
}

type BatcherOutFn func(*WorkerData, *Batch)
type BatcherMaintenanceFn func(*WorkerData)

func NewBatcher(
	pipelineName string,
	outputType string,
	outFn BatcherOutFn,
	maintenanceFn BatcherMaintenanceFn,
	tail Tail,
	workers int,
	batchSize int,
	flushTimeout time.Duration,
	maintenanceInterval time.Duration,
) *Batcher {
	return &Batcher{
		pipelineName:        pipelineName,
		outputType:          outputType,
		outFn:               outFn,
		maintenanceFn:       maintenanceFn,
		tail:                tail,
		workerCount:         workers,
		batchSize:           batchSize,
		flushTimeout:        flushTimeout,
		maintenanceInterval: maintenanceInterval,
	}
}

func (b *Batcher) Start() {
	b.mu = &sync.Mutex{}
	b.cond = sync.NewCond(b.mu)

	go b.heartbeat()

	b.freeBatches = make(chan *Batch, b.workerCount)
	b.fullBatches = make(chan *Batch, b.workerCount)
	for i := 0; i < b.workerCount; i++ {
		b.freeBatches <- newBatch(b.batchSize, b.flushTimeout)
		go b.work()
	}
}

type WorkerData interface{}

func (b *Batcher) work() {
	t := time.Now()
	events := make([]*Event, 0, 0)
	data := WorkerData(nil)
	for batch := range b.fullBatches {
		b.outFn(&data, batch)
		events = b.commitBatch(events, batch)

		shouldRunMaintenance := b.maintenanceFn != nil && b.maintenanceInterval != 0 && time.Now().Sub(t) > b.maintenanceInterval
		if shouldRunMaintenance {
			t = time.Now()
			b.maintenanceFn(&data)
		}
	}
}

func (b *Batcher) commitBatch(events []*Event, batch *Batch) []*Event {
	// we need to release batch first and then commit events
	// so lets exchange local slice with batch slice to avoid data copying
	tmp := events
	events = batch.Events
	batch.Events = tmp

	batchSeq := batch.seq

	if !b.shouldStop {
		b.freeBatches <- batch
	}

	// lets restore the sequence of batches to make sure input will commit offsets incrementally
	b.mu.Lock()
	for b.commitSeq != batchSeq {
		b.cond.Wait()
	}
	b.commitSeq++

	for _, e := range events {
		b.tail.Commit(e)
	}

	logger.Infof("output has written a batch type=%s, pipeline=%s, events=%d", b.outputType, b.pipelineName, len(events))

	b.cond.Broadcast()
	b.mu.Unlock()

	return events
}

func (b *Batcher) heartbeat() {
	for {
		if b.shouldStop {
			return
		}

		b.mu.Lock()
		batch := b.getBatch()
		b.trySendBatchAndUnlock(batch)

		time.Sleep(time.Millisecond * 100)
	}
}

func (b *Batcher) Add(event *Event) {
	b.mu.Lock()

	batch := b.getBatch()
	batch.append(event)

	b.trySendBatchAndUnlock(batch)
}

// trySendBatch mu should be locked and it'll be unlocked after execution of this function
func (b *Batcher) trySendBatchAndUnlock(batch *Batch) {
	if !batch.isReady() {
		b.mu.Unlock()
		return
	}

	batch.seq = b.outSeq
	b.outSeq++
	b.batch = nil
	b.mu.Unlock()

	b.fullBatches <- batch
}

func (b *Batcher) getBatch() *Batch {
	if b.batch == nil {
		b.batch = <-b.freeBatches
		b.batch.reset()
	}
	return b.batch
}

func (b *Batcher) Stop() {
	b.shouldStop = true
	close(b.freeBatches)
	close(b.fullBatches)
}
