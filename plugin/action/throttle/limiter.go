package throttle

import (
	"sync"
	"time"
)

type limiter struct {
	limit       int64 // maximum number of events per bucket
	bucketCount int
	buckets     []int64
	interval    time.Duration // bucket interval
	minID       int           // minimum bucket id
	mu          sync.Mutex
}

func NewLimiter(interval time.Duration, bucketCount int, limit int64) *limiter {
	return &limiter{
		interval:    interval,
		bucketCount: bucketCount,
		limit:       limit,

		buckets: make([]int64, bucketCount),
	}
}

// isAllowed returns TRUE if event is allowed to be processed.
func (l *limiter) isAllowed(ts time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.minID == 0 {
		l.minID = l.timeToBucketID(ts) - l.bucketCount + 1
	}
	maxID := l.minID + len(l.buckets) - 1
	id := l.timeToBucketID(ts)

	// limiter doesn't track that bucket anymore.
	if id < l.minID {
		return false
	}

	// event from a new bucket, add N new buckets
	if id > maxID {
		n := id - maxID
		for i := 0; i < n; i++ {
			l.buckets = append(l.buckets, 0)
		}

		// remove old ones
		l.buckets = l.buckets[n:]

		// and set new min index
		l.minID += n
	}

	index := id - l.minID
	l.buckets[index]++

	return l.buckets[index] <= l.limit
}

// bucketIDToTime converts bucketID to time. This time is start of the bucket.
func (l *limiter) bucketIDToTime(id int) time.Time {
	nano := int64(id) * l.interval.Nanoseconds()
	return time.Unix(nano/100000000, nano%100000000)
}

// timeToBucketID converts time to bucketID.
func (l *limiter) timeToBucketID(t time.Time) int {
	return int(t.UnixNano() / l.interval.Nanoseconds())
}
