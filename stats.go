// Stats for recording the statsuses of NSQ consumer and producer
//
// Worker Stats
//
// In the worker stats, statuses only shared within the worker. The worker is managed by a nsqHandler
// that control the flow of the mssage from NSQ to the internal handler inside worker. The global stats pointer
// will be included into all worker stats object. This is because we will pass the stats into the message,
// so the worker stats will also have the information of global stats. This is useful for creating a middleware
// where stats information is needed to be evaluated or throwed somewhere else.
// These following items are inside the Worker Stats:
//
// - total_message_count
// - total_error_count
// - total_message_in_buffer_count
// - total_buffer_length
// - throttled
// - total_concurrency_count
// - current_worker_count

package gonsq

import (
	"sync"
	"sync/atomic"
)

// Throttleb is a boolean type for throttle status.
type Throttleb bool

// Int return int value of throttle status
func (t Throttleb) Int() int {
	if !t {
		return 0
	}
	return 1
}

// Boolean return boolean value of throttle status
func (t Throttleb) Boolean() bool {
	return bool(t)
}

// Stats object to be included in every nsq consumer worker
// to collect statuses of nsq consumers.
type Stats struct {
	// MessageCount is the total count of the message consumed.
	// This stat need atomic.
	messageCount uint64
	// ErrorCount is the total count of error when processing message.
	// This stat need atomic.
	errorCount uint64
	// MessageInBuffCount is the total number of message in buffered channel.
	// This number will determine if the message processing need to be
	// throttled  or not.
	// This stat need atomic.
	messageInBuffCount int64
	// BufferLength is the length of buffered channel for message queue,
	// the number of buffer length is replacing the number of MaxInFlight
	// NSQ configuration. Because gonsq will set MaxInFlight to the number
	// of BufferLength.
	bufferLength int
	// Worker is the current number of message processing worker.
	worker int64
	// Throttle is the status of throttle, true means throttle is on.
	throttle Throttleb
	// Concurrency is the number of concurrency intended for the consumer.
	concurrency int
	maxInFlight int
	mu          sync.Mutex
}

func (s *Stats) addMessageCount(count uint64) uint64 {
	return atomic.AddUint64(&s.messageCount, count)
}

// MessageCount return the total number of messages retrieved from NSQ.
func (s *Stats) MessageCount() uint64 {
	return atomic.LoadUint64(&s.messageCount)
}

// MessageCount return the total number of messages retrieved from NSQ.
func (s *Stats) addErrorCount(count uint64) uint64 {
	return atomic.AddUint64(&s.errorCount, count)
}

// ErrorCount return the total number of error when handle nsq message.
func (s *Stats) ErrorCount() uint64 {
	return atomic.LoadUint64(&s.errorCount)
}

func (s *Stats) setConcurrency(n int) {
	s.concurrency = n
}

// Concurrency return the number of concurrency in a handler.
func (s *Stats) Concurrency() int {
	return s.concurrency
}

func (s *Stats) setMaxInFlight(n int) {
	s.maxInFlight = n
}

// MaxInFlight return the number of maxInFlight used to calculate buffer length
func (s *Stats) MaxInFlight() int {
	return s.maxInFlight
}

func (s *Stats) setBufferLength(n int) {
	s.bufferLength = n
}

// BufferLength return length of the buffer used in a message handler
func (s *Stats) BufferLength() int {
	return s.bufferLength
}

func (s *Stats) addMessageInBuffCount(count int64) int64 {
	return atomic.AddInt64(&s.messageInBuffCount, count)
}

// MessageInBuffer return the total number of messages in buffer
func (s *Stats) MessageInBuffer() int64 {
	return atomic.LoadInt64(&s.messageInBuffCount)
}

func (s *Stats) addWorker(n int64) int64 {
	return atomic.AddInt64(&s.worker, n)
}

// Worker return the current number of worker in a message handler.
func (s *Stats) Worker() int64 {
	return s.worker
}

func (s *Stats) setThrottle(b bool) Throttleb {
	s.throttle = Throttleb(b)
	return s.throttle
}

// Throttle return whether the consumer/producer is being throttled or not.
func (s *Stats) Throttle() Throttleb {
	return s.throttle
}
