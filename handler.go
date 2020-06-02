package gonsq

import (
	"context"
	"sync"
	"time"

	gonsq "github.com/nsqio/go-nsq"
)

// HandlerFunc for nsq
type HandlerFunc func(ctx context.Context, message *Message) error

// MiddlewareFunc for nsq middleware
type MiddlewareFunc func(handler HandlerFunc) HandlerFunc

// Message for nsq
type Message struct {
	*gonsq.Message
	Topic   string
	Channel string
	Stats   *Stats
}

type gonsqHandler struct {
	handler HandlerFunc
	topic   string
	channel string
	// Message buffer properties to buffer message
	messageBuff chan *Message
	stopC       chan struct{}
	stats       *Stats
	mu          sync.Mutex
}

// Work to handle nsq message
func (gh *gonsqHandler) Start() {
	gh.mu.Lock()
	// Guard with lock, don't let worker number
	//  goes more than concurrency number.
	if int(gh.stats.Worker()) == gh.stats.Concurrency() {
		return
	}
	gh.stats.addWorker(1)
	gh.mu.Unlock()

	go func() {
		for {
			select {
			case <-gh.stopC:
				return
			case message := <-gh.messageBuff:
				gh.handler(context.Background(), message)
				gh.stats.addMessageInBuffCount(-1)
			}
		}
	}()
}

// Stop the work of nsq handler
func (gh *gonsqHandler) Stop() {
	close(gh.stopC)
	gh.stats.addWorker(-1)
}

// NSQHandler implements nsq.Handler by implement the
// HandleMessage function.
type nsqHandler struct {
	*gonsqHandler
	client ConsumerClient

	// throttleFunc is invoked in every message consumption
	// to determine if a throttle condition needs to happen
	// at that time or not.
	throttleFunc func(*Stats) bool
	// breakThrottleFunc will be invoked in a loop, and
	// used to check if the condition is fulfilled to
	// break the throttle condition.
	breakThrottleFunc func(*Stats) bool
}

// HandleMessage of nsq
func (nh *nsqHandler) HandleMessage(message *gonsq.Message) error {
	nh.stats.addMessageCount(1)

	if nh.throttleFunc(nh.stats) {
		// Set the handler throttle to true, so all message will be throttled right away.
		// This should give signal to all handler to start the throttle mechanism, if
		// the throttle middleware is activated.
		nh.stats.setThrottle(true)
		// Pause the message consumption to NSQD by set the MaxInFlight to 0.
		nh.client.ChangeMaxInFlight(0)
		// Add the number of throttle count.
		nh.stats.addThrottleCount(1)
		for {
			// Sleep every one second to check whether the message number is already decreased in the buffer,
			// it might be better to have a lower evaluation interval, but need some metrics first.
			// The default throttling here won't affect the message consumer because messages already buffered
			// but will have some effect for the nsqd itself because we pause the message consumption from nsqd.
			time.Sleep(time.Second * 1)
			if nh.breakThrottleFunc(nh.stats) {
				// Resume the message consumption to NSQD by set the MaxInFlight to buffer size.
				nh.client.ChangeMaxInFlight(int(nh.stats.BufferLength()))
				nh.stats.setThrottle(false)
				break
			}
		}
	}

	nh.messageBuff <- &Message{
		message,
		nh.topic,
		nh.channel,
		nh.stats,
	}
	nh.stats.addMessageInBuffCount(1)
	return nil
}
