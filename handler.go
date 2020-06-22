package gonsq

import (
	"context"
	"fmt"
	"time"

	nsqio "github.com/nsqio/go-nsq"
)

// HandlerFunc for nsq
type HandlerFunc func(ctx context.Context, message *Message) error

// MiddlewareFunc for nsq middleware
type MiddlewareFunc func(handler HandlerFunc) HandlerFunc

// Handler handle messages from NSQD and pass the message into the
// message handler via channel.
//
// Handler implements nsqio.Handler to consume the message from NSQD(via nsqio/go-nsq)
// and pass the message via Channel. The messages from channel is consumed
// via worker goroutines that runs when Consume method is called. The handler
// responsible to stop the worker goroutines via Stop method.
type Handler interface {
	// HandleMessage implements nsqio.Handler to directly consume
	// messages from nsqio client. HandleMessage then pass the message
	// into internal buffered channel, so the message can be consumed
	// by Consume method.
	HandleMessage(message *nsqio.Message)
	// Consume starts the worker goroutines, to consume messages
	// in the buffered channel.
	Consume() error
	// Stop stops all worker goroutines.
	Stop()
}

// Message for nsq
type Message struct {
	*nsqio.Message
	Topic   string
	Channel string
	Stats   *Stats
}

// gonsqHandler implements Handler by implementing the
// HandleMessage, Consume and Stop method.
//
// gonsqHandler implements nsq.Handler by implementing the
// HandleMessage method.
//
// The handler manage the flow control to the message handler
// by using throttling mechanism.
//
// To open the throttling mechanism, openThrottleFunc is used.
// If a given condition based on the stats is fulfilled, then
// the consumer will stop to consume message from nsq and set
// the throttle stats to true.
//
// To loosen the throttling, loosenThrottleFunc is used.
// ...
//
// To break the throttling, breakThrottleFunc is used.
// ...
type gonsqHandler struct {
	handler HandlerFunc
	client  ConsumerClient
	stopC   chan struct{}

	// messageBuff is the communication channel between NSQ
	// HandleMessage and the worker in gonsqHandler.
	messageBuff chan *Message
	stats       *Stats

	// openThrottleFunc is invoked in every message consumption
	// to determine if a throttle condition needs to happen
	// at that time or not.
	openThrottleFunc func(*Stats) bool
	// loosenThrottle is used to loosen the throttle condition
	// this will allow the consumer to consume messages at a
	// normal speed instead of throttled by the throttle middleware.
	loosenThrottleFunc func(*Stats) bool
	// breakThrottleFunc will be invoked in a loop, and
	// used to check if the condition is fulfilled to
	// break the throttle condition. When throttle is off,
	// then messages starts to consumed again from nsqd. And
	// the message consumption rate should return to normal.
	breakThrottleFunc func(*Stats) bool
}

// Work to handle nsq message
func (gh *gonsqHandler) Start() error {
	if int(gh.stats.Worker()) == gh.stats.Concurrency() {
		return fmt.Errorf("already at maximum number of concurrency: %d", gh.stats.Concurrency())
	}
	gh.stats.addWorker(1)

	for {
		select {
		case <-gh.stopC:
			return nil
		case message := <-gh.messageBuff:
			if err := gh.handler(context.Background(), message); err != nil {
				gh.stats.addErrorCount(1)
			}
			// The message in buffer count is reduced after the handler finished,
			// this means the count in buffer is going down slower. This have
			// effect for throttling, because the throttling mechanism rely on
			// the total count of message in buffer.
			gh.stats.addMessageInBuffCount(-1)
		}
	}
}

// Stop the work of nsq handler
func (gh *gonsqHandler) Stop() {
	close(gh.stopC)
	gh.stats.addWorker(-1)
}

// HandleMessage from nsqd. This function receive message directly from the go-nsq client.
// Then the message received from the client will be transformed and controlled by this
// function.
func (gh *gonsqHandler) HandleMessage(message *nsqio.Message) error {
	gh.stats.addMessageCount(1)

	if gh.openThrottleFunc(gh.stats) {
		// Set the handler throttle to true, so all message will be throttled right away.
		// This should give signal to all handler to start the throttle mechanism, if
		// the throttle middleware is activated.
		gh.stats.setThrottle(_statsThrottle)
		// Pause the message consumption to NSQD by set the MaxInFlight to 0.
		gh.client.ChangeMaxInFlight(0)
		// Add the number of throttle count.
		gh.stats.addThrottleCount(1)

		// Loop to wait until condition in breakThrottleFunc met.
		// The breakThrottleFunc will be checked first before loosenThrottleFunc is checked,
		// because there is no need to set the loosenThrottle stats if the throttle stat
		// is off.
		for {
			// Sleep every one second to check whether the message number is already decreased in the buffer,
			// it might be better to have a lower evaluation interval, but need some metrics first.
			// The default throttling here won't affect the message consumer because messages already buffered
			// but will have some effect for the nsqd itself because we pause the message consumption from nsqd.
			time.Sleep(time.Second * 1)

			// Break the throttle completely by contiue to consuming messages from nsqd.
			if gh.breakThrottleFunc(gh.stats) {
				// Resume the message consumption to NSQD by set the MaxInFlight to buffer size.
				gh.client.ChangeMaxInFlight(int(gh.stats.BufferLength()))
				gh.stats.setThrottle(0)
				break
			}

			// Loosen the throttle condition by set the throttle status to false, but don't continue to
			// consume the messages from nsqd yet, let the breakThrottleFunc continue the message consumption
			// from nsqd.
			if gh.loosenThrottleFunc(gh.stats) {
				gh.stats.setThrottle(_statsThrottleLoosen)
			}
		}
	}

	gh.messageBuff <- &Message{
		message,
		gh.client.Topic(),
		gh.client.Channel(),
		gh.stats,
	}
	gh.stats.addMessageInBuffCount(1)

	return nil
}
