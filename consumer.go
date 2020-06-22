package gonsq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	nsqio "github.com/nsqio/go-nsq"
)

// ConsumerClient is he consumer client of NSQ.
// This backend implements all communication protocol
// to lookupd and nsqd servers.
type ConsumerClient interface {
	Topic() string
	Channel() string
	Stop()
	AddHandler(handler nsqio.Handler)
	AddConcurrentHandlers(handler nsqio.Handler, concurrency int)
	ConnectToNSQLookupds(addresses []string) error
	ChangeMaxInFlight(n int)
	Concurrency() int
	MaxInFlight() int
}

// ConsumerManager manage the consumer flow control. The ConsumerManager manages
// multiple nsq consumers client, and expose apis for message handler to handle
// the incoming messages. The ConsumerManager also manage the lifecycle of the
// nsq consumers client and the concurrent handlers(start and stop).
type ConsumerManager struct {
	mu          sync.RWMutex
	handlers    map[string]*gonsqHandler
	middlewares []MiddlewareFunc

	startMu sync.Mutex
	started bool

	// Default functions for throttling.
	OpenThrottleFunc   func(*Stats) bool
	BreaKThrottleFunc  func(*Stats) bool
	LoosenThrottleFunc func(*Stats) bool

	// Error saves error in handle method, because
	// handle method does not return any error.
	// This error then evaluated when the manager
	// starts.
	err  error
	errC chan error
}

// ManageConsumers creates a new ConsumerManager
func ManageConsumers(clients ...ConsumerClient) (*ConsumerManager, error) {
	c := ConsumerManager{
		handlers:           make(map[string]*gonsqHandler),
		errC:               make(chan error),
		OpenThrottleFunc:   defaultOpenThrottleFunc,
		BreaKThrottleFunc:  defaultBreakThrottleFunc,
		LoosenThrottleFunc: defaultLoosenThrottleFunc,
	}
	return &c, c.AddConsumers(clients...)
}

// AddConsumers add more consumers to the consumer object.
func (c *ConsumerManager) AddConsumers(clients ...ConsumerClient) error {
	for _, cl := range clients {
		concurrency := cl.Concurrency()
		maxInFlight := cl.MaxInFlight()

		if concurrency <= 0 || maxInFlight <= 0 {
			return fmt.Errorf("%w,concurrency: %d, maxInFlight: %d", ErrInvalidConcurrencyConfiguration, cl.Concurrency(), cl.MaxInFlight())
		}

		// Determine the maximum length of buffer based on concurrency number
		// for example, the concurrency have multiplication factor of 5.
		//
		// |message_processed|buffer|buffer|buffer|limit|
		//          1           2     3      4      5
		//
		// Or in throttling case.
		//
		// |message_processed|buffer|throttle_limit|throttle_limit|limit|
		//          1           2            3             4         5
		//
		buffLen := concurrency * maxInFlight
		gHandler := &gonsqHandler{
			client:             cl,
			messageBuff:        make(chan *Message, buffLen),
			stats:              &Stats{},
			stopC:              make(chan struct{}),
			openThrottleFunc:   c.OpenThrottleFunc,
			loosenThrottleFunc: c.BreaKThrottleFunc,
			breakThrottleFunc:  c.LoosenThrottleFunc,
		}
		gHandler.stats.setConcurrency(concurrency)
		gHandler.stats.setMaxInFlight(maxInFlight)
		gHandler.stats.setBufferLength(buffLen)

		c.mu.Lock()
		c.handlers[mergeTopicAndChannel(cl.Topic(), cl.Channel())] = gHandler
		c.mu.Unlock()

	}
	return nil
}

func mergeTopicAndChannel(topic, channel string) string {
	return topic + ";" + channel
}

// Use middleware, this should be called before handle function
// this function will avoid to add the same middleware twice
// if the same middleware is used, it will skip the addition.
func (c *ConsumerManager) Use(middleware ...MiddlewareFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check whether the middleware is already exits if the middleware
	// already exist, avoid adding the middleware.
	for _, m := range middleware {
		found := false
		for _, im := range c.middlewares {
			if &im == &m {
				found = true
				break
			}
		}
		if !found {
			c.middlewares = append(c.middlewares, m)
		}
	}
}

// Handle to register the message handler function.
// Only for reigstering the message handler into the consumer.
func (c *ConsumerManager) Handle(topic, channel string, handler HandlerFunc) {
	for i := range c.middlewares {
		handler = c.middlewares[len(c.middlewares)-i-1](handler)
	}

	c.mu.RLock()
	gHandler, ok := c.handlers[mergeTopicAndChannel(topic, channel)]
	c.mu.RUnlock()

	if !ok {
		c.err = fmt.Errorf("gonsq: consumer with topic %s and channel %s does not exist", topic, channel)
		return
	}

	if gHandler.handler != nil {
		c.err = fmt.Errorf("gonsq: handler for topic %s and channel %s registered twice", topic, channel)
		return
	}

	// Set the gonsq handler.
	gHandler.handler = handler
	// Set the nsq handler for HandleMessage.
	// Use AddHandler instead of AddConcurrentHandler,
	// consuming message from nsqd is very fast and
	// most of the timewe don't need to use concurrent handler.
	gHandler.client.AddHandler(gHandler)
	// Change the MaxInFlight to buffLength as the number
	// of message won't exceed the buffLength.
	gHandler.client.ChangeMaxInFlight(gHandler.stats.BufferLength())

	c.mu.Lock()
	c.handlers[mergeTopicAndChannel(topic, channel)] = gHandler
	c.mu.Unlock()
}

// Start for start the consumer. This will trigger all workers to start.
func (c *ConsumerManager) Start(lookupdsAddr []string) error {
	if lookupdsAddr == nil {
		return ErrLookupdsAddrEmpty
	}

	c.startMu.Lock()

	if c.err != nil {
		c.startMu.Unlock()
		return c.err
	}

	if c.started {
		c.startMu.Unlock()
		return nil
	}

	// List of clients to separate the invocation of spawning
	// consumer for consuming message and connecting the client
	// to the NSQlookupds.
	var clients []ConsumerClient

	for _, handler := range c.handlers {
		// Invoke all handler to work, depends
		// on the concurrency number.
		for i := 0; i < handler.stats.Concurrency(); i++ {
			go func(handler *gonsqHandler) {
				err := handler.Start()
				if err != nil {
					c.errC <- err
				}
			}(handler)
		}

		clients = append(clients, handler.client)
	}

	// Connect to all NSQLookupds.
	for _, client := range clients {
		go func(client ConsumerClient) {
			if err := client.ConnectToNSQLookupds(lookupdsAddr); err != nil {
				c.errC <- fmt.Errorf("nsqio: topoic: %s channel: %s error connecting to lookupds: %v. Error: %v", client.Topic(), client.Channel(), lookupdsAddr, err)
			}
		}(client)
	}

	clients = nil
	c.started = true
	c.startMu.Unlock()

	return <-c.errC
}

// Stop for stopping all the nsq consumer.
func (c *ConsumerManager) Stop(ctx context.Context) (err error) {
	errC := make(chan error)

	go func() {
		errC <- c.stop()
	}()

	select {
	case err = <-errC:
	case <-ctx.Done():
		err = ErrStopDeadlineExceeded
	}

	return
}

func (c *ConsumerManager) stop() error {
	if len(c.handlers) == 0 {
		return nil
	}

	c.startMu.Lock()
	defer c.startMu.Unlock()

	if !c.started {
		return errors.New("gonsq: consumer manager already stopped")
	}

	// Stopping all NSQ clients. This should make message consumption
	// to nsqHandler stop.
	for _, handler := range c.handlers {
		handler.client.Stop()
	}

	for _, handler := range c.handlers {
		// Wait until all messages consumed, stopping gracefully.
		for handler.stats.MessageInBuffer() != 0 {
			time.Sleep(time.Millisecond * 300)
		}
		// Stop all the handler worker based on concurrency number
		// this step is expected to be blocking,
		// wait until all worker is exited.
		for i := 0; i < handler.stats.Concurrency(); i++ {
			handler.Stop()
		}
	}

	// Make the start function to return.
	close(c.errC)
	c.started = false

	return nil
}

func defaultOpenThrottleFunc(stats *Stats) bool {
	// Message in the buffer should always less than bufferLength/2
	// if its already more than half of the buffer size, we should pause the consumption
	// and wait for the buffer to be consumed first.
	if int(stats.MessageInBuffer()) >= (stats.BufferLength() / 2) {
		return true
	}
	return false
}

func defaultLoosenThrottleFunc(stats *Stats) bool {
	return false
}

func defaultBreakThrottleFunc(stats *Stats) bool {
	// Release the throttle status when message is already reduced to less
	// than half of buffer size.
	if int(stats.MessageInBuffer()) < (stats.BufferLength() / 2) {
		return true
	}
	return false
}
