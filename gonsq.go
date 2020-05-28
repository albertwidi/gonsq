package gonsq

import (
	"errors"
	"fmt"
	"sync"
	"time"

	gonsq "github.com/nsqio/go-nsq"
)

var (
	ErrLookupdsAddrEmpty = errors.New("nsq: lookupds addresses is empty")
	// ErrTopicWithChannelNotFound for error when channel and topic is not found
	ErrTopicWithChannelNotFound = errors.New("nsq: topic and channel not found")
)

// ProducerBackend for NSQ
type ProducerBackend interface {
	Ping() error
	Publish(topic string, body []byte) error
	MultiPublish(topic string, body [][]byte) error
	Stop()
}

// ConsumerBackend for NSQ
type ConsumerBackend interface {
	Topic() string
	Channel() string
	Stop()
	AddHandler(handler gonsq.Handler)
	AddConcurrentHandlers(handler gonsq.Handler, concurrency int)
	ConnectToNSQLookupds(addresses []string) error
	ChangeMaxInFlight(n int)
	Concurrency() int
	MaxInFlight() int
}

// Producer for nsq
type Producer struct {
	producer ProducerBackend
	topics   map[string]bool
}

// WrapProducer is a function to wrap the nsq producer.
// The function receive topics parameters because in NSQ, we can publish message
// without registering any new topics. This sometimes can be problematic as
// we don't have a list of topics that we will publish the message to.
func WrapProducer(backend ProducerBackend, topics ...string) (*Producer, error) {
	p := Producer{
		producer: backend,
		topics:   make(map[string]bool),
	}
	for _, t := range topics {
		p.topics[t] = true
	}
	return &p, nil
}

// Publish message to nsqd
func (p *Producer) Publish(topic string, body []byte) error {
	if ok := p.topics[topic]; !ok {
		return errors.New("nsq: topic is not allowed to be published by this producer")
	}
	return p.producer.Publish(topic, body)
}

// MultiPublish message to nsqd
func (p *Producer) MultiPublish(topic string, body [][]byte) error {
	if ok := p.topics[topic]; !ok {
		return errors.New("nsq: topic is not allowed to be published by this producer")
	}
	return p.producer.MultiPublish(topic, body)
}

// Consumer for nsq
type Consumer struct {
	lookupdsAddr []string
	handlers     []*gonsqHandler
	middlewares  []MiddlewareFunc

	mu       sync.RWMutex
	backends map[string]map[string]ConsumerBackend

	started bool
}

// WrapConsumers of gonsq
func WrapConsumers(lookupdsAddr []string, backends ...ConsumerBackend) (*Consumer, error) {
	if lookupdsAddr == nil {
		return nil, ErrLookupdsAddrEmpty
	}

	c := Consumer{
		lookupdsAddr: lookupdsAddr,
		backends:     make(map[string]map[string]ConsumerBackend),
	}
	return &c, c.AddConsumers(backends...)
}

// AddConsumers add more consumers to the consumer object.
func (c *Consumer) AddConsumers(backends ...ConsumerBackend) error {
	for _, b := range backends {
		topic := b.Topic()
		channel := b.Channel()

		if c.backends[topic] == nil {
			c.backends[topic] = make(map[string]ConsumerBackend)
		}
		c.backends[topic][channel] = b
	}
	return nil
}

// Backends return information regarding topic and channel that avaialbe
func (c *Consumer) Backends() map[string]map[string]bool {
	m := map[string]map[string]bool{}
	for topic, channels := range c.backends {
		for channel := range channels {
			if m[topic] == nil {
				m[topic] = map[string]bool{}
			}
			m[topic][channel] = true
		}
	}
	return m
}

// Use the middleware
// use should be called before handle function
// this function will avoid to add the same middleware twice
// if the same middleware is used, it will skip the addition
func (c *Consumer) Use(middleware ...MiddlewareFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// check whether the middleware is already exits
	// if middleware already exist, avoid adding the middleware
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
func (c *Consumer) Handle(topic, channel string, handler HandlerFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := range c.middlewares {
		handler = c.middlewares[len(c.middlewares)-i-1](handler)
	}
	backend := c.backends[topic][channel]

	h := &gonsqHandler{
		topic:   topic,
		channel: channel,
		handler: handler,
		stopC:   make(chan struct{}),
		stats:   &Stats{},
	}

	// Only append this information if backend is found
	// otherwise let the handler appended without this information.
	// If backend is nil in this step, it will reproduce error when consumer start,
	// this is because the name of backends will not detected in start state
	// so its safe to skip the error here.
	if backend != nil {
		concurrency := backend.Concurrency()
		maxInFlight := backend.MaxInFlight()

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
		h.messageBuff = make(chan *Message, buffLen)
		h.stats.setConcurrency(concurrency)
		h.stats.setMaxInFlight(maxInFlight)
		h.stats.setBufferLength(buffLen)
	}
	c.handlers = append(c.handlers, h)
}

// Start for start the consumer. This will trigger all workers to start.
func (c *Consumer) Start() error {
	for _, handler := range c.handlers {
		backend, ok := c.backends[handler.topic][handler.channel]
		if !ok {
			return fmt.Errorf("nsq: backend with topoc %s and channel %s not found. error: %w", handler.topic, handler.channel, ErrTopicWithChannelNotFound)
		}

		// Create a default handler for consuming message directly from nsq handler.
		dh := nsqHandler{
			handler,
			backend,
		}
		// ConsumerConcurrency for consuming message from NSQ.
		// Most of the time we don't need consumerConcurrency because consuming message from NSQ is very fast,
		// the handler or the true consumer might need time to handle the message.
		backend.AddHandler(&dh)
		// Change the MaxInFlight to buffLength as the number of message won't exceed the buffLength.
		backend.ChangeMaxInFlight(dh.stats.BufferLength())

		if err := backend.ConnectToNSQLookupds(c.lookupdsAddr); err != nil {
			return err
		}
		// Invoke all handler to work,
		// depends on the number of concurrency.
		for i := 0; i < handler.stats.Concurrency(); i++ {
			handler.Work()
		}
	}
	c.started = true
	return nil
}

// Started return the status of the consumer, whether the consumer started or not.
func (c *Consumer) Started() bool {
	return c.started
}

// Stop for stopping all the nsq consumer.
func (c *Consumer) Stop() error {
	// Stopping all NSQ backends. This should make message consumption to nsqHandler stop.
	for _, channels := range c.backends {
		for _, backend := range channels {
			backend.Stop()
		}
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
	c.started = false
	return nil
}
