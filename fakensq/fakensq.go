package fakensq

import (
	"errors"
	"sync"
	"time"

	nsqio "github.com/nsqio/go-nsq"
)

// FakeNSQ mimic several NSQ features, mostly for publishing and consuming message.
// The purpose is to not rely on the real NSQ for testing.
type FakeNSQ struct {
	lookupd *LookUpD
}

// New instance of fakensq
func New() *FakeNSQ {
	fq := FakeNSQ{
		lookupd: &LookUpD{
			registrar: make(map[string]map[string]chan *nsqio.Message),
		},
	}
	return &fq
}

// NewProducer return a fakensq producer for publishing message to fakensq.
// The producer will automatically publish the message into the designated topic
// if topic/channel is available.
func (fnsq *FakeNSQ) NewProducer() *Producer {
	prod := &Producer{lookupd: fnsq.lookupd}
	return prod
}

// NewConsumer return a fakensq consumer for consuming message from fakensq.
// The consumer will register the topic and channel to fakensq lookupd registrar
// for topic and channel discovery.
func (fnsq *FakeNSQ) NewConsumer(config ConsumerConfig) *Consumer {
	messageChan := fnsq.lookupd.register(config.Topic, config.Channel)
	consumer := &Consumer{
		config:      config,
		messageChan: messageChan,
		stopChan:    make(chan struct{}),
	}
	return consumer
}

// LookUpD for fakensq. The lookUpD will store all informations of messages
// channeling for specific topic and channel.
type LookUpD struct {
	registrar map[string]map[string]chan *nsqio.Message
	mu        sync.RWMutex
}

func (lkd *LookUpD) register(topic, channel string) chan *nsqio.Message {
	lkd.mu.Lock()
	defer lkd.mu.Unlock()

	_, ok := lkd.registrar[topic]
	if !ok {
		lkd.registrar[topic] = make(map[string]chan *nsqio.Message)
	}

	ch := make(chan *nsqio.Message)
	lkd.registrar[topic][channel] = ch
	return ch
}

func (lkd *LookUpD) channels(topic string) map[string]chan *nsqio.Message {
	lkd.mu.RLock()
	defer lkd.mu.RUnlock()

	return lkd.registrar[topic]
}

// Producer for fakensq. The producer will publish message to all channels
// for specific topic, based on information from the lookUpD.
type Producer struct {
	lookupd *LookUpD
}

// Ping will always return nil
func (prod *Producer) Ping() error {
	return nil
}

// Publish a message
// this function might block if the channel is full
func (prod *Producer) Publish(topic string, message []byte) error {
	channels := prod.lookupd.channels(topic)
	if channels == nil {
		return errors.New("fakensq: topic not found")
	}

	for _, c := range channels {
		c <- &nsqio.Message{
			Body: message,
			Delegate: &MessageDelegator{
				messageChan: c,
			},
		}
	}
	return nil
}

// MultiPublish message
func (prod *Producer) MultiPublish(topic string, messages [][]byte) error {
	for _, message := range messages {
		if err := prod.Publish(topic, message); err != nil {
			return err
		}
	}
	return nil
}

// Stop fake producer
func (prod *Producer) Stop() {
	return
}

// ConsumerConfig of fake nsq
type ConsumerConfig struct {
	Topic            string
	Channel          string
	Concurrency      int
	BufferMultiplier int
	maxInFlight      int
}

// Validate consumer configuration
func (cc *ConsumerConfig) Validate() error {
	if cc.Topic == "" || cc.Channel == "" {
		return errors.New("topic or channel cannot be empty")
	}
	return nil
}

// Consumer is the consumer of NSQ, the consumer relay message to message handler.
type Consumer struct {
	config      ConsumerConfig
	handlers    []nsqHandler
	messageChan chan *nsqio.Message
	stopChan    chan struct{}

	mu      sync.Mutex
	started bool
}

// Topic return the consumer topic
func (cons *Consumer) Topic() string {
	return cons.config.Topic
}

// Channel return the consumer channel
func (cons *Consumer) Channel() string {
	return cons.config.Channel
}

// AddHandler for nsq
func (cons *Consumer) AddHandler(handler nsqio.Handler) {
	cons.addHandlers(handler, 1)
}

// AddConcurrentHandlers for nsq
func (cons *Consumer) AddConcurrentHandlers(handler nsqio.Handler, concurrency int) {
	cons.addHandlers(handler, concurrency)
}

func (cons *Consumer) addHandlers(handler nsqio.Handler, concurrency int) {
	h := nsqHandler{
		concurrency: concurrency,
		messageChan: cons.messageChan,
		stopChan:    cons.stopChan,
		handler:     handler,
	}
	cons.handlers = append(cons.handlers, h)
}

// ConnectToNSQLookupds for nsq
func (cons *Consumer) ConnectToNSQLookupds(addresses []string) error {
	cons.start()
	return nil
}

// MaxInFlight return the maximum in flight number for nsq.
func (cons *Consumer) MaxInFlight() int {
	return cons.config.maxInFlight
}

// ChangeMaxInFlight message in nsq consumer
func (cons *Consumer) ChangeMaxInFlight(n int) {
	cons.config.maxInFlight = n
}

// Concurrency return the number of conccurent worker
func (cons *Consumer) Concurrency() int {
	return cons.config.Concurrency
}

// BufferMultiplier return the number of buffer multiplier
func (cons *Consumer) BufferMultiplier() int {
	return cons.config.BufferMultiplier
}

// Start will start the message sending
// the message sending mechanism will be concurrent
// based on how fast the consumer consumed the message
// using a single unbuffered channel
func (cons *Consumer) start() {
	cons.mu.Lock()
	defer cons.mu.Unlock()

	if cons.started {
		return
	}
	cons.started = true

	for _, handler := range cons.handlers {
		for i := 0; i < handler.concurrency; i++ {
			go func(h nsqHandler) {
				for {
					select {
					case <-h.stopChan:
						return
					case msg := <-cons.messageChan:
						h.handler.HandleMessage(msg)
						continue
					}
				}
			}(handler)
		}
	}
}

// Stop consumer backend mock
func (cons *Consumer) Stop() {
	cons.mu.Lock()
	defer cons.mu.Unlock()

	if !cons.started {
		return
	}

	for _, handler := range cons.handlers {
		for i := 0; i < handler.concurrency; i++ {
			handler.Stop()
		}
	}

	cons.started = false
	return
}

type nsqHandler struct {
	concurrency int
	messageChan chan *nsqio.Message
	handler     nsqio.Handler
	stopChan    chan struct{}
}

func (h *nsqHandler) Stop() {
	h.stopChan <- struct{}{}
	return
}

// MessageDelegator implement Delegator of nsqio
type MessageDelegator struct {
	messageChan chan *nsqio.Message
}

func (mdm *MessageDelegator) OnFinish(message *nsqio.Message) {
	return
}

func (mdm *MessageDelegator) OnRequeue(m *nsqio.Message, t time.Duration, backoff bool) {
	mdm.messageChan <- m
}

func (mdm *MessageDelegator) OnTouch(m *nsqio.Message) {
	return
}
