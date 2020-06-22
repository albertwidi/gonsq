package gonsq

import (
	"fmt"
	"sync"
)

// ProducerClient is the producer client of NSQ.
// This backend implements all communication protocol
// to nsqd servers.
type ProducerClient interface {
	Ping() error
	Publish(topic string, body []byte) error
	MultiPublish(topic string, body [][]byte) error
	Stop()
}

// ProducerManager manage the producer flow. If a
// given topic is not available in the manager,
// the producer will return a failure message.
type ProducerManager struct {
	producer ProducerClient
	topics   map[string]bool

	mu sync.RWMutex
}

// ManageProducers is a function to wrap the nsq producer.
// The function receive topics parameters because in NSQ, we can publish message
// without registering any new topics. This sometimes can be problematic as
// we don't have a list of topics that we will publish the message to.
func ManageProducers(backend ProducerClient, topics ...string) (*ProducerManager, error) {
	p := ProducerManager{
		producer: backend,
		topics:   make(map[string]bool),
	}
	for _, t := range topics {
		p.topics[t] = true
	}
	return &p, nil
}

func (p *ProducerManager) topicExist(topic string) error {
	p.mu.RLock()
	ok := p.topics[topic]
	p.mu.RUnlock()

	if !ok {
		return fmt.Errorf("nsq: topic %s is not eixst in this producer", topic)
	}
	return nil
}

// Publish message to nsqd, if a given topic does not exists, then return error.
func (p *ProducerManager) Publish(topic string, body []byte) error {
	if err := p.topicExist(topic); err != nil {
		return err
	}
	return p.producer.Publish(topic, body)
}

// MultiPublish message to nsqd, ifa given topic does not exists, then return error.
func (p *ProducerManager) MultiPublish(topic string, body [][]byte) error {
	if err := p.topicExist(topic); err != nil {
		return err
	}
	return p.producer.MultiPublish(topic, body)
}
