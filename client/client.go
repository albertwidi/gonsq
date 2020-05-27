package client

import (
	"context"
	"errors"
	"fmt"

	"github.com/albertwidi/gonsq"
)

// ConsumerGroup is not like consumer group in Kafka. The consumer group is only a group of
// consumer that have same topic.
type ConsumerGroup struct {
	Topic       string
	Channels    []Channel
	Lookupd     gonsq.LookupdConfig
	Timeout     gonsq.TimeoutConfig
	Queue       gonsq.QueueConfig
	Compression gonsq.CompressionConfig
	Concurrency gonsq.ConcurrencyConfig
}

// Channel configuration for consumer group
type Channel struct {
	Name string
	// Concurrency configuration for channel. If this configuration exist,
	// then this configuration will be used instead of consumer group
	// configuration for concurrency.
	Concurrency gonsq.ConcurrencyConfig
}

func (c *Channel) validate() error {
	if c.Name == "" {
		return errors.New("channel name cannot be empty")
	}
	return nil
}

// NewConsumerGroup to create a new consumer group using gonsq.WrapConsumers. This consumer group is not like Kafka consumer group,
// this consumer group is only grouping consumers with same topic. A consumer with different topic will grouped into another
// wrapper in gonsq.Consumer.
func NewConsumerGroup(ctx context.Context, lookupdAddresses []string, groups []ConsumerGroup) (*gonsq.Consumer, error) {
	wc, err := gonsq.WrapConsumers(lookupdAddresses)
	if err != nil {
		return nil, err
	}

	for _, group := range groups {
		for _, channel := range group.Channels {
			if err := channel.validate(); err != nil {
				return nil, fmt.Errorf("consumer group with topic %s error: %w", group.Topic, err)
			}

			ccConfig := group.Concurrency
			if !channel.Concurrency.IsEmpty() {
				ccConfig = channel.Concurrency
			}

			consumerConfig := gonsq.NSQConsumerConfig{
				Topic:       group.Topic,
				Channel:     channel.Name,
				Lookupd:     group.Lookupd,
				Timeout:     group.Timeout,
				Queue:       group.Queue,
				Compression: group.Compression,
				Concurrency: ccConfig,
			}
			consumer, err := gonsq.NewConsumer(ctx, consumerConfig)
			if err != nil {
				return nil, err
			}

			if err := wc.AddConsumers(consumer); err != nil {
				return nil, err
			}
		}
	}
	return nil, err
}
