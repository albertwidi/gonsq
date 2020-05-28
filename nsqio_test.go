package gonsq

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestConcurrencyConfig(t *testing.T) {
	tests := []struct {
		name   string
		config ConcurrencyConfig
		expect ConcurrencyConfig
		err    error
	}{
		{
			"positive concurrency and buffer multiplier",
			ConcurrencyConfig{10, 10},
			ConcurrencyConfig{10, 10},
			nil,
		},
		{
			"zero concurrency and buffer multiplier",
			ConcurrencyConfig{0, 0},
			ConcurrencyConfig{defaultConcurrency, defaultMaxInFlight},
			nil,
		},
		{
			"negative concurrency and buffer multiplier",
			ConcurrencyConfig{-1, -1},
			ConcurrencyConfig{defaultConcurrency, defaultMaxInFlight},
			nil,
		},
		{
			"negative buffer multiplier",
			ConcurrencyConfig{10, -1},
			ConcurrencyConfig{10, defaultMaxInFlight},
			nil,
		},
		{
			"negative concurrency",
			ConcurrencyConfig{-1, 10},
			ConcurrencyConfig{defaultConcurrency, 10},
			nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cc := test.config
			if err := cc.Validate(); err != test.err {
				t.Fatalf("expecting error %v but got %v", test.err, err)
			}

			if test.err != nil {
				return
			}

			if !cmp.Equal(cc, test.expect) {
				t.Fatalf("configuration is differen, diff:\n%s", cmp.Diff(cc, test.expect))
			}
		})
	}
}

func TestNewNSQConsumer(t *testing.T) {
	tests := []struct {
		name   string
		config NSQConsumerConfig
		err    error
	}{
		{
			"config with concurrency",
			NSQConsumerConfig{
				Topic:   "test1",
				Channel: "test1",
				Concurrency: ConcurrencyConfig{
					Concurrency: 1,
					MaxInFlight: 50,
				},
			},
			nil,
		},
		{
			"config topic empty",
			NSQConsumerConfig{
				Channel: "test1",
				Concurrency: ConcurrencyConfig{
					Concurrency: 1,
					MaxInFlight: 50,
				},
			},
			errTopicEmpty,
		},
		{
			"config channel empty",
			NSQConsumerConfig{
				Topic: "test1",
				Concurrency: ConcurrencyConfig{
					Concurrency: 1,
					MaxInFlight: 50,
				},
			},
			errChannelEmpty,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c, err := NewConsumer(context.Background(), test.config)
			if err != test.err {
				t.Fatalf("expecting error %v but got %v", test.err, err)
			}

			if test.err != nil {
				return
			}

			if c.Topic() != test.config.Topic {
				t.Fatalf("expecting topic %s but got %s", test.config.Topic, c.Topic())
			}

			if c.Channel() != test.config.Channel {
				t.Fatalf("expecting topic %s but got %s", test.config.Channel, c.Channel())
			}
		})
	}
}
