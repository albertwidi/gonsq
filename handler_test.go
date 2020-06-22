package gonsq

import (
	"context"
	"errors"
	"testing"
	"time"

	fakensq "github.com/albertwidi/gonsq/fakensq"
	gonsq "github.com/nsqio/go-nsq"
)

func TestNSQHandlerSetConcurrency(t *testing.T) {
	cases := []struct {
		concurrency       int
		buffMultiplier    int
		expectConcurreny  int
		expectMaxInFlight int
		expectError       error
	}{
		{
			concurrency:       1,
			buffMultiplier:    10,
			expectConcurreny:  1,
			expectMaxInFlight: 10,
			expectError:       nil,
		},
		{
			concurrency:       -1,
			buffMultiplier:    -1,
			expectConcurreny:  defaultConcurrency,
			expectMaxInFlight: defaultMaxInFlight,
			expectError:       ErrInvalidConcurrencyConfiguration,
		},
		{
			concurrency:       1,
			buffMultiplier:    1,
			expectConcurreny:  1,
			expectMaxInFlight: 1,
			expectError:       nil,
		},
		{
			concurrency:       1,
			buffMultiplier:    -1,
			expectConcurreny:  1,
			expectMaxInFlight: defaultMaxInFlight,
			expectError:       ErrInvalidConcurrencyConfiguration,
		},
	}

	var (
		topic   = "test_concurrency"
		channel = "test_concurrency"
	)

	for _, c := range cases {
		t.Logf("concurrency: %d", c.concurrency)
		t.Logf("buff_multiplier: %d", c.buffMultiplier)

		fake := fakensq.New()
		consumer := fake.NewConsumer(fakensq.ConsumerConfig{Topic: topic, Channel: channel, Concurrency: c.concurrency, MaxInFlight: c.buffMultiplier})

		wc, err := ManageConsumers(consumer)
		if !errors.Is(err, c.expectError) {
			t.Errorf("expecting error %v but got %v", c.expectError, err)
			return
		}

		if c.expectError != nil {
			return
		}

		// Trigger the creation of handler.
		wc.Handle(topic, channel, nil)

		handler := wc.handlers[mergeTopicAndChannel(topic, channel)]
		if handler == nil {
			t.Fatalf("handler should not be nil, as handle is triggered")
		}

		if handler.stats.Concurrency() != c.expectConcurreny {
			t.Fatalf("expecting concurrency %d but got %d", c.expectConcurreny, handler.stats.Concurrency())
		}
		if handler.stats.MaxInFlight() != c.expectMaxInFlight {
			t.Errorf("expecting max in flight of %d but got %d", c.expectMaxInFlight, handler.stats.MaxInFlight())
			return
		}
		if handler.stats.BufferLength() != c.expectConcurreny*c.expectMaxInFlight {
			t.Errorf("expecting buffer length of %d but got %d", c.expectConcurreny*c.expectMaxInFlight, handler.stats.BufferLength())
			return
		}
	}
}

func TestNSQHandlerConcurrencyControl(t *testing.T) {
	var (
		topic       = "test_concurrency"
		channel     = "test_concurrency"
		concurrency = 5
		maxInFlight = 500
	)

	fake := fakensq.New()
	consumer := fake.NewConsumer(fakensq.ConsumerConfig{Topic: topic, Channel: channel, Concurrency: concurrency, MaxInFlight: maxInFlight})

	wc, err := ManageConsumers(consumer)
	if err != nil {
		t.Error(err)
		return
	}
	// Trigger the creation of handler.
	wc.Handle(topic, channel, nil)

	handler := wc.handlers[mergeTopicAndChannel(topic, channel)]
	if handler == nil {
		t.Error("handler should not be nil, as handle is triggered")
	}

	for i := 1; i <= 5; i++ {
		go handler.Start()
		// Wait until the goroutines scheduled
		// this might be too long, but its ok.
		time.Sleep(time.Millisecond * 10)
		if int(handler.stats.Worker()) != i {
			t.Errorf("start: expecting number worker number of %d but got %d", i, handler.stats.Worker())
			return
		}
	}

	for i := handler.stats.Worker(); i < 0; i-- {
		handler.Stop()
		if handler.stats.Worker() != i-1 {
			t.Errorf("stop: expecting worker number of %d but got %d", i-1, handler.stats.Worker())
			return
		}
	}
}

func TestNSQHandlerHandleMessage(t *testing.T) {
	var (
		topic       = "test_topic"
		channel     = "test_channel"
		messageBody = []byte("test message")
	)

	fake := fakensq.New()
	backend := fake.NewConsumer(fakensq.ConsumerConfig{Topic: topic, Channel: channel})

	stats := &Stats{}
	df := &gonsqHandler{
		messageBuff: make(chan *Message, 2),
		stats:       stats,
		// Using buffered channel with length 1,
		// because in this test we don't listen the message using a worker,
		// and sending the message to this channel will block.
		client:             backend,
		openThrottleFunc:   defaultOpenThrottleFunc,
		loosenThrottleFunc: defaultLoosenThrottleFunc,
		breakThrottleFunc:  defaultBreakThrottleFunc,
	}
	stats.setBufferLength(cap(df.messageBuff))

	nsqMessage := &gonsq.Message{
		Body:     messageBody,
		Delegate: &fakensq.MessageDelegator{},
	}
	if err := df.HandleMessage(nsqMessage); err != nil {
		t.Error(err)
		return
	}

	message := <-df.messageBuff
	if message.Topic != topic {
		t.Errorf("expecting topic %s but got %s", topic, message.Topic)
		return
	}
	if message.Channel != channel {
		t.Errorf("expecting channel %s but got %s", channel, message.Channel)
		return
	}
	if string(message.Message.Body) != string(messageBody) {
		t.Errorf("expecting message body %s but got %s", string(message.Message.Body), string(messageBody))
		return
	}
}

func TestNSQHandlerThrottle(t *testing.T) {
	var (
		topic   = "random"
		channel = "random"
	)

	fake := fakensq.New()
	backend := fake.NewConsumer(fakensq.ConsumerConfig{Topic: topic, Channel: channel})

	stats := &Stats{}
	df := &gonsqHandler{
		messageBuff:        make(chan *Message, 3),
		stats:              stats,
		client:             backend,
		openThrottleFunc:   defaultOpenThrottleFunc,
		loosenThrottleFunc: defaultLoosenThrottleFunc,
		breakThrottleFunc:  defaultBreakThrottleFunc,
	}
	stats.setBufferLength(cap(df.messageBuff))
	doneChan := make(chan struct{})

	// TODO(albert) change the static number for limiter to dynamic
	for i := 0; i < 3; i++ {
		m := &gonsq.Message{}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		go func() {
			if err := df.HandleMessage(m); err != nil {
				t.Error(err)
				return
			}
			doneChan <- struct{}{}
		}()

		select {
		case <-doneChan:
			continue
		case <-ctx.Done():
			if i != 1 {
				t.Error("error while current buffer is less than half")
				return
			}

			if backend.MaxInFlight() != 0 {
				t.Error("error: max in flight is not being set to 0")
				return
			}
			return
		}
	}
}
