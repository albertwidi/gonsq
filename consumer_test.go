package gonsq

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/albertwidi/gonsq/fakensq"
)

// helper function to start the consumer manager
func startConsumer(t *testing.T, cm *ConsumerManager) error {
	t.Helper()

	var err error
	errC := make(chan error)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*200)
	defer cancel()

	go func() {
		if err := cm.Start([]string{"test"}); err != nil {
			errC <- err
		}
	}()

	select {
	case err = <-errC:
		break
	case <-ctx.Done():
		break
	}

	return err
}

func TestStartStop(t *testing.T) {
	var (
		topic   = "test_topic"
		channel = "test_channel"
	)

	fake := fakensq.New()
	consumer := fake.NewConsumer(fakensq.ConsumerConfig{Topic: topic, Channel: channel, Concurrency: 1, MaxInFlight: 10})

	wc, err := ManageConsumers(consumer)
	if err != nil {
		t.Error(err)
		return
	}

	if err := startConsumer(t, wc); err != nil {
		t.Error(err)
		return
	}

	// Give time for consumer to start the work.
	time.Sleep(time.Millisecond * 200)

	for _, h := range wc.handlers {
		if h.stats.Worker() == 0 {
			t.Error("worker number should not be 0 because consumer is started")
			return
		}
	}

	if err := wc.Stop(context.Background()); err != nil {
		t.Error(err)
		return
	}

	// Give time for consumer to stop the work.
	time.Sleep(time.Millisecond * 200)

	for _, h := range wc.handlers {
		if h.stats.Worker() != 0 {
			t.Error("worker number should be 0 because consumer is stopped")
			return
		}
	}
}

func TestMiddlewareChaining(t *testing.T) {
	var (
		topic             = "test_topic"
		channel           = "test_channel"
		middlewareTestVal = "middleware_test"
		errChan           = make(chan error)
		// To make sure that error is being sent back
		errNil = errors.New("error should be nil")

		// Test expect, to match the result of the test
		messageExpect = "testing middleware chaining"
		expectResult  = "test1:test2:test3"
	)

	mw1 := func(handler HandlerFunc) HandlerFunc {
		return func(ctx context.Context, message *Message) error {
			ctx = context.WithValue(ctx, &middlewareTestVal, "test1")
			return handler(ctx, message)
		}
	}

	mw2 := func(handler HandlerFunc) HandlerFunc {
		return func(ctx context.Context, message *Message) error {
			val := ctx.Value(&middlewareTestVal).(string)
			val += ":test2"
			ctx = context.WithValue(ctx, &middlewareTestVal, val)
			return handler(ctx, message)
		}
	}

	mw3 := func(handler HandlerFunc) HandlerFunc {
		return func(ctx context.Context, message *Message) error {
			val := ctx.Value(&middlewareTestVal).(string)
			val += ":test3"
			ctx = context.WithValue(ctx, &middlewareTestVal, val)
			return handler(ctx, message)
		}
	}

	fake := fakensq.New()
	consumer := fake.NewConsumer(fakensq.ConsumerConfig{Topic: topic, Channel: channel, Concurrency: 1, MaxInFlight: 10})
	publisher := fake.NewProducer()

	wc, err := ManageConsumers(consumer)
	if err != nil {
		t.Error(err)
		return
	}

	// Chain from left to right or top to bottom.
	wc.Use(
		mw1,
		mw2,
		mw3,
	)

	// Handle message and check whether the middleware chaining is correct.
	wc.Handle(topic, channel, func(ctx context.Context, message *Message) error {
		if string(message.Message.Body) != messageExpect {
			err := fmt.Errorf("epecting message %s but got %s", messageExpect, string(message.Message.Body))
			errChan <- err
			return err
		}
		val := ctx.Value(&middlewareTestVal).(string)
		if val != expectResult {
			err := fmt.Errorf("middleware chaining result is not as expected, expect %s but got %s", expectResult, val)
			errChan <- err
			return err
		}

		errChan <- errNil
		return err
	})

	if err := startConsumer(t, wc); err != nil {
		t.Error(err)
		return
	}

	if err := publisher.Publish(topic, []byte(messageExpect)); err != nil {
		t.Error(err)
		return
	}

	err = <-errChan
	if err != errNil {
		t.Error(err)
		return
	}

	if err := wc.Stop(context.Background()); err != nil {
		t.Error(err)
		return
	}
}

func TestGracefulStop(t *testing.T) {
	fake := fakensq.New()
	publisher := fake.NewProducer()
	wc, err := ManageConsumers()
	if err != nil {
		t.Fatal(err)
	}

	topics := []struct {
		topic    string
		channels []string
	}{
		{
			"topic_1",
			[]string{"channel_1", "channel_2", "channel_3"},
		},
		{
			"topic_2",
			[]string{"channel_1", "channel_2"},
		},
	}

	for _, topic := range topics {
		for _, channel := range topic.channels {
			// Use buffer length 20 because 10 message is sent to all topics and channels.
			config := fakensq.ConsumerConfig{Topic: topic.topic, Channel: channel, Concurrency: 1, MaxInFlight: 20}
			consumer := fake.NewConsumer(config)

			wc.AddConsumers(consumer)
			handler := HandlerFunc(func(ctx context.Context, message *Message) error {
				time.Sleep(time.Millisecond)
				return nil
			})
			wc.Handle(topic.topic, channel, handler)
		}
	}

	if err := startConsumer(t, wc); err != nil {
		t.Fatal(err)
	}

	errChan := make(chan error)
	for _, topic := range topics {
		go func(topic string) {
			for i := 0; i < 10; i++ {
				if err := publisher.Publish(topic, []byte("")); err != nil {
					errChan <- err
				}
			}
			errChan <- nil
		}(topic.topic)
	}

	for i := 0; i < len(topics); i++ {
		err := <-errChan
		if err != nil {
			t.Fatal(err)
		}
	}

	if err := wc.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}

	for _, handler := range wc.handlers {
		if handler.stats.MessageInBuffer() != 0 {
			t.Fatalf("message in buffer should be 0, graceful stop failed, message in buffer %d for topic %s and channel %s", handler.stats.MessageInBuffer(), handler.client.Topic(), handler.client.Channel())
		}
	}
}
