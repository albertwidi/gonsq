package throttle

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/albertwidi/gonsq"
	fakensq "github.com/albertwidi/gonsq/fakensq"
)

// helper function to start the consumer manager
func startConsumer(t *testing.T, cm *gonsq.ConsumerManager) error {
	t.Helper()

	var err error
	errC := make(chan error)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	go func() {
		if err := cm.Start(); err != nil {
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

func TestThrottleMiddleware(t *testing.T) {
	var (
		topic                 = "test_topic"
		channel               = "test_channel"
		publishedMessageCount int32
		workerMessageCount    int32
		messageThrottled      int32
		// Use a buffered channel as error channel to make sure the worker is not blocked.
		// Because we will always block the 1st message with time.Sleep, the channel will
		// always full at message number 2. Make it so it won't block message number 2 and
		// so on.
		errChan = make(chan error, 2)
		// To make sure that error is being sent back.
		errNil = errors.New("error should be nil")

		messageExpect = "test middleware throttling"
	)

	// We are using fake consumer, this means the concurrency is always 1
	// and the number of message buffer is 1 * maxInFlight
	maxInFlight := 10
	concurrency := 1

	fake := fakensq.New()
	consumer := fake.NewConsumer(fakensq.ConsumerConfig{Topic: topic, Channel: channel, Concurrency: concurrency, MaxInFlight: maxInFlight})
	publisher := fake.NewProducer()

	wc, err := gonsq.ManageConsumers([]string{"testing"}, consumer)
	if err != nil {
		t.Error(err)
		return
	}

	tm := Throttle{TimeDelay: time.Millisecond * 10}
	// Use throttle middleware.
	wc.Use(
		tm.Throttle,
	)

	wc.Handle(topic, channel, func(ctx context.Context, message *gonsq.Message) error {
		testpublishedMessageCount := atomic.AddInt32(&workerMessageCount, 1)

		if string(message.Message.Body) != messageExpect {
			err := fmt.Errorf("epecting message %s but got %s", messageExpect, string(message.Message.Body))
			errChan <- err
			return err
		}

		// This means this is the first message, sleep to make other message to wait
		// because if the handler is not finished, the worker is not back to consume state
		// to make sure the buffer is filled first before consuming more message.
		if testpublishedMessageCount == 1 {
			time.Sleep(time.Millisecond * 10)
		}

		// Check whether a throttled message is exists
		// this message should exists because throttle middleware is used.
		if message.Stats.Throttle() {
			atomic.AddInt32(&messageThrottled, 1)
			// Check if message throttled before it should be.
			if publishedMessageCount <= 2 {
				err := fmt.Errorf("message throttled before it should be, message number: %d", testpublishedMessageCount)
				errChan <- err
				return err
			}
			errChan <- errNil
			return nil
		}

		// This means the test have reach the end of message.
		if workerMessageCount == publishedMessageCount {
			if messageThrottled < 1 {
				errChan <- errors.New("message is never throttled")
				return err
			}

			// Check if the message is still throttled, because the throttle status should be gone.
			if message.Stats.Throttle() {
				errChan <- errors.New("message is still throttled at the end of the test")
			}
			errChan <- errNil
			return nil
		}

		errChan <- errNil
		return err
	})

	if err := startConsumer(t, wc); err != nil {
		t.Error(err)
		return
	}

	// Wait until the consumer is started.
	for !wc.Started() {
		time.Sleep(time.Millisecond * 100)
	}

	// Note that in this test, we set the maxInFlight to 10.
	// Send messages as much as (maxInFlight/2) + 3 to tirgger the throttle mechanism.
	//
	// c = consumed
	// d = done
	// m = message in buffer
	// <nil> = no message, buffer is empty
	//
	// _buffMultiplier/2 + 3 = 8 messages
	//
	// | m | m | m | m | m | m | m | m | <nil> | <nil> |
	//   1   2   3   4   5   6   7   8     9      10
	//
	// message_length: 8
	//
	//
	// When the program start, the message will be consumed into the worker right away
	// then the worker will pause themself for a while, so message is not consumed.
	// At this point, this is the look in the buffer:
	//
	// | m | m | m | m | m | m | m | m | <nil> | <nil> | <nil > |
	//   c   1   2   3   4   5   6   7     8       9      10
	//
	// Message_length: 7
	//
	// At this point when consuming more message which is message number 1, the buffer will become:
	//
	// | m | m | m | m | m | m | m | m | <nil> | <nil> | <nil > | <nil> |
	//   d   c   1   2   3   4   5   6     7       8       9       10
	//
	// message_length: 6
	//
	// When consuming the message, evaluation of the buffer length will kick in,
	// this is where the evaluator for thorttle knows that the number of messages
	// is more than half of the buffer size. Then throttle mechanism will be invoked
	// this is why, with lower number of messages the test won't pass,
	// because it depends on messages number in the buffer.
	//
	for i := 1; i <= (maxInFlight/2)+3; i++ {
		if err := publisher.Publish(topic, []byte(messageExpect)); err != nil {
			t.Error(err)
			return
		}
		// Put sleep here, because we don't want to publish very fast.
		// Wait until the worker catched-up a bit.
		time.Sleep(time.Millisecond * 2)
		publishedMessageCount++
	}

	for i := 1; i <= int(publishedMessageCount); i++ {
		err = <-errChan
		if err != errNil {
			t.Error(err)
			return
		}
	}
	close(errChan)

	if err := wc.Stop(); err != nil {
		t.Error(err)
		return
	}
}
