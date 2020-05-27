package fakensq

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	nsqio "github.com/nsqio/go-nsq"
)

func TestPublishConsume(t *testing.T) {
	nsq := New()
	publisher := nsq.NewProducer()

	type Message struct {
		Topic   string
		Message string
	}

	tests := []struct {
		topic    string
		channels []string
		message  string
		consumer *Consumer
	}{
		{
			"topic_1",
			[]string{"channel_1", "channel_2"},
			"message_topic_1",
			nil,
		},
		{
			"topic_2",
			[]string{"channel_1"},
			"message_topic_2",
			nil,
		},
	}

	// Register all consumers and handlers.
	for idx := range tests {
		for chanidx := range tests[idx].channels {
			expect := Message{
				Topic:   tests[idx].topic,
				Message: tests[idx].message,
			}

			consumer := nsq.NewConsumer(ConsumerConfig{Topic: tests[idx].topic, Channel: tests[idx].channels[chanidx]})
			handler := nsqio.HandlerFunc(func(message *nsqio.Message) error {
				expectMsg := expect

				msg := Message{}
				if err := json.Unmarshal(message.Body, &msg); err != nil {
					t.Fatal(err)
				}

				if !cmp.Equal(msg, expectMsg) {
					t.Fatalf("message is different:\n%s", cmp.Diff(msg, expectMsg))
				} else {
				}
				return nil
			})
			consumer.AddHandler(handler)
			consumer.start()

			tests[idx].consumer = consumer
		}
	}

	// Wait all consumer worker to up.
	time.Sleep(time.Millisecond * 100)

	for _, test := range tests {
		msg := Message{
			Topic:   test.topic,
			Message: test.message,
		}
		out, err := json.Marshal(msg)
		if err != nil {
			t.Fatal(err)
		}

		if err := publisher.Publish(test.topic, out); err != nil {
			t.Fatal(err)
		}
	}

	// Wait until all goroutines finished.
	time.Sleep(time.Millisecond * 200)

	// Stop all consumers.
	for _, test := range tests {
		test.consumer.Stop()
	}
}
