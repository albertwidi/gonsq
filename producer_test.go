package gonsq

import "testing"

func TestTopicExist(t *testing.T) {
	topics := []string{"foo", "bar", "baz"}

	p, err := ManageProducers(nil, topics...)
	if err != nil {
		t.Fatal(err)
	}

	for _, topic := range topics {
		if err := p.topicExist(topic); err != nil {
			t.Fatal(err)
		}
	}

	if err := p.topicExist("invalid"); err == nil {
		t.Fatalf("topic %s should not exist", "invalid")
	}
}
