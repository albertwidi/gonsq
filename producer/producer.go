package producer

import (
	"context"

	"github.com/albertwidi/gonsq"
)

type Producer struct {
	Hostname    string
	NSQAddress  string
	Timeout     gonsq.TimeoutConfig
	Compression gonsq.CompressionConfig
}

func New(ctx context.Context, producer Producer, topic ...string) (*gonsq.ProducerManager, error) {
	p, err := gonsq.NewProducer(context.Background(), gonsq.ProducerConfig{
		Hostname:    producer.Hostname,
		Address:     producer.NSQAddress,
		Timeout:     producer.Timeout,
		Compression: producer.Compression,
	})
	if err != nil {
		return nil, err
	}

	return gonsq.ManageProducers(p, topic...)
}
