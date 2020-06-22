package gonsq

import (
	"errors"
)

var (
	// ErrInvalidConcurrencyConfiguration happens when concurrency configuration number is not
	// as expected. The configuration is checked when adding new consumer.
	ErrInvalidConcurrencyConfiguration = errors.New("gonsq: invalid concurrency configuration")
	// ErrLookupdsAddrEmpty happens when NSQ lookupd address is empty when wrapping consumers.
	ErrLookupdsAddrEmpty = errors.New("gonsq: lookupds addresses is empty")
	// ErrTopicWithChannelNotFound for error when channel and topic is not found.
	ErrTopicWithChannelNotFound = errors.New("gonsq: topic and channel not found")
	// ErrStopDeadlineExceeded heppens when stop time exceeds context deadline time.
	ErrStopDeadlineExceeded = errors.New("gonsq: stop deadline exceeded")
)
