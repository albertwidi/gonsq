package gonsq

import (
	"context"
	"time"
)

// ThrottleMiddleware implement MiddlewareFunc
type ThrottleMiddleware struct {
	// TimeDelay means the duration of time to pause message consumption
	TimeDelay time.Duration
}

// Throttle middleware for nsq.
// This middleware check whether there is some information about throttling in the message.
func (tm *ThrottleMiddleware) Throttle(handler HandlerFunc) HandlerFunc {
	return func(ctx context.Context, message *Message) error {
		// This means the worker is being throttled
		if message.Stats.Throttle() {
			time.Sleep(tm.TimeDelay)
		}
		return handler(ctx, message)
	}
}
