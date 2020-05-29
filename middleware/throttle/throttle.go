package throttle

import (
	"context"
	"time"

	"github.com/albertwidi/gonsq"
)

// Throttle implements gonsq.MiddlewareFunc.
type Throttle struct {
	// TimeDelay means the duration of time to pause message consumption.
	TimeDelay time.Duration
}

// Throttle middleware for nsq.
// This middleware check whether there is some information about throttling in the message.
func (tm *Throttle) Throttle(handler gonsq.HandlerFunc) gonsq.HandlerFunc {
	return func(ctx context.Context, message *gonsq.Message) error {
		// This means the worker is being throttled
		if message.Stats.Throttle() {
			time.Sleep(tm.TimeDelay)
		}
		return handler(ctx, message)
	}
}
