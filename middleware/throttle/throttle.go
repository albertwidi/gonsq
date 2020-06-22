// Throttle middleware package use uber-go/ratelimit.

package throttle

import (
	"context"

	"go.uber.org/ratelimit"

	"github.com/albertwidi/gonsq"
)

// Throttle implements gonsq.MiddlewareFunc.
type Throttle struct {
	firstLimit  ratelimit.Limiter
	secondLimit ratelimit.Limiter
}

// New throttle middleware.
// Create a new object and initialize the rate-limiter
// based on the throttleLimit and loosenLimit. This
// because the gonsq have two level of throttling:
// throttle and loosenThrottle.
func New(throttleLimit, loosenLimit int) *Throttle {
	firstLimit := ratelimit.New(throttleLimit)
	secondLimit := ratelimit.New(loosenLimit)

	t := &Throttle{
		firstLimit:  firstLimit,
		secondLimit: secondLimit,
	}
	return t
}

// Throttle middleware for nsq.
// This middleware check whether there is some information about throttling in the message.
func (tm *Throttle) Throttle(handler gonsq.HandlerFunc) gonsq.HandlerFunc {
	return func(ctx context.Context, message *gonsq.Message) error {
		if message.Stats.Throttle().IsThrottleLoosen() {
			tm.secondLimit.Take()
		} else if message.Stats.Throttle().IsThrottled() {
			tm.firstLimit.Take()
		}
		return handler(ctx, message)
	}
}
