# Throttle Middleware

Throttle middleware for `gons`. This is only a usable example for throttle middleware.

This middleware is using `go.uber.org/ratelimit` to ratelimit the message consumption.

The middleawre expect two parameters:

1. Rate limit when throttle is on.
2. Rate limit when throttle is loosen.

For example:

```go
import (
    "github.com/albertwidi/gonsq"
    "github.com/albertwidi/gonsq/middleware/throttle"
)

// ...

cm := gonsq.ManageConsumers(...)

// This means 100 messages per second when throttled,
// and 200 messages per second when throttle is loosen.
tmw := throttle.New(100,200)

cm.Use(tmw)
```

You can always create your own middleware and use `message.Stats` as the trigger to throttle the message consumption:

```go
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
```
