# Prometheus Middleware For Gonsq

This is a prometheus middleware for NSQ to expose metrics data for NSQ

## Usage

To use this library, simply append the `Metrics` function into the `nsq` library. For example:

```go
import (
    promemw "github.com/albertwidi/gonsq/middleware/prometheus"
    "github.com/albertwidi/gonsq/"
)

consumer, err := nsq.WrapConsumers(...)
if err != nil {
    // Handle the error.
}
conusmer.Use(promemw.Metrics)
```