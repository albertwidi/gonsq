package prometheus

import (
	"context"
	"errors"
	"sync"
	"time"

	nsq "github.com/albertwidi/gonsq"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	_nsqMessageRetrievedCount = iota
	_nsqHandleCount
	_nsqHandleDurationHist
	_nsqWorkerCurrentGauge
	_nsqThrottleGauge
	_nsqMessageInBuffGauge
)

var metrics = []prometheus.Collector{
	_nsqMessageRetrievedCount: prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nsq_message_retrieved_total",
			Help: "total message being retrieved from nsq for certain topic and channel, retrieved doesn't mean it is been processed",
		},
		[]string{"topic", "channel"},
	),
	_nsqHandleCount: prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nsq_handle_error_total",
			Help: "total of message being handled",
		}, []string{"topic", "channel", "error"},
	),
	_nsqHandleDurationHist: prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "nsq_message_handle_duration",
		}, []string{"topic", "channel"},
	),
	_nsqWorkerCurrentGauge: prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nsq_worker_count_current",
		}, []string{"topic", "channel"},
	),
	_nsqThrottleGauge: prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nsq_throttle_status",
		}, []string{"topic", "channel"},
	),
	_nsqMessageInBuffGauge: prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nsq_message_in_buffer",
		}, []string{"topic", "channel"}),
}

var (
	regmMu        sync.Mutex
	metricsInited bool
)

func registerMetrics() error {
	regmMu.Lock()
	if metricsInited {
		return nil
	}
	defer regmMu.Unlock()

	for _, metric := range metrics {
		if err := prometheus.Register(metric); err != nil {
			if !errors.As(err, &prometheus.AlreadyRegisteredError{}) {
				return err
			}
		}
	}
	metricsInited = true
	return nil
}

func init() {
	registerMetrics()
}

// Metrics middleware for nsq
func Metrics(handler nsq.HandlerFunc) nsq.HandlerFunc {
	return func(ctx context.Context, message *nsq.Message) error {
		t := time.Now()
		e := "0"
		err := handler(ctx, message)
		if err != nil {
			e = "1"
		}

		metrics[_nsqHandleDurationHist].(*prometheus.HistogramVec).
			WithLabelValues(message.Topic, message.Channel).Observe(float64(time.Now().Sub(t).Milliseconds()))
		metrics[_nsqHandleCount].(*prometheus.CounterVec).
			WithLabelValues(message.Topic, message.Channel, e).Add(1)
		metrics[_nsqWorkerCurrentGauge].(*prometheus.GaugeVec).
			WithLabelValues(message.Topic, message.Channel).Set(float64(message.Stats.Worker()))
		metrics[_nsqThrottleGauge].(*prometheus.GaugeVec).
			WithLabelValues(message.Topic, message.Channel).Set(float64(message.Stats.Throttle().Int()))
		metrics[_nsqMessageInBuffGauge].(*prometheus.GaugeVec).
			WithLabelValues(message.Topic, message.Channel).Set(float64(message.Stats.MessageInBuffer()))

		return err
	}
}
