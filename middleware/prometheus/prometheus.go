package prometheus

import (
	"context"
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

var (
	countMetrics = []*prometheus.CounterVec{
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
	}

	histMetrics = []*prometheus.HistogramVec{
		_nsqHandleDurationHist: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "nsq_message_handle_duration",
				Help: "Handle duration of nsq message per topic and channel",
			}, []string{"topic", "channel"},
		),
	}

	gaugeMetrics = []*prometheus.GaugeVec{
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
)

var once sync.Once
var must = func() {
	for idx := range countMetrics {
		if countMetrics[idx] == nil {
			continue
		}
		prometheus.MustRegister(countMetrics[idx])
	}

	for idx := range histMetrics {
		if histMetrics[idx] == nil {
			continue
		}
		prometheus.MustRegister(histMetrics[idx])
	}

	for idx := range gaugeMetrics {
		if gaugeMetrics[idx] == nil {
			continue
		}
		prometheus.MustRegister(gaugeMetrics[idx])
	}
}

func init() {
	once.Do(must)
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

		histMetrics[_nsqHandleDurationHist].
			WithLabelValues(message.Topic, message.Channel).Observe(float64(time.Now().Sub(t).Milliseconds()))
		countMetrics[_nsqHandleCount].
			WithLabelValues(message.Topic, message.Channel, e).Add(1)
		gaugeMetrics[_nsqWorkerCurrentGauge].
			WithLabelValues(message.Topic, message.Channel).Set(float64(message.Stats.Worker()))
		gaugeMetrics[_nsqThrottleGauge].
			WithLabelValues(message.Topic, message.Channel).Set(float64(message.Stats.Throttle()))
		gaugeMetrics[_nsqMessageInBuffGauge].
			WithLabelValues(message.Topic, message.Channel).Set(float64(message.Stats.MessageInBuffer()))

		return err
	}
}
