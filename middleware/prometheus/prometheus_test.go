package prometheus

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// Every new metrics need to be tested for type casting.
// Because we use prometheus.Collector as the map value type.
func TestMetricsTypeCasting(t *testing.T) {
	err := errors.New("metrics: wrong type")

	if _, ok := metrics[_nsqMessageRetrievedCount].(*prometheus.CounterVec); !ok {
		t.Fatal(err)
	}
	if _, ok := metrics[_nsqHandleCount].(*prometheus.CounterVec); !ok {
		t.Fatal(err)
	}
	if _, ok := metrics[_nsqHandleDurationHist].(*prometheus.HistogramVec); !ok {
		t.Fatal(err)
	}
	if _, ok := metrics[_nsqWorkerCurrentGauge].(*prometheus.GaugeVec); !ok {
		t.Fatal(err)
	}
	if _, ok := metrics[_nsqThrottleGauge].(*prometheus.GaugeVec); !ok {
		t.Fatal(err)
	}
	if _, ok := metrics[_nsqMessageInBuffGauge].(*prometheus.GaugeVec); !ok {
		t.Fatal(err)
	}
}

func TestRegisterMetrics(t *testing.T) {
	if err := registerMetrics(); err != nil {
		t.Fatal(err)
	}
}
