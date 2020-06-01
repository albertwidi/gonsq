package prometheus

import (
	"errors"
	"testing"
)

func TestMetricsList(t *testing.T) {
	err := errors.New("metrics: got nil")

	// Count metrics.
	if m := countMetrics[_nsqMessageRetrievedCount]; m == nil {
		t.Fatal(err)
	}
	if m := countMetrics[_nsqHandleCount]; m == nil {
		t.Fatal(err)
	}

	// Hist metrics.
	if m := histMetrics[_nsqHandleDurationHist]; m == nil {
		t.Fatal(err)
	}

	// Gauge metrics.
	if m := gaugeMetrics[_nsqWorkerCurrentGauge]; m == nil {
		t.Fatal(err)
	}
	if m := gaugeMetrics[_nsqThrottleGauge]; m == nil {
		t.Fatal(err)
	}
	if m := gaugeMetrics[_nsqMessageInBuffGauge]; m == nil {
		t.Fatal(err)
	}
}
