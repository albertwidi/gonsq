package gonsq

import (
	"testing"
)

func TestMessageCount(t *testing.T) {
	counts := []uint64{
		1, 2, 3, 4, 10, 100, 1000, 20000,
	}

	s := Stats{}

	var counter uint64
	for _, count := range counts {
		counter += count
		result := s.addMessageCount(count)

		currentMessageCount := s.MessageCount()

		if result != counter && result != currentMessageCount {
			t.Fatalf("expecting counter of %d but got %d", counter, currentMessageCount)
		}
	}
}

func TestErrorCount(t *testing.T) {
	counts := []uint64{
		1, 2, 3, 4, 10, 100, 1000, 20000,
	}

	s := Stats{}

	var counter uint64
	for _, count := range counts {
		counter += count
		result := s.addErrorCount(count)

		currentErrorCount := s.ErrorCount()

		if result != counter && result != currentErrorCount {
			t.Fatalf("expecting counter of %d but got %d", counter, currentErrorCount)
		}
	}
}

func TestStatsMessageInBuff(t *testing.T) {
	counts := []int64{
		1, 2, 3, 4, 10, 100, 1000, 20000,
	}

	s := Stats{}

	var counter int64
	for _, count := range counts {
		counter += count
		result := s.addMessageInBuffCount(count)

		currentMessageInbuff := s.MessageInBuffer()

		if result != counter && result != currentMessageInbuff {
			t.Fatalf("expecting counter of %d but got %d", counter, currentMessageInbuff)
		}
	}
}

// func TestStatsThrottle(t *testing.T) {
// 	booleans := []bool{
// 		true, false, true, true, false, false, true,
// 	}

// 	s := Stats{}

// 	var b bool
// 	for _, boolean := range booleans {
// 		b = boolean
// 		s.setThrottle(boolean)

// 		if b != s.Throttle().Boolean() {
// 			t.Fatalf("expecting throttle value of %v but got %v", b, s.Throttle())
// 		}
// 	}
// }

func TestThrottleCount(t *testing.T) {
	counts := []int64{
		1, 2, 3, 4, 10, 100, 1000, 20000,
	}

	s := Stats{}

	var counter int64
	for _, count := range counts {
		counter += count
		result := s.addThrottleCount(count)

		currentThrottleCount := s.ThrottleCount()

		if result != counter && result != currentThrottleCount {
			t.Fatalf("expecting counter of %d but got %d", counter, currentThrottleCount)
		}
	}
}

func TestStatsWorker(t *testing.T) {
	x := 10
	s := Stats{}

	for i := 0; i < x; i++ {
		s.addWorker(1)
	}

	if s.Worker() != int64(x) {
		t.Fatalf("expecting worker %d but got %d", x, s.Worker())
	}

	for ; x > 0; x-- {
		s.addWorker(-1)
	}

	if s.Worker() != 0 {
		t.Fatal("expect worker to be 0")
	}
}

func TestStatsConcurrency(t *testing.T) {
	concurrencies := []int{
		10, 50, 100, 500, 1000, 10000, 100000,
	}
	s := Stats{}

	for _, concurrency := range concurrencies {
		s.setConcurrency(concurrency)

		if s.Concurrency() != concurrency {
			t.Fatalf("expect %d concurrency but got %d", concurrency, s.Concurrency())
		}
	}
}

func TestStatsMaxInFlight(t *testing.T) {
	maxInFlights := []int{
		10, 50, 100, 500, 1000, 10000, 100000,
	}
	s := Stats{}

	for _, maxInFlight := range maxInFlights {
		s.setMaxInFlight(maxInFlight)

		if s.MaxInFlight() != maxInFlight {
			t.Fatalf("expect %d max in flight but got %d", maxInFlight, s.MaxInFlight())
		}
	}
}
