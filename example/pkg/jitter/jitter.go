// Do not use this library, this library is exist only for example.
// The library spawn a goroutines and never stop the goroutines explicitly.
//

package jitter

import (
	"math/rand"
)

type Jitter struct {
	min, max, seed int64
	generator      *rand.Rand
	numC           chan int64
}

func New(min, max, seed int64) *Jitter {
	rnd := rand.New(rand.NewSource(seed))

	j := &Jitter{
		min:       min,
		max:       max,
		seed:      seed,
		generator: rnd,
		numC:      make(chan int64, 1),
	}
	j.runGenerator()

	return j
}

func (j *Jitter) runGenerator() {
	go func() {
		for {
			j.numC <- j.generator.Int63n(j.max - j.min)
		}
	}()
}

func (j *Jitter) Number() int64 {
	return <-j.numC
}
