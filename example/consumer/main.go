package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/albertwidi/gonsq"
	"github.com/albertwidi/gonsq/consumer"
	random_message "github.com/albertwidi/gonsq/example/message"
	prommw "github.com/albertwidi/gonsq/middlewares/prometheus"
)

type Flags struct {
	Address           string
	Topic             string
	NSQLookupdAddress string
	Concurrency       int
}

func main() {
	f := Flags{}
	flag.StringVar(&f.Address, "server.address", ":9000", "Server Address")
	flag.StringVar(&f.Topic, "nsq.topic", "", "NSQ Topic")
	flag.StringVar(&f.NSQLookupdAddress, "nsq.lookupd-address", "", "NSQ Lookupd Address")
	flag.IntVar(&f.Concurrency, "nsq.concurrency", 1, "concurrency number to consume message from nsq")
	flag.Parse()

	fmt.Printf("flags:\n%+v\n", f)

	c, err := consumer.NewGroup(context.Background(), []string{f.NSQLookupdAddress}, []consumer.Group{
		{
			Topic: f.Topic,
			Channels: []consumer.Channel{
				{
					Name: "random1",
				},
				{
					Name: "random2",
				},
				{
					Name: "random3",
				},
			},
			Concurrency: gonsq.ConcurrencyConfig{
				Concurrency: 2,
				MaxInFlight: 100,
			},
		},
	})

	if err != nil {
		panic(err)
	}

	// Initialize throttle middleware.
	throttle := gonsq.ThrottleMiddleware{
		TimeDelay: time.Second,
	}

	// Chain the middleware, make sure
	// metrics is always the first middleware.
	c.Use(prommw.Metrics, throttle.Throttle)

	c.Handle(f.Topic, "random1", handler)
	c.Handle(f.Topic, "random2", handler)
	c.Handle(f.Topic, "random3", handler)

	if err := c.Start(); err != nil {
		panic(err)
	}
	defer c.Stop()

	errC := make(chan error)

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		errC <- http.ListenAndServe(f.Address, nil)
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGQUIT)

	select {
	case err := <-errC:
		panic(err)
	case sig := <-sigChan:
		switch sig {
		case syscall.SIGTERM, syscall.SIGQUIT:
			break
		}
	}

	fmt.Println("exiting consumer")
}

func handler(ctx context.Context, message *gonsq.Message) error {
	rm := random_message.Random{}
	if err := proto.Unmarshal(message.Body, &rm); err != nil {
		return err
	}

	defer message.Finish()
	fmt.Println(message.Topic, message.Channel, rm.RandomString, rm.RandomNumber)

	return nil
}
