package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/albertwidi/gonsq"
	"github.com/albertwidi/gonsq/consumer"
	id_message "github.com/albertwidi/gonsq/example/message"
	prommw "github.com/albertwidi/gonsq/middleware/prometheus"
	"github.com/albertwidi/gonsq/middleware/throttle"
)

type Flags struct {
	Address           string
	Topics            string
	Channels          string
	NSQLookupdAddress string
	Concurrency       int
	MaxInFlight       int
}

func main() {
	f := Flags{}
	flag.StringVar(&f.Address, "server.address", ":9000", "Server Address")
	flag.StringVar(&f.Topics, "nsq.topics", "", "NSQ Topics")
	flag.StringVar(&f.Channels, "nsq.channels", "", "NSQ Channels")
	flag.StringVar(&f.NSQLookupdAddress, "nsq.lookupd-address", "", "NSQ Lookupd Address")
	flag.IntVar(&f.Concurrency, "nsq.concurrency", 1, "concurrency number to consume message from nsq")
	flag.IntVar(&f.MaxInFlight, "nsq.maxinflight", 100, "max in flight number")
	flag.Parse()

	fmt.Printf("flags:\n%+v\n", f)

	var groups []consumer.Group
	var groupChannels []consumer.Channel
	var topics = strings.Split(f.Topics, ",")
	var channels = strings.Split(f.Channels, ",")

	if len(topics) == 0 {
		panic("groups is empty")
	}
	if len(channels) == 0 {
		panic("channels is empty")
	}

	for _, channel := range channels {
		groupChannels = append(groupChannels, consumer.Channel{
			Name: strings.TrimSpace(channel),
		})
	}

	for _, topic := range topics {
		groups = append(groups, consumer.Group{
			Topic:    strings.TrimSpace(topic),
			Channels: groupChannels,
		})
	}

	c, err := consumer.NewGroup(context.Background(), []string{f.NSQLookupdAddress}, groups)
	if err != nil {
		panic(err)
	}

	// Initialize throttle middleware.
	tmw := throttle.Throttle{
		TimeDelay: time.Second,
	}

	// Chain the middleware, make sure
	// metrics is always the first middleware.
	c.Use(prommw.Metrics, tmw.Throttle)

	// Handle all topic and channels.
	for _, topic := range topics {
		for _, channel := range channels {
			c.Handle(topic, channel, handler)
		}
	}

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
	rm := id_message.ID{}
	if err := proto.Unmarshal(message.Body, &rm); err != nil {
		return err
	}

	defer message.Finish()

	// Sends some occasional error.
	if message.Stats.MessageCount()%3 == 0 {
		return errors.New("return some error")
	}

	return nil
}
