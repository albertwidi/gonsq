// TODO(albert): add helper package for producer

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/albertwidi/gonsq/example/pkg/jitter"

	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"

	id_message "github.com/albertwidi/gonsq/example/message"
	"github.com/albertwidi/gonsq/producer"
)

type Flags struct {
	Topics      string
	Channels    string
	NSQDAddress string
	Jitter      string
}

func main() {
	f := Flags{}
	flag.StringVar(&f.Topics, "nsq.topics", "", "NSQ Topic")
	flag.StringVar(&f.NSQDAddress, "nsq.nsqd-address", "", "NSQD Address")
	flag.StringVar(&f.Jitter, "publish.jitter", "100,300", "Jitter for publishing message <min,max>")
	flag.Parse()

	fmt.Printf("flags:\n%+v\n", f)

	var topics = strings.Split(f.Topics, ",")
	var stopC chan struct{}
	var err error

	var jitterStr = strings.Split(f.Jitter, ",")
	var jitterMin int64
	var jitterMax int64

	jitterMin, err = strconv.ParseInt(jitterStr[0], 10, 64)
	if err != nil {
		panic(err)
	}
	jitterMax, err = strconv.ParseInt(jitterStr[1], 10, 64)
	if err != nil {
		panic(err)
	}

	jt := jitter.New(jitterMin, jitterMax, time.Now().UnixNano())

	producer, err := producer.New(context.Background(), producer.Producer{
		Hostname:   "gonsq-producer",
		NSQAddress: f.NSQDAddress,
	}, topics...)
	if err != nil {
		panic(err)
	}

	for _, topic := range topics {
		go func(topic string) {
			for {
				select {
				case <-stopC:
					return
				default:
					time.Sleep(time.Millisecond * time.Duration(jt.Number()))

					message := &id_message.ID{
						UUID: uuid.New().String(),
					}

					out, _ := proto.Marshal(message)

					if err := producer.Publish(topic, out); err != nil {
						log.Println(err)
					}
				}
			}
		}(topic)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGQUIT)

	select {
	case sig := <-sigChan:
		switch sig {
		case syscall.SIGTERM, syscall.SIGQUIT:
			break
		}
	}

	// Stop all the goroutines for sending message.
	for i := 0; i < len(topics); i++ {
		stopC <- struct{}{}
	}

	fmt.Println("exiting producer")
}
