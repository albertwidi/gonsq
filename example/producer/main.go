package main

import "flag"

type Flags struct {
	Topic       string
	NSQDAddress string
}

func main() {
	f := Flags{}
	flag.StringVar(&f.Topic, "nsq.topic", "", "NSQ Topic")
	flag.StringVar(&f.NSQDAddress, "nsq.nsqd-address", "", "NSQD Address")
	flag.Parse()
}
