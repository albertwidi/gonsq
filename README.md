# Gonsq

**Work In Progress**

Gonsq is a wrapper of [go-nsq](https://github.com/nsqio/go-nsq) library

The library inspired by how [Lyft Building an Adaptive, Multi-Tenant Stream Bus with Kafka and Golang](https://eng.lyft.com/building-an-adaptive-multi-tenant-stream-bus-with-kafka-and-golang-5f1410bf2b40) and [Flow Control architecture in envoy proxy](https://github.com/envoyproxy/envoy/blob/master/source/docs/flow_control.md).

## Nsqio

Gonsq is not using standard `nsq.Consumer` and `nsq.Producer`, instead the library provides `NSQConsumer` and `NSQProducer` object to communicate directly with `nsq.Consumer` and `nsq.Producer`.

Some properties also added to the `NSQConsumer` object, for example `concurrency`. The `concurrency` information is used to inform the `Gonsq` about how many concurrent consumers that a given `NSQConsumer` want to run.

## Design

The library flow control is based on buffered channel for each `topic` and `channel`. This means every consumer for different `topic` and `channel` might has different size of buffered channel and number of concurrency.

The worker model will replace `ConcurrenHandler` for handling multiple message concurrently. This is because the library want to control the flow of the message by using buffered channel as the main communication channel for worker.

![nsq throttling design](../../docs/images/nsq_throttle_design.png)

## Throttling 

By design, the `handler` that registered by this library is not directly exposed to the `consumer handler`. This means the `handler` not directly asking for message from `nsq`.

The message is being sent into the `handler` from a go channel that is dedicated for specific `topic` and `channel`, and the message can be consumed from that go channel only. By using this mechanism, the rate of message consumed by the `concurrent handler` can be controlled when something wrong happened.

**Message Retrieval Throttling**

This throttling is on by default.

The message retrieval is throttled when the number of message in the `channel` is more than half of its size.

For example, if the length of buffer is 10, and the message already exceeding 5. The consumer will pause the message consumption until the number of message in the buffer is going back to less than half of the buffer.

**Message Processing Throttling**

This throttling can be enabled by using `Throttling` middleware

The message processing is throttled when the number of message in the `channel` is mor ethan half of its size.

For example if the length of buffer is 10, and the message already exceeding 5. The consumer will slow down the message processing, this throttling is being handled by the `Throttling` middleware in this library. If the throttle middleware is set, then the library will seek `throttled` status in the message.

## Stats

The library is exposing some metrics for internal usage and might be useful for the user to send the metrics to some monitoring backend. The `stats` object is available through `nsq.Message` struct and passed to the message handler.

The exposed metrics are:

- Total Message Count: The total count of messages consumed by particular worker of `topic` and `channel`.
- Total Error Count: The total count of error happens in particular worker of `topic` and `channel`.
- Total Message In Buffer Count: The total count of buffer used in particular worker of `topic` and `channel`. This stat is used to determine whether a throttling mechanism need to be triggered or not.
- Total Buffer Length: The total length of buffer available for particular worker of `topic` and `channel`
- Total Concurrency: The total number of concurrency/woker for particular worker of `topic` and `channel`
- Total Worker Count: The current total number of worker for particular worker of `topic` and `channel`. This stat will be useful if we have a mechanism to reduce/increase the number of worker based on condition. For now, this is used to determine the number of worker on startup and shutdown.
- Throttled: The status of particular `topic` and `channel`, is the consumer is being `throttled` or not.


## How To Use The Library

To use this library, the `consumer` must be created using `nsq/nsqio`.

## TODO

- DNS: make it possible to specify a single addresss with host or single/multiple address with IP. If a single host is given, then resolve to host.