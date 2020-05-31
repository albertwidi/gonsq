package consumer

// Consumer is a helper package for gonsq consumer.
// Default NSQ client does not allow one topic-multi channel connection creation for consumer.
// This client is wrapping the nsqio/go-nsq client and gonsq, and manage multiple connections
// to topics and clients in a single consumer manager.
