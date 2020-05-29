package producer

// Producer is a helper function for gonsq producer.
// NSQ do not have topic registration concept, any producer can
// freely publish any topic name. To prevent publishing message
// to a random tpoic, the wrapper provdes a mechanism to register
// topics locally. When publishing message, the producer will check
// whether a topic is available in the list of available topics or not.
