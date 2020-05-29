package gonsq

import (
	"context"
	"errors"
	"time"

	nsqio "github.com/nsqio/go-nsq"
)

var (
	errTopicEmpty   = errors.New("gonsq: topic is empty")
	errChannelEmpty = errors.New("gonsq: channel is empty")

	defaultConcurrency         = 1
	defaultMaxInFlight         = 50
	defaultLookupdPoolInterval = time.Second
	defaultWriteTimeout        = time.Second * 10
	defaultReadTimeout         = time.Second * 10
	// defaultHeartBeatInterval must be less than write timeout.
	defaultHeartBeatInterval = time.Second * 9
	defaultDeflateLevel      = 1
)

// Config of nsqio
type Config struct {
	Hostname string
	// This must be less than Timeout.WriteTimeout
	HeartbeatInterval time.Duration
	Lookupd           LookupdConfig
	Timeout           TimeoutConfig
	Queue             QueueConfig
	Compression       CompressionConfig
}

// TimeoutConfig for timeout configuration
type TimeoutConfig struct {
	Dial           time.Duration `toml:"dial" yaml:"dial"`
	Read           time.Duration `toml:"read" yaml:"read"`
	Write          time.Duration `toml:"write" yaml:"write"`
	MessageTimeout time.Duration `toml:"message" yaml:"message"`
}

func (tm *TimeoutConfig) Validate() error {
	if tm.Read == 0 {
		tm.Read = defaultReadTimeout
	}
	if tm.Write == 0 {
		tm.Write = defaultWriteTimeout
	}
	return nil
}

// LookupdConfig for lookupd configuration
type LookupdConfig struct {
	PoolInterval time.Duration `toml:"pool_interval" yaml:"pool_interval"`
	PollJitter   float64       `toml:"pool_jitter" yaml:"pool_jitter"`
}

func (ld *LookupdConfig) Validate() error {
	if ld.PoolInterval == 0 {
		ld.PoolInterval = defaultLookupdPoolInterval
	}
	return nil
}

// QueueConfig for message configuration. In the queue config, MaxInFlight is excluded
// because the MaxInFlight configuration will depends on the buffer length.
type QueueConfig struct {
	MsgTimeout          time.Duration `toml:"message_timeout" yaml:"message_timeout"`
	MaxRequeueDelay     time.Duration `toml:"max_requeue_delay" yaml:"max_requeue_delay"`
	DefaultRequeueDelay time.Duration `toml:"default_requeue_delay" yaml:"default_requeue_delay"`
}

// CompressionConfig to support compression
type CompressionConfig struct {
	Deflate      bool `toml:"deflate" yaml:"deflate"`
	DeflateLevel int  `toml:"deflate_level" yaml:"deflate_level"`
	Snappy       bool `toml:"snappy" yaml:"snappy"`
}

func (cm *CompressionConfig) Validate() error {
	if cm.DeflateLevel == 0 {
		cm.DeflateLevel = defaultDeflateLevel
	}
	return nil
}

// ConcurrencyConfig control the concurrency flow in gonsq.
// Concurrency and MaxInFlight are combined to determine the number of
// buffered channel. This number then affect how the library throttle
// the message consumption.
type ConcurrencyConfig struct {
	// Concurrency is the number of worker/goroutines intended for handling incoming/consumed messages.
	Concurrency int
	// MaxInFlight is sort of comparable to the TCP "window size", it controls how many messages nsqd will send
	// to the consumer before hearing back about any of them.
	// This description is taken from https://github.com/nsqio/go-nsq/issues/221#issuecomment-352865779.
	// Note that MaxInFlight number is the number per concurrent job. At the end, the length of buffered channel is
	// MaxInflight * Concurrency.
	MaxInFlight int
}

// Validate the value of concurrency config, if some value is not exist then set the default value.
func (cc *ConcurrencyConfig) Validate() error {
	if cc.Concurrency <= 0 {
		cc.Concurrency = defaultConcurrency
	}
	if cc.MaxInFlight <= 0 {
		cc.MaxInFlight = defaultMaxInFlight
	}
	return nil
}

func (cc ConcurrencyConfig) IsEmpty() bool {
	tmp := ConcurrencyConfig{}
	if cc == tmp {
		return true
	}
	return false
}

func newConfig(conf Config) (*nsqio.Config, error) {
	cfg := nsqio.NewConfig()

	// Basic configuration properties
	cfg.Hostname = conf.Hostname
	cfg.HeartbeatInterval = conf.HeartbeatInterval
	// Queue configuration properties.
	cfg.MsgTimeout = conf.Queue.MsgTimeout
	cfg.MaxRequeueDelay = conf.Queue.MaxRequeueDelay
	cfg.DefaultRequeueDelay = conf.Queue.DefaultRequeueDelay
	// Timeout configuration properties.
	cfg.DialTimeout = conf.Timeout.Dial
	cfg.ReadTimeout = conf.Timeout.Read
	cfg.WriteTimeout = conf.Timeout.Write
	cfg.MsgTimeout = conf.Timeout.MessageTimeout
	// Lookupd configuration properties.
	cfg.LookupdPollInterval = conf.Lookupd.PoolInterval
	cfg.LookupdPollJitter = conf.Lookupd.PollJitter
	// Compression configuration properties.
	cfg.Deflate = conf.Compression.Deflate
	cfg.DeflateLevel = conf.Compression.DeflateLevel
	cfg.Snappy = conf.Compression.Snappy

	return cfg, cfg.Validate()
}

// ProducerConfig struct
type ProducerConfig struct {
	Hostname    string
	Address     string
	Compression CompressionConfig
	Timeout     TimeoutConfig
}

func (pc *ProducerConfig) Validate() error {
	if err := pc.Timeout.Validate(); err != nil {
		return err
	}

	if err := pc.Compression.Validate(); err != nil {
		return err
	}

	return nil
}

// NSQProducer backend
type NSQProducer struct {
	producer *nsqio.Producer
}

// NewProducer return a new producer
func NewProducer(ctx context.Context, config ProducerConfig) (*NSQProducer, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	conf := Config{
		Hostname:    config.Hostname,
		Timeout:     config.Timeout,
		Compression: config.Compression,
	}

	nsqConf, err := newConfig(conf)
	if err != nil {
		return nil, err
	}

	p, err := nsqio.NewProducer(config.Address, nsqConf)
	if err != nil {
		return nil, err
	}

	prod := NSQProducer{
		producer: p,
	}
	return &prod, nil
}

// Ping the nsqd of producer
func (np *NSQProducer) Ping() error {
	return np.producer.Ping()
}

// Publish to nsqd
func (np *NSQProducer) Publish(topic string, body []byte) error {
	return np.producer.Publish(topic, body)
}

// MultiPublish to nsqd
func (np *NSQProducer) MultiPublish(topic string, body [][]byte) error {
	return np.producer.MultiPublish(topic, body)
}

// Stop the nsq producer
func (np *NSQProducer) Stop() {
	np.Stop()
}

// NSQConsumerConfig for nsq consumer
type NSQConsumerConfig struct {
	Hostname string
	Topic    string
	Channel  string
	// This must be less than Timeout.WriteTimeout
	HeartbeatInterval time.Duration
	Lookupd           LookupdConfig
	Timeout           TimeoutConfig
	Queue             QueueConfig
	Compression       CompressionConfig
	Concurrency       ConcurrencyConfig
}

// Validate consumer configuration
func (cf *NSQConsumerConfig) Validate() error {
	if cf.Topic == "" {
		return errTopicEmpty
	}
	if cf.Channel == "" {
		return errChannelEmpty
	}

	if cf.HeartbeatInterval == 0 {
		cf.HeartbeatInterval = defaultHeartBeatInterval
	}

	if err := cf.Lookupd.Validate(); err != nil {
		return err
	}

	if err := cf.Timeout.Validate(); err != nil {
		return err
	}

	if err := cf.Concurrency.Validate(); err != nil {
		return err
	}

	if err := cf.Compression.Validate(); err != nil {
		return err
	}

	return nil
}

// NSQConsumer backend
type NSQConsumer struct {
	consumer *nsqio.Consumer
	config   NSQConsumerConfig
}

// NewConsumer for nsq
func NewConsumer(ctx context.Context, config NSQConsumerConfig) (*NSQConsumer, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	conf := Config{
		Hostname:    config.Hostname,
		Lookupd:     config.Lookupd,
		Timeout:     config.Timeout,
		Queue:       config.Queue,
		Compression: config.Compression,
	}

	nsqioConfig, err := newConfig(conf)
	if err != nil {
		return nil, err
	}
	con, err := nsqio.NewConsumer(config.Topic, config.Channel, nsqioConfig)
	if err != nil {
		return nil, err
	}

	nc := NSQConsumer{
		consumer: con,
		config:   config,
	}
	return &nc, nil
}

// Topic return the topic of consumer
func (nc *NSQConsumer) Topic() string {
	return nc.config.Topic
}

// Channel return the channel of consumer
func (nc *NSQConsumer) Channel() string {
	return nc.config.Channel
}

// ConnectToNSQLookupds connecting to several nsq lookupd
func (nc *NSQConsumer) ConnectToNSQLookupds(addresses []string) error {
	return nc.consumer.ConnectToNSQLookupds(addresses)
}

// AddHandler to nsq
func (nc *NSQConsumer) AddHandler(handler nsqio.Handler) {
	nc.consumer.AddHandler(handler)
}

// AddConcurrentHandlers add concurrent handler to nsq
func (nc *NSQConsumer) AddConcurrentHandlers(handler nsqio.Handler, concurrency int) {
	nc.consumer.AddConcurrentHandlers(handler, concurrency)
}

// Stop nsq consumer
func (nc *NSQConsumer) Stop() {
	nc.consumer.Stop()
}

// ChangeMaxInFlight will change max in flight number in nsq consumer
func (nc *NSQConsumer) ChangeMaxInFlight(n int) {
	nc.consumer.ChangeMaxInFlight(n)
}

// Concurrency return the concurrency number for a given consumer
func (nc *NSQConsumer) Concurrency() int {
	return nc.config.Concurrency.Concurrency
}

// MaxInFlight return the max in flight number for a given consumer
func (nc *NSQConsumer) MaxInFlight() int {
	return nc.config.Concurrency.MaxInFlight
}
