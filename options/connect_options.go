package options

// MQType represents the type of message queue.
type MQType int

const (
	JetStreamMQ MQType = iota
	StanMQ
	RabbitMQ
	KafkaMQ
)

// ConnectOptions holds configuration for connecting to a message queue.
type ConnectOptions struct {
	Type      MQType
	URL       string
	ClusterID string // Used for Stan/Kafka
	ClientID  string // Used for Stan/Kafka
}
