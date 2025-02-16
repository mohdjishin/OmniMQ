package types

type MQConnection interface {
	Publish(subject string, data []byte) error
	Subscribe(subject string, handler func(msg []byte)) error
	Close() error
}
