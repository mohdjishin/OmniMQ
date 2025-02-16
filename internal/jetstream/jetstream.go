package jetstream

import (
	"context"

	"github.com/mohdjishin/OmniMQ/options"
	"github.com/mohdjishin/OmniMQ/types"

	"github.com/nats-io/nats.go"
)

// JetStreamConnection represents a connection to NATS JetStream.
type JetStreamConnection struct {
	conn *nats.Conn
	js   nats.JetStreamContext
}

// NewConnection initializes a new JetStream connection.
func NewConnection(ctx context.Context, opts *options.ConnectOptions) (types.MQConnection, error) {
	conn, err := nats.Connect(opts.URL)
	if err != nil {
		return nil, err
	}

	js, err := conn.JetStream()
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &JetStreamConnection{conn: conn, js: js}, nil
}

// Publish sends a message to a JetStream subject.
func (j *JetStreamConnection) Publish(subject string, data []byte) error {
	_, err := j.js.Publish(subject, data)
	return err
}

// Subscribe listens to a JetStream subject.
func (j *JetStreamConnection) Subscribe(subject string, handler func(msg []byte)) error {
	_, err := j.js.Subscribe(subject, func(msg *nats.Msg) {
		handler(msg.Data)
	})
	return err
}

// Close closes the JetStream connection.
func (j *JetStreamConnection) Close() error {
	j.conn.Close()
	return nil
}
