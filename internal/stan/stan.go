package stan

import (
	"context"

	"github.com/mohdjishin/OmniMQ/options"
	"github.com/mohdjishin/OmniMQ/types"

	"github.com/nats-io/stan.go"
)

type StanConnection struct {
	conn stan.Conn
}

// NewConnection initializes a new NATS Streaming (Stan) connection.
func NewConnection(ctx context.Context, opts *options.ConnectOptions) (types.MQConnection, error) {
	conn, err := stan.Connect(opts.ClusterID, opts.ClientID, stan.NatsURL(opts.URL))
	if err != nil {
		return nil, err
	}

	return &StanConnection{conn: conn}, nil
}

// Publish sends a message to a Stan subject.
func (s *StanConnection) Publish(subject string, data []byte) error {
	return s.conn.Publish(subject, data)
}

// Subscribe listens to a Stan subject.
func (s *StanConnection) Subscribe(subject string, handler func(msg []byte)) error {
	_, err := s.conn.Subscribe(subject, func(msg *stan.Msg) {
		handler(msg.Data)
	}, stan.StartWithLastReceived())
	return err
}

// Close closes the Stan connection.
func (s *StanConnection) Close() error {
	return s.conn.Close()
}
