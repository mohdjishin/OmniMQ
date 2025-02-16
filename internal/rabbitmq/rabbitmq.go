package rabbitmq

import (
	"context"

	"github.com/mohdjishin/OmniMQ/options"
	"github.com/mohdjishin/OmniMQ/types"

	"github.com/streadway/amqp"
)

// RabbitMQConnection represents a connection to RabbitMQ.
type RabbitMQConnection struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

// NewConnection initializes a new RabbitMQ connection.
func NewConnection(ctx context.Context, opts *options.ConnectOptions) (types.MQConnection, error) {
	conn, err := amqp.Dial(opts.URL)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &RabbitMQConnection{conn: conn, channel: ch}, nil
}

// Publish sends a message to a RabbitMQ queue.
func (r *RabbitMQConnection) Publish(subject string, data []byte) error {
	return r.channel.Publish("", subject, false, false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        data,
		})
}

// Subscribe listens to a RabbitMQ queue.
func (r *RabbitMQConnection) Subscribe(subject string, handler func(msg []byte)) error {
	msgs, err := r.channel.Consume(subject, "", true, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for d := range msgs {
			handler(d.Body)
		}
	}()
	return nil
}

// Close closes the RabbitMQ connection.
func (r *RabbitMQConnection) Close() error {
	return r.conn.Close()
}
