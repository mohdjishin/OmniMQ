package mq

import (
	"context"
	"errors"
	"log/slog"

	"github.com/mohdjishin/OmniMQ/internal/jetstream"
	"github.com/mohdjishin/OmniMQ/internal/kafka"
	"github.com/mohdjishin/OmniMQ/internal/rabbitmq"
	"github.com/mohdjishin/OmniMQ/internal/stan"
	"github.com/mohdjishin/OmniMQ/options"
	"github.com/mohdjishin/OmniMQ/types"
)

func NewConnection(ctx context.Context, opts *options.ConnectOptions) (types.MQConnection, error) {
	if opts == nil {
		return nil, errors.New("connect options cannot be nil")
	}

	switch opts.Type {
	case options.JetStreamMQ:
		return jetstream.NewConnection(ctx, opts)
	case options.StanMQ:
		return stan.NewConnection(ctx, opts)
	case options.RabbitMQ:
		return rabbitmq.NewConnection(ctx, opts)
	case options.KafkaMQ:
		return kafka.NewConnection(ctx, opts)
	default:
		slog.Warn("Unknown message queue type, falling back to JetStream", "mqType", opts.Type)
		return jetstream.NewConnection(ctx, opts)
	}
}
