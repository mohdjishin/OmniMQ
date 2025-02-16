package kafka

import (
	"context"
	"fmt"
	"log"

	"github.com/mohdjishin/OmniMQ/options"
	"github.com/mohdjishin/OmniMQ/types"

	"github.com/segmentio/kafka-go"
)

type KafkaConnection struct {
	broker string
}

func NewConnection(ctx context.Context, opts *options.ConnectOptions) (types.MQConnection, error) {
	if opts.URL == "" {
		return nil, fmt.Errorf("Kafka broker URL must be provided")
	}

	return &KafkaConnection{
		broker: opts.URL,
	}, nil
}

func (k *KafkaConnection) createTopic(topic string) error {
	conn, err := kafka.Dial("tcp", k.broker)
	if err != nil {
		return fmt.Errorf("failed to connect to Kafka broker: %w", err)
	}
	defer conn.Close()

	// Check if topic already exists
	partitions, err := conn.ReadPartitions(topic)
	if err == nil && len(partitions) > 0 {
		return nil // Topic already exists
	}

	// Create the topic
	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	log.Printf("Kafka: Topic %s created", topic)
	return nil
}

// Publish sends a message to a Kafka topic.
func (k *KafkaConnection) Publish(topic string, data []byte) error {
	if topic == "" {
		return fmt.Errorf("topic must be specified when publishing")
	}

	// Ensure topic exists before publishing
	err := k.createTopic(topic)
	if err != nil {
		return err
	}

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{k.broker},
		Topic:   topic,
	})
	defer writer.Close()

	return writer.WriteMessages(context.Background(), kafka.Message{
		Value: data,
	})
}

// Subscribe listens to a Kafka topic.
func (k *KafkaConnection) Subscribe(topic string, handler func(msg []byte)) error {
	if topic == "" {
		return fmt.Errorf("topic must be specified when subscribing")
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{k.broker},
		Topic:   topic,
		GroupID: "group_" + topic,
	})

	go func() {
		for {
			msg, err := reader.ReadMessage(context.Background())
			if err != nil {
				log.Println("Kafka read error:", err)
				break
			}
			handler(msg.Value)
		}
	}()

	return nil
}

// Close closes the Kafka connection.
func (k *KafkaConnection) Close() error {
	return nil // No global connection to close
}
