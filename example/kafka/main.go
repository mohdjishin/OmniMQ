package main

import (
	"context"
	"fmt"
	"log"
	"time"

	mq "github.com/mohdjishin/OmniMQ"
	"github.com/mohdjishin/OmniMQ/options"
)

func main() {
	ctx := context.Background()

	opts := &options.ConnectOptions{
		Type: options.KafkaMQ,
		URL:  "localhost:9092",
	}

	conn, err := mq.NewConnection(ctx, opts)
	if err != nil {
		log.Fatalf("Kafka connection failed: %v", err)
	}
	defer conn.Close()

	topic1 := "topic_A"
	topic2 := "topic_B"

	// Subscribe to topics
	go func() {
		err := conn.Subscribe(topic1, func(msg []byte) {
			fmt.Printf("[Listener] Received from %s: %s\n", topic1, msg)
		})
		if err != nil {
			log.Fatalf("Subscribe failed: %v", err)
		}
	}()

	go func() {
		err := conn.Subscribe(topic2, func(msg []byte) {
			fmt.Printf("[Listener] Received from %s: %s\n", topic2, msg)
		})
		if err != nil {
			log.Fatalf("Subscribe failed: %v", err)
		}
	}()

	time.Sleep(2 * time.Second) // Allow subscriptions to start

	// Publish messages to different topics
	err = conn.Publish(topic1, []byte("Message for Topic A"))
	if err != nil {
		log.Fatalf("Publish failed: %v", err)
	}
	fmt.Println("[Publisher] Sent to Topic A")

	err = conn.Publish(topic2, []byte("Message for Topic B"))
	if err != nil {
		log.Fatalf("Publish failed: %v", err)
	}
	fmt.Println("[Publisher] Sent to Topic B")

	time.Sleep(5 * time.Second) // Allow messages to be received
}
