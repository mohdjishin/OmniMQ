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
	opts := &options.ConnectOptions{
		Type: options.JetStreamMQ,
		URL:  "nats://localhost:4222",
	}

	conn, err := mq.NewConnection(context.Background(), opts)
	if err != nil {
		log.Fatal("Failed to connect to JetStream:", err)
	}
	defer conn.Close()

	// Publish a message
	err = conn.Publish("test_subject", []byte("Hello, JetStream via mq!"))
	if err != nil {
		log.Fatal("Failed to publish:", err)
	}
	fmt.Println("Published message to JetStream!")

	// Subscribe and print messages
	err = conn.Subscribe("test_subject", func(msg []byte) {
		fmt.Println("Received:", string(msg))
	})
	if err != nil {
		log.Fatal("Failed to subscribe:", err)
	}

	// Wait for messages
	time.Sleep(5 * time.Second)
}
