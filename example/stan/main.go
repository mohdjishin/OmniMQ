package main

import (
	"context"
	"fmt"
	"log"

	mq "github.com/mohdjishin/OmniMQ"
	"github.com/mohdjishin/OmniMQ/options"
)

func main() {
	opts := &options.ConnectOptions{
		Type:      options.StanMQ,
		URL:       "nats://localhost:4222",
		ClusterID: "test-cluster",
		ClientID:  "stan_client",
	}

	conn, err := mq.NewConnection(context.Background(), opts)
	if err != nil {
		log.Fatal("Failed to connect to STAN:", err)
	}
	defer conn.Close()

	err = conn.Publish("test_topic", []byte("Hello, STAN via mq!"))
	if err != nil {
		log.Fatal("Failed to publish:", err)
	}
	fmt.Println("Published message to STAN!")

	err = conn.Subscribe("test_topic", func(msg []byte) {
		fmt.Println("Received:", string(msg))
	})
	if err != nil {
		log.Fatal("Failed to subscribe:", err)
	}

}
