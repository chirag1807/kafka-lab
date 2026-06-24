package main

import (
	"fmt"
	"hello-kafka/02-partition-explorer/internal/kafka"
	"log"
)

func main() {
	// Ensure the topic exists with 3 partitions before consuming.
	fmt.Println("Creating topic (if not already exists)...")
	if err := kafka.CreateTopic(3, 1); err != nil {
		log.Fatal("create topic:", err)
	}
	fmt.Println("Topic ready.")

	consumer, err := kafka.NewConsumer()
	if err != nil {
		log.Fatal("new consumer:", err)
	}
	defer consumer.Close()

	fmt.Println("Listening on topic:", kafka.Topic)
	kafka.Poll(consumer)
}
