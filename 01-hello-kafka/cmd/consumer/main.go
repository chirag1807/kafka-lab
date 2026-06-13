package main

import (
	"hello-kafka/01-hello-kafka/internal/kafka"
)

func main() {
    consumer, err := kafka.NewConsumer()
    if err != nil {
        panic(err)
    }

    defer consumer.Close()

    kafka.Poll(consumer)
}
