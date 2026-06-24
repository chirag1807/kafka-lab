package main

import (
	"encoding/json"
	"fmt"
	"hello-kafka/02-partition-explorer/internal/kafka"
	"hello-kafka/02-partition-explorer/internal/models"
	"log"
)

func main() {
	producer, err := kafka.NewProducer()
	if err != nil {
		log.Fatal(err)
	}

	defer producer.Close()

	for i := range 100 {
		msg := models.Message{
			ID:      i,
			Content: fmt.Sprintf("message-%d", i),
		}

		data, _ := json.Marshal(msg)

		// Use a key derived from the message index modulo 3 so that messages
		// are spread across the three partitions in a deterministic way.
		key := fmt.Sprintf("key-%d", i%3)

		err := kafka.Produce(
			producer,
			[]byte(key),
			data,
		)

		if err != nil {
			panic(err)
		}

		fmt.Printf("sent msg=%-3d key=%s\n", i, key)
	}
}
