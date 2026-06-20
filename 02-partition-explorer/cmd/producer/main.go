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

		err := kafka.Produce(
			producer,
			data,
		)

		if err != nil {
			panic(err)
		}

		fmt.Println("sent", i)
	}
}
