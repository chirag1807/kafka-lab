package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

func NewConsumer() (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(Topic),
		kgo.ConsumerGroup("group-1"),
	)
}

func Poll(client *kgo.Client) {
	fetches := client.PollFetches(
		context.Background(),
	)

	fetches.EachRecord(func(r *kgo.Record) {
		fmt.Println(string(r.Value))
	})
}
