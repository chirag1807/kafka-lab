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
    for {
        fetches := client.PollFetches(context.Background())

        if errs := fetches.Errors(); len(errs) > 0 {
            for _, err := range errs {
                fmt.Println("fetch error:", err)
            }
            continue
        }

        fetches.EachRecord(func(r *kgo.Record) {
            fmt.Println(string(r.Value))

            if err := client.CommitRecords(context.Background(), r); err != nil {
                fmt.Println("commit error:", err)
            }
        })
    }
}
