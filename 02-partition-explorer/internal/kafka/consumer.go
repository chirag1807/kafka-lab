package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// CreateTopic uses the Kafka admin API to create the topic with the
// configured number of partitions. It is idempotent — if the topic already
// exists the error is silently ignored.
func CreateTopic(partitions int32, replicationFactor int16) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
	)
	if err != nil {
		return fmt.Errorf("admin client: %w", err)
	}
	defer client.Close()

	admin := kadm.NewClient(client)

	resp, err := admin.CreateTopic(context.Background(), partitions, replicationFactor, nil, Topic)
	if err != nil {
		return fmt.Errorf("create topic request: %w", err)
	}

	// TopicAlreadyExists (-36) is fine — nothing to do.
	if resp.Err != nil && resp.Err.Error() != "TOPIC_ALREADY_EXISTS" {
		return fmt.Errorf("create topic: %w", resp.Err)
	}

	return nil
}

// NewConsumer returns a kgo.Client configured as a consumer for the topic.
func NewConsumer() (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(Topic),
		kgo.ConsumerGroup("partition-explorer-group"),
	)
}

// Poll continuously polls for records and prints each message together with
// its partition and offset — the core point of the partition-explorer lab.
func Poll(client *kgo.Client) {
	fmt.Printf("%-12s %-10s %-30s %s\n", "PARTITION", "OFFSET", "KEY", "VALUE")
	fmt.Println("---------------------------------------------------------------")

	for {
		fetches := client.PollFetches(context.Background())

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				fmt.Println("fetch error:", err)
			}
			continue
		}

		fetches.EachRecord(func(r *kgo.Record) {
			fmt.Printf("%-12d %-10d %-30s %s\n",
				r.Partition,
				r.Offset,
				string(r.Key),
				string(r.Value),
			)

			if err := client.CommitRecords(context.Background(), r); err != nil {
				fmt.Println("commit error:", err)
			}
		})
	}
}

