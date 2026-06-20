package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func NewConsumer() {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
	)

	if err != nil {
		panic(err)
	}

	admin := kadm.NewClient(client)

	var partitions int32 = 3
	var replicationFactor int16 = 1

	resp, err := admin.CreateTopic(context.Background(), partitions, replicationFactor, nil, Topic)
	if err != nil {
		panic(err)
	}

	if resp.Err != nil {
		panic(resp.Err)
	}
}
