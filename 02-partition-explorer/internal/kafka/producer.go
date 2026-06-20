package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

func NewProducer() (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
	)
}

func Produce(client *kgo.Client, value []byte) error {
	record := &kgo.Record{
		Topic: Topic,
		Value: value,
	}
	return client.ProduceSync(
		context.Background(),
		record,
	).FirstErr()
}
