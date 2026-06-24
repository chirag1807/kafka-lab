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

// Produce sends a message with an explicit key. Franz-go applies murmur2
// hashing on the key to route the record to a deterministic partition, which
// is what makes partition routing visible in the consumer output.
func Produce(client *kgo.Client, key []byte, value []byte) error {
	record := &kgo.Record{
		Topic: Topic,
		Key:   key,
		Value: value,
	}
	return client.ProduceSync(
		context.Background(),
		record,
	).FirstErr()
}

