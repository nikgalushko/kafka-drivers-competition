package segmentio

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	r *kafka.Reader
}

func NewConsumer(groupID string, addr []string, topic string) (Consumer, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:               addr,
		GroupID:               groupID,
		Topic:                 topic,
		MinBytes:              1,
		MaxBytes:              1000000,
		MaxWait:               100 * time.Millisecond,
		WatchPartitionChanges: true,
		CommitInterval:        time.Second,
	})

	return Consumer{r: reader}, nil
}

func (c Consumer) Consume(f func([]byte) bool) error {
	for {
		msg, err := c.r.FetchMessage(context.Background())
		if err != nil {
			return err
		}

		if !f(msg.Value) {
			return nil
		}

		c.r.CommitMessages(context.Background(), msg)
	}
}
