package segmentio

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	sync  *kafka.Writer
	async *kafka.Writer
}

func NewProducer(addrs []string) (Producer, error) {
	return Producer{sync: newProducer(addrs, false), async: newProducer(addrs, true)}, nil
}

func newProducer(addrs []string, async bool) *kafka.Writer {
	cfg := kafka.WriterConfig{
		Brokers:      addrs,
		Balancer:     &kafka.Hash{},
		RequiredAcks: -1,
		Async:        async,
		ReadTimeout:  10 * time.Second,
		BatchSize:    2048,
		BatchTimeout: 10 * time.Millisecond,
	}

	return kafka.NewWriter(cfg)
}

func (p Producer) SendAsync(msg []byte, key string, topic string) error {
	return p.async.WriteMessages(context.Background(), kafka.Message{Key: []byte(key), Value: msg, Topic: topic})
}

func (p Producer) SendSync(msg []byte, key string, topic string) error {
	return p.sync.WriteMessages(context.Background(), kafka.Message{Key: []byte(key), Value: msg, Topic: topic})
}

func (p Producer) Close() {
	p.sync.Close()
	p.async.Close()
}
