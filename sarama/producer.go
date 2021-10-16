package sarama

import (
	"time"

	"github.com/Shopify/sarama"
	shopify "github.com/Shopify/sarama"
)

type Producer struct {
	p sarama.AsyncProducer
}

func NewProducer(addrs []string) (Producer, error) {
	sarCfg := shopify.NewConfig()

	sarCfg.ChannelBufferSize = 1024

	sarCfg.Producer.RequiredAcks = shopify.WaitForLocal
	sarCfg.Producer.Timeout = 10 * time.Second
	sarCfg.Producer.MaxMessageBytes = 1000000

	sarCfg.Producer.Return.Errors = false
	sarCfg.Producer.Return.Successes = false

	p, err := shopify.NewAsyncProducer(addrs, sarCfg)
	if err != nil {
		return Producer{}, nil
	}

	return Producer{p: p}, nil
}

func (p Producer) SendAsync(msg []byte, key string, topic string) error {
	sarMsg := &shopify.ProducerMessage{
		Topic:    topic,
		Key:      shopify.StringEncoder(key),
		Value:    shopify.ByteEncoder(msg),
		Metadata: nil,
	}

	p.p.Input() <- sarMsg

	return nil
}

func (p Producer) SendSync(msg []byte, key string, topic string) error {
	expectation := make(chan error, 1)
	sarMsg := &shopify.ProducerMessage{
		Topic:    topic,
		Key:      shopify.StringEncoder(key),
		Value:    shopify.ByteEncoder(msg),
		Metadata: expectation,
	}

	p.p.Input() <- sarMsg

	return <-expectation
}

func (p Producer) Close() {
	p.p.Close()
}
