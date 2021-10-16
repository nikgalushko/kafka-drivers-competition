package sarama

import (
	"context"
	"time"

	shopify "github.com/Shopify/sarama"
)

type Consumer struct {
	c     shopify.ConsumerGroup
	topic string
}

func NewConsumer(groupID string, addr []string, topic string) (Consumer, error) {
	cfg := shopify.NewConfig()
	cfg.Consumer.Offsets.CommitInterval = time.Second
	cfg.Consumer.Return.Errors = false

	cg, err := shopify.NewConsumerGroup(addr, groupID, cfg)
	if err != nil {
		return Consumer{}, err
	}

	return Consumer{c: cg, topic: topic}, nil
}

func (c Consumer) Consume(f func([]byte) bool) error {
	return c.c.Consume(context.Background(), []string{c.topic}, simpleHandler{f: f})
}

type simpleHandler struct {
	f func([]byte) bool
}

func (simpleHandler) Setup(_ shopify.ConsumerGroupSession) error   { return nil }
func (simpleHandler) Cleanup(_ shopify.ConsumerGroupSession) error { return nil }
func (h simpleHandler) ConsumeClaim(sess shopify.ConsumerGroupSession, claim shopify.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if !h.f(msg.Value) {
			return nil
		}

		sess.MarkMessage(msg, "")
	}
	return nil
}
