package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/nikgalushko/kafka-drivers-competition/sarama"
	"github.com/nikgalushko/kafka-drivers-competition/segmentio"
)

type Producer interface {
	SendAsync(msg []byte, key string, topic string) error
	SendSync(msg []byte, key string, topic string) error
	Close()
}

func initSaramaClusterProducer(addr []string) Producer {
	ret, err := sarama.NewProducer(addr)
	if err != nil {
		panic(fmt.Sprintf("sarama: %s", err.Error()))
	}

	return ret
}

func initSegmentioProducer(addr []string) Producer {
	ret, err := segmentio.NewProducer(addr)
	if err != nil {
		panic(fmt.Sprintf("segmentio: %s", err.Error()))
	}

	return ret
}

func produce() {
	producers := map[string]func([]string) Producer{
		"sarama-cluster": initSaramaClusterProducer,
		"segmentio":      initSegmentioProducer,
	}

	PrintMemUsage("before test")
	p := producers[driver]([]string{brokers})
	start := time.Now()
	for i := 0; i < records; i++ {
		s := strconv.Itoa(i)
		err := p.SendAsync([]byte(s), s, topic)
		if err != nil {
			panic(fmt.Sprintf("writer: %s", err))
		}
	}
	p.Close()
	PrintMemUsage(driver)
	fmt.Printf("%s finished: %s\n", driver, time.Since(start))
}
