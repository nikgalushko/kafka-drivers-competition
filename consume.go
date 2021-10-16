package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/nikgalushko/kafka-drivers-competition/sarama"
	"github.com/nikgalushko/kafka-drivers-competition/segmentio"
)

type Consumer interface {
	Consume(func([]byte) bool) error
}

func initSaramaCluster(groupID string, addr []string, topic string) Consumer {
	ret, err := sarama.NewConsumer(groupID, addr, topic)
	if err != nil {
		panic(fmt.Sprintf("sarama: %s", err.Error()))
	}

	return ret
}

func initSegmentio(groupID string, addr []string, topic string) Consumer {
	ret, err := segmentio.NewConsumer(groupID, addr, topic)
	if err != nil {
		panic(fmt.Sprintf("segmentio: %s", err.Error()))
	}

	return ret
}

func consume() {
	p, err := sarama.NewProducer([]string{brokers})
	if err != nil {
		panic(fmt.Sprintf("producer initialize failed %s", err))
	}

	goldenResult := 0
	go func() {
		time.Sleep(500 * time.Millisecond)

		defer p.Close()

		for i := 0; i < records; i++ {
			goldenResult += i
			s := strconv.Itoa(i)
			_ = p.SendAsync([]byte(s), s, topic)
		}
	}()

	consumers := map[string]func(string, []string, string) Consumer{
		"sarama-cluster": initSaramaCluster,
		"segmentio":      initSegmentio,
	}

	PrintMemUsage("before test")
	var (
		consumer = consumers[driver](time.Now().GoString(), []string{brokers}, topic)
		count    = uint(0)
		sum      = 0
		start    time.Time
	)

	err = consumer.Consume(func(data []byte) bool {
		if count == 0 {
			start = time.Now()
		}
		count++

		i, _ := strconv.Atoi(string(data))
		sum += i

		return count != uint(records)
	})

	PrintMemUsage(driver)
	fmt.Printf("%s finished: %s; result error: %v; consumed: %d. Is result valid - %t\n", driver, time.Since(start), err, count, sum == goldenResult)
}
