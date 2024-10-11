package kafka_lib

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Metrics interface {
	Increment(name string)
	Duration(timestamp int64, name string)
}

type KafkaConsumer struct {
	consumer *kafka.Reader
	m        Metrics
	topic    string
}

func NewConsumer(server string, topic string, metrics Metrics) (*KafkaConsumer, error) {
	if metrics == nil {
		return nil, fmt.Errorf("metrics must not be nil")
	}
	broker := []string{server}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: broker,
		Topic:   topic,
		GroupID: "123",
	})
	return &KafkaConsumer{consumer: reader, m: metrics, topic: topic}, nil
}

func (c *KafkaConsumer) RegisterHandler(ctx context.Context, handleFunc func(context.Context, []byte)) {
	go func() {
		for {
			msg, err := c.consumer.ReadMessage(ctx)
			t := time.Now()
			if err != nil {
				log.Printf("Error reading message: %v", err)
				c.m.Increment(fmt.Sprintf("consume.%s.error", c.topic))
				continue
			}
			c.m.Increment(fmt.Sprintf("consume.%s.ok", c.topic))
			handleFunc(ctx, msg.Value)
			c.m.Duration(time.Since(t).Milliseconds(), fmt.Sprintf("consume.%s", c.topic))
		}
	}()
}
