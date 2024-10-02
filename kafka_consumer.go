package kafka_lib

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
)

type Metrics interface {
	Increment(name string)
}

type KafkaConsumer struct {
	consumer *kafka.Reader
	m        Metrics
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
	return &KafkaConsumer{consumer: reader, m: metrics}, nil
}

func (c *KafkaConsumer) RegisterHandler(ctx context.Context, handleFunc func(context.Context, interface{})) {
	go func() {
		for {
			msg, err := c.consumer.ReadMessage(ctx)
			if err != nil {
				log.Printf("Error reading message: %v", err)
				c.m.Increment("new_friend.error")
				continue
			}
			c.m.Increment("new_friend.ok")
			handleFunc(ctx, msg)
		}
	}()
}
