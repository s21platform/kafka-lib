package kafka_lib

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type KafkaProducer struct {
	producer *kafka.Writer
}

func NewProducer(serverAddr string, topic string) *KafkaProducer {
	producer := &kafka.Writer{
		Addr:         kafka.TCP(serverAddr),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
	}

	return &KafkaProducer{producer: producer}
}

func (k *KafkaProducer) ProduceMessage(message interface{}) error {
	bMessage, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}

	err = k.producer.WriteMessages(context.Background(), kafka.Message{
		Value: bMessage,
	})
	if err != nil {
		return fmt.Errorf("failed to write message: %v", err)
	}
	return nil
}
