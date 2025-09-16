package kafka_lib

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// ErrNoMetrics ошибка при отсутствии метрик
var ErrNoMetrics = errors.New("metrics must not be nil")

// ErrConsumerClosed ошибка при попытке работы с закрытым consumer
var ErrConsumerClosed = errors.New("consumer is closed")

// Metrics интерфейс для сбора метрик потребления сообщений
type Metrics interface {
	// Increment увеличивает счетчик
	Increment(name string)
	// Duration регистрирует длительность операции
	Duration(timestamp int64, name string)
}

// ConsumerConfig содержит настройки для создания consumer'а
type ConsumerConfig struct {
	// Host адрес Kafka сервера
	Host []string
	// Port порт Kafka сервера
	Port []string
	// Topic имя топика
	Topic string
	// GroupID идентификатор группы потребителей
	GroupID string
	// MinBytes минимальный размер батча для получения (по умолчанию 1)
	MinBytes int
	// MaxBytes максимальный размер батча для получения (по умолчанию 1MB)
	MaxBytes int
	// MaxWait максимальное время ожидания сообщений
	MaxWait time.Duration
	// StartOffset начальное смещение (FirstOffset или LastOffset)
	StartOffset int64
}

// DefaultConsumerConfig возвращает конфигурацию по умолчанию
func DefaultConsumerConfig(host, port []string, topic, groupID string) ConsumerConfig {
	if groupID == "" {
		groupID = topic + "_group"
	}
	return ConsumerConfig{
		Host:        host,
		Port:        port,
		Topic:       topic,
		GroupID:     groupID,
		MinBytes:    1,
		MaxBytes:    1e6, // 1MB
		MaxWait:     time.Second * 3,
		StartOffset: kafka.FirstOffset,
	}
}

// KafkaConsumer представляет клиент для получения сообщений из Kafka
type KafkaConsumer struct {
	consumer *kafka.Reader
	m        Metrics
	topic    string
}

func NewConsumer(config ConsumerConfig, metrics Metrics) (*KafkaConsumer, error) {
	if metrics == nil {
		return nil, ErrNoMetrics
	}
	var broker []string
	for i, host := range config.Host {
		port := config.Port[i]
		broker = append(broker, host+":"+port)
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: broker,
		Topic:   config.Topic,
		GroupID: config.GroupID,
	})
	modifyTopic := strings.ReplaceAll(config.Topic, ".", "_")
	return &KafkaConsumer{consumer: reader, m: metrics, topic: modifyTopic}, nil
}

type handlerFunc func(ctx context.Context, msg []byte) error

func (c *KafkaConsumer) RegisterHandler(ctx context.Context, handleFunc handlerFunc) {
	go func() {
		clientId := c.consumer.Config().GroupID
		for {
			msg, err := c.consumer.FetchMessage(ctx)
			t := time.Now()
			if err != nil {
				log.Printf("failed to read message: %v", err)
				c.m.Increment(fmt.Sprintf("consume.%s.%s.error", clientId, c.topic))
				continue
			}

			err = handleFunc(ctx, msg.Value)
			if err != nil {
				log.Printf("failed to handle message: %v", err)
				c.m.Increment(fmt.Sprintf("consume.%s.%s.error", clientId, c.topic))
				continue
			}
			err = c.consumer.CommitMessages(ctx, msg)
			if err != nil {
				log.Printf("failed to commit message: %v", err)
				c.m.Increment(fmt.Sprintf("consume.%s.%s.error", clientId, c.topic))
				continue
			}
			c.m.Increment(fmt.Sprintf("consume.%s.%s.ok", clientId, c.topic))
			c.m.Duration(time.Since(t).Milliseconds(), fmt.Sprintf("consume.%s.%s", clientId, c.topic))
		}
	}()
}
