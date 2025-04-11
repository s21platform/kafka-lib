package kafka_lib

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaProducer представляет клиент для отправки сообщений в Kafka
type KafkaProducer struct {
	producer *kafka.Writer
}

// ProducerConfig содержит настройки для создания producer'а
type ProducerConfig struct {
	// Host адрес Kafka сервера
	Host string
	// Port порт Kafka сервера
	Port string
	// Topic имя топика
	Topic string
	// BatchSize размер пакета сообщений (0 - отключить)
	BatchSize int
	// RequiredAcks настройка подтверждения записи (0, 1, -1)
	RequiredAcks kafka.RequiredAcks
	// Timeout таймаут операций
	Timeout time.Duration
}

// DefaultProducerConfig возвращает конфигурацию по умолчанию
func DefaultProducerConfig(host, port, topic string) ProducerConfig {
	return ProducerConfig{
		Host:         host,
		Port:         port,
		Topic:        topic,
		BatchSize:    0,
		RequiredAcks: kafka.RequireAll,
		Timeout:      10 * time.Second,
	}
}

// NewProducer создает новый экземпляр KafkaProducer с заданными настройками
func NewProducer(config ProducerConfig) *KafkaProducer {
	producer := &kafka.Writer{
		Addr:         kafka.TCP(config.Host + ":" + config.Port),
		Topic:        config.Topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: config.RequiredAcks,
		BatchSize:    config.BatchSize,
		WriteTimeout: config.Timeout,
	}

	return &KafkaProducer{producer: producer}
}

// ProduceMessage отправляет сообщение в Kafka
// Принимает контекст для возможности отмены операции и таймаутов
// Параметр T определяет тип отправляемого сообщения
// Параметр K определяет тип ключа сообщения
func ProduceMessage[T any, K any](k *KafkaProducer, ctx context.Context, message T, key K) error {
	bMessage, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}

	keyBytes, err := convertKeyToBytes(key)
	if err != nil {
		return fmt.Errorf("failed to convert key to bytes: %v", err)
	}

	err = k.producer.WriteMessages(ctx, kafka.Message{
		Key:   keyBytes,
		Value: bMessage,
	})
	if err != nil {
		return fmt.Errorf("failed to write message: %v", err)
	}
	return nil
}

func convertKeyToBytes[K any](key K) ([]byte, error) {
	switch v := any(key).(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	case int, int32, int64, uint, uint32, uint64:
		return []byte(fmt.Sprintf("%v", v)), nil
	case nil:
		return nil, nil
	default:
		return json.Marshal(v)
	}
}

// Close закрывает соединение с Kafka
func (k *KafkaProducer) Close() error {
	return k.producer.Close()
}
