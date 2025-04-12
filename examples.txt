package kafka_lib

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Пример использования Producer
func ExampleProducer() {
	// Создаем конфигурацию Producer
	config := DefaultProducerConfig("localhost", "9092", "my-topic")

	// При необходимости настраиваем дополнительные параметры
	config.BatchSize = 100
	config.Timeout = 5 * time.Second

	// Создаем Producer
	producer := NewProducer(config)
	defer producer.Close()

	// Создаем сообщение для отправки
	type MyMessage struct {
		ID   string `json:"id"`
		Name string `json:"name"`
		Time int64  `json:"time"`
	}

	msg := MyMessage{
		ID:   "123",
		Name: "Test Message",
		Time: time.Now().Unix(),
	}

	// Отправляем сообщение с ключом строкового типа
	ctx := context.Background()
	strKey := "message-key"

	// Пример отправки сообщения со строковым ключом
	err := ProduceMessage[MyMessage, string](producer, ctx, msg, strKey)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}

	// Пример отправки сообщения с ключом из байтов
	byteKey := []byte("byte-key")
	err = ProduceMessage[MyMessage, []byte](producer, ctx, msg, byteKey)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}

	// Пример отправки с числовым ключом
	intKey := 12345
	err = ProduceMessage[MyMessage, int](producer, ctx, msg, intKey)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}

	// Пример отправки без ключа
	var nilKey *string
	err = ProduceMessage[MyMessage, *string](producer, ctx, msg, nilKey)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}

	fmt.Println("Message sent successfully")
}

// Пример использования Consumer
func ExampleConsumer() {
	// Создаем метрики
	metrics := NewMetrics()

	// Создаем конфигурацию
	config := DefaultConsumerConfig("localhost", "9092", "my-topic", "my-group")

	// Создаем Consumer
	consumer, err := NewConsumer(config, metrics)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}

	// Создаем контекст с возможностью отмены для контроля жизненного цикла
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Регистрируем обработчик сообщений
	consumer.RegisterHandler(ctx, func(ctx context.Context, msg []byte) error {
		// Пример десериализации JSON сообщения
		var data map[string]interface{}
		if err := json.Unmarshal(msg, &data); err != nil {
			return fmt.Errorf("failed to parse JSON: %w", err)
		}

		// Обработка сообщения
		fmt.Printf("Received message: %v\n", data)
		return nil
	})

	fmt.Println("Consumer started")


	// Дополнительное время для завершения обработки
	time.Sleep(time.Second)
}
