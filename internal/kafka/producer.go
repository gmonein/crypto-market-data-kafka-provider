package kafka

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond, // Low latency
        Async:        true,
	}
	return &Producer{writer: w}
}

func (p *Producer) WriteMessage(key, value []byte) error {
	return p.writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   key,
			Value: value,
		},
	)
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
