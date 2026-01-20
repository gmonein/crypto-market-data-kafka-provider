package nats

import (
	"log"
	"strings"
	"time"

	nats "github.com/nats-io/nats.go"
)

type Producer struct {
	conn    *nats.Conn
	subject string
}

func NewProducer(brokers []string, topic string) *Producer {
	serverList := strings.Join(brokers, ",")
	for {
		conn, err := nats.Connect(
			serverList,
			nats.Name("pm-market-data-producer"),
			nats.MaxReconnects(-1),
			nats.ReconnectWait(500*time.Millisecond),
		)
		if err == nil {
			return &Producer{conn: conn, subject: topic}
		}
		log.Printf("nats connect error: %v (retrying)", err)
		time.Sleep(time.Second)
	}
}

func (p *Producer) WriteMessage(key, value []byte) error {
	_ = key
	if p.conn == nil {
		return nats.ErrConnectionClosed
	}
	return p.conn.Publish(p.subject, value)
}

func (p *Producer) Close() error {
	if p.conn == nil {
		return nil
	}
	if err := p.conn.Drain(); err != nil {
		p.conn.Close()
		return err
	}
	return nil
}
