package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"market_follower/internal/nats"
	"market_follower/internal/models"
	"market_follower/internal/symbols"

	"github.com/gorilla/websocket"
)

var (
	kafkaBrokers = flag.String("brokers", "nats://localhost:4222", "NATS URLs")
	topicFlag    = flag.String("topic", "", "NATS subject")
	wsURL        = flag.String("ws-url", "wss://stream.bybit.com/v5/public/spot", "Bybit WebSocket URL")
	symbolFlag   = flag.String("symbol", symbols.FromEnv("BTCUSDT"), "Bybit symbol (e.g. BTCUSDT)")
)

type bybitTradeEnvelope struct {
	Op      string `json:"op"`
	Success bool   `json:"success"`
	RetMsg  string `json:"ret_msg"`

	Topic string `json:"topic"`
	Type  string `json:"type"`
	Data  []struct {
		TradeTime int64  `json:"T"`
		Price     string `json:"p"`
	} `json:"data"`
}

func main() {
	flag.Parse()
	brokers := strings.Split(*kafkaBrokers, ",")
	symbol := *symbolFlag
	if symbol == "" {
		log.Fatal("symbol is required")
	}
	symbolNorm := symbols.Normalize(symbol)
	topic := *topicFlag
	if topic == "" {
		topic = symbols.DefaultTopic("bybit", symbolNorm, "bybit_btcusd")
	}
	subSymbol := symbols.BybitSymbol(symbolNorm)

	producer := nats.NewProducer(brokers, topic)
	defer producer.Close()

	log.Printf("Starting Bybit Price Follower. Brokers: %v, Topic: %s, Symbol: %s", brokers, topic, symbolNorm)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	var mu sync.Mutex
	var lastPrice string
	var lastTime int64
	var hasNew bool

	emitDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		defer close(emitDone)

		for {
			select {
			case <-ticker.C:
				mu.Lock()
				if !hasNew {
					mu.Unlock()
					continue
				}
				p := lastPrice
				t := lastTime
				hasNew = false
				mu.Unlock()

				out := models.PriceOutput{Timestamp: t, Price: p, Symbol: symbolNorm}
				b, err := json.Marshal(out)
				if err != nil {
					continue
				}
				log.Printf("%s\n", b)
				if err := producer.WriteMessage(nil, b); err != nil {
					log.Printf("NATS publish error: %v", err)
				}
			}
		}
	}()

	subscribe := func(conn *websocket.Conn) error {
		msg := map[string]any{
			"op":   "subscribe",
			"args": []string{"publicTrade." + subSymbol},
		}
		return conn.WriteJSON(msg)
	}

	for {
		conn, _, err := websocket.DefaultDialer.Dial(*wsURL, nil)
		if err != nil {
			log.Printf("Error connecting to WebSocket: %v. Retrying in 5s...", err)
			select {
			case <-interrupt:
				return
			case <-time.After(5 * time.Second):
				continue
			}
		}

		if err := subscribe(conn); err != nil {
			log.Printf("Subscription error: %v", err)
			conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}

		done := make(chan struct{})

		go func() {
			pingTicker := time.NewTicker(1 * time.Second)
			defer pingTicker.Stop()
			for {
				select {
				case <-done:
					return
				case <-pingTicker.C:
					if err := conn.WriteJSON(map[string]string{"op": "ping"}); err != nil {
						conn.Close()
						return
					}
				}
			}
		}()

		go func() {
			defer close(done)
			defer conn.Close()

			for {
				conn.SetReadDeadline(time.Now().Add(30 * time.Second))
				_, message, err := conn.ReadMessage()
				if err != nil {
					log.Printf("Read error: %v", err)
					return
				}

				log.Printf("%s", message)
				var env bybitTradeEnvelope
				if err := json.Unmarshal(message, &env); err != nil {
					continue
				}

				if env.RetMsg == "pong" {
					mu.Lock()
					if lastPrice != "" {
						hasNew = true
					}
					mu.Unlock()
					continue
				}

				if env.Topic == "" || len(env.Data) == 0 {
					continue
				}

				// Can batch multiple trades. Use the last one.
				d := env.Data[len(env.Data)-1]
				if d.TradeTime <= 0 || d.Price == "" {
					continue
				}

				mu.Lock()
				lastPrice = d.Price
				lastTime = d.TradeTime
				hasNew = true
				mu.Unlock()
			}
		}()

		select {
		case <-interrupt:
			log.Println("Interrupt received, shutting down...")
			conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			select {
			case <-done:
			case <-time.After(time.Second):
			}
			return
		case <-done:
			log.Println("WebSocket closed, reconnecting...")
			time.Sleep(1 * time.Second)
		}
	}
}
