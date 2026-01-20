package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
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
	wsURL        = flag.String("ws-url", "wss://ws.okx.com:8443/ws/v5/public", "OKX WebSocket URL")
	instIDFlag   = flag.String("inst-id", "", "OKX instrument id (e.g. BTC-USDT)")
	symbolFlag   = flag.String("symbol", symbols.FromEnv("BTCUSDT"), "OKX symbol (e.g. BTCUSDT)")
)

type okxTradeEnvelope struct {
	Event string `json:"event"`
	Arg   struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	} `json:"arg"`
	Data []struct {
		Price     string `json:"px"`
		Timestamp string `json:"ts"`
	} `json:"data"`
}

func main() {
	flag.Parse()
	brokers := strings.Split(*kafkaBrokers, ",")
	symbol := *symbolFlag
	if symbol == "" && *instIDFlag == "" {
		log.Fatal("symbol or inst-id is required")
	}
	symbolNorm := symbols.Normalize(symbol)
	instID := *instIDFlag
	if instID == "" {
		instID = symbols.OkxInstID(symbolNorm)
	}
	if instID == "" {
		log.Fatal("unable to derive inst-id from symbol")
	}
	topic := *topicFlag
	if topic == "" {
		topic = symbols.DefaultTopic("okx", symbolNorm, "okx_btcusd")
	}

	producer := nats.NewProducer(brokers, topic)
	defer producer.Close()

	log.Printf("Starting OKX Price Follower. Brokers: %v, Topic: %s, InstID: %s", brokers, topic, instID)

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
				if err := producer.WriteMessage(nil, b); err != nil {
					log.Printf("NATS publish error: %v", err)
				}
			}
		}
	}()

	subscribe := func(conn *websocket.Conn) error {
		msg := map[string]any{
			"op": "subscribe",
			"args": []map[string]string{
				{"channel": "trades", "instId": instID},
			},
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
					if err := conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
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
				_, message, err := conn.ReadMessage()
				if err != nil {
					log.Printf("Read error: %v", err)
					return
				}

				if string(message) == "pong" {
					mu.Lock()
					p := lastPrice
					t := lastTime
					mu.Unlock()

					if p != "" {
						out := models.PriceOutput{Timestamp: t, Price: p, Symbol: symbolNorm}
						b, _ := json.Marshal(out)
						producer.WriteMessage(nil, b)
					}
					continue
				}

				var env okxTradeEnvelope
				if err := json.Unmarshal(message, &env); err != nil {
					continue
				}
				if env.Event != "" || env.Arg.Channel != "trades" || len(env.Data) == 0 {
					continue
				}

				// OKX can batch multiple trades. Use the last one.
				d := env.Data[len(env.Data)-1]
				if d.Price == "" || d.Timestamp == "" {
					continue
				}
				ts, err := strconv.ParseInt(d.Timestamp, 10, 64)
				if err != nil || ts <= 0 {
					continue
				}

				mu.Lock()
				lastPrice = d.Price
				lastTime = ts
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
