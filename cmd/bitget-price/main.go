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
	wsURL        = flag.String("ws-url", "wss://ws.bitget.com/v2/ws/public", "Bitget WebSocket URL")
	symbolFlag   = flag.String("symbol", symbols.FromEnv("BTCUSDT"), "Bitget symbol (e.g. BTCUSDT)")
)

type bitgetEnvelope struct {
	Action string `json:"action"`
	Arg    struct {
		InstType string `json:"instType"`
		Channel  string `json:"channel"`
		InstID   string `json:"instId"`
	} `json:"arg"`
	Data []struct {
		LastPr string `json:"lastPr"`
		Ts     string `json:"ts"`
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
		topic = symbols.DefaultTopic("bitget", symbolNorm, "bitget_btcusd")
	}
	subSymbol := symbols.BitgetSymbol(symbolNorm)

	producer := nats.NewProducer(brokers, topic)
	defer producer.Close()

	log.Printf("Starting Bitget Price Follower. Brokers: %v, Topic: %s, Symbol: %s", brokers, topic, symbolNorm)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	var mu sync.Mutex
	var lastPrice string
	var lastTime int64
	var hasNew bool

	// Emitter Routine (Throttle to 100Hz like others)
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
				// log.Printf("%s\n", b)
				if err := producer.WriteMessage(nil, b); err != nil {
					log.Printf("NATS publish error: %v", err)
				}
			case <-interrupt:
				return
			}
		}
	}()

	subscribe := func(conn *websocket.Conn) error {
		msg := map[string]any{
			"op": "subscribe",
			"args": []map[string]string{
				{
					"instType": "SPOT",
					"channel":  "ticker",
					"instId":   subSymbol,
				},
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

		// Ping routine
		go func() {
			pingTicker := time.NewTicker(20 * time.Second)
			defer pingTicker.Stop()
			for {
				select {
				case <-done:
					return
				case <-pingTicker.C:
					// Bitget supports "ping" string
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
					continue
				}

				var env bitgetEnvelope
				if err := json.Unmarshal(message, &env); err != nil {
					continue
				}

				if env.Action != "snapshot" && env.Action != "update" {
					continue
				}

				if len(env.Data) == 0 {
					continue
				}

				// Use the latest update
				d := env.Data[len(env.Data)-1]
				if d.LastPr == "" {
					continue
				}

				// Parse Timestamp
				ts, err := strconv.ParseInt(d.Ts, 10, 64)
				if err != nil {
					// Fallback if missing or invalid
					ts = time.Now().UnixMilli()
				}

				mu.Lock()
				lastPrice = d.LastPr
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
