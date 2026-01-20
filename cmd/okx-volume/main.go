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
	wsURL        = flag.String("ws-url", "wss://ws.okx.com:8443/ws/v5/business", "OKX WebSocket URL")
	instIDFlag   = flag.String("inst-id", "", "OKX instrument id (e.g. BTC-USDT)")
	symbolFlag   = flag.String("symbol", symbols.FromEnv("BTCUSDT"), "OKX symbol (e.g. BTCUSDT)")
)

type okxCandleEnvelope struct {
	Event string `json:"event"`
	Arg   struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	} `json:"arg"`
	Data [][]string `json:"data"`
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
		topic = "okx_" + symbols.Lower(symbolNorm) + "_volume"
	}

	producer := nats.NewProducer(brokers, topic)
	defer producer.Close()

	log.Printf("Starting OKX Volume Follower. Brokers: %v, Topic: %s, InstID: %s", brokers, topic, instID)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	var mu sync.Mutex
	var lastVolume string
	var lastTime int64

	subscribe := func(conn *websocket.Conn) error {
		msg := map[string]any{
			"op": "subscribe",
			"args": []map[string]string{
				{"channel": "candle1s", "instId": instID},
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
			pingTicker := time.NewTicker(20 * time.Second)
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
					v := lastVolume
					t := lastTime
					mu.Unlock()

					log.Printf("%s", lastVolume)
					if v != "" {
						out := models.VolumeOutput{Timestamp: t, Volume: v, Symbol: symbolNorm}
						b, _ := json.Marshal(out)
						producer.WriteMessage(nil, b)
					}
					continue
				}

				var env okxCandleEnvelope
				log.Printf("%s", message)
				if err := json.Unmarshal(message, &env); err != nil {
					continue
				}
				if env.Event != "" || env.Arg.Channel != "candle1s" || len(env.Data) == 0 {
					continue
				}
				// Candle array format (OKX): [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
				c := env.Data[0]
				if len(c) < 6 {
					continue
				}
				ts, err := strconv.ParseInt(c[0], 10, 64)
				if err != nil || ts <= 0 {
					continue
				}
				vol := c[5]
				if vol == "" {
					continue
				}

				mu.Lock()
				lastVolume = vol
				lastTime = ts
				mu.Unlock()

				out := models.VolumeOutput{Timestamp: ts, Volume: vol, Symbol: symbolNorm}
				b, err := json.Marshal(out)
				if err != nil {
					continue
				}
				if err := producer.WriteMessage(nil, b); err != nil {
					log.Printf("NATS publish error: %v", err)
				}
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
