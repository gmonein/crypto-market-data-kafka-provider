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

	"market_follower/internal/kafka"
	"market_follower/internal/models"
	"market_follower/internal/symbols"

	"github.com/gorilla/websocket"
)

var (
	kafkaBrokers = flag.String("brokers", "localhost:9092", "Kafka brokers")
	topicFlag    = flag.String("topic", "", "Kafka topic")
	wsURL        = flag.String("ws-url", "wss://ws.bitget.com/v2/ws/public", "Bitget WebSocket URL")
	symbolFlag   = flag.String("symbol", symbols.FromEnv("BTCUSDT"), "Bitget symbol (e.g. BTCUSDT)")
)

type bitgetTradeEnvelope struct {
	Action string `json:"action"`
	Arg    struct {
		InstType string `json:"instType"`
		Channel  string `json:"channel"`
		InstID   string `json:"instId"`
	} `json:"arg"`
	Data []struct {
		Ts    string `json:"ts"`
		Size  string `json:"size"`
		Price string `json:"price"`
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
	symbolLower := symbols.Lower(symbolNorm)
	topic := *topicFlag
	if topic == "" {
		if symbolNorm == "BTCUSDT" || symbolNorm == "BTCUSD" {
			topic = "bitget_volume"
		} else {
			topic = "bitget_" + symbolLower + "_volume"
		}
	}
	subSymbol := symbols.BitgetSymbol(symbolNorm)

	producer := kafka.NewProducer(brokers, topic)
	defer producer.Close()

	log.Printf("Starting Bitget Volume Follower. Brokers: %v, Topic: %s, Symbol: %s", brokers, topic, symbolNorm)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	var mu sync.Mutex
	var currentBucketSec int64 = -1
	var currentSum float64

	flush := func(sec int64, vol float64) {
		if sec <= 0 || vol <= 0 {
			return
		}
		// End-of-second timestamp (ms).
		ts := (sec+1)*1000 - 1
		out := models.VolumeOutput{
			Timestamp: ts,
			Volume:    strconv.FormatFloat(vol, 'f', -1, 64),
			Symbol:    symbolNorm,
		}
		b, err := json.Marshal(out)
		if err != nil {
			return
		}
		// log.Printf("%s\n", b)
		if err := producer.WriteMessage(nil, b); err != nil {
			log.Printf("Kafka write error: %v", err)
		}
	}

	subscribe := func(conn *websocket.Conn) error {
		msg := map[string]any{
			"op": "subscribe",
			"args": []map[string]string{
				{
					"instType": "SPOT",
					"channel":  "trade",
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
					continue
				}

				var env bitgetTradeEnvelope
				if err := json.Unmarshal(message, &env); err != nil {
					continue
				}

				if env.Action != "snapshot" && env.Action != "update" {
					continue
				}

				for _, d := range env.Data {
					if d.Ts == "" || d.Size == "" {
						continue
					}

					tsMs, err := strconv.ParseInt(d.Ts, 10, 64)
					if err != nil || tsMs <= 0 {
						continue
					}

					v, err := strconv.ParseFloat(d.Size, 64)
					if err != nil || v <= 0 {
						continue
					}

					sec := tsMs / 1000

					mu.Lock()
					if currentBucketSec < 0 {
						currentBucketSec = sec
					}
					if sec != currentBucketSec {
						if sec > currentBucketSec {
							flush(currentBucketSec, currentSum)
							currentBucketSec = sec
							currentSum = 0
						}
						// Ignore late trades for older buckets to keep stream linear
					}
					currentSum += v
					mu.Unlock()
				}
			}
		}()

		select {
		case <-interrupt:
			log.Println("Interrupt received, shutting down...")
			mu.Lock()
			flush(currentBucketSec, currentSum)
			mu.Unlock()
			conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			select {
			case <-done:
			case <-time.After(time.Second):
			}
			return
		case <-done:
			mu.Lock()
			flush(currentBucketSec, currentSum)
			mu.Unlock()
			log.Println("WebSocket closed, reconnecting...")
			time.Sleep(1 * time.Second)
		}
	}
}
