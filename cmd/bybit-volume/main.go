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
		Volume    string `json:"v"`
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
			topic = "bybit_volume"
		} else {
			topic = "bybit_" + symbolLower + "_volume"
		}
	}
	subSymbol := symbols.BybitSymbol(symbolNorm)

	producer := kafka.NewProducer(brokers, topic)
	defer producer.Close()

	log.Printf("Starting Bybit Volume Follower. Brokers: %v, Topic: %s, Symbol: %s", brokers, topic, symbolNorm)

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
		log.Printf("%s\n", b)
		if err := producer.WriteMessage(nil, b); err != nil {
			log.Printf("Kafka write error: %v", err)
		}
	}

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
			pingTicker := time.NewTicker(20 * time.Second)
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

				var env bybitTradeEnvelope
				if err := json.Unmarshal(message, &env); err != nil {
					continue
				}

				if env.RetMsg == "pong" {
					continue
				}

				if env.Topic == "" || len(env.Data) == 0 {
					continue
				}

				for _, d := range env.Data {
					if d.TradeTime <= 0 || d.Volume == "" {
						continue
					}
					v, err := strconv.ParseFloat(d.Volume, 64)
					if err != nil || v <= 0 {
						continue
					}

					sec := d.TradeTime / 1000

					mu.Lock()
					if currentBucketSec < 0 {
						currentBucketSec = sec
					}
					if sec != currentBucketSec {
						flush(currentBucketSec, currentSum)
						currentBucketSec = sec
						currentSum = 0
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
