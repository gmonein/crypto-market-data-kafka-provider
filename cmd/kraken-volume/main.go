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
	wsURL        = "wss://ws.kraken.com"
	pairFlag     = flag.String("pair", "", "Kraken pair (e.g. XBT/USD)")
	symbolFlag   = flag.String("symbol", symbols.FromEnv("BTCUSDT"), "Kraken symbol (e.g. BTCUSDT)")
)

func main() {
	flag.Parse()
	brokers := strings.Split(*kafkaBrokers, ",")
	symbol := *symbolFlag
	if symbol == "" && *pairFlag == "" {
		log.Fatal("symbol or pair is required")
	}
	symbolNorm := symbols.Normalize(symbol)
	pair := *pairFlag
	if pair == "" {
		pair = symbols.KrakenPair(symbolNorm)
	}
	if pair == "" {
		log.Fatal("unable to derive pair from symbol")
	}
	topic := *topicFlag
	if topic == "" {
		if symbolNorm == "BTCUSDT" || symbolNorm == "BTCUSD" {
			topic = "kraken_volume"
		} else {
			topic = "kraken_" + symbols.Lower(symbolNorm) + "_volume"
		}
	}

	producer := kafka.NewProducer(brokers, topic)
	defer producer.Close()

	log.Printf("Starting Kraken Volume Follower. Brokers: %v, Topic: %s, Pair: %s", brokers, topic, pair)

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

	for {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("Dial error: %v. Retrying...", err)
			select {
			case <-interrupt:
				return
			case <-time.After(5 * time.Second):
				continue
			}
		}

		// Subscribe to trades
		sub := map[string]interface{}{
			"event": "subscribe",
			"pair":  []string{pair},
			"subscription": map[string]string{
				"name": "trade",
			},
		}
		if err := conn.WriteJSON(sub); err != nil {
			log.Printf("Subscribe error: %v", err)
			conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}

		done := make(chan struct{})

		go func() {
			defer close(done)
			defer conn.Close()

			for {
				_, message, err := conn.ReadMessage()
				if err != nil {
					log.Printf("Read error: %v", err)
					return
				}

				// Check for heartbeat/system messages first (usually objects)
				// Array: [channelID, [[price, vol, time, side, type, misc], ...], channelName, pair]
				if len(message) > 0 && message[0] == '{' {
					continue
				}

				var dataArr []interface{}
				if err := json.Unmarshal(message, &dataArr); err != nil {
					continue
				}

				if len(dataArr) < 4 {
					continue
				}

				// Trade data is at index 1 (usually)
				// It is an array of arrays.
				trades, ok := dataArr[1].([]interface{})
				if !ok {
					continue
				}

				for _, tRaw := range trades {
					t, ok := tRaw.([]interface{})
					if !ok || len(t) < 3 {
						continue
					}

					// index 1: volume (string)
					// index 2: time (float string or float)

					volStr, ok1 := t[1].(string)
					timeVal, ok2 := t[2].(interface{})

					if !ok1 || !ok2 {
						continue
					}

					vol, err := strconv.ParseFloat(volStr, 64)
					if err != nil || vol <= 0 {
						continue
					}

					var sec int64
					switch v := timeVal.(type) {
					case string:
						f, err := strconv.ParseFloat(v, 64)
						if err == nil {
							sec = int64(f)
						}
					case float64:
						sec = int64(v)
					}

					if sec <= 0 {
						continue
					}

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
					}
					currentSum += vol
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
