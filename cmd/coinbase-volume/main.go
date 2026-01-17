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
	kafkaBrokers  = flag.String("brokers", "localhost:9092", "Kafka brokers")
	topicFlag     = flag.String("topic", "", "Kafka topic")
	wsURL         = "wss://advanced-trade-ws.coinbase.com"
	productIDFlag = flag.String("product-id", "", "Coinbase product id (e.g. BTC-USD or BTC-USDT)")
	symbolFlag    = flag.String("symbol", symbols.FromEnv("BTCUSDT"), "Coinbase symbol (e.g. BTCUSDT)")
)

// Coinbase Advanced Trade WebSocket Messages

type SubscribeMessage struct {
	Type       string   `json:"type"`
	ProductIDs []string `json:"product_ids"`
	Channel    string   `json:"channel"`
}

type GenericMessage struct {
	Channel   string          `json:"channel"`
	Timestamp string          `json:"timestamp"`
	Events    json.RawMessage `json:"events"`
}

type TradeEvent struct {
	Type   string `json:"type"`
	Trades []struct {
		TradeID   string `json:"trade_id"`
		ProductID string `json:"product_id"`
		Price     string `json:"price"`
		Size      string `json:"size"`
		Side      string `json:"side"`
		Time      string `json:"time"` // RFC3339
	} `json:"trades"`
}

func main() {
	flag.Parse()
	brokers := strings.Split(*kafkaBrokers, ",")
	symbol := *symbolFlag
	if symbol == "" && *productIDFlag == "" {
		log.Fatal("symbol or product-id is required")
	}
	symbolNorm := symbols.Normalize(symbol)
	productID := strings.TrimSpace(os.Getenv("COINBASE_PRODUCT_ID"))
	if *productIDFlag != "" {
		productID = *productIDFlag
	}
	if productID == "" {
		productID = symbols.CoinbaseProductID(symbolNorm)
	}
	if productID == "" {
		log.Fatal("unable to derive product-id from symbol")
	}
	topic := *topicFlag
	if topic == "" {
		if symbolNorm == "BTCUSDT" || symbolNorm == "BTCUSD" {
			topic = "coinbase_volume"
		} else {
			topic = "coinbase_" + symbols.Lower(symbolNorm) + "_volume"
		}
	}

	producer := kafka.NewProducer(brokers, topic)
	defer producer.Close()

	log.Printf("Starting Coinbase Volume Follower. Brokers: %v, Topic: %s, Product: %s", brokers, topic, productID)

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
			log.Printf("Dial error: %v. Retrying in 5s...", err)
			select {
			case <-interrupt:
				return
			case <-time.After(5 * time.Second):
				continue
			}
		}

		// Subscribe to Market Trades
		subMsg := SubscribeMessage{
			Type:       "subscribe",
			ProductIDs: []string{productID},
			Channel:    "market_trades",
		}
		if err := conn.WriteJSON(subMsg); err != nil {
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

				var baseMsg GenericMessage
				if err := json.Unmarshal(message, &baseMsg); err != nil {
					continue
				}

				if baseMsg.Channel == "market_trades" {
					var events []TradeEvent
					if err := json.Unmarshal(baseMsg.Events, &events); err != nil {
						continue
					}

					for _, e := range events {
						for _, t := range e.Trades {
							if t.Time == "" || t.Size == "" {
								continue
							}

							parsedTime, err := time.Parse(time.RFC3339Nano, t.Time)
							if err != nil {
								// Try standard RFC3339 if Nano fails
								parsedTime, err = time.Parse(time.RFC3339, t.Time)
								if err != nil {
									continue
								}
							}

							v, err := strconv.ParseFloat(t.Size, 64)
							if err != nil || v <= 0 {
								continue
							}

							sec := parsedTime.Unix()

							mu.Lock()
							if currentBucketSec < 0 {
								currentBucketSec = sec
							}
							if sec != currentBucketSec {
								// Time moved forward (or backward, but we assume forward for live stream)
								// Flush previous bucket if it's strictly older.
								// Note: Advanced Trade might send trades slightly out of order or batched.
								// Simple approach: if sec changes, flush old.
								// Better approach for out-of-order: strict > check.
								// Given this is a follower, we'll assume roughly chronological.
								// If we receive a trade for the SAME second, we add to it.
								// If we receive a NEW second, we flush the OLD one.
								// If we receive an OLDER second (unlikely but possible), we might ignore or just log.
								// For simplicity and matching bybit-volume logic:
								if sec > currentBucketSec {
									flush(currentBucketSec, currentSum)
									currentBucketSec = sec
									currentSum = 0
								} else if sec < currentBucketSec {
									// Late arrival for previous bucket.
									// We already flushed it. Ignoring or handling would require buffering.
									// Bybit-volume implementation just resets.
									// We'll just ignore late trades to keep it simple and linear.
									mu.Unlock()
									continue
								}
							}
							currentSum += v
							mu.Unlock()
						}
					}
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
