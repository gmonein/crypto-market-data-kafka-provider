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
	wsURL        = "wss://ws.kraken.com"
	pairFlag     = flag.String("pair", "", "Kraken pair (e.g. XBT/USD)")
	symbolFlag   = flag.String("symbol", symbols.FromEnv("BTCUSDT"), "Kraken symbol (e.g. BTCUSDT)")
)

type SystemEvent struct {
	Event string `json:"event"`
}

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
		topic = symbols.DefaultTopic("kraken", symbolNorm, "kraken_btcusd")
	}

	producer := nats.NewProducer(brokers, topic)
	defer producer.Close()

	log.Printf("Starting Kraken Price Follower. Brokers: %v, Topic: %s, Pair: %s", brokers, topic, pair)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	var mu sync.Mutex
	var lastPrice string
	var lastBestBid string
	var lastBestAsk string
	var lastBestBidQuantity string
	var lastBestAskQuantity string
	var hasNew bool

	// Emitter Loop (Throttle to 10ms/100Hz)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-interrupt:
				return
			case <-ticker.C:
				mu.Lock()
				if !hasNew {
					mu.Unlock()
					continue
				}
				p := lastPrice
				bb := lastBestBid
				ba := lastBestAsk
				bq := lastBestBidQuantity
				aq := lastBestAskQuantity
				hasNew = false
				mu.Unlock()

				ts := time.Now().UnixMilli()

				out := models.PriceOutput{
					Timestamp:       ts,
					Price:           p,
					BestBid:         bb,
					BestAsk:         ba,
					BestBidQuantity: bq,
					BestAskQuantity: aq,
					Symbol:          symbolNorm,
				}
				b, _ := json.Marshal(out)
				log.Printf("%s\n", b)
				producer.WriteMessage(nil, b)
			}
		}
	}()

	for {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("Dial error: %v. Retrying...", err)
			time.Sleep(5 * time.Second)
			continue
		}

		// Subscribe
		sub := map[string]interface{}{
			"event": "subscribe",
			"pair":  []string{pair},
			"subscription": map[string]string{
				"name": "ticker",
			},
		}
		if err := conn.WriteJSON(sub); err != nil {
			log.Printf("Subscribe error: %v", err)
			conn.Close()
			continue
		}

		done := make(chan struct{})

		go func() {
			defer close(done)
			for {
				_, message, err := conn.ReadMessage()
				if err != nil {
					log.Printf("Read error: %v", err)
					return
				}

				// Check for heartbeat/system messages first (usually objects)
				// Ticker data is an array: [channelID, {data}, channelName, pair]
				if len(message) > 0 && message[0] == '{' {
					var sys SystemEvent
					if err := json.Unmarshal(message, &sys); err == nil && sys.Event == "heartbeat" {
						mu.Lock()
						// Trigger emission if we have a valid state
						if lastPrice != "" {
							hasNew = true
						}
						mu.Unlock()
					}
					continue // Ignore system events
				}

				var dataArr []interface{}
				if err := json.Unmarshal(message, &dataArr); err != nil {
					continue
				}

				if len(dataArr) < 4 {
					continue
				}

				// The payload is usually at index 1
				payload, ok := dataArr[1].(map[string]interface{})
				if !ok {
					continue
				}

				// Parse "c" (Close/Price), "b" (Bid), "a" (Ask)
				// Format: "c": ["price", "lot_volume"]
				//         "b": ["price", "whole_lot_vol", "lot_vol"]

				var p, bb, ba, bq, aq string

				if c, ok := payload["c"].([]interface{}); ok && len(c) > 0 {
					p, _ = c[0].(string)
				}
				if b, ok := payload["b"].([]interface{}); ok && len(b) > 2 {
					bb, _ = b[0].(string)
					bq, _ = b[2].(string)
				}
				if a, ok := payload["a"].([]interface{}); ok && len(a) > 2 {
					ba, _ = a[0].(string)
					aq, _ = a[2].(string)
				}

				if p != "" {
					mu.Lock()
					lastPrice = p
					if bb != "" {
						lastBestBid = bb
						lastBestBidQuantity = bq
					}
					if ba != "" {
						lastBestAsk = ba
						lastBestAskQuantity = aq
					}
					hasNew = true
					mu.Unlock()
				}
			}
		}()

		select {
		case <-interrupt:
			log.Println("Shutting down...")
			conn.Close()
			return
		case <-done:
			conn.Close()
			time.Sleep(1 * time.Second)
		}
	}
}
