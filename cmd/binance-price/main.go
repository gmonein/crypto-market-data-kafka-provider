package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
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
	symbolFlag   = flag.String("symbol", symbols.FromEnv("BTCUSDT"), "Binance symbol (e.g. BTCUSDT)")
)

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
		topic = symbols.DefaultTopic("binance", symbolNorm, "binance_btcusd")
	}
	streamSymbol := symbols.BinanceStreamSymbol(symbolNorm)
	wsURL := fmt.Sprintf("wss://stream.binance.com:9443/ws/%s@trade", streamSymbol)

	producer := kafka.NewProducer(brokers, topic)
	defer producer.Close()

	log.Printf("Starting Binance Price Follower. Brokers: %v, Topic: %s, Symbol: %s", brokers, topic, symbolNorm)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	// Throttling State
	var mu sync.Mutex
	var lastPrice string
	var lastTime int64
	var hasNew bool

	// Emitter Routine
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

				output := models.PriceOutput{
					Timestamp: t,
					Price:     p,
					Symbol:    symbolNorm,
				}

				outputBytes, err := json.Marshal(output)
				if err != nil {
					continue
				}

				err = producer.WriteMessage(nil, outputBytes)
				if err != nil {
					log.Printf("Kafka write error: %v", err)
				}
			case <-interrupt: // Stop if interrupt triggered (shared channel logic needs care)
				// We'll handle shutdown via main loop return
				return
			}
		}
	}()

	for {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("Error connecting to WebSocket: %v. Retrying in 5s...", err)
			select {
			case <-interrupt:
				return
			case <-time.After(5 * time.Second):
				continue
			}
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

				var trade models.BinanceTrade
				if err := json.Unmarshal(message, &trade); err != nil {
					continue
				}

				mu.Lock()
				lastPrice = trade.Price
				lastTime = trade.TradeTime
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
