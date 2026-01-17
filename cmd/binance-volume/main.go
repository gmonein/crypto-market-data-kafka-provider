package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
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
	topic := *topicFlag
	symbolNorm := symbols.Normalize(symbol)
	symbolLower := symbols.Lower(symbolNorm)
	if topic == "" {
		if symbolNorm == "BTCUSDT" || symbolNorm == "BTCUSD" {
			topic = "binance_volume"
		} else {
			topic = fmt.Sprintf("binance_%s_volume", symbolLower)
		}
	}
	streamSymbol := symbols.BinanceStreamSymbol(symbolNorm)
	wsURL := fmt.Sprintf("wss://stream.binance.com:9443/ws/%s@kline_1s", streamSymbol)

	producer := kafka.NewProducer(brokers, topic)
	defer producer.Close()

	log.Printf("Starting Binance Volume Follower. Brokers: %v, Topic: %s, Symbol: %s", brokers, topic, symbol)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	for {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("Error connecting to WebSocket: %v. Retrying in 5s...", err)
			time.Sleep(5 * time.Second)
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

				var kline models.BinanceKline
				if err := json.Unmarshal(message, &kline); err != nil {
					log.Printf("Unmarshal error: %v", err)
					continue
				}

				output := models.VolumeOutput{
					Timestamp: kline.Kline.CloseTime,
					Volume:    kline.Kline.Volume.String(),
					Symbol:    symbolNorm,
				}

				outputBytes, err := json.Marshal(output)
				if err != nil {
					log.Printf("Marshal output error: %v", err)
					continue
				}

				err = producer.WriteMessage(nil, outputBytes)
				if err != nil {
					log.Printf("Kafka write error: %v", err)
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
