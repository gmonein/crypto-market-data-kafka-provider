package main

import (
	"encoding/json"
	"flag"
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
	symbolFlag   = flag.String("symbol", symbols.FromEnv("BTCUSDT"), "Chainlink symbol (e.g. BTCUSDT)")
	wsURL        = "wss://ws-live-data.polymarket.com"
)

type SubscriptionMsg struct {
	Action        string               `json:"action"`
	Subscriptions []SubscriptionDetail `json:"subscriptions"`
}

type SubscriptionDetail struct {
	Topic   string `json:"topic"`
	Type    string `json:"type"`
	Filters string `json:"filters"`
}

type PolymarketResponse struct {
	Payload struct {
		Data interface{} `json:"data"` // Can be array or object
	} `json:"payload"`
}

func main() {
	flag.Parse()
	brokers := strings.Split(*kafkaBrokers, ",")
	symbol := *symbolFlag
	if symbol == "" {
		log.Fatal("symbol is required")
	}
	symbolNorm := symbols.Normalize(symbol)
	chainlinkSymbol := symbols.ChainlinkSymbol(symbolNorm)
	if chainlinkSymbol == "" {
		log.Fatal("unable to derive chainlink symbol from input")
	}
	topic := *topicFlag
	if topic == "" {
		topic = symbols.DefaultTopic("chainlink", symbolNorm, "chainlink_btcusd")
	}

	producer := kafka.NewProducer(brokers, topic)
	defer producer.Close()

	log.Printf("Starting Chainlink Price Follower. Brokers: %v, Topic: %s, Symbol: %s", brokers, topic, chainlinkSymbol)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	for {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("Error connecting to WebSocket: %v. Retrying in 5s...", err)
			time.Sleep(5 * time.Second)
			continue
		}

		// Subscribe
		// Ruby: filters: { symbol: @symbol }.to_json
		// We emulate this by sending a JSON string as the value of "filters"
		filterJSON := `{"symbol":"` + chainlinkSymbol + `"}`

		subMsg := SubscriptionMsg{
			Action: "subscribe",
			Subscriptions: []SubscriptionDetail{
				{
					Topic:   "crypto_prices_chainlink",
					Type:    "*",
					Filters: filterJSON,
				},
			},
		}

		if err := conn.WriteJSON(subMsg); err != nil {
			log.Printf("Subscription error: %v", err)
			conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}

		done := make(chan struct{})

		// Set initial deadline
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))

		// Set Pong Handler
		conn.SetPongHandler(func(string) error {
			// log.Printf("Received PONG")
			return nil
		})

		// Start Pinger (Keep-Alive)
		go func() {
			pingTicker := time.NewTicker(5 * time.Second)
			defer pingTicker.Stop()
			for {
				select {
				case <-done:
					return
				case <-pingTicker.C:
					if err := conn.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
						log.Printf("Ping write error: %v", err)
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
					log.Printf("Read error (likely timeout): %v", err)
					return
				}

				// Check for keepalive/empty messages if any (Polymarket sometimes sends weird stuff)
				if len(message) == 0 {
					continue
				}

				// Handle multi-line messages and extract payload/data, like Ruby
				lines := strings.Split(string(message), "\n")

				for _, lineStr := range lines {
					// log.Printf("%v", lineStr)
					if lineStr == "" {
						continue
					}

					var rawMsg struct {
						Payload json.RawMessage `json:"payload"`
					}

					if err := json.Unmarshal([]byte(lineStr), &rawMsg); err != nil {

						continue

					}

					// Strategy: Try to parse Payload as "Wrapped Data" first, then as "Direct Data"

					// 1. Check for "data" field inside payload

					var wrapped struct {
						Data json.RawMessage `json:"data"`
					}

					var prices []models.ChainlinkPrice

					// Try unmarshal into wrapper

					if err := json.Unmarshal(rawMsg.Payload, &wrapped); err == nil && len(wrapped.Data) > 0 {

						// It has a "data" field. Parse that.

						if err := json.Unmarshal(wrapped.Data, &prices); err != nil {

							// Maybe single object in data?

							var p models.ChainlinkPrice

							if err2 := json.Unmarshal(wrapped.Data, &p); err2 == nil {

								prices = append(prices, p)

							}

						}

					} else {

						// 2. No "data" field? Try parsing payload directly as the price object

						var p models.ChainlinkPrice

						if err := json.Unmarshal(rawMsg.Payload, &p); err == nil && p.Timestamp != 0 {

							prices = append(prices, p)

						}

					}

					type ChainlinkPriceOutput struct {
						Value     float64 `json:"value"`
						Timestamp int64   `json:"timestamp"`
						Symbol    string  `json:"s"`
					}

					for _, p := range prices {
						output := ChainlinkPriceOutput{
							Value:     p.Value,
							Timestamp: p.Timestamp,
							Symbol:    symbolNorm,
						}
						b, _ := json.Marshal(output)
						// log.Printf("Sent: %f", p.Value)
						producer.WriteMessage(nil, b)
						conn.SetReadDeadline(time.Now().Add(5 * time.Second))
					}
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
