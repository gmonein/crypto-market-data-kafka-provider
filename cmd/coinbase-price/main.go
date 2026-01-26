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

	"market_follower/internal/models"
	"market_follower/internal/nats"
	"market_follower/internal/symbols"

	"github.com/gorilla/websocket"
)

var (
	kafkaBrokers  = flag.String("brokers", "nats://localhost:4222", "NATS URLs")
	topicFlag     = flag.String("topic", "", "NATS subject")
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
	Timestamp string          `json:timestamp"`
	Events    json.RawMessage `json:"events"`
}

type TickerEvent struct {
	Type    string `json:"type"`
	Tickers []struct {
		Type            string `json:"type"`
		ProductID       string `json:"product_id"`
		Price           string `json:"price"`
		BestBid         string `json:"best_bid"`
		BestAsk         string `json:"best_ask"`
		BestBidQuantity string `json:"best_bid_quantity"`
		BestAskQuantity string `json:"best_ask_quantity"`
	} `json:"tickers"`
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
		topic = symbols.DefaultTopic("coinbase", symbolNorm, "coinbase_btcusd")
	}

	producer := nats.NewProducer(brokers, topic)
	defer producer.Close()

	log.Printf("Starting Coinbase Price Follower. Brokers: %v, Topic: %s, Product: %s", brokers, topic, productID)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	var mu sync.Mutex
	// State
	var lastPrice string
	var lastBestBid string
	var lastBestAsk string
	var lastBestBidQuantity string
	var lastBestAskQuantity string
	var lastTime int64

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

		// Subscribe to Ticker
		subTicker := SubscribeMessage{
			Type:       "subscribe",
			ProductIDs: []string{productID},
			Channel:    "ticker",
		}
		if err := conn.WriteJSON(subTicker); err != nil {
			log.Printf("Subscribe Ticker error: %v", err)
			conn.Close()
			continue
		}

		done := make(chan struct{})

		go func() {
			defer close(done)

			sendToNats := func(p, bb, ba, bq, aq string, t int64) {
				if p == "" {
					return
				}
				out := models.PriceOutput{
					Timestamp:       t,
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

				if baseMsg.Channel == "ticker" {
					var events []TickerEvent
					if err := json.Unmarshal(baseMsg.Events, &events); err != nil {
						continue
					}
					if len(events) > 0 && len(events[0].Tickers) > 0 {
						t := events[0].Tickers[0]
						if t.BestBid != "" && t.BestAsk != "" {
							// Calculate weighted midpoint using bid/ask sizes when available.
							bb, errBid := strconv.ParseFloat(t.BestBid, 64)
							ba, errAsk := strconv.ParseFloat(t.BestAsk, 64)
							if errBid != nil || errAsk != nil || bb <= 0 || ba <= 0 {
								continue
							}
							bq, _ := strconv.ParseFloat(t.BestBidQuantity, 64)
							aq, _ := strconv.ParseFloat(t.BestAskQuantity, 64)

							val := (bb + ba) / 2
							if (bq + aq) > 0 {
								val = (bb*aq + ba*bq) / (bq + aq)
							}
							calcPrice := strconv.FormatFloat(val, 'f', 2, 64)

							// Parse timestamp
							var currentTs int64
							parsedTime, err := time.Parse(time.RFC3339Nano, baseMsg.Timestamp)
							if err == nil {
								currentTs = parsedTime.UnixMilli()
							} else {
								currentTs = time.Now().UnixMilli()
							}

							mu.Lock()
							lastPrice = calcPrice
							lastBestBid = t.BestBid
							lastBestAsk = t.BestAsk
							lastBestBidQuantity = t.BestBidQuantity
							lastBestAskQuantity = t.BestAskQuantity
							lastTime = currentTs
							mu.Unlock()

							// Emit on Ticker
							sendToNats(calcPrice, t.BestBid, t.BestAsk, t.BestBidQuantity, t.BestAskQuantity, currentTs)
						}
					}
				} else if baseMsg.Channel == "heartbeat" {
					// Emit on Heartbeat (Sync)
					mu.Lock()
					p := lastPrice
					bb := lastBestBid
					ba := lastBestAsk
					bq := lastBestBidQuantity
					aq := lastBestAskQuantity
					t := lastTime
					mu.Unlock()

					sendToNats(p, bb, ba, bq, aq, t)
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
