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

	"market_follower/internal/models"
	"market_follower/internal/nats"
	"market_follower/internal/symbols"

	"github.com/gorilla/websocket"
)

var (
	kafkaBrokers = flag.String("brokers", "nats://localhost:4222", "NATS URLs")
	topicFlag    = flag.String("topic", "", "NATS subject")
	wsURL        = flag.String("ws-url", "wss://api.gateio.ws/ws/v4/", "Gate.io WebSocket URL")
	symbolFlag   = flag.String("symbol", symbols.FromEnv("BTCUSDT"), "Gate.io symbol (e.g. BTCUSDT)")
)

type gateTickerEnvelope struct {
	Time    int64           `json:"time"`
	TimeMS  int64           `json:"time_ms"`
	Channel string          `json:"channel"`
	Event   string          `json:"event"`
	Result  json.RawMessage `json:"result"`
}

type gateTicker struct {
	CurrencyPair string `json:"currency_pair"`
	Last         string `json:"last"`
	LowestAsk    string `json:"lowest_ask"`
	HighestBid   string `json:"highest_bid"`
}

func parseGateTicker(raw json.RawMessage) (gateTicker, bool) {
	if len(raw) == 0 {
		return gateTicker{}, false
	}
	var ticker gateTicker
	if err := json.Unmarshal(raw, &ticker); err == nil {
		return ticker, true
	}
	var list []gateTicker
	if err := json.Unmarshal(raw, &list); err != nil || len(list) == 0 {
		return gateTicker{}, false
	}
	return list[0], true
}

func main() {
	flag.Parse()
	brokers := strings.Split(*kafkaBrokers, ",")
	symbol := *symbolFlag
	if symbol == "" {
		log.Fatal("symbol is required")
	}
	symbolNorm := symbols.Normalize(symbol)
	subSymbol := symbols.GateSpotSymbol(symbolNorm)
	if subSymbol == "" {
		log.Fatal("unable to derive Gate.io symbol")
	}
	topic := *topicFlag
	if topic == "" {
		topic = symbols.DefaultTopic("gate", symbolNorm, "gate_btcusd")
	}

	producer := nats.NewProducer(brokers, topic)
	defer producer.Close()

	log.Printf("Starting Gate.io Price Follower. Brokers: %v, Topic: %s, Symbol: %s", brokers, topic, symbolNorm)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	var mu sync.Mutex
	var lastPrice string
	var lastBid string
	var lastAsk string
	var lastTime int64
	var hasNew bool

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
				b := lastBid
				a := lastAsk
				t := lastTime
				hasNew = false
				mu.Unlock()

				out := models.PriceOutput{
					Timestamp: t,
					Price:     p,
					BestBid:   b,
					BestAsk:   a,
					Symbol:    symbolNorm,
				}
				bts, err := json.Marshal(out)
				if err != nil {
					continue
				}
				if err := producer.WriteMessage(nil, bts); err != nil {
					log.Printf("NATS publish error: %v", err)
				}
			case <-interrupt:
				return
			}
		}
	}()

	subscribe := func(conn *websocket.Conn) error {
		msg := map[string]any{
			"time":    time.Now().Unix(),
			"channel": "spot.tickers",
			"event":   "subscribe",
			"payload": []string{subSymbol},
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
			defer close(done)
			defer conn.Close()

			for {
				_, message, err := conn.ReadMessage()
				if err != nil {
					log.Printf("Read error: %v", err)
					return
				}

				var env gateTickerEnvelope
				if err := json.Unmarshal(message, &env); err != nil {
					continue
				}

				if env.Channel != "spot.tickers" || env.Event != "update" {
					continue
				}

				ticker, ok := parseGateTicker(env.Result)
				if !ok || ticker.Last == "" {
					continue
				}
				if ticker.CurrencyPair != "" && ticker.CurrencyPair != subSymbol {
					continue
				}

				ts := env.TimeMS
				if ts == 0 && env.Time > 0 {
					ts = env.Time * 1000
				}
				if ts == 0 {
					ts = time.Now().UnixMilli()
				}

				mu.Lock()
				lastPrice = ticker.Last
				lastBid = ticker.HighestBid
				lastAsk = ticker.LowestAsk
				lastTime = ts
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
