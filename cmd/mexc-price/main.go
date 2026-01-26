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
	kafkaBrokers = flag.String("brokers", "nats://localhost:4222", "NATS URLs")
	topicFlag    = flag.String("topic", "", "NATS subject")
	wsURL        = flag.String("ws-url", "wss://contract.mexc.com/edge", "MEXC contract WebSocket URL")
	symbolFlag   = flag.String("symbol", symbols.FromEnv("BTCUSDT"), "MEXC symbol (e.g. BTCUSDT)")
)

type mexcTickerEnvelope struct {
	Channel string `json:"channel"`
	Data    struct {
		Symbol    string          `json:"symbol"`
		LastPrice json.RawMessage `json:"lastPrice"`
		Bid1      json.RawMessage `json:"bid1"`
		Ask1      json.RawMessage `json:"ask1"`
		Timestamp json.RawMessage `json:"timestamp"`
	} `json:"data"`
	Ts json.RawMessage `json:"ts"`
}

func rawToString(raw json.RawMessage) (string, bool) {
	if len(raw) == 0 {
		return "", false
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s, true
	}
	var num json.Number
	if err := json.Unmarshal(raw, &num); err == nil {
		return num.String(), true
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil {
		return strconv.FormatFloat(f, 'f', -1, 64), true
	}
	return "", false
}

func rawToInt64(raw json.RawMessage) (int64, bool) {
	if len(raw) == 0 {
		return 0, false
	}
	var i int64
	if err := json.Unmarshal(raw, &i); err == nil {
		return i, true
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil {
		return int64(f), true
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil {
			return v, true
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return int64(f), true
		}
	}
	return 0, false
}

func main() {
	flag.Parse()
	brokers := strings.Split(*kafkaBrokers, ",")
	symbol := *symbolFlag
	if symbol == "" {
		log.Fatal("symbol is required")
	}
	symbolNorm := symbols.Normalize(symbol)
	subSymbol := symbols.MexcFuturesSymbol(symbolNorm)
	if subSymbol == "" {
		log.Fatal("unable to derive MEXC symbol")
	}
	topic := *topicFlag
	if topic == "" {
		topic = symbols.DefaultTopic("mexc", symbolNorm, "mexc_btcusd")
	}

	producer := nats.NewProducer(brokers, topic)
	defer producer.Close()

	log.Printf("Starting MEXC Price Follower. Brokers: %v, Topic: %s, Symbol: %s", brokers, topic, symbolNorm)

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
			"method": "sub.ticker",
			"param":  map[string]string{"symbol": subSymbol},
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
					if err := conn.WriteJSON(map[string]string{"method": "ping"}); err != nil {
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

				var env mexcTickerEnvelope
				if err := json.Unmarshal(message, &env); err != nil {
					continue
				}

				if env.Channel != "push.ticker" {
					continue
				}
				if env.Data.Symbol != "" && env.Data.Symbol != subSymbol {
					continue
				}

				price, ok := rawToString(env.Data.LastPrice)
				if !ok || price == "" {
					continue
				}
				bid, _ := rawToString(env.Data.Bid1)
				ask, _ := rawToString(env.Data.Ask1)

				ts, _ := rawToInt64(env.Data.Timestamp)
				if ts == 0 {
					ts, _ = rawToInt64(env.Ts)
				}
				if ts == 0 {
					ts = time.Now().UnixMilli()
				}

				mu.Lock()
				lastPrice = price
				lastBid = bid
				lastAsk = ask
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
