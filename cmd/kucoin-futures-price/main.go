package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"market_follower/internal/kucoin"
	"market_follower/internal/models"
	"market_follower/internal/nats"
	"market_follower/internal/symbols"

	"github.com/gorilla/websocket"
)

var (
	kafkaBrokers = flag.String("brokers", "nats://localhost:4222", "NATS URLs")
	topicFlag    = flag.String("topic", "", "NATS subject")
	restURL      = flag.String("rest-url", "https://api-futures.kucoin.com", "KuCoin Futures REST base URL")
	symbolFlag   = flag.String("symbol", symbols.FromEnv("BTCUSDT"), "KuCoin Futures symbol (e.g. BTCUSDT)")
	contractFlag = flag.String("contract", "", "KuCoin Futures contract symbol (e.g. XBTUSDTM)")
)

type kucoinEnvelope struct {
	Type    string          `json:"type"`
	Topic   string          `json:"topic"`
	Subject string          `json:"subject"`
	Data    json.RawMessage `json:"data"`
}

type kucoinTickerData struct {
	Price        json.RawMessage `json:"price"`
	BestBidPrice json.RawMessage `json:"bestBidPrice"`
	BestAskPrice json.RawMessage `json:"bestAskPrice"`
	BestBidSize  json.RawMessage `json:"bestBidSize"`
	BestAskSize  json.RawMessage `json:"bestAskSize"`
	Ts           json.RawMessage `json:"ts"`
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

func normalizeMillis(v int64) int64 {
	switch {
	case v > 1e15:
		return v / 1e6
	case v > 1e12:
		return v
	case v > 1e9:
		return v * 1000
	default:
		return v
	}
}

func main() {
	flag.Parse()
	brokers := strings.Split(*kafkaBrokers, ",")
	symbol := *symbolFlag
	if symbol == "" {
		log.Fatal("symbol is required")
	}
	symbolNorm := symbols.Normalize(symbol)
	contract := strings.TrimSpace(*contractFlag)
	if contract == "" {
		contract = strings.TrimSpace(os.Getenv("KUCOIN_FUTURES_SYMBOL"))
	}
	if contract == "" {
		contract = symbols.KucoinFuturesSymbol(symbolNorm)
	}
	if contract == "" {
		log.Fatal("unable to derive KuCoin Futures contract symbol")
	}

	topic := *topicFlag
	if topic == "" {
		topic = symbols.DefaultTopic("kucoinfutures", symbolNorm, "kucoinfutures_btcusd")
	}

	producer := nats.NewProducer(brokers, topic)
	defer producer.Close()

	log.Printf("Starting KuCoin Futures Price Follower. Brokers: %v, Topic: %s, Symbol: %s, Contract: %s", brokers, topic, symbolNorm, contract)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	var mu sync.Mutex
	var lastPrice string
	var lastBid string
	var lastAsk string
	var lastBidQty string
	var lastAskQty string
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
				bq := lastBidQty
				aq := lastAskQty
				t := lastTime
				hasNew = false
				mu.Unlock()

				out := models.PriceOutput{
					Timestamp:       t,
					Price:           p,
					BestBid:         b,
					BestAsk:         a,
					BestBidQuantity: bq,
					BestAskQuantity: aq,
					Symbol:          symbolNorm,
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
			"id":       time.Now().UnixNano(),
			"type":     "subscribe",
			"topic":    "/contractMarket/ticker:" + contract,
			"response": true,
		}
		return conn.WriteJSON(msg)
	}

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		cfg, err := kucoin.FetchPublicConfig(ctx, *restURL)
		cancel()
		if err != nil {
			log.Printf("KuCoin futures token error: %v. Retrying in 5s...", err)
			select {
			case <-interrupt:
				return
			case <-time.After(5 * time.Second):
				continue
			}
		}

		conn, _, err := websocket.DefaultDialer.Dial(kucoin.WSURL(cfg), nil)
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
			interval := cfg.PingInterval
			if interval <= 0 {
				interval = 20 * time.Second
			}
			pingTicker := time.NewTicker(interval)
			defer pingTicker.Stop()
			for {
				select {
				case <-done:
					return
				case <-pingTicker.C:
					if err := conn.WriteJSON(map[string]any{"id": time.Now().UnixNano(), "type": "ping"}); err != nil {
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

				var env kucoinEnvelope
				if err := json.Unmarshal(message, &env); err != nil {
					continue
				}
				if env.Type != "message" || env.Subject != "ticker" {
					continue
				}

				var data kucoinTickerData
				if err := json.Unmarshal(env.Data, &data); err != nil {
					continue
				}

				price, ok := rawToString(data.Price)
				if !ok || price == "" {
					continue
				}
				bid, _ := rawToString(data.BestBidPrice)
				ask, _ := rawToString(data.BestAskPrice)
				bq, _ := rawToString(data.BestBidSize)
				aq, _ := rawToString(data.BestAskSize)

				tsRaw, _ := rawToInt64(data.Ts)
				ts := normalizeMillis(tsRaw)
				if ts == 0 {
					ts = time.Now().UnixMilli()
				}

				mu.Lock()
				lastPrice = price
				lastBid = bid
				lastAsk = ask
				lastBidQty = bq
				lastAskQty = aq
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
