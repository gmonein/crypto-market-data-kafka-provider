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

type kucoinExecutionData struct {
	MatchSize json.RawMessage `json:"matchSize"`
	Size      json.RawMessage `json:"size"`
	Qty       json.RawMessage `json:"qty"`
	Time      json.RawMessage `json:"time"`
	Ts        json.RawMessage `json:"ts"`
}

func rawToFloat(raw json.RawMessage) (float64, bool) {
	if len(raw) == 0 {
		return 0, false
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil {
		return f, true
	}
	var num json.Number
	if err := json.Unmarshal(raw, &num); err == nil {
		if v, err := num.Float64(); err == nil {
			return v, true
		}
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		if v, err := strconv.ParseFloat(s, 64); err == nil {
			return v, true
		}
	}
	return 0, false
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
	symbolLower := symbols.Lower(symbolNorm)
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
		topic = "kucoinfutures_" + symbolLower + "_volume"
	}

	producer := nats.NewProducer(brokers, topic)
	defer producer.Close()

	log.Printf("Starting KuCoin Futures Volume Follower. Brokers: %v, Topic: %s, Symbol: %s, Contract: %s", brokers, topic, symbolNorm, contract)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	var mu sync.Mutex
	var currentBucketSec int64 = -1
	var currentSum float64

	flush := func(sec int64, vol float64) {
		if sec <= 0 || vol <= 0 {
			return
		}
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
		if err := producer.WriteMessage(nil, b); err != nil {
			log.Printf("NATS publish error: %v", err)
		}
	}

	subscribe := func(conn *websocket.Conn) error {
		msg := map[string]any{
			"id":       time.Now().UnixNano(),
			"type":     "subscribe",
			"topic":    "/contractMarket/execution:" + contract,
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
				if env.Type != "message" || env.Subject != "match" {
					continue
				}

				var data kucoinExecutionData
				if err := json.Unmarshal(env.Data, &data); err != nil {
					continue
				}

				vol, ok := rawToFloat(data.MatchSize)
				if !ok || vol <= 0 {
					vol, ok = rawToFloat(data.Size)
				}
				if !ok || vol <= 0 {
					vol, ok = rawToFloat(data.Qty)
				}
				if !ok || vol <= 0 {
					continue
				}
				tsRaw, _ := rawToInt64(data.Time)
				if tsRaw == 0 {
					tsRaw, _ = rawToInt64(data.Ts)
				}
				tsMs := normalizeMillis(tsRaw)
				if tsMs == 0 {
					tsMs = time.Now().UnixMilli()
				}
				sec := tsMs / 1000

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
					mu.Unlock()
					continue
				}
				currentSum += vol
				mu.Unlock()
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
