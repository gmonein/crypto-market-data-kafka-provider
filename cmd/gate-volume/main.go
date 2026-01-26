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
	wsURL        = flag.String("ws-url", "wss://api.gateio.ws/ws/v4/", "Gate.io WebSocket URL")
	symbolFlag   = flag.String("symbol", symbols.FromEnv("BTCUSDT"), "Gate.io symbol (e.g. BTCUSDT)")
)

type gateTradeEnvelope struct {
	Time    int64           `json:"time"`
	TimeMS  int64           `json:"time_ms"`
	Channel string          `json:"channel"`
	Event   string          `json:"event"`
	Result  json.RawMessage `json:"result"`
}

type gateTrade struct {
	CreateTime   json.RawMessage `json:"create_time"`
	CreateTimeMS json.RawMessage `json:"create_time_ms"`
	Amount       json.RawMessage `json:"amount"`
}

func parseGateTrades(raw json.RawMessage) []gateTrade {
	if len(raw) == 0 {
		return nil
	}
	var trades []gateTrade
	if err := json.Unmarshal(raw, &trades); err == nil {
		return trades
	}
	var single gateTrade
	if err := json.Unmarshal(raw, &single); err == nil {
		return []gateTrade{single}
	}
	return nil
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

func tradeTimestampMs(trade gateTrade, fallback int64) int64 {
	if v, ok := rawToInt64(trade.CreateTimeMS); ok && v > 0 {
		return v
	}
	if sec, ok := rawToInt64(trade.CreateTime); ok && sec > 0 {
		return sec * 1000
	}
	return fallback
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
	subSymbol := symbols.GateSpotSymbol(symbolNorm)
	if subSymbol == "" {
		log.Fatal("unable to derive Gate.io symbol")
	}
	topic := *topicFlag
	if topic == "" {
		topic = "gate_" + symbolLower + "_volume"
	}

	producer := nats.NewProducer(brokers, topic)
	defer producer.Close()

	log.Printf("Starting Gate.io Volume Follower. Brokers: %v, Topic: %s, Symbol: %s", brokers, topic, symbolNorm)

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
			"time":    time.Now().Unix(),
			"channel": "spot.trades",
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

				var env gateTradeEnvelope
				if err := json.Unmarshal(message, &env); err != nil {
					continue
				}
				if env.Channel != "spot.trades" || env.Event != "update" {
					continue
				}

				trades := parseGateTrades(env.Result)
				if len(trades) == 0 {
					continue
				}

				fallback := env.TimeMS
				if fallback == 0 && env.Time > 0 {
					fallback = env.Time * 1000
				}
				if fallback == 0 {
					fallback = time.Now().UnixMilli()
				}

				for _, trade := range trades {
					v, ok := rawToFloat(trade.Amount)
					if !ok || v <= 0 {
						continue
					}
					tsMs := tradeTimestampMs(trade, fallback)
					if tsMs <= 0 {
						continue
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
					currentSum += v
					mu.Unlock()
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
