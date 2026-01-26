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

type mexcDealEnvelope struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
	Ts      json.RawMessage `json:"ts"`
}

type mexcDeal struct {
	T json.RawMessage `json:"t"`
	V json.RawMessage `json:"v"`
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

func parseMexcDeal(raw json.RawMessage) (mexcDeal, bool) {
	if len(raw) == 0 {
		return mexcDeal{}, false
	}
	var deal mexcDeal
	if err := json.Unmarshal(raw, &deal); err == nil && (len(deal.V) > 0 || len(deal.T) > 0) {
		return deal, true
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err == nil && len(arr) >= 2 {
		deal.V = arr[1]
		if len(arr) >= 3 {
			deal.T = arr[2]
		}
		return deal, true
	}
	return mexcDeal{}, false
}

func parseMexcDeals(raw json.RawMessage) []mexcDeal {
	if len(raw) == 0 {
		return nil
	}
	var list []mexcDeal
	if err := json.Unmarshal(raw, &list); err == nil && len(list) > 0 {
		return list
	}
	var single mexcDeal
	if err := json.Unmarshal(raw, &single); err == nil && (len(single.V) > 0 || len(single.T) > 0) {
		return []mexcDeal{single}
	}
	var rawList []json.RawMessage
	if err := json.Unmarshal(raw, &rawList); err == nil && len(rawList) > 0 {
		out := make([]mexcDeal, 0, len(rawList))
		for _, item := range rawList {
			if deal, ok := parseMexcDeal(item); ok {
				out = append(out, deal)
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return nil
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
	subSymbol := symbols.MexcFuturesSymbol(symbolNorm)
	if subSymbol == "" {
		log.Fatal("unable to derive MEXC symbol")
	}
	topic := *topicFlag
	if topic == "" {
		topic = "mexc_" + symbolLower + "_volume"
	}

	producer := nats.NewProducer(brokers, topic)
	defer producer.Close()

	log.Printf("Starting MEXC Volume Follower. Brokers: %v, Topic: %s, Symbol: %s", brokers, topic, symbolNorm)

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
			"method": "sub.deal",
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

				var env mexcDealEnvelope
				if err := json.Unmarshal(message, &env); err != nil {
					continue
				}
				if !strings.HasPrefix(env.Channel, "push.deal") {
					continue
				}

				deals := parseMexcDeals(env.Data)
				if len(deals) == 0 {
					continue
				}

				for _, deal := range deals {
					vol, ok := rawToFloat(deal.V)
					if !ok || vol <= 0 {
						continue
					}
					tsMs, _ := rawToInt64(deal.T)
					if tsMs == 0 {
						tsMs, _ = rawToInt64(env.Ts)
					}
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
