package symbols

import (
	"fmt"
	"os"
	"strings"
)

const (
	envSymbol    = "SYMBOL"
	envSymbolAlt = "PM_SYMBOL"
)

func FromEnv(defaultSymbol string) string {
	if v := strings.TrimSpace(os.Getenv(envSymbol)); v != "" {
		return normalize(v)
	}
	if v := strings.TrimSpace(os.Getenv(envSymbolAlt)); v != "" {
		return normalize(v)
	}
	return normalize(defaultSymbol)
}

func Normalize(symbol string) string {
	return normalize(symbol)
}

func Lower(symbol string) string {
	return strings.ToLower(normalize(symbol))
}

func normalize(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	replacer := strings.NewReplacer("-", "", "_", "", "/", "", " ", "")
	return strings.ToUpper(replacer.Replace(s))
}

func splitSymbol(symbol string) (string, string) {
	s := normalize(symbol)
	if s == "" {
		return "", ""
	}
	if strings.HasSuffix(s, "USDT") {
		return strings.TrimSuffix(s, "USDT"), "USDT"
	}
	if strings.HasSuffix(s, "USD") {
		return strings.TrimSuffix(s, "USD"), "USD"
	}
	return s, "USDT"
}

func DefaultTopic(exchange, symbol, legacy string) string {
	s := normalize(symbol)
	if s == "" {
		return legacy
	}
	return fmt.Sprintf("%s_%s", exchange, strings.ToLower(s))
}

func BinanceStreamSymbol(symbol string) string {
	return strings.ToLower(normalize(symbol))
}

func BybitSymbol(symbol string) string {
	return normalize(symbol)
}

func BitgetSymbol(symbol string) string {
	return normalize(symbol)
}

func OkxInstID(symbol string) string {
	base, quote := splitSymbol(symbol)
	if base == "" || quote == "" {
		return ""
	}
	return fmt.Sprintf("%s-%s", base, quote)
}

func CoinbaseProductID(symbol string) string {
	base, quote := splitSymbol(symbol)
	if base == "" || quote == "" {
		return ""
	}
	return fmt.Sprintf("%s-%s", base, quote)
}

func KrakenPair(symbol string) string {
	base, quote := splitSymbol(symbol)
	if base == "" || quote == "" {
		return ""
	}
	if base == "BTC" {
		base = "XBT"
	}
	return fmt.Sprintf("%s/%s", base, quote)
}

func ChainlinkSymbol(symbol string) string {
	base, _ := splitSymbol(symbol)
	if base == "" {
		return ""
	}
	return strings.ToLower(base) + "/usd"
}
