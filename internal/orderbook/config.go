package orderbook

import (
	"os"
	"strings"
)

const (
	OutputFull     = "full"
	OutputFeatures = "features"
)

func OutputMode() string {
	return parseOutputMode(os.Getenv("ORDERBOOK_OUTPUT"))
}

func parseOutputMode(raw string) string {
	mode := strings.ToLower(strings.TrimSpace(raw))
	switch mode {
	case "", OutputFull:
		return OutputFull
	case OutputFeatures, "feature", "compact":
		return OutputFeatures
	default:
		return OutputFull
	}
}
