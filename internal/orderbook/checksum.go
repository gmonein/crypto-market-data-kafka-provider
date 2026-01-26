package orderbook

import (
	"hash/crc32"
	"strings"
)

func ChecksumOKX(bids, asks [][]string, depth int) uint32 {
	max := depth
	if max <= 0 || max > 25 {
		max = 25
	}
	if len(bids) > max {
		bids = bids[:max]
	}
	if len(asks) > max {
		asks = asks[:max]
	}

	var b strings.Builder
	appendField := func(val string) {
		if val == "" {
			return
		}
		if b.Len() > 0 {
			b.WriteString(":")
		}
		b.WriteString(val)
	}

	for i := 0; i < max; i++ {
		if i < len(bids) && len(bids[i]) >= 2 {
			appendField(bids[i][0])
			appendField(bids[i][1])
		}
		if i < len(asks) && len(asks[i]) >= 2 {
			appendField(asks[i][0])
			appendField(asks[i][1])
		}
	}

	return crc32.ChecksumIEEE([]byte(b.String()))
}

func ChecksumKraken(bids, asks [][]string, depth int) uint32 {
	max := depth
	if max <= 0 {
		max = 10
	}
	if len(bids) > max {
		bids = bids[:max]
	}
	if len(asks) > max {
		asks = asks[:max]
	}

	var b strings.Builder
	appendField := func(val string) {
		if val == "" {
			return
		}
		s := strings.ReplaceAll(val, ".", "")
		s = strings.TrimLeft(s, "0")
		if s == "" {
			s = "0"
		}
		b.WriteString(s)
	}

	for i := 0; i < len(bids); i++ {
		if len(bids[i]) >= 2 {
			appendField(bids[i][0])
			appendField(bids[i][1])
		}
	}
	for i := 0; i < len(asks); i++ {
		if len(asks[i]) >= 2 {
			appendField(asks[i][0])
			appendField(asks[i][1])
		}
	}

	return crc32.ChecksumIEEE([]byte(b.String()))
}
