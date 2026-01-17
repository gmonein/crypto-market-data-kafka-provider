package clob

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"strings"
	"time"
)

const zeroAddress = "0x0000000000000000000000000000000000000000"

type Side string

const (
	SideBuy  Side = "BUY"
	SideSell Side = "SELL"
)

type OrderForSigning struct {
	Salt          string
	Maker         string
	Signer        string
	Taker         string
	TokenID       string
	MakerAmount   string
	TakerAmount   string
	Expiration    string
	Nonce         string
	FeeRateBps    string
	Side          uint8
	SignatureType uint8
}

type SignedOrder struct {
	OrderForSigning
	Signature string
}

type tickRounding struct {
	TickSize       string
	PriceDecimals  int
	SizeDecimals   int
	AmountDecimals int
}

var tickRoundingConfig = map[string]tickRounding{
	"0.1":    {TickSize: "0.1", PriceDecimals: 1, SizeDecimals: 2, AmountDecimals: 3},
	"0.01":   {TickSize: "0.01", PriceDecimals: 2, SizeDecimals: 2, AmountDecimals: 4},
	"0.001":  {TickSize: "0.001", PriceDecimals: 3, SizeDecimals: 2, AmountDecimals: 5},
	"0.0001": {TickSize: "0.0001", PriceDecimals: 4, SizeDecimals: 2, AmountDecimals: 6},
}

func NormalizeTickSize(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("tick size is empty")
	}
	if _, ok := tickRoundingConfig[raw]; ok {
		return raw, nil
	}
	// Best-effort normalization (e.g. "0.0100" -> "0.01")
	f, _, err := big.ParseFloat(raw, 10, 64, big.ToNearestEven)
	if err != nil {
		return "", fmt.Errorf("parse tick size %q: %w", raw, err)
	}
	for k := range tickRoundingConfig {
		kk, _, _ := big.ParseFloat(k, 10, 64, big.ToNearestEven)
		if f.Cmp(kk) == 0 {
			return k, nil
		}
	}
	return "", fmt.Errorf("unsupported tick size %q", raw)
}

type roundingMode int

const (
	roundDown roundingMode = iota
	roundHalfUp
	roundUp
)

// scaleDecimalString returns an integer equal to (s * 10^decimals), rounded according to mode.
// It supports positive decimal numbers in the common forms used for prices/sizes (e.g. "0.01").
func scaleDecimalString(s string, decimals int, mode roundingMode) (*big.Int, error) {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "+")
	if s == "" {
		return nil, fmt.Errorf("empty decimal")
	}
	if strings.HasPrefix(s, "-") {
		return nil, fmt.Errorf("negative decimals not supported: %q", s)
	}
	intPart, fracPart, _ := strings.Cut(s, ".")
	intPart = strings.TrimSpace(intPart)
	fracPart = strings.TrimSpace(fracPart)
	if intPart == "" {
		intPart = "0"
	}
	for _, r := range intPart {
		if r < '0' || r > '9' {
			return nil, fmt.Errorf("invalid integer part in %q", s)
		}
	}
	for _, r := range fracPart {
		if r < '0' || r > '9' {
			return nil, fmt.Errorf("invalid fractional part in %q", s)
		}
	}

	// Pad/truncate fractional digits to requested decimals.
	fracPadded := fracPart
	if len(fracPadded) < decimals {
		fracPadded += strings.Repeat("0", decimals-len(fracPadded))
	}

	keep := ""
	if decimals > 0 && len(fracPadded) > 0 {
		if len(fracPadded) >= decimals {
			keep = fracPadded[:decimals]
		} else {
			keep = fracPadded
		}
	}

	scaledStr := strings.TrimLeft(intPart, "0") + keep
	if scaledStr == "" {
		scaledStr = "0"
	}
	scaled := new(big.Int)
	if _, ok := scaled.SetString(scaledStr, 10); !ok {
		return nil, fmt.Errorf("failed to parse scaled number for %q", s)
	}

	// Apply rounding based on the first dropped digit / remaining digits.
	if len(fracPart) > decimals {
		switch mode {
		case roundHalfUp:
			next := fracPart[decimals]
			if next >= '5' {
				scaled.Add(scaled, big.NewInt(1))
			}
		case roundUp:
			rest := fracPart[decimals:]
			for i := 0; i < len(rest); i++ {
				if rest[i] != '0' {
					scaled.Add(scaled, big.NewInt(1))
					break
				}
			}
		case roundDown:
			// noop
		}
	}
	return scaled, nil
}

func pow10(n int) *big.Int {
	if n <= 0 {
		return big.NewInt(1)
	}
	x := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n)), nil)
	return x
}

func generateSalt() (string, error) {
	// Keep salt within JS safe-integer range since official clients send it as a number.
	max := big.NewInt(1_000_000_000_000) // 1e12
	n, err := rand.Int(rand.Reader, max)
	if err != nil {
		return "", fmt.Errorf("generate salt: %w", err)
	}
	return n.String(), nil
}

type BuildOrderParams struct {
	TokenID       string
	Side          Side
	Price         string // decimal string
	Size          string // decimal string
	TickSize      string // e.g. "0.01"
	NegRisk       bool
	ChainID       int64
	SignerAddress string
	FunderAddress string // optional; if empty uses signer address
	SignatureType uint8  // 0=EOA, 1=POLY_PROXY, 2=POLY_GNOSIS_SAFE (matches @polymarket/order-utils)
	FeeRateBps    string // uint256 string (usually "0")
	Nonce         string // uint256 string (usually "0")
	Expiration    string // uint256 string (usually "0")
}

func BuildSignedOrder(signer *Signer, params BuildOrderParams, exchangeAddress string) (*SignedOrder, error) {
	tickSize, err := NormalizeTickSize(params.TickSize)
	if err != nil {
		return nil, err
	}
	cfg := tickRoundingConfig[tickSize]

	salt, err := generateSalt()
	if err != nil {
		return nil, err
	}

	signerAddress := strings.TrimSpace(params.SignerAddress)
	if signerAddress == "" {
		signerAddress = signer.Address().Hex()
	}
	if !strings.EqualFold(signerAddress, signer.Address().Hex()) {
		return nil, fmt.Errorf("signer address %s does not match private key address %s", signerAddress, signer.Address().Hex())
	}

	maker := strings.TrimSpace(params.FunderAddress)
	if maker == "" {
		maker = signerAddress
	}

	feeRateBps := strings.TrimSpace(params.FeeRateBps)
	if feeRateBps == "" {
		feeRateBps = "0"
	}
	nonce := strings.TrimSpace(params.Nonce)
	if nonce == "" {
		nonce = "0"
	}
	expiration := strings.TrimSpace(params.Expiration)
	if expiration == "" {
		expiration = "0"
	}

	sizeScaled, err := scaleDecimalString(params.Size, cfg.SizeDecimals, roundDown)
	if err != nil {
		return nil, fmt.Errorf("size: %w", err)
	}
	priceScaled, err := scaleDecimalString(params.Price, cfg.PriceDecimals, roundHalfUp)
	if err != nil {
		return nil, fmt.Errorf("price: %w", err)
	}

	sizeMicro := new(big.Int).Mul(sizeScaled, pow10(6-cfg.SizeDecimals)) // size decimals -> 6

	// maker/taker amounts are expressed with 6 decimals as strings (USDC / conditional tokens)
	var makerMicro, takerMicro *big.Int
	var sideEnum uint8

	switch params.Side {
	case SideBuy:
		sideEnum = 0
		makerAmountScaled := new(big.Int).Mul(sizeScaled, priceScaled)                // scale 10^(sizeDec+priceDec) = 10^amountDec
		makerMicro = new(big.Int).Mul(makerAmountScaled, pow10(6-cfg.AmountDecimals)) // amount decimals -> 6
		takerMicro = new(big.Int).Set(sizeMicro)
	case SideSell:
		sideEnum = 1
		takerAmountScaled := new(big.Int).Mul(sizeScaled, priceScaled)                // scale 10^(amountDec)
		takerMicro = new(big.Int).Mul(takerAmountScaled, pow10(6-cfg.AmountDecimals)) // amount decimals -> 6
		makerMicro = new(big.Int).Set(sizeMicro)
	default:
		return nil, fmt.Errorf("invalid side %q", params.Side)
	}

	o := OrderForSigning{
		Salt:          salt,
		Maker:         maker,
		Signer:        signerAddress,
		Taker:         zeroAddress,
		TokenID:       strings.TrimSpace(params.TokenID),
		MakerAmount:   makerMicro.String(),
		TakerAmount:   takerMicro.String(),
		Expiration:    expiration,
		Nonce:         nonce,
		FeeRateBps:    feeRateBps,
		Side:          sideEnum,
		SignatureType: params.SignatureType,
	}
	if o.TokenID == "" {
		return nil, fmt.Errorf("token id is required")
	}

	sig, err := BuildOrderSignature(signer, params.ChainID, exchangeAddress, o)
	if err != nil {
		return nil, err
	}
	return &SignedOrder{OrderForSigning: o, Signature: sig}, nil
}

type OrderPayload struct {
	Salt          int64  `json:"salt"`
	Maker         string `json:"maker"`
	Signer        string `json:"signer"`
	Taker         string `json:"taker"`
	TokenID       string `json:"tokenId"`
	MakerAmount   string `json:"makerAmount"`
	TakerAmount   string `json:"takerAmount"`
	Expiration    string `json:"expiration"`
	Nonce         string `json:"nonce"`
	FeeRateBps    string `json:"feeRateBps"`
	Side          string `json:"side"`
	SignatureType uint8  `json:"signatureType"`
	Signature     string `json:"signature"`
}

type NewOrderRequest struct {
	DeferExec bool         `json:"deferExec"`
	Order     OrderPayload `json:"order"`
	Owner     string       `json:"owner"`
	OrderType string       `json:"orderType"`
}

func (o *SignedOrder) ToNewOrderRequest(owner string, orderType string, deferExec bool) (*NewOrderRequest, error) {
	saltI, ok := new(big.Int).SetString(o.Salt, 10)
	if !ok {
		return nil, fmt.Errorf("invalid salt %q", o.Salt)
	}
	if !saltI.IsInt64() {
		return nil, fmt.Errorf("salt too large for JSON number: %s", o.Salt)
	}
	sideStr := "BUY"
	if o.Side != 0 {
		sideStr = "SELL"
	}
	return &NewOrderRequest{
		DeferExec: deferExec,
		Order: OrderPayload{
			Salt:          saltI.Int64(),
			Maker:         o.Maker,
			Signer:        o.Signer,
			Taker:         o.Taker,
			TokenID:       o.TokenID,
			MakerAmount:   o.MakerAmount,
			TakerAmount:   o.TakerAmount,
			Side:          sideStr,
			Expiration:    o.Expiration,
			Nonce:         o.Nonce,
			FeeRateBps:    o.FeeRateBps,
			SignatureType: o.SignatureType,
			Signature:     o.Signature,
		},
		Owner:     owner,
		OrderType: strings.ToUpper(orderType),
	}, nil
}

func DefaultSlug(now time.Time) string {
	startOfWindow := (now.Unix() / 900) * 900
	return fmt.Sprintf("btc-updown-15m-%d", startOfWindow)
}

