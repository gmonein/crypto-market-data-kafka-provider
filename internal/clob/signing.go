package clob

import (
	"crypto/ecdsa"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/signer/core/apitypes"
)

const (
	clobAuthDomainName = "ClobAuthDomain"
	clobAuthVersion    = "1"
	clobAuthMessage    = "This message attests that I control the given wallet"

	exchangeDomainName = "Polymarket CTF Exchange"
	exchangeVersion    = "1"
)

type Signer struct {
	privateKey *ecdsa.PrivateKey
	address    common.Address
}

func NewSignerFromHexPrivateKey(hexKey string) (*Signer, error) {
	hexKey = strings.TrimSpace(hexKey)
	hexKey = strings.TrimPrefix(hexKey, "0x")
	if hexKey == "" {
		return nil, fmt.Errorf("private key is required")
	}
	pk, err := crypto.HexToECDSA(hexKey)
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}
	addr := crypto.PubkeyToAddress(pk.PublicKey)
	return &Signer{privateKey: pk, address: addr}, nil
}

func (s *Signer) Address() common.Address {
	return s.address
}

func (s *Signer) SignEIP712TypedData(td apitypes.TypedData) (string, error) {
	digest, _, err := apitypes.TypedDataAndHash(td)
	if err != nil {
		return "", fmt.Errorf("eip712 typed data hash: %w", err)
	}

	sig, err := crypto.Sign(digest, s.privateKey)
	if err != nil {
		return "", fmt.Errorf("sign digest: %w", err)
	}
	// ethers.js returns v as 27/28
	sig[64] += 27
	return "0x" + common.Bytes2Hex(sig), nil
}

func BuildL1ClobAuthSignature(signer *Signer, chainID int64, timestamp int64, nonce int64) (string, error) {
	nonceBig := new(big.Int).SetInt64(nonce)
	td := apitypes.TypedData{
		Types: apitypes.Types{
			"EIP712Domain": []apitypes.Type{
				{Name: "name", Type: "string"},
				{Name: "version", Type: "string"},
				{Name: "chainId", Type: "uint256"},
			},
			"ClobAuth": []apitypes.Type{
				{Name: "address", Type: "address"},
				{Name: "timestamp", Type: "string"},
				{Name: "nonce", Type: "uint256"},
				{Name: "message", Type: "string"},
			},
		},
		PrimaryType: "ClobAuth",
		Domain: apitypes.TypedDataDomain{
			Name:    clobAuthDomainName,
			Version: clobAuthVersion,
			ChainId: math.NewHexOrDecimal256(chainID),
		},
		Message: apitypes.TypedDataMessage{
			"address":   signer.Address().Hex(),
			"timestamp": fmt.Sprintf("%d", timestamp),
			"nonce":     nonceBig,
			"message":   clobAuthMessage,
		},
	}
	return signer.SignEIP712TypedData(td)
}

func BuildOrderSignature(
	signer *Signer,
	chainID int64,
	verifyingContract string,
	order OrderForSigning,
) (string, error) {
	salt, ok := new(big.Int).SetString(order.Salt, 10)
	if !ok {
		return "", fmt.Errorf("invalid salt %q", order.Salt)
	}
	tokenID, ok := new(big.Int).SetString(order.TokenID, 10)
	if !ok {
		return "", fmt.Errorf("invalid token id %q", order.TokenID)
	}
	makerAmount, ok := new(big.Int).SetString(order.MakerAmount, 10)
	if !ok {
		return "", fmt.Errorf("invalid maker amount %q", order.MakerAmount)
	}
	takerAmount, ok := new(big.Int).SetString(order.TakerAmount, 10)
	if !ok {
		return "", fmt.Errorf("invalid taker amount %q", order.TakerAmount)
	}
	expiration, ok := new(big.Int).SetString(order.Expiration, 10)
	if !ok {
		return "", fmt.Errorf("invalid expiration %q", order.Expiration)
	}
	nonce, ok := new(big.Int).SetString(order.Nonce, 10)
	if !ok {
		return "", fmt.Errorf("invalid nonce %q", order.Nonce)
	}
	feeRateBps, ok := new(big.Int).SetString(order.FeeRateBps, 10)
	if !ok {
		return "", fmt.Errorf("invalid feeRateBps %q", order.FeeRateBps)
	}

	td := apitypes.TypedData{
		Types: apitypes.Types{
			"EIP712Domain": []apitypes.Type{
				{Name: "name", Type: "string"},
				{Name: "version", Type: "string"},
				{Name: "chainId", Type: "uint256"},
				{Name: "verifyingContract", Type: "address"},
			},
			"Order": []apitypes.Type{
				{Name: "salt", Type: "uint256"},
				{Name: "maker", Type: "address"},
				{Name: "signer", Type: "address"},
				{Name: "taker", Type: "address"},
				{Name: "tokenId", Type: "uint256"},
				{Name: "makerAmount", Type: "uint256"},
				{Name: "takerAmount", Type: "uint256"},
				{Name: "expiration", Type: "uint256"},
				{Name: "nonce", Type: "uint256"},
				{Name: "feeRateBps", Type: "uint256"},
				{Name: "side", Type: "uint8"},
				{Name: "signatureType", Type: "uint8"},
			},
		},
		PrimaryType: "Order",
		Domain: apitypes.TypedDataDomain{
			Name:              exchangeDomainName,
			Version:           exchangeVersion,
			ChainId:           math.NewHexOrDecimal256(chainID),
			VerifyingContract: verifyingContract,
		},
		Message: apitypes.TypedDataMessage{
			"salt":          salt,
			"maker":         order.Maker,
			"signer":        order.Signer,
			"taker":         order.Taker,
			"tokenId":       tokenID,
			"makerAmount":   makerAmount,
			"takerAmount":   takerAmount,
			"expiration":    expiration,
			"nonce":         nonce,
			"feeRateBps":    feeRateBps,
			"side":          new(big.Int).SetUint64(uint64(order.Side)),
			"signatureType": new(big.Int).SetUint64(uint64(order.SignatureType)),
		},
	}
	return signer.SignEIP712TypedData(td)
}

func BuildPolyHmacSignature(
	base64Secret string,
	timestamp int64,
	method string,
	requestPath string,
	body string,
) (string, error) {
	method = strings.ToUpper(method)
	message := fmt.Sprintf("%d%s%s", timestamp, method, requestPath)
	if body != "" {
		message += body
	}

	secret, err := base64.StdEncoding.DecodeString(base64Secret)
	if err != nil {
		return "", fmt.Errorf("decode base64 secret: %w", err)
	}

	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write([]byte(message))
	sig := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	sig = strings.ReplaceAll(sig, "+", "-")
	sig = strings.ReplaceAll(sig, "/", "_")
	return sig, nil
}

