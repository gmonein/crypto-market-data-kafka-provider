package clob

import "fmt"

type ContractConfig struct {
	Exchange        string
	NegRiskExchange string
}

var contractConfigByChain = map[int64]ContractConfig{
	137: {
		Exchange:        "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
		NegRiskExchange: "0xC5d563A36AE78145C45a50134d48A1215220f80a",
	},
	80002: {
		Exchange:        "0xdFE02Eb6733538f8Ea35D585af8DE5958AD99E40",
		NegRiskExchange: "0xC5d563A36AE78145C45a50134d48A1215220f80a",
	},
}

func ExchangeAddress(chainID int64, negRisk bool) (string, error) {
	cfg, ok := contractConfigByChain[chainID]
	if !ok {
		return "", fmt.Errorf("unsupported chain id %d", chainID)
	}
	if negRisk {
		return cfg.NegRiskExchange, nil
	}
	return cfg.Exchange, nil
}

