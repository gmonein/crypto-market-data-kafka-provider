package kucoin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type WSConfig struct {
	Endpoint     string
	Token        string
	PingInterval time.Duration
	PingTimeout  time.Duration
}

func FetchPublicConfig(ctx context.Context, restBase string) (WSConfig, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, restBase+"/api/v1/bullet-public", nil)
	if err != nil {
		return WSConfig{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return WSConfig{}, err
	}
	defer resp.Body.Close()

	var payload struct {
		Code string `json:"code"`
		Data struct {
			Token           string `json:"token"`
			InstanceServers []struct {
				Endpoint     string `json:"endpoint"`
				PingInterval int64  `json:"pingInterval"`
				PingTimeout  int64  `json:"pingTimeout"`
			} `json:"instanceServers"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return WSConfig{}, err
	}
	if payload.Code != "200000" {
		return WSConfig{}, fmt.Errorf("unexpected response code: %s", payload.Code)
	}
	if payload.Data.Token == "" || len(payload.Data.InstanceServers) == 0 {
		return WSConfig{}, fmt.Errorf("missing token or instanceServers")
	}
	server := payload.Data.InstanceServers[0]
	cfg := WSConfig{
		Endpoint:     server.Endpoint,
		Token:        payload.Data.Token,
		PingInterval: time.Duration(server.PingInterval) * time.Millisecond,
		PingTimeout:  time.Duration(server.PingTimeout) * time.Millisecond,
	}
	if cfg.Endpoint == "" {
		return WSConfig{}, fmt.Errorf("missing websocket endpoint")
	}
	return cfg, nil
}

func WSURL(cfg WSConfig) string {
	connectID := fmt.Sprintf("%d", time.Now().UnixNano())
	return fmt.Sprintf("%s?token=%s&connectId=%s", cfg.Endpoint, cfg.Token, connectID)
}
