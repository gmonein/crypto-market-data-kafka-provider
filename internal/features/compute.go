package features

import (
	"time"
)

type FeatureVector struct {
	Symbol             string  `json:"symbol"`
	Timestamp          int64   `json:"timestamp"` // unix seconds
	CurrentPrice       float64 `json:"current_price"`
	
	Volatility15m      float64 `json:"volatility_15m"`
	Volatility5m       float64 `json:"volatility_5m"`
	Volatility1m       float64 `json:"volatility_1m"`
	Volatility30s      float64 `json:"volatility_30s"`
	Volatility10s      float64 `json:"volatility_10s"`
	
	Momentum15m        float64 `json:"price_momentum_15m"`
	Momentum5m         float64 `json:"price_momentum_5m"`
	Momentum1m         float64 `json:"price_momentum_1m"`
	Momentum30s        float64 `json:"price_momentum_30s"`
	Momentum10s        float64 `json:"price_momentum_10s"`
	
	RSI15m             float64 `json:"rsi_15m"`
	RSI5m              float64 `json:"rsi_5m"`
	RSI30s             float64 `json:"rsi_30s"`
	
	ZScore15m          float64 `json:"z_score_15m"`
	ZScore5m           float64 `json:"z_score_5m"`
	ZScore1m           float64 `json:"z_score_1m"`
	ZScore30s          float64 `json:"z_score_30s"`
	
	BollingerPosition  float64 `json:"bollinger_position"`
	PricePercentile15m float64 `json:"price_percentile_15m"`
	DistanceFromSMA    float64 `json:"distance_from_sma_pct"`
	
	VolRatio10s1m      float64 `json:"vol_ratio_10s_1m"`
	VolRatio30s5m      float64 `json:"vol_ratio_30s_5m"`
	VolRatio1m5m       float64 `json:"vol_ratio_1m_5m"`
	VolRatio5m15m      float64 `json:"vol_ratio_5m_15m"`
	
	SMA15              float64 `json:"sma_15"`
	EMA15              float64 `json:"ema_15"`
	
	RSISlope           float64 `json:"rsi_slope"`
	TSISlope           float64 `json:"tsi_slope"`
	MomentumAccel      float64 `json:"momentum_acceleration"`
	
	PolySlope          float64 `json:"poly_slope"`
	PolyCurvature      float64 `json:"poly_curvature"`
	PolyResidual       float64 `json:"poly_residual"`
	
	Volume15mSum       float64 `json:"volume_15m_sum"`
	Volume5mSum        float64 `json:"volume_5m_sum"`
	Volume1mSum        float64 `json:"volume_1m_sum"`
	Volume30sSum       float64 `json:"volume_30s_sum"`
	Volume10sSum       float64 `json:"volume_10s_sum"`
	VolumeZScore       float64 `json:"volume_z_score"`
	
	HourOfDay          int     `json:"hour_of_day"`
	DayOfWeek          int     `json:"day_of_week"`
}

func ComputeFeatures(symbol string, ts int64, currentPrice float64, timestamps []int64, prices, volumes []float64, prevRSI *float64, prevTSI *float64, prevMom *float64, hasPrev *bool) *FeatureVector {
	n := len(timestamps)

	// Helper to get slice
	getSlice := func(seconds int) []float64 {
		cutoff := ts - int64(seconds)
		for i := n - 1; i >= 0; i-- {
			if timestamps[i] < cutoff {
				return prices[i+1:]
			}
		}
		return prices
	}

	getVolSlice := func(seconds int) []float64 {
		cutoff := ts - int64(seconds)
		for i := n - 1; i >= 0; i-- {
			if timestamps[i] < cutoff {
				return volumes[i+1:]
			}
		}
		return volumes
	}

	prices15m := prices // Buffer is roughly 15m
	prices5m := getSlice(300)
	prices1m := getSlice(60)
	prices30s := getSlice(30)
	prices10s := getSlice(10)

	vols15m := volumes
	vols5m := getVolSlice(300)
	vols1m := getVolSlice(60)
	vols30s := getVolSlice(30)
	vols10s := getVolSlice(10)

	if len(prices15m) < 100 {
		return nil
	}

	// --- Calculate ---

	// Volatility
	v15m := Volatility(prices15m)
	v5m := Volatility(prices5m)
	v1m := Volatility(prices1m)
	v30s := Volatility(prices30s)
	v10s := Volatility(prices10s)

	// Momentum
	m15m := Momentum(prices15m)
	m5m := Momentum(prices5m)
	m1m := Momentum(prices1m)
	m30s := Momentum(prices30s)
	m10s := Momentum(prices10s)

	// RSI
	r15m := RSI(prices15m, 0)
	r5m := RSI(prices5m, 0)
	r30s := RSI(prices30s, 0)

	// Z-Scores
	z15m := ZScore(currentPrice, prices15m)
	z5m := ZScore(currentPrice, prices5m)
	z1m := ZScore(currentPrice, prices1m)
	z30s := ZScore(currentPrice, prices30s)

	// Others
	boll := BollingerPosition(currentPrice, prices15m, 2.0)
	pct := PricePercentile(currentPrice, prices15m)
	smaVal := Mean(prices15m)
	emaVal := EMA(prices15m, 0)

	distSMA := 0.0
	if smaVal != 0 {
		distSMA = (currentPrice - smaVal) / smaVal
	}

	tsiVal := TSI(prices15m, 25, 13)

	poly := PolynomialFeatures(prices30s, 3)

	// Ratios
	var r10s1m, r30s5m, r1m5m, r5m15m float64
	if v1m > 0 {
		r10s1m = v10s / v1m
	}
	if v5m > 0 {
		r30s5m = v30s / v5m
	}
	if v5m > 0 {
		r1m5m = v1m / v5m
	}
	if v15m > 0 {
		r5m15m = v5m / v15m
	}

	// Slopes
	var rsiSlope, tsiSlope, momAccel float64

	if *hasPrev {
		rsiSlope = r5m - *prevRSI
		tsiSlope = tsiVal - *prevTSI 
		momAccel = m5m - *prevMom
	}

	*prevRSI = r5m
	*prevTSI = tsiVal 
	*prevMom = m5m
	*hasPrev = true

	// Volume Sums
	volSum15m := Sum(vols15m)
	volSum5m := Sum(vols5m)
	volSum1m := Sum(vols1m)
	volSum30s := Sum(vols30s)
	volSum10s := Sum(vols10s)

	// Vol Z-Score
	volMean := Mean(vols15m)
	volStd := Std(vols15m)
	currentVol := 0.0
	if len(vols15m) > 0 {
		currentVol = vols15m[len(vols15m)-1]
	}
	volZ := 0.0
	if volStd > 0 {
		volZ = (currentVol - volMean) / volStd
	}

	tObj := time.Unix(ts, 0)

	return &FeatureVector{
		Symbol:             symbol,
		Timestamp:          ts,
		CurrentPrice:       currentPrice,
		Volatility15m:      v15m,
		Volatility5m:       v5m,
		Volatility1m:       v1m,
		Volatility30s:      v30s,
		Volatility10s:      v10s,
		Momentum15m:        m15m,
		Momentum5m:         m5m,
		Momentum1m:         m1m,
		Momentum30s:        m30s,
		Momentum10s:        m10s,
		RSI15m:             r15m,
		RSI5m:              r5m,
		RSI30s:             r30s,
		ZScore15m:          z15m,
		ZScore5m:           z5m,
		ZScore1m:           z1m,
		ZScore30s:          z30s,
		BollingerPosition:  boll,
		PricePercentile15m: pct,
		DistanceFromSMA:    distSMA,
		VolRatio10s1m:      r10s1m,
		VolRatio30s5m:      r30s5m,
		VolRatio1m5m:       r1m5m,
		VolRatio5m15m:      r5m15m,
		SMA15:              smaVal,
		EMA15:              emaVal,
		RSISlope:           rsiSlope,
		TSISlope:           tsiSlope,
		MomentumAccel:      momAccel,
		PolySlope:          poly.Slope,
		PolyCurvature:      poly.Curvature,
		PolyResidual:       poly.Residual,
		Volume15mSum:       volSum15m,
		Volume5mSum:        volSum5m,
		Volume1mSum:        volSum1m,
		Volume30sSum:       volSum30s,
		Volume10sSum:       volSum10s,
		VolumeZScore:       volZ,
		HourOfDay:          tObj.Hour(),
		DayOfWeek:          int(tObj.Weekday()),
	}
}
