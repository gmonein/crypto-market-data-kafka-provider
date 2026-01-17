package features

import (
	"math"
)

// --- Basic Statistics ---

func Mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func Std(values []float64) float64 {
	if len(values) < 2 {
		return 0
	}
	m := Mean(values)
	variance := 0.0
	for _, v := range values {
		variance += math.Pow(v-m, 2)
	}
	return math.Sqrt(variance / float64(len(values)-1))
}

// --- Returns ---

func Returns(prices []float64) []float64 {
	if len(prices) < 2 {
		return []float64{}
	}
	res := make([]float64, len(prices)-1)
	for i := 0; i < len(prices)-1; i++ {
		res[i] = (prices[i+1] - prices[i]) / prices[i]
	}
	return res
}

// --- Volatility ---

func Volatility(prices []float64) float64 {
	r := Returns(prices)
	if len(r) < 2 {
		return 0
	}
	return Std(r)
}

// --- Momentum ---

func Momentum(prices []float64) float64 {
	if len(prices) < 2 {
		return 0
	}
	return (prices[len(prices)-1] - prices[0]) / prices[0]
}

// --- Z-Score ---

func ZScore(currentPrice float64, prices []float64) float64 {
	if len(prices) < 2 {
		return 0
	}
	m := Mean(prices)
	s := Std(prices)
	if s == 0 {
		return 0
	}
	return (currentPrice - m) / s
}

// --- RSI ---

func RSI(prices []float64, period int) float64 {
	if len(prices) < 2 {
		return 0
	}
	if period <= 0 {
		period = len(prices) - 1
	}

	changes := make([]float64, len(prices)-1)
	for i := 0; i < len(prices)-1; i++ {
		changes[i] = prices[i+1] - prices[i]
	}

	if len(changes) < period {
		return 0
	}

	// Use last 'period' changes
	recentChanges := changes[len(changes)-period:]
	
	avgGain := 0.0
	avgLoss := 0.0
	
	for _, c := range recentChanges {
		if c > 0 {
			avgGain += c
		} else {
			avgLoss += math.Abs(c)
		}
	}
	
	avgGain /= float64(period)
	avgLoss /= float64(period)

	if avgLoss == 0 {
		return 100.0
	}
	if avgGain == 0 {
		return 0.0
	}

	rs := avgGain / avgLoss
	return 100.0 - (100.0 / (1.0 + rs))
}

// --- Bollinger ---

func BollingerPosition(currentPrice float64, prices []float64, numStd float64) float64 {
	if len(prices) < 2 {
		return 0
	}
	m := Mean(prices)
	s := Std(prices)
	if s == 0 {
		return 0
	}
	
	lower := m - numStd*s
	upper := m + numStd*s
	
	pos := (currentPrice - lower) / (upper - lower)
	return math.Max(0, math.Min(1, pos)) // Clamp 0-1
}

// --- Percentile ---

func PricePercentile(currentPrice float64, prices []float64) float64 {
	if len(prices) == 0 {
		return 0
	}
	minP := prices[0]
	maxP := prices[0]
	for _, p := range prices {
		if p < minP {
			minP = p
		}
		if p > maxP {
			maxP = p
		}
	}
	
	if maxP == minP {
		return 0.5
	}
	return (currentPrice - minP) / (maxP - minP)
}

// --- Moving Averages ---

func EMA(prices []float64, span int) float64 {
	if len(prices) == 0 {
		return 0
	}
	if span <= 0 {
		span = len(prices)
	}
	
	multiplier := 2.0 / float64(span+1)
	ema := prices[0]
	
	for i := 1; i < len(prices); i++ {
		ema = (prices[i]-ema)*multiplier + ema
	}
	return ema
}

// --- TSI ---

func TSI(prices []float64, longPeriod, shortPeriod int) float64 {
	if len(prices) < longPeriod+shortPeriod {
		return 0
	}
	
	changes := make([]float64, len(prices)-1)
	for i := 0; i < len(prices)-1; i++ {
		changes[i] = prices[i+1] - prices[i]
	}
	
	if len(changes) < longPeriod {
		return 0
	}
	
	// Double Smoothed PC
	// 1. First smoothing
	ema1Changes := calculateEMASeries(changes, longPeriod)
	// 2. Second smoothing
	ema2Changes := calculateEMASeries(ema1Changes, shortPeriod)
	
	// Double Smoothed Absolute PC
	absChanges := make([]float64, len(changes))
	for i, c := range changes {
		absChanges[i] = math.Abs(c)
	}
	
	ema1Abs := calculateEMASeries(absChanges, longPeriod)
	ema2Abs := calculateEMASeries(ema1Abs, shortPeriod)
	
	finalChange := ema2Changes[len(ema2Changes)-1]
	finalAbs := ema2Abs[len(ema2Abs)-1]
	
	if finalAbs == 0 {
		return 0
	}
	
	return 100.0 * finalChange / finalAbs
}

func calculateEMASeries(values []float64, span int) []float64 {
	if len(values) == 0 {
		return []float64{}
	}
	res := make([]float64, len(values))
	res[0] = values[0]
	multiplier := 2.0 / float64(span+1)
	for i := 1; i < len(values); i++ {
		res[i] = (values[i]-res[i-1])*multiplier + res[i-1]
	}
	return res
}

// --- Polynomial ---

type PolyResult struct {
	Slope     float64
	Curvature float64
	Residual  float64
}

func PolynomialFeatures(prices []float64, degree int) PolyResult {
	if len(prices) < degree+1 {
		return PolyResult{}
	}
	
	n := len(prices)
	x := make([]float64, n)
	y := make([]float64, n)
	for i := 0; i < n; i++ {
		x[i] = float64(i)
		y[i] = prices[i]
	}
	
	// Fit
	coeffs, err := PolyFit(x, y, degree)
	if err != nil {
		return PolyResult{}
	}
	
	// Evaluate at last point t = n-1
	t := float64(n - 1)
	
	// Slope (1st derivative)
	slope := 0.0
	for i := 1; i < len(coeffs); i++ {
		slope += float64(i) * coeffs[i] * math.Pow(t, float64(i-1))
	}
	
	// Curvature (2nd derivative)
	curvature := 0.0
	for i := 2; i < len(coeffs); i++ {
		curvature += float64(i) * float64(i-1) * coeffs[i] * math.Pow(t, float64(i-2))
	}
	
	// Residual
	sumSqRes := 0.0
	for i := 0; i < n; i++ {
		xi := x[i]
		fi := 0.0
		for j, c := range coeffs {
			fi += c * math.Pow(xi, float64(j))
		}
		sumSqRes += math.Pow(y[i]-fi, 2)
	}
	residual := math.Sqrt(sumSqRes / float64(n))
	
	return PolyResult{Slope: slope, Curvature: curvature, Residual: residual}
}

// Simple Matrix math for PolyFit
func PolyFit(x, y []float64, degree int) ([]float64, error) {
	// Vandermonde matrix V
	n := len(x)
	v := make([][]float64, n)
	for i := 0; i < n; i++ {
		v[i] = make([]float64, degree+1)
		for j := 0; j <= degree; j++ {
			v[i][j] = math.Pow(x[i], float64(j))
		}
	}
	
	// Transpose V -> Vt
	vt := make([][]float64, degree+1)
	for i := 0; i <= degree; i++ {
		vt[i] = make([]float64, n)
		for j := 0; j < n; j++ {
			vt[i][j] = v[j][i]
		}
	}
	
	// Vt * V
	vtv := MatrixMult(vt, v)
	
	// Inverse (VtV)^-1
	inv, err := MatrixInverse(vtv)
	if err != nil {
		return nil, err
	}
	
	// Vt * y
	vty := make([]float64, degree+1)
	for i := 0; i <= degree; i++ {
		sum := 0.0
		for j := 0; j < n; j++ {
			sum += vt[i][j] * y[j]
		}
		vty[i] = sum
	}
	
	// Coeffs = Inv * Vty
	coeffs := make([]float64, degree+1)
	for i := 0; i <= degree; i++ {
		sum := 0.0
		for j := 0; j <= degree; j++ {
			sum += inv[i][j] * vty[j]
		}
		coeffs[i] = sum
	}
	
	return coeffs, nil
}

func MatrixMult(a, b [][]float64) [][]float64 {
	rowsA := len(a)
	colsA := len(a[0])
	colsB := len(b[0])
	
	res := make([][]float64, rowsA)
	for i := 0; i < rowsA; i++ {
		res[i] = make([]float64, colsB)
		for j := 0; j < colsB; j++ {
			sum := 0.0
			for k := 0; k < colsA; k++ {
				sum += a[i][k] * b[k][j]
			}
			res[i][j] = sum
		}
	}
	return res
}

func MatrixInverse(m [][]float64) ([][]float64, error) {
	// Gauss-Jordan elimination or similar. 
	// Since degree is small (3 -> 4x4 matrix), we can use a simple hardcoded inverse or recursive adjugate.
	// Given complexity, let's implement a simple Gaussian elimination for N dimensions.
	n := len(m)
	aug := make([][]float64, n)
	for i := 0; i < n; i++ {
		aug[i] = make([]float64, 2*n)
		copy(aug[i], m[i])
		aug[i][n+i] = 1.0
	}
	
	// Pivot
	for i := 0; i < n; i++ {
		pivot := aug[i][i]
		if math.Abs(pivot) < 1e-10 {
			return nil, nil // Singular
		}
		
		// Normalize row
		for j := 0; j < 2*n; j++ {
			aug[i][j] /= pivot
		}
		
		// Eliminate other rows
		for k := 0; k < n; k++ {
			if k != i {
				factor := aug[k][i]
				for j := 0; j < 2*n; j++ {
					aug[k][j] -= factor * aug[i][j]
				}
			}
		}
	}
	
	// Extract inverse
	inv := make([][]float64, n)
	for i := 0; i < n; i++ {
		inv[i] = make([]float64, n)
		copy(inv[i], aug[i][n:])
	}
	
	return inv, nil
}

func Sum(v []float64) float64 {
	s := 0.0
	for _, x := range v {
		s += x
	}
	return s
}
