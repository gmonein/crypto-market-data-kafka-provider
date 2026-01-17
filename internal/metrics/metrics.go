package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

type metricKind string

const (
	kindGauge   metricKind = "gauge"
	kindCounter metricKind = "counter"
)

type metric struct {
	name   string
	help   string
	kind   metricKind
	labels string
	value  atomic.Int64
}

var (
	registryMu sync.RWMutex
	registry   []*metric
)

type Gauge struct {
	m *metric
}

type Counter struct {
	m *metric
}

func NewGauge(name string, help string, labels map[string]string) *Gauge {
	return &Gauge{m: registerMetric(name, help, kindGauge, labels)}
}

func NewCounter(name string, help string, labels map[string]string) *Counter {
	return &Counter{m: registerMetric(name, help, kindCounter, labels)}
}

func (g *Gauge) Set(v int64) {
	if g == nil || g.m == nil {
		return
	}
	g.m.value.Store(v)
}

func (g *Gauge) Add(delta int64) {
	if g == nil || g.m == nil {
		return
	}
	g.m.value.Add(delta)
}

func (c *Counter) Inc() {
	c.Add(1)
}

func (c *Counter) Add(delta int64) {
	if c == nil || c.m == nil {
		return
	}
	if delta <= 0 {
		return
	}
	c.m.value.Add(delta)
}

func StartServer(listen string, logf func(string, ...any)) {
	if strings.TrimSpace(listen) == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", Handler())
	srv := &http.Server{
		Addr:    listen,
		Handler: mux,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			if logf != nil {
				logf("metrics server error: %v", err)
			}
		}
	}()
}

func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		metrics := snapshotRegistry()
		for _, m := range metrics {
			_, _ = fmt.Fprintf(w, "# HELP %s %s\n", m.name, escapeHelp(m.help))
			_, _ = fmt.Fprintf(w, "# TYPE %s %s\n", m.name, m.kind)
			value := m.value.Load()
			if m.labels == "" {
				_, _ = fmt.Fprintf(w, "%s %d\n", m.name, value)
				continue
			}
			_, _ = fmt.Fprintf(w, "%s%s %d\n", m.name, m.labels, value)
		}
	})
}

func snapshotRegistry() []*metric {
	registryMu.RLock()
	defer registryMu.RUnlock()
	out := make([]*metric, len(registry))
	copy(out, registry)
	sort.Slice(out, func(i, j int) bool {
		return out[i].name < out[j].name
	})
	return out
}

func registerMetric(name string, help string, kind metricKind, labels map[string]string) *metric {
	m := &metric{
		name:   strings.TrimSpace(name),
		help:   strings.TrimSpace(help),
		kind:   kind,
		labels: formatLabels(labels),
	}
	registryMu.Lock()
	registry = append(registry, m)
	registryMu.Unlock()
	return m
}

func formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf(`%s="%s"`, k, escapeLabelValue(labels[k])))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func escapeHelp(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "\n", "\\n")
	return s
}

func escapeLabelValue(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "\n", "\\n")
	s = strings.ReplaceAll(s, "\"", "\\\"")
	return s
}
