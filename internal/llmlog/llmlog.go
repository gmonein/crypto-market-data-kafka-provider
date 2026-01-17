package llmlog

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Logger struct {
	mu      sync.Mutex
	enc     *json.Encoder
	f       *os.File
	path    string
	service string
}

func New(path, service string) *Logger {
	path = filepath.Clean(path)
	if path == "." || path == "" {
		return nil
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf("[llm-log] mkdir failed path=%s err=%v", dir, err)
		return nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		log.Printf("[llm-log] open failed path=%s err=%v", path, err)
		return nil
	}
	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	return &Logger{enc: enc, f: f, path: path, service: service}
}

func (l *Logger) Close() {
	if l == nil || l.f == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f != nil {
		_ = l.f.Close()
		l.f = nil
	}
}

func (l *Logger) Log(event string, fields map[string]any) {
	if l == nil || l.enc == nil {
		return
	}
	rec := map[string]any{
		"ts":      time.Now().UTC().Format(time.RFC3339Nano),
		"service": l.service,
		"event":   event,
	}
	for k, v := range fields {
		if _, exists := rec[k]; !exists {
			rec[k] = v
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.enc.Encode(rec); err != nil {
		log.Printf("[llm-log] write failed path=%s err=%v", l.path, err)
	}
}
