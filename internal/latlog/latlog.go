package latlog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Logger struct {
	dir     string
	service string

	mu   sync.Mutex
	file *os.File
	hour string
}

func New(dir, service string) *Logger {
	dir = strings.TrimSpace(dir)
	service = strings.TrimSpace(service)
	if dir == "" || service == "" {
		return nil
	}
	_ = os.MkdirAll(dir, 0o755)
	return &Logger{dir: dir, service: service}
}

func (l *Logger) Close() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		_ = l.file.Close()
		l.file = nil
	}
}

func (l *Logger) Log(event string, fields map[string]any) {
	if l == nil {
		return
	}
	now := time.Now().UTC()
	hour := now.Format("20060102-15")

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil || l.hour != hour {
		if l.file != nil {
			_ = l.file.Close()
			l.file = nil
		}
		path := filepath.Join(l.dir, fmt.Sprintf("%s-%s.jsonl", l.service, hour))
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return
		}
		l.file = f
		l.hour = hour
	}

	entry := map[string]any{
		"ts":      now.Format(time.RFC3339Nano),
		"service": l.service,
		"event":   strings.TrimSpace(event),
	}
	for k, v := range fields {
		if k == "" {
			continue
		}
		entry[k] = v
	}
	b, err := json.Marshal(entry)
	if err != nil || l.file == nil {
		return
	}
	_, _ = l.file.Write(append(b, '\n'))
}
