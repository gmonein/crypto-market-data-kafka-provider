package dotenv

import (
	"bufio"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// Load loads environment variables from the first .env file found in paths.
// It is intentionally minimal and supports the common KEY=VALUE format used in this repo.
// Existing environment variables are not overwritten.
func Load(paths ...string) error {
	var firstErr error
	for _, p := range paths {
		if p == "" {
			continue
		}
		if err := loadFile(p); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		return nil
	}
	return firstErr
}

func loadFile(path string) error {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		value = strings.TrimSpace(value)
		value = strings.TrimSuffix(strings.TrimPrefix(value, `"`), `"`)
		value = strings.TrimSuffix(strings.TrimPrefix(value, `'`), `'`)
		_ = os.Setenv(key, value)
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

