// Package envfile loads repository .env files without overriding process values.
package envfile

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Load searches the working directory and its parents for .env. A missing file
// is allowed; the application can then use its normal defaults.
func Load() error {
	dir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("find .env: %w", err)
	}
	for {
		path := filepath.Join(dir, ".env")
		if _, err := os.Stat(path); err == nil {
			return LoadFile(path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect %s: %w", path, err)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return nil
		}
		dir = parent
	}
}

// LoadFile applies assignments only when the process environment is unset or
// empty. It supports KEY=value, export KEY=value, quoted values, and comments.
func LoadFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	for lineNo := 1; scanner.Scan(); lineNo++ {
		key, value, ok, err := parseLine(scanner.Text())
		if err != nil {
			return fmt.Errorf("parse %s line %d: %w", path, lineNo, err)
		}
		if !ok || os.Getenv(key) != "" {
			continue
		}
		if err := os.Setenv(key, value); err != nil {
			return fmt.Errorf("set %s from %s line %d: %w", key, path, lineNo, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	return nil
}

func parseLine(line string) (string, string, bool, error) {
	line = strings.TrimSpace(strings.TrimPrefix(line, "\ufeff"))
	if line == "" || strings.HasPrefix(line, "#") {
		return "", "", false, nil
	}
	line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
	i := strings.IndexByte(line, '=')
	if i <= 0 {
		return "", "", false, errors.New("expected KEY=value")
	}
	key := strings.TrimSpace(line[:i])
	if !validKey(key) {
		return "", "", false, errors.New("invalid variable name")
	}
	value, err := parseValue(strings.TrimSpace(line[i+1:]))
	return key, value, true, err
}

func parseValue(value string) (string, error) {
	if value == "" {
		return "", nil
	}
	if value[0] == '\'' || value[0] == '"' {
		quote := value[0]
		end := -1
		escaped := false
		for i := 1; i < len(value); i++ {
			if quote == '"' && value[i] == '\\' && !escaped {
				escaped = true
				continue
			}
			if value[i] == quote && !escaped {
				end = i
				break
			}
			escaped = false
		}
		if end < 0 {
			return "", errors.New("unterminated quoted value")
		}
		remainder := strings.TrimSpace(value[end+1:])
		if remainder != "" && !strings.HasPrefix(remainder, "#") {
			return "", errors.New("unexpected text after quoted value")
		}
		if quote == '\'' {
			return value[1:end], nil
		}
		parsed, err := strconv.Unquote(value[:end+1])
		if err != nil {
			return "", errors.New("invalid quoted value")
		}
		return parsed, nil
	}
	for i, r := range value {
		if r == '#' && (i == 0 || value[i-1] == ' ' || value[i-1] == '\t') {
			value = value[:i]
			break
		}
	}
	return strings.TrimSpace(value), nil
}

func validKey(key string) bool {
	if key == "" || !isLetter(key[0]) && key[0] != '_' {
		return false
	}
	for i := 1; i < len(key); i++ {
		if !isLetter(key[i]) && key[i] != '_' && (key[i] < '0' || key[i] > '9') {
			return false
		}
	}
	return true
}

func isLetter(b byte) bool { return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' }
