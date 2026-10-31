package envfile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFilePrecedenceAndQuotes(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte("FROM_PROCESS=file\nFROM_FILE=file-value\nQUOTED=\"value with spaces\" # note\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FROM_PROCESS", "process-value")
	t.Setenv("FROM_FILE", "")
	t.Setenv("QUOTED", "")
	if err := LoadFile(path); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{"FROM_PROCESS": "process-value", "FROM_FILE": "file-value", "QUOTED": "value with spaces"} {
		if got := os.Getenv(key); got != want {
			t.Errorf("%s=%q, want %q", key, got, want)
		}
	}
}

func TestLoadAllowsMissingFile(t *testing.T) {
	workingDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(workingDir) })
	if err := Load(); err != nil {
		t.Fatalf("Load() with no .env: %v", err)
	}
}
