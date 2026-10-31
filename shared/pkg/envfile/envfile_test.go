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

func TestLoadLocalDisabledSkipsMissingOrInvalidFile(t *testing.T) {
	workingDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(workingDir) })
	if err := LoadLocal(false); err != nil {
		t.Fatalf("LoadLocal(false) with no .env: %v", err)
	}
	if err := os.WriteFile(".env", []byte("not-a-valid-assignment\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := LoadLocal(false); err != nil {
		t.Fatalf("LoadLocal(false) with invalid .env: %v", err)
	}
}

func TestLoadLocalEnabledRequiresValidFileAndPreservesProcessValues(t *testing.T) {
	workingDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(workingDir) })
	if err := LoadLocal(true); err == nil {
		t.Fatal("LoadLocal(true) accepted a missing .env")
	}
	if err := os.Mkdir(".env", 0o700); err != nil {
		t.Fatal(err)
	}
	if err := LoadLocal(true); err == nil {
		t.Fatal("LoadLocal(true) accepted an unreadable .env path")
	}
	if err := os.Remove(".env"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(".env", []byte("ENVFILE_TEST_PROCESS=file\nENVFILE_TEST_FILE=file-value\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ENVFILE_TEST_PROCESS", "process-value")
	t.Setenv("ENVFILE_TEST_FILE", "")
	if err := LoadLocal(true); err != nil {
		t.Fatalf("LoadLocal(true): %v", err)
	}
	if got := os.Getenv("ENVFILE_TEST_PROCESS"); got != "process-value" {
		t.Fatalf("process value=%q, want process-value", got)
	}
	if got := os.Getenv("ENVFILE_TEST_FILE"); got != "file-value" {
		t.Fatalf("file value=%q, want file-value", got)
	}
	if err := os.WriteFile(".env", []byte("not-a-valid-assignment\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := LoadLocal(true); err == nil {
		t.Fatal("LoadLocal(true) accepted invalid .env syntax")
	}
}
