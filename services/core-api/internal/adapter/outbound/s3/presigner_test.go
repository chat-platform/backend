package s3

import (
	"context"
	"net/url"
	"testing"
	"time"
)

func TestSignUploadBindsContentLengthAndType(t *testing.T) {
	presigner, err := New("http://localhost:9000", "us-east-1", "chat-media", "access", "secret")
	if err != nil {
		t.Fatal(err)
	}
	presigner.now = func() time.Time { return time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC) }
	got, headers, err := presigner.SignUpload(context.Background(), "media/3f7060f8-46da-4bd6-b6d2-0cac6b111111", "image/jpeg", 1024, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(got)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Path != "/chat-media/media/3f7060f8-46da-4bd6-b6d2-0cac6b111111" {
		t.Fatalf("path=%q", parsed.Path)
	}
	query := parsed.Query()
	if query.Get("X-Amz-Signature") == "" || query.Get("X-Amz-Expires") != "300" {
		t.Fatalf("unexpected signing query: %v", query)
	}
	if query.Get("X-Amz-SignedHeaders") != "content-length;content-type;host" {
		t.Fatalf("signed headers=%q", query.Get("X-Amz-SignedHeaders"))
	}
	if headers["Content-Type"] != "image/jpeg" {
		t.Fatalf("upload response headers=%v URL=%s", headers, got)
	}
}

func TestSignDownloadUsesOnlyOpaqueMediaObjectKey(t *testing.T) {
	presigner, err := New("http://localhost:9000", "us-east-1", "chat-media", "access", "secret")
	if err != nil {
		t.Fatal(err)
	}
	got, err := presigner.SignDownload(context.Background(), "media/3f7060f8-46da-4bd6-b6d2-0cac6b111111", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(got)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Path != "/chat-media/media/3f7060f8-46da-4bd6-b6d2-0cac6b111111" || parsed.Query().Get("X-Amz-Signature") == "" {
		t.Fatalf("unexpected download URL: %s", got)
	}
}

func TestNewRejectsIncompleteConfiguration(t *testing.T) {
	if _, err := New("http://localhost:9000", "us-east-1", "bucket", "access", ""); err == nil {
		t.Fatal("expected incomplete configuration to fail")
	}
}
