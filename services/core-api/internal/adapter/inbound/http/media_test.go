package httpadapter

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/chat-platform/backend/services/core-api/internal/app"
	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
)

type mediaSignerStub struct {
	key, contentType   string
	size               int64
	putCalls, getCalls int
	putErr, getErr     error
}

func (m *mediaSignerStub) SignUpload(_ context.Context, key, contentType string, size int64, _ time.Duration) (string, map[string]string, error) {
	m.putCalls++
	m.key, m.contentType, m.size = key, contentType, size
	if m.putErr != nil {
		return "", nil, m.putErr
	}
	return "https://storage.test/upload", map[string]string{"Content-Type": contentType}, nil
}

func (m *mediaSignerStub) SignDownload(_ context.Context, key string, _ time.Duration) (string, error) {
	m.getCalls++
	m.key = key
	if m.getErr != nil {
		return "", m.getErr
	}
	return "https://storage.test/download", nil
}

func mediaTestServer(t *testing.T, media app.MediaSigner) (*Server, string) {
	t.Helper()
	now := time.Unix(1_800_000_000, 0)
	secret := []byte("a sufficiently long media test signing secret")
	repo := &sessionRepoStub{active: true}
	service := app.NewWithMedia(repo, &sessionCacheStub{status: sessiontoken.StatusActive}, media, app.Config{
		Secret: secret, MediaURLTTL: 5 * time.Minute, Now: func() time.Time { return now },
	})
	server := New(service, testLogger())
	claims := sessiontoken.Claims{SessionID: "sid", UserID: "user", DeviceID: "device", IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Hour).Unix()}
	token, err := sessiontoken.Sign(claims, secret)
	if err != nil {
		t.Fatal(err)
	}
	return server, token
}

func mediaRequest(method, path, body, token string) *http.Request {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	return request
}

func TestCreateMediaUploadSignsOpaqueObjectAndEnforcesCategoryLimit(t *testing.T) {
	signer := &mediaSignerStub{}
	server, token := mediaTestServer(t, signer)
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, mediaRequest(http.MethodPost, "/v1/media/upload-url", `{"category":"image","size":5242880,"contentType":"image/jpeg"}`, token))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), `"mediaId"`) || !strings.Contains(recorder.Body.String(), `"uploadUrl":"https://storage.test/upload"`) || !strings.Contains(recorder.Body.String(), `"method":"PUT"`) {
		t.Fatalf("unexpected upload response: %s", recorder.Body.String())
	}
	if !strings.HasPrefix(signer.key, "media/") || signer.contentType != "image/jpeg" || signer.size != 5<<20 {
		t.Fatalf("unexpected signer arguments: %+v", signer)
	}
	var first struct {
		MediaID string `json:"mediaId"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	recorder = httptest.NewRecorder()
	server.ServeHTTP(recorder, mediaRequest(http.MethodPost, "/v1/media/upload-url", `{"category":"image","size":5242880,"contentType":"image/jpeg"}`, token))
	var second struct {
		MediaID string `json:"mediaId"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &second); err != nil {
		t.Fatal(err)
	}
	if recorder.Code != http.StatusOK || first.MediaID == "" || second.MediaID == "" || first.MediaID == second.MediaID {
		t.Fatalf("media IDs are not unique: first=%q second=%q status=%d", first.MediaID, second.MediaID, recorder.Code)
	}

	recorder = httptest.NewRecorder()
	server.ServeHTTP(recorder, mediaRequest(http.MethodPost, "/v1/media/upload-url", `{"category":"image","size":5242881,"contentType":"image/jpeg"}`, token))
	if recorder.Code != http.StatusBadRequest || signer.putCalls != 2 {
		t.Fatalf("over-limit status=%d signer calls=%d body=%s", recorder.Code, signer.putCalls, recorder.Body.String())
	}
}

func TestCreateMediaUploadRejectsInvalidCategoryAndContentType(t *testing.T) {
	signer := &mediaSignerStub{}
	server, token := mediaTestServer(t, signer)
	for _, body := range []string{
		`{"category":"archive","size":1024,"contentType":"application/zip"}`,
		`{"category":"document","size":1024,"contentType":"not a media type"}`,
	} {
		recorder := httptest.NewRecorder()
		server.ServeHTTP(recorder, mediaRequest(http.MethodPost, "/v1/media/upload-url", body, token))
		if recorder.Code != http.StatusBadRequest {
			t.Errorf("body=%s status=%d response=%s", body, recorder.Code, recorder.Body.String())
		}
	}
	if signer.putCalls != 0 {
		t.Fatalf("signer called for invalid requests: %d", signer.putCalls)
	}
}

func TestMediaEndpointsRequireAuthenticationAndSignDownloadWithoutDatabaseRecord(t *testing.T) {
	signer := &mediaSignerStub{}
	server, token := mediaTestServer(t, signer)
	id := "3f7060f8-46da-4bd6-b6d2-0cac6b111111"
	unauthenticated := httptest.NewRecorder()
	server.ServeHTTP(unauthenticated, httptest.NewRequest(http.MethodGet, "/v1/media/"+id+"/download-url", nil))
	if unauthenticated.Code != http.StatusUnauthorized || signer.getCalls != 0 {
		t.Fatalf("unauthenticated status=%d signer calls=%d", unauthenticated.Code, signer.getCalls)
	}

	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, mediaRequest(http.MethodGet, "/v1/media/"+id+"/download-url", "", token))
	if recorder.Code != http.StatusOK || signer.getCalls != 1 || signer.key != "media/"+id {
		t.Fatalf("download status=%d signer=%+v body=%s", recorder.Code, signer, recorder.Body.String())
	}
}

func TestMediaSigningFailureReturnsUnavailable(t *testing.T) {
	signer := &mediaSignerStub{putErr: errors.New("storage unavailable")}
	server, token := mediaTestServer(t, signer)
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, mediaRequest(http.MethodPost, "/v1/media/upload-url", `{"category":"document","size":1024,"contentType":"application/pdf"}`, token))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}
