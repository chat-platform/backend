package httpadapter

import (
	"net/http"
	"testing"
)

func TestPhoneEndpointsRequireE164(t *testing.T) {
	repo := &groupHTTPRepo{sessionRepoStub: &sessionRepoStub{}}
	server, token := groupTestServer(repo)

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		want   int
	}{
		{name: "request otp accepts e164", method: http.MethodPost, path: "/v1/auth/otp/request", body: `{"phone":"+919876543210"}`, want: http.StatusAccepted},
		{name: "request otp rejects formatted phone", method: http.MethodPost, path: "/v1/auth/otp/request", body: `{"phone":"+91 98765 43210"}`, want: http.StatusBadRequest},
		{name: "verify otp rejects phone without plus", method: http.MethodPost, path: "/v1/auth/otp/verify", body: `{"phone":"919876543210","code":"000000","platform":"WEB"}`, want: http.StatusBadRequest},
		{name: "direct chat accepts e164", method: http.MethodPost, path: "/v1/chats/direct", body: `{"phone":"+919876543210"}`, want: http.StatusOK},
		{name: "direct chat rejects overlong e164", method: http.MethodPost, path: "/v1/chats/direct", body: `{"phone":"+1234567890123456"}`, want: http.StatusBadRequest},
		{name: "user lookup rejects formatted phone", method: http.MethodGet, path: "/v1/users/lookup?phone=%2B44%207911123456", want: http.StatusBadRequest},
		{name: "user lookup accepts e164", method: http.MethodGet, path: "/v1/users/lookup?phone=%2B447911123456", want: http.StatusOK},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := callGroupAPI(server, token, test.method, test.path, test.body)
			if response.Code != test.want {
				t.Fatalf("status=%d, want %d; body=%s", response.Code, test.want, response.Body.String())
			}
		})
	}

}
