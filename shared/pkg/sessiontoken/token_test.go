package sessiontoken

import (
	"testing"
	"time"
)

func TestSignAndVerify(t *testing.T) {
	secret := []byte("a sufficiently long test signing secret 123456")
	now := time.Unix(1_800_000_000, 0)
	want := Claims{SessionID: "sid", UserID: "user", DeviceID: "device", IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Hour).Unix()}
	token, err := Sign(want, secret)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Verify(token, secret, now)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("claims = %+v, want %+v", got, want)
	}
	if _, err := Verify(token, []byte("different sufficiently long secret value"), now); err == nil {
		t.Fatal("expected bad signature rejection")
	}
	if _, err := Verify(token, secret, now.Add(2*time.Hour)); err == nil {
		t.Fatal("expected expired token rejection")
	}
	malformed := token[:len(token)-1] + "!"
	if _, err := Verify(malformed, secret, now); err == nil {
		t.Fatal("expected malformed signature rejection")
	}
}
