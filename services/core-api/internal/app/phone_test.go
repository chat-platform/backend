package app

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestIsE164Phone(t *testing.T) {
	for _, phone := range []string{"+919876543210", "+447911123456", "+123456789012345"} {
		if !IsE164Phone(phone) {
			t.Errorf("IsE164Phone(%q) = false, want true", phone)
		}
	}
	for _, phone := range []string{"", "1234567890", "+1", "+0123456789", "+1234567890123456", "+1 234567890", " +1234567890", "+123-456-7890"} {
		if IsE164Phone(phone) {
			t.Errorf("IsE164Phone(%q) = true, want false", phone)
		}
	}
}

func TestPhoneServiceMethodsRequireE164(t *testing.T) {
	repo := &repoFake{}
	service := New(repo, &cacheFake{}, Config{OTPCode: "000000", Secret: []byte("a sufficiently long test signing secret 123456"), SessionTTL: time.Hour})

	if err := service.RequestOTP(context.Background(), "+1 555 000 0000"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("RequestOTP invalid phone error=%v, want ErrInvalidRequest", err)
	}
	if _, err := service.VerifyOTP(context.Background(), "15550000000", "000000", "WEB", "test", ""); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("VerifyOTP invalid phone error=%v, want ErrInvalidRequest", err)
	}
	if _, err := service.LookupUserByPhone(context.Background(), "+15550000000 "); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("LookupUserByPhone invalid phone error=%v, want ErrInvalidRequest", err)
	}
	if _, err := service.DirectChat(context.Background(), "user-1", "+015550000000"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("DirectChat invalid phone error=%v, want ErrInvalidRequest", err)
	}
	if _, err := service.VerifyOTP(context.Background(), "+15550000000", "000000", "WEB", "test", ""); err != nil {
		t.Fatalf("VerifyOTP valid E.164 phone: %v", err)
	}
}
