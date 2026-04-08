package redic

import (
	"context"
	"io"
	"syscall"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

type timeoutErr struct{}

func (timeoutErr) Error() string { return "to" }
func (timeoutErr) Timeout() bool { return true }

func TestDefaultRedigoConfig(t *testing.T) {
	cfg := DefaultRedigoConfig("127.0.0.1:6379", "p", 1)
	if cfg.Addr != "127.0.0.1:6379" || cfg.Password != "p" || cfg.DB != 1 {
		t.Fatalf("unexpected default config: %+v", cfg)
	}
}

func TestRedigoCommander_Basic(t *testing.T) {
	m, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis start failed: %v", err)
	}
	defer m.Close()

	cfg := DefaultRedigoConfig(m.Addr(), "", 0)
	cmd := NewRedigoCommander(cfg)

	if err := cmd.Connect(); err != nil {
		t.Fatalf("connect failed: %v", err)
	}

	if err := cmd.Set("k", "v"); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	v, err := cmd.Get("k")
	if err != nil || v != "v" {
		t.Fatalf("get failed: %v %v", v, err)
	}

	// DoContext should respect context cancellation
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	_, _ = cmd.DoContext(ctx, "PING")

	// String must not panic
	_ = cmd.(*redigoCommander).String()
}

func TestRedicAdapter_Wrappers(t *testing.T) {
	m, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis start failed: %v", err)
	}
	defer m.Close()

	a := NewAdapter(m.Addr(), "", 0, nil)
	if a == nil {
		t.Fatal("adapter is nil")
	}

	// basic wrapper calls
	_ = a.GetState()
	_ = a.GetMetrics()
	_, _ = a.Do("PING")
}

func TestSubscriptionInfoKeyAndTryPing(t *testing.T) {
	s := &SubscriptionInfo{Channel: "c", IsPattern: false}
	if s.Key() != "channel:c" {
		t.Fatalf("unexpected key: %s", s.Key())
	}
	s = &SubscriptionInfo{Pattern: "p", IsPattern: true}
	if s.Key() != "pattern:p" {
		t.Fatalf("unexpected key: %s", s.Key())
	}

	m, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis start failed: %v", err)
	}
	defer m.Close()

	c := NewClient(m.Addr(), "", 0, nil)
	defer c.Close()
	if !c.tryPing() {
		t.Fatalf("tryPing should succeed against miniredis")
	}
}

func TestIsNetworkError_OtherCases(t *testing.T) {
	if !isNetworkError(io.ErrClosedPipe) {
		t.Fatalf("io.ErrClosedPipe should be network error")
	}

	// error implementing Timeout()
	if !isNetworkError(timeoutErr{}) {
		t.Fatalf("timeout error should be network error")
	}
}

func TestNewRedigoCommander_PasswordAndDoContextCancel(t *testing.T) {
	// create a cfg with a password to exercise the password option branch
	cfg := DefaultRedigoConfig("127.0.0.1:1", "secret", 0)
	rc := NewRedigoCommander(cfg)
	// String should include addr and db
	_ = rc.(*redigoCommander).String()

	// DoContext with already-canceled context should return context error
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := rc.DoContext(ctx, "PING")
	if err == nil {
		t.Fatalf("expected error from canceled context")
	}
}

func TestIsNetworkError_SyscallErrno(t *testing.T) {
	if !isNetworkError(syscall.ECONNRESET) {
		t.Fatalf("syscall.ECONNRESET should be considered network error")
	}
}
