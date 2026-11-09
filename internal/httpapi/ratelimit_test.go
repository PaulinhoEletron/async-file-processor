package httpapi

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRateLimiterBurstAndRefill(t *testing.T) {
	now := time.Unix(1000, 0)
	l := newRateLimiter(60, 3) // one token per second, burst 3
	l.now = func() time.Time { return now }
	for i := 0; i < 3; i++ {
		if ok, _ := l.allow("a"); !ok {
			t.Fatalf("request %d within the burst was rejected", i+1)
		}
	}
	ok, wait := l.allow("a")
	if ok || wait <= 0 || wait > time.Second {
		t.Fatalf("4th request: ok=%v wait=%v, want rejected with wait <= 1s", ok, wait)
	}
	if ok, _ := l.allow("b"); !ok {
		t.Fatal("another client must have its own bucket")
	}
	now = now.Add(1100 * time.Millisecond)
	if ok, _ := l.allow("a"); !ok {
		t.Fatal("a token should have refilled after 1.1s")
	}
	if ok, _ := l.allow("a"); ok {
		t.Fatal("only one token should have refilled")
	}
}

func TestRateLimiterEvictsIdleClients(t *testing.T) {
	now := time.Unix(1000, 0)
	l := newRateLimiter(60, 2)
	l.now = func() time.Time { return now }
	for _, k := range []string{"a", "b", "c"} {
		l.allow(k)
	}
	if len(l.clients) != 3 {
		t.Fatalf("clients = %d, want 3", len(l.clients))
	}
	now = now.Add(2 * limiterSweepEvery)
	l.allow("d")
	if len(l.clients) != 1 {
		t.Fatalf("idle clients were not evicted: %d left", len(l.clients))
	}
}

func TestRateLimiterConcurrent(t *testing.T) {
	l := newRateLimiter(1, 5)
	var wg sync.WaitGroup
	var mu sync.Mutex
	allowed := 0
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, _ := l.allow("same"); ok {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if allowed < 5 || allowed > 6 { // 6 only if a token refills during the run
		t.Fatalf("allowed = %d, want 5", allowed)
	}
}

func TestClientKey(t *testing.T) {
	for _, tc := range []struct {
		name, remote, cf, xff string
		trust                 bool
		want                  string
	}{
		{"remote only", "192.0.2.1:5555", "", "", false, "192.0.2.1"},
		{"forged header ignored by default", "192.0.2.1:5555", "203.0.113.7", "", false, "192.0.2.1"},
		{"header honoured when trusted", "127.0.0.1:5555", "203.0.113.7", "", true, "203.0.113.7"},
		{"trusted but absent", "192.0.2.1:5555", "", "", true, "192.0.2.1"},
		{"forwarded-for ignored", "192.0.2.1:5555", "", "1.2.3.4", true, "192.0.2.1"},
		{"invalid header ignored", "192.0.2.1:5555", "not-an-ip", "", true, "192.0.2.1"},
		{"ipv4-mapped header", "127.0.0.1:5555", "::ffff:203.0.113.7", "", true, "203.0.113.7"},
		{"ipv4-mapped remote", "[::ffff:192.0.2.9]:5555", "", "", false, "192.0.2.9"},
		{"ipv6 keyed by /64", "[2001:db8:1:2:aaaa:bbbb:cccc:dddd]:5555", "", "", false, "2001:db8:1:2::/64"},
		{"ipv6 zone stripped", "[fe80::1%eth0]:5555", "", "", false, "fe80::/64"},
		{"ipv6 zone in header", "127.0.0.1:5555", "fe80::1%eth0", "", true, "fe80::/64"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/api/jobs", nil)
			r.RemoteAddr = tc.remote
			if tc.cf != "" {
				r.Header.Set("CF-Connecting-IP", tc.cf)
			}
			if tc.xff != "" {
				r.Header.Set("X-Forwarded-For", tc.xff)
			}
			if got := clientKey(r, tc.trust); got != tc.want {
				t.Fatalf("clientKey = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestClientKeySameIPv6PrefixSharesKey(t *testing.T) {
	key := func(remote string) string {
		r := httptest.NewRequest("POST", "/", nil)
		r.RemoteAddr = remote
		return clientKey(r, false)
	}
	if key("[2001:db8:1:2::1]:1") != key("[2001:db8:1:2:ffff::9]:2") {
		t.Fatal("addresses in one /64 must share a key")
	}
	if key("[2001:db8:1:2::1]:1") == key("[2001:db8:1:3::1]:1") {
		t.Fatal("different /64 prefixes must not share a key")
	}
}

func TestRateLimiterOverflowBucket(t *testing.T) {
	now := time.Unix(1000, 0)
	l := newRateLimiter(60, 2)
	l.maxKeys = 2
	l.now = func() time.Time { return now }
	l.allow("a")
	l.allow("b")
	// The table is full: new clients are not refused but share one bucket.
	for i := 0; i < 2; i++ {
		if ok, _ := l.allow(fmt.Sprintf("new-%d", i)); !ok {
			t.Fatalf("overflow client %d refused while the shared bucket had tokens", i)
		}
	}
	if ok, _ := l.allow("new-9"); ok {
		t.Fatal("the shared overflow bucket should be exhausted")
	}
	if len(l.clients) != 3 {
		t.Fatalf("clients = %d, want 2 + overflow", len(l.clients))
	}
	if ok, _ := l.allow("a"); !ok {
		t.Fatal("an existing client keeps its own bucket")
	}
}

func TestUploadRateLimitResponse(t *testing.T) {
	h := NewHandler(Deps{
		Store: &fakeStore{}, Jobs: &fakeJobs{}, Queue: &fakeQueue{},
		MaxUploadBytes: 1 << 20, UploadRatePerMin: 1, UploadRateBurst: 2, TrustCFConnectingIP: true,
	})
	post := func(ip string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", "/api/jobs", strings.NewReader("x"))
		req.Header.Set("CF-Connecting-IP", ip)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	for i := 0; i < 2; i++ {
		if rec := post("203.0.113.1"); rec.Code == http.StatusTooManyRequests {
			t.Fatalf("request %d limited too early", i+1)
		}
	}
	rec := post("203.0.113.1")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" || rec.Header().Get("Content-Type") != "application/json" ||
		!strings.Contains(rec.Body.String(), `"error"`) {
		t.Fatalf("429 must be JSON with Retry-After: %v %q", rec.Header(), rec.Body.String())
	}
	if rec := post("203.0.113.2"); rec.Code == http.StatusTooManyRequests {
		t.Fatal("a different client must not share the limit")
	}
	// Only uploads are limited.
	req := httptest.NewRequest("GET", "/healthz", nil)
	req.Header.Set("CF-Connecting-IP", "203.0.113.1")
	if rec := httptest.NewRecorder(); func() int { h.ServeHTTP(rec, req); return rec.Code }() != 200 {
		t.Fatal("healthz must not be rate limited")
	}
}

func TestForgedClientIPDoesNotEvadeLimitByDefault(t *testing.T) {
	h := NewHandler(Deps{
		Store: &fakeStore{}, Jobs: &fakeJobs{}, Queue: &fakeQueue{},
		MaxUploadBytes: 1 << 20, UploadRatePerMin: 1, UploadRateBurst: 2,
	})
	var last int
	for i := 0; i < 4; i++ {
		req := httptest.NewRequest("POST", "/api/jobs", strings.NewReader("x"))
		req.RemoteAddr = "192.0.2.50:4000"
		req.Header.Set("CF-Connecting-IP", fmt.Sprintf("203.0.113.%d", i+1))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		last = rec.Code
	}
	if last != http.StatusTooManyRequests {
		t.Fatalf("status after 4 requests with rotating forged headers = %d, want 429", last)
	}
}
