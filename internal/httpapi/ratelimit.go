package httpapi

import (
	"math"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// limiterSweepEvery is how often idle buckets are dropped.
	limiterSweepEvery = time.Minute
	// limiterMaxClients bounds the table. A bucket is only kept while its client is below
	// the burst, so reaching this means a very large number of clients are active at once.
	// Clients that arrive while the table is full share one overflow bucket.
	limiterMaxClients = 100_000
	overflowKey       = "\x00overflow"
)

// rateLimiter is a token bucket per client key, safe for concurrent use. A client may send a
// burst of requests and then one every 1/rate seconds.
type rateLimiter struct {
	mu        sync.Mutex
	rate      float64 // tokens added per second
	burst     float64
	clients   map[string]*bucket
	maxKeys   int
	lastSweep time.Time
	now       func() time.Time
}

type bucket struct {
	tokens float64
	last   time.Time
}

func newRateLimiter(perMinute float64, burst int) *rateLimiter {
	return &rateLimiter{
		rate:    perMinute / 60,
		burst:   float64(burst),
		clients: make(map[string]*bucket),
		maxKeys: limiterMaxClients,
		now:     time.Now,
	}
}

// allow takes one token for key. When none is left it returns false and how long until one is.
func (l *rateLimiter) allow(key string) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	l.sweep(now)
	b, ok := l.clients[key]
	if !ok {
		if len(l.clients) >= l.maxKeys {
			// Table full: new clients share one bucket instead of being refused outright.
			key = overflowKey
			b, ok = l.clients[key]
		}
		if !ok {
			b = &bucket{tokens: l.burst, last: now}
			l.clients[key] = b
		}
	}
	b.tokens = math.Min(l.burst, b.tokens+now.Sub(b.last).Seconds()*l.rate)
	b.last = now
	if b.tokens < 1 {
		return false, time.Duration((1 - b.tokens) / l.rate * float64(time.Second))
	}
	b.tokens--
	return true, 0
}

// sweep drops buckets that have been idle long enough to be full again: a missing bucket
// behaves exactly like a full one, so nothing is lost. Called with mu held.
func (l *rateLimiter) sweep(now time.Time) {
	if now.Sub(l.lastSweep) < limiterSweepEvery {
		return
	}
	l.lastSweep = now
	refill := time.Duration(l.burst / l.rate * float64(time.Second))
	for k, b := range l.clients {
		if now.Sub(b.last) >= refill {
			delete(l.clients, k)
		}
	}
}

// clientKey identifies the caller for rate limiting and job caps.
//
// Trust rule: the connection's remote address is always used, unless trustCF is set, in which
// case a valid CF-Connecting-IP header takes precedence. Enable trustCF only when the API can be
// reached solely through Cloudflare (for example a Cloudflare Tunnel to a loopback-published
// port); otherwise any client could forge the header and dodge every per-client limit.
// X-Forwarded-For is never used, since any client can forge it.
//
// The address is normalised so one client cannot obtain many keys: IPv4-mapped IPv6 addresses
// are unmapped, IPv6 zones are dropped, and IPv6 addresses are keyed by their /64 prefix, the
// smallest block a single subscriber typically controls.
func clientKey(r *http.Request, trustCF bool) string {
	var addr netip.Addr
	var found bool
	if trustCF {
		if v := strings.TrimSpace(r.Header.Get("CF-Connecting-IP")); v != "" {
			if a, err := netip.ParseAddr(v); err == nil {
				addr, found = a, true
			}
		}
	}
	if !found {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		a, err := netip.ParseAddr(host)
		if err != nil {
			return host
		}
		addr = a
	}
	addr = addr.Unmap().WithZone("")
	if addr.Is6() {
		return netip.PrefixFrom(addr, 64).Masked().String()
	}
	return addr.String()
}

// rateLimit rejects requests above the per-client limit with a JSON 429 and Retry-After.
func rateLimit(l *rateLimiter, key func(*http.Request) string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ok, wait := l.allow(key(r))
		if !ok {
			w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(wait.Seconds()))))
			writeError(w, http.StatusTooManyRequests, "too many uploads; retry later")
			return
		}
		next(w, r)
	}
}
