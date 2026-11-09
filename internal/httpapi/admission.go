package httpapi

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

const retryAfterBusy = 30 // seconds advised to clients turned away because of load

// clientKey is the caller's rate-limit key (see clientKey in ratelimit.go).
func (a *api) clientKey(r *http.Request) string { return clientKey(r, a.TrustCFConnectingIP) }

// hashedClientKey is stored on jobs instead of the address itself.
func (a *api) hashedClientKey(r *http.Request) string {
	sum := sha256.Sum256([]byte(a.clientKey(r)))
	return hex.EncodeToString(sum[:])
}

func tooBusy(w http.ResponseWriter, status int, retryAfter int, msg string) {
	w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
	writeError(w, status, msg)
}

// uploadSlots limits how many uploads stream at once. It runs before the body is read, so a
// refused request costs nothing.
func (a *api) uploadSlots(next http.HandlerFunc) http.HandlerFunc {
	if a.slots == nil {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		select {
		case a.slots <- struct{}{}:
			defer func() { <-a.slots }()
		default:
			tooBusy(w, http.StatusServiceUnavailable, 5, "too many uploads in progress, try again shortly")
			return
		}
		next(w, r)
	}
}

// queueCaps refuses new uploads while too many jobs are unfinished, in total (503) or for this
// client (429). The count and the later insert are separate statements, so concurrent requests
// can overshoot a cap slightly; that is acceptable for a safeguard against monopolising the
// workers.
func (a *api) queueCaps(next http.HandlerFunc) http.HandlerFunc {
	if a.MaxUnfinishedJobs <= 0 && a.MaxUnfinishedPerClient <= 0 {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		total, mine, err := a.Jobs.Unfinished(r.Context(), a.hashedClientKey(r))
		if err != nil {
			loggerFrom(r.Context(), a.Logger).Error("count unfinished jobs", "error", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		if a.MaxUnfinishedJobs > 0 && total >= a.MaxUnfinishedJobs {
			tooBusy(w, http.StatusServiceUnavailable, retryAfterBusy, "the processing queue is full, try again later")
			return
		}
		if a.MaxUnfinishedPerClient > 0 && mine >= a.MaxUnfinishedPerClient {
			tooBusy(w, http.StatusTooManyRequests, retryAfterBusy, "you have too many jobs in progress, try again when one finishes")
			return
		}
		next(w, r)
	}
}

// errUploadTooLong is returned by deadlineReader once the overall upload time is used up.
var errUploadTooLong = errors.New("upload exceeded its maximum duration")

// deadlineReader refreshes the connection's read deadline before every Read, so an upload that
// stops sending is cut off after idle, and never lets one run past hard. A zero idle or hard
// disables that bound.
type deadlineReader struct {
	r        io.ReadCloser
	setDead  func(time.Time) error
	idle     time.Duration
	hard     time.Time
	timedOut atomic.Bool
}

func (d *deadlineReader) Read(p []byte) (int, error) {
	now := time.Now()
	if !d.hard.IsZero() && !now.Before(d.hard) {
		d.timedOut.Store(true)
		return 0, errUploadTooLong
	}
	var dl time.Time
	if d.idle > 0 {
		dl = now.Add(d.idle)
	}
	if !d.hard.IsZero() && (dl.IsZero() || dl.After(d.hard)) {
		dl = d.hard
	}
	if !dl.IsZero() {
		_ = d.setDead(dl) // not every ResponseWriter supports deadlines (for example test recorders)
	}
	n, err := d.r.Read(p)
	if err != nil && errors.Is(err, os.ErrDeadlineExceeded) {
		d.timedOut.Store(true)
	}
	return n, err
}

func (d *deadlineReader) Close() error { return d.r.Close() }
