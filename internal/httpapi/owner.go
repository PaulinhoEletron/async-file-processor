package httpapi

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
)

const (
	ownerCookie    = "owner"
	ownerTokenSize = 32
	ownerCookieAge = 30 * 24 * 60 * 60 // seconds
)

var tokenEncoding = base64.RawURLEncoding.Strict()

// ownerFromRequest returns the hash of the request's owner token. ok is false when there is no
// cookie or it is not a well-formed token, so a forged or damaged value never matches a job.
// Only the hash is ever stored or compared; the raw token is not logged.
func ownerFromRequest(r *http.Request) (hash []byte, ok bool) {
	c, err := r.Cookie(ownerCookie)
	if err != nil {
		return nil, false
	}
	raw, err := tokenEncoding.DecodeString(c.Value)
	if err != nil || len(raw) != ownerTokenSize {
		return nil, false
	}
	sum := sha256.Sum256(raw)
	return sum[:], true
}

// newOwnerToken returns a random token and its hash.
func newOwnerToken() (token string, hash []byte, err error) {
	raw := make([]byte, ownerTokenSize)
	if _, err := rand.Read(raw); err != nil {
		return "", nil, err
	}
	sum := sha256.Sum256(raw)
	return tokenEncoding.EncodeToString(raw), sum[:], nil
}

func (a *api) setOwnerCookie(w http.ResponseWriter, token string) {
	http.SetCookie(w, &http.Cookie{
		Name: ownerCookie, Value: token, Path: "/", MaxAge: ownerCookieAge,
		HttpOnly: true, Secure: a.CookieSecure, SameSite: http.SameSiteLaxMode,
	})
}
