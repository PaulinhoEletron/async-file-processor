// Package healthcheck implements the container health probe: a tiny HTTP GET against the
// local listener, used because the distroless images have no shell, curl or wget.
package healthcheck

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"
)

// URL turns a listen address such as ":8080" or "0.0.0.0:9091" into a loopback URL for path.
func URL(listenAddr, path string) string {
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return "http://" + listenAddr + path
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port) + path
}

// Probe GETs url and returns an error unless it answers 200 within timeout.
func Probe(url string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s returned %s", url, resp.Status)
	}
	return nil
}
